from __future__ import annotations

import logging
from typing import Sequence

from . import discover
from .cleaning import clean_text
from .db import fetch_documents_by_status, init_db, update_status
from .es_client import ESClient
from .extractor import ExtractionError, extract_text
from .models import DocumentStatus

logger = logging.getLogger(__name__)


def run_pipeline(batch_size: int | None = 50) -> None:
    """
    Orchestrate:
    - ensure DB schema
    - discover PDFs -> register in DB
    - ensure ES index
    - pull NEW/FAILED docs, extract, index, update status
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    logger.info("Initialising database schema...")
    init_db()

    logger.info("Discovering PDFs in processing directory...")
    new_count = discover.sync_files()
    logger.info("Discovery complete. %d new files registered.", new_count)

    es = ESClient()
    logger.info("Ensuring Elasticsearch index...")
    es.ensure_index()

    # Only process NEW documents (don't retry FAILED - they need manual intervention)
    statuses: Sequence[DocumentStatus] = [DocumentStatus.NEW]

    while True:
        docs = fetch_documents_by_status(statuses, limit=batch_size)
        if not docs:
            logger.info("No more NEW/FAILED documents to process. Done.")
            break

        logger.info("Processing batch of %d documents", len(docs))

        for doc in docs:
            logger.info("Processing doc id=%s path=%s", doc.id, doc.file_path)
            try:
                text = extract_text(doc.file_path)
                if not text.strip():
                    raise ExtractionError("Empty text extracted")

                cleaned = clean_text(text)
                if not cleaned.strip():
                    raise ExtractionError("Empty text after cleaning")

                es.index_document(doc, cleaned)
                update_status(doc.id, DocumentStatus.INDEXED, last_error=None)
            except ExtractionError as e:
                logger.warning("Extraction error for id=%s: %s", doc.id, e)
                update_status(doc.id, DocumentStatus.FAILED, last_error=str(e))
            except Exception as e:
                logger.exception("Unexpected error for id=%s: %s", doc.id, e)
                update_status(doc.id, DocumentStatus.FAILED, last_error=str(e))
