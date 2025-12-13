"""
Worker process for processing document jobs from the queue.

Usage:
    python -m pdf_ingest.worker

Or via CLI:
    python cli.py run-worker
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from .cleaning import clean_text
from .db import (
    fetch_document_by_id,
    fetch_next_pending_job,
    init_db,
    update_job_status,
    update_status,
)
from .es_client import ESClient
from .extractor import ExtractionError, extract_text
from .models import DocumentStatus, JobStatus, Job

logger = logging.getLogger(__name__)


def process_one_job() -> bool:
    """
    Claim and process a single pending job.

    Returns True if a job was processed (success or failure),
    False if no pending jobs were available.
    """
    job = fetch_next_pending_job()
    if job is None:
        return False

    logger.info("Processing job id=%s for document_id=%s", job.id, job.document_id)

    doc = fetch_document_by_id(job.document_id)
    if doc is None:
        logger.error("Document id=%s not found for job id=%s", job.document_id, job.id)
        update_job_status(job.id, JobStatus.FAILED, last_error="Document not found")
        return True

    try:
        # Extract text
        text = extract_text(doc.file_path)
        if not text.strip():
            raise ExtractionError("Empty text extracted")

        # Clean text
        cleaned = clean_text(text)
        if not cleaned.strip():
            raise ExtractionError("Empty text after cleaning")

        # Index to Elasticsearch
        es = ESClient()
        es.ensure_index()
        es.index_document(doc, cleaned)

        # Mark success
        update_status(doc.id, DocumentStatus.INDEXED, last_error=None)
        update_job_status(job.id, JobStatus.DONE)
        logger.info("Job id=%s completed successfully", job.id)

    except ExtractionError as e:
        error_msg = str(e)
        logger.warning("Extraction error for job id=%s: %s", job.id, error_msg)
        update_status(doc.id, DocumentStatus.FAILED, last_error=error_msg)
        update_job_status(job.id, JobStatus.FAILED, last_error=error_msg)

    except Exception as e:
        error_msg = str(e)
        logger.exception("Unexpected error for job id=%s: %s", job.id, error_msg)
        update_status(doc.id, DocumentStatus.FAILED, last_error=error_msg)
        update_job_status(job.id, JobStatus.FAILED, last_error=error_msg)

    return True


def run_worker_loop(
    poll_interval: float = 1.0,
    max_iterations: Optional[int] = None,
) -> None:
    """
    Continuously poll for and process jobs.

    Args:
        poll_interval: Seconds to wait between polls when no jobs available.
        max_iterations: If set, stop after this many iterations (for testing).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logger.info("Worker starting...")
    init_db()

    iterations = 0
    while True:
        if max_iterations is not None and iterations >= max_iterations:
            logger.info("Reached max iterations (%d), stopping.", max_iterations)
            break

        processed = process_one_job()
        iterations += 1

        if not processed:
            logger.debug("No pending jobs, sleeping for %.1fs", poll_interval)
            time.sleep(poll_interval)


if __name__ == "__main__":
    run_worker_loop()