from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from .config import get_settings
from .models import (
    Document,
    Enhancement,
    EnhancementType,
    get_full_text,
    get_metadata,
)

logger = logging.getLogger(__name__)


class ESClient:
    def __init__(self):
        settings = get_settings()
        self.index = settings.es_index
        self.client = Elasticsearch(settings.es_url)

    def ensure_index(self) -> None:
        """Create index with mappings if it doesn't exist."""
        if self.client.indices.exists(index=self.index):
            return

        mappings = {
            "properties": {
                "title": {
                    "type": "text",
                    "fields": {"raw": {"type": "keyword"}},
                },
                "venue": {"type": "keyword"},
                "year": {"type": "integer"},
                "tags": {"type": "keyword"},
                "file_path": {"type": "keyword"},
                "full_text": {"type": "text"},
            }
        }
        self.client.indices.create(index=self.index, mappings=mappings)

    def index_document_with_enhancements(
        self,
        doc: Document,
        enhancements: List[Enhancement],
    ) -> None:
        """
        Index a document using data from its enhancements.

        Builds the ES document body from:
        - FULL_TEXT enhancement → full_text field
        - PAPERPILE_METADATA enhancement → title, venue, year, tags
        """
        # Extract data from enhancements
        full_text = get_full_text(enhancements) or ""
        metadata = get_metadata(enhancements)

        body: Dict[str, Any] = {
            "title": metadata.get("title"),
            "venue": metadata.get("venue"),
            "year": metadata.get("year"),
            "tags": metadata.get("tags", []),
            "file_path": str(doc.file_path),
            "full_text": full_text,
        }
        self.client.index(index=self.index, id=doc.id, document=body)

    def bulk_index(
        self,
        docs_with_enhancements: List[tuple[Document, List[Enhancement]]],
    ) -> int:
        """
        Bulk index documents with their enhancements.

        Returns count of successfully indexed documents.
        """
        def generate_actions():
            for doc, enhancements in docs_with_enhancements:
                full_text = get_full_text(enhancements) or ""
                metadata = get_metadata(enhancements)

                yield {
                    "_index": self.index,
                    "_id": doc.id,
                    "_source": {
                        "title": metadata.get("title"),
                        "venue": metadata.get("venue"),
                        "year": metadata.get("year"),
                        "tags": metadata.get("tags", []),
                        "file_path": str(doc.file_path),
                        "full_text": full_text,
                    },
                }

        success, errors = bulk(self.client, generate_actions(), raise_on_error=False)
        if errors:
            logger.warning("Bulk index had %d errors", len(errors))
        return success

    def delete_index(self) -> None:
        """Delete the index if it exists."""
        if self.client.indices.exists(index=self.index):
            self.client.indices.delete(index=self.index)

    def refresh(self) -> None:
        """Refresh the index for immediate searchability."""
        self.client.indices.refresh(index=self.index)


def bulk_sql_to_es(document_ids: Optional[List[int]] = None) -> int:
    """
    Sync documents from SQL to Elasticsearch.

    ES index is a derived view from SQL (source of truth).

    Args:
        document_ids: If provided, only sync these documents. Otherwise sync all.

    Returns:
        Count of documents indexed.
    """
    from .db import fetch_documents_with_enhancements

    logger.info("Starting bulk SQL to ES sync...")

    docs_with_enhancements = fetch_documents_with_enhancements(document_ids=document_ids)
    logger.info("Fetched %d documents with enhancements", len(docs_with_enhancements))

    if not docs_with_enhancements:
        return 0

    es = ESClient()
    es.ensure_index()

    count = es.bulk_index(docs_with_enhancements)
    es.refresh()

    logger.info("Indexed %d documents to ES", count)
    return count