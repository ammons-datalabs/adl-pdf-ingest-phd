from __future__ import annotations

from typing import Any, Dict

from elasticsearch import Elasticsearch

from .config import get_settings
from .models import Document


class ESClient:
    def __init__(self):
        settings = get_settings()
        self.index = settings.es_index
        self.client = Elasticsearch(settings.es_url)

    def ensure_index(self) -> None:
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

    def index_document(self, doc: Document, full_text: str) -> None:
        body: Dict[str, Any] = {
            "title": doc.title,
            "venue": doc.venue,
            "year": doc.year,
            "tags": doc.tags,
            "file_path": str(doc.file_path),
            "full_text": full_text,
        }
        self.client.index(index=self.index, id=doc.id, document=body)
