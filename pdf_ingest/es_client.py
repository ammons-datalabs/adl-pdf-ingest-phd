"""
Elasticsearch client with alias-based zero-downtime migrations.

Pattern:
- Application always queries the alias (e.g., "papers")
- Actual data lives in versioned indices (papers_v1, papers_v2, ...)
- Migrations create new index, reindex data, atomically switch alias
- Old indices kept for rollback
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

from .config import get_settings
from .models import (
    Document,
    Enhancement,
    get_full_text,
    get_metadata,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Index Mapping (versioned)
# =============================================================================
# Bump this when mapping changes require migration
INDEX_VERSION = 2

INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            # Core bibliographic fields
            "title": {
                "type": "text",
                "analyzer": "english",
                "fields": {"raw": {"type": "keyword"}},
            },
            "abstract": {
                "type": "text",
                "analyzer": "english",
            },
            "authors": {
                "type": "text",
                "fields": {"raw": {"type": "keyword"}},
            },
            "keywords": {
                "type": "keyword",
            },
            "venue": {"type": "keyword"},
            "year": {"type": "integer"},
            "tags": {"type": "keyword"},
            "item_type": {"type": "keyword"},
            # Identifiers
            "doi": {"type": "keyword"},
            "arxiv_id": {"type": "keyword"},
            # File info
            "file_path": {"type": "keyword"},
            # Full text content
            "full_text": {"type": "text"},
        }
    }
}


# =============================================================================
# Index Manager (alias-based migrations)
# =============================================================================
class IndexManager:
    """
    Manages ES index with zero-downtime migrations via aliases.

    Usage:
        manager = IndexManager(client, "papers")
        manager.initialize()  # Creates papers_v1, alias "papers" -> papers_v1
        manager.migrate()     # Creates papers_v2, reindexes, switches alias
        manager.rollback()    # Switches alias back to papers_v1
    """

    def __init__(self, client: Elasticsearch, alias: str):
        self.client = client
        self.alias = alias

    def get_current_index(self) -> Optional[str]:
        """Get the actual index behind the alias, or None if alias doesn't exist."""
        try:
            alias_info = self.client.indices.get_alias(name=self.alias)
            # Returns dict like {"papers_v1": {"aliases": {"papers": {}}}}
            return list(alias_info.keys())[0] if alias_info else None
        except NotFoundError:
            return None

    def get_version(self, index_name: str) -> int:
        """Extract version from index name like 'papers_v3' -> 3."""
        if "_v" not in index_name:
            raise ValueError(f"Invalid versioned index name: {index_name}")
        return int(index_name.split("_v")[-1])

    def _generate_index_name(self, version: int) -> str:
        """Generate versioned index name."""
        return f"{self.alias}_v{version}"

    def initialize(self) -> str:
        """
        Initialize index if none exists.

        Creates v1 index with current mapping and points alias to it.
        Returns the index name.
        """
        current = self.get_current_index()
        if current:
            logger.info("Index already exists: %s -> %s", self.alias, current)
            return current

        index_name = self._generate_index_name(1)
        logger.info("Creating initial index: %s", index_name)

        self.client.indices.create(
            index=index_name,
            settings=INDEX_MAPPING.get("settings", {}),
            mappings=INDEX_MAPPING.get("mappings", {}),
        )

        self.client.indices.put_alias(index=index_name, name=self.alias)
        logger.info("Created alias: %s -> %s", self.alias, index_name)

        return index_name

    def migrate(self) -> str:
        """
        Migrate to a new index version with updated mapping.

        Steps:
        1. Create new index with current mapping
        2. Reindex data from old to new (ES server-side)
        3. Atomically switch alias
        4. Block writes to old index

        Returns the new index name.
        """
        old_index = self.get_current_index()
        if not old_index:
            logger.info("No existing index, initializing...")
            return self.initialize()

        old_version = self.get_version(old_index)
        new_version = old_version + 1
        new_index = self._generate_index_name(new_version)

        logger.info("Migrating %s -> %s", old_index, new_index)

        # 1. Create new index with updated mapping
        logger.info("Creating new index with updated mapping...")
        self.client.indices.create(
            index=new_index,
            settings=INDEX_MAPPING.get("settings", {}),
            mappings=INDEX_MAPPING.get("mappings", {}),
        )

        # 2. Reindex data (ES does this server-side)
        logger.info("Reindexing data...")
        result = self.client.reindex(
            source={"index": old_index},
            dest={"index": new_index},
            wait_for_completion=True,
        )
        logger.info(
            "Reindexed %d documents (took %dms)",
            result.get("total", 0),
            result.get("took", 0),
        )

        # 3. Atomic alias switch
        logger.info("Switching alias atomically...")
        self.client.indices.update_aliases(
            actions=[
                {"remove": {"index": old_index, "alias": self.alias}},
                {"add": {"index": new_index, "alias": self.alias}},
            ]
        )

        # 4. Block writes to old index (safety)
        logger.info("Blocking writes to old index...")
        self.client.indices.add_block(index=old_index, block="write")

        logger.info("Migration complete: %s -> %s", old_index, new_index)
        return new_index

    def rollback(self) -> str:
        """
        Roll back to previous index version.

        Returns the rolled-back-to index name.
        """
        current = self.get_current_index()
        if not current:
            raise ValueError("No index to roll back from")

        version = self.get_version(current)
        if version <= 1:
            raise ValueError("Cannot rollback past v1")

        old_index = self._generate_index_name(version - 1)

        # Check old index exists
        if not self.client.indices.exists(index=old_index):
            raise ValueError(f"Previous index {old_index} does not exist")

        logger.info("Rolling back %s -> %s", current, old_index)

        # Remove write block from old index
        self.client.indices.put_settings(
            index=old_index,
            settings={"index.blocks.write": False},
        )

        # Atomic switch back
        self.client.indices.update_aliases(
            actions=[
                {"remove": {"index": current, "alias": self.alias}},
                {"add": {"index": old_index, "alias": self.alias}},
            ]
        )

        logger.info("Rollback complete: %s -> %s", current, old_index)
        return old_index

    def delete_old_versions(self, keep_latest: int = 2) -> List[str]:
        """
        Delete old index versions, keeping N most recent.

        Returns list of deleted index names.
        """
        current = self.get_current_index()
        if not current:
            return []

        current_version = self.get_version(current)
        deleted = []

        for v in range(1, current_version - keep_latest + 1):
            old_index = self._generate_index_name(v)
            try:
                self.client.indices.delete(index=old_index)
                deleted.append(old_index)
                logger.info("Deleted old index: %s", old_index)
            except NotFoundError:
                pass  # Already deleted

        return deleted

    def delete_all(self) -> List[str]:
        """Delete all versioned indices and the alias. Use for full rebuild."""
        deleted = []

        # Find and delete all versioned indices
        for v in range(1, 100):  # Reasonable upper bound
            index_name = self._generate_index_name(v)
            try:
                if self.client.indices.exists(index=index_name):
                    self.client.indices.delete(index=index_name)
                    deleted.append(index_name)
                    logger.info("Deleted index: %s", index_name)
                else:
                    break  # No more versions
            except Exception as e:
                logger.warning("Failed to delete %s: %s", index_name, e)
                break

        return deleted

    def status(self) -> Dict[str, Any]:
        """Get current index status."""
        current = self.get_current_index()
        if not current:
            return {"alias": self.alias, "exists": False}

        # Get document count
        count = self.client.count(index=self.alias)["count"]

        # Find all versioned indices
        all_indices = []
        for v in range(1, 100):  # Reasonable upper bound
            idx = self._generate_index_name(v)
            if self.client.indices.exists(index=idx):
                all_indices.append(idx)
            elif v > self.get_version(current):
                break

        return {
            "alias": self.alias,
            "exists": True,
            "current_index": current,
            "version": self.get_version(current),
            "document_count": count,
            "all_versions": all_indices,
        }


# =============================================================================
# ES Client
# =============================================================================
class ESClient:
    """Elasticsearch client for document indexing and search."""

    def __init__(self):
        settings = get_settings()
        self.alias = settings.es_index  # Treated as alias, not direct index
        self.client = Elasticsearch(settings.es_url)
        self.manager = IndexManager(self.client, self.alias)

    def ensure_index(self) -> None:
        """Ensure index exists with alias."""
        self.manager.initialize()

    def index_document_with_enhancements(
        self,
        doc: Document,
        enhancements: List[Enhancement],
    ) -> None:
        """
        Index a document using data from its enhancements.

        Builds the ES document body from:
        - FULL_TEXT enhancement -> full_text field
        - PAPERPILE_METADATA enhancement -> title, venue, year, tags, etc.
        """
        full_text = get_full_text(enhancements) or ""
        metadata = get_metadata(enhancements)

        body: Dict[str, Any] = {
            "title": metadata.get("title"),
            "abstract": metadata.get("abstract"),
            "authors": metadata.get("authors", []),
            "keywords": metadata.get("keywords", []),
            "venue": metadata.get("venue"),
            "year": metadata.get("year"),
            "tags": metadata.get("tags", []),
            "item_type": metadata.get("item_type"),
            "doi": metadata.get("doi"),
            "arxiv_id": metadata.get("arxiv_id"),
            "file_path": str(doc.file_path),
            "full_text": full_text,
        }
        self.client.index(index=self.alias, id=doc.id, document=body)

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
                    "_index": self.alias,
                    "_id": doc.id,
                    "_source": {
                        "title": metadata.get("title"),
                        "abstract": metadata.get("abstract"),
                        "authors": metadata.get("authors", []),
                        "keywords": metadata.get("keywords", []),
                        "venue": metadata.get("venue"),
                        "year": metadata.get("year"),
                        "tags": metadata.get("tags", []),
                        "item_type": metadata.get("item_type"),
                        "doi": metadata.get("doi"),
                        "arxiv_id": metadata.get("arxiv_id"),
                        "file_path": str(doc.file_path),
                        "full_text": full_text,
                    },
                }

        success, errors = bulk(self.client, generate_actions(), raise_on_error=False)
        if errors:
            logger.warning("Bulk index had %d errors", len(errors))
        return success

    def delete_index(self) -> None:
        """Delete all versioned indices (for full rebuild)."""
        self.manager.delete_all()

    def refresh(self) -> None:
        """Refresh the index for immediate searchability."""
        self.client.indices.refresh(index=self.alias)


# =============================================================================
# Bulk sync function
# =============================================================================
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