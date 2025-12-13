"""
Tests for Elasticsearch IndexManager migrations.

Tests the alias-based zero-downtime migration pattern:
- Versioned indices (papers_v1, papers_v2, ...)
- Atomic alias switching
- Rollback capability
- Old version cleanup
"""
from unittest.mock import MagicMock, call

import pytest
from elasticsearch import NotFoundError

from pdf_ingest.es_client import IndexManager, INDEX_MAPPING


@pytest.fixture
def mock_client():
    """Create a mock Elasticsearch client."""
    return MagicMock()


TEST_ALIAS = "test_papers"


@pytest.fixture
def manager(mock_client):
    """Create an IndexManager with mock client using test alias."""
    return IndexManager(mock_client, TEST_ALIAS)


class TestGetVersion:
    """Tests for version extraction from index names."""

    def test_extracts_version_number(self, manager):
        assert manager.get_version(f"{TEST_ALIAS}_v1") == 1
        assert manager.get_version(f"{TEST_ALIAS}_v2") == 2
        assert manager.get_version(f"{TEST_ALIAS}_v10") == 10
        assert manager.get_version(f"{TEST_ALIAS}_v123") == 123

    def test_raises_on_invalid_name(self, manager):
        with pytest.raises(ValueError, match="Invalid versioned index name"):
            manager.get_version(TEST_ALIAS)

        with pytest.raises(ValueError, match="Invalid versioned index name"):
            manager.get_version("invalid")


class TestGenerateIndexName:
    """Tests for index name generation."""

    def test_generates_versioned_name(self, manager):
        assert manager._generate_index_name(1) == f"{TEST_ALIAS}_v1"
        assert manager._generate_index_name(2) == f"{TEST_ALIAS}_v2"
        assert manager._generate_index_name(99) == f"{TEST_ALIAS}_v99"


class TestGetCurrentIndex:
    """Tests for getting the current index behind an alias."""

    def test_returns_index_name_when_alias_exists(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v1": {"aliases": {TEST_ALIAS: {}}}
        }

        result = manager.get_current_index()

        assert result == f"{TEST_ALIAS}_v1"
        mock_client.indices.get_alias.assert_called_once_with(name=TEST_ALIAS)

    def test_returns_none_when_alias_not_found(self, manager, mock_client):
        mock_client.indices.get_alias.side_effect = NotFoundError(
            404, "alias_not_found", "alias not found"
        )

        result = manager.get_current_index()

        assert result is None


class TestInitialize:
    """Tests for index initialization."""

    def test_creates_v1_index_and_alias(self, manager, mock_client):
        # No existing alias
        mock_client.indices.get_alias.side_effect = NotFoundError(
            404, "alias_not_found", "alias not found"
        )

        result = manager.initialize()

        assert result == f"{TEST_ALIAS}_v1"

        # Verify index creation
        mock_client.indices.create.assert_called_once_with(
            index=f"{TEST_ALIAS}_v1",
            settings=INDEX_MAPPING.get("settings", {}),
            mappings=INDEX_MAPPING.get("mappings", {}),
        )

        # Verify alias creation
        mock_client.indices.put_alias.assert_called_once_with(
            index=f"{TEST_ALIAS}_v1", name=TEST_ALIAS
        )

    def test_returns_existing_index_if_already_initialized(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v3": {"aliases": {TEST_ALIAS: {}}}
        }

        result = manager.initialize()

        assert result == f"{TEST_ALIAS}_v3"
        mock_client.indices.create.assert_not_called()
        mock_client.indices.put_alias.assert_not_called()


class TestMigrate:
    """Tests for index migration."""

    def test_creates_new_version_and_switches_alias(self, manager, mock_client):
        # Current index is v1
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v1": {"aliases": {TEST_ALIAS: {}}}
        }
        mock_client.reindex.return_value = {"total": 100, "took": 500}

        result = manager.migrate()

        assert result == f"{TEST_ALIAS}_v2"

        # Verify new index created
        mock_client.indices.create.assert_called_once_with(
            index=f"{TEST_ALIAS}_v2",
            settings=INDEX_MAPPING.get("settings", {}),
            mappings=INDEX_MAPPING.get("mappings", {}),
        )

        # Verify reindex called
        mock_client.reindex.assert_called_once_with(
            source={"index": f"{TEST_ALIAS}_v1"},
            dest={"index": f"{TEST_ALIAS}_v2"},
            wait_for_completion=True,
        )

        # Verify atomic alias switch
        mock_client.indices.update_aliases.assert_called_once_with(
            actions=[
                {"remove": {"index": f"{TEST_ALIAS}_v1", "alias": TEST_ALIAS}},
                {"add": {"index": f"{TEST_ALIAS}_v2", "alias": TEST_ALIAS}},
            ]
        )

        # Verify write block on old index
        mock_client.indices.add_block.assert_called_once_with(
            index=f"{TEST_ALIAS}_v1", block="write"
        )

    def test_initializes_if_no_existing_index(self, manager, mock_client):
        mock_client.indices.get_alias.side_effect = NotFoundError(
            404, "alias_not_found", "alias not found"
        )

        result = manager.migrate()

        assert result == f"{TEST_ALIAS}_v1"
        mock_client.indices.create.assert_called_once()
        mock_client.reindex.assert_not_called()

    def test_migrates_from_v2_to_v3(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v2": {"aliases": {TEST_ALIAS: {}}}
        }
        mock_client.reindex.return_value = {"total": 500, "took": 1000}

        result = manager.migrate()

        assert result == f"{TEST_ALIAS}_v3"
        mock_client.indices.create.assert_called_once()
        assert mock_client.indices.create.call_args[1]["index"] == f"{TEST_ALIAS}_v3"


class TestRollback:
    """Tests for index rollback."""

    def test_switches_alias_back_to_previous_version(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v2": {"aliases": {TEST_ALIAS: {}}}
        }
        mock_client.indices.exists.return_value = True

        result = manager.rollback()

        assert result == f"{TEST_ALIAS}_v1"

        # Verify write block removed from old index
        mock_client.indices.put_settings.assert_called_once_with(
            index=f"{TEST_ALIAS}_v1",
            settings={"index.blocks.write": False},
        )

        # Verify atomic alias switch back
        mock_client.indices.update_aliases.assert_called_once_with(
            actions=[
                {"remove": {"index": f"{TEST_ALIAS}_v2", "alias": TEST_ALIAS}},
                {"add": {"index": f"{TEST_ALIAS}_v1", "alias": TEST_ALIAS}},
            ]
        )

    def test_raises_if_no_index_exists(self, manager, mock_client):
        mock_client.indices.get_alias.side_effect = NotFoundError(
            404, "alias_not_found", "alias not found"
        )

        with pytest.raises(ValueError, match="No index to roll back from"):
            manager.rollback()

    def test_raises_if_already_at_v1(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v1": {"aliases": {TEST_ALIAS: {}}}
        }

        with pytest.raises(ValueError, match="Cannot rollback past v1"):
            manager.rollback()

    def test_raises_if_previous_index_deleted(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v3": {"aliases": {TEST_ALIAS: {}}}
        }
        mock_client.indices.exists.return_value = False

        with pytest.raises(ValueError, match="does not exist"):
            manager.rollback()


class TestDeleteOldVersions:
    """Tests for cleaning up old index versions."""

    def test_deletes_old_versions_keeping_latest_n(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v5": {"aliases": {TEST_ALIAS: {}}}
        }

        result = manager.delete_old_versions(keep_latest=2)

        # Should delete v1, v2, v3 (keeping v4, v5)
        assert result == [f"{TEST_ALIAS}_v1", f"{TEST_ALIAS}_v2", f"{TEST_ALIAS}_v3"]
        assert mock_client.indices.delete.call_count == 3

    def test_returns_empty_if_no_index(self, manager, mock_client):
        mock_client.indices.get_alias.side_effect = NotFoundError(
            404, "alias_not_found", "alias not found"
        )

        result = manager.delete_old_versions()

        assert result == []
        mock_client.indices.delete.assert_not_called()

    def test_handles_already_deleted_indices(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v4": {"aliases": {TEST_ALIAS: {}}}
        }
        # v1 already deleted, v2 exists
        mock_client.indices.delete.side_effect = [
            NotFoundError(404, "not_found", "index not found"),  # v1
            None,  # v2
        ]

        result = manager.delete_old_versions(keep_latest=2)

        # Only v2 was actually deleted
        assert result == [f"{TEST_ALIAS}_v2"]


class TestStatus:
    """Tests for index status reporting."""

    def test_returns_status_when_index_exists(self, manager, mock_client):
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v2": {"aliases": {TEST_ALIAS: {}}}
        }
        mock_client.count.return_value = {"count": 620}
        mock_client.indices.exists.side_effect = [True, True, False]  # v1, v2, v3

        result = manager.status()

        assert result["alias"] == TEST_ALIAS
        assert result["exists"] is True
        assert result["current_index"] == f"{TEST_ALIAS}_v2"
        assert result["version"] == 2
        assert result["document_count"] == 620
        assert result["all_versions"] == [f"{TEST_ALIAS}_v1", f"{TEST_ALIAS}_v2"]

    def test_returns_not_exists_when_no_index(self, manager, mock_client):
        mock_client.indices.get_alias.side_effect = NotFoundError(
            404, "alias_not_found", "alias not found"
        )

        result = manager.status()

        assert result == {"alias": TEST_ALIAS, "exists": False}


class TestMigrationWorkflow:
    """Integration-style tests for complete migration workflows."""

    def test_full_migration_cycle(self, manager, mock_client):
        """Test initialize -> migrate -> migrate -> rollback -> cleanup."""

        # Step 1: Initialize (no existing index)
        mock_client.indices.get_alias.side_effect = NotFoundError(
            404, "not_found", "not found"
        )
        result = manager.initialize()
        assert result == f"{TEST_ALIAS}_v1"

        # Step 2: First migration (v1 -> v2)
        mock_client.indices.get_alias.side_effect = None
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v1": {"aliases": {TEST_ALIAS: {}}}
        }
        mock_client.reindex.return_value = {"total": 100, "took": 500}

        result = manager.migrate()
        assert result == f"{TEST_ALIAS}_v2"

        # Step 3: Second migration (v2 -> v3)
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v2": {"aliases": {TEST_ALIAS: {}}}
        }

        result = manager.migrate()
        assert result == f"{TEST_ALIAS}_v3"

        # Step 4: Rollback (v3 -> v2)
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v3": {"aliases": {TEST_ALIAS: {}}}
        }
        mock_client.indices.exists.return_value = True

        result = manager.rollback()
        assert result == f"{TEST_ALIAS}_v2"

        # Step 5: Cleanup (delete v1, keep v2, v3)
        mock_client.indices.get_alias.return_value = {
            f"{TEST_ALIAS}_v3": {"aliases": {TEST_ALIAS: {}}}
        }

        result = manager.delete_old_versions(keep_latest=2)
        assert result == [f"{TEST_ALIAS}_v1"]