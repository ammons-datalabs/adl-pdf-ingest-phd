"""
Tests for metadata loading and application.
"""
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pdf_ingest.metadata import load_manifest, apply_manifest_to_db, ManifestRow
from pdf_ingest.models import Document, DocumentStatus


class TestLoadManifest:
    """Tests for load_manifest function."""

    def test_loads_complete_row(self, tmp_path):
        """Happy path: loads a CSV with all fields populated."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "paper.pdf,My Paper Title,SIGMOD,2023,Tag1;Tag2\n"
        )

        rows = load_manifest(csv_file)

        assert len(rows) == 1
        assert rows[0].file_name == "paper.pdf"
        assert rows[0].title == "My Paper Title"
        assert rows[0].venue == "SIGMOD"
        assert rows[0].year == 2023
        assert rows[0].tags == ["Tag1", "Tag2"]

    def test_handles_missing_optional_fields(self, tmp_path):
        """Loads rows with missing title/venue/year/tags."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "paper.pdf,,,,\n"
        )

        rows = load_manifest(csv_file)

        assert len(rows) == 1
        assert rows[0].file_name == "paper.pdf"
        assert rows[0].title is None
        assert rows[0].venue is None
        assert rows[0].year is None
        assert rows[0].tags == []

    def test_skips_rows_without_filename(self, tmp_path):
        """Rows without file_name are skipped."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            ",Some Title,Venue,2023,Tag\n"
            "valid.pdf,Title,Venue,2024,\n"
        )

        rows = load_manifest(csv_file)

        assert len(rows) == 1
        assert rows[0].file_name == "valid.pdf"

    def test_parses_multiple_tags(self, tmp_path):
        """Tags are split by semicolon and trimmed."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "paper.pdf,Title,Venue,2023,  Tag1 ; Tag2;Tag3  \n"
        )

        rows = load_manifest(csv_file)

        assert rows[0].tags == ["Tag1", "Tag2", "Tag3"]

    def test_handles_empty_csv(self, tmp_path):
        """Empty CSV (header only) returns empty list."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text("file_name,title,venue,year,tags\n")

        rows = load_manifest(csv_file)

        assert rows == []

    def test_strips_whitespace(self, tmp_path):
        """Whitespace is stripped from all fields."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "  paper.pdf  ,  Title  ,  Venue  ,  2023  ,  Tag  \n"
        )

        rows = load_manifest(csv_file)

        assert rows[0].file_name == "paper.pdf"
        assert rows[0].title == "Title"
        assert rows[0].venue == "Venue"
        assert rows[0].year == 2023
        assert rows[0].tags == ["Tag"]


class TestApplyManifestToDb:
    """Tests for apply_manifest_to_db with mocked dependencies."""

    @patch("pdf_ingest.metadata.get_settings")
    @patch("pdf_ingest.metadata.fetch_documents_by_status")
    @patch("pdf_ingest.metadata.update_metadata")
    @patch("pdf_ingest.metadata.update_status")
    def test_updates_matching_document(
        self, mock_update_status, mock_update_metadata, mock_fetch, mock_settings, tmp_path
    ):
        """Document matching manifest row gets metadata updated."""
        # Setup
        processing_dir = tmp_path / "processing"
        processing_dir.mkdir()

        mock_settings.return_value = MagicMock(pdf_processing=processing_dir)

        doc = Document(
            id=1,
            file_path=processing_dir / "paper.pdf",
            title=None,
            venue=None,
            year=None,
            tags=[],
            status=DocumentStatus.NEW,
            last_error=None,
        )
        mock_fetch.return_value = [doc]

        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "paper.pdf,New Title,VLDB,2023,Dedup;Storage\n"
        )

        # Execute
        updated = apply_manifest_to_db(csv_file, reset_status=True)

        # Verify
        assert updated == 1
        mock_update_metadata.assert_called_once_with(
            doc_id=1,
            title="New Title",
            venue="VLDB",
            year=2023,
            tags=["Dedup", "Storage"],
        )
        mock_update_status.assert_called_once_with(1, DocumentStatus.NEW, last_error=None)

    @patch("pdf_ingest.metadata.get_settings")
    @patch("pdf_ingest.metadata.fetch_documents_by_status")
    @patch("pdf_ingest.metadata.update_metadata")
    @patch("pdf_ingest.metadata.update_status")
    def test_skips_document_not_in_manifest(
        self, mock_update_status, mock_update_metadata, mock_fetch, mock_settings, tmp_path
    ):
        """Document not in manifest is not updated."""
        processing_dir = tmp_path / "processing"
        processing_dir.mkdir()

        mock_settings.return_value = MagicMock(pdf_processing=processing_dir)

        doc = Document(
            id=1,
            file_path=processing_dir / "unknown.pdf",
            title=None,
            venue=None,
            year=None,
            tags=[],
            status=DocumentStatus.NEW,
            last_error=None,
        )
        mock_fetch.return_value = [doc]

        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "other.pdf,Title,Venue,2023,Tag\n"
        )

        updated = apply_manifest_to_db(csv_file, reset_status=True)

        assert updated == 0
        mock_update_metadata.assert_not_called()

    @patch("pdf_ingest.metadata.get_settings")
    @patch("pdf_ingest.metadata.fetch_documents_by_status")
    @patch("pdf_ingest.metadata.update_metadata")
    @patch("pdf_ingest.metadata.update_status")
    def test_skips_document_outside_processing_dir(
        self, mock_update_status, mock_update_metadata, mock_fetch, mock_settings, tmp_path
    ):
        """Document outside processing directory is not updated."""
        processing_dir = tmp_path / "processing"
        processing_dir.mkdir()
        other_dir = tmp_path / "other"
        other_dir.mkdir()

        mock_settings.return_value = MagicMock(pdf_processing=processing_dir)

        doc = Document(
            id=1,
            file_path=other_dir / "paper.pdf",
            title=None,
            venue=None,
            year=None,
            tags=[],
            status=DocumentStatus.NEW,
            last_error=None,
        )
        mock_fetch.return_value = [doc]

        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "paper.pdf,Title,Venue,2023,Tag\n"
        )

        updated = apply_manifest_to_db(csv_file, reset_status=True)

        assert updated == 0
        mock_update_metadata.assert_not_called()

    @patch("pdf_ingest.metadata.get_settings")
    @patch("pdf_ingest.metadata.fetch_documents_by_status")
    @patch("pdf_ingest.metadata.update_metadata")
    @patch("pdf_ingest.metadata.update_status")
    def test_no_status_reset_when_disabled(
        self, mock_update_status, mock_update_metadata, mock_fetch, mock_settings, tmp_path
    ):
        """Status is not reset when reset_status=False."""
        processing_dir = tmp_path / "processing"
        processing_dir.mkdir()

        mock_settings.return_value = MagicMock(pdf_processing=processing_dir)

        doc = Document(
            id=1,
            file_path=processing_dir / "paper.pdf",
            title=None,
            venue=None,
            year=None,
            tags=[],
            status=DocumentStatus.INDEXED,
            last_error=None,
        )
        mock_fetch.return_value = [doc]

        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "paper.pdf,Title,Venue,2023,Tag\n"
        )

        updated = apply_manifest_to_db(csv_file, reset_status=False)

        assert updated == 1
        mock_update_metadata.assert_called_once()
        mock_update_status.assert_not_called()

    @patch("pdf_ingest.metadata.get_settings")
    @patch("pdf_ingest.metadata.fetch_documents_by_status")
    @patch("pdf_ingest.metadata.update_metadata")
    @patch("pdf_ingest.metadata.update_status")
    def test_case_insensitive_filename_matching(
        self, mock_update_status, mock_update_metadata, mock_fetch, mock_settings, tmp_path
    ):
        """Filename matching is case-insensitive."""
        processing_dir = tmp_path / "processing"
        processing_dir.mkdir()

        mock_settings.return_value = MagicMock(pdf_processing=processing_dir)

        doc = Document(
            id=1,
            file_path=processing_dir / "PAPER.PDF",
            title=None,
            venue=None,
            year=None,
            tags=[],
            status=DocumentStatus.NEW,
            last_error=None,
        )
        mock_fetch.return_value = [doc]

        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_name,title,venue,year,tags\n"
            "paper.pdf,Title,Venue,2023,Tag\n"
        )

        updated = apply_manifest_to_db(csv_file, reset_status=True)

        assert updated == 1
        mock_update_metadata.assert_called_once()
