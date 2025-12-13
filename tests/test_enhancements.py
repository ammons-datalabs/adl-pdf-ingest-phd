"""
Tests for the enhancement model and robots.

Requires PostgreSQL running (see PG_DSN env var or default localhost:5432).
"""
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pdf_ingest.db import (
    init_db,
    get_conn,
    register_files,
    register_document,
    fetch_document_by_id,
    fetch_document_by_path,
    fetch_all_documents,
    fetch_documents_with_enhancements,
    create_enhancement,
    fetch_enhancements_for_document,
    fetch_enhancement,
    create_pending_enhancement,
    fetch_next_pending,
    update_pending_status,
    fetch_pending_by_status,
)
from pdf_ingest.models import (
    EnhancementType,
    PendingEnhancementStatus,
    get_full_text,
    get_metadata,
)


def _cleanup_tables():
    """Clean up test data."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pending_enhancements")
            cur.execute("DELETE FROM enhancements")
            cur.execute("DELETE FROM documents")
        conn.commit()


@pytest.mark.integration
class TestDocumentRegistration:
    """Test document registration."""

    def test_register_document(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_doc.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")

        count = register_files([fake_pdf])
        assert count == 1

        docs = fetch_all_documents()
        assert len(docs) == 1
        assert docs[0].file_path == fake_pdf

    def test_register_is_idempotent(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_idem.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")

        count1 = register_files([fake_pdf])
        count2 = register_files([fake_pdf])

        assert count1 == 1
        assert count2 == 0


@pytest.mark.integration
class TestEnhancements:
    """Test enhancement CRUD."""

    def test_create_and_fetch_enhancement(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_enh.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        # Create full_text enhancement
        content = {"text": "Hello world", "raw_length": 100}
        enh_id = create_enhancement(
            document_id=doc.id,
            enhancement_type=EnhancementType.FULL_TEXT,
            content=content,
            robot_id="test-robot",
        )
        assert enh_id > 0

        # Fetch enhancement
        enhancements = fetch_enhancements_for_document(doc.id)
        assert len(enhancements) == 1
        assert enhancements[0].enhancement_type == EnhancementType.FULL_TEXT
        assert enhancements[0].content["text"] == "Hello world"
        assert enhancements[0].robot_id == "test-robot"

    def test_create_multiple_enhancement_types(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_multi.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        # Create full_text enhancement
        create_enhancement(
            document_id=doc.id,
            enhancement_type=EnhancementType.FULL_TEXT,
            content={"text": "Extracted text"},
            robot_id="pdf-extractor",
        )

        # Create metadata enhancement
        create_enhancement(
            document_id=doc.id,
            enhancement_type=EnhancementType.PAPERPILE_METADATA,
            content={"title": "Test Paper", "year": 2024, "tags": ["test"]},
            robot_id="paperpile-sync",
        )

        # Fetch all enhancements
        enhancements = fetch_enhancements_for_document(doc.id)
        assert len(enhancements) == 2

        # Test helper functions
        full_text = get_full_text(enhancements)
        assert full_text == "Extracted text"

        metadata = get_metadata(enhancements)
        assert metadata["title"] == "Test Paper"
        assert metadata["year"] == 2024
        assert metadata["tags"] == ["test"]


@pytest.mark.integration
class TestPendingEnhancements:
    """Test pending enhancement state machine."""

    def test_create_pending_enhancement(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_pending.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        pending_id = create_pending_enhancement(doc.id, EnhancementType.FULL_TEXT)
        assert pending_id > 0

        pending_list = fetch_pending_by_status([PendingEnhancementStatus.PENDING])
        assert len(pending_list) >= 1
        pending = [p for p in pending_list if p.id == pending_id][0]
        assert pending.document_id == doc.id
        assert pending.enhancement_type == EnhancementType.FULL_TEXT
        assert pending.status == PendingEnhancementStatus.PENDING

    def test_fetch_next_pending_claims_and_processes(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_claim.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        create_pending_enhancement(doc.id, EnhancementType.FULL_TEXT)

        # Claim the pending enhancement
        pending = fetch_next_pending(EnhancementType.FULL_TEXT)
        assert pending is not None
        assert pending.status == PendingEnhancementStatus.PROCESSING
        assert pending.attempts == 1

    def test_state_machine_transitions(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_states.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        pending_id = create_pending_enhancement(doc.id, EnhancementType.FULL_TEXT)

        # PENDING -> PROCESSING (via fetch_next_pending)
        pending = fetch_next_pending(EnhancementType.FULL_TEXT)
        assert pending.status == PendingEnhancementStatus.PROCESSING

        # PROCESSING -> IMPORTING
        update_pending_status(pending.id, PendingEnhancementStatus.IMPORTING)
        pending_list = fetch_pending_by_status([PendingEnhancementStatus.IMPORTING])
        assert any(p.id == pending.id for p in pending_list)

        # IMPORTING -> COMPLETED
        update_pending_status(pending.id, PendingEnhancementStatus.COMPLETED)
        pending_list = fetch_pending_by_status([PendingEnhancementStatus.COMPLETED])
        assert any(p.id == pending.id for p in pending_list)


@pytest.mark.integration
class TestPdfExtractorRobot:
    """Test PDF extractor robot."""

    def test_process_one_creates_enhancement(self, tmp_path):
        from pdf_ingest.robots.pdf_extractor import process_one

        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_robot.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        create_pending_enhancement(doc.id, EnhancementType.FULL_TEXT)

        with patch("pdf_ingest.robots.pdf_extractor.extract_text") as mock_extract:
            mock_extract.return_value = "Extracted text content"

            result = process_one()

        assert result is True

        # Check enhancement was created
        enhancements = fetch_enhancements_for_document(doc.id)
        full_text_enh = [e for e in enhancements if e.enhancement_type == EnhancementType.FULL_TEXT]
        assert len(full_text_enh) == 1
        assert "Extracted text content" in full_text_enh[0].content["text"]

        # Check pending status is COMPLETED
        pending_list = fetch_pending_by_status([PendingEnhancementStatus.COMPLETED])
        assert any(p.document_id == doc.id for p in pending_list)

    def test_process_one_handles_error(self, tmp_path):
        from pdf_ingest.robots.pdf_extractor import process_one
        from pdf_ingest.extractor import ExtractionError

        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "test_error.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        create_pending_enhancement(doc.id, EnhancementType.FULL_TEXT)

        with patch("pdf_ingest.robots.pdf_extractor.extract_text") as mock_extract:
            mock_extract.side_effect = ExtractionError("Test error")

            result = process_one()

        assert result is True

        # Check pending status is FAILED
        pending_list = fetch_pending_by_status([PendingEnhancementStatus.FAILED])
        pending = [p for p in pending_list if p.document_id == doc.id][0]
        assert "Test error" in pending.last_error


@pytest.mark.integration
class TestPaperpileSyncRobot:
    """Test Paperpile sync robot with tracking."""

    def test_process_one_creates_metadata_enhancement(self, tmp_path):
        from pdf_ingest.robots.paperpile_sync import process_one, load_manifest

        init_db()
        _cleanup_tables()

        # Create a fake PDF with a name that matches the manifest
        fake_pdf = tmp_path / "Test Paper 2024.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        # Create pending enhancement for metadata
        create_pending_enhancement(doc.id, EnhancementType.PAPERPILE_METADATA)

        # Create a fake manifest
        manifest_csv = tmp_path / "manifest.csv"
        manifest_csv.write_text(
            "file_name,title,venue,year,tags\n"
            "Test Paper 2024.pdf,A Test Paper,Test Conference,2024,tag1;tag2\n"
        )
        manifest_map = load_manifest(manifest_csv)

        result = process_one(manifest_map)

        assert result == "completed"

        # Check enhancement was created
        enhancements = fetch_enhancements_for_document(doc.id)
        meta_enh = [e for e in enhancements if e.enhancement_type == EnhancementType.PAPERPILE_METADATA]
        assert len(meta_enh) == 1
        assert meta_enh[0].content["title"] == "A Test Paper"
        assert meta_enh[0].content["year"] == 2024
        assert meta_enh[0].content["tags"] == ["tag1", "tag2"]

        # Check pending status is COMPLETED
        pending_list = fetch_pending_by_status([PendingEnhancementStatus.COMPLETED])
        assert any(p.document_id == doc.id for p in pending_list)

    def test_process_one_discards_when_no_manifest_match(self, tmp_path):
        from pdf_ingest.robots.paperpile_sync import process_one, load_manifest

        init_db()
        _cleanup_tables()

        # Create a fake PDF with a name NOT in the manifest
        fake_pdf = tmp_path / "Unknown Paper.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        # Create pending enhancement for metadata
        create_pending_enhancement(doc.id, EnhancementType.PAPERPILE_METADATA)

        # Create an empty manifest (no matching entries)
        manifest_csv = tmp_path / "manifest.csv"
        manifest_csv.write_text("file_name,title,venue,year,tags\n")
        manifest_map = load_manifest(manifest_csv)

        result = process_one(manifest_map)

        assert result == "discarded"

        # Check NO enhancement was created
        enhancements = fetch_enhancements_for_document(doc.id)
        assert len(enhancements) == 0

        # Check pending status is DISCARDED
        pending_list = fetch_pending_by_status([PendingEnhancementStatus.DISCARDED])
        pending = [p for p in pending_list if p.document_id == doc.id][0]
        assert "No manifest entry found" in pending.last_error


@pytest.mark.integration
class TestDocumentFetchFunctions:
    """Tests for document fetch functions."""

    def test_register_document_returns_id(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "single_doc.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")

        doc_id = register_document(fake_pdf)
        assert doc_id is not None
        assert doc_id > 0

    def test_register_document_returns_none_on_duplicate(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "dup_doc.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")

        doc_id1 = register_document(fake_pdf)
        doc_id2 = register_document(fake_pdf)

        assert doc_id1 is not None
        assert doc_id2 is None

    def test_fetch_document_by_path(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "by_path.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        doc = fetch_document_by_path(fake_pdf)
        assert doc is not None
        assert doc.file_path == fake_pdf

    def test_fetch_document_by_path_returns_none_for_missing(self, tmp_path):
        init_db()
        _cleanup_tables()

        missing_path = tmp_path / "nonexistent.pdf"
        doc = fetch_document_by_path(missing_path)
        assert doc is None

    def test_fetch_document_by_id_returns_none_for_missing(self):
        init_db()
        _cleanup_tables()

        doc = fetch_document_by_id(99999)
        assert doc is None

    def test_fetch_all_documents_with_limit(self, tmp_path):
        init_db()
        _cleanup_tables()

        # Create multiple documents
        for i in range(5):
            pdf = tmp_path / f"doc_{i}.pdf"
            pdf.write_bytes(b"%PDF-1.4\n%test\n")
            register_document(pdf)

        docs = fetch_all_documents(limit=3)
        assert len(docs) == 3

        all_docs = fetch_all_documents()
        assert len(all_docs) == 5


@pytest.mark.integration
class TestEnhancementFetchFunctions:
    """Tests for enhancement fetch functions."""

    def test_fetch_enhancement_by_type(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "fetch_enh.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        create_enhancement(
            document_id=doc.id,
            enhancement_type=EnhancementType.FULL_TEXT,
            content={"text": "Test text"},
            robot_id="test-robot",
        )

        enh = fetch_enhancement(doc.id, EnhancementType.FULL_TEXT)
        assert enh is not None
        assert enh.content["text"] == "Test text"

    def test_fetch_enhancement_returns_none_for_missing(self, tmp_path):
        init_db()
        _cleanup_tables()

        fake_pdf = tmp_path / "no_enh.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([fake_pdf])

        docs = fetch_all_documents()
        doc = docs[0]

        enh = fetch_enhancement(doc.id, EnhancementType.FULL_TEXT)
        assert enh is None

    def test_fetch_documents_with_enhancements(self, tmp_path):
        init_db()
        _cleanup_tables()

        # Create two documents with enhancements
        pdf1 = tmp_path / "doc1.pdf"
        pdf2 = tmp_path / "doc2.pdf"
        pdf1.write_bytes(b"%PDF-1.4\n%test\n")
        pdf2.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([pdf1, pdf2])

        docs = fetch_all_documents()

        # Add enhancements to first doc
        create_enhancement(
            document_id=docs[0].id,
            enhancement_type=EnhancementType.FULL_TEXT,
            content={"text": "Doc 1 text"},
            robot_id="extractor",
        )
        create_enhancement(
            document_id=docs[0].id,
            enhancement_type=EnhancementType.PAPERPILE_METADATA,
            content={"title": "Doc 1"},
            robot_id="paperpile",
        )

        # Add enhancement to second doc
        create_enhancement(
            document_id=docs[1].id,
            enhancement_type=EnhancementType.FULL_TEXT,
            content={"text": "Doc 2 text"},
            robot_id="extractor",
        )

        results = fetch_documents_with_enhancements()
        assert len(results) == 2

        # Check first doc has 2 enhancements
        doc1_result = [r for r in results if r[0].id == docs[0].id][0]
        assert len(doc1_result[1]) == 2

        # Check second doc has 1 enhancement
        doc2_result = [r for r in results if r[0].id == docs[1].id][0]
        assert len(doc2_result[1]) == 1

    def test_fetch_documents_with_enhancements_by_ids(self, tmp_path):
        init_db()
        _cleanup_tables()

        pdf1 = tmp_path / "sel1.pdf"
        pdf2 = tmp_path / "sel2.pdf"
        pdf1.write_bytes(b"%PDF-1.4\n%test\n")
        pdf2.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([pdf1, pdf2])

        docs = fetch_all_documents()

        # Only fetch the first doc
        results = fetch_documents_with_enhancements(document_ids=[docs[0].id])
        assert len(results) == 1
        assert results[0][0].id == docs[0].id

    def test_fetch_documents_with_enhancements_empty(self):
        init_db()
        _cleanup_tables()

        results = fetch_documents_with_enhancements()
        assert results == []


@pytest.mark.integration
class TestPendingEnhancementFilters:
    """Tests for pending enhancement filters."""

    def test_fetch_pending_by_status_with_type_filter(self, tmp_path):
        init_db()
        _cleanup_tables()

        pdf1 = tmp_path / "filter1.pdf"
        pdf2 = tmp_path / "filter2.pdf"
        pdf1.write_bytes(b"%PDF-1.4\n%test\n")
        pdf2.write_bytes(b"%PDF-1.4\n%test\n")
        register_files([pdf1, pdf2])

        docs = fetch_all_documents()

        # Create different types
        create_pending_enhancement(docs[0].id, EnhancementType.FULL_TEXT)
        create_pending_enhancement(docs[1].id, EnhancementType.PAPERPILE_METADATA)

        # Filter by type
        full_text_pending = fetch_pending_by_status(
            [PendingEnhancementStatus.PENDING],
            enhancement_type=EnhancementType.FULL_TEXT,
        )
        assert len(full_text_pending) == 1
        assert full_text_pending[0].enhancement_type == EnhancementType.FULL_TEXT

    def test_fetch_pending_by_status_with_limit(self, tmp_path):
        init_db()
        _cleanup_tables()

        # Create multiple documents
        for i in range(5):
            pdf = tmp_path / f"limit_{i}.pdf"
            pdf.write_bytes(b"%PDF-1.4\n%test\n")
            register_document(pdf)

        docs = fetch_all_documents()
        for doc in docs:
            create_pending_enhancement(doc.id, EnhancementType.FULL_TEXT)

        # Fetch with limit
        pending = fetch_pending_by_status(
            [PendingEnhancementStatus.PENDING],
            limit=2,
        )
        assert len(pending) == 2

    def test_fetch_next_pending_returns_none_when_empty(self):
        init_db()
        _cleanup_tables()

        pending = fetch_next_pending(EnhancementType.FULL_TEXT)
        assert pending is None