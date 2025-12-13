"""
Integration tests for the full ingest pipeline.

These tests require:
- PostgreSQL running (see PG_DSN env var or default localhost:5432)
- Elasticsearch running (see ES_URL env var or default localhost:9200)

Run with: pytest tests/test_integration.py -v

Run only unit tests (skip integration):
    pytest -m "not integration"

Run only integration tests:
    pytest -m integration
"""
import os
from pathlib import Path

import psycopg2
import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration
from elasticsearch import Elasticsearch

from pdf_ingest.config import get_settings
from pdf_ingest.db import get_conn, init_db, fetch_documents_by_status, register_files
from pdf_ingest.es_client import ESClient
from pdf_ingest.models import DocumentStatus
from pdf_ingest.pipeline import run_pipeline
from pdf_ingest.cleaning import clean_text
from pdf_ingest.extractor import extract_text
from pdf_ingest.queries import search_full_text


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "pdfs"


def _pg_available() -> bool:
    """Check if Postgres is reachable."""
    try:
        settings = get_settings()
        conn = psycopg2.connect(settings.pg_dsn)
        conn.close()
        return True
    except Exception:
        return False


def _es_available() -> bool:
    """Check if Elasticsearch is reachable."""
    try:
        settings = get_settings()
        es = Elasticsearch(settings.es_url)
        return es.ping()
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _pg_available(),
    reason="PostgreSQL not available"
)

requires_elasticsearch = pytest.mark.skipif(
    not _es_available(),
    reason="Elasticsearch not available"
)

requires_services = pytest.mark.skipif(
    not (_pg_available() and _es_available()),
    reason="PostgreSQL and/or Elasticsearch not available"
)


@pytest.fixture
def clean_test_index():
    """Delete and recreate the test ES index before/after test."""
    settings = get_settings()
    es = Elasticsearch(settings.es_url)
    test_index = settings.es_index

    # Delete if exists
    if es.indices.exists(index=test_index):
        es.indices.delete(index=test_index)

    yield test_index

    # Cleanup after test
    if es.indices.exists(index=test_index):
        es.indices.delete(index=test_index)


@pytest.fixture
def clean_test_db():
    """Truncate jobs and documents tables before/after test."""
    init_db()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM jobs")
            cur.execute("DELETE FROM documents")
        conn.commit()

    yield

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM jobs")
            cur.execute("DELETE FROM documents")
        conn.commit()


class TestFixturesExist:
    """Verify test fixtures are in place."""

    def test_fixtures_directory_exists(self):
        assert FIXTURES_DIR.exists(), f"Fixtures dir missing: {FIXTURES_DIR}"

    def test_synthetic_pdfs_exist(self):
        pdfs = list(FIXTURES_DIR.glob("*.pdf"))
        assert len(pdfs) >= 3, f"Expected 3+ PDFs, found {len(pdfs)}"

    def test_pdfs_are_extractable(self):
        """Each PDF should yield non-empty text."""
        for pdf in FIXTURES_DIR.glob("*.pdf"):
            text = extract_text(pdf)
            assert len(text) > 100, f"PDF {pdf.name} yielded too little text"


class TestExtractAndClean:
    """Unit-ish tests for extraction and cleaning on fixtures."""

    def test_chunking_paper_contains_expected_terms(self):
        pdf = FIXTURES_DIR / "synthetic_chunking_2024.pdf"
        text = extract_text(pdf)
        cleaned = clean_text(text)

        assert "content-defined chunking" in cleaned.lower()
        assert "rabin" in cleaned.lower()
        assert "fastcdc" in cleaned.lower()

    def test_secure_dedup_paper_contains_expected_terms(self):
        pdf = FIXTURES_DIR / "synthetic_secure_dedup_2023.pdf"
        text = extract_text(pdf)
        cleaned = clean_text(text)

        assert "message-locked encryption" in cleaned.lower()
        assert "convergent encryption" in cleaned.lower()
        assert "proof-of-ownership" in cleaned.lower()

    def test_backup_paper_contains_expected_terms(self):
        pdf = FIXTURES_DIR / "synthetic_backup_systems_2022.pdf"
        text = extract_text(pdf)
        cleaned = clean_text(text)

        assert "backup" in cleaned.lower()
        assert "deduplication" in cleaned.lower()
        assert "fragmentation" in cleaned.lower()


@requires_postgres
class TestDatabaseOperations:
    """Tests that require PostgreSQL."""

    def test_register_fixture_pdfs(self, clean_test_db):
        """Register fixture PDFs and verify they appear in DB."""
        pdfs = list(FIXTURES_DIR.glob("*.pdf"))

        inserted = register_files(pdfs)
        assert inserted == len(pdfs), f"Expected {len(pdfs)} inserts, got {inserted}"

        docs = fetch_documents_by_status([DocumentStatus.NEW])
        fixture_docs = [d for d in docs if FIXTURES_DIR in d.file_path.parents]
        assert len(fixture_docs) == len(pdfs)

    def test_register_is_idempotent(self, clean_test_db):
        """Registering same files twice doesn't duplicate."""
        pdfs = list(FIXTURES_DIR.glob("*.pdf"))

        first = register_files(pdfs)
        second = register_files(pdfs)

        assert first == len(pdfs)
        assert second == 0, "Second register should insert 0 (already exist)"


@requires_services
class TestFullPipelineIntegration:
    """End-to-end integration tests requiring both Postgres and ES."""

    def test_ingest_fixtures_creates_documents(self, clean_test_db, clean_test_index):
        """
        Ingest fixture PDFs and verify:
        1. Documents appear in Postgres with INDEXED status
        2. Documents are searchable in Elasticsearch
        """
        # Register fixtures manually (since pipeline uses config paths)
        pdfs = list(FIXTURES_DIR.glob("*.pdf"))
        register_files(pdfs)

        # Verify registered as NEW
        new_docs = fetch_documents_by_status([DocumentStatus.NEW])
        fixture_docs = [d for d in new_docs if FIXTURES_DIR in d.file_path.parents]
        assert len(fixture_docs) == len(pdfs), "Fixtures not registered as NEW"

        # Create ES client and ensure index
        es_client = ESClient()
        es_client.ensure_index()

        # Process each document (mimicking pipeline logic)
        for doc in fixture_docs:
            text = extract_text(doc.file_path)
            cleaned = clean_text(text)
            es_client.index_document(doc, cleaned)

            from pdf_ingest.db import update_status
            update_status(doc.id, DocumentStatus.INDEXED)

        # Force ES refresh for immediate searchability
        settings = get_settings()
        es = Elasticsearch(settings.es_url)
        es.indices.refresh(index=settings.es_index)

        # Verify Postgres: all should be INDEXED now
        indexed_docs = fetch_documents_by_status([DocumentStatus.INDEXED])
        fixture_indexed = [d for d in indexed_docs if FIXTURES_DIR in d.file_path.parents]
        assert len(fixture_indexed) == len(pdfs), "Not all fixtures marked INDEXED"

        # Verify ES: search for known terms
        hits = search_full_text("chunking", size=10)
        assert len(hits) >= 1, "Expected at least 1 hit for 'chunking'"

        hits = search_full_text("message-locked encryption", size=10)
        assert len(hits) >= 1, "Expected at least 1 hit for 'message-locked encryption'"

        hits = search_full_text("backup storage", size=10)
        assert len(hits) >= 1, "Expected at least 1 hit for 'backup storage'"

    def test_search_returns_correct_documents(self, clean_test_db, clean_test_index):
        """
        Search for specific terms and verify correct documents are returned.
        """
        # Setup: ingest all fixtures
        pdfs = list(FIXTURES_DIR.glob("*.pdf"))
        register_files(pdfs)

        es_client = ESClient()
        es_client.ensure_index()

        new_docs = fetch_documents_by_status([DocumentStatus.NEW])
        fixture_docs = [d for d in new_docs if FIXTURES_DIR in d.file_path.parents]

        for doc in fixture_docs:
            text = extract_text(doc.file_path)
            cleaned = clean_text(text)
            es_client.index_document(doc, cleaned)
            from pdf_ingest.db import update_status
            update_status(doc.id, DocumentStatus.INDEXED)

        settings = get_settings()
        es = Elasticsearch(settings.es_url)
        es.indices.refresh(index=settings.es_index)

        # Search for FastCDC - should find chunking paper
        hits = search_full_text("FastCDC", size=10)
        assert len(hits) >= 1
        found_paths = [h["_source"]["file_path"] for h in hits]
        assert any("chunking" in p for p in found_paths), \
            f"FastCDC search should find chunking paper, got: {found_paths}"

        # Search for convergent encryption - should find secure dedup paper
        hits = search_full_text("convergent encryption", size=10)
        assert len(hits) >= 1
        found_paths = [h["_source"]["file_path"] for h in hits]
        assert any("secure_dedup" in p for p in found_paths), \
            f"Convergent encryption search should find secure_dedup paper, got: {found_paths}"