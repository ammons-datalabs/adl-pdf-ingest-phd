"""
Database round-trip test.

Requires PostgreSQL running (see PG_DSN env var or default localhost:5432).
"""
from pathlib import Path

import pytest

from pdf_ingest.db import (
    init_db,
    register_files,
    fetch_documents_by_status,
    update_status,
)
from pdf_ingest.models import DocumentStatus


@pytest.mark.integration
def test_db_roundtrip(tmp_path):
    init_db()

    fake_pdf = tmp_path / "fake.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\n%fake\n")

    inserted = register_files([fake_pdf])
    assert inserted in (0, 1)

    docs = fetch_documents_by_status([DocumentStatus.NEW])
    match = [d for d in docs if d.file_path == fake_pdf]
    assert match, "registered document not returned with NEW status"

    doc = match[0]
    assert doc.status == DocumentStatus.NEW

    update_status(doc.id, DocumentStatus.INDEXED)

    updated_docs = fetch_documents_by_status([DocumentStatus.INDEXED])
    match2 = [d for d in updated_docs if d.id == doc.id]
    assert match2, "document not updated to INDEXED"
