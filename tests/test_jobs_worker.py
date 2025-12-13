"""
Tests for job queue and worker functionality.

Requires PostgreSQL running (see PG_DSN env var or default localhost:5432).
"""
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pdf_ingest.db import (
    init_db,
    register_files,
    fetch_documents_by_status,
    create_job,
    fetch_next_pending_job,
    update_job_status,
    fetch_jobs_by_status,
    fetch_document_by_id,
    get_conn,
)
from pdf_ingest.models import DocumentStatus, JobStatus


def _cleanup_pending_jobs():
    """Helper to clean up any pending/processing jobs for test isolation."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = %s WHERE status IN (%s, %s)",
                (JobStatus.DONE.value, JobStatus.PENDING.value, JobStatus.PROCESSING.value),
            )
        conn.commit()


@pytest.mark.integration
def test_create_job_creates_pending_job_for_existing_document(tmp_path):
    """Creating a job for an existing document should result in a PENDING job."""
    init_db()

    fake_pdf = tmp_path / "test_job.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\n%fake\n")

    register_files([fake_pdf])
    docs = fetch_documents_by_status([DocumentStatus.NEW])
    doc = [d for d in docs if d.file_path == fake_pdf][0]

    job_id = create_job(doc.id)

    assert job_id > 0

    pending_jobs = fetch_jobs_by_status([JobStatus.PENDING])
    job = [j for j in pending_jobs if j.id == job_id][0]
    assert job.document_id == doc.id
    assert job.status == JobStatus.PENDING
    assert job.attempts == 0


@pytest.mark.integration
def test_fetch_next_pending_job_claims_and_marks_processing(tmp_path):
    """fetch_next_pending_job should atomically claim a job and mark it PROCESSING."""
    init_db()
    _cleanup_pending_jobs()

    fake_pdf = tmp_path / "test_claim.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\n%fake\n")

    register_files([fake_pdf])
    docs = fetch_documents_by_status([DocumentStatus.NEW])
    doc = [d for d in docs if d.file_path == fake_pdf][0]

    create_job(doc.id)

    job = fetch_next_pending_job()

    assert job is not None
    assert job.status == JobStatus.PROCESSING
    assert job.attempts == 1
    assert job.document_id == doc.id


@pytest.mark.integration
def test_fetch_next_pending_job_returns_none_when_no_pending(tmp_path):
    """When no pending jobs exist, fetch_next_pending_job should return None."""
    init_db()
    _cleanup_pending_jobs()

    fake_pdf = tmp_path / "test_none.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\n%fake\n")

    register_files([fake_pdf])
    docs = fetch_documents_by_status([DocumentStatus.NEW])
    doc = [d for d in docs if d.file_path == fake_pdf][0]

    job_id = create_job(doc.id)
    update_job_status(job_id, JobStatus.DONE)

    job = fetch_next_pending_job()
    assert job is None


@pytest.mark.integration
def test_worker_processes_pending_job_and_marks_done(tmp_path):
    """Worker should process a job and mark both job and document as done/indexed."""
    from pdf_ingest.worker import process_one_job

    init_db()
    _cleanup_pending_jobs()

    fake_pdf = tmp_path / "test_worker_success.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\n%fake\n")

    register_files([fake_pdf])
    docs = fetch_documents_by_status([DocumentStatus.NEW])
    doc = [d for d in docs if d.file_path == fake_pdf][0]

    create_job(doc.id)

    with patch("pdf_ingest.worker.extract_text") as mock_extract, \
         patch("pdf_ingest.worker.ESClient") as mock_es_class:
        mock_extract.return_value = "Some extracted text content"
        mock_es = MagicMock()
        mock_es_class.return_value = mock_es

        result = process_one_job()

    assert result is True

    done_jobs = fetch_jobs_by_status([JobStatus.DONE])
    assert any(j.document_id == doc.id for j in done_jobs)

    updated_doc = fetch_document_by_id(doc.id)
    assert updated_doc.status == DocumentStatus.INDEXED


@pytest.mark.integration
def test_worker_handles_extraction_error_and_marks_failed(tmp_path):
    """Worker should mark job and document as FAILED on extraction error."""
    from pdf_ingest.worker import process_one_job
    from pdf_ingest.extractor import ExtractionError

    init_db()
    _cleanup_pending_jobs()

    fake_pdf = tmp_path / "test_worker_fail.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\n%fake\n")

    register_files([fake_pdf])
    docs = fetch_documents_by_status([DocumentStatus.NEW])
    doc = [d for d in docs if d.file_path == fake_pdf][0]

    create_job(doc.id)

    with patch("pdf_ingest.worker.extract_text") as mock_extract:
        mock_extract.side_effect = ExtractionError("Test extraction failure")

        result = process_one_job()

    assert result is True  # Still processed (even though failed)

    failed_jobs = fetch_jobs_by_status([JobStatus.FAILED])
    job = [j for j in failed_jobs if j.document_id == doc.id][0]
    assert "Test extraction failure" in job.last_error

    updated_doc = fetch_document_by_id(doc.id)
    assert updated_doc.status == DocumentStatus.FAILED
    assert "Test extraction failure" in updated_doc.last_error


@pytest.mark.integration
def test_worker_returns_false_when_no_jobs(tmp_path):
    """Worker should return False when there are no pending jobs."""
    from pdf_ingest.worker import process_one_job

    init_db()
    _cleanup_pending_jobs()

    # Don't create any jobs
    result = process_one_job()

    assert result is False