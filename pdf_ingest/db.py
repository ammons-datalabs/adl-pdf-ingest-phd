from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import get_settings
from .models import Document, DocumentStatus, Job, JobStatus


@contextmanager
def get_conn():
    settings = get_settings()
    conn = psycopg2.connect(settings.pg_dsn)
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Create documents and jobs tables if they don't exist."""
    documents_ddl = """
    CREATE TABLE IF NOT EXISTS documents (
        id SERIAL PRIMARY KEY,
        file_path TEXT UNIQUE NOT NULL,
        title TEXT,
        venue TEXT,
        year INT,
        tags TEXT[] DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'NEW',
        last_error TEXT
    );
    """
    jobs_ddl = """
    CREATE TABLE IF NOT EXISTS jobs (
        id SERIAL PRIMARY KEY,
        document_id INT NOT NULL REFERENCES documents(id),
        status TEXT NOT NULL DEFAULT 'PENDING',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        attempts INT DEFAULT 0,
        last_error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_document_id ON jobs(document_id);
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(documents_ddl)
            cur.execute(jobs_ddl)
        conn.commit()


def register_files(paths: Iterable[Path]) -> int:
    """
    Insert any new file paths as NEW.
    Returns number of newly inserted rows.
    """
    sql = """
    INSERT INTO documents (file_path, status)
    VALUES (%s, %s)
    ON CONFLICT (file_path) DO NOTHING;
    """
    count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for p in paths:
                cur.execute(sql, (str(p), DocumentStatus.NEW.value))
                if cur.rowcount > 0:
                    count += 1
        conn.commit()
    return count


def fetch_documents_by_status(
    statuses: Sequence[DocumentStatus],
    limit: int | None = None,
) -> List[Document]:
    placeholders = ",".join(["%s"] * len(statuses))
    sql = f"""
    SELECT id, file_path, title, venue, year, tags, status, last_error
    FROM documents
    WHERE status IN ({placeholders})
    ORDER BY id
    """
    if limit is not None:
        sql += " LIMIT %s"

    params: list = [s.value for s in statuses]
    if limit is not None:
        params.append(limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    docs: List[Document] = []
    for r in rows:
        docs.append(
            Document(
                id=r["id"],
                file_path=Path(r["file_path"]),
                title=r["title"],
                venue=r["venue"],
                year=r["year"],
                tags=r["tags"] or [],
                status=DocumentStatus(r["status"]),
                last_error=r["last_error"],
            )
        )
    return docs


def update_status(
    doc_id: int,
    status: DocumentStatus,
    last_error: str | None = None,
) -> None:
    sql = """
    UPDATE documents
    SET status = %s,
        last_error = %s
    WHERE id = %s;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status.value, last_error, doc_id))
        conn.commit()


def update_metadata(
    doc_id: int,
    title: str | None = None,
    venue: str | None = None,
    year: int | None = None,
    tags: list[str] | None = None,
) -> None:
    sql = """
    UPDATE documents
    SET title = COALESCE(%s, title),
        venue = COALESCE(%s, venue),
        year = COALESCE(%s, year),
        tags = COALESCE(%s, tags)
    WHERE id = %s;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (title, venue, year, tags, doc_id))
        conn.commit()


# ---------------------------------------------------------------------------
# Job queue functions
# ---------------------------------------------------------------------------


def fetch_document_by_id(doc_id: int) -> Optional[Document]:
    """Fetch a single document by ID."""
    sql = """
    SELECT id, file_path, title, venue, year, tags, status, last_error
    FROM documents
    WHERE id = %s;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (doc_id,))
            row = cur.fetchone()

    if not row:
        return None

    return Document(
        id=row["id"],
        file_path=Path(row["file_path"]),
        title=row["title"],
        venue=row["venue"],
        year=row["year"],
        tags=row["tags"] or [],
        status=DocumentStatus(row["status"]),
        last_error=row["last_error"],
    )


def create_job(document_id: int) -> int:
    """
    Create a new job for a document.
    Returns the job ID.
    """
    sql = """
    INSERT INTO jobs (document_id, status)
    VALUES (%s, %s)
    RETURNING id;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (document_id, JobStatus.PENDING.value))
            job_id = cur.fetchone()[0]
        conn.commit()
    return job_id


def fetch_next_pending_job() -> Optional[Job]:
    """
    Atomically claim the next pending job using FOR UPDATE SKIP LOCKED.
    Marks the job as PROCESSING and increments attempts.
    Returns the Job or None if no pending jobs.
    """
    sql = """
    UPDATE jobs
    SET status = %s,
        attempts = attempts + 1,
        updated_at = NOW()
    WHERE id = (
        SELECT id FROM jobs
        WHERE status = %s
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING id, document_id, status, created_at, updated_at, attempts, last_error;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (JobStatus.PROCESSING.value, JobStatus.PENDING.value))
            row = cur.fetchone()
        conn.commit()

    if not row:
        return None

    return Job(
        id=row["id"],
        document_id=row["document_id"],
        status=JobStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        attempts=row["attempts"],
        last_error=row["last_error"],
    )


def update_job_status(
    job_id: int,
    status: JobStatus,
    last_error: str | None = None,
) -> None:
    """Update a job's status and optionally set last_error."""
    sql = """
    UPDATE jobs
    SET status = %s,
        last_error = %s,
        updated_at = NOW()
    WHERE id = %s;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status.value, last_error, job_id))
        conn.commit()


def fetch_jobs_by_status(
    statuses: Sequence[JobStatus],
    limit: int | None = None,
) -> List[Job]:
    """Fetch jobs by status (for testing/inspection)."""
    placeholders = ",".join(["%s"] * len(statuses))
    sql = f"""
    SELECT id, document_id, status, created_at, updated_at, attempts, last_error
    FROM jobs
    WHERE status IN ({placeholders})
    ORDER BY created_at
    """
    if limit is not None:
        sql += " LIMIT %s"

    params: list = [s.value for s in statuses]
    if limit is not None:
        params.append(limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        Job(
            id=r["id"],
            document_id=r["document_id"],
            status=JobStatus(r["status"]),
            created_at=r["created_at"],
            updated_at=r["updated_at"],
            attempts=r["attempts"],
            last_error=r["last_error"],
        )
        for r in rows
    ]
