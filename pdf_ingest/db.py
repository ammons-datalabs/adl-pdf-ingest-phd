from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterable, List, Sequence

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from .config import get_settings
from .models import Document, DocumentStatus


@contextmanager
def get_conn():
    settings = get_settings()
    conn = psycopg2.connect(settings.pg_dsn)
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Create documents table if it doesn't exist."""
    ddl = """
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
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
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
