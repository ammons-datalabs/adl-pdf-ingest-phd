from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import get_settings
from .models import (
    Document,
    Enhancement,
    EnhancementType,
    PendingEnhancement,
    PendingEnhancementStatus,
)


def _sanitize_for_jsonb(obj: Any) -> Any:
    """
    Recursively sanitize an object for PostgreSQL JSONB storage.
    Removes null bytes and other characters that JSONB doesn't support.
    """
    if isinstance(obj, str):
        # Remove null bytes and other problematic Unicode characters
        # PostgreSQL JSONB doesn't support \u0000
        return obj.replace("\x00", "").replace("\u0000", "")
    elif isinstance(obj, dict):
        return {k: _sanitize_for_jsonb(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_for_jsonb(item) for item in obj]
    return obj


@contextmanager
def get_conn():
    settings = get_settings()
    conn = psycopg2.connect(settings.pg_dsn)
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Create documents, enhancements, and pending_enhancements tables."""
    documents_ddl = """
    CREATE TABLE IF NOT EXISTS documents (
        id SERIAL PRIMARY KEY,
        file_path TEXT UNIQUE NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """

    enhancements_ddl = """
    CREATE TABLE IF NOT EXISTS enhancements (
        id SERIAL PRIMARY KEY,
        document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
        enhancement_type TEXT NOT NULL,
        content JSONB NOT NULL,
        robot_id TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(document_id, enhancement_type, robot_id)
    );
    CREATE INDEX IF NOT EXISTS idx_enhancements_document_id ON enhancements(document_id);
    CREATE INDEX IF NOT EXISTS idx_enhancements_type ON enhancements(enhancement_type);
    """

    pending_enhancements_ddl = """
    CREATE TABLE IF NOT EXISTS pending_enhancements (
        id SERIAL PRIMARY KEY,
        document_id INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
        enhancement_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        attempts INT DEFAULT 0,
        last_error TEXT,
        UNIQUE(document_id, enhancement_type)
    );
    CREATE INDEX IF NOT EXISTS idx_pending_enhancements_status ON pending_enhancements(status);
    CREATE INDEX IF NOT EXISTS idx_pending_enhancements_type ON pending_enhancements(enhancement_type);
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(documents_ddl)
            cur.execute(enhancements_ddl)
            cur.execute(pending_enhancements_ddl)
        conn.commit()


# ---------------------------------------------------------------------------
# Document functions
# ---------------------------------------------------------------------------


def register_document(file_path: Path) -> Optional[int]:
    """
    Register a document. Returns document ID, or None if already exists.
    """
    sql = """
    INSERT INTO documents (file_path)
    VALUES (%s)
    ON CONFLICT (file_path) DO NOTHING
    RETURNING id;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (str(file_path),))
            row = cur.fetchone()
        conn.commit()
    return row[0] if row else None


def register_files(paths: Iterable[Path]) -> int:
    """
    Register multiple documents. Returns count of newly inserted.
    """
    sql = """
    INSERT INTO documents (file_path)
    VALUES (%s)
    ON CONFLICT (file_path) DO NOTHING;
    """
    count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for p in paths:
                cur.execute(sql, (str(p),))
                if cur.rowcount > 0:
                    count += 1
        conn.commit()
    return count


def fetch_document_by_id(doc_id: int) -> Optional[Document]:
    """Fetch a single document by ID."""
    sql = """
    SELECT id, file_path, created_at
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
        created_at=row["created_at"],
    )


def fetch_document_by_path(file_path: Path) -> Optional[Document]:
    """Fetch a document by file path."""
    sql = """
    SELECT id, file_path, created_at
    FROM documents
    WHERE file_path = %s;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (str(file_path),))
            row = cur.fetchone()

    if not row:
        return None

    return Document(
        id=row["id"],
        file_path=Path(row["file_path"]),
        created_at=row["created_at"],
    )


def fetch_all_documents(limit: Optional[int] = None) -> List[Document]:
    """Fetch all documents."""
    sql = "SELECT id, file_path, created_at FROM documents ORDER BY id"
    if limit:
        sql += f" LIMIT {limit}"

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    return [
        Document(
            id=r["id"],
            file_path=Path(r["file_path"]),
            created_at=r["created_at"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Enhancement functions
# ---------------------------------------------------------------------------


def create_enhancement(
    document_id: int,
    enhancement_type: EnhancementType,
    content: dict[str, Any],
    robot_id: str,
) -> int:
    """
    Create an enhancement record. Upserts on conflict.
    Returns enhancement ID.
    """
    sql = """
    INSERT INTO enhancements (document_id, enhancement_type, content, robot_id)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (document_id, enhancement_type, robot_id)
    DO UPDATE SET content = EXCLUDED.content, created_at = NOW()
    RETURNING id;
    """
    sanitized_content = _sanitize_for_jsonb(content)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (document_id, enhancement_type.value, json.dumps(sanitized_content), robot_id))
            enhancement_id = cur.fetchone()[0]
        conn.commit()
    return enhancement_id


def fetch_enhancements_for_document(document_id: int) -> List[Enhancement]:
    """Fetch all enhancements for a document."""
    sql = """
    SELECT id, document_id, enhancement_type, content, robot_id, created_at
    FROM enhancements
    WHERE document_id = %s
    ORDER BY created_at;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (document_id,))
            rows = cur.fetchall()

    return [
        Enhancement(
            id=r["id"],
            document_id=r["document_id"],
            enhancement_type=EnhancementType(r["enhancement_type"]),
            content=r["content"],
            robot_id=r["robot_id"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


def fetch_enhancement(
    document_id: int,
    enhancement_type: EnhancementType,
) -> Optional[Enhancement]:
    """Fetch a specific enhancement by document and type."""
    sql = """
    SELECT id, document_id, enhancement_type, content, robot_id, created_at
    FROM enhancements
    WHERE document_id = %s AND enhancement_type = %s
    ORDER BY created_at DESC
    LIMIT 1;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (document_id, enhancement_type.value))
            row = cur.fetchone()

    if not row:
        return None

    return Enhancement(
        id=row["id"],
        document_id=row["document_id"],
        enhancement_type=EnhancementType(row["enhancement_type"]),
        content=row["content"],
        robot_id=row["robot_id"],
        created_at=row["created_at"],
    )


# ---------------------------------------------------------------------------
# PendingEnhancement functions (state machine)
# ---------------------------------------------------------------------------


def create_pending_enhancement(
    document_id: int,
    enhancement_type: EnhancementType,
) -> int:
    """
    Create a pending enhancement request.
    Returns pending enhancement ID, or existing ID if already exists.
    """
    sql = """
    INSERT INTO pending_enhancements (document_id, enhancement_type, status)
    VALUES (%s, %s, %s)
    ON CONFLICT (document_id, enhancement_type) DO UPDATE
    SET status = CASE
        WHEN pending_enhancements.status IN ('COMPLETED', 'FAILED', 'EXPIRED', 'DISCARDED', 'INDEXING_FAILED')
        THEN 'PENDING'
        ELSE pending_enhancements.status
    END,
    updated_at = NOW()
    RETURNING id;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (document_id, enhancement_type.value, PendingEnhancementStatus.PENDING.value))
            pending_id = cur.fetchone()[0]
        conn.commit()
    return pending_id


def fetch_next_pending(
    enhancement_type: EnhancementType,
) -> Optional[PendingEnhancement]:
    """
    Atomically claim the next pending enhancement using FOR UPDATE SKIP LOCKED.
    Moves status from PENDING to PROCESSING.
    """
    sql = """
    UPDATE pending_enhancements
    SET status = %s,
        attempts = attempts + 1,
        updated_at = NOW()
    WHERE id = (
        SELECT id FROM pending_enhancements
        WHERE status = %s AND enhancement_type = %s
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING id, document_id, enhancement_type, status, created_at, updated_at, attempts, last_error;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (
                PendingEnhancementStatus.PROCESSING.value,
                PendingEnhancementStatus.PENDING.value,
                enhancement_type.value,
            ))
            row = cur.fetchone()
        conn.commit()

    if not row:
        return None

    return PendingEnhancement(
        id=row["id"],
        document_id=row["document_id"],
        enhancement_type=EnhancementType(row["enhancement_type"]),
        status=PendingEnhancementStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        attempts=row["attempts"],
        last_error=row["last_error"],
    )


def update_pending_status(
    pending_id: int,
    status: PendingEnhancementStatus,
    last_error: Optional[str] = None,
) -> None:
    """Update pending enhancement status."""
    sql = """
    UPDATE pending_enhancements
    SET status = %s,
        last_error = %s,
        updated_at = NOW()
    WHERE id = %s;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status.value, last_error, pending_id))
        conn.commit()


def fetch_pending_by_status(
    statuses: Sequence[PendingEnhancementStatus],
    enhancement_type: Optional[EnhancementType] = None,
    limit: Optional[int] = None,
) -> List[PendingEnhancement]:
    """Fetch pending enhancements by status."""
    placeholders = ",".join(["%s"] * len(statuses))
    sql = f"""
    SELECT id, document_id, enhancement_type, status, created_at, updated_at, attempts, last_error
    FROM pending_enhancements
    WHERE status IN ({placeholders})
    """
    params: list = [s.value for s in statuses]

    if enhancement_type:
        sql += " AND enhancement_type = %s"
        params.append(enhancement_type.value)

    sql += " ORDER BY created_at"

    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        PendingEnhancement(
            id=r["id"],
            document_id=r["document_id"],
            enhancement_type=EnhancementType(r["enhancement_type"]),
            status=PendingEnhancementStatus(r["status"]),
            created_at=r["created_at"],
            updated_at=r["updated_at"],
            attempts=r["attempts"],
            last_error=r["last_error"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Bulk operations for ES sync
# ---------------------------------------------------------------------------


def fetch_documents_with_enhancements(
    document_ids: Optional[List[int]] = None,
    limit: Optional[int] = None,
) -> List[tuple[Document, List[Enhancement]]]:
    """
    Fetch documents with their enhancements for ES indexing.
    Returns list of (Document, [Enhancement]) tuples.
    """
    doc_sql = "SELECT id, file_path, created_at FROM documents"
    if document_ids:
        placeholders = ",".join(["%s"] * len(document_ids))
        doc_sql += f" WHERE id IN ({placeholders})"
    doc_sql += " ORDER BY id"
    if limit:
        doc_sql += f" LIMIT {limit}"

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if document_ids:
                cur.execute(doc_sql, document_ids)
            else:
                cur.execute(doc_sql)
            doc_rows = cur.fetchall()

            if not doc_rows:
                return []

            doc_ids = [r["id"] for r in doc_rows]
            enh_placeholders = ",".join(["%s"] * len(doc_ids))
            enh_sql = f"""
            SELECT id, document_id, enhancement_type, content, robot_id, created_at
            FROM enhancements
            WHERE document_id IN ({enh_placeholders})
            ORDER BY document_id, created_at;
            """
            cur.execute(enh_sql, doc_ids)
            enh_rows = cur.fetchall()

    # Group enhancements by document_id
    enh_by_doc: dict[int, List[Enhancement]] = {}
    for r in enh_rows:
        enh = Enhancement(
            id=r["id"],
            document_id=r["document_id"],
            enhancement_type=EnhancementType(r["enhancement_type"]),
            content=r["content"],
            robot_id=r["robot_id"],
            created_at=r["created_at"],
        )
        enh_by_doc.setdefault(r["document_id"], []).append(enh)

    results = []
    for r in doc_rows:
        doc = Document(
            id=r["id"],
            file_path=Path(r["file_path"]),
            created_at=r["created_at"],
        )
        results.append((doc, enh_by_doc.get(r["id"], [])))

    return results