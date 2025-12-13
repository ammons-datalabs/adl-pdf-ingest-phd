from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List

from .config import get_settings
from .db import fetch_documents_by_status, update_metadata, update_status
from .models import DocumentStatus


@dataclass
class ManifestRow:
    file_name: str
    title: str | None
    venue: str | None
    year: int | None
    tags: list[str]


def load_manifest(path: Path) -> List[ManifestRow]:
    rows: List[ManifestRow] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            file_name = (r.get("file_name") or "").strip()
            if not file_name:
                continue

            title = (r.get("title") or "").strip() or None
            venue = (r.get("venue") or "").strip() or None

            year_str = (r.get("year") or "").strip()
            year = int(year_str) if year_str else None

            tags_raw = r.get("tags") or ""
            tags = [t.strip() for t in tags_raw.split(";") if t.strip()]

            rows.append(
                ManifestRow(
                    file_name=file_name,
                    title=title,
                    venue=venue,
                    year=year,
                    tags=tags,
                )
            )
    return rows


def apply_manifest_to_db(
    manifest_path: Path,
    reset_status: bool = True,
) -> int:
    """
    Apply metadata to documents based on file_name match.
    Optionally reset status to NEW so the pipeline will re-index.
    Returns count of updated documents.
    """
    settings = get_settings()
    root = settings.pdf_processing

    manifest = load_manifest(manifest_path)
    manifest_map = {row.file_name.lower(): row for row in manifest}

    # Pull all docs regardless of status
    docs = fetch_documents_by_status(
        [DocumentStatus.NEW, DocumentStatus.INDEXED, DocumentStatus.FAILED],
        limit=None,
    )

    updated = 0
    for doc in docs:
        doc_path = doc.file_path

        # Only touch docs under the relevant root
        try:
            # Python 3.9+: is_relative_to
            if not doc_path.is_relative_to(root):
                continue
        except AttributeError:  # defensive; not needed on 3.12
            # Fallback: manual check
            if root not in doc_path.parents:
                continue

        key = doc_path.name.lower()
        row = manifest_map.get(key)

        # Try without (1) suffix if no direct match
        if not row and "(1)" in key:
            alt_key = key.replace("(1)", "").replace("  ", " ").strip()
            row = manifest_map.get(alt_key)

        if not row:
            continue

        update_metadata(
            doc_id=doc.id,
            title=row.title,
            venue=row.venue,
            year=row.year,
            tags=row.tags,
        )

        if reset_status:
            update_status(doc.id, DocumentStatus.NEW, last_error=None)

        updated += 1

    return updated
