"""
Paperpile Sync Robot

Syncs metadata from Paperpile CSV manifest to Enhancement records.

This robot works in bulk mode (not from pending queue) since metadata
comes from an external CSV file rather than document-by-document processing.

Usage:
    python -m pdf_ingest.robots.paperpile_sync
    python -m pdf_ingest.robots.paperpile_sync --manifest path/to/manifest.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from ..db import (
    create_enhancement,
    fetch_all_documents,
    init_db,
)
from ..models import EnhancementType

logger = logging.getLogger(__name__)

ROBOT_ID = "paperpile-sync"


@dataclass
class ManifestRow:
    """Parsed row from Paperpile CSV manifest."""
    file_name: str
    title: Optional[str]
    venue: Optional[str]
    year: Optional[int]
    tags: list[str]


def load_manifest(path: Path) -> List[ManifestRow]:
    """Load and parse Paperpile CSV manifest."""
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


def sync_manifest(manifest_path: Path) -> int:
    """
    Sync metadata from manifest to Enhancement records.

    Returns count of documents updated.
    """
    manifest = load_manifest(manifest_path)
    manifest_map = {row.file_name.lower(): row for row in manifest}
    logger.info("Loaded %d entries from manifest", len(manifest_map))

    documents = fetch_all_documents()
    logger.info("Found %d documents in database", len(documents))

    updated = 0
    for doc in documents:
        key = doc.file_path.name.lower()
        row = manifest_map.get(key)

        # Try without (1) suffix if no direct match
        if not row and "(1)" in key:
            alt_key = key.replace("(1)", "").replace("  ", " ").strip()
            row = manifest_map.get(alt_key)

        if not row:
            continue

        # Create enhancement with metadata
        content = {
            "title": row.title,
            "venue": row.venue,
            "year": row.year,
            "tags": row.tags,
        }

        create_enhancement(
            document_id=doc.id,
            enhancement_type=EnhancementType.PAPERPILE_METADATA,
            content=content,
            robot_id=ROBOT_ID,
        )
        updated += 1

        if updated % 100 == 0:
            logger.info("Synced %d documents...", updated)

    logger.info("Sync complete. Updated %d documents.", updated)
    return updated


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Sync Paperpile metadata to Enhancement records"
    )
    parser.add_argument(
        "--manifest",
        type=str,
        default="metadata/papers_manifest_normalized.csv",
        help="Path to Paperpile CSV manifest",
    )
    args = parser.parse_args()

    init_db()

    manifest_path = Path(args.manifest).resolve()
    if not manifest_path.exists():
        logger.error("Manifest not found: %s", manifest_path)
        return

    sync_manifest(manifest_path)


if __name__ == "__main__":
    main()