"""
Paperpile Sync Robot

Syncs metadata from Paperpile CSV manifest to Enhancement records.

Supports two CSV formats:
1. Full Paperpile export (papers_manifest.csv) - extracts all rich metadata
2. Normalized format (papers_manifest_normalized.csv) - basic fields only

Uses PendingEnhancement tracking:
- Claims PAPERPILE_METADATA pending enhancements
- Loads manifest CSV once, matches against pending documents
- Creates enhancement + marks COMPLETED, or marks DISCARDED if no match

Usage:
    pdf-ingest queue-metadata                    # Queue documents for metadata sync
    pdf-ingest run-robot paperpile-sync          # Process queue (uses full CSV)
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from ..db import (
    create_enhancement,
    fetch_document_by_id,
    fetch_next_pending,
    init_db,
    update_pending_status,
)
from ..models import EnhancementType, PendingEnhancementStatus

logger = logging.getLogger(__name__)

ROBOT_ID = "paperpile-sync"


@dataclass
class ManifestRow:
    """Parsed row from Paperpile CSV manifest."""
    file_name: str
    title: Optional[str] = None
    venue: Optional[str] = None
    year: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    # Rich metadata (from full Paperpile export)
    abstract: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    item_type: Optional[str] = None


def _parse_authors(authors_str: str) -> List[str]:
    """Parse authors string into list of author names."""
    if not authors_str:
        return []
    # Authors are comma-separated, e.g., "Smith J,Jones A,Brown K"
    return [a.strip() for a in authors_str.split(",") if a.strip()]


def _parse_keywords(keywords_str: str) -> List[str]:
    """Parse keywords string into list."""
    if not keywords_str:
        return []
    # Keywords can be semicolon or comma separated
    if ";" in keywords_str:
        return [k.strip() for k in keywords_str.split(";") if k.strip()]
    return [k.strip() for k in keywords_str.split(",") if k.strip()]


def _extract_filename_from_attachments(attachments: str) -> Optional[str]:
    """Extract PDF filename from Paperpile Attachments field."""
    if not attachments:
        return None
    # Format: "All Papers/X/Xia et al. 2025 - Title.pdf"
    # Take first attachment if multiple (semicolon separated)
    first_attachment = attachments.split(";")[0].strip()
    if first_attachment:
        return os.path.basename(first_attachment)
    return None


def load_manifest(path: Path) -> Dict[str, ManifestRow]:
    """
    Load and parse Paperpile CSV manifest into a lookup dict.

    Supports both full Paperpile export and normalized format.
    """
    manifest_map: Dict[str, ManifestRow] = {}

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        # Detect format based on columns
        is_full_format = "Attachments" in fieldnames or "Abstract" in fieldnames
        is_normalized = "file_name" in fieldnames

        if not is_full_format and not is_normalized:
            logger.warning("Unknown CSV format, trying to parse as normalized")

        for r in reader:
            # Extract filename
            if is_full_format:
                file_name = _extract_filename_from_attachments(r.get("Attachments", ""))
            else:
                file_name = (r.get("file_name") or "").strip()

            if not file_name:
                continue

            # Basic fields (both formats)
            if is_full_format:
                title = (r.get("Title") or "").strip() or None
                # Venue: prefer Journal, then Proceedings title
                venue = (r.get("Journal") or r.get("Proceedings title") or "").strip() or None
                year_str = (r.get("Publication year") or "").strip()
                tags_raw = r.get("Labels filed in") or ""
            else:
                title = (r.get("title") or "").strip() or None
                venue = (r.get("venue") or "").strip() or None
                year_str = (r.get("year") or "").strip()
                tags_raw = r.get("tags") or ""

            year = int(year_str) if year_str else None
            tags = [t.strip() for t in tags_raw.split(";") if t.strip()]

            # Rich metadata (full format only)
            if is_full_format:
                abstract = (r.get("Abstract") or "").strip() or None
                authors = _parse_authors(r.get("Authors") or "")
                keywords = _parse_keywords(r.get("Keywords") or "")
                doi = (r.get("DOI") or "").strip() or None
                arxiv_id = (r.get("Arxiv ID") or "").strip() or None
                item_type = (r.get("Item type") or "").strip() or None
            else:
                abstract = None
                authors = []
                keywords = []
                doi = None
                arxiv_id = None
                item_type = None

            row = ManifestRow(
                file_name=file_name,
                title=title,
                venue=venue,
                year=year,
                tags=tags,
                abstract=abstract,
                authors=authors,
                keywords=keywords,
                doi=doi,
                arxiv_id=arxiv_id,
                item_type=item_type,
            )
            manifest_map[file_name.lower()] = row

    return manifest_map


# Pattern to match duplicate suffixes like (1), (2), (9), etc.
_DUPLICATE_SUFFIX_PATTERN = re.compile(r"\(\d+\)")


def _lookup_manifest(
    file_name: str,
    manifest_map: Dict[str, ManifestRow],
) -> Optional[ManifestRow]:
    """Look up a file in the manifest, with fallback for duplicate suffixes."""
    key = file_name.lower()
    row = manifest_map.get(key)

    # Try without duplicate suffix (1), (2), etc. if no direct match
    if not row and _DUPLICATE_SUFFIX_PATTERN.search(key):
        alt_key = _DUPLICATE_SUFFIX_PATTERN.sub("", key).replace("  ", " ").strip()
        row = manifest_map.get(alt_key)

    return row


def process_one(manifest_map: Dict[str, ManifestRow]) -> Optional[str]:
    """
    Process a single pending PAPERPILE_METADATA enhancement.

    Returns:
        "completed" if enhancement was created
        "discarded" if no manifest match found
        None if queue is empty
    """
    pending = fetch_next_pending(EnhancementType.PAPERPILE_METADATA)
    if pending is None:
        return None

    doc = fetch_document_by_id(pending.document_id)
    if doc is None:
        logger.warning("Document %d not found, marking DISCARDED", pending.document_id)
        update_pending_status(pending.id, PendingEnhancementStatus.DISCARDED)
        return "discarded"

    # Look up in manifest
    row = _lookup_manifest(doc.file_path.name, manifest_map)

    if row is None:
        # No metadata found for this document
        logger.debug("No manifest entry for %s, marking DISCARDED", doc.file_path.name)
        update_pending_status(
            pending.id,
            PendingEnhancementStatus.DISCARDED,
            last_error="No manifest entry found",
        )
        return "discarded"

    # Create enhancement with metadata
    update_pending_status(pending.id, PendingEnhancementStatus.IMPORTING)

    content = {
        "title": row.title,
        "venue": row.venue,
        "year": row.year,
        "tags": row.tags,
        # Rich metadata
        "abstract": row.abstract,
        "authors": row.authors,
        "keywords": row.keywords,
        "doi": row.doi,
        "arxiv_id": row.arxiv_id,
        "item_type": row.item_type,
    }

    create_enhancement(
        document_id=doc.id,
        enhancement_type=EnhancementType.PAPERPILE_METADATA,
        content=content,
        robot_id=ROBOT_ID,
    )

    update_pending_status(pending.id, PendingEnhancementStatus.COMPLETED)
    logger.debug("Synced metadata for %s", doc.file_path.name)
    return "completed"


def run_loop(
    manifest_path: Path,
    max_iterations: Optional[int] = None,
    poll_interval: float = 1.0,
) -> None:
    """
    Run the paperpile sync robot loop.

    Args:
        manifest_path: Path to Paperpile CSV manifest
        max_iterations: Stop after N iterations (for testing); None = run forever
        poll_interval: Seconds to wait when queue is empty
    """
    logger.info("Loading manifest from %s", manifest_path)
    manifest_map = load_manifest(manifest_path)
    logger.info("Loaded %d entries from manifest", len(manifest_map))

    # Log stats about rich metadata
    with_abstract = sum(1 for r in manifest_map.values() if r.abstract)
    with_authors = sum(1 for r in manifest_map.values() if r.authors)
    with_doi = sum(1 for r in manifest_map.values() if r.doi)
    logger.info(
        "Rich metadata: %d with abstract, %d with authors, %d with DOI",
        with_abstract, with_authors, with_doi
    )

    logger.info("Starting paperpile-sync robot loop...")
    iterations = 0
    completed = 0
    discarded = 0

    while True:
        if max_iterations is not None and iterations >= max_iterations:
            logger.info("Reached max iterations (%d), stopping", max_iterations)
            break

        result = process_one(manifest_map)
        iterations += 1

        if result == "completed":
            completed += 1
            if (completed + discarded) % 100 == 0:
                logger.info("Processed %d documents...", completed + discarded)
        elif result == "discarded":
            discarded += 1
            if (completed + discarded) % 100 == 0:
                logger.info("Processed %d documents...", completed + discarded)
        else:
            # Queue empty
            if max_iterations is None:
                time.sleep(poll_interval)
            else:
                # In test mode with max_iterations, don't wait
                break

    logger.info(
        "Paperpile sync complete: %d completed, %d discarded (no manifest match)",
        completed,
        discarded,
    )


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
        default="metadata/papers_manifest.csv",
        help="Path to Paperpile CSV manifest (default: full export)",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Stop after N iterations (for testing)",
    )
    args = parser.parse_args()

    init_db()

    manifest_path = Path(args.manifest).resolve()
    if not manifest_path.exists():
        logger.error("Manifest not found: %s", manifest_path)
        return

    run_loop(manifest_path, max_iterations=args.max_iterations)


if __name__ == "__main__":
    main()