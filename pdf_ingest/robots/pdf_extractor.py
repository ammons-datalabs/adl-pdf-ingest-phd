"""
PDF Extractor Robot

Processes pending FULL_TEXT enhancements by:
1. Extracting text from PDFs
2. Cleaning the extracted text
3. Creating Enhancement records with the content

Usage:
    python -m pdf_ingest.robots.pdf_extractor
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from ..cleaning import clean_text
from ..db import (
    create_enhancement,
    fetch_document_by_id,
    fetch_next_pending,
    update_pending_status,
)
from ..extractor import ExtractionError, extract_text
from ..models import EnhancementType, PendingEnhancementStatus

logger = logging.getLogger(__name__)

ROBOT_ID = "pdf-extractor"


def process_one() -> bool:
    """
    Process a single pending FULL_TEXT enhancement.

    State machine:
        PENDING → PROCESSING → IMPORTING → COMPLETED
                      ↓
                   FAILED

    Returns True if work was done, False if no pending items.
    """
    # Claim next pending (moves to PROCESSING)
    pending = fetch_next_pending(EnhancementType.FULL_TEXT)
    if pending is None:
        return False

    logger.info(
        "Processing pending_id=%s document_id=%s",
        pending.id,
        pending.document_id,
    )

    doc = fetch_document_by_id(pending.document_id)
    if doc is None:
        logger.error("Document id=%s not found", pending.document_id)
        update_pending_status(
            pending.id,
            PendingEnhancementStatus.FAILED,
            last_error="Document not found",
        )
        return True

    try:
        # Extract text
        raw_text = extract_text(doc.file_path)
        if not raw_text.strip():
            raise ExtractionError("Empty text extracted")

        # Clean text
        cleaned_text = clean_text(raw_text)
        if not cleaned_text.strip():
            raise ExtractionError("Empty text after cleaning")

        # Move to IMPORTING
        update_pending_status(pending.id, PendingEnhancementStatus.IMPORTING)

        # Create enhancement
        content = {
            "text": cleaned_text,
            "raw_length": len(raw_text),
            "cleaned_length": len(cleaned_text),
        }
        create_enhancement(
            document_id=doc.id,
            enhancement_type=EnhancementType.FULL_TEXT,
            content=content,
            robot_id=ROBOT_ID,
        )

        # Mark completed
        update_pending_status(pending.id, PendingEnhancementStatus.COMPLETED)
        logger.info("Completed pending_id=%s", pending.id)

    except ExtractionError as e:
        error_msg = str(e)
        logger.warning("Extraction error pending_id=%s: %s", pending.id, error_msg)
        update_pending_status(
            pending.id,
            PendingEnhancementStatus.FAILED,
            last_error=error_msg,
        )

    except Exception as e:
        error_msg = str(e)
        logger.exception("Unexpected error pending_id=%s: %s", pending.id, error_msg)
        update_pending_status(
            pending.id,
            PendingEnhancementStatus.FAILED,
            last_error=error_msg,
        )

    return True


def run_loop(
    poll_interval: float = 1.0,
    max_iterations: Optional[int] = None,
) -> None:
    """
    Continuously poll for and process pending FULL_TEXT enhancements.

    Args:
        poll_interval: Seconds to wait when queue is empty (daemon mode only)
        max_iterations: Stop after N iterations; if set and queue empties, exit immediately
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    logger.info("PDF Extractor robot starting...")

    from ..db import init_db
    init_db()

    iterations = 0
    processed_count = 0

    while True:
        if max_iterations is not None and iterations >= max_iterations:
            logger.info("Reached max iterations (%d), stopping.", max_iterations)
            break

        processed = process_one()
        iterations += 1

        if processed:
            processed_count += 1
            if processed_count % 100 == 0:
                logger.info("Processed %d documents...", processed_count)
        else:
            # Queue empty
            if max_iterations is not None:
                # Batch mode: exit when queue empties
                logger.info("Queue empty, processed %d documents.", processed_count)
                break
            else:
                # Daemon mode: keep polling
                logger.debug("No pending items, sleeping %.1fs", poll_interval)
                time.sleep(poll_interval)


if __name__ == "__main__":
    run_loop()