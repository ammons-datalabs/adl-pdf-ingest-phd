from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class EnhancementType(str, Enum):
    """Types of enhancements that robots can create."""
    FULL_TEXT = "full_text"
    PAPERPILE_METADATA = "paperpile_metadata"


class PendingEnhancementStatus(str, Enum):
    """
    State machine for pending enhancements.

    PENDING → PROCESSING → IMPORTING → INDEXING → COMPLETED
                  ↓            ↓           ↓
               EXPIRED     DISCARDED   INDEXING_FAILED
                  ↓            ↓
               FAILED       FAILED
    """
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    IMPORTING = "IMPORTING"
    INDEXING = "INDEXING"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"
    DISCARDED = "DISCARDED"
    INDEXING_FAILED = "INDEXING_FAILED"
    FAILED = "FAILED"


@dataclass
class Document:
    """
    Core document record.

    Metadata is stored in enhancements, not on the document itself.
    """
    id: int
    file_path: Path
    created_at: datetime


@dataclass
class Enhancement:
    """
    Enhancement record created by a robot.

    Contains metadata or extracted content for a document.
    """
    id: int
    document_id: int
    enhancement_type: EnhancementType
    content: dict[str, Any]  # JSONB payload
    robot_id: str
    created_at: datetime


@dataclass
class PendingEnhancement:
    """
    Tracks pending enhancement work with state machine.

    Robots pick up PENDING items, process them, and create Enhancement records.
    """
    id: int
    document_id: int
    enhancement_type: EnhancementType
    status: PendingEnhancementStatus
    created_at: datetime
    updated_at: datetime
    attempts: int
    last_error: Optional[str]


# --- Convenience accessors for enhancement content ---

def get_full_text(enhancements: list[Enhancement]) -> Optional[str]:
    """Extract full_text from enhancements list."""
    for e in enhancements:
        if e.enhancement_type == EnhancementType.FULL_TEXT:
            return e.content.get("text")
    return None


def get_metadata(enhancements: list[Enhancement]) -> dict[str, Any]:
    """Extract paperpile metadata from enhancements list."""
    for e in enhancements:
        if e.enhancement_type == EnhancementType.PAPERPILE_METADATA:
            return e.content
    return {}