from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Set, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Self


class StateTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""

    def __init__(self, current: str, target: str, allowed: Set[str]):
        self.current = current
        self.target = target
        self.allowed = allowed
        allowed_str = ", ".join(sorted(s for s in allowed)) if allowed else "none"
        super().__init__(
            f"Invalid transition: {current} -> {target}. "
            f"Allowed transitions from {current}: {allowed_str}"
        )


class StateMachineMixin:
    """
    Mixin providing guarded state transitions.

    Subclasses must implement `transitions()` returning a dict of
    {state: {allowed_next_states}}.
    """

    @classmethod
    def transitions(cls) -> Dict[Self, Set[Self]]:
        """Return allowed transitions as {from_state: {to_states}}."""
        raise NotImplementedError("Subclasses must implement transitions()")

    def can_transition_to(self, new_status: Self) -> bool:
        """Check if transition from current state to new_status is allowed."""
        allowed = self.transitions().get(self, set())
        return new_status in allowed

    def guard_transition(self, new_status: Self) -> None:
        """
        Raise StateTransitionError if transition is not allowed.

        Call this before updating status to catch invalid transitions early.
        """
        if not self.can_transition_to(new_status):
            allowed = self.transitions().get(self, set())
            raise StateTransitionError(
                current=self.value,
                target=new_status.value,
                allowed={s.value for s in allowed},
            )


class EnhancementType(str, Enum):
    """Types of enhancements that robots can create."""
    FULL_TEXT = "full_text"
    PAPERPILE_METADATA = "paperpile_metadata"


class PendingEnhancementStatus(StateMachineMixin, str, Enum):
    """
    State machine for pending enhancements.

    PENDING → PROCESSING → IMPORTING → INDEXING → COMPLETED
                  ↓   ↘        ↓           ↓
               EXPIRED  ↘   DISCARDED   INDEXING_FAILED
                  ↓      ↘     ↓
               FAILED     → DISCARDED
                            (no match)

    Terminal states: COMPLETED, DISCARDED, INDEXING_FAILED
    Retriable states: FAILED, EXPIRED -> can return to PENDING
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

    @classmethod
    def transitions(cls) -> Dict[PendingEnhancementStatus, Set[PendingEnhancementStatus]]:
        """
        Define allowed state transitions.

        Returns dict mapping each state to the set of states it can transition to.
        """
        return {
            cls.PENDING: {cls.PROCESSING},
            cls.PROCESSING: {cls.IMPORTING, cls.EXPIRED, cls.FAILED, cls.DISCARDED},
            cls.IMPORTING: {cls.INDEXING, cls.COMPLETED, cls.DISCARDED, cls.FAILED},
            cls.INDEXING: {cls.COMPLETED, cls.INDEXING_FAILED},
            # Terminal states - no outgoing transitions
            cls.COMPLETED: set(),
            cls.EXPIRED: {cls.PENDING},  # Can be retried
            cls.DISCARDED: set(),
            cls.INDEXING_FAILED: set(),
            cls.FAILED: {cls.PENDING},  # Can be retried
        }


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