"""
Tests for the state machine pattern.

Tests the StateMachineMixin and guarded transitions on PendingEnhancementStatus.
"""
import pytest

from pdf_ingest.models import (
    PendingEnhancementStatus,
    StateTransitionError,
)


class TestPendingEnhancementStatusTransitions:
    """Tests for the transition map definition."""

    def test_all_states_have_transitions_defined(self):
        """Every state should have an entry in the transitions map."""
        transitions = PendingEnhancementStatus.transitions()
        for status in PendingEnhancementStatus:
            assert status in transitions, f"{status} missing from transitions map"

    def test_pending_can_only_go_to_processing(self):
        """PENDING can only transition to PROCESSING."""
        status = PendingEnhancementStatus.PENDING
        allowed = PendingEnhancementStatus.transitions()[status]
        assert allowed == {PendingEnhancementStatus.PROCESSING}

    def test_processing_transitions(self):
        """PROCESSING can go to IMPORTING, EXPIRED, FAILED, or DISCARDED."""
        status = PendingEnhancementStatus.PROCESSING
        allowed = PendingEnhancementStatus.transitions()[status]
        assert allowed == {
            PendingEnhancementStatus.IMPORTING,
            PendingEnhancementStatus.EXPIRED,
            PendingEnhancementStatus.FAILED,
            PendingEnhancementStatus.DISCARDED,  # For "no match" cases
        }

    def test_importing_transitions(self):
        """IMPORTING can go to INDEXING, COMPLETED, DISCARDED, or FAILED."""
        status = PendingEnhancementStatus.IMPORTING
        allowed = PendingEnhancementStatus.transitions()[status]
        assert allowed == {
            PendingEnhancementStatus.INDEXING,
            PendingEnhancementStatus.COMPLETED,
            PendingEnhancementStatus.DISCARDED,
            PendingEnhancementStatus.FAILED,
        }

    def test_indexing_transitions(self):
        """INDEXING can go to COMPLETED or INDEXING_FAILED."""
        status = PendingEnhancementStatus.INDEXING
        allowed = PendingEnhancementStatus.transitions()[status]
        assert allowed == {
            PendingEnhancementStatus.COMPLETED,
            PendingEnhancementStatus.INDEXING_FAILED,
        }

    def test_terminal_states_have_no_outgoing_transitions(self):
        """Terminal states should have empty transition sets."""
        terminal_states = [
            PendingEnhancementStatus.COMPLETED,
            PendingEnhancementStatus.DISCARDED,
            PendingEnhancementStatus.INDEXING_FAILED,
        ]
        transitions = PendingEnhancementStatus.transitions()
        for status in terminal_states:
            assert transitions[status] == set(), f"{status} should be terminal"

    def test_failed_and_expired_can_retry(self):
        """FAILED and EXPIRED can transition back to PENDING for retry."""
        assert PendingEnhancementStatus.PENDING in PendingEnhancementStatus.transitions()[
            PendingEnhancementStatus.FAILED
        ]
        assert PendingEnhancementStatus.PENDING in PendingEnhancementStatus.transitions()[
            PendingEnhancementStatus.EXPIRED
        ]


class TestCanTransitionTo:
    """Tests for the can_transition_to method."""

    def test_valid_transition_returns_true(self):
        """Valid transitions return True."""
        status = PendingEnhancementStatus.PENDING
        assert status.can_transition_to(PendingEnhancementStatus.PROCESSING) is True

    def test_invalid_transition_returns_false(self):
        """Invalid transitions return False."""
        status = PendingEnhancementStatus.PENDING
        assert status.can_transition_to(PendingEnhancementStatus.COMPLETED) is False

    def test_self_transition_not_allowed(self):
        """Transitioning to the same state is not allowed (unless explicit)."""
        status = PendingEnhancementStatus.PENDING
        assert status.can_transition_to(PendingEnhancementStatus.PENDING) is False

    def test_terminal_state_cannot_transition(self):
        """Terminal states cannot transition to anything."""
        status = PendingEnhancementStatus.COMPLETED
        for target in PendingEnhancementStatus:
            assert status.can_transition_to(target) is False


class TestGuardTransition:
    """Tests for the guard_transition method."""

    def test_valid_transition_does_not_raise(self):
        """Valid transitions should not raise."""
        status = PendingEnhancementStatus.PENDING
        # Should not raise
        status.guard_transition(PendingEnhancementStatus.PROCESSING)

    def test_invalid_transition_raises_state_transition_error(self):
        """Invalid transitions should raise StateTransitionError."""
        status = PendingEnhancementStatus.PENDING

        with pytest.raises(StateTransitionError) as exc_info:
            status.guard_transition(PendingEnhancementStatus.COMPLETED)

        error = exc_info.value
        assert error.current == "PENDING"
        assert error.target == "COMPLETED"
        assert error.allowed == {"PROCESSING"}

    def test_error_message_is_descriptive(self):
        """Error message should describe the invalid transition."""
        status = PendingEnhancementStatus.COMPLETED

        with pytest.raises(StateTransitionError) as exc_info:
            status.guard_transition(PendingEnhancementStatus.PENDING)

        assert "Invalid transition: COMPLETED -> PENDING" in str(exc_info.value)
        assert "Allowed transitions from COMPLETED: none" in str(exc_info.value)

    def test_error_message_lists_allowed_transitions(self):
        """Error message should list allowed transitions."""
        status = PendingEnhancementStatus.PROCESSING

        with pytest.raises(StateTransitionError) as exc_info:
            status.guard_transition(PendingEnhancementStatus.COMPLETED)

        # PROCESSING can go to EXPIRED, FAILED, IMPORTING
        error_msg = str(exc_info.value)
        assert "EXPIRED" in error_msg
        assert "FAILED" in error_msg
        assert "IMPORTING" in error_msg


class TestStateTransitionError:
    """Tests for the StateTransitionError exception."""

    def test_attributes_are_set(self):
        """Error should have current, target, and allowed attributes."""
        error = StateTransitionError(
            current="PENDING",
            target="COMPLETED",
            allowed={"PROCESSING"},
        )
        assert error.current == "PENDING"
        assert error.target == "COMPLETED"
        assert error.allowed == {"PROCESSING"}

    def test_empty_allowed_set(self):
        """Error message handles empty allowed set."""
        error = StateTransitionError(
            current="COMPLETED",
            target="PENDING",
            allowed=set(),
        )
        assert "none" in str(error)


class TestStateWorkflows:
    """Integration-style tests for complete state workflows."""

    def test_happy_path_workflow(self):
        """Test the happy path: PENDING -> PROCESSING -> IMPORTING -> COMPLETED."""
        status = PendingEnhancementStatus.PENDING

        # Each step should be valid
        assert status.can_transition_to(PendingEnhancementStatus.PROCESSING)
        status = PendingEnhancementStatus.PROCESSING

        assert status.can_transition_to(PendingEnhancementStatus.IMPORTING)
        status = PendingEnhancementStatus.IMPORTING

        assert status.can_transition_to(PendingEnhancementStatus.COMPLETED)
        status = PendingEnhancementStatus.COMPLETED

        # Terminal - can't go anywhere
        for target in PendingEnhancementStatus:
            assert not status.can_transition_to(target)

    def test_failure_and_retry_workflow(self):
        """Test failure and retry: PENDING -> PROCESSING -> FAILED -> PENDING."""
        status = PendingEnhancementStatus.PENDING

        assert status.can_transition_to(PendingEnhancementStatus.PROCESSING)
        status = PendingEnhancementStatus.PROCESSING

        assert status.can_transition_to(PendingEnhancementStatus.FAILED)
        status = PendingEnhancementStatus.FAILED

        # Can retry
        assert status.can_transition_to(PendingEnhancementStatus.PENDING)

    def test_discard_workflow(self):
        """Test discard: PENDING -> PROCESSING -> IMPORTING -> DISCARDED."""
        status = PendingEnhancementStatus.PENDING

        status.guard_transition(PendingEnhancementStatus.PROCESSING)
        status = PendingEnhancementStatus.PROCESSING

        status.guard_transition(PendingEnhancementStatus.IMPORTING)
        status = PendingEnhancementStatus.IMPORTING

        status.guard_transition(PendingEnhancementStatus.DISCARDED)
        status = PendingEnhancementStatus.DISCARDED

        # DISCARDED is terminal
        assert not status.can_transition_to(PendingEnhancementStatus.PENDING)

    def test_direct_discard_workflow(self):
        """Test direct discard (no match): PENDING -> PROCESSING -> DISCARDED."""
        status = PendingEnhancementStatus.PENDING

        status.guard_transition(PendingEnhancementStatus.PROCESSING)
        status = PendingEnhancementStatus.PROCESSING

        # Can discard directly from PROCESSING (e.g., no manifest match)
        status.guard_transition(PendingEnhancementStatus.DISCARDED)
        status = PendingEnhancementStatus.DISCARDED

        # DISCARDED is terminal
        assert not status.can_transition_to(PendingEnhancementStatus.PENDING)