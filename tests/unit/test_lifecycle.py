"""Destination lifecycle state machine (viaduck/lifecycle.py).

The invariants under test, from the design review:
- absent row = active; unknown value = paused + ERROR (fail-safe both ways)
- paused/retired discard the buffer and evict the pooled connection ONCE
  per stint; the cursor is the resume point (controlled-crash semantics)
- draining keeps flushing; the connection is evicted only once clean
- returning to active tears nothing down and re-arms the evict-once latch
- viaduck code can never write 'retired' (StateManager refusal)
"""

from unittest.mock import MagicMock, patch

import pytest

from viaduck import lifecycle
from viaduck.lifecycle import ACTIVE, DRAINING, PAUSED, RETIRED, LifecycleTracker


@pytest.fixture(autouse=True)
def _mock_metrics():
    with patch("viaduck.lifecycle.metrics", MagicMock()) as m:
        yield m


def _tracker(dests=("d1", "d2")):
    t = LifecycleTracker(list(dests))
    delivery = MagicMock()
    delivery.discard_buffer.return_value = 0
    delivery.is_clean.return_value = True
    delivery.last_error.return_value = None
    pool = MagicMock()
    return t, delivery, pool


class TestNormalize:
    def test_absent_row_is_active(self):
        assert lifecycle.normalize(None, "d1") == ACTIVE

    def test_known_states_pass_through(self):
        for s in (ACTIVE, PAUSED, DRAINING, RETIRED):
            assert lifecycle.normalize(s, "d1") == s

    def test_unknown_state_is_paused(self):
        # Don't deliver on semantics we don't understand; don't discard
        # anything either (paused holds the cursor, nothing else).
        assert lifecycle.normalize("frobnicated", "d1") == PAUSED


class TestTracker:
    def test_default_everything_active(self):
        t, delivery, pool = _tracker()
        t.apply({}, delivery, pool)
        assert t.readable_ids() == ["d1", "d2"]
        assert t.suspended_ids() == set()
        delivery.discard_buffer.assert_not_called()
        pool.evict.assert_not_called()

    def test_paused_discards_and_evicts_once_clean(self):
        t, delivery, pool = _tracker()
        delivery.discard_buffer.return_value = 42
        t.apply({"d1": PAUSED}, delivery, pool)
        assert t.readable_ids() == ["d2"]
        assert t.suspended_ids() == {"d1"}
        pool.evict.assert_called_once_with("d1")
        # Subsequent cycles in the same stint: discard is re-run (cheap
        # no-op once empty; re-drops a raced read) but evict must not
        # repeat.
        t.apply({"d1": PAUSED}, delivery, pool)
        pool.evict.assert_called_once()
        assert delivery.discard_buffer.call_count == 2

    def test_paused_evict_deferred_while_flush_in_flight(self):
        # Review finding: an in-flight flush's retry loop re-creates the
        # pool entry after an evict, so the latch must wait for clean —
        # otherwise a paused destination holds a heavy catalog connection
        # for the whole stint.
        t, delivery, pool = _tracker()
        delivery.is_clean.return_value = False
        t.apply({"d1": PAUSED}, delivery, pool)
        pool.evict.assert_not_called()
        # Flush finishes → clean → evict exactly once.
        delivery.is_clean.return_value = True
        t.apply({"d1": PAUSED}, delivery, pool)
        pool.evict.assert_called_once_with("d1")
        t.apply({"d1": PAUSED}, delivery, pool)
        pool.evict.assert_called_once()

    def test_resume_rearms_evict_latch(self):
        t, delivery, pool = _tracker()
        t.apply({"d1": PAUSED}, delivery, pool)
        t.apply({"d1": ACTIVE}, delivery, pool)
        assert t.readable_ids() == ["d1", "d2"]
        assert t.suspended_ids() == set()
        # A second pause stint evicts again.
        t.apply({"d1": PAUSED}, delivery, pool)
        assert pool.evict.call_count == 2

    def test_draining_flushes_and_evicts_only_when_clean(self):
        t, delivery, pool = _tracker()
        delivery.is_clean.return_value = False
        delivery.last_error.return_value = None
        t.apply({"d1": DRAINING}, delivery, pool)
        # Not readable, but NOT suspended — draining exists to flush out.
        assert t.readable_ids() == ["d2"]
        assert t.suspended_ids() == set()
        delivery.discard_buffer.assert_not_called()
        pool.evict.assert_not_called()
        # Once clean, the connection is released exactly once.
        delivery.is_clean.return_value = True
        t.apply({"d1": DRAINING}, delivery, pool)
        pool.evict.assert_called_once_with("d1")
        t.apply({"d1": DRAINING}, delivery, pool)
        pool.evict.assert_called_once()

    def test_draining_reversal_is_gap_free(self):
        t, delivery, pool = _tracker()
        delivery.is_clean.return_value = True
        t.apply({"d1": DRAINING}, delivery, pool)
        t.apply({"d1": ACTIVE}, delivery, pool)
        # Nothing was discarded during the drain; the cursor is wherever
        # the flush-out left it and reads simply resume.
        delivery.discard_buffer.assert_not_called()
        assert t.readable_ids() == ["d1", "d2"]

    def test_retired_behaves_like_paused_for_teardown(self):
        t, delivery, pool = _tracker()
        delivery.discard_buffer.return_value = 7
        t.apply({"d1": RETIRED}, delivery, pool)
        assert t.readable_ids() == ["d2"]
        assert t.suspended_ids() == {"d1"}
        pool.evict.assert_called_once_with("d1")

    def test_retired_severs_cursor_every_cycle(self):
        # Re-add = new tenant = fresh seed: the cursor rows are deleted
        # idempotently each cycle while retired, closing the resurrect
        # race (an in-flight flush completing after the delete upserts the
        # row back; the next cycle removes it again).
        t, delivery, pool = _tracker()
        sm = MagicMock()
        t.apply({"d1": RETIRED}, delivery, pool, sm)
        sm.delete_destination_state.assert_called_once_with("d1")
        t.apply({"d1": RETIRED}, delivery, pool, sm)
        assert sm.delete_destination_state.call_count == 2
        # Severing failure must not crash the poll loop.
        sm.delete_destination_state.side_effect = RuntimeError("pg down")
        t.apply({"d1": RETIRED}, delivery, pool, sm)

    def test_drain_via_failure_rewind_reports_distinctly(self, caplog):
        # A flush failure rewinds position to the cursor, which also reads
        # as clean — the completion log must say the range was NOT
        # delivered instead of announcing a successful drain.
        import logging as _logging

        t, delivery, pool = _tracker()
        delivery.last_error.return_value = "Flush failed; range will be re-read"
        with caplog.at_level(_logging.WARNING, logger="viaduck.lifecycle"):
            t.apply({"d1": DRAINING}, delivery, pool)
        assert any("NOT delivered" in r.message for r in caplog.records)
        assert not any("drain complete" in r.message for r in caplog.records)
        # One report per stint.
        caplog.clear()
        with caplog.at_level(_logging.WARNING, logger="viaduck.lifecycle"):
            t.apply({"d1": DRAINING}, delivery, pool)
        assert not any("NOT delivered" in r.message for r in caplog.records)

    def test_unknown_state_gates_without_discard_confusion(self):
        t, delivery, pool = _tracker()
        t.apply({"d1": "wat"}, delivery, pool)
        assert t.state("d1") == PAUSED
        assert t.readable_ids() == ["d2"]

    def test_unknown_state_errors_once_not_every_cycle(self, caplog):
        import logging as _logging

        t, delivery, pool = _tracker()
        with caplog.at_level(_logging.ERROR, logger="viaduck.lifecycle"):
            t.apply({"d1": "wat"}, delivery, pool)
            t.apply({"d1": "wat"}, delivery, pool)
            t.apply({"d1": "wat"}, delivery, pool)
        assert sum("unknown lifecycle state" in r.message for r in caplog.records) == 1

    def test_transition_updates_metric(self, _mock_metrics):
        t, delivery, pool = _tracker()
        t.apply({"d1": PAUSED}, delivery, pool)
        _mock_metrics.set_destination_lifecycle.assert_called_with("d1", PAUSED, lifecycle.VALID_STATES)


class TestWritableStates:
    def test_retired_is_not_code_writable(self):
        # The constant the StateManager refusal is built on: retirement is
        # an operator ack through SQL, never a code path.
        assert RETIRED not in lifecycle.WRITABLE_STATES
        assert lifecycle.WRITABLE_STATES == {ACTIVE, PAUSED, DRAINING}


class TestStateManagerRefusal:
    def test_set_lifecycle_state_refuses_retired(self):
        from viaduck.state import StateManager

        sm = StateManager.__new__(StateManager)  # no DB needed for the guard
        with pytest.raises(ValueError, match="retired"):
            StateManager.set_lifecycle_state(sm, "d1", "retired", reason="x", updated_by="test")

    def test_set_lifecycle_state_refuses_unknown(self):
        from viaduck.state import StateManager

        sm = StateManager.__new__(StateManager)
        with pytest.raises(ValueError):
            StateManager.set_lifecycle_state(sm, "d1", "wat", reason="x", updated_by="test")


# ---------------------------------------------------------------------------
# Membership (C3 reconciler)
# ---------------------------------------------------------------------------


def test_tracker_add_makes_id_visible_to_apply():
    tracker = LifecycleTracker(["d1"])
    tracker.add("d2")
    assert "d2" in tracker.readable_ids()
    # A later pause of the dynamically added id applies — the frozen-
    # membership defect (v4 review F2) is what this pins.
    tracker.apply({"d2": PAUSED}, MagicMock(), MagicMock(), None)
    assert "d2" not in tracker.readable_ids()
    assert "d2" in tracker.suspended_ids()
    tracker.add("d2")  # idempotent; state preserved
    assert "d2" in tracker.suspended_ids()


def test_tracker_remove_stops_tracking():
    tracker = LifecycleTracker(["d1", "d2"])
    tracker.apply({"d2": PAUSED}, MagicMock(), MagicMock(), None)
    tracker.remove("d2")
    assert "d2" not in tracker.readable_ids()
    assert "d2" not in tracker.suspended_ids()
    assert "d2" not in tracker.states()
    tracker.remove("d2")  # idempotent
    # Re-add starts ACTIVE; the caller honors the durable lifecycle row at
    # activation and the next apply() absorbs it.
    tracker.add("d2")
    assert "d2" in tracker.readable_ids()
