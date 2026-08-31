"""Unit tests for DeliveryManager: triggers, buffer swap, failure semantics,
per-destination serialization. Mirrors the actions in tla/Viaduck.tla —
BufferRead, FlushStart, FlushCommit, FlushFail."""

from __future__ import annotations

import threading
import time as _time_mod
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.config import ConfigError, DeliveryConfig
from viaduck.delivery import DeliveryManager


def setup_module():
    metrics.init("test")


def _table(n: int) -> pa.Table:
    return pa.table({"company": ["a"] * n, "value": list(range(n))})


def _state_mgr(cursors: dict[str, int] | None = None):
    sm = MagicMock()
    loaded = {}
    for d, snap in (cursors or {}).items():
        c = MagicMock()
        c.last_snapshot_id = snap
        c.rows_replicated = 0
        c.last_error = None
        loaded[d] = c
    sm.load_cursors.return_value = loaded
    return sm


def _recording_flush(mgr):
    """Patch target for _flush that records calls and maintains the
    in-flight guard contract (the real _flush's finally block)."""
    calls = []

    def _fake(dest, tables, through, trigger):
        calls.append((dest, tables, through, trigger))
        with mgr._lock:
            mgr._inflight.discard(dest)

    return _fake, calls


def _manager(dests=("d1",), cursors=None, mode="append_only", key_columns=None, **cfg_overrides):
    """Default to append_only (key_columns=[]) since that's the production posthog
    shape; tests that need the full_cdc path pass mode="full_cdc" and a
    non-empty key_columns."""
    defaults = dict(workers=2, flush_interval_seconds=3600.0)
    defaults.update(cfg_overrides)
    cfg = DeliveryConfig(**defaults)
    sm = _state_mgr(cursors or {d: 0 for d in dests})
    pool = MagicMock()
    mgr = DeliveryManager(cfg, sm, pool, key_columns or [], list(dests), mode=mode)
    return mgr, sm, pool


# ---------------------------------------------------------------------------
# BufferRead / positions
# ---------------------------------------------------------------------------


def test_buffer_advances_position_and_accumulates():
    mgr, _, _ = _manager()
    assert mgr.positions() == {"d1": 0}
    mgr.buffer("d1", _table(3), through_snapshot=7)
    mgr.buffer("d1", _table(2), through_snapshot=9)
    assert mgr.positions() == {"d1": 9}
    snap = mgr.status_snapshot()["d1"]
    assert snap.buffer_rows == 5
    assert snap.flushed_snapshot == 0


def test_advance_position_is_monotonic():
    mgr, _, _ = _manager()
    mgr.advance_position("d1", 5)
    mgr.advance_position("d1", 3)  # stale, ignored
    assert mgr.positions() == {"d1": 5}


# ---------------------------------------------------------------------------
# Triggers (FlushStart conditions)
# ---------------------------------------------------------------------------


def test_no_trigger_below_thresholds():
    mgr, _, _ = _manager()
    mgr.buffer("d1", _table(1), 5)
    assert mgr.maybe_flush() == 0  # interval 1h, tiny buffer


def test_interval_trigger_uses_buffer_age():
    mgr, _, _ = _manager(flush_interval_seconds=0.0)
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", side_effect=fake):
        mgr.buffer("d1", _table(1), 5)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    assert calls[0][3] == "interval"


def test_interval_trigger_persists_position_only_advances():
    """Idle destinations: position ahead of cursor with an empty buffer
    still flushes on the interval (the spec's FlushStart second disjunct)."""
    mgr, _, _ = _manager(flush_interval_seconds=0.0)
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", side_effect=fake):
        mgr.advance_position("d1", 9)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    dest, tables, through, _trigger = calls[0]
    assert dest == "d1" and tables == [] and through == 9


def test_shutdown_trigger_flushes_everything():
    mgr, _, _ = _manager(dests=("d1", "d2"))
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", side_effect=fake):
        mgr.buffer("d1", _table(1), 5)
        mgr.advance_position("d2", 5)
        assert mgr.maybe_flush(shutdown=True) == 2
        assert mgr.wait_idle(5)
    assert {c[3] for c in calls} == {"shutdown"}


def test_memory_trigger_largest_first():
    mgr, _, _ = _manager(dests=("small", "big"), buffer_total_max_bytes=1)
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", side_effect=fake):
        mgr.buffer("small", _table(1), 5)
        mgr.buffer("big", _table(100), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle(5)
    assert (calls[0][0], calls[0][3]) == ("big", "memory")  # largest relieved first


def test_clean_destination_never_flushes():
    mgr, _, _ = _manager(flush_interval_seconds=0.0)
    assert mgr.maybe_flush() == 0  # nothing buffered, position == flushed


# ---------------------------------------------------------------------------
# Per-destination serialization + buffer swap
# ---------------------------------------------------------------------------


def test_no_second_flush_while_in_flight_and_swap_keeps_buffering():
    mgr, _, _ = _manager(flush_interval_seconds=0.0)
    release = threading.Event()
    started = threading.Event()
    seen = []

    def slow_flush(dest, tables, through, trigger):
        seen.append((sum(t.num_rows for t in tables), through))
        started.set()
        release.wait(5)
        with mgr._lock:
            mgr._flushed[dest] = through
            mgr._inflight.discard(dest)

    with patch.object(mgr, "_flush", side_effect=slow_flush):
        mgr.buffer("d1", _table(2), 5)
        assert mgr.maybe_flush() == 1
        assert started.wait(5)

        # While in flight: new reads land in the fresh buffer (the swap)...
        mgr.buffer("d1", _table(3), 8)
        assert mgr.status_snapshot()["d1"].buffer_rows == 3
        # ...and no second flush is submitted for the same destination.
        assert mgr.maybe_flush() == 0

        release.set()
        assert mgr.wait_idle()
        # Prior flush done: the fresh buffer is now eligible.
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle()

    assert seen == [(2, 5), (3, 8)]


# ---------------------------------------------------------------------------
# FlushCommit / FlushFail (real _flush, mocked apply layer)
# ---------------------------------------------------------------------------


def test_flush_commit_advances_cursor_and_clears_error():
    mgr, sm, _ = _manager(cursors={"d1": 2}, flush_interval_seconds=0.0)
    with patch("viaduck.delivery.append_only", return_value=4) as ap:
        mgr.buffer("d1", _table(4), 7)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    ap.assert_called_once()
    sm.advance_cursor.assert_called_once_with("d1", 7, cumulative_rows=4)
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 7
    assert snap.last_error is None
    assert snap.buffer_rows == 0


def test_flush_fail_drops_live_buffer_and_resets_position():
    """The TLC-verified rule: a failed flush discards the live buffer too —
    keeping it would leave a coverage gap over (flushed, through]."""
    mgr, sm, pool = _manager(cursors={"d1": 2}, flush_interval_seconds=0.0)
    release = threading.Event()

    def failing_apply(*a, **k):
        release.wait(5)
        raise RuntimeError("dest down")

    with patch("viaduck.delivery.append_only", side_effect=failing_apply):
        mgr.buffer("d1", _table(2), 7)
        mgr.maybe_flush()
        # Reads continue during the in-flight flush — this is the live
        # buffer that must ALSO be dropped on failure.
        mgr.buffer("d1", _table(3), 9)
        release.set()
        assert mgr.wait_idle()

    snap = mgr.status_snapshot()["d1"]
    assert snap.buffer_rows == 0  # live buffer dropped
    assert mgr.positions() == {"d1": 2}  # reset to persisted cursor
    assert snap.flushed_snapshot == 2
    assert snap.last_error is not None
    sm.advance_cursor.assert_not_called()
    sm.record_error.assert_called_once()
    pool.evict.assert_called_once_with("d1")


def test_flush_failure_then_recovery():
    mgr, sm, _ = _manager(cursors={"d1": 2}, flush_interval_seconds=0.0)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("down")):
        mgr.buffer("d1", _table(1), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert mgr.positions() == {"d1": 2}

    # Re-read the range (as the poll loop would) and flush successfully.
    with patch("viaduck.delivery.append_only", return_value=1):
        mgr.buffer("d1", _table(1), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 5
    assert snap.last_error is None


def test_full_cdc_flush_concats_buffered_reads():
    """Multiple buffered reads are concatenated before Phase 2 — cross-read
    conflicts resolve exactly like within-read ones."""
    mgr, _, _ = _manager(flush_interval_seconds=0.0, mode="full_cdc", key_columns=["value"])
    captured = {}

    def fake_apply(pool, dest, batch, key_columns, stop_event=None, deadline=None):
        captured["rows"] = batch.num_rows
        return batch.num_rows

    with patch("viaduck.delivery.apply_full_cdc", side_effect=fake_apply):
        mgr.buffer("d1", _table(2), 5)
        mgr.buffer("d1", _table(3), 6)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert captured["rows"] == 5


def test_delivery_manager_rejects_unknown_mode():
    """The mode arg is a closed enum at the DeliveryManager layer too.
    Operators interact with the YAML config (which goes through
    RoutingConfig.__post_init__'s enum check); this guard catches direct-
    instantiation misuse in tests / future call sites."""
    cfg = DeliveryConfig(workers=1, flush_interval_seconds=3600.0)
    sm = _state_mgr({"d1": 0})
    pool = MagicMock()
    with pytest.raises(ConfigError, match="must be 'full_cdc' or 'append_only'"):
        DeliveryManager(cfg, sm, pool, [], ["d1"], mode="weird")


# ---------------------------------------------------------------------------
# Watermark / pause
# ---------------------------------------------------------------------------


def test_should_pause_reads_for_is_destination_local():
    """A destination at its cap pauses ITS reads only; a peer with headroom
    keeps reading. This locality is the whole point of per-destination
    backpressure — the old global watermark let one destination's full
    queue pause everyone."""
    mgr, _, _ = _manager(dests=("d1", "d2"), buffer_total_max_bytes=2)
    # Auto-derived per-dest cap: 2 // 2 = 1 byte each.
    assert mgr._per_dest_cap == 1
    assert not mgr.should_pause_reads_for("d1")
    assert not mgr.should_pause_reads_for("d2")

    mgr.buffer("d1", _table(10), 5)  # d1's queue is now over its cap
    assert mgr.should_pause_reads_for("d1")
    assert not mgr.should_pause_reads_for("d2"), "peer with headroom must keep reading"
    assert not mgr.should_pause_all_reads()

    mgr.buffer("d2", _table(10), 5)  # now both are at cap
    assert mgr.should_pause_all_reads()


def test_should_pause_all_reads_false_with_zero_destinations():
    """A partitioned instance can own zero destinations; vacuous all() must
    not report it permanently wedged (the poll loop would sit in the
    watermark-WARN branch every cycle forever)."""
    mgr, _, _ = _manager(dests=())
    assert not mgr.should_pause_all_reads()


def test_flush_failure_clears_cap_and_reads_resume():
    """QE must-fix #1: `_inflight_bytes` zeroing lives in _flush's finally.
    If it moved to the success-only branch, a destination that fails ONE
    flush would sit at cap forever with its reads paused — the exact prod
    wedge shape. Pin the failure path: after a failed flush, the queue
    drains (buffer dropped + inflight zeroed) and reads resume."""
    mgr, _, pool = _manager(
        dests=("d1",),
        buffer_max_bytes_per_destination=1,
        flush_interval_seconds=0.0,
        workers=1,
    )
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("dest down")):
        mgr.buffer("d1", _table(5), 7)
        assert mgr.should_pause_reads_for("d1")  # over the 1-byte cap
        mgr.maybe_flush()
        assert mgr.wait_idle()
    # FlushFail: buffer dropped, in-flight zeroed → queue empty → reads resume.
    assert not mgr.should_pause_reads_for("d1"), (
        "failed flush must clear the destination's queue accounting or it wedges at cap"
    )


def test_should_pause_reads_for_counts_inflight_bytes():
    """In-flight (swapped-out) bytes count toward the destination's queue:
    a failing flush that holds its swap pins the destination at cap until
    the flush resolves — reads stay paused for it, and only it."""
    mgr, _, _ = _manager(dests=("d1", "d2"), buffer_max_bytes_per_destination=1)
    with mgr._lock:
        mgr._inflight_bytes["d1"] = 100
    assert mgr.should_pause_reads_for("d1")
    assert not mgr.should_pause_reads_for("d2")


def test_per_dest_cap_explicit_override_beats_auto_derive():
    mgr, _, _ = _manager(dests=("d1", "d2"), buffer_total_max_bytes=1000, buffer_max_bytes_per_destination=7)
    assert mgr._per_dest_cap == 7


def test_dest_at_cap_triggers_flush_even_below_global_thresholds():
    """A destination pinned at its cap has its reads paused, so it must
    flush on the cap trigger rather than waiting for the rows target /
    interval — otherwise it wedges: full queue, paused intake, no flush."""
    mgr, _, pool = _manager(
        dests=("d1",),
        buffer_max_bytes_per_destination=1,
        flush_batch_max_rows=10**12,  # rows target unreachable
        flush_interval_seconds=3600.0,  # interval trigger unreachable
    )
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", side_effect=fake):
        mgr.buffer("d1", _table(10), 5)  # over the 1-byte cap
        submitted = mgr.maybe_flush()
    assert submitted == 1
    assert calls and calls[0][3] == "memory"


# ---------------------------------------------------------------------------
# Drain
# ---------------------------------------------------------------------------


def test_drain_flushes_and_stops():
    mgr, sm, _ = _manager()
    with patch("viaduck.delivery.append_only", return_value=2):
        mgr.buffer("d1", _table(2), 5)
        mgr.drain(timeout_s=10)
    sm.advance_cursor.assert_called_once_with("d1", 5, cumulative_rows=2)
    with pytest.raises(RuntimeError):
        mgr._executor.submit(lambda: None)  # executor is shut down


# ---------------------------------------------------------------------------
# M3 review findings: epoch guard, hardened failure path, drain, watermark
# ---------------------------------------------------------------------------


def test_stale_epoch_read_discarded_after_flush_failure():
    """THE race (QE H1 / architect #1): poll thread snapshots its read plan,
    a flush fails and resets the position mid-read, then the poll thread
    delivers the (now stale) batch. The epoch guard must discard it —
    accepting it would stamp the position past the dropped range, which
    would then never be re-read."""
    mgr, _, _ = _manager(cursors={"d1": 2}, flush_interval_seconds=0.0)

    # Poll thread captures its plan BEFORE the failure.
    plan = mgr.read_plan()
    pos, epoch = plan["d1"]
    assert (pos, epoch) == (2, 0)

    # In-flight flush fails -> reset + epoch bump.
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("down")):
        mgr.buffer("d1", _table(1), 5)  # pre-failure buffered data
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert mgr.positions() == {"d1": 2}

    # The overlapped read arrives with the stale epoch: discarded entirely.
    mgr.buffer("d1", _table(3), 9, epoch=epoch)
    mgr.advance_position("d1", 9, epoch=epoch)
    assert mgr.positions() == {"d1": 2}  # NOT 9
    assert mgr.status_snapshot()["d1"].buffer_rows == 0

    # The next cycle's plan sees the fresh epoch and re-reads the range.
    new_pos, new_epoch = mgr.read_plan()["d1"]
    assert (new_pos, new_epoch) == (2, 1)
    with patch("viaduck.delivery.append_only", return_value=3):
        mgr.buffer("d1", _table(3), 9, epoch=new_epoch)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 9


def test_failure_path_survives_record_error_raising():
    """QE H2: the invariant-restoring reset must happen even when the PG
    error write or pool evict ALSO fail (plausibly correlated outages)."""
    mgr, sm, pool = _manager(cursors={"d1": 2}, flush_interval_seconds=0.0)
    sm.record_error.side_effect = RuntimeError("pg also down")
    pool.evict.side_effect = RuntimeError("pool sad")

    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("dest down")):
        mgr.buffer("d1", _table(1), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()

    # Reset happened despite the secondary failures; nothing escaped.
    assert mgr.positions() == {"d1": 2}
    assert mgr.status_snapshot()["d1"].buffer_rows == 0
    assert "d1" not in mgr._inflight


def test_cursor_persist_retry_avoids_failure_path():
    """Architect #4: a transient cursor-persist failure after a successful
    destination commit retries instead of dropping the buffer."""
    mgr, sm, pool = _manager(cursors={"d1": 2}, flush_interval_seconds=0.0)
    sm.advance_cursor.side_effect = [RuntimeError("pg blip"), None]

    with patch("viaduck.delivery.append_only", return_value=1), patch("viaduck.delivery.time.sleep"):
        mgr.buffer("d1", _table(1), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()

    assert sm.advance_cursor.call_count == 2
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 5
    sm.record_error.assert_not_called()
    pool.evict.assert_not_called()


def test_drain_second_pass_flushes_rows_buffered_during_inflight_flush():
    """QE M1: rows buffered while a flush is in flight at shutdown get a
    second trigger pass — drain loops until quiet."""
    mgr, sm, _ = _manager(flush_interval_seconds=0.0)
    release = threading.Event()
    flushed_batches = []

    def slow_apply(pool, dest, batch, stop_event=None, deadline=None):
        flushed_batches.append(batch.num_rows)
        if len(flushed_batches) == 1:
            release.wait(5)
        return batch.num_rows

    with patch("viaduck.delivery.append_only", side_effect=slow_apply):
        mgr.buffer("d1", _table(2), 5)
        mgr.maybe_flush()  # first flush in flight
        mgr.buffer("d1", _table(3), 8)  # lands during the in-flight flush
        release.set()
        mgr.drain(timeout_s=30)

    assert flushed_batches == [2, 3]
    assert sm.advance_cursor.call_count == 2
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 8


def test_watermark_counts_inflight_bytes():
    """QE M2: swapped-out (in-flight) tables count toward the destination's
    queue. While the flush is in flight, the destination is at cap and its
    reads pause; once the flush completes, the queue drains and reads
    resume."""
    mgr, _, _ = _manager(buffer_total_max_bytes=1, flush_interval_seconds=3600.0)
    release = threading.Event()

    def slow_apply(pool, dest, batch, stop_event=None, deadline=None):
        release.wait(5)
        return batch.num_rows

    with patch("viaduck.delivery.append_only", side_effect=slow_apply):
        mgr.buffer("d1", _table(50), 5)
        assert mgr.maybe_flush(shutdown=True) == 1  # swap to in-flight
        # Live buffer empty, but in-flight bytes keep d1's queue at cap ->
        # d1's reads must pause (and, d1 being the only dest, all reads).
        assert mgr.should_pause_reads_for("d1")
        assert mgr.should_pause_all_reads()
        release.set()
        assert mgr.wait_idle()
    assert not mgr.should_pause_reads_for("d1")
    assert not mgr.should_pause_all_reads()


def test_on_flush_success_fires_for_data_not_for_idle_persists():
    hits = []
    cfg = DeliveryConfig(workers=1, flush_interval_seconds=0.0)
    sm = _state_mgr({"d1": 0})
    mgr = DeliveryManager(cfg, sm, MagicMock(), [], ["d1"], mode="append_only", on_flush_success=lambda: hits.append(1))

    # Idle position-only persist: no success signal.
    mgr.advance_position("d1", 3)
    mgr.maybe_flush()
    assert mgr.wait_idle()
    assert hits == []

    # Data flush: success signal fires.
    with patch("viaduck.delivery.append_only", return_value=1):
        mgr.buffer("d1", _table(1), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert hits == [1]


# ---------------------------------------------------------------------------
# Destination lifecycle hooks (viaduck/lifecycle.py)
# ---------------------------------------------------------------------------


def test_discard_buffer_rewinds_position_and_bumps_epoch():
    mgr, _, _ = _manager(cursors={"d1": 5})
    plan = mgr.read_plan()
    pos, epoch = plan["d1"]
    assert pos == 5
    mgr.buffer("d1", _table(4), through_snapshot=9, epoch=epoch)
    assert mgr.positions() == {"d1": 9}

    dropped = mgr.discard_buffer("d1")
    assert dropped == 4
    # Position rewound to the durable cursor — the discarded range will be
    # re-read on resume (controlled-crash semantics, same as FlushFail).
    assert mgr.positions() == {"d1": 5}
    # A read that overlapped the discard is rejected by the epoch guard.
    mgr.buffer("d1", _table(2), through_snapshot=9, epoch=epoch)
    assert mgr.status_snapshot()["d1"].buffer_rows == 0
    assert mgr.positions() == {"d1": 5}


def test_discard_buffer_noop_when_clean():
    mgr, _, _ = _manager(cursors={"d1": 5})
    assert mgr.discard_buffer("d1") == 0
    # Epoch untouched on the no-op path: an in-flight read may still land.
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(1), through_snapshot=6, epoch=epoch)
    assert mgr.status_snapshot()["d1"].buffer_rows == 1


def test_suspended_destination_never_flushes():
    mgr, _, _ = _manager()
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", fake):
        mgr.buffer("d1", _table(3), through_snapshot=7)
        mgr.set_suspended({"d1"})
        assert mgr.maybe_flush(shutdown=True) == 0
        assert calls == []
        # Unsuspend: the same trigger now fires.
        mgr.set_suspended(set())
        assert mgr.maybe_flush(shutdown=True) == 1
        assert calls[0][0] == "d1"


def test_is_clean_tracks_buffer_and_position():
    mgr, _, _ = _manager(cursors={"d1": 5})
    assert mgr.is_clean("d1")
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(2), through_snapshot=8, epoch=epoch)
    assert not mgr.is_clean("d1")
    mgr.discard_buffer("d1")
    assert mgr.is_clean("d1")


def test_position_only_advance_is_not_clean():
    # An advanced position with no data still means the durable cursor is
    # behind (a lazy persist is pending) — draining must wait for it.
    mgr, _, _ = _manager(cursors={"d1": 5})
    _, epoch = mgr.read_plan()["d1"]
    mgr.advance_position("d1", 9, epoch=epoch)
    assert not mgr.is_clean("d1")


def test_flush_success_after_discard_restores_position():
    # Review finding: pause racing an in-flight flush left position <
    # flushed after the flush committed — on resume the already-applied
    # range was re-read and re-applied (deterministic duplicates in
    # append_only). The success path must restore position >= through.
    import threading

    mgr, sm, _ = _manager(cursors={"d1": 5})
    _, epoch = mgr.read_plan()["d1"]
    mgr.advance_position("d1", 9, epoch=epoch)  # position-only: no data write path

    gate = threading.Event()
    entered = threading.Event()

    def _blocking_advance(dest_id, through, cumulative, attempts=3):
        entered.set()
        gate.wait(timeout=10)
        sm.advance_cursor(dest_id, through, cumulative)

    with patch.object(mgr, "_advance_cursor_with_retry", _blocking_advance):
        assert mgr.maybe_flush(shutdown=True) == 1  # real _flush, empty tables
        assert entered.wait(timeout=10)
        # Lifecycle pause lands mid-flush: rewinds position to flushed=5.
        mgr.discard_buffer("d1")
        assert mgr.positions()["d1"] == 5
        gate.set()
        assert mgr.wait_idle(timeout_s=10)

    # Flush succeeded through 9: position restored, no re-read of (5, 9].
    assert mgr.positions()["d1"] == 9
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 9
    assert mgr.is_clean("d1")


# ---------------------------------------------------------------------------
# Retention-edge clamp
# ---------------------------------------------------------------------------


def test_clamp_to_retention_advances_cursor_and_persists():
    mgr, sm, _ = _manager(cursors={"d1": 10})
    old = mgr.clamp_to_retention("d1", 99)
    assert old == 10
    # Durable persist first, count preserved, loss recorded on the row.
    sm.advance_cursor.assert_called_once_with("d1", 99, cumulative_rows=None)
    sm.record_error.assert_called_once()
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 99
    assert snap.position_snapshot == 99
    assert "clamped to retention edge" in snap.last_error


def test_clamp_to_retention_noop_when_at_or_past_floor():
    mgr, sm, _ = _manager(cursors={"d1": 99})
    assert mgr.clamp_to_retention("d1", 99) is None
    assert mgr.clamp_to_retention("d1", 50) is None
    sm.advance_cursor.assert_not_called()


def test_clamp_keeps_buffer_and_epoch_when_position_ahead_of_floor():
    # Flushed expired but position past the floor (buffered data): buffered
    # rows are valid reads and are kept; no epoch bump (no read-overlap
    # hazard — position was not moved). Only flushed comes up to the floor,
    # so a later failure rewind targets the floor, not the expired cursor.
    mgr, _, _ = _manager(cursors={"d1": 10})
    _, epoch0 = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(3), through_snapshot=120, epoch=epoch0)
    assert mgr.clamp_to_retention("d1", 99) == 10
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 99
    assert snap.position_snapshot == 120
    assert snap.buffer_rows == 3
    _, epoch1 = mgr.read_plan()["d1"]
    assert epoch1 == epoch0


def test_clamp_discards_overlapping_read_via_epoch():
    # A CDC read captured before the clamp lands after it: buffer() stamps
    # position unconditionally, so without the epoch bump the read would
    # rewind position below the floor. The stale epoch rejects it.
    mgr, _, _ = _manager(cursors={"d1": 10})
    _, epoch0 = mgr.read_plan()["d1"]
    mgr.clamp_to_retention("d1", 99)
    mgr.buffer("d1", _table(2), through_snapshot=40, epoch=epoch0)
    assert mgr.positions() == {"d1": 99}
    assert mgr.status_snapshot()["d1"].buffer_rows == 0


def test_flush_failure_after_clamp_rewinds_to_floor():
    mgr, _, _ = _manager(cursors={"d1": 10})
    mgr.clamp_to_retention("d1", 99)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(2), through_snapshot=150, epoch=epoch)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("down")):
        assert mgr.maybe_flush(shutdown=True) == 1
        assert mgr.wait_idle()
    # The failure rewind targets the clamped cursor, not the expired one —
    # the re-read starts inside retention instead of raising again.
    assert mgr.positions() == {"d1": 99}


def test_zombie_flush_cannot_regress_clamp():
    # A flush submitted BEFORE the clamp completes AFTER it with
    # through < floor: the success path's max-guard on flushed keeps the
    # clamp, the cursor gauge is not regressed, and the clamp's loss note
    # survives (a zombie flush must not clear an error it knows nothing
    # about). The gate blocks only the worker's advance (through=40); the
    # clamp's own durable write (through=99) proceeds.
    mgr, sm, _ = _manager(cursors={"d1": 10})
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(2), through_snapshot=40, epoch=epoch)

    gate = threading.Event()
    entered = threading.Event()

    def _gated_advance(dest_id, through, cumulative, attempts=3):
        if through == 40:
            entered.set()
            gate.wait(timeout=10)
        return 1

    with (
        patch.object(mgr, "_advance_cursor_with_retry", _gated_advance),
        patch("viaduck.delivery.append_only", return_value=2),
    ):
        assert mgr.maybe_flush(shutdown=True) == 1
        assert entered.wait(timeout=5)
        assert mgr.clamp_to_retention("d1", 99) == 10
        gate.set()
        assert mgr.wait_idle()
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 99
    assert snap.position_snapshot >= 99
    assert snap.last_error is not None and "clamped to retention edge" in snap.last_error


def test_clamp_lost_race_returns_none_without_loss_note():
    # TOCTOU guard: a concurrent flush advanced the durable cursor past
    # the floor between the clamp's check and its write — the monotonic
    # guard reports 0 rows. Nothing was lost; no phantom loss note.
    mgr, sm, _ = _manager(cursors={"d1": 10})
    sm.advance_cursor.return_value = 0
    assert mgr.clamp_to_retention("d1", 99) is None
    sm.record_error.assert_not_called()
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 10  # memory untouched
    assert snap.last_error is None


def test_clamp_memory_race_recheck_under_lock():
    # The durable write landed (rowcount 1) but a flush success updated
    # the in-memory cursor past the floor before the clamp's stamp: the
    # under-lock re-check bails without stamping a stale loss note.
    mgr, sm, _ = _manager(cursors={"d1": 10})

    def _advance_then_race(dest_id, snapshot_id, cumulative_rows=None):
        with mgr._lock:
            mgr._flushed[dest_id] = 150
            mgr._position[dest_id] = 150
        return 1

    sm.advance_cursor.side_effect = _advance_then_race
    assert mgr.clamp_to_retention("d1", 99) is None
    sm.record_error.assert_not_called()
    assert mgr.status_snapshot()["d1"].last_error is None


def test_clamp_durable_write_precedes_memory_stamp():
    # Pins durable-before-memory ordering: at advance_cursor time the
    # in-memory cursor must still be the old value (crash between the two
    # re-runs the clamp as a no-op; the reverse order would not).
    mgr, sm, _ = _manager(cursors={"d1": 10})
    seen = []

    def _observe(dest_id, snapshot_id, cumulative_rows=None):
        seen.append(mgr.flushed_snapshots()["d1"])
        return 1

    sm.advance_cursor.side_effect = _observe
    assert mgr.clamp_to_retention("d1", 99) == 10
    assert seen == [10]
    assert mgr.flushed_snapshots()["d1"] == 99


def test_clamp_durable_failure_leaves_memory_untouched():
    # Failure atomicity: if the durable write fails (after the retry
    # budget), the exception propagates to the caller (which excludes the
    # destination from the cycle) and NO in-memory state moved.
    mgr, sm, _ = _manager(cursors={"d1": 10})
    sm.advance_cursor.side_effect = RuntimeError("pg down")
    with patch("viaduck.delivery.time.sleep"), pytest.raises(RuntimeError):
        mgr.clamp_to_retention("d1", 99)
    sm.record_error.assert_not_called()
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 10
    assert snap.position_snapshot == 10
    assert snap.last_error is None


def test_clamp_outcome_wording_lost_vs_at_risk():
    # Never-read range: hard loss. Read-but-buffered range: at risk only —
    # the note must not claim "unread" for rows sitting in the buffer.
    mgr, _, _ = _manager(dests=("lost", "risk"), cursors={"lost": 10, "risk": 10})
    _, epoch = mgr.read_plan()["risk"]
    mgr.buffer("risk", _table(3), through_snapshot=120, epoch=epoch)

    assert mgr.clamp_to_retention("lost", 99) == 10
    assert mgr.clamp_to_retention("risk", 99) == 10
    snaps = mgr.status_snapshot()
    assert "expired UNREAD" in snaps["lost"].last_error
    assert "lost only if" in snaps["risk"].last_error


# ---------------------------------------------------------------------------
# Membership (C3 §1 stop contract: active set, dicts persist)
# ---------------------------------------------------------------------------


def test_remove_destination_stops_submissions_but_keeps_state():
    mgr, _, _ = _manager(dests=("d1", "d2"))
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(3), through_snapshot=7, epoch=epoch)
    mgr.remove_destination("d1")
    with patch.object(mgr, "_flush", fake):
        # No submission for the removed id, even at shutdown pressure —
        # without the active filter a stopped dest with position > flushed
        # would submit position-persist flushes forever.
        assert mgr.maybe_flush(shutdown=True) == 0
    assert calls == []
    # Out of the read plan and status; dict family retained underneath.
    assert "d1" not in mgr.read_plan()
    assert "d1" not in mgr.status_snapshot()
    assert "d1" in mgr._buffers
    assert mgr.is_clean("d1") is False  # still queryable (pending drain latch)
    mgr.remove_destination("d1")  # idempotent


def test_re_add_max_merges_and_preserves_epoch():
    mgr, sm, _ = _manager(cursors={"d1": 5})
    _, epoch0 = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(2), through_snapshot=9, epoch=epoch0)
    mgr.remove_destination("d1")
    sm.load_cursors.reset_mock()
    mgr.add_destination("d1")
    # Max-merge: surviving entries reused — no cursor reload, no epoch
    # reset (a reset would let a pre-stop read regress position), no
    # position/flushed rewind.
    sm.load_cursors.assert_not_called()
    pos, epoch1 = mgr.read_plan()["d1"]
    assert pos == 9
    assert epoch1 == epoch0
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 5


def test_add_destination_new_id_initializes_from_cursor():
    mgr, sm, _ = _manager(dests=("d1",))
    c = MagicMock()
    c.last_snapshot_id = 42
    c.rows_replicated = 7
    c.last_error = None
    sm.load_cursors.return_value = {"dyn": c}
    mgr.add_destination("dyn")
    snap = mgr.status_snapshot()["dyn"]
    assert snap.flushed_snapshot == 42
    assert snap.position_snapshot == 42
    assert snap.rows_replicated == 7


def test_membership_change_recomputes_auto_cap():
    mgr, _, _ = _manager(dests=("d1", "d2"), buffer_total_max_bytes=100)
    assert mgr._per_dest_cap == 50
    mgr.remove_destination("d2")
    assert mgr._per_dest_cap == 100
    mgr.add_destination("d2")
    assert mgr._per_dest_cap == 50


def test_membership_change_shrinks_oversubscribed_explicit_cap():
    # Startup honors an oversubscribed explicit cap (WARN only — existing
    # contract); a MEMBERSHIP CHANGE enforces the total as the bound and
    # shrinks to fair share, because dynamic growth would otherwise erode
    # buffer_total_max_bytes as the effective memory limit.
    mgr, sm, _ = _manager(
        dests=("d1", "d2"),
        buffer_total_max_bytes=100,
        buffer_max_bytes_per_destination=80,
    )
    assert mgr._per_dest_cap == 80  # startup: honored
    c = MagicMock()
    c.last_snapshot_id = 0
    c.rows_replicated = 0
    c.last_error = None
    sm.load_cursors.return_value = {"d3": c}
    mgr.add_destination("d3")
    assert mgr._per_dest_cap == 100 // 3  # 3 x 80 > 100 -> fair share
    mgr.remove_destination("d3")
    mgr.remove_destination("d2")
    assert mgr._per_dest_cap == 80  # 1 x 80 <= 100 -> explicit honored again


def test_add_destination_loads_cursor_outside_lock_and_fails_atomically():
    mgr, sm, _ = _manager(dests=("d1",))

    def _probe_lock(ids):
        # The module's lock discipline: no state-store I/O under _lock. If
        # the manager lock were held here, this acquire would fail and a
        # real PG stall would freeze every flush worker.
        assert mgr._lock.acquire(blocking=False), "state-store read under the manager lock"
        mgr._lock.release()
        raise RuntimeError("pg down")

    sm.load_cursors.side_effect = _probe_lock
    with pytest.raises(RuntimeError):
        mgr.add_destination("dyn")
    # Failure atomicity: not activated, no partial dict entries.
    assert "dyn" not in mgr.active_ids()
    assert "dyn" not in mgr._buffers


def test_draining_destination_still_flushes_under_active_filter():
    # Draining is in _active and NOT in _suspended; the new active filter
    # in maybe_flush must not stop its flush-out (parity invariant from
    # the stage-2 no-op review).
    mgr, _, _ = _manager()
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(3), through_snapshot=7, epoch=epoch)
    # Lifecycle 'draining': excluded from reads by the caller, not
    # suspended, still active.
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush(shutdown=True) == 1
    assert calls[0][0] == "d1"


# ---------------------------------------------------------------------------
# Flush-batch slicing (2026-07-29: bounded append batches — the fork's
# native layer corrupts on 170-440K-row appends; one swap takes at most
# flush_batch_max_rows, cut at chunk boundaries)
# ---------------------------------------------------------------------------


def test_slice_takes_chunks_up_to_cap_with_boundary_cursor():
    mgr, sm, _ = _manager(flush_batch_max_rows=5)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(3), through_snapshot=10, epoch=epoch)
    mgr.buffer("d1", _table(2), through_snapshot=20, epoch=epoch)
    mgr.buffer("d1", _table(4), through_snapshot=30, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush(shutdown=True) == 1
    # 3+2=5 fits the cap; adding the 4-row chunk would exceed it.
    dest, tables, through, _trigger = calls[0]
    assert sum(t.num_rows for t in tables) == 5
    # Cursor for a partial swap = last INCLUDED chunk's through, never
    # the live position (which covers the chunk left behind).
    assert through == 20
    # Remainder stays buffered.
    assert mgr.status_snapshot()["d1"].buffer_rows == 4


def test_single_oversize_chunk_is_split_within_entry():
    # 2026-08: a single destination-heavy read unit arrives as ONE entry
    # that used to bypass both caps and flush whole (the team-2
    # ~100K-op/145-468s mega-append episode). Now the entry is cut
    # within: the head slice flushes at the cap, and — because the
    # entry's coverage is indivisible — the head's flush must NOT
    # persist the entry's cov. The tail keeps cov/hi at the buffer head.
    mgr, _, _ = _manager(flush_batch_max_rows=2)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(7), through_snapshot=10, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush(shutdown=True) == 1
    assert sum(t.num_rows for t in calls[0][1]) == 2  # head slice at cap
    assert calls[0][2] is None  # partial entry: persist nothing
    assert mgr.status_snapshot()["d1"].buffer_rows == 5  # tail buffered


def test_split_entry_drains_and_advances_cursor_only_on_completion():
    # Drain the 7-row entry in 2-row slices: cursor persists the entry's
    # cov only on the flush that completes it, never before.
    mgr, _, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(7), through_snapshot=10, epoch=epoch)
    seen = []

    def _record(pool, d, b, **kw):
        seen.append(b.num_rows)
        return b.num_rows

    with patch("viaduck.delivery.append_only", side_effect=_record):
        for _ in range(8):
            mgr.maybe_flush(shutdown=True)
            assert mgr.wait_idle()
            snap = mgr.status_snapshot()["d1"]
            if snap.buffer_rows > 0:
                # Entry not yet fully delivered: cov must not be durable.
                assert snap.flushed_snapshot == 0
            if mgr.is_clean("d1"):
                break
    assert seen == [2, 2, 2, 1]  # bounded slices, tail remainder last
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 10


def test_full_swap_persists_through_position():
    # No remainder: historical behavior — a position-only advance beyond
    # the last chunk still persists (lazy cursor persist).
    mgr, _, _ = _manager(flush_batch_max_rows=100)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(3), through_snapshot=10, epoch=epoch)
    mgr.advance_position("d1", 15, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush(shutdown=True) == 1
    assert calls[0][2] == 15


def test_sliced_pile_drains_in_bounded_batches():
    mgr, sm, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    for i in range(1, 5):
        mgr.buffer("d1", _table(2), through_snapshot=i * 10, epoch=epoch)
    seen = []

    def _record(pool, d, b, **kw):
        seen.append(b.num_rows)
        return b.num_rows

    with patch("viaduck.delivery.append_only", side_effect=_record):
        for _ in range(6):
            mgr.maybe_flush(shutdown=True)
            assert mgr.wait_idle()
            if mgr.is_clean("d1"):
                break
    assert seen == [2, 2, 2, 2]  # four bounded batches, never the pile
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 40


def test_failure_drops_remainder_too():
    # FlushFail semantics unchanged: the live buffer (incl. the sliced
    # remainder) drops with the in-flight tables; the whole range
    # re-reads from the durable cursor.
    mgr, _, _ = _manager(flush_batch_max_rows=2)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(2), through_snapshot=10, epoch=epoch)
    mgr.buffer("d1", _table(2), through_snapshot=20, epoch=epoch)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("down")):
        assert mgr.maybe_flush(shutdown=True) == 1
        assert mgr.wait_idle()
    snap = mgr.status_snapshot()["d1"]
    assert snap.buffer_rows == 0
    assert snap.position_snapshot == snap.flushed_snapshot == 0


def test_sliced_remainder_drains_without_waiting_for_interval():
    # Review F1 (confirmed empirically): a remainder below the rows/bytes
    # thresholds must NOT wait out flush_interval — realistic interval,
    # non-shutdown maybe_flush calls only.
    mgr, _, _ = _manager(flush_batch_max_rows=4, flush_interval_seconds=120.0)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(4), through_snapshot=10, epoch=epoch)
    mgr.buffer("d1", _table(2), through_snapshot=20, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1  # rows trigger fires slice 1 (4 rows)
        assert mgr.wait_idle()
        # The 2-row remainder is below every ordinary trigger — the
        # sliced fast-path submits it instead of waiting out 120s.
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle()
    assert [sum(t.num_rows for t in c[1]) for c in calls] == [4, 2]
    assert [c[3] for c in calls] == ["target", "sliced"]
    assert [c[2] for c in calls] == [10, 20]


# ---------------------------------------------------------------------------
# Adaptive flush sizing (AIMD on flush duration)
# ---------------------------------------------------------------------------

_K = 1_000  # rows


def _adaptive_manager(dests=("d1", "d2"), **over):
    defaults = dict(
        flush_batch_max_rows=128 * _K,
        flush_adaptive_low_seconds=5.0,
        flush_adaptive_high_seconds=30.0,
        flush_adaptive_step_rows=16 * _K,
        flush_adaptive_min_rows=8 * _K,
    )
    defaults.update(over)
    return _manager(dests=dests, **defaults)


def _full(mgr, dest="d1"):
    """batch_rows that satisfies the growth fill-gate for dest's current target."""
    return mgr._flush_target[dest]


def test_adaptive_slow_flush_halves_target():
    mgr, _, _ = _adaptive_manager()
    mgr._adapt_flush_target("d1", 31.0, _full(mgr))
    assert mgr._flush_target["d1"] == 64 * _K


def test_adaptive_fast_full_flush_grows_additively_to_cap():
    mgr, _, _ = _adaptive_manager()
    mgr._flush_target["d1"] = 100 * _K
    mgr._adapt_flush_target("d1", 1.0, _full(mgr))
    assert mgr._flush_target["d1"] == 116 * _K
    mgr._adapt_flush_target("d1", 1.0, _full(mgr))
    assert mgr._flush_target["d1"] == 128 * _K  # capped at flush_batch_max_rows
    mgr._adapt_flush_target("d1", 1.0, _full(mgr))
    assert mgr._flush_target["d1"] == 128 * _K


def test_adaptive_fast_but_small_flush_does_not_grow():
    # The growth fill-gate: a tiny interval flush finishing in <1s carries
    # no evidence that a TARGET-sized batch is sustainable. Without the
    # gate, quiet-period trickle flushes walk a learned-down target back
    # to the cap and the next burst re-runs the oversize-flush cycle.
    mgr, _, _ = _adaptive_manager()
    mgr._flush_target["d1"] = 16 * _K
    mgr._adapt_flush_target("d1", 0.5, 1 * _K)  # well under 70% fill
    assert mgr._flush_target["d1"] == 16 * _K
    mgr._adapt_flush_target("d1", 0.5, 12 * _K)  # 75% fill: grows
    assert mgr._flush_target["d1"] == 32 * _K


def test_adaptive_in_band_holds():
    mgr, _, _ = _adaptive_manager()
    mgr._flush_target["d1"] = 64 * _K
    for d in (5.0, 12.0, 30.0):  # hold band is inclusive at both edges
        mgr._adapt_flush_target("d1", d, _full(mgr))
        assert mgr._flush_target["d1"] == 64 * _K


def test_adaptive_halving_floors_at_min_rows():
    mgr, _, _ = _adaptive_manager()
    for _ in range(20):
        mgr._adapt_flush_target("d1", 999.0, _full(mgr))
    assert mgr._flush_target["d1"] == 8 * _K


def test_adaptive_floor_clamps_to_ceiling():
    # flush_batch_max_rows below flush_adaptive_min_rows: the effective floor is
    # the ceiling (one-knob change), never above it.
    mgr, _, _ = _adaptive_manager(flush_batch_max_rows=4 * _K, flush_adaptive_min_rows=8 * _K)
    mgr._adapt_flush_target("d1", 999.0, _full(mgr))
    assert mgr._flush_target["d1"] == 4 * _K


def test_adaptive_failed_flush_shrinks_when_slow_never_grows():
    mgr, _, _ = _adaptive_manager()
    mgr._flush_target["d1"] = 64 * _K
    # Fast failure (connection blip): must not inflate the target.
    mgr._adapt_flush_target("d1", 0.5, _full(mgr), failed=True)
    assert mgr._flush_target["d1"] == 64 * _K
    # Slow failure (retry budget burned): the flush was too big — shrink.
    mgr._adapt_flush_target("d1", 300.0, _full(mgr), failed=True)
    assert mgr._flush_target["d1"] == 32 * _K


def test_adaptive_disabled_holds_target():
    mgr, _, _ = _adaptive_manager(flush_adaptive=False)
    mgr._adapt_flush_target("d1", 999.0, _full(mgr))
    mgr._adapt_flush_target("d1", 0.1, _full(mgr))
    assert mgr._flush_target["d1"] == 128 * _K


def test_adaptive_targets_are_per_destination():
    mgr, _, _ = _adaptive_manager()
    mgr._adapt_flush_target("d1", 999.0, _full(mgr))
    assert mgr._flush_target["d1"] == 64 * _K
    assert mgr._flush_target["d2"] == 128 * _K


# ---------------------------------------------------------------------------
# AIMD feedback-signal span: the controller eats the DESTINATION-WRITE span,
# never the shared-PG cursor-persist tail (2026-08-28 review round 2).
# ---------------------------------------------------------------------------


def test_slow_cursor_persist_does_not_halve_target(monkeypatch):
    """A slow cursor persist (shared PG) after a healthy write is not
    destination-contention evidence — the target must not move."""
    mgr, sm, _ = _manager(flush_adaptive_low_seconds=0.01, flush_adaptive_high_seconds=0.05)
    tbl = _table(5)
    monkeypatch.setattr("viaduck.delivery.append_only", lambda *a, **k: tbl.num_rows)

    def slow_advance(*a, **k):
        _time_mod.sleep(0.2)  # cursor tail blows past the high bound
        return 1

    monkeypatch.setattr(sm, "advance_cursor", slow_advance)
    mgr._flush("d1", [tbl], 10, "interval")
    assert mgr._flush_target["d1"] == mgr._cfg.flush_batch_max_rows


def test_slow_apply_halves_target(monkeypatch):
    """...and the span that IS the destination's still teaches the controller."""
    mgr, _, _ = _manager(flush_adaptive_low_seconds=0.01, flush_adaptive_high_seconds=0.05)

    def slow_apply(*a, **k):
        _time_mod.sleep(0.2)
        return 5

    monkeypatch.setattr("viaduck.delivery.append_only", slow_apply)
    mgr._flush("d1", [_table(5)], 10, "interval")
    assert mgr._flush_target["d1"] == mgr._cfg.flush_batch_max_rows // 2


def test_cursor_persist_failure_after_healthy_write_does_not_halve(monkeypatch):
    """Apply committed, cursor persist failed: the retry burn is shared-PG
    evidence; the (fast) write is the destination's. No halve."""
    mgr, sm, _ = _manager(flush_adaptive_low_seconds=0.01, flush_adaptive_high_seconds=0.05)
    monkeypatch.setattr("viaduck.delivery.append_only", lambda *a, **k: 5)
    sm.advance_cursor.side_effect = RuntimeError("pg down")
    mgr._flush("d1", [_table(5)], 10, "interval")
    assert mgr._flush_target["d1"] == mgr._cfg.flush_batch_max_rows


def test_target_trigger_uses_adapted_target():
    # Shrink d1's target below the buffered rows while the ceiling
    # (flush_batch_max_rows) stays far above them.
    mgr, _, _ = _manager(
        dests=("d1", "d2"),
        flush_batch_max_rows=1_000_000,
        flush_adaptive_min_rows=1,
    )
    tbl = _table(100)
    fake, calls = _recording_flush(mgr)
    mgr.buffer("d1", tbl, 5)
    mgr.buffer("d2", tbl, 5)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 0  # below the ceiling: nothing fires
        with mgr._lock:
            mgr._flush_target["d1"] = 100  # adapted down
        assert mgr.maybe_flush() == 1  # d1 fires on ITS target; d2 holds
    assert [c[0] for c in calls] == ["d1"]
    assert calls[0][3] == "target"


def test_swap_is_row_cut_at_adapted_target():
    # The QE-review MAJOR: the target must bound the SWAP, not just the
    # trigger — otherwise a backlogged destination (flush in flight while
    # reads continue) drains in ceiling-sized batches no matter how far
    # the target adapted down, which is exactly the contended-catalog
    # regime the controller exists for.
    mgr, _, _ = _manager(flush_batch_max_rows=1_000_000, flush_adaptive_min_rows=1)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    for through in (10, 20, 30, 40):
        mgr.buffer("d1", _table(100), through_snapshot=through, epoch=epoch)
    with mgr._lock:
        mgr._flush_target["d1"] = 200  # fits two 100-row chunks
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle()
        assert mgr.maybe_flush() == 1  # sliced remainder drains, also row-cut
        assert mgr.wait_idle()
    assert [len(c[1]) for c in calls] == [2, 2]  # two chunks per swap, not four
    assert [c[2] for c in calls] == [20, 40]  # cursor at last included chunk
    assert calls[1][3] == "sliced"


def test_swap_row_cut_fixed_at_ceiling_with_adaptive_off():
    # flush_adaptive: false = fixed target at the flush_batch_max_rows
    # ceiling; the swap takes up to the ceiling in rows, nothing more.
    mgr, _, _ = _manager(flush_batch_max_rows=250, flush_adaptive=False)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    for through in (10, 20, 30):
        mgr.buffer("d1", _table(100), through_snapshot=through, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
    assert len(calls[0][1]) == 2  # 250-row target: two whole 100-row chunks
    assert mgr.status_snapshot()["d1"].buffer_rows == 100  # tail stays buffered


def test_swap_row_cut_splits_single_chunk_at_floor():
    # A single entry larger than the rows target is cut WITHIN, so the
    # target binds unconditionally. At a degenerate 1-row target the head
    # slice is the 1-row minimum, and the partial-entry flush persists
    # nothing.
    mgr, _, _ = _manager(flush_batch_max_rows=1_000_000, flush_adaptive_min_rows=1)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(100), through_snapshot=10, epoch=epoch)
    with mgr._lock:
        mgr._flush_target["d1"] = 1  # far below one chunk
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
    assert sum(t.num_rows for t in calls[0][1]) == 1  # 1-row floor slice
    assert calls[0][2] is None  # partial entry: persist nothing
    assert mgr.status_snapshot()["d1"].buffer_rows == 99


def test_memory_trigger_fires_regardless_of_target():
    # Watermark pressure must not wait for the rows target — a grown
    # target cannot defer memory relief.
    mgr, _, _ = _manager(buffer_total_max_bytes=1, flush_batch_max_rows=1_000_000_000)
    fake, calls = _recording_flush(mgr)
    mgr.buffer("d1", _table(10), 5)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
    assert calls[0][3] == "memory"


def test_add_destination_initializes_target_and_keeps_learned_on_readd():
    mgr, _, _ = _adaptive_manager(dests=("d1",))
    mgr.add_destination("d3")
    assert mgr._flush_target["d3"] == 128 * _K
    mgr._adapt_flush_target("d3", 999.0, _full(mgr, "d3"))
    mgr.remove_destination("d3")
    mgr.add_destination("d3")  # MAX-MERGE: surviving entries are reused
    assert mgr._flush_target["d3"] == 64 * _K


def test_flush_success_feeds_controller_with_measured_duration():
    # End-to-end wiring: a real _flush whose apply layer is slow must
    # halve the target from the MEASURED duration (no synthetic values).
    mgr, _, _ = _manager(
        cursors={"d1": 0},
        flush_interval_seconds=0.0,
        flush_batch_max_rows=128 * _K,
        flush_adaptive_high_seconds=0.02,
        flush_adaptive_low_seconds=0.01,
    )

    def slow_apply(*a, **k):
        import time as _t

        _t.sleep(0.05)
        return 3

    with patch("viaduck.delivery.append_only", side_effect=slow_apply):
        mgr.buffer("d1", _table(3), 7)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert mgr._flush_target["d1"] == 64 * _K


def test_flush_failure_feeds_controller_as_failed():
    mgr, _, _ = _manager(cursors={"d1": 0}, flush_interval_seconds=0.0)
    with (
        patch("viaduck.delivery.append_only", side_effect=RuntimeError("down")),
        patch.object(mgr, "_adapt_flush_target") as adapt,
    ):
        mgr.buffer("d1", _table(3), 7)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    adapt.assert_called_once()
    assert adapt.call_args.kwargs["failed"] is True
    assert adapt.call_args.args[0] == "d1"
    assert adapt.call_args.args[2] > 0  # batch_rows threaded through


def test_position_only_flush_does_not_feed_controller():
    # No tables — an empty position persist's duration carries no signal
    # about batch size and must not move the target. Success path.
    mgr, _, _ = _manager(cursors={"d1": 0}, flush_interval_seconds=0.0)
    with patch.object(mgr, "_adapt_flush_target") as adapt:
        mgr.advance_position("d1", 9)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 9
    adapt.assert_not_called()


def test_position_only_flush_failure_does_not_feed_controller():
    # Failure path of an empty persist: the `if tables` guard in the
    # except block, not just the success path.
    mgr, sm, _ = _manager(cursors={"d1": 0}, flush_interval_seconds=0.0)
    sm.advance_cursor.side_effect = RuntimeError("pg down")
    with patch.object(mgr, "_adapt_flush_target") as adapt:
        mgr.advance_position("d1", 9)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    adapt.assert_not_called()


def test_flush_target_gauge_seeded_at_startup_and_add():
    from viaduck import metrics

    mgr, _, _ = _adaptive_manager(dests=("d1",))
    g = metrics.dest_flush_target_rows.labels(destination="d1")
    assert g._value.get() == 128 * _K
    mgr.add_destination("d9")
    assert metrics.dest_flush_target_rows.labels(destination="d9")._value.get() == 128 * _K


def test_adaptive_converges_from_ceiling_on_sustained_slow_flushes():
    # A destination that opens at the ceiling into a contended catalog
    # (portola's shape): one halving per slow flush, straight to the floor —
    # 128K -> 64K -> 32K -> 16K -> 8K (floor), then pinned.
    mgr, _, _ = _adaptive_manager()
    seq = []
    for _ in range(6):
        mgr._adapt_flush_target("d1", 999.0, _full(mgr))
        seq.append(mgr._flush_target["d1"])
    assert seq == [64 * _K, 32 * _K, 16 * _K, 8 * _K, 8 * _K, 8 * _K]


def test_adaptive_reprobe_heals_dead_zone_ratchet():
    # The [low, high] dead zone is a one-way ratchet WITHOUT the re-probe:
    # a destination halved by one transient >high blip must be able to grow
    # back without a restart. reprobe_after consecutive in-band, >=70%-fill,
    # successful flushes step the target up once.
    mgr, _, _ = _adaptive_manager(flush_adaptive_reprobe_after=3)
    mgr._adapt_flush_target("d1", 999.0, _full(mgr))  # halve to 64K
    assert mgr._flush_target["d1"] == 64 * _K
    # Two in-band full flushes: streak builds, no growth yet.
    for _ in range(2):
        mgr._adapt_flush_target("d1", 12.0, _full(mgr))
        assert mgr._flush_target["d1"] == 64 * _K
    # An under-filled in-band flush resets the streak...
    mgr._adapt_flush_target("d1", 12.0, 1 * _K)
    for _ in range(2):
        mgr._adapt_flush_target("d1", 12.0, _full(mgr))
        assert mgr._flush_target["d1"] == 64 * _K
    # ...and the third consecutive full in-band flush re-probes upward.
    mgr._adapt_flush_target("d1", 12.0, _full(mgr))
    assert mgr._flush_target["d1"] == 80 * _K
    # Failures reset the streak and never grow.
    mgr._adapt_flush_target("d1", 12.0, _full(mgr), failed=True)
    for _ in range(2):
        mgr._adapt_flush_target("d1", 12.0, _full(mgr))
        assert mgr._flush_target["d1"] == 80 * _K


def test_byte_invariance_fencing():
    # THE kill-the-class test (flush-sizing endgame, review finding 10): a
    # buffer whose tables' nbytes are inflated ~150x by a shared dictionary
    # must produce byte-IDENTICAL trigger, cut, and AIMD decisions as its
    # uninflated twin. Bytes cannot re-enter the control path — this test
    # is the executable form of that contract.
    def inflated_table(n: int) -> pa.Table:
        # Slice of a dictionary-encoded parent: every slice counts the
        # ENTIRE shared dictionary in nbytes (the 08-26 mechanism).
        dictionary = pa.array(["v" * 200 + str(i) for i in range(20_000)])
        indices = pa.array([i % 10 for i in range(100_000)], type=pa.int32())
        parent = pa.table(
            {
                "a": pa.array(range(100_000), type=pa.int64()),
                "s": pa.DictionaryArray.from_arrays(indices, dictionary),
            }
        )
        return parent.slice(0, n)

    plain = _table(100)
    inflated = inflated_table(100)
    assert inflated.nbytes > 100 * plain.nbytes, "fixture must actually inflate"

    mgr_plain, _, _ = _manager(dests=("d1",), flush_interval_seconds=3600.0)
    mgr_infl, _, _ = _manager(dests=("d1",), flush_interval_seconds=3600.0)
    fake_p, calls_p = _recording_flush(mgr_plain)
    fake_i, calls_i = _recording_flush(mgr_infl)
    _, epoch_p = mgr_plain.read_plan()["d1"]
    _, epoch_i = mgr_infl.read_plan()["d1"]
    mgr_plain.buffer("d1", plain, through_snapshot=10, epoch=epoch_p)
    mgr_infl.buffer("d1", inflated, through_snapshot=10, epoch=epoch_i)
    with patch.object(mgr_plain, "_flush", fake_p), patch.object(mgr_infl, "_flush", fake_i):
        # Identical trigger decisions despite ~100x different buffer bytes...
        assert mgr_plain.maybe_flush() == mgr_infl.maybe_flush() == 0  # 100 rows < 60K target
        for m, t in ((mgr_plain, plain), (mgr_infl, inflated)):
            with m._lock:
                m._flush_target["d1"] = 100
        assert mgr_plain.maybe_flush() == mgr_infl.maybe_flush() == 1
    # ...identical cut (rows taken) and trigger labels...
    assert [(c[0], sum(t.num_rows for t in c[1]), c[2], c[3]) for c in calls_p] == [
        (c[0], sum(t.num_rows for t in c[1]), c[2], c[3]) for c in calls_i
    ]
    # ...and identical AIMD decisions for identical durations.
    for dur in (999.0, 1.0, 12.0):
        mgr_plain._adapt_flush_target("d1", dur, 100)
        mgr_infl._adapt_flush_target("d1", dur, 100)
        assert mgr_plain._flush_target["d1"] == mgr_infl._flush_target["d1"]


# ---------------------------------------------------------------------------
# Flush circuit breaker + flush deadline (slow-consumer isolation)
# ---------------------------------------------------------------------------


def _fail_flushes(mgr, n, start_through=1):
    """Drive n consecutive flush failures for d1 (rows trigger), simulating
    the resubmit backoff elapsing between iterations so each one submits.
    The state after the FINAL failure is left intact for assertions."""
    for i in range(n):
        with mgr._lock:
            mgr._circuit_open_until.pop("d1", None)  # backoff elapsed -> probe eligible
        mgr.buffer("d1", _table(1), start_through + i)
        submitted = mgr.maybe_flush()
        assert submitted == 1, f"failure-drive iteration {i}: expected a submission"
        assert mgr.wait_idle(5)


def test_circuit_opens_at_threshold_and_pauses_submissions():
    mgr, _, _ = _manager(
        flush_batch_max_rows=1, flush_circuit_failures=3, flush_interval_seconds=100.0, flush_circuit_max_seconds=1000.0
    )
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 3)
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 3
        assert mgr._circuit_open_until["d1"] > 0
    # Submissions paused even though data is buffered and a trigger is due.
    mgr.buffer("d1", _table(1), 99)
    assert mgr.maybe_flush() == 0
    assert metrics.delivery_circuit_open.labels(destination="d1")._value.get() == 1


def test_circuit_probe_success_closes_and_resubmits():
    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_circuit_failures=2, flush_interval_seconds=100.0)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 2)
    with mgr._lock:
        assert mgr._circuit_open_until["d1"] > 0
        mgr._circuit_open_until["d1"] = 0.0  # backoff elapsed -> probe eligible
    with patch("viaduck.delivery.append_only", return_value=1):
        mgr.buffer("d1", _table(1), 99)
        assert mgr.maybe_flush() == 1, "elapsed backoff must let the probe through"
        assert mgr.wait_idle(5)
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 0
        assert "d1" not in mgr._circuit_open_until
    assert metrics.delivery_circuit_open.labels(destination="d1")._value.get() == 0


def test_circuit_probe_failure_reopens_with_next_backoff_step():
    mgr, _, _ = _manager(
        flush_batch_max_rows=1, flush_circuit_failures=2, flush_interval_seconds=100.0, flush_circuit_max_seconds=1000.0
    )
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 2)
        # backoff=2s step; then a failing probe
        with mgr._lock:
            mgr._circuit_open_until["d1"] = 0.0
        mgr.buffer("d1", _table(1), 99)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    # Third consecutive failure: failures=3 -> backoff = 100 * 2^(3-2) = 200s.
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 3
        remaining = mgr._circuit_open_until["d1"] - _time_mod.monotonic()
    assert 190 < remaining <= 200, f"backoff must step to 2x interval, got {remaining:.1f}s"
    mgr.buffer("d1", _table(1), 100)
    assert mgr.maybe_flush() == 0


def test_circuit_backoff_capped_at_max_seconds():
    mgr, _, _ = _manager(
        flush_batch_max_rows=1, flush_circuit_failures=2, flush_interval_seconds=100.0, flush_circuit_max_seconds=150.0
    )
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 4)  # uncapped backoff would reach 100*2^2=400s
    with mgr._lock:
        remaining = mgr._circuit_open_until["d1"] - _time_mod.monotonic()
    assert 140 < remaining <= 150, f"backoff must cap at flush_circuit_max_seconds, got {remaining:.1f}s"


def test_success_before_threshold_resets_consecutive_failures():
    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_circuit_failures=3, flush_interval_seconds=100.0)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 2)
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 2
        assert "d1" not in mgr._circuit_open_until  # still closed below threshold
    with patch("viaduck.delivery.append_only", return_value=1):
        mgr.buffer("d1", _table(1), 99)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 0
    with patch("viaduck.delivery.append_only", return_value=1):
        mgr.buffer("d1", _table(1), 100)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)


def test_circuit_is_per_destination():
    mgr, _, _ = _manager(
        dests=("d1", "d2"), flush_batch_max_rows=1, flush_circuit_failures=2, flush_interval_seconds=100.0
    )
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 2)
    with patch("viaduck.delivery.append_only", return_value=1):
        mgr.buffer("d2", _table(1), 50)
        assert mgr.maybe_flush() == 1, "d2's flush must submit while d1's circuit is open"
        assert mgr.wait_idle(5)
    with mgr._lock:
        assert mgr._circuit_open_until.get("d1", 0) > 0
        assert "d2" not in mgr._circuit_open_until


def test_flush_passes_derived_deadline_to_apply():
    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_interval_seconds=100.0, flush_deadline_seconds=0.0)
    with patch("viaduck.delivery.append_only", return_value=1) as ao:
        mgr.buffer("d1", _table(1), 5)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    deadline = ao.call_args.kwargs["deadline"]
    assert deadline is not None
    # Derived default: 2x flush_interval_seconds from the flush's start time.
    assert abs(deadline - (_time_mod.monotonic() + 200.0)) < 60, f"derived deadline should be ~200s out, got {deadline}"


def test_flush_deadline_disabled_when_derived_nonpositive():
    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_interval_seconds=0.0, flush_deadline_seconds=0.0)
    with patch("viaduck.delivery.append_only", return_value=1) as ao:
        mgr.buffer("d1", _table(1), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle(5)
    assert ao.call_args.kwargs["deadline"] is None


def test_flush_deadline_exceeded_counts_metric_and_fails_flush():
    from viaduck.apply import FlushDeadlineExceeded

    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_interval_seconds=100.0)
    before = metrics.delivery_flush_deadlines_total.labels(destination="d1")._value.get()
    with patch("viaduck.delivery.append_only", side_effect=FlushDeadlineExceeded("too slow")):
        mgr.buffer("d1", _table(1), 5)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    after = metrics.delivery_flush_deadlines_total.labels(destination="d1")._value.get()
    assert after == before + 1
    # A deadline abort is an ordinary FlushFail for the position model.
    assert mgr.positions() == {"d1": 0}


def test_position_only_flush_does_not_close_circuit():
    """A position-only persist (tables empty) never touches the destination
    — it must not count as probe evidence and phantom-close the circuit on
    PG health alone."""
    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_interval_seconds=0.0, flush_circuit_failures=3)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 3)
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 3
        mgr._circuit_open_until["d1"] = 0.0  # backoff elapsed -> probe eligible
    # Position-only advance: submits a tables-empty flush which "succeeds"
    # (nothing to write, cursor persists). append_only must NOT be called.
    with patch("viaduck.delivery.append_only", side_effect=AssertionError("must not be called")):
        mgr.advance_position("d1", 50)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 3, "empty flush must not reset the failure count"
    assert metrics.delivery_circuit_open.labels(destination="d1")._value.get() == 1


def test_circuit_backoff_floored_at_one_second_when_interval_zero():
    """flush_interval_seconds=0 (unbuffered-repro mode): the raw formula
    collapses to 0s and the breaker would never suppress a submission.
    The 1s floor keeps the gate real."""
    mgr, _, _ = _manager(
        flush_batch_max_rows=1, flush_interval_seconds=0.0, flush_circuit_failures=3, flush_circuit_max_seconds=900.0
    )
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 3)
    with mgr._lock:
        remaining = mgr._circuit_open_until["d1"] - _time_mod.monotonic()
    assert 0.9 < remaining <= 1.0, f"backoff must floor at 1s, got {remaining:.2f}s"
    mgr.buffer("d1", _table(1), 99)
    assert mgr.maybe_flush() == 0, "gate must actually suppress submissions at interval=0"


def test_cursor_persist_failure_does_not_count_toward_circuit():
    """The destination write SUCCEEDED; the cursor-store PG is down. Shared
    infrastructure must not trip the destination's breaker (same stance as
    lifecycle: a PG blip must not punish destinations)."""
    mgr, sm, _ = _manager(flush_batch_max_rows=1, flush_interval_seconds=100.0, flush_circuit_failures=2)
    sm.advance_cursor.side_effect = RuntimeError("pg down")
    with (
        patch("viaduck.delivery.append_only", return_value=1),
        patch("viaduck.delivery.time.sleep"),  # skip the cursor-retry backoff
    ):
        for _ in range(3):
            mgr.buffer("d1", _table(1), 5)
            assert mgr.maybe_flush() == 1
            assert mgr.wait_idle(5)
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 0, "cursor-persist failures must not count toward the circuit"
        assert "d1" not in mgr._circuit_open_until


def test_drain_bypasses_circuit():
    """Shutdown must attempt every destination even with an open circuit:
    a recovered destination drains cleanly instead of burning the whole
    drain timeout and abandoning rows."""
    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_interval_seconds=100.0, flush_circuit_failures=2)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 2)
    with mgr._lock:
        assert mgr._circuit_open_until["d1"] > 0
    flushed = []
    with patch("viaduck.delivery.append_only", side_effect=lambda *a, **k: flushed.append(1) or 1):
        mgr.buffer("d1", _table(1), 50)
        t0 = _time_mod.monotonic()
        mgr.drain(timeout_s=5.0)
        elapsed = _time_mod.monotonic() - t0
    assert flushed == [1], "drain must bypass the circuit and attempt the flush"
    assert elapsed < 2.0, f"drain must not spin its timeout behind an open circuit ({elapsed:.1f}s)"


def test_readd_resets_circuit_state():
    """remove_destination -> add_destination: activation-scoped circuit
    state (a stopped, fixed, re-added destination gets a fresh probe, not
    its predecessor's open circuit)."""
    mgr, _, _ = _manager(flush_batch_max_rows=1, flush_interval_seconds=100.0, flush_circuit_failures=2)
    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("boom")):
        _fail_flushes(mgr, 2)
    with mgr._lock:
        assert mgr._circuit_open_until["d1"] > 0
        assert mgr._flush_failures["d1"] == 2
    mgr.remove_destination("d1")
    mgr.add_destination("d1")
    with mgr._lock:
        assert mgr._flush_failures["d1"] == 0
        assert "d1" not in mgr._circuit_open_until
    assert metrics.delivery_circuit_open.labels(destination="d1")._value.get() == 0


def test_remove_destination_series_covers_circuit_and_deadline_series():
    """A removed destination's circuit/deadline series must not freeze —
    phantom 'circuit open' gauges page for tenants that no longer exist."""
    metrics.delivery_circuit_open.labels(destination="dz").set(1)
    metrics.delivery_circuit_opens_total.labels(destination="dz").inc()
    metrics.delivery_flush_deadlines_total.labels(destination="dz").inc()
    metrics.remove_destination_series("dz")
    for raw in (
        metrics._delivery_circuit_open,
        metrics._delivery_circuit_opens_total,
        metrics._delivery_flush_deadlines_total,
    ):
        assert all("dz" not in key for key in raw._metrics), f"series for dz survived removal in {raw}"


# ---------------------------------------------------------------------------
# Per-destination buffer-cap overrides (DestinationConfig.buffer_max_bytes)
# ---------------------------------------------------------------------------


def test_cap_override_gates_reads_per_destination():
    # d1 carries an explicit override; d2 rides the global. Each pauses at
    # its OWN cap — the override must not leak to peers.
    tbl = _table(100)
    mgr, _, _ = _manager(
        dests=("d1", "d2"),
        buffer_max_bytes_per_destination=1000,  # below one table: d2 pauses
        buffer_total_max_bytes=10_000_000,
    )
    mgr._cap_overrides = {"d1": tbl.nbytes * 3}  # above one table: d1 flows
    mgr.buffer("d1", tbl, 5)
    mgr.buffer("d2", tbl, 5)
    assert not mgr.should_pause_reads_for("d1")
    assert mgr.should_pause_reads_for("d2")
    # …and d1 pauses at ITS cap once it accumulates past the override.
    mgr.buffer("d1", tbl, 6)
    mgr.buffer("d1", tbl, 7)
    assert mgr.should_pause_reads_for("d1")


def test_cap_override_constructor_wiring():
    from viaduck.config import DeliveryConfig
    from viaduck.delivery import DeliveryManager

    cfg = DeliveryConfig(workers=2, buffer_max_bytes_per_destination=1000, buffer_total_max_bytes=10_000_000)
    sm = _state_mgr({"d1": 0, "d2": 0})
    from unittest.mock import MagicMock

    mgr = DeliveryManager(
        cfg,
        sm,
        MagicMock(),
        [],
        ["d1", "d2"],
        mode="append_only",
        per_dest_cap_overrides={"d1": 7777, "ghost": 0},
    )
    with mgr._lock:
        assert mgr._cap_for_locked("d1") == 7777
        assert mgr._cap_for_locked("d2") == 1000
        # zero/absent overrides fall through to the default
        assert "ghost" not in mgr._cap_overrides


def test_membership_recompute_leaves_overrides_alone():
    # Auto mode: default cap re-derives on membership change; the override
    # is a fixed contract and must not move.
    mgr, _, _ = _manager(dests=("d1",), buffer_total_max_bytes=1000)
    mgr._cap_overrides = {"d1": 999_999}
    before = mgr._cap_overrides["d1"]
    mgr.add_destination("d9")  # auto default shrinks to total/2
    with mgr._lock:
        assert mgr._cap_for_locked("d1") == before
        assert mgr._cap_for_locked("d9") == mgr._per_dest_cap


def test_memory_trigger_respects_override():
    # The per-dest-cap "memory" trigger is the second consumer of the
    # override: a dest between the global default and its own override
    # must NOT force-flush; one at its override must.
    tbl = _table(100)
    mgr, _, _ = _manager(
        dests=("d1",),
        buffer_max_bytes_per_destination=1000,  # global default below one table
        buffer_total_max_bytes=10_000_000,
        flush_interval_seconds=3600.0,
        flush_batch_max_rows=10_000_000,
    )
    mgr._cap_overrides = {"d1": tbl.nbytes * 3}
    fake, calls = _recording_flush(mgr)
    mgr.buffer("d1", tbl, 5)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 0  # above global default, below override: no trigger
        mgr.buffer("d1", tbl, 6)
        mgr.buffer("d1", tbl, 7)
        assert mgr.maybe_flush() == 1  # at/over its override: memory trigger fires
    assert calls[0][3] == "memory"


def test_explicit_recompute_with_overrides_present():
    # Prod shape: explicit global + overrides. Membership growth re-derives
    # only the non-override default; the override never moves.
    mgr, _, _ = _manager(
        dests=("d1", "d2"),
        buffer_max_bytes_per_destination=1000,
        buffer_total_max_bytes=2500,
    )
    mgr._cap_overrides = {"d1": 50_000}
    mgr.add_destination("d3")  # 3 x 1000 > 2500 -> explicit shrinks to fair 833
    with mgr._lock:
        assert mgr._cap_for_locked("d1") == 50_000
        assert mgr._cap_for_locked("d2") == mgr._per_dest_cap
        assert mgr._per_dest_cap == 2500 // 3


def test_startup_warn_counts_assigned_overrides_only(caplog):
    import logging
    from unittest.mock import MagicMock

    from viaduck.config import DeliveryConfig
    from viaduck.delivery import DeliveryManager

    cfg = DeliveryConfig(workers=2, buffer_max_bytes_per_destination=1000, buffer_total_max_bytes=1500)
    sm = _state_mgr({"d1": 0, "d2": 0})
    with caplog.at_level(logging.WARNING, logger="viaduck.delivery"):
        DeliveryManager(
            cfg,
            sm,
            MagicMock(),
            [],
            ["d1", "d2"],
            mode="append_only",
            # d1 assigned+overridden; "elsewhere" overridden but NOT assigned
            # (another partition's destination) — must not be counted.
            per_dest_cap_overrides={"d1": 5000, "elsewhere": 9_999_999},
        )
    warn = next(
        r for r in caplog.records if "exceeding buffer_total_max_bytes" in r.message or "exceeding" in r.getMessage()
    )
    msg = warn.getMessage()
    assert "6000" in msg  # cap_sum = 5000 (d1 override) + 1000 (d2 default)
    assert "(1 override(s) + default 1000 x 1)" in msg


def test_flush_commit_drops_covered_replay_entries():
    """The pair-split phantom chain (TLA witness in tla/Viaduck.tla):
    full-swap flush in flight -> pause rewinds position -> replay re-buffers
    the same range -> zombie commits -> the covered replay entries must be
    dropped, or a later sliced flush can split a conflicting pair across a
    crash boundary into a permanent phantom."""
    import threading

    mgr, sm, pool = _manager(("d1",), flush_interval_seconds=0.0)
    apply_gate = threading.Event()

    def gated_append(_pool, _dest, batch, **_kw):
        apply_gate.wait(10)
        return batch.num_rows

    ins = pa.table({"company": ["acme"], "value": [1]})
    del_ = pa.table({"company": ["acme"], "value": [2]})
    later = pa.table({"company": ["acme"], "value": [3]})

    with patch("viaduck.delivery.append_only", side_effect=gated_append):
        # 1. Buffer the pair; flush takes the full swap (through=20).
        mgr.buffer("d1", ins, 10, epoch=0)
        mgr.buffer("d1", del_, 20, epoch=0)
        assert mgr.maybe_flush() == 1
        # In flight now; _flush is parked inside apply.
        import time

        for _ in range(100):
            if "d1" in mgr.inflight_ids():
                break
            time.sleep(0.01)

        # 2. Pause: position rewinds to cursor (0), epoch bumped.
        mgr.discard_buffer("d1")
        _, epoch = mgr.read_plan()["d1"]

        # 3. Replay re-buffers the pair + one entry PAST the zombie's range.
        mgr.buffer("d1", ins, 10, epoch=epoch)
        mgr.buffer("d1", del_, 20, epoch=epoch)
        mgr.buffer("d1", later, 30, epoch=epoch)
        assert mgr._buffers["d1"].rows == 3

        # 4. Zombie commits.
        apply_gate.set()
        assert mgr.wait_idle(10)

    # Covered replay entries (through <= 20) dropped; the post-range entry
    # survives. Positions/cursors are at the zombie's through.
    buf = mgr._buffers["d1"]
    assert [t.num_rows for t, _, _ in buf.entries] == [1]
    assert buf.entries[0][1] == 30
    assert buf.rows == 1
    assert mgr._flushed["d1"] == 20


def test_split_flush_after_retention_clamp_does_not_drop_tail():
    # Adversarial-review HIGH-1 regression pin: the retention clamp is the
    # one writer that raises `flushed` WITHOUT delivering. A head-slice
    # flush must not carry that value into the commit path — doing so ran
    # DropCoveredPrefix at the clamp floor and silently dropped the
    # undelivered tail (data loss). With through=None the tail survives,
    # drains, and the entry's cov persists on completion.
    mgr, _, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(7), through_snapshot=10, epoch=epoch)
    mgr.clamp_to_retention("d1", 50)  # buffered rows are kept per contract
    seen = []

    def _record(pool, d, b, **kw):
        seen.append(b.num_rows)
        return b.num_rows

    with patch("viaduck.delivery.append_only", side_effect=_record):
        for _ in range(8):
            mgr.maybe_flush(shutdown=True)
            assert mgr.wait_idle()
            if mgr.is_clean("d1"):
                break
    assert sum(seen) == 7  # every buffered row delivered, none dropped
    # Cursor: clamp floor wins (50 > entry cov 10); the entry completing
    # must not regress it, and nothing may persist beyond it early.
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 50
