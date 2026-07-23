"""Unit tests for DeliveryManager: triggers, buffer swap, failure semantics,
per-destination serialization. Mirrors the actions in tla/Viaduck.tla —
BufferRead, FlushStart, FlushCommit, FlushFail."""

from __future__ import annotations

import threading
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


def test_rows_trigger():
    mgr, sm, _ = _manager(flush_max_rows=4)
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", side_effect=fake):
        mgr.buffer("d1", _table(5), 5)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    dest, _tables, through, trigger = calls[0]
    assert dest == "d1" and through == 5 and trigger == "rows"


def test_bytes_trigger():
    mgr, _, _ = _manager(flush_max_bytes=8)  # any table exceeds 8 bytes
    fake, calls = _recording_flush(mgr)
    with patch.object(mgr, "_flush", side_effect=fake):
        mgr.buffer("d1", _table(1), 5)
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(5)
    assert calls[0][3] == "bytes"


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

    def fake_apply(pool, dest, batch, key_columns, stop_event=None):
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
    flush on the cap trigger rather than waiting for flush_max_bytes /
    interval — otherwise it wedges: full queue, paused intake, no flush."""
    mgr, _, pool = _manager(
        dests=("d1",),
        buffer_max_bytes_per_destination=1,
        flush_max_bytes=10**12,  # global byte trigger unreachable
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

    def slow_apply(pool, dest, batch, stop_event=None):
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

    def slow_apply(pool, dest, batch, stop_event=None):
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
