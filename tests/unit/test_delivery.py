"""Unit tests for DeliveryManager: triggers, buffer swap, failure semantics,
per-destination serialization. Mirrors the actions in tla/Viaduck.tla —
BufferRead, FlushStart, FlushCommit, FlushFail."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.config import DeliveryConfig
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


def _manager(dests=("d1",), cursors=None, **cfg_overrides):
    defaults = dict(workers=2, flush_interval_seconds=3600.0)
    defaults.update(cfg_overrides)
    cfg = DeliveryConfig(**defaults)
    sm = _state_mgr(cursors or {d: 0 for d in dests})
    pool = MagicMock()
    mgr = DeliveryManager(cfg, sm, pool, [], list(dests))
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
    mgr, _, _ = _manager(flush_interval_seconds=0.0)
    mgr._full_cdc = True
    mgr._key_columns = ["value"]
    captured = {}

    def fake_apply(pool, dest, batch, key_columns, append_at_least_once=False):
        captured["rows"] = batch.num_rows
        captured["aalo"] = append_at_least_once
        return batch.num_rows

    with patch("viaduck.delivery.apply_full_cdc", side_effect=fake_apply):
        mgr.buffer("d1", _table(2), 5)
        mgr.buffer("d1", _table(3), 6)
        mgr.maybe_flush()
        assert mgr.wait_idle()
    assert captured["rows"] == 5


def test_full_cdc_flush_threads_append_at_least_once_per_destination():
    """Per-destination flags survive the buffer → worker → apply path,
    keyed by dest_id. Two destinations with different flag values must
    each see their own value at apply time."""
    cfg = DeliveryConfig(workers=2, flush_interval_seconds=0.0)
    sm = _state_mgr({"d-fast": 0, "d-slow": 0})
    pool = MagicMock()
    mgr = DeliveryManager(
        cfg,
        sm,
        pool,
        ["value"],
        ["d-fast", "d-slow"],
        append_at_least_once_by_dest={"d-fast": True, "d-slow": False},
    )

    seen: dict[str, bool] = {}

    def fake_apply(pool, dest, batch, key_columns, append_at_least_once=False):
        seen[dest] = append_at_least_once
        return batch.num_rows

    with patch("viaduck.delivery.apply_full_cdc", side_effect=fake_apply):
        mgr.buffer("d-fast", _table(2), 5)
        mgr.buffer("d-slow", _table(2), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()

    assert seen == {"d-fast": True, "d-slow": False}


def test_full_cdc_flush_defaults_append_at_least_once_to_false():
    """No append_at_least_once_by_dest arg → every destination behaves as
    before. Defends against accidentally flipping default behaviour for
    every existing deployment."""
    cfg = DeliveryConfig(workers=1, flush_interval_seconds=0.0)
    sm = _state_mgr({"d1": 0})
    pool = MagicMock()
    mgr = DeliveryManager(cfg, sm, pool, ["value"], ["d1"])  # no kwarg

    seen: dict[str, bool] = {}

    def fake_apply(pool, dest, batch, key_columns, append_at_least_once=False):
        seen[dest] = append_at_least_once
        return batch.num_rows

    with patch("viaduck.delivery.apply_full_cdc", side_effect=fake_apply):
        mgr.buffer("d1", _table(2), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()

    assert seen == {"d1": False}


def test_apply_mode_gauge_set_for_each_destination_at_init():
    """The dest_apply_mode gauge is the operator-visible answer to "is this
    dest on the fast path?". Verify both states (on/off) are reflected per
    destination at constructor time, before any flush runs.

    Mock keys on .labels() returning a single shared child mock, so the .set()
    calls land on it regardless of which destination label was passed in. We
    then read the two .set() args and verify both 0 and 1 appeared (some
    destination got each)."""
    cfg = DeliveryConfig(workers=1, flush_interval_seconds=3600.0)
    sm = _state_mgr({"d-fast": 0, "d-slow": 0})
    pool = MagicMock()

    with patch("viaduck.delivery.metrics.dest_apply_mode") as gauge:
        DeliveryManager(
            cfg,
            sm,
            pool,
            ["value"],
            ["d-fast", "d-slow"],
            append_at_least_once_by_dest={"d-fast": True, "d-slow": False},
        )

    gauge.labels.assert_any_call(destination="d-fast")
    gauge.labels.assert_any_call(destination="d-slow")
    assert gauge.labels.call_count == 2

    set_values = [c.args[0] for c in gauge.labels.return_value.set.call_args_list]
    assert sorted(set_values) == [0, 1]


def test_init_logs_for_fast_path_destinations(caplog):
    """A startup log line per fast-path destination so the operator-visible
    signal is plain in the deploy log, not just in /metrics."""
    cfg = DeliveryConfig(workers=1, flush_interval_seconds=3600.0)
    sm = _state_mgr({"d-fast": 0, "d-slow": 0})
    pool = MagicMock()

    with caplog.at_level("INFO", logger="viaduck.delivery"):
        DeliveryManager(
            cfg,
            sm,
            pool,
            ["value"],
            ["d-fast", "d-slow"],
            append_at_least_once_by_dest={"d-fast": True, "d-slow": False},
        )

    fast_path_log_lines = [r.message for r in caplog.records if "append_at_least_once" in r.message]
    assert any("d-fast" in m for m in fast_path_log_lines)
    assert not any("d-slow" in m for m in fast_path_log_lines)


def test_full_cdc_flush_unknown_destination_defaults_to_false():
    """A destination present in assigned_ids but absent from the per-dest
    flag dict must default to False — never promote to fast path on a
    missing key. Catches future plumbing bugs where a new destination
    is added but the dict isn't kept in sync."""
    cfg = DeliveryConfig(workers=1, flush_interval_seconds=0.0)
    sm = _state_mgr({"d1": 0})
    pool = MagicMock()
    mgr = DeliveryManager(
        cfg,
        sm,
        pool,
        ["value"],
        ["d1"],
        append_at_least_once_by_dest={"some-other-dest": True},
    )

    seen: dict[str, bool] = {}

    def fake_apply(pool, dest, batch, key_columns, append_at_least_once=False):
        seen[dest] = append_at_least_once
        return batch.num_rows

    with patch("viaduck.delivery.apply_full_cdc", side_effect=fake_apply):
        mgr.buffer("d1", _table(2), 5)
        mgr.maybe_flush()
        assert mgr.wait_idle()

    assert seen == {"d1": False}


# ---------------------------------------------------------------------------
# Watermark / pause
# ---------------------------------------------------------------------------


def test_should_pause_reads_only_when_saturated():
    mgr, _, _ = _manager(buffer_total_max_bytes=1)
    assert not mgr.should_pause_reads()  # under watermark
    mgr.buffer("d1", _table(10), 5)
    # Over watermark but flushable -> no pause (maybe_flush will relieve).
    assert not mgr.should_pause_reads()
    with mgr._lock:
        mgr._inflight.add("d1")
    # Over watermark AND everything in flight -> pause.
    assert mgr.should_pause_reads()
    with mgr._lock:
        mgr._inflight.discard("d1")


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

    def slow_apply(pool, dest, batch):
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
    """QE M2: swapped-out (in-flight) tables count toward the watermark."""
    mgr, _, _ = _manager(buffer_total_max_bytes=1, flush_interval_seconds=3600.0)
    release = threading.Event()

    def slow_apply(pool, dest, batch):
        release.wait(5)
        return batch.num_rows

    with patch("viaduck.delivery.append_only", side_effect=slow_apply):
        mgr.buffer("d1", _table(50), 5)
        assert mgr.maybe_flush(shutdown=True) == 1  # swap to in-flight
        # Live buffer empty, but in-flight bytes keep us over the watermark
        # and d1 is in flight -> reads must pause.
        assert mgr.should_pause_reads()
        release.set()
        assert mgr.wait_idle()
    assert not mgr.should_pause_reads()


def test_on_flush_success_fires_for_data_not_for_idle_persists():
    hits = []
    cfg = DeliveryConfig(workers=1, flush_interval_seconds=0.0)
    sm = _state_mgr({"d1": 0})
    mgr = DeliveryManager(cfg, sm, MagicMock(), [], ["d1"], on_flush_success=lambda: hits.append(1))

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
