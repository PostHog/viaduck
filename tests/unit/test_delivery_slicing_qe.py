"""QE tests for within-entry flush-batch slicing (2026-08 patch).

The patch cuts WITHIN the first buffered entry when it alone exceeds the
rows cap or the adaptive byte target: the head slice flushes with
``through = flushed`` (no cursor advance), the tail returns to the buffer
head with the SAME (cov, hi), and the cursor advances only on the flush
that completes the entry. These tests try to break that contract through
the public API: the durable cursor must never pass undelivered rows
(at-least-once; duplicate window bounded by one entry).
"""

from __future__ import annotations

import logging
import random
import threading
from unittest.mock import patch

import pyarrow as pa
import pytest

from tests.unit.test_delivery import _manager, _recording_flush, _table
from viaduck import metrics


def setup_module():
    metrics.init("test")


def _drain_to_clean(mgr, dest="d1", max_iters=64):
    """Drive maybe_flush(shutdown=True) until the destination is clean.
    Bounded: a stall is a test failure, not a hang."""
    for _ in range(max_iters):
        mgr.maybe_flush(shutdown=True)
        assert mgr.wait_idle(10)
        if mgr.is_clean(dest):
            return
    pytest.fail(f"drain did not converge within {max_iters} iterations")


def _recording_apply(delivered: list):
    def _ok(pool, dest, batch, **kw):
        delivered.append(batch.num_rows)
        return batch.num_rows

    return _ok


def _persists(sm):
    """Sequence of `through` values handed to the durable cursor store."""
    return [c.args[1] for c in sm.advance_cursor.call_args_list]


# ---------------------------------------------------------------------------
# S1: failure injected mid-split-drain
# ---------------------------------------------------------------------------


def test_failure_mid_split_drain_rewinds_and_converges():
    """Head slice committed (through=flushed, no advance), the next slice's
    apply raises: buffer clears, position rewinds to the durable cursor,
    epoch bumps, and a re-read + full drain converges with the cursor at
    the entry's cov exactly once. Duplicate window == the committed head."""
    mgr, sm, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    delivered = []
    _, epoch = mgr.read_plan()["d1"]

    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        mgr.buffer("d1", _table(7), through_snapshot=10, epoch=epoch)
        assert mgr.maybe_flush() == 1  # head slice
        assert mgr.wait_idle(10)
    assert delivered == [2]
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 0  # no advance on the partial

    with patch("viaduck.delivery.append_only", side_effect=RuntimeError("dest down")):
        assert mgr.maybe_flush() == 1  # next slice fails
        assert mgr.wait_idle(10)

    snap = mgr.status_snapshot()["d1"]
    assert snap.buffer_rows == 0, "FlushFail must drop the split tail with the in-flight slice"
    assert mgr.positions() == {"d1": 0}, "position must rewind to the durable cursor"
    assert snap.flushed_snapshot == 0
    new_pos, new_epoch = mgr.read_plan()["d1"]
    assert (new_pos, new_epoch) == (0, epoch + 1), "failure reset must bump the epoch"

    # A read that overlapped the reset is discarded by the stale epoch.
    mgr.buffer("d1", _table(7), through_snapshot=10, epoch=epoch)
    assert mgr.status_snapshot()["d1"].buffer_rows == 0

    # Re-read with the fresh epoch; full drain converges.
    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        mgr.buffer("d1", _table(7), through_snapshot=10, epoch=new_epoch)
        _drain_to_clean(mgr)

    assert mgr.status_snapshot()["d1"].flushed_snapshot == 10
    assert delivered == [2, 2, 2, 2, 1]  # 9 total: 7 + one duplicated head slice (<= one entry)
    persists = _persists(sm)
    assert persists.count(10) == 1, "the entry's cov must persist exactly once"
    assert all(p in (0, 10) for p in persists)


# ---------------------------------------------------------------------------
# S2: new entries buffered behind a split tail (poll thread racing flush)
# ---------------------------------------------------------------------------


def test_new_entry_lands_while_split_head_in_flight():
    """Poll thread buffers a new entry WHILE the split head slice is in
    flight: ordering is preserved, the later entry's cov is never persisted
    before the split entry completes, and DropCoveredPrefix never drops the
    tail or the new entry."""
    mgr, sm, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    gate = threading.Event()
    entered = threading.Event()
    batches: list[list[int]] = []

    def _gated(pool, dest, batch, **kw):
        batches.append(batch.column("value").to_pylist())
        if len(batches) == 1:
            entered.set()
            gate.wait(10)
        return batch.num_rows

    tbl_a = pa.table({"company": ["a"] * 5, "value": [1, 2, 3, 4, 5]})
    tbl_b = pa.table({"company": ["a"] * 2, "value": [6, 7]})
    with patch("viaduck.delivery.append_only", side_effect=_gated):
        mgr.buffer("d1", tbl_a, through_snapshot=10, epoch=epoch)
        assert mgr.maybe_flush() == 1
        assert entered.wait(5)
        mgr.buffer("d1", tbl_b, through_snapshot=20, epoch=epoch)  # racing read
        assert mgr.maybe_flush() == 0  # in-flight guard holds
        gate.set()
        assert mgr.wait_idle(10)
        _drain_to_clean(mgr)

    assert [v for b in batches for v in b] == [1, 2, 3, 4, 5, 6, 7], (
        "split tail + later entry must drain in order, none dropped"
    )
    # Cursor persist order: nothing until the split entry completes (10),
    # then the later entry's cov (20) — 20 never before 10, 10 never early.
    assert _persists(sm) == [10, 20]  # partial slices persist nothing (through=None)
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 20


# ---------------------------------------------------------------------------
# S3: straddle entries (hi > cov)
# ---------------------------------------------------------------------------


def test_straddle_entry_split_preserves_hi_and_persists_cov_not_hi():
    """Splitting a straddle entry (hi > cov) keeps the SAME (cov, hi) on the
    tail; the completing flush persists cov (never hi); the commit-time
    covered-prefix drop keys on hi and leaves the tail and later entries
    alone."""
    mgr, sm, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    delivered = []

    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        mgr.buffer("d1", _table(5), through_snapshot=10, epoch=epoch, hi=30)
        assert mgr.maybe_flush() == 1  # head slice, through=0
        assert mgr.wait_idle(10)
        with mgr._lock:
            entries = [(cov, hi, t.num_rows) for t, cov, hi in mgr._buffers["d1"].entries]
        assert entries == [(10, 30, 3)], "tail must keep the entry's exact (cov, hi)"
        mgr.buffer("d1", _table(2), through_snapshot=40, epoch=epoch)  # entry behind the straddle tail
        _drain_to_clean(mgr)

    assert sum(delivered) == 7, "no slice or entry may be dropped by the hi-keyed commit drop"
    persists = _persists(sm)
    assert 30 not in persists, "hi is a drop key, never a cursor value"
    assert persists == [10, 40]  # partial slices persist nothing (through=None)
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 40


def test_clamp_over_buffered_oversize_entry_still_delivers_buffered_rows():
    # QE-FINDING: retention clamp + within-entry split silently drops the
    # undelivered tail. clamp_to_retention keeps buffered rows and its
    # docstring promises "anything already read is valid data, and its
    # flush advances the cursor from the floor onward". With the patch, an
    # OVERSIZE buffered entry whose cov sits below the clamp floor is
    # split; the head slice flushes with through = flushed (= the clamp
    # floor, e.g. 99). On that commit, DropCoveredPrefix (keyed on
    # entry hi <= through) drops the still-undelivered tail as a "covered
    # replay" — head delivered, tail silently discarded, cursor never
    # re-reads it. Pre-patch the whole entry flushed and all 5 rows were
    # delivered. Reachable only via clamp (the one path that raises
    # `flushed` above a buffered entry's hi without a commit-drop pass).
    mgr, sm, _ = _manager(cursors={"d1": 10}, flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    delivered = []

    mgr.buffer("d1", _table(5), through_snapshot=50, epoch=epoch)  # read landed below the incoming floor
    assert mgr.clamp_to_retention("d1", 99) == 10
    snap = mgr.status_snapshot()["d1"]
    assert snap.flushed_snapshot == 99
    assert snap.buffer_rows == 5  # clamp keeps buffered rows: "valid data"

    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        _drain_to_clean(mgr)

    assert sum(delivered) == 5, (
        "clamp kept 5 buffered rows as deliverable, but the split head's "
        f"through=99 commit dropped the tail via DropCoveredPrefix (delivered={delivered})"
    )


# ---------------------------------------------------------------------------
# S4: position-only advance landing while a split tail is buffered
# ---------------------------------------------------------------------------


def test_advance_position_during_split_drain_no_premature_persist():
    """advance_position (empty range past the entry) while the split tail is
    buffered: the advanced position must not persist before the entry
    completes, and must not starve after — the completing full swap takes
    through = position."""
    mgr, sm, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    delivered = []

    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        mgr.buffer("d1", _table(5), through_snapshot=10, epoch=epoch)
        assert mgr.maybe_flush() == 1  # split head
        assert mgr.wait_idle(10)
        mgr.advance_position("d1", 15, epoch=epoch)  # empty range (10, 15]
        assert mgr.maybe_flush() == 1  # another partial slice
        assert mgr.wait_idle(10)
        assert mgr.status_snapshot()["d1"].flushed_snapshot == 0, (
            "neither cov nor the advanced position may persist while the tail is buffered"
        )
        _drain_to_clean(mgr)

    assert delivered == [2, 2, 1]
    persists = _persists(sm)
    assert all(p == 0 for p in persists[:-1])
    assert persists[-1] == 15, "completing full swap must persist through=position (no starvation)"
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 15


# ---------------------------------------------------------------------------
# S5: degenerate geometry
# ---------------------------------------------------------------------------


def test_one_row_oversize_entry_flushes_whole():
    """A 1-row entry over the byte target cannot split (num_rows > 1 guard):
    it flushes whole as a full swap and the cursor advances to its cov."""
    mgr, _, _ = _manager(flush_batch_max_rows=0, flush_interval_seconds=0.0)
    with mgr._lock:
        mgr._flush_target["d1"] = 1  # far below one row
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(1), through_snapshot=5, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
    assert sum(t.num_rows for t in calls[0][1]) == 1
    assert calls[0][2] == 5  # full swap: cursor advances
    assert mgr.status_snapshot()["d1"].buffer_rows == 0


def test_zero_nbytes_entry_splits_by_rows():
    """nbytes == 0 (null-typed column): the rows cap still splits, and the
    byte-proportional formula's division guard never runs (over_bytes is
    False at 0 bytes)."""
    mgr, sm, _ = _manager(flush_batch_max_rows=2, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    delivered = []
    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        mgr.buffer("d1", pa.table({"company": pa.nulls(5)}), through_snapshot=10, epoch=epoch)
        _drain_to_clean(mgr)
    assert delivered == [2, 2, 1]
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 10


def test_zero_nbytes_entry_byte_cap_inert_no_division_error():
    """nbytes == 0 with ONLY the byte target set: 0 > byte_cap is False, so
    no split, no ZeroDivisionError — the entry goes whole with its cov."""
    mgr, _, _ = _manager(flush_batch_max_rows=0, flush_interval_seconds=0.0)
    with mgr._lock:
        mgr._flush_target["d1"] = 1
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", pa.table({"company": pa.nulls(5)}), through_snapshot=10, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
    assert sum(t.num_rows for t in calls[0][1]) == 5
    assert calls[0][2] == 10


def test_byte_cap_below_one_row_drains_one_row_slices():
    """byte target smaller than a single row's bytes: the 1-row floor makes
    every slice one row; the drain converges and the cursor lands on cov
    exactly once, at the end."""
    # flush_adaptive_low_seconds=0: the AIMD controller cannot grow the
    # target mid-drain (a fast tiny-target slice trivially satisfies the
    # fill gate and would jump the target by step_bytes — by design).
    mgr, sm, _ = _manager(flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive_low_seconds=0.0)
    with mgr._lock:
        mgr._flush_target["d1"] = 1
    _, epoch = mgr.read_plan()["d1"]
    delivered = []
    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        mgr.buffer("d1", _table(6), through_snapshot=10, epoch=epoch)
        _drain_to_clean(mgr)
    assert delivered == [1] * 6
    persists = _persists(sm)
    assert persists == [10]  # partial slices persist nothing (through=None)


def test_adaptive_off_rows_cap_still_splits_within_entry():
    """flush_adaptive=false leaves byte_cap=0; the rows cap alone must still
    cut within the oversize entry."""
    mgr, sm, _ = _manager(flush_batch_max_rows=3, flush_adaptive=False, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    delivered = []
    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        mgr.buffer("d1", _table(7), through_snapshot=10, epoch=epoch)
        _drain_to_clean(mgr)
    assert delivered == [3, 3, 1]
    assert _persists(sm) == [10]  # partial slices persist nothing (through=None)


def test_target_mutation_between_slices_converges():
    """The adaptive target moving mid-drain (shrink hard, then grow to the
    ceiling — AIMD does both) must not break convergence or the cursor
    rule: nothing persists until the entry completes."""
    # Growth disabled (low_seconds=0) so only OUR explicit mutations move
    # the target between slices.
    mgr, sm, _ = _manager(flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive_low_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    tbl = _table(12)
    per_row = tbl.nbytes // 12
    delivered = []
    with patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)):
        with mgr._lock:
            mgr._flush_target["d1"] = per_row * 4
        mgr.buffer("d1", tbl, through_snapshot=10, epoch=epoch)
        assert mgr.maybe_flush() == 1  # ~4-row head at the initial target
        assert mgr.wait_idle(10)
        with mgr._lock:
            mgr._flush_target["d1"] = 1  # shrink to the 1-row floor
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(10)
        with mgr._lock:
            mgr._flush_target["d1"] = 10**9  # grow: rest of the entry in one swap
        _drain_to_clean(mgr)
    assert delivered[0] == 4
    assert delivered[1] == 1
    assert sum(delivered) == 12
    persists = _persists(sm)
    assert all(p == 0 for p in persists[:-1])
    assert persists[-1] == 10
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 10


def test_rows_cap_and_byte_cap_together_take_the_tighter_cut():
    """Both caps set: the head slice honors min(rows cap, byte-proportional
    rows)."""
    mgr, _, _ = _manager(flush_batch_max_rows=5, flush_interval_seconds=0.0)
    _, epoch = mgr.read_plan()["d1"]
    tbl = _table(20)
    per_row = tbl.nbytes // 20
    with mgr._lock:
        mgr._flush_target["d1"] = per_row * 2  # byte cut (2 rows) tighter than rows cap (5)
    fake, calls = _recording_flush(mgr)
    mgr.buffer("d1", tbl, through_snapshot=10, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
    assert sum(t.num_rows for t in calls[0][1]) == 2
    assert calls[0][2] is None  # partial entry: persist nothing


# ---------------------------------------------------------------------------
# S6: shutdown drain with a split tail buffered
# ---------------------------------------------------------------------------


def test_drain_completes_split_entry_and_lands_on_cov(caplog):
    """drain() must walk the split pipeline to completion well inside the
    timeout: all slices delivered, cursor at the entry's cov, nothing
    abandoned (no deadline warning)."""
    mgr, sm, _ = _manager(flush_batch_max_rows=2)  # interval 3600: only shutdown drives it
    _, epoch = mgr.read_plan()["d1"]
    delivered = []
    with (
        caplog.at_level(logging.WARNING, logger="viaduck.delivery"),
        patch("viaduck.delivery.append_only", side_effect=_recording_apply(delivered)),
    ):
        mgr.buffer("d1", _table(7), through_snapshot=10, epoch=epoch)
        mgr.drain(timeout_s=15)
    assert delivered == [2, 2, 2, 1]
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 10
    assert mgr.is_clean("d1")
    assert not any("Drain deadline reached" in r.getMessage() for r in caplog.records), (
        "drain must not abandon a split tail below the timeout"
    )


# ---------------------------------------------------------------------------
# S7: trigger liveness for the split tail
# ---------------------------------------------------------------------------


def test_sliced_trigger_keeps_split_tail_draining_without_interval():
    """flush_interval huge, tail below every ordinary threshold: the
    sliced_remainder fast-path must keep the tail draining back-to-back —
    no stall until the entry completes."""
    mgr, _, _ = _manager(flush_batch_max_rows=2, flush_max_rows=5, flush_interval_seconds=3600.0)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(5), through_snapshot=10, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1  # rows trigger admits the oversize entry
        assert mgr.wait_idle(10)
        assert mgr.maybe_flush() == 1  # 3-row tail: below rows/bytes, must not wait 3600s
        assert mgr.wait_idle(10)
        assert mgr.maybe_flush() == 1  # 1-row tail: same
        assert mgr.wait_idle(10)
        assert mgr.maybe_flush() == 0  # drained; nothing left to submit
    assert [c[3] for c in calls] == ["rows", "sliced", "sliced"]
    assert [sum(t.num_rows for t in c[1]) for c in calls] == [2, 2, 1]
    assert [c[2] for c in calls] == [None, None, 10]


# ---------------------------------------------------------------------------
# S8: memory/watermark trigger with an oversize entry
# ---------------------------------------------------------------------------


def test_watermark_forced_flush_respects_within_entry_cut():
    """Over the global watermark, the forced 'memory' flush still takes only
    the head slice — memory relief comes from repeated bounded slices, not
    one unbounded swap."""
    mgr, _, _ = _manager(flush_batch_max_rows=2, buffer_total_max_bytes=1)
    fake, calls = _recording_flush(mgr)
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _table(10), through_snapshot=5, epoch=epoch)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 1
    assert calls[0][3] == "memory"
    assert sum(t.num_rows for t in calls[0][1]) == 2, "watermark pressure must not bypass the cut"
    assert calls[0][2] is None  # partial entry: persist nothing
    assert mgr.status_snapshot()["d1"].buffer_rows == 8


# ---------------------------------------------------------------------------
# S9: property-style randomized drain
# ---------------------------------------------------------------------------


def test_property_random_entries_and_caps_drain_exactly_once():
    """Random entry sizes and caps: every flush respects the caps (within
    the documented slice tolerance), every buffered row is delivered
    exactly once, the final cursor is the max cov, and no cursor persist
    ever covers rows not yet delivered."""
    rng = random.Random(20260825)
    one_row_bytes = _table(1).nbytes
    per_row = _table(10).nbytes // 10

    for iteration in range(25):
        rows_cap = rng.choice([0, 1, 2, 3, 5])
        byte_rows = rng.choice([0, 1, 2, 4, 8])  # byte target in ~rows; 0 = inert
        sizes = [rng.randint(1, 12) for _ in range(rng.randint(1, 4))]
        entries = [(s, (i + 1) * 10) for i, s in enumerate(sizes)]
        ctx = f"iter={iteration} rows_cap={rows_cap} byte_rows={byte_rows} sizes={sizes}"

        # Growth disabled (low_seconds=0): the byte target under test must
        # stay where the iteration pinned it, not AIMD-grow after the
        # first fast slice.
        mgr, sm, _ = _manager(flush_batch_max_rows=rows_cap, flush_interval_seconds=0.0, flush_adaptive_low_seconds=0.0)
        byte_cap = 0
        if byte_rows:
            byte_cap = max(1, per_row * byte_rows)
            with mgr._lock:
                mgr._flush_target["d1"] = byte_cap
        _, epoch = mgr.read_plan()["d1"]

        delivered = {"rows": 0}
        violations: list[str] = []

        def _apply(pool, dest, batch, **kw):
            if rows_cap > 0 and batch.num_rows > max(rows_cap, 1):
                violations.append(f"{ctx}: batch rows {batch.num_rows} > rows cap {rows_cap}")
            # Byte tolerance: rows-proportional cut rounding + the 1-row floor.
            if byte_cap and batch.nbytes > byte_cap + 3 * one_row_bytes:
                violations.append(f"{ctx}: batch bytes {batch.nbytes} > byte cap {byte_cap} (+tolerance)")
            delivered["rows"] += batch.num_rows
            return batch.num_rows

        def _cursor(dest, through, cumulative_rows=None):
            covered = sum(s for s, cov in entries if cov <= through)
            if covered > delivered["rows"]:
                violations.append(f"{ctx}: cursor {through} covers {covered} rows, only {delivered['rows']} delivered")
            return 1

        sm.advance_cursor.side_effect = _cursor
        with patch("viaduck.delivery.append_only", side_effect=_apply):
            for size, cov in entries:
                mgr.buffer("d1", _table(size), through_snapshot=cov, epoch=epoch)
            _drain_to_clean(mgr, max_iters=200)

        assert not violations, "\n".join(violations)
        assert delivered["rows"] == sum(sizes), f"{ctx}: delivered {delivered['rows']} != buffered {sum(sizes)}"
        assert mgr.status_snapshot()["d1"].flushed_snapshot == entries[-1][1], ctx
