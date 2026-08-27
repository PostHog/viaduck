"""QE breaker reproductions for the honest-bytes patch (jakob/honest-bytes).

Everything drives DeliveryManager through the public surface the poll
thread uses (buffer / maybe_flush / read_plan / drain-style loops), the
way tests/unit/test_delivery.py's _manager fixture does. pyarrow 23.0.1.

Findings encoded here (see each test's comment for the numbers):

  QE-BUG-1  (FIXED — plain assert)  bytes trigger now compares in
            honest units via the cached per-destination estimate.
  QE-BUG-2  (FIXED — plain assert)  fixed_size_list/list_view branches
            added to _honest_type.
  QE-BUG-3  (FIXED — plain assert)  per-row sampling falls back past
            unpriceable head entries (bounded scan).
  QE-RISK-4 (passing, quantified)  head entry pricing Nx under the tail
            entries → ~Nx oversize flush AND the AIMD grows on the
            mispriced fill evidence.
  QE-RISK-5 (passing, quantified)  within-entry width gradient: first
            slice overshoots by the width ratio, then self-corrects on
            resample.
  QE-PIN-*  behavior pins worth keeping (convergence, clamp interaction,
            multi-chunk pricing, degrade contract, isolation, dedup).
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pyarrow as pa
import pytest

from tests.unit.test_delivery import _manager
from viaduck import metrics
from viaduck.delivery import _estimate_row_bytes, _honest_type


def setup_module():
    metrics.init("test")


# ---------------------------------------------------------------------------
# Builders / helpers
# ---------------------------------------------------------------------------

_PARENT_CACHE: dict = {}


def _dict_parent(rows: int = 100_000) -> pa.Table:
    """Parent table with a dictionary column carrying a 4.1MB shared
    dictionary; any slice reports ~full-dictionary nbytes (the production
    inflation shape, ~20x for a 400-row slice, ~150x smaller ones)."""
    if rows not in _PARENT_CACHE:
        dictionary = pa.array(["v" * 200 + str(i) for i in range(20_000)])
        indices = pa.array([i % 10 for i in range(rows)], type=pa.int32())
        _PARENT_CACHE[rows] = pa.table(
            {
                "a": pa.array(range(rows), type=pa.int64()),
                "s": pa.DictionaryArray.from_arrays(indices, dictionary),
            }
        )
    return _PARENT_CACHE[rows]


def _dict_entry(offset: int, n: int, parent_rows: int = 100_000) -> pa.Table:
    return _dict_parent(parent_rows).slice(offset, n)


def _poison_entry(n: int) -> pa.Table:
    """dict<struct>: pyarrow cannot cast it, so _estimate_row_bytes takes
    its exception path and returns 0 (unpriceable)."""
    struct_vals = pa.StructArray.from_arrays([pa.array(["x" * 100] * 50)], names=["f"])
    indices = pa.array([i % 50 for i in range(n)], type=pa.int32())
    return pa.table({"d": pa.DictionaryArray.from_arrays(indices, struct_vals)})


def _honest_nbytes(tbl: pa.Table) -> int:
    """Reference honest size: full dictionary-free decode of the WHOLE
    table (what the estimator approximates from its 1,024-row sample)."""
    target = pa.schema([pa.field(f.name, _honest_type(f.type)) for f in tbl.schema])
    if target != tbl.schema:
        tbl = tbl.cast(target)
    return tbl.combine_chunks().nbytes


def _capture_flush(mgr):
    """_flush stand-in recording (dest, tables, through, trigger, est_bytes)
    and honoring the in-flight guard contract."""
    calls = []

    def _fake(dest, tables, through, trigger, est_bytes=None):
        calls.append((dest, tables, through, trigger, est_bytes))
        with mgr._lock:
            mgr._inflight.discard(dest)

    return _fake, calls


def _drain_with_fake(mgr, dest="d1", max_cycles=400):
    """Repeated maybe_flush with a recording _flush until the buffer
    empties; fails the test if the split chain does not terminate."""
    fake, calls = _capture_flush(mgr)
    with patch.object(mgr, "_flush", fake):
        for _ in range(max_cycles):
            mgr.maybe_flush()
            assert mgr.wait_idle(10)
            with mgr._lock:
                if mgr._buffers[dest].rows == 0:
                    break
        else:
            pytest.fail(f"split chain did not terminate within {max_cycles} cycles")
    return calls


# ---------------------------------------------------------------------------
# QE-BUG-1: trigger in raw units vs growth gate in honest units
# ---------------------------------------------------------------------------


def test_bytes_trigger_and_growth_gate_agree_on_units():
    mgr, _, _ = _manager(
        flush_batch_max_rows=0,
        flush_interval_seconds=3600.0,  # only the bytes trigger can fire
        flush_adaptive=True,
        flush_max_bytes=64 * 2**20,  # ceiling well above the learned-down target
    )
    target = 2_000_000  # ~23 honest cycles at ~86KB/cycle
    with mgr._lock:
        mgr._flush_target["d1"] = target  # simulate a halving under contention
    honest_batches = []

    def _append(_pool, _dest, batch, **_kw):
        honest_batches.append(_honest_nbytes(batch))
        return batch.num_rows

    # Poll-loop emulation: one ~4.17MB-raw / ~86KB-honest entry per cycle
    # (inflation ~48x). The bytes trigger fires every 2 entries — at
    # ~172KB honest content against an 8MB target.
    with patch("viaduck.delivery.append_only", side_effect=_append):
        for cycle in range(40):
            _, epoch = mgr.read_plan()["d1"]
            mgr.buffer("d1", _dict_entry((cycle * 400) % 50_000, 400), cycle + 1, epoch=epoch)
            mgr.maybe_flush()
            assert mgr.wait_idle(10)
    assert honest_batches, "bytes trigger never fired"
    with mgr._lock:
        after = mgr._flush_target["d1"]
    # Honest-units end to end would accumulate ~target honest bytes before
    # triggering, so batches approach the target and fast flushes grow it.
    # Actual: every batch is ~0.02*target and the target never moves.
    assert after > target or max(honest_batches) >= 0.7 * target


def _fsl_dict_entry(offset: int, n: int, parent_rows: int = 100_000) -> pa.Table:
    dictionary = pa.array(["v" * 200 + str(i) for i in range(20_000)])
    indices = pa.array([i % 10 for i in range(parent_rows)], type=pa.int32())
    inner = pa.DictionaryArray.from_arrays(indices, dictionary)
    fsl = pa.FixedSizeListArray.from_arrays(inner, 1)
    return pa.table({"f": fsl}).slice(offset, n)


def test_fixed_size_list_dict_split_is_honest():
    entry = _fsl_dict_entry(5_000, 10_000)
    # Honest reference: identical payload as a plain (variable) list<dict>,
    # which _honest_type DOES decode.
    dictionary = pa.array(["v" * 200 + str(i) for i in range(20_000)])
    indices = pa.array([i % 10 for i in range(100_000)], type=pa.int32())
    inner = pa.DictionaryArray.from_arrays(indices, dictionary)
    list_equiv = pa.table({"f": pa.ListArray.from_arrays(pa.array(range(0, 100_001), type=pa.int32()), inner)}).slice(
        5_000, 10_000
    )
    ref = _estimate_row_bytes(list_equiv)
    assert 0 < ref < 2_000  # fixture sanity: the list<dict> path IS honest

    est = _estimate_row_bytes(entry)
    assert est <= ref * 3, f"fsl<dict> estimate {est} vs honest ~{ref}"

    # Manager-level consequence: honest target of ~1,000 rows gets crumbed.
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=ref * 1_000
    )
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", entry, 5, epoch=epoch)
    fake, calls = _capture_flush(mgr)
    with patch.object(mgr, "_flush", fake):
        mgr.maybe_flush()
        assert mgr.wait_idle(10)
    rows = sum(t.num_rows for t in calls[0][1])
    assert rows >= 500, f"crumb slice: {rows} rows against an honest ~1,000-row target"


# ---------------------------------------------------------------------------
# QE-BUG-3: zero-row head entry disarms the byte-cut for the whole buffer
# ---------------------------------------------------------------------------


def test_zero_row_head_entry_keeps_byte_cut_alive():
    entry = _dict_entry(5_000, 10_000)
    per_row = _estimate_row_bytes(entry)
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=per_row * 500
    )
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", entry.slice(0, 0), 1, epoch=epoch)  # zero-row head — buffer() admits it
    mgr.buffer("d1", entry, 5, epoch=epoch)
    fake, calls = _capture_flush(mgr)
    with patch.object(mgr, "_flush", fake):
        mgr.maybe_flush()
        assert mgr.wait_idle(10)
    rows = sum(t.num_rows for t in calls[0][1])
    assert rows <= 1_000, f"byte-cut disarmed: flushed {rows} rows against a ~500-row honest target"


# ---------------------------------------------------------------------------
# QE-RISK-4: head entry prices Nx under the tail entries (cross-entry skew)
# ---------------------------------------------------------------------------


def test_head_entry_underprices_tail_oversize_flush_and_spurious_growth():
    """Passing quantification of the head-entry sampling assumption
    ("entries of one destination share shape"). A 6 B/row head in front of
    ~1,010 B/row tail entries: admission prices the tail at 6 B/row, so a
    12KB target admits ~1.4MB honest (~117x oversize), and — because
    est_bytes is computed in the same mispriced units — the fill gate
    passes and a fast flush GROWS the target on top of the overshoot.
    Only the (real-world) duration signal can pull this back."""
    head = pa.table({"s": pa.array(["ab"] * 500)})
    wide = pa.table({"s": pa.array(["W" * 1000 + str(i) for i in range(200)])})
    per_row = _estimate_row_bytes(head)  # ~6 B/row
    target = per_row * 2_000
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=64 * 2**20
    )
    with mgr._lock:
        mgr._flush_target["d1"] = target
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", head, 1, epoch=epoch)
    for i in range(20):
        mgr.buffer("d1", wide, 2 + i, epoch=epoch)
    honest = []

    def _append(_pool, _dest, batch, **_kw):
        honest.append(_honest_nbytes(batch))
        return batch.num_rows

    with patch("viaduck.delivery.append_only", side_effect=_append):
        assert mgr.maybe_flush() == 1
        assert mgr.wait_idle(10)
    # The oversize flush: >10x the byte target in honest bytes (measured
    # ~117x with this shape; the factor is the tail/head width ratio).
    assert honest[0] > 10 * target
    # And the controller called it a target-sized fill and grew.
    with mgr._lock:
        assert mgr._flush_target["d1"] > target


# ---------------------------------------------------------------------------
# QE-RISK-5: within-entry width gradient (narrow sampled head, wide tail)
# ---------------------------------------------------------------------------


def test_within_entry_gradient_first_slice_overshoots_then_self_corrects():
    """Passing quantification of the 1,024-row head sample inside ONE
    entry: 1,024 narrow rows followed by 2,000 ~1KB rows. The first
    byte-cut slice overshoots the target by ~the width ratio (measured
    ~80x); every later slice re-samples the (now wide) remainder head and
    comes out target-sized. The chain terminates, the entry's cov is held
    (through=None) until the final completing slice."""
    narrow = pa.table({"s": pa.array(["ab"] * 1024)})
    wide = pa.table({"s": pa.array(["W" * 1000 + str(i) for i in range(2000)])})
    entry = pa.concat_tables([narrow, wide])  # ONE 3,024-row entry, 2 chunks
    per_row = _estimate_row_bytes(entry)  # sees only the narrow 1,024
    target = per_row * 2_048
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=64 * 2**20
    )
    with mgr._lock:
        mgr._flush_target["d1"] = target
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", entry, 5, epoch=epoch)
    calls = _drain_with_fake(mgr)

    sizes = [sum(t.num_rows for t in tables) for _, tables, _, _, _ in calls]
    honest = [sum(_honest_nbytes(t) for t in tables) for _, tables, _, _, _ in calls]
    throughs = [th for _, _, th, _, _ in calls]
    assert sum(sizes) == 3_024  # nothing lost, chain terminated
    assert honest[0] > 10 * target  # first slice: mispriced overshoot
    assert all(h <= 3 * target for h in honest[1:])  # resample self-corrects
    # Coverage discipline: cursor held on every partial slice, released
    # only by the completing one.
    assert all(th is None for th in throughs[:-1])
    assert throughs[-1] == 5


# ---------------------------------------------------------------------------
# QE-PIN: split-chain convergence on the uniform production shape (keeper)
# ---------------------------------------------------------------------------


def test_split_chain_converges_honestly_on_uniform_dict_entry():
    """A 10,000-row dictionary-encoded offset slice against a target of
    1,000 honest rows drains in exactly 10 honest-sized slices — pre-patch
    the inflated arithmetic produced ~50x more crumb slices. through=None
    on every slice but the last; the final full swap persists position."""
    entry = _dict_entry(5_000, 10_000)
    per_row = _estimate_row_bytes(entry)
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=per_row * 1_000
    )
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", entry, 5, epoch=epoch)
    calls = _drain_with_fake(mgr, max_cycles=30)

    sizes = [sum(t.num_rows for t in tables) for _, tables, _, _, _ in calls]
    throughs = [th for _, _, th, _, _ in calls]
    ests = [eb for _, _, _, _, eb in calls]
    assert sum(sizes) == 10_000
    assert len(sizes) == 10, f"expected 10 honest slices, got {len(sizes)}: {sizes[:20]}"
    assert min(sizes) >= 900  # no crumbs
    assert throughs == [None] * 9 + [5]
    # est_bytes handed to _flush is honest (rows x per_row), never raw nbytes
    for size, eb in zip(sizes, ests):
        assert eb == size * per_row


def test_one_row_floor_terminates():
    """Target below one row's honest price: the split floors at 1-row
    slices (documented) — must terminate, never emit a zero-row slice."""
    entry = _dict_entry(5_000, 25)
    per_row = _estimate_row_bytes(entry)
    assert per_row > 1
    mgr, _, _ = _manager(
        flush_batch_max_rows=0,
        flush_interval_seconds=0.0,
        flush_adaptive=True,
        flush_max_bytes=max(1, per_row // 2),
    )
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", entry, 5, epoch=epoch)
    calls = _drain_with_fake(mgr, max_cycles=60)
    sizes = [sum(t.num_rows for t in tables) for _, tables, _, _, _ in calls]
    assert sum(sizes) == 25
    assert all(s == 1 for s in sizes)


# ---------------------------------------------------------------------------
# QE-PIN: retention clamp + adaptive byte-split (HIGH-1 class, keeper)
# ---------------------------------------------------------------------------


def test_retention_clamp_with_adaptive_byte_split_does_not_drop_tail():
    """Re-run of the #80 HIGH-1 regression with the split driven by the
    ADAPTIVE byte-cut under honest sizing (rows cap off): a clamp raising
    `flushed` above the entry's cov mid-pile must not let any head-slice
    flush run DropCoveredPrefix over the undelivered tail. Every row must
    land; the clamp floor wins the cursor."""
    entry = _dict_entry(5_000, 5_000)
    per_row = _estimate_row_bytes(entry)
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=per_row * 1_000
    )
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", entry, through_snapshot=10, epoch=epoch)
    assert mgr.clamp_to_retention("d1", 50) == 0  # pre-clamp flushed was 0

    seen = []

    def _append(_pool, _dest, batch, **_kw):
        seen.append(batch.num_rows)
        return batch.num_rows

    with patch("viaduck.delivery.append_only", side_effect=_append):
        for _ in range(20):
            mgr.maybe_flush(shutdown=True)
            assert mgr.wait_idle(10)
            if mgr.is_clean("d1"):
                break
    assert sum(seen) == 5_000  # every buffered row delivered, none dropped
    assert len(seen) >= 5  # and it really was byte-split, not one swap
    assert mgr.status_snapshot()["d1"].flushed_snapshot == 50


# ---------------------------------------------------------------------------
# QE-PIN: adversarial chunk layouts price honestly (keepers)
# ---------------------------------------------------------------------------


def _mixed_chunks_entry() -> pa.Table:
    """One entry, one dictionary column, FIVE chunks: two different shared
    dictionaries, empty chunks interleaved. (A single column cannot mix
    dictionary and plain chunks — ChunkedArray is type-uniform — so the
    cross-dictionary + empty-chunk mix is the adversarial ceiling here.)"""
    d1 = pa.table(
        {
            "s": pa.DictionaryArray.from_arrays(
                pa.array([i % 10 for i in range(10_000)], type=pa.int32()),
                pa.array(["v" * 200 + str(i) for i in range(5_000)]),
            )
        }
    )
    d2 = pa.table(
        {
            "s": pa.DictionaryArray.from_arrays(
                pa.array([0, 1] * 500, type=pa.int32()),
                pa.array(["x" * 300 + str(i) for i in range(9_000)]),
            )
        }
    )
    empty = d1.slice(0, 0)
    return pa.concat_tables([d1.slice(0, 400), empty, d2.slice(0, 400), empty, d1.slice(500, 400)])


def test_multichunk_mixed_dictionaries_and_empty_chunks_price_honestly():
    cat = _mixed_chunks_entry()
    est = _estimate_row_bytes(cat)
    honest = _honest_nbytes(cat) / cat.num_rows
    assert honest * 0.5 <= est <= honest * 2, f"est {est} vs honest {honest:.0f}"
    assert cat.nbytes > est * cat.num_rows * 10  # fixture sanity: raw IS inflated

    # And the split lands near the target through the manager.
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=est * 400
    )
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", cat, 5, epoch=epoch)
    fake, calls = _capture_flush(mgr)
    with patch.object(mgr, "_flush", fake):
        mgr.maybe_flush()
        assert mgr.wait_idle(10)
    rows = sum(t.num_rows for t in calls[0][1])
    assert 200 <= rows <= 600, f"split of {rows} rows against a ~400-row honest target"


def test_map_of_dict_priced_honestly():
    """map<string, dict<string>> DOES cast on pyarrow 23 — the nested
    dictionary inside a map is decoded, not degraded to 0."""
    keys = pa.array(["k"] * 5_000)
    items = pa.DictionaryArray.from_arrays(
        pa.array([i % 10 for i in range(5_000)], type=pa.int32()),
        pa.array(["v" * 200 + str(i) for i in range(20_000)]),
    )
    offsets = pa.array(range(0, 5_001), type=pa.int32())
    tm = pa.table({"m": pa.MapArray.from_arrays(offsets, keys, items)}).slice(100, 500)
    est = _estimate_row_bytes(tm)
    assert 0 < est < 2_000
    assert tm.nbytes > est * tm.num_rows * 10


# ---------------------------------------------------------------------------
# QE-PIN: unpriceable entries degrade to flag-off, not crash (keeper)
# ---------------------------------------------------------------------------


def test_unpriceable_entries_degrade_to_rows_cap_and_frozen_target():
    """dict<struct> raises inside the estimator: per_row=0. Contract pinned
    here: (1) maybe_flush does not raise (poll cycle survives), (2) the
    byte-cut is inert — the whole pile flushes in one batch, (3) est_bytes
    is 0, not raw nbytes, so the AIMD holds instead of growing on inflated
    evidence (the new `elif byte_cap > 0` branch)."""
    mgr, _, _ = _manager(
        flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True, flush_max_bytes=64 * 2**20
    )
    target = 1_000_000
    with mgr._lock:
        mgr._flush_target["d1"] = target
    _, epoch = mgr.read_plan()["d1"]
    mgr.buffer("d1", _poison_entry(2_500), 5, epoch=epoch)
    mgr.buffer("d1", _poison_entry(2_500), 9, epoch=epoch)
    seen = []

    def _append(_pool, _dest, batch, **_kw):
        seen.append(batch.num_rows)
        return batch.num_rows

    with patch("viaduck.delivery.append_only", side_effect=_append):
        assert mgr.maybe_flush() == 1  # did not raise
        assert mgr.wait_idle(10)
    assert seen == [5_000]  # one unbounded batch: byte-cut inert by contract
    with mgr._lock:
        assert mgr._flush_target["d1"] == target  # est_bytes=0 → no spurious growth
    assert mgr.is_clean("d1")


def test_poisoned_destination_does_not_affect_healthy_peer():
    """Concurrent destinations: d1's entries are unpriceable, d2's are the
    honest dictionary shape. Sampling is per-destination, so d2 still gets
    honest-sized splits while d1 degrades — no cross-contamination."""
    entry2 = _dict_entry(5_000, 10_000)
    per_row = _estimate_row_bytes(entry2)
    mgr, _, _ = _manager(
        ("d1", "d2"),
        flush_batch_max_rows=0,
        flush_interval_seconds=0.0,
        flush_adaptive=True,
        flush_max_bytes=per_row * 1_000,
    )
    plan = mgr.read_plan()
    mgr.buffer("d1", _poison_entry(5_000), 5, epoch=plan["d1"][1])
    mgr.buffer("d2", entry2, 7, epoch=plan["d2"][1])
    fake, calls = _capture_flush(mgr)
    with patch.object(mgr, "_flush", fake):
        assert mgr.maybe_flush() == 2
        assert mgr.wait_idle(10)
    by_dest = {dest: sum(t.num_rows for t in tables) for dest, tables, _, _, _ in calls}
    assert by_dest["d1"] == 5_000  # poisoned: inert cut, whole pile
    assert 900 <= by_dest["d2"] <= 1_100  # healthy: honest ~1,000-row slice


# ---------------------------------------------------------------------------
# QE-OBS: failure-warning dedup is global per exception TYPE
# ---------------------------------------------------------------------------


def test_estimate_failure_warning_dedup_is_per_destination(caplog):
    """Post-fix: _estimate_failure_logged keys on (dest_id, exception type),
    so each destination's estimator failure logs once — inert destinations
    are individually visible in logs — while repeats stay suppressed."""
    import viaduck.delivery as delivery_mod

    delivery_mod._estimate_failure_logged.clear()
    mgr, _, _ = _manager(("d1", "d2"), flush_batch_max_rows=0, flush_interval_seconds=0.0, flush_adaptive=True)
    plan = mgr.read_plan()
    mgr.buffer("d1", _poison_entry(100), 5, epoch=plan["d1"][1])
    mgr.buffer("d2", _poison_entry(100), 7, epoch=plan["d2"][1])
    fake, _ = _capture_flush(mgr)
    with caplog.at_level(logging.WARNING, logger="viaduck.delivery"):
        with patch.object(mgr, "_flush", fake):
            mgr.maybe_flush()
            assert mgr.wait_idle(10)
        warnings = [r for r in caplog.records if "Honest-bytes estimate failed" in r.message]
        assert len(warnings) == 2  # one per destination

        plan = mgr.read_plan()
        mgr.buffer("d1", _poison_entry(100), 6, epoch=plan["d1"][1])
        fake2, _ = _capture_flush(mgr)
        with patch.object(mgr, "_flush", fake2):
            mgr.maybe_flush()
            assert mgr.wait_idle(10)
        warnings = [r for r in caplog.records if "Honest-bytes estimate failed" in r.message]
        assert len(warnings) == 2  # repeats suppressed per (dest, type)
