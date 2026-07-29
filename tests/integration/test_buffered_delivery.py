"""End-to-end buffered delivery: real DeliveryManager + worker pool + real
pyducklake destination catalogs (local DuckDB) + real Postgres cursor store.

Covers the FlushCommit/FlushFail paths with nothing mocked below the
delivery API: buffered CDC batches land in destination tables, cursors
persist to PG, failures drop buffers and leave cursors untouched, drain
flushes everything at shutdown, and concurrent multi-destination flushes
don't interfere.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyducklake import Schema
from pyducklake.types import IntegerType, NestedField, StringType

from viaduck import metrics
from viaduck.config import DeliveryConfig, StateConfig
from viaduck.delivery import DeliveryManager
from viaduck.destination import DestinationPool
from viaduck.registry import DestinationRegistry
from viaduck.state import StateManager

pytestmark = pytest.mark.integration

SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="company", field_type=StringType(), required=True),
    NestedField(field_id=3, name="value", field_type=IntegerType()),
)


def setup_module():
    metrics.init("integration_test")


def _dest_config(tmp_path, dest_id: str, *, data_path: str | None = None):
    base = tmp_path / dest_id
    os.makedirs(base / "data", exist_ok=True)
    d = MagicMock()
    d.id = dest_id
    d.name = dest_id
    d.postgres_uri = str(base / "meta.duckdb")
    d.data_path = data_path if data_path is not None else str(base / "data")
    d.table = "events"
    d.resolved_properties.return_value = {}
    return d


def _make_pool(tmp_path, dest_ids: list[str], broken: set[str] = frozenset()):
    cfg = MagicMock()
    dests = {
        d: _dest_config(
            tmp_path,
            d,
            # A nonexistent, uncreatable data path makes writes fail.
            data_path="/dev/null/nope" if d in broken else None,
        )
        for d in dest_ids
    }
    registry = DestinationRegistry.from_configs(list(dests.values()))
    for did in dest_ids:
        assert registry.config_for(did).id == did  # fixture wiring guard
    pool = DestinationPool(cfg, registry, max_open=10)
    pool.set_source_schema(SCHEMA)
    return pool


def _cdc_batch(company: str, event_ids: list[int], change_type: str = "insert") -> pa.Table:
    n = len(event_ids)
    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int32()),
            "company": pa.array([company] * n, type=pa.string()),
            "value": pa.array([i * 10 for i in event_ids], type=pa.int32()),
            "change_type": pa.array([change_type] * n, type=pa.string()),
            "snapshot_id": pa.array([7] * n, type=pa.int64()),
            "rowid": pa.array(event_ids, type=pa.int64()),
        }
    )


@pytest.fixture()
def state(pg_uri, state_table_name):
    sm = StateManager(pg_uri, "i1", StateConfig(table=state_table_name))
    yield sm
    sm.close()


def _read_dest(pool: DestinationPool, dest_id: str) -> pa.Table:
    catalog, table = pool.get(dest_id)
    try:
        return catalog.load_table("events").scan().to_arrow()
    finally:
        pool.release(dest_id)


def test_buffered_flush_end_to_end(tmp_path, state):
    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    mgr = DeliveryManager(
        DeliveryConfig(workers=2, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"], mode="full_cdc"
    )

    # Two buffered reads over adjacent ranges, flushed as one batch.
    mgr.buffer("d1", _cdc_batch("acme", [1, 2]), through_snapshot=7)
    mgr.buffer("d1", _cdc_batch("acme", [3]), through_snapshot=9)
    assert mgr.maybe_flush() == 1
    assert mgr.wait_idle()

    rows = _read_dest(pool, "d1")
    assert sorted(rows.column("event_id").to_pylist()) == [1, 2, 3]
    # Meta columns stripped by the apply path.
    assert "change_type" not in rows.column_names

    cur = state.load_cursors(["d1"])["d1"]
    assert cur.last_snapshot_id == 9
    assert cur.rows_replicated == 3
    pool.close_all()


def test_cross_read_conflict_resolution_at_flush(tmp_path, state):
    """Insert buffered in one read, its delete in a later read: Phase 2 at
    flush drops the insert and applies the surviving tombstone delete —
    the row never appears in the destination."""
    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"], mode="full_cdc"
    )

    mgr.buffer("d1", _cdc_batch("acme", [1, 2], "insert"), through_snapshot=7)
    mgr.buffer("d1", _cdc_batch("acme", [2], "delete"), through_snapshot=8)
    mgr.maybe_flush()
    assert mgr.wait_idle()

    rows = _read_dest(pool, "d1")
    assert rows.column("event_id").to_pylist() == [1]
    assert state.load_cursors(["d1"])["d1"].last_snapshot_id == 8
    pool.close_all()


def test_flush_failure_leaves_cursor_and_recovers(tmp_path, state):
    pool = _make_pool(tmp_path, ["d-bad"], broken={"d-bad"})
    state.initialize_destinations(["d-bad"])
    state.advance_cursor("d-bad", snapshot_id=3)
    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d-bad"], mode="full_cdc"
    )

    # Patch OCC retry sleeps to zero — the pool is permanently broken, so
    # every retry attempt fails identically, and the test only cares that
    # the flush ultimately raises. Without the patch the retry policy
    # (`_WRITE_MAX_RETRIES` attempts with backoff capped at
    # `_WRITE_MAX_DELAY_S`) would burn minutes of real time before
    # exhausting. Patch `_backoff_sleep` (not `time.sleep`) because the
    # delivery layer threads a stop_event and the retry loop uses
    # `Event.wait` under the covers.
    mgr.buffer("d-bad", _cdc_batch("acme", [1]), through_snapshot=7)
    with patch("viaduck.apply._backoff_sleep", return_value=False):
        mgr.maybe_flush()
        assert mgr.wait_idle(60)

    # FlushFail: cursor untouched, error recorded, position reset.
    cur = state.load_cursors(["d-bad"])["d-bad"]
    assert cur.last_snapshot_id == 3
    assert cur.last_error is not None
    assert mgr.positions() == {"d-bad": 3}
    assert mgr.status_snapshot()["d-bad"].buffer_rows == 0
    pool.close_all()


def test_broken_destination_at_cap_does_not_block_healthy_peer(tmp_path, state):
    """QE must-fix #3, on the REAL DeliveryManager + real pyducklake: one
    destination broken (writes fail), one healthy, both queued past a tiny
    per-destination cap. The healthy destination's flush must land and its
    cursor advance while the broken one fails — and after the dust settles
    NEITHER destination is stuck at its cap (the broken one's failure path
    drops its buffer and zeroes its in-flight accounting). Catches
    real-accounting regressions the mocked poll-cycle tests structurally
    cannot (e.g. inflight_bytes zeroing moved out of _flush's finally).
    """
    pool = _make_pool(tmp_path, ["d-ok", "d-bad"], broken={"d-bad"})
    state.initialize_destinations(["d-ok", "d-bad"])
    mgr = DeliveryManager(
        DeliveryConfig(workers=2, flush_interval_seconds=0.0, buffer_max_bytes_per_destination=1),
        state,
        pool,
        ["event_id"],
        ["d-ok", "d-bad"],
        mode="full_cdc",
    )

    mgr.buffer("d-ok", _cdc_batch("acme", [1, 2]), through_snapshot=7)
    mgr.buffer("d-bad", _cdc_batch("evil", [3]), through_snapshot=7)
    # Both over the 1-byte cap: reads paused for both, destination-locally.
    assert mgr.should_pause_reads_for("d-ok")
    assert mgr.should_pause_reads_for("d-bad")

    with patch("viaduck.apply._backoff_sleep", return_value=False):
        assert mgr.maybe_flush() == 2
        assert mgr.wait_idle(60)

    # Healthy destination: rows landed, cursor advanced.
    rows = _read_dest(pool, "d-ok")
    assert sorted(rows.column("event_id").to_pylist()) == [1, 2]
    cursors = state.load_cursors(["d-ok", "d-bad"])
    assert cursors["d-ok"].last_snapshot_id == 7
    # Broken destination: cursor untouched, error recorded.
    assert cursors["d-bad"].last_snapshot_id == 0
    assert cursors["d-bad"].last_error is not None
    # Both queues drained — nobody is wedged at cap.
    assert not mgr.should_pause_reads_for("d-ok")
    assert not mgr.should_pause_reads_for("d-bad")
    pool.close_all()


def test_concurrent_multi_destination_flushes(tmp_path, state):
    dest_ids = [f"d{i}" for i in range(6)]
    pool = _make_pool(tmp_path, dest_ids)
    state.initialize_destinations(dest_ids)
    mgr = DeliveryManager(
        DeliveryConfig(workers=4, flush_interval_seconds=0.0), state, pool, ["event_id"], dest_ids, mode="full_cdc"
    )

    for i, d in enumerate(dest_ids):
        mgr.buffer(d, _cdc_batch(d, list(range(1, 4 + i))), through_snapshot=5)
    assert mgr.maybe_flush() == len(dest_ids)
    assert mgr.wait_idle(60)

    cursors = state.load_cursors(dest_ids)
    for i, d in enumerate(dest_ids):
        rows = _read_dest(pool, d)
        assert rows.num_rows == 3 + i
        assert cursors[d].last_snapshot_id == 5
        assert cursors[d].rows_replicated == 3 + i
    pool.close_all()


def test_drain_flushes_buffered_data_on_shutdown(tmp_path, state):
    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    # Hour-long interval: nothing would flush without the shutdown trigger.
    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=3600.0), state, pool, ["event_id"], ["d1"], mode="full_cdc"
    )

    mgr.buffer("d1", _cdc_batch("acme", [1, 2]), through_snapshot=4)
    mgr.drain(timeout_s=60)

    assert _read_dest(pool, "d1").num_rows == 2
    assert state.load_cursors(["d1"])["d1"].last_snapshot_id == 4
    pool.close_all()


def test_multi_update_window_no_duplicate_keys(tmp_path, state):
    """Soak-found bug lock: several updates to the same key inside one
    buffered window must collapse to one destination row (Winner(k)),
    not duplicate rows from a duplicate-join-key upsert."""
    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"], mode="full_cdc"
    )

    insert = _cdc_batch("acme", [1])
    update1 = pa.table(
        {
            "event_id": pa.array([1], type=pa.int32()),
            "company": pa.array(["acme"], type=pa.string()),
            "value": pa.array([111], type=pa.int32()),
            "change_type": pa.array(["update_postimage"], type=pa.string()),
            "snapshot_id": pa.array([8], type=pa.int64()),
            "rowid": pa.array([1], type=pa.int64()),
        }
    )
    update2 = update1.set_column(2, "value", pa.array([222], type=pa.int32())).set_column(
        4, "snapshot_id", pa.array([9], type=pa.int64())
    )
    mgr.buffer("d1", insert, 7)
    mgr.buffer("d1", update1, 8)
    mgr.buffer("d1", update2, 9)
    mgr.maybe_flush()
    assert mgr.wait_idle()

    rows = _read_dest(pool, "d1")
    assert rows.num_rows == 1
    assert rows.column("value").to_pylist() == [222]  # last write wins
    pool.close_all()


def test_phantom_heal_after_commit_cursor_gap(tmp_path, state):
    """The commit/cursor-gap replay heals phantoms (spec: tombstone rule).

    Simulate the gap exactly: apply a batch directly to the destination
    WITHOUT advancing the cursor (the crashed flush), then replay the
    range through the DeliveryManager with the row's delete included.
    Under the old cancel-both rule the insert+delete pair cancelled and
    the committed row stayed forever; the tombstone removes it."""
    from viaduck.apply import apply_full_cdc

    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"], mode="full_cdc"
    )

    # Normal flush through snapshot 7: row 1 delivered, cursor = 7.
    mgr.buffer("d1", _cdc_batch("acme", [1]), through_snapshot=7)
    mgr.maybe_flush()
    assert mgr.wait_idle()
    assert state.load_cursors(["d1"])["d1"].last_snapshot_id == 7

    # Crashed flush: destination commit lands (row 2), cursor does NOT move.
    crashed = pa.table(
        {
            "event_id": pa.array([2], type=pa.int32()),
            "company": pa.array(["acme"], type=pa.string()),
            "value": pa.array([20], type=pa.int32()),
            "change_type": pa.array(["insert"], type=pa.string()),
            "snapshot_id": pa.array([8], type=pa.int64()),
            "rowid": pa.array([2], type=pa.int64()),
        }
    )
    apply_full_cdc(pool, "d1", crashed, ["event_id"])
    assert sorted(_read_dest(pool, "d1").column("event_id").to_pylist()) == [1, 2]

    # Source deletes row 2 before recovery. The replay range (7, 9]
    # carries BOTH the insert (snap 8) and the delete (snap 9).
    replay = pa.table(
        {
            "event_id": pa.array([2, 2], type=pa.int32()),
            "company": pa.array(["acme", "acme"], type=pa.string()),
            "value": pa.array([20, 20], type=pa.int32()),
            "change_type": pa.array(["insert", "delete"], type=pa.string()),
            "snapshot_id": pa.array([8, 9], type=pa.int64()),
            "rowid": pa.array([2, 2], type=pa.int64()),
        }
    )
    mgr.buffer("d1", replay, through_snapshot=9)
    mgr.maybe_flush()
    assert mgr.wait_idle()

    # Phantom healed; cursor advanced past the replay.
    assert _read_dest(pool, "d1").column("event_id").to_pylist() == [1]
    assert state.load_cursors(["d1"])["d1"].last_snapshot_id == 9
    pool.close_all()


def test_phantom_heal_with_key_reuse_in_replay(tmp_path, state):
    """Replay contains the phantom's insert+delete AND a new same-key
    insert (key reuse, new rowid): the tombstone removes the old copy and
    the Winner upsert lands the new row — delete-before-upsert ordering
    is load-bearing here."""
    from viaduck.apply import apply_full_cdc

    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"], mode="full_cdc"
    )

    crashed = pa.table(
        {
            "event_id": pa.array([2], type=pa.int32()),
            "company": pa.array(["acme"], type=pa.string()),
            "value": pa.array([20], type=pa.int32()),
            "change_type": pa.array(["insert"], type=pa.string()),
            "snapshot_id": pa.array([8], type=pa.int64()),
            "rowid": pa.array([2], type=pa.int64()),
        }
    )
    apply_full_cdc(pool, "d1", crashed, ["event_id"])

    replay = pa.table(
        {
            "event_id": pa.array([2, 2, 2], type=pa.int32()),
            "company": pa.array(["acme"] * 3, type=pa.string()),
            "value": pa.array([20, 20, 99], type=pa.int32()),
            "change_type": pa.array(["insert", "delete", "insert"], type=pa.string()),
            "snapshot_id": pa.array([8, 9, 10], type=pa.int64()),
            "rowid": pa.array([2, 2, 5], type=pa.int64()),
        }
    )
    mgr.buffer("d1", replay, through_snapshot=10)
    mgr.maybe_flush()
    assert mgr.wait_idle()

    rows = _read_dest(pool, "d1")
    assert rows.column("event_id").to_pylist() == [2]
    assert rows.column("value").to_pylist() == [99]  # the reused key's NEW row
    pool.close_all()


def test_applied_counters_accumulate_by_change_type(tmp_path, state):
    """Vanity counters for the UI: per-type applied counts (pre-Phase-2)
    and cumulative buffered rows accumulate on successful flushes."""
    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"], mode="full_cdc"
    )

    mgr.buffer("d1", _cdc_batch("acme", [1, 2]), through_snapshot=7)  # 2 inserts
    mgr.buffer("d1", _cdc_batch("acme", [2], "delete"), through_snapshot=8)  # 1 delete
    mgr.maybe_flush()
    assert mgr.wait_idle()

    st = mgr.status_snapshot()["d1"]
    # Pre-Phase-2 counts: the insert+delete pair for event 2 still counts
    # one insert and one delete even though Phase 2 tombstoned it.
    assert (st.applied_inserts, st.applied_updates, st.applied_deletes) == (2, 0, 1)
    assert st.buffered_rows_total == 3
    pool.close_all()
