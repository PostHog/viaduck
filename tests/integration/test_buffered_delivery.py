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
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pyducklake import Schema
from pyducklake.types import IntegerType, NestedField, StringType

from viaduck import metrics
from viaduck.config import DeliveryConfig, StateConfig
from viaduck.delivery import DeliveryManager
from viaduck.destination import DestinationPool
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
    cfg.destination_by_id = lambda d: dests[d]
    pool = DestinationPool(cfg, max_open=10)
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
    mgr = DeliveryManager(DeliveryConfig(workers=2, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"])

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
    flush cancels them — the row never reaches the destination."""
    pool = _make_pool(tmp_path, ["d1"])
    state.initialize_destinations(["d1"])
    mgr = DeliveryManager(DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d1"])

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
    mgr = DeliveryManager(DeliveryConfig(workers=1, flush_interval_seconds=0.0), state, pool, ["event_id"], ["d-bad"])

    mgr.buffer("d-bad", _cdc_batch("acme", [1]), through_snapshot=7)
    mgr.maybe_flush()
    assert mgr.wait_idle(60)

    # FlushFail: cursor untouched, error recorded, position reset.
    cur = state.load_cursors(["d-bad"])["d-bad"]
    assert cur.last_snapshot_id == 3
    assert cur.last_error is not None
    assert mgr.positions() == {"d-bad": 3}
    assert mgr.status_snapshot()["d-bad"].buffer_rows == 0
    pool.close_all()


def test_concurrent_multi_destination_flushes(tmp_path, state):
    dest_ids = [f"d{i}" for i in range(6)]
    pool = _make_pool(tmp_path, dest_ids)
    state.initialize_destinations(dest_ids)
    mgr = DeliveryManager(DeliveryConfig(workers=4, flush_interval_seconds=0.0), state, pool, ["event_id"], dest_ids)

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
    mgr = DeliveryManager(DeliveryConfig(workers=1, flush_interval_seconds=3600.0), state, pool, ["event_id"], ["d1"])

    mgr.buffer("d1", _cdc_batch("acme", [1, 2]), through_snapshot=4)
    mgr.drain(timeout_s=60)

    assert _read_dest(pool, "d1").num_rows == 2
    assert state.load_cursors(["d1"])["d1"].last_snapshot_id == 4
    pool.close_all()
