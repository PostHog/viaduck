"""Integration locks for the append_at_least_once fast path.

These tests drive _apply_changes end-to-end against a real pyducklake catalog
(local DuckDB, no Postgres/Docker) to validate the three properties the
contract advertises:

  1. Pure-insert batches actually land via tbl.append() — rows are present in
     the destination table after the call.
  2. The per-batch safety net works under real pyducklake: a mixed batch
     (any non-insert row) transparently uses the upsert path, with both the
     deletes and the updates applied correctly.
  3. The documented duplicate window is real: replaying the same pure-insert
     batch under the fast path produces duplicates in the destination, which
     is the contract the flag advertises. This guards against a future
     re-introduction of dedup-against-existing-rows masking the tradeoff.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pytest
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, NestedField, StringType

from viaduck import metrics
from viaduck.apply import _apply_changes

pytestmark = pytest.mark.integration


def setup_module():
    metrics.init("integration_test")


def _make_catalog(tmp_path, name: str) -> Catalog:
    base = tmp_path / name
    meta_db = str(base / "meta.duckdb")
    data_dir = str(base / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog(name, meta_db, data_path=data_dir)


SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="region", field_type=StringType()),
    NestedField(field_id=3, name="value", field_type=IntegerType()),
)


def _insert_batch(event_ids: list[int], *, snapshot_id: int = 1, start_rowid: int = 0) -> pa.Table:
    n = len(event_ids)
    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int32()),
            "region": pa.array([f"r{i % 3}" for i in range(n)], type=pa.string()),
            "value": pa.array([i * 10 for i in range(n)], type=pa.int32()),
            "change_type": pa.array(["insert"] * n, type=pa.string()),
            "snapshot_id": pa.array([snapshot_id] * n, type=pa.int64()),
            "rowid": pa.array(list(range(start_rowid, start_rowid + n)), type=pa.int64()),
        }
    )


@pytest.fixture()
def dest(tmp_path):
    catalog = _make_catalog(tmp_path, "dest")
    table = catalog.create_table("events", SCHEMA)
    yield catalog, table
    catalog.close()


def test_fast_path_inserts_land_in_destination(dest):
    """Pure-insert batch + flag on: rows are present after the call. Smoke
    test that tbl.append() is actually wired and the rows aren't dropped
    on the floor."""
    catalog, table = dest
    batch = _insert_batch([1, 2, 3])

    counts = _apply_changes(catalog, table, batch, ["event_id"], append_at_least_once=True)

    assert counts == {"deleted": 0, "upserted": 3, "upsert_matched": 0, "used_append": True}
    rows = catalog.load_table("events").scan().to_arrow()
    assert rows.num_rows == 3
    assert sorted(rows.column("event_id").to_pylist()) == [1, 2, 3]


def test_fast_path_replay_produces_duplicates(dest):
    """The documented tradeoff: applying the same pure-insert batch twice
    under the fast path lands the rows twice. This is the contract the
    flag advertises — at-least-once delivery into the destination.

    If a future refactor silently re-introduces dedupe-against-existing-rows
    on the fast path, this test will fail, which is exactly the surface we
    want to fail loudly."""
    catalog, table = dest
    batch = _insert_batch([1, 2])

    _apply_changes(catalog, table, batch, ["event_id"], append_at_least_once=True)
    _apply_changes(catalog, table, batch, ["event_id"], append_at_least_once=True)

    rows = catalog.load_table("events").scan().to_arrow()
    assert rows.num_rows == 4
    assert sorted(rows.column("event_id").to_pylist()) == [1, 1, 2, 2]


def test_upsert_path_replay_is_idempotent(dest):
    """Counterpoint to the above: with the flag OFF, replaying the same
    batch is idempotent (MERGE WHEN MATCHED collapses dupes). This is the
    semantics callers are giving up by enabling the flag."""
    catalog, table = dest
    batch = _insert_batch([1, 2])

    _apply_changes(catalog, table, batch, ["event_id"], append_at_least_once=False)
    _apply_changes(catalog, table, batch, ["event_id"], append_at_least_once=False)

    rows = catalog.load_table("events").scan().to_arrow()
    assert rows.num_rows == 2
    assert sorted(rows.column("event_id").to_pylist()) == [1, 2]


def test_mixed_batch_with_delete_falls_back_to_upsert(dest):
    """Safety net under real pyducklake: flag on, batch contains a delete.
    The delete actually deletes the existing row; remaining inserts land
    via upsert. None of this should silently take the append path and
    corrupt the destination."""
    catalog, table = dest
    seed = _insert_batch([1, 2, 3])
    _apply_changes(catalog, table, seed, ["event_id"], append_at_least_once=False)

    n = 1
    mixed = pa.table(
        {
            "event_id": pa.array([1, 4], type=pa.int32()),
            "region": pa.array(["r0", "rN"], type=pa.string()),
            "value": pa.array([0, 40], type=pa.int32()),
            "change_type": pa.array(["delete", "insert"], type=pa.string()),
            "snapshot_id": pa.array([2, 2], type=pa.int64()),
            "rowid": pa.array([100, 101], type=pa.int64()),
        }
    )

    counts = _apply_changes(catalog, table, mixed, ["event_id"], append_at_least_once=True)
    assert counts["deleted"] == n
    assert counts["upserted"] == 1

    rows = catalog.load_table("events").scan().to_arrow()
    assert sorted(rows.column("event_id").to_pylist()) == [2, 3, 4]


def test_mixed_batch_with_update_postimage_falls_back_to_upsert(dest):
    """Safety net for the schema-evolution case: a future CDC change introduces
    update_postimage rows. The flag must not turn those into duplicate
    rows via append — they must overwrite the existing key via MERGE."""
    catalog, table = dest
    seed = _insert_batch([1, 2])
    _apply_changes(catalog, table, seed, ["event_id"], append_at_least_once=False)

    update = pa.table(
        {
            "event_id": pa.array([1], type=pa.int32()),
            "region": pa.array(["updated"], type=pa.string()),
            "value": pa.array([999], type=pa.int32()),
            "change_type": pa.array(["update_postimage"], type=pa.string()),
            "snapshot_id": pa.array([2], type=pa.int64()),
            "rowid": pa.array([1], type=pa.int64()),
        }
    )

    _apply_changes(catalog, table, update, ["event_id"], append_at_least_once=True)
    rows = catalog.load_table("events").scan().to_arrow().sort_by([("event_id", "ascending")])
    # event_id=1 should have been updated, not duplicated.
    assert rows.num_rows == 2
    event_1 = rows.filter(pa.compute.equal(rows.column("event_id"), pa.scalar(1, type=pa.int32())))
    assert event_1.column("value").to_pylist() == [999]
    assert event_1.column("region").to_pylist() == ["updated"]


def test_fast_path_cursor_advance_failure_replays_through_delivery(tmp_path):
    """The stated retry scenario in production is: destination apply commits,
    then cursor advance fails (PG blip, etc.), and the next poll re-reads
    the same source range and presents the same batch to apply again. This
    test drives that scenario through DeliveryManager._flush (not just two
    direct _apply_changes calls):

      1. First flush: apply commits, cursor advance is mocked to raise.
      2. Second flush: same range buffered again (simulating the re-read),
         apply commits a second time.

    Fast path: 4 rows in destination (2 from each apply).
    Upsert path (counterpoint): 2 rows (MERGE collapsed the replay).

    Defends against a refactor that silently introduces dedup-against-existing
    on the fast path, or that catches the cursor advance exception silently
    and never replays.
    """
    from unittest.mock import MagicMock

    from viaduck.config import DeliveryConfig
    from viaduck.delivery import DeliveryManager

    catalog = _make_catalog(tmp_path, "dest")
    table = catalog.create_table("events", SCHEMA)

    pool = MagicMock()
    pool.get.return_value = (catalog, table)
    pool.release = MagicMock()
    pool.evict = MagicMock()

    state = MagicMock()
    state.load_cursors.return_value = {}
    # Raise once, then succeed. The 3-attempt retry inside
    # _advance_cursor_with_retry burns through all 3 attempts here so the
    # whole _flush call surfaces an exception, which is the exact production
    # behavior we want to simulate (cursor-blip → range re-read next cycle).
    advance_calls = {"n": 0}

    def advance_cursor_raises_first_time(*args, **kwargs):
        advance_calls["n"] += 1
        if advance_calls["n"] <= 3:
            raise RuntimeError("simulated PG cursor advance failure")

    state.advance_cursor.side_effect = advance_cursor_raises_first_time

    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0),
        state,
        pool,
        ["event_id"],
        ["d1"],
        append_at_least_once_by_dest={"d1": True},
    )

    batch = _insert_batch([1, 2])

    # Apply round 1: commits to destination, cursor advance fails 3x → flush raises.
    mgr.buffer("d1", batch, through_snapshot=7)
    mgr.maybe_flush()
    mgr.wait_idle()

    # Reset side_effect so round 2 lets cursor advance through.
    state.advance_cursor.side_effect = None

    # Round 2: same range re-presented (production: poll re-reads because
    # cursor didn't move). New buffer, same batch.
    mgr.buffer("d1", batch, through_snapshot=7)
    mgr.maybe_flush()
    mgr.wait_idle()

    rows = catalog.load_table("events").scan().to_arrow()
    assert rows.num_rows == 4, "fast path must duplicate the replayed batch (no MERGE collapse)"
    assert sorted(rows.column("event_id").to_pylist()) == [1, 1, 2, 2]


def test_upsert_path_cursor_advance_failure_replays_idempotently(tmp_path):
    """Counterpoint to the above: flag OFF, same scenario, the replayed
    batch is collapsed by MERGE WHEN MATCHED — only 2 rows in destination.
    This is the property the fast path explicitly trades away."""
    from unittest.mock import MagicMock

    from viaduck.config import DeliveryConfig
    from viaduck.delivery import DeliveryManager

    catalog = _make_catalog(tmp_path, "dest")
    table = catalog.create_table("events", SCHEMA)

    pool = MagicMock()
    pool.get.return_value = (catalog, table)
    pool.release = MagicMock()
    pool.evict = MagicMock()

    state = MagicMock()
    state.load_cursors.return_value = {}
    advance_calls = {"n": 0}

    def advance_cursor_raises_first_time(*args, **kwargs):
        advance_calls["n"] += 1
        if advance_calls["n"] <= 3:
            raise RuntimeError("simulated PG cursor advance failure")

    state.advance_cursor.side_effect = advance_cursor_raises_first_time

    mgr = DeliveryManager(
        DeliveryConfig(workers=1, flush_interval_seconds=0.0),
        state,
        pool,
        ["event_id"],
        ["d1"],
        # No append_at_least_once_by_dest → defaults to upsert path
    )

    batch = _insert_batch([1, 2])

    mgr.buffer("d1", batch, through_snapshot=7)
    mgr.maybe_flush()
    mgr.wait_idle()

    state.advance_cursor.side_effect = None

    mgr.buffer("d1", batch, through_snapshot=7)
    mgr.maybe_flush()
    mgr.wait_idle()

    rows = catalog.load_table("events").scan().to_arrow()
    assert rows.num_rows == 2, "upsert path must collapse the replayed batch via MERGE WHEN MATCHED"
    assert sorted(rows.column("event_id").to_pylist()) == [1, 2]


def test_fast_path_within_batch_dupes_collapse_to_winner(dest):
    """Within-batch dedup runs on the fast path too: three copies of the
    same key in one batch land as one row, not three. The deterministic
    winner (highest snapshot_id then highest rowid) is the version that
    lands."""
    catalog, table = dest
    batch = pa.table(
        {
            "event_id": pa.array([1, 1, 1], type=pa.int32()),
            "region": pa.array(["old", "newer", "newest"], type=pa.string()),
            "value": pa.array([10, 20, 30], type=pa.int32()),
            "change_type": pa.array(["insert", "insert", "insert"], type=pa.string()),
            "snapshot_id": pa.array([1, 2, 3], type=pa.int64()),
            "rowid": pa.array([1, 2, 3], type=pa.int64()),
        }
    )

    counts = _apply_changes(catalog, table, batch, ["event_id"], append_at_least_once=True)
    assert counts["upserted"] == 1

    rows = catalog.load_table("events").scan().to_arrow()
    assert rows.num_rows == 1
    assert rows.column("event_id").to_pylist() == [1]
    assert rows.column("value").to_pylist() == [30]  # highest snapshot wins
    assert rows.column("region").to_pylist() == ["newest"]
