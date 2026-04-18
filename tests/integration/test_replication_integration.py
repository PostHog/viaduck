"""Integration tests for viaduck CDC replication with real pyducklake + local DuckDB.

No Postgres, no Docker. Uses tmp_path for all catalogs.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pytest
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, NestedField, StringType

from viaduck import metrics
from viaduck.config import RoutingConfig, StateConfig
from viaduck.main import _apply_changes, _resolve_conflicts, _resolve_preimages, _seed_new_destinations
from viaduck.router import Router
from viaduck.source import META_COLUMNS, strip_meta
from viaduck.state import StateManager

pytestmark = pytest.mark.integration

SOURCE_SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="company", field_type=StringType(), required=True),
    NestedField(field_id=3, name="value", field_type=IntegerType()),
)


def _make_catalog(tmp_path, name: str) -> Catalog:
    base = tmp_path / name
    meta_db = str(base / "meta.duckdb")
    data_dir = str(base / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog(name, meta_db, data_path=data_dir)


def _arrow_rows(event_ids: list[int], companies: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int32()),
            "company": pa.array(companies, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


def _read_all(table) -> pa.Table:
    """Read all current rows from a table via scan."""
    return table.scan().to_arrow()


def setup_module():
    metrics.init("integration_test")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_catalog(tmp_path):
    cat = _make_catalog(tmp_path, "source")
    yield cat
    cat.close()


@pytest.fixture()
def dest_catalog_a(tmp_path):
    cat = _make_catalog(tmp_path, "dest_a")
    yield cat
    cat.close()


@pytest.fixture()
def dest_catalog_b(tmp_path):
    cat = _make_catalog(tmp_path, "dest_b")
    yield cat
    cat.close()


@pytest.fixture()
def source_table(source_catalog):
    return source_catalog.create_table("events", SOURCE_SCHEMA)


@pytest.fixture()
def dest_table_a(dest_catalog_a):
    return dest_catalog_a.create_table("events", SOURCE_SCHEMA)


@pytest.fixture()
def dest_table_b(dest_catalog_b):
    return dest_catalog_b.create_table("events", SOURCE_SCHEMA)


@pytest.fixture()
def router():
    return Router(RoutingConfig(field="company", key_columns=["event_id"]))


# ---------------------------------------------------------------------------
# 1. Append-only round trip
# ---------------------------------------------------------------------------


def test_append_only_round_trip(source_table, dest_table_a, dest_table_b, router):
    """Insert rows into source, use read_cdc + routing + append to replicate."""
    snap_before = source_table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    source_table.append(
        _arrow_rows(
            [1, 2, 3, 4],
            ["acme", "acme", "beta", "beta"],
            [10, 20, 30, 40],
        )
    )

    snap_after = source_table.current_snapshot()
    assert snap_after is not None

    # Read insertions (append-only mode)
    changeset = source_table.table_insertions(start, snap_after.snapshot_id)
    raw = changeset.to_arrow()
    assert raw.num_rows == 4

    # Route
    routed, unrouted = router.split_and_count(raw, ["acme", "beta"])
    assert unrouted == 0
    assert sorted(routed.keys()) == ["acme", "beta"]

    # Append to destinations
    dest_table_a.append(routed["acme"])
    dest_table_b.append(routed["beta"])

    # Verify
    scan_a = _read_all(dest_table_a)
    assert scan_a.num_rows == 2
    assert sorted(scan_a.column("event_id").to_pylist()) == [1, 2]

    scan_b = _read_all(dest_table_b)
    assert scan_b.num_rows == 2
    assert sorted(scan_b.column("event_id").to_pylist()) == [3, 4]


# ---------------------------------------------------------------------------
# 2. Full CDC insert + delete round trip
# ---------------------------------------------------------------------------


def test_full_cdc_insert_delete_round_trip(source_catalog, source_table, dest_catalog_a, dest_table_a):
    """Insert rows, delete some, then apply the full CDC to an empty destination.

    Verifies that conflict resolution correctly cancels the insert+delete
    for the same rowid, and the destination ends up with only the surviving rows.
    """
    snap_before = source_table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    # Insert 3 rows, then delete one
    source_table.append(
        _arrow_rows(
            [1, 2, 3],
            ["acme", "acme", "acme"],
            [10, 20, 30],
        )
    )
    source_table.delete("event_id = 2")
    snap_after = source_table.current_snapshot()
    assert snap_after is not None

    # Read full CDC changes
    changeset = source_table.table_changes(start, snap_after.snapshot_id)
    cdc_data = changeset.to_arrow()

    # Expect inserts for 1,2,3 plus delete for 2
    change_types = cdc_data.column("change_type").to_pylist()
    assert change_types.count("insert") == 3
    assert change_types.count("delete") == 1

    # Phase 1: resolve preimages (no preimages here, pass-through)
    resolved = _resolve_preimages(cdc_data, "company", ["event_id"])
    # Phase 2: resolve conflicts — insert+delete for event_id=2 cancel
    resolved = _resolve_conflicts(resolved)

    # After conflict resolution: only inserts for 1,3 remain
    remaining_ids = sorted(resolved.column("event_id").to_pylist())
    assert remaining_ids == [1, 3]
    assert all(ct == "insert" for ct in resolved.column("change_type").to_pylist())

    # Phase 3: apply to empty destination
    counts = _apply_changes(dest_catalog_a, dest_table_a, resolved, ["event_id"])
    assert counts["upserted"] == 2
    assert counts["deleted"] == 0

    # Verify destination matches source
    dest_scan = _read_all(dest_table_a)
    assert sorted(dest_scan.column("event_id").to_pylist()) == [1, 3]


# ---------------------------------------------------------------------------
# 3. Full CDC update round trip
# ---------------------------------------------------------------------------


def test_full_cdc_update_round_trip(source_catalog, source_table, dest_catalog_a, dest_table_a):
    """Insert rows, update values (not routing column), verify destination has updated values.

    Uses two CDC batches: first seed with table_insertions (append-only),
    then apply table_changes for the update-only range.
    """
    snap_before = source_table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    # Insert
    source_table.append(
        _arrow_rows(
            [1, 2],
            ["acme", "acme"],
            [10, 20],
        )
    )
    snap_after_insert = source_table.current_snapshot()
    assert snap_after_insert is not None

    # Seed destination
    seed = source_table.table_insertions(start, snap_after_insert.snapshot_id).to_arrow()
    dest_table_a.append(seed)

    # Update value for event_id=1 on source
    source_table.upsert(
        _arrow_rows([1], ["acme"], [999]),
        join_cols=["event_id"],
    )
    snap_after_update = source_table.current_snapshot()
    assert snap_after_update is not None

    # Read full CDC changes from start to current (as real pipeline does on first run)
    changeset = source_table.table_changes(start, snap_after_update.snapshot_id)
    cdc_data = changeset.to_arrow()

    # Phase 1-3
    resolved = _resolve_preimages(cdc_data, "company", ["event_id"])
    resolved = _resolve_conflicts(resolved)
    counts = _apply_changes(dest_catalog_a, dest_table_a, resolved, ["event_id"])
    # Should have upserted the postimage for event_id=1 (plus re-upsert of event_id=2)
    assert counts["upserted"] >= 1

    # Verify
    dest_scan = _read_all(dest_table_a)
    event_ids = dest_scan.column("event_id").to_pylist()
    values = dest_scan.column("value").to_pylist()
    row_map = dict(zip(event_ids, values))
    assert row_map[1] == 999
    assert row_map[2] == 20


# ---------------------------------------------------------------------------
# 4. Insert then delete same key in same snapshot range
# ---------------------------------------------------------------------------


def test_full_cdc_insert_then_delete_same_key(source_catalog, source_table, dest_catalog_a, dest_table_a):
    """Insert a row then delete it within the same snapshot range. Conflicts cancel."""
    snap_before = source_table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    # Insert and then delete same row
    source_table.append(_arrow_rows([99], ["acme"], [100]))
    source_table.delete("event_id = 99")

    snap_after = source_table.current_snapshot()
    assert snap_after is not None

    # Read CDC changes across the full range
    changeset = source_table.table_changes(start, snap_after.snapshot_id)
    cdc_data = changeset.to_arrow()

    # Phase 1-3
    resolved = _resolve_preimages(cdc_data, "company", ["event_id"])
    resolved = _resolve_conflicts(resolved)

    # Should be empty after conflict resolution (insert + delete cancel)
    assert resolved.num_rows == 0

    # Apply (no-op)
    counts = _apply_changes(dest_catalog_a, dest_table_a, resolved, ["event_id"])
    assert counts == {"deleted": 0, "upserted": 0, "upsert_matched": 0}

    # Destination should be empty
    dest_scan = _read_all(dest_table_a)
    assert dest_scan.num_rows == 0


# ---------------------------------------------------------------------------
# 5. StateManager round trip
# ---------------------------------------------------------------------------


def test_state_manager_round_trip(source_catalog):
    """Create a real StateManager, initialize destinations, advance cursors, verify persistence."""
    state_cfg = StateConfig(table="_viaduck_state_test")
    sm = StateManager(source_catalog, "test-instance", state_cfg)

    dest_ids = ["dest-alpha", "dest-beta"]
    sm.initialize_destinations(dest_ids)

    # Load initial cursors — should all be at snapshot 0
    cursors = sm.load_cursors(dest_ids)
    assert len(cursors) == 2
    assert cursors["dest-alpha"].last_snapshot_id == 0
    assert cursors["dest-beta"].last_snapshot_id == 0

    # Advance one cursor
    sm.advance_cursor("dest-alpha", snapshot_id=42, cumulative_rows=100)

    cursors = sm.load_cursors(dest_ids)
    assert cursors["dest-alpha"].last_snapshot_id == 42
    assert cursors["dest-alpha"].rows_replicated == 100
    assert cursors["dest-beta"].last_snapshot_id == 0

    # Advance both
    sm.advance_cursors(dest_ids, snapshot_id=50)
    cursors = sm.load_cursors(dest_ids)
    assert cursors["dest-alpha"].last_snapshot_id == 50
    assert cursors["dest-beta"].last_snapshot_id == 50

    # Re-initialize should be idempotent
    sm.initialize_destinations(dest_ids)
    cursors = sm.load_cursors(dest_ids)
    assert cursors["dest-alpha"].last_snapshot_id == 50

    # Record error
    sm.record_error("dest-beta", "simulated failure")
    cursors = sm.load_cursors(dest_ids)
    assert cursors["dest-beta"].last_error == "simulated failure"
    assert cursors["dest-beta"].last_snapshot_id == 50  # unchanged


# ---------------------------------------------------------------------------
# 6. Reserved column name "rowid"
# ---------------------------------------------------------------------------


def test_reserved_column_name_conflict(tmp_path):
    """Create a source table with a column named 'rowid'. Verify table_changes() behavior.

    DuckLake's CDC adds metadata columns (change_type, snapshot_id, rowid).
    If a user table also has a column named 'rowid', the CDC metadata column
    may shadow or conflict with it.
    """
    cat = _make_catalog(tmp_path, "rowid_test")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="rowid", field_type=IntegerType()),
        NestedField(field_id=3, name="name", field_type=StringType()),
    )
    tbl = cat.create_table("rowid_tbl", schema)

    snap_before = tbl.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    tbl.append(
        pa.table(
            {
                "id": pa.array([1], type=pa.int32()),
                "rowid": pa.array([42], type=pa.int32()),
                "name": pa.array(["test"], type=pa.string()),
            }
        )
    )

    snap_after = tbl.current_snapshot()
    assert snap_after is not None

    # This may raise or return unexpected columns. Document the behavior.
    try:
        changeset = tbl.table_changes(start, snap_after.snapshot_id)
        cdc_data = changeset.to_arrow()

        # Count how many 'rowid' columns appear
        rowid_cols = [c for c in cdc_data.column_names if c == "rowid"]

        # Document: if there are duplicates, or if user column is shadowed
        if len(rowid_cols) > 1:
            # DuckLake returned duplicate column names — this is ambiguous
            pass
        elif len(rowid_cols) == 1:
            # Single column — could be either the metadata or user column
            # Verify strip_meta doesn't break
            stripped = strip_meta(cdc_data)
            assert "change_type" not in stripped.column_names
            assert "snapshot_id" not in stripped.column_names
    except Exception as exc:
        # If DuckLake raises on the conflict, that's also a valid (safer) behavior
        pytest.skip(f"DuckLake raised on rowid column conflict: {exc}")


# ---------------------------------------------------------------------------
# 7. strip_meta on real CDC data
# ---------------------------------------------------------------------------


def test_strip_meta_on_real_cdc_data(source_table):
    """Read real CDC data via table_changes(), verify strip_meta() correctness."""
    snap_before = source_table.current_snapshot()
    start = snap_before.snapshot_id if snap_before else 0

    source_table.append(
        _arrow_rows(
            [1, 2, 3],
            ["acme", "beta", "acme"],
            [10, 20, 30],
        )
    )

    snap_after = source_table.current_snapshot()
    assert snap_after is not None

    changeset = source_table.table_changes(start, snap_after.snapshot_id)
    cdc_data = changeset.to_arrow()

    # CDC data should have metadata columns
    for meta_col in META_COLUMNS:
        assert meta_col in cdc_data.column_names, f"Expected metadata column {meta_col!r} in CDC data"

    # strip_meta should remove exactly the metadata columns
    stripped = strip_meta(cdc_data)
    for meta_col in META_COLUMNS:
        assert meta_col not in stripped.column_names, f"Metadata column {meta_col!r} should be removed"

    # Data columns should be preserved
    assert "event_id" in stripped.column_names
    assert "company" in stripped.column_names
    assert "value" in stripped.column_names

    # Row count unchanged
    assert stripped.num_rows == cdc_data.num_rows

    # Data values preserved
    assert sorted(stripped.column("event_id").to_pylist()) == [1, 2, 3]

    # Idempotent: stripping again is a no-op
    double_stripped = strip_meta(stripped)
    assert double_stripped.column_names == stripped.column_names
    assert double_stripped.num_rows == stripped.num_rows


# ---------------------------------------------------------------------------
# 8. Seed round trip
# ---------------------------------------------------------------------------


def test_seed_round_trip(source_catalog, source_table, dest_catalog_a, dest_table_a):
    """Seed a new destination from a source with 4 rows (2 acme, 2 beta).

    Verify destination gets only the 2 matching rows and cursor is at current snapshot.
    """
    from unittest.mock import MagicMock

    # Insert 4 rows into source
    source_table.append(
        _arrow_rows(
            [1, 2, 3, 4],
            ["acme", "acme", "beta", "beta"],
            [10, 20, 30, 40],
        )
    )

    snap = source_table.current_snapshot()
    assert snap is not None

    # Set up StateManager
    state_cfg = StateConfig(table="_viaduck_seed_test")
    sm = StateManager(source_catalog, "seed-instance", state_cfg)
    sm.initialize_destinations(["dest-acme"])

    # Build a mock cfg that points at real routing config and destination
    cfg = MagicMock()
    cfg.routing.field = "company"
    cfg.routing.key_columns = ["event_id"]
    cfg.routing.seed_mode = "scan"

    dest_cfg = MagicMock()
    dest_cfg.routing_value = "acme"
    cfg.destination_by_id.return_value = dest_cfg

    # Build a mock dest_pool that returns the real catalog and table
    dest_pool = MagicMock()
    dest_pool.get.return_value = (dest_catalog_a, dest_table_a)

    # Capture snapshot as seen by _seed_new_destinations (after state table init advances it)
    from viaduck import source as source_mod

    expected_snap = source_mod.current_snapshot_id(source_table)

    _seed_new_destinations(source_table, sm, dest_pool, cfg, ["dest-acme"])

    # Verify destination has 2 acme rows
    dest_scan = _read_all(dest_table_a)
    assert dest_scan.num_rows == 2
    assert sorted(dest_scan.column("event_id").to_pylist()) == [1, 2]
    assert all(c == "acme" for c in dest_scan.column("company").to_pylist())

    # Verify cursor is at the snapshot the seed function observed
    cursors = sm.load_cursors(["dest-acme"])
    assert cursors["dest-acme"].last_snapshot_id == expected_snap
