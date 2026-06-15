"""Integration tests for seed_mode: earliest / latest / scan transitions.

Uses real pyducklake catalogs (local DuckDB files) + real Postgres-backed
StateManager (testcontainers). This exercises the full initialization path
including snapshot queries that MagicMock-based unit tests cannot validate.

Scenarios covered:
  A. Fresh destination — latest: cursor initializes at current head
  B. Fresh destination — earliest: cursor initializes at MIN-1, CDC starts at MIN
  B2. latest: rows before startup excluded from CDC
  B3. earliest: CDC from MIN-1 cursor reaches all user rows
  B4. _initial_snapshot_id("earliest") = MIN - 1
  B5. cursor=-1 (MIN=0 catalog) safe to advance from (end-to-end)
  C. Fresh destination — scan: cursor initializes at 0
  D. Mode switch: latest → scan (existing non-zero cursor, seed skipped)
  E. Mode switch: scan → latest (existing cursor preserved, no jump to head)
  F. Mode switch: earliest → latest (existing cursor preserved)
  G. Stale cursor (0) + latest (documented known behavior: cursor NOT reset)
  G2. Stale cursor (0) + earliest (same DO NOTHING, different risk profile)
  H. earliest_snapshot_id < current_snapshot_id after several inserts
  H2. earliest_snapshot_id stable across inserts
  H3. earliest_snapshot_id = 0 after create_table (not None on real catalog)
  I. Multiple destinations: existing cursor preserved, new destination initialized
  J. current_snapshot_id tracks head after inserts
"""

from __future__ import annotations

import os

import pyarrow as pa
import pytest
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, NestedField

from viaduck import metrics
from viaduck.config import StateConfig
from viaduck.main import _initial_snapshot_id
from viaduck.source import current_snapshot_id, earliest_snapshot_id
from viaduck.state import StateManager

pytestmark = pytest.mark.integration

_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="team_id", field_type=IntegerType(), required=True),
)


def setup_module():
    metrics.init("seed_mode_integration")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_catalog(tmp_path, name: str) -> Catalog:
    base = tmp_path / name
    os.makedirs(base / "data", exist_ok=True)
    return Catalog(name, str(base / "meta.duckdb"), data_path=str(base / "data"))


def _insert(table, rows: list[tuple[int, int]]) -> int:
    """Insert rows and return the resulting snapshot_id."""
    arrow = pa.table(
        {"id": [r[0] for r in rows], "team_id": [r[1] for r in rows]},
        schema=pa.schema([pa.field("id", pa.int32()), pa.field("team_id", pa.int32())]),
    )
    table.append(arrow)
    snap = table.current_snapshot()
    assert snap is not None
    return snap.snapshot_id


@pytest.fixture()
def src(tmp_path):
    cat = _make_catalog(tmp_path, "src")
    tbl = cat.create_table("events", _SCHEMA)
    yield tbl
    cat.close()


@pytest.fixture()
def sm(pg_uri, state_table_name):
    mgr = StateManager(pg_uri, "i0", StateConfig(table=state_table_name))
    yield mgr
    mgr.close()


# ---------------------------------------------------------------------------
# A. latest — fresh destination initializes at current head
# ---------------------------------------------------------------------------


def test_latest_fresh_destination_starts_at_head(src, sm):
    _insert(src, [(1, 1), (2, 1)])
    head_before = _insert(src, [(3, 2)])

    initial = _initial_snapshot_id("latest", src)
    assert initial == head_before

    sm.initialize_destinations(["d1"], initial_snapshot_id=initial)
    cursor = sm.load_cursors(["d1"])["d1"].last_snapshot_id
    assert cursor == head_before


def test_latest_events_before_startup_not_replicated(src, sm):
    """Rows inserted before initialization are invisible to latest-mode CDC."""
    from viaduck.source import read_cdc

    _insert(src, [(1, 1)])
    head = _insert(src, [(2, 2)])

    sm.initialize_destinations(["d1"], initial_snapshot_id=head)

    # Insert more rows after initialization
    snap_after = _insert(src, [(3, 1), (4, 2)])

    changeset = read_cdc(src, after_snapshot=head, end_snapshot=snap_after)
    assert len(changeset) == 2
    assert set(changeset["id"].to_pylist()) == {3, 4}


# ---------------------------------------------------------------------------
# B. earliest — fresh destination starts at MIN, replays from first snapshot
# ---------------------------------------------------------------------------


def test_earliest_fresh_destination_starts_at_min_minus_one(src, sm):
    _insert(src, [(1, 1)])
    _insert(src, [(2, 2)])

    min_snap = earliest_snapshot_id(src)
    assert min_snap is not None

    initial = _initial_snapshot_id("earliest", src)
    assert initial == min_snap - 1

    sm.initialize_destinations(["d1"], initial_snapshot_id=initial)
    cursor = sm.load_cursors(["d1"])["d1"].last_snapshot_id
    assert cursor == min_snap - 1


def test_earliest_cdc_read_includes_min_snapshot(src):
    """CDC from cursor=MIN-1 correctly starts at MIN (read_cdc adds +1)."""
    from viaduck.source import read_cdc

    # Insert rows into already-initialized catalog (which has snapshots 0,1 from create)
    _insert(src, [(1, 1)])
    snap_b = _insert(src, [(2, 2)])

    min_snap = earliest_snapshot_id(src)
    assert min_snap is not None
    initial = min_snap - 1

    # CDC from cursor=MIN-1 should see all user-inserted rows
    changeset = read_cdc(src, after_snapshot=initial, end_snapshot=snap_b)
    assert set(changeset["id"].to_pylist()) == {1, 2}


def test_earliest_negative_cursor_safe_to_advance(src, sm):
    """When MIN=0, cursor is stored as -1. Advancing from -1 to a real snapshot
    must not be blocked by the monotonicity guard (which requires new >= old)."""
    min_snap = earliest_snapshot_id(src)
    assert min_snap == 0, "local pyducklake starts at 0; test relies on MIN=0"

    initial = _initial_snapshot_id("earliest", src)
    assert initial == -1

    sm.initialize_destinations(["d1"], initial_snapshot_id=initial)
    assert sm.load_cursors(["d1"])["d1"].last_snapshot_id == -1

    snap = _insert(src, [(1, 1)])
    sm.advance_cursor("d1", snapshot_id=snap)
    assert sm.load_cursors(["d1"])["d1"].last_snapshot_id == snap


def test_earliest_initial_snapshot_is_min_minus_one(src):
    """_initial_snapshot_id('earliest') always returns MIN(snapshot_id) - 1."""
    _insert(src, [(1, 1)])
    _insert(src, [(2, 2)])
    min_snap = earliest_snapshot_id(src)
    assert _initial_snapshot_id("earliest", src) == min_snap - 1


# ---------------------------------------------------------------------------
# C. scan — initializes at 0, _seed_new_destinations is caller's job
# ---------------------------------------------------------------------------


def test_scan_initial_snapshot_id_is_zero(src):
    _insert(src, [(1, 1)])
    assert _initial_snapshot_id("scan", src) == 0


# ---------------------------------------------------------------------------
# D. Mode switch: latest → scan (existing cursor, seed scan skipped)
# ---------------------------------------------------------------------------


def test_latest_to_scan_existing_cursor_preserved(src, sm):
    """A destination that ran in latest mode keeps its cursor when config
    switches to scan. _seed_new_destinations checks cursor == 0 to skip
    already-running destinations."""
    _insert(src, [(1, 1)])
    head = _insert(src, [(2, 2)])

    # First boot: latest
    initial = _initial_snapshot_id("latest", src)
    sm.initialize_destinations(["d1"], initial_snapshot_id=initial)
    sm.advance_cursor("d1", snapshot_id=head)

    # Config changes to scan; initialize_destinations is called again on restart
    # ON CONFLICT DO NOTHING — cursor must NOT revert to 0
    sm.initialize_destinations(["d1"], initial_snapshot_id=0)
    cursor = sm.load_cursors(["d1"])["d1"].last_snapshot_id
    assert cursor == head, "existing cursor must not be overwritten by scan re-init"


# ---------------------------------------------------------------------------
# E. Mode switch: scan → latest (existing cursor preserved)
# ---------------------------------------------------------------------------


def test_scan_to_latest_existing_cursor_preserved(src, sm):
    """Switching from scan to latest on a destination already at cursor X
    does not jump the cursor to head — it stays at X and continues CDC."""
    head = _insert(src, [(1, 1)])

    # Simulate scan mode: cursor initialized and advanced to current head
    sm.initialize_destinations(["d1"], initial_snapshot_id=0)
    sm.advance_cursor("d1", snapshot_id=head)

    # Config changes to latest; restart calls initialize with head
    _insert(src, [(2, 2)])
    latest_initial = _initial_snapshot_id("latest", src)
    sm.initialize_destinations(["d1"], initial_snapshot_id=latest_initial)

    cursor = sm.load_cursors(["d1"])["d1"].last_snapshot_id
    assert cursor == head, "cursor should remain at scan-mode position, not jump to new head"


# ---------------------------------------------------------------------------
# F. Mode switch: earliest → latest (existing cursor preserved)
# ---------------------------------------------------------------------------


def test_earliest_to_latest_existing_cursor_preserved(src, sm):
    snap1 = _insert(src, [(1, 1)])

    earliest_initial = _initial_snapshot_id("earliest", src)
    sm.initialize_destinations(["d1"], initial_snapshot_id=earliest_initial)
    sm.advance_cursor("d1", snapshot_id=snap1)

    # Config changes to latest
    _insert(src, [(2, 2)])
    latest_initial = _initial_snapshot_id("latest", src)
    sm.initialize_destinations(["d1"], initial_snapshot_id=latest_initial)

    cursor = sm.load_cursors(["d1"])["d1"].last_snapshot_id
    assert cursor == snap1, "cursor preserved at earliest-mode position after mode switch"


# ---------------------------------------------------------------------------
# G. Stale cursor (0) + latest — known limitation: cursor NOT reset
# This test documents the current behavior. The fix (DO UPDATE WHERE
# last_snapshot_id = 0) is tracked in FOLLOWUP.md.
# ---------------------------------------------------------------------------


def test_stale_zero_cursor_not_reset_by_latest(src, sm):
    """A destination stuck at cursor 0 (e.g. from a failed cdc_replay run)
    is NOT reset to latest by initialize_destinations due to ON CONFLICT DO
    NOTHING semantics. This is a known limitation documented in FOLLOWUP.md."""
    _insert(src, [(1, 1)])
    head = _insert(src, [(2, 2)])

    # Simulate stale state: destination row exists at cursor 0
    sm.initialize_destinations(["d1"], initial_snapshot_id=0)
    assert sm.load_cursors(["d1"])["d1"].last_snapshot_id == 0

    # Config is now latest; restart calls initialize with head
    sm.initialize_destinations(["d1"], initial_snapshot_id=head)

    cursor = sm.load_cursors(["d1"])["d1"].last_snapshot_id
    # Current behavior: cursor stays at 0 (DO NOTHING)
    assert cursor == 0, "Known limitation: stale cursor-0 is not reset to latest. See FOLLOWUP.md for the planned fix."


def test_stale_zero_cursor_not_reset_by_earliest(src, sm):
    """Same DO NOTHING behavior for earliest as for latest, but the risk profile
    differs: earliest passes initial=-1 (not head), so the stale cursor-0 means
    CDC will attempt to read from snapshot 1 onward — potentially replaying a
    large history or crashing on an expired catalog. Documented in FOLLOWUP.md."""
    _insert(src, [(1, 1)])

    # Simulate stale state at 0
    sm.initialize_destinations(["d1"], initial_snapshot_id=0)
    assert sm.load_cursors(["d1"])["d1"].last_snapshot_id == 0

    # Config switches to earliest; initial=-1 (MIN=0), but DO NOTHING preserves 0
    earliest_initial = _initial_snapshot_id("earliest", src)
    sm.initialize_destinations(["d1"], initial_snapshot_id=earliest_initial)

    cursor = sm.load_cursors(["d1"])["d1"].last_snapshot_id
    assert cursor == 0, "Known limitation: stale cursor-0 not reset by earliest. See FOLLOWUP.md for the planned fix."


# ---------------------------------------------------------------------------
# I. Multiple destinations: new destination gets initial_snapshot_id,
#    existing destination cursor is preserved (DO NOTHING)
# ---------------------------------------------------------------------------


def test_multi_destination_new_gets_initial_existing_preserved(src, sm):
    """initialize_destinations with mixed existing/new destinations:
    existing cursor must not be overwritten; new destination gets initial_snapshot_id."""
    snap1 = _insert(src, [(1, 1)])

    # Boot d1 as latest, advance it
    initial_latest = _initial_snapshot_id("latest", src)
    sm.initialize_destinations(["d1"], initial_snapshot_id=initial_latest)
    sm.advance_cursor("d1", snapshot_id=snap1)

    # New snapshot arrives; d2 is a new destination
    snap2 = _insert(src, [(2, 2)])
    new_latest = _initial_snapshot_id("latest", src)
    assert new_latest == snap2

    # Initialize both together — d1 must keep snap1, d2 gets snap2
    sm.initialize_destinations(["d1", "d2"], initial_snapshot_id=new_latest)

    cursors = sm.load_cursors(["d1", "d2"])
    assert cursors["d1"].last_snapshot_id == snap1, "d1 cursor must not be overwritten"
    assert cursors["d2"].last_snapshot_id == snap2, "d2 must be initialized at latest"


# ---------------------------------------------------------------------------
# H. earliest_snapshot_id — real catalog with multiple snapshots
# ---------------------------------------------------------------------------


def test_earliest_snapshot_id_is_less_than_latest(src):
    """earliest_snapshot_id < current_snapshot_id after several inserts."""
    _insert(src, [(1, 1)])
    _insert(src, [(2, 2)])
    _insert(src, [(3, 3)])

    earliest = earliest_snapshot_id(src)
    latest = current_snapshot_id(src)
    assert earliest is not None
    assert latest is not None
    assert earliest < latest


def test_earliest_snapshot_id_stable_across_inserts(src):
    """Inserting more rows moves current head but not earliest snapshot."""
    first_earliest = earliest_snapshot_id(src)
    _insert(src, [(1, 1)])
    snap2 = _insert(src, [(2, 2)])
    assert earliest_snapshot_id(src) == first_earliest
    # Confirm head actually moved so the stability assertion isn't vacuous
    assert snap2 > first_earliest


# ---------------------------------------------------------------------------
# I. earliest_snapshot_id — None only when catalog has no snapshot rows
# (pyducklake creates snapshots 0+1 on create_table, so None is only
#  reachable via a mock; the real None path is tested in unit tests)
# ---------------------------------------------------------------------------


def test_earliest_snapshot_id_is_not_none_after_create(tmp_path):
    """pyducklake creates initial snapshots on create_table; earliest is never
    None on a real catalog — the None fallback exists for robustness."""
    cat = _make_catalog(tmp_path, "notnone")
    try:
        tbl = cat.create_table("events", _SCHEMA)
        result = earliest_snapshot_id(tbl)
        assert result is not None
        assert result == 0  # pyducklake local catalogs start at 0
    finally:
        cat.close()


# ---------------------------------------------------------------------------
# J. current_snapshot_id matches expected head after inserts
# ---------------------------------------------------------------------------


def test_current_snapshot_id_tracks_head(src):
    snap1 = _insert(src, [(1, 1)])
    assert current_snapshot_id(src) == snap1

    snap2 = _insert(src, [(2, 2)])
    assert current_snapshot_id(src) == snap2
