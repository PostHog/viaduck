"""Integration tests for the Postgres-backed StateManager.

Real Postgres via the session-scoped testcontainer (see conftest.py).
Replaces the mock-catalog unit tests that covered the DuckLake-backed
predecessor — the contract is identical (same public API, same
semantics), the store is not.
"""

from __future__ import annotations

import threading

import psycopg
import pytest

from viaduck.config import StateConfig
from viaduck.state import StateManager

pytestmark = pytest.mark.integration


@pytest.fixture()
def sm(pg_uri, state_table_name):
    mgr = StateManager(pg_uri, "i1", StateConfig(table=state_table_name))
    yield mgr
    mgr.close()


def test_initialize_and_load_round_trip(sm):
    sm.initialize_destinations(["d1", "d2"])
    cursors = sm.load_cursors(["d1", "d2"])
    assert set(cursors) == {"d1", "d2"}
    assert all(c.last_snapshot_id == 0 and c.rows_replicated == 0 for c in cursors.values())


def test_initialize_is_idempotent_and_preserves_progress(sm):
    sm.initialize_destinations(["d1"])
    sm.advance_cursor("d1", snapshot_id=7, cumulative_rows=42)
    sm.initialize_destinations(["d1", "d2"])  # d1 must NOT reset
    cursors = sm.load_cursors(["d1", "d2"])
    assert cursors["d1"].last_snapshot_id == 7
    assert cursors["d1"].rows_replicated == 42
    assert cursors["d2"].last_snapshot_id == 0


def test_load_cursors_filters_by_instance(sm, pg_uri, state_table_name):
    sm.initialize_destinations(["d1"])
    other = StateManager(pg_uri, "i2", StateConfig(table=state_table_name))
    try:
        other.initialize_destinations(["d1"])
        other.advance_cursor("d1", snapshot_id=99)
        assert sm.load_cursors(["d1"])["d1"].last_snapshot_id == 0
        assert other.load_cursors(["d1"])["d1"].last_snapshot_id == 99
    finally:
        other.close()


def test_load_cursors_filters_by_destination_ids(sm):
    sm.initialize_destinations(["d1", "d2", "d3"])
    cursors = sm.load_cursors(["d1", "d3"])
    assert set(cursors) == {"d1", "d3"}


def test_load_cursors_empty_input(sm):
    assert sm.load_cursors([]) == {}


def test_advance_cursor_preserves_rows_when_none(sm):
    sm.initialize_destinations(["d1"])
    sm.advance_cursor("d1", snapshot_id=5, cumulative_rows=10)
    sm.advance_cursor("d1", snapshot_id=6, cumulative_rows=None)
    cur = sm.load_cursors(["d1"])["d1"]
    assert cur.last_snapshot_id == 6
    assert cur.rows_replicated == 10


def test_advance_cursor_clears_error(sm):
    sm.initialize_destinations(["d1"])
    sm.record_error("d1", "boom")
    assert sm.load_cursors(["d1"])["d1"].last_error == "boom"
    sm.advance_cursor("d1", snapshot_id=3)
    assert sm.load_cursors(["d1"])["d1"].last_error is None


def test_advance_cursors_batch_preserves_rows_and_clears_errors(sm):
    sm.initialize_destinations(["d1", "d2"])
    sm.advance_cursor("d1", snapshot_id=2, cumulative_rows=20)
    sm.record_error("d2", "boom")
    sm.advance_cursors(["d1", "d2"], snapshot_id=9)
    cursors = sm.load_cursors(["d1", "d2"])
    assert cursors["d1"].last_snapshot_id == 9
    assert cursors["d1"].rows_replicated == 20  # preserved through batch advance
    assert cursors["d2"].last_snapshot_id == 9
    assert cursors["d2"].last_error is None


def test_record_error_keeps_cursor_and_is_silent_on_missing(sm):
    sm.initialize_destinations(["d1"])
    sm.advance_cursor("d1", snapshot_id=4, cumulative_rows=8)
    sm.record_error("d1", "write failed")
    cur = sm.load_cursors(["d1"])["d1"]
    assert cur.last_snapshot_id == 4
    assert cur.rows_replicated == 8
    assert cur.last_error == "write failed"
    # No row for d-missing: must not raise, must not create a row.
    sm.record_error("d-missing", "whatever")
    assert "d-missing" not in sm.load_cursors(["d-missing"])


def test_reconnects_after_connection_loss(sm):
    sm.initialize_destinations(["d1"])
    # Kill the connection out from under the manager; next call must
    # reconnect transparently (the _run retry path).
    sm._conn.close()
    sm.advance_cursor("d1", snapshot_id=11)
    assert sm.load_cursors(["d1"])["d1"].last_snapshot_id == 11


def test_unsafe_table_name_rejected(pg_uri):
    with pytest.raises(ValueError, match="unsafe"):
        StateManager(pg_uri, "i1", StateConfig(table="state; DROP TABLE x"))


def test_concurrent_advances_no_lost_updates(sm, pg_uri, state_table_name):
    """Cursor writes will come from flush worker threads (M3). Hammer the
    lock + reconnect path from several threads and verify every
    destination lands on its final snapshot with no torn state."""
    dest_ids = [f"d{i}" for i in range(8)]
    sm.initialize_destinations(dest_ids)
    errors: list[Exception] = []

    def _worker(dest_id: str):
        try:
            for snap in range(1, 26):
                sm.advance_cursor(dest_id, snapshot_id=snap, cumulative_rows=snap * 10)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(d,)) for d in dest_ids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    cursors = sm.load_cursors(dest_ids)
    assert all(cursors[d].last_snapshot_id == 25 and cursors[d].rows_replicated == 250 for d in dest_ids)


def test_state_rows_visible_via_plain_sql(sm, pg_uri, state_table_name):
    """The store is intentionally inspectable with psql — lock the shape,
    including its home in the dedicated `viaduck` schema (never the
    ducklake catalog's namespace)."""
    sm.initialize_destinations(["d1"])
    sm.advance_cursor("d1", snapshot_id=12, cumulative_rows=3)
    with psycopg.connect(pg_uri) as conn:
        row = conn.execute(
            f"SELECT last_snapshot_id, rows_replicated, last_error FROM viaduck.{state_table_name} "
            "WHERE destination_id = 'd1' AND instance_id = 'i1'"
        ).fetchone()
    assert row == (12, 3, None)


def test_stale_advance_is_dropped(sm):
    """Monotonicity guard: a stale ack (lower snapshot) must not move the
    cursor backwards nor clobber rows_replicated — defense-in-depth under
    M3's concurrent flush workers."""
    sm.initialize_destinations(["d1"])
    sm.advance_cursor("d1", snapshot_id=10, cumulative_rows=100)
    sm.advance_cursor("d1", snapshot_id=5, cumulative_rows=50)  # stale
    cur = sm.load_cursors(["d1"])["d1"]
    assert cur.last_snapshot_id == 10
    assert cur.rows_replicated == 100
    # Equal snapshot is allowed (idempotent re-ack).
    sm.advance_cursor("d1", snapshot_id=10, cumulative_rows=110)
    assert sm.load_cursors(["d1"])["d1"].rows_replicated == 110


def test_stale_batch_advance_is_dropped(sm):
    sm.initialize_destinations(["d1", "d2"])
    sm.advance_cursor("d1", snapshot_id=20)
    sm.advance_cursors(["d1", "d2"], snapshot_id=8)  # stale for d1, fresh for d2
    cursors = sm.load_cursors(["d1", "d2"])
    assert cursors["d1"].last_snapshot_id == 20
    assert cursors["d2"].last_snapshot_id == 8


def test_advance_without_initialize_creates_row(sm):
    """The INSERT arm: advancing a never-initialized destination creates its
    row (cumulative_rows None -> 0), matching the predecessor's
    delete+insert behavior."""
    sm.advance_cursor("d-fresh", snapshot_id=3, cumulative_rows=None)
    cur = sm.load_cursors(["d-fresh"])["d-fresh"]
    assert cur.last_snapshot_id == 3
    assert cur.rows_replicated == 0


def test_reconnect_after_backend_killed_mid_session(sm, pg_uri):
    """Kill the manager's backend server-side (not a clean local close) —
    the next op must hit OperationalError and recover via the retry. All
    statements are idempotent under retry-after-partial-apply."""
    sm.initialize_destinations(["d1"])
    with psycopg.connect(pg_uri, autocommit=True) as admin:
        admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid() AND application_name = '' AND state = 'idle'"
        )
    sm.advance_cursor("d1", snapshot_id=21)
    assert sm.load_cursors(["d1"])["d1"].last_snapshot_id == 21


def test_concurrent_create_table_race(pg_uri, state_table_name):
    """Two instances bootstrapping the same table concurrently must both
    survive (IF NOT EXISTS can still raise a unique violation in one)."""
    managers = [StateManager(pg_uri, f"i{n}", StateConfig(table=state_table_name)) for n in range(4)]
    errors: list[Exception] = []

    def _boot(mgr, n):
        try:
            mgr.initialize_destinations([f"d{n}"])
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=_boot, args=(m, n)) for n, m in enumerate(managers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for m in managers:
        m.close()
    assert not errors


def test_ensure_table_tolerates_duplicate_object_race(pg_uri, state_table_name, monkeypatch):
    """Deterministic pin for the CI flake (2026-08-29): PG17's concurrent
    CREATE TABLE can raise DuplicateObject (the implicit pg_type/composite
    insert loses the race), not just DuplicateTable/UniqueViolation. The
    loser of a boot race must survive all three error classes."""
    sm = StateManager(pg_uri, "i_dupobj", StateConfig(table=state_table_name))
    try:
        conn = sm._connection()
        real_execute = conn.execute
        state = {"raised": False}

        def flaky_execute(stmt, *args, **kwargs):
            if not state["raised"] and "CREATE TABLE" in stmt:
                state["raised"] = True
                raise psycopg.errors.DuplicateObject(f'type "{state_table_name}" already exists')
            return real_execute(stmt, *args, **kwargs)

        monkeypatch.setattr(conn, "execute", flaky_execute)
        sm._table_ensured = False
        sm._ensure_table(conn)  # must not raise
        assert sm._table_ensured
        assert state["raised"]
    finally:
        sm.close()


# ---------------------------------------------------------------------------
# Destination lifecycle (viaduck/lifecycle.py semantics live in the tracker;
# these pin the StateManager's storage contract against real Postgres)
# ---------------------------------------------------------------------------


def test_lifecycle_round_trip_and_isolation(sm):
    sm.initialize_destinations(["d1"])
    # Absent rows: empty result (tracker normalizes absent -> active).
    assert sm.load_lifecycle_states(["d1", "d2"]) == {}

    sm.set_lifecycle_state("d1", "paused", reason="broken catalog", updated_by="test")
    assert sm.load_lifecycle_states(["d1"]) == {"d1": "paused"}
    rows = sm.load_lifecycle_rows(["d1"])
    assert rows["d1"]["state"] == "paused"
    assert rows["d1"]["reason"] == "broken catalog"
    assert rows["d1"]["updated_by"] == "test"
    assert rows["d1"]["updated_at"] is not None

    # Update in place (upsert path).
    sm.set_lifecycle_state("d1", "active", reason="fixed", updated_by="test2")
    assert sm.load_lifecycle_states(["d1"]) == {"d1": "active"}


def test_lifecycle_table_is_per_pipeline(pg_uri, state_table_name):
    # The lifecycle table derives from the cursor table name — two
    # pipelines with colliding destination ids must not share operator
    # intent (review finding: shared hard-coded table let one pipeline's
    # pause hit the other).
    a = StateManager(pg_uri, "i1", StateConfig(table=state_table_name))
    b = StateManager(pg_uri, "i1", StateConfig(table=state_table_name + "_other"))
    try:
        a.initialize_destinations(["d1"])
        b.initialize_destinations(["d1"])
        a.set_lifecycle_state("d1", "paused", reason="pipeline A only", updated_by="test")
        assert a.load_lifecycle_states(["d1"]) == {"d1": "paused"}
        assert b.load_lifecycle_states(["d1"]) == {}
    finally:
        a.close()
        b.close()


def test_lifecycle_check_constraint_rejects_unknown_state(sm, pg_uri, state_table_name):
    sm.initialize_destinations(["d1"])
    # Direct SQL (bypassing the code-level guard): the DB CHECK holds the
    # vocabulary, which is what makes the tracker's unknown-state paused
    # fallback forward-compat-only rather than a live path.
    with psycopg.connect(pg_uri, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                f"INSERT INTO viaduck.{state_table_name}_lifecycle "
                "(destination_id, state, updated_at) VALUES ('d1', 'frobnicated', now())"
            )


def test_retirement_severs_cursor_rows_for_all_instances(pg_uri, state_table_name):
    # Two instances own rows for the same destination (partitioning drift
    # over time); retirement is per-destination and must sever both, so a
    # re-add seeds fresh regardless of which instance picks it up.
    i1 = StateManager(pg_uri, "i1", StateConfig(table=state_table_name))
    i2 = StateManager(pg_uri, "i2", StateConfig(table=state_table_name))
    try:
        i1.initialize_destinations(["d1"])
        i2.initialize_destinations(["d1"])
        i1.advance_cursor("d1", snapshot_id=9, cumulative_rows=10)

        deleted = i1.delete_destination_state("d1")
        assert deleted == 2
        assert i1.load_cursors(["d1"]) == {}
        assert i2.load_cursors(["d1"]) == {}
        # Idempotent (the per-cycle resurrect-race sweep).
        assert i1.delete_destination_state("d1") == 0
        # Re-add: initialize creates a FRESH cursor-0 row -> re-seed path.
        i1.initialize_destinations(["d1"], initial_snapshot_id=0)
        assert i1.load_cursors(["d1"])["d1"].last_snapshot_id == 0
    finally:
        i1.close()
        i2.close()
