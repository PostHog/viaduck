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
    """The store is intentionally inspectable with psql — lock the shape."""
    sm.initialize_destinations(["d1"])
    sm.advance_cursor("d1", snapshot_id=12, cumulative_rows=3)
    with psycopg.connect(pg_uri) as conn:
        row = conn.execute(
            f"SELECT last_snapshot_id, rows_replicated, last_error FROM {state_table_name} "
            "WHERE destination_id = 'd1' AND instance_id = 'i1'"
        ).fetchone()
    assert row == (12, 3, None)
