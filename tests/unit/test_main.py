"""Tests for main loop logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.apply import (
    _apply_changes,
    _build_delete_filter,
    _resolve_conflicts,
    _write_with_retry,
)
from viaduck.main import (
    _derive_dest_status,
    _fmt_duration,
    _group_by_cursor,
    _poll_cycle,
    _resolve_preimages,
    _scan_progress_suffix,
    _seed_new_destinations,
    _start_progress_heartbeat,
)
from viaduck.router import RoutingError


def setup_module():
    metrics.init("test")


# ---------------------------------------------------------------------------
# _group_by_cursor
# ---------------------------------------------------------------------------


def test_group_by_cursor_all_same():
    cursors = {"a": 10, "b": 10, "c": 10}
    groups = _group_by_cursor(cursors, ["a", "b", "c"])
    assert groups == {10: ["a", "b", "c"]}


def test_group_by_cursor_mixed():
    cursors = {"a": 10, "b": 5, "c": 10}
    groups = _group_by_cursor(cursors, ["a", "b", "c"])
    assert groups == {10: ["a", "c"], 5: ["b"]}


def test_group_by_cursor_missing_defaults_to_zero():
    cursors = {"a": 10}
    groups = _group_by_cursor(cursors, ["a", "b"])
    assert groups == {10: ["a"], 0: ["b"]}


def test_group_by_cursor_empty():
    groups = _group_by_cursor({}, [])
    assert groups == {}


# ---------------------------------------------------------------------------
# _write_with_retry
# ---------------------------------------------------------------------------


def test_write_with_retry_success_first_attempt():
    pool = MagicMock()
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    pool.get.return_value = (mock_catalog, mock_table)
    called = {}

    def op(catalog, table):
        called["catalog"] = catalog
        called["table"] = table

    _write_with_retry(pool, "dest-1", op)

    assert called["catalog"] is mock_catalog
    assert called["table"] is mock_table


def test_write_with_retry_retries_on_failure():
    pool = MagicMock()
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    pool.get.return_value = (mock_catalog, mock_table)
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise Exception("fail")

    with patch("viaduck.apply.time.sleep"):
        _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == 2
    pool.evict.assert_called_once_with("dest-1")


def test_write_with_retry_exhausted():
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    def op(catalog, table):
        raise Exception("persistent failure")

    with patch("viaduck.apply.time.sleep"):
        with pytest.raises(Exception, match="persistent failure"):
            _write_with_retry(pool, "dest-1", op)


def test_write_with_retry_logs_exception_message():
    """Retry should log the actual exception message."""
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("connection refused")

    with patch("viaduck.apply.time.sleep"), patch("viaduck.apply.log") as mock_log:
        _write_with_retry(pool, "dest-1", op)

    warning_call = mock_log.warning.call_args
    assert "connection refused" in str(warning_call)


# ---------------------------------------------------------------------------
# _poll_cycle helpers
# ---------------------------------------------------------------------------


def _make_cfg(dest_ids_and_rvs: list[tuple[str, str]]):
    """Create a mock config with given (dest_id, routing_value) pairs."""
    cfg = MagicMock()
    dests = []
    for did, rv in dest_ids_and_rvs:
        d = MagicMock()
        d.id = did
        d.routing_value = rv
        dests.append(d)
    cfg.destinations = dests

    def by_id(dest_id):
        for d in dests:
            if d.id == dest_id:
                return d
        raise KeyError(dest_id)

    cfg.destination_by_id = by_id
    return cfg


# ---------------------------------------------------------------------------
# _poll_cycle (buffered delivery: reads + routing land in the DeliveryManager;
# writes are worker-side and covered by tests/unit/test_delivery.py)
# ---------------------------------------------------------------------------


def _make_delivery(positions: dict[str, int]):
    """Mock DeliveryManager: position map + status snapshot for all dests."""
    from viaduck.delivery import DestDeliveryStatus

    delivery = MagicMock()
    delivery.positions.return_value = dict(positions)
    delivery.read_plan.return_value = {d: (snap, 0) for d, snap in positions.items()}
    delivery.should_pause_reads.return_value = False
    delivery.maybe_flush.return_value = 0
    delivery.status_snapshot.return_value = {
        d: DestDeliveryStatus(
            flushed_snapshot=snap,
            position_snapshot=snap,
            rows_replicated=0,
            last_error=None,
            buffer_rows=0,
            buffer_age_s=0.0,
            flushing=False,
        )
        for d, snap in positions.items()
    }
    return delivery


def test_poll_cycle_no_snapshots():
    """If source has no snapshots, no reads happen — but triggers are still
    evaluated (position-only persists may be due)."""
    delivery = _make_delivery({})
    router = MagicMock()
    cfg = _make_cfg([])

    with patch("viaduck.main.source.current_snapshot_id", return_value=None):
        _poll_cycle(MagicMock(), delivery, MagicMock(), router, cfg, [], {}, key_columns=[], full_cdc=False)

    delivery.read_plan.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_all_caught_up():
    """If every position is at the current snapshot, no CDC reads occur."""
    delivery = _make_delivery({"dest-1": 10})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with patch("viaduck.main.source.current_snapshot_id", return_value=10):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )

    router.build_filter_expr.assert_not_called()
    delivery.buffer.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_routes_and_buffers():
    """Read CDC, route, buffer (BufferRead) — no synchronous writes."""
    delivery = _make_delivery({"dest-1": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    arrow_data = pa.table({"company": ["quacksworth", "quacksworth"], "value": [10, 20]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )

    delivery.buffer.assert_called_once_with("dest-1", arrow_data, 10, epoch=0)
    delivery.advance_position.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_empty_changeset_advances_positions():
    """An empty CDC range advances in-memory positions (no PG write)."""
    delivery = _make_delivery({"dest-1": 5, "dest-2": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a"), ("dest-2", "b")])

    empty = pa.table({"company": pa.array([], type=pa.string())})
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=empty),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            full_cdc=False,
        )

    assert delivery.advance_position.call_count == 2
    delivery.advance_position.assert_any_call("dest-1", 10, epoch=0)
    delivery.advance_position.assert_any_call("dest-2", 10, epoch=0)
    delivery.buffer.assert_not_called()


def test_poll_cycle_routing_error_breaks_gracefully():
    """A routing failure stops reads for the cycle without buffering."""
    delivery = _make_delivery({"dest-1": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    arrow_data = pa.table({"other": ["x"]})
    router.split_and_count.side_effect = RoutingError("routing field missing")

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )

    delivery.buffer.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_advances_no_data_destinations():
    """Destinations in the group with no routed rows advance positions."""
    delivery = _make_delivery({"dest-1": 5, "dest-2": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a"), ("dest-2", "b")])

    arrow_data = pa.table({"company": ["a"], "value": [1]})
    router.split_and_count.return_value = ({"a": arrow_data}, 0)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            full_cdc=False,
        )

    delivery.buffer.assert_called_once_with("dest-1", arrow_data, 10, epoch=0)
    delivery.advance_position.assert_called_once_with("dest-2", 10, epoch=0)


def test_poll_cycle_pauses_reads_at_watermark():
    """When the global buffer watermark is hit with all flushes in flight,
    the cycle skips reads but still evaluates triggers."""
    delivery = _make_delivery({"dest-1": 5})
    delivery.should_pause_reads.return_value = True
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with patch("viaduck.main.source.current_snapshot_id", return_value=10):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )

    delivery.read_plan.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_snapshot_at_zero():
    """Source snapshot 0 with positions at 0: nothing to read, triggers run."""
    delivery = _make_delivery({"dest-1": 0})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with patch("viaduck.main.source.current_snapshot_id", return_value=0):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )

    router.build_filter_expr.assert_not_called()
    delivery.maybe_flush.assert_called_once()


# ---------------------------------------------------------------------------
# _resolve_preimages
# ---------------------------------------------------------------------------


def _cdc_table(rows, routing_field="company"):
    """Build a pyarrow table with CDC metadata columns from list of dicts."""
    if not rows:
        return pa.table(
            {
                routing_field: pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.int64()),
                "change_type": pa.array([], type=pa.string()),
                "snapshot_id": pa.array([], type=pa.int64()),
                "rowid": pa.array([], type=pa.int64()),
            }
        )
    cols = {}
    for key in rows[0]:
        cols[key] = [r[key] for r in rows]
    return pa.table(cols)


def test_resolve_preimages_same_tenant_drops():
    """Preimage with same routing value as postimage should be dropped."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "update_postimage"


def test_resolve_preimages_cross_tenant_converts_to_delete():
    """Different routing values: preimage becomes delete. Metric incremented."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with patch("viaduck.main.metrics.cdc_routing_mutations_total") as mock_metric:
        result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    types = result.column("change_type").to_pylist()
    assert types[0] == "delete"
    assert types[1] == "update_postimage"
    mock_metric.inc.assert_called_once()


def test_resolve_preimages_orphaned_converts_to_delete():
    """Preimage with no matching postimage becomes delete. Metric incremented."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    with patch("viaduck.main.metrics.cdc_orphaned_preimages_total") as mock_metric:
        result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    types = result.column("change_type").to_pylist()
    assert types[0] == "delete"
    assert types[1] == "insert"
    mock_metric.inc.assert_called_once()


def test_resolve_preimages_no_preimages():
    """Batch with no preimages should pass through unchanged."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    assert result.column("change_type").to_pylist() == ["insert", "delete"]


def test_resolve_preimages_mixed_same_and_cross():
    """Mix of same-tenant and cross-tenant updates."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "old", "value": 3, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 200},
            {"company": "new", "value": 4, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    # Same-tenant (rowid=100): preimage dropped -> postimage only
    # Cross-tenant (rowid=200): preimage -> delete, postimage kept
    assert result.num_rows == 3
    types = result.column("change_type").to_pylist()
    assert types == ["update_postimage", "delete", "update_postimage"]


def test_resolve_preimages_preserves_non_update_rows():
    """Inserts and deletes pass through unmodified."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    assert result.column("change_type").to_pylist() == ["insert", "delete"]
    assert result.column("value").to_pylist() == [1, 2]


def test_resolve_preimages_validates_key_columns_exist():
    """Missing key column should raise RoutingError."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with pytest.raises(RoutingError, match="Key column 'missing_col' not found"):
        _resolve_preimages(batch, "company", ["missing_col"])


# ---------------------------------------------------------------------------
# _resolve_conflicts
# ---------------------------------------------------------------------------


def test_resolve_conflicts_insert_delete_keeps_tombstone():
    """Same rowid insert + delete: the insert drops, the delete SURVIVES
    (tombstone). Against a destination that never saw the insert it is an
    idempotent no-op; against one that did (commit/cursor-gap replay) it
    is the only event that can remove the phantom. Both metrics fire."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with (
        patch("viaduck.main.metrics.cdc_conflicts_resolved_total") as mock_conflicts,
        patch("viaduck.main.metrics.cdc_tombstones_emitted_total") as mock_tombstones,
    ):
        result = _resolve_conflicts(batch)
    assert result.column("change_type").to_pylist() == ["delete"]
    assert result.column("rowid").to_pylist() == [100]
    mock_conflicts.inc.assert_called_once_with(1)
    mock_tombstones.inc.assert_called_once_with(1)


def test_resolve_conflicts_update_delete_keeps_delete():
    """Same rowid postimage + delete: postimage dropped, delete kept."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "delete"


def test_resolve_conflicts_no_conflicts():
    """No overlapping rowids: unchanged."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 2


def test_resolve_conflicts_mixed_pairs_and_plain():
    """A paired rowid keeps only its tombstone delete; unrelated rows pass."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    by_rowid = dict(zip(result.column("rowid").to_pylist(), result.column("change_type").to_pylist()))
    assert by_rowid == {100: "delete", 200: "insert"}


def test_resolve_conflicts_empty_batch():
    """Empty batch should return empty."""
    batch = _cdc_table([])
    result = _resolve_conflicts(batch)
    assert result.num_rows == 0


def test_resolve_conflicts_insert_update_delete_sequence():
    """Same rowid: insert + postimage + delete. Insert and postimage drop;
    the delete survives as the tombstone (matches spec Phase2)."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.column("change_type").to_pylist() == ["delete"]


def test_resolve_conflicts_duplicate_keys_last_wins():
    """Multiple inserts for same rowid: verify no crash."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    # No crash expected, both rows preserved (no delete to trigger cancellation)
    result = _resolve_conflicts(batch)
    assert result.num_rows == 2


def test_resolve_conflicts_same_key_different_rowid_no_cancel():
    """Same key_columns value but different rowid should NOT cancel."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    # Different rowids, no cancellation
    assert result.num_rows == 2


def test_resolve_conflicts_uses_rowid_not_just_key():
    """Explicit test that rowid is used for matching, not key column values."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 300},
        ]
    )
    result = _resolve_conflicts(batch)
    # rowid 100: insert drops, tombstone delete survives. rowid 300: insert preserved.
    by_rowid = dict(zip(result.column("rowid").to_pylist(), result.column("change_type").to_pylist()))
    assert by_rowid == {100: "delete", 300: "insert"}


def test_resolve_conflicts_insert_postimage_same_rowid_drops_insert():
    """Same rowid with insert + update_postimage: drop the insert, keep postimage.

    Repro for the flaky CI failure in tests/integration::test_full_cdc_update_round_trip.
    When the source's table_changes range covers an INSERT and a later same-rowid
    UPDATE (because the upsert reused the rowid rather than delete+insert), Phase 1
    drops the same-tenant preimage, leaving INSERT(rowid=R, value=old) and
    UPDATE_POSTIMAGE(rowid=R, value=new) for the same key. Phase 3 _apply_changes
    feeds both rows into a single tbl.upsert(join_cols=...) — which has undefined
    ordering for duplicate join keys, so the older value can win non-deterministically.

    Phase 2 must collapse this pair: the postimage represents the newer state.
    """
    batch = _cdc_table(
        [
            {"company": "acme", "value": 10, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 999, "change_type": "update_postimage", "snapshot_id": 2, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "update_postimage"
    assert result.column("value")[0].as_py() == 999


# ---------------------------------------------------------------------------
# _build_delete_filter
# ---------------------------------------------------------------------------


def test_build_delete_filter_single_key():
    """Single key column with multiple values produces IN expression."""
    rows = pa.table(
        {
            "id": [1, 2, 3],
            "change_type": ["delete", "delete", "delete"],
            "snapshot_id": [1, 1, 1],
            "rowid": [10, 20, 30],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "IN" in sql
    assert "1" in sql
    assert "2" in sql
    assert "3" in sql


def test_build_delete_filter_composite_key():
    """Composite key produces OR(AND(...), AND(...))."""
    rows = pa.table(
        {
            "a": [1, 2],
            "b": ["x", "y"],
            "change_type": ["delete", "delete"],
            "snapshot_id": [1, 1],
            "rowid": [10, 20],
        }
    )
    sql = _build_delete_filter(rows, ["a", "b"])
    assert "OR" in sql or "AND" in sql


def test_build_delete_filter_single_row():
    """Single row produces simple equality."""
    rows = pa.table(
        {
            "id": [42],
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "42" in sql


def test_build_delete_filter_null_in_key():
    """NULL value in key should use IS NULL."""
    rows = pa.table(
        {
            "id": pa.array([None, 1], type=pa.int64()),
            "change_type": ["delete", "delete"],
            "snapshot_id": [1, 1],
            "rowid": [10, 20],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "NULL" in sql.upper()


def test_build_delete_filter_all_null_composite_key():
    """All NULLs in composite key."""
    rows = pa.table(
        {
            "a": pa.array([None], type=pa.int64()),
            "b": pa.array([None], type=pa.string()),
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    sql = _build_delete_filter(rows, ["a", "b"])
    assert "NULL" in sql.upper()


def test_build_delete_filter_mixed_null_and_values():
    """Mix of NULL and non-NULL for single key column."""
    rows = pa.table(
        {
            "id": pa.array([None, 5, None, 7], type=pa.int64()),
            "change_type": ["delete"] * 4,
            "snapshot_id": [1] * 4,
            "rowid": [10, 20, 30, 40],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "NULL" in sql.upper()
    assert "IN" in sql or "5" in sql


def test_build_delete_filter_missing_key_column_raises():
    """Missing key column should raise RoutingError."""
    rows = pa.table(
        {
            "id": [1],
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    with pytest.raises(RoutingError, match="Key column 'missing' not found"):
        _build_delete_filter(rows, ["missing"])


# ---------------------------------------------------------------------------
# _apply_changes
# ---------------------------------------------------------------------------


def _mock_catalog_and_table():
    """Create mock catalog with transaction context manager and table."""
    catalog = MagicMock()
    dest_table = MagicMock()
    dest_table.identifier = "test_table"
    txn = MagicMock()
    txn_table = MagicMock()
    # Mock upsert to return UpsertResult-like object
    upsert_result = MagicMock()
    upsert_result.rows_updated = 0
    upsert_result.rows_inserted = 0
    txn_table.upsert.return_value = upsert_result
    txn.load_table.return_value = txn_table
    catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    return catalog, dest_table, txn, txn_table


def test_apply_changes_inserts_only():
    """Only inserts: upsert called, no delete.

    These _apply_changes tests key on "value" (unique per fixture row), not
    "company" (constant): with a realistic key, duplicate-key rows collapse
    via Winner(k) and the pass-through counts asserted here would change.
    Same-key behavior is covered by the Winner(k) tests below."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["value"])
    assert counts["upserted"] == 2
    assert counts["deleted"] == 0
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["value"]
    txn_table.delete.assert_not_called()


def test_apply_changes_deletes_only():
    """Only deletes: delete called, no upsert."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["deleted"] == 1
    assert counts["upserted"] == 0
    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_not_called()


def test_apply_changes_updates_only():
    """Postimages should be upserted."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 5, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 1
    assert counts["deleted"] == 0
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["company"]


def test_apply_changes_mixed():
    """Mixed deletes and inserts: both called, correct counts."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["value"])
    assert counts["deleted"] == 1
    assert counts["upserted"] == 2
    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["value"]


def test_apply_changes_empty():
    """Empty batch: no-op, no transaction."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table([])
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts == {"deleted": 0, "upserted": 0, "upsert_matched": 0}
    catalog.begin_transaction.assert_not_called()


def test_apply_changes_uses_transaction():
    """begin_transaction should be called."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    _apply_changes(catalog, dest_table, batch, ["company"])
    catalog.begin_transaction.assert_called_once()


def test_apply_changes_transaction_rollback_on_failure():
    """Exception inside transaction should propagate (context manager handles rollback)."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    txn_table.upsert.side_effect = Exception("write error")
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with pytest.raises(Exception, match="write error"):
        _apply_changes(catalog, dest_table, batch, ["company"])


def test_apply_changes_strips_metadata():
    """change_type, snapshot_id, rowid should be removed before write."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    _apply_changes(catalog, dest_table, batch, ["company"])
    upsert_call = txn_table.upsert.call_args
    written_table = upsert_call[0][0]
    assert "change_type" not in written_table.column_names
    assert "snapshot_id" not in written_table.column_names
    assert "rowid" not in written_table.column_names
    assert "company" in written_table.column_names
    assert "value" in written_table.column_names


# ---------------------------------------------------------------------------
# _poll_cycle (full CDC mode)
# ---------------------------------------------------------------------------


def _make_real_delivery(state_mgr, dest_pool, key_columns, assigned_ids):
    """Real DeliveryManager in flush-every-cycle mode so end-to-end poll
    tests keep their write coverage. wait_idle() joins the single worker."""
    from viaduck.config import DeliveryConfig
    from viaduck.delivery import DeliveryManager

    dcfg = DeliveryConfig(workers=1, flush_interval_seconds=0.0)
    return DeliveryManager(dcfg, state_mgr, dest_pool, key_columns, assigned_ids)


def _txn_catalog():
    """Mock catalog whose begin_transaction context yields a txn table."""
    mock_catalog = MagicMock()
    txn = MagicMock()
    txn_table = MagicMock()
    upsert_result = MagicMock()
    upsert_result.rows_updated = 0
    upsert_result.rows_inserted = 0
    txn_table.upsert.return_value = upsert_result
    txn.load_table.return_value = txn_table
    mock_catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    mock_catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    return mock_catalog, txn_table


def _cursor(snapshot=5, rows=0):
    c = MagicMock()
    c.last_snapshot_id = snapshot
    c.rows_replicated = rows
    c.last_error = None
    return c


def test_poll_cycle_full_cdc_routes_and_writes():
    """Full CDC end-to-end: read, resolve preimages, route, buffer, flush
    (conflict resolution + apply on the worker), advance cursor."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 100)}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 10, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
            {"company": "quacksworth", "value": 20, "change_type": "insert", "snapshot_id": 6, "rowid": 2},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog, txn_table = _txn_catalog()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["value"], ["dest-1"])
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["value"],
            full_cdc=True,
        )
    assert delivery.wait_idle()

    txn_table.upsert.assert_called_once()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=102)


def test_poll_cycle_append_only_unchanged():
    """Append-only mode uses read_cdc and table.append (on the worker)."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 0)}

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, [], ["dest-1"])
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data) as mock_read_cdc,
        patch("viaduck.main.source.read_cdc_changes") as mock_read_changes,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )
    assert delivery.wait_idle()

    mock_read_cdc.assert_called_once()
    mock_read_changes.assert_not_called()
    mock_dest_table.append.assert_called_once()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=1)


def test_poll_cycle_cdc_delete_only_changeset():
    """CDC mode with only deletes flows through the flush worker."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 50)}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "delete", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog, txn_table = _txn_catalog()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["company"], ["dest-1"])
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["company"],
            full_cdc=True,
        )
    assert delivery.wait_idle()

    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_not_called()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=51)


def test_poll_cycle_cdc_write_failure_isolation():
    """Flush failure: error recorded, connection evicted, buffer dropped,
    position reset to the persisted cursor — and the process survives."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 0)}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog = MagicMock()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_catalog.begin_transaction.side_effect = Exception("catalog down")
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["company"], ["dest-1"])
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
        patch("viaduck.apply.time.sleep"),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["company"],
            full_cdc=True,
        )
    assert delivery.wait_idle()

    state_mgr.record_error.assert_called_once()
    dest_pool.evict.assert_called()
    state_mgr.advance_cursor.assert_not_called()
    # FlushFail semantics: position reset to the persisted cursor.
    assert delivery.positions()["dest-1"] == 5


def test_poll_cycle_cdc_routing_value_mutation():
    """Cross-tenant update: preimage converted to delete for old destination;
    both destinations flush and advance."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth"), ("dest-2", "mallardine")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 10), "dest-2": _cursor(5, 20)}

    # Row moves from quacksworth to mallardine
    raw_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "update_preimage", "snapshot_id": 6, "rowid": 100},
            {"company": "mallardine", "value": 1, "change_type": "update_postimage", "snapshot_id": 6, "rowid": 100},
        ]
    )
    quacks_batch = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "delete", "snapshot_id": 6, "rowid": 100},
        ]
    )
    mallard_batch = _cdc_table(
        [
            {"company": "mallardine", "value": 1, "change_type": "update_postimage", "snapshot_id": 6, "rowid": 100},
        ]
    )

    router.build_filter_expr.return_value = "company IN ('quacksworth', 'mallardine')"
    router.split_and_count.return_value = ({"quacksworth": quacks_batch, "mallardine": mallard_batch}, 0)

    mock_catalog, _txn_table = _txn_catalog()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["company"], ["dest-1", "dest-2"])
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=raw_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"quacksworth": "dest-1", "mallardine": "dest-2"},
            key_columns=["company"],
            full_cdc=True,
        )
    assert delivery.wait_idle()

    # Both destinations should have their cursors advanced
    assert state_mgr.advance_cursor.call_count == 2


def test_poll_cycle_branches_on_key_columns():
    """key_columns presence determines CDC mode: non-empty -> full_cdc, empty -> append."""
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    delivery = _make_delivery({"dest-1": 5})
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data) as mock_read_cdc,
        patch("viaduck.main.source.read_cdc_changes") as mock_read_changes,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )
        mock_read_cdc.assert_called_once()
        mock_read_changes.assert_not_called()

    cdc_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 10, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.split_and_count.return_value = ({"quacksworth": cdc_data}, 0)
    delivery = _make_delivery({"dest-1": 5})
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc") as mock_read_cdc2,
        patch("viaduck.main.source.read_cdc_changes", return_value=cdc_data) as mock_read_changes2,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["company"],
            full_cdc=True,
        )
        mock_read_changes2.assert_called_once()
        mock_read_cdc2.assert_not_called()


# ---------------------------------------------------------------------------
# Torture tests
# ---------------------------------------------------------------------------


def test_torture_insert_update_delete_same_key():
    """3 ops on same rowid: insert + postimage + delete -> only the
    tombstone delete survives conflict resolution."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.column("change_type").to_pylist() == ["delete"]


def test_torture_routing_value_mutation_cross_tenant():
    """Cross-tenant update: preimage becomes delete at old tenant, postimage upserts at new."""
    batch = _cdc_table(
        [
            {"company": "old_tenant", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "new_tenant", "value": 1, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    types = result.column("change_type").to_pylist()
    assert types[0] == "delete"
    assert types[1] == "update_postimage"
    # The delete retains the old routing value
    assert result.column("company")[0].as_py() == "old_tenant"


def test_torture_same_key_different_rows_no_cancel():
    """Different rowids with same key value should both be preserved."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 2


def test_torture_delete_filter_null_composite_key():
    """NULL in composite key produces IS NULL in filter."""
    rows = pa.table(
        {
            "a": pa.array([None], type=pa.int64()),
            "b": ["x"],
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    sql = _build_delete_filter(rows, ["a", "b"])
    assert "NULL" in sql.upper()
    assert "x" in sql


def test_torture_large_composite_key():
    """5-column key, 100 rows should not crash."""
    data = {
        "k1": list(range(100)),
        "k2": [f"v{i}" for i in range(100)],
        "k3": list(range(100, 200)),
        "k4": [f"w{i}" for i in range(100)],
        "k5": list(range(200, 300)),
        "change_type": ["delete"] * 100,
        "snapshot_id": [1] * 100,
        "rowid": list(range(100)),
    }
    rows = pa.table(data)
    sql = _build_delete_filter(rows, ["k1", "k2", "k3", "k4", "k5"])
    assert "OR" in sql or "AND" in sql


def test_torture_deletes_only_changeset():
    """Batch with only deletes: no upsert, only delete filter."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["deleted"] == 2
    assert counts["upserted"] == 0
    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_not_called()


def test_torture_orphaned_preimage():
    """Preimage without postimage should become delete."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "delete"


def test_torture_empty_string_vs_null_key():
    """Empty string and None are different routing values."""
    batch = _cdc_table(
        [
            {"company": "", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    # Same routing value ("" == ""), preimage should be dropped
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "update_postimage"


def test_torture_multiple_updates_same_key():
    """3 postimages for same key: all preserved (no conflict between postimages)."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 3


def test_torture_key_column_missing_from_data():
    """Missing key column raises RoutingError in preimage resolution."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with pytest.raises(RoutingError, match="Key column 'nonexistent' not found"):
        _resolve_preimages(batch, "company", ["nonexistent"])


def test_torture_all_change_types_mixed():
    """All change types in one batch: insert, delete, update_preimage, update_postimage."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 300},
            {"company": "acme", "value": 4, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
        ]
    )
    # Preimage resolution: same tenant, drop preimage
    resolved = _resolve_preimages(batch, "company", ["value"])
    assert resolved.num_rows == 3
    types = resolved.column("change_type").to_pylist()
    assert "update_preimage" not in types
    # Conflict resolution: no conflicts (different rowids)
    final = _resolve_conflicts(resolved)
    assert final.num_rows == 3


def test_torture_special_chars_in_key_column_name():
    """Key column with @ character should work."""
    batch = pa.table(
        {
            "user@domain": ["a", "b"],
            "value": [1, 2],
            "change_type": ["insert", "insert"],
            "snapshot_id": [1, 1],
            "rowid": [100, 200],
        }
    )
    result = _resolve_preimages(batch, "user@domain", ["user@domain"])
    assert result.num_rows == 2


# ---------------------------------------------------------------------------
# Metric coverage for new CDC metrics
# ---------------------------------------------------------------------------


def test_apply_changes_upsert_matched_nonzero():
    """Verify upsert_matched reflects UpsertResult.rows_updated."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    # Simulate 1 row matched (updated), 1 row inserted
    txn_table.upsert.return_value.rows_updated = 3
    txn_table.upsert.return_value.rows_inserted = 1
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
            {"company": "acme", "value": 4, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 400},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["value"])
    assert counts["upserted"] == 4
    assert counts["upsert_matched"] == 3


def test_cdc_batch_rows_metric_observed():
    """cdc_batch_rows histogram should be observed with raw CDC row count."""
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    arrow_data = pa.table({"company": ["quacksworth", "quacksworth", "quacksworth"], "value": [1, 2, 3]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    delivery = _make_delivery({"dest-1": 5})
    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
        patch("viaduck.main.metrics.cdc_batch_rows") as mock_batch_metric,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            full_cdc=False,
        )

    mock_batch_metric.observe.assert_called_once_with(3)


# ---------------------------------------------------------------------------
# _seed_new_destinations
# ---------------------------------------------------------------------------


def test_seed_new_destinations_populates_from_scan():
    """Scan streams 3 rows in one batch; they get appended to the destination."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}  # no cursors -> snapshot_id=0

    rows = pa.table({"company": ["acme", "acme", "acme"], "value": [1, 2, 3]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    assert mock_table.append.call_count == 1
    written = mock_table.append.call_args[0][0]
    assert written.num_rows == 3
    assert written.equals(rows)
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 100, cumulative_rows=3)


def test_seed_new_destinations_skips_existing():
    """State manager returns a cursor with snapshot_id=50 (not 0). No scan happens."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    cursor = MagicMock()
    cursor.last_snapshot_id = 50
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    src_table.scan.assert_not_called()
    dest_pool.get.assert_not_called()


def test_seed_new_destinations_empty_source():
    """source.current_snapshot_id returns None. Nothing happens."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    with patch("viaduck.main.source.current_snapshot_id", return_value=None):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    state_mgr.load_cursors.assert_not_called()
    src_table.scan.assert_not_called()


def test_seed_new_destinations_no_matching_rows():
    """Scan returns 0 rows. No append but cursor still advanced."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter([])
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    # Only the REPLACE-guard get/release; no rows means no writes.
    dest_pool.get.assert_called_once_with("dest-1")
    dest_pool.release.assert_called_once_with("dest-1")
    mock_table.upsert.assert_not_called()
    mock_table.append.assert_not_called()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 100, cumulative_rows=0)


def test_seed_new_destinations_uses_upsert_with_key_columns():
    """cfg has key_columns=['event_id']. table.upsert() called instead of append."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = ["event_id"]
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"event_id": [1, 2], "company": ["acme", "acme"], "value": [10, 20]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    assert mock_table.upsert.call_count == 1
    written = mock_table.upsert.call_args[0][0]
    assert written.equals(rows)
    assert mock_table.upsert.call_args[1] == {"join_cols": ["event_id"]}
    mock_table.append.assert_not_called()


def test_seed_new_destinations_uses_append_without_key_columns():
    """cfg has key_columns=[]. table.append() called."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"company": ["acme", "acme"], "value": [10, 20]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    assert mock_table.append.call_count == 1
    written = mock_table.append.call_args[0][0]
    assert written.equals(rows)
    mock_table.upsert.assert_not_called()


def test_seed_new_destinations_multiple():
    """Multiple new destinations seeded independently."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme"), ("dest-2", "beta")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}  # both at snapshot_id=0

    rows_acme = pa.table({"company": ["acme"], "value": [1]})
    rows_beta = pa.table({"company": ["beta"], "value": [2]})

    def mock_scan(row_filter, snapshot_id=None):
        scan = MagicMock()
        # EqualTo stores the value — extract it from the filter
        if "acme" in str(row_filter):
            scan.to_arrow_batch_reader.return_value = iter(rows_acme.to_batches())
        else:
            scan.to_arrow_batch_reader.return_value = iter(rows_beta.to_batches())
        return scan

    src_table.scan.side_effect = mock_scan

    tables = {}

    def mock_get(dest_id):
        t = tables.get(dest_id) or MagicMock()
        t.scan.return_value.count.return_value = 0
        tables[dest_id] = t
        return (MagicMock(), t)

    dest_pool.get.side_effect = mock_get

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1", "dest-2"])

    assert state_mgr.advance_cursor.call_count == 2
    assert "dest-1" in tables
    assert "dest-2" in tables


def test_seed_new_destinations_pins_snapshot():
    """Scan should be pinned to the captured snapshot_id."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter([])
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    # Verify scan was called with snapshot_id pinned to the captured value
    call_kwargs = src_table.scan.call_args.kwargs
    assert call_kwargs["snapshot_id"] == 42


# ---------------------------------------------------------------------------
# _start_progress_heartbeat
# ---------------------------------------------------------------------------
#
# These tests run the tick loop synchronously: the spawned thread is replaced
# with a no-op, `time.monotonic` is driven by a virtual clock dict, and
# `stop.wait` advances the clock by its `timeout` argument and decides when
# to terminate the loop. No real sleeps, no real threads — fully deterministic.


def _install_fake_heartbeat_runtime(monkeypatch):
    """Patch threading.Thread + time.monotonic. Returns (captured, clock).

    `captured["target"]` is the tick callable, runnable synchronously.
    `clock["t"]` is the virtual clock; advance it from the fake `wait`.
    """
    captured: dict = {}

    class _FakeThread:
        def __init__(self, *, target, daemon):
            captured["target"] = target

        def start(self):
            pass

    monkeypatch.setattr("viaduck.main.threading.Thread", _FakeThread)

    clock = {"t": 0.0}
    monkeypatch.setattr("viaduck.main.time.monotonic", lambda: clock["t"])
    return captured, clock


def test_progress_heartbeat_logs_state_with_rate(caplog, monkeypatch):
    """With a state dict, the heartbeat logs rows + batches + derived rate."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 10, "batches": 4}
    stop = _start_progress_heartbeat("test-label", interval_s=2.0, state=state)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2  # one tick logs, then exit

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "test-label" in r.message]
    assert msgs, f"No heartbeat log captured; records: {[r.message for r in caplog.records]}"
    msg = msgs[0]
    assert "10 rows" in msg
    assert "4 batches" in msg
    # rate = 10 rows / 2.0s elapsed = 5 rows/s
    assert "5 rows/s" in msg


def test_progress_heartbeat_falls_back_to_still_working_without_state(caplog, monkeypatch):
    """Without a state arg, the heartbeat keeps the still-working format."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    stop = _start_progress_heartbeat("plain-label", interval_s=2.0)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "plain-label" in r.message]
    assert msgs
    assert "still working" in msgs[0]


def test_progress_heartbeat_state_updates_visible_to_thread(caplog, monkeypatch):
    """A mid-loop mutation by the writer is visible to the next reader log."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat("growing", interval_s=1.0, state=state)

    call_count = {"n": 0}

    def fake_wait(timeout):
        call_count["n"] += 1
        clock["t"] += timeout
        if call_count["n"] == 1:
            state["rows"] = 100
            state["batches"] = 10
        return call_count["n"] >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "growing" in r.message]
    assert any("100 rows" in m and "10 batches" in m for m in msgs)


def test_progress_heartbeat_uses_pre_progress_label_when_rows_zero(caplog, monkeypatch):
    """With state present but `rows == 0`, the tick uses `pre_progress_label`, not the rate format."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat(
        "seed-label",
        interval_s=2.0,
        state=state,
        pre_progress_label="DuckDB pre-execution",
    )

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "seed-label" in r.message]
    assert msgs
    msg = msgs[0]
    assert "DuckDB pre-execution" in msg
    assert "rows/s" not in msg
    assert "0 rows in 0 batches" not in msg


def test_progress_heartbeat_pre_progress_default_label(caplog, monkeypatch):
    """Without an explicit `pre_progress_label`, the default `no progress yet` is used."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat("default-label", interval_s=2.0, state=state)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "default-label" in r.message]
    assert msgs
    assert "no progress yet" in msgs[0]


def test_progress_heartbeat_transitions_pre_progress_to_rate(caplog, monkeypatch):
    """First tick logs pre-progress; once `rows` is non-zero, subsequent ticks log the rate format."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat(
        "transition-label",
        interval_s=2.0,
        state=state,
        pre_progress_label="DuckDB pre-execution",
    )

    call_count = {"n": 0}

    def fake_wait(timeout):
        call_count["n"] += 1
        clock["t"] += timeout
        # Leave rows at 0 through the first log; mutate before the second wait
        # so the second tick reads a positive rows count.
        if call_count["n"] == 2:
            state["rows"] = 50
            state["batches"] = 5
        return call_count["n"] >= 3

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "transition-label" in r.message]
    assert len(msgs) >= 2, f"Expected at least 2 ticks; got: {msgs}"
    assert "DuckDB pre-execution" in msgs[0]
    assert "rows/s" not in msgs[0]
    assert "50 rows" in msgs[1]
    assert "5 batches" in msgs[1]
    assert "rows/s" in msgs[1]


def test_progress_heartbeat_early_to_normal_cadence_transition(monkeypatch):
    """Heartbeat fires every `early_interval_s` until elapsed >= early_duration_s, then `interval_s`."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    stop = _start_progress_heartbeat(
        "transition-label",
        interval_s=30.0,
        early_interval_s=5.0,
        early_duration_s=60.0,
    )

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        # Stop once we have at least one early + one normal tick.
        return any(w == 30.0 for w in waits)

    monkeypatch.setattr(stop, "wait", fake_wait)

    captured["target"]()

    early = [w for w in waits if w == 5.0]
    normal = [w for w in waits if w == 30.0]
    assert early, f"Expected early-window waits of 5.0s; got: {waits}"
    assert normal, f"Expected at least one normal-cadence wait of 30.0s; got: {waits}"
    # All early waits precede all normal waits.
    last_early_idx = max(i for i, w in enumerate(waits) if w == 5.0)
    first_normal_idx = min(i for i, w in enumerate(waits) if w == 30.0)
    assert last_early_idx < first_normal_idx, f"Early/normal waits interleaved: {waits}"


# ---------------------------------------------------------------------------
# _seed_new_destinations: pre-scan stats + first-batch milestone
# ---------------------------------------------------------------------------


def test_seed_new_destinations_logs_prescan_stats(caplog):
    """`inspect().files(...)` is summed and a one-line stats summary is logged."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    files_table = pa.table(
        {
            "data_file_size_bytes": [1024**3, 2 * 1024**3],  # 1 GiB + 2 GiB = 3 GiB
            "delete_file_size_bytes": [512 * 1024, 256 * 1024],  # 768 KiB = 0.75 MiB
        }
    )
    src_table.inspect.return_value.files.return_value = files_table

    rows = pa.table({"company": ["acme"], "value": [1]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        with caplog.at_level("INFO", logger="viaduck.main"):
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    src_table.inspect.return_value.files.assert_called_once_with(snapshot_id=42)

    msgs = [r.message for r in caplog.records]
    prescan = [m for m in msgs if "Source snapshot 42" in m and "data files" in m]
    assert prescan, f"No prescan stats line; got: {msgs}"
    line = prescan[0]
    assert "2 data files" in line
    assert "3.00 GiB" in line
    assert "768.00 KiB" in line


def test_seed_new_destinations_continues_when_prescan_stats_fail(caplog):
    """If `inspect().files` raises, seed completes anyway and the failure is logged."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}
    src_table.inspect.return_value.files.side_effect = RuntimeError("metadata unavailable")

    rows = pa.table({"company": ["acme"], "value": [1]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        with caplog.at_level("WARNING", logger="viaduck.main"):
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    state_mgr.advance_cursor.assert_called_once()
    err = [r for r in caplog.records if r.levelname == "ERROR" and "snapshot file inventory" in r.message]
    assert err, f"Expected ERROR log for failed prescan; got: {[(r.levelname, r.message) for r in caplog.records]}"


def test_seed_new_destinations_logs_first_batch_milestone(caplog):
    """The first batch from `to_arrow_batch_reader` triggers a milestone log line."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"company": ["acme", "acme"], "value": [1, 2]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=99):
        with caplog.at_level("INFO", logger="viaduck.main"):
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    msgs = [r.message for r in caplog.records]
    milestone = [m for m in msgs if "first batch in" in m and "dest-1" in m]
    assert milestone, f"No first-batch milestone line; got: {msgs}"
    assert "DuckDB pre-execution complete" in milestone[0]
    assert "streaming started" in milestone[0]


# ---------------------------------------------------------------------------
# Winner(k): per-key last-write-wins on upsert candidates (spec Phase3Apply)
# ---------------------------------------------------------------------------


def test_apply_changes_dedupes_upsert_candidates_per_key():
    """Multiple upsert candidates for one key (an insert + later postimages
    across a buffered window) collapse to the highest-snapshot candidate
    BEFORE the upsert — the spec's Winner(k). Found by the M3 soak:
    without this, pyducklake upsert receives duplicate join keys and the
    destination grows duplicate rows."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 3, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 5, "rowid": 100},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 9, "rowid": 100},
            {"company": "other", "value": 7, "change_type": "insert", "snapshot_id": 4, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 2  # one winner per key
    sent = txn_table.upsert.call_args[0][0]
    by_company = dict(zip(sent.column("company").to_pylist(), sent.column("value").to_pylist()))
    assert by_company == {"acme": 3, "other": 7}  # highest snapshot wins


def test_apply_changes_winner_tiebreak_by_rowid():
    """Same snapshot for two candidates of one key (out-of-contract but
    possible in a replayed union): rowid breaks the tie deterministically."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 5, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 5, "rowid": 300},
        ]
    )
    _apply_changes(catalog, dest_table, batch, ["company"])
    sent = txn_table.upsert.call_args[0][0]
    assert sent.column("value").to_pylist() == [2]  # rowid 300 wins


def test_apply_changes_winner_preserves_null_keys():
    """NULL key values must survive Winner(k): Acero hash joins silently
    drop null keys, which is why winner selection is take()-based. Null
    keys form their own group and dedupe last-write-wins like any other."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": None, "value": 1, "change_type": "insert", "snapshot_id": 3, "rowid": 100},
            {"company": None, "value": 2, "change_type": "update_postimage", "snapshot_id": 6, "rowid": 100},
            {"company": "acme", "value": 7, "change_type": "insert", "snapshot_id": 4, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 2
    sent = txn_table.upsert.call_args[0][0]
    by_company = dict(zip(sent.column("company").to_pylist(), sent.column("value").to_pylist()))
    assert by_company == {None: 2, "acme": 7}


def test_apply_changes_winner_composite_key_null_component():
    """A NULL in one component of a composite key must also survive."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "region": None, "value": 1, "change_type": "insert", "snapshot_id": 3, "rowid": 100},
            {
                "company": "acme",
                "region": None,
                "value": 2,
                "change_type": "update_postimage",
                "snapshot_id": 5,
                "rowid": 100,
            },
            {"company": "acme", "region": "us", "value": 9, "change_type": "insert", "snapshot_id": 4, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company", "region"])
    assert counts["upserted"] == 2
    sent = txn_table.upsert.call_args[0][0]
    got = set(zip(sent.column("region").to_pylist(), sent.column("value").to_pylist()))
    assert got == {(None, 2), ("us", 9)}


def test_apply_changes_key_reuse_delete_before_upsert():
    """Key takeover within one window: delete of rowid r1 (key k) plus a
    surviving insert of rowid r2 (same k). Phase 2 keys on rowid so both
    survive to Phase 3, which must apply the delete BEFORE the upsert or
    the reinserted row is eaten (spec afterDelete/afterUpsert order)."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    order = MagicMock()
    order.attach_mock(txn_table.delete, "delete")
    order.attach_mock(txn_table.upsert, "upsert")
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 4, "rowid": 100},
            {"company": "acme", "value": 5, "change_type": "insert", "snapshot_id": 6, "rowid": 300},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["deleted"] == 1
    assert counts["upserted"] == 1
    calls = [name for name, _, _ in order.mock_calls if name in ("delete", "upsert")]
    assert calls == ["delete", "upsert"]
    sent = txn_table.upsert.call_args[0][0]
    assert sent.column("value").to_pylist() == [5]


# ---------------------------------------------------------------------------
# Destination status derivation (web UI / status API)
# ---------------------------------------------------------------------------


def _delivery_status(**kw):
    from viaduck.delivery import DestDeliveryStatus

    base = dict(
        flushed_snapshot=10,
        position_snapshot=10,
        rows_replicated=0,
        last_error=None,
        buffer_rows=0,
        buffer_age_s=0.0,
        flushing=False,
    )
    base.update(kw)
    return DestDeliveryStatus(**base)


@pytest.mark.parametrize(
    ("snap_now", "kw", "expected"),
    [
        # error beats everything
        (12, {"last_error": "boom", "buffer_rows": 5}, "error"),
        (10, {"flushing": True}, "flushing"),
        # reads behind the source: genuinely lagging
        (12, {"position_snapshot": 10}, "lagging"),
        # read-current, data awaiting flush: the buffering design working
        (12, {"position_snapshot": 12, "buffer_rows": 5}, "buffering"),
        # read-current, empty buffer but unpersisted position advance
        (12, {"position_snapshot": 12, "flushed_snapshot": 10}, "buffering"),
        (10, {}, "healthy"),
    ],
)
def test_derive_dest_status(snap_now, kw, expected):
    """Between flushes the cursor always trails the source; that must NOT
    display as 'lagging' — only reads being behind is operationally lag."""
    assert _derive_dest_status(_delivery_status(**kw), snap_now) == expected


def test_phase1_converted_delete_survives_phase2_as_tombstone():
    """Phase 1 converts a cross-tenant preimage to a delete; when the same
    rowid's insert is in the window (row created then migrated), the OLD
    tenant's routed batch carries insert+converted-delete for one rowid —
    Phase 2 must keep the converted delete (tombstone) so the old copy is
    removed. Mirrors the real pipeline order: Phase 1 → route → Phase 2
    per destination."""
    import pyarrow.compute as pc

    batch = _cdc_table(
        [
            {"company": "old_tenant", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "old_tenant", "value": 1, "change_type": "update_preimage", "snapshot_id": 2, "rowid": 100},
            {"company": "new_tenant", "value": 1, "change_type": "update_postimage", "snapshot_id": 2, "rowid": 100},
        ]
    )
    resolved = _resolve_preimages(batch, "company", ["value"])

    # Route by company (what the Router does), then Phase 2 per destination.
    old_routed = resolved.filter(pc.equal(resolved.column("company"), "old_tenant"))
    new_routed = resolved.filter(pc.equal(resolved.column("company"), "new_tenant"))

    old_resolved = _resolve_conflicts(old_routed)
    assert old_resolved.column("change_type").to_pylist() == ["delete"]  # tombstone survives

    new_resolved = _resolve_conflicts(new_routed)
    assert new_resolved.column("change_type").to_pylist() == ["update_postimage"]


# --- seed-scan progress reporting ---


def test_fmt_duration():
    assert _fmt_duration(42) == "42s"
    assert _fmt_duration(90) == "2m"  # rounds
    assert _fmt_duration(25 * 60) == "25m"
    assert _fmt_duration(3 * 3600 + 17 * 60) == "3h 17m"


def test_scan_progress_suffix_no_conn():
    assert _scan_progress_suffix(None, 100.0) == ""


def test_scan_progress_suffix_no_query_running():
    conn = MagicMock()
    conn.query_progress.return_value = -1.0
    assert _scan_progress_suffix(conn, 100.0) == ""


def test_scan_progress_suffix_sub_one_percent_suppressed():
    """Fractional early readings must not render '~0%' with a noise ETA."""
    conn = MagicMock()
    for pct in (0.0, 0.3, 0.99):
        conn.query_progress.return_value = pct
        assert _scan_progress_suffix(conn, 100.0) == ""


def test_scan_progress_suffix_midway():
    conn = MagicMock()
    conn.query_progress.return_value = 25.0
    # 25% in 100s -> 300s remaining -> 5m
    assert _scan_progress_suffix(conn, 100.0) == " (~25% scanned, est. 5m remaining)"


def test_scan_progress_suffix_complete():
    conn = MagicMock()
    conn.query_progress.return_value = 100.0
    assert _scan_progress_suffix(conn, 100.0) == " (~100% scanned)"


def test_scan_progress_suffix_swallows_errors_warning_once(caplog, monkeypatch):
    monkeypatch.setattr("viaduck.main._scan_progress_poll_warned", False)
    conn = MagicMock()
    conn.query_progress.side_effect = RuntimeError("connection busy")
    with caplog.at_level("WARNING", logger="viaduck.main"):
        assert _scan_progress_suffix(conn, 100.0) == ""
        assert _scan_progress_suffix(conn, 200.0) == ""
    warnings = [r for r in caplog.records if "progress polling failed" in r.message]
    assert len(warnings) == 1


def test_scan_progress_suffix_recovers_after_transient_error(monkeypatch):
    """A failed poll (e.g. closed-connection race) must not disable polling."""
    monkeypatch.setattr("viaduck.main._scan_progress_poll_warned", False)
    conn = MagicMock()
    conn.query_progress.side_effect = [RuntimeError("teardown race"), 50.0]
    assert _scan_progress_suffix(conn, 100.0) == ""
    assert "50%" in _scan_progress_suffix(conn, 100.0)


def test_progress_heartbeat_suffix_in_rate_line(caplog, monkeypatch):
    """progress_conn suffix lands in the rows-flowing log variant."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)
    conn = MagicMock()
    conn.query_progress.return_value = 25.0

    state = {"rows": 10, "batches": 4}
    stop = _start_progress_heartbeat("rate-label", interval_s=100.0, state=state, progress_conn=conn)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)
    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "rate-label" in r.message]
    assert msgs
    # 25% in 100s -> 300s -> 5m remaining
    assert "10 rows" in msgs[0]
    assert "(~25% scanned, est. 5m remaining)" in msgs[0]


def test_progress_heartbeat_suffix_in_pre_progress_line(caplog, monkeypatch):
    """progress_conn suffix lands in the pre-execution variant — the original blind spot."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)
    conn = MagicMock()
    conn.query_progress.return_value = 10.0

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat(
        "pre-label",
        interval_s=100.0,
        state=state,
        pre_progress_label="DuckDB pre-execution",
        progress_conn=conn,
    )

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)
    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "pre-label" in r.message]
    assert msgs
    assert "DuckDB pre-execution" in msgs[0]
    # 10% in 100s -> 900s -> 15m remaining
    assert "(~10% scanned, est. 15m remaining)" in msgs[0]


def test_progress_heartbeat_suffix_in_still_working_line(caplog, monkeypatch):
    """progress_conn suffix lands in the no-state variant."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)
    conn = MagicMock()
    conn.query_progress.return_value = 50.0

    stop = _start_progress_heartbeat("plain-progress", interval_s=100.0, progress_conn=conn)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)
    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "plain-progress" in r.message]
    assert msgs
    assert "still working" in msgs[0]
    assert "(~50% scanned" in msgs[0]


def test_seed_passes_source_connection_to_heartbeat():
    """The seed path must wire src_table.catalog.connection into the heartbeat;
    a regression to progress_conn=None passes every other test silently."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"company": ["acme"], "value": [1]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        with patch("viaduck.main._start_progress_heartbeat") as mock_hb:
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    seed_calls = [c for c in mock_hb.call_args_list if "Seed scan" in c.args[0]]
    assert seed_calls, f"No seed-scan heartbeat started; calls: {mock_hb.call_args_list}"
    assert seed_calls[0].kwargs["progress_conn"] is src_table.catalog.connection
