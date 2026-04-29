"""Tests for main loop logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.main import (
    _apply_changes,
    _build_delete_filter,
    _group_by_cursor,
    _poll_cycle,
    _resolve_conflicts,
    _resolve_preimages,
    _seed_new_destinations,
    _write_with_retry,
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

    with patch("viaduck.main.time.sleep"):
        _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == 2
    pool.evict.assert_called_once_with("dest-1")


def test_write_with_retry_exhausted():
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    def op(catalog, table):
        raise Exception("persistent failure")

    with patch("viaduck.main.time.sleep"):
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

    with patch("viaduck.main.time.sleep"), patch("viaduck.main.log") as mock_log:
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
# _poll_cycle (append-only mode)
# ---------------------------------------------------------------------------


def test_poll_cycle_no_snapshots():
    """If source has no snapshots, poll cycle should be a no-op."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([])
    rv_to_dest = {}

    with patch("viaduck.main.source.current_snapshot_id", return_value=None):
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, [], rv_to_dest, key_columns=[], full_cdc=False)

    state_mgr.load_cursors.assert_not_called()


def test_poll_cycle_all_caught_up():
    """If all destinations are at current snapshot, no CDC reads should occur."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 10
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    with patch("viaduck.main.source.current_snapshot_id", return_value=10):
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
        )

    router.build_filter_expr.assert_not_called()


def test_poll_cycle_routes_and_writes():
    """Full poll cycle: read CDC, route, write, advance cursor (append-only)."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 100
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = pa.table({"company": ["quacksworth", "quacksworth"], "value": [10, 20]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
        )

    mock_dest_table.append.assert_called_once_with(arrow_data)
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=102)


def test_poll_cycle_handles_write_failure():
    """Write failure should record error and evict, not crash (append-only)."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 0
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    mock_dest_table.append.side_effect = Exception("connection lost")
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
        patch("viaduck.main.time.sleep"),
    ):
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
        )

    state_mgr.record_error.assert_called_once()
    dest_pool.evict.assert_called()


def test_poll_cycle_empty_changeset_advances_cursors():
    """Empty CDC changeset should advance all destinations in the group."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth"), ("dest-2", "mallardine")])
    rv_to_dest = {"quacksworth": "dest-1", "mallardine": "dest-2"}

    cursor1 = MagicMock()
    cursor1.last_snapshot_id = 5
    cursor2 = MagicMock()
    cursor2.last_snapshot_id = 5
    state_mgr.load_cursors.return_value = {"dest-1": cursor1, "dest-2": cursor2}

    empty_data = pa.table({"company": pa.array([], type=pa.string()), "value": pa.array([], type=pa.int64())})
    router.build_filter_expr.return_value = "company IN ('quacksworth', 'mallardine')"

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=empty_data),
    ):
        _poll_cycle(
            src_table,
            state_mgr,
            dest_pool,
            router,
            cfg,
            ["dest-1", "dest-2"],
            rv_to_dest,
            key_columns=[],
            full_cdc=False,
        )

    state_mgr.advance_cursors.assert_called_once_with(["dest-1", "dest-2"], 10)


def test_poll_cycle_routing_error_breaks_gracefully():
    """RoutingError should break out of group processing, not crash."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = pa.table({"other_col": [1]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.side_effect = RoutingError("field 'company' not found")

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
        )


def test_poll_cycle_advances_no_data_destinations():
    """Destinations with no matching rows should still advance their cursor."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth"), ("dest-2", "mallardine")])
    rv_to_dest = {"quacksworth": "dest-1", "mallardine": "dest-2"}

    cursor1 = MagicMock()
    cursor1.last_snapshot_id = 5
    cursor1.rows_replicated = 10
    cursor2 = MagicMock()
    cursor2.last_snapshot_id = 5
    cursor2.rows_replicated = 0
    state_mgr.load_cursors.return_value = {"dest-1": cursor1, "dest-2": cursor2}

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth', 'mallardine')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            src_table,
            state_mgr,
            dest_pool,
            router,
            cfg,
            ["dest-1", "dest-2"],
            rv_to_dest,
            key_columns=[],
            full_cdc=False,
        )

    state_mgr.advance_cursors.assert_called_once_with(["dest-2"], 10)


def test_poll_cycle_snapshot_at_zero():
    """First snapshot on source: destinations start at 0, current is also small."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    state_mgr.load_cursors.return_value = {}

    empty = pa.table({"company": pa.array([], type=pa.string())})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=1),
        patch("viaduck.main.source.read_cdc", return_value=empty),
    ):
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
        )

    state_mgr.advance_cursors.assert_called_once()


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


def test_resolve_conflicts_insert_delete_cancel():
    """Same rowid insert + delete should cancel both. Metric incremented."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with patch("viaduck.main.metrics.cdc_conflicts_resolved_total") as mock_metric:
        result = _resolve_conflicts(batch)
    assert result.num_rows == 0
    mock_metric.inc.assert_called_once()


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


def test_resolve_conflicts_mixed_some_cancel():
    """Some cancel, some don't."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 1
    assert result.column("rowid")[0].as_py() == 200


def test_resolve_conflicts_empty_batch():
    """Empty batch should return empty."""
    batch = _cdc_table([])
    result = _resolve_conflicts(batch)
    assert result.num_rows == 0


def test_resolve_conflicts_insert_update_delete_sequence():
    """Same rowid: insert + postimage + delete. Insert+delete cancel, postimage dropped."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    # insert+delete cancel (both removed), postimage dropped because delete exists
    assert result.num_rows == 0


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
    # rowid 100: insert+delete cancel. rowid 300: insert preserved.
    assert result.num_rows == 1
    assert result.column("rowid")[0].as_py() == 300


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
    """Only inserts: upsert called, no delete."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 2
    assert counts["deleted"] == 0
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["company"]
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
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["deleted"] == 1
    assert counts["upserted"] == 2
    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["company"]


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


def test_poll_cycle_full_cdc_routes_and_writes():
    """Full CDC mode end-to-end: read, resolve preimages, route, resolve conflicts, apply."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 100
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 10, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
            {"company": "quacksworth", "value": 20, "change_type": "insert", "snapshot_id": 6, "rowid": 2},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog = MagicMock()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    txn = MagicMock()
    txn_table = MagicMock()
    _upsert_result = MagicMock()
    _upsert_result.rows_updated = 0
    _upsert_result.rows_inserted = 0
    txn_table.upsert.return_value = _upsert_result
    txn.load_table.return_value = txn_table
    mock_catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    mock_catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
    ):
        _poll_cycle(
            src_table,
            state_mgr,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            rv_to_dest,
            key_columns=["company"],
            full_cdc=True,
        )

    state_mgr.advance_cursor.assert_called_once()
    call_args = state_mgr.advance_cursor.call_args
    assert call_args[0][0] == "dest-1"
    assert call_args[0][1] == 10


def test_poll_cycle_append_only_unchanged():
    """Backward compat: append-only mode uses read_cdc and table.append."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 0
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data) as mock_read_cdc,
        patch("viaduck.main.source.read_cdc_changes") as mock_read_changes,
    ):
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
        )

    mock_read_cdc.assert_called_once()
    mock_read_changes.assert_not_called()
    mock_dest_table.append.assert_called_once()


def test_poll_cycle_cdc_delete_only_changeset():
    """CDC mode with only deletes."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 50
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "delete", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog = MagicMock()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    txn = MagicMock()
    txn_table = MagicMock()
    _upsert_result = MagicMock()
    _upsert_result.rows_updated = 0
    _upsert_result.rows_inserted = 0
    txn_table.upsert.return_value = _upsert_result
    txn.load_table.return_value = txn_table
    mock_catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    mock_catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
    ):
        _poll_cycle(
            src_table,
            state_mgr,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            rv_to_dest,
            key_columns=["company"],
            full_cdc=True,
        )

    state_mgr.advance_cursor.assert_called_once()


def test_poll_cycle_cdc_write_failure_isolation():
    """Error isolation in CDC mode: write failure doesn't crash."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 0
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

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
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
        patch("viaduck.main.time.sleep"),
    ):
        _poll_cycle(
            src_table,
            state_mgr,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            rv_to_dest,
            key_columns=["company"],
            full_cdc=True,
        )

    state_mgr.record_error.assert_called_once()
    dest_pool.evict.assert_called()


def test_poll_cycle_cdc_routing_value_mutation():
    """Cross-tenant update: preimage converted to delete for old destination."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth"), ("dest-2", "mallardine")])
    cfg.routing.field = "company"
    rv_to_dest = {"quacksworth": "dest-1", "mallardine": "dest-2"}

    cursor1 = MagicMock()
    cursor1.last_snapshot_id = 5
    cursor1.rows_replicated = 10
    cursor2 = MagicMock()
    cursor2.last_snapshot_id = 5
    cursor2.rows_replicated = 20
    state_mgr.load_cursors.return_value = {"dest-1": cursor1, "dest-2": cursor2}

    # Row moves from quacksworth to mallardine
    raw_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "update_preimage", "snapshot_id": 6, "rowid": 100},
            {"company": "mallardine", "value": 1, "change_type": "update_postimage", "snapshot_id": 6, "rowid": 100},
        ]
    )

    # After preimage resolution, preimage becomes delete. Router splits accordingly.
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

    mock_catalog = MagicMock()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    txn = MagicMock()
    txn_table = MagicMock()
    _upsert_result = MagicMock()
    _upsert_result.rows_updated = 0
    _upsert_result.rows_inserted = 0
    txn_table.upsert.return_value = _upsert_result
    txn.load_table.return_value = txn_table
    mock_catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    mock_catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc_changes", return_value=raw_data),
    ):
        _poll_cycle(
            src_table,
            state_mgr,
            dest_pool,
            router,
            cfg,
            ["dest-1", "dest-2"],
            rv_to_dest,
            key_columns=["company"],
            full_cdc=True,
        )

    # Both destinations should have their cursors advanced
    assert state_mgr.advance_cursor.call_count == 2


def test_poll_cycle_branches_on_key_columns():
    """key_columns presence determines CDC mode: non-empty -> full_cdc, empty -> append."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 0
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data) as mock_read_cdc,
        patch("viaduck.main.source.read_cdc_changes") as mock_read_changes,
    ):
        # Empty key_columns -> append-only
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
        )
        mock_read_cdc.assert_called_once()
        mock_read_changes.assert_not_called()

    # Reset mocks
    state_mgr.reset_mock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 0
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    cdc_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 10, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.split_and_count.return_value = ({"quacksworth": cdc_data}, 0)

    mock_catalog = MagicMock()
    mock_dest_table2 = MagicMock()
    mock_dest_table2.identifier = "dest_table"
    txn = MagicMock()
    txn_table = MagicMock()
    txn.load_table.return_value = txn_table
    mock_catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    mock_catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    dest_pool.get.return_value = (mock_catalog, mock_dest_table2)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc") as mock_read_cdc2,
        patch("viaduck.main.source.read_cdc_changes", return_value=cdc_data) as mock_read_changes2,
    ):
        cfg.routing.field = "company"
        _poll_cycle(
            src_table,
            state_mgr,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            rv_to_dest,
            key_columns=["company"],
            full_cdc=True,
        )
        mock_read_changes2.assert_called_once()
        mock_read_cdc2.assert_not_called()


# ---------------------------------------------------------------------------
# Torture tests
# ---------------------------------------------------------------------------


def test_torture_insert_update_delete_same_key():
    """3 ops on same rowid: insert + postimage + delete -> net no-op after conflict resolution."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 0


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
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 4
    assert counts["upsert_matched"] == 3


def test_cdc_batch_rows_metric_observed():
    """cdc_batch_rows histogram should be observed with raw CDC row count."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    cursor = MagicMock()
    cursor.last_snapshot_id = 5
    cursor.rows_replicated = 0
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    arrow_data = pa.table({"company": ["quacksworth", "quacksworth", "quacksworth"], "value": [1, 2, 3]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
        patch("viaduck.main.metrics.cdc_batch_rows") as mock_batch_metric,
    ):
        _poll_cycle(
            src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest, key_columns=[], full_cdc=False
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
    mock_scan.count.return_value = rows.num_rows
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
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
    mock_scan.count.return_value = 0
    mock_scan.to_arrow_batch_reader.return_value = iter([])
    src_table.scan.return_value = mock_scan

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    dest_pool.get.assert_not_called()
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
    mock_scan.count.return_value = rows.num_rows
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
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
    mock_scan.count.return_value = rows.num_rows
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
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
            scan.count.return_value = rows_acme.num_rows
            scan.to_arrow_batch_reader.return_value = iter(rows_acme.to_batches())
        else:
            scan.count.return_value = rows_beta.num_rows
            scan.to_arrow_batch_reader.return_value = iter(rows_beta.to_batches())
        return scan

    src_table.scan.side_effect = mock_scan

    tables = {}

    def mock_get(dest_id):
        t = MagicMock()
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
    mock_scan.count.return_value = 0
    mock_scan.to_arrow_batch_reader.return_value = iter([])
    src_table.scan.return_value = mock_scan

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    # Verify scan was called with snapshot_id pinned to the captured value
    call_kwargs = src_table.scan.call_args.kwargs
    assert call_kwargs["snapshot_id"] == 42
