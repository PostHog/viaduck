"""Tests for main loop logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.main import _group_by_cursor, _poll_cycle, _write_with_retry
from viaduck.router import RoutingError


def setup_module():
    metrics.init("test")


# --- _group_by_cursor ---


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


# --- _write_with_retry ---


def test_write_with_retry_success_first_attempt():
    pool = MagicMock()
    mock_table = MagicMock()
    pool.get.return_value = (MagicMock(), mock_table)
    batch = pa.table({"x": [1]})

    _write_with_retry(pool, "dest-1", batch)

    mock_table.append.assert_called_once_with(batch)


def test_write_with_retry_retries_on_failure():
    pool = MagicMock()
    mock_table = MagicMock()
    mock_table.append.side_effect = [Exception("fail"), None]
    pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.time.sleep"):
        _write_with_retry(pool, "dest-1", pa.table({"x": [1]}))

    assert mock_table.append.call_count == 2
    pool.evict.assert_called_once_with("dest-1")


def test_write_with_retry_exhausted():
    pool = MagicMock()
    mock_table = MagicMock()
    mock_table.append.side_effect = Exception("persistent failure")
    pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.time.sleep"):
        with pytest.raises(Exception, match="persistent failure"):
            _write_with_retry(pool, "dest-1", pa.table({"x": [1]}))


def test_write_with_retry_logs_exception_message():
    """Retry should log the actual exception message (H6)."""
    pool = MagicMock()
    mock_table = MagicMock()
    mock_table.append.side_effect = [ConnectionError("connection refused"), None]
    pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.time.sleep"), patch("viaduck.main.log") as mock_log:
        _write_with_retry(pool, "dest-1", pa.table({"x": [1]}))

    # The warning should contain the exception message
    warning_call = mock_log.warning.call_args
    assert "connection refused" in str(warning_call)


# --- _poll_cycle ---


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


def test_poll_cycle_no_snapshots():
    """If source has no snapshots, poll cycle should be a no-op."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([])
    rv_to_dest = {}

    with patch("viaduck.main.source.current_snapshot_id", return_value=None):
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, [], rv_to_dest)

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
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest)

    router.build_filter_expr.assert_not_called()


def test_poll_cycle_routes_and_writes():
    """Full poll cycle: read CDC, route, write, advance cursor."""
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
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest)

    mock_dest_table.append.assert_called_once_with(arrow_data)
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=102)


def test_poll_cycle_handles_write_failure():
    """Write failure should record error and evict, not crash."""
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
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest)

    state_mgr.record_error.assert_called_once()
    dest_pool.evict.assert_called()


def test_poll_cycle_empty_changeset_advances_cursors():
    """Empty CDC changeset should advance all destinations in the group (M4)."""
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
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, ["dest-1", "dest-2"], rv_to_dest)

    state_mgr.advance_cursors.assert_called_once_with(["dest-1", "dest-2"], 10)


def test_poll_cycle_routing_error_breaks_gracefully():
    """RoutingError should break out of group processing, not crash (C4)."""
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
        # Should not raise — breaks out of the group loop
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest)


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
    # Only "quacksworth" has data, "mallardine" gets nothing
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=10),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, ["dest-1", "dest-2"], rv_to_dest)

    # dest-2 (mallardine) had no data, should be advanced via advance_cursors
    state_mgr.advance_cursors.assert_called_once_with(["dest-2"], 10)


# --- Snapshot edge cases (M4) ---


def test_poll_cycle_snapshot_at_zero():
    """First snapshot on source: destinations start at 0, current is also small."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    rv_to_dest = {"quacksworth": "dest-1"}

    # No cursor yet (defaults to 0)
    state_mgr.load_cursors.return_value = {}

    empty = pa.table({"company": pa.array([], type=pa.string())})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"

    with (
        patch("viaduck.main.source.current_snapshot_id", return_value=1),
        patch("viaduck.main.source.read_cdc", return_value=empty),
    ):
        _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, ["dest-1"], rv_to_dest)

    # Should advance from 0 to 1
    state_mgr.advance_cursors.assert_called_once()
