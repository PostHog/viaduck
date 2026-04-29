"""Tests for source CDC reading wrapper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa

from viaduck import metrics
from viaduck.source import current_snapshot_id, read_cdc, read_cdc_changes, strip_meta


def setup_module():
    metrics.init("test")


def test_current_snapshot_id_returns_id():
    table = MagicMock()
    snap = MagicMock()
    snap.snapshot_id = 42
    table.current_snapshot.return_value = snap
    assert current_snapshot_id(table) == 42


def test_current_snapshot_id_returns_none():
    table = MagicMock()
    table.current_snapshot.return_value = None
    assert current_snapshot_id(table) is None


def test_read_cdc_calls_table_insertions():
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    changeset.to_arrow.return_value = arrow_table
    table.table_insertions.return_value = changeset

    result = read_cdc(table, start_snapshot=0, end_snapshot=5)

    table.table_insertions.assert_called_once_with(start_snapshot=0, end_snapshot=5)
    assert result.num_rows == 2


def test_read_cdc_with_filter():
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1]})
    changeset.to_arrow.return_value = arrow_table
    table.table_insertions.return_value = changeset

    result = read_cdc(table, start_snapshot=0, end_snapshot=5, filter_expr="team_id IN (123)")

    table.table_insertions.assert_called_once_with(
        start_snapshot=0,
        end_snapshot=5,
        filter_expr="team_id IN (123)",
    )
    assert result.num_rows == 1


def test_read_cdc_empty_result():
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": pa.array([], type=pa.int64())})
    changeset.to_arrow.return_value = arrow_table
    table.table_insertions.return_value = changeset

    result = read_cdc(table, start_snapshot=5, end_snapshot=5)
    assert result.num_rows == 0


def test_connect():
    """Test that connect() creates a Catalog with correct params."""
    from viaduck.config import SourceConfig

    cfg = SourceConfig(
        name="src",
        postgres_uri_env="SRC_PG",
        data_path="/tmp/data",
        table="events",
    )

    with patch.dict("os.environ", {"SRC_PG": "postgres:host=localhost dbname=test"}):
        with patch("pyducklake.Catalog") as MockCatalog:
            from viaduck.source import connect

            connect(cfg)
            MockCatalog.assert_called_once_with(
                "src",
                "postgres:host=localhost dbname=test",
                data_path="/tmp/data",
                properties={},
            )


# --- read_cdc_changes tests ---


def test_read_cdc_changes_calls_table_changes():
    """Verify table_changes() is called (not table_insertions)."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1, 2], "change_type": ["INSERT", "INSERT"]})
    changeset.to_arrow.return_value = arrow_table
    table.table_changes.return_value = changeset

    result = read_cdc_changes(table, start_snapshot=0, end_snapshot=5)

    table.table_changes.assert_called_once_with(start_snapshot=0, end_snapshot=5)
    table.table_insertions.assert_not_called()
    assert result.num_rows == 2


def test_read_cdc_changes_with_filter():
    """filter_expr passed through to table_changes."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1], "change_type": ["INSERT"]})
    changeset.to_arrow.return_value = arrow_table
    table.table_changes.return_value = changeset

    result = read_cdc_changes(table, start_snapshot=0, end_snapshot=5, filter_expr="team_id IN (123)")

    table.table_changes.assert_called_once_with(
        start_snapshot=0,
        end_snapshot=5,
        filter_expr="team_id IN (123)",
    )
    assert result.num_rows == 1


def test_read_cdc_changes_empty():
    """0-row changeset."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "change_type": pa.array([], type=pa.string()),
        }
    )
    changeset.to_arrow.return_value = arrow_table
    table.table_changes.return_value = changeset

    result = read_cdc_changes(table, start_snapshot=5, end_snapshot=5)
    assert result.num_rows == 0


def test_read_cdc_changes_with_all_change_types():
    """Verify mixed change types pass through."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table(
        {
            "id": [1, 2, 3, 4],
            "change_type": ["INSERT", "DELETE", "UPDATE_PREIMAGE", "UPDATE_POSTIMAGE"],
            "snapshot_id": [10, 10, 10, 10],
            "rowid": [100, 101, 102, 102],
        }
    )
    changeset.to_arrow.return_value = arrow_table
    table.table_changes.return_value = changeset

    result = read_cdc_changes(table, start_snapshot=0, end_snapshot=10)

    assert result.num_rows == 4
    assert result.column("change_type").to_pylist() == ["INSERT", "DELETE", "UPDATE_PREIMAGE", "UPDATE_POSTIMAGE"]


def test_read_cdc_changes_rowid_preservation():
    """rowid column preserved in output."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table(
        {
            "id": [1, 2],
            "change_type": ["INSERT", "INSERT"],
            "snapshot_id": [10, 10],
            "rowid": [100, 101],
        }
    )
    changeset.to_arrow.return_value = arrow_table
    table.table_changes.return_value = changeset

    result = read_cdc_changes(table, start_snapshot=0, end_snapshot=10)

    assert "rowid" in result.column_names
    assert result.column("rowid").to_pylist() == [100, 101]


def test_read_cdc_unchanged():
    """Backward compat: read_cdc still works (calls table_insertions)."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1, 2, 3]})
    changeset.to_arrow.return_value = arrow_table
    table.table_insertions.return_value = changeset

    result = read_cdc(table, start_snapshot=0, end_snapshot=5)

    table.table_insertions.assert_called_once_with(start_snapshot=0, end_snapshot=5)
    table.table_changes.assert_not_called()
    assert result.num_rows == 3


# --- strip_meta tests ---


def test_strip_meta_removes_columns():
    """Strips change_type, snapshot_id, rowid."""
    t = pa.table(
        {
            "id": [1, 2],
            "name": ["a", "b"],
            "change_type": ["INSERT", "INSERT"],
            "snapshot_id": [10, 10],
            "rowid": [100, 101],
        }
    )
    result = strip_meta(t)
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 2


def test_strip_meta_no_meta_columns():
    """No-op when columns absent."""
    t = pa.table({"id": [1, 2], "name": ["a", "b"]})
    result = strip_meta(t)
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 2


def test_strip_meta_partial_metadata():
    """Only some meta columns present."""
    t = pa.table(
        {
            "id": [1],
            "change_type": ["INSERT"],
            "name": ["a"],
        }
    )
    result = strip_meta(t)
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 1
