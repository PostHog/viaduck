"""Tests for source CDC reading wrapper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa

from viaduck import metrics
from viaduck.source import current_snapshot_id, read_cdc


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

    with patch.dict("os.environ", {"SRC_PG": "postgres://localhost/test"}):
        with patch("pyducklake.Catalog") as MockCatalog:
            from viaduck.source import connect

            connect(cfg)
            MockCatalog.assert_called_once_with(
                "src",
                "postgres://localhost/test",
                data_path="/tmp/data",
                properties={},
            )
