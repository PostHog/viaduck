"""Tests for source CDC reading wrapper."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pyarrow as pa

from viaduck import metrics
from viaduck.source import (
    current_snapshot_id,
    earliest_snapshot_id,
    read_cdc,
    read_cdc_changes,
    snapshot_times,
    strip_meta,
)


def setup_module():
    metrics.init("test")


def _make_table_with_catalog(catalog_name: str, snapshot_agg: int | None):
    """Build a mock Table whose catalog connection returns snapshot_agg from any snapshot-aggregate query."""
    table = MagicMock()
    table._catalog.name = catalog_name
    row = (snapshot_agg,) if snapshot_agg is not None else (None,)
    table._catalog.connection.execute.return_value.fetchone.return_value = row
    return table


def test_current_snapshot_id_returns_max():
    table = _make_table_with_catalog("megaduck-mw-prod-us", 5_000_042)
    result = current_snapshot_id(table)
    assert result == 5_000_042
    sql = table._catalog.connection.execute.call_args[0][0]
    assert "MAX(snapshot_id)" in sql
    # Must NOT delegate to pyducklake's Table.current_snapshot() — that path loads every snapshot
    # row into memory and produced a per-poll-cycle allocator leak on large catalogs.
    table.current_snapshot.assert_not_called()
    # Schema name must be double-quoted so hyphens in catalog names are valid identifiers
    assert '"__ducklake_metadata_megaduck-mw-prod-us"' in sql


def test_current_snapshot_id_returns_none_on_empty_catalog():
    table = _make_table_with_catalog("empty-cat", None)
    assert current_snapshot_id(table) is None
    table.current_snapshot.assert_not_called()


def test_earliest_snapshot_id_returns_min():
    table = _make_table_with_catalog("megaduck-mw-prod-us", 5_000_000)
    result = earliest_snapshot_id(table)
    assert result == 5_000_000
    sql = table._catalog.connection.execute.call_args[0][0]
    assert "MIN(snapshot_id)" in sql
    # Schema name must be double-quoted so hyphens in catalog names are valid identifiers
    assert '"__ducklake_metadata_megaduck-mw-prod-us"' in sql


def test_earliest_snapshot_id_returns_none_on_empty_catalog():
    table = _make_table_with_catalog("lake", None)
    assert earliest_snapshot_id(table) is None


def test_read_cdc_calls_table_insertions():
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    changeset.to_arrow.return_value = arrow_table
    table.table_insertions.return_value = changeset

    result = read_cdc(table, after_snapshot=0, end_snapshot=5)

    # after_snapshot is exclusive (last delivered): reads (0, 5] -> pyducklake start 1
    table.table_insertions.assert_called_once_with(start_snapshot=1, end_snapshot=5)
    assert result.num_rows == 2


def test_read_cdc_with_filter():
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1]})
    changeset.to_arrow.return_value = arrow_table
    table.table_insertions.return_value = changeset

    result = read_cdc(table, after_snapshot=0, end_snapshot=5, filter_expr="team_id IN (123)")

    table.table_insertions.assert_called_once_with(
        start_snapshot=1,
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

    result = read_cdc(table, after_snapshot=5, end_snapshot=5)
    assert result.num_rows == 0


def test_connect(tmp_path):
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

            # settings snapshots come back empty -> verified apply sees no
            # collateral and treats every SET as clean
            MockCatalog.return_value.connection.execute.return_value.fetchall.return_value = []
            connect(cfg)
            MockCatalog.assert_called_once()
            args, kwargs = MockCatalog.call_args
            assert args == ("src", "postgres:host=localhost dbname=test")
            assert kwargs["data_path"] == "/tmp/data"
            props = kwargs["properties"]
            # temp_directory is a fresh per-connection spill dir (isolation
            # against cross-instance spill-file collisions) — assert shape,
            # not value. The conftest fixture points the base at tmp_path.
            temp_dir = props.pop("temp_directory")
            assert temp_dir.startswith(str(tmp_path))
            assert f"{os.sep}viaduck-spill{os.sep}src-" in temp_dir
            # ducklake_* settings are DELIBERATELY absent: pyducklake would
            # SET them pre-attach, where the fork-engine/stock-extension
            # registry mismatch corrupts core settings (TimeZone, Calendar).
            # They go through apply_extension_settings_verified post-attach
            # instead (asserted below).
            assert props == {
                "pg_connection_limit": "64",
                "arrow_large_buffer_size": "true",
                "enable_progress_bar": "true",
                "enable_progress_bar_print": "false",
                "enable_external_file_cache": "false",
            }
            set_calls = [c.args[0] for c in MockCatalog.return_value.connection.execute.call_args_list]
            assert "SELECT name, value FROM duckdb_settings()" in set_calls[0]
            assert any(c.startswith("SET ducklake_max_retry_count") for c in set_calls)
            assert any(c.startswith("SET ducklake_retry_wait_ms") for c in set_calls)
            assert any(c.startswith("SET ducklake_retry_backoff") for c in set_calls)
            # NO TimeZone pin — on the mismatched registry `SET TimeZone`
            # poisons the extension's UINT64 retry slot with a string and
            # every commit fails (2026-08-15 zero-flush outage). Nothing
            # outside the verified loop may SET anything.
            assert not any("TimeZone" in c and c.startswith("SET ") for c in set_calls)


# --- read_cdc_changes tests ---


def test_read_cdc_changes_calls_table_changes():
    """Verify table_changes() is called (not table_insertions)."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1, 2], "change_type": ["INSERT", "INSERT"]})
    changeset.to_arrow.return_value = arrow_table
    table.table_changes.return_value = changeset

    result = read_cdc_changes(table, after_snapshot=0, end_snapshot=5)

    table.table_changes.assert_called_once_with(start_snapshot=1, end_snapshot=5)
    table.table_insertions.assert_not_called()
    assert result.num_rows == 2


def test_read_cdc_changes_with_filter():
    """filter_expr passed through to table_changes."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1], "change_type": ["INSERT"]})
    changeset.to_arrow.return_value = arrow_table
    table.table_changes.return_value = changeset

    result = read_cdc_changes(table, after_snapshot=0, end_snapshot=5, filter_expr="team_id IN (123)")

    table.table_changes.assert_called_once_with(
        start_snapshot=1,
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

    result = read_cdc_changes(table, after_snapshot=5, end_snapshot=5)
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

    result = read_cdc_changes(table, after_snapshot=0, end_snapshot=10)

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

    result = read_cdc_changes(table, after_snapshot=0, end_snapshot=10)

    assert "rowid" in result.column_names
    assert result.column("rowid").to_pylist() == [100, 101]


def test_read_cdc_unchanged():
    """Backward compat: read_cdc still works (calls table_insertions)."""
    table = MagicMock()
    changeset = MagicMock()
    arrow_table = pa.table({"id": [1, 2, 3]})
    changeset.to_arrow.return_value = arrow_table
    table.table_insertions.return_value = changeset

    result = read_cdc(table, after_snapshot=0, end_snapshot=5)

    table.table_insertions.assert_called_once_with(start_snapshot=1, end_snapshot=5)
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


class TestSnapshotTimes:
    """snapshot_times: one indexed lookup, clamp-to-oldest for expired cursors."""

    def _table(self, side_effect):
        table = MagicMock()
        table._catalog.name = "lake"
        table._catalog.connection.execute.side_effect = side_effect
        return table

    def test_empty_ids_queries_nothing(self):
        table = self._table(AssertionError("must not query"))
        assert snapshot_times(table, []) == {}

    def test_maps_ids_to_times(self):
        result = MagicMock()
        result.fetchall.return_value = [(10, "t10"), (20, "t20")]
        table = self._table([result])
        out = snapshot_times(table, [10, 20])
        assert out == {10: "t10", 20: "t20"}
        sql = table._catalog.connection.execute.call_args[0][0]
        assert '"__ducklake_metadata_lake".ducklake_snapshot' in sql
        assert "IN (10, 20)" in sql

    def test_missing_id_clamped_to_oldest_retained(self):
        # Cursor 5 was expired out of ducklake_snapshot: the IN lookup only
        # finds 20; the follow-up MIN(snapshot_time) supplies the clamp so a
        # badly-lagged destination reports at least the window age.
        in_result = MagicMock()
        in_result.fetchall.return_value = [(20, "t20")]
        min_result = MagicMock()
        min_result.fetchone.return_value = ("t_oldest",)
        table = self._table([in_result, min_result])
        out = snapshot_times(table, [5, 20])
        assert out == {5: "t_oldest", 20: "t20"}
        min_sql = table._catalog.connection.execute.call_args_list[1][0][0]
        assert "MIN(snapshot_time)" in min_sql

    def test_meta_schema_quoted_for_hyphenated_catalogs(self):
        result = MagicMock()
        result.fetchall.return_value = [(10, "t10")]
        table = self._table([result])
        table._catalog.name = "megaduck-mw-prod-us"
        snapshot_times(table, [10])
        sql = table._catalog.connection.execute.call_args[0][0]
        assert '"__ducklake_metadata_megaduck-mw-prod-us".ducklake_snapshot' in sql

    def test_clamp_warns_once_per_cursor(self, caplog):
        import logging as _logging

        from viaduck import source as source_mod

        source_mod._clamp_warned_cursors.clear()
        in_result = MagicMock()
        in_result.fetchall.return_value = []
        min_result = MagicMock()
        min_result.fetchone.return_value = ("t_oldest",)
        table = self._table([in_result, min_result, in_result, min_result])
        with caplog.at_level(_logging.WARNING, logger="viaduck.source"):
            snapshot_times(table, [5])
            snapshot_times(table, [5])  # same cursor again: no second warning
        warnings = [r for r in caplog.records if "older than" in r.message]
        assert len(warnings) == 1

    def test_empty_catalog_returns_found_only(self):
        in_result = MagicMock()
        in_result.fetchall.return_value = []
        min_result = MagicMock()
        min_result.fetchone.return_value = (None,)
        table = self._table([in_result, min_result])
        assert snapshot_times(table, [5]) == {}


# ---------------------------------------------------------------------------
# --- extension-setting verified apply (viaduck#71 fork-wheel registry mismatch) ---


class _SettingsResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeSettingsConn:
    """DuckDB-shaped connection where SETs can be aliased to the wrong slot
    (the engine/extension registry-mismatch signature) or fail outright."""

    def __init__(self, alias: dict[str, str] | None = None, fail: set[str] | None = None):
        self.settings = {"TimeZone": "Etc/UTC", "Calendar": "gregorian"}
        self.alias = alias or {}
        self.fail = fail or set()
        self.sql: list[str] = []

    def execute(self, sql):
        import re as _re

        self.sql.append(sql)
        if sql.startswith("SELECT name, value"):
            return _SettingsResult(list(self.settings.items()))
        m = _re.match(r"SET (\w+) = '(.*)'", sql)
        assert m, sql
        key, val = m.group(1), m.group(2)
        if key in self.fail:
            raise RuntimeError(f"unrecognized configuration parameter {key}")
        target = self.alias.get(key, key)
        self.settings[target] = val
        if target != key:
            # the real mismatch mirrors the value under the extension name too
            self.settings[key] = val
        return _SettingsResult([])


def test_split_extension_settings():
    from viaduck.source import split_extension_settings

    safe, ext = split_extension_settings(
        {"s3_region": "us-east-1", "ducklake_max_retry_count": "20", "memory_limit": "4GB"}
    )
    assert ext == {"ducklake_max_retry_count": "20"}
    assert "ducklake_max_retry_count" not in safe
    assert safe["s3_region"] == "us-east-1"


def test_verified_apply_healthy_engine_applies_cleanly(caplog):
    from viaduck.source import apply_extension_settings_verified

    conn = _FakeSettingsConn()
    with caplog.at_level("WARNING"):
        apply_extension_settings_verified(
            conn, {"ducklake_max_retry_count": "20", "ducklake_retry_backoff": "1.0"}, context="t"
        )
    assert conn.settings["ducklake_max_retry_count"] == "20"
    assert conn.settings["ducklake_retry_backoff"] == "1.0"
    assert conn.settings["TimeZone"] == "Etc/UTC"
    assert not [r for r in caplog.records if r.name == "viaduck.source"]


def test_verified_apply_reverts_aliased_collateral(caplog):
    """The prod signature: SET ducklake_max_retry_count lands in TimeZone.
    The corrupted slot must be restored and the mismatch WARNed."""
    from viaduck.source import apply_extension_settings_verified

    conn = _FakeSettingsConn(alias={"ducklake_max_retry_count": "TimeZone"})
    with caplog.at_level("WARNING"):
        apply_extension_settings_verified(conn, {"ducklake_max_retry_count": "20"}, context="t")
    assert conn.settings["TimeZone"] == "Etc/UTC"  # reverted
    assert any("corrupted unrelated settings" in r.getMessage() for r in caplog.records)


def test_verified_apply_set_failure_continues(caplog):
    from viaduck.source import apply_extension_settings_verified

    conn = _FakeSettingsConn(fail={"ducklake_retry_wait_ms"})
    with caplog.at_level("WARNING"):
        apply_extension_settings_verified(
            conn, {"ducklake_retry_wait_ms": "50", "ducklake_retry_backoff": "1.0"}, context="t"
        )
    assert conn.settings["ducklake_retry_backoff"] == "1.0"  # later key still applied
    assert any("SET ducklake_retry_wait_ms failed" in r.getMessage() for r in caplog.records)
