"""Unit tests for the direct-SQL feed reader (viaduck/feed.py).

psycopg and the duckdb connection are mocked; SQL shape and control flow
are what's pinned here. Real-semantics parity against a live DuckLake
catalog lives in tests/integration/test_feed_parity.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.feed import FeedError, FeedReader


@pytest.fixture(autouse=True, scope="module")
def _metrics():
    metrics.init("test")


def _mock_pg(meta_schema: str = "public", version: str = "1.0", encrypted: str = "false"):
    """A psycopg connection mock that answers the verification queries."""
    pg = MagicMock()

    def execute(sql, params=None):
        cur = MagicMock()
        if "information_schema.tables" in sql:
            cur.fetchall.return_value = [(meta_schema,)]
        elif "ducklake_metadata" in sql and params == ("version",):
            cur.fetchone.return_value = (version,)
        elif "ducklake_metadata" in sql and params == ("encrypted",):
            cur.fetchone.return_value = (encrypted,)
        elif "ducklake_metadata" in sql and params == ("data_path",):
            cur.fetchone.return_value = None  # fall back to constructor data_path
        return cur

    pg.execute.side_effect = execute
    return pg


def _make_reader(pg) -> FeedReader:
    pg.closed = False
    r = FeedReader(postgres_uri="postgresql://x", catalog_name="lake", data_path="s3://bucket/")
    r._conn = pg
    return r


def _make_table(columns=("team_id", "event"), identifier=("main", "events")):
    table = MagicMock()
    table._identifier = identifier
    fields = []
    for i, name in enumerate(columns, start=1):
        f = MagicMock()
        f.name = name
        fields.append(f)
    table.schema.fields = fields
    table.schema.as_arrow.return_value = pa.schema([pa.field(c, pa.string()) for c in columns])
    return table


class TestVerification:
    def test_version_pin_refuses_unknown(self):
        pg = _mock_pg(version="9.9")
        r = _make_reader(pg)
        with pytest.raises(FeedError, match="9.9"):
            r.verify_catalog()

    def test_version_pin_accepts_supported(self):
        r = _make_reader(_mock_pg())
        r.verify_catalog()
        assert r._meta_schema == "public"
        assert r._verified

    def test_encrypted_catalog_refused(self):
        r = _make_reader(_mock_pg(encrypted="true"))
        with pytest.raises(FeedError, match="encrypted"):
            r.verify_catalog()

    def test_meta_schema_prefers_catalog_named(self):
        pg = MagicMock()

        def execute(sql, params=None):
            cur = MagicMock()
            if "information_schema.tables" in sql:
                cur.fetchall.return_value = [("public",), ("__ducklake_metadata_lake",)]
            elif "ducklake_metadata" in sql:
                cur.fetchone.return_value = ("1.0",) if params == ("version",) else ("false",)
            return cur

        pg.execute.side_effect = execute
        r = _make_reader(pg)
        r.verify_catalog()
        assert r._meta_schema == "__ducklake_metadata_lake"

    def test_meta_schema_ambiguous_fails_loudly(self):
        pg = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [("public",), ("other",)]
        pg.execute.return_value = cur
        r = _make_reader(pg)
        with pytest.raises(FeedError, match="Cannot locate"):
            r.verify_catalog()


class TestRead:
    def _read_setup(self, file_rows, inline_registry=(), inline_rows=None):
        """Reader whose PG returns the given file rows; duckdb conn mocked."""
        pg = _mock_pg()

        def execute(sql, params=None):
            cur = MagicMock()
            if "information_schema.tables" in sql or "ducklake_metadata" in sql:
                return _mock_pg().execute(sql, params)
            if "ducklake_schema" in sql and "table_id" in sql:
                cur.fetchone.return_value = (16,)
            elif "ducklake_schema" in sql:  # path lookup: (t.path, t.rel, s.path, s.rel)
                cur.fetchone.return_value = (None, None, None, None)
            elif "ducklake_inlined_data_tables" in sql:
                cur.fetchall.return_value = list(inline_registry)
            elif "ducklake_inlined_data_" in sql:
                cur.fetchall.return_value = list(inline_rows or [])
            elif "ducklake_data_file" in sql:
                cur.fetchall.return_value = list(file_rows)
            return cur

        pg.execute.side_effect = execute
        reader = _make_reader(pg)
        duck = MagicMock()
        duck.execute.return_value.to_arrow_table.return_value = pa.table({"team_id": ["2"], "event": ["e"]})
        return reader, duck, pg

    def test_empty_range_short_circuits(self):
        reader, duck, pg = self._read_setup(file_rows=[(1, 5, None, 0, 10, "a.parquet", False, 100)])
        table = _make_table()
        out = reader.read(table, duck, 10, 10)
        assert out.num_rows == 0
        # No catalog work at all beyond verification.
        assert not any("ducklake_data_file" in str(c) for c in pg.execute.call_args_list)

    def test_selection_predicate_matches_extension(self):
        reader, duck, pg = self._read_setup(file_rows=[])
        reader.read(_make_table(), duck, 100, 200, filter_expr="team_id IN ('2')")
        calls = [str(c) for c in pg.execute.call_args_list]
        file_query = next(c for c in calls if "ducklake_data_file" in c)
        assert "begin_snapshot <= " in file_query
        assert "partial_max IS NOT NULL" in file_query
        # params: (table_id, hi, lo+1, lo+1)
        params = next(c.args[1] for c in pg.execute.call_args_list if "ducklake_data_file" in str(c) and c.args[1])
        assert params == (16, 200, 101, 101)

    def test_repeatable_read_wraps_catalog_queries(self):
        reader, duck, pg = self._read_setup(file_rows=[])
        reader.read(_make_table(), duck, 100, 200)
        executed = [str(c) for c in pg.execute.call_args_list]
        assert any("REPEATABLE READ" in e for e in executed)
        pg.transaction.assert_called()

    def test_plain_and_partial_files_split_and_filtered(self):
        files = [
            (1, 101, None, 0, 10, "plain.parquet", False, 100),
            (2, 50, 150, 0, 10, "merged.parquet", False, 100),
        ]
        reader, duck, pg = self._read_setup(file_rows=files)
        reader.read(_make_table(), duck, 100, 200, filter_expr="team_id IN ('2')")
        sqls = [c.args[0] for c in duck.execute.call_args_list]
        plain_sql = next(s for s in sqls if "plain.parquet" in s and "merged.parquet" not in s)
        partial_sql = next(s for s in sqls if "merged.parquet" in s)
        assert "__viaduck_snap" not in plain_sql
        assert "__viaduck_snap > 100 AND __viaduck_snap <= 200" in partial_sql
        assert "team_id IN ('2')" in partial_sql

    def test_inline_union_across_schema_versions(self):
        reader, duck, pg = self._read_setup(
            file_rows=[],
            inline_registry=[(1,), (2,)],
            inline_rows=[("2", "a"), ("2", "b")],
        )
        out = reader.read(_make_table(), duck, 100, 200)
        calls = [str(c) for c in pg.execute.call_args_list]
        assert any("ducklake_inlined_data_16_1" in c for c in calls)
        assert any("ducklake_inlined_data_16_2" in c for c in calls)
        # two stores × two rows each (the mock returns the same rows per store)
        assert out.num_rows == 4
        assert out.column_names == ["team_id", "event"]

    def test_no_files_and_no_inline_returns_empty_with_schema(self):
        reader, duck, pg = self._read_setup(file_rows=[])
        out = reader.read(_make_table(), duck, 100, 200, columns=("team_id", "event"))
        assert out.num_rows == 0
        assert out.column_names == ["team_id", "event"]
        duck.execute.assert_not_called()


class TestPathResolution:
    def test_absolute_passthrough(self):
        r = _make_reader(_mock_pg())
        assert r._resolve_path("s3://x/y.parquet", False, (None, False), (None, False)) == "s3://x/y.parquet"

    def test_relative_chain_all_levels(self):
        # file relative to table path, table relative to schema, schema
        # relative to data_path (the observed ducklake layout).
        r = _make_reader(_mock_pg())
        got = r._resolve_path("f.parquet", True, ("ev/", True), ("main/", True))
        assert got == "s3://bucket/main/ev/f.parquet"

    def test_relative_file_absolute_table_path(self):
        r = _make_reader(_mock_pg())
        got = r._resolve_path("f.parquet", True, ("s3://other/ev/", False), ("main/", True))
        assert got == "s3://other/ev/f.parquet"

    def test_relative_falls_back_to_data_path(self):
        r = _make_reader(_mock_pg())
        assert r._resolve_path("f.parquet", True, (None, False), (None, False)) == "s3://bucket/f.parquet"

    def test_relative_without_base_refuses(self):
        r = FeedReader(postgres_uri="postgresql://x", catalog_name="lake", data_path=None)
        with pytest.raises(FeedError, match="Relative data file path"):
            r._resolve_path("f.parquet", True, (None, False), (None, False))
