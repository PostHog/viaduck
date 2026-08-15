"""Golden parity: the direct-SQL feed vs ducklake's table_insertions.

Real Postgres-backed DuckLake catalog (testcontainers), real compaction
(MERGE_ADJACENT straddles), real inline stores (inlining enabled on a
second catalog). For a matrix of (lo, hi] ranges the feed's rows must equal
the extension's rows exactly — same tuples, same counts, no duplicates
across adjacent ranges.

The proposal's correctness claims (log-consumer-proposal.md §3/§6.1) stand
or fall here; unit tests in tests/unit/test_feed.py pin only SQL shape.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyducklake import Catalog
from testcontainers.postgres import PostgresContainer

from viaduck import metrics
from viaduck.feed import FeedReader


def setup_module():
    metrics.init("integration_test")


@pytest.fixture(scope="module")
def pg_dsn():
    """Keyword DSN serving both ducklake ATTACH ('ducklake:postgres:<dsn>')
    and psycopg (which accepts the keyword form natively)."""
    with PostgresContainer("postgres:16-alpine") as pg:
        yield (
            f"host={pg.get_container_host_ip()} port={pg.get_exposed_port(5432)} "
            f"dbname={pg.dbname} user={pg.username} password={pg.password}"
        )


@pytest.fixture(scope="module")
def catalog(pg_dsn, tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("feed-data")
    cat = Catalog("lake", f"postgres:{pg_dsn}", data_path=str(data_dir))
    yield cat
    cat.close()


@pytest.fixture(scope="module")
def reader(pg_dsn, catalog):
    r = FeedReader(postgres_uri=pg_dsn, catalog_name="lake", data_path=catalog._data_path)
    r.verify_catalog()
    yield r
    r.close()


def _insert(catalog: Catalog, rows: list[tuple[int, str, int]], table: str = "main.events"):
    """One commit per call → one snapshot per call."""
    vals = ", ".join(f"({t}, '{e}', {v})" for t, e, v in rows)
    catalog.connection.execute(f"INSERT INTO lake.{table} VALUES {vals}")


def _rows(tbl: pa.Table) -> set[tuple]:
    return {tuple(row[c] for c in tbl.column_names) for row in tbl.to_pylist()}


def _insertions(table, lo: int, hi: int, filter_expr=None) -> set[tuple]:
    """Extension path with the same exclusive-lo convention viaduck uses."""
    kwargs: dict = {"start_snapshot": lo + 1, "end_snapshot": hi}
    if filter_expr:
        kwargs["filter_expr"] = filter_expr
    t = table.table_insertions(**kwargs).to_arrow()
    return _rows(t)


def _feed(reader: FeedReader, conn, table, lo: int, hi: int, filter_expr=None) -> set[tuple]:
    return _rows(reader.read(table, conn, lo, hi, filter_expr=filter_expr))


@pytest.fixture(scope="module")
def table(catalog):
    conn = catalog.connection
    conn.execute("CREATE SCHEMA IF NOT EXISTS lake.main")
    conn.execute("CREATE TABLE lake.main.events (team_id BIGINT, event VARCHAR, value BIGINT)")
    tbl = catalog.load_table("main.events")
    # Snapshots 1..5 of plain ingest files.
    _insert(catalog, [(2, "a", 1), (7, "b", 2)])
    _insert(catalog, [(2, "c", 3)])
    _insert(catalog, [(50689, "d", 4), (2, "e", 5)])
    _insert(catalog, [(7, "f", 6)])
    _insert(catalog, [(2, "g", 7), (7, "h", 8), (50689, "i", 9)])
    return tbl


@pytest.fixture(scope="module")
def snaps(catalog, table):
    """Snapshot ids of the five inserts into `table` (CREATE TABLE itself is
    a snapshot, so insert k is NOT snapshot k)."""
    # The table fixture committed 5 inserts; their snapshot ids are the 5
    # most recent in the catalog. Ordered ascending.
    rows = catalog.connection.execute(
        'SELECT snapshot_id FROM "__ducklake_metadata_lake".ducklake_snapshot ORDER BY snapshot_id'
    ).fetchall()
    return [int(r[0]) for r in rows][-5:]


class TestPlainParity:
    @pytest.mark.parametrize("lo,hi", [(0, 5), (1, 4), (2, 3), (3, 5), (0, 1), (4, 5)])
    def test_ranges_match_extension(self, reader, catalog, table, lo, hi):
        assert _feed(reader, catalog.connection, table, lo, hi) == _insertions(table, lo, hi)

    def test_exclusive_lower_bound(self, reader, catalog, table, snaps):
        # Rows committed AT the cursor snapshot are never re-delivered.
        got = _feed(reader, catalog.connection, table, snaps[0], snaps[1])
        assert got == _insertions(table, snaps[0], snaps[1])
        assert (2, "a", 1) not in got  # insert 1 lives at snaps[0]

    def test_filter_expr_parity(self, reader, catalog, table):
        filt = "team_id IN (2)"
        for lo, hi in [(0, 5), (1, 4), (2, 5)]:
            assert _feed(reader, catalog.connection, table, lo, hi, filt) == _insertions(table, lo, hi, filt)

    def test_empty_range(self, reader, catalog, table):
        out = reader.read(table, catalog.connection, 5, 5)
        assert out.num_rows == 0

    def test_no_meta_columns_leak(self, reader, catalog, table):
        out = reader.read(table, catalog.connection, 0, 5)
        assert out.column_names == ["team_id", "event", "value"]


class TestMergeStraddle:
    """MERGE_ADJACENT deletes the source file rows and writes one file with
    begin=first_source, partial_max=max(sources): a range whose lo falls
    INSIDE the merged file's window must not re-deliver already-seen rows,
    and adjacent ranges must partition its rows without overlap."""

    @pytest.fixture(scope="class")
    def merged(self, catalog):
        conn = catalog.connection
        conn.execute("CREATE TABLE lake.main.merged (team_id BIGINT, event VARCHAR, value BIGINT)")
        tbl = catalog.load_table("main.merged")
        # The extension refuses to read ranges predating the table's creation
        # snapshot ("does not exist at version N") — ranges start here.
        created = tbl.current_snapshot().snapshot_id
        # This ducklake build inlines small inserts BY DEFAULT — disable it so
        # the six inserts write real parquet files (the thing we then merge).
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            for i in range(6):
                _insert(catalog, [(2, f"m{i}a", i * 10), (7, f"m{i}b", i * 10 + 1)], "main.merged")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        base = tbl.current_snapshot().snapshot_id
        # Compact ALL of it into one merged file whose window spans [1..base].
        conn.execute("CALL ducklake_merge_adjacent_files('lake')")
        after_merge = tbl.current_snapshot().snapshot_id
        assert after_merge > base  # the merge itself commits
        # Two more snapshots AFTER the merge output's window.
        _insert(catalog, [(2, "post1", 100)], "main.merged")
        _insert(catalog, [(7, "post2", 101)], "main.merged")
        return tbl, created

    def test_straddle_parity(self, reader, catalog, merged):
        tbl, created = merged
        head = tbl.current_snapshot().snapshot_id
        for lo in range(created, head - 1):
            for hi in range(lo + 1, head + 1):
                assert _feed(reader, catalog.connection, tbl, lo, hi) == _insertions(tbl, lo, hi), f"range ({lo}, {hi}]"

    def test_adjacent_ranges_partition_exactly(self, reader, catalog, merged):
        tbl, created = merged
        head = tbl.current_snapshot().snapshot_id
        whole = _feed(reader, catalog.connection, tbl, created, head)
        for cut in range(created + 1, head):
            left = _feed(reader, catalog.connection, tbl, created, cut)
            right = _feed(reader, catalog.connection, tbl, cut, head)
            assert not (left & right), f"duplicate delivery across cut at {cut}"
            assert left | right == whole, f"coverage gap across cut at {cut}"


class TestInline:
    """The inline branch: rows committed to the PG-side store (no parquet
    file) must surface in the feed with begin_snapshot attribution, and the
    flush to parquet must not re-deliver them (physical snapshot column)."""

    @pytest.fixture(scope="class")
    def inline_catalog(self, pg_dsn, tmp_path_factory):
        # A PG database hosts ONE ducklake catalog's metadata (public schema);
        # the inline test needs its own database in the same container.
        import psycopg

        admin = psycopg.connect(pg_dsn, autocommit=True)
        admin.execute("DROP DATABASE IF EXISTS feed_inline_db")
        admin.execute("CREATE DATABASE feed_inline_db")
        admin.close()
        inline_dsn = pg_dsn.replace("dbname=test", "dbname=feed_inline_db")

        data_dir = tmp_path_factory.mktemp("feed-inline-data")
        cat = Catalog("laked", f"postgres:{inline_dsn}", data_path=str(data_dir))
        conn = cat.connection
        # Enable inlining on the catalog (prod disables it at creation; the
        # branch must be correct either way).
        conn.execute("SET ducklake_default_data_inlining_row_limit = 100")
        conn.execute("CREATE SCHEMA IF NOT EXISTS laked.main")
        conn.execute("CREATE TABLE laked.main.inlined (team_id BIGINT, event VARCHAR, value BIGINT)")
        tbl = cat.load_table("main.inlined")
        _inline_insert(cat, [(2, "i1", 1), (7, "i2", 2)])
        _inline_insert(cat, [(2, "i3", 3)])
        yield cat, tbl, inline_dsn
        cat.close()

    def test_inline_rows_seen(self, inline_catalog):
        cat, tbl, dsn = inline_catalog
        head = tbl.current_snapshot().snapshot_id
        r = FeedReader(postgres_uri=dsn, catalog_name="laked", data_path=cat._data_path)
        try:
            got = _feed(r, cat.connection, tbl, 0, head)
        finally:
            r.close()
        want = _insertions(tbl, 0, head)
        assert got == want
        assert (2, "i1", 1) in got  # actually reached us (registry non-empty path)

    def test_flush_to_parquet_no_redelivery(self, inline_catalog):
        cat, tbl, dsn = inline_catalog
        r = FeedReader(postgres_uri=dsn, catalog_name="laked", data_path=cat._data_path)
        try:
            head_before = tbl.current_snapshot().snapshot_id
            pre = _feed(r, cat.connection, tbl, 0, head_before)
            cat.connection.execute(
                "CALL ducklake_flush_inlined_data('laked', schema_name := 'main', table_name := 'inlined')"
            )
            head_after = tbl.current_snapshot().snapshot_id
            # Reading the whole range after the flush yields the same set…
            post = _feed(r, cat.connection, tbl, 0, head_after)
            assert post == pre
            # …and the range covering only the flush snapshot is empty of
            # those rows (they keep their original begin_snapshot).
            if head_after > head_before:
                assert _feed(r, cat.connection, tbl, head_before, head_after) == set()
        finally:
            r.close()


def _inline_insert(catalog: Catalog, rows: list[tuple[int, str, int]]):
    vals = ", ".join(f"({t}, '{e}', {v})" for t, e, v in rows)
    catalog.connection.execute(f"INSERT INTO laked.main.inlined VALUES {vals}")
