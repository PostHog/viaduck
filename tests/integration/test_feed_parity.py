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

import collections
from unittest.mock import MagicMock

import psycopg
import pyarrow as pa
import pytest
from pyducklake import Catalog
from testcontainers.postgres import PostgresContainer

from viaduck import metrics
from viaduck.config import RoutingConfig
from viaduck.feed import FeedError, FeedReader
from viaduck.main import _poll_cycle, _ReadPool
from viaduck.router import Router


def setup_module():
    metrics.init("integration_test")


def _fresh_database(pg_dsn: str, name: str) -> str:
    """A PG database hosts ONE ducklake catalog's metadata (public schema) —
    every catalog fixture beyond the first gets its own database."""
    import psycopg

    assert "dbname=test" in pg_dsn, f"unexpected container dbname in {pg_dsn!r} — update the replace() below"
    admin = psycopg.connect(pg_dsn, autocommit=True)
    admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    admin.execute(f'CREATE DATABASE "{name}"')
    admin.close()
    return pg_dsn.replace("dbname=test", f"dbname={name}")


def _new_catalog(tmp_path_factory, name: str, db: str, *, encrypted: bool = False):
    data_dir = tmp_path_factory.mktemp(f"feed-{name}-data")
    return Catalog(name, f"postgres:{db}", data_path=str(data_dir), encrypted=encrypted)


def _head_snapshot(catalog: Catalog) -> int:
    """Catalog head snapshot id (direct metadata read — never
    table.snapshots(), the OOM-pattern the app avoids)."""
    name = f"__ducklake_metadata_{catalog.name}".replace('"', '""')
    row = catalog.connection.execute(f'SELECT MAX(snapshot_id) FROM "{name}".ducklake_snapshot').fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _new_reader(catalog: Catalog, dsn: str) -> FeedReader:
    r = FeedReader(postgres_uri=dsn, catalog_name=catalog.name, data_path=catalog._data_path)
    r.verify_catalog()
    return r


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


def _rows(tbl: pa.Table) -> collections.Counter:
    """MULTISET of row tuples — parity is a claim about counts too; a plain
    set would let the feed double-deliver a row within one range unnoticed."""
    return collections.Counter(tuple(row[c] for c in tbl.column_names) for row in tbl.to_pylist())


def _insertions(table, lo: int, hi: int, filter_expr=None) -> set[tuple]:
    """Extension path with the same exclusive-lo convention viaduck uses."""
    kwargs: dict = {"start_snapshot": lo + 1, "end_snapshot": hi}
    if filter_expr:
        kwargs["filter_expr"] = filter_expr
    t = table.table_insertions(**kwargs).to_arrow()
    return _rows(t)


def _feed(reader: FeedReader, conn, table, lo: int, hi: int, filter_expr=None) -> set[tuple]:
    return _rows(reader.read(table, conn, lo, hi, filter_expr=filter_expr))


_EVENT_SNAPS: list[int] = []


@pytest.fixture(scope="module")
def table(catalog):
    conn = catalog.connection
    conn.execute("CREATE SCHEMA IF NOT EXISTS lake.main")
    conn.execute("CREATE TABLE lake.main.events (team_id BIGINT, event VARCHAR, value BIGINT)")
    tbl = catalog.load_table("main.events")
    _EVENT_SNAPS.clear()
    for rows in [
        [(2, "a", 1), (7, "b", 2)],
        [(2, "c", 3)],
        [(50689, "d", 4), (2, "e", 5)],
        [(7, "f", 6)],
        [(2, "g", 7), (7, "h", 8), (50689, "i", 9)],
    ]:
        _insert(catalog, rows)
        # Captured at insert time, so later test classes committing into the
        # shared catalog cannot shift these (qe: fixture-order trap).
        _EVENT_SNAPS.append(tbl.current_snapshot().snapshot_id)
    return tbl


@pytest.fixture(scope="module")
def snaps(table):
    """Snapshot ids of the five inserts into `table` (CREATE TABLE itself
    is a snapshot, so insert k is NOT snapshot k)."""
    return list(_EVENT_SNAPS)


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
            assert left + right == whole, f"partition broken across cut at {cut}"


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

    def test_inline_rows_filtered(self, inline_catalog):
        """The routing filter applies to inline rows — an unfiltered inline
        read is a cross-tenant data leak, not a perf bug."""
        cat, tbl, dsn = inline_catalog
        head = tbl.current_snapshot().snapshot_id
        r = FeedReader(postgres_uri=dsn, catalog_name="laked", data_path=cat._data_path)
        try:
            got = _feed(r, cat.connection, tbl, 0, head, "team_id IN (2)")
            want = _insertions(tbl, 0, head, "team_id IN (2)")
        finally:
            r.close()
        assert got == want
        assert (2, "i1", 1) in got and (7, "i2", 2) not in got

    def test_inline_add_column_null_fill_parity(self, inline_catalog):
        """Restart-behind-the-boundary for the inline leg: a reader pinning
        the NEW column set reads ranges whose inline stores predate the ADD —
        v1-store rows NULL-fill, and the result matches the extension's
        field-id-mapped output exactly. (Distinguishing coverage for the
        per-store projection rewrite — before it, this wedged on
        UndefinedColumn.)"""
        cat, _tbl, dsn = inline_catalog
        conn = cat.connection
        conn.execute("CREATE TABLE laked.main.inlined_evol (team_id BIGINT, event VARCHAR, value BIGINT)")
        conn.execute("INSERT INTO laked.main.inlined_evol VALUES (2, 'old', 1)")
        conn.execute("ALTER TABLE laked.main.inlined_evol ADD COLUMN extra BIGINT")
        conn.execute("INSERT INTO laked.main.inlined_evol VALUES (2, 'new', 2, 42)")
        tbl2 = cat.load_table("main.inlined_evol")
        head = tbl2.current_snapshot().snapshot_id
        r = FeedReader(postgres_uri=dsn, catalog_name="laked", data_path=cat._data_path)
        try:
            got = r.read(tbl2, conn, 0, head)
        finally:
            r.close()
        ext = tbl2.table_insertions(start_snapshot=1, end_snapshot=head).to_arrow()
        assert _rows(got) == _rows(ext)
        assert got.column_names == ["team_id", "event", "value", "extra"]
        # the v1 row carries a real typed NULL, not an absent column
        old_row = next(r for r in got.to_pylist() if r["event"] == "old")
        assert old_row["extra"] is None

    def test_inline_rename_raises_with_rows_present(self, inline_catalog):
        """The rename signature with rows in BOTH stores — the destructive
        case must never silently NULL-fill."""
        cat, _tbl, dsn = inline_catalog
        conn = cat.connection
        conn.execute("CREATE TABLE laked.main.inlined_ren (team_id BIGINT, event VARCHAR)")
        conn.execute("INSERT INTO laked.main.inlined_ren VALUES (2, 'x')")
        conn.execute("ALTER TABLE laked.main.inlined_ren RENAME COLUMN event TO event_renamed")
        conn.execute("INSERT INTO laked.main.inlined_ren VALUES (2, 'y')")
        tbl2 = cat.load_table("main.inlined_ren")
        head = tbl2.current_snapshot().snapshot_id
        r = FeedReader(postgres_uri=dsn, catalog_name="laked", data_path=cat._data_path)
        try:
            with pytest.raises(FeedError, match="non-additive"):
                r.read(tbl2, conn, 0, head)
        finally:
            r.close()

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
            # those rows (they keep their original begin_snapshot). The flush
            # MUST have committed — a no-op flush must not pass this test.
            assert head_after > head_before, "flush produced no snapshot; test is vacuous"
            assert _feed(r, cat.connection, tbl, head_before, head_after) == collections.Counter()
        finally:
            r.close()


def _inline_insert(catalog: Catalog, rows: list[tuple[int, str, int]]):
    vals = ", ".join(f"({t}, '{e}', {v})" for t, e, v in rows)
    catalog.connection.execute(f"INSERT INTO laked.main.inlined VALUES {vals}")

    def test_variant_inline_plane(self, inline_catalog):
        """VARIANT with inlining active: the inline store carries the variant
        column, the feed's pinned projection reads inline rows without
        touching it (mixed-plane correctness, not just parquet)."""
        cat, _tbl, dsn = inline_catalog
        conn = cat.connection
        conn.execute(
            "CREATE TABLE laked.main.inlined_variant (team_id BIGINT, event VARCHAR, properties_variant VARIANT)"
        )
        conn.execute("INSERT INTO laked.main.inlined_variant VALUES (2, 'i1', {\"k\": 9}), (7, 'i2', NULL)")
        tbl2 = cat.load_table("main.inlined_variant")
        head = tbl2.current_snapshot().snapshot_id
        r = FeedReader(postgres_uri=dsn, catalog_name="laked", data_path=cat._data_path)
        try:
            got = r.read(tbl2, conn, 0, head, columns=("team_id", "event"))
        finally:
            r.close()
        assert _rows(got) == collections.Counter({(2, "i1"): 1, (7, "i2"): 1})


class TestForeignSnapshots:
    """Other tables' commits interleave (snapshot ids are global); the feed
    must neither skip nor double-read across the noise."""

    def test_interleaved_noise(self, reader, catalog, table, snaps):
        conn = catalog.connection
        conn.execute("CREATE TABLE IF NOT EXISTS lake.main.noise (x BIGINT)")
        # Noise commits BETWEEN events reads.
        conn.execute("INSERT INTO lake.main.noise VALUES (1), (2)")
        conn.execute("INSERT INTO lake.main.noise VALUES (3)")
        lo, hi = snaps[1], snaps[3]
        assert _feed(reader, catalog.connection, table, lo, hi) == _insertions(table, lo, hi)
        # A range of pure noise for the events table: empty on both paths.
        noise_head = catalog.load_table("main.noise").current_snapshot().snapshot_id
        assert _feed(reader, catalog.connection, table, snaps[-1], noise_head) == _insertions(
            table, snaps[-1], noise_head
        )


class TestTypeFidelity:
    """Type/values matrix through BOTH the parquet and inline read paths —
    the inline path converts PG→psycopg→Arrow and is the riskier one."""

    TYPES_DDL = (
        "id BIGINT, s VARCHAR, f DOUBLE, b BOOLEAN, ts TIMESTAMP, "
        "tstz TIMESTAMPTZ, d DATE, dec DECIMAL(10,2), i INTEGER"
    )
    ROWS = [
        (1, "plain", 1.5, True, "2026-01-01 10:00:00", "2026-01-01 10:00:00+00", "2026-01-01", 1.25, 7),
        (
            2,
            "unicode-ü-emoji-🦆",
            -0.0,
            False,
            "2026-06-30 23:59:59",
            "2026-06-30 23:59:59+00",
            "2026-06-30",
            -99.99,
            -3,
        ),
        (3, "quote'inside", 3.25, None, None, None, None, None, None),
        (4, 'double"quote', 4.5, True, "2026-08-15 00:00:00", "2026-08-15 00:00:00+00", "2026-08-15", 0.01, 0),
    ]

    def _build(self, catalog, table_name, inlining: bool):
        conn = catalog.connection
        conn.execute(f"SET ducklake_default_data_inlining_row_limit = {100 if inlining else 0}")
        try:
            conn.execute(f"CREATE TABLE lake.main.{table_name} ({self.TYPES_DDL})")
            for r in self.ROWS:
                esc_s = r[1].replace("'", "''")
                vals = (
                    f"({r[0]}, '{esc_s}', {r[2]}, "
                    + ("NULL" if r[3] is None else str(r[3]))
                    + ", "
                    + (f"'{r[4]}'" if r[4] else "NULL")
                    + ", "
                    + (f"'{r[5]}'::TIMESTAMPTZ" if r[5] else "NULL")
                    + ", "
                    + (f"'{r[6]}'::DATE" if r[6] else "NULL")
                    + ", "
                    + (f"{r[7]}::DECIMAL(10,2)" if r[7] is not None else "NULL")
                    + f", {r[8] if r[8] is not None else 'NULL'})"
                )
                conn.execute(f"INSERT INTO lake.main.{table_name} VALUES {vals}")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        return catalog.load_table(f"main.{table_name}")

    @pytest.mark.parametrize("inlining", [True, False], ids=["inline", "parquet"])
    def test_type_fidelity(self, reader, catalog, inlining):
        name = "types_inline" if inlining else "types_parquet"
        tbl = self._build(catalog, name, inlining)
        head = tbl.current_snapshot().snapshot_id
        got = _feed(reader, catalog.connection, tbl, 0, head)
        want = _insertions(tbl, 0, head)
        assert got == want
        # Non-vacuous: every fixture row present.
        assert sum(got.values()) == len(self.ROWS)


class TestVariantFeed:
    def test_variant_column_never_projected(self, reader, catalog):
        """The production VARIANT shape on the feed path: the pinned
        projection excludes the unrepresentable column (load_table's
        EXCLUDABLE_SOURCE_TYPES), so the read never touches it — a source
        with VARIANT columns must not stall or fail the feed."""
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute(
                "CREATE TABLE lake.main.with_variant (team_id BIGINT, event VARCHAR, properties_variant VARIANT)"
            )
            created = _head_snapshot(catalog)
            conn.execute("INSERT INTO lake.main.with_variant VALUES (2, 'a', {\"k\": 1}), (7, 'b', NULL)")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        # load_table excludes the VARIANT column (the fleet's behavior) —
        # the feed reads the surviving projection.
        from viaduck.source import load_table, replicated_column_names

        tbl = load_table(catalog, "main.with_variant")
        pinned = replicated_column_names(tbl)
        assert "properties_variant" not in pinned
        head = tbl.current_snapshot().snapshot_id
        got = reader.read(tbl, catalog.connection, created, head, columns=pinned)
        assert sorted(got.column("event").to_pylist()) == ["a", "b"]
        assert "properties_variant" not in got.column_names

    def test_variant_survives_merge(self, reader, catalog):
        """A compaction merge rewrites variant-bearing files end-to-end; the
        feed's pinned projection must read the merged output exactly. (The
        merge runs through the extension's writer — this pins that its
        output files keep the pinned columns readable, variant intact
        notwithstanding.)"""
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute(
                "CREATE TABLE lake.main.variant_merge (team_id BIGINT, event VARCHAR, properties_variant VARIANT)"
            )
            created = _head_snapshot(catalog)
            conn.execute("INSERT INTO lake.main.variant_merge VALUES (2, 'm1', {\"k\": 1})")
            conn.execute("INSERT INTO lake.main.variant_merge VALUES (2, 'm2', NULL), (7, 'm3', {\"k\": 3})")
            conn.execute("CALL ducklake_merge_adjacent_files('lake')")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        from viaduck.source import load_table, replicated_column_names

        tbl = load_table(catalog, "main.variant_merge")
        pinned = replicated_column_names(tbl)
        head = tbl.current_snapshot().snapshot_id
        got = reader.read(tbl, catalog.connection, created, head, columns=pinned)
        want = tbl.table_insertions(start_snapshot=created + 1, end_snapshot=head, columns=pinned).to_arrow()
        assert _rows(got) == _rows(want)
        assert sum(_rows(got).values()) == 3


class TestSchemaEvolution:
    """Pinned projection (startup schema) over evolving source files; a
    rename fails LOUDLY (no mapping_id support) — never silently NULL."""

    def test_add_column_with_pinned_projection(self, reader, catalog):
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute("CREATE TABLE lake.main.evol (team_id BIGINT, event VARCHAR, value BIGINT)")
            created = _head_snapshot(catalog)
            conn.execute("INSERT INTO lake.main.evol VALUES (2, 'before', 1)")
            conn.execute("ALTER TABLE lake.main.evol ADD COLUMN extra VARCHAR")
            conn.execute("INSERT INTO lake.main.evol VALUES (2, 'after', 2, 'yes'), (7, 'after7', 3, 'yes')")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        tbl = catalog.load_table("main.evol")
        head = tbl.current_snapshot().snapshot_id
        # Startup-pinned projection (the production shape): reads fine.
        pinned = ("team_id", "event", "value")
        got = reader.read(tbl, catalog.connection, created, head, columns=pinned)
        assert _rows(got) == _rows(
            tbl.table_insertions(start_snapshot=created + 1, end_snapshot=head, columns=pinned).to_arrow()
        )
        assert sum(_rows(got).values()) == 3

    def test_rename_fails_loudly(self, reader, catalog):
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute("CREATE TABLE lake.main.renamed (team_id BIGINT, event VARCHAR)")
            renamed_created = _head_snapshot(catalog)
            conn.execute("INSERT INTO lake.main.renamed VALUES (2, 'x')")
            conn.execute("ALTER TABLE lake.main.renamed RENAME COLUMN event TO event_renamed")
            conn.execute("INSERT INTO lake.main.renamed VALUES (2, 'y')")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        tbl = catalog.load_table("main.renamed")
        head = tbl.current_snapshot().snapshot_id
        # The extension maps old files' columns by field id; the feed does
        # not (yet). Pin the loud failure — never silent NULLs.
        import duckdb
        import psycopg

        with pytest.raises((duckdb.Error, psycopg.Error, FeedError)) as exc_info:
            reader.read(tbl, catalog.connection, renamed_created, head)
        assert "event_renamed" in str(exc_info.value)


class TestAddDataFiles:
    def test_registered_external_file_is_delivered(self, reader, catalog, tmp_path):
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute("CREATE TABLE lake.main.ext (team_id BIGINT, event VARCHAR, value BIGINT)")
            conn.execute("INSERT INTO lake.main.ext VALUES (2, 'native', 1)")
            tbl = catalog.load_table("main.ext")
            before = tbl.current_snapshot().snapshot_id

            import pyarrow.parquet as pq

            ext_path = str(tmp_path / "external.parquet")
            pq.write_table(
                pa.table({"team_id": [2, 7], "event": ["imported", "imported7"], "value": [10, 11]}),
                ext_path,
            )
            tbl.add_files(ext_path)
            head = tbl.current_snapshot().snapshot_id
            assert head > before
            got = _feed(reader, catalog.connection, tbl, before, head)
            want = _insertions(tbl, before, head)
            assert got == want
            assert (2, "imported", 10) in got
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")


class TestEncryptionRefusal:
    def test_encrypted_catalog_refused_loudly(self, pg_dsn, tmp_path_factory):
        enc_db = _fresh_database(pg_dsn, "feed_enc_db")
        cat = _new_catalog(tmp_path_factory, "enclake", enc_db, encrypted=True)
        try:
            cat.connection.execute("CREATE TABLE enclake.main.t (x BIGINT)")
            r = FeedReader(postgres_uri=enc_db, catalog_name="enclake", data_path=cat._data_path)
            with pytest.raises(FeedError, match="encrypted"):
                r.verify_catalog()
        finally:
            cat.close()


class TestMissingFileDrill:
    """Plan/execute skew: a merge+cleanup committing between the catalog
    plan and the parquet GET. The feed must re-plan once and recover; a file
    missing WITHOUT a catalog change must propagate loudly after one
    re-plan."""

    def _files_table(self, catalog, name):
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute(f"CREATE TABLE lake.main.{name} (team_id BIGINT, event VARCHAR, value BIGINT)")
            created = _head_snapshot(catalog)
            for i in range(4):
                _insert(catalog, [(2, f"d{i}", i)], f"main.{name}")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        return catalog.load_table(f"main.{name}"), created

    def test_merge_skew_recovers(self, pg_dsn, catalog):
        tbl, fresh_created = self._files_table(catalog, "drill_recover")
        fresh = _new_reader(catalog, pg_dsn)
        try:
            orig_plan = fresh._plan
            state = {"armed": True}

            def stale_plan(*args, **kwargs):
                plan = orig_plan(*args, **kwargs)
                if state["armed"]:
                    state["armed"] = False
                    # The merge commits (deletes the listed source rows) and
                    # cleanup deletes the physical files — between plan & GET.
                    catalog.connection.execute("CALL ducklake_merge_adjacent_files('lake')")
                    catalog.connection.execute(
                        "CALL ducklake_cleanup_old_files('lake', older_than := '2100-01-01 00:00:00')"
                    )
                return plan

            fresh._plan = stale_plan
            head = tbl.current_snapshot().snapshot_id
            created = fresh_created
            from viaduck import metrics as m

            before = m.cdc_feed_replans_total._value.get()
            got = fresh.read(tbl, catalog.connection, created, head)
            want = _insertions(tbl, created, head)
            assert _rows(got) == want
            assert m.cdc_feed_replans_total._value.get() == before + 1  # the re-plan actually fired
        finally:
            fresh.close()

    def test_untracked_missing_file_propagates(self, pg_dsn, catalog):
        import duckdb

        tbl, fresh_created = self._files_table(catalog, "drill_propagate")
        fresh = _new_reader(catalog, pg_dsn)
        try:
            head = tbl.current_snapshot().snapshot_id
            file_rows, _inline, path_info = fresh._plan(
                tbl, "main", "drill_propagate", fresh_created, head, ("team_id", "event", "value"), None
            )
            # Delete the physical file WITHOUT a catalog change: the re-plan
            # still lists it, the second GET must fail, and the error
            # propagates (no silent skip).
            victim = file_rows[0]
            path = fresh._resolve_path(victim[4], victim[5], *path_info)
            import os

            os.remove(path)
            with pytest.raises(duckdb.IOException):
                fresh.read(tbl, catalog.connection, fresh_created, head)
        finally:
            fresh.close()
            # Don't leave the dangling catalog row for other tests' merges.
            catalog.connection.execute("DROP TABLE IF EXISTS lake.main.drill_propagate")


class TestTorture:
    """Deterministic-seed interleave of plain inserts, inline commits,
    flushes, merges, and foreign-table noise; then the full O(n²) range
    matrix + filter matrix + adjacent-range partition exactness."""

    def test_torture_parity(self, pg_dsn, reader, catalog):
        import random

        rng = random.Random(20260815)
        conn = catalog.connection
        conn.execute("CREATE TABLE IF NOT EXISTS lake.main.torture_noise (x BIGINT)")
        conn.execute("CREATE TABLE lake.main.torture (team_id BIGINT, event VARCHAR, value BIGINT)")
        created = _head_snapshot(catalog)
        tbl = catalog.load_table("main.torture")
        teams = [2, 7, 50689, 23104, 81505]
        n = 0
        for round_ in range(30):
            inlining = rng.random() < 0.5
            conn.execute(f"SET ducklake_default_data_inlining_row_limit = {100 if inlining else 0}")
            try:
                for _ in range(rng.randint(0, 3)):
                    rows = [(rng.choice(teams), f"r{round_}_{n + k}", n + k) for k in range(rng.randint(1, 4))]
                    _insert(catalog, rows, "main.torture")
                    n += len(rows)
                conn.execute("INSERT INTO lake.main.torture_noise VALUES (1)")
                if round_ % 5 == 4:
                    conn.execute(
                        "CALL ducklake_flush_inlined_data('lake', schema_name := 'main', table_name := 'torture')"
                    )
                if round_ % 7 == 6:
                    conn.execute("CALL ducklake_merge_adjacent_files('lake')")
            finally:
                conn.execute("RESET ducklake_default_data_inlining_row_limit")
        conn.execute("CALL ducklake_flush_inlined_data('lake', schema_name := 'main', table_name := 'torture')")
        conn.execute("CALL ducklake_merge_adjacent_files('lake')")

        head = tbl.current_snapshot().snapshot_id
        assert head - created > 30  # the torture actually committed

        whole = _feed(reader, catalog.connection, tbl, created, head)
        assert sum(whole.values()) == n
        assert whole == _insertions(tbl, created, head)

        for lo in range(created, head - 1, 3):
            for hi in range(lo + 1, head + 1, 3):
                assert _feed(reader, catalog.connection, tbl, lo, hi) == _insertions(tbl, lo, hi), f"({lo}, {hi}]"

        for cut in range(created + 1, head):
            left = _feed(reader, catalog.connection, tbl, created, cut)
            right = _feed(reader, catalog.connection, tbl, cut, head)
            assert left + right == whole, f"partition broken at {cut}"

        for team in teams:
            filt = f"team_id IN ({team})"
            assert _feed(reader, catalog.connection, tbl, created, head, filt) == _insertions(tbl, created, head, filt)


class TestSortedTable:
    """Sorted tables flush inline rows in SORT order (file position ≠ rowid
    order) — the feed never does rowid arithmetic, so parity must hold."""

    def test_sorted_table_parity(self, pg_dsn, reader, catalog):
        conn = catalog.connection
        conn.execute("CREATE TABLE lake.main.sorted (team_id BIGINT, event VARCHAR, value BIGINT)")
        conn.execute("ALTER TABLE lake.main.sorted SET SORTED BY (value DESC)")
        created = _head_snapshot(catalog)
        tbl = catalog.load_table("main.sorted")
        for i in range(4):
            _insert(catalog, [(2, f"s{i}", 100 - i), (7, f"t{i}", i)], "main.sorted")
        # Flush (sorted, physical snapshot/rowid columns) then merge.
        conn.execute("CALL ducklake_flush_inlined_data('lake', schema_name := 'main', table_name := 'sorted')")
        conn.execute("CALL ducklake_merge_adjacent_files('lake')")
        head = tbl.current_snapshot().snapshot_id
        assert _feed(reader, catalog.connection, tbl, created, head) == _insertions(tbl, created, head)
        for lo in range(created, head - 1):
            for hi in range(lo + 1, head + 1):
                assert _feed(reader, catalog.connection, tbl, lo, hi) == _insertions(tbl, lo, hi), f"({lo}, {hi}]"


class TestFilterAcrossBuckets:
    """filter_expr must hold in ALL THREE scan buckets — plain parquet,
    true-straddle (two-sided-filtered), and inline (covered in TestInline).
    The M1 leak shipped in exactly one bucket while the others were fine."""

    def test_plain_parquet_filter(self, pg_dsn, reader, catalog):
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute("CREATE TABLE lake.main.pf (team_id BIGINT, event VARCHAR, value BIGINT)")
            created = _head_snapshot(catalog)
            _insert(catalog, [(2, "keep", 1), (7, "drop", 2)], "main.pf")
            _insert(catalog, [(2, "keep2", 3)], "main.pf")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        tbl = catalog.load_table("main.pf")
        head = tbl.current_snapshot().snapshot_id
        filt = "team_id IN (2)"
        got = reader.read(tbl, catalog.connection, created, head, filter_expr=filt)
        want = _insertions(tbl, created, head, filt)
        assert _rows(got) == want
        assert _rows(got).get((7, "drop", 2)) is None  # filtered out
        assert _rows(got).get((2, "keep", 1)) == 1  # present once

    def test_straddle_filter(self, pg_dsn, reader, catalog):
        """A merged file read mid-window WITH a routing filter: the two
        filters must compose (snapshot window AND team)."""
        conn = catalog.connection
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute("CREATE TABLE lake.main.sf (team_id BIGINT, event VARCHAR, value BIGINT)")
            created = _head_snapshot(catalog)
            for i in range(6):
                _insert(catalog, [(2, f"a{i}", i), (7, f"b{i}", i)], "main.sf")
            conn.execute("CALL ducklake_merge_adjacent_files('lake')")
            merged_snap = _head_snapshot(catalog)
            _insert(catalog, [(2, "post", 100), (7, "post7", 101)], "main.sf")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        tbl = catalog.load_table("main.sf")
        head = _head_snapshot(catalog)
        filt = "team_id IN (2)"
        # Windows straddling the merged file's range, with the team filter.
        for lo in range(created, head - 1):
            for hi in range(lo + 1, head + 1):
                got = _feed(reader, catalog.connection, tbl, lo, hi, filt)
                want = _insertions(tbl, lo, hi, filt)
                assert got == want, f"({lo}, {hi}] filter"
                assert all(t[0] == 2 for t in got), f"filter leak in ({lo}, {hi}]"
        assert merged_snap > created + 6  # the merge committed (non-vacuous)


class TestMixedPlaneFidelity:
    """One read spanning BOTH planes (inline rows + parquet files) — the
    shape that exposes cross-plane type/tz divergence (a UTC-pinned inline
    column concat'd with a session-tz parquet column)."""

    def test_mixed_inline_and_parquet_with_timestamps(self, pg_dsn, reader, catalog):
        conn = catalog.connection
        conn.execute(
            "CREATE TABLE lake.main.mixed (team_id BIGINT, s VARCHAR, tstz TIMESTAMPTZ, d DATE, dec DECIMAL(10,2))"
        )
        created = _head_snapshot(catalog)
        tbl = catalog.load_table("main.mixed")
        # Rows 1-2 stay inline (default limit > 0 in this build)…
        conn.execute("SET ducklake_default_data_inlining_row_limit = 100")
        try:
            conn.execute(
                "INSERT INTO lake.main.mixed VALUES "
                "(2, 'inlined', '2026-03-01 12:00:00+00', '2026-03-01', 1.50), "
                "(7, 'inlined7', '2026-03-01 13:00:00+00', '2026-03-02', 2.25)"
            )
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        # …rows 3-4 are real parquet files.
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            conn.execute(
                "INSERT INTO lake.main.mixed VALUES (2, 'filed', '2026-03-03 09:00:00+00', '2026-03-03', 3.75)"
            )
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        head = _head_snapshot(catalog)
        got = reader.read(tbl, catalog.connection, created, head)  # raises if planes' tz diverge
        want = _insertions(tbl, created, head)
        assert _rows(got) == want
        assert sum(_rows(got).values()) == 3
        # The parquet leg must have been normalized to the pinned UTC schema.
        assert got.column("tstz").type == pa.timestamp("us", tz="UTC")


class TestPollCycleWithFeed:
    """The M4 loop end-to-end locally: _poll_cycle driving the real feed
    reader + read pool against a PG-backed source — cluster planning, unit
    reads, per-row attribution, slicing, and buffering into a real
    DeliveryManager (mocked pool/state so no destination writes land)."""

    def test_poll_cycle_feed_end_to_end(self, pg_dsn, reader, catalog):

        conn = catalog.connection
        conn.execute("CREATE TABLE IF NOT EXISTS lake.main.e2e (team_id BIGINT, event VARCHAR, value BIGINT)")
        created = _head_snapshot(catalog)
        tbl = catalog.load_table("main.e2e")
        for i in range(6):
            _insert(catalog, [(2, f"e{i}a", i), (7, f"e{i}b", i)], "main.e2e")
        head = _head_snapshot(catalog)
        assert head - created >= 6

        router = Router(RoutingConfig(field="team_id", mode="append_only", key_columns=[]))

        delivery = MagicMock()
        buffered: list = []
        positions = {"dest-a": created, "dest-b": created}

        def fake_buffer(dest_id, batch, through, epoch=None, hi=None):
            buffered.append((dest_id, batch.num_rows, through, hi, batch.column("team_id").to_pylist()))

        delivery.buffer.side_effect = fake_buffer
        delivery.advance_position.side_effect = lambda d, s, epoch=None: None
        delivery.read_plan.side_effect = lambda: {d: (positions[d], 0) for d in positions}
        delivery.should_pause_all_reads.return_value = False
        delivery.should_pause_reads_for.return_value = False
        delivery.maybe_flush.return_value = 0
        delivery.flushed_snapshots.return_value = dict(positions)
        delivery.status_snapshot.return_value = {}
        delivery.active_ids.return_value = set(positions)

        cfg = MagicMock()
        cfg.source.name = "lake"
        cfg.source.table = "main.e2e"
        cfg.routing.field = "team_id"
        cfg.routing.mode = "append_only"
        cfg.poll.interval_seconds = 5.0
        cfg.poll.read_unit_max_rows = 50_000
        cfg.poll.read_unit_max_bytes = 256 * 1024 * 1024
        cfg.poll.read_unit_max_span = 10_000
        cfg.poll.read_workers = 4
        cfg.poll.read_unit_timeout_seconds = 300.0
        cfg.destinations = []
        dest_a = MagicMock(id="dest-a", routing_value="2")
        dest_b = MagicMock(id="dest-b", routing_value="7")
        cfg.destinations = [dest_a, dest_b]
        cfg.destination_by_id = lambda d: {"dest-a": dest_a, "dest-b": dest_b}[d]

        pool = _ReadPool({}, 2)
        try:
            from unittest.mock import patch as _patch

            with _patch("viaduck.main._log_memory_stats"):
                _poll_cycle(
                    tbl,
                    delivery,
                    MagicMock(),
                    router,
                    cfg,
                    ["dest-a", "dest-b"],
                    {"2": "dest-a", "7": "dest-b"},
                    [],
                    "append_only",
                    source_columns=("team_id", "event", "value"),
                    feed_reader=reader,
                    read_pool=pool,
                )
        finally:
            pool.close()

        # Every source row landed exactly once across the two buffers.
        total = sum(n for _, n, _, _, _ in buffered)
        assert total == 12
        teams_a = [t for d, _, _, _, ts in buffered if d == "dest-a" for t in ts]
        teams_b = [t for d, _, _, _, ts in buffered if d == "dest-b" for t in ts]
        assert set(teams_a) == {2} and set(teams_b) == {7}
        # Cov chains are sane: non-decreasing per destination, last == unit hi.
        for dest in ("dest-a", "dest-b"):
            covs = [c for d, _, c, _, _ in buffered if d == dest]
            assert covs == sorted(covs) and covs and covs[-1] == head


class TestPollCycleFanInAndSlicing:
    """QE/SWE M5-gating coverage: cluster fan-in with DIVERGENT positions
    over the real feed + slicing through a real DeliveryManager's buffer
    chain (cov chains asserted at the _Buffer level)."""

    def test_divergent_positions_and_slicing(self, pg_dsn, catalog):
        conn = catalog.connection
        conn.execute("CREATE TABLE IF NOT EXISTS lake.main.fanin (team_id BIGINT, event VARCHAR, value BIGINT)")
        tbl = catalog.load_table("main.fanin")
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        try:
            for i in range(8):
                _insert(catalog, [(2, f"a{i}", i), (7, f"b{i}", i)], "main.fanin")
        finally:
            conn.execute("RESET ducklake_default_data_inlining_row_limit")
        head = _head_snapshot(catalog)
        snaps = [
            r[0]
            for r in psycopg.connect(pg_dsn, autocommit=True)
            .execute(
                "SELECT df.begin_snapshot FROM public.ducklake_data_file df "
                "JOIN public.ducklake_table t ON t.table_id = df.table_id "
                "WHERE t.table_name='fanin' ORDER BY df.begin_snapshot"
            )
            .fetchall()
        ]
        assert len(snaps) == 8

        from viaduck.config import DeliveryConfig
        from viaduck.delivery import DeliveryManager

        sm = MagicMock()
        cursor = MagicMock()
        cursor.last_snapshot_id = snaps[1]  # destinations start here
        cursor.rows_replicated = 0
        cursor.last_error = None
        sm.load_cursors.return_value = {"dest-a": cursor, "dest-b": cursor}
        delivery = DeliveryManager(
            DeliveryConfig(workers=1, flush_interval_seconds=3600.0),
            sm,
            MagicMock(),
            [],
            ["dest-a", "dest-b"],
            mode="append_only",
        )
        # dest-b is AHEAD: at snaps[5] (its read position has already
        # covered the first four commits).
        delivery._position["dest-b"] = snaps[5]

        cfg = MagicMock()
        cfg.source.name = "lake"
        cfg.source.table = "main.fanin"
        cfg.routing.field = "team_id"
        cfg.routing.mode = "append_only"
        cfg.poll.interval_seconds = 5.0
        cfg.poll.read_unit_max_rows = 3  # force slicing
        cfg.poll.read_unit_max_bytes = 256 * 1024 * 1024
        cfg.poll.read_unit_max_span = 10_000
        cfg.poll.read_workers = 2
        cfg.poll.read_unit_timeout_seconds = 300.0
        dest_a = MagicMock(id="dest-a", routing_value="2")
        dest_b = MagicMock(id="dest-b", routing_value="7")
        cfg.destinations = [dest_a, dest_b]
        cfg.destination_by_id = lambda d: {"dest-a": dest_a, "dest-b": dest_b}[d]

        reader = FeedReader(postgres_uri=pg_dsn, catalog_name="lake", data_path=catalog._data_path)
        pool2 = _ReadPool({}, 2)
        from unittest.mock import patch as _patch

        # One unit per cluster per cycle — catch-up is a cycle loop. Drive
        # cycles until both destinations' positions reach head.
        try:
            with _patch("viaduck.main._log_memory_stats"):
                for _cycle in range(20):
                    _poll_cycle(
                        tbl,
                        delivery,
                        MagicMock(),
                        Router(RoutingConfig(field="team_id", mode="append_only", key_columns=[])),
                        cfg,
                        ["dest-a", "dest-b"],
                        {"2": "dest-a", "7": "dest-b"},
                        [],
                        "append_only",
                        source_columns=("team_id", "event", "value"),
                        feed_reader=reader,
                        read_pool=pool2,
                    )
                    if all(delivery._position[d] >= head for d in ("dest-a", "dest-b")):
                        break
                else:
                    raise AssertionError("fleet did not converge in 20 cycles")
        finally:
            pool2.close()
            reader.close()

        # Each commit carries 1 row per team. dest-a (started at snaps[1]):
        # commits 3..9 → 6 team-2 rows. dest-b (started at snaps[5]): commits
        # 8,9 → 2 team-7 rows — the mask kept it from re-reading its range.
        entries_a = delivery._buffers["dest-a"].entries
        entries_b = delivery._buffers["dest-b"].entries
        assert sum(t.num_rows for t, _, _ in entries_a) == 6
        assert sum(t.num_rows for t, _, _ in entries_b) == 2
        covs_a = [cov for _, cov, _ in entries_a]
        assert covs_a == sorted(covs_a) and covs_a[-1] >= snaps[-1]
        covs_b = [cov for _, cov, _ in entries_b]
        assert covs_b and covs_b[0] >= snaps[5]
        # dest-a read 3 units (each 2 rows ≤ max_rows=3 → one entry each).
        assert len(entries_a) >= 3, f"expected one entry per unit, got {len(entries_a)}"
