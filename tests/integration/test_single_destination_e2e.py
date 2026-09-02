"""End-to-end: the per-destination duckling against real catalogs.

PG testcontainer holds both ducklake catalogs (source + destination) and
the viaduck_state cursor table on the SOURCE database (the colocated
assumption, per-destination-duckling.md §10). Parquet data lives in
tmp_path. These tests exercise the wrapper's loop against real metadata —
the read core itself is pinned by test_feed_parity.py.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from pyducklake import Catalog
from testcontainers.postgres import PostgresContainer

from viaduck import metrics
from viaduck.single_destination import Duckling, DucklingConfig, FatalDucklingError


def setup_module():
    metrics.init("duckling_integration_test")


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield (
            f"host={pg.get_container_host_ip()} port={pg.get_exposed_port(5432)} "
            f"dbname={pg.dbname} user={pg.username} password={pg.password}"
        )


def _mkdb(pg_dsn: str, name: str) -> str:
    admin = psycopg.connect(pg_dsn, autocommit=True)
    admin.execute(f'CREATE DATABASE "{name}"')
    admin.close()
    return pg_dsn.replace("dbname=test", f"dbname={name}")


class Env:
    """Test handle: both catalogs plus a DucklingConfig factory."""

    def __init__(self, src, dst, src_dsn, dst_dsn, tmp_path):
        self.src = src
        self.dst = dst
        self.src_dsn = src_dsn
        self.dst_dsn = dst_dsn
        self.tmp_path = tmp_path
        self._destination_id = f"org-test-team-2-{uuid.uuid4().hex[:6]}"

    def cfg(self, **kw) -> DucklingConfig:
        base = dict(
            # ATTACH format on purpose: exercises the F2 conninfo translation
            source_pg_uri=f"postgres:{self.src_dsn}",
            source_catalog="lake",
            source_data_path=str(self.tmp_path / "src"),
            source_table="main.events",
            dest_pg_uri=f"postgres:{self.dst_dsn}",
            dest_catalog="dst",
            dest_data_path=str(self.tmp_path / "dst"),
            dest_table="main.events",
            team_field="team_id",
            team_value="2",
            cursor_pg_uri="",
            destination_id=self._destination_id,  # stable per test: a restart
            # resumes the SAME cursor row
            instance_id="duckling-test-0",
            poll_interval_s=0.1,
            start_snapshot_id=0,  # tests want full catch-up, not start-at-head
        )
        base.update(kw)
        return DucklingConfig(**base)


@pytest.fixture()
def env(pg_dsn, tmp_path):
    """Fresh source + destination databases per test (catalog metadata
    persists in PG; reusing the test db would collide across tests)."""
    tag = uuid.uuid4().hex[:8]
    src_dsn = _mkdb(pg_dsn, f"src_{tag}")
    dst_dsn = _mkdb(pg_dsn, f"dst_{tag}")
    src = Catalog("lake", f"postgres:{src_dsn}", data_path=str(tmp_path / "src"))
    src.connection.execute("SET ducklake_default_data_inlining_row_limit = 0")
    src.connection.execute("CREATE TABLE lake.main.events (team_id BIGINT, event VARCHAR)")
    dst = Catalog("dst", f"postgres:{dst_dsn}", data_path=str(tmp_path / "dst"))
    yield Env(src, dst, src_dsn, dst_dsn, tmp_path)
    src.close()
    dst.close()


def _insert(src, rows, table="main.events"):
    vals = ", ".join(f"({t}, '{e}')" for t, e in rows)
    src.connection.execute(f"INSERT INTO lake.{table} VALUES {vals}")


def _dest_rows(dst):
    return sorted(dst.connection.execute("SELECT team_id, event FROM dst.main.events").fetchall())


def _close(d):
    if getattr(d, "feed", None) is not None:
        d.feed.close()
    if getattr(d, "_cursor_conn", None) is not None:
        d._cursor_conn.close()


class TestEndToEnd:
    def test_catchup_filters_and_advances(self, env):
        src, dst = env.src, env.dst
        _insert(src, [(2, "a"), (3, "b")])
        _insert(src, [(2, "c")])
        _insert(src, [(9, "d"), (2, "e")])

        d = Duckling(env.cfg())
        d.boot()
        try:
            d.poll_once()
            assert _dest_rows(dst) == [(2, "a"), (2, "c"), (2, "e")]
            head = int(
                src.connection.execute(
                    'SELECT MAX(snapshot_id) FROM "__ducklake_metadata_lake".ducklake_snapshot'
                ).fetchone()[0]
            )
            assert d._cursor == head
            d.poll_once()  # idle: nothing new
            assert d._cursor == head
        finally:
            _close(d)

    def test_crash_between_append_and_cursor_redelivers_once(self, env):
        """The at-least-once window, demonstrated: cursor strictly after
        commit; a crash in between re-delivers the unit."""
        src, dst = env.src, env.dst
        _insert(src, [(2, "a"), (2, "b")])

        d = Duckling(env.cfg())
        d.boot()
        d._cursor_advance = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("simulated crash"))
        try:
            with pytest.raises(RuntimeError, match="simulated crash"):
                d.poll_once()
        finally:
            _close(d)
        assert _dest_rows(dst) == [(2, "a"), (2, "b")]  # committed; cursor not

        d2 = Duckling(env.cfg())  # "restart": same cursor row
        d2.boot()
        try:
            d2.poll_once()
        finally:
            _close(d2)
        assert _dest_rows(dst) == [(2, "a"), (2, "a"), (2, "b"), (2, "b")]  # one unit of dupes

    def test_add_column_restart_picks_up(self, env):
        """Boot-pinned schema: a new source column is invisible to the running
        process (no wedge across the boundary) and picked up at restart."""
        src, dst = env.src, env.dst
        _insert(src, [(2, "a")])

        d = Duckling(env.cfg())
        d.boot()
        try:
            # NB: on the deployed build the ALTER resurrects the session's
            # inlining default — re-pin it so b/c land in parquet, not inline.
            src.connection.execute("ALTER TABLE lake.main.events ADD COLUMN extra BIGINT")
            src.connection.execute("SET ducklake_default_data_inlining_row_limit = 0")
            src.connection.execute("INSERT INTO lake.main.events (team_id, event, extra) VALUES (2, 'b', 42)")
            d.poll_once()  # running process, pinned-old projection: no wedge
        finally:
            _close(d)
        assert _dest_rows(dst) == [(2, "a"), (2, "b")]

        d2 = Duckling(env.cfg())
        d2.boot()
        try:
            assert "extra" in d2.columns
            assert "extra" in d2.dst_table.schema.column_names()
            src.connection.execute("SET ducklake_default_data_inlining_row_limit = 0")
            src.connection.execute("INSERT INTO lake.main.events (team_id, event, extra) VALUES (2, 'c', 43)")
            d2.poll_once()
        finally:
            _close(d2)
        out = dst.connection.execute("SELECT team_id, event, extra FROM dst.main.events ORDER BY event").fetchall()
        # b landed while d1's boot-pinned projection was still (team_id, event):
        # the new column is invisible until restart — b carries NULL by design.
        assert out == [(2, "a", None), (2, "b", None), (2, "c", 43)]

    def test_delete_appearance_crashes(self, env):
        src = env.src
        _insert(src, [(2, "a")])
        src.connection.execute("DELETE FROM lake.main.events WHERE team_id = 2")

        d = Duckling(env.cfg())
        try:
            # the baseline assertions at boot already see the delete
            with pytest.raises(RuntimeError, match="contract violated|delete"):
                d.boot()
        finally:
            _close(d)

    def test_restart_resumes_from_cursor(self, env):
        """The production restart path: poll → advance → clean restart →
        only NEW rows arrive. (The crash test proves the duplicate bound;
        this proves the cursor round-trip — off-by-ones and wrong-row reads
        are caught here and nowhere else.)"""
        src, dst = env.src, env.dst
        _insert(src, [(2, "a")])

        d = Duckling(env.cfg())
        d.boot()
        try:
            d.poll_once()
            cursor_after = d._cursor
        finally:
            _close(d)
        assert _dest_rows(dst) == [(2, "a")]

        d2 = Duckling(env.cfg())  # same destination_id → same cursor row
        d2.boot()
        try:
            assert d2._cursor == cursor_after  # resumed, not restarted
            _insert(src, [(2, "b"), (3, "x")])
            d2.poll_once()
        finally:
            _close(d2)
        assert _dest_rows(dst) == [(2, "a"), (2, "b")]  # no dupes, no re-read

    def test_retention_clamp_then_poll_composes(self, env):
        """Clamp advances loudly WITH a durable note; the in-txn floor guard
        then accepts the clamped cursor (an off-by-one divergence between the
        two would crash-loop exactly inside the retention window)."""
        src = env.src
        _insert(src, [(2, "a")])
        _insert(src, [(2, "b")])
        _insert(src, [(2, "c")])
        pg = psycopg.connect(env.src_dsn, autocommit=True)
        meta = pg.execute(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = 'ducklake_snapshot'"
        ).fetchone()[0]
        snaps = [
            int(r[0]) for r in pg.execute(f'SELECT snapshot_id FROM "{meta}".ducklake_snapshot ORDER BY 1').fetchall()
        ]
        floor = snaps[-1]
        pg.execute(f'DELETE FROM "{meta}".ducklake_snapshot WHERE snapshot_id < {floor}')

        d = Duckling(env.cfg())
        d.boot()  # writes the cursor row at start_snapshot_id=0
        try:
            assert d._cursor == 0
            d._clamp_to_retention()
            assert d._cursor == floor - 1
            note = pg.execute(
                "SELECT last_error FROM viaduck.viaduck_state WHERE destination_id = %s",
                (d.cfg.destination_id,),
            ).fetchone()
            assert note and "DATA LOSS" in note[0]
            d.poll_once()  # the guard accepts the clamped cursor; no crash-loop
            assert d._cursor >= floor - 1
            # the boundary row (the last surviving snapshot) actually delivered
            assert _dest_rows(env.dst) == [(2, "c")]
        finally:
            _close(d)
            pg.close()

    def test_below_floor_plan_refused_by_feed_guard(self, env):
        """The in-transaction backstop: planning with a cursor under the
        retained floor raises instead of silently skipping (here WITHOUT the
        clamp running first)."""
        src = env.src
        _insert(src, [(2, "a")])
        _insert(src, [(2, "b")])
        pg = psycopg.connect(env.src_dsn, autocommit=True)
        meta = pg.execute(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = 'ducklake_snapshot'"
        ).fetchone()[0]
        snaps = [
            int(r[0]) for r in pg.execute(f'SELECT snapshot_id FROM "{meta}".ducklake_snapshot ORDER BY 1').fetchall()
        ]
        pg.execute(f'DELETE FROM "{meta}".ducklake_snapshot WHERE snapshot_id < {snaps[-1]}')

        from viaduck import feed as feed_mod

        reader = feed_mod.FeedReader(postgres_uri=env.src_dsn, catalog_name="lake", data_path=str(env.tmp_path / "src"))
        try:
            with pytest.raises(feed_mod.FeedError, match="retained snapshot floor"):
                reader.plan_unit(env.src.load_table("main.events"), snaps[0], snaps[-1])
        finally:
            reader.close()
            pg.close()

    def test_inlined_delete_of_flushed_rows_crashes(self, env):
        """The store-probe leg in isolation (round-4 H2): with the witness
        rows erased (the delete+expire-between-polls shape), the
        ducklake_inlined_delete_<tid> probe is the ONLY remaining detector."""
        src = env.src
        src.connection.execute("SET ducklake_default_data_inlining_row_limit = 100")
        src.connection.execute("CREATE TABLE lake.main.inlined_del2 (team_id BIGINT, event VARCHAR)")
        src.connection.execute("INSERT INTO lake.main.inlined_del2 VALUES (2, 'a'), (2, 'b')")
        src.connection.execute(
            "CALL ducklake_flush_inlined_data('lake', schema_name := 'main', table_name := 'inlined_del2')"
        )
        src.connection.execute("DELETE FROM lake.main.inlined_del2 WHERE event = 'a'")

        pg = psycopg.connect(env.src_dsn, autocommit=True)
        meta = pg.execute(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = 'ducklake_snapshot'"
        ).fetchone()[0]
        tid = pg.execute(
            f'SELECT table_id FROM "{meta}".ducklake_table WHERE table_name = %s', ("inlined_del2",)
        ).fetchone()[0]
        # the store exists and is non-empty (parquet-resident delete, inlined)
        n = pg.execute(f'SELECT count(*) FROM "{meta}"."ducklake_inlined_delete_{tid}"').fetchone()[0]
        assert n == 1, "expected the inlined-delete store to exist with 1 row"
        # erase the witness (expire shape): the store leg must fire alone
        pg.execute(f'DELETE FROM "{meta}".ducklake_snapshot_changes')
        pg.close()

        d = Duckling(env.cfg(source_table="main.inlined_del2", dest_table="main.inlined_del2"))
        try:
            with pytest.raises(FatalDucklingError, match="inlined deletes"):
                d.boot()  # baseline assertions: store probe fires
        finally:
            _close(d)

    def test_accepted_delete_below_cursor_boots_clean(self, env):
        """The accept path (round-3 C1's remedy): a delete adjudicated by
        scoping the cursor past it must not fire — and only post-cursor rows
        deliver."""
        src, dst = env.src, env.dst
        _insert(src, [(2, "a")])
        src.connection.execute("DELETE FROM lake.main.events WHERE event = 'a'")
        pg = psycopg.connect(env.src_dsn, autocommit=True)
        meta = pg.execute(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = 'ducklake_snapshot'"
        ).fetchone()[0]
        head = int(pg.execute(f'SELECT MAX(snapshot_id) FROM "{meta}".ducklake_snapshot').fetchone()[0])
        pg.close()

        d = Duckling(env.cfg(start_snapshot_id=head))  # the documented accept: cursor past the delete
        d.boot()
        try:
            _insert(src, [(2, "b")])
            d.poll_once()
            assert _dest_rows(dst) == [(2, "b")]
        finally:
            _close(d)

    def test_dest_reordered_columns_map_by_name(self, env):
        """The silent-corruption pin (round-3 SWE H2): a dest table with the
        same columns in DIFFERENT physical order must map by name —
        positional insert would silently swap values."""
        src, dst = env.src, env.dst
        _insert(src, [(2, "evt-a"), (3, "evt-b")])
        dst.connection.execute(
            "CREATE TABLE dst.main.events (event VARCHAR, _inserted_at TIMESTAMPTZ DEFAULT now(), team_id BIGINT)"
        )

        d = Duckling(env.cfg())
        d.boot()
        try:
            d.poll_once()
            out = dst.connection.execute("SELECT team_id, event FROM dst.main.events").fetchall()
            assert out == [(2, "evt-a")]  # by name, not position
        finally:
            _close(d)

    def test_dest_managed_column_default_filled(self, env):
        """SWE H2 pin: a dest table pre-provisioned with a managed DEFAULT
        column must accept appends (BY NAME fills it) — pyducklake's
        positional append would crash-loop here."""
        src, dst = env.src, env.dst
        _insert(src, [(2, "a")])
        dst.connection.execute(
            "CREATE TABLE dst.main.events (team_id BIGINT, event VARCHAR, _inserted_at TIMESTAMPTZ DEFAULT now())"
        )

        d = Duckling(env.cfg())
        d.boot()
        try:
            d.poll_once()
            out = dst.connection.execute(
                "SELECT team_id, event, _inserted_at IS NOT NULL FROM dst.main.events"
            ).fetchall()
            assert out == [(2, "a", True)]
        finally:
            _close(d)

    def test_inlined_delete_pages_via_witness(self, env):
        """A small DELETE under inlining produces NO delete_file row and sets
        NO end_snapshot — only the snapshot_changes witness / the
        inlined-delete store see it (the round-2 hole)."""
        src = env.src
        src.connection.execute("SET ducklake_default_data_inlining_row_limit = 100")
        src.connection.execute("CREATE TABLE lake.main.inlined_del (team_id BIGINT, event VARCHAR)")
        src.connection.execute("INSERT INTO lake.main.inlined_del VALUES (2, 'a'), (2, 'b')")
        src.connection.execute("DELETE FROM lake.main.inlined_del WHERE event = 'a'")

        d = Duckling(env.cfg(source_table="main.inlined_del", dest_table="main.inlined_del"))
        try:
            with pytest.raises(FatalDucklingError, match="delete/drop activity"):
                d.boot()
        finally:
            d.feed.close()
            if d._cursor_conn is not None:
                d._cursor_conn.close()


class TestVariantSourceColumn:
    """The production shape: events_nrt carries millpond's VARIANT dual-write
    companions. The duckling must boot, read, and deliver as if they don't
    exist (load_table's EXCLUDABLE_SOURCE_TYPES exclusion)."""

    def test_boot_and_deliver_with_variant_column(self, env):
        src, dst = env.src, env.dst
        src.connection.execute(
            "CREATE TABLE lake.main.events_v (team_id BIGINT, event VARCHAR, properties_variant VARIANT)"
        )
        src.connection.execute("INSERT INTO lake.main.events_v VALUES (2, 'a', {\"k\": 1}), (3, 'b', NULL)")

        d = Duckling(env.cfg(source_table="main.events_v", dest_table="main.events_v"))
        d.boot()
        try:
            assert "properties_variant" not in d.columns
            d.poll_once()
            out = dst.connection.execute("SELECT team_id, event FROM dst.main.events_v").fetchall()
            assert out == [(2, "a")]
            # the destination never gained the column
            assert "properties_variant" not in d.dst_table.schema.column_names()
        finally:
            _close(d)

    def test_variant_added_mid_stream(self, env):
        """A VARIANT column added mid-stream: the running process never sees
        it (boot-pinned projection), a restart excludes it again — no wedge
        in either phase."""
        src, dst = env.src, env.dst
        _insert(src, [(2, "a")])

        d = Duckling(env.cfg())
        d.boot()
        try:
            src.connection.execute("ALTER TABLE lake.main.events ADD COLUMN properties_variant VARIANT")
            src.connection.execute(
                "INSERT INTO lake.main.events (team_id, event, properties_variant) VALUES (2, 'b', {\"k\": 1})"
            )
            d.poll_once()
        finally:
            _close(d)
        assert _dest_rows(dst) == [(2, "a"), (2, "b")]

        d2 = Duckling(env.cfg())
        d2.boot()  # restart: load_table excludes the VARIANT again
        try:
            assert "properties_variant" not in d2.columns
            src.connection.execute(
                "INSERT INTO lake.main.events (team_id, event, properties_variant) VALUES (2, 'c', NULL)"
            )
            d2.poll_once()
        finally:
            _close(d2)
        assert _dest_rows(dst) == [(2, "a"), (2, "b"), (2, "c")]


class TestInlineServedAndPaged:
    def test_inline_rows_served_with_page(self, env):
        """Inlining drifting ON is not a correctness event (the feed's tested
        inline path serves it) — but it pages: the writer broke the contract."""
        src, dst = env.src, env.dst
        src.connection.execute("SET ducklake_default_data_inlining_row_limit = 100")
        src.connection.execute("CREATE TABLE lake.main.inlined (team_id BIGINT, event VARCHAR)")
        src.connection.execute("INSERT INTO lake.main.inlined VALUES (2, 'i1'), (3, 'i2')")
        src.connection.execute("RESET ducklake_default_data_inlining_row_limit")

        d = Duckling(env.cfg(source_table="main.inlined", dest_table="main.inlined"))
        d.boot()
        try:
            d.poll_once()
            assert sorted(dst.connection.execute("SELECT team_id, event FROM dst.main.inlined").fetchall()) == [
                (2, "i1")
            ]
        finally:
            _close(d)
