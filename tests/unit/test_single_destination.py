"""Unit tests for viaduck.single_destination — the single-destination viaduck.

The wrapper is the only layer that can silently lose data (the read core is
pinned by the feed parity suite), so the cursor/ordering/crash semantics
here are the wrapper's entire correctness contract.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from viaduck import single_destination as sd
from viaduck.single_destination import FatalSingleDestinationError, SingleDestinationConfig, SingleDestinationViaduck


def _cfg(**kw) -> SingleDestinationConfig:
    base = dict(
        source_pg_uri="postgres:host=src port=5432 dbname=megaduck user=m password=pw",
        source_catalog="lake",
        source_data_path="s3://b/src",
        source_table="main.events_nrt",
        dest_pg_uri="postgres:host=dst port=5432 dbname=dest user=d password=pw",
        dest_catalog="dest",
        dest_data_path="s3://b/dst",
        dest_table="posthog.events",
        team_field="team_id",
        team_value="2",
        destination_id="org-abc-team-2",
    )
    base.update(kw)
    return SingleDestinationConfig(**base)


def _single_destination(cfg=None, cursor=100) -> SingleDestinationViaduck:
    """A SingleDestinationViaduck without boot(): all collaborators mocked. Catalog-side
    queries route through feed._pg(); cursor writes through _cursor_conn."""
    d = SingleDestinationViaduck.__new__(SingleDestinationViaduck)
    d.cfg = cfg or _cfg()
    d._stop = threading.Event()
    d._last_poll_ok = time.monotonic()
    d._budget_rows = d.cfg.unit_max_rows
    d._cursor = cursor
    d.table_id = 16
    d.meta = "lake_meta"
    d.columns = ("team_id", "event")
    d._team_array = pa.array([2], type=pa.int64())
    d.src_table = MagicMock()
    d.src_catalog = MagicMock()
    d.dst_table = MagicMock()
    d.dst_catalog = MagicMock()
    d._dest_fqn = "dest.posthog.events"
    d.feed = MagicMock()
    d.feed._pg.return_value.closed = False
    d._catalog_pg = lambda: d.feed._pg()  # same wiring as the real class
    d._cursor_conn = MagicMock()
    d._cursor_conn.closed = False
    # identity check defaults to "unchanged"; tests override as needed
    d._resolve_table_id = MagicMock(return_value=d.table_id)
    return d


def _poll_ready(d: SingleDestinationViaduck, rows: pa.Table, head=500, hi=500):
    """Wire a single-destination viaduck for poll_once: assertions no-op, plan/read mocked."""
    d._assert_no_deletes = MagicMock()
    d._check_inline_stores = MagicMock()
    d._clamp_to_retention = MagicMock()
    d._head = MagicMock(return_value=head)
    d.feed.plan_unit.return_value = hi
    d.feed.read.return_value = rows


# ---------------------------------------------------------------------------
# Cursor ordering — the one rule
# ---------------------------------------------------------------------------


class TestCursorOrdering:
    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch):
        monkeypatch.setattr(sd.time, "sleep", lambda *_: None)

    def test_cursor_strictly_after_commit(self):
        d = _single_destination()
        rows = pa.table({"team_id": [2, 3, 2], "event": ["a", "b", "c"]})
        _poll_ready(d, rows)
        parent = MagicMock()
        parent.attach_mock(d.dst_catalog.connection.execute, "append")
        parent.attach_mock(d._cursor_conn.execute, "pg")
        d.poll_once()
        append_idx = next(i for i, c in enumerate(parent.mock_calls) if c[0] == "append" and "INSERT INTO" in str(c))
        upsert_idx = next(i for i, c in enumerate(parent.mock_calls) if c[0] == "pg" and "ON CONFLICT" in str(c))
        assert append_idx < upsert_idx
        assert d._cursor == 500

    def test_append_sql_is_by_name(self):
        """The silent-corruption guard's shape pin: the append MUST be
        INSERT BY NAME (positional silently swaps same-typed columns on
        reordered dest tables)."""
        d = _single_destination()
        d._append(pa.table({"team_id": [2]}))
        appends = [c for c in d.dst_catalog.connection.execute.call_args_list if "INSERT INTO" in str(c)]
        assert len(appends) == 1 and "BY NAME" in appends[0].args[0]

    def test_append_failure_never_advances_cursor_and_is_fatal(self):
        d = _single_destination()
        rows = pa.table({"team_id": [2], "event": ["a"]})
        _poll_ready(d, rows)
        d.dst_catalog.connection.execute.side_effect = RuntimeError("catalog down")
        with pytest.raises(FatalSingleDestinationError, match="append failed after 3 attempts"):
            d.poll_once()
        assert d._cursor == 100
        assert not any("ON CONFLICT" in str(c) for c in d._cursor_conn.execute.call_args_list)
        # millpond: 3 attempts then crash (register/unregister don't count)
        appends = [c for c in d.dst_catalog.connection.execute.call_args_list if "INSERT INTO" in str(c)]
        assert len(appends) == 3

    def test_empty_range_still_advances(self):
        """Foreign-commit polls: hi is a valid coverage boundary; idling
        forever would burn the retention window."""
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": pa.array([], type=pa.int64()), "event": pa.array([], type=pa.string())}))
        d.poll_once()
        assert d._cursor == 500
        assert not any("INSERT INTO" in str(c) for c in d.dst_catalog.connection.execute.call_args_list)

    def test_empty_range_rechecks_table_identity(self):
        """A silent drop+recreate is invisible to the feed's cached table_id
        (its plans come back empty forever) — the empty path re-resolves."""
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": pa.array([], type=pa.int64()), "event": pa.array([], type=pa.string())}))
        d._resolve_table_id = MagicMock(return_value=17)
        with pytest.raises(FatalSingleDestinationError, match="table_id changed"):
            d.poll_once()
        assert d._cursor == 100  # frozen

    def test_idle_when_at_head(self):
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": [2]}), head=100)
        d.poll_once()
        d.feed.plan_unit.assert_not_called()
        assert d._cursor == 100


class TestDropCreate:
    def test_table_id_change_on_read_error_freezes_and_pages(self):
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": [2]}))
        d.feed.read.side_effect = RuntimeError("catalog read exploded")
        d._resolve_table_id = MagicMock(return_value=17)
        with pytest.raises(FatalSingleDestinationError, match="table_id changed 16 → 17"):
            d.poll_once()
        assert d._cursor == 100

    def test_table_dropped_pages(self):
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": [2]}))
        d.feed.read.side_effect = RuntimeError("boom")
        d._resolve_table_id = MagicMock(side_effect=sd.ConfigError("not found"))
        with pytest.raises(FatalSingleDestinationError, match="source table dropped"):
            d.poll_once()

    def test_transient_read_error_reraises(self):
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": [2]}))
        d.feed.read.side_effect = RuntimeError("s3 flaked")
        d._resolve_table_id = MagicMock(return_value=16)  # unchanged → transient
        with pytest.raises(RuntimeError, match="s3 flaked"):
            d.poll_once()
        assert d._cursor == 100

    def test_cursor_provenance_mismatch_at_boot(self):
        """Drop+recreate WHILE DOWN: the stored table_id is the only evidence."""
        d = _single_destination()
        d.table_id = 99  # resolved fresh at boot
        d._cursor_pg().execute.return_value.fetchone.return_value = (100, 16)  # stored against 16
        with pytest.raises(FatalSingleDestinationError, match="table_id=16"):
            d._cursor_load()

    def test_cursor_provenance_backfilled_when_null(self):
        """The fleet-transplant path: a pre-existing row with NULL provenance
        gets stamped at boot — otherwise a drop+recreate-while-down is
        undetectable (the witness is keyed to the NEW table_id)."""
        d = _single_destination()
        d._cursor_pg().execute.return_value.fetchone.return_value = (100, None)
        d._cursor_load()
        assert d._cursor == 100
        backfills = [c for c in d._cursor_conn.execute.call_args_list if "SET source_table_id" in str(c)]
        assert len(backfills) == 1 and backfills[0].args[1][0] == 16


class TestClampAssertionOrdering:
    def test_clamp_runs_before_assertions(self):
        """A reorder regression crash-loops the single-destination viaduck inside the retention
        window (a delete whose evidence outlives its snapshots must not veto
        the clamp's loud advance)."""
        d = _single_destination(cursor=100)
        calls = []
        d._clamp_to_retention = MagicMock(side_effect=lambda: calls.append("clamp"))
        d._assert_no_deletes = MagicMock(side_effect=lambda: calls.append("assert"))
        d._check_inline_stores = MagicMock()
        d._head = MagicMock(return_value=100)  # idle after
        d.poll_once()
        assert calls == ["clamp", "assert"]


class TestFeedErrorClassification:
    def test_floor_feederror_is_transient(self):
        """The floor guard's FeedError re-raises as-is: next poll's clamp
        advances past it. Making it fatal would crash-loop in the window."""
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": [2]}))
        d.feed.read.side_effect = sd.feed.FeedError("cursor 5 is below the retained snapshot floor 9")
        with pytest.raises(sd.feed.FeedError, match="retained snapshot floor"):
            d.poll_once()

    def test_other_feederror_is_fatal(self):
        """A real refusal (non-additive schema, encryption) must crash —
        transient retry is a silent stall with a growing lag gauge."""
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": [2]}))
        d.feed.read.side_effect = sd.feed.FeedError("non-additive schema change (rename/drop) is unsupported")
        with pytest.raises(FatalSingleDestinationError, match="non-additive"):
            d.poll_once()

    def test_missing_team_column_is_fatal(self):
        d = _single_destination()
        with pytest.raises(FatalSingleDestinationError, match="schema contract"):
            d._arrow_filter(pa.table({"other": [1]}))


class TestHeadRegression:
    def test_head_below_cursor_is_fatal(self):
        """A source restore/rebuild regresses head under the cursor: the
        ONE alarm (lag) must not read 0 while wedged — crash loudly."""
        d = _single_destination(cursor=100)
        _poll_ready(d, pa.table({"team_id": [2]}), head=50)
        with pytest.raises(FatalSingleDestinationError, match="regressed below cursor"):
            d.poll_once()
        assert d._cursor == 100


# ---------------------------------------------------------------------------
# run() supervision: fatal crashes, transient retries
# ---------------------------------------------------------------------------


class TestRunSupervision:
    def _runnable(self, d: SingleDestinationViaduck):
        d.boot = MagicMock()
        d._maybe_recycle = MagicMock()
        with patch.object(sd, "_start_health_server", return_value=MagicMock()):
            d.src_catalog = MagicMock()
            d.dst_catalog = MagicMock()
            yield d

    def test_fatal_crashes(self):
        d = _single_destination()
        for d in self._runnable(d):
            d.poll_once = MagicMock(side_effect=FatalSingleDestinationError("assertion fired"))
            with pytest.raises(FatalSingleDestinationError):
                d.run()

    def test_transient_retries_next_poll(self, monkeypatch):
        monkeypatch.setattr(sd.time, "sleep", lambda *_: None)  # backoff budget
        d = _single_destination(_cfg(poll_interval_s=0.01))
        for d in self._runnable(d):
            calls = [0]

            def flaky():
                calls[0] += 1
                if calls[0] < 3:
                    raise RuntimeError("s3 flaked")
                d._stop.set()

            d.poll_once = MagicMock(side_effect=flaky)
            d.run()  # returns normally after the stop
            assert calls[0] == 3

    def test_poll_wait_is_jittered(self, monkeypatch):
        """The thundering-herd mitigation is load-bearing at N=300 (a fixed
        interval synchronizes the assertion burst after any fleet-wide
        restart). Pin the wait's shape deterministically."""
        monkeypatch.setattr(sd.random, "random", lambda: 0.5)
        d = _single_destination(_cfg(poll_interval_s=10.0))
        for d in self._runnable(d):
            seen = []

            def stop_after_one():
                d._stop.set()

            d.poll_once = MagicMock(side_effect=stop_after_one)
            monkeypatch.setattr(d._stop, "wait", lambda t: seen.append(t) or True)
            d.run()
            assert seen and abs(seen[0] - 10.0) < 1e-9  # (0.5 + 0.5) × interval


# ---------------------------------------------------------------------------
# Retention clamp
# ---------------------------------------------------------------------------


class TestRetentionClamp:
    def test_below_floor_advances_loudly_with_loss_note(self):
        d = _single_destination(cursor=100)
        d.feed._pg.return_value.execute.return_value.fetchone.return_value = (600,)  # MIN(snapshot_id)
        d._clamp_to_retention()
        assert d._cursor == 599
        update = next(c for c in d._cursor_conn.execute.call_args_list if "last_error" in str(c))
        assert "DATA LOSS" in str(update)
        assert "100" in str(update) and "599" in str(update)

    def test_at_floor_is_quiet(self):
        d = _single_destination(cursor=599)
        d.feed._pg.return_value.execute.return_value.fetchone.return_value = (600,)
        d._clamp_to_retention()
        assert d._cursor == 599
        assert not any("last_error" in str(c) for c in d._cursor_conn.execute.call_args_list)

    def test_empty_snapshot_table_is_noop(self):
        d = _single_destination(cursor=0)
        d.feed._pg.return_value.execute.return_value.fetchone.return_value = (None,)
        d._clamp_to_retention()
        assert d._cursor == 0


# ---------------------------------------------------------------------------
# AIMD
# ---------------------------------------------------------------------------


class TestAimd:
    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch):
        monkeypatch.setattr(sd.time, "sleep", lambda *_: None)

    def test_flush_failure_halves_budget(self):
        d = _single_destination()
        d.dst_catalog.connection.execute.side_effect = [RuntimeError("occ contention"), None]
        d._append(pa.table({"team_id": [2]}))
        # halve on the failed attempt (50000 → 25000), then +10% recovery on
        # the successful retry (25000 → 27500)
        assert d._budget_rows == 27_500

    def test_slow_flush_halves(self):
        d = _single_destination()
        d.cfg = _cfg(slow_flush_seconds=0.0)  # every flush is "slow"
        d._append(pa.table({"team_id": [2]}))
        assert d._budget_rows == 25_000

    def test_floor_and_recovery(self):
        d = _single_destination()
        d._budget_rows = d.cfg.aimd_floor_rows
        d._aimd_halve("test")
        assert d._budget_rows == d.cfg.aimd_floor_rows
        for _ in range(50):
            d._aimd_recover()
        assert d._budget_rows == d.cfg.unit_max_rows


# ---------------------------------------------------------------------------
# Cursor write retries
# ---------------------------------------------------------------------------


class TestCursorWrites:
    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch):
        monkeypatch.setattr(sd.time, "sleep", lambda *_: None)

    def test_cursor_advance_has_monotonic_guard(self):
        """A maxSurge pair sharing the row must never regress it (fleet
        semantics; the guard is what makes a racing advance a no-op)."""
        d = _single_destination()
        d._cursor_advance(500, 3)
        sql = d._cursor_conn.execute.call_args.args[0]
        assert "WHERE viaduck.viaduck_state.last_snapshot_id <= EXCLUDED.last_snapshot_id" in sql

    def test_retry_then_success(self):
        d = _single_destination()
        import psycopg

        d._cursor_conn.execute.side_effect = [psycopg.OperationalError("pg blip"), None]
        d._cursor_advance(500, 10)
        assert d._cursor == 500
        assert d._cursor_conn.execute.call_count == 2

    def test_exhaustion_is_fatal(self):
        d = _single_destination()
        import psycopg

        d._cursor_conn.execute.side_effect = psycopg.OperationalError("pg down")
        with pytest.raises(FatalSingleDestinationError, match="cursor update failed"):
            d._cursor_advance(500, 10)
        assert d._cursor == 100  # in-memory cursor did not move


# ---------------------------------------------------------------------------
# Arrow correctness layer (the ONLY filter — no SQL pushdown by design)
# ---------------------------------------------------------------------------


class TestArrowFilter:
    def test_filters_to_team(self):
        d = _single_destination()
        rows = pa.table({"team_id": [2, 3, 2], "event": ["a", "b", "c"]})
        out = d._arrow_filter(rows)
        assert out.num_rows == 2
        assert set(out.column("event").to_pylist()) == {"a", "c"}

    def test_unfiltered_read_contract(self):
        """poll_once must NOT pass a filter to the feed: parquet zone-maps
        can lie on add_files-registered files, and SQL pushdown under-
        delivery is invisible. The Arrow layer is the whole filter."""
        d = _single_destination()
        _poll_ready(d, pa.table({"team_id": [2]}))
        d.poll_once()
        assert "filter_expr" not in d.feed.read.call_args.kwargs or d.feed.read.call_args.kwargs["filter_expr"] is None

    def test_missing_team_column_crashes_loudly(self):
        d = _single_destination()
        with pytest.raises(Exception):
            d._arrow_filter(pa.table({"other": [1]}))

    def test_string_team_value(self):
        d = _single_destination(_cfg(team_field="team", team_value="blue"))
        d._team_array = pa.array(["blue"], type=pa.string())
        rows = pa.table({"team": ["blue", "red"]})
        assert d._arrow_filter(rows).num_rows == 1


# ---------------------------------------------------------------------------
# Assertion quartet
# ---------------------------------------------------------------------------


class TestAssertions:
    @staticmethod
    def _queries(pg):
        return pg.execute.call_args_list

    def test_assertions_scoped_to_cursor(self):
        """The accept path's linchpin: every assertion query is scoped to
        UN-CROSSED history (snapshot > cursor). Dropping a scope predicate
        reintroduces the no-accept-path trap (round-3 C1)."""
        d = _single_destination(cursor=100)
        pg = self._pg_with_counts({}, regclass=None)
        d.feed._pg.return_value = pg
        d._assert_no_deletes()
        df = next(c for c in pg.execute.call_args_list if "ducklake_delete_file" in str(c))
        es = next(c for c in pg.execute.call_args_list if "end_snapshot IS NOT NULL" in str(c))
        wt = next(c for c in pg.execute.call_args_list if "snapshot_changes" in str(c))
        assert "begin_snapshot > %s" in df.args[0] and df.args[1] == (16, 100)
        assert "end_snapshot > %s" in es.args[0] and es.args[1] == (16, 100)
        assert "snapshot_id > %s" in wt.args[0] and wt.args[1][0] == 100

    def test_delete_below_cursor_does_not_fire(self):
        """An adjudicated (pre-cursor) delete: all checks stay quiet."""
        d = _single_destination(cursor=100)

        pg = MagicMock()

        def execute(sql, params=None):
            cur = MagicMock()
            if "to_regclass" in sql:
                cur.fetchone.return_value = (None,)
            elif "ducklake_inlined_data_tables" in sql:
                cur.fetchall.return_value = []
            elif "snapshot_changes" in sql:
                # a delete witness exists, but BELOW the cursor
                assert params[0] == 100  # the scope param is present
                cur.fetchone.return_value = (0,)
            else:
                cur.fetchone.return_value = (0,)
            return cur

        pg.execute.side_effect = execute
        d.feed._pg.return_value = pg
        d._assert_no_deletes()  # no raise

    def test_inlined_delete_store_scoped_to_cursor(self):
        """The store-probe leg carries the same cursor scope — dropping that
        predicate reintroduces the no-accept-path trap for inlined deletes."""
        d = _single_destination(cursor=100)
        pg = self._pg_with_counts({"ducklake_inlined_delete_16": 0}, regclass="lake_meta.ducklake_inlined_delete_16")
        d.feed._pg.return_value = pg
        d._assert_no_deletes()
        q = next(c for c in pg.execute.call_args_list if "ducklake_inlined_delete_16" in str(c) and "count(" in str(c))
        assert "begin_snapshot > %s" in q.args[0] and q.args[1] == (100,)

    def _pg_with_counts(self, counts: dict[str, int], regclass=None, stores=()):
        pg = MagicMock()

        def execute(sql, params=None):
            cur = MagicMock()
            # existence probe FIRST (ordering pins the regclass-before-count
            # contract — counting a nonexistent table would raise in prod)
            if "to_regclass" in sql:
                cur.fetchone.return_value = (regclass,)
                return cur
            if "ducklake_inlined_data_tables" in sql:
                cur.fetchall.return_value = [(s,) for s in stores]
                return cur
            for key, n in counts.items():
                if key in sql:
                    cur.fetchone.return_value = (n,)
                    return cur
            cur.fetchone.return_value = (0,)
            return cur

        pg.execute.side_effect = execute
        return pg

    def test_delete_file_appearance_crashes(self):
        d = _single_destination()
        d.feed._pg.return_value = self._pg_with_counts({"ducklake_delete_file": 1})
        with pytest.raises(FatalSingleDestinationError, match="append-only contract violated"):
            d._assert_no_deletes()

    def test_end_snapshot_appearance_crashes(self):
        d = _single_destination()
        d.feed._pg.return_value = self._pg_with_counts({"ducklake_data_file": 3})
        with pytest.raises(FatalSingleDestinationError, match="end_snapshot"):
            d._assert_no_deletes()

    def test_witness_regex_scoped_to_table_and_vocabulary(self):
        """The fork's exact delete vocabulary: deleted_from_table /
        inlined_delete / rewrite_delete / dropped_table (verified against
        ducklake_transaction.cpp AddChangeInfo)."""
        d = _single_destination()
        d.feed._pg.return_value = self._pg_with_counts({"ducklake_snapshot_changes": 1})
        with pytest.raises(FatalSingleDestinationError, match="delete/drop activity"):
            d._assert_no_deletes()
        witness = next(c for c in d.feed._pg.return_value.execute.call_args_list if "snapshot_changes" in str(c))
        pattern = witness.args[1][1]  # params: (cursor, pattern)
        for word in ("deleted_from_table", "inlined_delete", "rewrite_delete", "dropped_table"):
            assert word in pattern
        assert ":16" in pattern

    def test_inlined_delete_table_nonempty_crashes(self):
        d = _single_destination()
        d.feed._pg.return_value = self._pg_with_counts(
            {"ducklake_inlined_delete_16": 2}, regclass="lake_meta.ducklake_inlined_delete_16"
        )
        with pytest.raises(FatalSingleDestinationError, match="inlined deletes"):
            d._assert_no_deletes()

    def test_inlined_delete_table_absent_passes(self):
        d = _single_destination()
        d.feed._pg.return_value = self._pg_with_counts({}, regclass=None)
        d._assert_no_deletes()

    def test_inline_rows_page_but_continue(self):
        from prometheus_client import REGISTRY

        d = _single_destination()
        d.feed._pg.return_value = self._pg_with_counts(
            {"ducklake_inlined_data_16_1": 2}, stores=["ducklake_inlined_data_16_1"]
        )
        metric = "viaduck_single_destination_assertion_failures_total"
        before = REGISTRY.get_sample_value(metric, {"check": "inline_rows_present"}) or 0
        d._check_inline_stores()  # no raise — the feed serves inline correctly
        after = REGISTRY.get_sample_value(metric, {"check": "inline_rows_present"})
        assert after == before + 1  # the page WIRED, not just the log line

    def test_inline_registry_alone_is_quiet(self):
        """Registry membership is normal (stores register at CREATE TABLE
        even with row_limit=0); only ROWS in stores are drift."""
        d = _single_destination()
        d.feed._pg.return_value = self._pg_with_counts({}, stores=["ducklake_inlined_data_16_1"])
        d._check_inline_stores()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def test_from_env_minimal(self, monkeypatch):
        for k, v in {
            "SOURCE_PG_URI": "postgres:host=s dbname=m user=u password=p",
            "SOURCE_CATALOG": "lake",
            "SOURCE_DATA_PATH": "s3://b/src",
            "SOURCE_TABLE": "main.events_nrt",
            "DEST_PG_URI": "postgres:host=d dbname=x user=u password=p",
            "DEST_CATALOG": "dest",
            "DEST_DATA_PATH": "s3://b/dst",
            "DEST_TABLE": "posthog.events",
            "TEAM_FIELD": "team_id",
            "TEAM_VALUE": "2",
            "DESTINATION_ID": "org-abc-team-2",
        }.items():
            monkeypatch.setenv(k, v)
        cfg = SingleDestinationConfig.from_env()
        assert cfg.cursor_pg_uri == cfg.source_pg_uri  # colocated default
        assert cfg.instance_id == "single-destination"  # STABLE — never a pod name
        assert cfg.unit_max_rows == 50_000

    def test_unsafe_identifier_refused(self, monkeypatch):
        monkeypatch.setenv("SOURCE_TABLE", "events; DROP TABLE x")
        with pytest.raises(sd.ConfigError):
            SingleDestinationConfig.from_env()

    def test_dest_managed_columns_parsed(self, monkeypatch):
        monkeypatch.setenv("DEST_MANAGED_COLUMNS", "_inserted_at, _raw")
        # via from_env with the full minimal set
        for k, v in {
            "SOURCE_PG_URI": "postgres:host=s dbname=m user=u password=p",
            "SOURCE_CATALOG": "lake",
            "SOURCE_DATA_PATH": "s3://b/src",
            "SOURCE_TABLE": "main.t",
            "DEST_PG_URI": "postgres:host=d dbname=x user=u password=p",
            "DEST_CATALOG": "dest",
            "DEST_DATA_PATH": "s3://b/dst",
            "DEST_TABLE": "main.t",
            "TEAM_FIELD": "team_id",
            "TEAM_VALUE": "2",
            "DESTINATION_ID": "d",
        }.items():
            monkeypatch.setenv(k, v)
        assert SingleDestinationConfig.from_env().dest_managed_columns == frozenset({"_inserted_at", "_raw"})


# ---------------------------------------------------------------------------
# Boot wiring
# ---------------------------------------------------------------------------


def _boot_mocks(d: SingleDestinationViaduck, cursor_row=(100, 16), head=900, winner_row=None):
    """Patch catalog/feed/psycopg collaborators for boot(); returns mocks.

    winner_row: when cursor_row is None (first boot), the re-SELECT after
    ON CONFLICT DO NOTHING returns THIS — simulating a maxSurge winner's
    row, so tests pin that the loser ADOPTS the stored row."""
    src_table = MagicMock()
    src_table.schema.column_names.return_value = ("team_id", "event")
    src_table.schema.as_arrow.return_value = pa.schema([("team_id", pa.int64()), ("event", pa.string())])

    cursor_pg = MagicMock()
    state = {"loaded": False}

    def cursor_execute(sql, params=None):
        cur = MagicMock()
        if "SELECT last_snapshot_id" in sql:
            if not state["loaded"]:
                state["loaded"] = True
                cur.fetchone.return_value = cursor_row  # None = no row yet
            else:
                # persist re-SELECT: the row as stored (winner's value)
                cur.fetchone.return_value = cursor_row if cursor_row is not None else (winner_row or (head,))
        return cur

    cursor_pg.execute.side_effect = cursor_execute

    catalog_pg = MagicMock()

    def catalog_execute(sql, params=None):
        cur = MagicMock()
        if "ducklake_table" in sql:
            cur.fetchone.return_value = (16,)
        elif "MAX(snapshot_id)" in sql:
            cur.fetchone.return_value = (head,)
        elif "MIN(snapshot_id)" in sql:
            cur.fetchone.return_value = (None,)
        elif "to_regclass" in sql:
            cur.fetchone.return_value = (None,)
        elif "pg_indexes" in sql:
            cur.fetchall.return_value = []
        elif "ducklake_inlined_data_tables" in sql:
            cur.fetchall.return_value = []
        else:
            cur.fetchone.return_value = (0,)
        return cur

    catalog_pg.execute.side_effect = catalog_execute
    catalog = MagicMock()
    catalog.load_table.return_value = src_table
    return src_table, cursor_pg, catalog_pg, catalog


class TestBoot:
    def test_boot_wiring_and_conninfo_translation(self):
        """The F2 pin at this layer: ATTACH-format secret in, libpq conninfo
        to psycopg — or first boot crashes."""
        cfg = _cfg()
        d = SingleDestinationViaduck(cfg)
        src_table, cursor_pg, catalog_pg, catalog = _boot_mocks(d)

        with (
            patch.object(sd.source, "safe_catalog", return_value=catalog),
            patch.object(sd.source, "load_table", return_value=src_table),
            patch.object(sd.feed, "FeedReader") as fr_cls,
            patch.object(sd.psycopg, "connect", return_value=cursor_pg),
        ):
            fr_cls.return_value._meta_schema = "lake_meta"
            fr_cls.return_value._pg.return_value = catalog_pg
            d.boot()

        assert fr_cls.call_args.kwargs["postgres_uri"] == "host=src port=5432 dbname=megaduck user=m password=pw"
        assert d._cursor == 100  # existing row read back (not reset to head)
        assert d.columns == ("team_id", "event")
        # team value precomputed as a typed Arrow array
        assert d._team_array.type == pa.int64()

    def test_cursor_initialized_at_head_when_absent(self):
        cfg = _cfg()
        d = SingleDestinationViaduck(cfg)
        src_table, cursor_pg, catalog_pg, catalog = _boot_mocks(d, cursor_row=None)

        with (
            patch.object(sd.source, "safe_catalog", return_value=catalog),
            patch.object(sd.source, "load_table", return_value=src_table),
            patch.object(sd.feed, "FeedReader") as fr_cls,
            patch.object(sd.psycopg, "connect", return_value=cursor_pg),
        ):
            fr_cls.return_value._meta_schema = "lake_meta"
            fr_cls.return_value._pg.return_value = catalog_pg
            d.boot()

        assert d._cursor == 900  # head at boot
        insert = next(c for c in cursor_pg.execute.call_args_list if "INSERT INTO viaduck.viaduck_state" in str(c))
        assert 16 in insert.args[1]  # source_table_id provenance recorded

    def test_first_boot_race_adopts_winner_row(self):
        """maxSurge: two pods boot one destination concurrently; the ON
        CONFLICT loser's own head reading could be AHEAD of the winner's
        cursor — adopting it would silently skip the between range. The
        re-SELECT pins adoption."""
        cfg = _cfg()
        d = SingleDestinationViaduck(cfg)
        src_table, cursor_pg, catalog_pg, catalog = _boot_mocks(d, cursor_row=None, winner_row=(850,))

        with (
            patch.object(sd.source, "safe_catalog", return_value=catalog),
            patch.object(sd.source, "load_table", return_value=src_table),
            patch.object(sd.feed, "FeedReader") as fr_cls,
            patch.object(sd.psycopg, "connect", return_value=cursor_pg),
        ):
            fr_cls.return_value._meta_schema = "lake_meta"
            fr_cls.return_value._pg.return_value = catalog_pg
            d.boot()

        assert d._cursor == 850  # the winner's stored row, not our head (900)

    def test_dest_column_reconciliation_adds_missing(self):
        cfg = _cfg()
        d = SingleDestinationViaduck(cfg)
        src_table, cursor_pg, catalog_pg, catalog = _boot_mocks(d)
        catalog.create_table_if_not_exists.return_value.schema.column_names.return_value = ("team_id",)

        with (
            patch.object(sd.source, "safe_catalog", return_value=catalog),
            patch.object(sd.source, "load_table", return_value=src_table),
            patch.object(sd.feed, "FeedReader") as fr_cls,
            patch.object(sd.psycopg, "connect", return_value=cursor_pg),
        ):
            fr_cls.return_value._meta_schema = "lake_meta"
            fr_cls.return_value._pg.return_value = catalog_pg
            d.boot()

        dst_table = catalog.create_table_if_not_exists.return_value
        dst_table.update_schema.assert_called_once()

    def test_dest_extra_column_wedges_but_managed_columns_pass(self):
        cfg = _cfg()
        d = SingleDestinationViaduck(cfg)
        src_table, cursor_pg, catalog_pg, catalog = _boot_mocks(d)
        catalog.create_table_if_not_exists.return_value.schema.column_names.return_value = (
            "team_id",
            "event",
            "mystery",
            "_inserted_at",  # dest-managed default: excluded from the wedge
        )

        with (
            patch.object(sd.source, "safe_catalog", return_value=catalog),
            patch.object(sd.source, "load_table", return_value=src_table),
            patch.object(sd.feed, "FeedReader") as fr_cls,
            patch.object(sd.psycopg, "connect", return_value=cursor_pg),
        ):
            fr_cls.return_value._meta_schema = "lake_meta"
            fr_cls.return_value._pg.return_value = catalog_pg
            with pytest.raises(FatalSingleDestinationError, match="mystery"):
                d.boot()

    def test_integer_team_value_validated_at_boot(self):
        cfg = _cfg(team_value="not-an-int")
        d = SingleDestinationViaduck(cfg)
        src_table, cursor_pg, catalog_pg, catalog = _boot_mocks(d)

        with (
            patch.object(sd.source, "safe_catalog", return_value=catalog),
            patch.object(sd.source, "load_table", return_value=src_table),
            patch.object(sd.feed, "FeedReader") as fr_cls,
            patch.object(sd.psycopg, "connect", return_value=cursor_pg),
        ):
            fr_cls.return_value._meta_schema = "lake_meta"
            fr_cls.return_value._pg.return_value = catalog_pg
            with pytest.raises(FatalSingleDestinationError, match="not an integer"):
                d.boot()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_healthy_after_recent_poll(self):
        d = _single_destination()
        assert d.is_healthy()

    def test_stale_after_threshold(self):
        d = _single_destination()
        d._last_poll_ok = time.monotonic() - 400  # 300s floor
        assert not d.is_healthy()


class TestMetricsMove:
    def test_lag_gauge_and_delivered_counter(self):
        from prometheus_client import REGISTRY

        d = _single_destination()
        rows = pa.table({"team_id": [2, 3], "event": ["a", "b"]})
        _poll_ready(d, rows)
        before = REGISTRY.get_sample_value("viaduck_single_destination_rows_delivered_total") or 0
        d.poll_once()
        assert (REGISTRY.get_sample_value("viaduck_single_destination_rows_delivered_total") or 0) == before + 1
        assert REGISTRY.get_sample_value("viaduck_single_destination_lag_snapshots") == 400  # head 500 - cursor 100
