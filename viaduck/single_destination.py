"""viaduck.single_destination — the per-destination append-only replicator.

Design: per-destination-duckling.md (v3). Millpond posture: single thread,
single loop, one destination per process; flush first, cursor second;
3 attempts then crash and let K8s restart; at-least-once by design
(duplicates are deduped downstream by event uuid).

One process = one source table + one destination + one team_id row filter.
The filter is Arrow-side ONLY — the parquet/inline SQL path deliberately
gets no filter: a zone-map/statistics lie in a foreign-registered
(add_files) file would be invisible under-delivery for the SQL layer; the
Arrow filter is the only correctness layer.

The cursor is a row in the existing viaduck_state table on the SOURCE
catalog's Postgres (operator-owned; override with CURSOR_PG_URI), keyed by
(destination_id, instance_id) where instance_id is a STABLE per-destination
constant — never a pod name: a rollout under a pod-name key would find no
cursor row, start at head, and silently skip the undelivered range
(review C2).

    python -m viaduck.single_destination
"""

from __future__ import annotations

import logging
import os
import random
import re
import signal
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from viaduck import feed, metrics, source
from viaduck.config import _to_libpq_conninfo
from viaduck.logging_config import setup as setup_logging
from viaduck.scrub import scrub_credentials

log = logging.getLogger("viaduck.duckling")


class ConfigError(Exception):
    pass


class FatalDucklingError(RuntimeError):
    """Crash-class failure (append exhausted, cursor exhausted, assertion
    violation, source rebuild): the process must die, not retry — K8s
    backoff is the supervisor. run() re-raises these; everything else is a
    transient read error retried next poll (the cursor never advanced)."""


# ---------------------------------------------------------------------------
# Metrics (one process = one destination: no pipeline label needed)
# ---------------------------------------------------------------------------

rows_read_total = Counter("viaduck_duckling_rows_read_total", "Rows read from the source (pre-filter)")
rows_delivered_total = Counter("viaduck_duckling_rows_delivered_total", "Rows appended to the destination")
flush_seconds = Histogram("viaduck_duckling_flush_seconds", "Destination append latency")
unit_budget_rows = Gauge("viaduck_duckling_unit_budget_rows", "Current AIMD read-unit row budget")
lag_snapshots = Gauge("viaduck_duckling_lag_snapshots", "Source head minus committed cursor")
cursor_below_floor = Gauge(
    "viaduck_duckling_cursor_below_floor", "1 while the cursor is under the retained snapshot floor"
)
assertion_failures_total = Counter(
    "viaduck_duckling_assertion_failures_total", "Per-poll assertion failures", ["check"]
)
polls_total = Counter("viaduck_duckling_polls_total", "Loop iterations", ["result"])

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUALIFIED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _env(name: str, default: str | None = None, required: bool = True) -> str:
    v = os.environ.get(name, default)
    if required and (v is None or v == ""):
        raise ConfigError(f"{name} is required")
    return v or ""


@dataclass
class DucklingConfig:
    source_pg_uri: str
    source_catalog: str
    source_data_path: str
    source_table: str  # possibly schema-qualified
    dest_pg_uri: str
    dest_catalog: str
    dest_data_path: str
    dest_table: str
    team_field: str
    team_value: str  # validated against the pinned column's type at boot
    destination_id: str
    cursor_pg_uri: str = ""  # defaults to source_pg_uri
    instance_id: str = "duckling"  # STABLE per destination — never a pod name
    dest_managed_columns: frozenset[str] = frozenset({"_inserted_at"})
    s3_properties: dict[str, str] = field(default_factory=dict)
    poll_interval_s: float = 5.0
    unit_max_rows: int = 50_000
    unit_max_bytes: int = 256 * 1024 * 1024
    unit_max_span: int = 10_000
    aimd_floor_rows: int = 5_000
    slow_flush_seconds: float = 30.0
    start_snapshot_id: int | None = None  # None = start at head
    rss_limit_bytes: int = 0  # 0 = disabled
    port: int = 8000
    attempts: int = 3

    def __post_init__(self) -> None:
        if not self.cursor_pg_uri:
            # Colocated default: the cursor lives on the source catalog's PG.
            self.cursor_pg_uri = self.source_pg_uri

    @classmethod
    def from_env(cls) -> DucklingConfig:
        def ident(name: str, pattern=_IDENT) -> str:
            v = _env(name)
            if not pattern.match(v):
                raise ConfigError(f"{name}={v!r} is not a safe identifier")
            return v

        s3 = {k[len("VIADUCK_S3_") :].lower(): v for k, v in os.environ.items() if k.startswith("VIADUCK_S3_")}
        managed = frozenset(
            c.strip() for c in _env("DEST_MANAGED_COLUMNS", "_inserted_at", required=False).split(",") if c.strip()
        )
        return cls(
            source_pg_uri=_env("SOURCE_PG_URI"),
            source_catalog=ident("SOURCE_CATALOG"),
            source_data_path=_env("SOURCE_DATA_PATH"),
            source_table=ident("SOURCE_TABLE", _QUALIFIED),
            dest_pg_uri=_env("DEST_PG_URI"),
            dest_catalog=ident("DEST_CATALOG"),
            dest_data_path=_env("DEST_DATA_PATH"),
            dest_table=ident("DEST_TABLE", _QUALIFIED),
            team_field=ident("TEAM_FIELD"),
            team_value=_env("TEAM_VALUE"),
            destination_id=_env("DESTINATION_ID"),
            cursor_pg_uri=_env("CURSOR_PG_URI", "", required=False),
            instance_id=_env("INSTANCE_ID", "duckling", required=False),
            dest_managed_columns=managed,
            s3_properties=s3,
            poll_interval_s=float(_env("POLL_INTERVAL_S", "5", required=False)),
            unit_max_rows=int(_env("UNIT_MAX_ROWS", "50000", required=False)),
            unit_max_bytes=int(_env("UNIT_MAX_BYTES", str(256 * 1024 * 1024), required=False)),
            unit_max_span=int(_env("UNIT_MAX_SPAN", "10000", required=False)),
            start_snapshot_id=(int(v) if (v := os.environ.get("START_SNAPSHOT_ID")) else None),
            rss_limit_bytes=int(_env("RSS_LIMIT_BYTES", "0", required=False)),
            port=int(_env("PORT", "8000", required=False)),
        )


# ---------------------------------------------------------------------------
# The duckling
# ---------------------------------------------------------------------------


class Duckling:
    def __init__(self, cfg: DucklingConfig):
        self.cfg = cfg
        self._stop = threading.Event()
        self._last_poll_ok = time.monotonic()
        self._budget_rows = cfg.unit_max_rows
        unit_budget_rows.set(self._budget_rows)  # seed: floor-pinned must be
        # distinguishable from never-flushed (viaduck#63 lesson)
        self._cursor = 0
        self._cursor_row_exists = False
        self._cursor_conn: psycopg.Connection | None = None

    # ---------------- connections ---------------- #

    def _catalog_pg(self) -> psycopg.Connection:
        """Catalog-side queries (assertions, clamp, table_id resolution) run
        on the FEED's connection: they must hit the SOURCE catalog's Postgres
        even when CURSOR_PG_URI points elsewhere. The feed's connection
        reconnects on close (feed.py's own discipline)."""
        return self.feed._pg()

    def _cursor_pg(self) -> psycopg.Connection:
        if self._cursor_conn is None or self._cursor_conn.closed:
            self._cursor_conn = psycopg.connect(
                _to_libpq_conninfo(self.cfg.cursor_pg_uri),
                autocommit=True,
                connect_timeout=10,
                prepare_threshold=None,
                options="-c statement_timeout=30000 -c idle_in_transaction_session_timeout=60000",
            )
        return self._cursor_conn

    # ---------------- boot ---------------- #

    def boot(self) -> None:
        try:
            self._boot_inner()
        except FatalDucklingError:
            raise
        except Exception as e:
            # ATTACH/connect errors embed the full conninfo — scrub before
            # the message can reach pod logs.
            raise FatalDucklingError(f"boot failed: {scrub_credentials(str(e))}") from None

    def _boot_inner(self) -> None:
        cfg = self.cfg
        # Source catalog (ducklake attach via pyducklake; carries S3 creds
        # for the parquet data plane).
        props = source.with_connection_defaults(cfg.s3_properties, name=cfg.source_catalog)
        self.src_catalog = source.safe_catalog(
            cfg.source_catalog, cfg.source_pg_uri, data_path=cfg.source_data_path, properties=props
        )
        self.src_table = source.load_table(self.src_catalog, cfg.source_table, required_columns=(cfg.team_field,))
        # Pin the source column set at boot (fleet semantics): a NEW source
        # column is invisible until restart — restart is the schema-refresh
        # mechanism. A DISAPPEARING pinned column wedges loudly below.
        self.columns: tuple[str, ...] = tuple(self.src_table.schema.column_names())
        self._validate_team_value()

        # Direct-SQL feed over the same catalog (psycopg planning plane).
        self.feed = feed.FeedReader(
            postgres_uri=_to_libpq_conninfo(cfg.source_pg_uri),
            catalog_name=cfg.source_catalog,
            data_path=cfg.source_data_path,
        )
        self.feed.verify_catalog()
        self.meta = self.feed._meta_schema  # same-repo pin (feed detects it)

        self.table_id = self._resolve_table_id()
        self._probe_assertion_indexes()

        # Destination: create-if-missing with the pinned schema, then add
        # any missing pinned columns (additive evolution picked up at boot).
        dprops = source.with_connection_defaults(cfg.s3_properties, name=cfg.dest_catalog)
        self.dst_catalog = source.safe_catalog(
            cfg.dest_catalog, cfg.dest_pg_uri, data_path=cfg.dest_data_path, properties=dprops
        )
        if "." in cfg.dest_table:
            self.dst_catalog.create_namespace_if_not_exists(cfg.dest_table.rsplit(".", 1)[0])
        dst_schema = self.src_table.schema.select(*self.columns)
        self.dst_table = self.dst_catalog.create_table_if_not_exists(cfg.dest_table, dst_schema)
        self._reconcile_dest_columns()
        fqn = self.dst_table.fully_qualified_name
        if any(c in fqn for c in (";", "--", "/*", "*/", "\n", "\r")):
            raise ConfigError(f"destination FQN {fqn!r} contains SQL metacharacters")
        self._dest_fqn = fqn

        # Cursor row in viaduck_state (existing table; the duckling adds one
        # additive column for table_id provenance — a drop+recreate WHILE
        # DOWN is otherwise undetectable: boot would resolve the new id and
        # the witness is keyed to it). Baseline assertions run BEFORE the
        # first cursor write: a first boot on a contract-violated table
        # crashes without leaving a head-positioned cursor row behind.
        self._ensure_state_table()
        self._cursor_load()
        self._assert_no_deletes()
        self._check_inline_stores()
        self._cursor_persist()
        log.info(
            "duckling up: %s → %s (team %s=%s), cursor=%d, columns=%d",
            cfg.source_table,
            cfg.dest_table,
            cfg.team_field,
            cfg.team_value,
            self._cursor,
            len(self.columns),
        )

    def _validate_team_value(self) -> None:
        """Type-check the team filter value once at boot against the pinned
        column's Arrow type; precompute the comparison array."""
        field = self.src_table.schema.as_arrow().field(self.cfg.team_field)
        if pa.types.is_integer(field.type):
            try:
                value = int(self.cfg.team_value)
            except ValueError:
                raise ConfigError(
                    f"TEAM_VALUE={self.cfg.team_value!r} is not an integer but {self.cfg.team_field} is {field.type}"
                ) from None
            self._team_array = pa.array([value], type=field.type)
        else:
            self._team_array = pa.array([self.cfg.team_value], type=field.type)

    def _reconcile_dest_columns(self) -> None:
        dst_cols = set(self.dst_table.schema.column_names())
        missing = [c for c in self.columns if c not in dst_cols]
        if missing:
            evo = self.dst_table.update_schema()
            for c in missing:
                evo = evo.add_column(c, self.src_table.schema.find_type(c))
            evo.commit()
            log.info("destination gained %d pinned columns: %s", len(missing), missing)
        extra = dst_cols - set(self.columns) - set(self.cfg.dest_managed_columns)
        if extra:
            raise ConfigError(
                f"destination has columns the source no longer has: {sorted(extra)} — "
                "rename/drop is not supported (no mapping_id story); operator adjudication required "
                "(dest-managed columns belong in DEST_MANAGED_COLUMNS)"
            )

    def _probe_assertion_indexes(self) -> None:
        """WARN if the §5.2 assertion queries' supporting indexes are
        missing — they are seq-scans without them (fine at N=1, catalog-melt
        at N=300; the §13 index creation is the deploy precondition)."""
        wanted = {
            "viaduck_delete_file_table": f'"{self.meta}".ducklake_delete_file',
            "viaduck_data_file_end_snapshot": f'"{self.meta}".ducklake_data_file',
        }
        rows = (
            self._catalog_pg()
            .execute("SELECT indexname, tablename FROM pg_indexes WHERE schemaname = %s", (self.meta,))
            .fetchall()
        )
        present = {r[0] for r in rows}
        for name, table in wanted.items():
            if name not in present:
                log.warning(
                    "assertion index %s on %s is MISSING — per-poll delete checks seq-scan until it exists",
                    name,
                    table,
                )

    def _resolve_table_id(self) -> int:
        namespace, _, table = self.cfg.source_table.rpartition(".")
        namespace = namespace or "main"
        row = (
            self._catalog_pg()
            .execute(
                f'SELECT t.table_id FROM "{self.meta}".ducklake_table t '
                f'JOIN "{self.meta}".ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL '
                f"WHERE t.table_name = %s AND s.schema_name = %s AND t.end_snapshot IS NULL",
                (table, namespace),
            )
            .fetchone()
        )
        if row is None:
            raise ConfigError(f"source table {self.cfg.source_table!r} not found (or dropped)")
        return int(row[0])

    # ---------------- cursor ---------------- #

    def _ensure_state_table(self) -> None:
        pg = self._cursor_pg()
        try:
            pg.execute("CREATE SCHEMA IF NOT EXISTS viaduck")
            pg.execute(
                """
                CREATE TABLE IF NOT EXISTS viaduck.viaduck_state (
                    destination_id     text        NOT NULL,
                    instance_id        text        NOT NULL,
                    last_snapshot_id   bigint      NOT NULL,
                    last_replicated_at timestamptz,
                    rows_replicated    bigint      NOT NULL DEFAULT 0,
                    last_error         text,
                    last_error_at      timestamptz,
                    updated_at         timestamptz NOT NULL,
                    PRIMARY KEY (destination_id, instance_id)
                )
                """
            )
        except (psycopg.errors.DuplicateSchema, psycopg.errors.DuplicateTable, psycopg.errors.UniqueViolation):
            pass  # concurrent first boot (state.py's race guard)
        # Duckling-managed additive column (the fleet never reads it): the
        # table_id the cursor position was earned against.
        pg.execute("ALTER TABLE viaduck.viaduck_state ADD COLUMN IF NOT EXISTS source_table_id bigint")

    def _cursor_load(self) -> None:
        """Read the cursor row (or compute the start position) WITHOUT
        writing — the baseline assertions run before any cursor write."""
        row = (
            self._cursor_pg()
            .execute(
                "SELECT last_snapshot_id, source_table_id FROM viaduck.viaduck_state"
                " WHERE destination_id = %s AND instance_id = %s",
                (self.cfg.destination_id, self.cfg.instance_id),
            )
            .fetchone()
        )
        if row is not None:
            self._cursor = int(row[0])
            self._cursor_row_exists = True
            stored_tid = row[1]
            if stored_tid is not None and int(stored_tid) != self.table_id:
                raise FatalDucklingError(
                    f"cursor was earned against table_id={int(stored_tid)} but the source table is now "
                    f"table_id={self.table_id}: the source was dropped+recreated while this duckling was "
                    "down — mandatory re-seed (delete the cursor row to restart at head)"
                )
            if stored_tid is None:
                # Backfill provenance (fleet-transplanted cursors arrive
                # with NULL — the documented migration path; without this a
                # drop+recreate-while-down is undetectable).
                self._cursor_pg().execute(
                    "UPDATE viaduck.viaduck_state SET source_table_id = %s"
                    " WHERE destination_id = %s AND instance_id = %s AND source_table_id IS NULL",
                    (self.table_id, self.cfg.destination_id, self.cfg.instance_id),
                )
            return
        self._cursor_row_exists = False
        start = self.cfg.start_snapshot_id
        if start is None:
            start = self._head()  # start at head
        self._cursor = start

    def _cursor_persist(self) -> None:
        if self._cursor_row_exists:
            return
        self._cursor_pg().execute(
            "INSERT INTO viaduck.viaduck_state (destination_id, instance_id, last_snapshot_id,"
            " last_replicated_at, rows_replicated, updated_at, source_table_id)"
            " VALUES (%s, %s, %s, now(), 0, now(), %s)"
            " ON CONFLICT (destination_id, instance_id) DO NOTHING",
            (self.cfg.destination_id, self.cfg.instance_id, self._cursor, self.table_id),
        )
        # A concurrent first boot (Deployment maxSurge) may have won the
        # insert — ALWAYS adopt the stored row, never our own head reading
        # (the loser's head could be ahead of the winner's cursor; adopting
        # it would skip delivery of the between range).
        row = (
            self._cursor_pg()
            .execute(
                "SELECT last_snapshot_id FROM viaduck.viaduck_state WHERE destination_id = %s AND instance_id = %s",
                (self.cfg.destination_id, self.cfg.instance_id),
            )
            .fetchone()
        )
        self._cursor = int(row[0])
        log.info("cursor initialized at %d", self._cursor)

    def _cursor_write(self, sql: str, params, what: str) -> None:
        """Cursor-DB write with the millpond retry posture: attempts, then
        fatal. (The clamp's loss-note write gets the same treatment as the
        cursor advance — a cursor-DB outage is never a quiet retry loop.)"""
        last_err = None
        for attempt in range(self.cfg.attempts):
            try:
                self._cursor_pg().execute(sql, params)
                return
            except Exception as e:
                last_err = e
                log.warning(
                    "%s failed (attempt %d/%d): %s", what, attempt + 1, self.cfg.attempts, scrub_credentials(str(e))
                )
                time.sleep(0.5 * (attempt + 1))
        raise FatalDucklingError(
            f"{what} failed after {self.cfg.attempts} attempts: {scrub_credentials(str(last_err))}"
        )

    def _cursor_advance(self, hi: int, rows: int) -> None:
        self._cursor_write(
            # source_table_id rides the INSERT leg: if the row vanished
            # mid-run (operator delete; fleet `retired` severing rows in the
            # migration window), the UPSERT re-creates it — with provenance,
            # or the drop+recreate-while-down hole reopens.
            "INSERT INTO viaduck.viaduck_state (destination_id, instance_id, last_snapshot_id,"
            " last_replicated_at, rows_replicated, updated_at, source_table_id)"
            " VALUES (%s, %s, %s, now(), %s, now(), %s)"
            " ON CONFLICT (destination_id, instance_id) DO UPDATE"
            " SET last_snapshot_id = EXCLUDED.last_snapshot_id, last_replicated_at = now(),"
            # rows_replicated accumulates DELIVERY ATTEMPTS — crash-replays
            # re-add the unit, so the gauge drifts above distinct rows.
            # Semantics match the fleet's; operators comparing against
            # source counts should read rows_delivered with that lens.
            " rows_replicated = viaduck.viaduck_state.rows_replicated + EXCLUDED.rows_replicated,"
            " updated_at = now()"
            # Monotonic guard (fleet semantics): a maxSurge pair sharing the
            # row must never regress it (a regression re-delivers — bounded
            # duplicates, but pointless). The clamp's loss note is NOT
            # cleared here on purpose: retention loss is operator-visible
            # until adjudicated, unlike transient write errors.
            " WHERE viaduck.viaduck_state.last_snapshot_id <= EXCLUDED.last_snapshot_id",
            (self.cfg.destination_id, self.cfg.instance_id, hi, rows, self.table_id),
            "cursor update",
        )
        self._cursor = hi

    # ---------------- head ---------------- #

    def _head(self) -> int:
        """MAX(snapshot_id) on the psycopg plane. NEVER pyducklake's
        table.snapshots()/current_snapshot() — that materializes the whole
        snapshot table through the DuckDB postgres scanner per call (the
        fleet measured the RSS slope; source.py documents it)."""
        row = self._catalog_pg().execute(f'SELECT MAX(snapshot_id) FROM "{self.meta}".ducklake_snapshot').fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # ---------------- assertions (per-poll) ---------------- #

    def _count(self, sql: str, params=()) -> int:
        return int(self._catalog_pg().execute(sql, params).fetchone()[0])

    def _assert_no_deletes(self) -> None:
        """The append-only contract, enforced operationally. Merge/expire
        hard-DELETE catalog rows (never set end_snapshot), so a nonzero
        count here means a real delete / rewrite-deletes / drop — all of
        which silently break offset-tailing (REWRITE_DELETES re-offsets
        surviving rows). Crash; the operator adjudicates (re-seed or accept).

        All four checks are scoped to UN-CROSSED history (snapshot >
        cursor): commits are id-ordered, so a delete can never land at a
        snapshot the reader already passed — and "accept" becomes
        implementable as one cursor UPDATE + restart instead of catalog
        surgery or waiting out retention (round-3 C1)."""
        tid = self.table_id
        cur = self._cursor
        checks = {
            "delete_file": (
                f'SELECT count(*) FROM "{self.meta}".ducklake_delete_file WHERE table_id = %s AND begin_snapshot > %s',
                (tid, cur),
            ),
            "end_snapshot": (
                f'SELECT count(*) FROM "{self.meta}".ducklake_data_file'
                " WHERE table_id = %s AND end_snapshot IS NOT NULL AND end_snapshot > %s",
                (tid, cur),
            ),
        }
        for name, (sql, params) in checks.items():
            n = self._count(sql, params)
            if n:
                assertion_failures_total.labels(check=name).inc()
                raise FatalDucklingError(f"append-only contract violated: {n} rows in {name} for table_id={tid}")
        # Retention-lived witness: survives the delete+merge-between-polls
        # race that erases the two checks above. changes_made is a
        # comma-joined `type:table_id` list (ducklake_transaction.cpp
        # AddChangeInfo); the delete vocabulary is exactly:
        # deleted_from_table / inlined_delete / rewrite_delete / dropped_table.
        witness = self._count(
            f'SELECT count(*) FROM "{self.meta}".ducklake_snapshot_changes '
            "WHERE snapshot_id > %s AND changes_made ~* %s",
            (cur, rf"(^|,)(deleted_from_table|inlined_delete|rewrite_delete|dropped_table):{tid}(,|$)"),
        )
        if witness:
            assertion_failures_total.labels(check="snapshot_changes_witness").inc()
            raise FatalDucklingError(f"delete/drop activity for table_id={tid} in snapshot_changes")
        # Inlined deletes: their own per-table PG table, invisible to all of
        # the above (round-2 C2). Existence probe first, then count.
        store = (
            self._catalog_pg()
            .execute(
                "SELECT to_regclass(%s)",
                (f'"{self.meta}"."ducklake_inlined_delete_{tid}"',),  # quoted: PG case-folds unquoted input
            )
            .fetchone()[0]
        )
        if store is not None:
            n = self._count(
                f'SELECT count(*) FROM "{self.meta}"."ducklake_inlined_delete_{tid}" WHERE begin_snapshot > %s',
                (cur,),
            )
            if n:
                assertion_failures_total.labels(check="inlined_delete").inc()
                raise FatalDucklingError(f"{n} inlined deletes for table_id={tid}")

    def _check_inline_stores(self) -> None:
        """Inlining is an attach-scoped writer option — unverifiable in the
        catalog, and the registry is populated at CREATE TABLE even with
        row_limit=0 (verified on d8a1881e), so membership is NOT the signal.
        Rows in the stores are. If inline data appears the feed's (parity-
        tested) inline path still SERVES it; page so the drift is known."""
        stores = (
            self._catalog_pg()
            .execute(
                f'SELECT table_name FROM "{self.meta}".ducklake_inlined_data_tables WHERE table_id = %s',
                (self.table_id,),
            )
            .fetchall()
        )
        for (store,) in stores:
            store = store.replace('"', '""')
            n = self._count(f'SELECT EXISTS(SELECT 1 FROM "{self.meta}"."{store}")')
            if n:
                assertion_failures_total.labels(check="inline_rows_present").inc()
                log.error(
                    "INLINE DATA PRESENT for table_id=%d (store %s) — served correctly, "
                    "but the writer drifted from contract",
                    self.table_id,
                    store,
                )

    # ---------------- retention clamp ---------------- #

    def _clamp_to_retention(self) -> None:
        """Cursor below the retained floor = the expired prefix may have held
        this table's commits. Advance LOUDLY with a durable loss note (fleet
        _clamp_expired_cursors semantics); never crash-loop in the window.
        The feed's in-transaction floor guard is the backstop for an expire
        landing between this check and the plan."""
        row = self._catalog_pg().execute(f'SELECT MIN(snapshot_id) FROM "{self.meta}".ducklake_snapshot').fetchone()
        floor = int(row[0]) if row and row[0] is not None else None
        if floor is None or self._cursor >= floor - 1:
            cursor_below_floor.set(0)
            return
        cursor_below_floor.set(1)
        note = (
            f"retention clamp: cursor {self._cursor} advanced to {floor - 1}; "
            f"snapshots ({self._cursor}, {floor - 1}] expired unread — DATA LOSS, re-seed if the window mattered"
        )
        log.error(note)
        self._cursor_write(
            "UPDATE viaduck.viaduck_state SET last_snapshot_id = %s, last_error = %s, last_error_at = now(),"
            " updated_at = now() WHERE destination_id = %s AND instance_id = %s"
            # never regress a maxSurge peer that's ahead of the clamp
            " AND last_snapshot_id <= %s",
            (floor - 1, note, self.cfg.destination_id, self.cfg.instance_id, floor - 1),
            "retention-clamp loss note",
        )
        self._cursor = floor - 1

    # ---------------- append with retry + AIMD ---------------- #

    def _append_once(self, batch: pa.Table) -> None:
        """INSERT BY NAME. pyducklake's table.append is POSITIONAL
        (INSERT INTO t SELECT * FROM tmp): a dest-managed column with a
        DEFAULT fails every append, and a reordered-but-same-named dest
        table silently swaps values (the fleet's schema_projection exists
        for this class). BY NAME maps by column name and default-fills
        dest-managed columns."""
        conn = self.dst_catalog.connection
        view = "_duckling_append"
        conn.register(view, batch)
        try:
            conn.execute(f"INSERT INTO {self._dest_fqn} BY NAME SELECT * FROM {view}")
        finally:
            conn.unregister(view)

    def _append(self, batch: pa.Table) -> None:
        last_err = None
        for attempt in range(self.cfg.attempts):
            t0 = time.monotonic()
            try:
                self._append_once(batch)
                dt = time.monotonic() - t0
                flush_seconds.observe(dt)
                if dt > self.cfg.slow_flush_seconds:
                    self._aimd_halve(f"slow flush {dt:.1f}s")
                else:
                    self._aimd_recover()
                return
            except Exception as e:
                last_err = e
                log.warning(
                    "append failed (attempt %d/%d): %s", attempt + 1, self.cfg.attempts, scrub_credentials(str(e))
                )
                self._aimd_halve(f"flush failure: {type(e).__name__}")
                time.sleep(1.0 * (attempt + 1))
        raise FatalDucklingError(
            f"append failed after {self.cfg.attempts} attempts: {scrub_credentials(str(last_err))}"
        )

    def _aimd_halve(self, why: str) -> None:
        new = max(self.cfg.aimd_floor_rows, self._budget_rows // 2)
        if new != self._budget_rows:
            log.warning("AIMD: read-unit budget %d → %d (%s)", self._budget_rows, new, why)
            self._budget_rows = new
            unit_budget_rows.set(new)

    def _aimd_recover(self) -> None:
        if self._budget_rows < self.cfg.unit_max_rows:
            self._budget_rows = min(self.cfg.unit_max_rows, self._budget_rows + max(1000, self._budget_rows // 10))
            unit_budget_rows.set(self._budget_rows)

    # ---------------- the loop ---------------- #

    def poll_once(self) -> None:
        cfg = self.cfg
        # Clamp BEFORE assertions: a delete whose evidence lags snapshot
        # cleanup inside an already-expired window must not veto the clamp's
        # loud advance (§5.6: never crash-loop inside the window).
        self._clamp_to_retention()
        self._assert_no_deletes()
        self._check_inline_stores()

        head = self._head()
        lag_snapshots.set(head - self._cursor)  # signed: a NEGATIVE lag is
        # a source regression (PITR restore / rebuild) — never clamp it to 0
        if head < self._cursor:
            raise FatalDucklingError(
                f"source head {head} regressed below cursor {self._cursor}: the source was "
                "restored or rebuilt — the cursor's basis is gone; operator adjudication (re-seed)"
            )
        if head == self._cursor:
            polls_total.labels(result="idle").inc()
            return

        hi = self.feed.plan_unit(
            self.src_table,
            self._cursor,
            head,
            max_rows=self._budget_rows,
            max_bytes=cfg.unit_max_bytes,
            max_span=cfg.unit_max_span,
        )
        try:
            rows = self.feed.read(
                self.src_table,
                self.src_catalog.connection,
                self._cursor,
                hi,
                columns=self.columns,
            )
        except feed.FeedError as read_err:
            if "retained snapshot floor" in str(read_err):
                raise  # transient in composition: next poll's clamp heals it
            raise FatalDucklingError(scrub_credentials(str(read_err))) from None
        except Exception as read_err:
            # DROP+CREATE detection on the error path: a changed (or
            # vanished) table_id means the source was rebuilt — freeze and
            # page; the continuation is a mandatory re-seed, never carry on.
            self._check_table_identity(read_err)
            raise

        rows_read_total.inc(rows.num_rows)
        if rows.num_rows:
            filtered = self._arrow_filter(rows)
            if filtered.num_rows:
                self._append(filtered)
            # an all-foreign unit: nothing to write, cursor still advances
        else:
            # Empty range: either foreign commits (normal on a multi-table
            # catalog) or a silent drop+recreate the feed's cached table_id
            # can't see (its plans come back empty forever). Re-resolve
            # before advancing (design §14.3).
            self._check_table_identity(None)
            filtered = rows
        # Cursor strictly after the destination commit. Crash between =
        # one unit of duplicates, deduped downstream. An empty range still
        # advances: hi is a valid coverage boundary (plan_unit semantics),
        # and idling would burn the retention window.
        self._cursor_advance(hi, filtered.num_rows)
        rows_delivered_total.inc(filtered.num_rows)
        polls_total.labels(result="read" if rows.num_rows else "empty").inc()

    def _check_table_identity(self, cause: Exception | None) -> None:
        try:
            new_tid = self._resolve_table_id()
        except ConfigError:
            raise FatalDucklingError("source table dropped — operator adjudication required") from cause
        if new_tid != self.table_id:
            raise FatalDucklingError(
                f"source table_id changed {self.table_id} → {new_tid}: DROP+CREATE requires re-seed"
            ) from cause

    def _arrow_filter(self, rows: pa.Table) -> pa.Table:
        try:
            col = rows.column(self.cfg.team_field)
        except Exception:
            raise FatalDucklingError(
                f"team field {self.cfg.team_field!r} missing from the read batch — schema contract broken"
            ) from None
        return rows.filter(pc.is_in(col, value_set=self._team_array))

    def run(self) -> None:
        self.boot()
        server = _start_health_server(self)
        log.info("polling every %.1fs on port %d", self.cfg.poll_interval_s, self.cfg.port)
        try:
            while not self._stop.is_set():
                try:
                    self.poll_once()
                    self._last_poll_ok = time.monotonic()
                except FatalDucklingError:
                    raise  # crash-class: die, let K8s restart
                except Exception:
                    # Transient read/plan errors: the cursor never advanced,
                    # the range is retried next poll. Append/cursor/
                    # assertion failures are FatalDucklingError (above).
                    polls_total.labels(result="error").inc()
                    log.exception("poll failed (transient)")
                self._maybe_recycle()
                # Jittered: 300 pods restarted together (node drain, image
                # rollout) must not poll in lockstep — the assertion burst
                # would hit the catalog synchronized.
                self._stop.wait(self.cfg.poll_interval_s * (0.5 + random.random()))
        finally:
            server.shutdown()
            server.server_close()
            self.feed.close()
            self.src_catalog.close()
            self.dst_catalog.close()
            if self._cursor_conn is not None:
                self._cursor_conn.close()

    def _maybe_recycle(self) -> None:
        if self.cfg.rss_limit_bytes <= 0:
            return
        try:
            with open("/proc/self/status") as f:
                rss_kb = next(int(ln.split()[1]) for ln in f if ln.startswith("VmRSS"))
        except Exception:
            return
        if rss_kb * 1024 > self.cfg.rss_limit_bytes:
            log.warning("RSS %d over limit %d — clean exit for K8s restart", rss_kb * 1024, self.cfg.rss_limit_bytes)
            self._stop.set()

    # ---------------- health ---------------- #

    def is_healthy(self) -> bool:
        return (time.monotonic() - self._last_poll_ok) < max(3 * self.cfg.poll_interval_s, 300)


def _start_health_server(duck: Duckling) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                ok = duck.is_healthy()
                body = b"ok" if ok else b"stale"
                self.send_response(200 if ok else 503)
            elif self.path == "/metrics":
                body = generate_latest()
                self.send_response(200)
            else:
                body = b"not found"
                self.send_response(404)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("", duck.cfg.port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main() -> None:
    setup_logging()
    source.sweep_spill_dirs()  # crash-loops otherwise accumulate leftovers in the pod emptyDir
    cfg = DucklingConfig.from_env()
    metrics.init(f"duckling-{cfg.destination_id}")
    duck = Duckling(cfg)
    signal.signal(signal.SIGTERM, lambda *_: duck._stop.set())
    duck.run()


if __name__ == "__main__":
    main()
