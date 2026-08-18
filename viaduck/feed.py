"""Direct-SQL CDC feed reader (append_only path).

Reads the source DuckLake's catalog metadata straight from Postgres and the
data files through stock DuckDB parquet scans, bypassing the ducklake
extension's changefeed bind path — the ~2.4-3s fixed cost per call on the
production catalog is bind-time work (optimizer-stats registration at the
latest snapshot + full metadata COPY pulls through DuckDB's postgres
scanner), not per-snapshot work, so no index or setting can fix the
extension path (verified 2026-08-15: no stats knob exists in the deployed
extension build). Design: log-consumer-proposal.md §3/§6.1.

Semantics are pinned to the extension's GetTableInsertions by the parity
suite (tests/integration/test_feed_parity.py):

- File selection is verbatim-equivalent to the extension's predicate
  (inclusive bounds; shifted +1 here for viaduck's exclusive-cursor
  convention, matching read_cdc).
- Merged / inline-flushed files (partial_max NOT NULL) get the extension's
  two-sided per-row snapshot filter on the physical
  _ducklake_internal_snapshot_id column. The filter is applied ONLY under
  partial_max NOT NULL: single-source merge outputs carry the physical
  column with partial_max NULL and the extension applies no row filter to
  them — parity, not an optimization to "fix".
- Plain files need no row filter: selection already guarantees begin > lo
  (the low side is vacuous for them — verified against the extension).
- Inline stores are unioned across ALL registered schema versions
  (ducklake_inlined_data_<table_id>_<sv>). Dormant in prod (inlining
  disabled at catalog creation) but always live code: a future catalog or
  per-table override must never become silent row loss.
- One REPEATABLE READ transaction per read covers the file query, the
  inline registry, and the inline rows: an inline→parquet flush committing
  between those queries could otherwise double-sight a row (both shapes
  visible) or strand it (neither shape visible).
- The catalog metadata schema version is pinned; unknown versions refuse
  loudly rather than drift (SUPPORTED_METADATA_VERSIONS).

Scope: append_only only (no delete stream). Encrypted catalogs are refused
loudly until the key-grouped read path is built (per-file random keys,
encryption_config.footer_key_value per parquet_scan — DuckDB secrets do
not map per-file keys).

Two data-plane contracts worth knowing:

- filter_expr is evaluated by TWO SQL engines (DuckDB for parquet scans,
  Postgres for inline stores) — it must stay in the dialect intersection.
  router.build_filter_expr's `col IN (...)` literal form is; anything
  fancier needs a parity test first.
- Temporal columns are normalized to the pinned schema's tz/unit after the
  parquet scan (pyducklake pins timestamp[us, tz=UTC]; a parquet read
  inherits the DuckDB session TimeZone — without the cast, a mixed
  parquet+inline read raises or drifts on a non-UTC host).
"""

from __future__ import annotations

import datetime
import decimal
import logging
import time
from typing import TYPE_CHECKING

import psycopg
import pyarrow as pa
import pyarrow.compute as pc

from viaduck import metrics

if TYPE_CHECKING:
    from pyducklake import Table

log = logging.getLogger(__name__)

# Catalog metadata schema versions this reader is verified against. The
# feed reads ducklake's internal catalog schema directly, so a ducklake
# upgrade that changes it must trip this pin loudly (fail-closed) instead
# of silently misreading — the golden parity suite is the re-verification
# gate for widening the set.
SUPPORTED_METADATA_VERSIONS = frozenset({"1.0"})

# Physical snapshot attribution column present in every merge/flush output
# (ducklake_compaction_functions.cpp: write_snapshot_id; inline flush uses
# WRITE_ROW_ID_AND_SNAPSHOT_ID). Only read under partial_max NOT NULL.
_PHYSICAL_SNAPSHOT_COL = "_ducklake_internal_snapshot_id"

# Alias for the injected attribution column in the straddle-filter subquery.
# A source column with this exact name would collide — read() refuses loudly.
_SNAP_OUT = "__viaduck_snap"
SNAP_COL = _SNAP_OUT


def _is_missing_file_error(exc: Exception) -> bool:
    """True when a parquet read failed because a listed file vanished —
    the plan/execute-skew race. Local FS raises duckdb IOException ("No
    files found that match"); S3 raises duckdb HTTPException with the HTTP
    404 body ("NoSuchKey" / "Not Found" — verified against the httpfs
    1.5.5 binary). Matching on messages is brittle across duckdb builds,
    so this checks the exception type AND the message shape together.
    Asymmetry is deliberate: a false positive costs one spurious re-plan;
    a false negative fails loudly without one."""
    if type(exc).__name__ not in ("IOException", "HTTPException"):
        return False
    msg = str(exc)
    return any(
        needle in msg
        for needle in ("No files found that match", "NoSuchKey", "404", "Not Found", "The specified key does not exist")
    )


# One SELECT-list addition would not hurt, but the obligation under
# encrypted=true is key-grouped parquet reads, which this module does not
# implement — refuse the catalog instead of half-supporting it.
_ENCRYPTION_REFUSAL = (
    "Catalog {catalog!r} has ducklake_metadata.encrypted=true; the direct feed does not "
    "support encrypted catalogs (per-file encryption_key would need key-grouped "
    "parquet_scan calls with encryption_config). Use cdc_reader=ducklake."
)


class FeedError(Exception):
    """Configuration/version/capability refusal from the feed (fail loud)."""


class FeedReader:
    """Direct-SQL reader for one source catalog.

    Holds one psycopg connection to the catalog's Postgres (the metadata
    queries must NOT go through the DuckDB postgres scanner — its COPY
    behavior is exactly the tax this reader exists to avoid) and borrows a
    DuckDB connection for the parquet data plane (stock parquet_scan; the
    connection carries the S3 credentials). The DuckDB connection is used
    only from the calling thread (the poll loop), same discipline as the
    extension-path reads it replaces.
    """

    def __init__(self, *, postgres_uri: str, catalog_name: str, data_path: str | None):
        self._pg_uri = postgres_uri
        self._catalog_name = catalog_name
        self._data_path = data_path
        self._conn: psycopg.Connection | None = None
        self._meta_schema: str | None = None
        # table_id/path caches: a DROP+CREATE of the source table changes its
        # table_id, but that also invalidates every cursor — the process
        # restart that requires (mirrors source.py's cache contract) clears
        # these too.
        self._table_id_cache: dict[tuple[str, str], int] = {}
        self._path_cache: dict[tuple[str, str], tuple[tuple[str | None, bool], tuple[str | None, bool]]] = {}
        self._verified = False

    # ------------------------------------------------------------------ #
    # Connection and one-time catalog verification
    # ------------------------------------------------------------------ #

    def _pg(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            # autocommit=True: verification queries are standalone; each read
            # wraps its own explicit REPEATABLE READ transaction on top.
            # Timeouts mirror state.py: a black-holed PG (failover, SG drop)
            # must trip the fatal path, not hang the poll thread on a socket
            # read forever.
            self._conn = psycopg.connect(
                self._pg_uri,
                autocommit=True,
                connect_timeout=10,
                options="-c statement_timeout=30000",
            )
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    def _detect_meta_schema(self, conn: psycopg.Connection) -> str:
        """The PG-side schema holding the ducklake catalog tables.

        ducklake templates the schema name at ATTACH time; prod megaduck
        uses `public`, the duckdb-side view is `__ducklake_metadata_<name>`,
        and a future catalog could use either — detect rather than assume.
        Prefer the catalog-named schema when both exist (multiple catalogs
        can share one PG database).
        """
        rows = conn.execute(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = 'ducklake_snapshot'"
        ).fetchall()
        schemas = {r[0] for r in rows}
        named = f"__ducklake_metadata_{self._catalog_name}"
        if named in schemas:
            return named
        if len(schemas) == 1:
            return schemas.pop()
        raise FeedError(
            f"Cannot locate the ducklake metadata schema for catalog {self._catalog_name!r}: "
            f"ducklake_snapshot found in schemas {sorted(schemas) or 'NOWHERE'}; "
            f"expected exactly one, or {named!r}"
        )

    def verify_catalog(self) -> None:
        """One-time startup gate: schema detection, version pin, encryption
        refusal. Runs before the first read so every failure here is a
        startup error, not a poll-cycle error."""
        if self._verified:
            return
        conn = self._pg()
        self._meta_schema = self._detect_meta_schema(conn)
        row = conn.execute(
            f'SELECT value FROM "{self._meta_schema}".ducklake_metadata WHERE key = %s',
            ("version",),
        ).fetchone()
        version = row[0] if row else None
        if version not in SUPPORTED_METADATA_VERSIONS:
            raise FeedError(
                f"Catalog {self._catalog_name!r} metadata schema version {version!r} is not in "
                f"{sorted(SUPPORTED_METADATA_VERSIONS)} — refusing to read internals directly. "
                f"Re-verify the golden parity suite against the new schema before widening."
            )
        row = conn.execute(
            f'SELECT value FROM "{self._meta_schema}".ducklake_metadata WHERE key = %s',
            ("encrypted",),
        ).fetchone()
        if row and row[0] == "true":
            raise FeedError(_ENCRYPTION_REFUSAL.format(catalog=self._catalog_name))
        # The catalog's recorded data_path is authoritative for relative-path
        # resolution (config is the fallback for legacy catalogs without it).
        row = conn.execute(
            f'SELECT value FROM "{self._meta_schema}".ducklake_metadata WHERE key = %s',
            ("data_path",),
        ).fetchone()
        if row and row[0]:
            self._data_path = row[0]
        # Index-presence probe: the feed's range query is index-only with
        # viaduck_data_file_range and a seq-scan without it (22M-row table).
        # Missing index = loud WARN, not a failure (correctness unaffected).
        idx = conn.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = %s AND tablename = 'ducklake_data_file' "
            "AND indexname = 'viaduck_data_file_range'",
            (self._meta_schema,),
        ).fetchone()
        if idx is None:
            log.warning(
                "Feed index viaduck_data_file_range is MISSING on %s.ducklake_data_file — catalog reads "
                "seq-scan until it exists (cdc-mark-2.md / log-consumer-proposal.md §9.1)",
                self._meta_schema,
            )
        self._verified = True
        log.info(
            "Feed reader verified catalog %s (metadata schema %s, version %s)",
            self._catalog_name,
            self._meta_schema,
            version,
        )

    # ------------------------------------------------------------------ #
    # Catalog lookups
    # ------------------------------------------------------------------ #

    def _table_id(self, conn: psycopg.Connection, namespace: str, table: str) -> int:
        key = (namespace, table)
        cached = self._table_id_cache.get(key)
        if cached is not None:
            return cached
        row = conn.execute(
            f'SELECT t.table_id FROM "{self._meta_schema}".ducklake_table t '
            f'JOIN "{self._meta_schema}".ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL '
            f"WHERE t.table_name = %s AND s.schema_name = %s AND t.end_snapshot IS NULL",
            (table, namespace),
        ).fetchone()
        if row is None:
            raise LookupError(f"table_id not found in catalog for {namespace}.{table}")
        self._table_id_cache[key] = int(row[0])
        return self._table_id_cache[key]

    def _data_path_for(
        self, conn: psycopg.Connection, namespace: str, table: str
    ) -> tuple[tuple[str | None, bool], tuple[str | None, bool]]:
        """((table path, is_relative), (schema path, is_relative)) — cached."""
        key = (namespace, table)
        if key in self._path_cache:
            return self._path_cache[key]
        row = conn.execute(
            f"SELECT t.path, t.path_is_relative, s.path, s.path_is_relative "
            f'FROM "{self._meta_schema}".ducklake_table t '
            f'JOIN "{self._meta_schema}".ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL '
            f"WHERE t.table_name = %s AND s.schema_name = %s AND t.end_snapshot IS NULL",
            (table, namespace),
        ).fetchone()
        value = (
            (row[0], bool(row[1])) if row else (None, False),
            (row[2], bool(row[3])) if row else (None, False),
        )
        self._path_cache[key] = value
        return value

    def plan_unit(
        self,
        table: Table,
        after_snapshot: int,
        head: int,
        *,
        max_rows: int = 50_000,
        max_bytes: int = 256 * 1024 * 1024,
        max_span: int = 10_000,
    ) -> int:
        """The read unit's hi: extend from `after_snapshot` toward `head`
        until a budget trips (proposal §6.2). Row/byte budgets bound memory
        AND keep the flush unit under the destination commit cliff; the span
        cap bounds inline-row overrun and GET fan-out. Snapshot-atomic: a
        unit never splits a commit's files (hi lands on a begin_snapshot
        group boundary). Catalog cost: one indexed range query (+ inline
        COUNTs) — ~ms.
        """
        self.verify_catalog()
        namespace, table_name = table._identifier[0], table._identifier[1]
        pg = self._pg()
        tid = self._table_id(pg, namespace, table_name)
        lo1 = after_snapshot + 1
        rows = pg.execute(
            f"SELECT begin_snapshot, partial_max, record_count, file_size_bytes "
            f'FROM "{self._meta_schema}".ducklake_data_file '
            f"WHERE table_id = %s AND begin_snapshot <= %s "
            f"AND (begin_snapshot >= %s OR (partial_max IS NOT NULL AND partial_max >= %s)) "
            f"ORDER BY begin_snapshot, data_file_id",
            (tid, head, lo1, lo1),
        ).fetchall()
        # Inline rows count toward the budget but carry no record_count —
        # group them per begin_snapshot so they budget exactly like file
        # groups (an all-inline range must not become an unbounded unit).
        registry = pg.execute(
            f'SELECT table_name FROM "{self._meta_schema}".ducklake_inlined_data_tables WHERE table_id = %s',
            (tid,),
        ).fetchall()
        inline_groups: list[tuple[int, int, int, None]] = []  # (begin, count, bytes=0, no partial_max)
        for (store,) in registry:
            store = store.replace('"', '""')
            inline_groups.extend(
                (int(b), int(c), 0, None)
                for b, c in pg.execute(
                    f'SELECT begin_snapshot, COUNT(*) FROM "{self._meta_schema}"."{store}" '
                    f"WHERE begin_snapshot > %s AND begin_snapshot <= %s GROUP BY begin_snapshot",
                    (after_snapshot, head),
                ).fetchall()
            )
        oversized = sum(1 for r in rows if int(r[2]) > max_rows or int(r[3]) > max_bytes)
        if oversized:
            log.warning(
                "plan_unit: %d file(s) individually exceed the unit budget (a single file is "
                "read whole — the bound resumes next unit)",
                oversized,
            )

        # Unified (begin, rows, bytes, partial_max) stream ordered by begin.
        entries = sorted(
            [(int(r[0]), int(r[2]), int(r[3]), int(r[1]) if r[1] is not None else None) for r in rows] + inline_groups,
            key=lambda e: e[0],
        )
        total_rows = 0
        total_bytes = 0
        hi = head
        cur_begin: int | None = None
        cur_pmax: int | None = None
        for begin, record_count, size_bytes, partial_max in entries:
            if cur_begin is not None and begin != cur_begin:
                if begin > after_snapshot + max_span:
                    # Span trip: end at the cap boundary (a content-free tail
                    # is fine — file/inline selection is range-based).
                    hi = min(head, after_snapshot + max_span)
                    break
                if total_rows >= max_rows or total_bytes >= max_bytes:
                    # Row/byte trip: end at the previous group boundary so
                    # the unit stays snapshot-atomic.
                    hi = cur_begin
                    break
            cur_begin = begin
            if partial_max is not None:
                cur_pmax = partial_max
            total_rows += record_count
            total_bytes += size_bytes
        if hi <= after_snapshot:
            # The budget tripped ON a straddle group whose begin predates the
            # cursor (a merged file selected via partial_max): ending the unit
            # there plans a reversed/empty range that can never advance. End
            # at the straddle's coverage boundary instead.
            hi = min(head, max(after_snapshot + 1, cur_pmax or 0))
        # Span cap also applies when the row/byte budgets never trip.
        return min(hi, after_snapshot + max_span)

    def read(
        self,
        table: Table,
        conn,
        after_snapshot: int,
        end_snapshot: int,
        *,
        filter_expr: str | None = None,
        columns: tuple[str, ...] | None = None,
        with_snapshot: bool = False,
    ) -> pa.Table:
        """Inserted rows in (after_snapshot, end_snapshot] for the filter's
        routing values — the drop-in replacement for source.read_cdc.

        `conn` is the source catalog's DuckDB connection (carries the S3
        credentials); used for parquet scans only. `with_snapshot` appends a
        per-row __viaduck_snap column (the read loop slices on it; it is
        stripped before buffering — destinations never see it).
        """
        self.verify_catalog()
        if end_snapshot <= after_snapshot:
            return _empty_table(table, columns, with_snapshot)

        t0 = time.monotonic()
        planned = self.plan_read(
            table,
            after_snapshot,
            end_snapshot,
            filter_expr=filter_expr,
            columns=columns,
            with_snapshot=with_snapshot,
        )
        if planned is None:
            return _empty_table(table, columns, with_snapshot)

        try:
            result = execute_read(conn, planned)
        except Exception as exc:
            if not _is_missing_file_error(exc):
                raise
            # Plan/execute skew (proposal §3): a merge/expire committed
            # between the catalog read and the S3 GET, and a listed file is
            # gone. Re-plan once against a fresh catalog snapshot (the merged
            # replacement covers the rows; the two-sided filter keeps the
            # range exact), then fail loudly if it still doesn't hold.
            log.warning(
                "Feed read (%d, %d] hit a vanished file (%s); re-planning once",
                after_snapshot,
                end_snapshot,
                type(exc).__name__,
            )
            metrics.cdc_feed_replans_total.inc()
            planned = self.plan_read(
                table,
                after_snapshot,
                end_snapshot,
                filter_expr=filter_expr,
                columns=columns,
                with_snapshot=with_snapshot,
            )
            if planned is None:
                return _empty_table(table, columns, with_snapshot)
            result = execute_read(conn, planned)

        if result is None:
            # The re-plan found the whole range compacted/emptied out from
            # under us — legal (all-inline ranges get flushed; files can
            # vanish between plan and GET). Return the empty projection.
            return _empty_table(table, columns, with_snapshot)

        duration = time.monotonic() - t0
        metrics.cdc_read_seconds.observe(duration)
        metrics.cdc_rows_read_total.inc(result.num_rows)
        log.debug(
            "Feed read: snapshots (%d, %d], %d rows in %.3fs",
            after_snapshot,
            end_snapshot,
            result.num_rows,
            duration,
        )
        return result

    def plan_read(
        self,
        table: Table,
        after_snapshot: int,
        end_snapshot: int,
        *,
        filter_expr: str | None,
        columns: tuple[str, ...] | None,
        with_snapshot: bool,
    ) -> _PlannedRead | None:
        """Plan one read unit (POLL THREAD — catalog SQL, ~ms): bucket the
        file list into plain/partial-unfiltered/partial-filtered parquet
        scans and carry the inline rows. The data plane (execute_read) runs
        on the read pool. Returns None when the range has nothing.
        """
        namespace, table_name = table._identifier[0], table._identifier[1]
        data_columns = columns if columns is not None else tuple(f.name for f in table.schema.fields)
        file_rows, inline_rows, path_info = self._plan(
            table, namespace, table_name, after_snapshot, end_snapshot, data_columns, filter_expr, with_snapshot
        )
        if not file_rows and not inline_rows:
            return None
        full_schema = table.schema.as_arrow()
        target_schema = pa.schema([full_schema.field(c) for c in data_columns])

        lo1 = after_snapshot + 1
        table_path, schema_path = path_info
        plain_by_begin: dict[int, list[str]] = {}
        partial_unfiltered: list[str] = []
        partial_filtered: list[str] = []
        for _fid, begin, partial_max, _rid_start, _rc, path, path_is_relative, _size in file_rows:
            resolved = self._resolve_path(path, path_is_relative, table_path, schema_path)
            if partial_max is None:
                plain_by_begin.setdefault(begin, []).append(resolved)
            elif begin < lo1 or partial_max > end_snapshot:
                partial_filtered.append(resolved)
            else:
                partial_unfiltered.append(resolved)
        metrics.cdc_feed_files_total.inc(len(file_rows))
        if (partial_filtered or with_snapshot) and _SNAP_OUT in data_columns:
            raise FeedError(f"Source column named {_SNAP_OUT!r} collides with the feed's internal alias; rename it")

        return _PlannedRead(
            plain_groups=(
                sorted(plain_by_begin.items())
                if with_snapshot
                else [(None, [p for ps in plain_by_begin.values() for p in ps])]
            ),
            partial_unfiltered=partial_unfiltered,
            partial_filtered=partial_filtered,
            inline_rows=inline_rows,
            after_snapshot=after_snapshot,
            end_snapshot=end_snapshot,
            data_columns=data_columns,
            filter_expr=filter_expr,
            with_snapshot=with_snapshot,
            target_schema=target_schema,
        )

    def _plan(
        self,
        table: Table,
        namespace: str,
        table_name: str,
        after_snapshot: int,
        end_snapshot: int,
        data_columns: tuple[str, ...],
        filter_expr: str | None,
        with_snapshot: bool = False,
    ) -> tuple[list[tuple], list[tuple], tuple[tuple[str | None, bool], tuple[str | None, bool]]]:
        """(file rows, inline rows, (table_path, schema_path)) for the
        range, from ONE consistent
        catalog snapshot (REPEATABLE READ): an inline→parquet flush
        committing between the file query and the inline queries could
        otherwise double-sight a row (both shapes visible) or strand it
        (neither visible). The parquet reads happen OUTSIDE this txn —
        holding a snapshot across multi-second S3 GETs would pin megaduck's
        vacuum horizon; the accepted cost is the plan/execute skew (a file
        deleted between plan and GET), handled by the re-plan in read().
        NOTE: the SET TRANSACTION must be the first statement inside the
        transaction() block or PG silently keeps READ COMMITTED.
        """
        t0 = time.monotonic()
        pg = self._pg()
        with pg.transaction():
            pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            tid = self._table_id(pg, namespace, table_name)
            path_info = self._data_path_for(pg, namespace, table_name)
            lo1 = after_snapshot + 1
            file_rows = pg.execute(
                f"SELECT data_file_id, begin_snapshot, partial_max, row_id_start, record_count, "
                f'path, path_is_relative, file_size_bytes FROM "{self._meta_schema}".ducklake_data_file '
                f"WHERE table_id = %s AND begin_snapshot <= %s "
                f"AND (begin_snapshot >= %s OR (partial_max IS NOT NULL AND partial_max >= %s)) "
                f"ORDER BY begin_snapshot, data_file_id",
                (tid, end_snapshot, lo1, lo1),
            ).fetchall()
            inline_rows = self._read_inline(
                pg, tid, after_snapshot, end_snapshot, data_columns, filter_expr, with_snapshot
            )
        metrics.cdc_feed_query_seconds.labels(surface="catalog").observe(time.monotonic() - t0)
        return file_rows, inline_rows, path_info

    def _read_inline(
        self,
        conn: psycopg.Connection,
        table_id: int,
        after_snapshot: int,
        end_snapshot: int,
        columns: tuple[str, ...],
        filter_expr: str | None,
        with_snapshot: bool = False,
    ) -> list[tuple]:
        """Union of all registered inline stores for the table, in-range.
        When with_snapshot, each row carries begin_snapshot appended.

        Called inside the read's REPEATABLE READ transaction. Returns raw
        rows of the projected data columns (in projection order); Arrow
        conversion happens in _inline_to_arrow.
        """
        registry = conn.execute(
            f'SELECT table_name FROM "{self._meta_schema}".ducklake_inlined_data_tables WHERE table_id = %s',
            (table_id,),
        ).fetchall()
        rows: list[tuple] = []
        # Data columns only — the store prepends row_id/begin/end_snapshot.
        # Projection uses the CURRENT column list: multi-schema-version
        # column mapping is a golden-suite concern (the branch is dormant in
        # prod), and a mismatch raises loudly rather than drifting.
        data_cols = ", ".join(f'"{c.replace(chr(34), chr(34) * 2)}"' for c in columns)
        if with_snapshot:
            data_cols += ", begin_snapshot"
        # The routing filter applies here too — inline rows are rows, full
        # stop. (Caught by the parity suite: unfiltered inline rows leak
        # other tenants' data into the read.)
        filter_sql = f" AND ({filter_expr})" if filter_expr else ""
        for (store,) in registry:
            store = store.replace('"', '""')
            rows.extend(
                conn.execute(
                    f'SELECT {data_cols} FROM "{self._meta_schema}"."{store}" '
                    f"WHERE begin_snapshot > %s AND begin_snapshot <= %s{filter_sql}",
                    (after_snapshot, end_snapshot),
                ).fetchall()
            )
        return rows

    def _resolve_path(
        self,
        path: str,
        path_is_relative: bool,
        table_path: tuple[str | None, bool],
        schema_path: tuple[str | None, bool],
    ) -> str:
        """file path → absolute. EVERY level of the chain may itself be
        relative to the next (extension FromRelativePath semantics): the
        file resolves against the table path, the table path against the
        schema path, the schema path against the catalog data_path."""
        if not path_is_relative:
            return path
        base = self._resolve_component(*schema_path, parent=self._data_path)
        base = self._resolve_component(*table_path, parent=base)
        if base is None:
            raise FeedError(
                f"Relative data file path {path!r} but no table/schema/catalog data_path to resolve against"
            )
        return base.rstrip("/") + "/" + path

    @staticmethod
    def _resolve_component(path: str | None, is_relative: bool, *, parent: str | None) -> str | None:
        if path is None:
            return parent
        if not is_relative:
            return path
        if parent is None:
            return path
        return parent.rstrip("/") + "/" + path


def _align_temporal_types(tbl: pa.Table, target: pa.Schema) -> pa.Table:
    """Cast timestamp columns to the pinned schema's tz/unit when they
    differ. parquet_scan inherits the DuckDB session TimeZone while the
    pinned schema (pyducklake) is timestamp[us, tz=UTC] — without this, a
    read mixing parquet and inline rows raises (or worse, drifts) on any
    non-UTC host. Instant-preserving cast; naive↔aware mismatches raise
    (loud). Non-timestamp columns are untouched (concat's promote rules
    handle the rest).
    """
    out = tbl
    for field in target:
        idx = out.schema.get_field_index(field.name)
        if idx == -1:
            continue
        actual = out.schema.field(field.name).type
        if pa.types.is_timestamp(field.type) and pa.types.is_timestamp(actual) and actual != field.type:
            out = out.set_column(idx, field, pc.cast(out.column(field.name), field.type))
    return out


def _projection_sql(columns: tuple[str, ...] | None) -> str:
    """Explicit projection (see read_cdc: pins the read to the startup
    schema). Identifiers are double-quoted with embedded quotes escaped —
    same convention as _read_inline's column list."""
    if columns is None:
        return "*"
    return ", ".join(f'"{c.replace(chr(34), chr(34) * 2)}"' for c in columns)


def _path_list_sql(paths: list[str]) -> str:
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


class _PlannedRead:
    """One read unit's data-plane plan: bucketed file lists + inline rows +
    everything execute_read needs. Built on the poll thread (catalog SQL,
    ~ms); executed on a read-pool thread (pure I/O + Arrow compute).

    NOT a dataclass: slots only, built once per unit."""

    __slots__ = (
        "plain_groups",
        "partial_unfiltered",
        "partial_filtered",
        "inline_rows",
        "after_snapshot",
        "end_snapshot",
        "data_columns",
        "filter_expr",
        "with_snapshot",
        "target_schema",
    )

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


def execute_read(conn, plan: _PlannedRead) -> pa.Table | None:
    """Data plane for one planned read unit (READ POOL thread): parquet
    scans + inline conversion + temporal alignment. Returns None when the
    plan has no content (a re-plan that came back empty). Raises
    duckdb.IOException/HTTPException on a vanished file — the caller
    (poll thread) re-plans and re-dispatches once.

    Bucket semantics (verified against the extension's SetSnapshotFilter):
    the two-sided per-row snapshot filter applies only to a partial_max
    file that genuinely straddles the range; a contained partial file
    scans unfiltered (the per-row physical-column read is the hot-path
    cost worth skipping).
    """
    from viaduck import metrics

    proj_sql = _projection_sql(plan.data_columns)
    snap_select = f", {_SNAP_OUT}" if plan.with_snapshot else ""
    parts: list[pa.Table] = []

    read_t0 = time.monotonic()
    # Plain files carry no attribution column; when the caller wants
    # per-row snapshots, group by begin_snapshot and inject the constant
    # (cheap: one scan per distinct begin; groups per unit are few).
    for begin, paths in plan.plain_groups:
        if not paths:
            continue
        # CAST is load-bearing: DuckDB types a bare literal that fits in
        # 32 bits as INTEGER, so without it plain parts carry an int32
        # attribution column while merged-file parts carry the physical
        # BIGINT column — and concat_tables refuses to unify the widths.
        # Only a unit mixing plain + merged files hits the mismatch,
        # which is why this survived until a compaction commit landed
        # inside an active read range (prod wedge, 2026-08-18).
        snap_col = f", CAST({int(begin)} AS BIGINT) AS {_SNAP_OUT}" if plan.with_snapshot else ""
        sql = f"SELECT {proj_sql}{snap_col} FROM parquet_scan({_path_list_sql(paths)})"
        if plan.filter_expr:
            sql += f" WHERE {plan.filter_expr}"
        parts.append(_align_temporal_types(conn.execute(sql).to_arrow_table(), plan.target_schema))
    if plan.partial_unfiltered:
        # Contained merged files need no filter; attribution still
        # available via the physical column when requested.
        phys = f", CAST({_PHYSICAL_SNAPSHOT_COL} AS BIGINT) AS {_SNAP_OUT}" if plan.with_snapshot else ""
        sql = f"SELECT {proj_sql}{phys} FROM parquet_scan({_path_list_sql(plan.partial_unfiltered)})"
        if plan.filter_expr:
            sql += f" WHERE {plan.filter_expr}"
        parts.append(_align_temporal_types(conn.execute(sql).to_arrow_table(), plan.target_schema))
    if plan.partial_filtered:
        paths_sql = _path_list_sql(plan.partial_filtered)
        inner_snap = f"CAST({_PHYSICAL_SNAPSHOT_COL} AS BIGINT) AS {_SNAP_OUT}"
        sql = (
            f"SELECT {proj_sql}{snap_select} FROM ("
            f"SELECT *, {inner_snap} FROM parquet_scan({paths_sql})) "
            f"WHERE {_SNAP_OUT} > {int(plan.after_snapshot)} AND {_SNAP_OUT} <= {int(plan.end_snapshot)}"
        )
        if plan.filter_expr:
            sql += f" AND ({plan.filter_expr})"
        parts.append(_align_temporal_types(conn.execute(sql).to_arrow_table(), plan.target_schema))
    metrics.cdc_feed_query_seconds.labels(surface="parquet").observe(time.monotonic() - read_t0)

    if plan.inline_rows:
        inline_rows = plan.inline_rows
        if plan.with_snapshot:
            snaps = [int(r[-1]) for r in inline_rows]
            inline_rows = [r[:-1] for r in inline_rows]
            inline_table = _inline_to_arrow(plan.target_schema, plan.data_columns, inline_rows).append_column(
                _SNAP_OUT, pa.array(snaps, type=pa.int64())
            )
        else:
            inline_table = _inline_to_arrow(plan.target_schema, plan.data_columns, inline_rows)
        parts.append(inline_table)
        metrics.cdc_feed_inlined_rows_total.inc(len(plan.inline_rows))

    if not parts:
        return None
    return parts[0] if len(parts) == 1 else pa.concat_tables(parts, promote_options="default")


def _empty_table(table: Table, columns: tuple[str, ...] | None, with_snapshot: bool = False) -> pa.Table:
    """Zero-row table with the projected schema (parity with an empty
    table_insertions result)."""
    schema = table.schema.as_arrow()
    if columns is not None:
        schema = pa.schema([schema.field(c) for c in columns])
    if with_snapshot:
        schema = schema.append(pa.field(_SNAP_OUT, pa.int64()))
    return schema.empty_table()


def _inline_to_arrow(schema: pa.Schema, columns: tuple[str, ...], rows: list[tuple]) -> pa.Table:
    """Inline PG rows → Arrow, typed per the (projected) source schema.

    The inline store's PG column types do NOT match the source types
    (verified on a live catalog): VARCHAR lands in `bytea` (psycopg hands
    back bytes), TIMESTAMP/TIMESTAMPTZ/DATE land in `varchar` (str), the
    numerics are native. Coercion is an explicit ALLOWLIST of
    (python type, arrow type) pairs — anything else raises FeedError with
    column context. A bare pass-through is a silent-corruption vector
    (e.g. PG interval arrives as a timedelta that pyarrow would silently
    pack into month_day_nano with 30-day months).
    """

    def coerce(value, pa_type, col: str):
        if value is None:
            return None
        if isinstance(value, bool) and pa.types.is_boolean(pa_type):
            return value
        if isinstance(value, bytes | memoryview):
            if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
                return bytes(value).decode("utf-8")
            if pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
                return bytes(value)
        elif isinstance(value, str):
            if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
                return value
            if pa.types.is_timestamp(pa_type):
                return datetime.datetime.fromisoformat(value)
            if pa.types.is_date32(pa_type) or pa.types.is_date64(pa_type):
                return datetime.date.fromisoformat(value)
            if pa.types.is_time32(pa_type) or pa.types.is_time64(pa_type):
                return datetime.time.fromisoformat(value)
        elif isinstance(value, int) and (
            pa.types.is_integer(pa_type) or pa.types.is_floating(pa_type) or pa.types.is_decimal(pa_type)
        ):
            return value
        elif isinstance(value, float) and (pa.types.is_floating(pa_type) or pa.types.is_decimal(pa_type)):
            return value
        elif isinstance(value, decimal.Decimal) and pa.types.is_decimal(pa_type):
            return value
        elif isinstance(value, datetime.datetime) and pa.types.is_timestamp(pa_type):
            return value
        elif isinstance(value, datetime.date) and (pa.types.is_date32(pa_type) or pa.types.is_date64(pa_type)):
            return value
        elif isinstance(value, datetime.time) and (pa.types.is_time32(pa_type) or pa.types.is_time64(pa_type)):
            return value
        raise FeedError(
            f"inline store column {col!r}: cannot convert {type(value).__name__} to {pa_type} "
            f"— refusing to guess (extend the allowlist deliberately)"
        )

    arrays = [
        pa.array([coerce(r[i], field.type, field.name) for r in rows], type=field.type)
        for i, field in enumerate(schema)
    ]
    return pa.Table.from_arrays(arrays, schema=schema)
