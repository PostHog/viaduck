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
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import psycopg
import pyarrow as pa

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
_SNAP_ALIAS = "__viaduck_snap"


def _is_missing_file_error(exc: Exception) -> bool:
    """True when a parquet read failed because a listed file vanished —
    the plan/execute-skew race (duckdb raises IOException with the path).
    Matching on the message is brittle across duckdb builds, so this checks
    the exception type and the message shape together."""
    return type(exc).__name__ == "IOException" and (
        "No files found that match" in str(exc) or "NoSuchKey" in str(exc) or "404" in str(exc)
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

    # ------------------------------------------------------------------ #
    # The read
    # ------------------------------------------------------------------ #

    def read(
        self,
        table: Table,
        conn,
        after_snapshot: int,
        end_snapshot: int,
        *,
        filter_expr: str | None = None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        """Inserted rows in (after_snapshot, end_snapshot] for the filter's
        routing values — the drop-in replacement for source.read_cdc.

        `conn` is the source catalog's DuckDB connection (carries the S3
        credentials); used for parquet scans only.
        """
        from viaduck import metrics

        self.verify_catalog()
        if end_snapshot <= after_snapshot:
            return _empty_table(table, columns)

        # The projection is ALWAYS explicit: the inline store prepends
        # row_id/begin/end_snapshot to the data columns, so a `*` read would
        # misalign rows against the data schema.
        data_columns = columns if columns is not None else tuple(f.name for f in table.schema.fields)
        namespace, table_name = table._identifier[0], table._identifier[1]
        t0 = time.monotonic()
        file_rows, inline_rows, path_info = self._plan(
            table, namespace, table_name, after_snapshot, end_snapshot, data_columns, filter_expr
        )

        if not file_rows and not inline_rows:
            return _empty_table(table, columns)

        def scan(rows) -> pa.Table | None:
            return self._scan_files(conn, rows, path_info, after_snapshot, end_snapshot, data_columns, filter_expr)

        try:
            result = scan(file_rows)
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
            file_rows, inline_rows, path_info = self._plan(
                table, namespace, table_name, after_snapshot, end_snapshot, data_columns, filter_expr
            )
            result = scan(file_rows)

        if inline_rows:
            inline_table = _inline_to_arrow(table, data_columns, inline_rows)
            result = (
                inline_table if result is None else pa.concat_tables([result, inline_table], promote_options="default")
            )
            metrics.cdc_feed_inlined_rows_total.inc(len(inline_rows))

        if result is None:
            # The re-plan found the whole range compacted/emptied out from
            # under us — legal (all-inline ranges get flushed; files can
            # vanish between plan and GET). Return the empty projection.
            return _empty_table(table, columns)

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

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _plan(
        self,
        table: Table,
        namespace: str,
        table_name: str,
        after_snapshot: int,
        end_snapshot: int,
        data_columns: tuple[str, ...],
        filter_expr: str | None,
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
        from viaduck import metrics

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
            inline_rows = self._read_inline(pg, tid, after_snapshot, end_snapshot, data_columns, filter_expr)
        metrics.cdc_feed_query_seconds.labels(surface="catalog").observe(time.monotonic() - t0)
        return file_rows, inline_rows, path_info

    def _scan_files(
        self,
        conn,
        file_rows: list[tuple],
        path_info: tuple[tuple[str | None, bool], tuple[str | None, bool]],
        after_snapshot: int,
        end_snapshot: int,
        data_columns: tuple[str, ...],
        filter_expr: str | None,
    ) -> pa.Table | None:
        """Parquet data plane for one plan. None when the plan has no files.

        The two-sided snapshot filter is applied to a partial_max file only
        when the extension would apply it (verified against
        SetSnapshotFilter call sites): low side when the file's
        begin_snapshot predates the range, high side when partial_max
        exceeds it. A partial file fully inside the window scans
        unfiltered — the filter forces a physical-column materialization
        per row, and on megaduck's compactor cadence that difference is the
        hot path.
        """
        from viaduck import metrics

        if not file_rows:
            return None
        table_path, schema_path = path_info
        lo1 = after_snapshot + 1
        plain_paths: list[str] = []
        partial_unfiltered: list[str] = []
        partial_filtered: list[str] = []
        for _fid, begin, partial_max, _rid_start, _rc, path, path_is_relative, _size in file_rows:
            resolved = self._resolve_path(path, path_is_relative, table_path, schema_path)
            if partial_max is None:
                plain_paths.append(resolved)
            elif begin < lo1 or partial_max > end_snapshot:
                partial_filtered.append(resolved)
            else:
                partial_unfiltered.append(resolved)
        metrics.cdc_feed_files_total.inc(len(file_rows))
        if partial_filtered and _SNAP_ALIAS in data_columns:
            raise FeedError(f"Source column named {_SNAP_ALIAS!r} collides with the feed's internal alias; rename it")

        proj_sql = _projection_sql(data_columns)
        parts: list[pa.Table] = []

        read_t0 = time.monotonic()
        for paths in (plain_paths, partial_unfiltered):
            if paths:
                sql = f"SELECT {proj_sql} FROM parquet_scan({_path_list_sql(paths)})"
                if filter_expr:
                    sql += f" WHERE {filter_expr}"
                parts.append(conn.execute(sql).to_arrow_table())
        if partial_filtered:
            paths_sql = _path_list_sql(partial_filtered)
            sql = (
                f"SELECT {proj_sql} FROM ("
                f"SELECT *, {_PHYSICAL_SNAPSHOT_COL} AS {_SNAP_ALIAS} FROM parquet_scan({paths_sql})) "
                f"WHERE {_SNAP_ALIAS} > {int(after_snapshot)} AND {_SNAP_ALIAS} <= {int(end_snapshot)}"
            )
            if filter_expr:
                sql += f" AND ({filter_expr})"
            parts.append(conn.execute(sql).to_arrow_table())
        metrics.cdc_feed_query_seconds.labels(surface="parquet").observe(time.monotonic() - read_t0)
        if not parts:
            return None
        return parts[0] if len(parts) == 1 else pa.concat_tables(parts, promote_options="default")

    def _read_inline(
        self,
        conn: psycopg.Connection,
        table_id: int,
        after_snapshot: int,
        end_snapshot: int,
        columns: tuple[str, ...],
        filter_expr: str | None,
    ) -> list[tuple]:
        """Union of all registered inline stores for the table, in-range.

        Called inside the read's REPEATABLE READ transaction. Returns raw
        rows of the projected data columns (in projection order); Arrow
        conversion happens in _inline_to_arrow.
        """
        registry = conn.execute(
            f'SELECT schema_version FROM "{self._meta_schema}".ducklake_inlined_data_tables WHERE table_id = %s',
            (table_id,),
        ).fetchall()
        rows: list[tuple] = []
        # Data columns only — the store prepends row_id/begin/end_snapshot.
        # Projection uses the CURRENT column list: multi-schema-version
        # column mapping is a golden-suite concern (the branch is dormant in
        # prod), and a mismatch raises loudly rather than drifting.
        data_cols = ", ".join(f'"{c.replace(chr(34), chr(34) * 2)}"' for c in columns)
        # The routing filter applies here too — inline rows are rows, full
        # stop. (Caught by the parity suite: unfiltered inline rows leak
        # other tenants' data into the read.)
        filter_sql = f" AND ({filter_expr})" if filter_expr else ""
        for (sv,) in registry:
            store = f"ducklake_inlined_data_{table_id}_{sv}"
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


def _projection_sql(columns: tuple[str, ...] | None) -> str:
    """Explicit projection (see read_cdc: pins the read to the startup
    schema). Identifiers are double-quoted with embedded quotes escaped —
    same convention as _read_inline's column list."""
    if columns is None:
        return "*"
    return ", ".join(f'"{c.replace(chr(34), chr(34) * 2)}"' for c in columns)


def _path_list_sql(paths: list[str]) -> str:
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def _empty_table(table: Table, columns: tuple[str, ...] | None) -> pa.Table:
    """Zero-row table with the projected schema (parity with an empty
    table_insertions result)."""
    schema = table.schema.as_arrow()
    if columns is not None:
        schema = pa.schema([schema.field(c) for c in columns])
    return schema.empty_table()


def _inline_to_arrow(table: Table, columns: tuple[str, ...], rows: list[tuple]) -> pa.Table:
    """Inline PG rows → Arrow, typed per the (projected) source schema."""
    schema = table.schema.as_arrow()
    schema = pa.schema([schema.field(c) for c in columns])
    return pa.Table.from_pylist([dict(zip(columns, r, strict=True)) for r in rows], schema=schema)
