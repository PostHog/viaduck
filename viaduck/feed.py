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
        self._table_id_cache: dict[tuple[str, str], int] = {}
        self._path_cache: dict[tuple[str, str], tuple[str | None, str | None]] = {}
        self._verified = False

    # ------------------------------------------------------------------ #
    # Connection and one-time catalog verification
    # ------------------------------------------------------------------ #

    def _pg(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            # autocommit=True: verification queries are standalone; each read
            # wraps its own explicit REPEATABLE READ transaction on top.
            self._conn = psycopg.connect(self._pg_uri, autocommit=True)
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
        import pyarrow as pa

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
        pg = self._pg()
        # One consistent snapshot of the catalog per read (see module
        # docstring): file list + inline registry + inline rows.
        with pg.transaction():
            pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            tid = self._table_id(pg, namespace, table_name)
            table_path, schema_path = self._data_path_for(pg, namespace, table_name)
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

        if not file_rows and not inline_rows:
            return _empty_table(table, columns)

        plain_paths: list[str] = []
        partial_paths: list[str] = []
        total_files = 0
        for _fid, _begin, partial_max, _rid_start, _rc, path, path_is_relative, _size in file_rows:
            resolved = self._resolve_path(path, path_is_relative, table_path, schema_path)
            (partial_paths if partial_max is not None else plain_paths).append(resolved)
            total_files += 1
        metrics.cdc_feed_files_total.inc(total_files)

        proj_sql = _projection_sql(data_columns)
        parts: list[pa.Table] = []

        read_t0 = time.monotonic()
        if plain_paths:
            sql = f"SELECT {proj_sql} FROM parquet_scan({_path_list_sql(plain_paths)})"
            if filter_expr:
                sql += f" WHERE {filter_expr}"
            parts.append(conn.execute(sql).to_arrow_table())
        if partial_paths:
            # Two-sided per-row snapshot filter on the physical attribution
            # column (see module docstring): rows <= after_snapshot were
            # delivered from the deleted merge sources; rows > end_snapshot
            # belong to a later read.
            paths_sql = _path_list_sql(partial_paths)
            sql = (
                f"SELECT {proj_sql} FROM ("
                f"SELECT *, {_PHYSICAL_SNAPSHOT_COL} AS __viaduck_snap FROM parquet_scan({paths_sql})) "
                f"WHERE __viaduck_snap > {int(after_snapshot)} AND __viaduck_snap <= {int(end_snapshot)}"
            )
            if filter_expr:
                sql += f" AND ({filter_expr})"
            parts.append(conn.execute(sql).to_arrow_table())
        metrics.cdc_feed_query_seconds.labels(surface="parquet").observe(time.monotonic() - read_t0)

        if inline_rows:
            parts.append(_inline_to_arrow(table, data_columns, inline_rows))
            metrics.cdc_feed_inlined_rows_total.inc(len(inline_rows))

        result = parts[0] if len(parts) == 1 else pa.concat_tables(parts, promote_options="default")

        duration = time.monotonic() - t0
        metrics.cdc_read_seconds.observe(duration)
        metrics.cdc_rows_read_total.inc(result.num_rows)
        log.debug(
            "Feed read: snapshots (%d, %d], %d files (%d partial), %d inline rows, %d rows in %.3fs",
            after_snapshot,
            end_snapshot,
            total_files,
            len(partial_paths),
            len(inline_rows),
            result.num_rows,
            duration,
        )
        return result

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

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
    schema). Identifiers are double-quoted; names embedding quotes never
    reach here (replicated_column_names returns None for them)."""
    if columns is None:
        return "*"
    return ", ".join(f'"{c}"' for c in columns)


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
