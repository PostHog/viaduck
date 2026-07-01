"""Source DuckLake connection and CDC reading."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from viaduck import metrics

if TYPE_CHECKING:
    from pyducklake import Catalog, Table
    from pyducklake.cdc import ChangeSet

    from viaduck.config import SourceConfig

log = logging.getLogger(__name__)

META_COLUMNS = ("change_type", "snapshot_id", "rowid")

# pg_connection_limit: DuckDB's postgres extension sized
# `pg_pool_max_connections` to the thread count (e.g. 12) in older builds. A
# parallel CDC scan on the source catalog can claim every slot, leaving a
# concurrent state-table write to wait the full 30s pool timeout and fail
# (observed: production container exits with the loop's fatal-error path).
# Pin an explicit floor regardless of upstream defaults drifting.
# `pg_connection_limit` is the legacy alias for `pg_pool_max_connections`;
# the new name is registered but not SET-able in duckdb 1.5.2's postgres
# extension.
#
# arrow_large_buffer_size: DuckDB's Arrow export defaults to 32-bit string
# offsets, capping any one exported buffer at 2 GiB of string data — large
# seed scans blow past it (observed: backfill of a 12.5M-file source died
# mid-read with "maximum total string size ... exceeds this"). Large buffers
# use 64-bit offsets (pyarrow large_string), which Arrow compute and the
# postgres writer handle transparently.
# enable_progress_bar(+print=false): DuckDB only computes query progress
# when the progress bar is enabled; print-off keeps it out of the terminal.
# This is what lets `conn.query_progress()` (polled cross-thread by the
# seed-scan heartbeat) return a real percentage instead of -1.
_CONNECTION_DEFAULTS = {
    "pg_connection_limit": "64",
    "arrow_large_buffer_size": "true",
    "enable_progress_bar": "true",
    "enable_progress_bar_print": "false",
    # Disable DuckDB's external_file_cache. viaduck is a CDC streamer — the
    # source connection reads a forward-moving snapshot range every poll, and
    # each range touches a fresh set of parquet files (new source snapshots
    # produce new files). The cache is pure dead weight: it retains every
    # row-group range ever fetched but nothing ever re-reads them.
    #
    # Left at the default (unlimited), it climbed at ~5 GiB/h on the events_nrt
    # pipeline and hit the container's 48 GiB limit in ~8h — confirmed via
    # [MEMTRACE] on prod (EXTERNAL_FILE_CACHE was the entire duckdb_memory()
    # footprint at 14+ GiB and still climbing).
    #
    # Applies to both source and destination via with_connection_defaults().
    # On the destination the cache could help on upsert (DuckLake reads the
    # target row groups to resolve conflicts), but today there's only one
    # destination per viaduck deployment so the pool never evicts it — and
    # even so, unbounded is unbounded. Once we run many destinations per
    # viaduck we should split the defaults and give destinations a bounded
    # cap (e.g. `external_file_cache_size=256MB`) rather than off; for now,
    # off keeps the memory picture symmetric with the source and easy to
    # reason about.
    "enable_external_file_cache": "false",
}


def with_connection_defaults(props: dict[str, str]) -> dict[str, str]:
    """Merge DuckDB connection defaults under user-supplied properties (user wins)."""
    merged = dict(_CONNECTION_DEFAULTS)
    merged.update(props)
    return merged


def connect(cfg: SourceConfig) -> Catalog:
    """Create a Catalog connection to the source DuckLake."""
    from pyducklake import Catalog

    return Catalog(
        cfg.name,
        cfg.postgres_uri,
        data_path=cfg.data_path,
        properties=with_connection_defaults(cfg.resolved_properties()),
    )


def load_table(catalog: Catalog, table_name: str) -> Table:
    """Load the source table. Raises if it doesn't exist."""
    return catalog.load_table(table_name)


def current_snapshot_id(table: Table) -> int | None:
    """Get the current (max) snapshot ID, or None if no snapshots exist.

    Queries MAX(snapshot_id) directly rather than delegating to pyducklake's
    Table.current_snapshot(), which loads every snapshot row via
    Table.snapshots() and returns the last. On a catalog with hundreds of
    thousands of snapshots, that materializes the whole table into DuckDB's
    sort pipeline + Python Snapshot objects on every call — the source
    connection's allocator arenas retain the freed extents and the process
    RSS climbs linearly per poll cycle. Same fix pattern as
    earliest_snapshot_id below.
    """
    catalog_name = table._catalog.name
    meta_schema = f"__ducklake_metadata_{catalog_name}".replace('"', '""')
    row = table._catalog.connection.execute(
        f'SELECT MAX(snapshot_id) FROM "{meta_schema}".ducklake_snapshot'
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def earliest_snapshot_id(table: Table) -> int | None:
    """Get the earliest available snapshot ID, or None if no snapshots exist.

    Queries MIN(snapshot_id) directly rather than loading all snapshots —
    safe on large catalogs where table.snapshots() would OOM.
    """
    catalog_name = table._catalog.name
    meta_schema = f"__ducklake_metadata_{catalog_name}".replace('"', '""')
    row = table._catalog.connection.execute(
        f'SELECT MIN(snapshot_id) FROM "{meta_schema}".ducklake_snapshot'
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def read_cdc(
    table: Table,
    after_snapshot: int,
    end_snapshot: int,
    *,
    filter_expr: str | None = None,
) -> pa.Table:
    """Read CDC insertions in the range (after_snapshot, end_snapshot].

    after_snapshot is the last already-delivered snapshot and is EXCLUDED:
    ducklake's table_insertions is inclusive on both bounds, and re-reading
    the cursor snapshot pairs a re-read insert with its later delete in
    conflict resolution — both cancel and the delete is lost (phantom row).

    Uses table_insertions with optional filter pushdown for efficiency.
    For append-only mode (no key_columns).
    """
    t0 = time.monotonic()

    kwargs: dict = {
        "start_snapshot": after_snapshot + 1,
        "end_snapshot": end_snapshot,
    }
    if filter_expr is not None:
        kwargs["filter_expr"] = filter_expr

    changeset: ChangeSet = table.table_insertions(**kwargs)
    result = changeset.to_arrow()

    duration = time.monotonic() - t0
    metrics.cdc_read_seconds.observe(duration)
    metrics.cdc_rows_read_total.inc(result.num_rows)

    log.debug(
        "CDC read (insertions): snapshots (%d, %d], %d rows in %.3fs%s",
        after_snapshot,
        end_snapshot,
        result.num_rows,
        duration,
        f" (filter: {filter_expr})" if filter_expr else "",
    )

    return result


def read_cdc_changes(
    table: Table,
    after_snapshot: int,
    end_snapshot: int,
    *,
    filter_expr: str | None = None,
) -> pa.Table:
    """Read all CDC changes in the range (after_snapshot, end_snapshot].

    after_snapshot is the last already-delivered snapshot and is EXCLUDED
    (see read_cdc for the phantom-row failure mode of an inclusive read).

    Uses table_changes which includes inserts, deletes, and update pre/post images.
    The result contains metadata columns: change_type, snapshot_id, rowid.
    For full CDC mode (key_columns configured).
    """
    t0 = time.monotonic()

    kwargs: dict = {
        "start_snapshot": after_snapshot + 1,
        "end_snapshot": end_snapshot,
    }
    if filter_expr is not None:
        kwargs["filter_expr"] = filter_expr

    changeset: ChangeSet = table.table_changes(**kwargs)
    result = changeset.to_arrow()

    duration = time.monotonic() - t0
    metrics.cdc_read_seconds.observe(duration)
    metrics.cdc_rows_read_total.inc(result.num_rows)

    log.debug(
        "CDC read (changes): snapshots (%d, %d], %d rows in %.3fs%s",
        after_snapshot,
        end_snapshot,
        result.num_rows,
        duration,
        f" (filter: {filter_expr})" if filter_expr else "",
    )

    return result


def strip_meta(table: pa.Table) -> pa.Table:
    """Remove CDC metadata columns (change_type, snapshot_id, rowid) from an Arrow table.

    Only drops columns that are actually present. Safe to call on tables
    that don't have metadata columns (e.g., already stripped).
    """
    cols_to_drop = [c for c in META_COLUMNS if c in table.column_names]
    if not cols_to_drop:
        return table
    return table.drop(cols_to_drop)
