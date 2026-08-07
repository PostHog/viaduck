"""Source DuckLake connection and CDC reading."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from viaduck import metrics

if TYPE_CHECKING:
    from datetime import datetime

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
    # DuckLake's internal transaction-retry loop is the ONLY layer that can
    # retry a commit cheaply: on conflict it re-runs CheckForConflicts, bumps
    # the snapshot id, and re-executes just the catalog SQL — the parquet
    # files already written to object storage are reused, not re-uploaded
    # (CleanupFiles only fires on final abort). Snapshot-id collisions
    # (`ducklake_snapshot_pkey` duplicate — any peer commit on the catalog
    # takes the optimistically-allocated max+1 id) are pure mechanical
    # conflicts and MUST be absorbed here; escalating them to
    # viaduck.apply._write_with_retry re-does the entire flush including the
    # parquet upload.
    #
    # The loop's sleep (ducklake_transaction.cpp:2669) is
    # `wait_ms * random(0.5,1.0) * pow(retry_backoff, i)` with the retry
    # index as an uncapped exponent — at defaults (max=10, wait=100ms,
    # backoff=1.5) that's ~8.5s of dead time per attempt, and raising the
    # count blows sleeps out to hours. backoff=1.0 sidesteps the exponent
    # entirely: constant 25-50ms jittered sleeps, worst case ~1s across all
    # 20 retries. viaduck's outer retry loop (jitter, cap, stop_event
    # awareness) remains the backstop for real failures — semantic conflicts,
    # network errors — and rarely fires.
    "ducklake_max_retry_count": "20",
    "ducklake_retry_wait_ms": "50",
    "ducklake_retry_backoff": "1.0",
    # DuckDB spills intermediates to `temp_directory`; unset, it defaults to
    # `.tmp` relative to CWD (`/`), which is read-only under the pod's
    # readOnlyRootFilesystem. A large flush that spills then fails with
    # `Failed to create directory ".tmp": Read-only file system`, stalling that
    # destination (only the biggest-flush destination hits it, so it looks
    # per-destination). Point spill at the mounted writable /tmp emptyDir.
    # NOTE: this value is the BASE directory only — with_connection_defaults()
    # always replaces it with a fresh per-connection subdirectory beneath it
    # (see the isolation comment there). A user-supplied temp_directory is
    # likewise treated as a base, never used directly.
    "temp_directory": "/tmp",
}

# Subdirectory of the spill base that holds all per-connection spill dirs.
# sweep_spill_dirs() removes the whole thing at startup, so nothing else may
# ever live under it.
_SPILL_SUBDIR = "viaduck-spill"


def with_connection_defaults(props: dict[str, str], name: str = "conn") -> dict[str, str]:
    """Merge DuckDB connection defaults under user-supplied properties (user wins),
    then isolate `temp_directory` to a fresh per-connection subdirectory.

    Isolation is unconditional: DuckDB spill filenames
    (`duckdb_temp_storage_<sizeclass>-<idx>.tmp`) carry no instance token, so
    two embedded DuckDB instances sharing one temp_directory collide on the
    same spill files — concurrent spills corrupt the native temp accounting
    (counter wraps to ~2^64, all further spills refused) and crash the
    process (2026-07-29 crash-loop incident, reproduced in isolation). A
    user-supplied temp_directory is honored as the BASE for the subdirectory.

    `name` is a debugging aid only (prefixes the directory so `du` output is
    attributable per destination); uniqueness comes from mkdtemp.

    DuckDB deletes its temp FILES on clean instance close but knows nothing
    of the directory itself, so each connection leaks one empty dir until the
    next sweep_spill_dirs() at process start. Crash leftovers (with real
    bytes — /tmp is a pod-level emptyDir that survives container restarts)
    are cleaned by the same sweep.
    """
    merged = dict(_CONNECTION_DEFAULTS)
    merged.update(props)
    root = os.path.join(merged["temp_directory"], _SPILL_SUBDIR)
    os.makedirs(root, exist_ok=True)
    # Truncate: names come from CP payloads unbounded; uniqueness is
    # mkdtemp's job, the prefix is only for attribution — a 300-char id
    # must degrade to a shorter prefix, not ENAMETOOLONG the connect.
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)[:64] or "conn"
    merged["temp_directory"] = tempfile.mkdtemp(prefix=f"{safe}-", dir=root)
    return merged


def sweep_spill_dirs(base: str | None = None) -> None:
    """Remove spill dirs left by previous containers (crash leftovers hold
    real bytes; clean closes leave empty dirs). Call ONCE at process start,
    strictly before the first with_connection_defaults() call.

    Sweeps the DEFAULT base only: a per-connection temp_directory override
    moves that connection's spill dirs to <override>/viaduck-spill, which
    this never touches — leftovers there persist until whoever configured
    the override cleans them up. No production config overrides it today.
    """
    root = os.path.join(base or _CONNECTION_DEFAULTS["temp_directory"], _SPILL_SUBDIR)
    if os.path.isdir(root):
        shutil.rmtree(root, ignore_errors=True)
        if os.path.isdir(root):
            log.warning("Spill-dir sweep left %s behind (permissions?); stale spill bytes may persist", root)
        else:
            log.info("Swept leftover spill dirs under %s", root)


def connect(cfg: SourceConfig) -> Catalog:
    """Create a Catalog connection to the source DuckLake."""
    from pyducklake import Catalog

    return Catalog(
        cfg.name,
        cfg.postgres_uri,
        data_path=cfg.data_path,
        properties=with_connection_defaults(cfg.resolved_properties(), name=cfg.name),
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


# table_id cache for skip-scan probes, keyed by (catalog, namespace, table).
# The id is stable for the life of the table; a DROP+CREATE of the source
# table changes it, but that also invalidates every cursor, so a process
# restart (which clears this) is already required.
_table_id_cache: dict[tuple[str, str, str], int] = {}


def ducklake_table_id(table: Table) -> int:
    """The catalog's numeric table_id for this table (cached after first call)."""
    catalog_name = table._catalog.name
    namespace, table_name = table._identifier[0], table._identifier[1]
    key = (catalog_name, namespace, table_name)
    cached = _table_id_cache.get(key)
    if cached is not None:
        return cached
    meta_schema = f"__ducklake_metadata_{catalog_name}".replace('"', '""')
    esc_ns = namespace.replace("'", "''")
    esc_tbl = table_name.replace("'", "''")
    row = table._catalog.connection.execute(
        f'SELECT t.table_id FROM "{meta_schema}".ducklake_table t '
        f'JOIN "{meta_schema}".ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL '
        f"WHERE t.table_name = '{esc_tbl}' AND s.schema_name = '{esc_ns}' AND t.end_snapshot IS NULL"
    ).fetchone()
    if row is None or row[0] is None:
        raise LookupError(f"table_id not found in catalog for {namespace}.{table_name}")
    tid = int(row[0])
    _table_id_cache[key] = tid
    return tid


def next_snapshot_touching_table(table: Table, after_snapshot: int, until_snapshot: int) -> int | None:
    """Smallest snapshot id in (after_snapshot, until_snapshot] whose commit
    touched THIS table, or None if the whole span is other tables' churn.

    Powers the poll loop's skip-scan: snapshot ids are global to the
    catalog, so a multi-table source mints ids the CDC table never
    appears in — measured 88% of megaduck snapshots were the `events`
    consumers' commits while viaduck replicates `events_nrt`. Reading
    those spans as CDC chunks costs a full planning round-trip
    (catalog file-listing on a 22M-row ducklake_data_file) per chunk
    just to return 0 rows; one indexed probe of
    ducklake_snapshot_changes replaces all of them.

    "Touched" means ANY change verb (inserted_into_table, merge_adjacent,
    and whatever future verbs appear): the probe must never skip past a
    snapshot table_insertions would have surfaced, and being conservative
    about unknown verbs only costs a normal chunk read.
    """
    catalog_name = table._catalog.name
    meta_schema = f"__ducklake_metadata_{catalog_name}".replace('"', '""')
    tid = ducklake_table_id(table)
    # changes_made is a comma-joined list like "inserted_into_table:16";
    # the (^|,) / (,|$) anchors keep table_id 1 from matching 16, 160, …
    row = table._catalog.connection.execute(
        f'SELECT MIN(snapshot_id) FROM "{meta_schema}".ducklake_snapshot_changes '
        f"WHERE snapshot_id > {int(after_snapshot)} AND snapshot_id <= {int(until_snapshot)} "
        f"AND regexp_matches(changes_made, '(^|,)[a-z_]+:{tid}(,|$)')"
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


# Warn-once registry for cursors found older than snapshot retention: the
# clamp below keeps the gauge honest ("at least window age") but the state
# itself is an operational incident — the CDC range past that cursor may no
# longer be fully readable. One warning per cursor id, not one per 2s cycle.
_clamp_warned_cursors: set[int] = set()


def snapshot_times(table: Table, snapshot_ids: list[int]) -> dict[int, datetime]:
    """snapshot_id -> snapshot_time for the given ids, in one indexed lookup.

    Powers the exact time-lag gauge: unlike the snapshot-count lag, the
    wall-clock age of a destination's cursor needs no commit-rate assumption.

    Ids missing from ducklake_snapshot (a cursor older than snapshot
    retention — its row was expired) are CLAMPED to the oldest retained
    snapshot_time, so a badly-lagged destination reports "at least the
    window age" instead of silently disappearing from the gauge.

    Same direct-SQL pattern as current_snapshot_id/earliest_snapshot_id:
    never materialize the snapshot table through pyducklake.
    """
    if not snapshot_ids:
        return {}
    catalog_name = table._catalog.name
    meta_schema = f"__ducklake_metadata_{catalog_name}".replace('"', '""')
    id_list = ", ".join(str(int(s)) for s in snapshot_ids)
    rows = table._catalog.connection.execute(
        f'SELECT snapshot_id, snapshot_time FROM "{meta_schema}".ducklake_snapshot WHERE snapshot_id IN ({id_list})'
    ).fetchall()
    out = {int(r[0]): r[1] for r in rows}
    missing = [s for s in snapshot_ids if int(s) not in out]
    if missing:
        newly = [int(s) for s in missing if int(s) not in _clamp_warned_cursors]
        if newly:
            _clamp_warned_cursors.update(newly)
            log.warning(
                "cursor snapshot(s) %s older than %s's snapshot retention; clamping time-lag "
                "to the oldest retained snapshot — the CDC range past these cursors may no "
                "longer be fully readable",
                newly,
                catalog_name,
            )
        row = table._catalog.connection.execute(
            f'SELECT MIN(snapshot_time) FROM "{meta_schema}".ducklake_snapshot'
        ).fetchone()
        if row is not None and row[0] is not None:
            for s in missing:
                out[int(s)] = row[0]
    return out


def snapshot_bounds(table: Table) -> tuple[int, int] | None:
    """(earliest, current) snapshot ids in ONE statement, or None if the
    catalog has no snapshots.

    The poll cycle needs both every cycle (current for the read ceiling,
    earliest for the retention-edge clamp). DuckDB's postgres scanner does
    no aggregate pushdown — MIN or MAX each cost a full single-column pull
    of ducklake_snapshot — so issuing them separately doubles the per-cycle
    metadata-table tax for nothing. One scan yields both.
    """
    catalog_name = table._catalog.name
    meta_schema = f"__ducklake_metadata_{catalog_name}".replace('"', '""')
    row = table._catalog.connection.execute(
        f'SELECT MIN(snapshot_id), MAX(snapshot_id) FROM "{meta_schema}".ducklake_snapshot'
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        return None
    return int(row[0]), int(row[1])


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

    phase_t0 = time.monotonic()
    changeset: ChangeSet = table.table_insertions(**kwargs)
    metrics.cdc_read_phase_seconds.labels(phase="changeset").observe(time.monotonic() - phase_t0)
    phase_t0 = time.monotonic()
    result = changeset.to_arrow()
    metrics.cdc_read_phase_seconds.labels(phase="to_arrow").observe(time.monotonic() - phase_t0)

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

    phase_t0 = time.monotonic()
    changeset: ChangeSet = table.table_changes(**kwargs)
    metrics.cdc_read_phase_seconds.labels(phase="changeset").observe(time.monotonic() - phase_t0)
    phase_t0 = time.monotonic()
    result = changeset.to_arrow()
    metrics.cdc_read_phase_seconds.labels(phase="to_arrow").observe(time.monotonic() - phase_t0)

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
