"""Entry point and main poll loop for viaduck.

CDC Replication Algorithm
=========================

Viaduck replicates changes from a source DuckLake table to N destination
DuckLake tables using a 3-phase CDC algorithm. The algorithm is eventually
consistent under four assumptions:

1. **Routing column immutability**: The routing field (e.g. ``company``) must
   not be updated on the source. The CDC read uses ``filter_expr`` pushdown
   with the routing values of assigned destinations. If a row's routing value
   changes, the preimage (with the old value) may be filtered out at the source
   level, making the delete on the old destination unrecoverable. Violations
   are detected and logged at ERROR, but data integrity is not guaranteed.

2. **Rowid monotonicity**: DuckLake's internal ``rowid`` is assumed to be
   monotonically increasing and never reused. Conflict resolution (Phase 2)
   uses rowid to identify the same logical row across change types within a
   single CDC batch. If rowids were recycled (as in SQLite VACUUM), unrelated
   rows could be incorrectly cancelled.

3. **Single-master destinations**: Each destination table must only be written
   to by viaduck from the configured source. Concurrent writes from other
   sources would break at-least-once idempotency — a retried delete could
   remove a row inserted by another writer.

4. **Key uniqueness**: ``key_columns`` values are unique per row in the source.
   DuckLake has no unique constraints, so this cannot be enforced declaratively;
   violations mean delete-by-key over-deletes and duplicate-key upserts
   duplicate. Verified per partition at seed time
   (``_verify_seed_key_uniqueness``); post-seed inserts are not re-verified.

The algorithm processes CDC batches as unordered sets (not sequences). This is
sound because: (a) each batch covers a closed snapshot range, (b) batches are
always applied in ascending snapshot order via cursor tracking, and (c)
within-batch conflicts are resolved by rowid grouping before application.

Phases:
  1. Preimage Resolution (before routing) — pair update pre/postimages by
     rowid, convert cross-tenant preimages to deletes, drop same-tenant
     preimages.
  2. Conflict Resolution (per-destination, after routing) — drop inserts
     shadowed by a same-rowid postimage or delete (the delete survives as
     a tombstone), drop postimages shadowed by deletes.
  3. Apply (per-destination, atomic) — delete then upsert within a
     destination catalog transaction.
"""

import argparse
import gc
import logging
import os
import signal
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc

from viaduck import config, feed, lifecycle, logging_config, metrics, source
from viaduck.apply import _require_non_null_rowids
from viaduck.arrowutil import full_bool, row_indices
from viaduck.config import ConfigError
from viaduck.delivery import DeliveryManager
from viaduck.destination import DestinationPool
from viaduck.registry import DestinationRegistry
from viaduck.router import Router, RoutingError
from viaduck.server import DestStatus, health, status
from viaduck.state import StateManager

log = logging.getLogger(__name__)


def _validate_feed_mode(cfg) -> None:
    """source.cdc_reader=direct requires append_only: the feed implements
    table_insertions semantics only (no delete stream). Startup refuses the
    combination loudly rather than silently reading inserts-only."""
    if cfg.routing.mode != "append_only":
        raise ConfigError(
            f"source.cdc_reader='direct' requires routing.mode='append_only', got {cfg.routing.mode!r}: "
            "the feed implements table_insertions semantics only (no delete stream)"
        )


def _start_progress_heartbeat(
    label: str,
    interval_s: float = 30.0,
    state: dict | None = None,
    early_interval_s: float | None = None,
    early_duration_s: float = 60.0,
    pre_progress_label: str = "no progress yet",
    progress_conn=None,
) -> threading.Event:
    """Start a background heartbeat for a long-running blocking operation.

    Each tick while running:
      - logs a progress line for `<label>`, including elapsed seconds and any
        counters the caller put in `state` ("rows", "batches"); throughput is
        derived as rows / elapsed if "rows" is present;
      - calls `health.record_poll()` to touch `_last_poll`, keeping liveness
        green during operations that exceed `max_poll_age_s` (default 300s).

    `state` is a plain dict the caller mutates from its own thread (e.g.
    `state["rows"] += batch.num_rows` after each batch). Reads here are not
    locked: this relies on a strict single-writer / single-reader contract
    on a fixed key set, so the reader may see a stale value but never a
    torn one. If you parallelize the writer side, add a `Lock` or switch
    to atomics — `+=` is read-modify-write and concurrent writers will
    lose updates.

    `early_interval_s` (if set) is used for the first `early_duration_s`
    seconds, then the interval falls back to `interval_s`. This gives
    operators faster confirmation the pod is alive during cold-start without
    spamming the log forever.

    `progress_conn` (a duckdb connection, e.g. `table.catalog.connection`)
    enables percentage reporting: each tick polls `query_progress()` —
    a lightweight cross-thread read of DuckDB's executor state — and, when
    a query is live (>= 0), appends "~N% scanned, est. M remaining" to the
    line (ETA extrapolated as elapsed * (100-pct)/pct). Requires
    `enable_progress_bar=true` on the connection (set in
    source._CONNECTION_DEFAULTS) or the poll returns -1 forever. The
    percentage belongs to whatever query is currently running on that
    connection, so this is only meaningful while the caller holds the
    connection's single query slot (true for the seed scan: DuckDB
    invalidates an open streaming result if another query starts on the
    same connection, so one-scan-per-connection is already structurally
    enforced).

    The rate format only kicks in once `state["rows"] > 0`. While the
    counter is still at 0 — i.e. no batch has arrived yet — the tick
    instead logs `<label>: <pre_progress_label>, Ns elapsed`, since
    "0 rows/s" conveys no signal beyond elapsed time. Callers should
    set `pre_progress_label` to something that names the opaque phase
    being waited on (e.g. "DuckDB pre-execution").

    Returns a `threading.Event` the caller `.set()`s when the operation
    finishes (use try/finally). The thread is a daemon so it won't block
    process exit even if .set() is missed.
    """
    stop = threading.Event()
    start_t = time.monotonic()

    def _tick() -> None:
        from viaduck.server import health  # local to avoid import cycles

        while True:
            elapsed = time.monotonic() - start_t
            wait_s = early_interval_s if (early_interval_s is not None and elapsed < early_duration_s) else interval_s
            if stop.wait(timeout=wait_s):
                return
            elapsed = time.monotonic() - start_t
            scanned = _scan_progress_suffix(progress_conn, elapsed)
            if state and "rows" in state:
                rows = state["rows"]
                if rows > 0:
                    batches = state.get("batches", 0)
                    rate = rows / elapsed if elapsed > 0 else 0
                    log.info(
                        "%s: %d rows in %d batches, %.0fs elapsed (%.0f rows/s)%s",
                        label,
                        rows,
                        batches,
                        elapsed,
                        rate,
                        scanned,
                    )
                else:
                    log.info("%s: %s, %.0fs elapsed%s", label, pre_progress_label, elapsed, scanned)
            else:
                log.info("%s: still working (%.0fs elapsed)%s", label, elapsed, scanned)
            health.record_poll()

    threading.Thread(target=_tick, daemon=True).start()
    return stop


def _interruptible_sleep(total_seconds: float, should_stop, tick: float = 1.0) -> None:
    """Sleep for `total_seconds` but check `should_stop()` every `tick` seconds.

    Returns early as soon as `should_stop()` is truthy. Used so SIGTERM during
    a long poll interval is honored within ~1s rather than waiting for the
    full interval.
    """
    if total_seconds <= 0:
        return
    elapsed = 0.0
    while elapsed < total_seconds:
        if should_stop():
            return
        chunk = min(tick, total_seconds - elapsed)
        time.sleep(chunk)
        elapsed += chunk


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    return f"{int(minutes // 60)}h {int(minutes % 60)}m"


_scan_progress_poll_warned = False


def _scan_progress_suffix(progress_conn, elapsed: float) -> str:
    """Format DuckDB query progress as a log suffix, or '' when unavailable.

    query_progress() returns -1 when no query is running (or progress
    tracking is disabled). Readings below 1% are suppressed: early in a
    huge scan DuckDB reports fractional percentages, and extrapolating an
    ETA from "0.3% in 30s" yields a wildly noisy estimate attached to a
    display of "~0%". Failures are swallowed: progress is decoration,
    never worth killing the heartbeat over — but the first one is warned
    so API drift (e.g. a duckdb upgrade renaming query_progress) doesn't
    silently erase progress reporting forever. Polling continues after a
    failure since closed-connection races during scan teardown are
    transient.
    """
    global _scan_progress_poll_warned
    if progress_conn is None:
        return ""
    try:
        pct = float(progress_conn.query_progress())
    except Exception:
        if not _scan_progress_poll_warned:
            _scan_progress_poll_warned = True
            log.warning("Scan progress polling failed; omitting progress suffix", exc_info=True)
        return ""
    if pct < 1:
        return ""
    if pct >= 100:
        return " (~100% scanned)"
    eta = elapsed * (100.0 - pct) / pct
    return f" (~{pct:.0f}% scanned, est. {_fmt_duration(eta)} remaining)"


def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:.2f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.2f} PiB"


def _resolve_preimages(batch: pa.Table, routing_field: str, key_columns: list[str]) -> pa.Table:
    """Resolve update preimages before routing.

    - Pairs preimages with postimages by rowid.
    - Same routing value: drop preimage (same-tenant update, upsert handles it).
    - Different routing value: convert preimage to 'delete' (cross-tenant migration).
    - Orphaned preimages (no matching postimage): convert to 'delete' (defensive).

    Returns the batch with preimages resolved.

    Arrow-native: the preimage↔postimage pairing is a hash join on rowid;
    classification and the change_type rewrite are compute kernels. No
    per-row Python. Equivalence with the row-loop predecessor is locked by
    tests/unit/test_phase_equivalence.py.
    """
    # Validate key columns exist
    for col in key_columns:
        if col not in batch.column_names:
            raise RoutingError(f"Key column {col!r} not found in CDC data. Available: {batch.column_names}")

    ct_col = batch.column("change_type")
    pre_mask = pc.equal(ct_col, pa.scalar("update_preimage"))
    if not pc.any(pre_mask).as_py():
        return batch

    # Flatten chunking once so kernel outputs and masks align as plain arrays.
    batch = batch.combine_chunks()
    _require_non_null_rowids(batch)
    ct_col = batch.column("change_type")
    pre_mask = pc.equal(ct_col, pa.scalar("update_preimage")).combine_chunks()
    n = batch.num_rows
    row_idx = row_indices(n)

    # rowid -> postimage routing value. Duplicate postimage rowids within a
    # batch are out of contract (stable rowids + closed snapshot range), but
    # the predecessor's dict build made the last row win — preserve that via
    # max(row index) so behavior is identical even under contract violations.
    post_tbl = pa.table(
        {
            "rowid": batch.column("rowid"),
            "__post_routing": batch.column(routing_field),
            "__post_idx": row_idx,
        }
    ).filter(pc.equal(ct_col, pa.scalar("update_postimage")))
    last_idx = post_tbl.group_by("rowid").aggregate([("__post_idx", "max")])
    post_map = post_tbl.join(
        last_idx, keys=["rowid", "__post_idx"], right_keys=["rowid", "__post_idx_max"], join_type="inner"
    ).drop(["__post_idx"])
    # __matched disambiguates a join miss (orphan) from a genuinely-null
    # postimage routing value.
    post_map = post_map.append_column("__matched", full_bool(post_map.num_rows, True))

    # Join preimage rows against the postimage map. Joins don't preserve
    # order; carry the original row index and sort back afterwards.
    pre_tbl = pa.table(
        {
            "rowid": batch.column("rowid"),
            "__pre_routing": batch.column(routing_field),
            "__pre_idx": row_idx,
        }
    ).filter(pre_mask)
    joined = pre_tbl.join(post_map, keys="rowid", join_type="left outer").sort_by("__pre_idx")

    orphaned = pc.is_null(joined.column("__matched"))
    pre_r = joined.column("__pre_routing")
    post_r = joined.column("__post_routing")
    # Null-safe equality: null == null is a match (the predecessor compared
    # Python values, where None == None).
    routing_same = pc.or_(
        pc.fill_null(pc.equal(pre_r, post_r), False),
        pc.and_(pc.is_null(pre_r), pc.is_null(post_r)),
    )
    mutated = pc.and_(pc.invert(orphaned), pc.invert(routing_same))

    n_orphaned = pc.sum(pc.cast(orphaned, pa.int64())).as_py() or 0
    n_mutated = pc.sum(pc.cast(mutated, pa.int64())).as_py() or 0
    if n_orphaned:
        metrics.cdc_orphaned_preimages_total.inc(n_orphaned)
        log.debug("%d orphaned preimage(s) converted to delete", n_orphaned)
    if n_mutated:
        metrics.cdc_routing_mutations_total.inc(n_mutated)
        sample = joined.filter(mutated).column("rowid").slice(0, 5).to_pylist()
        log.error(
            "Routing column mutation detected on %d row(s) (sample rowids: %s). "
            "The routing column should not be updated. Handling defensively "
            "(delete from old destination, upsert to new), but CDC filter "
            "pushdown may have dropped other preimages. Verify data integrity.",
            n_mutated,
            sample,
        )

    # Per-preimage-row verdicts, in original row order: keep (as delete) when
    # orphaned or mutated; drop when same-tenant. Scatter back into full-size
    # arrays via replace_with_mask (replacements align positionally with the
    # mask's true slots, which are in row order).
    pre_keep = pc.or_(orphaned, mutated)
    keep_mask = pc.replace_with_mask(full_bool(n, True), pre_mask, pre_keep.combine_chunks())
    # Preserve the change_type column's exact dtype (string vs large_string):
    # downstream buffering concatenates Phase-1 outputs across reads, and
    # concat_tables rejects mixed schemas.
    delete_fill = pc.cast(pc.fill_null(pa.nulls(len(joined), pa.string()), "delete"), ct_col.type)
    new_ct = pc.replace_with_mask(ct_col.combine_chunks(), pre_mask, delete_fill)

    idx = batch.column_names.index("change_type")
    result = batch.set_column(idx, "change_type", new_ct)
    if not pc.all(keep_mask).as_py():
        result = result.filter(keep_mask)

    # Post-condition: no preimages should remain after resolution
    remaining = pc.sum(
        pc.cast(pc.equal(result.column("change_type"), pa.scalar("update_preimage")), pa.int64())
    ).as_py()
    assert not remaining, f"Bug: {remaining} update_preimage rows remain after Phase 1 resolution"
    return result


# ---------------------------------------------------------------------------
# Destination seeding
# ---------------------------------------------------------------------------


def _verify_seed_key_uniqueness(
    dest_id: str, seen_key_batches: list[pa.Table], key_columns: list[str], total_rows: int
):
    """Verify key_columns are unique within the seeded partition.

    DuckLake has no unique constraints, so key uniqueness is an unenforced
    contract — and a violated contract is silent data corruption: the
    delete path removes every destination row matching a key, so duplicate
    source keys cause over-deletes, and upserting duplicate-key batches
    writes duplicate rows. The seed scan already streams every row of the
    partition, so this check is free of extra I/O. Raises before the
    cursor advances; the partial seed re-runs after the operator fixes
    the source (or the key_columns config).

    Scope: per destination partition at seed time. Rows inserted AFTER the
    seed are not re-verified — the contract still applies, this just
    catches pre-existing violations at the cheapest possible moment.
    """
    distinct = pa.concat_tables(seen_key_batches).group_by(key_columns).aggregate([]).num_rows
    if distinct != total_rows:
        raise RoutingError(
            f"Destination {dest_id}: key_columns {key_columns} are not unique in the "
            f"source partition ({total_rows} rows, {distinct} distinct keys). "
            "DuckLake cannot enforce uniqueness; viaduck's delete-by-key would "
            "over-delete and upserts would duplicate. Fix the source data or the "
            "key_columns config before seeding."
        )


def _derive_dest_status(d, snap_now: int, lifecycle_state: str = "active") -> str:
    """Operational status for a destination, from its delivery snapshot.

    Raw flush lag is the wrong signal here: between flushes the persisted
    cursor is always behind the source — that's the buffering design
    working, not a problem. "lagging" means READS are behind (the data
    hasn't even been seen); read-current with data awaiting flush is
    "buffering".
    """
    if lifecycle_state != "active":
        # Operator intent short-circuits everything: an intentionally
        # paused destination is "paused", not a page-worthy "lagging".
        return lifecycle_state
    if d.last_error:
        return "error"
    if d.flushing:
        return "flushing"
    if snap_now > d.position_snapshot:
        return "lagging"
    if d.buffer_rows > 0 or d.position_snapshot > d.flushed_snapshot:
        return "buffering"
    return "healthy"


def _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, assigned_ids, *, source_columns):
    """Seed newly added destinations from a source table scan.

    For each destination at snapshot_id=0 (just initialized), reads the current
    source state filtered by routing value and bulk-loads the destination. Sets
    the cursor to the current source snapshot.

    This avoids replaying the entire CDC history for new destinations. Instead,
    one filtered scan captures the current state in a single read.

    When key_columns is configured, uses upsert for idempotency (safe if the
    destination already has data from a prior run). Without key_columns, uses
    append — crash between write and cursor advance will duplicate rows on
    re-seed (same at-least-once semantics as CDC).

    Memory: streams the filtered scan via `scan.to_arrow_batch_reader()` and
    writes one Arrow batch at a time, so peak memory is bounded by DuckDB's
    record-batch size rather than the total filtered result. Cursor
    advancement happens once after all batches succeed; a partial seed
    leaves the cursor at 0 and re-seeds on restart (at-least-once).
    """
    from pyducklake.expressions import EqualTo

    current_id = source.current_snapshot_id(src_table)
    if current_id is None:
        log.info("Source has no snapshots yet; nothing to seed")
        return  # empty source, nothing to seed

    cursors = state_mgr.load_cursors(assigned_ids)
    new_dest_ids = [did for did in assigned_ids if did not in cursors or cursors[did].last_snapshot_id == 0]

    if not new_dest_ids:
        log.info(
            "Seed scan: all %d assigned destinations already past snapshot 0; skipping",
            len(assigned_ids),
        )
        return

    log.info(
        "Seed scan: %d destinations need initial seed (source snapshot=%d)",
        len(new_dest_ids),
        current_id,
    )

    # Snapshot-level metadata stats — order-of-magnitude sense of how much
    # work the source scan will do. One quick metadata query, shared across
    # all destinations being seeded since they all read the same snapshot.
    try:
        files = src_table.inspect().files(snapshot_id=current_id)
        file_count = files.num_rows
        data_bytes = pc.sum(files.column("data_file_size_bytes")).as_py() or 0
        delete_bytes = pc.sum(files.column("delete_file_size_bytes")).as_py() or 0
        log.info(
            "Source snapshot %d: %d data files (%s), %s of delete data",
            current_id,
            file_count,
            _fmt_bytes(data_bytes),
            _fmt_bytes(delete_bytes),
        )
    except Exception:
        log.exception("Could not read snapshot file inventory; continuing without it")

    key_columns = cfg.routing.key_columns

    for dest_id in new_dest_ids:
        # Startup-only path with the post-merge cfg passed per call — the
        # same object the registry was built from, so this is not the
        # stale-STARTUP-capture class. If seeding is ever reused for a
        # mid-run add, resolve via the registry instead.
        dest_cfg = cfg.destination_by_id(dest_id)
        routing_value = dest_cfg.routing_value

        log.info(
            "Seeding destination %s: scanning source for routing_value=%s, snapshot=%d",
            dest_id,
            routing_value,
            current_id,
        )

        # Heartbeat keeps liveness green and emits progress while the scan +
        # write block (both can run minutes against a large source). Without
        # this, a >max_poll_age_s seed gets the pod killed by the liveness
        # probe before the first poll cycle ever runs.
        progress: dict[str, int] = {"rows": 0, "batches": 0}
        # Faster heartbeat for the first 60s so operators see liveness during
        # the often-slow DuckDB pre-execution phase (snapshot resolution,
        # zone-map evaluation) before the first batch arrives. Backs off to
        # 30s once streaming is under way.
        # Seeds run one at a time on this thread, so the source connection's
        # single query slot belongs to this scan — query_progress() can't
        # report someone else's query (see _start_progress_heartbeat).
        stop_heartbeat = _start_progress_heartbeat(
            f"Seed scan for destination {dest_id}",
            state=progress,
            early_interval_s=5.0,
            early_duration_s=60.0,
            pre_progress_label="DuckDB pre-execution",
            progress_conn=src_table.catalog.connection,
        )
        write_secs_total = 0.0
        try:
            seed_t0 = time.monotonic()
            # REPLACE-semantics guard: cursor 0 + existing rows for THIS
            # routing value can only mean a crashed prior seed
            # (single-master assumption). Truncate the partition so the
            # re-seed is a full repair — the spec's SeedDestination is
            # REPLACE; an upsert/append seed onto leftovers would preserve
            # phantoms (rows deleted in the source since the crashed
            # attempt) or duplicate (append mode). Crash mid-seed leaves
            # the cursor at 0, so the next attempt re-truncates:
            # convergent. Scoped by routing value, not whole-table, so a
            # misconfigured second destination sharing the table can never
            # wipe a sibling's data. Runs inside the heartbeat: a large
            # leftover delete can be slow.
            dest_filter = EqualTo(cfg.routing.field, routing_value)
            catalog, table = dest_pool.get(dest_id)
            try:
                existing_rows = table.scan(row_filter=dest_filter).count()
                if existing_rows > 0:
                    if not cfg.routing.seed_truncate:
                        raise RoutingError(
                            f"Destination {dest_id}: cursor is 0 but the destination table "
                            f"already has {existing_rows} rows for routing_value="
                            f"{routing_value!r}, and routing.seed_truncate is false. "
                            "Refusing to seed onto existing data — fix the destination "
                            "config or enable seed_truncate for REPLACE-semantics seeding."
                        )
                    log.warning(
                        "Destination %s: cursor 0 with %d existing rows for routing_value=%s "
                        "(crashed prior seed); truncating the partition before re-seed "
                        "(REPLACE semantics)",
                        dest_id,
                        existing_rows,
                        routing_value,
                    )
                    table.delete(dest_filter)
            finally:
                dest_pool.release(dest_id)
            # Pin scan to the captured snapshot to avoid skew — ensures the
            # cursor and the scanned data refer to the same point in time.
            scan = src_table.scan(
                row_filter=EqualTo(cfg.routing.field, routing_value),
                # Explicit projection (see run()'s source_columns): unrepresentable
                # or post-startup source columns must not enter the seed either.
                selected_fields=source_columns if source_columns is not None else ("*",),
                snapshot_id=current_id,
            )
            # Stream record batches so peak memory is bounded by DuckDB's
            # batch size, not the full filtered dataset. Per-batch progress
            # surfaces via the heartbeat thread reading `progress`.
            reader = scan.to_arrow_batch_reader()
            first_batch_logged = False
            # Key-uniqueness probe data: per-batch DISTINCT keys (key
            # columns only), verified after the stream. Duplicates collapse
            # within each batch, so memory is bounded by the destination's
            # distinct-key count, not its row count.
            seen_key_batches: list[pa.Table] = []
            for batch in reader:
                if not first_batch_logged:
                    pre_exec_secs = time.monotonic() - seed_t0
                    log.info(
                        "Seed scan for destination %s: first batch in %.1fs "
                        "(DuckDB pre-execution complete; streaming started)",
                        dest_id,
                        pre_exec_secs,
                    )
                    first_batch_logged = True
                if batch.num_rows == 0:
                    continue
                batch_table = pa.Table.from_batches([batch])
                if key_columns:
                    seen_key_batches.append(batch_table.select(key_columns).group_by(key_columns).aggregate([]))
                # get/release per batch: get() pins the pool entry, and an
                # unmatched pin makes the catalog unevictable for the
                # process lifetime (this loop leaked pins before).
                catalog, table = dest_pool.get(dest_id)
                write_t0 = time.monotonic()
                try:
                    if key_columns:
                        table.upsert(batch_table, join_cols=key_columns)
                    else:
                        table.append(batch_table)
                finally:
                    dest_pool.release(dest_id)
                write_secs_total += time.monotonic() - write_t0
                progress["rows"] += batch.num_rows
                progress["batches"] += 1

            seed_secs = time.monotonic() - seed_t0
            total_rows = progress["rows"]
            batch_count = progress["batches"]

            if key_columns and total_rows > 0:
                _verify_seed_key_uniqueness(dest_id, seen_key_batches, key_columns, total_rows)

            if total_rows > 0:
                log.info(
                    "Seeded destination %s: %d rows in %d batches (total=%.1fs, write=%.1fs)",
                    dest_id,
                    total_rows,
                    batch_count,
                    seed_secs,
                    write_secs_total,
                )
            else:
                log.info(
                    "Destination %s: no matching rows in source (scan=%.1fs); cursor advanced",
                    dest_id,
                    seed_secs,
                )
        finally:
            stop_heartbeat.set()

        state_mgr.advance_cursor(dest_id, current_id, cumulative_rows=total_rows)


# ---------------------------------------------------------------------------
# Poll cycle
# ---------------------------------------------------------------------------


def _initial_snapshot_id(seed_mode: str, src_table) -> int:
    """Return the snapshot_id to pass to initialize_destinations for new destinations.

    Kafka-style semantics:
      "scan"     — 0; _seed_new_destinations advances to head via full table scan
      "earliest" — MIN(snapshot_id) - 1; CDC range starts at the first available snapshot
      "latest"   — MAX(snapshot_id); only events arriving after startup are replicated
    """
    if seed_mode == "latest":
        head = source.current_snapshot_id(src_table)
        initial = head if head is not None else 0
        log.info("seed_mode=latest: new destinations start at snapshot %d", initial)
        return initial
    if seed_mode == "earliest":
        first = source.earliest_snapshot_id(src_table)
        initial = (first - 1) if first is not None else 0
        log.info("seed_mode=earliest: new destinations start at snapshot %d", initial)
        return initial
    return 0  # scan or default: _seed_new_destinations advances from 0


def run(cfg: config.ViaduckConfig) -> None:
    """Main poll loop."""
    metrics.init(cfg.pipeline_name)

    # One INFO line per leaf config field, before any external connections.
    # Lets ops grep the deploy log for individual values without re-reading
    # the rendered yaml or execing into the pod. Resolved secrets (postgres
    # URIs, S3 creds) are never logged — only env var names.
    cfg.log_summary(log)

    from viaduck import server

    http = server.start(cfg.server.port, web_enabled=cfg.web.enabled)

    # Mark the process as started BEFORE any heavy bring-up work (catalog
    # ATTACH, destination seeding). Otherwise /healthz returns 503 for the
    # whole bring-up phase and kubelet kills the pod long before viaduck
    # ever reaches its poll loop. mark_started seeds `_last_poll` with now,
    # so we get `max_poll_age_s` (default 300s) of grace for bring-up.
    # If the initial seed legitimately takes longer than that, raise
    # `max_poll_age_s` rather than reverting this ordering.
    health.mark_started()

    # Reclaim spill dirs from previous containers BEFORE the first
    # connection creates a new one (/tmp is a pod-level emptyDir — crash
    # leftovers survive container restarts and hold real bytes).
    source.sweep_spill_dirs()

    # Connect to source. required_columns: an excludable-typed routing/key
    # column must fail startup loudly — replication cannot function without
    # them and every downstream error would be more confusing than this one.
    src_catalog = source.connect(cfg.source)
    src_table = source.load_table(
        src_catalog,
        cfg.source.table,
        required_columns=(cfg.routing.field, *cfg.routing.key_columns),
    )
    # Explicit projection for every source read (CDC + seed): pins the
    # column set to the startup schema — which load_table already filtered
    # to types pyducklake can represent — so an upstream column added
    # mid-stream (e.g. millpond's VARIANT dual-write companions) can't
    # fail reads or destination appends; it stays invisible until restart.
    # None only when a column name embeds a double quote (legacy SELECT *).
    source_columns = source.replicated_column_names(src_table)

    # Direct-SQL feed (source.cdc_reader=direct): bypasses the extension
    # changefeed's per-bind cost (~2.4-3s/call fixed) with indexed catalog
    # SQL + stock parquet scans. append_only only — full_cdc's delete stream
    # and preimage resolution have no feed implementation (fail loudly at
    # startup rather than silently reading inserts-only). The extension path
    # stays as the rollback while the flag exists.
    feed_reader = None
    if cfg.source.cdc_reader == "direct":
        _validate_feed_mode(cfg)
        feed_reader = feed.FeedReader(
            # The chart's SOURCE_POSTGRES_URI is DuckDB-ATTACH format
            # (postgres:host=…) which psycopg rejects verbatim — same
            # translation the StateManager already gets (review F2).
            postgres_uri=config._to_libpq_conninfo(cfg.source.postgres_uri),
            catalog_name=cfg.source.name,
            data_path=cfg.source.data_path,
        )
        feed_reader.verify_catalog()
        log.info("Direct-SQL feed reader enabled for source %s", cfg.source.name)

    # Read pool: the feed's parallel data plane. Legacy extension mode stays
    # serial on the source connection (the extension path is one query slot).
    read_pool = None
    if feed_reader is not None:
        read_pool = _ReadPool(cfg.source.resolved_properties(), cfg.poll.read_workers)

    # Initialize state and destinations. Cursor state lives on plain
    # Postgres (NOT a DuckLake table): a cursor advance must not create
    # catalog snapshots, or idle destinations generate CDC work forever.
    state_mgr = StateManager(cfg.state.resolve_postgres_uri(cfg.source), cfg.instance.id, cfg.state)
    router = Router(cfg.routing)

    # CP-driven destination discovery (M4: additive at startup, static
    # wins, set fixed for the process lifetime; the DriftWatcher below
    # only detects divergence). Fail-open: an unreachable CP starts us
    # static-only with viaduck_discovery_synced=0 — the durable source
    # makes late pickup safe and the gauge makes it loud.
    drift_watcher = None
    all_destinations = list(cfg.destinations)
    discovered_ids: set[str] = set()
    if cfg.discovery.enabled:
        from viaduck import discovery as disco

        # The ENTIRE discovery block fails open: no payload problem, CP
        # bug, config typo (incl. a missing token env), or unexpected
        # exception may take static tenants down. Any failure → static-
        # only + synced=0 (the alertable signal) + the drift watcher
        # still running so recovery is visible.
        auth = None
        baseline: dict[str, disco.MappedDestination] = {}
        generation = -1
        discovered: list = []
        static_routing_values = {d.routing_value for d in cfg.destinations}
        static_only_ids = {d.id for d in cfg.destinations}
        try:
            auth = cfg.discovery.auth_header()
            payload = None
            last_err: Exception | None = None
            for attempt in range(3):
                try:
                    payload = disco.fetch(cfg.discovery.url, auth, cfg.discovery.request_timeout_s)
                    break
                except disco.DiscoveryError as e:
                    last_err = e
                    if attempt < 2:
                        time.sleep(min(2**attempt, 5))
            if payload is None:
                raise last_err  # type: ignore[misc]
            mapped = disco.map_payload(payload)
            if len(mapped) < cfg.discovery.min_destinations:
                raise disco.DiscoveryError(
                    f"payload mapped {len(mapped)} destination(s) < discovery.min_destinations "
                    f"({cfg.discovery.min_destinations}) — refusing a suspiciously empty payload "
                    "(set min_destinations: 0 for a genuine zero-tenant bootstrap)"
                )
            discovered = disco.materialize(
                mapped,
                static_routing_values,
                cfg.discovery.defaults,
                static_ids=static_only_ids,
                # Bounded wall time: N sequential Secret reads against a
                # blackholed API server must not eat the liveness grace
                # and crashloop static tenants (round-2 review). The
                # heartbeat keeps /healthz green while reads progress.
                deadline_s=cfg.discovery.materialize_deadline_s,
                heartbeat=health.record_poll,
                secret_timeout_s=cfg.discovery.request_timeout_s,
                secret_cache_ttl_s=cfg.discovery.secret_cache_ttl_s,
                allowed_endpoint_suffixes=cfg.discovery.allowed_endpoint_suffixes,
                allowed_secret_namespaces=cfg.discovery.allowed_secret_namespaces,
            )
            generation = payload["config_generation"]
            baseline = {m.dest_id: m for m in mapped}
            all_destinations = list(cfg.destinations) + discovered
            cfg = replace(cfg, destinations=all_destinations)
            # Success only after the merged config validated — synced=1
            # means "this process reflects the CP", and ONLY the startup
            # path may set it (the drift poller's success must not clear
            # the static-only alert; round-2 review).
            disco.record_success_metrics(payload)
            metrics.discovery_destinations.set(len(discovered))
            discovered_ids = {d.id for d in discovered}
            if mapped and not discovered:
                log.error(
                    "Discovery mapped %d destination(s) but materialized NONE — every entry was "
                    "dropped (see discovery_broken_entries_total); running static-only",
                    len(mapped),
                )
            else:
                log.info(
                    "Discovery: %d destination(s) materialized (generation %s), %d static",
                    len(discovered),
                    generation,
                    len(cfg.destinations) - len(discovered),
                )
            for d in discovered:
                # Non-secret summary for grep parity with log_summary
                # (which ran before the merge). Never the URI.
                log.info("Discovered destination %s: table=%s data_path=%s", d.id, d.table, d.data_path)
        except Exception:
            metrics.discovery_synced.set(0)
            metrics.discovery_destinations.set(0)
            metrics.discovery_poll_failures_total.inc()
            baseline = {}
            generation = -1
            discovered_ids = set()
            log.error(
                "Discovery failed at startup — running STATIC-ONLY until restart "
                "(discovered tenants get no delivery this process lifetime)",
                exc_info=True,
            )
        # Constructed AFTER the try/except so the baseline is final —
        # no post-construction mutation, no cross-thread hazard.
        drift_watcher = disco.DriftWatcher(
            url=cfg.discovery.url,
            auth_header=auth,
            timeout_s=cfg.discovery.request_timeout_s,
            poll_interval_s=cfg.discovery.poll_interval_s,
            baseline=baseline,
            startup_generation=generation,
            apply_mode=cfg.discovery.apply_enabled,
        )
    # The registry is built from the post-merge cfg and is THE runtime
    # resolution path for destination configs — the pool, the poll cycle,
    # and the status export all read through it (or a per-cycle snapshot
    # of it), never through the frozen startup cfg. That capture-at-
    # startup pattern produced three separate stale-config defects in the
    # discovery effort; see viaduck/registry.py.
    registry = DestinationRegistry.from_configs(cfg.destinations, discovered_ids=discovered_ids)
    dest_pool = DestinationPool(cfg, registry, max_open=cfg.delivery.pool_max_open)
    # Cache source schema for destination table creation.
    # `Table.schema` is a property in pyducklake — do not call it.
    dest_pool.set_source_schema(src_table.schema)

    assigned_ids = cfg.assigned_destination_ids()

    # Destination lifecycle (viaduck/lifecycle.py): retired destinations are
    # excluded from EVERYTHING at startup — no state row, no seed, no buffer,
    # no pool slot. Paused/draining start constructed but gated (the tracker
    # applies their state before the first cycle).
    lifecycle_rows = state_mgr.load_lifecycle_rows(assigned_ids)
    raw_lifecycle = {d: r["state"] for d, r in lifecycle_rows.items()}
    retired_ids = [d for d in assigned_ids if lifecycle.normalize(raw_lifecycle.get(d), d) == lifecycle.RETIRED]
    if retired_ids:
        log.warning(
            "Excluding %d retired destination(s) from this run: %s "
            "(re-activation requires a lifecycle row update and re-seeds per seed_mode)",
            len(retired_ids),
            retired_ids,
        )
    assigned_ids = [d for d in assigned_ids if d not in retired_ids]
    for did in retired_ids:
        # Retired = not routable. The registry retains the config entry
        # (never-delete: an in-flight anything must still resolve it) but
        # drops it from the routing index, which is what excludes it from
        # every per-cycle rv_to_dest below.
        registry.remove(did)
        # Sever the resume point: re-add = new tenant = fresh seed. Also
        # done per-cycle for a mid-run retire; this is the restart backstop.
        state_mgr.delete_destination_state(did)
        metrics.set_destination_lifecycle(did, lifecycle.RETIRED, lifecycle.VALID_STATES)

    initial_snapshot_id = _initial_snapshot_id(cfg.routing.seed_mode, src_table)
    static_assigned = [d for d in assigned_ids if d not in discovered_ids]
    state_mgr.initialize_destinations(static_assigned, initial_snapshot_id=initial_snapshot_id)
    disc_assigned = [d for d in assigned_ids if d in discovered_ids]
    if disc_assigned:
        # C5 seed semantics: discovery STARTS THE STREAM, it never
        # backfills (that stays with provisioning/DLT). Discovered
        # destinations initialize at the current source head regardless
        # of the pipeline's seed_mode — a fresh cursor at latest also
        # keeps them out of the scan-seed pass by construction
        # (_seed_new_destinations only touches cursor-0 rows).
        latest = source.current_snapshot_id(src_table) or 0
        state_mgr.initialize_destinations(disc_assigned, initial_snapshot_id=latest)
        log.info(
            "Discovery: %d assigned destination(s) initialized at source head (snapshot %d)",
            len(disc_assigned),
            latest,
        )

    # Seed new destinations from source scan (avoids CDC replay from
    # snapshot 0). Only lifecycle-ACTIVE destinations seed: a paused
    # destination must not receive bulk writes (pausing a broken
    # destination is the obvious operator move, and a failing seed here
    # would crashloop the whole instance). A skipped cursor-0 destination
    # is remembered and kept out of the read set until a restart seeds it
    # (see seed_pending in the poll loop).
    seed_pending: set[str] = set()
    if cfg.routing.seed_mode == "scan":
        seed_eligible = [
            d
            for d in assigned_ids
            if d not in discovered_ids and lifecycle.normalize(raw_lifecycle.get(d), d) == lifecycle.ACTIVE
        ]
        # Discovered dests are neither seedable nor "skipped" — they
        # initialize at head below; without this exclusion a genuinely
        # empty source (head=0) would trap them in seed_pending forever
        # with a misleading restart-to-seed ERROR (round-2 review).
        skipped = [d for d in assigned_ids if d not in seed_eligible and d not in discovered_ids]
        if skipped:
            skipped_cursors = state_mgr.load_cursors(skipped)
            seed_pending = {d for d in skipped if d not in skipped_cursors or skipped_cursors[d].last_snapshot_id == 0}
            if seed_pending:
                log.warning(
                    "Skipping seed for non-active destination(s) %s; they stay read-gated until a restart seeds them",
                    sorted(seed_pending),
                )
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, seed_eligible, source_columns=source_columns)

    key_columns = cfg.routing.key_columns
    mode = cfg.routing.mode

    # Buffered delivery: per-destination buffers + flush worker pool
    # (constructed AFTER seeding so positions initialize from the
    # post-seed cursors). Workers report successful flushes into the
    # readiness signal.
    delivery = DeliveryManager(
        cfg.delivery,
        state_mgr,
        dest_pool,
        key_columns,
        assigned_ids,
        mode=mode,
        on_flush_success=health.record_replication,
        # Static destinations may carry explicit queue-cap overrides
        # (DestinationConfig.buffer_max_bytes); discovered destinations
        # use the global default.
        per_dest_cap_overrides={d.id: d.buffer_max_bytes for d in cfg.destinations if d.buffer_max_bytes > 0},
    )

    tracker = lifecycle.LifecycleTracker(assigned_ids)
    tracker.apply(raw_lifecycle, delivery, dest_pool, state_mgr)
    delivery.set_suspended(tracker.suspended_ids())
    tracker.export_metrics()
    server.set_lifecycle_states(tracker.states(), rows=lifecycle_rows)

    log.info(
        "Viaduck started: source=%s.%s, routing_field=%s, mode=%s, destinations=%d, instance=%s",
        cfg.source.name,
        cfg.source.table,
        cfg.routing.field,
        mode,
        len(assigned_ids),
        cfg.instance.id,
    )

    reconciler = None
    if drift_watcher is not None and cfg.discovery.apply_enabled:
        from viaduck.reconciler import Reconciler

        reconciler = Reconciler(
            cfg,
            registry,
            delivery,
            dest_pool,
            tracker,
            state_mgr,
            static_routing_values=static_routing_values,
            static_ids=static_only_ids,
            baseline_mapped=dict(baseline),
            src_head_fn=lambda: source.current_snapshot_id(src_table),
            heartbeat=health.record_poll,
        )
        log.info(
            "C3 reconciler ACTIVE (discovery.apply_enabled=true): CP-driven start/stop/restart, "
            "k=%d clean fetches to stop, floor=%.0f%% (min_destinations=%d), "
            "restart_min_interval_s=%.0f",
            cfg.discovery.absent_stop_fetches,
            cfg.discovery.stop_floor_fraction * 100,
            cfg.discovery.min_destinations,
            cfg.discovery.restart_min_interval_s,
        )

    if drift_watcher is not None:
        drift_watcher.start()

    shutdown = False

    recycle_watermark_gib = resolve_recycle_watermark(cfg.memory)
    loop_started_at = time.monotonic()
    recycling = False

    def _signal_handler(signum, frame):
        nonlocal shutdown
        log.info("Received signal %s, shutting down", signal.Signals(signum).name)
        shutdown = True

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    while not shutdown:
        try:
            # C3 reconciler: apply the latest classified view BEFORE this
            # cycle's lifecycle load and read planning, so an activation
            # delivers this cycle and the retention-edge clamp covers a
            # re-added stale cursor before its first read.
            if reconciler is not None:
                reconciler.apply(drift_watcher.latest())
            # LIVE membership: the delivery manager's active set is the
            # single authority (statics + reconciler adds − reconciler
            # removes); with the reconciler off it equals the startup
            # capture. Feeds the lifecycle load, read gating, and the
            # status export.
            current_ids = sorted(delivery.active_ids())
            # Refresh operator lifecycle intent. A state-store blip keeps
            # the last-known states (fail-safe: intent changes rarely; not
            # delivering on a stale ACTIVE would be worse than delivering
            # one cycle late on a stale PAUSE).
            try:
                lifecycle_rows = state_mgr.load_lifecycle_rows(current_ids)
                raw_lifecycle = {d: r["state"] for d, r in lifecycle_rows.items()}
            except Exception:
                log.warning("Lifecycle state load failed; keeping last-known states", exc_info=True)
            tracker.apply(raw_lifecycle, delivery, dest_pool, state_mgr)
            delivery.set_suspended(tracker.suspended_ids())
            server.set_lifecycle_states(tracker.states(), rows=lifecycle_rows)

            read_ids = [d for d in tracker.readable_ids() if d not in seed_pending]
            for d in tracker.readable_ids():
                if d in seed_pending:
                    log.error(
                        "Destination %s is active but was never seeded (skipped while non-active); "
                        "restart viaduck to seed it — until then it takes no reads",
                        d,
                    )

            # ONE immutable registry snapshot per cycle: routing, config
            # resolution, and status all read this view — no mid-cycle
            # reads of the live registry (C3 §5).
            reg_snap = registry.snapshot()
            _poll_cycle(
                src_table,
                delivery,
                dest_pool,
                router,
                cfg,
                current_ids,
                reg_snap.rv_to_dest,
                key_columns,
                mode,
                source_columns=source_columns,
                read_ids=read_ids,
                lifecycle_states=tracker.states(),
                dest_configs=reg_snap.configs,
                feed_reader=feed_reader,
                read_pool=read_pool,
            )
        except Exception:
            log.exception("Fatal error in poll cycle")
            break

        # Watermark self-recycle: preempt the residual-leak OOM with a clean
        # exit through the SAME graceful path SIGTERM takes — drain() flushes
        # every buffer so cursors land tight at the read position, and the
        # kubelet restarts a fresh process with no rewind and no
        # cursor-group scatter.
        if not shutdown and _should_self_recycle(
            recycle_watermark_gib, loop_started_at, cfg.memory.self_recycle_min_uptime_seconds
        ):
            # Best-effort metric: the process exits shortly after and the
            # counter resets, so a scrape can miss it. The durable signals
            # are the [SELF-RECYCLE] WARN and the container's last-state
            # `Completed`/exit 0 (vs `OOMKilled`/137).
            metrics.self_recycles_total.inc()
            recycling = True
            shutdown = True

        if not shutdown:
            # Chunked sleep so SIGTERM is honored within ~1s rather than
            # waiting up to `interval_seconds`. With long poll intervals (e.g.
            # 300s) and k8s `terminationGracePeriodSeconds` (default 30s), an
            # uninterruptible sleep would let kubelet SIGKILL mid-poll.
            _interruptible_sleep(cfg.poll.interval_seconds, lambda: shutdown)

    # Graceful shutdown: flush everything buffered (the spec's
    # shutdown-trigger FlushStart), wait for workers, then close.
    #
    # The recycle path gets a longer drain budget than SIGTERM: SIGTERM is
    # bounded by terminationGracePeriodSeconds (kubelet SIGKILLs at the
    # deadline, so a drain longer than the grace just dies mid-close), but a
    # self-recycle has NO grace clock — nothing external is killing the
    # process — so it can afford to flush a fat catch-up buffer instead of
    # abandoning it to a cursor rewind. The bound that DOES apply here is
    # the liveness probe: with the poll loop stopped, /healthz goes stale
    # after ~300s poll-age plus the probe's 10x30s failure budget (~600s
    # total), which is also the backstop that reaps a flush worker wedged
    # in a native call (the interpreter's exit-join would otherwise wait
    # forever — kubelet SIGKILL via failed liveness is the way out of that
    # already-pathological state).
    log.info("Shutting down...")
    delivery.drain(timeout_s=_RECYCLE_DRAIN_TIMEOUT_S if recycling else 60.0)
    dest_pool.close_all()
    state_mgr.close()
    if feed_reader is not None:
        try:
            feed_reader.close()
        except Exception:
            log.warning("Feed reader close failed", exc_info=True)
    if read_pool is not None:
        try:
            read_pool.close()
        except Exception:
            log.warning("Read pool close failed", exc_info=True)
    try:
        src_catalog.close()
    except Exception:
        pass
    # Tell SSE handlers to exit before calling http.shutdown(), otherwise
    # an open /ui/sse client would block shutdown() forever.
    if drift_watcher is not None:
        drift_watcher.stop()
    server.signal_shutdown()
    http.shutdown()
    log.info("Shutdown complete")


# Time-lag export failures are throttled like _MEM_STATS_FAILURES: a
# persistent failure (revoked grant, meta-schema rename) would otherwise
# freeze the gauge at its last value with only DEBUG logs — a flat lag
# line that reads as "healthy" is worse than an absent one.
_TIME_LAG_FAILURES = 0
_TIME_LAG_LOG_EVERY_NTH_FAILURE = 100


def _export_dest_time_lag(src_table, delivery_snapshot, assigned_ids, snap_now: int) -> None:
    """Export viaduck_dest_time_lag_seconds: wall-clock age of each
    destination's last flushed source snapshot (now - snapshot_time), from
    ONE indexed ducklake_snapshot lookup shared by all destinations. Exact —
    unlike dest_lag_snapshots, which needs a commit-rate assumption to
    convert to time.

    A destination whose cursor is AT the source head is caught up and reads
    0 — without this, a quiet source (no commits) would grow the gauge
    without bound while dest_lag_snapshots correctly reads 0 in the same
    state (the classic idle-stream lag trap; caught up => 0). The healthy-
    state floor for a busy source is ~poll+flush cadence: the honest
    staleness of the durable cursor.

    Best-effort: a lookup failure must never break the poll cycle. Clock
    skew between this pod and the catalog writer is acceptable at seconds
    granularity. A never-flushed destination (cursor 0) is skipped —
    there is no meaningful cursor to age.
    """
    global _TIME_LAG_FAILURES
    try:
        caught_up = {did for did in assigned_ids if delivery_snapshot[did].flushed_snapshot >= snap_now}
        for did in caught_up:
            metrics.dest_time_lag_seconds.labels(destination=did).set(0.0)
        cursors = {delivery_snapshot[did].flushed_snapshot for did in assigned_ids if did not in caught_up}
        cursors.discard(0)
        if not cursors:
            return
        times = source.snapshot_times(src_table, sorted(cursors))
        now = datetime.now(UTC)
        for did in assigned_ids:
            if did in caught_up:
                continue
            snap = delivery_snapshot[did].flushed_snapshot
            ts = times.get(snap)
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            lag_s = max((now - ts).total_seconds(), 0.0)
            metrics.dest_time_lag_seconds.labels(destination=did).set(lag_s)
        _TIME_LAG_FAILURES = 0
    except Exception:
        _TIME_LAG_FAILURES += 1
        if _TIME_LAG_FAILURES == 1 or _TIME_LAG_FAILURES % _TIME_LAG_LOG_EVERY_NTH_FAILURE == 0:
            log.warning(
                "time-lag export failed (%d consecutive); gauge is stale until this recovers",
                _TIME_LAG_FAILURES,
                exc_info=True,
            )
        else:
            log.debug("time-lag export failed; skipping this cycle", exc_info=True)


def _clamp_expired_cursors(delivery, dest_ids, earliest_snapshot) -> set[str]:
    """Retention-edge clamp (bug fix, not policy): advance any cursor that
    has fallen below the earliest retained source snapshot up to the edge,
    acknowledging the expired range — loudly (WARNING + counter, emitted
    by DeliveryManager.clamp_to_retention, which owns the race-safe
    check-and-stamp).

    Without this, the CDC read from an expired cursor raises inside the
    poll cycle and the run loop's fatal-error handler exits the loop: ONE
    destination whose cursor outlived snapshot retention (re-add after a
    long stop, a pause that outlived retention, a multi-day flush-failure
    loop) takes delivery down for the WHOLE instance.

    The clamp floor is earliest-1: reads are exclusive of the cursor, so a
    cursor at earliest-1 reads from the oldest retained snapshot. Keyed on
    the durable cursor (not position): flushed below the floor with
    position above it is the flush-failure rewind hazard — the rewind goes
    to flushed, and the next read from there would be the fatal one.

    Failures are contained PER DESTINATION: one candidate's state-store
    blip must not abort its peers' clamps, and a destination whose clamp
    failed is returned so the caller excludes it from this cycle's reads —
    proceeding to read from its still-expired cursor would be the exact
    fatal path this function exists to remove. Retried next cycle.
    """
    still_expired: set[str] = set()
    if earliest_snapshot is None:
        return still_expired
    floor = earliest_snapshot - 1
    try:
        flushed = delivery.flushed_snapshots()
    except Exception:
        log.warning("Retention-edge clamp check failed; skipping this cycle", exc_info=True)
        return still_expired
    for did in dest_ids:
        if flushed.get(did, floor) >= floor:
            continue
        try:
            delivery.clamp_to_retention(did, floor)
        except Exception:
            still_expired.add(did)
            log.warning(
                "Retention-edge clamp for %s failed (cursor %d < earliest retained %d); "
                "excluding it from this cycle's reads, retrying next cycle",
                did,
                flushed[did],
                earliest_snapshot,
                exc_info=True,
            )
    return still_expired


class _InlineFuture:
    """Synchronous stand-in for a pool future when no read pool exists
    (tests): result() runs the thunk LAZILY, so per-unit failure
    containment in the apply loop works identically to the pool path."""

    def __init__(self, thunk):
        self._thunk = thunk

    def result(self):
        return self._thunk()


class _ReadPool:
    """N bare DuckDB connections behind a small executor (the feed's
    parallel data plane, proposal §6.3). A DuckDB connection has ONE query
    slot, so unit reads check out a connection for their duration. Feed
    planning (psycopg) stays on the poll thread — _plan_read never runs
    here."""

    def __init__(self, props: dict[str, str], workers: int):
        import queue
        from concurrent.futures import ThreadPoolExecutor

        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="read")
        self._conns: queue.Queue = queue.Queue()
        for i in range(workers):
            self._conns.put(source.open_read_connection(props, name=f"read-{i}"))

    def submit(self, fn, **kwargs):
        def run():
            conn = self._conns.get()
            try:
                return fn(read_conn=conn, **kwargs)
            finally:
                self._conns.put(conn)

        return self._executor.submit(run)

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
        while True:
            try:
                self._conns.get_nowait().close()
            except Exception:
                return


def _position_clusters(positions: dict[str, int], read_ids, span_cap: int) -> list[tuple[int, list[str]]]:
    """Cluster ACTIVE destinations into read units: [(cluster_lo, members)],
    ascending by lo. span_cap > 0 (the feed's per-row snapshot attribution)
    merges positions within one budget span — the shared read is masked per
    destination by its own position. span_cap = 0 (legacy extension reads
    carry no per-row snapshot) falls back to exact-position groups only.
    """
    by_pos: dict[int, list[str]] = {}
    for d in read_ids:
        if d in positions:
            by_pos.setdefault(positions[d], []).append(d)
    clusters: list[tuple[int, list[str]]] = []
    cur_lo: int | None = None
    cur_members: list[str] = []
    for p in sorted(by_pos):
        if cur_lo is not None and (span_cap <= 0 or p - cur_lo > span_cap):
            clusters.append((cur_lo, cur_members))
            cur_lo, cur_members = None, []
        if cur_lo is None:
            cur_lo, cur_members = p, []
        cur_members.extend(by_pos[p])
    if cur_lo is not None:
        clusters.append((cur_lo, cur_members))
    return clusters


def _read_unit(
    *,
    src_table,
    read_conn,
    feed_reader,
    planned,
    lo: int,
    hi: int,
    members,
    router,
    dest_configs,
    full_cdc: bool,
    source_columns,
) -> tuple[pa.Table, int]:
    """Read one unit (lo, hi] for the cluster's union filter. Runs on a
    read-pool thread: pure I/O, no shared state, no buffer writes (the poll
    thread applies results). Feed reads are pre-planned on the poll thread
    (`planned`) — the FeedReader's psycopg connection is single-threaded;
    the pool never plans. The legacy extension path reads the span-bounded
    range it is given (its chunk equivalent) since it has no per-row
    attribution to slice on.
    """
    if planned is not None:
        t0 = time.monotonic()
        rows = feed.execute_read(read_conn, planned)
        if rows is None:
            # A re-planned unit can come back empty (flushed/compacted
            # away between plan and execute) — the empty path advances
            # positions in _apply_unit.
            rows = pa.table({})
        # Metric continuity with the extension path (which observes these
        # inside read_cdc): the loop path must not darken the dashboards
        # on flag-flip (review O1).
        metrics.cdc_read_seconds.observe(time.monotonic() - t0)
        metrics.cdc_rows_read_total.inc(rows.num_rows)
        log.info("CDC unit →%d: %d rows (feed)", hi, rows.num_rows)
        return rows, hi
    routing_values = [dest_configs[d].routing_value for d in members]
    filter_expr = router.build_filter_expr(routing_values)
    if full_cdc:
        rows = source.read_cdc_changes(
            src_table, after_snapshot=lo, end_snapshot=hi, filter_expr=filter_expr, columns=source_columns
        )
    else:
        rows = source.read_cdc(
            src_table, after_snapshot=lo, end_snapshot=hi, filter_expr=filter_expr, columns=source_columns
        )
    log.info("CDC unit %d→%d: %d rows (extension)", lo, hi, rows.num_rows)
    return rows, hi


def _slice_batch(batch: pa.Table, snaps: list[int], lo: int, hi: int, max_rows: int) -> list[tuple[pa.Table, int, int]]:
    """Slice a routed per-destination batch into <= max_rows buffer entries
    with the slice-cursor rule (tla/Viaduck.tla; proposal §6.2):
    cov_k = max(lo, min(snapshot over slices > k) - 1), cov_last = hi, and
    entry hi_k = the slice's own max snapshot. The durable cursor never
    passes undelivered rows: no row with snapshot <= cov_k exists in a
    later slice. append_only only (any row order is safe — no conflicting
    pairs; full_cdc units are never sliced, preserving snapshot order).
    """
    # Fast path (small batch): hi_k is the UNIT hi, not MaxSnap(rows) —
    # inflated on purpose (conservative for the drop rule: fewer entries
    # droppable). The sliced path below computes the own-max per slice.
    if batch.num_rows <= max_rows:
        return [(batch, hi, hi)]
    slices = []
    offset = 0
    while offset < batch.num_rows:
        slices.append(batch.slice(offset, max_rows))
        offset += max_rows
    out = []
    running = lo
    assert all(sn > lo for sn in snaps) and all(sn <= hi for sn in snaps), (
        "slice rule precondition: every row's snapshot must be in (lo, hi]"
    )
    for k, sl in enumerate(slices):
        later = snaps[(k + 1) * max_rows :]  # snaps of slices after k
        cov = max(running, (min(later) - 1) if later else hi)
        cov = hi if k == len(slices) - 1 else cov
        out.append((sl, cov, max(snaps[k * max_rows : (k + 1) * max_rows])))
        running = cov
    assert [c for _, c, _ in out] == sorted(c for _, c, _ in out), "cov chain must be non-decreasing"
    return out


def _apply_unit(
    delivery,
    rows: pa.Table,
    hi: int,
    *,
    members,
    positions,
    epochs,
    router,
    rv_to_dest,
    full_cdc: bool,
    key_columns,
    unit_cfg,
    routing_field,
    dest_configs,
) -> None:
    """Apply one completed read unit on the POLL THREAD: Phase 1 (full_cdc),
    route, mask per destination by its own position (cluster fan-in), slice
    (append_only feed reads carry per-row snapshots), buffer with the
    per-entry cov chain. Members with no routed rows just advance their
    read position. A flush failure landing mid-apply rewinds the
    destination and bumps its epoch — the stale remainder is discarded by
    the epoch guard in buffer()/advance_position().
    """
    routing_values = [dest_configs[d].routing_value for d in members]
    if rows.num_rows == 0:
        for d in members:
            delivery.advance_position(d, hi, epoch=epochs[d])
        return

    if full_cdc:
        rows = _resolve_preimages(rows, routing_field, key_columns)
        # The extension's meta columns ride into the batch; slicing is
        # skipped for full_cdc (pair-order contract) — whole-unit entries.
        routed, unrouted = router.split_and_count(rows, routing_values)
        if unrouted > 0:
            metrics.unrouted_rows_total.inc(unrouted)
        routed_ids = set()
        for rv, batch in routed.items():
            dest_id = rv_to_dest[rv]
            if batch.num_rows > 0:
                delivery.buffer(dest_id, batch, hi, epoch=epochs[dest_id])
                # Mark routed only when rows actually landed: an empty batch
                # must not suppress the position advance below (review F4).
                routed_ids.add(dest_id)
        for d in members:
            if d not in routed_ids:
                delivery.advance_position(d, hi, epoch=epochs[d])
        return

    # append_only: mask per destination by its own position, then slice.
    routed, unrouted = router.split_and_count(rows, routing_values)
    if unrouted > 0:
        metrics.unrouted_rows_total.inc(unrouted)
    routed_ids = set()
    for rv, batch in routed.items():
        dest_id = rv_to_dest[rv]
        if batch.num_rows == 0:
            continue
        has_snaps = feed.SNAP_COL in batch.column_names
        if has_snaps:
            # Cluster fan-in: drop rows this member already has (its
            # position is past the cluster min). Vectorized.
            pos = positions[dest_id]
            batch = batch.filter(pc.greater(batch.column(feed.SNAP_COL), pa.scalar(pos)))
            if batch.num_rows == 0:
                # Masked to empty: (pos, hi] is provably empty for this
                # member — advance it like the zero-row path, or it re-reads
                # and re-discards the same span every cycle until the
                # cluster separates.
                delivery.advance_position(dest_id, hi, epoch=epochs[dest_id])
                continue
            snaps = batch.column(feed.SNAP_COL).to_pylist()
            clean = batch.drop([feed.SNAP_COL])
            for sub, cov, sub_hi in _slice_batch(clean, snaps, pos, hi, unit_cfg.read_unit_max_rows):
                delivery.buffer(dest_id, sub, cov, epoch=epochs[dest_id], hi=sub_hi)
        else:
            # Legacy extension read: no per-row attribution — whole-unit
            # entry (chunk-equivalent semantics).
            delivery.buffer(dest_id, batch, hi, epoch=epochs[dest_id])
        # Mark routed only when rows actually landed (or the member was
        # soundly advanced): an empty post-mask batch must not suppress the
        # advance path (review F4).
        routed_ids.add(dest_id)
    for d in members:
        if d not in routed_ids:
            delivery.advance_position(d, hi, epoch=epochs[d])


def _poll_cycle(
    src_table,
    delivery,
    dest_pool,
    router,
    cfg,
    assigned_ids,
    rv_to_dest,
    key_columns,
    mode,
    *,
    # Required, no default: the implicit fallback would be the unprojected
    # SELECT-* read that a VARIANT source column stalls. Pass None only to
    # deliberately read unprojected (quote-bearing column names).
    source_columns,
    read_ids=None,
    lifecycle_states=None,
    dest_configs=None,
    feed_reader=None,
    read_pool=None,
):
    # Local boolean so the existing branch sites stay terse. Threading mode
    # (not full_cdc) through the call signature avoids reconstructing the
    # original config value from a derived bool later (status payload).
    full_cdc = mode == "full_cdc"
    if full_cdc and feed_reader is not None:
        # Startup refuses this combination (run() raises ConfigError); guard
        # here too so a future caller can't silently get changefeed reads
        # while a feed reader exists.
        raise ConfigError("feed_reader requires append_only mode; full_cdc has no feed implementation")
    # Lifecycle gating: only ACTIVE destinations read; None (tests, and any
    # future caller without lifecycle) means everything assigned reads.
    if read_ids is None:
        read_ids = assigned_ids
    if lifecycle_states is None:
        lifecycle_states = {}
    # Production passes the per-cycle registry snapshot's config view; the
    # per-call cfg derivation is the test-harness convenience (deriving
    # from an argument passed each call is not the stale-STARTUP-capture
    # defect the registry exists to prevent).
    if dest_configs is None:
        dest_configs = {d.id: d for d in cfg.destinations}
    """One poll cycle: read CDC from each position group into buffers,
    advance in-memory positions, evaluate flush triggers.

    Writes happen on the delivery manager's worker pool at flush cadence —
    this thread only reads, routes (Phase 1 included), and buffers. See
    viaduck/delivery.py and tla/Viaduck.tla (BufferRead / FlushStart).

    `read_pool`: the parallel unit-read executor (None in tests — reads
    run inline on the poll thread).
    """
    metrics.polls_total.inc()
    health.record_poll()

    cycle_t0 = time.monotonic()
    cycle_rows_read = 0
    cycle_units = 0

    # One combined MIN/MAX statement: the postgres scanner does no aggregate
    # pushdown, so separate earliest/current queries would each pull the
    # full snapshot-id column per cycle.
    bounds = source.snapshot_bounds(src_table)
    earliest_id, current_id = bounds if bounds is not None else (None, None)
    if current_id is not None:
        metrics.source_snapshot_id.set(current_id)

        if delivery.should_pause_all_reads():
            # Every destination's queue is at its per-destination cap —
            # no read anywhere would help. Flushes in flight will relieve.
            _log_watermark_paused("all destinations at buffer cap")
        else:
            _log_watermark_cleared()
            # Retention-edge clamp BEFORE the plan snapshot, so a clamped
            # destination's read starts from the clamped cursor this cycle.
            # A destination whose clamp FAILED sits out this cycle's reads:
            # reading from its still-expired cursor would raise, and the
            # run loop treats poll-cycle errors as fatal.
            still_expired = _clamp_expired_cursors(delivery, read_ids, earliest_id)
            if still_expired:
                read_ids = [d for d in read_ids if d not in still_expired]
            plan = delivery.read_plan()
            positions = {d: pos for d, (pos, _epoch) in plan.items()}
            epochs = {d: epoch for d, (_pos, epoch) in plan.items()}
            # Only lifecycle-ACTIVE destinations participate in reads;
            # draining destinations keep flushing what they have, paused/
            # retired are fully inert (assigned_ids still drives the lag/
            # status exports below so a draining destination stays visible).
            #
            # Read loop (log-consumer-proposal.md §6.3): destinations whose
            # positions fit one read-unit span share ONE read (cluster
            # fan-in, masked per destination by its own position); clusters
            # read in parallel on the read pool and their results are
            # applied HERE, on the poll thread, as each completes (buffer-
            # writer discipline unchanged). The retired scheduler — cursor
            # groups, rotation, cycle time budget, per-group chunk cap,
            # skip-scan — redistributed a fixed serial read supply; the
            # feed's ~ms catalog reads make the supply parallel, so the
            # machinery is deleted, not retuned. There is deliberately no
            # cycle time budget: every unit is row/byte-bounded and every
            # cluster dispatches every cycle (the barrier), so nothing
            # starves.
            clusters = _position_clusters(
                # Span-merge clustering needs the per-row mask — feed only.
                # Legacy/full_cdc reads can't mask (no per-row snapshot), so
                # they cluster by exact position (span_cap=0) or they'd
                # re-deliver (lo, pos_member] on every read.
                # Cluster over READABLE (not-at-cap) members' positions ONLY:
                # a wedged member's frozen position must not become a
                # cluster's lo — the unit would re-plan the same span every
                # cycle and pin every peer within span-cap indefinitely
                # (review F1; §5 "pace never bounded by peers").
                {d: p for d, p in positions.items() if not delivery.should_pause_reads_for(d)},
                read_ids,
                span_cap=cfg.poll.read_unit_max_span if (feed_reader is not None and not full_cdc) else 0,
            )
            metrics.read_clusters.set(len(clusters))
            futures: dict = {}
            for lo, members in clusters:
                if lo >= current_id:
                    continue  # this cluster has already read through head
                # The barrier below means no destination has two reads
                # outstanding across cycles (tla/ViaduckReads.tla witnesses
                # why that matters the day the barrier goes away).
                readable = members
                # Plan on the POLL thread (catalog SQL on the FeedReader's
                # psycopg connection is single-threaded; the pool never
                # plans). Planning failures are contained per cluster like
                # read failures — a catalog blip skips one cluster for one
                # cycle, never crashes the instance (review: M4-SWE #2).
                try:
                    if feed_reader is not None and not full_cdc:
                        hi = feed_reader.plan_unit(
                            src_table,
                            lo,
                            current_id,
                            max_rows=cfg.poll.read_unit_max_rows,
                            max_bytes=cfg.poll.read_unit_max_bytes,
                            max_span=cfg.poll.read_unit_max_span,
                        )
                        planned = feed_reader.plan_read(
                            src_table,
                            lo,
                            hi,
                            filter_expr=router.build_filter_expr([dest_configs[d].routing_value for d in readable]),
                            columns=source_columns,
                            with_snapshot=True,
                        )
                    else:
                        # Legacy fallback unit: the retired scheduler read <= 4 x
                        # 120 = 480 snapshots per group per cycle. The legacy unit
                        # keeps that pacing (one 480-snapshot extension read is
                        # cheaper than four 120s) so flag-off catch-up behavior is
                        # unchanged regardless of read_unit_max_span.
                        legacy_span = min(cfg.poll.read_unit_max_span, 480)
                        hi = min(lo + legacy_span, current_id)
                        planned = None
                except Exception:
                    log.exception("CDC unit planning failed for cluster at %d; skipping it this cycle", lo)
                    metrics.errors_total.labels(type="cdc_read", destination="").inc()
                    continue
                if feed_reader is not None and not full_cdc and planned is None:
                    # Empty unit: no read needed — advance positions directly.
                    for d in readable:
                        delivery.advance_position(d, hi, epoch=epochs[d])
                    continue
                future = (
                    read_pool.submit(
                        _read_unit,
                        src_table=src_table,
                        feed_reader=feed_reader,
                        planned=planned,
                        lo=lo,
                        hi=hi,
                        members=readable,
                        router=router,
                        dest_configs=dest_configs,
                        full_cdc=full_cdc,
                        source_columns=source_columns,
                    )
                    if read_pool is not None
                    else _InlineFuture(
                        # Bind loop variables NOW (default args): a bare
                        # closure over loop variables would see the final
                        # iteration's values in every thunk.
                        lambda lo=lo, readable=readable, hi=hi, planned=planned: _read_unit(
                            src_table=src_table,
                            read_conn=src_table._catalog.connection,
                            feed_reader=feed_reader,
                            planned=planned,
                            lo=lo,
                            hi=hi,
                            members=readable,
                            router=router,
                            dest_configs=dest_configs,
                            full_cdc=full_cdc,
                            source_columns=source_columns,
                        )
                    )
                )
                futures[future] = (lo, hi, readable)
            metrics.read_pool_inflight.set(len(futures))
            # Barrier per cycle: read results are applied HERE on the poll
            # thread as each unit COMPLETES (as_completed — a slow lagging
            # unit never head-of-line-blocks a completed head cluster's
            # apply + flush). The heartbeat keeps the liveness budget fed
            # while the poll thread waits on pool I/O.
            _hb = _start_progress_heartbeat(
                label=f"CDC read barrier ({len(futures)} units)",
                pre_progress_label="reading",
            )
            remaining_units = 0
            try:
                # Completion order under a real pool (as_completed);
                # dispatch order on the inline (test) path.
                if read_pool is not None:
                    from concurrent.futures import as_completed

                    # Overall barrier budget: per-unit timeout x unit count —
                    # a wedged S3 GET must fail contained, never hang the
                    # poll thread behind a heartbeat-green liveness probe
                    # for hours (review O2).
                    barrier_timeout = cfg.poll.read_unit_timeout_seconds * max(1, len(futures))
                    pending = as_completed(futures, timeout=barrier_timeout)
                else:
                    pending = list(futures)
                for future in pending:
                    # Failure containment per unit (the retired per-group
                    # containment's successor): a read/route/apply failure
                    # skips THIS cluster this cycle — nothing fleet-wide.
                    # Source-connection death still exits via snapshot_bounds
                    # at the top of the cycle. The as_completed iterator's
                    # own TimeoutError (overall barrier budget) propagates out
                    # of the loop into the except below.
                    lo, unit_hi, members = futures[future]
                    try:
                        rows, hi = (
                            future.result(timeout=cfg.poll.read_unit_timeout_seconds)
                            if read_pool is not None
                            else future.result()
                        )
                    except TimeoutError:
                        log.exception("CDC unit at %d exceeded read_unit_timeout_seconds; skipping it this cycle", lo)
                        metrics.errors_total.labels(type="cdc_read", destination="").inc()
                        continue
                    except Exception as exc:
                        # Plan/execute skew (a listed file vanished between
                        # plan and GET): re-plan once on the poll thread and
                        # retry INLINE (this is feed.py's read() recovery,
                        # now at the loop level). Second failure skips the
                        # cluster for the cycle.
                        if feed_reader is not None and feed._is_missing_file_error(exc):
                            log.warning("CDC unit at %d hit a vanished file; re-planning once inline", lo)
                            metrics.cdc_feed_replans_total.inc()
                            try:
                                planned = feed_reader.plan_read(
                                    src_table,
                                    lo,
                                    unit_hi,  # retry exactly (lo, unit_hi] — never widen a budgeted unit
                                    filter_expr=router.build_filter_expr(
                                        [dest_configs[d].routing_value for d in members]
                                    ),
                                    columns=source_columns,
                                    with_snapshot=True,
                                )
                                rows = (
                                    feed.execute_read(src_table._catalog.connection, planned)
                                    if planned is not None
                                    else pa.table({})
                                )
                                if rows is None:
                                    rows = pa.table({})
                                hi = unit_hi
                            except Exception:
                                log.exception("CDC unit re-plan failed for cluster at %d", lo)
                                metrics.errors_total.labels(type="cdc_read", destination="").inc()
                                continue
                        else:
                            log.exception("CDC unit read failed for cluster at %d; skipping it this cycle", lo)
                            metrics.errors_total.labels(type="cdc_read", destination="").inc()
                            continue
                    try:
                        cycle_rows_read += rows.num_rows
                        cycle_units += 1
                        metrics.cdc_batch_rows.observe(rows.num_rows)
                        _apply_unit(
                            delivery,
                            rows,
                            hi,
                            members=members,
                            positions=positions,
                            epochs=epochs,
                            router=router,
                            rv_to_dest=rv_to_dest,
                            full_cdc=full_cdc,
                            key_columns=key_columns,
                            unit_cfg=cfg.poll,
                            routing_field=cfg.routing.field,
                            dest_configs=dest_configs,
                        )
                        delivery.maybe_flush()
                    except Exception:
                        # route/apply failures (Phase 1, router) are
                        # contained per cluster exactly like read failures.
                        log.exception(
                            "CDC unit apply failed for cluster at %d; skipping it this cycle",
                            lo,
                        )
                        metrics.errors_total.labels(type="routing", destination="").inc()
            except TimeoutError:
                # The overall barrier budget expired with units outstanding:
                # count and skip them (they retry next cycle from their
                # frozen positions — at-least-once holds).
                log.exception("CDC read barrier timed out with %d unit(s) outstanding", remaining_units)
                metrics.errors_total.labels(type="cdc_read", destination="").inc(remaining_units)
            finally:
                _hb.set()
                metrics.read_pool_inflight.set(0)

    # Evaluate flush triggers (FlushStart) — also persists position-only

    # advances for idle destinations on the flush cadence.
    flushes_submitted = delivery.maybe_flush()

    # Status + lag from the delivery manager's snapshot (authoritative
    # in-memory view; PG is the durability layer).
    snap_now = current_id if current_id is not None else 0
    dest_statuses = []
    delivery_snapshot = delivery.status_snapshot()
    # status_snapshot() is active-filtered and assigned_ids is the
    # caller's per-cycle membership (delivery.active_ids() in production).
    # The two are computed a few lines apart, so the intersection keeps a
    # reconciler mutation landing between them from KeyError-ing the
    # cycle (which the run loop treats as fatal).
    status_ids = [d for d in assigned_ids if d in delivery_snapshot]
    _export_dest_time_lag(src_table, delivery_snapshot, status_ids, snap_now)
    for did in status_ids:
        d = delivery_snapshot[did]
        lag = max(snap_now - d.flushed_snapshot, 0)
        metrics.dest_lag_snapshots.labels(destination=did).set(lag)

        st = _derive_dest_status(d, snap_now, lifecycle_states.get(did, "active"))

        dest_statuses.append(
            DestStatus(
                id=did,
                routing_value=dest_configs[did].routing_value,
                snapshot=d.flushed_snapshot,
                lag=lag,
                rows_replicated=d.rows_replicated,
                status=st,
                last_error=d.last_error,
                buffer_rows=d.buffer_rows,
                buffer_age_s=round(d.buffer_age_s, 1),
                applied_inserts=d.applied_inserts,
                applied_updates=d.applied_updates,
                applied_deletes=d.applied_deletes,
                buffered_rows_total=d.buffered_rows_total,
                lag_seconds=round(d.lag_seconds, 1),
            )
        )

    status.update(
        source_table=f"{cfg.source.name}.{cfg.source.table}",
        source_snapshot=snap_now,
        mode=mode,
        poll_interval=cfg.poll.interval_seconds,
        flush_interval=cfg.delivery.flush_interval_seconds,
        delivery_config={
            "workers": cfg.delivery.workers,
            "flush_max_rows": cfg.delivery.flush_max_rows,
            "flush_max_bytes": cfg.delivery.flush_max_bytes,
            "buffer_total_max_bytes": cfg.delivery.buffer_total_max_bytes,
            "buffer_max_bytes_per_destination": cfg.delivery.buffer_max_bytes_per_destination,
            "pool_max_open": cfg.delivery.pool_max_open,
        },
        destinations=dest_statuses,
        pool_open=dest_pool.size,
        pool_max=dest_pool.max_open,
    )

    # Per-cycle summary log. Quiet for empty cycles (no work) so steady-state
    # idleness doesn't flood the log; verbose when there's work to report.
    cycle_secs = time.monotonic() - cycle_t0
    if cycle_units > 0 or cycle_rows_read > 0 or flushes_submitted > 0:
        max_lag = max(((snap_now - delivery_snapshot[did].flushed_snapshot) for did in status_ids), default=0)
        buffered_rows = sum(delivery_snapshot[did].buffer_rows for did in status_ids)
        log.info(
            "Poll cycle: snapshot=%d, units=%d, cdc_rows_read=%d, buffered_rows=%d, "
            "flushes_submitted=%d, max_lag=%d, duration=%.2fs",
            snap_now,
            cycle_units,
            cycle_rows_read,
            buffered_rows,
            flushes_submitted,
            max_lag,
            cycle_secs,
        )

    # Temporary leak diagnostic: log DuckDB's own memory tracker + OS RSS every
    # poll cycle. duckdb_memory() reports process-wide DuckDB buffer-manager
    # tags but NOT extension `malloc` / `std::vector` allocations, so "flat
    # tracker" does not prove the leak is outside DuckDB — it could still be
    # inside the DuckLake extension. Grep-anchor `[MEMTRACE]`. REMOVE once the
    # OOM leak is diagnosed.
    _log_memory_stats(src_table.catalog.connection)


_MEM_STATS_FAILURES = 0
_MEM_STATS_LOG_EVERY_NTH_FAILURE = 100
# Poll cycles run every ~2s; MEMTRACE logs at INFO were drowning the log
# under sustained buffer-watermark stall. Throttle to at most one entry per
# _MEMTRACE_MIN_INTERVAL_S seconds. The line is still useful to correlate
# with events, just not at 2s cadence.
_MEMTRACE_MIN_INTERVAL_S = 300.0
_last_memtrace_at: float = 0.0

# Buffer-watermark stall tracking. Under sustained stall the poll thread
# would emit the WARN line every ~2s, drowning the log. Log the transition
# edges (entering / clearing) and a "still stalled" heartbeat at a low
# cadence in between.
_WATERMARK_HEARTBEAT_INTERVAL_S = 60.0
_watermark_stall_start: float = 0.0
_last_watermark_warn_at: float = 0.0


def log_startup_memory() -> None:
    """One-shot at process start so on-call can slice memory-since-boot."""
    try:
        log.info("[MEMTRACE] startup pid=%d rss=%.2fGiB", os.getpid(), _read_rss_gib())
    except Exception:
        log.warning("[MEMTRACE] startup log failed", exc_info=True)


def _read_rss_gib() -> float:
    """Process RSS in GiB from /proc; raises on platforms without procfs.

    A missing VmRSS line raises too: on Linux with the watermark armed that
    is abnormal, and returning 0.0 would silently disable the recycle while
    looking like a successful read.
    """
    for line in open("/proc/self/status"):
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024 / 1024
    raise RuntimeError("VmRSS not present in /proc/self/status")


def _cgroup_memory_limit_gib() -> float:
    """Container memory limit in GiB from the cgroup (v2 then v1); 0 when
    unreadable or unlimited ("max" / the v1 no-limit sentinel)."""
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            raw = open(path).read().strip()
        except OSError:
            continue
        if raw == "max":
            return 0.0
        limit = int(raw) / 1024**3
        # cgroup v1 reports ~8 EiB when unlimited; anything implausibly
        # large is "no limit".
        return limit if limit < 4096 else 0.0
    return 0.0


def resolve_recycle_watermark(cfg_memory) -> float:
    """Resolve the self-recycle RSS watermark in GiB; 0 = disabled.

    Logged once at startup so the effective threshold (and why it is what it
    is) is always on the record: absolute knob wins, else fraction x cgroup
    limit, else disabled when no limit is readable (bare-metal/dev runs).
    """
    if not cfg_memory.self_recycle_enabled:
        log.info("Self-recycle disabled by config")
        return 0.0
    if cfg_memory.self_recycle_rss_gib > 0:
        log.info("Self-recycle watermark: %.1fGiB (absolute)", cfg_memory.self_recycle_rss_gib)
        return cfg_memory.self_recycle_rss_gib
    limit = _cgroup_memory_limit_gib()
    if limit <= 0:
        log.info("Self-recycle disabled: no cgroup memory limit readable and no absolute watermark set")
        return 0.0
    watermark = limit * cfg_memory.self_recycle_rss_fraction
    log.info(
        "Self-recycle watermark: %.1fGiB (%.0f%% of %.1fGiB cgroup limit)",
        watermark,
        cfg_memory.self_recycle_rss_fraction * 100,
        limit,
    )
    return watermark


# RSS read failures never trip a restart, but a PERSISTENT failure with the
# watermark armed means the recycle is silently dark (the leak then runs to
# OOM unpreempted) — worth a rate-limited WARN. Same count-and-log-every-Nth
# shape as _MEM_STATS_FAILURES; at the ~5s cycle interval, every 360th ≈
# one line per half hour.
_RSS_READ_FAILURES = 0
_RSS_READ_LOG_EVERY_NTH_FAILURE = 360

# Drain budget for the self-recycle path. Must stay comfortably under the
# liveness reap window (~600s: 300s poll-age staleness + 10x30s probe
# failures) so a healthy long drain is never killed mid-flush.
_RECYCLE_DRAIN_TIMEOUT_S = 300.0


def _should_self_recycle(watermark_gib: float, started_at: float, min_uptime_s: float) -> bool:
    """One cheap check per poll cycle. Failures never trip a restart."""
    global _RSS_READ_FAILURES
    if watermark_gib <= 0 or time.monotonic() - started_at < min_uptime_s:
        return False
    try:
        rss = _read_rss_gib()
    except Exception:
        _RSS_READ_FAILURES += 1
        if _RSS_READ_FAILURES == 1 or _RSS_READ_FAILURES % _RSS_READ_LOG_EVERY_NTH_FAILURE == 0:
            log.warning(
                "[SELF-RECYCLE] RSS read failed (occurrence #%d) — watermark armed but not checking",
                _RSS_READ_FAILURES,
                exc_info=True,
            )
        return False
    if rss < watermark_gib:
        return False
    log.warning(
        "[SELF-RECYCLE] rss=%.1fGiB >= watermark %.1fGiB; draining and exiting 0 for a clean restart. "
        "Whatever the drain flushes lands tight; anything past the drain deadline re-reads from "
        "persisted cursors — still strictly less rewind than the mid-flight OOM this preempts",
        rss,
        watermark_gib,
    )
    return True


def _log_watermark_paused(kind: str) -> None:
    """Rate-limited buffer-watermark WARN. Fires once when the stall starts,
    then a heartbeat every `_WATERMARK_HEARTBEAT_INTERVAL_S` for as long as
    it persists. Callers should invoke `_log_watermark_cleared` when reads
    resume.
    """
    global _watermark_stall_start, _last_watermark_warn_at
    now = time.monotonic()
    if _watermark_stall_start == 0.0:
        _watermark_stall_start = now
        _last_watermark_warn_at = now
        log.warning("Buffer watermark exceeded (%s); pausing CDC reads", kind)
        return
    if now - _last_watermark_warn_at >= _WATERMARK_HEARTBEAT_INTERVAL_S:
        _last_watermark_warn_at = now
        stall_s = int(now - _watermark_stall_start)
        log.warning("Buffer watermark still exceeded (%s), paused for %ds", kind, stall_s)


def _log_watermark_cleared() -> None:
    """Emit an INFO edge when the stall ends. Cheap to call every cycle."""
    global _watermark_stall_start, _last_watermark_warn_at
    if _watermark_stall_start == 0.0:
        return
    stall_s = int(time.monotonic() - _watermark_stall_start)
    log.info("Buffer watermark cleared after %ds", stall_s)
    _watermark_stall_start = 0.0
    _last_watermark_warn_at = 0.0


def _log_memory_stats(conn) -> None:
    """Log OS RSS, DuckDB per-tag memory tracker, and Python object count in one line.

    Best-effort; failures are rate-limited (log every 100th) to avoid a log
    storm if this consistently breaks. Must never kill the CDC loop.
    """
    global _MEM_STATS_FAILURES, _last_memtrace_at
    now = time.monotonic()
    if now - _last_memtrace_at < _MEMTRACE_MIN_INTERVAL_S:
        return
    _last_memtrace_at = now
    try:
        rss_kb = vms_kb = 0
        for line in open("/proc/self/status"):
            if line.startswith("VmRSS:"):
                rss_kb = int(line.split()[1])
            elif line.startswith("VmSize:"):
                vms_kb = int(line.split()[1])
        rows = conn.execute(
            "SELECT tag, memory_usage_bytes, temporary_storage_bytes "
            "FROM duckdb_memory() ORDER BY memory_usage_bytes DESC"
        ).fetchall()
        total_used = sum(r[1] for r in rows)
        total_temp = sum(r[2] for r in rows)
        top = ", ".join(f"{tag}={used / 1024 / 1024:.0f}MB" for tag, used, _ in rows[:5])
        py_objs = len(gc.get_objects())
        log.info(
            "[MEMTRACE] rss=%.2fGiB vms=%.2fGiB duckdb_used=%.2fGiB duckdb_temp=%.2fGiB py_objs=%d top5={%s}",
            rss_kb / 1024 / 1024,
            vms_kb / 1024 / 1024,
            total_used / 1024 / 1024 / 1024,
            total_temp / 1024 / 1024 / 1024,
            py_objs,
            top,
        )
    except Exception:
        _MEM_STATS_FAILURES += 1
        if _MEM_STATS_FAILURES == 1 or _MEM_STATS_FAILURES % _MEM_STATS_LOG_EVERY_NTH_FAILURE == 0:
            log.warning("[MEMTRACE] logging failed (occurrence #%d)", _MEM_STATS_FAILURES, exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="Viaduck — DuckLake to DuckLake CDC replication")
    parser.add_argument("--config", "-c", default="viaduck.yaml", help="Path to config YAML file")
    args = parser.parse_args()

    logging_config.setup()
    try:
        from viaduck._version import __version__
    except ImportError:
        __version__ = "unknown"
    log.info("viaduck %s starting", __version__)
    log_startup_memory()
    cfg = config.load(args.config)
    run(cfg)


if __name__ == "__main__":
    main()
