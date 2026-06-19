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
import logging
import signal
import threading
import time

import pyarrow as pa
import pyarrow.compute as pc

from viaduck import config, logging_config, metrics, source
from viaduck.apply import _require_non_null_rowids
from viaduck.arrowutil import full_bool, row_indices
from viaduck.delivery import DeliveryManager
from viaduck.destination import DestinationPool
from viaduck.router import Router, RoutingError
from viaduck.server import DestStatus, health, status
from viaduck.state import StateManager

log = logging.getLogger(__name__)


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


def _group_by_cursor(
    cursors: dict[str, int],
    all_dest_ids: list[str],
) -> dict[int, list[str]]:
    """Group destination IDs by their last_snapshot_id.

    Returns a dict mapping snapshot_id -> [destination_ids at that snapshot].
    Destinations not in cursors are treated as snapshot_id=0.
    """
    groups: dict[int, list[str]] = {}
    for did in all_dest_ids:
        snap = cursors.get(did, 0)
        groups.setdefault(snap, []).append(did)
    return groups


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


def _derive_dest_status(d, snap_now: int) -> str:
    """Operational status for a destination, from its delivery snapshot.

    Raw flush lag is the wrong signal here: between flushes the persisted
    cursor is always behind the source — that's the buffering design
    working, not a problem. "lagging" means READS are behind (the data
    hasn't even been seen); read-current with data awaiting flush is
    "buffering".
    """
    if d.last_error:
        return "error"
    if d.flushing:
        return "flushing"
    if snap_now > d.position_snapshot:
        return "lagging"
    if d.buffer_rows > 0 or d.position_snapshot > d.flushed_snapshot:
        return "buffering"
    return "healthy"


def _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, assigned_ids):
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

    # Connect to source
    src_catalog = source.connect(cfg.source)
    src_table = source.load_table(src_catalog, cfg.source.table)

    # Initialize state and destinations. Cursor state lives on plain
    # Postgres (NOT a DuckLake table): a cursor advance must not create
    # catalog snapshots, or idle destinations generate CDC work forever.
    state_mgr = StateManager(cfg.state.resolve_postgres_uri(cfg.source), cfg.instance.id, cfg.state)
    dest_pool = DestinationPool(cfg, max_open=cfg.delivery.pool_max_open)
    router = Router(cfg.routing)

    # Cache source schema for destination table creation.
    # `Table.schema` is a property in pyducklake — do not call it.
    dest_pool.set_source_schema(src_table.schema)

    # Build routing_value -> dest_id mapping
    rv_to_dest: dict[str, str] = {d.routing_value: d.id for d in cfg.destinations}

    assigned_ids = cfg.assigned_destination_ids()
    initial_snapshot_id = _initial_snapshot_id(cfg.routing.seed_mode, src_table)
    state_mgr.initialize_destinations(assigned_ids, initial_snapshot_id=initial_snapshot_id)

    # Seed new destinations from source scan (avoids CDC replay from snapshot 0)
    if cfg.routing.seed_mode == "scan":
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, assigned_ids)

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
    )

    log.info(
        "Viaduck started: source=%s.%s, routing_field=%s, mode=%s, destinations=%d, instance=%s",
        cfg.source.name,
        cfg.source.table,
        cfg.routing.field,
        mode,
        len(assigned_ids),
        cfg.instance.id,
    )

    shutdown = False

    def _signal_handler(signum, frame):
        nonlocal shutdown
        log.info("Received signal %s, shutting down", signal.Signals(signum).name)
        shutdown = True

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    while not shutdown:
        try:
            _poll_cycle(src_table, delivery, dest_pool, router, cfg, assigned_ids, rv_to_dest, key_columns, mode)
        except Exception:
            log.exception("Fatal error in poll cycle")
            break

        if not shutdown:
            # Chunked sleep so SIGTERM is honored within ~1s rather than
            # waiting up to `interval_seconds`. With long poll intervals (e.g.
            # 300s) and k8s `terminationGracePeriodSeconds` (default 30s), an
            # uninterruptible sleep would let kubelet SIGKILL mid-poll.
            _interruptible_sleep(cfg.poll.interval_seconds, lambda: shutdown)

    # Graceful shutdown: flush everything buffered (the spec's
    # shutdown-trigger FlushStart), wait for workers, then close.
    log.info("Shutting down...")
    delivery.drain()
    dest_pool.close_all()
    state_mgr.close()
    try:
        src_catalog.close()
    except Exception:
        pass
    # Tell SSE handlers to exit before calling http.shutdown(), otherwise
    # an open /ui/sse client would block shutdown() forever.
    server.signal_shutdown()
    http.shutdown()
    log.info("Shutdown complete")


def _poll_cycle(src_table, delivery, dest_pool, router, cfg, assigned_ids, rv_to_dest, key_columns, mode):
    # Local boolean so the existing branch sites stay terse. Threading mode
    # (not full_cdc) through the call signature avoids reconstructing the
    # original config value from a derived bool later (status payload).
    full_cdc = mode == "full_cdc"
    """One poll cycle: read CDC from each position group into buffers,
    advance in-memory positions, evaluate flush triggers.

    Writes happen on the delivery manager's worker pool at flush cadence —
    this thread only reads, routes (Phase 1 included), and buffers. See
    viaduck/delivery.py and tla/Viaduck.tla (BufferRead / FlushStart).
    """
    metrics.polls_total.inc()
    health.record_poll()

    cycle_t0 = time.monotonic()
    cycle_rows_read = 0
    cycle_groups_processed = 0

    current_id = source.current_snapshot_id(src_table)
    if current_id is not None:
        metrics.source_snapshot_id.set(current_id)

        if delivery.should_pause_reads():
            # Global buffer watermark exceeded and every buffering
            # destination is already in flight — reading more only grows
            # memory. Skip reads; flushes in flight will relieve it.
            log.warning("Buffer watermark exceeded with all flushes in flight; pausing CDC reads this cycle")
        else:
            plan = delivery.read_plan()
            positions = {d: pos for d, (pos, _epoch) in plan.items()}
            epochs = {d: epoch for d, (_pos, epoch) in plan.items()}
            groups = _group_by_cursor(positions, assigned_ids)

            for start_snap, dest_ids in groups.items():
                if start_snap >= current_id:
                    continue  # already read through the current snapshot
                cycle_groups_processed += 1

                # Map dest_ids to their routing values for filter/split
                routing_values = [cfg.destination_by_id(d).routing_value for d in dest_ids]
                filter_expr = router.build_filter_expr(routing_values)

                # Read CDC in snapshot chunks to bound memory use.
                # Each chunk is read, routed, and buffered before the next is fetched.
                chunk_size = cfg.poll.cdc_chunk_snapshots
                chunk_start = start_snap
                routing_error = False
                while chunk_start < current_id:
                    if delivery.should_pause_reads():
                        log.warning("Buffer watermark exceeded mid-chunk; pausing CDC reads")
                        break
                    chunk_end = min(chunk_start + chunk_size, current_id)
                    snap_range = f"{chunk_start}→{chunk_end}"
                    chunk_t0 = time.monotonic()
                    state = {"rows": 0}
                    _hb = _start_progress_heartbeat(
                        label=f"CDC read {snap_range}",
                        pre_progress_label="reading",
                        state=state,
                    )
                    try:
                        if full_cdc:
                            raw_data = source.read_cdc_changes(
                                src_table, after_snapshot=chunk_start, end_snapshot=chunk_end, filter_expr=filter_expr
                            )
                        else:
                            raw_data = source.read_cdc(
                                src_table, after_snapshot=chunk_start, end_snapshot=chunk_end, filter_expr=filter_expr
                            )

                        read_elapsed = time.monotonic() - chunk_t0
                        state["rows"] = raw_data.num_rows

                        try:
                            metrics.cdc_batch_rows.observe(raw_data.num_rows)
                        except Exception:
                            log.warning("Failed to record CDC batch size metric")

                        cycle_rows_read += raw_data.num_rows

                        if raw_data.num_rows == 0:
                            for did in dest_ids:
                                delivery.advance_position(did, chunk_end, epoch=epochs[did])
                            log.info(
                                "CDC chunk %s: 0 rows in %.1fs, %d snapshots remaining",
                                snap_range,
                                read_elapsed,
                                current_id - chunk_end,
                            )
                            delivery.maybe_flush()
                            chunk_start = chunk_end
                            continue

                        # Phase 1: Resolve preimages (full CDC only, before routing)
                        if full_cdc:
                            try:
                                raw_data = _resolve_preimages(raw_data, cfg.routing.field, key_columns)
                            except RoutingError:
                                log.exception("Preimage resolution failed — key column may be missing from CDC data")
                                metrics.errors_total.labels(type="routing", destination="").inc()
                                routing_error = True
                                break

                        # Route in a single pass (split + count unrouted)
                        try:
                            routed, unrouted = router.split_and_count(raw_data, routing_values)
                        except RoutingError:
                            log.exception("Routing failed — routing field may be missing from source schema")
                            metrics.errors_total.labels(type="routing", destination="").inc()
                            routing_error = True
                            break

                        if unrouted > 0:
                            metrics.unrouted_rows_total.inc(unrouted)

                        # Buffer routed batches (BufferRead); destinations with no
                        # routed rows just advance their read position in memory.
                        routed_dest_ids = set()
                        for routing_val, batch in routed.items():
                            dest_id = rv_to_dest[routing_val]
                            routed_dest_ids.add(dest_id)
                            if batch.num_rows > 0:
                                delivery.buffer(dest_id, batch, chunk_end, epoch=epochs[dest_id])
                        for did in dest_ids:
                            if did not in routed_dest_ids:
                                delivery.advance_position(did, chunk_end, epoch=epochs[did])

                        log.info(
                            "CDC chunk %s: %d rows in %.1fs (%.0f rows/s), %d snapshots remaining",
                            snap_range,
                            raw_data.num_rows,
                            read_elapsed,
                            raw_data.num_rows / read_elapsed if read_elapsed > 0 else 0,
                            current_id - chunk_end,
                        )
                        delivery.maybe_flush()
                        chunk_start = chunk_end

                    finally:
                        _hb.set()

                if routing_error:
                    break

    # Evaluate flush triggers (FlushStart) — also persists position-only
    # advances for idle destinations on the flush cadence.
    flushes_submitted = delivery.maybe_flush()

    # Status + lag from the delivery manager's snapshot (authoritative
    # in-memory view; PG is the durability layer).
    snap_now = current_id if current_id is not None else 0
    dest_statuses = []
    delivery_snapshot = delivery.status_snapshot()
    for did in assigned_ids:
        d = delivery_snapshot[did]
        lag = max(snap_now - d.flushed_snapshot, 0)
        metrics.dest_lag_snapshots.labels(destination=did).set(lag)

        st = _derive_dest_status(d, snap_now)

        dest_statuses.append(
            DestStatus(
                id=did,
                routing_value=cfg.destination_by_id(did).routing_value,
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
            "pool_max_open": cfg.delivery.pool_max_open,
        },
        destinations=dest_statuses,
        pool_open=dest_pool.size,
        pool_max=dest_pool.max_open,
    )

    # Per-cycle summary log. Quiet for empty cycles (no work) so steady-state
    # idleness doesn't flood the log; verbose when there's work to report.
    cycle_secs = time.monotonic() - cycle_t0
    if cycle_groups_processed > 0 or cycle_rows_read > 0 or flushes_submitted > 0:
        max_lag = max((snap_now - delivery_snapshot[did].flushed_snapshot) for did in assigned_ids)
        buffered_rows = sum(delivery_snapshot[did].buffer_rows for did in assigned_ids)
        log.info(
            "Poll cycle: snapshot=%d, groups=%d, cdc_rows_read=%d, buffered_rows=%d, "
            "flushes_submitted=%d, max_lag=%d, duration=%.2fs",
            snap_now,
            cycle_groups_processed,
            cycle_rows_read,
            buffered_rows,
            flushes_submitted,
            max_lag,
            cycle_secs,
        )


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
    cfg = config.load(args.config)
    run(cfg)


if __name__ == "__main__":
    main()
