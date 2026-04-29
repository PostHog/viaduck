"""Entry point and main poll loop for viaduck.

CDC Replication Algorithm
=========================

Viaduck replicates changes from a source DuckLake table to N destination
DuckLake tables using a 3-phase CDC algorithm. The algorithm is eventually
consistent under three assumptions:

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

The algorithm processes CDC batches as unordered sets (not sequences). This is
sound because: (a) each batch covers a closed snapshot range, (b) batches are
always applied in ascending snapshot order via cursor tracking, and (c)
within-batch conflicts are resolved by rowid grouping before application.

Phases:
  1. Preimage Resolution (before routing) — pair update pre/postimages by
     rowid, convert cross-tenant preimages to deletes, drop same-tenant
     preimages.
  2. Conflict Resolution (per-destination, after routing) — cancel
     insert+delete pairs by rowid, drop postimages shadowed by deletes.
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
from viaduck.destination import DestinationPool
from viaduck.router import Router, RoutingError
from viaduck.server import DestStatus, health, status
from viaduck.source import strip_meta
from viaduck.state import StateManager

log = logging.getLogger(__name__)

_WRITE_MAX_RETRIES = 3
_WRITE_BASE_DELAY_S = 1.0


def _start_progress_heartbeat(label: str, interval_s: float = 30.0) -> threading.Event:
    """Start a background heartbeat for a long-running blocking operation.

    While the heartbeat runs, it:
      - logs `<label>: still working (T seconds elapsed)` every `interval_s`,
        so operators see the pod isn't hung;
      - calls `health.record_poll()` on each tick, which touches `_last_poll`
        and keeps liveness green during operations that exceed
        `max_poll_age_s` (default 300s) — e.g. an initial source scan
        against a large catalog.

    Returns a `threading.Event` the caller `.set()`s when the operation
    finishes (use try/finally). The thread is a daemon so it won't block
    process exit even if .set() is missed.
    """
    stop = threading.Event()
    start_t = time.monotonic()

    def _tick() -> None:
        from viaduck.server import health  # local to avoid import cycles

        while not stop.wait(timeout=interval_s):
            elapsed = time.monotonic() - start_t
            log.info("%s: still working (%.0fs elapsed)", label, elapsed)
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


def _write_with_retry(dest_pool, destination_id, operation):
    """Execute a write operation on a destination with exponential backoff.

    operation: callable that takes (catalog, table) and performs the write.
    """
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            catalog, table = dest_pool.get(destination_id)
            return operation(catalog, table)
        except Exception as exc:
            if attempt == _WRITE_MAX_RETRIES - 1:
                raise
            delay = _WRITE_BASE_DELAY_S * (2**attempt)
            log.warning(
                "Write to %s failed (attempt %d/%d, error: %s), retrying in %.1fs",
                destination_id,
                attempt + 1,
                _WRITE_MAX_RETRIES,
                exc,
                delay,
            )
            dest_pool.evict(destination_id)
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Phase 1: Preimage Resolution (before routing)
# ---------------------------------------------------------------------------


def _resolve_preimages(batch: pa.Table, routing_field: str, key_columns: list[str]) -> pa.Table:
    """Resolve update preimages before routing.

    - Pairs preimages with postimages by rowid.
    - Same routing value: drop preimage (same-tenant update, upsert handles it).
    - Different routing value: convert preimage to 'delete' (cross-tenant migration).
    - Orphaned preimages (no matching postimage): convert to 'delete' (defensive).

    Returns the batch with preimages resolved.

    Memory note: converts Arrow columns to Python lists via to_pylist() for
    row-level pairing logic. For very large CDC batches (10M+ rows), this
    materializes ~3 Python lists. If this becomes a bottleneck, reduce
    poll.interval_seconds to keep batch sizes smaller.
    """
    # Validate key columns exist
    for col in key_columns:
        if col not in batch.column_names:
            raise RoutingError(f"Key column {col!r} not found in CDC data. Available: {batch.column_names}")

    ct_col = batch.column("change_type")

    # Check if there are any preimages to resolve
    has_preimages = False
    for val in ct_col.to_pylist():
        if val == "update_preimage":
            has_preimages = True
            break
    if not has_preimages:
        return batch

    # Batch-convert columns to Python lists for performance (avoid per-cell .as_py())
    ct_list = ct_col.to_pylist()
    routing_list = batch.column(routing_field).to_pylist()
    rowid_list = batch.column("rowid").to_pylist()

    # Build rowid -> routing value map for postimages
    postimage_routing: dict = {}
    for i in range(batch.num_rows):
        if ct_list[i] == "update_postimage":
            postimage_routing[rowid_list[i]] = routing_list[i]

    # Process each row
    keep_mask = []
    new_change_types = list(ct_list)

    _SENTINEL = object()

    for i in range(batch.num_rows):
        ct = ct_list[i]
        if ct != "update_preimage":
            keep_mask.append(True)
            continue

        rowid = rowid_list[i]
        pre_routing = routing_list[i]
        post_routing = postimage_routing.get(rowid, _SENTINEL)

        if post_routing is _SENTINEL:
            # Orphaned preimage — no matching postimage. Convert to delete.
            new_change_types[i] = "delete"
            keep_mask.append(True)
            metrics.cdc_orphaned_preimages_total.inc()
            log.debug("Orphaned preimage (rowid=%s) converted to delete", rowid)
        elif pre_routing != post_routing:
            # Routing column mutation detected — this violates the design assumption
            # that the routing column is immutable. Handle defensively by converting
            # the preimage to a delete on the old destination, but log loudly.
            new_change_types[i] = "delete"
            keep_mask.append(True)
            metrics.cdc_routing_mutations_total.inc()
            log.error(
                "Routing column mutation detected (rowid=%s): %s → %s. "
                "The routing column should not be updated. Handling defensively "
                "(delete from old destination, upsert to new), but CDC filter "
                "pushdown may have dropped other preimages. Verify data integrity.",
                rowid,
                pre_routing,
                post_routing,
            )
        else:
            # Same-tenant update — drop preimage (upsert handles it).
            keep_mask.append(False)

    # Rebuild batch with modified change_types
    if all(keep_mask):
        # Only need to update change_type column
        idx = batch.column_names.index("change_type")
        result = batch.set_column(idx, "change_type", pa.array(new_change_types, type=pa.string()))
    else:
        # Filter out dropped preimages and update change_types
        idx = batch.column_names.index("change_type")
        batch = batch.set_column(idx, "change_type", pa.array(new_change_types, type=pa.string()))
        result = batch.filter(pa.array(keep_mask))

    # Post-condition: no preimages should remain after resolution
    remaining_types = result.column("change_type").to_pylist()
    assert "update_preimage" not in remaining_types, (
        f"Bug: {remaining_types.count('update_preimage')} update_preimage rows remain after Phase 1 resolution"
    )
    return result


# ---------------------------------------------------------------------------
# Phase 2: Conflict Resolution (per-destination, after routing)
# ---------------------------------------------------------------------------


def _resolve_conflicts(batch: pa.Table) -> pa.Table:
    """Resolve conflicting changes for the same rowid within a batch.

    Uses rowid (not just key_columns) to identify the same logical row.
    This depends on DuckLake rowids being monotonically increasing and never
    reused. If rowids were recycled, unrelated rows could be incorrectly
    cancelled.

    Rules:
    - insert + delete for same rowid → cancel both (net no-op)
    - update_postimage + delete for same rowid → drop postimage, keep delete
    - insert + update_postimage for same rowid → drop insert, keep postimage
      (postimage carries the newer state; passing both to a single upsert
      yields undefined ordering on the destination join key)
    """
    if batch.num_rows == 0:
        return batch

    ct_list = batch.column("change_type").to_pylist()
    rowid_list = batch.column("rowid").to_pylist()

    # Index rows by (rowid, change_type)
    delete_rowids: dict[int, list[int]] = {}  # rowid -> [row indices]
    insert_rowids: dict[int, list[int]] = {}
    postimage_rowids: dict[int, list[int]] = {}

    for i in range(batch.num_rows):
        ct = ct_list[i]
        rowid = rowid_list[i]
        if ct == "delete":
            delete_rowids.setdefault(rowid, []).append(i)
        elif ct == "insert":
            insert_rowids.setdefault(rowid, []).append(i)
        elif ct == "update_postimage":
            postimage_rowids.setdefault(rowid, []).append(i)

    rows_to_remove: set[int] = set()

    for rowid, ins_indices in insert_rowids.items():
        if rowid in postimage_rowids and rowid not in delete_rowids:
            # insert + postimage same rowid → keep postimage (newer state)
            rows_to_remove.update(ins_indices)
            metrics.cdc_conflicts_resolved_total.inc()

    for rowid, del_indices in delete_rowids.items():
        if rowid in insert_rowids:
            # insert + delete for same rowid → cancel both
            rows_to_remove.update(del_indices)
            rows_to_remove.update(insert_rowids[rowid])
            metrics.cdc_conflicts_resolved_total.inc()
        if rowid in postimage_rowids:
            # update_postimage + delete for same rowid → drop postimage, keep delete
            rows_to_remove.update(postimage_rowids[rowid])

    if not rows_to_remove:
        result = batch
    else:
        keep_mask = [i not in rows_to_remove for i in range(batch.num_rows)]
        result = batch.filter(pa.array(keep_mask))

    # Post-condition: no rowid should appear in both insert and delete sets
    if result.num_rows > 0:
        remaining_ct = result.column("change_type").to_pylist()
        remaining_rid = result.column("rowid").to_pylist()
        insert_rids = {remaining_rid[i] for i in range(len(remaining_ct)) if remaining_ct[i] == "insert"}
        delete_rids = {remaining_rid[i] for i in range(len(remaining_ct)) if remaining_ct[i] == "delete"}
        overlap = insert_rids & delete_rids
        assert not overlap, f"Bug: rowids {overlap} appear in both insert and delete after Phase 2 conflict resolution"

    return result


# ---------------------------------------------------------------------------
# Phase 3: Apply changes (per-destination, atomic)
# ---------------------------------------------------------------------------


def _build_delete_filter(delete_rows: pa.Table, key_columns: list[str]) -> str:
    """Build a SQL filter expression to delete rows matching the given keys.

    Uses pyducklake expressions for proper escaping and NULL handling.
    """
    from pyducklake.expressions import And, EqualTo, In, IsNull, Or

    # Validate key columns exist
    for col in key_columns:
        if col not in delete_rows.column_names:
            raise RoutingError(f"Key column {col!r} not found in delete data. Available: {delete_rows.column_names}")

    if len(key_columns) == 1:
        col = key_columns[0]
        values = delete_rows.column(col).to_pylist()
        non_null = [v for v in values if v is not None]
        has_null = None in values

        if non_null and has_null:
            return Or(In(col, tuple(non_null)), IsNull(col)).to_sql()
        elif has_null:
            return IsNull(col).to_sql()
        else:
            return In(col, tuple(non_null)).to_sql()

    # Multi-column composite key: Or(And(col1=v1, col2=v2), And(col1=v3, col2=v4), ...)
    key_lists = {col: delete_rows.column(col).to_pylist() for col in key_columns}
    row_filters = []
    for i in range(delete_rows.num_rows):
        col_eqs = []
        for col in key_columns:
            val = key_lists[col][i]
            if val is None:
                col_eqs.append(IsNull(col))
            else:
                col_eqs.append(EqualTo(col, val))
        # Chain And for multiple columns
        expr = col_eqs[0]
        for eq in col_eqs[1:]:
            expr = And(expr, eq)
        row_filters.append(expr)

    # Chain Or for multiple rows
    if len(row_filters) == 1:
        return row_filters[0].to_sql()
    result = row_filters[0]
    for f in row_filters[1:]:
        result = Or(result, f)
    return result.to_sql()


def _apply_changes(catalog, dest_table, batch: pa.Table, key_columns: list[str]) -> dict[str, int]:
    """Apply CDC changes to a destination table atomically.

    Deletes are applied first, then upserts, within a single catalog transaction.
    If the transaction fails, both are rolled back — no partial state on the
    destination.

    Delete and upsert are idempotent under single-master assumptions: deleting an
    already-deleted row is a no-op, and upserting the same row twice produces the
    same result. This enables safe at-least-once retry on crash recovery.
    Destinations must not be written to by other sources.

    Returns dict of counts: {"deleted": N, "upserted": N, "upsert_matched": N}.

    - deleted: rows sent to delete (input count; delete API doesn't return affected count)
    - upserted: rows sent to upsert (input count)
    - upsert_matched: rows that matched existing rows during upsert (from UpsertResult.rows_updated)
    """
    ct_col = batch.column("change_type")

    # Separate by change type
    delete_mask = pc.equal(ct_col, pa.scalar("delete"))
    delete_rows = strip_meta(batch.filter(delete_mask))

    upsert_mask = pc.or_(
        pc.equal(ct_col, pa.scalar("insert")),
        pc.equal(ct_col, pa.scalar("update_postimage")),
    )
    upsert_rows = strip_meta(batch.filter(upsert_mask))

    counts = {"deleted": 0, "upserted": 0, "upsert_matched": 0}

    if delete_rows.num_rows == 0 and upsert_rows.num_rows == 0:
        return counts

    with catalog.begin_transaction() as txn:
        tbl = txn.load_table(dest_table.identifier)

        if delete_rows.num_rows > 0:
            filter_sql = _build_delete_filter(delete_rows, key_columns)
            tbl.delete(filter_sql)
            counts["deleted"] = delete_rows.num_rows

        if upsert_rows.num_rows > 0:
            upsert_result = tbl.upsert(upsert_rows, join_cols=key_columns)
            counts["upserted"] = upsert_rows.num_rows
            counts["upsert_matched"] = upsert_result.rows_updated

    return counts


# ---------------------------------------------------------------------------
# Destination seeding
# ---------------------------------------------------------------------------


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

    Memory note: scan().to_arrow() materializes the entire filtered result.
    For very large source tables, reduce poll.interval_seconds to keep the
    source small, or accept the initial memory spike during seeding.
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
        stop_heartbeat = _start_progress_heartbeat(f"Seed scan for destination {dest_id}")
        try:
            scan_t0 = time.monotonic()
            # Pin scan to the captured snapshot to avoid skew — ensures the
            # cursor and the scanned data refer to the same point in time.
            scan = src_table.scan(
                row_filter=EqualTo(cfg.routing.field, routing_value),
                snapshot_id=current_id,
            )
            rows = scan.to_arrow()
            scan_secs = time.monotonic() - scan_t0

            if rows.num_rows > 0:
                write_t0 = time.monotonic()
                catalog, table = dest_pool.get(dest_id)
                if key_columns:
                    table.upsert(rows, join_cols=key_columns)
                else:
                    table.append(rows)
                write_secs = time.monotonic() - write_t0
                log.info(
                    "Seeded destination %s: %d rows (scan=%.1fs, write=%.1fs)",
                    dest_id,
                    rows.num_rows,
                    scan_secs,
                    write_secs,
                )
            else:
                log.info(
                    "Destination %s: no matching rows in source (scan=%.1fs); cursor advanced",
                    dest_id,
                    scan_secs,
                )
        finally:
            stop_heartbeat.set()

        state_mgr.advance_cursor(dest_id, current_id, cumulative_rows=rows.num_rows)


# ---------------------------------------------------------------------------
# Poll cycle
# ---------------------------------------------------------------------------


def run(cfg: config.ViaduckConfig) -> None:
    """Main poll loop."""
    metrics.init(cfg.pipeline_name)

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

    # Initialize state and destinations
    state_mgr = StateManager(src_catalog, cfg.instance.id, cfg.state)
    dest_pool = DestinationPool(cfg, max_open=50)
    router = Router(cfg.routing)

    # Cache source schema for destination table creation.
    # `Table.schema` is a property in pyducklake — do not call it.
    dest_pool.set_source_schema(src_table.schema)

    # Build routing_value -> dest_id mapping
    rv_to_dest: dict[str, str] = {d.routing_value: d.id for d in cfg.destinations}

    assigned_ids = cfg.assigned_destination_ids()
    state_mgr.initialize_destinations(assigned_ids)

    # Seed new destinations from source scan (avoids CDC replay from snapshot 0)
    if cfg.routing.seed_mode == "scan":
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, assigned_ids)

    key_columns = cfg.routing.key_columns
    full_cdc = len(key_columns) > 0

    log.info(
        "Viaduck started: source=%s.%s, routing_field=%s, mode=%s, destinations=%d, instance=%s",
        cfg.source.name,
        cfg.source.table,
        cfg.routing.field,
        "full_cdc" if full_cdc else "append_only",
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
            _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, assigned_ids, rv_to_dest, key_columns, full_cdc)
        except Exception:
            log.exception("Fatal error in poll cycle")
            break

        if not shutdown:
            # Chunked sleep so SIGTERM is honored within ~1s rather than
            # waiting up to `interval_seconds`. With long poll intervals (e.g.
            # 300s) and k8s `terminationGracePeriodSeconds` (default 30s), an
            # uninterruptible sleep would let kubelet SIGKILL mid-poll.
            _interruptible_sleep(cfg.poll.interval_seconds, lambda: shutdown)

    # Graceful shutdown
    log.info("Shutting down...")
    dest_pool.close_all()
    try:
        src_catalog.close()
    except Exception:
        pass
    # Tell SSE handlers to exit before calling http.shutdown(), otherwise
    # an open /ui/sse client would block shutdown() forever.
    server.signal_shutdown()
    http.shutdown()
    log.info("Shutdown complete")


def _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, assigned_ids, rv_to_dest, key_columns, full_cdc):
    """Execute one poll cycle: read CDC, route, write to destinations, update state."""
    metrics.polls_total.inc()
    health.record_poll()

    cycle_t0 = time.monotonic()
    cycle_rows_read = 0
    cycle_rows_written = 0
    cycle_groups_processed = 0

    current_id = source.current_snapshot_id(src_table)
    if current_id is None:
        log.debug("No snapshots on source table yet")
        return

    metrics.source_snapshot_id.set(current_id)

    # Load cursors and group by snapshot
    cursor_map = state_mgr.load_cursors(assigned_ids)
    cursor_snapshots = {did: c.last_snapshot_id for did, c in cursor_map.items()}
    groups = _group_by_cursor(cursor_snapshots, assigned_ids)

    for start_snap, dest_ids in groups.items():
        if start_snap >= current_id:
            continue  # already caught up
        cycle_groups_processed += 1

        # Map dest_ids to their routing values for filter/split
        routing_values = [cfg.destination_by_id(d).routing_value for d in dest_ids]
        filter_expr = router.build_filter_expr(routing_values)

        # Read CDC — full changes or insertions only
        if full_cdc:
            raw_data = source.read_cdc_changes(
                src_table, start_snapshot=start_snap, end_snapshot=current_id, filter_expr=filter_expr
            )
        else:
            raw_data = source.read_cdc(
                src_table, start_snapshot=start_snap, end_snapshot=current_id, filter_expr=filter_expr
            )

        try:
            metrics.cdc_batch_rows.observe(raw_data.num_rows)
        except Exception:
            log.warning("Failed to record CDC batch size metric")

        cycle_rows_read += raw_data.num_rows

        if raw_data.num_rows == 0:
            state_mgr.advance_cursors(dest_ids, current_id)
            continue

        # Phase 1: Resolve preimages (full CDC only, before routing)
        if full_cdc:
            try:
                raw_data = _resolve_preimages(raw_data, cfg.routing.field, key_columns)
            except RoutingError:
                log.exception("Preimage resolution failed — key column may be missing from CDC data")
                metrics.errors_total.labels(type="routing", destination="").inc()
                break

        # Route in a single pass (split + count unrouted)
        try:
            routed, unrouted = router.split_and_count(raw_data, routing_values)
        except RoutingError:
            log.exception("Routing failed — routing field may be missing from source schema")
            metrics.errors_total.labels(type="routing", destination="").inc()
            break

        if unrouted > 0:
            metrics.unrouted_rows_total.inc(unrouted)

        # Write to each destination
        for routing_val, batch in routed.items():
            dest_id = rv_to_dest[routing_val]
            if batch.num_rows == 0:
                continue

            cursor = cursor_map.get(dest_id)
            prev_rows = cursor.rows_replicated if cursor else 0

            try:
                t0 = time.monotonic()

                if full_cdc:
                    # Phase 2: Resolve conflicts, Phase 3: Apply
                    resolved = _resolve_conflicts(batch)
                    if resolved.num_rows > 0:
                        counts = _write_with_retry(
                            dest_pool,
                            dest_id,
                            lambda cat, tbl, b=resolved, kc=key_columns: _apply_changes(cat, tbl, b, kc),
                        )
                        if counts["deleted"] > 0:
                            metrics.dest_rows_deleted_total.labels(destination=dest_id).inc(counts["deleted"])
                        if counts["upserted"] > 0:
                            metrics.dest_rows_upserted_total.labels(destination=dest_id).inc(counts["upserted"])
                        if counts["upsert_matched"] > 0:
                            metrics.dest_upsert_matched_total.labels(destination=dest_id).inc(counts["upsert_matched"])
                        ops_count = counts["deleted"] + counts["upserted"]
                    else:
                        ops_count = 0
                else:
                    _write_with_retry(dest_pool, dest_id, lambda cat, tbl, b=batch: tbl.append(b))
                    metrics.dest_rows_written_total.labels(destination=dest_id).inc(batch.num_rows)
                    ops_count = batch.num_rows

                duration = time.monotonic() - t0
                metrics.dest_write_seconds.labels(destination=dest_id).observe(duration)
                state_mgr.advance_cursor(dest_id, current_id, cumulative_rows=prev_rows + ops_count)
                metrics.dest_last_snapshot_id.labels(destination=dest_id).set(current_id)
                health.record_replication()
                cycle_rows_written += ops_count
            except Exception:
                log.exception("Failed to write to destination %s", dest_id)
                metrics.errors_total.labels(type="dest_write", destination=dest_id).inc()
                state_mgr.record_error(dest_id, f"Write failed after {_WRITE_MAX_RETRIES} retries")
                dest_pool.evict(dest_id)

        # Advance cursors for destinations that had no matching rows in this group
        routed_dest_ids = {rv_to_dest[rv] for rv in routed}
        no_data_ids = [did for did in dest_ids if did not in routed_dest_ids]
        if no_data_ids:
            state_mgr.advance_cursors(no_data_ids, current_id)

    # Update lag metrics and status snapshot.
    # Uses cursor_map loaded at the start of the cycle — status is one cycle stale.
    dest_statuses = []
    for did in assigned_ids:
        cursor = cursor_map.get(did)
        snap = getattr(cursor, "last_snapshot_id", 0) or 0
        lag = current_id - snap
        metrics.dest_lag_snapshots.labels(destination=did).set(lag)

        rows = cursor.rows_replicated if cursor else 0
        last_err = cursor.last_error if cursor else None
        if last_err:
            st = "error"
        elif lag > 0:
            st = "lagging"
        else:
            st = "healthy"

        dest_statuses.append(
            DestStatus(
                id=did,
                routing_value=cfg.destination_by_id(did).routing_value,
                snapshot=snap,
                lag=lag,
                rows_replicated=rows,
                status=st,
                last_error=last_err,
            )
        )

    status.update(
        source_table=f"{cfg.source.name}.{cfg.source.table}",
        source_snapshot=current_id,
        mode="full_cdc" if full_cdc else "append_only",
        poll_interval=cfg.poll.interval_seconds,
        destinations=dest_statuses,
        pool_open=dest_pool.size,
        pool_max=dest_pool.max_open,
    )

    # Per-cycle summary log. Quiet for empty cycles (no work) so steady-state
    # idleness doesn't flood the log; verbose when there's work to report.
    cycle_secs = time.monotonic() - cycle_t0
    if cycle_groups_processed > 0 or cycle_rows_read > 0 or cycle_rows_written > 0:
        max_lag = max((current_id - (getattr(cursor_map.get(did), "last_snapshot_id", 0) or 0)) for did in assigned_ids)
        log.info(
            "Poll cycle: snapshot=%d, groups=%d, cdc_rows_read=%d, rows_written=%d, max_lag=%d, duration=%.2fs",
            current_id,
            cycle_groups_processed,
            cycle_rows_read,
            cycle_rows_written,
            max_lag,
            cycle_secs,
        )


def main():
    parser = argparse.ArgumentParser(description="Viaduck — DuckLake to DuckLake CDC replication")
    parser.add_argument("--config", "-c", default="viaduck.yaml", help="Path to config YAML file")
    args = parser.parse_args()

    logging_config.setup()
    cfg = config.load(args.config)
    run(cfg)


if __name__ == "__main__":
    main()
