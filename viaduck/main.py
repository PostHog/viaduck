"""Entry point and main poll loop for viaduck."""

import argparse
import logging
import signal
import time

import pyarrow as pa
import pyarrow.compute as pc

from viaduck import config, logging_config, metrics, source
from viaduck.destination import DestinationPool
from viaduck.router import Router, RoutingError
from viaduck.server import health
from viaduck.source import strip_meta
from viaduck.state import StateManager

log = logging.getLogger(__name__)

_WRITE_MAX_RETRIES = 3
_WRITE_BASE_DELAY_S = 1.0


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
        return batch.set_column(idx, "change_type", pa.array(new_change_types, type=pa.string()))

    # Filter out dropped preimages and update change_types
    idx = batch.column_names.index("change_type")
    batch = batch.set_column(idx, "change_type", pa.array(new_change_types, type=pa.string()))
    return batch.filter(pa.array(keep_mask))


# ---------------------------------------------------------------------------
# Phase 2: Conflict Resolution (per-destination, after routing)
# ---------------------------------------------------------------------------


def _resolve_conflicts(batch: pa.Table) -> pa.Table:
    """Resolve conflicting changes for the same rowid within a batch.

    Uses rowid (not just key_columns) to identify the same logical row.

    Rules:
    - insert + delete for same rowid → cancel both (net no-op)
    - update_postimage + delete for same rowid → drop postimage, keep delete
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
        return batch

    keep_mask = [i not in rows_to_remove for i in range(batch.num_rows)]
    return batch.filter(pa.array(keep_mask))


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
# Poll cycle
# ---------------------------------------------------------------------------


def run(cfg: config.ViaduckConfig) -> None:
    """Main poll loop."""
    metrics.init(cfg.pipeline_name)

    from viaduck import server

    http = server.start(cfg.server.port)

    # Connect to source
    src_catalog = source.connect(cfg.source)
    src_table = source.load_table(src_catalog, cfg.source.table)

    # Initialize state and destinations
    state_mgr = StateManager(src_catalog, cfg.instance.id, cfg.state)
    dest_pool = DestinationPool(cfg, max_open=50)
    router = Router(cfg.routing)

    # Cache source schema for destination table creation
    dest_pool.set_source_schema(src_table.schema())

    # Build routing_value -> dest_id mapping
    rv_to_dest: dict[str, str] = {d.routing_value: d.id for d in cfg.destinations}

    assigned_ids = cfg.assigned_destination_ids()
    state_mgr.initialize_destinations(assigned_ids)

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

    health.mark_started()

    while not shutdown:
        try:
            _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, assigned_ids, rv_to_dest, key_columns, full_cdc)
        except Exception:
            log.exception("Fatal error in poll cycle")
            break

        if not shutdown:
            time.sleep(cfg.poll.interval_seconds)

    # Graceful shutdown
    log.info("Shutting down...")
    dest_pool.close_all()
    try:
        src_catalog.close()
    except Exception:
        pass
    http.shutdown()
    log.info("Shutdown complete")


def _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, assigned_ids, rv_to_dest, key_columns, full_cdc):
    """Execute one poll cycle: read CDC, route, write to destinations, update state."""
    metrics.polls_total.inc()
    health.record_poll()

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

    # Update lag metrics
    for did in assigned_ids:
        cursor = cursor_map.get(did)
        snap = cursor.last_snapshot_id if cursor else 0
        metrics.dest_lag_snapshots.labels(destination=did).set(current_id - snap)


def main():
    parser = argparse.ArgumentParser(description="Viaduck — DuckLake to DuckLake CDC replication")
    parser.add_argument("--config", "-c", default="viaduck.yaml", help="Path to config YAML file")
    args = parser.parse_args()

    logging_config.setup()
    cfg = config.load(args.config)
    run(cfg)


if __name__ == "__main__":
    main()
