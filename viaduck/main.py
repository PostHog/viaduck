"""Entry point and main poll loop for viaduck."""

import argparse
import logging
import signal
import time

from viaduck import config, logging_config, metrics, source
from viaduck.destination import DestinationPool
from viaduck.router import Router, RoutingError
from viaduck.server import health
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


def _write_with_retry(dest_pool, destination_id, batch):
    """Write to a destination with exponential backoff on transient failures."""
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            _, table = dest_pool.get(destination_id)
            table.append(batch)
            return
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

    log.info(
        "Viaduck started: source=%s.%s, routing_field=%s, destinations=%d, instance=%s",
        cfg.source.name,
        cfg.source.table,
        cfg.routing.field,
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
            _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, assigned_ids, rv_to_dest)
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


def _poll_cycle(src_table, state_mgr, dest_pool, router, cfg, assigned_ids, rv_to_dest):
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

        inserted = source.read_cdc(
            src_table,
            start_snapshot=start_snap,
            end_snapshot=current_id,
            filter_expr=filter_expr,
        )

        if inserted.num_rows == 0:
            state_mgr.advance_cursors(dest_ids, current_id)
            continue

        # Route in a single pass (split + count unrouted)
        try:
            routed, unrouted = router.split_and_count(inserted, routing_values)
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

            # Compute cumulative rows before writing so we can pass it to advance_cursor
            cursor = cursor_map.get(dest_id)
            prev_rows = cursor.rows_replicated if cursor else 0

            try:
                t0 = time.monotonic()
                _write_with_retry(dest_pool, dest_id, batch)
                duration = time.monotonic() - t0
                metrics.dest_write_seconds.labels(destination=dest_id).observe(duration)
                metrics.dest_rows_written_total.labels(destination=dest_id).inc(batch.num_rows)
                state_mgr.advance_cursor(dest_id, current_id, cumulative_rows=prev_rows + batch.num_rows)
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
