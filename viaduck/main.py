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
from viaduck.arrowutil import full_bool, row_indices
from viaduck.destination import DestinationPool
from viaduck.router import Router, RoutingError
from viaduck.server import DestStatus, health, status
from viaduck.source import strip_meta
from viaduck.state import StateManager

log = logging.getLogger(__name__)

_WRITE_MAX_RETRIES = 3
_WRITE_BASE_DELAY_S = 1.0
_DELETE_CHUNK_ROWS = 1000


def _start_progress_heartbeat(
    label: str,
    interval_s: float = 30.0,
    state: dict | None = None,
    early_interval_s: float | None = None,
    early_duration_s: float = 60.0,
    pre_progress_label: str = "no progress yet",
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
            if state and "rows" in state:
                rows = state["rows"]
                if rows > 0:
                    batches = state.get("batches", 0)
                    rate = rows / elapsed if elapsed > 0 else 0
                    log.info(
                        "%s: %d rows in %d batches, %.0fs elapsed (%.0f rows/s)",
                        label,
                        rows,
                        batches,
                        elapsed,
                        rate,
                    )
                else:
                    log.info("%s: %s, %.0fs elapsed", label, pre_progress_label, elapsed)
            else:
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


def _require_non_null_rowids(batch: pa.Table) -> None:
    """Reject null rowids loudly. Stable, non-null rowids are a contract
    assumption (see the module docstring + tla/Viaduck.tla); the Arrow hash
    joins in Phases 1/2 do not match null keys, so a null rowid would be
    silently misclassified (orphaned / never cancelled) rather than paired.
    Fail fast instead."""
    rowid_nulls = batch.column("rowid").null_count
    if rowid_nulls:
        raise ValueError(
            f"CDC batch contains {rowid_nulls} null rowid(s); rowids are assumed "
            "stable and non-null (DuckLake contract). Refusing to resolve."
        )


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

    batch = batch.combine_chunks()
    _require_non_null_rowids(batch)
    ct_col = batch.column("change_type")
    is_insert = pc.equal(ct_col, pa.scalar("insert"))
    is_delete = pc.equal(ct_col, pa.scalar("delete"))
    is_post = pc.equal(ct_col, pa.scalar("update_postimage"))

    # Per-rowid presence flags via group_by, joined back onto every row.
    # Joins don't preserve order; carry the row index and sort back.
    n = batch.num_rows
    flags = (
        pa.table(
            {
                "rowid": batch.column("rowid"),
                "__ins": is_insert,
                "__del": is_delete,
                "__post": is_post,
            }
        )
        .group_by("rowid")
        .aggregate([("__ins", "max"), ("__del", "max"), ("__post", "max")])
    )
    work = (
        pa.table({"rowid": batch.column("rowid"), "__idx": row_indices(n)})
        .join(flags, keys="rowid", join_type="left outer")
        .sort_by("__idx")
    )

    has_ins = work.column("__ins_max")
    has_del = work.column("__del_max")
    has_post = work.column("__post_max")

    # Drop rules (same as the row-loop predecessor):
    #   insert row:    dropped when a postimage exists (newer state wins) or
    #                  a delete exists (insert+delete cancel)
    #   delete row:    dropped when an insert exists (cancel pair)
    #   postimage row: dropped when a delete exists (delete wins)
    drop = pc.or_(
        pc.or_(
            pc.and_(is_insert, pc.or_(has_post, has_del)),
            pc.and_(is_delete, has_ins),
        ),
        pc.and_(is_post, has_del),
    )
    keep_mask = pc.invert(pc.fill_null(drop, False)).combine_chunks()

    # Metric parity with the predecessor: one increment per rowid with an
    # insert+postimage (no delete) conflict, one per rowid with an
    # insert+delete cancellation.
    n_conflicts = pc.sum(
        pc.cast(
            pc.or_(
                pc.and_(
                    pc.and_(flags.column("__ins_max"), flags.column("__post_max")), pc.invert(flags.column("__del_max"))
                ),
                pc.and_(flags.column("__ins_max"), flags.column("__del_max")),
            ),
            pa.int64(),
        )
    ).as_py()
    if n_conflicts:
        metrics.cdc_conflicts_resolved_total.inc(n_conflicts)

    result = batch if pc.all(keep_mask).as_py() else batch.filter(keep_mask)

    # Post-condition: no rowid should appear in both insert and delete sets
    if result.num_rows > 0:
        res_ct = result.column("change_type")
        ins_rids = result.filter(pc.equal(res_ct, pa.scalar("insert"))).column("rowid")
        del_rids = result.filter(pc.equal(res_ct, pa.scalar("delete"))).column("rowid")
        overlap = pc.filter(ins_rids, pc.is_in(ins_rids, value_set=del_rids.combine_chunks()))
        assert len(overlap) == 0, (
            f"Bug: rowids {overlap.to_pylist()} appear in both insert and delete after Phase 2 conflict resolution"
        )

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

    # Combine per-row filters with a BALANCED Or tree. A left-fold builds a
    # right-deep chain whose to_sql() recurses once per node — Python's
    # recursion limit (~1000) makes a single full delete chunk crash. Tree
    # reduction keeps the depth at log2(rows) (~10 for a 1000-row chunk).
    while len(row_filters) > 1:
        row_filters = [
            Or(row_filters[i], row_filters[i + 1]) if i + 1 < len(row_filters) else row_filters[i]
            for i in range(0, len(row_filters), 2)
        ]
    return row_filters[0].to_sql()


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
            # Chunked: a single filter over 100k+ keys builds an O(rows)
            # expression tree and a giant SQL string (the composite-key path
            # is one Or(And(...)) per row). 1,000 keys per delete() keeps
            # each statement parseable; all chunks share the transaction, so
            # atomicity is unchanged.
            for start in range(0, delete_rows.num_rows, _DELETE_CHUNK_ROWS):
                chunk = delete_rows.slice(start, _DELETE_CHUNK_ROWS)
                tbl.delete(_build_delete_filter(chunk, key_columns))
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
            "Source snapshot %d: %d data files (%.2f GiB), %.2f MiB of delete data",
            current_id,
            file_count,
            data_bytes / (1024**3),
            delete_bytes / (1024**2),
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
        stop_heartbeat = _start_progress_heartbeat(
            f"Seed scan for destination {dest_id}",
            state=progress,
            early_interval_s=5.0,
            early_duration_s=60.0,
            pre_progress_label="DuckDB pre-execution",
        )
        write_secs_total = 0.0
        try:
            seed_t0 = time.monotonic()
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
                catalog, table = dest_pool.get(dest_id)
                write_t0 = time.monotonic()
                if key_columns:
                    table.upsert(batch_table, join_cols=key_columns)
                else:
                    table.append(batch_table)
                write_secs_total += time.monotonic() - write_t0
                progress["rows"] += batch.num_rows
                progress["batches"] += 1

            seed_secs = time.monotonic() - seed_t0
            total_rows = progress["rows"]
            batch_count = progress["batches"]

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
    try:
        from viaduck._version import __version__
    except ImportError:
        __version__ = "unknown"
    log.info("viaduck %s starting", __version__)
    cfg = config.load(args.config)
    run(cfg)


if __name__ == "__main__":
    main()
