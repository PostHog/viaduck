"""Destination apply path: Phase 2 conflict resolution + Phase 3 atomic
delete/upsert, with retry and connection-pool leasing.

Runs on flush worker threads (viaduck/delivery.py). Everything here is
Arrow kernels + pyducklake calls — the GIL is released for the heavy
parts. The Phase 1 preimage resolution stays in main.py: it runs on the
poll thread, per CDC read, before routing.
"""

from __future__ import annotations

import logging
import time

import pyarrow as pa
import pyarrow.compute as pc

from viaduck import metrics
from viaduck.arrowutil import row_indices
from viaduck.router import RoutingError
from viaduck.source import strip_meta

log = logging.getLogger(__name__)

_WRITE_MAX_RETRIES = 3
_WRITE_BASE_DELAY_S = 1.0
_DELETE_CHUNK_ROWS = 1000


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


def _write_with_retry(dest_pool, destination_id, operation):
    """Execute a write operation on a destination with exponential backoff.

    operation: callable that takes (catalog, table) and performs the write.
    The pool lease (get/release) pins the connection so concurrent LRU
    eviction by other workers can't close it mid-transaction.
    """
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            catalog, table = dest_pool.get(destination_id)
            try:
                return operation(catalog, table)
            finally:
                dest_pool.release(destination_id)
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
# Flush-worker entry points
# ---------------------------------------------------------------------------


def apply_full_cdc(dest_pool, dest_id: str, batch: pa.Table, key_columns: list[str]) -> int:
    """Phase 2 + Phase 3 for one destination flush. Returns ops applied."""
    resolved = _resolve_conflicts(batch)
    if resolved.num_rows == 0:
        return 0
    counts = _write_with_retry(
        dest_pool,
        dest_id,
        lambda cat, tbl: _apply_changes(cat, tbl, resolved, key_columns),
    )
    if counts["deleted"] > 0:
        metrics.dest_rows_deleted_total.labels(destination=dest_id).inc(counts["deleted"])
    if counts["upserted"] > 0:
        metrics.dest_rows_upserted_total.labels(destination=dest_id).inc(counts["upserted"])
    if counts["upsert_matched"] > 0:
        metrics.dest_upsert_matched_total.labels(destination=dest_id).inc(counts["upsert_matched"])
    return counts["deleted"] + counts["upserted"]


def append_only(dest_pool, dest_id: str, batch: pa.Table) -> int:
    """Append-only mode (no key_columns): one append per flush."""
    if batch.num_rows == 0:
        return 0
    _write_with_retry(dest_pool, dest_id, lambda cat, tbl, b=batch: tbl.append(b))
    metrics.dest_rows_written_total.labels(destination=dest_id).inc(batch.num_rows)
    return batch.num_rows
