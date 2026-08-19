"""Destination apply path: Phase 2 conflict resolution + Phase 3 atomic
delete/upsert, with retry and connection-pool leasing.

Runs on flush worker threads (viaduck/delivery.py). Everything here is
Arrow kernels + pyducklake calls — the GIL is released for the heavy
parts. The Phase 1 preimage resolution stays in main.py: it runs on the
poll thread, per CDC read, before routing.
"""

from __future__ import annotations

import logging
import random
import threading
import time

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from viaduck import metrics
from viaduck.arrowutil import row_indices
from viaduck.router import RoutingError
from viaduck.schema_projection import SchemaProjectionError
from viaduck.source import strip_meta

log = logging.getLogger(__name__)

# OCC retry policy. DuckLake refuses viaduck's commit whenever ANY concurrent
# commit lands on the same table between viaduck's read-snapshot and its
# commit — including commits that touch disjoint file sets (e.g., a peer
# compactor rewriting old files while viaduck adds a new one). Empirically
# a shared-table catalog (team-50689: backfill + peer compactor) commits at
# ~12s mean, 47s p99 gap. A 2-5s viaduck flush window vs a 12s mean gap
# gives ~50% per-attempt success, so 3 attempts (7s worth of backoff)
# deterministically fails. 15 attempts with exp backoff capped at 30s +
# ±50% jitter probes over ~5 min — enough headroom to catch the P99 gap
# without stalling the flush worker forever. Fix the OCC over-strictness
# at the DuckLake fork layer is separate work; this is the viaduck-side
# operator that keeps replication moving until then.
_WRITE_MAX_RETRIES = 15
_WRITE_BASE_DELAY_S = 1.0
_WRITE_MAX_DELAY_S = 30.0
# ±50%: adversarial review pointed out that ±30% at the 30s cap gives only
# ~±9s of spread, which starts to thin under fleets of ~30 shared-catalog
# destinations racing a peer writer. ±50% doubles the spread at trivial cost.
_WRITE_JITTER_FRACTION = 0.5
# Attempts 1..N below this get DEBUG log lines (routine OCC dance); attempts
# at or above this get WARN. Prevents WARN log floods under sustained OCC
# pressure while keeping the "something is actually going wrong" signal
# audible from attempt 4 onward.
_WRITE_WARN_ATTEMPT_THRESHOLD = 4
_DELETE_CHUNK_ROWS = 1000

# Permanent errors that must NOT retry: retrying wastes the whole ~5 min
# budget on an error whose next attempt would fail identically, and floods
# logs with misleading "Write to X failed" messages when the actual error
# is a build-time or config-time issue the operator needs to see raw.
# Anything raised INSIDE the retry lambda (i.e., inside `_apply_changes`)
# that's structurally permanent goes here. Errors raised OUTSIDE the retry
# lambda don't need listing — they never enter the retry loop.
_PERMANENT_ERROR_TYPES: tuple[type[BaseException], ...] = (
    SchemaProjectionError,
    RoutingError,  # _build_delete_filter — missing key column, config bug
)


class FlushDeadlineExceeded(Exception):
    """The flush's overall wall-clock deadline (delivery.flush_deadline_seconds)
    expired mid retry loop. Raised out of _write_with_retry to take the normal
    FlushFail path (buffer drop + range re-read). Bounds how long one
    pathological destination can hold a shared flush worker: the attempt
    budget alone permits ~5.5 min of backoff sleeps plus per-attempt write
    time — unbounded wall time on a contended catalog. Scope, honestly: this
    bounds the WRITE retry loop only. A single blocking attempt is NOT
    preempted, the pool get() (catalog ATTACH, seconds) runs outside it, and
    the cursor-persist retry tail in delivery._flush is not covered either —
    worst-case worker occupancy is deadline + one in-flight attempt + cursor
    tail, not deadline exactly."""


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


# DuckLake OCC conflict signatures (DuckLake wraps the underlying conflict
# in a TransactionException whose message carries these markers). String
# matching is regrettable but pyducklake surfaces no typed exception for
# commit conflicts; both strings come from ducklake's commit path
# (ducklake_transaction.cpp) and are stable across the fork.
_OCC_CONFLICT_MARKERS = (
    "Failed to commit DuckLake transaction",
    "Transaction conflict",
)


def _is_occ_conflict(exc: BaseException) -> bool:
    msg = str(exc)
    return any(marker in msg for marker in _OCC_CONFLICT_MARKERS)


# Signatures that a write failure genuinely implicates the CONNECTION (dead
# socket, RDS restart/failover, TLS drop) and thus needs an evict + reconnect
# to recover. Everything NOT matched here — OCC conflicts AND write/IO errors
# (read-only fs, disk full, permission) — leaves the connection usable and
# MUST retry on the SAME connection. Closing mid-write orphans the fork httpfs
# write-retry buffer (~one flush, 130-160MB native) that a same-connection
# retry reuses; force-evicting on "any non-OCC error" was the dominant driver
# of the writer OOM loop (447 reconnects ≈ 415 team-2 read-only-/tmp write
# retries). This is a positive allowlist, not a not-OCC negative, precisely so
# a non-connection error can never trigger a leaking reconnect.
_CONNECTION_ERROR_MARKERS = (
    "server closed the connection",
    "connection to server",
    "could not connect",
    "connection refused",
    "connection reset",
    "terminating connection",  # RDS restart / failover / admin action
    "SSL connection has been closed",
    "no connection to the server",
    "broken pipe",
    "Connection Error",  # DuckDB-surfaced socket failure
)


def _is_connection_error(exc: BaseException) -> bool:
    msg = str(exc)
    return any(marker in msg for marker in _CONNECTION_ERROR_MARKERS)


# Signal that the DuckDB INSTANCE (not the socket) is dead. An Internal- or
# Fatal-class error — e.g. a fork bug in the DuckLake commit path ("Calling
# GetValueInternal on a value that is NULL", team-50689 2026-07-16) —
# permanently invalidates the whole database; every subsequent statement on
# any connection to it fails with "database has been invalidated because of
# a previous fatal error" until the instance is recreated. Retrying on the
# same instance is futile by construction, so this is the one
# non-connection case that MUST evict. The write-retry-buffer preservation
# argument for retrying in place (see _CONNECTION_ERROR_MARKERS) doesn't
# apply: nothing inside an invalidated instance is reusable, holding it
# just pins its memory.
#
# Typed check first: pyducklake propagates duckdb exceptions unwrapped, and
# duckdb.InternalException / duckdb.FatalException are exactly the two
# classes that invalidate the instance (OutOfMemoryException and
# TransactionException are NOT subclasses of either — verified MROs — so
# OOM/OCC can't false-positive here). The string marker is a fallback for
# any future wrapping layer; the text comes from duckdb's
# ErrorManager::InvalidatedDatabase and is duckdb-core, not fork-owned.
#
# NOTE: the invalidated-database wrapper message also embeds the original
# commit error, so it substring-matches _OCC_CONFLICT_MARKERS too. That
# overlap is harmless only because the retry loop checks instance-fatal for
# its evict/fail-fast decisions and never consults _is_occ_conflict — keep
# it that way (asserted in test_apply_retry.py).
_INSTANCE_FATAL_MARKERS = ("database has been invalidated because of a previous fatal error",)


def _is_instance_fatal(exc: BaseException) -> bool:
    if isinstance(exc, (duckdb.InternalException, duckdb.FatalException)):
        return True
    msg = str(exc)
    return any(marker in msg for marker in _INSTANCE_FATAL_MARKERS)


def _backoff_sleep(delay: float, stop_event: threading.Event | None) -> bool:
    """Sleep `delay` seconds between retry attempts. If `stop_event` fires,
    wake early and return True. Otherwise return False.

    Extracted so tests can patch this single seam to zero out backoff
    without threading extra plumbing through the retry loop.
    """
    if stop_event is None:
        time.sleep(delay)
        return False
    return stop_event.wait(delay)


def _write_with_retry(
    dest_pool, destination_id, operation, stop_event: threading.Event | None = None, deadline: float | None = None
):
    """Execute a write operation on a destination with exponential backoff.

    operation: callable that takes (catalog, table) and performs the write.
    The pool lease (get/release) pins the connection so concurrent LRU
    eviction by other workers can't close it mid-transaction.

    stop_event: optional shutdown signal. When set, the retry loop's next
    backoff wakes early and the current exception propagates. Without this,
    a worker deep in the retry backoff (up to ~30s per sleep) would ignore
    SIGTERM and get SIGKILLed once k8s' terminationGracePeriodSeconds
    elapses — cursor stays where it was (safe) but drain() reports an
    abandoned buffer on every rolling restart.

    deadline: optional monotonic timestamp (time.monotonic() basis) bounding
    the whole retry loop's wall time. Before each attempt the loop checks
    the deadline and raises FlushDeadlineExceeded instead of starting a new
    attempt; backoff sleeps are capped at the remaining time so a sleep
    never carries the flush past it. Attempt-count exhaustion semantics
    are unchanged — the deadline is an outer bound, not a replacement for
    the attempt budget.
    """
    metrics.dest_write_retrying.labels(destination=destination_id).set(1)
    instance_fatals = 0
    try:
        for attempt in range(_WRITE_MAX_RETRIES):
            if deadline is not None and time.monotonic() >= deadline:
                # Raised OUTSIDE the inner try: the generic except below
                # would otherwise route it through another retry pass.
                raise FlushDeadlineExceeded(
                    f"flush deadline exceeded before attempt {attempt + 1}/{_WRITE_MAX_RETRIES}"
                )
            try:
                catalog, table = dest_pool.get(destination_id)
                try:
                    return operation(catalog, table)
                finally:
                    dest_pool.release(destination_id)
            except _PERMANENT_ERROR_TYPES:
                # Permanent by construction — retrying wastes the retry budget
                # on an error whose next attempt fails identically, and floods
                # the log with misleading "Write to X failed" for what is a
                # build-time / config-time issue. Surface raw.
                raise
            except Exception as exc:
                instance_fatal = _is_instance_fatal(exc)
                if instance_fatal:
                    instance_fatals += 1
                if attempt == _WRITE_MAX_RETRIES - 1:
                    raise
                # Instance-fatal budget: ONE fresh-instance retry. The first
                # fatal evicts below and retries on a recreated instance —
                # that heals the transient case (concurrency-dependent fork
                # bug). A SECOND fatal means a fresh instance died on the
                # same batch: deterministic. Burning the remaining OCC-tuned
                # budget would cost 10-20s ATTACH + up-to-30s backoff + the
                # known ~160MB native close-leak per further attempt while
                # progressing nothing — fail fast to delivery._flush, which
                # evicts, drops the buffer for a cadence-paced re-read, and
                # records the destination error.
                if instance_fatal and instance_fatals >= 2:
                    log.error(
                        "Write to %s hit a second instance-fatal on a fresh instance "
                        "(attempt %d/%d) — deterministic failure, giving up early: %s",
                        destination_id,
                        attempt + 1,
                        _WRITE_MAX_RETRIES,
                        exc,
                    )
                    raise
                # Exponential backoff capped at _WRITE_MAX_DELAY_S so the
                # schedule doesn't sprint past the P99 peer-writer gap into
                # pathological wait times. Jittered so a fleet of destinations
                # racing the same peer commit doesn't herd on the same tick.
                base = min(_WRITE_BASE_DELAY_S * (2**attempt), _WRITE_MAX_DELAY_S)
                delay = base * (1.0 + random.uniform(-_WRITE_JITTER_FRACTION, _WRITE_JITTER_FRACTION))
                if deadline is not None:
                    # Never sleep past the deadline; the loop-top check
                    # raises FlushDeadlineExceeded when we wake.
                    delay = min(delay, max(0.0, deadline - time.monotonic()))
                metrics.dest_write_retries_total.labels(destination=destination_id).inc()
                # Log severity ladder: routine OCC dance (attempts 1..3) is
                # DEBUG; attempt 4+ is genuinely worth an operator's eye and
                # gets WARN. Keeps the WARN channel scannable during
                # sustained retry pressure. An instance invalidation is never
                # routine (it means a Fatal/Internal error killed the DuckDB
                # instance — fork bug) and is WARN from first sight.
                if instance_fatal or attempt + 1 >= _WRITE_WARN_ATTEMPT_THRESHOLD:
                    log_level = logging.WARNING
                else:
                    log_level = logging.DEBUG
                log.log(
                    log_level,
                    "Write to %s failed (attempt %d/%d, error: %s), retrying in %.1fs",
                    destination_id,
                    attempt + 1,
                    _WRITE_MAX_RETRIES,
                    exc,
                    delay,
                )
                # Evict + reconnect ONLY for errors that implicate the
                # connection itself (network, RDS restart, dead socket).
                # An OCC commit conflict leaves the connection perfectly
                # healthy — DuckLake takes its snapshot at BeginTransaction,
                # not at connect, so the next attempt's fresh transaction
                # sees the peer's commit on the SAME connection (this is
                # exactly how DuckLake's own internal retry loop behaved
                # before we disabled it). Reconnecting per conflict was
                # doubly expensive in prod: it added 10-20s of catalog
                # ATTACH to every retry attempt's window, and each
                # pyducklake Catalog create leaks ~160MB of native memory
                # on close (fork-side hunt pending) — 274 reconnects in
                # 106 min OOM-killed the pod at 48Gi, six times in one
                # night.
                # Was `if not _is_occ_conflict(exc)` — evict on ANY non-OCC
                # error — which force-closed the connection on write/IO errors
                # (read-only /tmp, disk, permission) too, orphaning the fork
                # httpfs write-retry buffer per failure (the OOM leak; see
                # _CONNECTION_ERROR_MARKERS). Now only a positively-identified
                # connection-death signal reconnects — plus a fatally-
                # invalidated instance (_is_instance_fatal), where the
                # instance is already dead and every same-instance retry
                # fails identically (team-50689 burned all 15 attempts
                # against one invalidated instance, 2026-07-16).
                if _is_connection_error(exc) or instance_fatal:
                    metrics.pool_force_evictions_total.labels(
                        destination=destination_id,
                        reason="instance_fatal" if instance_fatal else "connection",
                    ).inc()
                    dest_pool.evict(destination_id)
                # Wake early on shutdown — the flush worker's job is to make
                # progress OR let the process exit, not to hold a sleep past
                # SIGTERM. If `_backoff_sleep` returns True, the stop_event
                # fired mid-wait; re-raise so the caller (delivery._flush)
                # takes the FlushFail path immediately instead of another
                # retry attempt.
                if _backoff_sleep(delay, stop_event):
                    raise
    finally:
        metrics.dest_write_retrying.labels(destination=destination_id).set(0)


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
    reused.

    KNOWN OPEN ISSUE (2026-06-11): DuckLake empirically REUSES a row's
    rowid when an upsert re-creates a previously deleted key (observed:
    insert@s4/delete@s5/re-insert@s18 all carrying one rowid in a churn
    soak; 6 of ~8400 rows at 200 rows/s). When that happens within one
    flush window, the re-insert pairs with the tombstone and the new row
    is lost — and the OLD cancel-both rule lost it identically, so this
    predates the tombstone change. Pending fix: a snapshot-ordered
    "latest event wins" Phase 2 rule (spec-first redesign) and/or an
    upstream DuckLake ruling on rowid stability. Does not affect
    append-only mode (Phase 2 unused).

    Rules:
    - insert + delete for same rowid → drop the insert, KEEP the delete
      (tombstone). Against a destination that never saw the insert the
      delete is an idempotent no-op; against one that DID see it via a
      commit/cursor-gap replay it is the only event that can ever remove
      the row. Cancelling both (the old rule) made such phantoms
      permanent — the spec's retired everCrashed limitation.
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

    has_del = work.column("__del_max")
    has_post = work.column("__post_max")

    # Drop rules:
    #   insert row:    dropped when a postimage exists (newer state wins) or
    #                  a delete exists (the delete survives as a tombstone)
    #   delete row:    NEVER dropped — see the tombstone rule above
    #   postimage row: dropped when a delete exists (delete wins)
    drop = pc.or_(
        pc.and_(is_insert, pc.or_(has_post, has_del)),
        pc.and_(is_post, has_del),
    )
    keep_mask = pc.invert(pc.fill_null(drop, False)).combine_chunks()

    # Metric parity with the predecessor: one increment per rowid with an
    # insert+postimage (no delete) conflict, one per rowid with an
    # insert+delete pair.
    has_tombstone = pc.and_(flags.column("__ins_max"), flags.column("__del_max"))
    n_conflicts = pc.sum(
        pc.cast(
            pc.or_(
                pc.and_(
                    pc.and_(flags.column("__ins_max"), flags.column("__post_max")), pc.invert(flags.column("__del_max"))
                ),
                has_tombstone,
            ),
            pa.int64(),
        )
    ).as_py()
    if n_conflicts:
        metrics.cdc_conflicts_resolved_total.inc(n_conflicts)
    # Tombstones: deletes surviving from insert+delete pairs. Normally
    # no-ops at the destination; the count is the write-amplification cost
    # of phantom healing (and a churn signal worth alerting on).
    n_tombstones = pc.sum(pc.cast(has_tombstone, pa.int64())).as_py()
    if n_tombstones:
        metrics.cdc_tombstones_emitted_total.inc(n_tombstones)

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
        col_arr = delete_rows.column(col)
        has_null = col_arr.null_count > 0
        non_null = col_arr.drop_null().to_pylist()

        if non_null and has_null:
            return Or(In(col, tuple(non_null)), IsNull(col)).to_sql()
        elif has_null:
            return IsNull(col).to_sql()
        else:
            return In(col, tuple(non_null)).to_sql()

    # Multi-column composite key: Or(And(col1=v1, col2=v2), And(col1=v3, col2=v4), ...)
    # Precompute per-column null presence so the inner loop skips the None check
    # for columns that have no nulls (the common case).
    col_has_nulls = {col: delete_rows.column(col).null_count > 0 for col in key_columns}
    key_lists = {col: delete_rows.column(col).to_pylist() for col in key_columns}
    row_filters = []
    for i in range(delete_rows.num_rows):
        col_eqs = []
        for col in key_columns:
            val = key_lists[col][i]
            if col_has_nulls[col] and val is None:
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


def _dedupe_upserts_last_write_wins(upsert_rows: pa.Table, key_columns: list[str]) -> pa.Table:
    """Per-key last-write-wins over the upsert candidates — the spec's
    Phase3Apply Winner(k) (tla/Viaduck.tla).

    A buffered flush batch unions multiple CDC reads, so the same key can
    legitimately carry several upsert candidates (an insert + later
    postimages, or successive postimages). Passing duplicate-key rows to
    pyducklake's upsert produces duplicate rows in the destination (found
    by the M3 soak: multi-update windows at 15s buffering). Keep the
    candidate with the highest snapshot_id, rowid as a deterministic
    tiebreaker. Requires the CDC meta columns — call BEFORE strip_meta.

    Selection is take()-on-winner-indices, not a join back: Acero hash
    joins don't match NULL keys, which would silently drop null-keyed
    candidates. group_by treats NULLs as a regular group, so they
    round-trip through take.
    """
    if upsert_rows.num_rows <= 1:
        return upsert_rows
    ordered = upsert_rows.combine_chunks().sort_by([("snapshot_id", "ascending"), ("rowid", "ascending")])
    ordered = ordered.append_column("__idx", row_indices(ordered.num_rows))
    winners = ordered.group_by(key_columns).aggregate([("__idx", "max")])
    return ordered.take(winners.column("__idx_max")).drop(["__idx"])


def _apply_changes(
    catalog,
    dest_table,
    batch: pa.Table,
    key_columns: list[str],
    projection=None,
    on_null_fallback=None,
) -> dict[str, int]:
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
    # Winner(k) BEFORE stripping meta — the dedup orders by snapshot_id/rowid.
    upsert_rows = strip_meta(_dedupe_upserts_last_write_wins(batch.filter(upsert_mask), key_columns))

    # `_build_delete_filter` only consumes `key_columns`, and B2 guards
    # guarantee key columns pass through the projection untouched. So skip
    # projection on delete_rows entirely and narrow the batch to key columns
    # only — avoids running (potentially per-value fallback) casts on payload
    # columns whose values we're about to discard anyway.
    if delete_rows.num_rows > 0:
        delete_rows = delete_rows.select(key_columns)

    # Upsert rows still need the full projection: tbl.upsert writes every
    # column into the destination shape.
    if projection is not None and upsert_rows.num_rows > 0:
        upsert_rows = projection.apply(upsert_rows, on_null_fallback=on_null_fallback)

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


def apply_full_cdc(
    dest_pool,
    dest_id: str,
    batch: pa.Table,
    key_columns: list[str],
    stop_event: threading.Event | None = None,
    deadline: float | None = None,
) -> int:
    """Phase 2 + Phase 3 for one destination flush. Returns ops applied."""
    resolved = _resolve_conflicts(batch)
    if resolved.num_rows == 0:
        return 0

    # Accumulate per-column fallback counts across retry attempts. Overwrite
    # per column: projection.apply is deterministic on the same batch, so
    # each attempt reproduces the same count — overwriting is idempotent
    # under retry, and .inc() runs exactly once after the write commits. A
    # per-attempt .inc() would inflate `viaduck_projection_cast_null_fallback_total`
    # by up to `_WRITE_MAX_RETRIES`, drowning the drift alarm signal in
    # retry noise.
    fallback_counts: dict[str, int] = {}

    def _null_fallback(column: str, count: int) -> None:
        fallback_counts[column] = count

    # Resolve projection INSIDE the lambda: `_write_with_retry` may evict
    # and reopen the destination between attempts (destination.py:_projections
    # is repopulated by `_create()`), so capturing the plan outside the loop
    # would keep a stale reference across schema-drift-triggered retries.
    counts = _write_with_retry(
        dest_pool,
        dest_id,
        lambda cat, tbl: _apply_changes(
            cat,
            tbl,
            resolved,
            key_columns,
            projection=dest_pool.projection_for(dest_id),
            on_null_fallback=_null_fallback,
        ),
        stop_event=stop_event,
        deadline=deadline,
    )
    for column, count in fallback_counts.items():
        metrics.projection_cast_null_fallback_total.labels(destination=dest_id, column=column).inc(count)
    if counts["deleted"] > 0:
        metrics.dest_rows_deleted_total.labels(destination=dest_id).inc(counts["deleted"])
    if counts["upserted"] > 0:
        metrics.dest_rows_upserted_total.labels(destination=dest_id).inc(counts["upserted"])
    if counts["upsert_matched"] > 0:
        metrics.dest_upsert_matched_total.labels(destination=dest_id).inc(counts["upsert_matched"])
    return counts["deleted"] + counts["upserted"]


def append_only(
    dest_pool,
    dest_id: str,
    batch: pa.Table,
    stop_event: threading.Event | None = None,
    deadline: float | None = None,
) -> int:
    """Append-only mode (mode=append_only): one tbl.append() per flush.

    Precondition: the operator declared the source insert-only via
    `routing.mode=append_only`. The batch is expected to come from
    the feed (table_insertions semantics) and is
    therefore structurally insert-only — `ducklake_table_insertions` does
    NOT synthesize a `change_type` column, so the boundary check below
    treats the presence of that column as proof that the batch came from
    `read_cdc_changes` / `ducklake_table_changes` instead (a viaduck
    routing bug that would otherwise silently misclassify deletes and
    update_postimages as inserts in the destination).

    The check is cheap (O(1) — just a column-name lookup) and runs once
    per flush. It does NOT catch the case where DuckLake's
    `ducklake_table_insertions` macro itself starts returning a
    `change_type` column — that's a contract change we'd want to know
    about anyway.
    """
    if batch.num_rows == 0:
        return 0
    if "change_type" in batch.column_names:
        raise RuntimeError(
            f"append_only got a batch with a 'change_type' column for destination {dest_id!r}; "
            "this batch came from ducklake_table_changes (CDC stream including deletes / update_*) "
            "rather than ducklake_table_insertions. mode=append_only assumes the source-read path "
            "is insert-only — either the dispatch in _poll_cycle is routing the wrong batch into "
            "append_only(), or DuckLake's table_insertions contract changed. Refusing to write "
            "potentially-misclassified rows."
        )

    # See comment in apply_full_cdc: accumulate per-column fallback counts
    # and emit exactly once post-commit so retries don't inflate the drift
    # alarm signal.
    fallback_counts: dict[str, int] = {}

    def _null_fallback(column: str, count: int) -> None:
        fallback_counts[column] = count

    def _apply(cat, tbl):
        # Resolve INSIDE the retry lambda — see comment in apply_full_cdc.
        projection = dest_pool.projection_for(dest_id)
        b = batch if projection is None else projection.apply(batch, on_null_fallback=_null_fallback)
        tbl.append(b)

    _write_with_retry(dest_pool, dest_id, _apply, stop_event=stop_event, deadline=deadline)
    for column, count in fallback_counts.items():
        metrics.projection_cast_null_fallback_total.labels(destination=dest_id, column=column).inc(count)
    metrics.dest_rows_written_total.labels(destination=dest_id).inc(batch.num_rows)
    return batch.num_rows
