"""Buffered, concurrently-flushed delivery.

Implements the design verified in tla/Viaduck.tla: CDC reads happen at
poll cadence and accumulate in per-destination buffers (``BufferRead``);
destination writes happen at flush cadence on a worker pool (``FlushStart``
swaps the buffer out, ``FlushCommit``/``FlushFail`` are the outcomes).

Position model (the spec's dual tracking):
  - ``flushed``  — persisted cursor (Postgres, survives crashes)
  - ``position`` — in-memory read position (``bufferedThrough``); CDC reads
    for a destination issue from here, so successive polls cover disjoint
    ranges. Invariant: flushed[d] <= position[d].

Failure semantics (the spec's ``FlushFail``): a failed flush discards the
in-flight tables AND the live buffer, and resets ``position`` to
``flushed`` — keeping the live buffer would leave a coverage gap over
(flushed, inflight_through]. The range is re-read next cycle;
at-least-once + idempotent apply make the retry safe.

Threading discipline:
  - The poll thread is the only buffer WRITER (BufferRead) and the only
    flush SUBMITTER (maybe_flush) — submission swaps the buffer under the
    manager lock and marks the destination in-flight.
  - Workers never touch live buffers except through _on_flush_failure
    (under the lock). At most one flush per destination is in flight
    (the spec's in-flight guard).
  - StateManager and DestinationPool are internally locked; prometheus
    metrics are thread-safe.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

from viaduck import metrics
from viaduck.apply import append_only, apply_full_cdc
from viaduck.config import ConfigError

if TYPE_CHECKING:
    from viaduck.config import DeliveryConfig
    from viaduck.destination import DestinationPool
    from viaduck.state import StateManager

log = logging.getLogger(__name__)


@dataclass
class _Buffer:
    tables: list[pa.Table] = field(default_factory=list)
    rows: int = 0
    bytes: int = 0
    first_buffered_at: float | None = None  # monotonic; None when empty


@dataclass(frozen=True)
class DestDeliveryStatus:
    """Per-destination snapshot for the status page / metrics."""

    flushed_snapshot: int
    position_snapshot: int
    rows_replicated: int
    last_error: str | None
    buffer_rows: int
    buffer_age_s: float
    flushing: bool
    # This-run cumulative counters (in-memory, reset on restart). Applied
    # counts are taken from the flushed batch's change_type column BEFORE
    # Phase 2 — post-resolution the types are mangled (inserts collapse
    # into upserts, tombstones inflate deletes), so pre-Phase-2 is the
    # count that means something to a human.
    applied_inserts: int = 0
    applied_updates: int = 0
    applied_deletes: int = 0
    buffered_rows_total: int = 0
    # Wall-clock staleness of the durable cursor: age of the oldest
    # position advance not yet submitted for flush (0 when clean or
    # mid-flush). Reads better than snapshot counts on busy sources.
    lag_seconds: float = 0.0


class DeliveryManager:
    """Per-destination buffers + flush triggers + worker pool."""

    def __init__(
        self,
        cfg: DeliveryConfig,
        state_mgr: StateManager,
        dest_pool: DestinationPool,
        key_columns: list[str],
        assigned_ids: list[str],
        *,
        mode: str,
        on_flush_success=None,
    ):
        self._cfg = cfg
        self._state = state_mgr
        self._pool = dest_pool
        self._key_columns = key_columns
        # Apply mode comes from explicit routing.mode (validated upstream in
        # config.RoutingConfig.__post_init__): full_cdc → upsert path with
        # delete+upsert under one txn, append_only → straight tbl.append().
        # The previous "infer full_cdc from len(key_columns) > 0" derivation
        # was a silent misconfig hazard. mode is keyword-only so a positional
        # caller (test, future external) fails loudly at the call site rather
        # than running an unrelated string past the enum check.
        if mode not in ("full_cdc", "append_only"):
            raise ConfigError(
                f"DeliveryManager mode must be 'full_cdc' or 'append_only', got {mode!r}. "
                "This should have been caught by RoutingConfig.__post_init__ at config load; "
                "if you're seeing this, the caller bypassed config validation."
            )
        self._mode = mode
        self._full_cdc = mode == "full_cdc"
        self._on_flush_success = on_flush_success

        self._lock = threading.Lock()
        self._buffers: dict[str, _Buffer] = {d: _Buffer() for d in assigned_ids}
        self._inflight: set[str] = set()
        self._executor = ThreadPoolExecutor(max_workers=cfg.workers, thread_name_prefix="flush")
        # Shutdown signal threaded into apply._write_with_retry: a worker
        # deep in the OCC retry backoff (up to ~30s per sleep, ~5 min per
        # flush worst case) would otherwise ignore SIGTERM and get SIGKILLed
        # by k8s at the end of terminationGracePeriodSeconds (60s default),
        # tripping drain()'s "unflushed at deadline" warning on every rolling
        # restart. drain() sets this so the sleep wakes early and the retry
        # loop raises. Cursor semantics unchanged — an aborted retry is a
        # FlushFail like any other.
        self._stopping = threading.Event()

        # Position model. Initialized from the persisted cursors; the poll
        # thread reads positions for range grouping, workers move flushed
        # forward on success and positions backward on failure.
        cursors = state_mgr.load_cursors(assigned_ids)
        self._flushed: dict[str, int] = {d: cursors[d].last_snapshot_id if d in cursors else 0 for d in assigned_ids}
        self._position: dict[str, int] = dict(self._flushed)
        self._rows_replicated: dict[str, int] = {
            d: cursors[d].rows_replicated if d in cursors else 0 for d in assigned_ids
        }
        self._last_error: dict[str, str | None] = {
            d: cursors[d].last_error if d in cursors else None for d in assigned_ids
        }
        # Positions advanced in memory but not yet persisted (no data, or
        # data sitting in the buffer) age toward the flush interval from
        # the first un-persisted advance.
        self._position_dirty_since: dict[str, float | None] = {d: None for d in assigned_ids}
        # Read epochs make BufferRead effectively atomic against FlushFail:
        # the poll thread captures the epoch BEFORE its (slow) CDC read and
        # presents it with buffer()/advance_position(); _on_flush_failure
        # bumps the epoch when it resets the position, so a read that
        # overlapped the reset is discarded instead of stamping a stale
        # position over it (which would leave the dropped range unread —
        # the spec's BufferRead is a single atomic action; this restores
        # that atomicity).
        self._epoch: dict[str, int] = {d: 0 for d in assigned_ids}
        # Bytes held by in-flight (swapped-out) tables — counted toward the
        # watermark and the total-bytes gauge so memory backpressure sees
        # the worker-owned data too, not just live buffers.
        self._inflight_bytes: dict[str, int] = {d: 0 for d in assigned_ids}
        # This-run counters for the status page: pre-Phase-2 change-type
        # counts of successfully flushed batches, and total rows ever
        # buffered. In-memory only; at-least-once replays may double-count
        # (same caveat as rows_replicated).
        self._applied: dict[str, dict[str, int]] = {d: {"inserts": 0, "updates": 0, "deletes": 0} for d in assigned_ids}
        self._buffered_rows_total: dict[str, int] = {d: 0 for d in assigned_ids}

    # ------------------------------------------------------------------ #
    # Poll-thread API
    # ------------------------------------------------------------------ #

    def positions(self) -> dict[str, int]:
        """Read positions for CDC range grouping (bufferedThrough)."""
        with self._lock:
            return dict(self._position)

    def read_plan(self) -> dict[str, tuple[int, int]]:
        """Atomic snapshot of (position, epoch) per destination. The epoch
        must be passed back to buffer()/advance_position() so reads that
        overlapped a failure reset are discarded."""
        with self._lock:
            return {d: (self._position[d], self._epoch[d]) for d in self._position}

    def buffer(self, dest_id: str, table: pa.Table, through_snapshot: int, epoch: int | None = None) -> None:
        """Accumulate a routed, Phase-1-resolved batch (BufferRead).

        `epoch` is the value captured by read_plan() before the CDC read;
        a mismatch means a flush failure reset this destination while the
        read was in flight — the batch is discarded (safe: the failure
        path rewound the position, so the range will be re-read)."""
        now = time.monotonic()
        with self._lock:
            if epoch is not None and epoch != self._epoch[dest_id]:
                log.info(
                    "Discarding stale read for %s (epoch %d != %d): flush failure reset the position mid-read",
                    dest_id,
                    epoch,
                    self._epoch[dest_id],
                )
                return
            buf = self._buffers[dest_id]
            buf.tables.append(table)
            buf.rows += table.num_rows
            buf.bytes += table.nbytes
            self._buffered_rows_total[dest_id] += table.num_rows
            if buf.first_buffered_at is None:
                buf.first_buffered_at = now
            self._position[dest_id] = through_snapshot
            if self._position_dirty_since[dest_id] is None:
                self._position_dirty_since[dest_id] = now
            metrics.delivery_buffer_rows.labels(destination=dest_id).set(buf.rows)
            metrics.delivery_buffer_bytes.labels(destination=dest_id).set(buf.bytes)
            metrics.delivery_buffer_total_bytes.set(self._total_bytes_locked())

    def advance_position(self, dest_id: str, through_snapshot: int, epoch: int | None = None) -> None:
        """Advance the read position with no data (empty range for this
        destination). Persisted lazily on the flush cadence. Epoch-guarded
        like buffer()."""
        with self._lock:
            if epoch is not None and epoch != self._epoch[dest_id]:
                return
            if through_snapshot > self._position[dest_id]:
                self._position[dest_id] = through_snapshot
                if self._position_dirty_since[dest_id] is None:
                    self._position_dirty_since[dest_id] = time.monotonic()

    def should_pause_reads(self) -> bool:
        """True when the global buffer watermark is exceeded and every
        buffering destination is already in flight — reading more would
        grow memory without any way to relieve it."""
        with self._lock:
            if self._total_bytes_locked() < self._cfg.buffer_total_max_bytes:
                return False
            return all(self._buffers[d].rows == 0 or d in self._inflight for d in self._buffers)

    def maybe_flush(self, *, shutdown: bool = False) -> int:
        """Evaluate flush triggers and submit eligible flushes (FlushStart).

        Returns the number of flushes submitted. Called from the poll
        thread once per cycle, and from drain() at shutdown.
        """
        now = time.monotonic()
        submitted = 0
        over_watermark = False
        with self._lock:
            over_watermark = self._total_bytes_locked() >= self._cfg.buffer_total_max_bytes
            # Largest-first when over the watermark so forced flushes
            # relieve the most memory soonest.
            order = sorted(self._buffers, key=lambda d: self._buffers[d].bytes, reverse=over_watermark)
            for dest_id in order:
                if dest_id in self._inflight:
                    continue
                trigger = self._trigger_for_locked(dest_id, now, shutdown, over_watermark)
                if trigger is None:
                    continue
                buf = self._buffers[dest_id]
                tables, through = buf.tables, self._position[dest_id]
                # Buffer swap: the live buffer resets; reads keep landing
                # in the fresh one while the worker owns the snapshot.
                self._buffers[dest_id] = _Buffer()
                self._inflight.add(dest_id)
                self._inflight_bytes[dest_id] = buf.bytes
                self._position_dirty_since[dest_id] = None
                metrics.delivery_buffer_rows.labels(destination=dest_id).set(0)
                metrics.delivery_buffer_bytes.labels(destination=dest_id).set(0)
                future = self._executor.submit(self._flush, dest_id, tables, through, trigger)
                # _flush catches everything it expects; anything escaping
                # (a bug) must not vanish into an unobserved Future.
                future.add_done_callback(self._log_escaped_exception)
                submitted += 1
            metrics.delivery_buffer_total_bytes.set(self._total_bytes_locked())
        return submitted

    def drain(self, timeout_s: float = 60.0) -> None:
        """Shutdown: flush everything buffered, wait for workers, stop.

        Loops trigger evaluation so destinations whose flush was already in
        flight at shutdown get a second pass for rows buffered during it.
        Bounded by timeout_s; anything still unflushed at the deadline is
        abandoned with a warning (safe: persisted cursors make the ranges
        re-readable on restart — the spec's ProcessCrash path).
        """
        # Signal to `apply._write_with_retry` that any in-flight OCC retry
        # loop should wake and give up. Without this, workers blocked in a
        # 30s backoff sleep continue past the drain deadline and get
        # SIGKILLed by k8s.
        self._stopping.set()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            self.maybe_flush(shutdown=True)
            with self._lock:
                quiet = not self._inflight and all(
                    b.rows == 0 and self._position[d] <= self._flushed[d] for d, b in self._buffers.items()
                )
            if quiet:
                break
            time.sleep(0.05)
        with self._lock:
            leftover = {d: b.rows for d, b in self._buffers.items() if b.rows} or None
            still_inflight = set(self._inflight) or None
        if leftover or still_inflight:
            log.warning(
                "Drain deadline reached; abandoning buffers=%s inflight=%s "
                "(ranges re-read from persisted cursors on restart)",
                leftover,
                still_inflight,
            )
        # Don't block on stragglers past the deadline — kubelet's grace
        # period is the real bound and SIGKILL is coming either way.
        self._executor.shutdown(wait=False, cancel_futures=True)

    def status_snapshot(self) -> dict[str, DestDeliveryStatus]:
        now = time.monotonic()
        with self._lock:
            return {
                d: DestDeliveryStatus(
                    flushed_snapshot=self._flushed[d],
                    position_snapshot=self._position[d],
                    rows_replicated=self._rows_replicated[d],
                    last_error=self._last_error[d],
                    buffer_rows=self._buffers[d].rows,
                    buffer_age_s=(now - self._buffers[d].first_buffered_at)
                    if self._buffers[d].first_buffered_at is not None
                    else 0.0,
                    flushing=d in self._inflight,
                    applied_inserts=self._applied[d]["inserts"],
                    applied_updates=self._applied[d]["updates"],
                    applied_deletes=self._applied[d]["deletes"],
                    buffered_rows_total=self._buffered_rows_total[d],
                    lag_seconds=(now - self._position_dirty_since[d])
                    if self._position_dirty_since[d] is not None
                    else 0.0,
                )
                for d in self._buffers
            }

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _change_type_counts(self, batch: pa.Table) -> tuple[int, int, int]:
        """(inserts, updates, deletes) from the PRE-Phase-2 batch — the
        counts as the source meant them, before conflict resolution
        collapses inserts into upserts and emits tombstone deletes."""
        if not self._full_cdc or "change_type" not in batch.column_names:
            return (batch.num_rows, 0, 0)  # append-only: everything is an insert
        ct = batch.column("change_type")
        inserts = pc.sum(pc.cast(pc.equal(ct, "insert"), pa.int64())).as_py() or 0
        updates = pc.sum(pc.cast(pc.equal(ct, "update_postimage"), pa.int64())).as_py() or 0
        deletes = pc.sum(pc.cast(pc.equal(ct, "delete"), pa.int64())).as_py() or 0
        return (inserts, updates, deletes)

    @staticmethod
    def _log_escaped_exception(future) -> None:
        exc = future.exception()
        if exc is not None:  # pragma: no cover - bug guard
            log.critical("Flush worker raised outside its handler (bug): %r", exc, exc_info=exc)

    def _total_bytes_locked(self) -> int:
        return sum(b.bytes for b in self._buffers.values()) + sum(self._inflight_bytes.values())

    def _trigger_for_locked(self, dest_id: str, now: float, shutdown: bool, over_watermark: bool) -> str | None:
        buf = self._buffers[dest_id]
        has_data = buf.rows > 0
        position_ahead = self._position[dest_id] > self._flushed[dest_id]
        if not has_data and not position_ahead:
            return None
        if shutdown:
            return "shutdown"
        if has_data and over_watermark:
            return "memory"
        if has_data and buf.rows >= self._cfg.flush_max_rows:
            return "rows"
        if has_data and buf.bytes >= self._cfg.flush_max_bytes:
            return "bytes"
        dirty_since = self._position_dirty_since[dest_id]
        age_start = buf.first_buffered_at if has_data else dirty_since
        if age_start is not None and (now - age_start) >= self._cfg.flush_interval_seconds:
            return "interval"
        return None

    def _flush(self, dest_id: str, tables: list[pa.Table], through: int, trigger: str) -> None:
        """Worker: FlushCommit / FlushFail."""
        t0 = time.monotonic()
        try:
            ops_count = 0
            if tables:
                batch = tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")
                if self._full_cdc:
                    ops_count = apply_full_cdc(self._pool, dest_id, batch, self._key_columns, stop_event=self._stopping)
                else:
                    ops_count = append_only(self._pool, dest_id, batch, stop_event=self._stopping)
            # Cursor persist AFTER the destination commit (the gap is the
            # spec's CrashDuringFlush window). A short retry here avoids
            # invoking the full failure path (buffer drop + healthy-catalog
            # evict + range re-read) for a transient PG blip when the
            # destination write already landed.
            with self._lock:
                cumulative = self._rows_replicated[dest_id] + ops_count
            self._advance_cursor_with_retry(dest_id, through, cumulative)
            duration = time.monotonic() - t0
            type_counts = self._change_type_counts(batch) if tables else None
            with self._lock:
                self._flushed[dest_id] = max(self._flushed[dest_id], through)
                self._rows_replicated[dest_id] = cumulative
                self._last_error[dest_id] = None
                if type_counts is not None:
                    applied = self._applied[dest_id]
                    applied["inserts"] += type_counts[0]
                    applied["updates"] += type_counts[1]
                    applied["deletes"] += type_counts[2]
            metrics.delivery_flushes_total.labels(destination=dest_id, trigger=trigger).inc()
            metrics.dest_last_snapshot_id.labels(destination=dest_id).set(through)
            if tables:
                # Data flushes only: empty position-only persists must not
                # report write latency or readiness "replication" signals.
                metrics.delivery_flush_seconds.labels(destination=dest_id).observe(duration)
                # dest_write_seconds continuity: pre-buffering dashboards
                # observe per-destination write latency under this name.
                metrics.dest_write_seconds.labels(destination=dest_id).observe(duration)
                if self._on_flush_success is not None:
                    self._on_flush_success()
                log.info(
                    "Flushed %s: %d ops through snapshot %d (trigger=%s, %.2fs)",
                    dest_id,
                    ops_count,
                    through,
                    trigger,
                    duration,
                )
        except Exception:
            log.exception("Flush failed for destination %s", dest_id)
            # Invariant-restoring reset FIRST: if the bookkeeping below
            # (PG write, pool close) also fails, the position/buffer state
            # must already be consistent — otherwise the dropped range
            # would never be re-read.
            self._on_flush_failure(dest_id)
            metrics.errors_total.labels(type="dest_write", destination=dest_id).inc()
            metrics.delivery_buffers_dropped_total.labels(destination=dest_id).inc()
            try:
                self._state.record_error(dest_id, f"Flush failed (trigger={trigger})")
            except Exception:
                log.exception("Could not record flush error for %s", dest_id)
            try:
                self._pool.evict(dest_id)
            except Exception:
                log.exception("Could not evict connection for %s", dest_id)
        finally:
            with self._lock:
                self._inflight.discard(dest_id)
                self._inflight_bytes[dest_id] = 0
                metrics.delivery_buffer_total_bytes.set(self._total_bytes_locked())

    def _advance_cursor_with_retry(self, dest_id: str, through: int, cumulative: int, attempts: int = 3) -> None:
        for attempt in range(attempts):
            try:
                self._state.advance_cursor(dest_id, through, cumulative_rows=cumulative)
                return
            except Exception:
                if attempt == attempts - 1:
                    raise
                log.warning(
                    "Cursor persist for %s failed (attempt %d/%d); destination write already "
                    "committed — retrying before falling back to the re-read path",
                    dest_id,
                    attempt + 1,
                    attempts,
                    exc_info=True,
                )
                time.sleep(0.5 * (attempt + 1))

    def _on_flush_failure(self, dest_id: str) -> None:
        """FlushFail: discard the in-flight tables (already owned by this
        worker, simply dropped) AND the live buffer, and reset the read
        position to the persisted cursor. Keeping the live buffer would
        leave a coverage gap over (flushed, through]; the whole range is
        re-read from the persisted cursor next cycle."""
        with self._lock:
            dropped = self._buffers[dest_id]
            self._buffers[dest_id] = _Buffer()
            self._position[dest_id] = self._flushed[dest_id]
            self._epoch[dest_id] += 1  # invalidate reads that overlapped the reset
            self._position_dirty_since[dest_id] = None
            self._last_error[dest_id] = "Flush failed; range will be re-read"
            metrics.delivery_buffer_rows.labels(destination=dest_id).set(0)
            metrics.delivery_buffer_bytes.labels(destination=dest_id).set(0)
            metrics.delivery_buffer_total_bytes.set(self._total_bytes_locked())
            if dropped.rows:
                log.warning(
                    "Dropped %d buffered rows for %s after flush failure; re-reading from snapshot %d",
                    dropped.rows,
                    dest_id,
                    self._flushed[dest_id],
                )

    def wait_idle(self, timeout_s: float = 30.0) -> bool:
        """Test helper: wait until no flush is in flight. Returns True if idle."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            with self._lock:
                if not self._inflight:
                    return True
            time.sleep(0.02)
        return False
