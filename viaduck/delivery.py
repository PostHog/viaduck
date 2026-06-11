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

from viaduck import metrics
from viaduck.apply import append_only, apply_full_cdc

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


class DeliveryManager:
    """Per-destination buffers + flush triggers + worker pool."""

    def __init__(
        self,
        cfg: DeliveryConfig,
        state_mgr: StateManager,
        dest_pool: DestinationPool,
        key_columns: list[str],
        assigned_ids: list[str],
        on_flush_success=None,
    ):
        self._cfg = cfg
        self._state = state_mgr
        self._pool = dest_pool
        self._key_columns = key_columns
        self._full_cdc = len(key_columns) > 0
        self._on_flush_success = on_flush_success

        self._lock = threading.Lock()
        self._buffers: dict[str, _Buffer] = {d: _Buffer() for d in assigned_ids}
        self._inflight: set[str] = set()
        self._executor = ThreadPoolExecutor(max_workers=cfg.workers, thread_name_prefix="flush")

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

    # ------------------------------------------------------------------ #
    # Poll-thread API
    # ------------------------------------------------------------------ #

    def positions(self) -> dict[str, int]:
        """Read positions for CDC range grouping (bufferedThrough)."""
        with self._lock:
            return dict(self._position)

    def buffer(self, dest_id: str, table: pa.Table, through_snapshot: int) -> None:
        """Accumulate a routed, Phase-1-resolved batch (BufferRead)."""
        now = time.monotonic()
        with self._lock:
            buf = self._buffers[dest_id]
            buf.tables.append(table)
            buf.rows += table.num_rows
            buf.bytes += table.nbytes
            if buf.first_buffered_at is None:
                buf.first_buffered_at = now
            self._position[dest_id] = through_snapshot
            if self._position_dirty_since[dest_id] is None:
                self._position_dirty_since[dest_id] = now
            metrics.delivery_buffer_rows.labels(destination=dest_id).set(buf.rows)
            metrics.delivery_buffer_bytes.labels(destination=dest_id).set(buf.bytes)
            metrics.delivery_buffer_total_bytes.set(self._total_bytes_locked())

    def advance_position(self, dest_id: str, through_snapshot: int) -> None:
        """Advance the read position with no data (empty range for this
        destination). Persisted lazily on the flush cadence."""
        with self._lock:
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
                self._position_dirty_since[dest_id] = None
                metrics.delivery_buffer_rows.labels(destination=dest_id).set(0)
                metrics.delivery_buffer_bytes.labels(destination=dest_id).set(0)
                self._executor.submit(self._flush, dest_id, tables, through, trigger)
                submitted += 1
            metrics.delivery_buffer_total_bytes.set(self._total_bytes_locked())
        return submitted

    def drain(self, timeout_s: float = 60.0) -> None:
        """Shutdown: flush everything buffered, wait for workers, stop."""
        self.maybe_flush(shutdown=True)
        # Wait for in-flight flushes (including the ones just submitted).
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            with self._lock:
                if not self._inflight:
                    break
            time.sleep(0.05)
        self._executor.shutdown(wait=True, cancel_futures=True)

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
                )
                for d in self._buffers
            }

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _total_bytes_locked(self) -> int:
        return sum(b.bytes for b in self._buffers.values())

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
                    ops_count = apply_full_cdc(self._pool, dest_id, batch, self._key_columns)
                else:
                    ops_count = append_only(self._pool, dest_id, batch)
            # Cursor persist AFTER the destination commit (the gap is the
            # spec's CrashDuringFlush window).
            with self._lock:
                cumulative = self._rows_replicated[dest_id] + ops_count
            self._state.advance_cursor(dest_id, through, cumulative_rows=cumulative)
            duration = time.monotonic() - t0
            with self._lock:
                self._flushed[dest_id] = max(self._flushed[dest_id], through)
                self._rows_replicated[dest_id] = cumulative
                self._last_error[dest_id] = None
            metrics.delivery_flushes_total.labels(destination=dest_id, trigger=trigger).inc()
            metrics.delivery_flush_seconds.labels(destination=dest_id).observe(duration)
            # dest_write_seconds continuity: pre-buffering dashboards
            # observe per-destination write latency under this name.
            metrics.dest_write_seconds.labels(destination=dest_id).observe(duration)
            metrics.dest_last_snapshot_id.labels(destination=dest_id).set(through)
            if self._on_flush_success is not None:
                self._on_flush_success()
            if tables:
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
            metrics.errors_total.labels(type="dest_write", destination=dest_id).inc()
            metrics.delivery_buffers_dropped_total.labels(destination=dest_id).inc()
            self._state.record_error(dest_id, f"Flush failed (trigger={trigger})")
            self._pool.evict(dest_id)
            self._on_flush_failure(dest_id)
        finally:
            with self._lock:
                self._inflight.discard(dest_id)

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
