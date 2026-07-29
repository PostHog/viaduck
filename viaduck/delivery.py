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
    # (table, through_snapshot) per buffered CDC chunk: the through value
    # is what flush-batch SLICING needs — a partial swap's cursor must
    # advance exactly to the last included chunk's through, never to the
    # live read position (which covers chunks left behind).
    # INVARIANT (slice-cursor correctness and the TLA refinement both
    # stand on it): entries are strictly ascending by through — the poll
    # thread is the only writer, chunk_end is monotone per group, and
    # every position REWIND clears the buffer and bumps the epoch.
    entries: list[tuple[pa.Table, int]] = field(default_factory=list)
    rows: int = 0
    bytes: int = 0
    first_buffered_at: float | None = None  # monotonic; None when empty
    # Set when this buffer is the REMAINDER of a partial swap: the next
    # flush must not wait out the interval trigger (the pile is already
    # older than one flush; stalling its tail would add up to a full
    # interval of latency exactly when catch-up is being watched).
    sliced_remainder: bool = False


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
    """Per-destination buffers + flush triggers + worker pool.

    Membership invariants (C3 stage-2 review; pinned so nobody "fixes"
    them into a second authority):

    - I1: ``_active ⊆ _buffers.keys()`` — add_destination creates the dict
      entries under the lock BEFORE activating; nothing ever deletes a
      dict key (never-delete, §1 stop contract).
    - I2: all ten per-destination dicts share one monotonically-growing
      "ever-member" key set; constructor and add_destination write all
      ten atomically under the lock.
    - I3: ``_suspended`` is NOT constrained relative to ``_active`` — it
      is replaced wholesale from the lifecycle tracker (a different
      authority, operator intent). Its only reader consults it after the
      active filter, so suspended-but-removed is inert. It must never
      become a second membership authority.
    - I4: ``_inflight`` may contain inactive ids (in-flight flushes finish
      naturally); maybe_flush submits active-only, so removed ids drain
      to none.
    - Queries (``is_clean``, ``last_error``, ``discard_buffer``,
      ``clamp_to_retention``) accept any ever-member id, active or not —
      the reconciler's pending-restart loop polls ``is_clean`` on
      deactivated ids and depends on this.
    - A removed destination's leftover buffer bytes still count toward
      the watermark until the caller composes ``discard_buffer()`` — the
      deactivate recipe (§4) does; nothing here enforces it.
    """

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
        # Explicit membership (C3 §1 stop contract): the active set is the
        # ONLY authority on which destinations participate in reads,
        # flush submission, drain, and status. The per-destination dict
        # family deliberately persists past removal — the flush worker
        # touches ~10 dict sites after the point a stop can land, and
        # deleting entries turns an in-flight flush into a KeyError after
        # the destination write committed but before the cursor advanced
        # (deterministic duplicates on re-add); a delete-and-recreate also
        # resets the epoch and lets a pre-stop CDC read regress position.
        self._active: set[str] = set(assigned_ids)
        self._inflight: set[str] = set()
        self._executor = ThreadPoolExecutor(max_workers=cfg.workers, thread_name_prefix="flush")
        # Per-destination byte cap: the bound on each destination's CDC
        # "queue" (live buffer + in-flight swap). Reads for a destination
        # pause when ITS OWN queue is full — never because a peer's is.
        # Kafka-partition semantics: one slow/stuck consumer doesn't halt
        # the others' intake. Auto-derive a fair share of the global cap
        # when not explicitly configured; the global cap survives as the
        # forced-flush trigger in maybe_flush and the all-at-cap early-exit.
        if cfg.buffer_max_bytes_per_destination > 0:
            self._per_dest_cap = cfg.buffer_max_bytes_per_destination
            if len(assigned_ids) * self._per_dest_cap > cfg.buffer_total_max_bytes:
                # With destination-local read gating, the sum of per-dest
                # caps IS the effective memory bound — the global value no
                # longer stops reads. An explicit cap that oversubscribes
                # it is a one-knob misconfig worth a loud line.
                log.warning(
                    "delivery.buffer_max_bytes_per_destination=%d x %d destinations = %d "
                    "exceeds buffer_total_max_bytes=%d; effective memory bound is the sum "
                    "of per-destination caps",
                    self._per_dest_cap,
                    len(assigned_ids),
                    len(assigned_ids) * self._per_dest_cap,
                    cfg.buffer_total_max_bytes,
                )
        else:
            self._per_dest_cap = max(1, cfg.buffer_total_max_bytes // max(1, len(assigned_ids)))
        log.info(
            "Per-destination buffer cap: %d bytes (%s) across %d destinations",
            self._per_dest_cap,
            "explicit" if cfg.buffer_max_bytes_per_destination > 0 else "auto: buffer_total_max_bytes / N",
            len(assigned_ids),
        )
        # Startup semantics above are unchanged (explicit oversubscription
        # only warns — existing deployments keep their contract);
        # _recompute_per_dest_cap_locked enforces the bound on MEMBERSHIP
        # CHANGES, where dynamic growth would otherwise erode
        # buffer_total_max_bytes as the effective memory bound.
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
        # Lifecycle-suspended destinations (paused/retired): no flush
        # submissions. Draining destinations are NOT here — draining exists
        # to flush out. Owned by the poll thread via set_suspended().
        self._suspended: set[str] = set()

    # ------------------------------------------------------------------ #
    # Poll-thread API
    # ------------------------------------------------------------------ #

    def positions(self) -> dict[str, int]:
        """Read positions for CDC range grouping (bufferedThrough)."""
        with self._lock:
            return dict(self._position)

    def flushed_snapshots(self) -> dict[str, int]:
        """Durable-cursor view (in-memory mirror) for the retention-edge
        clamp check in the poll cycle."""
        with self._lock:
            return dict(self._flushed)

    def clamp_to_retention(self, dest_id: str, floor_snapshot: int) -> int | None:
        """Retention-edge clamp — a data-loss acknowledgment, never policy.

        The durable cursor has fallen below the earliest retained source
        snapshot: the range (flushed, floor] is no longer re-readable. Left
        alone, the next CDC read from it raises inside the poll cycle and
        the run loop treats that as fatal — one expired destination takes
        the whole instance down. Advance flushed (durable + in-memory) and
        position to the floor so the next read starts at the oldest
        retained snapshot.

        Durable persist happens FIRST (with the flush path's retry): a
        crash in between re-runs the clamp as a no-op next cycle, whereas
        the reverse order would let a first-flush failure rewind position
        to the un-clamped durable cursor and re-read the expired range.
        Race-safe against a concurrent flush success: the durable write
        reports whether the monotonic guard dropped it, and the in-memory
        stamp re-checks under the lock — a flush that advanced past the
        floor in the check/write gap means nothing was lost, so no note,
        no counter, return None. A zombie flush with through < floor
        cannot regress the clamp (success path max-guards _flushed);
        position is epoch-bumped when raised so a CDC read that overlapped
        the raise is discarded (buffer() stamps position unconditionally).

        Buffered rows are kept: anything already read is valid data, and
        its flush advances the cursor from the floor onward. The
        acknowledgment is outcome-honest: rows never read (position below
        the floor) are LOST; a range that was read but not durably flushed
        is AT RISK — it materializes as loss only if the pending flush
        fails (the rewind then lands on the floor).

        The durable loss note (record_error) is best-effort by
        construction: advance_cursor just cleared the error columns, and a
        crash before the re-record leaves the WARNING + counter as the
        only record. The next successful flush clears the note either way.

        Returns the pre-clamp flushed snapshot, or None if no clamp was
        needed (including a lost race).
        """
        with self._lock:
            old_flushed = self._flushed[dest_id]
            if old_flushed >= floor_snapshot:
                return None
        # cumulative_rows=None preserves the persisted count — no rows moved.
        if self._advance_cursor_with_retry(dest_id, floor_snapshot, None) == 0:
            # Monotonic guard dropped the write: a concurrent flush advanced
            # the durable cursor past the floor. Nothing was lost.
            return None
        with self._lock:
            if self._flushed[dest_id] >= floor_snapshot:
                # Same race, memory side: the flush's success path got here
                # between our durable write and this stamp.
                return None
            self._flushed[dest_id] = floor_snapshot
            position = self._position[dest_id]
            if position < floor_snapshot:
                self._position[dest_id] = floor_snapshot
                self._epoch[dest_id] += 1
            unread_through = min(position, floor_snapshot)
            if position < floor_snapshot:
                loss_note = (
                    f"cursor clamped to retention edge: snapshots ({unread_through}, {floor_snapshot}] "
                    f"expired UNREAD and are lost"
                )
                outcome = "lost"
            else:
                loss_note = (
                    f"cursor clamped to retention edge: re-read window for ({old_flushed}, {floor_snapshot}] "
                    f"expired; the range is buffered/in-flight and is lost only if its pending flush fails"
                )
                outcome = "at_risk"
            self._last_error[dest_id] = loss_note
        metrics.retention_clamp_total.labels(destination=dest_id, outcome=outcome).inc()
        log.warning(
            "RETENTION-EDGE CLAMP: destination %s cursor %d -> %d — %s",
            dest_id,
            old_flushed,
            floor_snapshot,
            loss_note,
        )
        try:
            self._state.record_error(dest_id, loss_note)
        except Exception:
            log.warning("Could not record retention-clamp note for %s", dest_id, exc_info=True)
        return old_flushed

    def read_plan(self) -> dict[str, tuple[int, int]]:
        """Atomic snapshot of (position, epoch) per ACTIVE destination. The
        epoch must be passed back to buffer()/advance_position() so reads
        that overlapped a failure reset are discarded."""
        with self._lock:
            return {d: (self._position[d], self._epoch[d]) for d in self._position if d in self._active}

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
            buf.entries.append((table, through_snapshot))
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

    def _dest_bytes_locked(self, dest_id: str) -> int:
        return self._buffers[dest_id].bytes + self._inflight_bytes[dest_id]

    def should_pause_reads_for(self, dest_id: str) -> bool:
        """True when THIS destination's queue (live buffer + in-flight swap)
        is at or above its per-destination cap. The poll thread stops reading
        for this destination until a flush drains it — backpressure is
        destination-local, so a stuck destination's full queue never pauses
        a healthy peer's intake. Sets the reads-paused gauge as a side
        signal: a destination pinned at cap for a long stretch is the
        operator-visible symptom of a slow/hung flush downstream."""
        with self._lock:
            paused = self._dest_bytes_locked(dest_id) >= self._per_dest_cap
        metrics.delivery_reads_paused.labels(destination=dest_id).set(1 if paused else 0)
        return paused

    def should_pause_all_reads(self) -> bool:
        """True when EVERY destination's queue is at or above its cap —
        no read anywhere would help. The poll thread uses this as a
        cycle-level early-exit and the operator-facing "pod is genuinely
        wedged" log signal; per-destination pauses are routine operation
        and intentionally quiet (gauge, not WARN)."""
        with self._lock:
            if not self._active:
                # A partitioned instance can legitimately own zero
                # destinations; vacuous all() would report it permanently
                # wedged and spam the watermark WARN state machine.
                return False
            return all(self._dest_bytes_locked(d) >= self._per_dest_cap for d in self._active)

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
            # relieve the most memory soonest. Active only: a removed
            # destination gets no NEW submissions — without the filter, a
            # stopped destination with position > flushed would keep
            # submitting position-persist flushes forever.
            order = sorted(
                (d for d in self._buffers if d in self._active),
                key=lambda d: self._buffers[d].bytes,
                reverse=over_watermark,
            )
            for dest_id in order:
                if dest_id in self._inflight:
                    continue
                if dest_id in self._suspended:
                    # Lifecycle pause/retire: nothing may be delivered. The
                    # buffer was discarded on suspension; this guard covers
                    # a read that raced the transition.
                    continue
                trigger = self._trigger_for_locked(dest_id, now, shutdown, over_watermark)
                if trigger is None:
                    continue
                buf = self._buffers[dest_id]
                # Flush-batch slicing: one swap takes at most
                # flush_batch_max_rows, cut at CHUNK boundaries (a single
                # oversize chunk still goes whole — cdc_chunk_snapshots
                # bounds that). Without the cap, a slow flush lets the
                # buffer pile up and the next swap takes everything — the
                # feedback loop that produced 170-440K-row batches and
                # drove the fork's native layer into buffer-manager
                # corruption (2026-07-29). The remainder stays buffered
                # with its age preserved, so the interval trigger keeps
                # the pipeline of bounded slices draining.
                cap = self._cfg.flush_batch_max_rows
                take = len(buf.entries)
                if cap > 0:
                    taken_rows = 0
                    take = 0
                    for tbl, _through in buf.entries:
                        if take > 0 and taken_rows + tbl.num_rows > cap:
                            break
                        taken_rows += tbl.num_rows
                        take += 1
                sliced = buf.entries[:take]
                remainder = buf.entries[take:]
                tables = [tbl for tbl, _ in sliced]
                if remainder:
                    # Cursor for a partial swap: the last included chunk's
                    # through — NOT the live position, which covers the
                    # chunks left behind.
                    through = sliced[-1][1] if sliced else self._flushed[dest_id]
                else:
                    # Full swap: position may be ahead of the last chunk
                    # (position-only advances); preserve the historical
                    # persist-through-position behavior.
                    through = self._position[dest_id]
                rem_buf = _Buffer()
                for tbl, thr in remainder:
                    rem_buf.entries.append((tbl, thr))
                    rem_buf.rows += tbl.num_rows
                    rem_buf.bytes += tbl.nbytes
                if remainder:
                    rem_buf.first_buffered_at = buf.first_buffered_at
                    rem_buf.sliced_remainder = True
                self._buffers[dest_id] = rem_buf
                self._inflight.add(dest_id)
                self._inflight_bytes[dest_id] = buf.bytes - rem_buf.bytes
                if not remainder:
                    self._position_dirty_since[dest_id] = None
                metrics.delivery_buffer_rows.labels(destination=dest_id).set(rem_buf.rows)
                metrics.delivery_buffer_bytes.labels(destination=dest_id).set(rem_buf.bytes)
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
                    self._buffers[d].rows == 0 and self._position[d] <= self._flushed[d] for d in self._active
                )
            if quiet:
                break
            time.sleep(0.05)
        with self._lock:
            leftover = {d: self._buffers[d].rows for d in self._active if self._buffers[d].rows} or None
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
                if d in self._active
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
        # A destination at its own queue cap must flush even if the global
        # thresholds aren't met — reads for it are paused until this drains,
        # so waiting for flush_max_bytes/interval would leave it wedged at
        # the cap with its intake stopped.
        if has_data and self._dest_bytes_locked(dest_id) >= self._per_dest_cap:
            return "memory"
        if has_data and buf.sliced_remainder:
            # The tail of a sliced pile drains as soon as the in-flight
            # slice completes — never on the interval cadence (slicing
            # review F1: a mid-size remainder below the rows/bytes
            # thresholds would otherwise stall up to flush_interval).
            return "sliced"
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
                # A lifecycle discard (pause/retire) may have rewound the
                # position below `through` while this flush was in flight.
                # The flush SUCCEEDED, so the destination has the range —
                # leaving position behind would re-read and re-apply it on
                # resume (deterministic duplicates in append_only). Restore
                # position >= flushed; epoch bump discards any read that
                # overlapped the restore.
                if self._position[dest_id] < through:
                    self._position[dest_id] = through
                    self._epoch[dest_id] += 1
                self._rows_replicated[dest_id] = cumulative
                # Clear the error only when this flush is at/ahead of the
                # cursor. A zombie flush (through < flushed — a retention
                # clamp landed mid-flight) must not clear the clamp's loss
                # note or regress the cursor gauge.
                flushed_after = self._flushed[dest_id]
                if through >= flushed_after:
                    self._last_error[dest_id] = None
                if type_counts is not None:
                    applied = self._applied[dest_id]
                    applied["inserts"] += type_counts[0]
                    applied["updates"] += type_counts[1]
                    applied["deletes"] += type_counts[2]
            metrics.delivery_flushes_total.labels(destination=dest_id, trigger=trigger).inc()
            metrics.dest_last_snapshot_id.labels(destination=dest_id).set(flushed_after)
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

    def _advance_cursor_with_retry(self, dest_id: str, through: int, cumulative: int | None, attempts: int = 3) -> int:
        for attempt in range(attempts):
            try:
                return self._state.advance_cursor(dest_id, through, cumulative_rows=cumulative)
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

    # ------------------------------------------------------------------ #
    # Membership (C3 reconciler; poll-thread only)
    # ------------------------------------------------------------------ #

    def add_destination(self, dest_id: str) -> None:
        """Add (or re-activate) a destination. MAX-MERGE on a returning id:
        surviving dict entries are reused, never recreated — recreating
        would reset the epoch (letting a pre-stop CDC read land in the
        post-re-add buffer and regress position) and could rewind
        flushed/position below values a zombie flush is about to confirm.
        A genuinely new id initializes from its persisted cursor (the
        caller persists any resume adjustment BEFORE calling this — the
        clamp-persist → register ordering from C3 §4 step 6).

        The cursor load happens OUTSIDE the lock (this module's uniform
        discipline: no state-store I/O under self._lock — a PG stall here
        would freeze every flush worker's success path plus buffer()/
        read_plan()). Race-free without the lock: membership mutations are
        poll-thread-only, so the check-then-load-then-install split cannot
        interleave with another add; the under-lock re-check is
        belt-and-suspenders."""
        with self._lock:
            known = dest_id in self._buffers
        cursors = {} if known else self._state.load_cursors([dest_id])
        with self._lock:
            if dest_id not in self._buffers:
                cursor = cursors[dest_id].last_snapshot_id if dest_id in cursors else 0
                self._buffers[dest_id] = _Buffer()
                self._flushed[dest_id] = cursor
                self._position[dest_id] = cursor
                self._rows_replicated[dest_id] = cursors[dest_id].rows_replicated if dest_id in cursors else 0
                self._last_error[dest_id] = cursors[dest_id].last_error if dest_id in cursors else None
                self._position_dirty_since[dest_id] = None
                self._epoch[dest_id] = 0
                self._inflight_bytes[dest_id] = 0
                self._applied[dest_id] = {"inserts": 0, "updates": 0, "deletes": 0}
                self._buffered_rows_total[dest_id] = 0
            self._active.add(dest_id)
            self._recompute_per_dest_cap_locked()

    def remove_destination(self, dest_id: str) -> None:
        """Deactivate a destination (C3 §1 stop contract): membership-set
        removal ONLY. Every dict entry persists for the process lifetime;
        an in-flight flush finishes naturally and harmlessly (durable
        state: delivered is delivered). The caller composes this with
        discard_buffer() and the is-clean-latched pool evict. Idempotent."""
        with self._lock:
            self._active.discard(dest_id)
            self._recompute_per_dest_cap_locked()

    def inflight_ids(self) -> set[str]:
        """Ids with a flush in flight — the reconciler defers metric
        label removal for a deactivated id until it leaves this set (an
        in-flight flush calling .labels() after a .remove() re-creates
        the series frozen)."""
        with self._lock:
            return set(self._inflight)

    def active_ids(self) -> set[str]:
        with self._lock:
            return set(self._active)

    def _recompute_per_dest_cap_locked(self) -> None:
        """Re-derive the per-destination cap on membership change. The sum
        of per-dest caps is the effective memory bound (backpressure is
        destination-local), so a growing fleet under a frozen cap
        oversubscribes buffer_total_max_bytes — an OOM path once dynamic
        adds arrive. Auto mode re-derives the fair share; an explicit cap
        that oversubscribes shrinks to the fair share with a WARN (unlike
        at startup, where the explicit value is honored with a WARN —
        membership changes are dynamic-fleet territory and the total is
        the contract there)."""
        n = max(1, len(self._active))
        fair = max(1, self._cfg.buffer_total_max_bytes // n)
        if self._cfg.buffer_max_bytes_per_destination > 0:
            explicit = self._cfg.buffer_max_bytes_per_destination
            if n * explicit > self._cfg.buffer_total_max_bytes:
                log.warning(
                    "buffer_max_bytes_per_destination=%d x %d active destinations exceeds "
                    "buffer_total_max_bytes=%d; shrinking per-destination cap to fair share %d",
                    explicit,
                    n,
                    self._cfg.buffer_total_max_bytes,
                    fair,
                )
                self._per_dest_cap = fair
            else:
                self._per_dest_cap = explicit
        else:
            self._per_dest_cap = fair

    # ------------------------------------------------------------------ #
    # Destination lifecycle (viaduck/lifecycle.py; poll-thread only)
    # ------------------------------------------------------------------ #

    def set_suspended(self, dest_ids: set[str]) -> None:
        """Replace the suspended set (paused/retired destinations — no
        flush submissions). Draining destinations must NOT be passed."""
        with self._lock:
            self._suspended = set(dest_ids)

    def discard_buffer(self, dest_id: str) -> int:
        """Lifecycle pause/retire: drop the live buffer and rewind the read
        position to the persisted cursor — deliberately the same semantics
        as _on_flush_failure (a pause is a controlled crash for this
        destination; the range is durable in the source and re-read from
        the cursor on resume). Epoch bump discards reads that overlapped
        the transition. Returns the number of rows dropped. An in-flight
        flush is left to finish naturally: completing an already-submitted
        write and advancing the cursor is strictly better than aborting
        mid-write."""
        with self._lock:
            dropped = self._buffers[dest_id]
            if dropped.rows == 0 and self._position[dest_id] <= self._flushed[dest_id]:
                return 0
            self._buffers[dest_id] = _Buffer()
            self._position[dest_id] = self._flushed[dest_id]
            self._epoch[dest_id] += 1
            self._position_dirty_since[dest_id] = None
            metrics.delivery_buffer_rows.labels(destination=dest_id).set(0)
            metrics.delivery_buffer_bytes.labels(destination=dest_id).set(0)
            metrics.delivery_buffer_total_bytes.set(self._total_bytes_locked())
            metrics.lifecycle_discarded_rows_total.labels(destination=dest_id).inc(dropped.rows)
            return dropped.rows

    def last_error(self, dest_id: str) -> str | None:
        """Last recorded delivery error for a destination (None after a
        successful flush). The lifecycle tracker uses this to distinguish
        a drain that flushed out from one that went "clean" via a flush
        failure's position rewind — retiring on the latter abandons the
        rewound range."""
        with self._lock:
            return self._last_error.get(dest_id)

    def is_clean(self, dest_id: str) -> bool:
        """True when nothing is buffered or in flight and the read position
        equals the durable cursor — the drain-complete condition."""
        with self._lock:
            return (
                dest_id not in self._inflight
                and self._buffers[dest_id].rows == 0
                and self._position[dest_id] <= self._flushed[dest_id]
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
