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
from viaduck.apply import FlushDeadlineExceeded, append_only, apply_full_cdc
from viaduck.config import ConfigError

if TYPE_CHECKING:
    from viaduck.config import DeliveryConfig
    from viaduck.destination import DestinationPool
    from viaduck.state import StateManager

log = logging.getLogger(__name__)

# Growth gate for the adaptive flush-target controller: additive increase
# only when the flushed batch filled at least this fraction of the current
# target. A batch well below the target completing fast is evidence about
# THAT batch size, not the target's — growing on it would let quiet-period
# trickle flushes re-inflate a learned-down target (see _adapt_flush_target).
_ADAPT_GROWTH_MIN_FILL = 0.7


@dataclass
class _Buffer:
    # (table, cov, hi) per buffered read/slice entry. `cov` is the coverage
    # watermark a partial swap may persist: the slice-cursor rule (tla/
    # Viaduck.tla, EntryCoverageInvariant) guarantees no row in a LATER
    # entry has snapshot <= an earlier entry's cov, so the durable cursor
    # never passes undelivered data. `hi` is the entry's own max snapshot
    # (hi > cov is the merged-file straddle shape) — the commit-time replay
    # drop keys on hi, never on cov. For legacy chunk-contiguous entries
    # cov == hi == chunk end.
    # INVARIANT: entries are non-decreasing by cov (equality is reachable
    # at unit boundaries and when a slice's later-min lands at lo+1) —
    # the poll thread is the only writer and every position REWIND clears
    # the buffer and bumps the epoch.
    entries: list[tuple[pa.Table, int, int]] = field(default_factory=list)
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
    - I2: all per-destination dicts share one monotonically-growing
      "ever-member" key set; constructor and add_destination write all
      of them atomically under the lock. Two exceptions by design:
      ``_circuit_open_until`` is sparse (only open circuits, always read
      via ``.get``-default) and circuit state is activation-scoped
      (add_destination resets it — a re-added destination gets a fresh
      probe, unlike position/epoch which max-merge).
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
        per_dest_cap_overrides: dict[str, int] | None = None,
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
        # Per-destination OVERRIDES (DestinationConfig.buffer_max_bytes):
        # destination volumes are heavily skewed, so the head tenants can
        # carry explicit caps while the global value serves the tail. An
        # override is a fixed contract — membership-change recompute never
        # touches it, only the default for non-override destinations.
        self._cap_overrides: dict[str, int] = {d: c for d, c in (per_dest_cap_overrides or {}).items() if c > 0}
        if cfg.buffer_max_bytes_per_destination > 0:
            self._per_dest_cap = cfg.buffer_max_bytes_per_destination
        else:
            self._per_dest_cap = max(1, cfg.buffer_total_max_bytes // max(1, len(assigned_ids)))
        # Pre-thread: workers/poll/reconciler start after __init__ returns,
        # so the _locked helper is safe to call here without the lock.
        cap_sum = sum(self._cap_for_locked(d) for d in assigned_ids)
        assigned_overrides = sum(1 for d in assigned_ids if d in self._cap_overrides)
        if cap_sum > cfg.buffer_total_max_bytes:
            # With destination-local read gating, the sum of per-dest caps
            # bounds transient absorption; the total watermark still forces
            # flushes on aggregate. Oversubscription is a deliberate
            # throughput posture (see values comments) — one loud line so
            # the arithmetic is never a surprise.
            log.warning(
                "per-destination caps sum to %d (%d override(s) + default %d x %d) "
                "exceeding buffer_total_max_bytes=%d; watermark force-flushes govern the aggregate",
                cap_sum,
                assigned_overrides,
                self._per_dest_cap,
                len(assigned_ids) - assigned_overrides,
                cfg.buffer_total_max_bytes,
            )
        log.info(
            "Per-destination buffer cap: default %d bytes (%s), %d assigned override(s), %d destinations",
            self._per_dest_cap,
            "explicit" if cfg.buffer_max_bytes_per_destination > 0 else "auto: buffer_total_max_bytes / N",
            assigned_overrides,
            len(assigned_ids),
        )
        # Startup semantics above are unchanged (explicit oversubscription
        # only warns — existing deployments keep their contract);
        # _recompute_per_dest_cap_locked bounds the NON-OVERRIDE share on
        # membership changes. With overrides present the effective
        # transient-absorption bound is buffer_total_max_bytes plus each
        # override's excess over fair share — the WATERMARK's forced
        # flushes remain the aggregate governor either way.
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
        # Adaptive flush-size target: the bytes flush-trigger threshold,
        # per destination (see DeliveryConfig.flush_adaptive). Starts at
        # the global cap and AIMD-adapts to observed flush duration in
        # _adapt_flush_target. In-memory only — a restart re-learns a
        # contended destination's target in ~log2(cap/floor) flushes.
        self._flush_target: dict[str, int] = {d: cfg.flush_max_bytes for d in assigned_ids}
        for d in assigned_ids:
            # Seed the gauge so "target == ceiling" is visible before the
            # first data flush — a dashboard reading "pinned at floor =
            # contended" must be able to tell "never flushed" apart.
            metrics.dest_flush_target_bytes.labels(destination=d).set(cfg.flush_max_bytes)
        # Lifecycle-suspended destinations (paused/retired): no flush
        # submissions. Draining destinations are NOT here — draining exists
        # to flush out. Owned by the poll thread via set_suspended().
        self._suspended: set[str] = set()
        # Flush circuit breaker (delivery.flush_circuit_failures /
        # flush_circuit_max_seconds): consecutive-failure count and, while
        # open, the monotonic time submissions may resume (a probe). A
        # broken destination without this cycles read->buffer->fail->
        # rewind->re-read forever — burning a shared flush worker for
        # minutes per attempt plus its per-cycle chunk quota, i.e. taxing
        # every peer. The breaker converts that to a bounded idle cost:
        # submissions pause, reads self-cap at the per-destination buffer
        # ceiling, and a probe closes the circuit on success.
        self._flush_failures: dict[str, int] = {d: 0 for d in assigned_ids}
        self._circuit_open_until: dict[str, float] = {}
        for d in assigned_ids:
            # Seed the gauge so "never opened" reads 0, not absent — same
            # alerting-contract reasoning as dest_flush_target_bytes above.
            metrics.delivery_circuit_open.labels(destination=d).set(0)
        # Overall per-flush wall-clock deadline (delivery.flush_deadline_seconds;
        # 0 derives 2x flush_interval). Bounds the retry loop's wall time —
        # the attempt budget alone permits ~5.5 min of backoff sleeps per
        # flush. Disabled when the derived value is <= 0 (flush_interval=0).
        derived_deadline = (
            cfg.flush_deadline_seconds if cfg.flush_deadline_seconds > 0 else 2 * cfg.flush_interval_seconds
        )
        self._flush_deadline_s: float | None = derived_deadline if derived_deadline > 0 else None

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

    def buffer(
        self, dest_id: str, table: pa.Table, through_snapshot: int, epoch: int | None = None, hi: int | None = None
    ) -> None:
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
            buf.entries.append((table, through_snapshot, hi if hi is not None else through_snapshot))
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

    def _cap_for_locked(self, dest_id: str) -> int:
        """Effective queue cap for a destination: explicit override wins,
        else the (possibly membership-recomputed) default."""
        return self._cap_overrides.get(dest_id, 0) or self._per_dest_cap

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
            paused = self._dest_bytes_locked(dest_id) >= self._cap_for_locked(dest_id)
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
            return all(self._dest_bytes_locked(d) >= self._cap_for_locked(d) for d in self._active)

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
                if not shutdown and self._circuit_open_locked(dest_id, now):
                    # Circuit open: submissions paused until the backoff
                    # elapses (then this gate passes and the probe goes
                    # out). Reads for the destination keep flowing under
                    # the normal buffer-cap rules. Shutdown bypasses the
                    # gate: drain() must attempt every destination (a
                    # recovered destination drains cleanly instead of
                    # burning the whole drain timeout and abandoning
                    # rows; a failed drain probe simply re-opens).
                    continue
                trigger = self._trigger_for_locked(dest_id, now, shutdown, over_watermark)
                if trigger is None:
                    continue
                buf = self._buffers[dest_id]
                # Flush-batch slicing: one swap takes at most
                # flush_batch_max_rows AND at most the adaptive per-dest
                # byte target, cut at CHUNK boundaries — and, when the
                # FIRST entry alone exceeds a cap, cut WITHIN it
                # (zero-copy pa.Table.slice). Without the rows cap, a slow
                # flush lets the buffer pile up and the next swap takes
                # everything — the feedback loop that produced 170-440K-row
                # batches and drove the fork's native layer into
                # buffer-manager corruption (2026-07-29). Without the BYTE
                # cut, the adaptive target would gate only the trigger
                # while backlogged swaps stay rows-cap-sized — the
                # controller would converge its target and actuate nothing
                # in exactly the contended-catalog regime it exists for.
                # And without the WITHIN-entry cut, a single destination-
                # heavy read unit arrives as one unsliceable entry that
                # bypasses both caps at once: the 2026-08 team-2 episode
                # (~100K-op/1.4GB single-entry appends at 145-468s while
                # the adaptive target sat pinned at its floor, powerless).
                # Flush payload is priced by the S3 upload of its bytes
                # (append profiles: ~90% of statement latency), so p95/p99
                # flush duration IS payload size — the cap must bind
                # for every entry shape (up to row-size skew: the split is
                # rows-proportional, so a few giant variable-width rows
                # can overshoot the byte cap; a single row is the floor).
                # The remainder stays buffered with its age preserved,
                # and the "sliced" trigger keeps the pipeline of bounded
                # slices draining back-to-back.
                cap = self._cfg.flush_batch_max_rows
                byte_cap = self._flush_target[dest_id] if self._cfg.flush_adaptive else 0
                take = len(buf.entries)
                if cap > 0 or byte_cap > 0:
                    taken_rows = 0
                    taken_bytes = 0
                    take = 0
                    for tbl, _cov, _hi in buf.entries:
                        if take > 0 and (
                            (cap > 0 and taken_rows + tbl.num_rows > cap)
                            or (byte_cap > 0 and taken_bytes + tbl.nbytes > byte_cap)
                        ):
                            break
                        taken_rows += tbl.num_rows
                        taken_bytes += tbl.nbytes
                        take += 1
                sliced = buf.entries[:take]
                remainder = buf.entries[take:]
                # Within-entry slice: the loop above always admits the
                # first entry whole. If that entry alone is over a cap,
                # split it and leave the tail (SAME cov/hi — the entry's
                # coverage is indivisible) at the buffer head. The head
                # slice's flush must NOT persist the entry's cov: rows
                # covered by cov are only fully delivered when the tail
                # lands, so `through` stays at the durable flushed
                # position (strictly conservative vs the slice-cursor
                # rule / EntryCoverageInvariant — the cursor advances
                # less eagerly than the model permits, never more).
                entry_split = False
                if take == 1:
                    tbl0, cov0, hi0 = sliced[0]
                    over_rows = cap > 0 and tbl0.num_rows > cap
                    over_bytes = byte_cap > 0 and tbl0.nbytes > byte_cap
                    if (over_rows or over_bytes) and tbl0.num_rows > 1:
                        n = tbl0.num_rows
                        if over_rows:
                            n = min(n, cap)
                        if over_bytes and tbl0.nbytes > 0:
                            n = min(n, max(1, (tbl0.num_rows * byte_cap) // tbl0.nbytes))
                        if 0 < n < tbl0.num_rows:
                            sliced = [(tbl0.slice(0, n), cov0, hi0)]
                            remainder = [(tbl0.slice(n), cov0, hi0)] + remainder
                            entry_split = True
                tables = [tbl for tbl, _cov, _hi in sliced]
                if entry_split:
                    # Partial-entry swap: no coverage completes — persist
                    # NOTHING (through=None). Using self._flushed here is
                    # wrong: the retention clamp is the one writer that
                    # raises _flushed WITHOUT delivering, so a head slice
                    # carrying that value into the commit path would run
                    # DropCoveredPrefix at the clamp floor and silently
                    # drop the undelivered tail (adversarial-review
                    # HIGH-1, reproduced). None makes the commit path skip
                    # cursor persist, position restore, AND the covered-
                    # prefix drop — a split flush contributes destination
                    # rows and adaptive/circuit evidence, nothing else.
                    # (NOTE: pa.Table.slice is zero-copy; slice nbytes can
                    # over-report shared buffers, so buffer-bytes gauges
                    # for a split are approximate until the entry drains.)
                    through = None
                elif remainder:
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
                for entry in remainder:
                    rem_buf.entries.append(entry)
                    rem_buf.rows += entry[0].num_rows
                    rem_buf.bytes += entry[0].nbytes
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

    def _circuit_open_locked(self, dest_id: str, now: float) -> bool:
        """True while the destination's flush circuit breaker is open AND
        the resubmit backoff has not elapsed. Once `now` passes open_until,
        the destination is eligible for a single probe flush (the in-flight
        guard keeps it to one); the probe's success closes the circuit, its
        failure re-opens it with the next backoff step."""
        return now < self._circuit_open_until.get(dest_id, 0.0)

    def _circuit_backoff_seconds_locked(self, dest_id: str) -> float:
        """Resubmit delay for the current consecutive-failure count:
        flush_interval x 2^(failures - threshold), floored at 1s (so the
        breaker still bites at flush_interval_seconds=0, where the raw
        formula collapses to 0 and never suppresses a submission), capped
        at flush_circuit_max_seconds. At the threshold itself this is one
        flush_interval — a flapping destination gets a quick first retry,
        then exponential isolation."""
        steps = max(0, self._flush_failures[dest_id] - self._cfg.flush_circuit_failures)
        raw = self._cfg.flush_interval_seconds * (2**steps)
        return min(max(raw, 1.0), self._cfg.flush_circuit_max_seconds)

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
        if has_data and self._dest_bytes_locked(dest_id) >= self._cap_for_locked(dest_id):
            return "memory"
        if has_data and buf.sliced_remainder:
            # The tail of a sliced pile drains as soon as the in-flight
            # slice completes — never on the interval cadence (slicing
            # review F1: a mid-size remainder below the rows/bytes
            # thresholds would otherwise stall up to flush_interval).
            return "sliced"
        if has_data and buf.rows >= self._cfg.flush_max_rows:
            return "rows"
        # Bytes trigger consults the ADAPTIVE per-destination target, not
        # the global cap — flush_max_bytes survives as the target's ceiling
        # and initial value. When the target has shrunk below one CDC
        # chunk, the trigger fires per chunk: one chunk is the floor by
        # construction (slicing never splits a chunk either).
        if has_data and buf.bytes >= self._flush_target[dest_id]:
            return "bytes"
        dirty_since = self._position_dirty_since[dest_id]
        age_start = buf.first_buffered_at if has_data else dirty_since
        if age_start is not None and (now - age_start) >= self._cfg.flush_interval_seconds:
            return "interval"
        return None

    def _adapt_flush_target(self, dest_id: str, duration: float, batch_bytes: int, *, failed: bool = False) -> None:
        """AIMD controller on flush duration (millpond's backpressure
        precedent, per-destination). The sustainable batch size is a
        property of the destination catalog's commit contention — on a
        contended catalog, throughput DECREASES with batch size (longer
        write+commit window → more peer-commit collisions → more
        DuckLake-internal OCC retries, each re-running multi-second
        catalog SQL) — so no global flush_max_bytes serves both a busy
        and an idle catalog. Called by the flush worker after every DATA
        flush (position-only persists carry no signal): slower than the
        high bound → halve (floored at flush_adaptive_min_bytes; the
        effective floor is one CDC chunk since neither the bytes trigger
        nor the swap byte-cut splits a chunk); faster than the low bound
        AND the batch nearly filled the current target → additive step
        up (capped at flush_max_bytes); otherwise hold. The fill
        condition is what makes growth evidence-based: a tiny interval
        flush finishing in <1s says nothing about whether a target-sized
        batch is sustainable, and without it a quiet period walks a
        learned-down target back to the cap, so the next burst re-runs
        the oversize-flush/drop/re-read cycle from scratch. Failures
        never grow. Converges from a cold start in ~log2(cap/floor)
        flushes and re-probes upward as contention subsides — but only
        under enough traffic to fill the target, which is the only time
        the target matters."""
        cfg = self._cfg
        if not cfg.flush_adaptive:
            return
        # Floor clamped to the ceiling: an operator lowering flush_max_bytes
        # below flush_adaptive_min_bytes shouldn't need a second knob.
        floor = min(cfg.flush_adaptive_min_bytes, cfg.flush_max_bytes)
        with self._lock:
            cur = self._flush_target[dest_id]
            if duration > cfg.flush_adaptive_high_seconds:
                new = max(floor, cur // 2)
            elif (
                not failed and duration < cfg.flush_adaptive_low_seconds and batch_bytes >= cur * _ADAPT_GROWTH_MIN_FILL
            ):
                new = min(cfg.flush_max_bytes, cur + cfg.flush_adaptive_step_bytes)
            else:
                new = cur
            self._flush_target[dest_id] = new
        metrics.dest_flush_target_bytes.labels(destination=dest_id).set(new)
        if new != cur:
            log.info(
                "Adaptive flush target for %s: %d -> %d bytes (%s flush took %.1fs)",
                dest_id,
                cur,
                new,
                "failed" if failed else "successful",
                duration,
            )

    def _flush(self, dest_id: str, tables: list[pa.Table], through: int | None, trigger: str) -> None:
        """Worker: FlushCommit / FlushFail.

        `through=None` marks a PARTIAL-ENTRY flush (within-entry slice):
        the destination write happens and counts as adaptive/circuit
        evidence, but no cursor state moves — no durable advance, no
        _flushed/position update, no covered-prefix drop, no error clear.
        The entry's coverage persists only with its completing slice.
        Consequence (accepted, at-least-once): every committed head slice
        widens the duplicate window on a later failure to at most one
        entry — a mid-drain failure re-reads the whole entry and
        re-applies landed slices (append_only duplicates them).
        """
        t0 = time.monotonic()
        batch_bytes = sum(t.nbytes for t in tables)
        deadline = t0 + self._flush_deadline_s if self._flush_deadline_s is not None else None
        circuit_was_open = False
        # apply_done tracks whether the destination-write phase completed (or
        # didn't exist — position-only flushes have none). Circuit failures
        # count ONLY apply-phase failures: a cursor-persist failure is shared
        # cursor-store infrastructure (PG), not a sick destination, and must
        # not trip the breaker for it (same stance as lifecycle: a PG blip
        # must not punish destinations).
        apply_done = not tables
        try:
            ops_count = 0
            if tables:
                batch = tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")
                if self._full_cdc:
                    ops_count = apply_full_cdc(
                        self._pool, dest_id, batch, self._key_columns, stop_event=self._stopping, deadline=deadline
                    )
                else:
                    ops_count = append_only(self._pool, dest_id, batch, stop_event=self._stopping, deadline=deadline)
                apply_done = True
            # Cursor persist AFTER the destination commit (the gap is the
            # spec's CrashDuringFlush window). A short retry here avoids
            # invoking the full failure path (buffer drop + healthy-catalog
            # evict + range re-read) for a transient PG blip when the
            # destination write already landed.
            with self._lock:
                cumulative = self._rows_replicated[dest_id] + ops_count
            if through is not None:
                self._advance_cursor_with_retry(dest_id, through, cumulative)
            duration = time.monotonic() - t0
            type_counts = self._change_type_counts(batch) if tables else None
            with self._lock:
                if through is not None:
                    self._flushed[dest_id] = max(self._flushed[dest_id], through)
                # A lifecycle discard (pause/retire) may have rewound the
                # position below `through` while this flush was in flight.
                # The flush SUCCEEDED, so the destination has the range —
                # leaving position behind would re-read and re-apply it on
                # resume (deterministic duplicates in append_only). Restore
                # position >= flushed; epoch bump discards any read that
                # overlapped the restore.
                if through is not None and self._position[dest_id] < through:
                    self._position[dest_id] = through
                    self._epoch[dest_id] += 1
                # Pair-split phantom fix (TLA witness in tla/Viaduck.tla:
                # BufferRead, FlushStart, PauseDest, BufferRead, FlushCommit,
                # FlushStart, CrashDuringFlush): the rewind above plus a
                # racing zombie commit can leave REPLAYED entries in the
                # buffer whose ranges this commit already covered. A later
                # sliced flush can split a re-buffered conflicting pair
                # (insert in one slice, its delete in a later slice) across
                # a crash boundary: the insert commits, the delete's slice
                # is dropped, and the cursor — already past it — never
                # re-reads it: a permanent phantom row. Drop the covered
                # prefix of the live buffer now: entries with
                # through <= this commit's through are redundant replays by
                # construction (the commit covered their range), and
                # dropping them is the pause's controlled-crash semantics
                # completed late. The drop keys on the entry's `hi` (its own
                # max snapshot), per the model's DropCoveredPrefix — for
                # contiguous units hi == cov; straddle entries (hi > cov)
                # are NOT droppable by cov, and the model proves why.
                buf = self._buffers[dest_id]
                if through is not None and buf.entries:
                    drop = 0
                    for _tbl, _cov, entry_hi in buf.entries:
                        if entry_hi > through:
                            break
                        drop += 1
                    if drop:
                        dropped_rows = sum(t.num_rows for t, _, _ in buf.entries[:drop])
                        buf.entries = buf.entries[drop:]
                        buf.rows -= dropped_rows
                        buf.bytes = sum(t.nbytes for t, _, _ in buf.entries)
                        if not buf.entries:
                            buf.first_buffered_at = None
                            buf.sliced_remainder = False
                        metrics.delivery_buffer_rows.labels(destination=dest_id).set(buf.rows)
                        metrics.delivery_buffer_bytes.labels(destination=dest_id).set(buf.bytes)
                        metrics.delivery_covered_replays_dropped_total.labels(destination=dest_id).inc(drop)
                        log.info(
                            "Dropped %d covered replay entries (%d rows) for %s after flush through %d "
                            "(pause/zombie race — the commit covered their ranges)",
                            drop,
                            dropped_rows,
                            dest_id,
                            through,
                        )
                self._rows_replicated[dest_id] = cumulative
                # Clear the error only when this flush is at/ahead of the
                # cursor. A zombie flush (through < flushed — a retention
                # clamp landed mid-flight) must not clear the clamp's loss
                # note or regress the cursor gauge.
                flushed_after = self._flushed[dest_id]
                if through is not None and through >= flushed_after:
                    self._last_error[dest_id] = None
                if type_counts is not None:
                    applied = self._applied[dest_id]
                    applied["inserts"] += type_counts[0]
                    applied["updates"] += type_counts[1]
                    applied["deletes"] += type_counts[2]
                # Flush succeeded: reset the consecutive-failure count and
                # close the circuit if it was open (this WAS the probe).
                # Only a DATA flush counts as probe evidence: a position-only
                # persist (tables empty) never touches the destination, so it
                # can't prove a broken destination healed — it would
                # phantom-close the circuit on PG health alone.
                if tables:
                    circuit_was_open = dest_id in self._circuit_open_until
                    self._flush_failures[dest_id] = 0
                    self._circuit_open_until.pop(dest_id, None)
                    if circuit_was_open:
                        metrics.delivery_circuit_open.labels(destination=dest_id).set(0)
            metrics.delivery_flushes_total.labels(destination=dest_id, trigger=trigger).inc()
            metrics.dest_last_snapshot_id.labels(destination=dest_id).set(flushed_after)
            if tables:
                # Data flushes only: empty position-only persists must not
                # report write latency or readiness "replication" signals.
                metrics.delivery_flush_seconds.labels(destination=dest_id).observe(duration)
                # dest_write_seconds continuity: pre-buffering dashboards
                # observe per-destination write latency under this name.
                metrics.dest_write_seconds.labels(destination=dest_id).observe(duration)
                self._adapt_flush_target(dest_id, duration, batch_bytes)
                if self._on_flush_success is not None:
                    self._on_flush_success()
                log.info(
                    "Flushed %s: %d ops through snapshot %s (trigger=%s, %.2fs)",
                    dest_id,
                    ops_count,
                    "PARTIAL-ENTRY (cursor held)" if through is None else through,
                    trigger,
                    duration,
                )
                if circuit_was_open:
                    log.info("Flush circuit closed for %s (probe flush succeeded)", dest_id)
        except Exception as exc:
            duration = time.monotonic() - t0
            log.exception("Flush failed for destination %s", dest_id)
            # Invariant-restoring reset FIRST: if the bookkeeping below
            # (PG write, pool close) also fails, the position/buffer state
            # must already be consistent — otherwise the dropped range
            # would never be re-read.
            self._on_flush_failure(dest_id)
            metrics.errors_total.labels(type="dest_write", destination=dest_id).inc()
            metrics.delivery_buffers_dropped_total.labels(destination=dest_id).inc()
            if isinstance(exc, FlushDeadlineExceeded):
                metrics.delivery_flush_deadlines_total.labels(destination=dest_id).inc()
            # Circuit breaker: count the consecutive failure ONLY when the
            # apply phase (destination write) is what failed — a cursor-
            # persist failure is shared PG infrastructure, not a sick
            # destination. At the threshold, pause submissions for this
            # destination behind an exponential resubmit backoff. Without
            # this the failure loop below (rewind -> re-read -> resubmit) is
            # IMMEDIATE — a broken destination burns a flush worker for
            # minutes per attempt plus its per-cycle chunk quota, forever,
            # taxing every peer.
            if not apply_done:
                with self._lock:
                    self._flush_failures[dest_id] += 1
                    failures = self._flush_failures[dest_id]
                    if failures >= self._cfg.flush_circuit_failures:
                        backoff = self._circuit_backoff_seconds_locked(dest_id)
                        self._circuit_open_until[dest_id] = time.monotonic() + backoff
                        metrics.delivery_circuit_open.labels(destination=dest_id).set(1)
                        metrics.delivery_circuit_opens_total.labels(destination=dest_id).inc()
                        log.warning(
                            "Flush circuit OPEN for %s after %d consecutive failures; "
                            "flush submissions paused for %.0fs (probe afterwards). "
                            "Reads continue under the buffer cap; the range re-reads from the cursor.",
                            dest_id,
                            failures,
                            backoff,
                        )
            try:
                self._state.record_error(dest_id, f"Flush failed (trigger={trigger})")
            except Exception:
                log.exception("Could not record flush error for %s", dest_id)
            try:
                self._pool.evict(dest_id)
            except Exception:
                log.exception("Could not evict connection for %s", dest_id)
            if tables:
                # A SLOW failure still teaches the controller: a flush that
                # burned its retry budget for minutes was too big for this
                # catalog, and without shrinking here a destination whose
                # every oversized flush FAILS would never adapt down — the
                # exact wedge this controller exists to break (team-2,
                # 2026-07-30). failed=True suppresses growth so a fast
                # failure (connection blip) can't inflate the target. Last
                # in the handler: nothing after it may be skipped if the
                # controller ever raises — the evict above must run.
                self._adapt_flush_target(dest_id, duration, batch_bytes, failed=True)
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
                self._flush_target[dest_id] = self._cfg.flush_max_bytes
                metrics.dest_flush_target_bytes.labels(destination=dest_id).set(self._cfg.flush_max_bytes)
            # Circuit state is activation-scoped, not ever-member: a stopped,
            # fixed, re-added destination deserves a fresh probe immediately,
            # not its predecessor's open circuit (re-add after retire = new
            # tenant per lifecycle semantics). Genuinely-new ids initialize
            # clean by the same line.
            self._flush_failures[dest_id] = 0
            self._circuit_open_until.pop(dest_id, None)
            metrics.delivery_circuit_open.labels(destination=dest_id).set(0)
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
        the contract there). OVERRIDES are exempt: an explicit
        per-destination cap is a sizing contract, not a fair-share
        participant — only the default for non-override destinations is
        re-derived here, so with overrides present this bounds the
        NON-OVERRIDE share only; the aggregate is governed by the
        watermark's forced flushes, not by this recompute."""
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
