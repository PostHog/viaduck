"""Destination lifecycle state machine.

Each destination has an operator-intent state, stored per-DESTINATION (not
per-instance — pausing a destination means pausing it everywhere) in the
``viaduck.destination_lifecycle`` table:

- ``active``    — normal delivery.
- ``paused``    — no reads, no flushes; buffered data is DISCARDED (it is
  durable in the source; holding it buys nothing) and the pooled DuckDB
  connection is evicted, so a paused destination costs ~nothing. The cursor
  stays at the last flushed position; resuming re-reads from there —
  semantically a controlled crash for that destination, reusing the
  crash-recovery path that already exists.
- ``draining``  — no NEW reads, but buffered data flushes out so the
  delivered position catches up to the read position; once drained the
  connection is evicted. Reversible: back to ``active`` resumes gap-free.
  This is the pre-retirement step (and, later, what CP-side removal maps
  to) so retirement never discards data that was already read.
- ``retired``   — terminal. Excluded from everything at startup; a re-add
  is a brand-new destination (re-seeded per ``seed_mode``).

Two load-bearing rules:

1. **Absent row = active.** Existing deployments need no backfill and the
   legacy static destinations are covered from the first deploy.
2. **Viaduck never writes ``retired``.** Retirement is an explicit operator
   ack — an UPDATE through the documented SQL, with ``updated_by`` saying
   who. ``StateManager.set_lifecycle_state`` refuses the value; the only
   writer of ``retired`` is a human. An unknown state value in the table is
   treated as ``paused`` with an ERROR log: don't deliver on semantics we
   don't understand, don't discard anything either.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from viaduck import metrics

log = logging.getLogger(__name__)

ACTIVE = "active"
PAUSED = "paused"
DRAINING = "draining"
RETIRED = "retired"

VALID_STATES = frozenset({ACTIVE, PAUSED, DRAINING, RETIRED})

# States viaduck code may write. RETIRED is deliberately absent — see the
# module docstring. Enforced in StateManager.set_lifecycle_state.
WRITABLE_STATES = frozenset({ACTIVE, PAUSED, DRAINING})


def normalize(state: str | None, dest_id: str, *, log_unknown: bool = False) -> str:
    """Map a raw table value to an effective state (absent row = active,
    unknown value = paused). The unknown-state ERROR is logged only when
    log_unknown is set — the tracker sets it on TRANSITIONS so one bad row
    doesn't emit an ERROR every poll cycle forever."""
    if state is None:
        return ACTIVE
    if state in VALID_STATES:
        return state
    if log_unknown:
        log.error(
            "Destination %s has unknown lifecycle state %r — treating as 'paused' "
            "(no delivery, nothing discarded) until the row is fixed",
            dest_id,
            state,
        )
    return PAUSED


@dataclass
class LifecycleTracker:
    """Per-cycle view of destination lifecycle states plus the transition
    actions they imply. Owned by the main poll loop (single-threaded); the
    delivery manager and pool are told what to do, they hold no lifecycle
    state of their own.
    """

    dest_ids: list[str]
    _states: dict[str, str] = field(default_factory=dict)
    # Destinations whose pooled connection we already evicted for the
    # current non-active stint. Latched only once the destination is CLEAN
    # (nothing in flight): an in-flight flush's retry loop re-creates the
    # pool entry after an early evict, so latching at transition time
    # would leave a paused destination holding a heavy catalog connection
    # for the whole stint. Cleared when the destination returns to active.
    _evicted: set[str] = field(default_factory=set)
    # Draining destinations whose completion we already reported (one log
    # per stint, honest about HOW the drain ended — see apply()).
    _drain_reported: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._states = {d: ACTIVE for d in self.dest_ids}

    # -- queries -------------------------------------------------------------

    def state(self, dest_id: str) -> str:
        return self._states.get(dest_id, ACTIVE)

    def readable_ids(self) -> list[str]:
        """Destinations that participate in CDC reads this cycle."""
        return [d for d in self.dest_ids if self._states.get(d, ACTIVE) == ACTIVE]

    def suspended_ids(self) -> set[str]:
        """Destinations whose flushes must not run (paused/retired).
        Draining is NOT suspended — draining exists to flush out."""
        return {d for d in self.dest_ids if self._states.get(d, ACTIVE) in (PAUSED, RETIRED)}

    def states(self) -> dict[str, str]:
        return dict(self._states)

    # -- cycle update --------------------------------------------------------

    def apply(self, raw_states: dict[str, str | None], delivery, dest_pool, state_mgr=None) -> None:
        """Absorb freshly loaded lifecycle rows and perform the transition
        actions. Called once per poll cycle from the main loop.

        Buffers are discarded on every cycle in paused/retired (cheap
        no-op once empty — and it re-drops anything a raced read landed);
        the pooled connection is evicted only once the destination is
        CLEAN, because an in-flight flush re-creates the pool entry on its
        next retry attempt. A retired destination's cursor rows are
        severed each cycle (idempotent; closes the resurrect race where a
        completing in-flight flush upserts the row back).
        """
        for dest_id in self.dest_ids:
            raw = raw_states.get(dest_id)
            new = normalize(raw, dest_id)
            old = self._states.get(dest_id, ACTIVE)
            if new != old:
                # Transition-time logging only (a bad row must not ERROR
                # every cycle forever). Re-run normalize loudly so the
                # unknown-state ERROR fires exactly once per change.
                normalize(raw, dest_id, log_unknown=True)
                log.warning(
                    "Destination %s lifecycle: %s -> %s",
                    dest_id,
                    old,
                    new,
                )
                self._states[dest_id] = new
                metrics.set_destination_lifecycle(dest_id, new, VALID_STATES)

            if new == ACTIVE:
                # Returning to active: nothing to tear down. The next read
                # plan picks the cursor up where the last flush left it.
                self._evicted.discard(dest_id)
                self._drain_reported.discard(dest_id)
                continue

            if new in (PAUSED, RETIRED):
                # Controlled crash: drop what's buffered (durable in the
                # source; the cursor was only ever advanced on flush) and
                # release the destination's real resources.
                discarded = delivery.discard_buffer(dest_id)
                if discarded:
                    log.warning(
                        "Destination %s (%s): discarded %d buffered rows (re-read from cursor on resume)",
                        dest_id,
                        new,
                        discarded,
                    )
                if dest_id not in self._evicted and delivery.is_clean(dest_id):
                    dest_pool.evict(dest_id)
                    self._evicted.add(dest_id)
                if new == RETIRED and state_mgr is not None:
                    # Sever the resume point (re-add = new tenant = fresh
                    # seed). Idempotent per cycle; also run at startup for
                    # destinations excluded as retired.
                    try:
                        state_mgr.delete_destination_state(dest_id)
                    except Exception:
                        log.warning(
                            "Could not sever cursor rows for retired %s; will retry next cycle", dest_id, exc_info=True
                        )

            elif new == DRAINING:
                # Flushes continue; evict the connection only once the
                # buffer has fully drained and nothing is in flight. Be
                # honest about HOW the drain ended: a flush failure rewinds
                # the position to the cursor, which also reads as "clean" —
                # but the rewound range was NOT delivered, and draining
                # excludes the destination from re-reads. Retiring on that
                # signal abandons it; resuming re-reads it.
                if dest_id not in self._evicted and delivery.is_clean(dest_id):
                    dest_pool.evict(dest_id)
                    self._evicted.add(dest_id)
                    if dest_id not in self._drain_reported:
                        self._drain_reported.add(dest_id)
                        err = delivery.last_error(dest_id)
                        if err:
                            log.warning(
                                "Destination %s: drain ended via a flush-failure rewind (%s) — "
                                "the rewound range was NOT delivered. Resume to re-read it "
                                "before retiring, or retire accepting the gap-to-cursor.",
                                dest_id,
                                err,
                            )
                        else:
                            log.warning("Destination %s: drain complete (flushed out), connection released", dest_id)

    def export_metrics(self) -> None:
        """Publish the state gauge for every destination (called once at
        startup so dashboards see the full fleet, not just transitions)."""
        for dest_id in self.dest_ids:
            metrics.set_destination_lifecycle(dest_id, self._states.get(dest_id, ACTIVE), VALID_STATES)
