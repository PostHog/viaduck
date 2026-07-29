"""The C3 reconciler: applies the classified CP view to the running set.

Two predicates, three rules (design §4, v6):

    startable ∧ not running                          → activate
    running ∧ startable ∧ config differs             → restart
    running ∧ unmentioned for k consecutive
      CLEAN fetches                                  → deactivate

Everything else — mentioned-only (fenced/degraded), poisoned views,
failed fetches, unchanged config — is nothing. A fenced tenant keeps
its worker and fails flushes against the DB-side NOLOGIN fence until
the changed config arrives (rare, fast, operationally routine).

Threading: this object is owned by the POLL THREAD exclusively. The
drift thread publishes immutable views; ``apply()`` runs between poll
cycles at cycle boundaries. All mutable state here (absent streaks,
pending restarts, deferred evictions) is therefore single-threaded by
construction — the same discipline that keeps the delivery layer's
dict family safe.

Scope: DISCOVERED-origin destinations only. Statics come from the
chart, are never in the CP view, and never enter any evaluation here
(derive_absent enforces it; a CP-served id colliding with a static
routing value is excluded from activation by the same static-wins
predicate materialization applies).
"""

from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING

from viaduck import lifecycle, metrics
from viaduck.discovery import ClassifiedView, MappedDestination, derive_absent, materialize_one
from viaduck.registry import ORIGIN_DISCOVERED, RegistryCollisionError

if TYPE_CHECKING:
    from viaduck.config import ViaduckConfig

log = logging.getLogger(__name__)

# Activation budget (§4): each activation is several PG round-trips plus
# one fresh k8s secret read on the poll thread — a CP recovery serving
# hundreds of new tenants must not stall the cycle past the liveness
# grace. Spillover is deferred (pending reason "budget") and retried on
# the next fresh view; the heartbeat between activations keeps /healthz
# green through a storm.
_ACTIVATIONS_PER_CYCLE = 10

# _activate outcomes: only FAILED blocks applied_generation (a fired
# primitive that did not succeed). RETIRED and COLLISION are deliberate
# deferrals — operator intent and the rv-handoff debounce window
# respectively (design §4: deferrals are exposed on the pending gauge,
# never counted as failures).
_OK = "ok"
_RETIRED = "retired"
_COLLISION = "collision"
_FAILED = "failed"


class Reconciler:
    def __init__(
        self,
        cfg: ViaduckConfig,
        registry,
        delivery,
        dest_pool,
        tracker,
        state_mgr,
        *,
        static_routing_values: set[str],
        static_ids: set[str],
        baseline_mapped: dict[str, MappedDestination],
        src_head_fn,
        heartbeat=None,
    ):
        self._cfg = cfg
        self._disc = cfg.discovery
        self._registry = registry
        self._delivery = delivery
        self._pool = dest_pool
        self._tracker = tracker
        self._state = state_mgr
        self._static_rvs = static_routing_values
        self._static_ids = static_ids
        self._src_head_fn = src_head_fn
        self._heartbeat = heartbeat

        self._last_view: ClassifiedView | None = None
        # id -> mapped content this process last APPLIED (rule 2's
        # comparison basis). Seeded from the startup baseline so a
        # reshard completing after boot reads as config-changed.
        self._applied_mapped: dict[str, MappedDestination] = dict(baseline_mapped)
        self._absent_streak: dict[str, int] = {}
        self._pending_restart: dict[str, MappedDestination] = {}
        self._last_restart_at: dict[str, float] = {}
        # Failures from pending-restart completions (fired primitives!)
        # observed since the last fresh view — folded into the failure
        # gauge so a failing restart is never invisible.
        self._deferred_failures = 0
        # Deferred deactivation side effects, processed once the id is
        # clean (nothing buffered or in flight): the pool evict must go
        # through the is-clean latch (an early evict lets the in-flight
        # flush's retry recreate the entry — and each evict/recreate
        # cycle fires the ~160MB fork leak), and metric label removal
        # must wait out in-flight ``.labels()`` calls (which re-create a
        # removed series frozen). Both are re-armed idempotently per
        # cycle and DISARMED by a subsequent activate.
        self._evict_pending: set[str] = set()
        self._label_pending: set[str] = set()
        # Transition-only logging state.
        self._suppressed_logged: set[str] = set()
        self._retired_refused_logged: set[str] = set()
        self._floor_logged = False

    # ------------------------------------------------------------------ #
    # Per-cycle entry point (poll thread)
    # ------------------------------------------------------------------ #

    def apply(self, view: ClassifiedView | None) -> None:
        """Reconcile the running set against the latest classified view.
        Called once per poll cycle, before the cycle's read planning, so
        an activation delivers within the same cycle and the
        retention-edge clamp covers a re-added stale cursor before its
        first read."""
        self._progress_deferred()
        if view is None or view is self._last_view:
            # No view yet, or a stale view (failed fetches republish
            # nothing; identity, never generation equality): all
            # counters frozen; only deferred work progresses.
            return
        self._last_view = view

        snap = self._registry.snapshot()
        running = frozenset(snap.discovered_ids() & snap.routable_ids)
        failures = self._deferred_failures
        self._deferred_failures = 0

        # Rule 3 bookkeeping first (deactivates before activates — the
        # rv-handoff order): absence streaks tick only on CLEAN views.
        stops: list[str] = []
        if not view.parse_poisoned:
            absent = derive_absent(view, snap)
            for did in absent:
                streak = self._absent_streak.get(did, 0) + 1
                self._absent_streak[did] = streak
                remaining = max(0, self._disc.absent_stop_fetches - streak)
                metrics.discovery_stop_countdown.labels(destination=did).set(remaining)
                if streak >= self._disc.absent_stop_fetches:
                    stops.append(did)
            for did in list(self._absent_streak):
                if did not in absent:
                    # Mentioned again (any classification) — reset.
                    self._absent_streak.pop(did, None)
                    try:
                        metrics.discovery_stop_countdown.remove(did)
                    except KeyError:
                        pass
            # Pending-restart cancellation: the tenant was deprovisioned
            # mid-reshard. The deactivate half already ran; completing
            # the swap would RESURRECT a deprovisioned tenant (and, with
            # its Secret gone, retry-fail forever). Immediate — the id
            # already survived the world telling us its config changed,
            # and a false cancel self-heals via rule 1 on re-mention.
            for did in list(self._pending_restart):
                if did not in view.entries:
                    del self._pending_restart[did]
                    self._applied_mapped.pop(did, None)
                    metrics.discovery_applied_total.labels(kind="stop").inc()
                    log.warning(
                        "Cancelled pending restart of %s: it left the CP view mid-swap "
                        "(deprovisioned during a reshard); cursor row retained",
                        did,
                    )

        # Mass-stop floor: availability guard (a false stop self-heals
        # from the cursor within retention), refuse-and-alert.
        # min_destinations guards MASS anomalies only (len > 1): a
        # single-id stop already survived k clean fetches of debounce,
        # and blocking the legitimate deprovision of the last discovered
        # tenant forever is worse than allowing it (stage-4 review).
        floor_refused = 0
        if stops:
            limit = max(1, math.ceil(self._disc.stop_floor_fraction * len(running)))
            over_fraction = len(stops) > limit
            under_min = len(stops) > 1 and (len(running) - len(stops)) < self._disc.min_destinations
            if over_fraction or under_min:
                if not self._floor_logged:
                    self._floor_logged = True
                    log.error(
                        "Refusing to deactivate %d of %d discovered destination(s) (floor: limit=%d, "
                        "min_destinations=%d) — starts/restarts still apply. Logged once per episode; "
                        "watch viaduck_reconciler_pending{reason='floor'}",
                        len(stops),
                        len(running),
                        limit,
                        self._disc.min_destinations,
                    )
                floor_refused = len(stops)
                stops = []
            else:
                self._floor_logged = False
        else:
            self._floor_logged = False
        for did in stops:
            self._deactivate(did, kind="stop")

        # Rules 1 and 2, under the activation budget.
        suppressed = 0
        budget_deferred = 0
        rate_capped = 0
        retired_deferred = 0
        collision_deferred = 0
        activations = 0
        for dest_id, entry in view.entries.items():
            if not entry.startable:
                continue
            m = entry.mapped
            if not self._cfg.is_assigned(dest_id):
                continue
            if str(m.team_id) in self._static_rvs or dest_id in self._static_ids:
                # Static wins — deterministic predicate, log on transition.
                suppressed += 1
                if dest_id not in self._suppressed_logged:
                    self._suppressed_logged.add(dest_id)
                    log.info("Discovered destination %s suppressed by a static twin (static wins)", dest_id)
                continue
            if dest_id in self._pending_restart:
                # Keep the freshest target config for the completion step.
                self._pending_restart[dest_id] = m
                continue
            if dest_id not in running:
                if activations >= _ACTIVATIONS_PER_CYCLE:
                    budget_deferred += 1
                    continue
                activations += 1
                if self._heartbeat is not None:
                    self._heartbeat()
                outcome = self._activate(dest_id, m, kind="start")
                if outcome == _OK:
                    running = running | {dest_id}
                elif outcome == _RETIRED:
                    retired_deferred += 1
                elif outcome == _COLLISION:
                    collision_deferred += 1
                else:
                    failures += 1
                continue
            if self._applied_mapped.get(dest_id) != m:
                # Rule 2: config changed (a completed reshard moved the
                # endpoint). Deactivate now; activate once clean —
                # is_clean is the one surviving quiesce (a flush must not
                # straddle the config swap). Rate-capped: each pool
                # evict/recreate costs ~160MB to the fork leak.
                now = time.monotonic()
                if now - self._last_restart_at.get(dest_id, -math.inf) < self._disc.restart_min_interval_s:
                    rate_capped += 1
                    continue
                self._last_restart_at[dest_id] = now
                log.warning(
                    "Destination %s config changed (endpoint/bucket/table moved) — restarting worker",
                    dest_id,
                )
                self._deactivate(dest_id, kind="restart_stop")
                self._pending_restart[dest_id] = m

        self._suppressed_logged &= set(view.entries)  # re-log if it ever re-appears after absence

        metrics.reconciler_pending.labels(reason="debounce").set(
            sum(1 for s in self._absent_streak.values() if s > 0) + collision_deferred
        )
        metrics.reconciler_pending.labels(reason="pending_restart").set(len(self._pending_restart))
        metrics.reconciler_pending.labels(reason="static_suppressed").set(suppressed)
        metrics.reconciler_pending.labels(reason="floor").set(floor_refused)
        metrics.reconciler_pending.labels(reason="budget").set(budget_deferred)
        metrics.reconciler_pending.labels(reason="rate_capped").set(rate_capped)
        metrics.reconciler_pending.labels(reason="retired").set(retired_deferred)
        metrics.reconciler_pending.labels(reason="failure").set(failures)

        # applied_generation advances iff every FIRED primitive succeeded.
        # Deliberate deferrals never block it: debounce (incl. the
        # rv-handoff collision window), suppression, floor, budget, rate
        # cap, pending restarts, and RETIRED refusals (operator intent —
        # a permanently retired tenant must not pin synced=0 forever).
        if failures == 0:
            metrics.discovery_applied_generation.set(view.generation)

    # ------------------------------------------------------------------ #
    # Primitives (poll thread)
    # ------------------------------------------------------------------ #

    def _activate(self, dest_id: str, m: MappedDestination, *, kind: str) -> str:
        """The startup path scoped to one id (§4). Returns an outcome
        constant; only _FAILED means a fired-and-unsuccessful primitive
        (blocks applied_generation and retries on the next fresh view)."""
        try:
            rows = self._state.load_lifecycle_rows([dest_id])
            raw_state = rows.get(dest_id, {}).get("state")
            eff = lifecycle.normalize(raw_state, dest_id)
            if eff == lifecycle.RETIRED:
                # Operator intent beats the CP view — that IS the
                # human-only invariant. Log once per stint.
                if dest_id not in self._retired_refused_logged:
                    self._retired_refused_logged.add(dest_id)
                    log.warning(
                        "Refusing to activate %s: lifecycle row says retired (operator intent beats "
                        "the CP view; clear the row to re-adopt)",
                        dest_id,
                    )
                return _RETIRED
            self._retired_refused_logged.discard(dest_id)

            cfg = materialize_one(
                m,
                self._static_rvs,
                self._disc.defaults,
                self._static_ids,
                secret_timeout_s=self._disc.request_timeout_s,
                allowed_endpoint_suffixes=self._disc.allowed_endpoint_suffixes,
                allowed_secret_namespaces=self._disc.allowed_secret_namespaces,
                probe_fresh=True,  # "validate now" — no TTL hit, no stale-fallback
            )
            if cfg is None:
                return _FAILED

            # Cursor row: our own row resumes as-is (never-delete made it
            # durable); no row anywhere → seed at head (discovery starts
            # the stream, never backfills); rows only on OTHER instances
            # (fleet resize reshuffled hash assignment) → adopt their max
            # so the resume contract survives the reshuffle.
            ours = self._state.load_cursors([dest_id])
            if dest_id not in ours:
                adopted = self._state.max_cursor_any_instance(dest_id)
                if adopted is not None:
                    log.warning(
                        "Destination %s has no cursor row for this instance; adopting max cursor %d "
                        "from other instances (fleet-resize reassignment)",
                        dest_id,
                        adopted,
                    )
                    init_snap = adopted
                else:
                    init_snap = self._src_head_fn() or 0
                self._state.initialize_destinations([dest_id], initial_snapshot_id=init_snap)

            # Disarm deferred deactivation side effects BEFORE anything
            # can fire them against a now-active tenant.
            self._evict_pending.discard(dest_id)
            self._label_pending.discard(dest_id)
            self._absent_streak.pop(dest_id, None)

            # Resolvable before reachable: registry config first; the
            # delivery registration (which loads the cursor, max-merging
            # surviving entries) before the tracker's membership.
            self._registry.add(cfg, origin=ORIGIN_DISCOVERED)
            try:
                self._delivery.add_destination(dest_id)
                self._tracker.add(dest_id, state=raw_state)
            except Exception:
                # Half-activated is worse than not activated: a routable-
                # but-not-delivering id would read as running-with-changed-
                # config next cycle and detour through a restart. Roll the
                # routing back (the config entry stays — never-delete) so
                # the retry is a clean rule-1 start.
                self._registry.remove(dest_id)
                raise
            self._applied_mapped[dest_id] = m
            metrics.discovery_applied_total.labels(kind="restart" if kind == "restart_finish" else "start").inc()
            log.info("Activated discovered destination %s (%s)", dest_id, kind)
            return _OK
        except RegistryCollisionError as e:
            # The rv-handoff debounce window (the old id's absence streak
            # hasn't completed, so its rv is still claimed): a deliberate
            # deferral per the registry's own contract, self-healing when
            # the streak completes — never a failure.
            log.warning("Activation of %s deferred by the registry (%s); retrying next reconcile", dest_id, e)
            return _COLLISION
        except Exception:
            log.warning("Activation of %s failed; retrying next reconcile", dest_id, exc_info=True)
            return _FAILED

    def _deactivate(self, dest_id: str, *, kind: str) -> None:
        """SIGTERM semantics, one id (§1 stop contract): membership-only.
        Never touches durable state; in-flight flushes finish naturally.
        Contained: every raiser here is structurally unreachable today
        (in-memory mutations on ever-member ids), but the reconciler runs
        inside the run loop's fatal handler and a deactivation must never
        take the instance down."""
        try:
            self._registry.remove(dest_id)
            self._delivery.remove_destination(dest_id)
            self._delivery.discard_buffer(dest_id)
            self._tracker.remove(dest_id)
        except Exception:
            log.error("Deactivation of %s failed partway; retrying next cycle", dest_id, exc_info=True)
            return
        self._evict_pending.add(dest_id)
        self._label_pending.add(dest_id)
        self._absent_streak.pop(dest_id, None)
        if kind == "stop":
            self._applied_mapped.pop(dest_id, None)
            metrics.discovery_applied_total.labels(kind="stop").inc()
            log.warning(
                "Deactivated discovered destination %s (absent %d consecutive clean fetches); "
                "cursor row retained — a re-add resumes where it left off",
                dest_id,
                self._disc.absent_stop_fetches,
            )
        try:
            metrics.discovery_stop_countdown.remove(dest_id)
        except KeyError:
            pass

    def _progress_deferred(self) -> None:
        """Advance pending restarts and deferred deactivation side
        effects. Runs every cycle regardless of view freshness."""
        for dest_id, m in list(self._pending_restart.items()):
            if self._delivery.is_clean(dest_id):
                outcome = self._activate(dest_id, m, kind="restart_finish")
                if outcome == _OK:
                    del self._pending_restart[dest_id]
                elif outcome == _RETIRED:
                    # Retired mid-swap: the operator wins. The deactivate
                    # half already ran — cancel the completion instead of
                    # retrying forever.
                    del self._pending_restart[dest_id]
                    self._applied_mapped.pop(dest_id, None)
                    log.warning("Cancelled pending restart of %s: lifecycle row says retired", dest_id)
                elif outcome == _FAILED:
                    # A fired restart that hasn't succeeded — visible on
                    # the failure gauge at the next fresh view.
                    self._deferred_failures += 1
        inflight = self._delivery.inflight_ids()
        for dest_id in list(self._evict_pending):
            if self._delivery.is_clean(dest_id):
                try:
                    self._pool.evict(dest_id)
                except Exception:
                    log.warning("Deferred pool evict for %s failed; retrying next cycle", dest_id, exc_info=True)
                    continue
                self._evict_pending.discard(dest_id)
        for dest_id in list(self._label_pending):
            if dest_id in inflight:
                continue  # a racing .labels() would re-create the series
            metrics.remove_destination_series(dest_id)
            if self._delivery.is_clean(dest_id):
                # Nothing can re-create the series once clean and
                # inactive; stop re-applying.
                self._label_pending.discard(dest_id)
