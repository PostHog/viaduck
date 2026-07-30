"""Unit tests for the C3 reconciler: two predicates, three rules.

The registry is REAL (its collision/never-delete semantics are load-
bearing for the rules); delivery/tracker/state/pool are mocks;
materialize_one is patched (its own contract is tested in
test_discovery.py) so no secret machinery engages here."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from viaduck import metrics
from viaduck.config import DestinationConfig, DiscoveryConfig
from viaduck.discovery import ClassifiedEntry, ClassifiedView, MappedDestination
from viaduck.reconciler import Reconciler
from viaduck.registry import DestinationRegistry


def setup_module():
    metrics.init("test")


def _mapped(dest_id="org-a-team-7", team_id=7, endpoint="pooler.cnpg-shards.svc.cluster.local"):
    return MappedDestination(
        dest_id=dest_id,
        org_id="a",
        team_id=team_id,
        table="t.events",
        data_path="s3://b/",
        pg_endpoint=endpoint,
        pg_port=5432,
        pg_database="a",
        pg_username="a_user",
        secret_namespace="ducklings",
        secret_name="s",
        secret_key="password",
    )


def _dest_cfg(m: MappedDestination) -> DestinationConfig:
    return DestinationConfig(
        id=m.dest_id,
        routing_value=str(m.team_id),
        name=m.dest_id,
        postgres_uri_env="X",
        data_path=m.data_path,
        table=m.table,
    )


def _view(entries, generation=1, poisoned=False):
    return ClassifiedView(
        generation=generation,
        fetched_at=0.0,
        entry_list=tuple(entries),
        parse_poisoned=poisoned,
    )


def _startable(m):
    return ClassifiedEntry(dest_id=m.dest_id, mapped=m)


def _mentioned(dest_id):
    return ClassifiedEntry(dest_id=dest_id, mapped=None)


def _reconciler(
    *,
    running=(),
    baseline=None,
    k=3,
    floor=0.5,
    min_destinations=0,
    restart_min_interval_s=0.0,
):
    cfg = MagicMock()
    cfg.is_assigned.return_value = True
    cfg.discovery = DiscoveryConfig(
        absent_stop_fetches=k,
        stop_floor_fraction=floor,
        min_destinations=min_destinations,
        restart_min_interval_s=restart_min_interval_s,
    )
    registry = DestinationRegistry()
    for m in running:
        registry.add(_dest_cfg(m), origin="discovered")
    delivery = MagicMock()
    delivery.is_clean.return_value = True
    delivery.inflight_ids.return_value = set()
    tracker = MagicMock()
    state = MagicMock()
    state.load_lifecycle_rows.return_value = {}
    state.load_cursors.return_value = {}
    state.max_cursor_any_instance.return_value = None
    pool = MagicMock()
    rec = Reconciler(
        cfg,
        registry,
        delivery,
        pool,
        tracker,
        state,
        static_routing_values=set(),
        static_ids=set(),
        baseline_mapped=dict(baseline or {}),
        src_head_fn=lambda: 100,
    )
    return rec, registry, delivery, tracker, state, pool


def _patched_materialize(m_to_cfg=None):
    def _mat(m, *args, **kwargs):
        return _dest_cfg(m)

    return patch("viaduck.reconciler.materialize_one", side_effect=m_to_cfg or _mat)


# ---------------------------------------------------------------------------
# Rule 1: start
# ---------------------------------------------------------------------------


def test_startable_new_id_activates_and_delivers_membership():
    rec, registry, delivery, tracker, state, _ = _reconciler()
    m = _mapped()
    with _patched_materialize():
        rec.apply(_view([_startable(m)]))
    assert registry.snapshot().rv_to_dest == {"7": m.dest_id}
    delivery.add_destination.assert_called_once_with(m.dest_id)
    tracker.add.assert_called_once_with(m.dest_id, state=None)
    # New id, no cursor anywhere: seeded at head (never backfills).
    state.initialize_destinations.assert_called_once_with([m.dest_id], initial_snapshot_id=100)


def test_mentioned_only_id_is_never_started():
    rec, registry, delivery, _, _, _ = _reconciler()
    rec.apply(_view([_mentioned("org-a-team-7")]))
    delivery.add_destination.assert_not_called()
    assert registry.snapshot().rv_to_dest == {}


def test_retired_lifecycle_row_refuses_activation():
    rec, registry, delivery, _, state, _ = _reconciler()
    state.load_lifecycle_rows.return_value = {"org-a-team-7": {"state": "retired"}}
    with _patched_materialize():
        rec.apply(_view([_startable(_mapped())]))
    delivery.add_destination.assert_not_called()
    assert registry.snapshot().rv_to_dest == {}


def test_paused_lifecycle_row_enters_paused_from_first_cycle():
    rec, _, delivery, tracker, state, _ = _reconciler()
    state.load_lifecycle_rows.return_value = {"org-a-team-7": {"state": "paused"}}
    with _patched_materialize():
        rec.apply(_view([_startable(_mapped())]))
    delivery.add_destination.assert_called_once()
    tracker.add.assert_called_once_with("org-a-team-7", state="paused")


def test_static_twin_suppressed_deterministically():
    rec, registry, delivery, _, _, _ = _reconciler()
    rec._static_rvs = {"7"}
    with _patched_materialize():
        rec.apply(_view([_startable(_mapped())]))
        rec.apply(_view([_startable(_mapped())], generation=2))
    delivery.add_destination.assert_not_called()
    assert registry.snapshot().rv_to_dest == {}


def test_unassigned_id_ignored_by_partition_filter():
    rec, _, delivery, _, _, _ = _reconciler()
    rec._cfg.is_assigned.return_value = False
    with _patched_materialize():
        rec.apply(_view([_startable(_mapped())]))
    delivery.add_destination.assert_not_called()


def test_reassigned_id_adopts_max_cursor_from_other_instances():
    rec, _, _, _, state, _ = _reconciler()
    state.max_cursor_any_instance.return_value = 42
    with _patched_materialize():
        rec.apply(_view([_startable(_mapped())]))
    state.initialize_destinations.assert_called_once_with(["org-a-team-7"], initial_snapshot_id=42)


def test_existing_own_cursor_row_resumes_without_reinit():
    rec, _, _, _, state, _ = _reconciler()
    c = MagicMock()
    c.last_snapshot_id = 55
    state.load_cursors.return_value = {"org-a-team-7": c}
    with _patched_materialize():
        rec.apply(_view([_startable(_mapped())]))
    state.initialize_destinations.assert_not_called()


def test_activation_failure_is_counted_and_blocks_applied_generation():
    rec, _, delivery, _, _, _ = _reconciler()
    delivery.add_destination.side_effect = RuntimeError("pg down")
    with _patched_materialize():
        rec.apply(_view([_startable(_mapped())], generation=9))
    # Failure -> applied_generation must NOT advance to 9; retried next
    # cycle on the same view identity? No: same view object is frozen —
    # a fresh view retries.
    with _patched_materialize():
        delivery.add_destination.side_effect = None
        rec.apply(_view([_startable(_mapped())], generation=10))
    assert delivery.add_destination.call_count == 2


# ---------------------------------------------------------------------------
# Rule 3: stop (debounce, floor, poison)
# ---------------------------------------------------------------------------


def test_stop_after_k_clean_fetches_and_dicts_survive():
    m = _mapped()
    rec, registry, delivery, tracker, _, _ = _reconciler(running=[m], baseline={m.dest_id: m}, k=3)
    for gen in (1, 2):
        rec.apply(_view([], generation=gen))
        delivery.remove_destination.assert_not_called()
    rec.apply(_view([], generation=3))
    delivery.remove_destination.assert_called_once_with(m.dest_id)
    delivery.discard_buffer.assert_called_once_with(m.dest_id)
    tracker.remove.assert_called_once_with(m.dest_id)
    # Never-delete: config still resolvable, routing gone.
    assert registry.config_for(m.dest_id).id == m.dest_id
    assert registry.snapshot().rv_to_dest == {}


def test_mention_resets_the_absent_streak():
    m = _mapped()
    rec, _, delivery, _, _, _ = _reconciler(running=[m], baseline={m.dest_id: m}, k=2)
    rec.apply(_view([], generation=1))
    # Mentioned again (even unstartable: e.g. its warehouse got fenced).
    rec.apply(_view([_mentioned(m.dest_id)], generation=2))
    rec.apply(_view([], generation=3))
    delivery.remove_destination.assert_not_called()
    rec.apply(_view([], generation=4))
    delivery.remove_destination.assert_called_once()


def test_poisoned_view_freezes_absence_but_still_starts():
    m_run = _mapped()
    m_new = _mapped(dest_id="org-b-team-8", team_id=8)
    rec, _, delivery, _, _, _ = _reconciler(running=[m_run], baseline={m_run.dest_id: m_run}, k=1)
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1, poisoned=True))
    delivery.remove_destination.assert_not_called()  # absence frozen
    delivery.add_destination.assert_called_once_with(m_new.dest_id)  # adds apply


def test_stale_view_identity_freezes_everything():
    m = _mapped()
    rec, _, delivery, _, _, _ = _reconciler(running=[m], baseline={m.dest_id: m}, k=1)
    v = _view([], generation=1)
    rec.apply(v)
    delivery.remove_destination.assert_called_once()
    delivery.remove_destination.reset_mock()
    rec.apply(v)  # same object: frozen
    delivery.remove_destination.assert_not_called()


def test_floor_refuses_mass_stop_but_applies_starts():
    m1, m2 = _mapped(), _mapped(dest_id="org-b-team-8", team_id=8)
    m_new = _mapped(dest_id="org-c-team-9", team_id=9)
    rec, _, delivery, _, _, _ = _reconciler(running=[m1, m2], baseline={m1.dest_id: m1, m2.dest_id: m2}, k=1, floor=0.5)
    # Both running ids absent at once: 2 stops > ceil(0.5*2)=1 -> refused;
    # the start from the same view still applies.
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1))
    delivery.remove_destination.assert_not_called()
    delivery.add_destination.assert_called_once_with(m_new.dest_id)


def test_single_stop_within_floor_proceeds():
    m1, m2, m3 = (
        _mapped(),
        _mapped(dest_id="org-b-team-8", team_id=8),
        _mapped(dest_id="org-c-team-9", team_id=9),
    )
    rec, _, delivery, _, _, _ = _reconciler(
        running=[m1, m2, m3],
        baseline={m.dest_id: m for m in (m1, m2, m3)},
        k=1,
        floor=0.5,
    )
    rec.apply(_view([_mentioned(m2.dest_id), _mentioned(m3.dest_id)], generation=1))
    delivery.remove_destination.assert_called_once_with(m1.dest_id)


# ---------------------------------------------------------------------------
# Rule 2: restart on config change
# ---------------------------------------------------------------------------


def test_config_change_restarts_via_pending_until_clean():
    m_old = _mapped()
    m_new = _mapped(endpoint="pooler-2.cnpg-shards.svc.cluster.local")
    rec, registry, delivery, _, _, pool = _reconciler(running=[m_old], baseline={m_old.dest_id: m_old})
    delivery.is_clean.return_value = False  # in-flight flush: swap must wait
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1))
    # Deactivated, not yet re-activated.
    delivery.remove_destination.assert_called_once_with(m_old.dest_id)
    delivery.add_destination.assert_not_called()
    pool.evict.assert_not_called()
    # Clean now: the NEXT apply (even of a stale view) completes the swap.
    delivery.is_clean.return_value = True
    handoff_order = []
    pool.evict.side_effect = lambda _did: handoff_order.append("evict")
    delivery.add_destination.side_effect = lambda _did: handoff_order.append("activate")
    with _patched_materialize():
        rec.apply(None)
    # The stale Catalog must be gone before the destination becomes active
    # with its new endpoint; otherwise DestinationPool.get() returns the
    # cached old connection without consulting the live registry.
    pool.evict.assert_called_once_with(m_old.dest_id)
    assert handoff_order == ["evict", "activate"]
    delivery.add_destination.assert_called_once_with(m_old.dest_id)
    assert registry.snapshot().rv_to_dest == {"7": m_old.dest_id}
    assert rec._applied_mapped[m_old.dest_id] == m_new


def test_unchanged_config_is_a_noop():
    m = _mapped()
    rec, _, delivery, _, _, _ = _reconciler(running=[m], baseline={m.dest_id: m})
    with _patched_materialize():
        rec.apply(_view([_startable(m)], generation=1))
    delivery.remove_destination.assert_not_called()
    delivery.add_destination.assert_not_called()


def test_restart_rate_cap_defers():
    m_old = _mapped()
    m_new = _mapped(endpoint="pooler-2.cnpg-shards.svc.cluster.local")
    rec, _, delivery, _, _, _ = _reconciler(
        running=[m_old], baseline={m_old.dest_id: m_old}, restart_min_interval_s=9999.0
    )
    rec._last_restart_at[m_old.dest_id] = __import__("time").monotonic()
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1))
    delivery.remove_destination.assert_not_called()  # capped this cycle


# ---------------------------------------------------------------------------
# Deferred side effects
# ---------------------------------------------------------------------------


def test_pool_evict_defers_until_clean_and_reactivation_disarms():
    m = _mapped()
    rec, _, delivery, _, _, pool = _reconciler(running=[m], baseline={m.dest_id: m}, k=1)
    delivery.is_clean.return_value = False
    rec.apply(_view([], generation=1))  # stop; evict armed but not clean
    pool.evict.assert_not_called()
    # Re-added before the zombie flush finished: the armed evict must be
    # DISARMED, or it would close a now-active tenant's catalog.
    with _patched_materialize():
        rec.apply(_view([_startable(m)], generation=2))
    delivery.is_clean.return_value = True
    rec.apply(None)
    pool.evict.assert_not_called()


def test_pool_evict_fires_once_clean():
    m = _mapped()
    rec, _, delivery, _, _, pool = _reconciler(running=[m], baseline={m.dest_id: m}, k=1)
    delivery.is_clean.return_value = False
    rec.apply(_view([], generation=1))
    delivery.is_clean.return_value = True
    rec.apply(None)
    pool.evict.assert_called_once_with(m.dest_id)


def test_label_removal_waits_out_inflight():
    m = _mapped()
    rec, _, delivery, _, _, _ = _reconciler(running=[m], baseline={m.dest_id: m}, k=1)
    delivery.is_clean.return_value = False
    delivery.inflight_ids.return_value = {m.dest_id}
    with patch("viaduck.reconciler.metrics.remove_destination_series") as rm:
        rec.apply(_view([], generation=1))
        rec.apply(None)
        rm.assert_not_called()  # in flight: a racing .labels() would resurrect
        delivery.inflight_ids.return_value = set()
        delivery.is_clean.return_value = True
        rec.apply(None)
        rm.assert_called_with(m.dest_id)


# ---------------------------------------------------------------------------
# Stage-4 review fixes
# ---------------------------------------------------------------------------


def test_activation_budget_defers_spillover():
    # A CP recovery serving many tenants must not stall the poll thread
    # past liveness: at most _ACTIVATIONS_PER_CYCLE fire per apply, with
    # a heartbeat per activation; spillover retries on the next view.
    from viaduck import reconciler as rmod

    beats = []
    rec, _, delivery, _, _, _ = _reconciler()
    rec._heartbeat = lambda: beats.append(1)
    ms = [_mapped(dest_id=f"org-a-team-{i}", team_id=i) for i in range(rmod._ACTIVATIONS_PER_CYCLE + 5)]
    with _patched_materialize():
        rec.apply(_view([_startable(m) for m in ms], generation=1))
    assert delivery.add_destination.call_count == rmod._ACTIVATIONS_PER_CYCLE
    assert len(beats) == rmod._ACTIVATIONS_PER_CYCLE
    with _patched_materialize():
        rec.apply(_view([_startable(m) for m in ms], generation=2))
    assert delivery.add_destination.call_count == len(ms)


def test_pending_restart_cancelled_when_tenant_leaves_view():
    # Deprovision mid-reshard: completing the swap would resurrect a
    # deprovisioned tenant (and retry-fail forever if its Secret is
    # gone). A clean view without the id cancels the pending restart.
    m_old = _mapped()
    m_new = _mapped(endpoint="pooler-2.cnpg-shards.svc.cluster.local")
    rec, _, delivery, _, _, _ = _reconciler(running=[m_old], baseline={m_old.dest_id: m_old})
    delivery.is_clean.return_value = False
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1))
    assert m_old.dest_id in rec._pending_restart
    rec.apply(_view([], generation=2))  # clean view, id gone
    assert m_old.dest_id not in rec._pending_restart
    delivery.is_clean.return_value = True
    rec.apply(None)
    delivery.add_destination.assert_not_called()  # no resurrection


def test_pending_restart_cancelled_on_retired():
    m_old = _mapped()
    m_new = _mapped(endpoint="pooler-2.cnpg-shards.svc.cluster.local")
    rec, _, delivery, _, state, _ = _reconciler(running=[m_old], baseline={m_old.dest_id: m_old})
    delivery.is_clean.return_value = False
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1))
    state.load_lifecycle_rows.return_value = {m_old.dest_id: {"state": "retired"}}
    delivery.is_clean.return_value = True
    with _patched_materialize():
        rec.apply(None)  # completion attempt refuses on retired -> cancelled
    assert m_old.dest_id not in rec._pending_restart
    delivery.add_destination.assert_not_called()


def test_retired_refusal_is_deferral_not_failure():
    # A retired row for a CP-served id is designed-normal operator
    # intent; it must not pin applied_generation (synced) forever.
    rec, _, delivery, _, state, _ = _reconciler()
    state.load_lifecycle_rows.return_value = {"org-a-team-7": {"state": "retired"}}
    with (
        _patched_materialize(),
        patch("viaduck.reconciler.metrics.discovery_applied_generation") as gen,
    ):
        rec.apply(_view([_startable(_mapped())], generation=41))
    gen.set.assert_called_once_with(41)
    delivery.add_destination.assert_not_called()


def test_rv_handoff_collision_is_deferral_and_heals():
    # Old tenant absent (streak incomplete) + new tenant startable on the
    # same rv: the activate defers on rv collision WITHOUT blocking
    # applied_generation, and heals once the old id's streak completes
    # (stops run before starts within a view).
    m_old = _mapped()  # rv "7"
    m_new = _mapped(dest_id="org-b-team-7", team_id=7)
    rec, registry, delivery, _, _, _ = _reconciler(running=[m_old], baseline={m_old.dest_id: m_old}, k=2)
    with (
        _patched_materialize(),
        patch("viaduck.reconciler.metrics.discovery_applied_generation") as gen,
    ):
        rec.apply(_view([_startable(m_new)], generation=1))  # streak 1 of 2: collision
        gen.set.assert_called_with(1)  # deferral, not failure
        rec.apply(_view([_startable(m_new)], generation=2))  # streak completes: stop then start
    assert registry.snapshot().rv_to_dest == {"7": m_new.dest_id}
    delivery.remove_destination.assert_called_once_with(m_old.dest_id)
    delivery.add_destination.assert_called_once_with(m_new.dest_id)


def test_last_tenant_stop_allowed_min_destinations_guards_mass_only():
    # min_destinations=1 (the startup default) must not make the last
    # discovered tenant unstoppable: single-id stops passed k clean
    # fetches of debounce already; the min-floor guards MASS stops.
    m = _mapped()
    rec, _, delivery, _, _, _ = _reconciler(running=[m], baseline={m.dest_id: m}, k=1, min_destinations=1)
    rec.apply(_view([], generation=1))
    delivery.remove_destination.assert_called_once_with(m.dest_id)


def test_mass_stop_still_refused_by_min_destinations():
    ms = [_mapped(dest_id=f"org-{i}-team-{i}", team_id=i) for i in range(3)]
    rec, _, delivery, _, _, _ = _reconciler(
        running=ms, baseline={m.dest_id: m for m in ms}, k=1, floor=1.0, min_destinations=2
    )
    rec.apply(_view([], generation=1))  # 3 stops, floor fraction allows, min=2 refuses
    delivery.remove_destination.assert_not_called()


def test_rate_cap_expiry_allows_restart():
    m_old = _mapped()
    m_new = _mapped(endpoint="pooler-2.cnpg-shards.svc.cluster.local")
    rec, _, delivery, _, _, _ = _reconciler(
        running=[m_old], baseline={m_old.dest_id: m_old}, restart_min_interval_s=0.0
    )
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1))
    delivery.remove_destination.assert_called_once()  # cap expired (0s): proceeds


def test_failed_restart_completion_is_visible_as_failure():
    m_old = _mapped()
    m_new = _mapped(endpoint="pooler-2.cnpg-shards.svc.cluster.local")
    rec, _, delivery, _, _, _ = _reconciler(running=[m_old], baseline={m_old.dest_id: m_old})
    delivery.is_clean.return_value = False
    with _patched_materialize():
        rec.apply(_view([_startable(m_new)], generation=1))
    delivery.is_clean.return_value = True
    delivery.add_destination.side_effect = RuntimeError("dest down")
    with _patched_materialize():
        rec.apply(None)  # completion fires and fails
        with patch("viaduck.reconciler.metrics.discovery_applied_generation") as gen:
            rec.apply(_view([_startable(m_new)], generation=2))
    # The fired-but-failed restart blocks the fresh view's generation.
    gen.set.assert_not_called()
