"""End-to-end reconciler tests (C3 §8.4): real Reconciler + real registry
+ real DeliveryManager/worker pool + real pyducklake destination catalogs
(local DuckDB files). The state store is a faithful in-memory fake (the
real-Postgres cursor semantics are covered by test_state_integration) so
this file runs without docker; materialize_one is patched to emit
local-file configs (the secret/allowlist machinery has its own tests).

Covers the four §8.4 assertions:
- mid-run provision delivers WITHOUT a restart;
- deprovision stops within k clean fetches;
- re-add resumes from the retained cursor (no reseed at head);
- a mentioned-only (fenced/degraded) tenant is never stopped.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyducklake import Schema
from pyducklake.types import IntegerType, NestedField, StringType

from viaduck import metrics
from viaduck.config import DeliveryConfig, DestinationConfig, DiscoveryConfig
from viaduck.delivery import DeliveryManager
from viaduck.destination import DestinationPool
from viaduck.discovery import ClassifiedEntry, ClassifiedView, MappedDestination
from viaduck.lifecycle import LifecycleTracker
from viaduck.reconciler import Reconciler
from viaduck.registry import DestinationRegistry
from viaduck.state import DestinationCursor

pytestmark = pytest.mark.integration

SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="company", field_type=StringType(), required=True),
)


def setup_module():
    metrics.init("reconciler_e2e")


class FakeStateManager:
    """Dict-backed StateManager with the real monotonic-guard semantics
    (the piece the resume contract leans on)."""

    def __init__(self):
        self.cursors: dict[str, DestinationCursor] = {}
        self.lifecycle: dict[str, dict] = {}

    def load_cursors(self, ids):
        return {d: self.cursors[d] for d in ids if d in self.cursors}

    def advance_cursor(self, dest_id, snapshot_id, cumulative_rows=None):
        cur = self.cursors.get(dest_id)
        if cur is not None and cur.last_snapshot_id > snapshot_id:
            return 0  # the WHERE guard dropped the stale write
        rows = cumulative_rows if cumulative_rows is not None else (cur.rows_replicated if cur else 0)
        self.cursors[dest_id] = DestinationCursor(dest_id, "e2e", snapshot_id, rows_replicated=rows)
        return 1

    def initialize_destinations(self, ids, initial_snapshot_id=0):
        for d in ids:
            self.cursors.setdefault(d, DestinationCursor(d, "e2e", initial_snapshot_id))

    def load_lifecycle_rows(self, ids):
        return {d: self.lifecycle[d] for d in ids if d in self.lifecycle}

    def max_cursor_any_instance(self, dest_id):
        cur = self.cursors.get(dest_id)
        return None if cur is None else cur.last_snapshot_id

    def record_error(self, dest_id, error):
        pass


def _mapped(dest_id="org-a-team-7", team_id=7):
    return MappedDestination(
        dest_id=dest_id,
        org_id="a",
        team_id=team_id,
        table="events",
        data_path="unused://",
        pg_endpoint="pooler.cnpg-shards.svc.cluster.local",
        pg_port=5432,
        pg_database="a",
        pg_username="a_user",
        secret_namespace="ducklings",
        secret_name="s",
        secret_key="password",
    )


def _harness(tmp_path, *, k=2, head=100):
    """Real registry/delivery/pool/tracker/reconciler over local DuckDB
    destination catalogs; startup destination set is EMPTY (everything
    arrives via the reconciler — the point of the exercise)."""
    state = FakeStateManager()
    registry = DestinationRegistry()
    cfg = MagicMock()
    cfg.is_assigned.return_value = True
    cfg.discovery = DiscoveryConfig(absent_stop_fetches=k, min_destinations=0, restart_min_interval_s=0.0)
    pool = DestinationPool(cfg, registry, max_open=5)
    pool.set_source_schema(SCHEMA)
    delivery = DeliveryManager(
        DeliveryConfig(workers=2, flush_interval_seconds=0.0),
        state,
        pool,
        [],
        [],
        mode="append_only",
    )
    tracker = LifecycleTracker([])
    rec = Reconciler(
        cfg,
        registry,
        delivery,
        pool,
        tracker,
        state,
        static_routing_values=set(),
        static_ids=set(),
        baseline_mapped={},
        src_head_fn=lambda: head,
    )

    def _local_materialize(m, *args, **kwargs):
        base = tmp_path / m.dest_id
        os.makedirs(base / "data", exist_ok=True)
        return DestinationConfig(
            id=m.dest_id,
            routing_value=str(m.team_id),
            name=m.dest_id,
            postgres_uri_env="",
            postgres_uri_direct=str(base / "meta.duckdb"),
            data_path=str(base / "data"),
            table="events",
        )

    return rec, registry, delivery, pool, state, _local_materialize


def _view(entries, generation, poisoned=False):
    return ClassifiedView(generation=generation, fetched_at=0.0, entry_list=tuple(entries), parse_poisoned=poisoned)


def _batch(ids):
    return pa.table(
        {
            "event_id": pa.array(ids, type=pa.int32()),
            "company": pa.array(["7"] * len(ids), type=pa.string()),
        }
    )


def _read_dest(pool, dest_id):
    catalog, _ = pool.get(dest_id)
    try:
        return catalog.load_table("events").scan().to_arrow()
    finally:
        pool.release(dest_id)


def _deliver(delivery, dest_id, ids, through):
    pos, epoch = delivery.read_plan()[dest_id]
    delivery.buffer(dest_id, _batch(ids), through_snapshot=through, epoch=epoch)
    assert delivery.maybe_flush(shutdown=True) == 1
    assert delivery.wait_idle()


def test_mid_run_provision_delivers_without_restart(tmp_path):
    rec, registry, delivery, pool, state, mat = _harness(tmp_path)
    m = _mapped()
    with patch("viaduck.reconciler.materialize_one", side_effect=mat):
        rec.apply(_view([ClassifiedEntry(dest_id=m.dest_id, mapped=m)], generation=1))
    # Activated: routable, delivering-capable, seeded at head — same process.
    assert registry.snapshot().rv_to_dest == {"7": m.dest_id}
    assert state.cursors[m.dest_id].last_snapshot_id == 100
    _deliver(delivery, m.dest_id, [1, 2, 3], through=105)
    assert sorted(_read_dest(pool, m.dest_id).column("event_id").to_pylist()) == [1, 2, 3]
    assert state.cursors[m.dest_id].last_snapshot_id == 105


def test_deprovision_stops_within_k_and_re_add_resumes_from_cursor(tmp_path):
    rec, registry, delivery, pool, state, mat = _harness(tmp_path, k=2)
    m = _mapped()
    with patch("viaduck.reconciler.materialize_one", side_effect=mat):
        rec.apply(_view([ClassifiedEntry(dest_id=m.dest_id, mapped=m)], generation=1))
        _deliver(delivery, m.dest_id, [1, 2], through=105)

        # Deprovision: absent for k=2 clean fetches -> stopped.
        rec.apply(_view([], generation=2))
        assert m.dest_id in delivery.active_ids()  # streak 1 of 2
        rec.apply(_view([], generation=3))
        assert m.dest_id not in delivery.active_ids()
        assert registry.snapshot().rv_to_dest == {}
        # Never-delete: the cursor row survives the stop.
        assert state.cursors[m.dest_id].last_snapshot_id == 105

        # Re-add: resumes from the retained cursor, NOT reseeded at head.
        rec.apply(_view([ClassifiedEntry(dest_id=m.dest_id, mapped=m)], generation=4))
    assert delivery.read_plan()[m.dest_id][0] == 105
    _deliver(delivery, m.dest_id, [4], through=110)
    assert sorted(_read_dest(pool, m.dest_id).column("event_id").to_pylist()) == [1, 2, 4]
    assert state.cursors[m.dest_id].last_snapshot_id == 110


def test_mentioned_only_tenant_is_never_stopped(tmp_path):
    rec, _, delivery, _, _, mat = _harness(tmp_path, k=1)
    m = _mapped()
    with patch("viaduck.reconciler.materialize_one", side_effect=mat):
        rec.apply(_view([ClassifiedEntry(dest_id=m.dest_id, mapped=m)], generation=1))
        # Fenced/degraded for many clean fetches: mentioned -> never absent.
        for gen in range(2, 8):
            rec.apply(_view([ClassifiedEntry(dest_id=m.dest_id, mapped=None)], generation=gen))
    assert m.dest_id in delivery.active_ids()


def test_poisoned_views_freeze_stops_indefinitely(tmp_path):
    rec, _, delivery, _, _, mat = _harness(tmp_path, k=1)
    m = _mapped()
    with patch("viaduck.reconciler.materialize_one", side_effect=mat):
        rec.apply(_view([ClassifiedEntry(dest_id=m.dest_id, mapped=m)], generation=1))
        for gen in range(2, 8):
            rec.apply(_view([], generation=gen, poisoned=True))
    assert m.dest_id in delivery.active_ids()
