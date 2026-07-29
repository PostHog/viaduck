"""Unit tests for DestinationRegistry: the live id→config + routing
authority (C3 §5). The load-bearing properties: incremental uniqueness,
static-wins, never-delete (removed configs stay resolvable), and
immutable per-cycle snapshots."""

from __future__ import annotations

import pytest

from viaduck.config import ConfigError, DestinationConfig
from viaduck.registry import (
    ORIGIN_DISCOVERED,
    ORIGIN_STATIC,
    DestinationRegistry,
    RegistryCollisionError,
)


def _dest(dest_id: str, rv: str, table: str = "t") -> DestinationConfig:
    return DestinationConfig(
        id=dest_id,
        routing_value=rv,
        name=f"cat-{dest_id}",
        postgres_uri_env="UNUSED",
        data_path=f"s3://bucket/{dest_id}",
        table=table,
        postgres_uri_direct="postgresql://u:p@h/db",
    )


def test_add_resolve_and_snapshot():
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_STATIC)
    assert reg.config_for("d1").routing_value == "acme"
    snap = reg.snapshot()
    assert snap.rv_to_dest == {"acme": "d1"}
    assert snap.routable_ids == frozenset({"d1"})
    assert snap.origins == {"d1": ORIGIN_STATIC}


def test_unknown_id_raises_config_error():
    reg = DestinationRegistry()
    with pytest.raises(ConfigError, match="Unknown destination ID"):
        reg.config_for("nope")
    with pytest.raises(ConfigError, match="Unknown destination ID"):
        reg.snapshot().config_for("nope")


def test_routing_value_collision_across_ids_refused():
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_DISCOVERED)
    with pytest.raises(RegistryCollisionError, match="already routes to 'd1'"):
        reg.add(_dest("d2", "acme"), origin=ORIGIN_DISCOVERED)


def test_static_wins_over_discovered_twin():
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_STATIC)
    with pytest.raises(RegistryCollisionError, match="is static"):
        reg.add(_dest("d1", "acme"), origin=ORIGIN_DISCOVERED)


def test_re_add_same_id_updates_config():
    # The reconciler's restart-with-new-config path: same id, new endpoint.
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme", table="old"), origin=ORIGIN_DISCOVERED)
    reg.add(_dest("d1", "acme", table="new"), origin=ORIGIN_DISCOVERED)
    assert reg.config_for("d1").table == "new"
    assert reg.snapshot().rv_to_dest == {"acme": "d1"}


def test_remove_keeps_config_resolvable_but_not_routable():
    # Never-delete, in-memory: an in-flight flush's OCC retry re-runs the
    # pool's _create, which must still resolve a stopped destination.
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_DISCOVERED)
    reg.remove("d1")
    assert reg.config_for("d1").id == "d1"  # still resolvable
    snap = reg.snapshot()
    assert snap.rv_to_dest == {}  # not routable
    assert snap.routable_ids == frozenset()
    assert snap.config_for("d1").id == "d1"  # snapshot resolves too
    reg.remove("d1")  # idempotent
    reg.remove("never-added")  # idempotent for unknown ids


def test_re_add_after_remove_restores_routing():
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_DISCOVERED)
    reg.remove("d1")
    reg.add(_dest("d1", "acme"), origin=ORIGIN_DISCOVERED)
    assert reg.snapshot().rv_to_dest == {"acme": "d1"}


def test_from_configs_origins_and_discovered_ids():
    reg = DestinationRegistry.from_configs(
        [_dest("s1", "static-co"), _dest("dyn1", "dyn-co")],
        discovered_ids={"dyn1"},
    )
    snap = reg.snapshot()
    assert snap.origins == {"s1": ORIGIN_STATIC, "dyn1": ORIGIN_DISCOVERED}
    assert snap.discovered_ids() == frozenset({"dyn1"})


def test_snapshot_is_immutable_view():
    # A snapshot captured at cycle start must not see mid-cycle mutations —
    # the no-mid-cycle-reads contract is what makes routing/state/status
    # agree within one cycle.
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_DISCOVERED)
    snap = reg.snapshot()
    reg.add(_dest("d2", "globex"), origin=ORIGIN_DISCOVERED)
    reg.remove("d1")
    assert snap.rv_to_dest == {"acme": "d1"}
    assert "d2" not in snap.configs
    assert reg.snapshot().rv_to_dest == {"globex": "d2"}


def test_re_add_with_changed_rv_frees_old_rv():
    # The reconciler's restart-with-new-config shape. Without the old-rv
    # cleanup, the stale mapping keeps the destination routable after
    # remove() and phantom-blocks the old rv forever (stage-2 review).
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_DISCOVERED)
    reg.add(_dest("d1", "acme-renamed"), origin=ORIGIN_DISCOVERED)
    assert reg.snapshot().rv_to_dest == {"acme-renamed": "d1"}
    reg.remove("d1")
    snap = reg.snapshot()
    assert snap.rv_to_dest == {}
    assert snap.routable_ids == frozenset()
    # The freed rv is assignable to a new id (the §4 handoff).
    reg.add(_dest("d2", "acme"), origin=ORIGIN_DISCOVERED)
    assert reg.snapshot().rv_to_dest == {"acme": "d2"}


def test_snapshot_mappings_are_read_only():
    reg = DestinationRegistry()
    reg.add(_dest("d1", "acme"), origin=ORIGIN_STATIC)
    snap = reg.snapshot()
    with pytest.raises(TypeError):
        snap.rv_to_dest["evil"] = "d1"  # type: ignore[index]
    with pytest.raises(TypeError):
        snap.configs["evil"] = None  # type: ignore[index]
