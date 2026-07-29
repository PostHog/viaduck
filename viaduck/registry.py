"""Live destination registry: the runtime authority on destination configs.

Before this module, destination configs were resolved from the frozen
startup ``ViaduckConfig`` at three sites (the pool's ``_create``, the poll
cycle's routing-value lookups, and the status export). That capture-at-
startup pattern produced three separate stale-config defects during the
discovery effort; the registry makes the class unrepresentable by being
the ONLY runtime resolution path (a test pins this).

Model (C3 design §5):

- Entries carry ``origin``: ``static`` (chart config) or ``discovered``
  (CP discovery). The reconciler manages discovered entries only; every
  reconciler counter/diff iterates discovered-origin ids — statics are
  never in the CP view and must never enter an ABSENT evaluation.
- ``remove()`` removes ROUTING VISIBILITY only. The config stays
  resolvable forever (process lifetime): an in-flight flush's OCC retry
  re-runs the pool's ``_create``, which must still resolve a stopped
  destination's config so "let in-flight finish naturally" means finish
  (§1 stop contract). Nothing is deleted — the in-memory mirror of the
  never-delete principle.
- ``add()`` enforces id/routing-value uniqueness on every apply
  (construction-time validation doesn't cover incremental adds) with
  static-wins: a discovered entry may never displace a static one.
- ``snapshot()`` returns an immutable view; the poll cycle captures ONE
  snapshot per cycle and never reads the live registry mid-cycle.

Thread-safety: mutations happen on the poll thread (the reconciler's
apply side); reads happen from flush workers (pool ``_create``) and the
poll thread. All state is behind one lock; the snapshot is rebuilt on
mutation and handed out by reference.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING

from viaduck.config import ConfigError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from viaduck.config import DestinationConfig

ORIGIN_STATIC = "static"
ORIGIN_DISCOVERED = "discovered"


class RegistryCollisionError(ConfigError):
    """An add() collided with an existing entry (id or routing value).

    Subclasses ConfigError so startup population failures stay fatal
    config problems. The reconciler must catch THIS type specifically —
    a collision at reconcile time is a deferrable refusal (skip-and-count,
    e.g. an rv handoff mid-debounce), not a config error."""


@dataclass(frozen=True)
class RegistrySnapshot:
    """Immutable per-cycle view. ``rv_to_dest`` contains ROUTABLE entries
    only (removed destinations drop out); ``configs`` contains every entry
    ever added (never-delete). ``routable_ids`` is DERIVED from
    ``rv_to_dest`` at construction so the two can never disagree."""

    configs: Mapping[str, DestinationConfig]
    rv_to_dest: Mapping[str, str]
    origins: Mapping[str, str]
    routable_ids: frozenset[str] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "routable_ids", frozenset(self.rv_to_dest.values()))

    def config_for(self, dest_id: str) -> DestinationConfig:
        cfg = self.configs.get(dest_id)
        if cfg is None:
            raise ConfigError(f"Unknown destination ID: {dest_id!r}")
        return cfg

    def discovered_ids(self) -> frozenset[str]:
        return frozenset(d for d, o in self.origins.items() if o == ORIGIN_DISCOVERED)


class DestinationRegistry:
    """Lock-guarded id→config + routing index with atomic snapshots."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._configs: dict[str, DestinationConfig] = {}
        self._origins: dict[str, str] = {}
        self._rv_to_dest: dict[str, str] = {}
        self._snapshot = RegistrySnapshot(configs={}, rv_to_dest={}, origins={})

    @classmethod
    def from_configs(
        cls,
        destinations: list[DestinationConfig],
        *,
        discovered_ids: set[str] | frozenset[str] = frozenset(),
    ) -> DestinationRegistry:
        """Startup population from the merged (static + discovered) config
        list. Uniqueness across the whole list was already validated by
        ``ViaduckConfig.__post_init__``; per-entry add() re-checks anyway."""
        reg = cls()
        for d in destinations:
            reg.add(d, origin=ORIGIN_DISCOVERED if d.id in discovered_ids else ORIGIN_STATIC)
        return reg

    def add(self, config: DestinationConfig, *, origin: str) -> None:
        """Add (or re-add) a destination. Re-adding the SAME id updates its
        config in place (the reconciler's restart-with-new-config path);
        collisions with a DIFFERENT id's routing value, or any collision
        against a static entry from a discovered add, raise."""
        if origin not in (ORIGIN_STATIC, ORIGIN_DISCOVERED):
            raise ValueError(f"origin must be static|discovered, got {origin!r}")
        with self._lock:
            existing_id = self._rv_to_dest.get(config.routing_value)
            if existing_id is not None and existing_id != config.id:
                raise RegistryCollisionError(
                    f"routing value {config.routing_value!r} already routes to {existing_id!r} "
                    f"(attempted add of {config.id!r})"
                )
            prior_origin = self._origins.get(config.id)
            if prior_origin == ORIGIN_STATIC and origin == ORIGIN_DISCOVERED:
                # Static wins — a CP-served twin of a static destination is
                # classified SUPPRESSED upstream and must never get here;
                # this is the belt-and-suspenders refusal.
                raise RegistryCollisionError(
                    f"destination {config.id!r} is static; a discovered entry may not replace it"
                )
            prior = self._configs.get(config.id)
            if (
                prior is not None
                and prior.routing_value != config.routing_value
                and self._rv_to_dest.get(prior.routing_value) == config.id
            ):
                # In-place re-add with a CHANGED routing value: without this
                # cleanup the stale rv stays in the index — the "removed"
                # destination remains routable via it, and the old rv is
                # phantom-blocked for any future handoff (stage-2 review,
                # both reviewers).
                del self._rv_to_dest[prior.routing_value]
            self._configs[config.id] = config
            self._origins[config.id] = origin
            self._rv_to_dest[config.routing_value] = config.id
            self._rebuild_snapshot_locked()

    def remove(self, dest_id: str) -> None:
        """Remove ROUTING VISIBILITY for a destination. The config entry is
        retained for the process lifetime — in-flight flushes and the pool's
        OCC-retry ``_create`` must still resolve it (§1 stop contract).
        Idempotent."""
        with self._lock:
            cfg = self._configs.get(dest_id)
            if cfg is None:
                return
            current = self._rv_to_dest.get(cfg.routing_value)
            if current == dest_id:
                del self._rv_to_dest[cfg.routing_value]
            self._rebuild_snapshot_locked()

    def config_for(self, dest_id: str) -> DestinationConfig:
        """Resolve a destination config — including removed (unroutable)
        entries, per never-delete. Raises ConfigError for ids never added."""
        with self._lock:
            cfg = self._configs.get(dest_id)
        if cfg is None:
            raise ConfigError(f"Unknown destination ID: {dest_id!r}")
        return cfg

    def snapshot(self) -> RegistrySnapshot:
        """The current immutable view (captured once per poll cycle)."""
        with self._lock:
            return self._snapshot

    def _rebuild_snapshot_locked(self) -> None:
        # MappingProxyType: snapshot holders share these mappings; a
        # consumer mutation would corrupt the view for every concurrent
        # holder, so make it structurally impossible rather than a typing
        # convention. Note add() is single-phase (config + routing become
        # visible together); C3 §4 step 5's "resolvable before reachable"
        # ordering is realized by the reconciler sequencing registry/
        # delivery calls within one poll-thread cycle boundary, not by a
        # two-phase registry API.
        self._snapshot = RegistrySnapshot(
            configs=MappingProxyType(dict(self._configs)),
            rv_to_dest=MappingProxyType(dict(self._rv_to_dest)),
            origins=MappingProxyType(dict(self._origins)),
        )
