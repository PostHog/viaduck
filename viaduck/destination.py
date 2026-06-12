"""LRU connection pool for destination DuckLake catalogs."""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from typing import TYPE_CHECKING

from viaduck import metrics
from viaduck.source import with_connection_defaults

if TYPE_CHECKING:
    from pyducklake import Catalog, Table
    from pyducklake.schema import Schema

    from viaduck.config import DestinationConfig, ViaduckConfig

log = logging.getLogger(__name__)

_MIN_MAX_OPEN = 1


class DestinationPool:
    """LRU pool of destination Catalog connections, safe for concurrent
    flush workers.

    Bounds the number of open DuckDB connections to avoid unbounded memory
    growth when the destination count is high (100s-1000s).

    Pinning: ``get()`` pins the entry until the matching ``release()``;
    LRU eviction skips pinned entries and ``evict()`` of a pinned entry
    defers the close to the final release. Without this, one worker's LRU
    pressure could close a catalog another worker is mid-transaction on.
    At most one worker uses a given destination at a time (the delivery
    layer's in-flight guard), so per-destination pin counts are 0 or 1 in
    practice — the counting is defensive.
    """

    def __init__(self, config: ViaduckConfig, max_open: int = 50):
        if max_open < _MIN_MAX_OPEN:
            raise ValueError(f"max_open must be >= {_MIN_MAX_OPEN}, got {max_open}")
        self._config = config
        self._max_open = max_open
        self._lock = threading.Lock()
        self._pool: OrderedDict[str, tuple[Catalog, Table]] = OrderedDict()
        self._pins: dict[str, int] = {}
        self._creating: set[str] = set()  # reserved slots; created outside the lock
        self._zombies: dict[str, list[Catalog]] = {}  # evicted-while-pinned, closed on release
        self._source_schema: Schema | None = None

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._pool)

    @property
    def max_open(self) -> int:
        return self._max_open

    def set_source_schema(self, schema: Schema) -> None:
        """Cache the source table schema to avoid repeated lookups."""
        self._source_schema = schema

    def get(self, destination_id: str) -> tuple[Catalog, Table]:
        """Get or create a (Catalog, Table) pair for a destination, pinned
        until the matching release().

        Returns the cached connection if available, otherwise creates a new
        one. Evicts the LRU unpinned entry if at capacity. Connection
        creation (a catalog connect, ~100ms) happens OUTSIDE the pool lock
        — a slot is reserved under the lock first, so capacity accounting
        stays correct while concurrent workers create connections for
        DIFFERENT destinations in parallel. (Concurrent creates for the
        same destination can't happen: the delivery layer's in-flight
        guard gives each destination at most one worker at a time.)
        """
        to_close: list[tuple[str, Catalog]] = []
        with self._lock:
            if destination_id in self._pool:
                self._pool.move_to_end(destination_id)
                self._pins[destination_id] = self._pins.get(destination_id, 0) + 1
                return self._pool[destination_id]

            # Evict LRU unpinned entries if at capacity (reserved slots count).
            # Pop under the lock; close OUTSIDE it — catalog.close() can take
            # tens of ms, and holding the lock serializes every concurrent
            # worker through this pool's hottest path under eviction pressure.
            while len(self._pool) + len(self._creating) >= self._max_open:
                evict_id = next(
                    (d for d in self._pool if self._pins.get(d, 0) == 0),
                    None,
                )
                if evict_id is None:
                    # Every entry is pinned (more workers than max_open —
                    # misconfiguration). Overshoot rather than deadlock.
                    log.warning(
                        "Destination pool over capacity (%d) with all entries pinned; overshooting",
                        self._max_open,
                    )
                    break
                evict_cat, _ = self._pool.pop(evict_id)
                to_close.append((evict_id, evict_cat))
                metrics.pool_evictions_total.inc()
                log.debug("Evicted connection for destination %s", evict_id)

            self._creating.add(destination_id)

        for evict_id, evict_cat in to_close:
            try:
                evict_cat.close()
            except Exception:
                log.warning("Error closing evicted connection for %s", evict_id)

        try:
            catalog, table = self._create(destination_id)
        except Exception:
            with self._lock:
                self._creating.discard(destination_id)
            raise

        with self._lock:
            self._creating.discard(destination_id)
            self._pool[destination_id] = (catalog, table)
            self._pins[destination_id] = self._pins.get(destination_id, 0) + 1
            metrics.pool_creates_total.inc()
            metrics.pool_open_connections.set(len(self._pool))
            return catalog, table

    def release(self, destination_id: str) -> None:
        """Unpin after use; closes any connection evicted while pinned."""
        with self._lock:
            count = self._pins.get(destination_id, 0)
            if count <= 1:
                self._pins.pop(destination_id, None)
            else:
                self._pins[destination_id] = count - 1
            if self._pins.get(destination_id, 0) == 0:
                for cat in self._zombies.pop(destination_id, []):
                    try:
                        cat.close()
                    except Exception:
                        log.warning("Error closing deferred connection for %s", destination_id)

    def evict(self, destination_id: str) -> None:
        """Force-evict a connection (e.g., after an error). If the entry is
        pinned (the caller itself, mid-retry), the close is deferred to the
        final release."""
        with self._lock:
            entry = self._pool.pop(destination_id, None)
            if entry is None:
                return
            cat, _ = entry
            if self._pins.get(destination_id, 0) > 0:
                self._zombies.setdefault(destination_id, []).append(cat)
            else:
                try:
                    cat.close()
                except Exception:
                    log.warning("Error closing evicted connection for %s", destination_id)
            metrics.pool_open_connections.set(len(self._pool))
            log.debug("Force-evicted connection for destination %s", destination_id)

    def close_all(self) -> None:
        """Close all open connections (and any deferred zombies)."""
        with self._lock:
            for dest_id, (cat, _) in self._pool.items():
                try:
                    cat.close()
                except Exception:
                    log.warning("Error closing connection for %s", dest_id)
            self._pool.clear()
            for dest_id, cats in self._zombies.items():
                for cat in cats:
                    try:
                        cat.close()
                    except Exception:
                        log.warning("Error closing deferred connection for %s", dest_id)
            self._zombies.clear()
            self._pins.clear()
            metrics.pool_open_connections.set(0)

    def _get_source_schema(self) -> Schema:
        """Get cached source schema, fetching it once if needed."""
        if self._source_schema is not None:
            return self._source_schema

        from pyducklake import Catalog as PyCatalog

        src_cfg = self._config.source
        src_catalog = PyCatalog(
            src_cfg.name,
            src_cfg.postgres_uri,
            data_path=src_cfg.data_path,
            properties=with_connection_defaults(src_cfg.resolved_properties()),
        )
        try:
            src_tbl = src_catalog.load_table(src_cfg.table)
            # `Table.schema` is a property in pyducklake -- do not call it.
            self._source_schema = src_tbl.schema
        finally:
            src_catalog.close()
        return self._source_schema

    def _create(self, destination_id: str) -> tuple[Catalog, Table]:
        """Create a new Catalog and load/create the destination table."""
        from pyducklake import Catalog

        dest_cfg: DestinationConfig = self._config.destination_by_id(destination_id)
        schema = self._get_source_schema()

        catalog = None
        try:
            catalog = Catalog(
                dest_cfg.name,
                dest_cfg.postgres_uri,
                data_path=dest_cfg.data_path,
                properties=with_connection_defaults(dest_cfg.resolved_properties()),
            )
            table = catalog.create_table_if_not_exists(dest_cfg.table, schema)
            log.info(
                "Connected to destination %s (catalog=%s, table=%s)", destination_id, dest_cfg.name, dest_cfg.table
            )
            return catalog, table
        except Exception:
            if catalog is not None:
                try:
                    catalog.close()
                except Exception:
                    pass
            raise
