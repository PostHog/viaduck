"""LRU connection pool for destination DuckLake catalogs."""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import TYPE_CHECKING

from viaduck import metrics

if TYPE_CHECKING:
    from pyducklake import Catalog, Table
    from pyducklake.schema import Schema

    from viaduck.config import DestinationConfig, ViaduckConfig

log = logging.getLogger(__name__)

_MIN_MAX_OPEN = 1


class DestinationPool:
    """LRU pool of destination Catalog connections.

    Bounds the number of open DuckDB connections to avoid unbounded memory growth
    when the destination count is high (100s-1000s).
    """

    def __init__(self, config: ViaduckConfig, max_open: int = 50):
        if max_open < _MIN_MAX_OPEN:
            raise ValueError(f"max_open must be >= {_MIN_MAX_OPEN}, got {max_open}")
        self._config = config
        self._max_open = max_open
        self._pool: OrderedDict[str, tuple[Catalog, Table]] = OrderedDict()
        self._source_schema: Schema | None = None

    @property
    def size(self) -> int:
        return len(self._pool)

    def set_source_schema(self, schema: Schema) -> None:
        """Cache the source table schema to avoid repeated lookups."""
        self._source_schema = schema

    def get(self, destination_id: str) -> tuple[Catalog, Table]:
        """Get or create a (Catalog, Table) pair for a destination.

        Returns the cached connection if available, otherwise creates a new one.
        Evicts the LRU entry if at capacity.
        """
        if destination_id in self._pool:
            self._pool.move_to_end(destination_id)
            return self._pool[destination_id]

        # Evict LRU if at capacity
        while len(self._pool) >= self._max_open:
            evict_id, (evict_cat, _) = self._pool.popitem(last=False)
            try:
                evict_cat.close()
            except Exception:
                log.warning("Error closing evicted connection for %s", evict_id)
            metrics.pool_evictions_total.inc()
            log.debug("Evicted connection for destination %s", evict_id)

        catalog, table = self._create(destination_id)
        self._pool[destination_id] = (catalog, table)
        metrics.pool_creates_total.inc()
        metrics.pool_open_connections.set(len(self._pool))
        return catalog, table

    def evict(self, destination_id: str) -> None:
        """Force-evict a connection (e.g., after an error)."""
        entry = self._pool.pop(destination_id, None)
        if entry is not None:
            cat, _ = entry
            try:
                cat.close()
            except Exception:
                log.warning("Error closing evicted connection for %s", destination_id)
            metrics.pool_open_connections.set(len(self._pool))
            log.debug("Force-evicted connection for destination %s", destination_id)

    def close_all(self) -> None:
        """Close all open connections."""
        for dest_id, (cat, _) in self._pool.items():
            try:
                cat.close()
            except Exception:
                log.warning("Error closing connection for %s", dest_id)
        self._pool.clear()
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
            properties=src_cfg.resolved_properties(),
        )
        try:
            src_tbl = src_catalog.load_table(src_cfg.table)
            self._source_schema = src_tbl.schema()
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
                properties=dest_cfg.resolved_properties(),
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
