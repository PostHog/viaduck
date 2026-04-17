"""Source DuckLake connection and CDC reading."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from viaduck import metrics

if TYPE_CHECKING:
    from pyducklake import Catalog, Table
    from pyducklake.cdc import ChangeSet

    from viaduck.config import SourceConfig

log = logging.getLogger(__name__)


def connect(cfg: SourceConfig) -> Catalog:
    """Create a Catalog connection to the source DuckLake."""
    from pyducklake import Catalog

    return Catalog(
        cfg.name,
        cfg.postgres_uri,
        data_path=cfg.data_path,
        properties=cfg.resolved_properties(),
    )


def load_table(catalog: Catalog, table_name: str) -> Table:
    """Load the source table. Raises if it doesn't exist."""
    return catalog.load_table(table_name)


def current_snapshot_id(table: Table) -> int | None:
    """Get the current snapshot ID, or None if no snapshots exist."""
    snap = table.current_snapshot()
    if snap is None:
        return None
    return snap.snapshot_id


def read_cdc(
    table: Table,
    start_snapshot: int,
    end_snapshot: int,
    *,
    filter_expr: str | None = None,
) -> pa.Table:
    """Read CDC insertions between two snapshots, returning an Arrow table.

    Uses table_insertions with optional filter pushdown for efficiency.
    """
    t0 = time.monotonic()

    kwargs: dict = {
        "start_snapshot": start_snapshot,
        "end_snapshot": end_snapshot,
    }
    if filter_expr is not None:
        kwargs["filter_expr"] = filter_expr

    changeset: ChangeSet = table.table_insertions(**kwargs)
    result = changeset.to_arrow()

    duration = time.monotonic() - t0
    metrics.cdc_read_seconds.observe(duration)
    metrics.cdc_rows_read_total.inc(result.num_rows)

    log.debug(
        "CDC read: snapshots %d→%d, %d rows in %.3fs%s",
        start_snapshot,
        end_snapshot,
        result.num_rows,
        duration,
        f" (filter: {filter_expr})" if filter_expr else "",
    )

    return result
