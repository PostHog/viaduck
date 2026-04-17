"""State tracking for per-destination replication cursors on the source DuckLake."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyducklake import Catalog, Table

    from viaduck.config import StateConfig

log = logging.getLogger(__name__)


@dataclass
class DestinationCursor:
    destination_id: str
    instance_id: str
    last_snapshot_id: int
    rows_replicated: int = 0
    last_error: str | None = None


class StateManager:
    """Manages the _viaduck_state table on the source DuckLake catalog."""

    def __init__(self, catalog: Catalog, instance_id: str, state_config: StateConfig):
        self._catalog = catalog
        self._instance_id = instance_id
        self._table_name = state_config.table
        self._table: Table | None = None

    def ensure_table(self) -> Table:
        """Create the state table if it doesn't exist, and return it."""
        if self._table is not None:
            return self._table

        from pyducklake import Schema
        from pyducklake.types import BigIntType, NestedField, StringType, TimestampTZType

        fields = [
            NestedField(field_id=1, name="destination_id", field_type=StringType(), required=True),
            NestedField(field_id=2, name="instance_id", field_type=StringType(), required=True),
            NestedField(field_id=3, name="last_snapshot_id", field_type=BigIntType(), required=True),
            NestedField(field_id=4, name="last_replicated_at", field_type=TimestampTZType()),
            NestedField(field_id=5, name="rows_replicated", field_type=BigIntType()),
            NestedField(field_id=6, name="last_error", field_type=StringType()),
            NestedField(field_id=7, name="last_error_at", field_type=TimestampTZType()),
            NestedField(field_id=8, name="updated_at", field_type=TimestampTZType(), required=True),
        ]
        schema = Schema(*fields)

        self._table = self._catalog.create_table_if_not_exists(self._table_name, schema)
        log.info("State table '%s' ready", self._table_name)
        return self._table

    def load_cursors(self, destination_ids: list[str]) -> dict[str, DestinationCursor]:
        """Load current cursors for the given destinations owned by this instance.

        Uses filter pushdown to avoid full table scans at scale.
        """
        from pyducklake.expressions import And, EqualTo, In

        table = self.ensure_table()

        row_filter = And(
            EqualTo("instance_id", self._instance_id),
            In("destination_id", tuple(destination_ids)),
        )
        scan = table.scan(row_filter=row_filter)
        arrow = scan.to_arrow()

        if arrow.num_rows == 0:
            return {}

        cursors = {}
        for i in range(arrow.num_rows):
            dest_id = arrow.column("destination_id")[i].as_py()
            cursors[dest_id] = DestinationCursor(
                destination_id=dest_id,
                instance_id=arrow.column("instance_id")[i].as_py(),
                last_snapshot_id=arrow.column("last_snapshot_id")[i].as_py(),
                rows_replicated=arrow.column("rows_replicated")[i].as_py() or 0,
                last_error=arrow.column("last_error")[i].as_py(),
            )
        return cursors

    def initialize_destinations(self, destination_ids: list[str]) -> None:
        """Ensure all destination IDs have a state row. Creates rows with snapshot_id=0."""
        import pyarrow as pa

        existing = self.load_cursors(destination_ids)
        new_ids = [did for did in destination_ids if did not in existing]

        if not new_ids:
            return

        now = datetime.now(UTC)
        table = self.ensure_table()

        rows = pa.table(
            {
                "destination_id": new_ids,
                "instance_id": [self._instance_id] * len(new_ids),
                "last_snapshot_id": [0] * len(new_ids),
                "last_replicated_at": [None] * len(new_ids),
                "rows_replicated": [0] * len(new_ids),
                "last_error": [None] * len(new_ids),
                "last_error_at": [None] * len(new_ids),
                "updated_at": [now] * len(new_ids),
            }
        )
        table.append(rows)
        log.info("Initialized state for %d new destinations", len(new_ids))

    def advance_cursor(self, destination_id: str, snapshot_id: int, cumulative_rows: int | None = None) -> None:
        """Update a destination's cursor after successful replication.

        Uses a catalog transaction to make the delete + insert atomic.
        If cumulative_rows is None, preserves the existing value from state.
        """
        import pyarrow as pa
        from pyducklake.expressions import And, EqualTo

        now = datetime.now(UTC)

        if cumulative_rows is None:
            cursors = self.load_cursors([destination_id])
            current = cursors.get(destination_id)
            cumulative_rows = current.rows_replicated if current else 0

        with self._catalog.begin_transaction() as txn:
            tbl = txn.load_table(self._table_name)
            tbl.delete(And(EqualTo("destination_id", destination_id), EqualTo("instance_id", self._instance_id)))
            row = pa.table(
                {
                    "destination_id": [destination_id],
                    "instance_id": [self._instance_id],
                    "last_snapshot_id": [snapshot_id],
                    "last_replicated_at": [now],
                    "rows_replicated": [cumulative_rows],
                    "last_error": [None],
                    "last_error_at": [None],
                    "updated_at": [now],
                }
            )
            tbl.append(row)

    def advance_cursors(self, destination_ids: list[str], snapshot_id: int) -> None:
        """Advance multiple destinations to the same snapshot atomically."""
        import pyarrow as pa
        from pyducklake.expressions import And, EqualTo

        now = datetime.now(UTC)
        cursors = self.load_cursors(destination_ids)

        with self._catalog.begin_transaction() as txn:
            tbl = txn.load_table(self._table_name)
            for did in destination_ids:
                current = cursors.get(did)
                cumulative_rows = current.rows_replicated if current else 0
                tbl.delete(And(EqualTo("destination_id", did), EqualTo("instance_id", self._instance_id)))
                row = pa.table(
                    {
                        "destination_id": [did],
                        "instance_id": [self._instance_id],
                        "last_snapshot_id": [snapshot_id],
                        "last_replicated_at": [now],
                        "rows_replicated": [cumulative_rows],
                        "last_error": [None],
                        "last_error_at": [None],
                        "updated_at": [now],
                    }
                )
                tbl.append(row)

    def record_error(self, destination_id: str, error: str) -> None:
        """Record an error for a destination without advancing its cursor."""
        import pyarrow as pa
        from pyducklake.expressions import And, EqualTo

        now = datetime.now(UTC)

        cursors = self.load_cursors([destination_id])
        current = cursors.get(destination_id)

        if current is None:
            return

        with self._catalog.begin_transaction() as txn:
            tbl = txn.load_table(self._table_name)
            tbl.delete(And(EqualTo("destination_id", destination_id), EqualTo("instance_id", self._instance_id)))
            row = pa.table(
                {
                    "destination_id": [destination_id],
                    "instance_id": [self._instance_id],
                    "last_snapshot_id": [current.last_snapshot_id],
                    "last_replicated_at": [None],
                    "rows_replicated": [current.rows_replicated],
                    "last_error": [error],
                    "last_error_at": [now],
                    "updated_at": [now],
                }
            )
            tbl.append(row)
        log.warning("Recorded error for destination %s: %s", destination_id, error)
