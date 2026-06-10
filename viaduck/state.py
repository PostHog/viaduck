"""State tracking for per-destination replication cursors, on plain Postgres.

The cursor store used to be a DuckLake table on the source catalog. That
put an analytical table format on an OLTP write pattern: every cursor
advance was a DuckLake commit, which created a catalog snapshot, which made
the next poll's CDC read see "new" changes — a self-perpetuating treadmill
of empty reads, tiny parquet files, and snapshot accretion even with zero
source traffic. Plain Postgres rows (by default in the same database that
already hosts the source catalog's metadata) make a cursor advance a single
upsert: no snapshots, no files, no treadmill.

Atomicity is unchanged by the move: the destination apply and the cursor
advance were ALWAYS separate transactions (at-least-once with idempotent
apply covers the gap — see tla/Viaduck.tla). The only atomicity the store
itself needs is per-advance, which a single ``INSERT ... ON CONFLICT DO
UPDATE`` provides natively.

Thread-safety: one connection guarded by a lock, with one reconnect retry
on connection errors. Cursor writes are low-rate (per flush, not per poll
cycle), so a pool is not warranted.
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg

if TYPE_CHECKING:
    from viaduck.config import StateConfig

log = logging.getLogger(__name__)

# State table name comes from config and is interpolated into DDL/DML
# (identifiers can't be bound as parameters). Restrict to a safe identifier
# so a config typo can't smuggle SQL.
_SAFE_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass
class DestinationCursor:
    destination_id: str
    instance_id: str
    last_snapshot_id: int
    rows_replicated: int = 0
    last_error: str | None = None


class StateManager:
    """Manages the viaduck_state table in Postgres."""

    def __init__(self, postgres_uri: str, instance_id: str, state_config: StateConfig):
        self._uri = postgres_uri
        self._instance_id = instance_id
        table = state_config.table
        if not _SAFE_TABLE_NAME.match(table):
            raise ValueError(f"state.table {table!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)")
        self._table = table
        self._lock = threading.Lock()
        self._conn: psycopg.Connection | None = None
        self._table_ensured = False

    # -- connection plumbing -------------------------------------------------

    def _connection(self) -> psycopg.Connection:
        """Open (or reuse) the connection. Caller must hold the lock."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._uri, autocommit=True)
            self._conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    destination_id     text        NOT NULL,
                    instance_id        text        NOT NULL,
                    last_snapshot_id   bigint      NOT NULL,
                    last_replicated_at timestamptz,
                    rows_replicated    bigint      NOT NULL DEFAULT 0,
                    last_error         text,
                    last_error_at      timestamptz,
                    updated_at         timestamptz NOT NULL,
                    PRIMARY KEY (destination_id, instance_id)
                )
                """
            )
            if not self._table_ensured:
                log.info("State table '%s' ready", self._table)
                self._table_ensured = True
        return self._conn

    def _run(self, op: Callable[[psycopg.Connection], Any]) -> Any:
        """Run op under the lock, with one reconnect retry on connection loss."""
        with self._lock:
            try:
                return op(self._connection())
            except psycopg.OperationalError:
                log.warning("State store connection lost; reconnecting once", exc_info=True)
                try:
                    if self._conn is not None:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
                return op(self._connection())

    # -- public API (unchanged from the DuckLake-backed predecessor) ---------

    def load_cursors(self, destination_ids: list[str]) -> dict[str, DestinationCursor]:
        """Load current cursors for the given destinations owned by this instance."""
        if not destination_ids:
            return {}

        def _op(conn: psycopg.Connection):
            rows = conn.execute(
                f"""
                SELECT destination_id, instance_id, last_snapshot_id, rows_replicated, last_error
                FROM {self._table}
                WHERE instance_id = %s AND destination_id = ANY(%s)
                """,
                (self._instance_id, destination_ids),
            ).fetchall()
            return {
                r[0]: DestinationCursor(
                    destination_id=r[0],
                    instance_id=r[1],
                    last_snapshot_id=r[2],
                    rows_replicated=r[3] or 0,
                    last_error=r[4],
                )
                for r in rows
            }

        return self._run(_op)

    def initialize_destinations(self, destination_ids: list[str]) -> None:
        """Ensure all destination IDs have a state row. Creates rows with snapshot_id=0."""
        if not destination_ids:
            return
        now = datetime.now(UTC)

        def _op(conn: psycopg.Connection):
            cur = conn.execute(
                f"""
                INSERT INTO {self._table}
                    (destination_id, instance_id, last_snapshot_id, rows_replicated, updated_at)
                SELECT unnest(%s::text[]), %s, 0, 0, %s
                ON CONFLICT (destination_id, instance_id) DO NOTHING
                """,
                (destination_ids, self._instance_id, now),
            )
            return cur.rowcount

        created = self._run(_op)
        if created:
            log.info("Initialized state for %d new destinations", created)
        else:
            log.info(
                "All %d assigned destinations already have state rows; nothing to initialize",
                len(destination_ids),
            )

    def advance_cursor(self, destination_id: str, snapshot_id: int, cumulative_rows: int | None = None) -> None:
        """Update a destination's cursor after successful replication.

        A single upsert — atomic by construction. If cumulative_rows is
        None, the existing value is preserved. A successful advance clears
        any recorded error.
        """
        now = datetime.now(UTC)

        def _op(conn: psycopg.Connection):
            conn.execute(
                f"""
                INSERT INTO {self._table}
                    (destination_id, instance_id, last_snapshot_id, last_replicated_at,
                     rows_replicated, last_error, last_error_at, updated_at)
                VALUES (%s, %s, %s, %s, COALESCE(%s, 0), NULL, NULL, %s)
                ON CONFLICT (destination_id, instance_id) DO UPDATE SET
                    last_snapshot_id   = EXCLUDED.last_snapshot_id,
                    last_replicated_at = EXCLUDED.last_replicated_at,
                    rows_replicated    = COALESCE(%s, {self._table}.rows_replicated),
                    last_error         = NULL,
                    last_error_at      = NULL,
                    updated_at         = EXCLUDED.updated_at
                """,
                (destination_id, self._instance_id, snapshot_id, now, cumulative_rows, now, cumulative_rows),
            )

        self._run(_op)

    def advance_cursors(self, destination_ids: list[str], snapshot_id: int) -> None:
        """Advance multiple destinations to the same snapshot atomically.

        One statement, one implicit transaction; rows_replicated and any
        recorded error handling match advance_cursor(cumulative_rows=None)
        semantics (preserve count, clear error).
        """
        if not destination_ids:
            return
        now = datetime.now(UTC)

        def _op(conn: psycopg.Connection):
            conn.execute(
                f"""
                INSERT INTO {self._table}
                    (destination_id, instance_id, last_snapshot_id, last_replicated_at,
                     rows_replicated, last_error, last_error_at, updated_at)
                SELECT unnest(%s::text[]), %s, %s, %s, 0, NULL, NULL, %s
                ON CONFLICT (destination_id, instance_id) DO UPDATE SET
                    last_snapshot_id   = EXCLUDED.last_snapshot_id,
                    last_replicated_at = EXCLUDED.last_replicated_at,
                    rows_replicated    = {self._table}.rows_replicated,
                    last_error         = NULL,
                    last_error_at      = NULL,
                    updated_at         = EXCLUDED.updated_at
                """,
                (destination_ids, self._instance_id, snapshot_id, now, now),
            )

        self._run(_op)

    def record_error(self, destination_id: str, error: str) -> None:
        """Record an error for a destination without advancing its cursor.

        Silent no-op when the destination has no state row (predecessor
        behavior). Clears last_replicated_at, also matching the predecessor.
        """
        now = datetime.now(UTC)

        def _op(conn: psycopg.Connection):
            cur = conn.execute(
                f"""
                UPDATE {self._table} SET
                    last_replicated_at = NULL,
                    last_error         = %s,
                    last_error_at      = %s,
                    updated_at         = %s
                WHERE destination_id = %s AND instance_id = %s
                """,
                (error, now, now, destination_id, self._instance_id),
            )
            return cur.rowcount

        updated = self._run(_op)
        if updated:
            log.warning("Recorded error for destination %s: %s", destination_id, error)

    def close(self) -> None:
        """Close the connection. Safe to call multiple times."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
