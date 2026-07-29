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

# Bound every state operation: the lock in _run serializes ALL cursor
# traffic (flush workers + the poll thread), so one hung connection during
# a catalog-PG failover would otherwise stall the whole pipeline silently
# instead of tripping the loop's fatal path.
_CONNECT_TIMEOUT_S = 10
_STATEMENT_TIMEOUT_MS = 30_000


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
        schema = state_config.schema
        for name, value in (("state.table", table), ("state.schema", schema)):
            if not _SAFE_TABLE_NAME.match(value):
                raise ValueError(f"{name} {value!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)")
        self._table = table
        self._schema = schema
        # Dedicated schema keeps viaduck's bookkeeping out of the ducklake
        # catalog's namespace (the default URI is the source catalog's
        # database), and gives a future scoped-down user a clean GRANT
        # boundary. Unqualified `{self._table}.col` references below are
        # intentional: the bare table name is the implicit alias of the
        # schema-qualified INSERT/UPDATE target.
        self._qualified = f"{schema}.{table}"
        # Lifecycle table name derives from the cursor table name so two
        # pipelines sharing one Postgres (the default URI is the source
        # catalog's DB) with colliding destination ids cannot pause each
        # other — same per-pipeline isolation the cursor table gets.
        self._lifecycle_qualified = f"{schema}.{table}_lifecycle"
        self._lock = threading.Lock()
        self._conn: psycopg.Connection | None = None
        self._table_ensured = False

    # -- connection plumbing -------------------------------------------------

    def _connection(self) -> psycopg.Connection:
        """Open (or reuse) the connection. Caller must hold the lock."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(
                self._uri,
                autocommit=True,
                connect_timeout=_CONNECT_TIMEOUT_S,
                options=f"-c statement_timeout={_STATEMENT_TIMEOUT_MS}",
            )
            self._ensure_table(self._conn)
        return self._conn

    def _ensure_table(self, conn: psycopg.Connection) -> None:
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        except (psycopg.errors.DuplicateSchema, psycopg.errors.UniqueViolation):
            log.info("State schema '%s' created concurrently by another instance", self._schema)
        try:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._qualified} (
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
        except (psycopg.errors.DuplicateTable, psycopg.errors.UniqueViolation):
            # Two instances racing the first boot: IF NOT EXISTS still
            # raises a unique violation on the pg_class/pg_type insert in
            # one of them. The table exists either way.
            log.info("State table '%s' created concurrently by another instance", self._table)
        try:
            # Lifecycle is per-DESTINATION operator intent (pausing a
            # destination pauses it on every instance), unlike the cursor
            # table's per-(destination, instance) rows. Absent row = active;
            # see viaduck/lifecycle.py for the state semantics.
            # NOTE: CREATE IF NOT EXISTS never updates the CHECK — adding a
            # state to lifecycle.VALID_STATES later needs a migration on
            # every existing deployment's table.
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._lifecycle_qualified} (
                    destination_id text        PRIMARY KEY,
                    state          text        NOT NULL
                        CHECK (state IN ('active', 'paused', 'draining', 'retired')),
                    reason         text,
                    updated_by     text,
                    updated_at     timestamptz NOT NULL
                )
                """
            )
        except (psycopg.errors.DuplicateTable, psycopg.errors.UniqueViolation):
            log.info("Lifecycle table created concurrently by another instance")
        if not self._table_ensured:
            log.info("State table '%s' ready", self._table)
            self._table_ensured = True

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
                FROM {self._qualified}
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

    def initialize_destinations(self, destination_ids: list[str], initial_snapshot_id: int = 0) -> None:
        """Ensure all destination IDs have a state row.

        Creates rows with last_snapshot_id=initial_snapshot_id for new destinations.
        Existing rows are left unchanged (ON CONFLICT DO NOTHING).
        """
        if not destination_ids:
            return
        now = datetime.now(UTC)

        def _op(conn: psycopg.Connection):
            cur = conn.execute(
                f"""
                INSERT INTO {self._qualified}
                    (destination_id, instance_id, last_snapshot_id, rows_replicated, updated_at)
                SELECT unnest(%s::text[]), %s, %s, 0, %s
                ON CONFLICT (destination_id, instance_id) DO NOTHING
                """,
                (destination_ids, self._instance_id, initial_snapshot_id, now),
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

    def advance_cursor(self, destination_id: str, snapshot_id: int, cumulative_rows: int | None = None) -> int:
        """Update a destination's cursor after successful replication.

        A single upsert — atomic by construction. If cumulative_rows is
        None, the existing value is preserved. A successful advance clears
        any recorded error. Stale writes (snapshot_id below the stored
        cursor) are dropped entirely — cursors never regress
        (CursorMonotonicity in tla/Viaduck.tla); per-destination flush
        serialization should prevent out-of-order acks, this is
        defense-in-depth.

        Returns the affected row count: 0 means the monotonic guard
        dropped the write (a concurrent advance got there first) — the
        retention clamp uses this to detect a lost race instead of
        recording a phantom data-loss note.
        """
        now = datetime.now(UTC)

        def _op(conn: psycopg.Connection):
            cur = conn.execute(
                f"""
                INSERT INTO {self._qualified}
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
                WHERE {self._table}.last_snapshot_id <= EXCLUDED.last_snapshot_id
                """,
                (destination_id, self._instance_id, snapshot_id, now, cumulative_rows, now, cumulative_rows),
            )
            return cur.rowcount

        return self._run(_op)

    def max_cursor_any_instance(self, destination_id: str) -> int | None:
        """MAX(last_snapshot_id) across ALL instance rows for one
        destination, or None when no row exists anywhere. The
        reconciler's activate uses this when a fleet resize reshuffled
        hash assignment: the new owner has no row of its own, and
        initializing at head would silently skip everything between the
        old owner's cursor and head (C3 §4 step 3)."""

        def _op(conn: psycopg.Connection):
            row = conn.execute(
                f"SELECT MAX(last_snapshot_id) FROM {self._qualified} WHERE destination_id = %s",
                (destination_id,),
            ).fetchone()
            return None if row is None or row[0] is None else int(row[0])

        return self._run(_op)

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
                INSERT INTO {self._qualified}
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
                WHERE {self._table}.last_snapshot_id <= EXCLUDED.last_snapshot_id
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
                UPDATE {self._qualified} SET
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

    # -- destination lifecycle (see viaduck/lifecycle.py) --------------------

    def load_lifecycle_states(self, destination_ids: list[str]) -> dict[str, str]:
        """Raw lifecycle rows for the given destinations. Missing ids are
        absent from the result (absent row = active; the caller normalizes
        via lifecycle.normalize so the fail-safe unknown-state handling
        lives in exactly one place)."""
        if not destination_ids:
            return {}

        def _op(conn: psycopg.Connection):
            rows = conn.execute(
                f"""
                SELECT destination_id, state
                FROM {self._lifecycle_qualified}
                WHERE destination_id = ANY(%s)
                """,
                (destination_ids,),
            ).fetchall()
            return {r[0]: r[1] for r in rows}

        return self._run(_op)

    def load_lifecycle_rows(self, destination_ids: list[str]) -> dict[str, dict]:
        """Full lifecycle rows (state/reason/updated_by/updated_at) for the
        read-only /lifecycle endpoint. Missing ids absent, like
        load_lifecycle_states."""
        if not destination_ids:
            return {}

        def _op(conn: psycopg.Connection):
            rows = conn.execute(
                f"""
                SELECT destination_id, state, reason, updated_by, updated_at
                FROM {self._lifecycle_qualified}
                WHERE destination_id = ANY(%s)
                """,
                (destination_ids,),
            ).fetchall()
            return {
                r[0]: {
                    "state": r[1],
                    "reason": r[2],
                    "updated_by": r[3],
                    "updated_at": r[4].isoformat() if r[4] else None,
                }
                for r in rows
            }

        return self._run(_op)

    def delete_destination_state(self, destination_id: str) -> int:
        """Sever a destination's resume point: delete its cursor rows for
        ALL instances (retirement is per-destination). Called when a
        destination is observed RETIRED — at startup before initialization,
        and idempotently each cycle for a mid-run retire (an in-flight
        flush completing after the delete would upsert the row back; the
        next cycle's delete removes it again). This is what makes
        "re-add = new tenant = fresh seed" true by construction: with no
        cursor row, a re-activated destination seeds per seed_mode instead
        of resuming from a stale snapshot the source may have expired."""

        def _op(conn: psycopg.Connection):
            cur = conn.execute(
                f"DELETE FROM {self._qualified} WHERE destination_id = %s",
                (destination_id,),
            )
            return cur.rowcount

        deleted = self._run(_op)
        if deleted:
            log.warning(
                "Severed resume point for retired destination %s (%d cursor row(s) deleted; re-add will re-seed)",
                destination_id,
                deleted,
            )
        return deleted

    def set_lifecycle_state(self, destination_id: str, state: str, *, reason: str, updated_by: str) -> None:
        """Write a lifecycle state. REFUSES 'retired': retirement is an
        explicit human ack through the documented SQL, never a code path —
        an erroneous automated retire would discard the resume point and
        turn a later re-add into a full re-seed."""
        from viaduck import lifecycle

        if state not in lifecycle.WRITABLE_STATES:
            raise ValueError(
                f"viaduck code may not write lifecycle state {state!r} "
                f"(writable: {sorted(lifecycle.WRITABLE_STATES)}); "
                "'retired' requires an operator UPDATE with updated_by set"
            )
        now = datetime.now(UTC)

        def _op(conn: psycopg.Connection):
            conn.execute(
                f"""
                INSERT INTO {self._lifecycle_qualified}
                    (destination_id, state, reason, updated_by, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (destination_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    reason = EXCLUDED.reason,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = EXCLUDED.updated_at
                """,
                (destination_id, state, reason, updated_by, now),
            )

        self._run(_op)
        log.warning(
            "Lifecycle state for %s set to %s (reason: %s, by: %s)",
            destination_id,
            state,
            reason,
            updated_by,
        )

    def close(self) -> None:
        """Close the connection. Safe to call multiple times."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
