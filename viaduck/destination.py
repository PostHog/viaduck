"""LRU connection pool for destination DuckLake catalogs."""

from __future__ import annotations

import logging
import re
import threading
from collections import OrderedDict
from typing import TYPE_CHECKING

from viaduck import metrics, schema_projection
from viaduck.source import with_connection_defaults

if TYPE_CHECKING:
    from pyducklake import Catalog, Table
    from pyducklake.schema import Schema

    from viaduck.config import DestinationConfig, ViaduckConfig
    from viaduck.schema_projection import ProjectionPlan

log = logging.getLogger(__name__)

_MIN_MAX_OPEN = 1


class DestinationConnectError(Exception):
    """Connect/attach failure with credentials scrubbed from the message.
    Raised `from None` so the original (password-bearing) exception text
    cannot reach logs via the traceback chain."""


_CRED_PATTERNS = (
    # libpq kv form incl. quoted values and space-padded '='; IGNORECASE
    # covers PASSWORD=. Quoted form stops at the escaped-quote boundary.
    re.compile(r"password\s*=\s*(?:'(?:\\.|[^'])*'|\S+)", re.IGNORECASE),
    # Python dict-repr style ('password': '...') from wrapped driver kwargs.
    re.compile(r"(['\"]password['\"]\s*:\s*)['\"](?:\\.|[^'\"])*['\"]", re.IGNORECASE),
    # URL userinfo. GREEDY password segment so a password containing '@'
    # scrubs to the LAST '@' (the host separator) — the non-greedy form
    # leaked the tail of such passwords (round-2 review).
    re.compile(r"://([^/:@\s]+):(\S+)@"),
)


def _scrub_credentials(text: str) -> str:
    text = _CRED_PATTERNS[0].sub("password=***", text)
    text = _CRED_PATTERNS[1].sub(r"\1'***'", text)
    return _CRED_PATTERNS[2].sub(r"://\1:***@", text)


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
        # Per-destination projection plan. Populated in _create() when the
        # destination has schema_projection_enabled=True. Callers read via
        # projection_for(destination_id); identity projections and disabled
        # destinations both return None so callers can skip .apply() on the
        # hot path.
        self._projections: dict[str, ProjectionPlan] = {}

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
                self._projections.pop(evict_id, None)
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
            catalog, table, plan = self._create(destination_id)
        except Exception:
            with self._lock:
                self._creating.discard(destination_id)
            raise

        with self._lock:
            self._creating.discard(destination_id)
            self._pool[destination_id] = (catalog, table)
            # Store projection under the same lock that owns _pool mutation.
            # Otherwise a concurrent evict() of this destination (LRU pressure
            # from a peer worker) could pop _projections[dest] in between our
            # projection build and our pool insert, leaving _pool populated
            # but _projections empty → projection_for(dest) returns None →
            # identity fall-through → positional insert into a misshaped
            # target. This is the silent-slot-corruption class the module
            # exists to prevent.
            if plan is not None:
                self._projections[destination_id] = plan
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
        to_close: Catalog | None = None
        with self._lock:
            entry = self._pool.pop(destination_id, None)
            if entry is None:
                return
            self._projections.pop(destination_id, None)
            cat, _ = entry
            if self._pins.get(destination_id, 0) > 0:
                self._zombies.setdefault(destination_id, []).append(cat)
            else:
                # Close OUTSIDE the lock, same as get()'s LRU path: closing a
                # ducklake-attached catalog does nontrivial native work, and
                # force-evict is now a per-attempt hot path when an instance
                # is fatally invalidated (see apply._write_with_retry) — a
                # slow close here must not serialize every flush worker and
                # the poll thread through the pool lock.
                to_close = cat
            metrics.pool_open_connections.set(len(self._pool))
            log.debug("Force-evicted connection for destination %s", destination_id)
        if to_close is not None:
            try:
                to_close.close()
            except Exception:
                log.warning("Error closing evicted connection for %s", destination_id)

    def close_all(self) -> None:
        """Close all open connections (and any deferred zombies)."""
        with self._lock:
            for dest_id, (cat, _) in self._pool.items():
                try:
                    cat.close()
                except Exception:
                    log.warning("Error closing connection for %s", dest_id)
            self._pool.clear()
            self._projections.clear()
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

    def projection_for(self, destination_id: str) -> ProjectionPlan | None:
        """Return the cached ProjectionPlan for a destination, or None when the
        destination is identity-shaped (or projection is disabled). Callers
        should apply the plan on the write path before invoking `tbl.append`
        / `tbl.upsert`.

        Reads under `self._lock` — a `_projections.get()` racing an `evict()`
        `pop()` is safe in CPython (GIL guards individual dict ops), but
        pairing the read with `_pool` requires ordering to reason about,
        and callers pull this immediately before invoking `tbl.append` /
        `tbl.upsert` on the pool entry.
        """
        with self._lock:
            plan = self._projections.get(destination_id)
        if plan is None or plan.is_identity:
            return None
        return plan

    def _maybe_build_projection(
        self,
        destination_id: str,
        dest_cfg: DestinationConfig,
        *,
        source_schema: Schema,
        table: Table,
    ) -> ProjectionPlan | None:
        """Build the ProjectionPlan for this destination when opted in.

        Called from `_create()` after the destination table exists; runs
        OUTSIDE `self._lock` (same as the catalog connect). The caller stores
        the returned plan into `self._projections` under `self._lock`, so
        every mutation of `_projections` happens with the pool lock held
        (avoids torn reads with concurrent `evict()`/`close_all()`).

        Raises `SchemaProjectionError` on unresolvable schema drift — that's
        the whole point of the module; silent shape drift is exactly what
        got us here.
        """
        if not dest_cfg.schema_projection_enabled:
            return None

        # `source_schema` here is a pyducklake Schema; convert to Arrow via
        # Schema.as_arrow() so the projection module can work with pa.Schema
        # directly. Same conversion the source read path applies before
        # sending batches to destinations, so source_schema.as_arrow() is
        # exactly the shape batches arrive in at write time. `Table.schema`
        # is a property; call as_arrow() on the returned Schema.
        src_arrow = source_schema.as_arrow()
        tgt_arrow = table.schema.as_arrow()

        plan = schema_projection.build(
            src_arrow,
            tgt_arrow,
            drop_allow=set(dest_cfg.drop_source_columns),
            key_columns=tuple(self._config.routing.key_columns),
            routing_field=self._config.routing.field,
        )
        log.info(
            "Schema projection built for destination %s: %s",
            destination_id,
            plan.describe(),
        )
        return plan

    def _create(self, destination_id: str) -> tuple[Catalog, Table, ProjectionPlan | None]:
        """Create a new Catalog and load/create the destination table.

        Returns `(catalog, table, plan)`. `plan` is None when the destination
        has schema projection disabled; the caller stores it into
        `self._projections` under `self._lock` — every `_projections` mutation
        must happen with the pool lock held.
        """
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
            # Discovered tenants land in per-team schemas that don't
            # pre-exist the way `posthog` does (e.g. `evilco.events` for a
            # team whose schema_name is evilco) — ensure the namespace
            # before the table. Idempotent; bare (unqualified) table names
            # skip it.
            if "." in dest_cfg.table:
                catalog.create_namespace_if_not_exists(dest_cfg.table.rsplit(".", 1)[0])
            table = catalog.create_table_if_not_exists(dest_cfg.table, schema)
            _ensure_partition_spec(table, dest_cfg, destination_id)
            plan = self._maybe_build_projection(destination_id, dest_cfg, source_schema=schema, table=table)
            log.info(
                "Connected to destination %s (catalog=%s, table=%s)", destination_id, dest_cfg.name, dest_cfg.table
            )
            return catalog, table, plan
        except Exception as e:
            if catalog is not None:
                try:
                    catalog.close()
                except Exception:
                    pass
            # DuckDB's ATTACH errors embed the FULL connection string —
            # password included — and the flush-failure path logs
            # exceptions with tracebacks. Scrub before re-raising, and
            # break the chain (`from None`): the original exception's
            # message would otherwise ride along in the traceback.
            raise DestinationConnectError(f"destination {destination_id}: {_scrub_credentials(str(e))}") from None


# SQL metacharacters that should never appear in a pyducklake-returned
# fully_qualified_name. A presence indicates either pyducklake's quoting
# contract is broken or operator-controlled config has injected SQL — either
# way, we refuse to probe rather than interpolate into the SELECT.
_FQN_FORBIDDEN_CHARS = (";", "--", "/*", "*/", "\n", "\r")


def _table_has_data(table: Table) -> bool:
    """Return True if the destination table has any rows committed.

    Used to gate the populated-table ALTER risk: a fresh, just-created
    table is always safe to ALTER (no files to mis-align); a table with
    existing data needs explicit operator opt-in per QE review.

    Uses a `SELECT 1 ... LIMIT 1` probe rather than COUNT(*) so the cost
    is bounded regardless of table size. Catalog-backend portable — does
    not depend on inspecting catalog tables directly.

    SQL safety: `fully_qualified_name` is interpolated directly into the
    probe — pyducklake is expected to return a properly-quoted identifier
    (its own internal SQL uses the same pattern, see partitioning.py
    UpdateSpec.commit). We add a defensive SQL-metacharacter check as a
    sharp-edge guard against either a pyducklake contract change or a
    misconfigured destination whose `table` field somehow leaks meta-chars.
    Suspicious fqn → refuse to probe → treat as populated (conservative).

    A failure to probe (network blip, transient ducklake error, permission
    denied, suspicious fqn) is treated as "has data" — conservative default;
    if we can't prove the table is empty, refuse to ALTER. The pod will
    retry on next cold-connect. The log is ERROR (not WARNING) because
    every call site here is in a context where partition_by is configured,
    so this directly causes a refusal that the operator needs to act on.
    """
    fqn = table.fully_qualified_name
    if any(c in fqn for c in _FQN_FORBIDDEN_CHARS):
        log.error(
            "Refusing to probe table with suspicious fully_qualified_name %r "
            "(contains SQL meta-character); treating as populated for safety",
            fqn,
        )
        return True
    try:
        result = table.catalog.connection.execute(f"SELECT 1 FROM {fqn} LIMIT 1").fetchone()
    except Exception as e:
        log.error(
            "Failed to probe table %s for emptiness; treating as populated for safety. "
            "Will refuse to ALTER until probe succeeds on next cold-connect. Error: %s",
            fqn,
            e,
        )
        return True
    return result is not None


def _get_transform_map() -> dict[str, object]:
    """Thin wrapper around `viaduck.partition_transforms.get_transform_map()`.

    The shared module owns the caching (via `@functools.cache`); we keep
    this wrapper to localize the import + give a single seam for tests
    to mock if they ever need to.
    """
    from viaduck.partition_transforms import get_transform_map

    return get_transform_map()


def _ensure_partition_spec(table: Table, dest_cfg: DestinationConfig, destination_id: str) -> None:
    """Apply `dest_cfg.partition_by` to `table` if the table is currently
    unpartitioned.

    Behavior matrix:
      - config empty + table unpartitioned → no-op
      - config empty + table partitioned   → no-op (leave existing spec alone)
      - config set   + table unpartitioned → ALTER TABLE SET PARTITIONED BY (...)
      - config set   + table partitioned   → compare spec to config:
          - matches → silent no-op
          - differs → log WARNING (we don't reconcile; operator decides)

    Concurrency model: N viaduck pods may cold-connect to the same
    destination simultaneously and all reach this function. We use an
    optimistic catch-verify pattern rather than an advisory lock:

      1. If the table is unpartitioned, attempt the ALTER.
      2. On any exception from `commit()`, refresh the table state and
         check whether a peer pod has applied the spec we wanted. If
         yes, treat as success (we lost the race, not a failure). If
         no, re-raise — the ALTER genuinely failed (column missing,
         RDS down, etc.).

    This works regardless of catalog backend (PG/sqlite/etc.) and doesn't
    require infrastructure beyond pyducklake's existing connection. The
    downside is we catch broadly because pyducklake doesn't currently
    surface a typed conflict exception (`CommitFailedError` is declared
    but not raised by `UpdateSpec.commit()` in the source).
    """
    if not dest_cfg.partition_by:
        metrics.partition_spec_total.labels(destination=destination_id, outcome="skipped_no_config").inc()
        return

    transforms = _get_transform_map()
    expected = tuple((column, transforms[tname]) for tname, column in dest_cfg.partition_by)

    # `spec` and `is_unpartitioned` are @property in pyducklake — NOT methods.
    # Calling them with () TypeErrors at runtime; MagicMock makes them callable
    # so unit tests can't catch this. The existing _get_source_schema has the
    # same landmine documented at the top of this file — keep an eye out.
    if not table.spec.is_unpartitioned:
        _verify_or_warn_partition_spec(table, expected, dest_cfg, destination_id)
        return

    # Populated-table safety gate. ALTER TABLE SET PARTITIONED BY on a
    # non-empty DuckLake table has not been verified at scale; the failure
    # mode we want to avoid is the mixed-layout state where existing files
    # have NULL partition_id and new writes have a valid partition_id, which
    # reproduces the megaduck compactor breakage from 2026-06-24..28
    # (line-546 hive-path assertion). Default-deny; operator must opt in
    # AFTER verifying behavior on a throwaway copy AND coordinating a
    # destination-compactor pause.
    if not dest_cfg.partition_by_allow_alter_populated and _table_has_data(table):
        log.error(
            "Refusing to ALTER destination %s: table has existing data and "
            "partition_by_allow_alter_populated is false. Leaving unpartitioned. "
            "Set the flag to true ONLY after verifying ALTER behavior on a copy "
            "and pausing the destination compactor.",
            destination_id,
        )
        metrics.partition_spec_total.labels(destination=destination_id, outcome="refused_populated").inc()
        return

    spec = table.update_spec()
    for column_name, transform in expected:
        spec.add_field(column_name, transform)
    try:
        spec.commit()
        log.info(
            "Applied partition spec to destination %s table: %s",
            destination_id,
            dest_cfg.partition_by,
        )
        metrics.partition_spec_total.labels(destination=destination_id, outcome="applied").inc()
        return
    except Exception as e:
        # Could be: peer pod won an ALTER race, OR a real failure (missing
        # column, RDS hiccup, etc.). Re-read to disambiguate.
        try:
            table.refresh()
        except Exception:
            # Refresh itself failed — definitely a real error path.
            log.error("ALTER on destination %s failed and refresh also failed: %s", destination_id, e)
            raise e from None
        if table.spec.is_unpartitioned:
            # No peer applied a spec; this was a genuine failure.
            log.error(
                "ALTER on destination %s failed and table is still unpartitioned: %s",
                destination_id,
                e,
            )
            raise
        log.info(
            "ALTER on destination %s lost a race to a peer pod; verifying applied spec matches config",
            destination_id,
        )
        metrics.partition_spec_total.labels(destination=destination_id, outcome="applied_by_peer").inc()
        _verify_or_warn_partition_spec(table, expected, dest_cfg, destination_id)


def _verify_or_warn_partition_spec(
    table: Table,
    expected: tuple[tuple[str, object], ...],
    dest_cfg: DestinationConfig,
    destination_id: str,
) -> None:
    """Compare the table's current partition spec against `expected`. Silent
    if they match, WARNING if they don't (we don't auto-reconcile spec drift;
    operator decides whether to ALTER manually).

    Called from two paths:
      1. The table was already partitioned at first observation
      2. We lost an ALTER race to a peer pod
    Both paths want the same "matches → ok, differs → operator-visible
    warning" outcome.
    """
    actual = tuple((f.source_column, f.transform) for f in table.spec.fields)
    if actual == expected:
        metrics.partition_spec_total.labels(destination=destination_id, outcome="skipped_matches").inc()
        return
    log.warning(
        "Destination %s has partition spec %s but config specifies %s; leaving in place. "
        "Use ALTER TABLE manually to change.",
        destination_id,
        actual,
        dest_cfg.partition_by,
    )
    metrics.partition_spec_total.labels(destination=destination_id, outcome="skipped_diverges").inc()
