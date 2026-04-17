# Viaduck — DuckLake to DuckLake CDC Replication

## Pre-push Checklist

Always run before pushing:

```bash
just lint       # ruff check
just fmt-check  # ruff format --check
just test       # unit tests
```

All three must pass. Do not push with lint or test failures.

Prefer fixup commits over amending and force-pushing.

## What This Is

A standalone Python app that replicates data from a source DuckLake table to N destination DuckLake tables using pyducklake's CDC (Change Data Capture) API. Supports INSERT, DELETE, and UPDATE replication. Single thread, single poll loop, no framework.

Routes rows by a configurable field (e.g. `company`) to per-destination tables. Designed for high fanout (100s-1000s of destinations).

## Architecture

```
Source DuckLake
  ├── {source_table}         ← CDC source (table_changes / table_insertions)
  └── _viaduck_state         ← per-destination replication cursors

Viaduck (single-threaded poll loop)
  1. current_snapshot() on source table
  2. Group destinations by last_snapshot_id → grouped CDC reads
  3. If key_columns: table_changes() → Phase 1-2-3 apply
     Else: table_insertions() → append()
  4. Update _viaduck_state on source

Destination DuckLakes (N independent catalogs)
  └── {dest_table}           ← receives routed rows
```

## CDC Algorithm: Three Assumptions

The 3-phase CDC algorithm is eventually consistent under these assumptions:

1. **Routing column immutability**: The routing field must not be updated on the
   source. CDC filter pushdown uses current destination routing values, so preimages
   with old routing values may be filtered out. Violations are detected (logged at
   ERROR, metricked via `cdc_routing_mutations_total`) but data integrity is not
   guaranteed.

2. **Rowid monotonicity**: DuckLake's internal `rowid` is assumed to be monotonically
   increasing and never reused. Conflict resolution (Phase 2) uses rowid to identify
   the same logical row across change types. If rowids were recycled, unrelated rows
   could be incorrectly cancelled.

3. **Single-master destinations**: Each destination table must only be written to by
   viaduck from the configured source. Concurrent writes from other sources break
   at-least-once idempotency — a retried delete could remove a row inserted by
   another writer.

## CDC Algorithm: Three Phases

**Phase 1: Preimage Resolution** (before routing) — `_resolve_preimages()`
- Pair update pre/postimages by rowid
- Same routing value → drop preimage (upsert handles it)
- Different routing value → convert preimage to delete (cross-tenant migration)
- Orphaned preimages → convert to delete (defensive)
- Post-condition assertion: no preimages remain

**Phase 2: Conflict Resolution** (per-destination, after routing) — `_resolve_conflicts()`
- insert + delete for same rowid → cancel both (net no-op)
- update_postimage + delete for same rowid → drop postimage, keep delete
- Post-condition assertion: no rowid in both insert and delete

**Phase 3: Apply** (per-destination, atomic) — `_apply_changes()`
- Within `catalog.begin_transaction()`: delete first, then upsert
- Crash mid-apply → transaction rolled back, no partial state

CDC batches are processed as unordered sets. This is sound because each batch
covers a closed snapshot range, batches are applied in ascending snapshot order,
and within-batch conflicts are resolved by rowid grouping.

## Key Design Decisions

- **Config via YAML** with `_env` suffix convention for credential indirection
- **At-least-once semantics**: no cross-catalog transactions; destinations tolerate duplicates
- **State on source DuckLake**: `_viaduck_state` table tracks per-destination cursors
- **LRU connection pool**: bounds memory at high fanout (default 50 open connections)
- **Per-destination error isolation**: one broken destination doesn't block others
- **Grouped CDC reads**: destinations at the same cursor share a single CDC call

## Module Layout

| Module | Responsibility |
|--------|---------------|
| `main.py` | Entry point, poll loop, 3-phase CDC algorithm, signal handling |
| `config.py` | YAML parsing, env var resolution, frozen dataclass |
| `source.py` | Source catalog connection, CDC reading (table_changes / table_insertions) |
| `router.py` | Arrow splitting by routing field |
| `destination.py` | LRU connection pool for destination catalogs |
| `state.py` | `_viaduck_state` table CRUD |
| `metrics.py` | Prometheus metric definitions (19 metrics) |
| `server.py` | HTTP /metrics, /healthz, /readyz |
| `logging_config.py` | Structured logging setup |

## Testing

- Unit tests: `tests/unit/` — mocked pyducklake, fast (187 tests)
- Integration tests: `tests/integration/` — real pyducklake with local DuckDB (planned)
- Performance tests: `tests/perf/` — high-fanout benchmarks (planned)
- E2E tests: `tests/e2e/` — full docker-compose stack (planned)
