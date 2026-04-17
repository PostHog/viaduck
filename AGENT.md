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

A standalone Python app that replicates data from a source DuckLake table to N destination DuckLake tables using pyducklake's CDC (Change Data Capture) API. Single thread, single poll loop, no framework.

Routes rows by a configurable field (e.g. `team_id`) to per-destination tables. Designed for high fanout (100s-1000s of destinations).

## Architecture

```
Source DuckLake
  ├── {source_table}         ← CDC source (table_insertions API)
  └── _viaduck_state         ← per-destination replication cursors

Viaduck (single-threaded poll loop)
  1. current_snapshot() on source table
  2. Group destinations by last_snapshot_id → grouped CDC reads
  3. table_insertions(start, end, filter_expr) with pushdown
  4. Arrow split by routing field (pyarrow.compute)
  5. append(batch) to each destination
  6. Update _viaduck_state on source

Destination DuckLakes (N independent catalogs)
  └── {dest_table}           ← receives routed rows
```

## Key Design Decisions

- **Config via YAML** with `_env` suffix convention for credential indirection
- **At-least-once semantics**: no cross-catalog transactions; destinations tolerate duplicates
- **State on source DuckLake**: `_viaduck_state` table tracks per-destination cursors
- **LRU connection pool**: bounds memory at high fanout (default 50 open connections)
- **Per-destination error isolation**: one broken destination doesn't block others
- **Grouped CDC reads**: destinations at the same cursor share a single `table_insertions` call

## Module Layout

| Module | Responsibility |
|--------|---------------|
| `main.py` | Entry point, poll loop, signal handling |
| `config.py` | YAML parsing, env var resolution, frozen dataclass |
| `source.py` | Source catalog connection, CDC reading |
| `router.py` | Arrow splitting by routing field |
| `destination.py` | LRU connection pool for destination catalogs |
| `state.py` | `_viaduck_state` table CRUD |
| `metrics.py` | Prometheus metric definitions |
| `server.py` | HTTP /metrics, /healthz, /readyz |
| `web.py` | SSE status page (optional) |
| `logging_config.py` | Structured logging setup |

## Testing

- Unit tests: `tests/unit/` — mocked pyducklake, fast
- Integration tests: `tests/integration/` — real pyducklake with local DuckDB
- Performance tests: `tests/perf/` — high-fanout benchmarks
- E2E tests: `tests/e2e/` — full docker-compose stack
