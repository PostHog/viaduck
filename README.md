# Viaduck — DuckLake to DuckLake CDC Replication

A standalone Python app that replicates data from a source DuckLake table to N destination DuckLake tables using CDC (Change Data Capture). Single thread, single poll loop, no framework.

## Naming

> **viaduct** (noun): a long bridge-like structure carrying a road or railroad across a valley or other low ground.

Viaduck carries data across DuckLakes. The name is a portmanteau of *viaduct* and *duck*, because [DuckLake](https://github.com/duckdb/ducklake).

## Why

Multi-tenant DuckLake architectures need per-tenant table isolation — separate catalogs, separate S3 paths, separate Postgres metadata stores. Viaduck reads CDC insertions from a shared source table, routes rows by a configurable field (e.g. `company`), and writes each partition to the correct tenant's DuckLake. Think of it as a data viaduct with N exits.

```
loop:
  current_snapshot() on source
  group destinations by cursor position
  for each group:
    table_insertions(start, end, filter_expr)  → CDC read
    split by routing field                      → Arrow routing
    append() to each destination                → write
    advance_cursor() in transaction             → state update
```

Single thread, single loop. DuckLake snapshots are the cursor. State is tracked on the source catalog. Designed for high fanout (100s-1000s of destinations).

## Architecture

![Architecture](docs/architecture.svg)

Source: [`docs/architecture.d2`](docs/architecture.d2)

Each viaduck instance consumes from one source table and writes to N destination DuckLakes. The source and every destination are independent catalogs with their own Postgres metadata store and S3 data path. No cross-catalog transactions are possible.

Core modules:

| Module | Responsibility | Source |
|--------|---------------|--------|
| [`main.py`](viaduck/main.py) | Poll loop, signal handling, retry logic | Entry point |
| [`config.py`](viaduck/config.py) | YAML parsing, env var resolution, validation | Configuration |
| [`source.py`](viaduck/source.py) | Source catalog connection, CDC reading | CDC |
| [`router.py`](viaduck/router.py) | Arrow splitting by routing field | Routing |
| [`destination.py`](viaduck/destination.py) | LRU connection pool for destination catalogs | Connections |
| [`state.py`](viaduck/state.py) | `_viaduck_state` table CRUD on source | State tracking |
| [`metrics.py`](viaduck/metrics.py) | Prometheus metric definitions | Observability |
| [`server.py`](viaduck/server.py) | HTTP `/metrics`, `/healthz`, `/readyz` | Health checks |

## Poll Cycle

![Poll Cycle](docs/poll-cycle.svg)

Source: [`docs/poll-cycle.d2`](docs/poll-cycle.d2)

Each poll cycle ([`main.py:_poll_cycle`](viaduck/main.py)):

1. **Snapshot check** — `current_snapshot()` on the source table. If no snapshots exist, sleep and retry.
2. **Load cursors** — read `_viaduck_state` to get each destination's `last_snapshot_id`. Uses filter pushdown with `In` + `EqualTo` expressions ([`state.py:load_cursors`](viaduck/state.py)).
3. **Group by cursor** — destinations at the same snapshot share a single CDC read ([`main.py:_group_by_cursor`](viaduck/main.py)). This avoids re-reading data for caught-up destinations.
4. **CDC read** — `table_insertions(start, end, filter_expr)` with a SQL `IN` clause pushed down to DuckLake for server-side filtering ([`source.py:read_cdc`](viaduck/source.py)).
5. **Route** — `split_and_count()` uses PyArrow compute to partition the Arrow table by routing field value in a single pass. Also counts unrouted rows ([`router.py:split_and_count`](viaduck/router.py)).
6. **Write** — `append()` each filtered batch to the corresponding destination. Retries 3x with exponential backoff ([`main.py:_write_with_retry`](viaduck/main.py)).
7. **Advance cursor** — `delete` + `insert` within a pyducklake `begin_transaction()` on the source catalog. Atomic — a crash during this operation triggers a rollback, preserving the previous cursor ([`state.py:advance_cursor`](viaduck/state.py)).
8. **Lag metrics** — update per-destination snapshot lag gauges.

## CDC Operations and Semantics

### What CDC events are supported

Viaduck uses pyducklake's `table_insertions()` API, which captures **append-only** CDC. This covers:

| Operation | Supported | Notes |
|-----------|-----------|-------|
| INSERT / append | Yes | Core use case. Rows inserted between snapshots are read and routed. |
| DELETE | **No** | `table_deletions()` exists in pyducklake but viaduck does not consume it. Deletes on the source are not propagated. |
| UPDATE | **No** | DuckLake models updates as delete + insert. Viaduck would need to consume both `table_deletions()` and `table_insertions()` to replicate updates, which it does not yet do. |
| UPSERT / MERGE | **No** | Same limitation as UPDATE. |
| Schema evolution | **Partial** | New columns on the source are included in CDC reads. However, destination table schemas are created once from the source schema at connection time and **not** automatically evolved. A viaduck restart picks up the new schema for newly created destination tables. |

### CDC permutations handled

| Scenario | Behavior | Tested |
|----------|----------|--------|
| No snapshots on source | Poll returns early, sleeps | [`test_poll_cycle_no_snapshots`](tests/unit/test_main.py) |
| All destinations caught up | No CDC reads, no writes | [`test_poll_cycle_all_caught_up`](tests/unit/test_main.py) |
| Empty changeset (snapshot advanced, no matching rows) | Cursors advanced without writing | [`test_poll_cycle_empty_changeset_advances_cursors`](tests/unit/test_main.py) |
| Destinations at different snapshots | Grouped CDC reads — one per distinct cursor position | [`test_group_by_cursor_mixed`](tests/unit/test_main.py) |
| New destination (no prior state) | Initialized at snapshot 0, replays full history | [`test_initialize_destinations_creates_new`](tests/unit/test_state.py), [`test_poll_cycle_snapshot_at_zero`](tests/unit/test_main.py) |
| Rows with no matching destination | Counted as unrouted, metricked via `viaduck_unrouted_rows_total`, silently dropped | [`test_split_string_no_match`](tests/unit/test_router.py) |
| NULL values in routing column | Not routed to any destination, counted as unrouted | [`test_split_null_values_not_routed`](tests/unit/test_router.py), [`test_split_all_null_routing_column`](tests/unit/test_router.py) |
| Routing field missing from source schema | `RoutingError` raised, group processing halted, error metricked | [`test_split_missing_field_raises_routing_error`](tests/unit/test_router.py), [`test_poll_cycle_routing_error_breaks_gracefully`](tests/unit/test_main.py) |
| Routing value type mismatch (e.g. non-numeric value for integer column) | `RoutingError` raised with descriptive message | [`test_split_invalid_integer_routing_value`](tests/unit/test_router.py), [`test_split_invalid_float_routing_value`](tests/unit/test_router.py) |

### Delivery guarantees

**At-least-once.** There is no cross-catalog transaction support in DuckLake. The write-then-advance-cursor pattern means:

- If the process crashes **after writing** to a destination but **before advancing the cursor**, the next poll will re-read the same CDC range and re-write the same rows. Destinations must tolerate duplicates.
- If the process crashes **during the cursor transaction**, the transaction is rolled back and the cursor stays at its previous value. The same data is re-read and re-written. No data loss.
- If a destination write fails, the cursor is not advanced for that destination. Other destinations are unaffected. The failed destination retries automatically on the next poll.

There is **no data loss path**. The source DuckLake's snapshot history is the durable log.

## Failure Modes

![Failure Modes](docs/failure-modes.svg)

Source: [`docs/failure-modes.d2`](docs/failure-modes.d2)

| Failure | Impact | Recovery | Isolation |
|---------|--------|----------|-----------|
| Source catalog unavailable | CDC read fails | Fatal — crash, K8s restart, resume from last cursor | All destinations blocked |
| Destination catalog unavailable | `append()` fails | 3 retries with backoff ([`main.py:_write_with_retry`](viaduck/main.py)), then error recorded, connection evicted. Cursor not advanced — automatic retry next poll. | **Per-destination** — other destinations unaffected ([`test_poll_cycle_handles_write_failure`](tests/unit/test_main.py)) |
| Crash after write, before state update | Destination has data but cursor not advanced | At-least-once: re-reads and re-writes on restart. Duplicates possible. | Per-destination |
| State transaction failure | `delete` + `insert` rolled back by pyducklake ([`transaction.py`](https://github.com/posthog/pyducklake)) | Cursor preserved at old value. Same data retried next poll. | Per-destination |
| Routing field missing from source | `RoutingError` halts group processing | Error metricked (`viaduck_errors_total{type="routing"}`), logged. Requires config or schema fix. | All destinations in group |
| Connection pool eviction storm | Frequent close/reopen of catalogs | Automatic via LRU. Increase `max_open` if thrashing. Metricked via `viaduck_pool_evictions_total`. | Performance, not correctness |

## Not Yet Supported

- **DELETE / UPDATE replication** — source deletes and updates are not propagated to destinations. Only inserts.
- **Dynamic destination discovery** — destinations are defined statically in YAML. No runtime discovery from a DuckLake table.
- **Schema evolution propagation** — source schema changes are not automatically applied to existing destination tables. Requires viaduck restart.
- **Exactly-once delivery** — at-least-once only. No deduplication layer.
- **Batching / coalescing** — writes are immediate per poll cycle. No local buffering across cycles to reduce small file count.
- **Web UI** — planned SSE-based status page, not yet implemented.
- **Grafana dashboard** — not yet created.
- **E2E / integration tests** — unit tests only. Docker-compose stack and integration tests are planned.

## Configuration

Config via YAML file (default: `viaduck.yaml`). Credentials are never in the YAML — use `_env` suffix to reference environment variables.

```yaml
source:
  name: "source"
  postgres_uri_env: "SOURCE_POSTGRES_URI"   # env var containing the connection string
  data_path: "s3://source-bucket/data"
  table: "events"
  properties:
    s3_endpoint: "minio:9000"
    s3_access_key_id_env: "S3_ACCESS_KEY_ID"  # _env suffix → read from env var
    s3_secret_access_key_env: "S3_SECRET_ACCESS_KEY"
    s3_use_ssl: "false"
    s3_url_style: "path"

routing:
  field: "company"                           # column in source table to route on

destinations:
  - id: "quacksworth-lake"                   # internal identifier (state tracking, logs)
    routing_value: "quacksworth"             # rows where company='quacksworth' go here
    name: "quacksworth_catalog"
    postgres_uri_env: "DEST_QUACKSWORTH_POSTGRES_URI"
    data_path: "s3://quacksworth-data/"
    table: "events"                          # defaults to source table name if omitted

  - id: "mallardine-lake"
    routing_value: "mallardine"
    name: "mallardine_catalog"
    postgres_uri_env: "DEST_MALLARDINE_POSTGRES_URI"
    data_path: "s3://mallardine-data/"

defaults:
  properties:                                # inherited by all destinations
    s3_endpoint: "minio:9000"
    s3_use_ssl: "false"
    s3_url_style: "path"

poll:
  interval_seconds: 5                       # how often to poll for new snapshots

server:
  port: 8000                                # Prometheus metrics + health checks

instance:
  id: "viaduck-0"                           # unique per scaling instance
  partition:
    mode: "all"                             # all | explicit | hash
    # explicit: include: ["quacksworth-lake", "mallardine-lake"]
    # hash: total: 4, ordinal: 0
```

Config parsing and validation: [`config.py`](viaduck/config.py), tests: [`test_config.py`](tests/unit/test_config.py).

Routing values can be strings or integers (YAML unquoted integers are coerced to strings). The router detects the source column's Arrow type at runtime and casts accordingly ([`router.py:_make_scalar`](viaduck/router.py)).

## State Tracking

Viaduck stores per-destination replication cursors in a `_viaduck_state` table on the **source** DuckLake catalog ([`state.py`](viaduck/state.py)):

| Column | Type | Purpose |
|--------|------|---------|
| `destination_id` | VARCHAR | Which destination (e.g. "quacksworth-lake") |
| `instance_id` | VARCHAR | Which viaduck instance owns this cursor |
| `last_snapshot_id` | BIGINT | Last successfully replicated source snapshot |
| `rows_replicated` | BIGINT | Cumulative rows written to this destination |
| `last_error` | VARCHAR | Last error message (NULL if healthy) |
| `updated_at` | TIMESTAMPTZ | When this row was last modified |

State updates use `begin_transaction()` for atomicity — the delete + insert pair either both commit or both roll back ([`state.py:advance_cursor`](viaduck/state.py), tested: [`test_advance_cursor_uses_transaction`](tests/unit/test_state.py)).

State is keyed by `(destination_id, instance_id)`, enabling multiple viaduck instances to independently track their assigned destinations without conflicts.

## Connection Management

With 1000s of destinations, holding all connections open is not viable (~20-30MB per DuckDB catalog connection). Viaduck uses an LRU pool ([`destination.py:DestinationPool`](viaduck/destination.py)):

- **Lazy initialization** — connections created on first access
- **LRU eviction** — when at capacity (`max_open`, default 50), the least recently used connection is closed
- **Error eviction** — failed connections are evicted immediately and recreated on next access
- **Schema caching** — source table schema fetched once at startup, reused for all destination table creation

Metrics: `viaduck_pool_open_connections`, `viaduck_pool_evictions_total`, `viaduck_pool_creates_total`.

Tests: [`test_destination.py`](tests/unit/test_destination.py) — LRU ordering, eviction, error handling, `max_open` validation.

## Horizontal Scaling

Three partition modes ([`config.py:assigned_destination_ids`](viaduck/config.py)):

| Mode | How it works | Use case |
|------|-------------|----------|
| `all` (default) | Single instance handles all destinations | Small deployments |
| `explicit` | YAML lists destination IDs for this instance | Operator-controlled assignment |
| `hash` | `md5(destination_id) % total == ordinal` | Automatic, no coordination |

Each instance only processes its assigned destinations. State rows are keyed by `instance_id`, so instances don't conflict.

## Metrics

12 Prometheus metrics exposed on `GET /metrics` (port 8000). Pipeline label auto-injected ([`metrics.py`](viaduck/metrics.py)):

| Metric | Type | Labels |
|--------|------|--------|
| `viaduck_polls_total` | Counter | — |
| `viaduck_cdc_read_seconds` | Histogram | — |
| `viaduck_cdc_rows_read_total` | Counter | — |
| `viaduck_source_snapshot_id` | Gauge | — |
| `viaduck_dest_write_seconds` | Histogram | destination |
| `viaduck_dest_rows_written_total` | Counter | destination |
| `viaduck_dest_last_snapshot_id` | Gauge | destination |
| `viaduck_dest_lag_snapshots` | Gauge | destination |
| `viaduck_unrouted_rows_total` | Counter | — |
| `viaduck_pool_open_connections` | Gauge | — |
| `viaduck_pool_evictions_total` | Counter | — |
| `viaduck_errors_total` | Counter | type, destination |

## Setup

```bash
just sync              # install dependencies
just run -- -c my.yaml # run with config file
```

## Development

```bash
just fmt               # format code
just lint              # lint code
just test              # run unit tests
just test-integration  # run integration tests (local DuckDB)
just test-e2e          # run E2E tests (docker-compose)
just ci                # format check + lint + unit tests
just up                # start docker-compose stack
just down              # stop docker-compose stack
```

## Deployment

```bash
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/pdb.yaml
kubectl apply -f k8s/deployment.yaml
```

Viaduck runs as a K8s Deployment (not StatefulSet — no ordinal-based identity needed). For horizontal scaling, deploy multiple instances with different `instance.partition` configs.

## Error Handling and Retries

| Operation | Attempts | Backoff | On exhaustion |
|-----------|----------|---------|---------------|
| Destination write | 3 | 1s, 2s | Error recorded, connection evicted, cursor not advanced. Automatic retry next poll. Other destinations unaffected. |
| Source CDC read | 1 | — | Fatal — crash, K8s restart, resume from cursors. |
| State update | 1 (transactional) | — | Transaction rolled back, cursor preserved. Retry next poll. |

Why crash on source failure? The source catalog is the single point of truth. If it's unreachable, there's nothing to do. Let K8s handle the restart. DuckLake snapshot history ensures no data loss.

---

MIT License. Copyright (c) 2026 PostHog, Inc.
