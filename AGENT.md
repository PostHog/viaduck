# Viaduck — DuckLake to DuckLake CDC Replication

## Pre-push Checklist

**Never push broken code to a PR.** Every push triggers CI for the whole org and wastes reviewer time. The push-test-push-test cycle is unacceptable.

Before every commit and push, run:

```bash
just ci
```

This runs the full local CI pipeline (mirrors CI workflow + Semgrep workflow):
1. `lock-check` — verifies `uv.lock` is consistent with `pyproject.toml`
2. `fmt-check` — ruff format (excludes auto-generated `_version.py`)
3. `lint` — ruff check
4. `test` — unit tests (`tests/unit/`)
5. `test-integration` — integration tests (`tests/integration/`)
6. `docs-check` — validates README links and d2 diagram SVGs
7. `semgrep` — security scanning (requires `brew install semgrep`)
8. `build` — Docker build with `--no-cache`

All steps must pass. Do not push with any failures.

If making changes to CI workflows, dependency pins, or the Dockerfile, spin up a QE lead engineer review (via Agent tool) focused specifically on local/CI parity before pushing. Check for:
- Caching that could mask failures locally (Docker layer cache, uv venv cache)
- Command differences between `justfile` and `.github/workflows/ci.yaml`
- `--frozen` vs non-frozen dependency resolution
- Python/uv version drift between local, CI, and Dockerfile
- Test discovery differences (path-based vs marker-based)

Prefer fixup commits over amending and force-pushing.

## What This Is

A standalone Python app that replicates data from a source DuckLake table to N destination DuckLake tables using pyducklake's CDC (Change Data Capture) API. Supports INSERT, DELETE, and UPDATE replication. One poll thread reads and buffers; a flush worker pool writes destinations. No framework.

Routes rows by a configurable field (e.g. `company`) to per-destination tables. Designed for high fanout (measured flat at ~43 destinations/s through 1000 destinations).

## Architecture

```
Source DuckLake
  └── {source_table}         ← CDC source (table_changes / table_insertions)

Postgres (same DB as source ducklake metadata by default)
  └── viaduck.viaduck_state  ← persisted cursors (plain table in a dedicated schema, NOT ducklake)

Viaduck
  poll thread (poll cadence):
    1. current_snapshot() on source table
    2. Cluster destinations by in-memory read position → shared CDC unit
       reads (masked per destination), half-open ranges (position, current]
    3. If key_columns: table_changes() → Phase 1 → route → buffer
       Else: table_insertions() → route → buffer
    4. Evaluate flush triggers (interval/target/memory/sliced/shutdown), gated
       by the per-destination flush circuit breaker
  flush workers (delivery.workers threads, flush cadence):
    5. Phase 2 (conflict resolution) on the concatenated buffer
    6. Phase 3 (Winner(k) dedup, delete+upsert in one txn)
    7. advance_cursor() → Postgres upsert, monotonicity-guarded

Destination DuckLakes (N independent catalogs)
  └── {dest_table}           ← receives routed rows
```

Position model: `flushed` (persisted cursor) <= `position` (in-memory
bufferedThrough). Reads issue from `position`; a flush failure drops the
buffers and resets `position = flushed` (range re-read, at-least-once).
Read epochs make the slow CDC read atomic against concurrent failure
resets. See `viaduck/delivery.py` module docstring and `tla/Viaduck.tla`.

## CDC Algorithm: Four Assumptions

The 3-phase CDC algorithm is eventually consistent under these assumptions:

1. **Routing column immutability**: The routing field must not be updated on the
   source. CDC filter pushdown uses current destination routing values, so preimages
   with old routing values may be filtered out. Violations are detected (logged at
   ERROR, metricked via `cdc_routing_mutations_total`) but data integrity is not
   guaranteed.

2. **Rowid monotonicity**: DuckLake's internal `rowid` is assumed to be monotonically
   increasing and never reused. Conflict resolution (Phase 2) uses rowid to identify
   the same logical row across change types. **KNOWN OPEN ISSUE (2026-06-11)**:
   DuckLake empirically reuses a rowid when an upsert re-creates a previously
   deleted key — within one flush window the re-created row pairs with its
   predecessor's tombstone and is lost (the pre-tombstone cancel rule lost it
   identically). Candidate fix: snapshot-ordered latest-event-wins Phase 2
   (spec-first); upstream rowid-stability question filed with the ducklake team.
   Append-only mode is unaffected.

3. **Single-master destinations**: Each destination table must only be written to by
   viaduck from the configured source. Concurrent writes from other sources break
   at-least-once idempotency — a retried delete could remove a row inserted by
   another writer.

4. **Key uniqueness**: `key_columns` must be unique per row in the source (DuckLake
   has no unique constraints to enforce this). Violations mean delete-by-key
   over-deletes and duplicate-key upserts duplicate. Verified at seed time per
   partition (`main.py:_verify_seed_key_uniqueness`, fails the seed loudly);
   post-seed inserts are not re-verified. NOTE: tombstone deletes widen the
   blast radius of a violation — a duplicate live key means a tombstone
   over-deletes the surviving duplicate, where the old cancel rule was inert.

## CDC Algorithm: Three Phases

**Phase 1: Preimage Resolution** (before routing) — `_resolve_preimages()`
- Pair update pre/postimages by rowid
- Same routing value → drop preimage (upsert handles it)
- Different routing value → convert preimage to delete (cross-tenant migration)
- Orphaned preimages → convert to delete (defensive)
- Post-condition assertion: no preimages remain

**Phase 2: Conflict Resolution** (per-destination, at flush time) — `apply.py:_resolve_conflicts()`
- Runs on the concatenation of all buffered reads for the flush
- insert + delete for same rowid → drop the insert, KEEP the delete
  (tombstone: idempotent no-op normally, heals commit/cursor-gap phantom
  replays — deletes are never dropped)
- update_postimage + delete for same rowid → drop postimage, keep delete
- insert + update_postimage for same rowid → drop insert, keep postimage
- Post-condition assertion: no rowid in both insert and delete

**Phase 3: Apply** (per-destination, atomic) — `apply.py:_apply_changes()`
- Winner(k): per-key last-write-wins dedup of upsert candidates by
  (snapshot_id, rowid) — a buffered window can carry several upserts per key
- Within `catalog.begin_transaction()`: chunked deletes first, then upsert
- Crash mid-apply → transaction rolled back, no partial state

**Pipeline mode** (`routing.mode`): the operator picks the entire shape of
the pipeline at config time:

- `mode: append_only` — read source via `ducklake_table_insertions` (inserts
  only, no delete stream from compaction-induced file end_snapshot churn),
  skip Phase 1 and Phase 2 entirely, write each flush via `tbl.append(rows)`.
  Requires `key_columns: []` (the apply path doesn't use them). The
  posthog/team-2 events pipeline runs in this mode.
- `mode: full_cdc` — read source via `ducklake_table_changes` (inserts +
  deletes + update preimages/postimages), run Phase 1 preimage resolution
  and Phase 2 conflict resolution, apply via `tbl.upsert(rows,
  join_cols=key_columns)`. Requires `key_columns` non-empty.

Both validated in `RoutingConfig.__post_init__`; a misconfig fails at
startup with the operator-actionable error rather than silently selecting
the wrong path. This replaced an earlier "infer mode from
`len(key_columns) > 0`" derivation which was a silent-misconfig hazard
(an empty list flipped the entire pipeline shape with no operator-visible
signal — and an earlier attempt to optimize the `mode: full_cdc` apply
path via a per-destination `append_at_least_once` flag was redundant for
posthog, which had been on `append_only` the whole time).

CDC batches are processed as unordered sets. This is sound because each
flush covers the union of adjacent half-open snapshot ranges
`(flushed, position]`, flushes apply in ascending range order, and
cross-read conflicts resolve by rowid grouping at flush time exactly like
within-read conflicts.

CDC read ranges are EXCLUSIVE of the cursor snapshot (`after_snapshot` in
`source.py`): ducklake's `table_changes`/`table_insertions` are inclusive
on both bounds, and re-reading the cursor snapshot lets a re-read insert
cancel a genuine later delete in Phase 2 (permanent phantom — found by
soak-tested at the seed boundary, locked by integration tests).

## TLA+ Formal Verification

The CDC algorithm is formally specified in `tla/Viaduck.tla` and verified by
TLC. Run via `flox activate` then `just tlc`. The spec models source operations,
buffered CDC reads as a SEQUENCE of slice entries with coverage watermarks
(the slice-cursor rule; prefix flushes persist `cov(k)`), two-step flushes
(buffer swap → commit/fail), commit-coverage drops of stale replay entries
(`DropCoveredPrefix` — the pause/zombie/prefix-split phantom chain TLC found
in the pre-slice model), concurrent per-destination flush workers, seeding,
and commit/cursor-gap scenarios both with and without process death — ALL
checked unconditionally; the tombstone rule eliminates the everCrashed phantom class
conditioning entirely — checking 8 invariants (incl. EntryCoverageInvariant)
across 132,190,573 distinct states. Modify the spec when changing
the CDC algorithm or adding new failure modes — and when designing semantic
changes, extend the spec FIRST and let TLC pass judgment before implementing.
Always run `just tlc` after spec changes.

## Key Design Decisions

- **Config via YAML** with `_env` suffix convention for credential indirection
- **At-least-once semantics**: no cross-catalog transactions; destinations tolerate duplicates
- **Buffered delivery**: reads at poll cadence, writes at flush cadence (default 120s) — decouples lag visibility from write amplification; `workers: 1, flush_interval_seconds: 0` reproduces unbuffered behavior
- **Slow-consumer isolation**: a per-destination flush circuit breaker (`flush_circuit_failures`, default 3; exponential resubmit backoff capped at `flush_circuit_max_seconds`) pauses submissions for a repeatedly-failing destination instead of letting it burn a shared worker plus its per-cycle chunk quota forever; an overall flush deadline (`flush_deadline_seconds`, default 2× flush interval) bounds the retry loop's wall time. Read-side backpressure stays destination-local (per-destination buffer caps). The read loop is unit-based: row/byte/span-bounded reads planned against the catalog (`poll.read_unit_*`), parallel across position clusters (`poll.read_workers`), with per-row snapshot attribution enabling per-destination masking and slice-cursor flushes — the 2026-08 cursor-group scheduler (rotation, cycle budget, chunk caps, skip-scan) was deleted outright once the feed made reads cheap.
- **Per-destination bounded buffers (queue semantics)**: each destination's queue (buffered + in-flight) is capped by, in precedence order, its `DestinationConfig.buffer_max_bytes` override (static destinations; a fixed sizing contract exempt from membership recompute), else the global `buffer_max_bytes_per_destination`, else the fair share `buffer_total_max_bytes / N`; an at-cap destination is excluded from CDC read filters and its position frozen — one slow destination backpressures only itself, like a per-partition Kafka consumer. Cap hit also force-flushes (trigger=`memory`). Modeled as BufferCap in the TLA+ spec.
- **State on plain Postgres**: cursor advances must not create catalog snapshots (the snapshot treadmill); lives in a dedicated `viaduck` schema so it never pollutes the ducklake catalog's namespace; upserts carry a monotonicity guard
- **LRU connection pool with lease pinning**: bounds memory at high fanout (default 100 open connections); eviction never closes a connection mid-transaction
- **Per-destination error isolation**: one broken destination doesn't block others; a failed flush drops only that destination's buffers
- **Clustered unit reads over the direct-SQL feed**: destinations at (or near) the same read position share one catalog-planned unit read (`poll.read_unit_max_rows`/`_max_span`/`_max_bytes`), masked per destination; clusters read in parallel on the read pool. The feed (viaduck/feed.py — unconditional for append_only) reads the catalog with indexed psycopg SQL + stock parquet scans instead of the extension's changefeed (~2.4s/bind fixed cost → ~ms/unit).
- **Two-layer write retry**: DuckLake's internal retry (`ducklake_max_retry_count=20`, FLAT 25-50ms backoff — `ducklake_retry_backoff` MUST stay 1.0: DuckLake's sleep is `wait_ms × random × backoff^attempt` with the attempt index as an uncapped exponent, so any backoff >1 compounds over 20 attempts into minutes-to-hours sleeps) absorbs snapshot-id commit collisions with catalog-SQL-only retries; viaduck's outer loop (15 attempts, exp backoff capped 30s, ±50% jitter) handles real failures. OCC conflicts do NOT evict the pooled connection (eviction per retry leaked ~160MB native each). `SchemaProjectionError`/`RoutingError` are permanent and fail fast.
- **Schema projection** (`schema_projection.py`): per-destination drops/reorder/safe-casts onto the destination's existing schema; build-time guards refuse key/routing-column mutation and NOT-NULL-violating casts; per-value cast fallback nulls unparseable values and alarms via `projection_cast_null_fallback_total`
- **Scan-based seeding with REPLACE semantics**: new destinations bulk-load from a filtered source scan; a cursor-0 destination with leftover rows (crashed prior seed) is truncated first (`routing.seed_truncate`, default true). Configurable via `seed_mode` (default: `scan`)
- **Worker threads are a concurrency knob, not a CPU multiplier**: Arrow's compute pool and DuckDB's threads are process-global underneath every flush worker — see README "Worker-thread sizing"
- **Destination lifecycle state machine** (`lifecycle.py` + `<state_table>_lifecycle` table — name derives from the cursor table so pipelines sharing a PG never share intent): per-DESTINATION operator intent — `active | paused | draining | retired`; absent row = active (no backfill), unknown value = paused + one ERROR per transition (not per cycle). Paused/retired = controlled crash: buffer discarded via the FlushFail machinery (`delivery.discard_buffer` rewinds position to the durable cursor + bumps the epoch; counted on `lifecycle_discarded_rows_total`), connection evicted ONLY once `delivery.is_clean` (an in-flight flush's retry loop re-creates the pool entry, so a transition-time evict latch leaks the connection for the stint), cursor is the resume point. A flush that COMPLETES after a discard restores `position >= flushed` in its success path (epoch-bumped) — without it, resume re-reads a committed range (deterministic duplicates in append_only). Draining = no new reads, flush out, evict when clean; the drain-complete log distinguishes "flushed out" from "ended via a flush-failure rewind" (the rewound range was NOT delivered; draining excludes re-reads, so retiring on the latter abandons it). RETIRED IS NEVER WRITTEN BY CODE (`StateManager.set_lifecycle_state` refuses) and viaduck SEVERS the cursor rows (all instances, idempotent per cycle + startup backstop against the in-flight-upsert resurrect race) — re-add = new tenant = fresh seed per `seed_mode`, deterministic regardless of partition drift. Seeding is lifecycle-gated: only ACTIVE destinations seed; a skipped cursor-0 destination stays read-gated after resume until a restart seeds it (ERROR per cycle). States re-read every poll cycle (live pause without restart); a state-store blip keeps last-known states (the lifecycle table shares the cursor store's PG — fail-to-paused would turn any PG blip into a fleet-wide self-inflicted mass discard). `/lifecycle` (read-only: state + reason/updated_by/updated_at + staleness age) + one-hot `viaduck_destination_lifecycle_state{destination,state}`; `/status` destination status short-circuits to the lifecycle state — join lag alerts on `state!="active"`. TLA: the spec HAS a `PauseDest` action (buffer discard + position rewind, in-flight flush PRESERVED — unlike ProcessCrash/FlushFail) and FlushCommit carries the implementation's success-path position restore; removing the restore lets TLC produce a 6-step BufferPositionBound counterexample (SrcInsert → BufferRead → FlushStart → PauseDest → FlushCommit), which is the formal witness for the pause-races-in-flight-flush duplicate-delivery bug. Paused DURATION needs no modeling (an action not firing is a pause; resume is BufferRead from the rewound position). The lifecycle table's DDL CHECK freezes the state vocabulary: adding a state later requires a migration on existing tables, not just a VALID_STATES change.
- **CP-driven discovery** (`discovery.py` + `k8s_secrets.py`): additive, static-wins, fixed-set. Mapping is PURE (payload → MappedDestination; per-entry failures skip-and-count on `discovery_broken_entries_total{reason}`, never raise); credential resolution happens at materialization via a stdlib-only in-cluster Secret GET (SA token + cluster CA; 403 message names the RBAC fix; error text never contains secret material). Table = `events_table` VERBATIM (CP owns naming; renames not allowed). Team `enabled` is IGNORED (query-serving switch; row presence is the only ingestion signal). Discovered destinations get `postgres_uri_direct` (bypasses the env indirection — it IS the credential: never log it) and initialize at the source head regardless of pipeline seed_mode (C5: discovery starts the stream, never backfills; cursor-at-head also keeps them out of the scan-seed pass by construction). Non-writable warehouses are skipped (reshard fence; C3 maps this to lifecycle instead). Startup is fail-open (CP down → static-only + `discovery_synced` 0 — static tenants are never hostage to CP availability). The DriftWatcher only DETECTS divergence vs the startup set (restart applies; the runtime-mutable set is C3). `destination._create` ensures the table's namespace (`create_namespace_if_not_exists`) before `create_table_if_not_exists` — discovered per-team schemas don't pre-exist the way `posthog` does.

## Module Layout

| Module | Responsibility |
|--------|---------------|
| `main.py` | Entry point, poll loop, Phase 1 preimage resolution, seeding, signal handling |
| `delivery.py` | DeliveryManager: per-destination buffers, flush triggers, worker pool, position model |
| `apply.py` | Phase 2 conflict resolution, Phase 3 delete/upsert + Winner(k), write retry |
| `config.py` | YAML parsing, env var resolution, frozen dataclass |
| `source.py` | Source catalog connection + DuckLake connection defaults (internal retry, cache off), CDC reading (table_changes / table_insertions, exclusive start) |
| `router.py` | Arrow splitting by routing field |
| `schema_projection.py` | Per-destination projection plans: drops, ordering, safe casts, build-time guards, per-value fallback |
| `destination.py` | LRU connection pool for destination catalogs, lease pinning |
| `state.py` | Per-destination cursors on plain Postgres (psycopg) |
| `arrowutil.py` | Shared Arrow kernel helpers (row_indices, full_bool) |
| `metrics.py` | Prometheus metric definitions (57 metrics) |
| `server.py` | HTTP /metrics, /healthz, /readyz, /status, /ui, /ui/sse |
| `logging_config.py` | Structured logging setup |

## Testing

- Unit tests: `tests/unit/` — mocked pyducklake, fast (COUNTS-REFRESH)
- Integration tests: `tests/integration/` — real pyducklake with local DuckDB; Postgres-backed state tests via testcontainers (COUNTS-REFRESH)
- Performance tests: `tests/perf/` — router, phases, delete filter, end-to-end delivery fanout at 200/500/1000 destinations (11 benchmarks)
- Soak: manual docker-compose kill sequence (SIGKILL + SIGTERM + convergence diff) — run for delivery-semantics changes
- Flush-sizing load harness: `just load-up` + `just load-check` (docker-compose.load.yaml: 3K rows/s Zipf-skewed wide-row loadgen with a mid-run thin→hot burst stream, dest-1 catalog Postgres behind toxiproxy so `test/loadcheck.py` can inject a catalog-latency wave; 6 destinations on dest-2 are the uncontended control group). Asserts the adaptive flush controller's behavior locally — head flushes target-triggered at full-size batches (absolute rows/flush band: the crumb-cut detector), contended destinations halve, control doesn't move, the burst tenant migrates interval→target with bounded lag, zero errors, drains clean — instead of learning it from a prod deploy. Reads both target-gauge generations (`_rows` preferred, `_bytes` fallback).

Run all: `just ci` (lock-check + format + lint + unit + integration + docs-check + Docker build). Perf: `just test-perf`.
Perf with JSON output: `just test-perf-json` → writes `perf-results.json`.

## Grafana

Dashboard at `grafana/dashboards/viaduck.json`. Available at `http://localhost:3000/d/viaduck/viaduck` when running `just up`.
