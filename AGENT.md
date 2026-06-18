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
    2. Group destinations by in-memory read position → grouped CDC reads,
       half-open ranges (position, current]
    3. If key_columns: table_changes() → Phase 1 → route → buffer
       Else: table_insertions() → route → buffer
    4. Evaluate flush triggers (interval/rows/bytes/memory/shutdown)
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

**Phase 3 fast path: `append_at_least_once`** (per-destination opt-in)

`DestinationConfig.append_at_least_once: bool` (default `false`). When `true`
AND `_is_pure_insert_batch(batch)` (every row's `change_type == "insert"`),
`_apply_changes` calls `tbl.append(rows)` instead of `tbl.upsert(rows, join_cols=key_columns)`.

Motivation: `pyducklake.Table.upsert()` runs `self.scan().count()` twice (before
and after the MERGE, just to populate `UpsertResult.rows_updated/rows_inserted`)
and the MERGE itself joins source keys against the target table — both scale
with destination size, and both are wasted work when the batch contains no
updates. For an insert-only events workload with UUID keys (no min/max stat
pruning helps), the join reads the whole target every flush to confirm zero
matches. `tbl.append()` skips all of that — pure write.

Contract change to flag carefully. The upsert path doesn't just "make apply
idempotent on retry" — it also silently collapses **upstream at-least-once
duplicates** (a CDC redelivery of the same source snapshot range arrives as
the same input batch; MERGE WHEN MATCHED reduces it to one destination row).
The fast path does neither. Both apply-retry duplicates AND upstream CDC
duplicates now physically materialize in the destination table and propagate
to every downstream consumer (queries, exports, lakehouse aggregations) —
they no longer stop at viaduck. The end-to-end "at-least-once" contract
remains identical from upstream → viaduck → downstream, but the previously-
hidden deduplication side-effect of upsert is gone. Enable only when **every
consumer of the destination table** can tolerate per-key duplicates, not
just the immediate consumer of viaduck.

Safety net: the check is per-batch, not config-only. A non-insert row anywhere
in the batch (a delete, an `update_postimage`) falls back to the upsert path
transparently, so a future schema/CDC change that introduces updates doesn't
silently corrupt the destination. `_dedupe_upserts_last_write_wins` still
runs on the fast path so within-batch duplicate keys collapse to one row
(the "duplicates only on retry" contract isn't weakened by the fast path
itself — only by retries).

Metric implication: `viaduck_dest_upsert_matched_total` (`metrics.dest_upsert_matched_total`)
stays silent on the fast path — `tbl.append()` has no "matched" concept, and
at-least-once semantics make the question ill-defined. A destination running
in fast-path mode that has zero scrapes for this counter is consistent with
the configuration, not a bug.

TLA+ spec coverage: `tla/Viaduck.tla` models destination contents as sets,
not bags — physical row duplicates introduced by the fast path are not
observable in the spec's safety invariants (`EventualConsistency` uses set
equality; `NoPhantomWhenCurrent` only requires every dest row to trace back
to a source row, which duplicates still do). The current spec therefore
neither breaks under the fast path nor verifies it; coverage is incidental.
A future spec update would need bag/multiset semantics to model the
duplicate-count semantic difference and re-run TLC. Note that moving cursor
advance into the same transaction as the apply (the cleanest way to restore
exactly-once and re-tighten the spec) is not achievable in the current
architecture: cursor lives in source-side Postgres, apply lives in the
destination DuckLake catalog, and there is no two-phase commit between them.

CDC batches are processed as unordered sets. This is sound because each
flush covers the union of adjacent half-open snapshot ranges
`(flushed, position]`, flushes apply in ascending range order, and
cross-read conflicts resolve by rowid grouping at flush time exactly like
within-read conflicts.

CDC read ranges are EXCLUSIVE of the cursor snapshot (`after_snapshot` in
`source.py`): ducklake's `table_changes`/`table_insertions` are inclusive
on both bounds, and re-reading the cursor snapshot lets a re-read insert
cancel a genuine later delete in Phase 2 (permanent phantom — found by
the M3 soak at the seed boundary, locked by integration tests).

## TLA+ Formal Verification

The CDC algorithm is formally specified in `tla/Viaduck.tla` and verified by
TLC. Run via `flox activate` then `just tlc`. The spec models source operations,
buffered CDC reads, two-step flushes (buffer swap → commit/fail), concurrent
per-destination flush workers, seeding, and commit/cursor-gap scenarios both
with and without process death — ALL checked unconditionally; the
tombstone rule retired the everCrashed phantom conditioning entirely —
checking 7 invariants across 22.6M distinct states (~3 min). Modify the spec when changing
the CDC algorithm or adding new failure modes — and when designing semantic
changes, extend the spec FIRST and let TLC pass judgment before implementing.
Always run `just tlc` after spec changes.

## Key Design Decisions

- **Config via YAML** with `_env` suffix convention for credential indirection
- **At-least-once semantics**: no cross-catalog transactions; destinations tolerate duplicates
- **Buffered delivery**: reads at poll cadence, writes at flush cadence (default 120s) — decouples lag visibility from write amplification; `workers: 1, flush_interval_seconds: 0` reproduces unbuffered behavior
- **State on plain Postgres**: cursor advances must not create catalog snapshots (the snapshot treadmill); lives in a dedicated `viaduck` schema so it never pollutes the ducklake catalog's namespace; upserts carry a monotonicity guard
- **LRU connection pool with lease pinning**: bounds memory at high fanout (default 100 open connections); eviction never closes a connection mid-transaction
- **Per-destination error isolation**: one broken destination doesn't block others; a failed flush drops only that destination's buffers
- **Grouped CDC reads**: destinations at the same read position share a single CDC call
- **Scan-based seeding with REPLACE semantics**: new destinations bulk-load from a filtered source scan; a cursor-0 destination with leftover rows (crashed prior seed) is truncated first (`routing.seed_truncate`, default true). Configurable via `seed_mode` (default: `scan`)
- **Worker threads are a concurrency knob, not a CPU multiplier**: Arrow's compute pool and DuckDB's threads are process-global underneath every flush worker — see README "Worker-thread sizing"

## Module Layout

| Module | Responsibility |
|--------|---------------|
| `main.py` | Entry point, poll loop, Phase 1 preimage resolution, seeding, signal handling |
| `delivery.py` | DeliveryManager: per-destination buffers, flush triggers, worker pool, position model |
| `apply.py` | Phase 2 conflict resolution, Phase 3 delete/upsert + Winner(k), write retry |
| `config.py` | YAML parsing, env var resolution, frozen dataclass |
| `source.py` | Source catalog connection, CDC reading (table_changes / table_insertions, exclusive start) |
| `router.py` | Arrow splitting by routing field |
| `destination.py` | LRU connection pool for destination catalogs, lease pinning |
| `state.py` | Per-destination cursors on plain Postgres (psycopg) |
| `arrowutil.py` | Shared Arrow kernel helpers (row_indices, full_bool) |
| `metrics.py` | Prometheus metric definitions (27 metrics) |
| `server.py` | HTTP /metrics, /healthz, /readyz, /status, /ui, /ui/sse |
| `logging_config.py` | Structured logging setup |

## Testing

- Unit tests: `tests/unit/` — mocked pyducklake, fast (356 tests)
- Integration tests: `tests/integration/` — real pyducklake with local DuckDB; Postgres-backed state tests via testcontainers (45 tests)
- Performance tests: `tests/perf/` — router, phases, delete filter, end-to-end delivery fanout at 200/500/1000 destinations (11 benchmarks)
- Soak: manual docker-compose kill sequence (SIGKILL + SIGTERM + convergence diff) — run for delivery-semantics changes

Run all: `just ci` (lock-check + format + lint + unit + integration + docs-check + Docker build). Perf: `just test-perf`.
Perf with JSON output: `just test-perf-json` → writes `perf-results.json`.

## Grafana

Dashboard at `grafana/dashboards/viaduck.json`. Available at `http://localhost:3000/d/viaduck/viaduck` when running `just up`.
