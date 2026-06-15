# Follow-up items

## 1. pyducklake: add public API for snapshot range queries

**Why**: `earliest_snapshot_id()` in `viaduck/source.py` accesses pyducklake internals
(`table._catalog.connection`, `table._catalog.name`) to query `MIN(snapshot_id)` directly.
This bypasses the public API and is fragile — a pyducklake refactor could break it silently
at runtime rather than at import time.

**What to add to pyducklake**:

```python
# pyducklake/table.py
def earliest_snapshot(self) -> Snapshot | None:
    """Return the oldest available snapshot, or None if the table has no snapshots."""
    ...

def earliest_snapshot_id(self) -> int | None:
    snap = self.earliest_snapshot()
    return snap.snapshot_id if snap is not None else None
```

The implementation is a single `SELECT MIN(snapshot_id) ...` against the metadata schema —
trivial to add. Once available, replace the private-attribute access in `viaduck/source.py`.

**Upstream PR**: open against PostHog/pyducklake.

---

## 2. StateManager: reset stale cursor-0 on latest/earliest restart

**Why**: `initialize_destinations` uses `ON CONFLICT DO NOTHING`, so a destination stuck at
`last_snapshot_id = 0` (e.g. from a failed `cdc_replay` run) is not reset when the operator
redeploys with `seed_mode: latest`. The destination silently replays from snapshot 0, which
either crashes (if snapshot 0/1 is expired) or replays the full history. Documented by
`test_stale_zero_cursor_not_reset_by_latest` in `tests/integration/test_seed_mode_integration.py`.

**Fix**: In `initialize_destinations`, when `initial_snapshot_id > 0`, also update rows
where the existing cursor is 0 (i.e. not yet usefully seeded):

```sql
INSERT INTO {table} (destination_id, instance_id, last_snapshot_id, rows_replicated, updated_at)
SELECT unnest(%s::text[]), %s, %s, 0, %s
ON CONFLICT (destination_id, instance_id) DO UPDATE SET
    last_snapshot_id = EXCLUDED.last_snapshot_id,
    updated_at = EXCLUDED.updated_at
WHERE {table}.last_snapshot_id = 0
  AND EXCLUDED.last_snapshot_id != 0
```

This resets stale-zero rows without clobbering destinations that are actively making progress.
`scan` mode always passes `initial_snapshot_id=0`, so the `!= 0` guard makes this a no-op
for scan — preserving the pending-seed-scan cursor at 0 until `_seed_new_destinations` advances it.

---

## 3. seed_mode: warn loudly when earliest is used in production

**Why**: `earliest` replays the full available snapshot history, which on megaduck (~138k
snapshots/day, 7-day retention) means ~1M CDC reads before the destination catches up.
There is no progress feedback and no ETA. An operator who accidentally deploys `earliest`
in production will see viaduck appear to hang.

**Fix**: In `_initial_snapshot_id`, when `seed_mode == "earliest"`, emit a prominent warning:

```python
log.warning(
    "seed_mode=earliest: this destination will replay ALL available snapshots "
    "(%d to %d). This is intended for test/dev catalogs only. "
    "Use seed_mode=latest to start from now.",
    first, head,
)
```

---

## 4. Consider disallowing scan → latest → scan round-trips

---

## 5. Scale and performance tests (from QE review)

Now that `latest` mode is in use, `scan` mode failures at 1 PB scale are lower priority. These
tests are still valuable for future scan use cases and for validating CDC behavior at scale.

**Would have caught the 1 PB scan failure:**
- `test_seed_heartbeat_calls_record_poll_within_early_interval` — verify liveness heartbeat fires within 5s window before first batch arrives (unit)
- `test_scan_file_inventory_blocks_before_heartbeat` — `inspect().files()` runs synchronously before heartbeat starts; test latency at 100k+ files (integration)
- `test_scan_first_batch_latency_heartbeat_transition` — heartbeat transitions from 5s → 30s correctly if first batch > 60s (unit)
- `test_seed_scan_peak_memory_bounded_by_batch_size` — streaming seed must not grow RSS linearly with total rows (perf, `tracemalloc`)
- Arrow 32-bit string guard: integration test that streams a batch large enough to confirm the `arrow_large_buffer_size` default prevents a crash

**CDC at scale:**
- `test_cdc_read_sparse_large_snapshot_range` — many snapshots, few rows: verify latency scales with rows not snapshot count (integration/perf)
- `test_cdc_read_large_window_memory` — `read_cdc` calls `to_arrow()` which materializes the full window; no streaming path; document and test memory bound
- `test_group_by_cursor_1000_group_poll_latency` — 1000 destinations at distinct cursors → 1000 CDC calls per poll; verify < poll interval (perf)

**Query performance:**
- `test_earliest_snapshot_id_uses_index` — `SELECT MIN(snapshot_id)` on 1M-row table must use index, not seq scan; assert via `EXPLAIN` or latency < 100ms (integration)

**Correctness at scale:**
- `test_interrupted_seed_at_scale` — truncation before re-seed when 100k+ leftover rows exist; only currently tested with 3 rows (integration/perf)
- `test_seed_scan_variable_batch_size_correctness` — one giant batch vs many tiny batches vs alternating empty; key uniqueness check must be correct in all cases (unit)

**Missing observability:**
- During `earliest` catch-up, no log line shows progress toward head snapshot. Add aggregate "N snapshots behind, est. Xh to drain" log.

**Context**: Switching from `scan` to `latest` is safe (existing cursor preserved).
Switching back to `scan` is also safe (cursor != 0, so `_seed_new_destinations` skips).
But the combination is confusing and the operator may not realize the second `scan`
is a no-op. A startup log that says "destination d1 already seeded at snapshot X,
skipping scan" would help.
