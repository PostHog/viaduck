# Runbook: resetting viaduck offsets (cursor skip-forward)

When the fleet's backlog is being abandoned (retention clamp imminent, or
an operator decision to skip a range — see `log-consumer-proposal.md`
§9.0), destinations' durable cursors are moved forward to the source head
so delivery resumes from "now". The skipped range is **acknowledged data
loss** for those destinations; the pre-check output is the accounting
record.

## Where the state lives

- **Database**: the SOURCE postgres (megaduck RDS) — viaduck's state
  manager uses the source URI unless `state.postgres_uri_env` overrides it
  (`config.StateConfig.resolve_postgres_uri`; no override in prod).
- **Table**: `viaduck.viaduck_state`, PK `(destination_id, instance_id)`.
  Prod instance id: `viaduck-0`.
- Connect the standard way (env creds / psql harness). Never paste the
  URI into commands whose error paths echo it.

## Procedure

### 1. Stop viaduck (required)

The running pod persists its in-memory cursor on every flush (~120s
cadence) and will clobber a concurrent UPDATE. The chart pins
`replicas: 1`, so a bare scale-down gets reverted by ArgoCD selfHeal —
pause auto-sync first:

```bash
kubectl -n argocd get applications | grep viaduck   # find the app name
kubectl -n argocd patch application <viaduck-app> --type merge \
  -p '{"spec":{"syncPolicy":{"automated":null}}}'
kubectl -n viaduck scale deploy/viaduck --replicas=0
kubectl -n viaduck get pods -l app=viaduck -w        # wait for termination
```

(The pod drains on SIGTERM — grace 120s — flushing buffers and
persisting final cursors; wait for it to be fully gone.)

### 2. Pre-check — save this output

```sql
SELECT MAX(snapshot_id) AS head FROM public.ducklake_snapshot;

SELECT destination_id, last_snapshot_id,
       (SELECT MAX(snapshot_id) FROM public.ducklake_snapshot) - last_snapshot_id AS lag_snapshots
FROM viaduck.viaduck_state
WHERE instance_id = 'viaduck-0'
ORDER BY lag_snapshots DESC;
```

### 3. Reset

All destinations:

```sql
BEGIN;
UPDATE viaduck.viaduck_state
   SET last_snapshot_id = (SELECT MAX(snapshot_id) FROM public.ducklake_snapshot),
       last_error = NULL
 WHERE instance_id = 'viaduck-0';

-- sanity before committing: expect <fleet size> rows, max lag 0
SELECT count(*) AS rows,
       max((SELECT MAX(snapshot_id) FROM public.ducklake_snapshot) - last_snapshot_id) AS max_lag
FROM viaduck.viaduck_state WHERE instance_id = 'viaduck-0';

COMMIT;
```

Specific destinations only: append
`AND destination_id = ANY(ARRAY['<dest-id>', ...])`.

### 4. Restart

```bash
kubectl -n viaduck scale deploy/viaduck --replicas=1
# re-enable auto-sync (restores the chart's replicas=1 authority too):
kubectl -n argocd patch application <viaduck-app> --type merge \
  -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
```

Verify on startup: first poll cycles show small lags,
`viaduck_dest_lag_snapshots` near zero for reset destinations, no
`retention clamp` warnings.

## Hazards

- **NEVER set `last_snapshot_id = 0`** unless you intend a full reseed:
  cursor 0 + `seed_truncate: true` (prod default) triggers
  TRUNCATE-and-reseed of the destination table on restart (REPLACE
  semantics, `config.RoutingConfig`).
- Forward-only. The state manager's monotonic guard treats backward
  cursor movement as an error condition; moving a cursor backward to
  re-deliver a range is a different procedure (delete + reseed or
  targeted re-read), not this one.
- If auto-sync isn't paused, selfHeal restores the pod mid-procedure and
  its first flush rewrites cursors from memory — the reset silently
  half-applies. Always verify max_lag ≈ 0 AFTER the restart, not just
  before commit.
- Lifecycle rows (`viaduck.viaduck_state_lifecycle`) are untouched by
  this procedure; paused/retired destinations stay paused/retired.

## History

- 2026-08-15: fleet-wide reset ahead of the log-consumer cutover
  (proposal §9.0: backlog abandoned at the retention decision; ~48–184k
  snapshots of lag skipped per destination).
- (Earlier resets predate this runbook; see incident notes.)
