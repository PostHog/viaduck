# Per-Destination Duckling — a millpond-shaped replicator

Status 2026-08-17: DESIGN v3 — incorporates `per-destination-review.md`
dispositions, two adversarial squad rounds (4 reviewers each; both rounds
block-as-written, all findings adjudicated herein), and the implementation
plan (§14) for `viaduck/single_destination.py`. Companions:
`log-consumer-proposal.md` (fleet plan of record, v3.1), `review_a.md`,
`still_to_do.md`.

## 1. Premise

Millpond's design (verified against `~/src/millpond/millpond/*.py`, ~4.3k
lines total, 628-line main loop):

```
loop:
  consume() → convert → accumulate
  when buffer full or time elapsed:
    write to lake → commit offsets
```

- Single thread, single loop, one topic→table per deployment.
- Static partition assignment by pod ordinal — **no consumer groups**.
- Offset commit synchronous and strictly **after** the write.
- 3 retries on write/commit failure, then **crash** — K8s restarts,
  Kafka holds the data. Duplicates bounded by one flush batch.
- At-least-once is a feature: correctness work is spent on never losing
  data, never on suppressing re-delivery.

The fleet viaduck couldn't adopt this shape because the official DuckLake
CDC API costs ~2.4s per call regardless of size (verified: no tuning knob
in the deployed extension build) — hundreds of independent readers were
uneconomic. The M4 direct-SQL feed (`viaduck/feed.py`, PR #75) removes
that constraint: a poll cycle is ~1 head/plan set of indexed SELECTs (ms
of RDS time) plus S3 parquet scans. With pgbouncer in front of the
catalog (a §9 gate — it does not exist yet), hundreds of
single-destination processes are cheap.

The accounting: the duckling is ~550 new lines against the fleet's
7,341-line core (11.6k with discovery/lifecycle). Per process it loads
none of: cluster fan-in, read pool, barrier, slice-cursors, router,
reconciler, web UI, seed modes, projection, fleet config surface. The
fleet's machinery was the cost of the 2.4s/call API forcing a
shared-reader architecture; the feed removed the constraint; the duckling
collects the dividend.

## 2. Concept mapping — with the compaction correction

| Millpond / Kafka | Per-destination duckling / DuckLake |
|---|---|
| Topic = the durable queue | Source catalog's snapshot sequence = the durable log |
| Partition offset | `snapshot_id` — globally monotonic, no partition bookkeeping |
| Static assignment by ordinal | Nothing to assign — one reader per destination |
| `kafka.commit` after write | cursor UPSERT strictly after flush commit |
| Crash after 3 attempts; K8s backoff | Identical — the source holds the data until retention |
| Topic retention | `expire_snapshots` + data-file retention — a fact of life |
| Consumer-lag alarm | Per-table snapshot-lag alarm — the ONE alert that matters |
| `auto.offset.reset` | Start-at-head knob; seeding is an out-of-band op |

Compaction, stated correctly:

- Kafka compaction rewrites segments but **preserves offsets**. DuckLake
  `MERGE_ADJACENT` is the exact analog: rewritten files carry per-row
  `_ducklake_internal_snapshot_id` from the source files (verified in the
  fork's compaction source and the parity suite).
- DuckLake `REWRITE_DELETES` has **no Kafka analog**: output files carry
  no lineage column and are stamped with the *rewrite's* snapshot id —
  survivors are re-offset. Kafka never re-offsets a surviving record.
  Invariant: **tailing by offset is safe exactly when rewrites preserve
  offsets** → replicated tables must never have deletes to rewrite.
  Enforced per-poll (§5.2), not socially.

## 3. Identity: snapshot_id is the only offset; row_id is not durable

DuckLake's `row_id` (`ducklake_data_file.row_id_start` + per-row lineage)
appears to map to the Kafka offset, but row ids can be **re-used** (known
bug; the fork's history carries eight row-id correctness patches).
Consequences, all absolute:

- Position/cursor = `snapshot_id` exclusively. Never row_id.
- Dedup key = the event's downstream `uuid`. Never row_id.
- Nothing builds on row_id for identity, ordering, or idempotency.
  feed.py's plan SELECT still carries `row_id_start` (unused); the §14
  feed delta drops it so no future contributor is tempted.

## 4. The read path — quoted, not paraphrased

The wrapper reuses `viaduck/feed.py` (plus the small §14 delta) and pins
the source column set **at boot** (fleet semantics; restart is the
schema-refresh mechanism — see §7 for why per-poll intersection was
rejected).

```
unit := FeedReader.plan_unit(table, after=cursor, budgets…)  -- hi is a
                                    -- COVERAGE boundary, never head
rows := FeedReader.read(unit, filter_expr="team_id = ?")     -- read(), not
                                    -- plan/execute: keeps the
                                    -- re-plan-on-404 recovery; two-sided
                                    -- lineage filter verbatim
                                    -- (lo < snap <= hi, conditional on
                                    -- partial_max NOT NULL)
rows := pc.is_in(rows, "team_id", team)   -- Arrow correctness layer
append(rows)                    -- 3 attempts; contention → halve budget;
                                -- exhausted → crash
cursor = unit.hi                -- strictly after commit
```

Non-obvious mechanics the reviews pinned:

- The byte/row budgets are **correctness surface**: they bound the
  duplicate window and keep destination commits off the team-2 cliff.
  The budget is **AIMD-adjusted** (port of viaduck#63 semantics: halve on
  flush contention/failure, slow recovery on success) — a fixed budget
  re-introduces the measured 07-30 wedge class, and crash-restart cannot
  shrink a fixed budget.
- **Advance-on-empty**: snapshot ids are catalog-global; a single-table
  reader sees empty plans on most polls. Empty plan ⇒
  `cursor = unit.hi` (plan_unit returns a valid coverage boundary) —
  EXCEPT: re-resolve `table_id`; if it changed (source DROP+CREATE),
  **freeze the cursor and page** — the continuation is a mandatory
  re-seed, never "carry on" (destination would silently hold old phantom
  rows + new rows).
- Duplicate bound, stated honestly: one read unit per crash; a
  committed-but-unacked append retried can make it attempts × unit.
  Absorbed by uuid dedup (a hard onboarding precondition).

## 5. Non-negotiables

1. **Lineage filter verbatim from feed.py** (two-sided, conditional).
2. **Delete-existence assertion, per poll** (before planning), all four
   checks scoped to UN-CROSSED history (`snapshot > cursor`; commits are
   id-ordered, so a delete can never land at an already-passed snapshot —
   and "accept" becomes one cursor UPDATE + restart instead of catalog
   surgery or waiting out retention):
   - `ducklake_delete_file`: zero rows for this table — AND —
   - `ducklake_data_file.end_snapshot IS NOT NULL`: zero rows for this
     table (fork-verified: merge/expire hard-DELETE catalog rows, so this
     never false-pages; only deletes, rewrite-deletes, drop/truncate set
     it) — AND —
   - witness `ducklake_snapshot_changes.changes_made` for delete activity
     (survives the delete+merge-between-polls race). The vocabulary is
     exactly the fork's: `deleted_from_table`, `inlined_delete`,
     `rewrite_delete`, `dropped_table` — comma-joined `type:table_id`
     entries (ducklake_transaction.cpp `AddChangeInfo`) — AND —
   - `to_regclass('{meta}.ducklake_inlined_delete_{table_id}') IS NULL
     OR count = 0` (inlined deletes create no delete_file row, set no
     end_snapshot, and dodge the inline registry — the round-2 hole;
     inlining defaults to 10 rows per attach, so one misconfigured writer
     opens it silently).
   Appearance ⇒ page. Requires two additive catalog indexes (§13).
3. **Flush first, cursor second.** The entire delivery correctness
   contract under at-least-once.
4. **Inline-drift tripwire, per poll**: `data_inlining_row_limit` is
   attach-scoped and unverifiable, and the `ducklake_inlined_data_tables`
   registry is populated at CREATE TABLE even with row_limit=0 (verified on
   d8a1881e) — membership is NOT the signal. **Rows in the registered
   stores are.** If inline data appears, feed.py's (parity-tested) inline
   path still SERVES it; the check pages so the drift is known. The inline
   code stays live; nothing is deleted from feed.py.
5. **Lag alarms**: page at 24h per-table lag (retention 3d; expire is
   best-effort cron; a Friday failure noticed Monday leaves ~9h). Boolean
   `cursor < MIN(snapshot_id)` = loss already happened. Alarm-evaluator
   failure is itself a page (the fleet learned the gauge can lie).
   **Hard dependency**: the megaduck source-stall alert (still_to_do §4)
   — advance-on-empty reads lag≈0 while the source is down, so duckling
   lag alarms are blind without it. Gate-worthy (§9).
6. **Retention clamp, ported**: in-transaction floor guard in the plan
   path (`lo >= MIN(snapshot_id)-1`, else raise) + the wrapper's loud
   advance with a durable loss note in `last_error`/`last_error_at`.
   Never crash-loop inside the window.

## 6. Whole-firehose reads, row-filtered — a deliberate property

Each duckling scans the full event stream and keeps its team's rows by
`team_id` **value** — the router reimplemented as a single-output-queue
filter (named as routing, deliberately).

- **Arrow-only filtering (changed in implementation review)**: the team
  filter runs ONLY as an Arrow post-filter. SQL pushdown was dropped, not
  layered: a parquet zone-map lie in a foreign-registered (`add_files`)
  file would be invisible *under*-delivery for the SQL layer, and the
  Arrow layer can never see rows the scan pruned. Full-unit Arrow
  materialization is already budget-bounded, so the pushdown bought
  nothing worth a correctness dependency.
- **No partition pruning → 300× S3 read amplification: accepted —
  wasteful but correct.** Corrected math (round 2): 2–3 GETs/file
  (footer + data), so at the unreconciled high rate (~8 snap/s) ≈
  690K files/day × 300 readers × ~2.5 GETs ≈ 500M GETs/day ≈ $160–240/day.
  Still cheap; bytes intra-region. Per-pod: ~16 Mbps average, ~300 Mbps
  catch-up bursts. Pruning stays available later as a fail-open
  optimization only.
- **Zero dependence on `partition_values`**: the hive-path corruption
  class (50689) cannot silently lose rows for this reader.
- **Catch-up amplification on compacted regions** is real (full scan of
  merge outputs, most rows lineage-filtered away): measure
  delivered:enumerated during the soak and price worst-case recovery.

## 7. Schema evolution — pinned at boot, refreshed at restart

Per-poll column intersection was REJECTED (round 2): reading a newly
added column from pre-ADD files raises binder errors in any lagged
reader — a deterministic crash-loop on the standing-rule additive change.
Fleet semantics instead:

- Boot: pin the source column set; dest DDL (millpond posture):
  `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS` for the pinned
  set. A source column *disappearing* wedges loudly. Dest-managed columns
  (`_inserted_at`, VARIANT companions) are excluded from drop detection.
- Runtime: the pinned projection reads old and new files alike (old
  columns exist in new files). A new source column is invisible until
  restart — pods restart routinely; an operator can force it.
- Rename/drop/type-change: wedges loudly (no `mapping_id` story — fleet
  shares this, `log-consumer-proposal.md` §11.4). Additive-only DDL is
  the standing rule for duckling sources.
- Known v1 limitation (fleet shares it): a process that restarts BEHIND
  an ADD COLUMN boundary pins the new column and then wedges on pre-ADD
  parquet files (binder error — the feed projects the pinned columns and
  old files lack it). The running-process case is safe (old projection
  reads new files fine). The wedge burns retention until the clamp fires
  (loud, with a loss note) — acceptable for v1; the real fix is
  per-schema-version read bucketing, deferred. (The inline leg of this
  bug — per-store projection — IS fixed in the implementation; the e2e
  caught it live.)
- VARIANT/nested source columns: unreadable via stock parquet_scan
  (shredded physical layout); boot-assert the source's column types are
  within the proven matrix, refuse loudly otherwise.

## 8. Registration and discovery (N viaducks)

**A preferred, decision at soak-start** (the v2 "DECISION: shape A" was
made one soak too early, against a misdescribed CP — the CP is
warehouse/org-shaped (`GET /warehouses`, `warehouse-team-ids`);
destination ids are derived client-side by viaduck, and scoped-token
authz does not exist as a mechanism. The real ask is a CP data-model
change + authz machinery, not "an endpoint next to #966").

- **Shape A** (each process polls a destination-scoped CP endpoint):
  requires CP destination vocabulary, ETag/304 conditional polls, jitter,
  fail-open semantics (`state: retired` is the ONLY stop signal;
  404/5xx/timeout = keep replicating, alarm; mass-retirement refused
  CP-side), per-destination tokens.
- **Shape B** (CP→ApplicationSet→Deployments) remains the fallback and is
  arguably the simpler shape (no new CP authz; fleet guards stay
  centralized; ArgoCD reconciles).
- **Provisioning plane is out of scope for v1 regardless**: at small N,
  manifests are hand-managed; automated provisioning (Deployment +
  ExternalSecret + token + cursor bootstrap per registration) belongs to
  the dynamic-sourcing program (`~/src/dynamic_viaduck_and_millpond.md`),
  which this design does not replace.
- **Onboarding ordering**: the team must be in millpond's
  IncludeValuesSource allowlist BEFORE the process starts at head.

## 9. Trust assessment and gates

Trust today: **high on correctness-by-construction, zero on operational
hours.** Proven: TLC (main 1.9B states; ViaduckJoin 100.8M;
ViaduckReads guarded PASS / unguarded FAILS as required); 939 tests incl.
the 917-line feed-parity suite; two adversarial branch passes with all
F-defects pinned; `viaduck_data_file_range` live on prod. Not earned: the
feed has zero production hours until the fleet M4 cutover (charts#14383
flips `cdc_reader=direct` at first boot; no shadow soak, by decision).

**Gates for per-destination prod rollout** (the build is not gated; prod
exposure is):

1. N≥2 fleet-weeks of direct-mode production with the O1 lag gauges
   live. Fleet hours accrue directly: identical planning SQL, lineage
   filter, compaction races.
2. Compaction-preserves-lineage drill on megaduck data (merge AND the
   rewrite-deletes ban verified operationally).
3. Planning-load test at simulated N against a prod-sized catalog —
   including the §5.2 assertion queries and the cursor UPSERT write load
   on the OCC writers' hot catalog DB.
4. **Wrapper test plan green** (§14.5) — the wrapper is the only layer
   that can silently lose data, and nothing else pins it.
5. pgbouncer stood up and enforced on the megaduck catalog (pool sizing
   is the thundering-herd bound; nothing else absorbs a fleet-wide
   recovery).
6. Megaduck source-stall alert live (§5.5 dependency).

Honest sequencing: fleet cutover → O-debt → soak → drills → build-window
puts per-destination production in **Q4**. Single operator throughout;
state the collision rather than discover it.

## 10. Operations

- One k8s Deployment per destination (no StatefulSet — no partitions),
  env config. Pod spec = duckdb memory limit + page cache + measured
  feed-path leak slope × target uptime (the 1–2Gi floor is a guess until
  the soak measures it; honest comparison: ~300 × (100–250m + 1–2Gi) ≈
  10–19× the fleet's compute, paid for isolation).
- **Cursor home: the existing `viaduck` schema on megaduck** (colocated
  with the source catalog), reusing the existing `viaduck_state` table.
  `destination_id` = the destination; `last_snapshot_id` = the cursor;
  `last_error`/`last_error_at` = the clamp loss note. Two implementation-
  review corrections baked in: (a) **`instance_id` is a stable
  per-destination constant, never a pod name** — a rollout under a
  pod-name key finds no row, starts at head, and silently skips the
  undelivered range (the best catch of the implementation review); (b)
  the duckling adds one additive column, `source_table_id bigint`, as
  provenance — a drop+recreate WHILE DOWN is otherwise undetectable (the
  boot resolves the new table_id and the witness is keyed to it).
  Operator-owned (the reshard-loss concern applied to *tenant* catalogs,
  not megaduck). Env override (`CURSOR_PG_URI`) for exceptions; catalog
  queries always run on the source connection regardless (the cursor
  connection is cursor-writes only).
- **Migration runbook note**: fleet lifecycle `retired` severs cursor
  rows for ALL instances of a destination. Fleet→duckling migration
  order: freeze fleet destination → transplant `last_snapshot_id` → boot
  duckling → verify first flush → remove from fleet discovery. Never
  retire while the duckling's row should survive.
- pgbouncer transaction pooling, as **enforced config**:
  `prepare_threshold=None` (psycopg3 auto-promotes after 5 executions),
  `idle_in_transaction_session_timeout` + `statement_timeout` on every
  catalog connection (feed.py delta, §14), transaction-scoped SETs only.
- Conninfo translation (F2 lesson): pods get ATTACH-format secrets;
  `config._to_libpq_conninfo` in the wrapper.
- Failure posture verbatim from millpond: 3 attempts then crash; K8s
  backoff is the supervisor. Jitter on poll loops (300 pods restart
  together after any fleet-wide event).
- Memory: the unit budget counts COMPRESSED parquet bytes; Arrow
  materialization multiplies ~4–10×, plus concat/filter copies — peak per
  poll ≈ 2–3× the decompressed unit, and a single oversized file reads
  whole (the budget resumes next unit). Size pods for
  `unit_max_bytes × expansion × 3`; ship a non-zero RSS_LIMIT_BYTES
  default; measure delivered:enumerated and the real expansion factor
  during the soak.
- Max-RSS watchdog calibrated on the feed path's measured slope (not the
  extension path's 2.9GiB/h).

## 11. Deleted vs the fleet

| Deleted | Fleet lines | Why it existed |
|---|---|---|
| Cluster fan-in / per-dest masking | ~250 | one reader serving N divergent destinations |
| Read pool + barrier + completion-order apply | ~200 | amortizing the shared 2.4s/call API |
| Slice-cursor rule + its TLA module | ~100 + spec | splitting shared units safely |
| Router / `split_and_count` | 153 | fan-out (§6's filter is routing, single queue) |
| Legacy extension path + dispatch | ~200 | fleet rollback |
| Fleet config surface | 1132 → ~50 | 30+ knobs → ~8 env vars |
| Fleet state/lifecycle machinery | ~420 of 472 | duckling = one UPSERT on the existing table |
| Reconciler, web UI, seed modes, projection | ~1500 | product features, not CDC |
| Discovery-client runtime secret resolution | — | each pod mounts only ITS secret |

feed.py ships with the small §14 delta. The fleet itself is NOT deleted —
it remains the escape hatch for feature gravity (§12), which is what
keeps the duckling at 550 lines.

## 12. Non-goals (refusals, not defaults)

- full_cdc / delete streams — append-only only. Delete *existence* is a
  pageable event (§5.2), not a feature gap.
- multi-destination fan-out — one destination per process.
- seeding/backfill — out-of-band snapshot copy; process starts at head
  (§8 onboarding ordering makes this safe).
- exactly-once — rejected; at-least-once + downstream uuid dedup.
- per-destination transforms beyond the team_id filter — projection,
  scrub, per-dest flush sizing are all *the fleet*. The answer to every
  "just add X" is "that's the fleet"; this refusal is what prevents
  millpond's own feature-accretion trajectory from repeating here.
- Known per-destination asks that WILL arrive (projection conventions
  like dropping `captured_at`): defer each explicitly at onboarding
  review; the dam is named, not pretended away.

## 13. Open items (carried to still_to_do.md)

- [ ] Reconcile the snapshot-rate number (2.0 vs ~8 snap/s) — §4/§6 math
      leans on it.
- [ ] Compaction-preserves-lineage drill on megaduck data — §9 gate 2.
- [ ] Planning-load test at simulated N (incl. §5.2 assertion queries +
      cursor UPSERT write load) — §9 gate 3.
- [ ] Catalog indexes for §5.2: `ducklake_delete_file(table_id)`; partial
      `ducklake_data_file(table_id) WHERE end_snapshot IS NOT NULL`
      (CONCURRENTLY; extend the boot index probe).
- [ ] CP shape A-vs-B decision at soak-start; provisioning belongs to the
      dynamic-sourcing program.
- [ ] Lag alarms: 24h per-table page; `cursor < MIN(snapshot_id)`
      boolean; evaluator-failure-as-page; source-stall alert dependency.

## 14. Implementation plan — `viaduck/single_destination.py`

Packaging: new entrypoint in the viaduck repo/image
(`python -m viaduck.single_destination`) — one image, two commands; the
parity suite and CI keep pinning the shared read core. Build can start
immediately; prod rollout is §9-gated.

### 14.1 File layout

```
viaduck/single_destination.py                  ~550 lines new
viaduck/feed.py                                +~10 lines (delta below)
tests/unit/test_single_destination.py          ~400 lines
tests/integration/test_single_destination_e2e.py ~300 lines (pg
                                          testcontainer + ducklake + minio)
```

### 14.2 The feed.py delta (entire)

1. `prepare_threshold=None` + `idle_in_transaction_session_timeout` on
   the catalog connection options (2 lines).
2. In-transaction floor guard in `_plan`: `lo >= MIN(snapshot_id)-1`
   else raise `FeedError` — closes the clamp's check-then-plan TOCTOU
   (~5 lines).
3. Drop the unused `row_id_start` from the plan SELECT (1 line, §3).

No schema bucketing, no filter changes (boot-pinned projection is already
feed.py's invoked shape).

### 14.3 The wrapper

**Env config (~8 vars + knobs):** `SOURCE_PG_URI` (ATTACH format →
`config._to_libpq_conninfo`), `SOURCE_CATALOG`, `SOURCE_TABLE`,
`DEST_PG_URI`, `DEST_CATALOG`, `DEST_TABLE`, `TEAM_ID`, optional
`CURSOR_PG_URI` (defaults to `SOURCE_PG_URI`); knobs with defaults:
poll interval, row/byte/span budgets (AIMD initial), RSS limit, port.

**Boot:**
1. Construct `FeedReader` → `verify_catalog()` (version pin, encryption
   refusal, path chain, index probe — free).
2. Resolve `table_id`; pin the source column set; boot-only dest DDL
   (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`; source
   column disappearance = wedge; dest-managed columns excluded).
3. Load/create the cursor row in `viaduck_state` (megaduck;
   `instance_id` = pod name).
4. Baseline §5.2/§5.4 assertions, loudly.

**Loop** (single thread — the §4 pseudocode is the contract):

1. Per-poll assertions (§5.2 delete quartet, §5.4 registry).
2. `plan_unit(after=cursor)` with the AIMD-adjusted budget.
3. Empty plan: `table_id` re-resolve on EVERY empty range — changed ⇒
   freeze + page (re-seed required); unchanged ⇒ `cursor = unit.hi`,
   continue. (The feed's cached table_id plans a dropped table empty
   forever; the re-resolve is the only detector.) Boot additionally
   verifies the cursor row's stored `source_table_id` — the
   drop-while-down case.
4. `FeedReader.read(unit)` — keeps the re-plan-on-404 recovery. **No
   `filter_expr`** (§6: Arrow-only filtering). Head reads go through the
   psycopg plane (`SELECT MAX(snapshot_id)`) — pyducklake's
   `current_snapshot()` materializes the whole snapshot table per call
   (the fleet measured that RSS slope).
5. Arrow `pc.is_in` filter (the only correctness layer; team value
   type-validated at boot against the pinned column).
6. Append: **INSERT BY NAME** (pyducklake's positional append
   crash-loops on managed DEFAULT columns and silently corrupts on
   reordered dest tables) — 3 attempts; contention/failure ⇒ halve
   budget (floor), success ⇒ slow recovery; exhausted ⇒ **fatal**.
   Head regression (`head < cursor`, a source restore/rebuild) is fatal
   too — the lag gauge must never read 0 while wedged.
7. Cursor UPSERT — strictly after the destination commit.
8. Sleep (jittered interval).

**Failure taxonomy** (the implementation-review C1): failures are
split at the code level — append-exhausted / cursor-exhausted /
assertion / source-rebuild raise `FatalDucklingError` and crash the
process (K8s backoff is the supervisor); read/plan errors are transient
and retried next poll (the cursor never advanced, so no loss). A
catch-all loop is the bug shape — a swallowed delete-assertion keeps
delivering from a violated table, and a swallowed cursor outage is an
unbounded duplicate storm.

**Metrics/health:** `/healthz`; rows read/delivered counters; per-table
lag (snapshots); `cursor < MIN(snapshot_id)` boolean; delivered:enumerated
ratio (priced during the soak, §6).

### 14.4 Destination write

Minimal DuckDB ATTACH of the destination catalog; `INSERT INTO dest
SELECT * FROM arrow` per unit; DDL at boot only. Cursor write is a
separate txn on the source side — the crash window between dest commit
and cursor UPSERT is the accepted at-least-once duplicate window.

### 14.5 Test plan (§9 gate 4)

- **Unit**: cursor strictly after commit; crash-between ⇒ exactly one
  unit re-delivered; clamp-loudly with durable loss note; empty-plan
  advance; `table_id` change ⇒ freeze+page; AIMD halving/recovery;
  Arrow filter catches what SQL pushdown missed (bad-stats drill);
  conninfo translation; assertion quartet pages correctly.
- **Integration**: catch-up; merge racing read; ADD COLUMN + restart
  picks it up (and: lagged reader across the boundary does NOT wedge —
  the pinned-projection property); delete appearance ⇒ page; expire
  clamp ⇒ loud advance + note; inlining appears ⇒ served + paged;
  crash-resume duplicate bound.
- The existing 917-line feed parity suite runs unchanged — this design
  does not fork feed semantics.
- No TLA for the wrapper (millpond has none; the invariant is one
  ordering rule, pinned by unit tests).

### 14.6 Build order

1. feed.py delta + its tests (half day).
2. Wrapper skeleton: config, boot, loop, cursor (day 1).
3. Retry/AIMD, assertion quartet, metrics/health (day 2).
4. Test suites (days 2–3).
5. E2E against a prod-shaped catalog on minio; then it sits until the
   §9 gates clear.

Estimate: **3–4 working days** including tests. Rollout is Q4, gated —
the build is the cheap part; registration/provisioning at N belongs to
the dynamic-sourcing program.
