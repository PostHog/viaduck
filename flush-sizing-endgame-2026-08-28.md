# Flush sizing endgame: retire byte-denominated control (plan v3, implementation-ready)

Status: APPROVED FOR IMPLEMENTATION. Adversarial review verdict: "approve —
this design can be the last one" with 10 amendments; repo-level review
2026-08-28 added 7 more. All folded below.

Constraint set by Jakob 2026-08-28: portola's recovery (charts#14834, byte-AIMD
growing under a 1GiB ceiling) must not regress; team-2 (crumb-cut to ~2-19K-row
flushes, lag 1.87d) is second priority; no more incident-per-fix iterations.

**Base tree (resolved 2026-08-28):** the PR cuts from `main` @ e5c1e6f
(v0.0.76). PR #80 (within-entry slicing) and PR #81 (honest-bytes estimator)
are MERGED AND DEPLOYED — the deletion inventory in #4 is written against that
tree and every item in it was verified to exist there. The deployed prod image
is a post-#81 build, which is why the 08-28 incident rows below describe the
estimator's units. The jakob/single-destination worktree predates #80/#81 and
is NOT the base.

## The lesson of 2026-08-26..28, stated as a design principle

Three incidents in three days, all the SAME defect wearing different units:

| Incident | The lie |
|---|---|
| 08-26 crumb slicing | pa.Table.nbytes counts shared dictionaries per chunk (~150x) |
| 08-28 portola ceiling | flushMaxBytes calibrated in inflated units; honest units changed its meaning |
| 08-28 team-2 crumb redux | decoded-logical size expands shared blobs per row (~20-100x for low-cardinality wide values) |

There is no single correct "bytes" for an arrow table: in-memory, decoded-
logical, and parquet-on-wire differ by orders of magnitude per shape, and
every consumer wants a different one. Any byte-denominated control loop
re-inherits this forever — the next fork upgrade, encoding change, or schema
shape re-opens it.

**The controller never needed bytes.** Its feedback signal is flush DURATION
(measured directly, unit-proof). Its actuator needs any monotone knob for
"batch size". ROWS is such a knob, it is the unit the slicer/caps/logs
already speak, and both of this week's PROVEN operating points are
row-denominated: team-2 healthy at 60K-row slices all week; portola healthy
right now at ~18-20K rows (the byte-AIMD found a row count; the bytes were
scaffolding).

## The change: row-denominated adaptive flush control

One viaduck PR, mostly deletion (~-1,200 lines net). Base: `main` @ e5c1e6f.

1. **Per-destination adaptive ROWS target** `flush_target_rows[dest]`:
   - init and CEILING = `flush_batch_max_rows` (60K — the week-proven cap;
     adaptation only ever SHRINKS below it; growth is min(ceiling,
     cur+step) so no path exceeds it),
   - floor = `flush_adaptive_min_rows` (new knob, default 4K), clamped
     `min(floor, ceiling)`; config REJECTS `flush_batch_max_rows == 0`
     UNCONDITIONALLY at load — the old "0 = unlimited" escape hatch dies:
     an unbounded init/ceiling is undefined in adaptive mode, and in
     non-adaptive mode it would leave no rows bound at all once
     `flush_max_rows` is deleted (repo-review amendment 3),
   - growth step = `flush_adaptive_step_rows`, default 4K (= ceiling/15,
     the old 16MiB/256MiB ratio). Sawtooth around a duration cliff at C:
     amplitude [C/2, C], one >30s flush per (C/2)/step fast flushes —
     same duty cycle as today's controller,
   - AIMD otherwise identical: halve on flush > high_seconds (30s),
     additive grow on flush < low_seconds (5s) with fill >= 0.7 where
     fill = batch_ROWS / target_rows. **The [5s,30s) dead zone is a
     RATCHET, stated explicitly**: an in-band destination that eats one
     >30s blip halves and cannot grow back until restart. Accepted with
     one cheap fix (KEPT per Jakob 2026-08-28): additive RE-PROBE after N
     consecutive in-band, >=0.7-fill, successful flushes — new knob
     `flush_adaptive_reprobe_after`, default 50; the counter resets on any
     halve, grow, out-of-band, under-filled, or failed flush. Bounded, and
     it heals ratchet-downs from transient contention spikes without
     waiting for a restart.
   - `flush_adaptive: false` semantics post-change: fixed rows trigger at
     the ceiling. Caveat stated honestly (repo-review amendment 2): that
     is stopgap-week behavior only for SLICE-bound destinations (team-2);
     for byte-bound ones (portola under charts#14834's 1GiB ceiling ≈
     18-20K rows) fixed-60K is a step UP in batch size. Moot while
     flushAdaptive stays true, which it does throughout the rollout.
2. **Slicer and admission cut in rows only**: `n = min(remaining,
   target_rows)`. The within-entry split, take-loop, and sliced_remainder
   machinery are untouched except the cut arithmetic loses its byte branch
   (delivery.py:721-726 `over_bytes` arm; the per-row-estimate admission
   sampling at :678-689 is deleted with the estimator).
3. **One rows trigger**: fire when `buf.rows >= target_rows`, metric
   label `trigger="target"`. The legacy 500K `flush_max_rows` trigger is
   strictly shadowed (target <= 60K) — DELETE it and its knob
   (warn-ignored), rather than carrying dead code. Survivors: interval,
   memory, sliced, shutdown. (Repo-review correction: there is no "cap"
   trigger label — the watermark AND the per-dest cap checks both emit
   `trigger="memory"` today; both survive unchanged.)
4. **Delete the byte path — full inventory (verified against main @
   e5c1e6f)**: `_estimate_row_bytes`, `_honest_type`, `_ROW_BYTES_SAMPLE`,
   `_ROW_BYTES_SAMPLE_ENTRIES`, `_estimate_failure_logged`, `_per_row_est`
   (delivery.py:64-135, :382), the est_bytes plumbing and fallback ladder
   in `_flush`/maybe_flush (:778-783, :1022, and `_flush`'s `est_bytes`
   param), the take-loop byte branch and `over_bytes` split arm, the
   bytes-valued `_flush_target` init/reset (:376 and add_destination),
   `flush_max_bytes`, `flush_adaptive_min_bytes`, `flush_adaptive_step_bytes`,
   `flush_max_rows` (per #3), the `dest_flush_target_bytes` gauge
   (metrics.py:136-139 + its `_AutoPipelineLabels` wrapper and export —
   see #6), the server.py `flushTip` status tooltip (:382-389) and main.py
   `delivery_config` dump (:1879-1885), the config log lines
   (config.py:810-823), and the whole associated test surface:
   `test_honest_bytes_qe.py` (whole file); in `test_delivery.py` —
   `test_rows_trigger`, `test_bytes_trigger`, the byte-denominated
   `test_adaptive_*` suite, `test_bytes_trigger_uses_adapted_target`,
   `test_swap_is_byte_cut_at_adapted_target`,
   `test_swap_byte_cut_disabled_with_adaptive_off`,
   `test_swap_byte_cut_splits_single_chunk_at_floor`,
   `test_byte_cap_splits_oversize_entry`, the gauge-seeding test at :1393;
   byte-cap cases in `test_delivery_slicing_qe.py`
   (`test_zero_nbytes_entry_byte_cap_inert_no_division_error`,
   `test_byte_cap_below_one_row_drains_one_row_slices`,
   `test_rows_cap_and_byte_cap_together_take_the_tighter_cut`, the byte
   arms of `test_target_mutation_between_slices_converges` and
   `test_one_row_oversize_entry_flushes_whole`); in `test_config.py` the
   config-log assertions (:720-721), adaptive defaults (:950-958), and
   loader passthrough (:968-984). Rewrites to rows: the adaptive suite,
   `test_add_destination_initializes_target_and_keeps_learned_on_readd`,
   `test_memory_trigger_fires_regardless_of_target`, the slicing-QE
   keepers.
   Config compat: the retired keys are WARN-IGNORED (KEPT per Jakob) — a
   new warning loop over {flush_max_bytes, flush_adaptive_min_bytes,
   flush_adaptive_step_bytes, flush_max_rows} in the DELIVERY section
   (unknown keys are silently ignored today; the warning is new code).
   Explicitly NOT added to the M4-style refusal list (config.py:999-1007)
   — that list is poll-section, so exclusion is structural; the M4
   rationale (silent default = deleted safety) does not apply because the
   replacement controller is on by default.
5. **Memory safety is explicitly NOT this controller's job**: buffer
   caps and the RSS watermark stay on raw nbytes — inflated, the
   conservative direction for a safety bound. **Fencing (review finding
   10, the kill-the-class artifact):** (a) comment contracts on
   `_Buffer.bytes` and every cap knob: "raw nbytes, inflated, caps
   calibrated to it — changing this unit re-rates every cap ~150x";
   (b) a BYTE-INVARIANCE TEST: a buffer whose tables' nbytes are
   inflated ~1000x must produce byte-identical trigger/cut/AIMD
   decisions vs its uninflated twin. That is the executable form of
   "bytes cannot re-enter the control path." Spot-check at deploy that
   no destination's cap/inflated-row-width < target-rows equivalent
   (team-2: 60K x ~45KB inflated ~= 2.7GiB << 32GiB cap; verify the
   4GiB-default destinations likewise).
6. **Observability renames, not repurposes**: new gauge
   `viaduck_dest_flush_target_rows`; the bytes gauge is deleted, never
   refilled with row values (that would be the unit-lie in the
   observability plane). grafana-dashboards PR updates panel 40
   ("Adaptive Flush Target by Destination") and the flush-mix
   by-trigger panels for the `target` label — an explicit rollout step,
   not an afterthought. A 60K-row batch's real arrow footprint is the
   envelope the stopgap ran all week; one stated step change: byte-cut
   destinations get one full-width 60K first flush post-deploy
   (portola ~1.4GB true + concat copy — a shape it ran for months).
7. **Docs in the SAME PR (Jakob, 2026-08-28):** README.md:125
   (flush-trigger paragraph), :277-290 (config sample), AGENT.md:56
   (trigger list → `interval/target/memory/sliced/shutdown`). Repo
   convention is README synced with behavior changes; docs-check CI
   validates README links.

### Where the two flagship destinations land

- **team-2**: flushes were 8-25s at 60K rows all week → duration stays
  under the high bound → target sits AT the 60K ceiling → behavior is
  byte-identical to the stopgap era, restored on deploy, no chart change
  needed. If shard-001 contention ever pushes its flushes past 30s, the
  target shrinks — the 07-30-era protection, in units that cannot lie.
- **portola**: starts at 60K (~35-45s flush) → halves after ONE slow
  flush to 30K, which (fixed-cost-dominated, sublinear) lands ~17-22s —
  in-band. Settles at ~30K@20s (review corrected my 15-25K estimate;
  either point is healthy, and 30K halves its per-row fixed cost vs the
  byte-AIMD's 18-20K). Transition cost: 1-2 slow flushes per restart —
  comparable to today's byte-AIMD restart re-learn from its 1GiB
  ceiling. The "seed from last batch" accelerant is CUT (incoherent
  without new persistence; scope creep).

### Same-class review findings folded into this PR (2026-08-28 second pass)

A three-sweep audit (metric parity, unit mismatches, time/counting) of the
whole repo for this week's bug class, all verified in-tree. All "real bug"
findings join this PR (Jakob: "let's not ship buggy code"):

1. **AIMD feedback-signal span (the one that matters here)**: `_flush`'s
   `duration` was measured through the cursor-persist retry tail and fed to
   both `_adapt_flush_target` and `dest_write_seconds` — shared-PG latency
   shaping a per-destination contention signal, and a silent widening of
   `dest_write_seconds`' pre-buffering write-latency contract. With the
   rows controller, duration is the SOLE feedback signal, so this is
   in-scope: split the span — `apply_seconds` (destination write incl. OCC
   retries) feeds the AIMD and `dest_write_seconds`;
   `delivery_flush_seconds` keeps the full span (its HELP was already
   honest). Failure path: apply span when the write committed, full burn
   when the apply itself failed.
2. **Seed-path write metrics**: seeds emitted no `dest_rows_written_total`,
   `dest_write_seconds`, or `errors_total{type=dest_write}` — the largest
   write volume a destination ever does, invisible. Now emitted per batch
   (post-write, once), sibling to the full_cdc rows_written parity fix
   already in this branch.
3. **Read-barrier timeout lie**: `remaining_units` was initialized to 0 and
   never reassigned — the overall-timeout handler logged "0 unit(s)
   outstanding" and `.inc(0)`'d. Now decrements per yielded unit.
4. **`read_unit_max_bytes` unit contract**: priced in parquet
   `file_size_bytes` (compressed, on-wire) while claiming to bound memory —
   the unit-lie class in the READ path. Comment contract restated (rows
   and span are the memory backstops; inline rows price at 0 bytes; the
   full_cdc extension path is span-only, hard-capped at 480 snapshots).
   Behavior unchanged.
5. **Re-plan recovery read metrics**: the missing-file inline re-plan
   delivered rows without `cdc_rows_read_total`/`cdc_read_seconds` — two
   sibling read metrics disagreed exactly when a re-plan fired. Parity
   restored.
6. **Replay-inflation caveats**: `rows_replicated` double-counts in the
   commit/cursor-gap replay window AND PERSISTS to PG; the "(same caveat
   as rows_replicated)" reference existed but the caveat itself was written
   nowhere. Now documented at the delivery.py definition, the status-UI
   tooltip, and in the HELP text of `dest_rows_written/deleted/upserted/
   upsert_matched_total`.

Deferred (suspicious/cosmetic, follow-up fodder): the RETIRED-refusal
cursor-sever hole for never-tracked ids (reconciler.py:303-313 — a
design-level hole: re-add silently resumes instead of re-seeding when no
pod tracked the id), the unwritable-org per-org/per-warehouse flap
(discovery.py:686-702), k8s secret TTL cache never evicting deprovisioned
tenants, `partition_spec_total`'s two raise paths emitting no outcome,
`ClassifiedView.fetched_at` and state.py's `advance_cursors`/
`load_lifecycle_states` dead APIs, and the barrier-timeout count being
pinned by inspection rather than a unit test (the poll-cycle barrier
isn't unit-testable without heavy fixtures; the three-line count logic
was hand-verified against as_completed's raise-from-__next__ semantics).

Round 2 (same-day, adversarial re-review of the round-1 diff + the
deferred list + the previously-unswept files) landed, all verified by
869 unit + 124 integration tests and a full load-harness re-run (8/8):

- **Reviewing my own fixes found the real one**: loadcheck's 60s default
  warmup was knife-edge against the rows-controller generation it exists
  to bless (head's first flush at ~58s ≥ 60s warmup → the "after" run
  would false-FAIL). Warmup default now 90s. Also from that review:
  control-group assertion now compares against the baseline MIN and skips
  destinations with no baseline flush (cold-start slow flushes were a
  false-FAIL vector); mallardine moved into the control group so the
  differential has a target-bound tenant to evaluate (the Zipf tail is
  interval-bound and gives the assertion nothing); toxiproxy-init is
  idempotent; loadgen's tick is wall-clock-scheduled (a slow append no
  longer sags the rate silently); loadcheck pre-flights the proxy and
  prints per-window head flush duration; my "~28%" head-share comment
  was wrong arithmetic (35%).
- **Deferred-list adjudications, fixed**: `dest_last_snapshot_id` and the
  buffer gauges now seed at startup/`add_destination` (the file's own
  "0, not absent" contract); `read_clusters` zeroes at cycle top;
  `lag_seconds` clears at flush COMMIT, not submit (the UI showed `-`
  through multi-minute in-flight flushes — the exact window lag is
  watched); `pool_open_connections` re-publishes after a create failure;
  HELP texts fixed (`delivery_circuit_open` half-open semantics,
  `sliced` trigger, full `reconciler_pending` reason set, truthful
  `discovery_synced`); `FeedReader.read()` docstring marked test/parity
  API.
- **New-surface finds, fixed**: k8s secret GETs now refuse redirects
  (SA-token forwarding parity with discovery's `_NoRedirect`); scrub
  handles SQL `''`-doubled quotes (a pre-parse ATTACH exception no longer
  leaks the password tail); schema_projection guards the `column(-1)`
  silent-last-column trap; the reconciler's activation retries report
  `_broken` once per stint instead of every poll cycle; the startup
  baseline is first-wins, matching materialize() (last-wins would
  spuriously restart a duplicate-id tenant on the first drift view).

### Charts side (one small PR after the viaduck deploy)

- `delivery.flushAdaptive: true` stays and now governs the rows controller.
- Remove `flushMaxBytes`; add `flushAdaptiveMinRows` (default 4K) and
  `flushAdaptiveStepRows` (default 4K).
- The 2026-08-27/28 comment archaeology gets one closing paragraph: byte
  sizing retired, units documented as the reason, pointer to this plan.

## Local validation: Docker load harness (added 2026-08-28, VERIFIED PASSING)

So flush-sizing behavior never again needs a prod deploy to observe, the PR
includes a Docker load harness reproducing the three prod shapes. Built and
verified against the byte controller on 2026-08-28 (all 8 hard assertions
PASS; head target 32→16→8MiB under the latency wave with 12/15 contended
destinations halving, control group pinned, recovery to 32MiB after, zero
errors; head baseline batches measured at ~11K rows/flush, inside the
crumb-detector band). Building the crumb detector surfaced a real metric
gap, fixed in the same PR: `dest_rows_written_total` was incremented only
by `append_only` — full_cdc destinations never counted. One-line parity fix
in apply.py (batch.num_rows, once, post-commit, mirroring append_only) plus
the test_metrics.py plumbing check.

- `test/loadgen.py` — ~3K rows/s aggregate, Zipf-skewed across the 20 dev
   tenants (head ~35% = target-triggered; tail = interval-bound), rows
  ~2KB wide with a LOW-CARDINALITY (64-value) payload pool — the
  dictionary-sharing shape behind the 08-26 nbytes lie.
- Catalog-latency waves via toxiproxy in front of the dest-1 catalog's
  Postgres (`docker-compose.load.yaml` adds the proxy + an init container;
  `test/loadcheck.py` adds/removes the latency toxic at phase boundaries,
  so wave alignment is exact and cleanup is try/finally). Design note,
  verified empirically: competing COMMITS do not work as a contention
  lever — DuckLake conflict detection is table-scoped and appends merge
  (20 commits/s to a noise table, then to the hot replicated tables
  themselves, moved flush durations not at all: mean stayed ~0.1s). PG
  round-trip latency on the catalog connection is what a contended
  catalog actually feels like, and it is deterministic.
- `test/viaduck-load.yaml` — the dev topology split into 15 destinations
  on the CONTENDED dest-1 catalog (routed through toxiproxy) and 5 on
  dest-2 as an UNCONTENDED CONTROL GROUP; delivery tuned prod-like (120s
  interval, 32MiB byte target so the head is target-bound under both
  controller generations) with a TEST-SCALED AIMD band [0.5s, 2.0s] —
  local flushes run ~0.1s, so the prod [5,30]s band is unreachable; the
  scaled band exercises the identical controller code path (~20x margin
  keeps baseline flushes under the high bound).
- `test/loadcheck.py` — polls /metrics through baseline → wave →
  recovery and asserts, HARD: (1) head flushes on the target trigger at
  baseline; (2) CRUMB DETECTOR — head's mean baseline rows/flush sits in
  an ABSOLUTE band ([8K, 70K] for the deterministic load shape: ~16K at
  32MiB × ~2KB rows, 60K at the rows ceiling); self-relative assertions
  cannot see crumb-cutting (the controller is internally consistent at
  crumb sizes — the batch shape is not), so this is the assertion that
  would have failed for 08-26 and the 08-28 team-2 redux. (The third
  incident — portola's mis-calibrated ceiling — is calibration intent,
  not batch shape; only chart review catches it.); (3) ≥1 contended
  destination's target HALVES under the wave; (4) NO control
   destination's target moves (the differential half); (5) zero errors;
   (6) buffers return to the baseline band and head lag < 2× interval at
   the end; (7) ≥5 tenants stayed interval-bound; (8) DISTRIBUTION SHIFT —
   loadgen's burst stream flips a tail tenant (brantley, on the
   uncontended catalog) from ~30 rows/s to 1500 rows/s mid-run, and its
   trigger mix must migrate interval→target-triggered with no config
   change; (9) the burst tenant's lag stays ≤ 2× interval THROUGH the
   shift (verified: interval ×1 pre-burst → target ×9 post-burst, max lag
   68s). SOFT: `sliced` trigger observed. Reads either gauge generation
   (`viaduck_dest_flush_target_rows` preferred, `_bytes` fallback) so
   before/after runs of this PR are directly comparable.
- `docker-compose.load.yaml` overlay + `just load-up` / `load-check` /
  `load-down`.

The harness is the pre-deploy gate for rollout step 4 and the permanent
regression rig for any future sizing change. It exercises controller
BEHAVIOR (duration → target), not byte-unit pathology — the
byte-invariance fencing test (#5b) covers the unit plane.

## Why this ends the treadmill (the falsifiable claims)

1. No component converts between byte flavors anywhere in the control path
   — the unit-lie class is structurally unrepresentable, not just fixed.
2. Every regime the system has actually been healthy in this week is a
   fixed point of the new controller (team-2 at ceiling; portola
   mid-band; thin tenants at interval cadence). Review finding 6
   corrected the thin-tenant story in the FAVORABLE direction: no
   thin/mid tenant reaches 60K rows inside a 120s interval (needs
   >500 rows/s — only the big three), so the 08-27 commit-rate
   reduction is RETAINED, not lost. Rollout checklist: verify the
   per-destination trigger mix pre/post from prod Grafana.
3. The failure modes left, stated honestly:
   - duration-noise ratchet-downs for in-band destinations (healed by
     the re-probe, #1);
   - load-independent-duration oscillation (bounded sawtooth, one step
     per flush, visible in the target gauge);
   - row-width regime shifts: a rows target does not auto-compensate a
     sudden 10x width change — one oversized flush, then duration
     halves it; bounded by the (inflated >= true) buffer caps,
     self-corrects in <=2 flushes.

## What this deliberately does NOT do

- No revert of charts#14834 while the viaduck PR is in flight — portola
  keeps recovering under the byte controller until the rows controller
  replaces it in one deploy.
- No buffer-cap/watermark re-rate (separate, optional, unhurried).
- No second estimator. If someone wants byte-accurate sizing later, the
  bar set here: parquet-encode the sample and prove the unit end-to-end —
  but nothing in this plan depends on anyone ever doing that.

## Rollout

1. viaduck PR (base `main` @ e5c1e6f): rows controller + byte-path
   deletion + config warn-ignore + docs (README/AGENT.md) + tests
   (existing rows-cap suites carry most coverage; new:
   convergence-from-ceiling, byte-invariance fencing, config-compat
   warn-ignore, `flush_batch_max_rows == 0` reject, re-probe) + the
   Docker load harness above + the same-class review bundle (previous
   section; the duration-span split is already implemented on this
   branch with the seed-metrics, barrier-count, re-plan-metrics,
   unit-comment, and replay-caveat fixes — 865 unit + 124 integration
   green).
2. Adversarial review x2 (design/failure-modes + QE on transition
   dynamics and config compat), fixes, merge.
3. **Local gate:** `just load-up` + `just load-check` against the PR
   image — the ten hard assertions must pass, with the rows gauge.
4. Deploy. team-2 restored at deploy; watch portola's 1-2-flush
   convergence dip. flushAdaptive stays true throughout. ORDERING
   (review finding 1 — the sharp edge): app-deploy-under-old-chart is
   the ONLY order-free direction. The charts cleanup MUST be gated on
   the new image VERIFIED LIVE (pod digest / status endpoint), never
   on PR merge: removing flushMaxBytes from the chart while an OLD app
   runs re-defaults it to 256MiB (config.py loader default) and
   re-creates the 08-28 portola incident (~11K-row slices at its width).
5. Charts cleanup PR (remove flushMaxBytes/flushMaxRows, add
   flushAdaptiveMinRows/StepRows, rewrite the dangling comments) —
   gated per step 4. grafana-dashboards PR for panel 40 + trigger-mix
   panels lands with it.
6. Close the incident series in the drift ledger with the three-incident
   table above as the postmortem seed.
