# Spec/Impl Audit — 2026-06-11 (buffered delivery)

Read-through comparing `tla/Viaduck.tla` (extended for buffered,
concurrently-flushed delivery) against the implementation:
`viaduck/main.py`, `viaduck/delivery.py`, `viaduck/apply.py`,
`viaduck/source.py`, `viaduck/state.py`, `viaduck/destination.py`,
`viaduck/router.py`. Captured at the close of the delivery rework
(PR #21), after the M3 soak fixes (Winner(k) dedup, exclusive CDC read
bounds). TLC re-checked green at this commit: 9 invariants over
31,406,425 distinct states (incl. `FlushCommitNoCursor` and the two
crash-unconditioned invariants added in this pass — see finding 1 and
the refinements section).

Supersedes the 2026-04-28 audit (pre-buffering; archived in git history).

## Spec action → code mapping

| Spec action | Code | Notes |
|---|---|---|
| `CDCReadFrom(d, fromSnap)` — `c.snap > fromSnap /\ c.snap <= srcSnap` | `source.read_cdc_changes` / `read_cdc` (`source.py:88,132`: `after_snapshot + 1` to ducklake's inclusive API), issued per position group in `_poll_cycle` (`main.py:605-612`) | Half-open `(after, end]` contract restored by the M3 fix — see "Conformance-gap case study" below |
| `Phase1(changes)` — drop preimages, at read time | `_resolve_preimages` (`main.py:178`), per poll read before routing | Code adds two defensive conversions (cross-tenant, orphaned preimage → delete) the spec excludes as constraint violations — extensions, not divergence |
| `Phase2(changes)` — rowid conflict rules, at flush time | `_resolve_conflicts` (`apply.py:84-177`), called by `apply_full_cdc` on the concatenated in-flight tables | Spec has exactly two rules (Viaduck.tla `Phase2`); the impl adds a third — insert+postimage same rowid → drop insert (`apply.py:95-97`). Equivalent: a same-rowid postimage always carries a higher snapshot than its insert, so the spec's `Winner(k)` discards the insert in Phase 3 anyway; the impl just does it earlier. Post-condition assert mirrors the cancelled-pair invariant |
| `Phase3Apply(d, resolved)` — delete-then-upsert, atomic, `Winner(k)` | `_apply_changes` (`apply.py:264-319`): chunked deletes then upsert inside one `begin_transaction`; `_dedupe_upserts_last_write_wins` (`apply.py:239-261`) is `Winner(k)` | Impl refines the spec's `CHOOSE` tie with a deterministic (snapshot_id, rowid) order — noted in the spec comment (Viaduck.tla:196-199) |
| `BufferRead(d, i)` | `DeliveryManager.buffer` / `advance_position` (`delivery.py:144-184`); poll thread is the only caller (`main.py:647-657`) | See "Read epochs" refinement below |
| `FlushStart(d, i)` — buffer swap, in-flight guard, fires for empty buffers with position ahead | `maybe_flush` (`delivery.py:195-231`): skips in-flight dests, swaps `_Buffer()`, submits worker | Spec's `buffered /= {} \/ bufferedThrough > cursors` guard ≡ `_trigger_for_locked`'s `has_data or position_ahead` (`delivery.py:297-315`). Triggers (interval/rows/bytes/memory/shutdown) are scheduling policy below spec granularity — any FlushStart timing is allowed by the spec |
| `FlushCommit(d, i)` | `_flush` (`delivery.py:317-359`): apply → `_advance_cursor_with_retry` → update `_flushed` | Spec persists cursor in the same action; the impl's commit/persist gap is exactly what `CrashDuringFlush` models |
| `FlushFail(d, i)` — drop in-flight + live buffer, reset position to cursor | `_on_flush_failure` (`delivery.py:401-423`) | Plus epoch bump (refinement, below). Reset-first ordering (`delivery.py:362-366`) keeps the state consistent even if error bookkeeping fails |
| `CrashDuringFlush(d, i)` — dest commit lands, cursor doesn't, process dies | The real gap between `_apply_changes`'s transaction commit and `advance_cursor` | `_advance_cursor_with_retry` (`delivery.py:383-399`) narrows the window |
| `FlushCommitNoCursor(d, i)` — dest commit lands, cursor persist fails, process lives | `_flush`'s except path entered via `_advance_cursor_with_retry` exhaustion: per-destination `_on_flush_failure` with the write already committed | Added in this pass (finding 1); TLC-checked |
| `ProcessCrash` — lose memory state, keep cursors/data | No persistence of buffers/positions; `DeliveryManager.__init__` re-initializes `flushed`/`position` from PG cursors (`delivery.py:99-110`) | Soak-verified (SIGKILL → re-read → convergence) |
| `SeedDestination(d, i)` | `_seed_new_destinations` (`main.py:299-449`): filtered scan pinned to `current_id`, cursor advanced once after all batches | Spec REPLACEs dest rows; code upserts (key_columns) or appends. Equivalent for the modeled case (cursor=0 ⇒ empty dest). Divergence on re-seed without key_columns: append duplicates — documented in the docstring (`main.py:309-312`) |
| `CrashAfterSeed(d, i)` | Partial seed leaves cursor at 0 → full re-seed on restart (`main.py:316-318`) | Same idempotency caveats as seeding |
| `DestOwner[d] = i` precondition | `cfg.assigned_destination_ids()` static hash partitioning | Spec assumes fixed assignment; dynamic reassignment remains unmodeled (out of scope, unchanged from previous audit) |
| `SrcInsert` / `SrcUpdate` / `SrcDelete` | — (environment actions) | Model the *source's* behavior, not viaduck's; intentionally unmapped |

## Invariant → enforcement mapping

| Invariant | Code enforcement |
|---|---|
| `BufferPositionBound` (`cursors <= bufferedThrough <= srcSnap`) | `position` initialized to `flushed`; `_on_flush_failure` resets `position = flushed`; `advance_position` is max-guarded (`delivery.py:181`). Note `buffer()` stamps `position = through_snapshot` *unconditionally* (`delivery.py:167`) — safe only because the poll thread is the single position writer reading forward from `read_plan()`, and the epoch check discards stamps that overlapped a failure reset. Not enforced by an assertion; the bound rests on the single-writer + epoch discipline |
| `FlushStateConsistency` | `_inflight.discard` + `_inflight_bytes = 0` in `_flush`'s `finally` (`delivery.py:377-381`) — holds on commit, fail, and escaped-exception paths |
| `CursorMonotonicity` | PG upsert guard `WHERE last_snapshot_id <= EXCLUDED.last_snapshot_id` (`state.py`, both advance paths) — enforced at the durability layer, not just in memory |
| `EventualConsistency` / `NoPhantom` / `NoDataLoss` / `PartitionCorrectness` | Not directly assertable in code; locked by the integration suite (phase round-trips, buffered-delivery end-to-end, seed-boundary regression) and the kill-sequence soak |
| `NoDataLossEvenAfterCrash` / `PartitionCorrectnessEvenAfterCrash` (new) | Same code paths; these drop the `~everCrashed` condition, machine-checking that the at-least-once window only ever ADDS rows (phantoms) — the README's "no data loss path" claim is now TLC-backed |

## Refinements (impl is finer-grained than spec, equivalence argued)

1. **Read epochs** (`delivery.py:122,153-160,411`). The spec's
   `BufferRead` is one atomic action; the implementation's CDC read is
   slow and can overlap a concurrent `FlushFail` position reset. The
   epoch captured at `read_plan()` and checked at `buffer()` discards
   any read that overlapped a reset — making the read-and-buffer pair
   atomic *in effect*, which is exactly the spec's granularity.
   Without it, a stale `position` stamp would leave the dropped range
   permanently unread (found by M3 review, locked by
   `test_delivery.py` epoch tests).

2. **Winner(k) tiebreak**. Spec: `CHOOSE` over equal-snapshot
   candidates (arbitrary but fixed). Impl: deterministic
   `(snapshot_id, rowid)` sort. A valid refinement of the
   nondeterminism; documented in the spec comment.

3. **Flush triggers**. Interval/rows/bytes/memory/shutdown are policy
   for *when* `FlushStart` fires; the spec allows any interleaving, so
   TLC has already explored every trigger schedule.

## Findings

1. **MEDIUM (RESOLVED in this pass) — cursor-persist failure after
   destination commit is a phantom window without a crash.** If
   `_advance_cursor_with_retry` exhausts its 3 attempts, `_flush`
   falls into the failure path with the destination transaction
   already committed: position resets, range re-reads — the
   commit/cursor gap without process death, and with only the one
   destination affected (the process keeps running). Originally this
   audit recommended a spec comment; review correctly pushed for
   modeling instead, since `FlushFail` is defined by ASSUMPTION 4
   rollback and cannot represent a committed write. Now modeled as
   `FlushCommitNoCursor(d, i)`: Phase3Apply + per-destination reset +
   `everCrashed`, TLC-checked. Operator takeaway stands: "PG down
   during a flush" is in the phantom window; the in-process retry
   narrows it to multi-second outages.

2. **LOW — `append_only` mode is unmodeled.** The spec is full-CDC
   only. Append-only shares the read/buffer/flush machinery but skips
   Phases 1–3; its at-least-once story is *weaker* (re-delivered rows
   duplicate; no idempotent upsert). The M3 exclusive-bounds fix
   removed the systematic cursor-snapshot re-read that would have
   duplicated rows every flush boundary; crash re-reads can still
   duplicate (documented in README's delivery guarantees).

3. **LOW — carried forward**: source key uniqueness now verified at
   seed time per partition (see follow-up 1), but post-seed inserts
   remain unverified; routing column immutability detected per-row,
   not rejected at config; pool schema cached for process lifetime
   (schema drift out of scope).

## Conformance-gap case study: the inclusive-read phantom

Worth recording as a methods lesson. The spec always said
`c.snap > fromSnap` (`CDCReadFrom`, Viaduck.tla:163-164). The
implementation passed the cursor straight to ducklake's
`table_changes`, which is inclusive on both bounds — re-reading the
cursor snapshot every cycle. TLC can never catch this class of bug:
the model checker verifies the *spec*, and the divergence lived in the
six inches between the spec and the code. It survived every unit and
integration test (idempotent upserts mask re-delivery) and fell only
to the kill-sequence soak, deterministically triggered by the seed
path (seed pins cursor = a snapshot that *contains* inserts; a
next-window delete then cancels against the re-read insert in Phase 2
and is lost — permanent phantom, `final count off by one`).

Lessons applied:
- Boundary semantics now live in the function signature
  (`after_snapshot`), not just a docstring.
- Locked by integration tests at both the source level
  (`test_read_cdc_changes_excludes_start_snapshot`,
  `test_read_cdc_excludes_after_snapshot_append_only`) and end-to-end
  (`test_seed_boundary_delete_not_cancelled_by_reread`).
- General rule: every spec comparison operator that crosses an
  external API boundary deserves an integration test of the boundary
  itself, not a mock assertion of the arguments.

## Bottom line

Every spec action maps to code; every divergence is either a
documented refinement (epochs, Winner tiebreak, triggers) or a known,
documented limitation shared with the spec (commit/cursor-gap
phantom, seed append duplication). The one behavioral gap found since
the spec extension — inclusive read bounds — was caught by the soak
the plan mandated, fixed, and regression-locked. The implementation
is a faithful refinement of `Viaduck.tla` for the modeled cases.

Follow-ups worth doing eventually (not blocking):
1. ~~Source key-uniqueness validation~~ (carried from 2026-04-28
   audit) — RESOLVED in this pass: the seed scan now verifies
   `key_columns` uniqueness per partition at zero extra I/O and fails
   the seed before the cursor advances
   (`main.py:_verify_seed_key_uniqueness`). Remaining gap: post-seed
   inserts are not re-verified (the contract still applies; a
   steady-state check would need a scan).
2. Property-based (hypothesis) equivalence test driving random CDC
   batches through `_resolve_conflicts` + `_apply_changes` against a
   set-semantics oracle of `Phase2`/`Phase3Apply`.
3. Worker-count sweep (1/2/4/8/16) on the fanout bench before making
   any saturation claims; the flat dests/s curve proves linear
   per-destination cost, not CPU saturation.
