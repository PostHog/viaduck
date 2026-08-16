---- MODULE Viaduck ----
(***************************************************************************)
(* TLA+ specification of viaduck's 3-phase CDC replication algorithm with  *)
(* buffered, concurrently-flushed delivery.                                *)
(*                                                                         *)
(* Models a source DuckLake table replicated to N destination DuckLakes    *)
(* via snapshot-based CDC with field-based routing. CDC reads (poll        *)
(* cadence) are decoupled from destination writes (flush cadence):        *)
(* routed changes accumulate in per-destination in-memory buffers and are *)
(* applied by a pool of flush workers. Flushes for different destinations *)
(* interleave arbitrarily (the worker pool); flushes for the SAME         *)
(* destination are serial (the in-flight guard). A flush is split into    *)
(* FlushStart (buffer swap) and FlushCommit/FlushFail/CrashDuringFlush    *)
(* (outcomes) so TLC explores reads racing in-flight flushes.             *)
(*                                                                         *)
(* ASSUMPTIONS:                                                            *)
(*   1. Routing column is immutable (updates don't change routing value).  *)
(*   2. Rowids are stable per row: assigned on insert, reused in all CDC   *)
(*      events for that row (delete, update pre/postimage). KNOWN OPEN     *)
(*      ISSUE: DuckLake empirically reuses a rowid when an upsert          *)
(*      re-creates a deleted key, violating this assumption (the model     *)
(*      excludes it by construction via nextRowid++). A snapshot-ordered   *)
(*      latest-event-wins Phase2 is the candidate fix — to be modeled.     *)
(*   3. Each destination is handled by exactly one viaduck instance, and   *)
(*      at most one flush per destination is in flight at a time.          *)
(*   4. Destination catalog provides atomic transactions: the delete+upsert*)
(*      in Phase 3 either both succeed or both roll back. A failed flush   *)
(*      leaves the destination untouched.                                  *)
(*                                                                         *)
(* CDC batches are processed as unordered sets, not sequences. With        *)
(* buffering, a flush applies the UNION of changes from one or more reads  *)
(* covering adjacent snapshot ranges (bufferedThrough tracking makes the   *)
(* ranges disjoint and gap-free). This is sound for the same reasons a     *)
(* single wide-range read is sound: (a) the union covers the closed range  *)
(* (cursors[d], inflightThrough[d]], (b) cursor monotonicity ensures       *)
(* flushes apply in ascending range order, and (c) cross-read conflicts    *)
(* (e.g. insert in one read, its delete in a later read) are resolved by   *)
(* rowid grouping in Phase 2 at flush time, exactly as within-read         *)
(* conflicts are.                                                          *)
(*                                                                         *)
(* M3 (2026-08, log-consumer-proposal.md): the buffer is now a SEQUENCE    *)
(* of cursor-carrying entries [rows, cov] — the implementation's flush     *)
(* slices. Reads append entries; FlushStart commits a nonempty PREFIX and  *)
(* advances the cursor only to the prefix's coverage watermark (cov),      *)
(* which the slice-cursor rule computes as (min snapshot over later        *)
(* slices) - 1: no row with snap <= cov may sit in a later entry, so the   *)
(* durable cursor never passes undelivered data even when an entry's own   *)
(* rows exceed its cov (merged-file straddle slices). A failed slice flush *)
(* discards the suffix and rewinds to the cursor; already-committed prefix *)
(* slices are re-read — the at-least-once duplicate window, absorbed by    *)
(* the same set semantics as every other replay. Cluster fan-in (one       *)
(* physical read feeding destinations at different positions with per-     *)
(* destination masks) is a refinement of per-destination BufferRead — the *)
(* union of per-destination logical reads of the same span, so no new      *)
(* action. BufferCap gating is unchanged (per-destination, hard guard);    *)
(* exclusion of an at-cap member from a shared physical read is the        *)
(* routing/mask layer, below this abstraction.                             *)
(*                                                                         *)
(* CRASH MODEL: every crash and failure transition is checked              *)
(* UNCONDITIONALLY — ProcessCrash (lose all in-memory state), FlushFail    *)
(* (destination rollback + drop-buffer recovery), CrashDuringFlush and     *)
(* FlushCommitNoCursor (destination commit lands, cursor does not), and    *)
(* CrashAfterSeed. Earlier revisions conditioned the consistency           *)
(* invariants on no commit/cursor-gap crash ever occurring (an             *)
(* `everCrashed` flag), because Phase 2 cancelled insert+delete pairs and  *)
(* a cancelled delete could never remove a crashed write (permanent        *)
(* phantom). Phase 2's tombstone rule (keep the delete) retired that       *)
(* limitation: the recovery replay's delete removes the phantom, and TLC   *)
(* now proves all invariants with no crash conditioning at all.            *)
(*                                                                         *)
(* MODEL SIZE: with Keys={1,2}, Dests={d1,d2}, MaxOps=4, BufferCap=3, TLC  *)
(* checks all 7 invariants over 85,012,333 distinct states (1.18B          *)
(* generated) in ~18 minutes. The pre-lifecycle model was 19,886,377       *)
(* distinct states — the growth is PauseDest's discard/rewind firing at    *)
(* any point, incl. under an in-flight flush. Removing FlushCommit's       *)
(* position-restore max() yields a 6-step BufferPositionBound              *)
(* counterexample (SrcInsert, BufferRead, FlushStart, PauseDest,           *)
(* FlushCommit) — the formal witness for the pause-races-in-flight-flush   *)
(* duplicate-delivery bug and the proof the restore is load-bearing. The   *)
(* unbuffered ancestor model was 730,153 distinct states.                  *)
(*                                                                         *)
(* SCHEMA PROJECTION: viaduck/schema_projection.py transforms each batch   *)
(* into the destination table shape before write. The implementation       *)
(* enforces at build() time that:                                          *)
(*   (a) key columns are NEITHER cast NOR dropped NOR null-filled          *)
(*       (schema_projection.build guards B2), and                          *)
(*   (b) the routing column is NEITHER cast NOR dropped NOR null-filled    *)
(*       (schema_projection.build guards B3).                              *)
(* These build-time refusals are what the spec's ValProj model relies on:  *)
(* ValProj is a per-destination transform of the `val` component of a row  *)
(* only. `key` and `rv` pass through identity. Applied inside              *)
(* Phase3Apply's row construction and SeedDestination's row construction   *)
(* (both are the only places dstRows is populated from source data). Under *)
(* this abstraction all 7 invariants continue to hold in the same form:    *)
(*   - EventualConsistency compares dstRows against ValProj-transformed    *)
(*     source rows — the destination shape is the target's shape.          *)
(*   - PartitionCorrectness holds trivially: rv is preserved.              *)
(*   - NoPhantomWhenCurrent / NoDataLossWhenCurrent compare by key, which  *)
(*     is preserved bijectively.                                           *)
(* Removing either guard (letting ValProj transform key or rv) breaks the  *)
(* invariants — this is the spec-level confirmation that the build-time    *)
(* refusals are load-bearing, not defensive.                               *)
(*                                                                         *)
(* PROJECTION FAILURE: whole-batch `pc.cast` failure is mitigated at the   *)
(* implementation layer by a per-value null fallback (millpond-style       *)
(* `_coerce_or_null`), so a producer-format drift no longer stalls the    *)
(* CDC window indefinitely. The failure surface is thus per-value nulling  *)
(* rather than a stuck flush, which does not violate any of the model's    *)
(* safety invariants (the value slot becoming NULL is a val-transform      *)
(* consistent with ValProj). Not modeled as a separate action.             *)
(***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Keys,           \* e.g. {1, 2}
    Dests,          \* e.g. {"d1", "d2"}
    RoutingMap,     \* function: dest -> routing value
    Instances,      \* e.g. {"i1"}
    DestOwner,      \* function: dest -> instance
    MaxOps,         \* bound on source operations
    BufferCap,      \* per-destination queue bound: BufferRead(d, _) is
                    \* enabled only while |buffered[d]| + |inflight[d]| <
                    \* BufferCap. Models the implementation's
                    \* buffer_max_bytes_per_destination (bytes there,
                    \* change-count here). Backpressure is destination-
                    \* local by construction — one destination's full
                    \* queue never disables a peer's BufferRead.
    ValProj         \* function: [Dests \X Vals -> Vals]. Per-destination
                    \* transform of the `val` slot only — the schema
                    \* projection abstraction. Key and rv pass through
                    \* identity (enforced at build() time by
                    \* schema_projection.py's B2 + B3 guards). Any total
                    \* function is admissible; TLC enumerates identity,
                    \* constant, and swap variants and checks all seven
                    \* invariants hold uniformly.

VARIABLES
    srcRows,        \* set of [key, rv, val, rowid]
    srcSnap,        \* current snapshot ID (monotonic)
    nextRowid,      \* next rowid to assign (monotonic, new rows only)
    cdcLog,         \* set of CDC change records
    dstRows,        \* function: dest -> set of [key, rv, val]
    cursors,        \* function: dest -> last PERSISTED snapshot id (flushed-through)
    buffered,       \* function: dest -> SEQUENCE of pending flush entries
                    \* [rows |-> set of Phase-1-resolved records, cov |-> Nat]
                    \* (M3: slice-cursor chain — FlushStart commits a prefix and
                    \* the cursor advances only to the prefix's coverage watermark;
                    \* see EntryCoverageInvariant). Row order inside an entry is
                    \* abstracted away (records are sets per entry).
    bufferedThrough,\* function: dest -> in-memory read position; reads issue from here.
                    \* Invariant: cursors[d] <= bufferedThrough[d] <= srcSnap.
                    \* NOTE (M3): position may exceed the last entry's cov — idle
                    \* advances persist through full swaps (see FlushStart).
    flushing,       \* function: dest -> BOOLEAN, a flush is in flight (in-flight guard)
    inflight,       \* function: dest -> set of records snapshot at FlushStart
    inflightThrough,\* function: dest -> cursor value to persist if the flush commits
    opCount         \* operation counter (bounded by MaxOps)

vars == <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
          buffered, bufferedThrough, flushing, inflight, inflightThrough,
          opCount>>

memVars == <<buffered, bufferedThrough, flushing, inflight, inflightThrough>>

RoutingValues == {RoutingMap[d] : d \in Dests}

(***************************************************************************)
(* Source Operations                                                       *)
(* Rowid is assigned on INSERT and persists through UPDATE and DELETE.      *)
(* Updates change val but NOT rv (routing column immutability).             *)
(***************************************************************************)

SrcInsert(key, rv, val) ==
    /\ opCount < MaxOps
    /\ ~\E r \in srcRows : r.key = key
    /\ rv \in RoutingValues
    /\ srcSnap' = srcSnap + 1
    /\ srcRows' = srcRows \cup {[key |-> key, rv |-> rv, val |-> val, rowid |-> nextRowid]}
    /\ cdcLog' = cdcLog \cup {[type |-> "insert", key |-> key, rv |-> rv,
                                val |-> val, snap |-> srcSnap + 1,
                                rowid |-> nextRowid]}
    /\ nextRowid' = nextRowid + 1
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<dstRows, cursors>>
    /\ UNCHANGED memVars

SrcDelete(key) ==
    /\ opCount < MaxOps
    /\ \E r \in srcRows : r.key = key
    /\ LET row == CHOOSE r \in srcRows : r.key = key
       IN /\ srcSnap' = srcSnap + 1
          /\ srcRows' = srcRows \ {row}
          /\ cdcLog' = cdcLog \cup {[type |-> "delete", key |-> key,
                                      rv |-> row.rv, val |-> row.val,
                                      snap |-> srcSnap + 1,
                                      rowid |-> row.rowid]}
          /\ opCount' = opCount + 1
    /\ UNCHANGED <<nextRowid, dstRows, cursors>>
    /\ UNCHANGED memVars

SrcUpdate(key, newVal) ==
    /\ opCount < MaxOps
    /\ \E r \in srcRows : r.key = key
    /\ LET old == CHOOSE r \in srcRows : r.key = key
       IN /\ old.val /= newVal
          /\ srcSnap' = srcSnap + 1
          /\ srcRows' = (srcRows \ {old}) \cup
                         {[key |-> key, rv |-> old.rv, val |-> newVal, rowid |-> old.rowid]}
          \* Pre and postimage share the row's stable rowid.
          \* rv is unchanged (routing column immutability).
          /\ cdcLog' = cdcLog \cup
               {[type |-> "update_preimage", key |-> key, rv |-> old.rv,
                 val |-> old.val, snap |-> srcSnap + 1, rowid |-> old.rowid],
                [type |-> "update_postimage", key |-> key, rv |-> old.rv,
                 val |-> newVal, snap |-> srcSnap + 1, rowid |-> old.rowid]}
          /\ opCount' = opCount + 1
    /\ UNCHANGED <<nextRowid, dstRows, cursors>>
    /\ UNCHANGED memVars

(***************************************************************************)
(* Viaduck 3-Phase CDC Algorithm — buffered delivery                       *)
(***************************************************************************)

\* CDC read: changes in (fromSnap, srcSnap] with routing value filter pushdown.
\* Filter pushdown is safe because routing column is immutable: a row's
\* routing value is the same in all CDC events (insert, delete, pre/postimage).
CDCReadFrom(d, fromSnap) ==
    {c \in cdcLog : c.snap > fromSnap /\ c.snap <= srcSnap /\ c.rv = RoutingMap[d]}

\* Phase 1: Preimage resolution. Runs at READ time (per poll read), matching
\* the implementation, where pre/postimages pair within a single read.
\* Under routing column immutability, all preimages have the same routing
\* value as their postimage, so dropping preimages is safe — the postimage
\* carries the current state, and upsert handles the merge. Filtering
\* commutes with set union, so per-read Phase 1 equals at-flush Phase 1.
\*
\* The implementation also handles two defensive cases (cross-tenant
\* mutations and orphaned preimages) by converting preimages to deletes,
\* but these are constraint violations and not modeled here.
Phase1(changes) ==
    {c \in changes : c.type /= "update_preimage"}

(***************************************************************************)
(* M3 buffer-entry helpers: the slice-cursor chain.                         *)
(*                                                                         *)
(* buffered[d] is a sequence of entries [rows, cov]. The coverage rule     *)
(* (log-consumer-proposal.md §6.2): an entry's cov is the highest cursor   *)
(* value its flush may persist — computed as (min snapshot over rows in    *)
(* LATER entries) - 1, with the last entry carrying the unit's hi. Hence   *)
(* the load-bearing property: no row in a later entry has snap <= an       *)
(* earlier entry's cov, so a prefix commit never advances the cursor past  *)
(* undelivered rows. An entry's OWN rows may exceed its cov (merged-file   *)
(* straddle slices) — those rows' delivery is certified by a later entry.  *)
(***************************************************************************)

\* All buffered records for a destination, flattened from the entry chain.
BufferRows(d) == UNION {buffered[d][k].rows : k \in 1..Len(buffered[d])}

\* Queue depth for the BufferCap guard: buffered + in-flight record count
\* (cardinality abstracts bytes — see BufferRead's comment).
QueueSize(d) == Cardinality(BufferRows(d)) + Cardinality(inflight[d])

\* The coverage watermark the next entry must meet or exceed: the chain's
\* current tail cov, or the persisted cursor when the buffer is empty.
LastCov(d) ==
    IF Len(buffered[d]) > 0 THEN buffered[d][Len(buffered[d])].cov ELSE cursors[d]

MinSnap(S) == CHOOSE m \in {c.snap : c \in S} : \A s \in {c.snap : c \in S} : m <= s

MaxSnap(S) == CHOOSE m \in {c.snap : c \in S} : \A s \in {c.snap : c \in S} : m >= s

\* Entries dropped at FlushCommit when fully covered by the commit's
\* through. (See the FlushCommit comment for the phantom chain this
\* closes.)
DropCoveredPrefix(seq, T) ==
    LET covered == {k \in 0..Len(seq) :
                      \A j \in 1..k : seq[j].hi <= T}
        n == CHOOSE k \in covered : \A m \in covered : m <= k
    IN SubSeq(seq, n + 1, Len(seq))

\* Phase 2: Conflict resolution by rowid. Runs at FLUSH time on the union
\* of all buffered reads — cross-read conflicts (insert read in one poll,
\* its delete read in a later poll, both still buffered) resolve exactly
\* like within-read conflicts.
\* - insert + delete for same rowid → drop the insert, KEEP the delete
\*   (tombstone). The delete is idempotent against a destination that
\*   never saw the insert; against a destination that DID see it via a
\*   commit/cursor-gap replay, it is the only event that can ever remove
\*   the row. Cancelling both (the previous rule) made such phantoms
\*   permanent — the retired everCrashed conditioning existed for that.
\* - update_postimage + delete for same rowid → drop postimage, keep delete
Phase2(changes) ==
    LET insertRids == {c.rowid : c \in {x \in changes : x.type = "insert"}}
        deleteRids == {c.rowid : c \in {x \in changes : x.type = "delete"}}
        tombstonedRids == insertRids \cap deleteRids
    IN {c \in changes :
          /\ ~(c.type = "insert" /\ c.rowid \in tombstonedRids)
          /\ ~(c.type = "update_postimage" /\ c.rowid \in deleteRids)}

\* Phase 3: Apply — delete then upsert.
\* Modeled as an atomic operation (ASSUMPTION 4: destination transactions).
\* For each key with multiple upsert candidates, the one with the highest
\* snapshot_id wins (last-write-wins within the batch). On equal
\* snapshot_id, CHOOSE picks an arbitrary-but-fixed candidate; the
\* implementation refines this with a deterministic rowid tiebreaker
\* (viaduck/apply.py _dedupe_upserts_last_write_wins).
Phase3Apply(d, resolved) ==
    LET keysToDelete == {c.key : c \in {r \in resolved : r.type = "delete"}}
        upsertChanges == {r \in resolved : r.type \in {"insert", "update_postimage"}}
        upsertKeys == {c.key : c \in upsertChanges}
        Winner(k) == CHOOSE c \in upsertChanges :
                        /\ c.key = k
                        /\ \A other \in upsertChanges :
                            other.key = k => other.snap <= c.snap
        \* Projection is applied at flush time (viaduck/apply.py
        \* _apply_changes: `projection.apply(upsert_rows)` — the batch is
        \* transformed to the target shape before tbl.upsert). Modeled by
        \* applying ValProj to the val slot; key and rv are preserved
        \* (schema_projection B2/B3 guards).
        rowsToUpsert == {[key |-> Winner(k).key, rv |-> Winner(k).rv,
                          val |-> ValProj[d, Winner(k).val]] : k \in upsertKeys}
        afterDelete == {r \in dstRows[d] : r.key \notin keysToDelete}
        afterUpsert == {r \in afterDelete : r.key \notin upsertKeys}
                        \cup rowsToUpsert
    IN afterUpsert

\* Buffer a CDC read: accumulate Phase-1-resolved changes, advance the
\* in-memory read position. Permitted while a flush for the same
\* destination is in flight — this is the buffer swap: FlushStart took the
\* old buffer, reads land in the fresh one. The poll thread is the only
\* buffer writer (single-writer discipline in the implementation).
\*
\* PER-DESTINATION BACKPRESSURE: the read is gated on THIS destination's
\* queue (live buffer + in-flight swap) being under BufferCap — never on a
\* peer's. This models delivery.should_pause_reads_for(d): each destination
\* is a bounded queue (Kafka-partition semantics), and a full/stuck
\* destination pauses only its own intake. NOTE this guard RESTORES spec
\* conformance rather than adding a constraint: BufferRead(d, i) was always
\* enabled per-destination independently here, but the implementation's
\* original GLOBAL buffer watermark coupled destinations' read-enablement
\* through shared memory accounting — a deviation the spec never modeled,
\* and precisely the mechanism behind two production starvation incidents
\* (team-50689 starved by iteration order; then team-2 starved by a peer's
\* in-flight bytes pinning the shared watermark). Cardinality abstracts
\* bytes: the implementation bounds queue BYTES, the model bounds queue
\* CHANGE-COUNT — both are "reads stop when this destination's queue is
\* full", which is the property the safety invariants care about.
\*
\* FIDELITY CAVEAT: this guard is HARD; the implementation's cap is SOFT
\* by at most one chunk — the poll thread checks the cap before each chunk
\* read, so a destination can cross its cap mid-chunk and land the slice
\* it was already reading. The spec therefore explores a strict subset of
\* the implementation's queue states (safety over the subset still holds;
\* the one-chunk overshoot adds no new conflict/ordering behavior, only a
\* bounded byte excess below the model's abstraction level). Wedge-freedom
\* at the cap (cap-triggered flush drains a paused queue) is enforced by
\* the implementation's maybe_flush trigger and unit tests, not by TLC —
\* this model checks safety only (CHECK_DEADLOCK FALSE, no liveness props).
BufferRead(d, i) ==
    /\ DestOwner[d] = i
    /\ bufferedThrough[d] < srcSnap
    /\ QueueSize(d) < BufferCap
    /\ LET changes == Phase1(CDCReadFrom(d, bufferedThrough[d]))
       IN  /\ \/ /\ changes = {}
                 \* Nothing routed to d: position-only advance, no entry.
                 /\ buffered' = buffered
              \/ /\ changes /= {}
                 \* Either one entry covering the unit, or a two-slice
                 \* split with the slice-cursor rule: cov(e1) = (min snap
                 \* of e2) - 1, e2 carries the unit hi.
                 \*
                 \* SPLITS ARE SNAPSHOT-CONTIGUOUS, not arbitrary subsets:
                 \* TLC's counterexample (an arbitrary split landed the
                 \* snap-3 delete in an earlier-flushed slice than its
                 \* snap-1 insert — the no-op delete healed nothing and
                 \* the insert became a permanent phantom). Within one
                 \* unit, a same-rowid insert/delete pair must flush in
                 \* snapshot order (the tombstone rule only heals the
                 \* later-flushed direction), which snap-contiguous splits
                 \* guarantee. append_only has no conflicting pairs and is
                 \* safe under any slice order; full_cdc slicing must be
                 \* snapshot-ordered. Both slices land in one BufferRead
                 \* (the poll thread / per-destination in-flight read
                 \* guard keeps buffer writes ordered).
                 /\ \/ buffered' = [buffered EXCEPT ![d] =
                            Append(@, [rows |-> changes, cov |-> srcSnap,
                                       hi |-> MaxSnap(changes)])]
                    \/ \E t \in {c.snap : c \in changes} :
                         LET e1 == {c \in changes : c.snap <= t}
                             e2 == {c \in changes : c.snap > t}
                         IN  /\ e1 /= {} /\ e2 /= {}
                             /\ buffered' = [buffered EXCEPT ![d] =
                                    Append(Append(@,
                                        [rows |-> e1, cov |-> MinSnap(e2) - 1,
                                         hi |-> MaxSnap(e1)]),
                                        [rows |-> e2, cov |-> srcSnap,
                                         hi |-> MaxSnap(e2)])]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = srcSnap]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   flushing, inflight, inflightThrough, opCount>>

\* Start a flush: swap a nonempty PREFIX of the entry chain out (the M3
\* slice semantics; k = Len is a full swap). The cursor persists the
\* prefix's coverage watermark: cov(k) for a partial swap (rows left behind
\* are covered by the suffix chain) and bufferedThrough[d] for a full swap
\* (the position may be ahead of the tail cov via idle position-only
\* advances — those ranges are empty FOR THIS DESTINATION by construction
\* of the read path, so persisting position is sound; main.py advances
\* position without an entry only when the unit routed zero rows to d).
\* Also fires for an empty buffer whose read position is ahead of the
\* persisted cursor — the lazy idle-destination persist.
FlushStart(d, i) ==
    /\ DestOwner[d] = i
    /\ ~flushing[d]
    /\ \/ /\ Len(buffered[d]) > 0
          /\ \E k \in 1..Len(buffered[d]) :
                /\ flushing' = [flushing EXCEPT ![d] = TRUE]
                /\ inflight' = [inflight EXCEPT ![d] =
                        UNION {buffered[d][j].rows : j \in 1..k}]
                /\ inflightThrough' = [inflightThrough EXCEPT ![d] =
                        IF k = Len(buffered[d])
                        THEN bufferedThrough[d]
                        ELSE buffered[d][k].cov]
                /\ buffered' = [buffered EXCEPT ![d] = SubSeq(buffered[d], k + 1, Len(buffered[d]))]
       \/ /\ buffered[d] = <<>>
          /\ bufferedThrough[d] > cursors[d]
          /\ flushing' = [flushing EXCEPT ![d] = TRUE]
          /\ inflight' = [inflight EXCEPT ![d] = {}]
          /\ inflightThrough' = [inflightThrough EXCEPT ![d] = bufferedThrough[d]]
          /\ UNCHANGED buffered
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   bufferedThrough, opCount>>

\* Flush commits: Phase 2 + Phase 3 on the in-flight set, then persist the
\* cursor. Destination commit and cursor persist are SEPARATE steps in the
\* implementation; the gap between them is modeled by CrashDuringFlush.
FlushCommit(d, i) ==
    /\ DestOwner[d] = i
    /\ flushing[d]
    /\ dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, Phase2(inflight[d]))]
    \* M3: max() with the current cursor, not assignment — the PG upsert's
    \* monotonicity guard (state.py advance_cursor; delivery.py:853). A
    \* prefix flush of a REPLAYED chain (pause/rewind raced a zombie commit)
    \* carries cov < cursor and must not regress it. Pre-M3 this was a
    \* plain assignment because inflightThrough = bufferedThrough >=
    \* cursors held by BufferPositionBound; prefix slices break that
    \* argument, so the guard is now explicit (and modeled as load-bearing).
    /\ cursors' = [cursors EXCEPT ![d] = IF @ < inflightThrough[d] THEN inflightThrough[d] ELSE @]
    \* Position restore (delivery._flush success path): a PauseDest that
    \* raced this flush rewound bufferedThrough below inflightThrough; the
    \* flush SUCCEEDED, so leaving the position behind would re-read a
    \* committed range on resume. In normal operation this is a no-op
    \* (bufferedThrough >= inflightThrough always).
    /\ bufferedThrough' = [bufferedThrough EXCEPT
                             ![d] = IF @ < inflightThrough[d] THEN inflightThrough[d] ELSE @]
    \* M3: drop still-buffered entries FULLY covered by this commit
    \* (hi <= inflightThrough). Normally a no-op — entries buffered after
    \* FlushStart cover ranges past inflightThrough. The live case: a
    \* pause/discard rewound the position mid-flush and the replay re-
    \* buffered rows the zombie's commit already resolved. Without the
    \* drop, a later prefix flush can split a re-buffered conflicting pair
    \* across a crash boundary and leave a permanent phantom (TLC witness:
    \* BufferRead, FlushStart, PauseDest, BufferRead, FlushCommit,
    \* FlushStart, CrashDuringFlush). Stale replay data is redundant by
    \* construction; dropping it is the pause's controlled-crash semantics
    \* completed late.
    /\ buffered' = [buffered EXCEPT ![d] = DropCoveredPrefix(@, inflightThrough[d])]
    /\ flushing' = [flushing EXCEPT ![d] = FALSE]
    /\ inflight' = [inflight EXCEPT ![d] = {}]
    /\ inflightThrough' = [inflightThrough EXCEPT ![d] = 0]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, opCount>>

\* Flush fails: the destination transaction rolls back (ASSUMPTION 4), the
\* in-flight set is discarded, AND the live buffer is discarded with the
\* read position reset to the persisted cursor — the next reads re-cover
\* both ranges. (Keeping the live buffer would leave a gap: it covers
\* (inflightThrough, bufferedThrough] but nothing covers
\* (cursors, inflightThrough] anymore.) This is the implementation's
\* drop-buffer failure path.
FlushFail(d, i) ==
    /\ DestOwner[d] = i
    /\ flushing[d]
    /\ flushing' = [flushing EXCEPT ![d] = FALSE]
    /\ inflight' = [inflight EXCEPT ![d] = {}]
    /\ inflightThrough' = [inflightThrough EXCEPT ![d] = 0]
    /\ buffered' = [buffered EXCEPT ![d] = <<>>]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = cursors[d]]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   opCount>>

\* Lifecycle pause/retire discard (viaduck/lifecycle.py + delivery.
\* discard_buffer): drop the live buffer and rewind the read position to
\* the persisted cursor. Unlike ProcessCrash the process lives, and
\* unlike FlushFail an IN-FLIGHT FLUSH IS PRESERVED — the implementation
\* deliberately lets an already-submitted write finish (completing and
\* advancing the cursor beats aborting mid-write). The zombie flush may
\* later FlushCommit, advancing cursors past the rewound read position;
\* FlushCommit's position-restore max() (mirroring the implementation's
\* success-path restore) is what keeps BufferPositionBound an invariant.
\* Firing PauseDest at any time over-approximates operator behavior; the
\* paused duration needs no modeling (an action not firing IS a pause),
\* and resume is just BufferRead continuing from the rewound position —
\* the crash-recovery re-read, which is why resume is gap-free.
PauseDest(d, i) ==
    /\ DestOwner[d] = i
    /\ Len(buffered[d]) > 0 \/ bufferedThrough[d] > cursors[d]
    /\ buffered' = [buffered EXCEPT ![d] = <<>>]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = cursors[d]]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   flushing, inflight, inflightThrough, opCount>>

\* Crash in the commit/cursor gap: the destination transaction committed
\* but the process died before the cursor persisted. All in-memory state
\* is lost (process death); persisted cursors and destination data remain.
\* The next start re-reads from the stale cursor (at-least-once).
\*
\* RETIRED LIMITATION: under the old cancel-both Phase 2 rule, a source
\* delete landing between this crash and the recovery read produced a
\* PERMANENT phantom (insert+delete cancelled; the crashed write stayed).
\* The tombstone rule heals it: the replayed delete survives Phase 2 and
\* removes the crashed write. All invariants now hold through this action
\* unconditionally. See FlushCommitNoCursor for the non-crash variant.
CrashDuringFlush(d, i) ==
    /\ DestOwner[d] = i
    /\ flushing[d]
    /\ dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, Phase2(inflight[d]))]
    /\ buffered' = [e \in Dests |-> <<>>]
    /\ bufferedThrough' = [e \in Dests |-> cursors[e]]
    /\ flushing' = [e \in Dests |-> FALSE]
    /\ inflight' = [e \in Dests |-> {}]
    /\ inflightThrough' = [e \in Dests |-> 0]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, cursors, opCount>>

\* The commit/cursor gap WITHOUT a process crash: the destination
\* transaction committed but the cursor persist failed (a PG outage
\* outlasting the implementation's in-process retry,
\* delivery.py:_advance_cursor_with_retry). The worker takes the failure
\* path with the write already landed: only THIS destination's buffers
\* and read position reset (the process keeps running, other destinations
\* are untouched — unlike CrashDuringFlush, which loses everything).
\* Same at-least-once window as CrashDuringFlush; healed the same way by
\* the tombstone rule on replay.
FlushCommitNoCursor(d, i) ==
    /\ DestOwner[d] = i
    /\ flushing[d]
    /\ dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, Phase2(inflight[d]))]
    /\ flushing' = [flushing EXCEPT ![d] = FALSE]
    /\ inflight' = [inflight EXCEPT ![d] = {}]
    /\ inflightThrough' = [inflightThrough EXCEPT ![d] = 0]
    /\ buffered' = [buffered EXCEPT ![d] = <<>>]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = cursors[d]]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, cursors, opCount>>

\* Plain process crash OUTSIDE the commit/cursor gap: all in-memory state
\* (buffers, read positions, in-flight sets) is lost; persisted cursors and
\* destination data are intact. An in-flight flush whose destination
\* transaction had not committed rolls back with the process. TLC checks
\* that losing buffers can never violate safety — re-reads from persisted
\* cursors re-cover everything that was buffered.
ProcessCrash ==
    /\ \E d \in Dests : Len(buffered[d]) > 0 \/ bufferedThrough[d] > cursors[d] \/ flushing[d]
    /\ buffered' = [d \in Dests |-> <<>>]
    /\ bufferedThrough' = [d \in Dests |-> cursors[d]]
    /\ flushing' = [d \in Dests |-> FALSE]
    /\ inflight' = [d \in Dests |-> {}]
    /\ inflightThrough' = [d \in Dests |-> 0]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   opCount>>

\* Seed: for a new destination (cursor at 0), read the current source state
\* filtered by routing value and bulk-load the destination. Advances cursor
\* AND the in-memory read position to srcSnap. This bypasses CDC — it reads
\* the current snapshot directly, not the change history.
\*
\* Seeding is a stronger operation than polling: it guarantees the destination
\* matches the source partition at the seeded snapshot, regardless of CDC
\* history or conflict resolution.
SeedDestination(d, i) ==
    /\ DestOwner[d] = i
    /\ cursors[d] = 0             \* only seed new destinations
    /\ ~flushing[d]
    /\ buffered[d] = <<>>
    /\ srcSnap > 0                \* source has data
    \* Seed also lands data on the destination, so the same projection
    \* applies (viaduck's seed path shares the write mechanism).
    /\ LET seedRows == {[key |-> r.key, rv |-> r.rv, val |-> ValProj[d, r.val]] :
                          r \in {s \in srcRows : s.rv = RoutingMap[d]}}
       IN dstRows' = [dstRows EXCEPT ![d] = seedRows]
    /\ cursors' = [cursors EXCEPT ![d] = srcSnap]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = srcSnap]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, buffered, flushing,
                   inflight, inflightThrough, opCount>>

\* Crash after seed: destination seeded but cursor NOT advanced; process
\* death loses all in-memory state. The next start re-seeds — and because
\* SeedDestination is REPLACE, the re-seed is a full repair even if the
\* source changed in between. (The implementation matches by truncating a
\* non-empty destination whose cursor is 0 before streaming the seed.)
CrashAfterSeed(d, i) ==
    /\ DestOwner[d] = i
    /\ cursors[d] = 0
    /\ ~flushing[d]
    /\ buffered[d] = <<>>
    /\ srcSnap > 0
    \* Seed also lands data on the destination, so the same projection
    \* applies (viaduck's seed path shares the write mechanism).
    /\ LET seedRows == {[key |-> r.key, rv |-> r.rv, val |-> ValProj[d, r.val]] :
                          r \in {s \in srcRows : s.rv = RoutingMap[d]}}
       IN dstRows' = [dstRows EXCEPT ![d] = seedRows]
    /\ buffered' = [e \in Dests |-> <<>>]
    /\ bufferedThrough' = [e \in Dests |-> cursors[e]]
    /\ flushing' = [e \in Dests |-> FALSE]
    /\ inflight' = [e \in Dests |-> {}]
    /\ inflightThrough' = [e \in Dests |-> 0]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, cursors, opCount>>

(***************************************************************************)
(* Safety Properties                                                       *)
(*                                                                         *)
(* The consistency invariants are conditioned ONLY on full quiescence —    *)
(* cursors persisted through the current source snapshot, nothing          *)
(* buffered, no flush in flight. There is NO crash conditioning: every     *)
(* crash and failure action (ProcessCrash, FlushFail, CrashDuringFlush,    *)
(* FlushCommitNoCursor, CrashAfterSeed) is fully explored and the          *)
(* invariants must hold through all of them. (The pre-tombstone spec       *)
(* conditioned consistency on an everCrashed flag — retired; see the       *)
(* CRASH MODEL header note.)                                               *)
(***************************************************************************)

AllCleanAndCurrent ==
    \A d \in Dests : cursors[d] = srcSnap /\ buffered[d] = <<>> /\ ~flushing[d]

\* Eventual consistency: destinations exactly match source partitions
\* AFTER the per-destination projection is applied. Key and rv pass through
\* identity by the B2/B3 guards; val is transformed by ValProj[d, .].
EventualConsistency ==
    AllCleanAndCurrent =>
    (\A d \in Dests :
        dstRows[d] = {[key |-> r.key, rv |-> r.rv, val |-> ValProj[d, r.val]] :
                       r \in {s \in srcRows : s.rv = RoutingMap[d]}})

\* No phantom data: no destination row without a matching source row.
\* Holds through commit/cursor-gap windows because Phase 2's tombstone
\* rule lets the recovery replay's delete remove a crashed write.
NoPhantomWhenCurrent ==
    AllCleanAndCurrent =>
    (\A d \in Dests :
        \A r \in dstRows[d] :
            \E s \in srcRows : s.key = r.key /\ s.rv = r.rv)

\* No data loss: every source row appears in the correct destination.
NoDataLossWhenCurrent ==
    AllCleanAndCurrent =>
    (\A r \in srcRows :
        \E d \in Dests :
            RoutingMap[d] = r.rv /\
            \E dr \in dstRows[d] : dr.key = r.key)

\* Cursors never regress.
CursorMonotonicity ==
    \A d \in Dests : cursors[d] >= 0

\* Rows only in the destination matching their routing value.
PartitionCorrectness ==
    AllCleanAndCurrent =>
    (\A d \in Dests :
        \A r \in dstRows[d] : r.rv = RoutingMap[d])

\* The read position never falls behind the persisted cursor and never
\* runs ahead of the source. Holds in every state (not just quiescence).
BufferPositionBound ==
    \A d \in Dests :
        /\ cursors[d] <= bufferedThrough[d]
        /\ bufferedThrough[d] <= srcSnap

\* No flush in flight => no in-flight state (the in-flight guard's
\* structural half; per-destination flush serialization is inherent in the
\* action atomicity of FlushStart..FlushCommit pairs guarded by flushing).
FlushStateConsistency ==
    \A d \in Dests :
        ~flushing[d] => (inflight[d] = {} /\ inflightThrough[d] = 0)

\* M3: the slice-cursor chain's load-bearing structure (proposal §6.2).
\* For every destination's buffer sequence:
\*   (a) covs are non-decreasing along the chain;
\*   (b) NO row in a later entry has snap <= an earlier entry's cov —
\*       the property that lets FlushStart persist cov(k) for a prefix
\*       without ever advancing the durable cursor past undelivered rows.
\* (There is deliberately NO "cov >= cursor" clause: a pause/rewind raced
\* by a zombie commit re-buffers an already-covered range, and such replay
\* entries legitimately carry cov < cursor — cursor monotonicity is
\* enforced by FlushCommit's max() guard, not by the chain.)
\* EventualConsistency catches a coverage bug end-to-end at quiescence;
\* this invariant catches it structurally at the moment of buffering.
EntryCoverageInvariant ==
    \A d \in Dests :
        \A i \in 1..Len(buffered[d]) :
            /\ i < Len(buffered[d]) =>
                    buffered[d][i].cov <= buffered[d][i + 1].cov
            /\ \A j \in i + 1..Len(buffered[d]) :
                \A r \in buffered[d][j].rows :
                    r.snap > buffered[d][i].cov

(***************************************************************************)
(* Specification                                                           *)
(***************************************************************************)

Init ==
    /\ srcRows = {}
    /\ srcSnap = 0
    /\ nextRowid = 1
    /\ cdcLog = {}
    /\ dstRows = [d \in Dests |-> {}]
    /\ cursors = [d \in Dests |-> 0]
    /\ buffered = [d \in Dests |-> <<>>]
    /\ bufferedThrough = [d \in Dests |-> 0]
    /\ flushing = [d \in Dests |-> FALSE]
    /\ inflight = [d \in Dests |-> {}]
    /\ inflightThrough = [d \in Dests |-> 0]
    /\ opCount = 0

Next ==
    \/ \E key \in Keys, rv \in RoutingValues, val \in 1..3 :
         SrcInsert(key, rv, val)
    \/ \E key \in Keys :
         SrcDelete(key)
    \/ \E key \in Keys, val \in 1..3 :
         SrcUpdate(key, val)
    \/ \E d \in Dests, i \in Instances :
         BufferRead(d, i)
    \/ \E d \in Dests, i \in Instances :
         FlushStart(d, i)
    \/ \E d \in Dests, i \in Instances :
         FlushCommit(d, i)
    \/ \E d \in Dests, i \in Instances :
         FlushFail(d, i)
    \/ \E d \in Dests, i \in Instances :
         PauseDest(d, i)
    \/ \E d \in Dests, i \in Instances :
         CrashDuringFlush(d, i)
    \/ \E d \in Dests, i \in Instances :
         FlushCommitNoCursor(d, i)
    \/ ProcessCrash
    \/ \E d \in Dests, i \in Instances :
         SeedDestination(d, i)
    \/ \E d \in Dests, i \in Instances :
         CrashAfterSeed(d, i)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* TLC config definition overrides                                         *)
(***************************************************************************)

RoutingMapDef == [d \in {"d1", "d2"} |-> IF d = "d1" THEN "a" ELSE "b"]
DestOwnerDef == [d \in {"d1", "d2"} |-> "i1"]

\* A concrete projection worth checking: identity for d1, "constant-1" for
\* d2 (a projection that collapses every val to 1 — a pathological but
\* still-total transform, models the drop/null-fill class). If the
\* invariants hold uniformly over Vals={1,2,3} (the range SrcInsert/Update
\* uses in Next), they hold for any total ValProj since the key/rv slots
\* are structurally identity. A wider ValProj enumeration would just
\* increase state count without adding invariant coverage.
ValProjDef == [<<d, v>> \in {"d1", "d2"} \X {1, 2, 3} |->
                 IF d = "d1" THEN v ELSE 1]

====
