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
(*      events for that row (delete, update pre/postimage).                *)
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
(* CRASH MODEL: ProcessCrash (lose all in-memory buffers, keep persisted   *)
(* cursors and destination data) is SAFE and unconditioned — invariants    *)
(* must hold through it, since lost buffers are re-read from persisted     *)
(* cursors. FlushFail (destination transaction rolls back) is likewise     *)
(* safe and unconditioned. Only CrashDuringFlush — destination commit      *)
(* lands but the process dies before the cursor persists — sets            *)
(* everCrashed, preserving the original spec's precisely-stated limitation:*)
(* eventual consistency is guaranteed for executions with no               *)
(* commit/cursor-gap crash (see the everCrashed comment below).            *)
(*                                                                         *)
(* MODEL SIZE: with Keys={1,2}, Dests={d1,d2}, MaxOps=4, TLC checks all 7  *)
(* invariants over 26,753,473 distinct states (251.8M generated, depth     *)
(* 20) in ~3 minutes. The unbuffered predecessor model was 730,153         *)
(* distinct states — the growth is the BufferRead/FlushStart/FlushCommit  *)
(* interleavings and the crash actions.                                    *)
(***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Keys,           \* e.g. {1, 2}
    Dests,          \* e.g. {"d1", "d2"}
    RoutingMap,     \* function: dest -> routing value
    Instances,      \* e.g. {"i1"}
    DestOwner,      \* function: dest -> instance
    MaxOps          \* bound on source operations

VARIABLES
    srcRows,        \* set of [key, rv, val, rowid]
    srcSnap,        \* current snapshot ID (monotonic)
    nextRowid,      \* next rowid to assign (monotonic, new rows only)
    cdcLog,         \* set of CDC change records
    dstRows,        \* function: dest -> set of [key, rv, val]
    cursors,        \* function: dest -> last PERSISTED snapshot id (flushed-through)
    buffered,       \* function: dest -> set of Phase-1-resolved CDC records awaiting flush
    bufferedThrough,\* function: dest -> in-memory read position; reads issue from here.
                    \* Invariant: cursors[d] <= bufferedThrough[d] <= srcSnap.
    flushing,       \* function: dest -> BOOLEAN, a flush is in flight (in-flight guard)
    inflight,       \* function: dest -> set of records snapshot at FlushStart
    inflightThrough,\* function: dest -> cursor value to persist if the flush commits
    opCount,        \* operation counter (bounded by MaxOps)
    everCrashed     \* BOOLEAN: has a commit/cursor-gap crash ever occurred?
                    \*
                    \* Invariants are conditioned on ~everCrashed. This is NOT a
                    \* hack — it's a precise statement: the algorithm provides
                    \* eventual consistency for executions free of crashes in
                    \* the window between a destination commit and its cursor
                    \* persist. A per-destination lastPollClean flag was tried
                    \* but is insufficient: a successful recovery poll CANNOT
                    \* fix phantom data because insert+delete for the same rowid
                    \* cancel in conflict resolution, leaving the crashed write
                    \* in place. Phantoms from that window are permanent without
                    \* full re-sync. This is inherent to at-least-once delivery
                    \* without cross-catalog transactions. Note that plain
                    \* process crashes (ProcessCrash) and flush failures
                    \* (FlushFail) do NOT set everCrashed — safety through
                    \* those paths is checked unconditionally.

vars == <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
          buffered, bufferedThrough, flushing, inflight, inflightThrough,
          opCount, everCrashed>>

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
    /\ UNCHANGED <<dstRows, cursors, everCrashed>>
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
    /\ UNCHANGED <<nextRowid, dstRows, cursors, everCrashed>>
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
    /\ UNCHANGED <<nextRowid, dstRows, cursors, everCrashed>>
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

\* Phase 2: Conflict resolution by rowid. Runs at FLUSH time on the union
\* of all buffered reads — cross-read conflicts (insert read in one poll,
\* its delete read in a later poll, both still buffered) resolve exactly
\* like within-read conflicts.
\* - insert + delete for same rowid → cancel both (net no-op)
\* - update_postimage + delete for same rowid → drop postimage, keep delete
Phase2(changes) ==
    LET insertRids == {c.rowid : c \in {x \in changes : x.type = "insert"}}
        deleteRids == {c.rowid : c \in {x \in changes : x.type = "delete"}}
        cancelledRids == insertRids \cap deleteRids
    IN {c \in changes :
          /\ ~(c.type \in {"insert", "delete"} /\ c.rowid \in cancelledRids)
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
        rowsToUpsert == {[key |-> Winner(k).key, rv |-> Winner(k).rv,
                          val |-> Winner(k).val] : k \in upsertKeys}
        afterDelete == {r \in dstRows[d] : r.key \notin keysToDelete}
        afterUpsert == {r \in afterDelete : r.key \notin upsertKeys}
                        \cup rowsToUpsert
    IN afterUpsert

\* Buffer a CDC read: accumulate Phase-1-resolved changes, advance the
\* in-memory read position. Permitted while a flush for the same
\* destination is in flight — this is the buffer swap: FlushStart took the
\* old buffer, reads land in the fresh one. The poll thread is the only
\* buffer writer (single-writer discipline in the implementation).
BufferRead(d, i) ==
    /\ DestOwner[d] = i
    /\ bufferedThrough[d] < srcSnap
    /\ buffered' = [buffered EXCEPT ![d] = @ \cup Phase1(CDCReadFrom(d, bufferedThrough[d]))]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = srcSnap]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   flushing, inflight, inflightThrough, opCount, everCrashed>>

\* Start a flush: swap the buffer out (worker takes a snapshot of the
\* accumulated changes + the position they cover; the live buffer resets).
\* Also fires for empty buffers whose read position is ahead of the
\* persisted cursor — that persists cursor advancement for destinations
\* that had no routed rows (the lazy idle-destination persist).
FlushStart(d, i) ==
    /\ DestOwner[d] = i
    /\ ~flushing[d]
    /\ buffered[d] /= {} \/ bufferedThrough[d] > cursors[d]
    /\ flushing' = [flushing EXCEPT ![d] = TRUE]
    /\ inflight' = [inflight EXCEPT ![d] = buffered[d]]
    /\ inflightThrough' = [inflightThrough EXCEPT ![d] = bufferedThrough[d]]
    /\ buffered' = [buffered EXCEPT ![d] = {}]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   bufferedThrough, opCount, everCrashed>>

\* Flush commits: Phase 2 + Phase 3 on the in-flight set, then persist the
\* cursor. Destination commit and cursor persist are SEPARATE steps in the
\* implementation; the gap between them is modeled by CrashDuringFlush.
FlushCommit(d, i) ==
    /\ DestOwner[d] = i
    /\ flushing[d]
    /\ dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, Phase2(inflight[d]))]
    /\ cursors' = [cursors EXCEPT ![d] = inflightThrough[d]]
    /\ flushing' = [flushing EXCEPT ![d] = FALSE]
    /\ inflight' = [inflight EXCEPT ![d] = {}]
    /\ inflightThrough' = [inflightThrough EXCEPT ![d] = 0]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, buffered,
                   bufferedThrough, opCount, everCrashed>>

\* Flush fails: the destination transaction rolls back (ASSUMPTION 4), the
\* in-flight set is discarded, AND the live buffer is discarded with the
\* read position reset to the persisted cursor — the next reads re-cover
\* both ranges. (Keeping the live buffer would leave a gap: it covers
\* (inflightThrough, bufferedThrough] but nothing covers
\* (cursors, inflightThrough] anymore.) This is the implementation's
\* drop-buffer failure path; it does NOT set everCrashed — safety through
\* this path is checked unconditionally.
FlushFail(d, i) ==
    /\ DestOwner[d] = i
    /\ flushing[d]
    /\ flushing' = [flushing EXCEPT ![d] = FALSE]
    /\ inflight' = [inflight EXCEPT ![d] = {}]
    /\ inflightThrough' = [inflightThrough EXCEPT ![d] = 0]
    /\ buffered' = [buffered EXCEPT ![d] = {}]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = cursors[d]]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   opCount, everCrashed>>

\* Crash in the commit/cursor gap: the destination transaction committed
\* but the process died before the cursor persisted. All in-memory state
\* is lost (process death); persisted cursors and destination data remain.
\* The next start re-reads from the stale cursor (at-least-once).
\*
\* KNOWN LIMITATION (carried over from the unbuffered spec): if the source
\* deletes a row between this crash and the recovery read, the insert+delete
\* for the same rowid cancel in Phase 2, but the destination already has the
\* row from the crashed write. This leaves phantom data — inherent to
\* at-least-once delivery without cross-catalog transactions.
CrashDuringFlush(d, i) ==
    /\ DestOwner[d] = i
    /\ flushing[d]
    /\ dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, Phase2(inflight[d]))]
    /\ buffered' = [e \in Dests |-> {}]
    /\ bufferedThrough' = [e \in Dests |-> cursors[e]]
    /\ flushing' = [e \in Dests |-> FALSE]
    /\ inflight' = [e \in Dests |-> {}]
    /\ inflightThrough' = [e \in Dests |-> 0]
    /\ everCrashed' = TRUE
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, cursors, opCount>>

\* Plain process crash OUTSIDE the commit/cursor gap: all in-memory state
\* (buffers, read positions, in-flight sets) is lost; persisted cursors and
\* destination data are intact. An in-flight flush whose destination
\* transaction had not committed rolls back with the process. This is the
\* SAFE crash: it does NOT set everCrashed, so TLC checks that losing
\* buffers can never violate safety — re-reads from persisted cursors
\* re-cover everything that was buffered.
ProcessCrash ==
    /\ \E d \in Dests : buffered[d] /= {} \/ bufferedThrough[d] > cursors[d] \/ flushing[d]
    /\ buffered' = [d \in Dests |-> {}]
    /\ bufferedThrough' = [d \in Dests |-> cursors[d]]
    /\ flushing' = [d \in Dests |-> FALSE]
    /\ inflight' = [d \in Dests |-> {}]
    /\ inflightThrough' = [d \in Dests |-> 0]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors,
                   opCount, everCrashed>>

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
    /\ buffered[d] = {}
    /\ srcSnap > 0                \* source has data
    /\ LET seedRows == {[key |-> r.key, rv |-> r.rv, val |-> r.val] :
                          r \in {s \in srcRows : s.rv = RoutingMap[d]}}
       IN dstRows' = [dstRows EXCEPT ![d] = seedRows]
    /\ cursors' = [cursors EXCEPT ![d] = srcSnap]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = srcSnap]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, buffered, flushing,
                   inflight, inflightThrough, opCount, everCrashed>>

\* Crash after seed: destination seeded but cursor NOT advanced; process
\* death loses all in-memory state. Re-seed on restart is idempotent (same
\* scan result applied again), but conservatively conditioned like the
\* unbuffered spec did.
CrashAfterSeed(d, i) ==
    /\ DestOwner[d] = i
    /\ cursors[d] = 0
    /\ ~flushing[d]
    /\ buffered[d] = {}
    /\ srcSnap > 0
    /\ LET seedRows == {[key |-> r.key, rv |-> r.rv, val |-> r.val] :
                          r \in {s \in srcRows : s.rv = RoutingMap[d]}}
       IN dstRows' = [dstRows EXCEPT ![d] = seedRows]
    /\ buffered' = [e \in Dests |-> {}]
    /\ bufferedThrough' = [e \in Dests |-> cursors[e]]
    /\ flushing' = [e \in Dests |-> FALSE]
    /\ inflight' = [e \in Dests |-> {}]
    /\ inflightThrough' = [e \in Dests |-> 0]
    /\ everCrashed' = TRUE
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, cursors, opCount>>

(***************************************************************************)
(* Safety Properties                                                       *)
(*                                                                         *)
(* The consistency invariants are conditioned on full quiescence —         *)
(* cursors persisted through the current source snapshot, nothing          *)
(* buffered, no flush in flight — and on no commit/cursor-gap crash        *)
(* having occurred (~everCrashed). ProcessCrash and FlushFail do NOT       *)
(* weaken the condition: executions containing them are fully checked.     *)
(***************************************************************************)

AllCleanAndCurrent ==
    /\ \A d \in Dests : cursors[d] = srcSnap /\ buffered[d] = {} /\ ~flushing[d]
    /\ ~everCrashed

\* Eventual consistency: destinations exactly match source partitions.
EventualConsistency ==
    AllCleanAndCurrent =>
    (\A d \in Dests :
        dstRows[d] = {[key |-> r.key, rv |-> r.rv, val |-> r.val] :
                       r \in {s \in srcRows : s.rv = RoutingMap[d]}})

\* No phantom data: no destination row without a matching source row.
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

\* NEW: the read position never falls behind the persisted cursor and never
\* runs ahead of the source. Holds unconditionally (incl. through
\* ProcessCrash / FlushFail resets).
BufferPositionBound ==
    \A d \in Dests :
        /\ cursors[d] <= bufferedThrough[d]
        /\ bufferedThrough[d] <= srcSnap

\* NEW: no flush in flight => no in-flight state (the in-flight guard's
\* structural half; per-destination flush serialization is inherent in the
\* action atomicity of FlushStart..FlushCommit pairs guarded by flushing).
FlushStateConsistency ==
    \A d \in Dests :
        ~flushing[d] => (inflight[d] = {} /\ inflightThrough[d] = 0)

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
    /\ buffered = [d \in Dests |-> {}]
    /\ bufferedThrough = [d \in Dests |-> 0]
    /\ flushing = [d \in Dests |-> FALSE]
    /\ inflight = [d \in Dests |-> {}]
    /\ inflightThrough = [d \in Dests |-> 0]
    /\ opCount = 0
    /\ everCrashed = FALSE

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
         CrashDuringFlush(d, i)
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

====
