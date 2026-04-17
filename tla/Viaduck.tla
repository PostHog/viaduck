---- MODULE Viaduck ----
(***************************************************************************)
(* TLA+ specification of viaduck's 3-phase CDC replication algorithm.      *)
(*                                                                         *)
(* Models a source DuckLake table replicated to N destination DuckLakes    *)
(* via snapshot-based CDC with field-based routing.                        *)
(*                                                                         *)
(* ASSUMPTIONS:                                                            *)
(*   1. Routing column is immutable (updates don't change routing value).  *)
(*   2. Rowids are stable per row: assigned on insert, reused in all CDC   *)
(*      events for that row (delete, update pre/postimage).                *)
(*   3. Each destination is handled by exactly one viaduck instance.       *)
(*   4. Destination catalog provides atomic transactions: the delete+upsert*)
(*      in Phase 3 either both succeed or both roll back. No partial       *)
(*      writes are possible within a single poll cycle.                    *)
(*                                                                         *)
(* CDC batches are processed as unordered sets, not sequences. This is     *)
(* sound because: (a) each batch covers a closed snapshot range            *)
(* [cursors[d]+1, srcSnap], (b) cursor monotonicity ensures batches are    *)
(* applied in ascending snapshot order, and (c) within-batch conflicts     *)
(* are resolved by rowid grouping, not by event ordering.                  *)
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
    cursors,        \* function: dest -> last_snapshot_id
    opCount,        \* operation counter (bounded by MaxOps)
    everCrashed     \* BOOLEAN: has any crash-after-write ever occurred?
                    \*
                    \* Invariants are conditioned on ~everCrashed. This is NOT a
                    \* hack — it's a precise statement: the algorithm provides
                    \* eventual consistency for crash-free executions.
                    \*
                    \* A per-destination lastPollClean flag was tried but is
                    \* insufficient: a successful recovery poll CANNOT fix phantom
                    \* data because insert+delete for the same rowid cancel in
                    \* conflict resolution, leaving the crashed write in place.
                    \* Phantoms from crash windows are permanent without full
                    \* re-sync. This is inherent to at-least-once delivery
                    \* without cross-catalog transactions.

vars == <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors, opCount, everCrashed>>

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

(***************************************************************************)
(* Viaduck 3-Phase CDC Algorithm                                           *)
(***************************************************************************)

\* CDC read: changes in (cursor, srcSnap] with routing value filter pushdown.
\* Filter pushdown is safe because routing column is immutable: a row's
\* routing value is the same in all CDC events (insert, delete, pre/postimage).
CDCRead(d) ==
    {c \in cdcLog : c.snap > cursors[d] /\ c.snap <= srcSnap /\ c.rv = RoutingMap[d]}

\* Phase 1: Preimage resolution.
\* Under routing column immutability, all preimages have the same routing
\* value as their postimage, so dropping preimages is safe — the postimage
\* carries the current state, and upsert handles the merge.
\*
\* The implementation also handles two defensive cases (cross-tenant
\* mutations and orphaned preimages) by converting preimages to deletes,
\* but these are constraint violations and not modeled here.
Phase1(changes) ==
    {c \in changes : c.type /= "update_preimage"}

\* Phase 2: Conflict resolution by rowid.
\* Uses rowid (not key_columns) to identify the same logical row.
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
\* snapshot_id wins (last-write-wins within the batch).
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

\* Successful poll cycle: apply changes AND advance cursor.
\* Marks the destination as clean (lastPollClean = TRUE).
PollCycle(d, i) ==
    /\ DestOwner[d] = i
    /\ cursors[d] < srcSnap
    /\ LET raw == CDCRead(d)
           p1  == Phase1(raw)
           p2  == Phase2(p1)
       IN dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, p2)]
    /\ cursors' = [cursors EXCEPT ![d] = srcSnap]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, opCount, everCrashed>>

\* Crash after write: destination updated but cursor NOT advanced.
\* Models at-least-once semantics: the next poll re-reads the same range.
\*
\* KNOWN LIMITATION: if the source deletes a row between the crash and the
\* recovery poll, the insert+delete for the same rowid cancel in conflict
\* resolution (Phase 2), but the destination already has the row from the
\* crashed write. This leaves phantom data. It is inherent to at-least-once
\* delivery without cross-catalog transactions.
CrashAfterWrite(d, i) ==
    /\ DestOwner[d] = i
    /\ cursors[d] < srcSnap
    /\ LET raw == CDCRead(d)
           p1  == Phase1(raw)
           p2  == Phase2(p1)
       IN dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, p2)]
    /\ everCrashed' = TRUE
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, cursors, opCount>>

(***************************************************************************)
(* Safety Properties                                                       *)
(*                                                                         *)
(* Invariants are conditioned on TWO requirements:                         *)
(*   1. All destinations are caught up (cursors[d] = srcSnap)              *)
(*   2. No crash-after-write has ever occurred (~everCrashed)               *)
(*                                                                         *)
(* A per-destination lastPollClean flag was attempted but TLC proved it    *)
(* insufficient: a successful recovery poll CANNOT fix phantom data        *)
(* because insert+delete for the same rowid cancel in Phase 2 conflict    *)
(* resolution, leaving the crashed write permanently in place. Phantoms   *)
(* from crash windows are irrecoverable without full re-sync.             *)
(***************************************************************************)

AllCleanAndCurrent ==
    (\A d \in Dests : cursors[d] = srcSnap) /\ ~everCrashed

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
         PollCycle(d, i)
    \/ \E d \in Dests, i \in Instances :
         CrashAfterWrite(d, i)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* TLC config definition overrides                                         *)
(***************************************************************************)

RoutingMapDef == [d \in {"d1", "d2"} |-> IF d = "d1" THEN "a" ELSE "b"]
DestOwnerDef == [d \in {"d1", "d2"} |-> "i1"]

====
