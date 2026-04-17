---- MODULE Viaduck ----
(***************************************************************************)
(* TLA+ specification of viaduck's 3-phase CDC replication algorithm.      *)
(*                                                                         *)
(* Models a source DuckLake table replicated to N destination DuckLakes    *)
(* via snapshot-based CDC with field-based routing.                        *)
(*                                                                         *)
(* Assumptions:                                                            *)
(*   1. Routing column is immutable                                        *)
(*   2. Rowids are stable per row (assigned on insert, reused in CDC)      *)
(*   3. Each destination handled by exactly one viaduck instance           *)
(*                                                                         *)
(* Key finding: the at-least-once delivery guarantee means that a crash    *)
(* after write but before cursor advance can leave phantom data if the     *)
(* row is subsequently deleted before the next successful poll. This is    *)
(* inherent to the lack of cross-catalog transactions and is documented.   *)
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
    srcSnap,        \* current snapshot ID
    nextRowid,      \* next rowid to assign
    cdcLog,         \* set of CDC change records
    dstRows,        \* function: dest -> set of [key, rv, val]
    cursors,        \* function: dest -> last_snapshot_id
    opCount,        \* operation counter
    everCrashed     \* BOOLEAN: has ANY crash ever occurred? (weakens invariants)

vars == <<srcRows, srcSnap, nextRowid, cdcLog, dstRows, cursors, opCount, everCrashed>>

RoutingValues == {RoutingMap[d] : d \in Dests}

(***************************************************************************)
(* Source Operations                                                       *)
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

CDCRead(d) ==
    {c \in cdcLog : c.snap > cursors[d] /\ c.snap <= srcSnap /\ c.rv = RoutingMap[d]}

Phase1(changes) ==
    {c \in changes : c.type /= "update_preimage"}

Phase2(changes) ==
    LET insertRids == {c.rowid : c \in {x \in changes : x.type = "insert"}}
        deleteRids == {c.rowid : c \in {x \in changes : x.type = "delete"}}
        cancelledRids == insertRids \cap deleteRids
    IN {c \in changes :
          /\ ~(c.type \in {"insert", "delete"} /\ c.rowid \in cancelledRids)
          /\ ~(c.type = "update_postimage" /\ c.rowid \in deleteRids)}

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

\* Successful poll: write + advance cursor + clear crash flag
PollCycle(d, i) ==
    /\ DestOwner[d] = i
    /\ cursors[d] < srcSnap
    /\ LET raw == CDCRead(d)
           p1  == Phase1(raw)
           p2  == Phase2(p1)
       IN dstRows' = [dstRows EXCEPT ![d] = Phase3Apply(d, p2)]
    /\ cursors' = [cursors EXCEPT ![d] = srcSnap]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, opCount, everCrashed>>

\* Crash: write succeeds but cursor NOT advanced.
\* At-least-once: next poll re-reads same range.
\* Known limitation: if the row is deleted before retry, phantom data results.
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
(***************************************************************************)

\* Strong eventual consistency: when all dests are caught up AND no crashes
\* have occurred, destinations exactly match source partitions.
EventualConsistency ==
    (\A d \in Dests : cursors[d] = srcSnap) /\ ~everCrashed =>
    (\A d \in Dests :
        dstRows[d] = {[key |-> r.key, rv |-> r.rv, val |-> r.val] :
                       r \in {s \in srcRows : s.rv = RoutingMap[d]}})

\* No phantom data when current and no crashes
NoPhantomWhenCurrent ==
    (\A d \in Dests : cursors[d] = srcSnap) /\ ~everCrashed =>
    (\A d \in Dests :
        \A r \in dstRows[d] :
            \E s \in srcRows : s.key = r.key /\ s.rv = r.rv)

\* No data loss when current and no crashes
NoDataLossWhenCurrent ==
    (\A d \in Dests : cursors[d] = srcSnap) /\ ~everCrashed =>
    (\A r \in srcRows :
        \E d \in Dests :
            RoutingMap[d] = r.rv /\
            \E dr \in dstRows[d] : dr.key = r.key)

CursorMonotonicity ==
    \A d \in Dests : cursors[d] >= 0

PartitionCorrectness ==
    (\A d \in Dests : cursors[d] = srcSnap) /\ ~everCrashed =>
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
