---- MODULE ViaduckJoin ----
(***************************************************************************)
(* Membership-epoch extension of Viaduck.tla for C3 dynamic destinations   *)
(* (viaduck_c3_dynamic_apply.md v6, section 7).                            *)
(*                                                                         *)
(* WHY A SEPARATE MODULE: the design pins "no additions to the 85M-state   *)
(* main run". EXTENDS keeps Viaduck.tla byte-identical — its state space,  *)
(* actions and invariants are untouched — while this module adds the C3    *)
(* activation action and the re-scoped consistency theorems. Every base    *)
(* action is imported unchanged (JNext conjoins UNCHANGED on the two new   *)
(* variables around the whole base Next).                                  *)
(*                                                                         *)
(* WHAT C3 CHANGES: the reconciler (design section 4) activates a          *)
(* destination mid-run with a cursor that JUMPS FORWARD without dstRows    *)
(* receiving the skipped range:                                            *)
(*   (a) a genuinely new destination initializes its cursor at the current *)
(*       source head with an EMPTY destination — "discovery starts the     *)
(*       stream, never backfills" (section 3). This is NOT the model's     *)
(*       SeedDestination (which bulk-loads data); statics keep seeding,    *)
(*       discovered destinations start empty at head.                      *)
(*   (b) a re-added destination whose cursor fell below source retention   *)
(*       is edge-clamped forward to the oldest retained snapshot,          *)
(*       KEEPING its existing rows (never-delete: durable state persists   *)
(*       across stop/re-add).                                              *)
(* Both are one action here, DestStart(d, i, s): jump cursor (and the      *)
(* read position — activate step 6 clamp-persists durably BEFORE delivery  *)
(* registration loads cursors) to some s > cursors[d], s <= srcSnap.       *)
(* Case (a) is the cursors[d] = 0, dstRows[d] = {} instance; case (b) is   *)
(* everything else. A re-add WITHIN retention (resume at the stored        *)
(* cursor, no jump) is deliberately NOT a DestStart: this model has no     *)
(* active-set variable (PauseDest not firing IS a pause, per the base      *)
(* model's lifecycle note), so a no-jump re-add is just BufferRead         *)
(* resuming — and it must NOT raise joinSnap, because the destination is   *)
(* still owed everything since its original join. Hence the strict         *)
(* s > cursors[d] guard.                                                   *)
(*                                                                         *)
(* joinSnap[d] is the MEMBERSHIP EPOCH: the snapshot at/below which d is   *)
(* NOT owed data. 0 for startup destinations (owed everything — the base   *)
(* model's implicit epoch). DestStart sets joinSnap := cursor := s. The    *)
(* base consistency invariants (EventualConsistency,                       *)
(* NoDataLossWhenCurrent) are FALSE BY CONSTRUCTION once DestStart can     *)
(* fire — a destination activated at head never receives rows from before  *)
(* its join point — so this module re-scopes them to source rows with CDC  *)
(* activity AFTER joinSnap[d]. At joinSnap = 0 the scoped forms are        *)
(* equivalent to the base forms (every source row's insert has snap > 0,   *)
(* and containment + DstKeyUniqueness + the phantom bound reconstruct the  *)
(* base equality), so the base theorem is the epoch-0 special case.        *)
(*                                                                         *)
(* WHAT THE SCOPED INVARIANTS DELIBERATELY DO NOT SAY: a row whose LAST    *)
(* CDC activity lies at/below joinSnap[d] is unconstrained in d. That is   *)
(* the design's acknowledged loss (section 3: the clamp is "a data-loss    *)
(* acknowledgment, never policy"): a clamped destination may retain a      *)
(* STALE version of a row whose update fell in the skipped window          *)
(* (skipped postimages never re-deliver), and may lack rows it never saw.  *)
(* The invariants bound the loss to exactly that window; everything with   *)
(* activity after the epoch must be exact.                                 *)
(*                                                                         *)
(* PHANTOM-FREEDOM STAYS GLOBAL, WITH ONE ACCOUNTED ESCAPE: the phantom    *)
(* invariant quantifies over ALL destination rows — it is not join-scoped  *)
(* (restricting it to post-join rows would let arbitrary garbage hide      *)
(* below the epoch). But a truly unconditional NoPhantomWhenCurrent is     *)
(* false by construction for case (b): a source delete falling in the      *)
(* skipped window (old cursor, s] never re-delivers, so the destination    *)
(* keeps a row the source no longer has. (6-step witness: SrcInsert,       *)
(* BufferRead, FlushStart, FlushCommit, SrcDelete, DestStart — remove the  *)
(* missedDeleteKeys escape below and TLC produces it.) missedDeleteKeys    *)
(* is a GHOST variable — no implementation counterpart, it exists only to  *)
(* state the theorem tightly: at DestStart it records the keys of delete   *)
(* events in the skipped window, and the phantom invariant admits a        *)
(* source-less destination row ONLY for those keys. This is the formal     *)
(* statement of section 3's loss bound: the clamp's phantoms are exactly   *)
(* the skipped deletes, never anything else. (The escape is key-grained,   *)
(* not event-grained, so a key deleted-in-window and later re-inserted     *)
(* and re-deleted post-join keeps its escape — a slightly generous bound,  *)
(* accepted for one ghost set instead of window bookkeeping.)              *)
(*                                                                         *)
(* CLEANLINESS GUARD, AND THE ONE DEVIATION FLAGGED FOR REVIEW: DestStart  *)
(* requires the destination clean (no in-flight flush, empty buffer, read  *)
(* position = persisted cursor — delivery.is_clean). This matches the      *)
(* restart rule (deactivate -> pending-restart until is_clean ->           *)
(* activate) and trivially covers case (a). NOT modeled: rule-1 re-add     *)
(* while a zombie flush is still in flight (design section 4 step 4/6).    *)
(* The implementation survives that race via max-guards — advance_cursor   *)
(* refuses to regress a clamped cursor and registration max-merges         *)
(* _flushed/_position — which collapse the racing clamp to either this     *)
(* clean-state clamp or a no-op. Modeling it here would require max()      *)
(* semantics inside the base FlushCommit (a base-model change, pinned      *)
(* out); the CursorNeverRewinds action property below is the model-level   *)
(* obligation any future in-flight-clamp refinement must keep.             *)
(***************************************************************************)

EXTENDS Viaduck

VARIABLES
    joinSnap,       \* function: dest -> membership epoch: the snapshot
                    \* at/below which the destination is NOT owed data.
                    \* 0 for startup destinations. Monotone (JoinSnapBound:
                    \* joinSnap[d] <= cursors[d], and DestStart's target is
                    \* above the cursor). Conceptually durable — like the
                    \* cursor row it is set alongside, it survives every
                    \* crash action, and PauseDest (= C3 deactivate, the
                    \* never-delete stop contract) does not touch it.
    missedDeleteKeys \* function: dest -> set of keys whose DELETE event was
                    \* skipped by a DestStart jump. GHOST: exists only so
                    \* the phantom invariant can name the acknowledged loss
                    \* exactly (see header). Never read by any action.

jvars == <<vars, joinSnap, missedDeleteKeys>>

\* delivery.is_clean(d): nothing in flight, nothing buffered, read position
\* not ahead of the durable cursor. (BufferPositionBound gives
\* bufferedThrough >= cursors, so "<=" is "=" here.)
IsCleanDest(d) ==
    /\ ~flushing[d]
    /\ buffered[d] = {}
    /\ bufferedThrough[d] = cursors[d]

(***************************************************************************)
(* C3 activation with a forward cursor jump (reconciler activate, design   *)
(* section 4 steps 3+6). One action covers both jump cases:                *)
(*   - new destination:  cursors[d] = 0, s = srcSnap (initialize at head); *)
(*   - retention clamp:  cursors[d] > 0, s = the oldest retained snapshot. *)
(* s ranges over ALL snapshots in (cursors[d], srcSnap] rather than just   *)
(* head/edge because the model has no retention variable — every choice of *)
(* s IS some reachable retention horizon, so nondeterminism over s checks  *)
(* every horizon at once.                                                  *)
(* Ordering fidelity: cursor and read position jump together — activate    *)
(* clamp-persists durably BEFORE registration loads cursors (section 3:    *)
(* register-then-clamp would leave the in-memory floor stale and a first   *)
(* flush failure would rewind into the expired range). dstRows is          *)
(* UNTOUCHED: never-delete means re-add keeps whatever the destination     *)
(* already has, and discovery never backfills.                             *)
(***************************************************************************)
DestStart(d, i, s) ==
    /\ DestOwner[d] = i
    /\ IsCleanDest(d)
    /\ s > cursors[d]             \* a real jump; no-jump re-adds are not
                                  \* DestStarts (see header) and must not
                                  \* raise the epoch
    /\ s <= srcSnap
    /\ cursors' = [cursors EXCEPT ![d] = s]
    /\ bufferedThrough' = [bufferedThrough EXCEPT ![d] = s]
    /\ joinSnap' = [joinSnap EXCEPT ![d] = s]
    \* Ghost bookkeeping: deletes in the skipped window (cursors[d], s]
    \* for this destination's partition will never re-deliver — record
    \* their keys as the acknowledged phantom set (section 3 loss bound).
    /\ missedDeleteKeys' = [missedDeleteKeys EXCEPT
         ![d] = @ \cup {c.key : c \in {x \in cdcLog :
                           /\ x.type = "delete"
                           /\ x.rv = RoutingMap[d]
                           /\ x.snap > cursors[d]
                           /\ x.snap <= s}}]
    /\ UNCHANGED <<srcRows, srcSnap, nextRowid, cdcLog, dstRows,
                   buffered, flushing, inflight, inflightThrough, opCount>>

(***************************************************************************)
(* Re-scoped safety properties                                             *)
(***************************************************************************)

\* The rows destination d is OWED: source rows in its partition with any
\* CDC activity after the membership epoch. Any event above the epoch
\* suffices — the destination reads everything above its cursor (>= epoch),
\* and the row's LATEST event has the maximal snap, so last-write-wins
\* lands the current value. Rows whose entire history is at/below the
\* epoch are the acknowledged not-owed set.
OwedRows(d) ==
    {r \in srcRows :
        /\ r.rv = RoutingMap[d]
        /\ \E c \in cdcLog : c.rowid = r.rowid /\ c.snap > joinSnap[d]}

\* Scoped eventual consistency: every owed row is present with the exact
\* projected value. Containment (not the base model's equality) because a
\* clamped destination legitimately retains not-owed rows; combined with
\* DstKeyUniqueness this still forces value-exactness for owed keys (a
\* stale duplicate under the same key cannot coexist), and combined with
\* NoPhantomBeyondMissedDeletes it reconstructs the base equality at
\* joinSnap = 0.
JoinScopedEventualConsistency ==
    AllCleanAndCurrent =>
    (\A d \in Dests :
        \A r \in OwedRows(d) :
            [key |-> r.key, rv |-> r.rv, val |-> ValProj[d, r.val]]
                \in dstRows[d])

\* Scoped no-data-loss: key-level presence of every owed row (the base
\* NoDataLossWhenCurrent, restricted to the owed set).
JoinScopedNoDataLoss ==
    AllCleanAndCurrent =>
    (\A d \in Dests :
        \A r \in OwedRows(d) :
            \E dr \in dstRows[d] : dr.key = r.key)

\* Phantom-freedom over ALL destination rows — NOT join-scoped — with the
\* single accounted escape: a source-less row is admissible only if its
\* key's delete fell in a DestStart-skipped window (the ghost set). This
\* is the section 3 loss bound as a theorem: clamp phantoms are exactly
\* the skipped deletes. Remove the second disjunct and TLC produces the
\* 6-step witness from the header.
NoPhantomBeyondMissedDeletes ==
    AllCleanAndCurrent =>
    (\A d \in Dests :
        \A r \in dstRows[d] :
            \/ \E s \in srcRows : s.key = r.key /\ s.rv = r.rv
            \/ r.key \in missedDeleteKeys[d])

\* The epoch never runs ahead of the cursor (DestStart sets them equal and
\* only cursors advance afterwards) — the structural half of "owed from
\* the join point onward".
JoinSnapBound ==
    \A d \in Dests : 0 <= joinSnap[d] /\ joinSnap[d] <= cursors[d]

\* At most one destination row per key. Base-model invariant (Phase3Apply
\* deletes-then-upserts one winner per key; seeds copy unique source keys)
\* stated here because JoinScopedEventualConsistency's containment form
\* leans on it for value-exactness (see comment there).
DstKeyUniqueness ==
    \A d \in Dests :
        \A r1, r2 \in dstRows[d] : r1.key = r2.key => r1 = r2

\* ACTION property: cursors never regress — "a clamp never REWINDS a
\* cursor". Strictly stronger than the base CursorMonotonicity state
\* invariant (>= 0). Holds because DestStart requires s > cursors[d] and
\* the cleanliness guard keeps a jump from landing under an in-flight
\* flush (whose FlushCommit would persist the older inflightThrough).
\* This is the obligation the implementation discharges with max-guards
\* when the race IS allowed (advance_cursor floor re-check, registration
\* max-merge) — any future refinement that admits in-flight clamps must
\* keep this property.
CursorNeverRewinds ==
    [][\A d \in Dests : cursors'[d] >= cursors[d]]_jvars

(***************************************************************************)
(* Specification                                                           *)
(***************************************************************************)

JInit ==
    /\ Init
    /\ joinSnap = [d \in Dests |-> 0]         \* startup destinations owe
                                              \* everything: epoch 0
    /\ missedDeleteKeys = [d \in Dests |-> {}]

\* Every base action imported unchanged; none touches the epoch or the
\* ghost set — in particular PauseDest (= C3 deactivate) leaves joinSnap
\* and cursors alone, which is the never-delete stop contract: membership
\* removal is not state removal.
JNext ==
    \/ (Next /\ UNCHANGED <<joinSnap, missedDeleteKeys>>)
    \/ \E d \in Dests, i \in Instances, s \in 1..srcSnap :
         DestStart(d, i, s)

JSpec == JInit /\ [][JNext]_jvars /\ WF_jvars(JNext)

====
