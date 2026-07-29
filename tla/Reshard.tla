---- MODULE Reshard ----
(***************************************************************************)
(* The reshard/config-swap quiesce — the ONE surviving quiesce in C3       *)
(* (viaduck_c3_dynamic_apply.md v6, sections 4 and 7).                     *)
(*                                                                         *)
(* Reconciler rule 2: running destination, startable, config differs ->    *)
(* deactivate -> pending-restart until is_clean -> activate with the new   *)
(* config. The property that gate buys: once the swap has completed, no    *)
(* flush ever writes to the OLD endpoint. The hazard is that a flush       *)
(* worker captures its endpoint (connection/config) at FlushStart and      *)
(* writes at FlushCommit — if the swap lands between the two, the commit   *)
(* goes to an endpoint the config no longer names (post-reshard: a dead    *)
(* or, worse, recycled catalog).                                           *)
(*                                                                         *)
(* Deliberately a SEPARATE tiny model, not a Viaduck.tla addition: the     *)
(* design pins "no additions to the 85M-state main run", and the property  *)
(* needs none of the CDC/conflict machinery — one destination, endpoint    *)
(* capture, and the is_clean gate are the whole story. Buffer contents     *)
(* are abstracted to a flag and cursor arithmetic to three integers; the   *)
(* Read/FlushStart/FlushCommit/FlushFail/Discard skeleton mirrors the      *)
(* base model's BufferRead/FlushStart/FlushCommit/FlushFail/PauseDest      *)
(* (including the position-restore max() and the drop-buffer failure       *)
(* path) so the gate is checked against the real flush lifecycle, not a   *)
(* strawman.                                                               *)
(*                                                                         *)
(* NEGATIVE TEST — the guard is load-bearing, not decorative: the          *)
(* GuardedSwap constant flips the is_clean gate off. Run with              *)
(*   Reshard.cfg          (GuardedSwap TRUE):  NoStaleEndpointWrite holds  *)
(*                        over the full state space;                       *)
(*   ReshardUnguarded.cfg (GuardedSwap FALSE): TLC finds the violation.    *)
(* The unguarded counterexample TLC produces is the minimal 5-step race:   *)
(*   SrcAdvance -> Read -> FlushStart (captures "old", carrying rows)      *)
(*   -> ConfigSwap (in-flight flush ignored) -> FlushCommit (writes the   *)
(*   captured "old" endpoint with endpoint = "new") => staleWrite.         *)
(* One swap direction (old -> new) suffices: repeated reshards are         *)
(* independent instances of the same race, adding states but no new        *)
(* behavior class.                                                         *)
(***************************************************************************)

EXTENDS Integers, TLC

CONSTANTS
    MaxSnaps,       \* bound on source snapshot advancement
    GuardedSwap     \* TRUE: ConfigSwap requires is_clean (the C3 design).
                    \* FALSE: the negative test — swap at any time.

VARIABLES
    srcSnap,        \* source snapshot (monotonic, bounded)
    endpoint,       \* the destination endpoint the CURRENT config names:
                    \* "old" before the reshard cutover, "new" after
    buffered,       \* BOOLEAN: destination buffer non-empty (abstracts
                    \* buffer contents — bytes/rows don't matter here)
    position,       \* in-memory read position (base bufferedThrough)
    flushed,        \* durable cursor (base cursors[d])
    flushing,       \* BOOLEAN: a flush is in flight (in-flight guard)
    flushEp,        \* endpoint captured at FlushStart — THE variable this
                    \* model exists for: the write lands here, not on
                    \* whatever endpoint is current at commit time
    flushHasRows,   \* BOOLEAN: the in-flight flush carries data. A
                    \* position-only flush (empty buffer, cursor persist)
                    \* writes the metadata store, NOT the endpoint, so it
                    \* cannot produce a stale ENDPOINT write
    flushThrough,   \* cursor value to persist if the flush commits
    staleWrite      \* GHOST history flag: set iff a data-carrying commit
                    \* ever landed on "old" after the swap completed.
                    \* Needed because "after the swap" is a history
                    \* condition, not a state predicate.

vars == <<srcSnap, endpoint, buffered, position, flushed,
          flushing, flushEp, flushHasRows, flushThrough, staleWrite>>

\* delivery.is_clean: nothing in flight, buffer empty, read position not
\* ahead of the durable cursor — the pending-restart drain-complete gate.
IsClean ==
    /\ ~flushing
    /\ ~buffered
    /\ position <= flushed

Init ==
    /\ srcSnap = 0
    /\ endpoint = "old"
    /\ buffered = FALSE
    /\ position = 0
    /\ flushed = 0
    /\ flushing = FALSE
    /\ flushEp = "old"        \* only read while flushing; reset value
    /\ flushHasRows = FALSE
    /\ flushThrough = 0
    /\ staleWrite = FALSE

\* Source makes progress (abstracts SrcInsert/Update/Delete — content is
\* irrelevant to the endpoint property, only "there is something to read").
SrcAdvance ==
    /\ srcSnap < MaxSnaps
    /\ srcSnap' = srcSnap + 1
    /\ UNCHANGED <<endpoint, buffered, position, flushed, flushing,
                   flushEp, flushHasRows, flushThrough, staleWrite>>

\* CDC read into the buffer (base BufferRead): advance the read position,
\* buffer becomes non-empty. Permitted during an in-flight flush (the
\* buffer swap), exactly as in the base model.
Read ==
    /\ position < srcSnap
    /\ position' = srcSnap
    /\ buffered' = TRUE
    /\ UNCHANGED <<srcSnap, endpoint, flushed, flushing, flushEp,
                   flushHasRows, flushThrough, staleWrite>>

\* Start a flush: capture the CURRENT endpoint and the buffer, swap the
\* buffer out. The endpoint capture is the implementation's pool-resolved
\* connection: resolved once per flush, used at commit. Also fires for
\* empty buffers with position ahead of the cursor (the lazy idle persist)
\* — those flushes capture the endpoint too but write nothing to it.
FlushStart ==
    /\ ~flushing
    /\ buffered \/ position > flushed
    /\ flushing' = TRUE
    /\ flushEp' = endpoint
    /\ flushHasRows' = buffered
    /\ flushThrough' = position
    /\ buffered' = FALSE
    /\ UNCHANGED <<srcSnap, endpoint, position, flushed, staleWrite>>

\* Commit: the data write (if any) lands on the CAPTURED endpoint. The
\* stale-write ghost fires iff that capture is "old" while the completed
\* swap has made "new" current — the exact hazard NoStaleEndpointWrite
\* forbids. Position-restore max() mirrors the base FlushCommit (a
\* Discard racing this flush rewound position; the flush succeeded, so
\* leaving position behind would re-read a committed range).
FlushCommit ==
    /\ flushing
    /\ flushed' = flushThrough
    /\ position' = IF position < flushThrough THEN flushThrough ELSE position
    /\ staleWrite' = (staleWrite \/ (flushHasRows /\ flushEp = "old"
                                                  /\ endpoint = "new"))
    /\ flushing' = FALSE
    /\ flushEp' = "old"           \* reset (only read while flushing)
    /\ flushHasRows' = FALSE
    /\ flushThrough' = 0
    /\ UNCHANGED <<srcSnap, endpoint, buffered>>

\* Flush fails: destination rollback — nothing landed on any endpoint —
\* plus the base model's drop-buffer recovery (discard live buffer, rewind
\* position to the durable cursor; next reads re-cover).
FlushFail ==
    /\ flushing
    /\ flushing' = FALSE
    /\ flushEp' = "old"
    /\ flushHasRows' = FALSE
    /\ flushThrough' = 0
    /\ buffered' = FALSE
    /\ position' = flushed
    /\ UNCHANGED <<srcSnap, endpoint, flushed, staleWrite>>

\* Deactivate's discard (base PauseDest / delivery.discard_buffer): drop
\* the live buffer, rewind the read position; an in-flight flush is
\* PRESERVED and finishes naturally. This is what makes the is_clean wait
\* reachable — and what makes the unguarded race real: the swap follows a
\* deactivate whose zombie flush is still holding the old endpoint.
Discard ==
    /\ buffered \/ position > flushed
    /\ buffered' = FALSE
    /\ position' = flushed
    /\ UNCHANGED <<srcSnap, endpoint, flushed, flushing, flushEp,
                   flushHasRows, flushThrough, staleWrite>>

\* The config swap: activate with the changed endpoint. Guarded on
\* is_clean — no in-flight flush, empty buffer, position drained to the
\* cursor — which is the pending-restart gate (rule 2). With the guard, no
\* capture of "old" can survive to commit after the swap; without it
\* (GuardedSwap = FALSE), an in-flight flush's captured endpoint outlives
\* the config change and FlushCommit produces the stale write.
ConfigSwap ==
    /\ endpoint = "old"
    /\ GuardedSwap => IsClean
    /\ endpoint' = "new"
    /\ UNCHANGED <<srcSnap, buffered, position, flushed, flushing,
                   flushEp, flushHasRows, flushThrough, staleWrite>>

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* THE property: no data-carrying commit ever writes the old endpoint
\* after the swap completed. Holds with GuardedSwap = TRUE; violated (by
\* the 5-step trace in the header) with GuardedSwap = FALSE.
NoStaleEndpointWrite == ~staleWrite

\* Sanity: cursor arithmetic stays well-formed (base BufferPositionBound).
PositionBound ==
    /\ flushed <= position
    /\ position <= srcSnap

\* Sanity: no flush in flight => no in-flight residue (base
\* FlushStateConsistency).
FlushStateOK ==
    ~flushing => (flushThrough = 0 /\ ~flushHasRows)

Next ==
    \/ SrcAdvance
    \/ Read
    \/ FlushStart
    \/ FlushCommit
    \/ FlushFail
    \/ Discard
    \/ ConfigSwap

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

====
