---- MODULE ViaduckReads ----
(***************************************************************************)
(* The per-destination in-flight read guard — M4's parallel reader pool    *)
(* (log-consumer-proposal.md §6.3).                                        *)
(*                                                                         *)
(* WHY A SEPARATE MODEL: the base spec's BufferRead is atomic — one        *)
(* destination can never have two reads outstanding, so the hazard this    *)
(* module checks is inexpressible there. The M4 read loop dispatches       *)
(* range reads to a pool: a read is dispatch (capture position/epoch) then *)
(* completion (append buffer entries + stamp position). Without a          *)
(* per-destination in-flight guard, a slow read overlaps a fast one for    *)
(* the same destination, and epochs do NOT save you: the epoch machinery   *)
(* only invalidates reads that raced a flush-failure/pause RESET — two     *)
(* overlapping reads with no intervening reset both land, the older        *)
(* stamping a stale position over the newer and appending an out-of-order  *)
(* entry (coverage chain breaks; a prefix flush can then persist the       *)
(* cursor past undelivered rows — the loss mode EntryCoverageInvariant     *)
(* guards in the base model).                                              *)
(*                                                                         *)
(* Abstraction: the buffer chain is a sequence of coverage watermarks      *)
(* (ints), reads are [hi, ep] records in a dispatch set. FlushStep and     *)
(* ResetEvent stand in for the base model's FlushCommit / FlushFail /      *)
(* PauseDest — only their cursor/order effects matter here.                *)
(*                                                                         *)
(* NEGATIVE TEST — the guard is load-bearing: GuardedReads FALSE must      *)
(* produce the overlap counterexample (dispatch R1 (hi=2); SrcAdvance;     *)
(* dispatch R2 unguarded (hi=4); R2 completes — chain <4>, position=4;     *)
(* R1 completes fresh — chain <4, 2>, position regresses to 2).            *)
(* Pattern per tla/Reshard.tla's guarded/unguarded pair.                   *)
(*                                                                         *)
(* GOVERNANCE (AGENT.md spec-first): this module witnesses the M4 reader   *)
(* pool's per-destination in-flight read guard before its implementation.  *)
(***************************************************************************)

EXTENDS Integers, Sequences, TLC

CONSTANTS
    MaxSnaps,       \* bound on source snapshot advancement
    MaxResets,      \* bound on ResetEvent firings (the epoch is unbounded
                    \* otherwise — every reset cycle is a new state and TLC
                    \* never terminates; same trick as the base model's
                    \* MaxOps). The witness needs a single reset.
    GuardedReads    \* TRUE: one in-flight read per destination (the design).
                    \* FALSE: the negative test — dispatch always allowed.

VARIABLES
    srcSnap,        \* source snapshot (monotonic, bounded)
    position,       \* in-memory read position (base bufferedThrough)
    flushed,        \* durable cursor (base cursors[d])
    chain,          \* SEQUENCE of entry coverage watermarks (the cov chain)
    reads,          \* set of in-flight reads [hi, ep] for THIS destination
    epoch,          \* current read epoch (bumps invalidate racing reads)
    epochBumped     \* GHOST: a reset happened — lets the invariant require
                    \* order only across epochs (a reset legitimately
                    \* restarts the chain from the cursor)

vars == <<srcSnap, position, flushed, chain, reads, epoch, epochBumped>>

Init ==
    /\ srcSnap = 0
    /\ position = 0
    /\ flushed = 0
    /\ chain = <<>>
    /\ reads = {}
    /\ epoch = 0
    /\ epochBumped = FALSE

\* Source makes progress.
SrcAdvance ==
    /\ srcSnap < MaxSnaps
    /\ srcSnap' = srcSnap + 1
    /\ UNCHANGED <<position, flushed, chain, reads, epoch, epochBumped>>

\* Dispatch a read of (position, srcSnap]. The base model's atomic
\* BufferRead is exactly "dispatch and complete in one step"; M4 splits it.
\* THE GUARD: at most one outstanding read per destination.
ReadDispatch ==
    /\ position < srcSnap
    /\ GuardedReads => reads = {}
    /\ reads' = reads \cup {[hi |-> srcSnap, ep |-> epoch]}
    /\ UNCHANGED <<srcSnap, position, flushed, chain, epoch, epochBumped>>

\* A fresh read lands: append its entry (cov = the read's hi) and stamp the
\* position. The implementation's buffer() stamps unconditionally — this
\* action is exactly that stamp, and out-of-order completion is the hazard.
ReadCompleteFresh(r) ==
    /\ r \in reads
    /\ r.ep = epoch                    \* epoch guard: raced-a-reset reads
    /\ chain' = Append(chain, r.hi)    \*        discard instead (below)
    /\ position' = r.hi
    /\ reads' = reads \ {r}
    /\ UNCHANGED <<srcSnap, flushed, epoch, epochBumped>>

\* A read that raced a reset is discarded by the epoch check: it never
\* stamps. (delivery.py's buffer()/advance_position() epoch guard.)
ReadCompleteStale(r) ==
    /\ r \in reads
    /\ r.ep /= epoch
    /\ reads' = reads \ {r}
    /\ UNCHANGED <<srcSnap, position, flushed, chain, epoch, epochBumped>>

\* Flush commits the whole chain: cursor takes the tail coverage, chain
\* drains. (Base FlushCommit full-swap; prefix commits are the base model's
\* business, this module checks read ordering.)
FlushStep ==
    /\ chain /= <<>>
    /\ flushed' = chain[Len(chain)]
    /\ chain' = <<>>
    /\ UNCHANGED <<srcSnap, position, reads, epoch, epochBumped>>

\* Flush-failure / pause / crash abstraction: drop the chain, rewind the
\* position to the cursor, and bump the epoch — racing reads discard.
ResetEvent ==
    /\ chain /= <<>> \/ position > flushed
    /\ epoch < MaxResets
    /\ chain' = <<>>
    /\ position' = flushed
    /\ epoch' = epoch + 1
    /\ epochBumped' = TRUE
    /\ reads' = reads  \* in-flight reads land stale later (epoch mismatch)
    /\ UNCHANGED <<srcSnap, flushed>>

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* Read ordering safety: the coverage chain is non-decreasing and never
\* runs ahead of the read position; the position never falls behind the
\* cursor. epochBumped carves out the reset boundary (a restart's chain
\* legitimately begins again at the cursor — within one epoch the chain is
\* what ResetEvent rebuilt, and a stale completion can't sneak in).
ReadOrderSafety ==
    /\ flushed <= position
    /\ \A i \in 1..Len(chain) :
         /\ chain[i] <= position
         /\ i < Len(chain) => chain[i] <= chain[i + 1]

Next ==
    \/ SrcAdvance
    \/ ReadDispatch
    \/ \E r \in reads : ReadCompleteFresh(r)
    \/ \E r \in reads : ReadCompleteStale(r)
    \/ FlushStep
    \/ ResetEvent

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

====
