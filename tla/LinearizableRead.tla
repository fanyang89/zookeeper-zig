----------------------------- MODULE LinearizableRead -----------------------------
EXTENDS Integers, Naturals, Sequences
(***************************************************************************)
(* Formal specification of Quorum.linearizableRead and raftz read-index.   *)
(*                                                                         *)
(* A leader accepts a read, confirms a read index with a quorum in the      *)
(* request's leadership term, and delays the success callback until the    *)
(* local state machine has applied through that index. raftz detaches and   *)
(* fails pending reads when the node loses leadership.                     *)
(*                                                                         *)
(* The log is abstracted as a sequence of register values. Index zero is    *)
(* the initial state. A completed read may observe a later applied value,   *)
(* but it must include every write committed before the read invocation.    *)
(***************************************************************************)

CONSTANTS Values, MaxIndex, MaxTerm

LogType == UNION {[1..length -> Values] : length \in 1..MaxIndex + 1}

VARIABLES role,
          term,
          log,
          commitIndex,
          appliedIndex,
          readPhase,
          requestTerm,
          invocationCommit,
          readIndex,
          resultValue,
          resultIndex,
          resultTerm

vars == <<role, term, log, commitIndex, appliedIndex, readPhase,
          requestTerm, invocationCommit, readIndex, resultValue,
          resultIndex, resultTerm>>

TypeOK ==
    /\ role \in {"leader", "follower"}
    /\ term \in 1..MaxTerm
    /\ log \in LogType
    /\ commitIndex \in 0..MaxIndex
    /\ appliedIndex \in 0..commitIndex
    /\ Len(log) = commitIndex + 1
    /\ readPhase \in {"idle", "pending", "confirmed", "done", "failed"}
    /\ requestTerm \in 1..MaxTerm
    /\ invocationCommit \in 0..MaxIndex
    /\ readIndex \in 0..MaxIndex
    /\ resultValue \in Values
    /\ resultIndex \in 0..MaxIndex
    /\ resultTerm \in 1..MaxTerm

CommitWrite(value) ==
    /\ role = "leader"
    /\ commitIndex < MaxIndex
    /\ value \in Values
    /\ log' = Append(log, value)
    /\ commitIndex' = commitIndex + 1
    /\ UNCHANGED <<role, term, appliedIndex, readPhase, requestTerm,
                    invocationCommit, readIndex, resultValue, resultIndex,
                    resultTerm>>

ApplyCommitted ==
    /\ appliedIndex < commitIndex
    /\ appliedIndex' = appliedIndex + 1
    /\ UNCHANGED <<role, term, log, commitIndex, readPhase, requestTerm,
                    invocationCommit, readIndex, resultValue, resultIndex,
                    resultTerm>>

StartRead ==
    /\ role = "leader"
    /\ readPhase = "idle"
    /\ readPhase' = "pending"
    /\ requestTerm' = term
    /\ invocationCommit' = commitIndex
    /\ UNCHANGED <<role, term, log, commitIndex, appliedIndex, readIndex,
                    resultValue, resultIndex, resultTerm>>

ConfirmReadIndex ==
    /\ readPhase = "pending"
    /\ role = "leader"
    /\ term = requestTerm
    /\ readPhase' = "confirmed"
    /\ readIndex' = commitIndex
    /\ UNCHANGED <<role, term, log, commitIndex, appliedIndex, requestTerm,
                    invocationCommit, resultValue, resultIndex, resultTerm>>

CompleteRead ==
    /\ readPhase = "confirmed"
    /\ role = "leader"
    /\ term = requestTerm
    /\ appliedIndex >= readIndex
    /\ readPhase' = "done"
    /\ resultValue' = log[appliedIndex + 1]
    /\ resultIndex' = appliedIndex
    /\ resultTerm' = term
    /\ UNCHANGED <<role, term, log, commitIndex, appliedIndex, requestTerm,
                    invocationCommit, readIndex>>

LoseLeadership ==
    /\ role = "leader"
    /\ term < MaxTerm
    /\ role' = "follower"
    /\ term' = term + 1
    /\ readPhase' = IF readPhase \in {"pending", "confirmed"}
                     THEN "failed"
                     ELSE readPhase
    /\ UNCHANGED <<log, commitIndex, appliedIndex, requestTerm,
                    invocationCommit, readIndex, resultValue, resultIndex,
                    resultTerm>>

BecomeLeader ==
    /\ role = "follower"
    /\ role' = "leader"
    /\ UNCHANGED <<term, log, commitIndex, appliedIndex, readPhase,
                    requestTerm, invocationCommit, readIndex, resultValue,
                    resultIndex, resultTerm>>

ResetRead ==
    /\ readPhase \in {"done", "failed"}
    /\ readPhase' = "idle"
    /\ UNCHANGED <<role, term, log, commitIndex, appliedIndex, requestTerm,
                    invocationCommit, readIndex, resultValue, resultIndex,
                    resultTerm>>

DoNothing == UNCHANGED vars

Init ==
    /\ role = "leader"
    /\ term = 1
    /\ log = <<CHOOSE value \in Values : TRUE>>
    /\ commitIndex = 0
    /\ appliedIndex = 0
    /\ readPhase = "idle"
    /\ requestTerm = 1
    /\ invocationCommit = 0
    /\ readIndex = 0
    /\ resultValue = log[1]
    /\ resultIndex = 0
    /\ resultTerm = 1

Next ==
    \/ \E value \in Values : CommitWrite(value)
    \/ ApplyCommitted
    \/ StartRead
    \/ ConfirmReadIndex
    \/ CompleteRead
    \/ LoseLeadership
    \/ BecomeLeader
    \/ ResetRead
    \/ DoNothing

ReadIndexCoversInvocation ==
    readPhase \in {"confirmed", "done"} => readIndex >= invocationCommit

CompletedAfterApply ==
    readPhase = "done" => resultIndex >= readIndex

CompletedReadReflectsAppliedPrefix ==
    readPhase = "done" => resultValue = log[resultIndex + 1]

CompletedReadIsLinearizable ==
    readPhase = "done" => resultIndex >= invocationCommit

PendingReadFencedByLeadership ==
    readPhase \in {"pending", "confirmed"} =>
        /\ role = "leader"
        /\ term = requestTerm

SuccessBelongsToRequestTerm ==
    readPhase = "done" => resultTerm = requestTerm

Spec == Init /\ [][Next]_vars

=============================================================================
