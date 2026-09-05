------------------------------- MODULE MultiAtomicity -------------------------------
EXTENDS Integers, Naturals
(***************************************************************************)
(* Formal specification of ZooKeeper multi transaction atomicity.         *)
(*                                                                         *)
(* src/server/rocks_store.zig evaluates every operation against an         *)
(* isolated MultiState workspace. A failed operation discards that         *)
(* workspace and only advances the applied Raft index. A successful multi  *)
(* publishes all dirty/deleted nodes in one RocksDB WriteBatch.            *)
(*                                                                         *)
(* This model deliberately splits workspace evaluation into individual    *)
(* actions so reads can interleave with partially evaluated transactions.  *)
(* Reads must still observe the committed store, never the workspace.       *)
(*                                                                         *)
(* The bounded transaction set covers:                                    *)
(*   - two successful writes to distinct nodes;                            *)
(*   - a successful first write followed by a failed version check;       *)
(*   - two successful writes to one node where operation two observes the *)
(*     version produced by operation one.                                 *)
(***************************************************************************)

CONSTANTS Nodes, A, B, Values, MaxVersion

StoreType == [Nodes -> Values]
VersionType == [Nodes -> 0..MaxVersion]
OperationSet ==
    {<<"set", n, value, expected>> :
        n \in Nodes,
        value \in Values,
        expected \in -1..MaxVersion}
OperationPairType == OperationSet \X OperationSet

InitialStore == [n \in Nodes |-> 0]
InitialVersions == [n \in Nodes |-> 0]

SuccessDistinct ==
    <<<<"set", A, 1, 0>>, <<"set", B, 1, 0>>>>
FailAfterFirst ==
    <<<<"set", A, 1, 0>>, <<"set", B, 1, 1>>>>
SuccessSequential ==
    <<<<"set", A, 1, 0>>, <<"set", A, 0, 1>>>>
Transactions == {SuccessDistinct, FailAfterFirst, SuccessSequential}

VARIABLES store,
          versions,
          phase,
          workspace,
          workspaceVersions,
          operations,
          nextOperation,
          readValid,
          readNode,
          readValue,
          readSource

vars == <<store, versions, phase, workspace, workspaceVersions, operations,
          nextOperation, readValid, readNode, readValue, readSource>>

OpNode(op) == op[2]
OpValue(op) == op[3]
OpExpected(op) == op[4]

ValidOperation(op, currentVersions) ==
    /\ currentVersions[OpNode(op)] < MaxVersion
    /\ (OpExpected(op) = -1 \/ OpExpected(op) = currentVersions[OpNode(op)])

ApplyValue(current, op) ==
    [current EXCEPT ![OpNode(op)] = OpValue(op)]

ApplyVersion(current, op) ==
    [current EXCEPT ![OpNode(op)] = @ + 1]

PairValues(initial, pair) ==
    ApplyValue(ApplyValue(initial, pair[1]), pair[2])

PairVersions(initial, pair) ==
    ApplyVersion(ApplyVersion(initial, pair[1]), pair[2])

TypeOK ==
    /\ A \in Nodes
    /\ B \in Nodes
    /\ A # B
    /\ 0 \in Values
    /\ 1 \in Values
    /\ MaxVersion >= 2
    /\ store \in StoreType
    /\ versions \in VersionType
    /\ phase \in {"idle", "running", "ready", "done_success", "done_failure"}
    /\ workspace \in StoreType
    /\ workspaceVersions \in VersionType
    /\ operations \in OperationPairType
    /\ nextOperation \in 1..3
    /\ readValid \in BOOLEAN
    /\ readNode \in Nodes
    /\ readValue \in Values
    /\ readSource \in StoreType

Begin(pair) ==
    /\ phase = "idle"
    /\ pair \in Transactions
    /\ phase' = "running"
    /\ workspace' = store
    /\ workspaceVersions' = versions
    /\ operations' = pair
    /\ nextOperation' = 1
    /\ UNCHANGED <<store, versions, readValid, readNode, readValue,
                    readSource>>

EvaluateOperation ==
    /\ phase = "running"
    /\ nextOperation <= 2
    /\ LET op == operations[nextOperation]
       IN IF ValidOperation(op, workspaceVersions)
          THEN /\ workspace' = ApplyValue(workspace, op)
               /\ workspaceVersions' = ApplyVersion(workspaceVersions, op)
               /\ nextOperation' = nextOperation + 1
               /\ phase' = IF nextOperation = 2 THEN "ready" ELSE "running"
          ELSE /\ phase' = "done_failure"
               /\ UNCHANGED <<workspace, workspaceVersions, nextOperation>>
    /\ UNCHANGED <<store, versions, operations, readValid, readNode,
                    readValue, readSource>>

Commit ==
    /\ phase = "ready"
    /\ phase' = "done_success"
    /\ store' = workspace
    /\ versions' = workspaceVersions
    /\ UNCHANGED <<workspace, workspaceVersions, operations, nextOperation,
                    readValid, readNode, readValue, readSource>>

Read(n) ==
    /\ n \in Nodes
    /\ readValid' = TRUE
    /\ readNode' = n
    /\ readValue' = store[n]
    /\ readSource' = store
    /\ UNCHANGED <<store, versions, phase, workspace, workspaceVersions,
                    operations, nextOperation>>

DoNothing == UNCHANGED vars

Init ==
    /\ store = InitialStore
    /\ versions = InitialVersions
    /\ phase = "idle"
    /\ workspace = store
    /\ workspaceVersions = versions
    /\ operations = SuccessDistinct
    /\ nextOperation = 1
    /\ readValid = FALSE
    /\ readNode = A
    /\ readValue = store[readNode]
    /\ readSource = store

Next ==
    \/ \E pair \in Transactions : Begin(pair)
    \/ EvaluateOperation
    \/ Commit
    \/ \E n \in Nodes : Read(n)
    \/ DoNothing

ReadIsolation ==
    ~readValid \/ readValue = readSource[readNode]

FailedMultiNoEffect ==
    phase = "done_failure" =>
        /\ store = InitialStore
        /\ versions = InitialVersions

SuccessfulMultiAllEffects ==
    phase = "done_success" =>
        /\ store = PairValues(InitialStore, operations)
        /\ versions = PairVersions(InitialVersions, operations)

CommittedStateNeverUsesWorkspace ==
    phase \in {"running", "ready"} =>
        /\ store = InitialStore
        /\ versions = InitialVersions

Spec == Init /\ [][Next]_vars

=============================================================================
