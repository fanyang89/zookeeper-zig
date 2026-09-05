# TLA+ specifications

Formal models of the correctness-critical replicated components, checked
with TLC bounded model checking. The specs abstract Raft into the totally
ordered log the state machine sees and model exactly the interleavings the
implementation permits, mirroring `src/server/rocks_store.zig` operation by
operation.

## SessionTracker

`SessionTracker.tla` / `MCSessionTracker.cfg` — the replicated session
tracker (roadmap Track 2 P0, highest risk). Actions: `Tick` (term-checked
session clock), `Open`, `Touch`, `Move` (reconnect generation+1 CAS),
`Close`, `Scan` (the reaper observes an expiry), `Expire` (the
`expected_expires_at_ms` compare-and-set).

Invariants:

- `NoPrematureExpiry` — a session removed by the reaper lived at least its
  full last renewal lifetime (`removedClock >= lastRenewClock + Timeout +
  Grace`); no scan/expire race kills a live session.
- `StaleGenerationNeverWins` / `GenerationFenced` — only the current
  generation of a session renews or closes it; a pre-`Move` connection can
  never affect the moved session.
- `NoResurrection` — once the clock passes a live session's expiry, the
  session is never renewed again; only the reaper or an explicit close end
  it.

Checked exhaustively (2,864 distinct states at `MaxClock=6, MaxGen=3,
Terms={1,2}`). Mutation-validated: removing the expire CAS, dropping the
touch generation guard, or allowing touch on expired sessions each violates
an invariant.

## ContainerCleanup

`ContainerCleanup.tla` / `MCContainerCleanup.cfg` — container and TTL
cleanup (Track 2 P0). The leader scans expired empty extended nodes and
proposes `delete_extended(path, expected_czxid, expected_kind)`; the apply
is a compare-and-set against the node's current incarnation.

Invariants:

- `CleanupRemovesOnlyScannedIncarnation` — every applied cleanup removed
  exactly the incarnation it scanned; a stale cleanup never deletes a
  recreated node (the czxid fence).
- `CleanupNeverRemovesLiveChildfulNode` — cleanups only remove empty
  nodes.
- `CzxidStrictlyIncreases` — every incarnation of a path gets a strictly
  larger czxid, which makes the fence value unique.

Checked exhaustively (3,526 distinct states at `MaxClock=5, MaxCzxid=4`).
Mutation-validated: dropping the czxid compare or the empty check each
violates an invariant.

## MultiAtomicity

`MultiAtomicity.tla` / `MCMultiAtomicity.cfg` — isolation and atomic commit
of ZooKeeper `multi`. Each operation evaluates against a private workspace;
reads may interleave between operation evaluations but can access only the
committed store. A successful transaction publishes the complete workspace,
while a failed operation publishes none of it.

Invariants:

- `ReadIsolation` — an interleaved read observes the committed snapshot,
  never a partially evaluated workspace.
- `FailedMultiNoEffect` — a failed multi leaves values and versions exactly
  at their initial state.
- `SuccessfulMultiAllEffects` — a successful multi publishes the sequential
  effect of both operations, including version increments.
- `CommittedStateNeverUsesWorkspace` — no intermediate evaluation changes
  committed state.

The bounded transaction set covers distinct-node success, failure after one
successful operation, and same-node sequential version dependence. TLC
checks all 38 distinct states. Mutation validation catches early commit,
workspace reads, and partial publication on failure.

## LinearizableRead

`LinearizableRead.tla` / `MCLinearizableRead.cfg` — the
`Quorum.linearizableRead` and `raftz.readIndex` path. It models commit/apply
lag, quorum confirmation, leader changes, callback completion, and writes
before and during a read.

Invariants:

- `ReadIndexCoversInvocation` — the quorum-confirmed index includes every
  write committed before invocation.
- `CompletedAfterApply` — success is delayed until the local state machine
  applies through the confirmed index.
- `CompletedReadReflectsAppliedPrefix` / `CompletedReadIsLinearizable` —
  the returned value belongs to an applied log prefix no earlier than the
  invocation boundary.
- `PendingReadFencedByLeadership` / `SuccessBelongsToRequestTerm` — a read
  cannot cross a leader epoch and then report success.

TLC checks all 138,284 distinct states at `MaxIndex=4, MaxTerm=3`.
Mutation validation catches a missing apply barrier, failure to cancel reads
on leadership loss, and an index chosen from stale applied state.

## Running

```sh
mise run tla-check   # or: bash tools/tla-check.sh
```

The script pins `tla2tools` 1.7.0 by SHA-256, caches it under
`~/.cache/zookeeper-zig`, and runs TLC for all four configurations. Bounded
state budgets are set by the constants in the `.cfg` files; all runs complete
exhaustively in seconds on a laptop.

## Verification boundary

These specifications do **not** verify the external `raftz` dependency or
provide a refinement proof from the standard Raft TLA+ specification.
Instead, the models explicitly assume that `raftz` provides:

- a totally ordered, gap-free committed log whose entries are applied once
  and in index order;
- leader completeness and quorum safety across terms;
- a successful read-index that was quorum-confirmed in the requesting
  leadership epoch;
- read-index callback completion only after the local state machine has
  applied through the confirmed index;
- failure of pending read-index requests when leadership is lost.

`LinearizableRead.tla` verifies the ZooKeeper-visible consequences of those
read-index guarantees, not their implementation inside `raftz`. The relevant
implementation boundary is `raftz`'s `ReadyProcessor`: it records ready read
indexes, calls `completeReadyReads(applied_index)` after applying committed
entries, and detaches pending callbacks on leadership loss.

The state-machine specs verify guards, CAS fencing, atomic publication, and
the replicated clock after that boundary. They additionally assume
server-generated session passwords are unique per incarnation, which fences
cross-incarnation confusion together with generations. Mixed Java/Zig Raft
ensembles remain outside the compatibility and verification boundary.
