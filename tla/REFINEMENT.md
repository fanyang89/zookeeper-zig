# Refinement mapping

This document maps each TLA+ model to the concrete Zig state and transition
that it abstracts. It is an engineering refinement argument, not a
mechanically checked refinement proof. TLC proves the model invariants; code
review, unit tests, mutation tests, and this mapping establish that the model
continues to represent the implementation.

## Common boundary

A state-machine transition starts when `raftz` calls
`ZooKeeperStateMachine.applyImpl` in `src/server/state_machine.zig` with one
committed log entry. `command.decode` selects the concrete operation and
`RocksStore.apply` performs it while the state-machine mutex is held.

The models erase serialization, allocation, RocksDB handles, response bytes,
ACL payloads, and retry mechanics. A successful RocksDB `WriteBatch` is one
abstract publication step. Rejected commands that only update the durable
applied index refine to stuttering with respect to the modeled ZooKeeper
state.

The external Raft assumptions are listed in [`README.md`](README.md). In
particular, the mapping assumes committed entries are applied once and in
index order. It does not prove those properties of `raftz`.

## SessionTracker

Model: [`SessionTracker.tla`](SessionTracker.tla)

| Abstract state/action | Concrete representation |
| --- | --- |
| `clock` | `RocksStore.session_clock_ms`, persisted under `session_clock_key` |
| `term` | `entry.term` passed to `RocksStore.apply` |
| `Present(gen, exp)` | encoded `Session` record: `generation`, `expires_at_ms` |
| `Absent` | missing `sessionKey(session_id)` RocksDB entry |
| `scan` | one `ExpiredSession.expires_at_ms` returned by `expiredSessions` and copied into an `expire_session` command |
| `Tick(t)` | `sessionTick(leader_term, ..., term)`; advances only when `leader_term == term` |
| `Open(g)` | `openSession`; rejects an existing id and stores the initial renewal expiry |
| `Touch(g)` | `touchSession`; password, generation, and `expires_at_ms > session_clock_ms` guards precede `commitSession` |
| `Move(g)` | `moveSession`; checks `new_generation == expected_generation + 1`, current generation, password, and expiry |
| `Close(g)` | `closeSession`; generation/password fenced removal through `commitSessionRemoval` |
| `Scan` | `RocksStore.expiredSessions` |
| `Expire` | `expireSession`; compares `expected_expires_at_ms` and the current replicated clock before removal |

`lastRenewClock`, `lastRenewGen`, `maxGen`, `removal`, `removedClock`, and
`expiredAt` are ghost variables used only to state invariants. They have no
persisted counterpart.

Password uniqueness across session incarnations is an explicit assumption;
the model covers generation fencing within one incarnation.

## ContainerCleanup

Model: [`ContainerCleanup.tla`](ContainerCleanup.tla)

| Abstract state/action | Concrete representation |
| --- | --- |
| `clock` | `RocksStore.maintenance_clock_ms`, advanced and persisted by `sessionTick` |
| `node` | one `Node` at a path: `czxid`, `kind`, `child_count`, and TTL/container eligibility fields |
| `next_czxid` | monotonic client zxid derived from the committed Raft index |
| `scan` | one `ExtendedCandidate` from `expiredExtendedNodes` |
| `Create` / `Delete` / child actions | normal data-tree mutations at the same path |
| `ScanCleanup` | `expiredExtendedNodes`, which returns path, `czxid`, and `kind` only for eligible empty nodes |
| `ApplyCleanup` | `deleteExtended(path, expected_czxid, expected_kind)` |

`victim` and `lastScan` are ghost variables recording what an abstract
cleanup removed. A failed compare-and-set that only advances the applied
index is a stuttering cleanup transition.

## MultiAtomicity

Model: [`MultiAtomicity.tla`](MultiAtomicity.tla)

| Abstract state/action | Concrete representation |
| --- | --- |
| `store`, `versions` | persisted node values and `Node.version` fields |
| `workspace`, `workspaceVersions` | `RocksStore.MultiState.nodes`, plus its `dirty` and `deleted` sets |
| `Begin` | `MultiState.load(self)` before request iteration |
| `EvaluateOperation` | `MultiState.applyOperation`, including reads through `MultiState.getNode` so later operations observe earlier workspace changes |
| `Commit` | `MultiState.commit`; one RocksDB `WriteBatch` contains every delete, dirty node, and the applied index |
| `Read` | ordinary store reads; they never receive a `MultiState` pointer |
| `done_failure` | the early error path in `applyMulti`, which discards `MultiState` and calls only `advanceApplied` |

The three bounded transaction pairs represent the equivalence classes needed
for atomicity: independent successful updates, failure after a successful
prefix, and same-node sequential dependence. Create/delete/ACL details are
abstracted to value/version updates because publication uses the same dirty
and deleted workspace sets.

## LinearizableRead

Model: [`LinearizableRead.tla`](LinearizableRead.tla)

| Abstract state/action | Concrete representation |
| --- | --- |
| `log`, `commitIndex` | the committed `raftz` log prefix |
| `appliedIndex` | `raftz.ReadyProcessor.applied_index` |
| `role`, `term` | `raftz` soft state and current term |
| `StartRead` | `Quorum.linearizableRead` calling `Raftor.readIndex` and `ProposalTracker.trackRead` |
| `ConfirmReadIndex` | a `ReadState` processed by `markReadReady(request_ctx, index)` |
| `ApplyCommitted` | `ReadyProcessor.applyCommittedEntries` / `applyEntry` |
| `CompleteRead` | `ProposalTracker.completeReadyReads(applied_index)` invoking the waiter callback |
| `LoseLeadership` | `ReadyProcessor.checkLeadershipChange`; `detachAll` fails reads with `LostLeadership` |
| register read value | the state-machine read performed after `linearizableRead` returns |

`invocationCommit`, `resultIndex`, and `resultTerm` are ghost history used to
state real-time and leadership-epoch constraints. The model verifies the
ZooKeeper-visible consequence of the read-index contract; the quorum
confirmation algorithm itself remains inside the unverified `raftz`
boundary.

## WatchDelivery

Model: [`WatchDelivery.tla`](WatchDelivery.tla)

| Abstract state/action | Concrete representation |
| --- | --- |
| `watchState` | `Connection.watches`; `Registration.kind`, `batch_id`, and `armed` |
| `barrierOpen` | at least one registration whose response batch has not reached `activateBatch` |
| `pending` | ordered `Connection.pending_events` |
| `delivered` | successful calls to `Sink.notify` |
| `Register` | `Manager.register`; duplicates replace `batch_id` and clear `armed` |
| `ActivateResponse` | `Manager.activateBatch`; sends matching pending events in order, then arms registrations in that batch |
| `Publish` | `Manager.publish`, `registrationMatches`, `removeMatchingStandard`, and `queuePendingEvent` |
| `Remove` | `Manager.remove` |

`expectedPresent`, `expectedEvents`, and `unsafeDelivery` are independent
ghost contract state. They let TLC detect a concrete-state mutation that
loses, duplicates, reorders, or prematurely delivers an event.

The model assumes an active authorized connection and sufficient pending
capacity. `deactivateConnection` deliberately clears registrations and
pending events after sink failure, overflow, session invalidation, or
connection removal; those terminal cases end the active-connection delivery
obligation rather than refining `Publish`.

## Stuttering and omitted detail

The following implementation steps refine to stuttering in all models unless
they cross a modeled publication boundary:

- Jute decoding and response encoding;
- allocator bookkeeping and owned-buffer transfer;
- RocksDB reads, iterator steps, WAL sync internals, and applied-index-only
  writes;
- request queueing, transport polling, Raft heartbeats, and retries;
- ACL evaluation (models begin after authorization unless ACL behavior is the
  property under study);
- watch summary/dump operations and administrative reads.

Any change that exposes a `MultiState` before commit, changes a session or
cleanup CAS guard, completes a read before `applied_index`, alters watch
barrier selection, or changes the external `raftz` guarantees requires an
update to the corresponding model and this mapping.
