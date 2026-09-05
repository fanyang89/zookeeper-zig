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

## Running

```sh
mise run tla-check   # or: bash tools/tla-check.sh
```

The script pins `tla2tools` 1.7.0 by SHA-256, caches it under
`~/.cache/zookeeper-zig`, and runs TLC for both configurations. Bounded
state budgets are set by the constants in the `.cfg` files; both runs
complete exhaustively in seconds on a laptop.

## Verification boundary

The specs verify the state-machine logic that commits through Raft: guards,
CAS fencing, and the replicated clock. They assume Raft delivers a totally
ordered, gap-free log (the `raftz` dependency's own verification remains
open, tracked in the roadmap) and assume server-generated session passwords
are unique per incarnation, which fences cross-incarnation confusion
together with generations.
