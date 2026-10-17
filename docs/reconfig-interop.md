# Reconfiguration interop: bounded ZooKeeper 3.9.5 adaptations

Source: Apache ZooKeeper commit `293c895a8d966a3ecb92872be4a1daf87d725da2`,
`zookeeper-server/src/test/java/org/apache/zookeeper/test/ReconfigTest.java`.
These are **adapted scenarios**, not execution of the unchanged upstream test
class or a claim that FAN-7 is complete. The overlay uses official
`ZooKeeperAdmin`/`ZooKeeper` clients with separate Zig processes and fresh data
directories. `ReconfigTest.testNormalOperation` and `testServerHasConfig` are
called from that pinned source **unchanged**, including their assertions.

| Overlay scenario | Upstream relationship and adaptations |
| --- | --- |
| `officialAdminReplacesVotingMember` | Existing incremental voter removal/join scenario retained. CAS now tests stale and future versions and unchanged config bytes/full Stat. |
| `testBulkReconfig` | Adapted from upstream lines 466-500: `newMembers` replaces 3 voters with 2, then with 3 using fresh ID 4. Uses explicit successful CAS rather than upstream `-1`. Reuses the same distributed quorum checks as incremental replacement. Excludes upstream seven-node topology, two observers, and simultaneous port changes: they would require unsupported role reconciliation or additional transport lifecycle coverage. |
| `testFreshObserverDoesNotVote` | Additional bounded learner scenario: bulk CAS replaces voter 3 with fresh observer 4, starts it in join mode, checks catch-up and unchanged upstream config/read-write helpers, then keeps it alive while voter 2 is stopped. Voter 1 cannot establish sessions throughout a fifteen-second window after stabilization; restoring voter 2 restores writes and observer reads. No same-ID transition or full upstream Observer claim. |
| `testIncrementalObserverDoesNotVote` | Runs the fresh-observer scenario with an observer-only `joiningServers` list and voter 3 in `leavingServers`. Both observer scenarios reject removing all participants without config mutation before accepting a valid target. Same catch-up, serving, sustained non-voting, and recovery assertions as the bulk case. |
| `testRemoveOneAsynchronous` | Adapted from lines 503-539: three instead of five nodes; removes fixed ID 3 (leader or follower) rather than introspecting ZAB leader identity. Uses bounded `CompletableFuture` instead of unbounded monitor wait. Checks success code, path, context identity, data, Stat, and asynchronous stale-CAS error/null payload plus config immutability. Excludes serving/config assertions on the retired node. |
| `testPublicConfigPoisonCannotChangeMembership` | Additional regression, not an upstream scenario: ordinary and multi `setData` write altered roles or malformed public config data; unsupported bulk changes still fail, preserving public bytes/Stat, and valid repair succeeds. Deletion cannot bypass the guard; a valid repair recreates the node. Positive writes still require the unchanged voter. Native tests inspect authoritative metadata immutability and all metadata/public-node presence combinations. |
| `testRoleChangeRejectedWithoutConfigMutation` | Negative adaptation of lines 541-616, **not** upstream success semantics. Rejects same-ID voter demotion via incremental and bulk APIs, checks unchanged config bytes/full Stat, accepts a subsequent valid request, and proves the unchanged node still votes. Native tests additionally exercise promotion, both request construction forms, CAS and `-1`, same-role address update, fixed-v6 replay and reopen. No live observer-promotion claim. |

For voter removal, after stopping the other retained voter the removed process
must remain alive throughout five seconds of stabilization and a full fifteen
seconds with no new session connection. This is sustained non-availability,
not one connection-loss reply. Restarting the retained voter restores writes.
For replacement, ID 3 is retired and stopped, ID 4 joins with seeds 1 and 2,
and writes plus the unchanged upstream cross-client read/write helper succeed
with ID 2 stopped: the new voter is needed. No IDs or old data directories are
reused; no mixed Java/Zig ensemble is started. Tests have per-operation and
whole-test deadlines and process cleanup through `AutoCloseable`.

## Bounded negative-CAS stabilization

The FAN-7 bulk case previously received `ConnectionLoss` instead of `BadVersion`
immediately after successful removal at `87d2a6d` (retained validation artifact:
`fix-java-full.log`). A passing unchanged-head rerun did
not erase this new-test instability; the original failure remains recorded.

Only the two negative-CAS checks in incremental/bulk voter replacement now use
`assertBadVersionEventually`. Every attempt retains the exact target, invalid
version, and post-removal baseline. One monotonic 30-second budget includes both
reconfiguration and verification reads; async callback waits consume the remaining
budget, with at most 100 ms between retries. Only `ConnectionLossException` is
retryable. A recovered read must match config bytes and the full captured Stat,
and the helper must eventually receive `BadVersionException`. Success, mutation,
other semantic errors, or deadline expiry fail with version/attempt/endpoint
diagnostics. Read retries never resubmit a CAS already rejected by BadVersion.
Positive reconfigs and the separate async-removal scenario are unchanged.

Four scripted tests (no server processes) cover transient request/read loss then
BadVersion, forbidden success/unexpected request errors, request/read deadline
expiry without reset, and mutation/unexpected read errors. The class selection
therefore contains seven distributed scenarios plus four helper tests. These
are local harness tests, not additional unchanged upstream scenarios.

## Role and recovery safety limits

Pinned raftz 0.2.0 commit `2230f09d9f44238fcaedd056544c3a180a16c256` exposes
`Raftor.addNode`/`addLearner` and `conf_changer.makeVoter`/`makeLearner`.
However, this server's `reconfig.LiveMember`/`diffMembership` compare only IDs
and addresses; `Quorum.driveMembership` does not reconcile same-ID roles or
establish safe leader-demotion/completion behavior. We do not change raftz or
invent a role-transition algorithm here. New same-ID role changes fail closed
with `BadArguments` at replicated apply time, before config publication. The
comparison uses private persisted `config_string_key` metadata, not the public
`/zookeeper/config` data or a request-side read. Ordinary and multi writes to the
public node retain their existing semantics; editing/deleting it cannot alter
the authoritative role baseline.

After existing target parsing and CAS checks, malformed authoritative metadata
fails with `BadArguments`; OOM/storage errors propagate. Missing metadata with
an existing public node also fails, without copying public data into metadata.
If both are absent there is no known baseline, so legitimate unseeded first
reconfiguration retains its existing behavior. This is not a guarantee of
recovery from undetectable loss of both representations. With intact metadata,
a valid same-role reconfig repairs malformed public data or recreates a deleted
public node (retaining the existing absent-node CAS/version-zero behavior).
`ensureConfigBaseline` import/bootstrap synchronization remains a separate,
unchanged pre-Raft boundary; this fix adds no apply-time public-data fallback.

Command format v7 carries these validation semantics; v6 log entries retain
their original semantics on replay/catch-up to avoid divergent historical
application. A fixed v6 byte fixture is applied through the state machine and
reopened from RocksDB; subsequent v7 promotion fails without config mutation.
The encoder cannot turn a permissive decoded v6 reconfig into a v7 command.
Old binaries reject v7: coordinated Zig upgrades are required, mixed old/new
Zig ensembles and downgrade after v7 writes are unsupported. Existing incorrect
role configurations are **not automatically repaired**. Applied Raft index may
advance on rejection even though config data/Stat and callbacks do not.

Fresh learner addition via bulk and observer-only incremental reconfiguration is
covered by this Java slice. Incremental entries are parsed as a subset and the
merged target must retain a participant. This changes only request construction:
replicated commands still contain a complete canonical target, so historical
v6/v7 replay semantics and the authoritative same-ID role guard are unchanged.
Initial observers,
full Observer serving semantics, ZAB/QuorumUtil/JMX behavior, membership completion
at successful response time, durable pending-reconciliation recovery, concurrent
reconfigs across ingress nodes, port-change lifecycle, mixed Java/Zig clusters,
and full upstream `ReconfigTest` remain outside this bounded coverage.
