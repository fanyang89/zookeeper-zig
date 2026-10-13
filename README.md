# ZooKeeper for Zig

This project is evolving into a native Zig implementation of ZooKeeper. The
first native layer is the Jute binary archive in `src/jute/`, with ZooKeeper
record structs in `src/protocol/`. Comptime reflection serializes fields in
declaration order, so the protocol structs require no generated codec methods.

See [`COMPATIBILITY.md`](COMPATIBILITY.md) for verified client versions,
feature gaps, ensemble boundaries, zxid semantics, and migration limits.

## Requirements

- mise 2026.7.13 or newer
- Zig 0.16.0 (installed by mise)
- Linux for the quorum server; client, Jute, protocol, and wire modules remain portable

## Quick start

```sh
mise install
mise run fmt-check
mise run lint
mise run build
mise run test
```

The lint task compiles the native test tree in `ReleaseSafe`; formatting is
checked separately with `zig fmt --check`.

ThreadSanitizer covers the project-owned worker pool, watch manager, async
client, and TCP transport concurrency paths:

```sh
mise run test-tsan
```

The task first runs the idempotent `patch-zig-tsan` workaround. Zig 0.16.0's
vendored libtsan contains an unused `linux/scc.h` include and two unused SCC
size declarations that fail to compile with its selected libc headers. The
workaround removes exactly those three lines from the mise-managed Zig
installation and refuses partially matching source. `src/tsan_test.zig`
keeps the sanitized dependency closure free of C++ runtimes; Raft, grpc-lite,
and RocksDB retain their upstream concurrency-testing boundaries.

## Native Zig API

The build exports portable `zookeeper` and `jute` modules. The native client API
includes the Jute archive, protocol records, ZooKeeper opcodes, length-prefixed
TCP framing, typed request/reply codecs, blocking and asynchronous clients, and
the client session state machine.

```zig
const jute = @import("jute");

var writer = jute.Writer.init(allocator);
defer writer.deinit();
try writer.writeInt(42);
try writer.writeString("/zookeeper");

var reader = jute.Reader.init(writer.bytes());
const value = try reader.readInt();
const path = (try reader.readString()).?;

const request = @import("zookeeper").protocol.proto.GetDataRequest{
    .path = "/zookeeper",
    .watch = true,
};
try jute.serialize(&writer, request);

const zookeeper = @import("zookeeper");
writer.truncate(0);
try zookeeper.wire.encodeRequest(&writer, 1, .get_data, request);
const frame = (try zookeeper.wire.parseFrame(
    writer.bytes(),
    zookeeper.wire.default_max_payload,
)).?;
```

The wire layer validates signed frame lengths, enforces configurable frame and
Jute limits, supports fragmented receive buffers, and handles header-only
control packets. Connect frames intentionally omit `RequestHeader`. Dedicated
Connect codecs accept legacy peers that omit the final `readOnly` byte and let
the server mirror that capability in its response.

Borrowed decode APIs require the receive buffer to remain stable. Owned decode
APIs keep an internal payload copy for responses retained across buffer reuse.

`zookeeper.client.BlockingClient` performs the Connect handshake, negotiates the
session, writes complete frames, tracks ordered XIDs, handles special replies,
and supports bounded connect, handshake, read, and write operations. It uses the
monotonic awake clock for session activity. Disconnects drain outstanding
requests into `failedRequests()` with a connection-loss reason, and `close()`
performs the ordered `closeSession` exchange. The client is single-threaded and
does not reconnect by itself; callers responsible for ping and expiration
checks should configure finite I/O timeouts.

`zookeeper.client.AsyncClient` adds a concurrent read pump, serialized writes,
request futures, watch notification delivery, automatic pings, timeout checks,
automatic reconnection with session resume: outstanding requests are re-sent
with fresh xids, `addAuth` credentials are replayed, registered watches are
restored through `setWatches2` relative to the last zxid seen, and synthetic
`WatcherEvent` state notifications (`Disconnected`, `SyncConnected`, `Expired`)
surface on the notification queue. Reconnection retries with exponential
backoff until the session expiry budget is exhausted; disable it with
`Options.reconnect.enabled = false`,
and an ordered `closeSession` exchange. It requires an `std.Io` implementation
that supports `concurrent`. The client is heap allocated so its address remains
stable while background tasks run.

```zig
const zookeeper = @import("zookeeper");

const client = try zookeeper.client.AsyncClient.connectAddress(
    allocator,
    io,
    address,
    .{ .connection = .{
        .handshake_timeout = one_second,
        .io_timeout = one_second,
    } },
    .{},
);
defer client.deinit();

var future = try client.requestAsync(
    .get_data,
    zookeeper.protocol.proto.GetDataRequest{
        .path = "/zookeeper",
        .watch = true,
    },
);
var inbound = try future.await(io);
defer inbound.deinit();

try client.close(one_second);
```

Request replies are delivered directly to their futures. Watch events are read
with `receiveNotification()`. Canceling a request future cancels only the local
wait; the client still consumes the server reply to preserve ZooKeeper's ordered
request stream. A full notification queue disconnects with
`NotificationQueueFull` rather than blocking request dispatch or session timers.
The options above cover the automatic-reconnection defaults; see the section above for the full resume behavior.

The protocol modules contain all 72 records from ZooKeeper 3.9.5's
`zookeeper.jute`: `data`, `proto`, `quorum`, `persistence`, and `txn`.
Deserialization borrows string and buffer bytes from the input while allocating
vector storage; release decoded vectors with `jute.deinitDecoded`.

`ustring` values currently use caller-supplied wire bytes without transcoding.
This matches the C archive and standard UTF-8 ZooKeeper clients. Java's legacy
`BinaryOutputArchive` emits CESU-8-like bytes for supplementary Unicode
characters; exact transcoding for that edge case remains compatibility work.
Collection decoding also intentionally rejects malformed
negative counts and applies a configurable element limit.

Use `zig build check-jute -Dtarget=<target>` to compile the Jute tests for a
non-native target without running them.

## Quorum server

Linux builds also produce `zig-out/bin/zookeeper-quorum-server`. The server uses
`raftz` with grpc-lite transport for replication. Each node stores Raft WAL and
snapshots under its data directory and stores the applied ZooKeeper tree in
RocksDB. A synchronous RocksDB write batch atomically commits every mutation
with its Raft applied index and term.

The default build compiles the pinned RocksDB dependency. Linux distributions
can instead provide RocksDB through their development package:

```bash
sudo apt-get install librocksdb-dev libsnappy-dev pkg-config
zig build -Dsystem-rocksdb=true
```

The system mode translates the installed `rocksdb/c.h` and dynamically links
`librocksdb` and `libsnappy`, avoiding the bundled C++ build.

Start a new three-node cluster in three terminals with one shared cluster ID:

```sh
zig-out/bin/zookeeper-quorum-server \
  --node-id 1 --cluster-id 0198f54d-5c2a-7000-8000-000000000001 \
  --client-listen 127.0.0.1:2181 --raft-listen 127.0.0.1:2881 \
  --data-dir ./data/node1 \
  --peer 1=127.0.0.1:2881 --peer 2=127.0.0.1:2882 --peer 3=127.0.0.1:2883

zig-out/bin/zookeeper-quorum-server \
  --node-id 2 --cluster-id 0198f54d-5c2a-7000-8000-000000000001 \
  --client-listen 127.0.0.1:2182 --raft-listen 127.0.0.1:2882 \
  --data-dir ./data/node2 \
  --peer 1=127.0.0.1:2881 --peer 2=127.0.0.1:2882 --peer 3=127.0.0.1:2883

zig-out/bin/zookeeper-quorum-server \
  --node-id 3 --cluster-id 0198f54d-5c2a-7000-8000-000000000001 \
  --client-listen 127.0.0.1:2183 --raft-listen 127.0.0.1:2883 \
  --data-dir ./data/node3 \
  --peer 1=127.0.0.1:2881 --peer 2=127.0.0.1:2882 --peer 3=127.0.0.1:2883
```

The same server can load a ZooKeeper-style configuration:

```properties
# zoo.cfg
tickTime=2000
dataDir=/var/lib/zookeeper
clientPortAddress=127.0.0.1
clientPort=2181
server.1=127.0.0.1:2881:3881
server.2=127.0.0.1:2882:3882
server.3=127.0.0.1:2883:3883
4lw.commands.whitelist=ruok,srvr,mntr
reconfigEnabled=true
zookeeper-zig.clusterId=0198f54d-5c2a-7000-8000-000000000001
```

Write the local server id to `/var/lib/zookeeper/myid`, then run:

```sh
zig-out/bin/zookeeper-quorum-server --config /etc/zookeeper/zoo.cfg
```

`dynamicConfigFile` and the modern `server.N=...;clientAddress:clientPort`
syntax are supported. The first server port becomes the Raft address; the
upstream election port is parsed but unused. For immutable static ensembles,
the Raft cluster ID is derived deterministically from sorted member IDs and
addresses. Dynamic files, enabled reconfiguration, joins, and CLI peer
replacement require an explicit `zookeeper-zig.clusterId` or `--cluster-id`
so membership changes cannot change cluster identity. A standalone file needs
no `myid`; it defaults to node 1 and `127.0.0.1:2888` for Raft.

Explicit CLI values override singular file values. Any CLI `--peer` entries
replace the file member list; `--join` omits the local `server.N` entry from
seed peers. Initial observer entries, hostnames/IPv6 addresses, property-line
continuations, and enabled TLS/SASL settings are rejected rather than silently
downgraded. AdminServer remains disabled unless its file settings explicitly
enable or configure it. Optional `zookeeper-zig.*` keys provide `clusterId`,
`nodeId`, `raftListen`, `raftAdvertise`, `join`, `clientRequestWorkers`, and
`clientRequestQueueCapacity` overrides.

The current server supports replicated sessions, session resume and expiration,
ephemeral, sequential, container, and TTL nodes, digest authentication, ACL
enforcement, Connect, ping, closeSession, create/create2/createContainer/createTTL,
delete, setData, getACL/setACL, exists, getData, getChildren/getChildren2,
sync, atomic write multi operations, multiRead, and ZooKeeper watches.
Write multi supports create, create2, container/TTL create, delete, setData, and
version checks. MultiRead supports getData and getChildren with independent
per-operation results. `exists`, `getData`, and `getChildren/getChildren2`
support one-shot watches for create, delete, data, and child events. `addWatch`
supports persistent exact-path and persistent-recursive modes. `checkWatches`
and `removeWatches` implement every ZooKeeper watcher type, including `Any` and
`NOWATCHER`. Clients can restore all watch modes after reconnecting with
`setWatches` or `setWatches2`.
Reads use Raft ReadIndex before accessing RocksDB. Session close and expiration
atomically remove all session-owned ephemeral nodes. ACLs support
`world`, `auth`, `digest`, and IPv4 `ip` identities, including CIDR masks.
Connection establishment and client requests pass through a bounded
`std.Io.Queue` and a fixed worker pool before entering Raft or RocksDB. The
worker count defaults to the number of logical CPUs and the queue capacity
defaults to 256; override them with `--client-request-workers` and
`--client-request-queue-capacity`. Watch notifications and pending persistent
watch events use bounded queues per connection; a slow, failed, or overflowing
notification sink is disconnected rather than blocking Raft apply. Watch events
are filtered using the connection's current READ authorization. IPv6 ACL
identities and SASL are not implemented yet. MultiRead watch flags are accepted
but do not register watches.

## Java client interoperability

Run the strict compatibility suite against the official Apache ZooKeeper Java
client with:

```sh
mise run test-interop-java
```

The runner checks out the official `release-3.9.5` source at the verified
upstream commit, injects the `top.fuis.zookeeperzig.interop` server lifecycle
adapter into `ClientBase`, and runs selected upstream tests unchanged alongside
focused interoperability cases. Each upstream test that normally starts a Java
server starts an isolated Zig server instead. The current 103-test selection
covers synchronous and asynchronous CRUD, create2, ACL, Stat, version,
sequential, container, TTL, write-multi, multiRead, one-shot and persistent
watches, large-data, sync, lifecycle cleanup, error-code, four-letter commands,
whitelist enforcement, three-node failover, and voting-member reconfiguration.
The watch selection includes unchanged upstream persistent,
persistent-recursive, ACL, disconnect, overlapping-mode, and root-watch tests.
The quorum cases verify official-client session continuity, re-authentication,
ephemeral ownership, rolling node restarts, follower catch-up, version-guarded
member removal, loss of the removed quorum, offline replacement join, and
post-reconfiguration availability. Reconfiguration also covers bulk voter
replacement, asynchronous removal, stale/future CAS rejection without config
mutation, and fail-closed same-ID role changes. See
[the adaptation and exclusion ledger](docs/reconfig-interop.md); the full
upstream `ReconfigTest` is not run unchanged. Focused watch runs also execute
unchanged upstream registration, one-shot, disconnect, and auto-reset tests.
Tests that directly inspect Java server internals remain excluded.

The source checkout is cached under `~/.cache/zookeeper-zig`. Set
`ZOOKEEPER_SOURCE_DIR` to use an existing official checkout containing commit
`293c895a8d966a3ecb92872be4a1daf87d725da2`. Focused runs of the unchanged
upstream `MultiOperationTest` cover synchronous and asynchronous multiRead.
Maven Central requests use the
Aliyun public mirror by default; set `ZOOKEEPER_MAVEN_MIRROR=central` to use
Central directly or `MAVEN_SETTINGS=/path/to/settings.xml` for custom settings.
Git, Maven, Python 3, and a JDK are required.

Run the compatible subset of Apache ZooKeeper's upstream server behavior tests
with:

```sh
mise run test-upstream-server
```

This suite runs 11 unchanged upstream JUnit cases against isolated Zig server
processes. It covers create responses, Stat fields, null data, root ACLs,
request retry, concurrent CRUD, session restoration, and session continuity
across a server restart. Set `ZOOKEEPER_SERVER_TEST_SELECTOR` to override the
Surefire selector while evaluating additional upstream cases. Tests requiring
watches or Java server internals remain excluded.

## Jepsen

Run the three-node linearizable-register smoke test or the full suite with:

```sh
mise run test-jepsen-smoke
mise run test-jepsen
```

Jepsen uses the official ZooKeeper 3.9.5 Java client. The full suite checks a
CAS register, independent multi-key registers, create/delete presence,
child-set behavior, persistent sequential IDs, a monotonic counter, and total
and linearizable queues. Faults include killing, pausing, or network-isolating
one node and crashing or suspending the full cluster. Process faults require no
SSH or root; network partitions use Docker-backed nodes with isolated network
namespaces and require Docker socket access. See
[`tests/jepsen/README.md`](tests/jepsen/README.md) for configuration and current
scope.

A new cluster can import an Apache ZooKeeper 3.9.5 `dataDir` before its first
start. The importer restores the newest valid snapshot, replays transaction
logs, and retains resumable sessions and ephemeral nodes. See
[Importing Apache ZooKeeper data](docs/zookeeper-import.md).

## Fuzzing

The import parser and the wire protocol are fuzzed with structure-aware
harnesses:

```sh
mise run fuzz-import      # build zq-fuzz-import
mise run fuzz-wire        # build zq-fuzz-wire
mise run test-fuzz-smoke  # fixed-seed bounded runs plus corpus replay
```

`zq-fuzz-import` generates valid snapshot, transaction-log, Snappy, and gzip
images with the Jute writer and feeds mutated bytes to the byte-level import
entry points. `zq-fuzz-wire` generates valid wire frames (requests, replies,
connect exchanges, control packets) and valid instances of every protocol
record via comptime reflection, then exercises the frame parser and Jute
deserialization. Inputs are a pure function of `(seed, target, index)`, so
every finding reproduces exactly. Worker processes run under `timeout`, and
any panic, signal, hang, or allocator leak is bisected to a single saved
input under `tests/fuzz/corpus/<suite>/crashes/`. Rejected inputs and
accepted seeds are committed under `tests/fuzz/corpus/` and replayed by the
smoke task as a regression corpus. CI runs a short fixed-seed campaign on
every pull request; a nightly workflow runs two million iterations per
target with random seeds.

## Four-letter words

The server accepts the ZooKeeper four-letter admin commands on the client
port: `conf`, `cons`, `crst`, `dump`, `envi`, `gtmk`, `isro`, `mntr`, `ruok`,
`srst`, `srvr`, `stat`, `stmk`, `wchc`, `wchp`, and `wchs`. Output formats
follow Apache ZooKeeper 3.9.5 so `echo ruok | nc host 2181`, `srvr`, and
`mntr`-based Prometheus exporters keep working. Commands are selected with
`--4lw-whitelist` (a comma-separated list, or `*`); the default is `srvr`,
matching `zookeeper.4lw.commands.whitelist`. While a node has no leader,
state-dependent commands reply `This server is not currently serving
requests`.

## Admin HTTP endpoints

Start with `--admin-listen 0.0.0.0:8080` to mirror the upstream AdminServer
URL space: `GET /commands` lists the available commands as links and
`GET /commands/<name>` executes one, returning upstream-shaped JSON
(`{"command": ..., "error": null, ...fields}`); `?format=txt` returns the
plain four-letter output. Both the four-letter names and the upstream admin
primary names (`monitor` for `mntr`, `server_stats` for `srvr`,
`configuration` for `conf`, ...) are accepted. Unlike the socket four-letter
path, the HTTP endpoints are not gated by the 4lw whitelist, matching
upstream. The server is disabled unless `--admin-listen` is set. If either configured
listener fails, the process exits unsuccessfully and cleans up the other
listener; `mise run test-listener-failure` covers port conflicts and restart. HTTP
headers must be complete and at most 4096 bytes (including the final CRLF pair),
and are read under one fixed 10-second deadline. Idle, truncated, oversized,
and timed-out requests are closed without executing a command or sending an
HTTP error response. Partial reads do not reset the deadline.

Admin admission defaults to **64 sockets**, including idle readers and blocked
writers. Set `--admin-max-connections N` or `zookeeper-zig.adminMaxConnections=N`
in `zoo.cfg` to override it (`0` disables the cap). Excess sockets are closed
without an HTTP response. The complete response write has a separate fixed
**10-second** deadline; a stalled reader is disconnected and its slot released.
A timeout may truncate a response and does not undo an executed command.
These limits do not bound command execution, generated response size, or total
memory and do not provide authentication. Keep this listener on a protected
management network; saturation alone is not a restart signal.

## Client connection admission

Set `--client-max-connections 1024` (or
`zookeeper-zig.clientMaxConnections=1024` in `zoo.cfg`) to cap the total admitted
sockets on the client listener. CLI overrides the file; `0` preserves unlimited
admission. Slots are reserved before handshake/task creation and returned after
socket closure, including malformed input and cancellation. Excess connections
are closed without a ZooKeeper response; existing clients remain served.

The cap includes idle/pre-handshake sockets and four-letter commands. It is not
upstream `maxClientCnxns` (a per-IP limit), and does not cover the AdminServer or
Raft listener. A saturated client listener may reject health probes; do not
interpret that alone as a reason to restart. This bounds admitted connection
work, not kernel backlog, total memory, session/watch counts, or connection
attempt rate. Per-IP admission and rate limiting remain separate production
hardening work.

Opening input has a default **10-second** absolute deadline. Configure
`--client-handshake-timeout-ms MS` or
`zookeeper-zig.clientHandshakeTimeoutMs=MS`; `0` disables it. The same awake-clock
deadline covers the initial four-byte probe, the full ConnectRequest, and the
8-byte `stmk` payload when applicable. Partial input does not reset the budget.
Expiry closes the socket and releases its connection slot. This is an input
receive deadline, not a timeout for leader election, session establishment,
4lw response writes, or already-established sessions. AdminServer and Raft
listeners are unaffected. Run `mise run test-handshake-timeout` for idle,
fragmented, slow-drip, and `stmk` regressions.

Run `mise run test-connection-limits` for the real-process saturation/recovery
regression, also run by CI.

## Offline backup recovery

Run `mise run test-backup-restore` for the single-node and fixed-three-voter cold-backup drills.
See [backup and restore](docs/backup-restore.md) for the capture procedure,
identity restrictions, rollback/fencing risks, and remaining production gates.
Online backup and recovery from incomplete or mixed-time ensemble backups remain
unverified.

## Fatal driver failures

A terminal Raft driver error stops the client listener and propagates out of
main as an unsuccessful exit. Temporary leader loss does not trigger this
path. See [health checks and service supervision](docs/fatal-driver-handling.md)
for readiness limits, restart policy, and storage-failure test coverage.
Run `mise run test-storage-failure` to verify WAL write failure, nonzero process
exit, and same-directory recovery with and without AdminServer.

## Dynamic membership (reconfig)

Start the server with `--reconfig-enabled` to accept the ZooKeeper `reconfig`
opcode. Requests take the upstream incremental form
(`joiningServers`/`leavingServers`) or the bulk `newMembers` form, guarded by
`curConfigId` against the version of `/zookeeper/config`; `-1` skips the
check and concurrent reconfigs answer `ReconfigInProgress` (-14). The
reply is a `GetDataResponse` carrying the new config string and the node
stat, matching upstream. Member entries use `server.<id>=<host>:<raft_port>`
with an optional `participant`/`observer` role; the upstream three- and
four-port spellings and `;<client address>` suffixes are accepted and
ignored (the Raft port is the first port). The canonical config is stored
at `/zookeeper/config` (watchable, version = configVersion), and each node
seeds a baseline from its static `--peer` list before its first start.
After a reconfig commits, the Raft leader reconciles cluster membership one
change at a time — adds and address updates before removes. If the leader
dies in the window between commit and reconciliation, any retry of the same
reconfig completes the membership change idempotently.

Same-ID participant/observer transitions are unsupported and fail closed with
`BadArguments` before changing the committed config or its version (both
incremental and bulk requests). Fresh IDs and same-role address updates are
unaffected. Observer-only incremental joining lists are supported when the final
configuration retains a participant; this does not establish full observer
serving support.

Replicated command format v7 enforces this guard at apply time against private
persisted membership metadata, not the publicly writable `/zookeeper/config`
node. Ordinary/multi `setData` and deletion of that node cannot bypass role
validation; valid same-role reconfiguration can repair its public contents. Historical v6 commands retain their original apply semantics
on replay/catch-up, including previously accepted role changes; upgrading does
not repair those configurations. Upgrade the Zig ensemble together: old binaries
reject v7 commands, mixed old/new Zig ensembles and downgrade after v7 writes
are unsupported. This is a storage-command version, not a ZooKeeper wire change.

## Formal verification (TLA+)

Correctness-critical replicated components are modeled in TLA+: session
tracking, container/TTL cleanup fencing, `multi` atomicity, linearizable
reads, and watch delivery. TLC exhaustively checks bounded configurations;
Apalache independently checks the session tracker with larger symbolic
bounds. The models are mutation-validated, and their concrete mappings and
external Raft assumptions are documented under [`tla/`](tla/README.md).

```sh
mise run tla-check
mise run apalache-check
```

## Roadmap

1. Complete reconnect and high-level client APIs.
2. Add IPv6 and SASL authentication.
3. Add dynamic quorum membership, operational metrics, and administration APIs.
4. Expand ZooKeeper 3.9.5 compatibility and interoperability testing.
