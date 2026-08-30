# ZooKeeper for Zig

This project is evolving into a native Zig implementation of ZooKeeper. The
first native layer is the Jute binary archive in `src/jute/`, with ZooKeeper
record structs in `src/protocol/`. Comptime reflection serializes fields in
declaration order, so the protocol structs require no generated codec methods.

## Requirements

- mise 2026.7.13 or newer
- Zig 0.16.0 (installed by mise)
- Linux for the quorum server; client, Jute, protocol, and wire modules remain portable

## Quick start

```sh
mise install
mise run build
mise run test
```

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
does not yet reconnect, re-authenticate, or restore watches automatically; callers
responsible for ping and expiration checks should configure finite I/O timeouts.

`zookeeper.client.AsyncClient` adds a concurrent read pump, serialized writes,
request futures, watch notification delivery, automatic pings, timeout checks,
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
Reconnection, re-authentication, and watch restoration remain future work.

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

The current server supports replicated sessions, session resume and expiration,
ephemeral and sequential nodes, digest authentication, ACL enforcement,
Connect, ping, closeSession, create/create2, delete, setData, getACL/setACL,
exists, getData, getChildren/getChildren2, and sync. Reads use Raft ReadIndex
before accessing RocksDB. Session close and expiration atomically remove all
session-owned ephemeral nodes. ACLs support `world`, `auth`, `digest`, and IPv4
`ip` identities, including CIDR masks. IPv6 ACL identities, SASL, watches, and
multi operations are not implemented yet.

## Java client interoperability

Run the strict compatibility suite against the official Apache ZooKeeper Java
client with:

```sh
mise run test-interop-java
```

The runner checks out the official `release-3.9.5` source at the verified
upstream commit, injects the `top.fuis.zookeeperzig.interop` server lifecycle
adapter into `ClientBase`, and runs selected upstream `AsyncOpsTest` and
`ClientTest` methods unchanged. Each upstream test that normally starts a Java
server starts an isolated Zig server instead. The current selection covers 46
synchronous and asynchronous CRUD, create2, ACL, Stat, version, sequential,
large-data, sync, and error-code tests; watch and multi tests remain excluded
until those operations are implemented.

The source checkout is cached under `~/.cache/zookeeper-zig`. Set
`ZOOKEEPER_SOURCE_DIR` to use an existing official checkout containing commit
`293c895a8d966a3ecb92872be4a1daf87d725da2`. Git, Maven, Python 3, and a JDK are
required.

A new cluster can import an Apache ZooKeeper 3.9.5 `dataDir` before its first
start. The importer restores the newest valid snapshot, replays transaction
logs, and retains resumable sessions and ephemeral nodes. See
[Importing Apache ZooKeeper data](docs/zookeeper-import.md).

## Roadmap

1. Complete reconnect, watches, and high-level client APIs.
2. Add IPv6/SASL authentication, watches, and multi operations.
3. Add dynamic quorum membership, operational metrics, and administration APIs.
4. Expand ZooKeeper 3.9.5 compatibility and interoperability testing.
