# ZooKeeper for Zig

This project is evolving into a native Zig implementation of ZooKeeper. The
first native layer is the Jute binary archive in `src/jute/`, with ZooKeeper
record structs in `src/protocol/`. Comptime reflection serializes fields in
declaration order, so the protocol structs require no generated codec methods.

The repository also builds the unmodified Apache ZooKeeper 3.9.5 C client. It
provides compatibility coverage while the native client and server are built.
The C client targets Linux and produces threaded (`zookeeper_mt`) and
single-threaded (`zookeeper_st`) libraries. Its sources are local; see
`vendor/UPSTREAM.md` for provenance.

## Requirements

- mise 2026.7.13 or newer
- Zig 0.16.0 (installed by mise)
- OpenSSL development files for TLS builds
- Cyrus SASL development files for SASL builds
- Cyrus SASL DIGEST-MD5 mechanism for SASL end-to-end tests
- Docker with Compose for end-to-end tests
- OpenSSL and JDK `keytool` for TLS end-to-end tests
- Clang with compiler-rt for address sanitizer tests

## Quick start

```sh
mise install
mise run build
mise run test-zig
mise run test
mise run test-asan
mise run test-ubsan
mise run test-tsan
mise run fuzz
mise run fuzz-asan
mise run e2e
mise run e2e-asan
mise run e2e-ubsan
mise run e2e-tsan
mise run e2e-sasl
mise run e2e-tls
mise run e2e-quorum
mise run chaos
```

More test and instrumentation tasks will be listed by `mise tasks`.

## Native Zig API

The build exports portable `zookeeper` and `jute` modules. The native API
includes the Jute archive, protocol records, ZooKeeper opcodes, length-prefixed
TCP framing, typed request/reply codecs, a blocking TCP transport, and the
client session state machine. Only the legacy C client build is Linux-specific.

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
does not yet reconnect, authenticate, or restore watches automatically; callers
responsible for ping and expiration checks should configure finite I/O timeouts.

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

## Roadmap

1. Jute binary archive, comptime reflection, and Zig protocol structs.
2. Native transport, sessions, authentication, watches, and client API.
3. ZooKeeper data tree, persistence, and server request processing.
4. Replicated server state backed by `raftz` consensus.

## Fuzzing

Fuzz targets use [libFuzzer](https://llvm.org/docs/LibFuzzer.html) and target the
jute deserialization layer, which is the boundary where untrusted server responses
are parsed.

```sh
# fuzz for 60 seconds (default)
mise run fuzz

# fuzz with address sanitizer (finds memory bugs)
mise run fuzz-asan

# customise duration and input length
FUZZ_DURATION=300 FUZZ_MAX_LEN=8192 mise run fuzz

# run a specific crash input through the built target
zig build fuzz -Dfuzz=true -Dclang-runtime-dir="$(clang --print-runtime-dir)"
./zig-cache/o/*/fuzz_jute tests/.runtime/fuzz/crash-*
```

Fuzzing requires `-Dclang-runtime-dir` (the path from `clang --print-runtime-dir`)
because libFuzzer's runtime ships with Clang's compiler-rt. The mise tasks set this
automatically.
