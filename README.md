# ZooKeeper for Zig

This project is evolving into a native Zig implementation of ZooKeeper. The
first native layer is the Jute binary archive in `src/jute/`. It implements the
wire primitives used by ZooKeeper records while enforcing configurable decode
limits.

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
mise run test-jute
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

The build exports portable `zookeeper` and `jute` modules. The Jute archive
supports big-endian primitives, nullable strings and buffers, collection
lengths, zero-copy reads, allocating reads, record hooks, and decode limits.
Only the legacy C client build is Linux-specific.

```zig
const jute = @import("jute");

var writer = jute.Writer.init(allocator);
defer writer.deinit();
try writer.writeInt(42);
try writer.writeString("/zookeeper");

var reader = jute.Reader.init(writer.bytes());
const value = try reader.readInt();
const path = (try reader.readString()).?;
```

`ustring` values currently use caller-supplied wire bytes without transcoding.
This matches the C archive and standard UTF-8 ZooKeeper clients. Java's legacy
`BinaryOutputArchive` emits CESU-8-like bytes for supplementary Unicode
characters; exact transcoding for that edge case remains part of record-codegen
compatibility work. Collection decoding also intentionally rejects malformed
negative counts and applies a configurable element limit.

Use `zig build check-jute -Dtarget=<target>` to compile the Jute tests for a
non-native target without running them.

## Roadmap

1. Jute binary archive and schema-driven Zig record generation.
2. Native protocol framing, sessions, authentication, watches, and client API.
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
