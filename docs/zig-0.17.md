# Zig 0.17 migration

The toolchain is pinned to Zig 0.17.0 in `mise.toml` and the package minimum
version. ZooKeeper compatibility remains **3.9.5**.

## Dependencies and builds

- raftz: `d5eba2b338cfd00f2306680cc2dcafb016add3a0` (0.2.0).
- zest: `420846a238e832b42c05bd857773c5a96d8cd586`.
- rocksdb-zig remains at `20cc086f2702d9ec4aff144c242962ea3b666ee5`.
- grpc-lite and other transitive versions follow the upstream raftz manifest;
  no new local dependency patches or vendored copies are introduced here.

Run `mise run fetch` before a first build. `tools/fetch-deps.sh` extracts the
single archive root before invoking `zig fetch` and verifies the expected Zig
package hash. Keep its pins synchronized with the package manifests when
updating dependencies. No hash checks are disabled and shared package sources
are not modified. The libcpucycles release mirror is additionally checked
against the SHA-256 pinned by upstream raftz.

CI retains Fedora 44 for system libraries, but installs Zig through mise.
The old 0.16 cache namespace is not reused. The SCC libtsan workaround is no
longer part of the test task; the historical patch script is retained only as
a reference, not run by CI or the normal sanitizer task.

## Source changes

- Use Zig 0.17 reflection field names/types/values and pointer attributes.
- Replace array repetition with typed `@splat` values and accept the builtin
  spelling changes made by `zig fmt`.
- Use sentinel duplication and the current EnumSet initialization API.
- Construct an owned ArrayList through `fromOwnedSlice`, preserving its
  internal bookkeeping; a regression test checks empty initialization and growth.
- Fix two async-client races observed during sanitizer validation: capture the
  timer interval before launching the engine, and read the shutdown flag under
  the same mutex used by its writer. Existing reconnect and outstanding-request
  shutdown tests exercise these paths under TSan.

## Local validation

- Default bundled-RocksDB build and native tests: 150 tests passed.
- System-RocksDB native tests: 150 tests passed, including a fresh project copy
  with an isolated, initially empty Zig global cache and verified prefetch.
- ReleaseSafe compilation and native tests.
- TSan: 69 tests passed in each of ten consecutive runs, without the SCC patch.
- Listener/storage failure, client admission, handshake deadline, one-/three-node
  offline backup/restore, and fuzz smoke entry points.
- Java interoperability: 105 tests passed; upstream server subset: 11 passed.
- Zig formatting, compile-only lint, shell syntax, and actionlint.

The first combined interoperability run reported one ConnectionLoss in
`PersistentWatcherACLTest.testNothingAllDenied`. The ACL class subsequently
passed alone (10 tests), and a complete separate Java run passed all 105 tests.
No timeout or assertion was relaxed; the original intermittent failure has not
been attributed to a proven root cause.

This migration does not establish mixed-version ensemble compatibility, rolling
upgrades, or broader production-readiness guarantees. The full Jepsen and formal
verification suites are not part of this local migration validation.
