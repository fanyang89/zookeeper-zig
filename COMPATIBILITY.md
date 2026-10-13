# Compatibility matrix

The compatibility target is the client-visible behavior of Apache ZooKeeper
3.9.5. This project replaces ZAB and the upstream persistence formats with
Raft and RocksDB, so wire compatibility does not imply ensemble or on-disk
compatibility.

Status meanings:

- **Verified** — exercised in CI against the named upstream implementation.
- **Implemented** — covered by native tests but not by the named upstream
  interoperability suite.
- **Partial** — usable subset with the listed behavioral differences.
- **Unsupported** — intentionally unavailable or not implemented yet.
- **Unverified** — expected to share compatible protocol ancestry, but no
  compatibility claim is made without tests.

## Client versions

| Client | Status | Evidence and boundary |
| --- | --- | --- |
| Apache ZooKeeper Java 3.9.5 | **Verified** | `tests/interop/java/run.sh`, the selected upstream server behavior suite, and Jepsen use the official 3.9.5 client in CI. |
| Apache ZooKeeper Java 3.8.x | **Unverified** | No version-specific CI run. |
| Apache ZooKeeper Java 3.7.x | **Unverified** | No version-specific CI run. |
| Apache ZooKeeper Java 3.6.x | **Unverified** | No version-specific CI run. |
| Apache ZooKeeper Java 3.5.x | **Unverified** | Record negotiation may be compatible, but persistent watches, TTL/container operations, errors, and reconnect behavior are not systematically tested. |
| Apache ZooKeeper Java 3.4.x | **Unverified** | Legacy connect framing is parsed, but the 3.4 client and its feature negotiation are not tested. |
| Native Zig `AsyncClient` | **Implemented** | Automatic session resume, request resend, auth replay, watch restoration, bounded backoff, and synthetic connection events are covered by native tests. |
| Native Zig `BlockingClient` | **Partial** | Core requests and sessions work; no high-level automatic reconnect wrapper. |
| Chrooted client connection strings | **Unsupported** | Paths are sent unchanged; client-side chroot rewriting is not implemented. |

Only Java 3.9.5 is currently a verified external-client compatibility claim.

## Client protocol and data-tree behavior

| Area | Status | Boundary |
| --- | --- | --- |
| Jute records and framing | **Verified** | All 72 ZooKeeper 3.9.5 records are represented; length limits, fragmentation, and connect compatibility have native and fuzz coverage. |
| Core CRUD, exists, children, ACL, sync | **Verified** | Official Java-client interop plus native tests. |
| Sequential and ephemeral nodes | **Verified** | Official Java-client interop plus native lifecycle tests. |
| Container and TTL nodes | **Implemented** | Native lifecycle, cleanup-fencing, persistence, and TLA+ tests; dedicated upstream interoperability coverage remains open. |
| `multi` and `multiRead` | **Implemented** | Atomic write-multi and linearizable multi-read are tested natively and modeled in TLA+. Unsupported operation kinds return `unimplemented`. |
| One-shot watches | **Verified** | Java interop and native tests cover data/existence/children watches. |
| Persistent and persistent-recursive watches | **Implemented** | Native tests and TLA+ cover response barriers, ordering, deduplication, removal, and recursive matching; upstream interoperability remains incomplete. |
| `setWatches` / `setWatches2` | **Implemented** | Used by async reconnect watch restoration and tested natively. |
| `getEphemerals`, `getAllChildrenNumber`, `whoAmI`, `deleteContainer` | **Implemented** | ZooKeeper 3.9.5 wire behavior and native tests; broader upstream-suite coverage remains open. |
| Dynamic reconfiguration | **Partial** | Incremental/bulk requests, version CAS, `/zookeeper/config`, and Raft membership reconciliation are implemented. Official Java 3.9.5 interop covers incremental/bulk voter replacement, successful and rejected CAS with config immutability, asynchronous removal, sustained quorum loss, offline fresh-ID join, and post-change writes needing the new voter. See [adaptations and exclusions](docs/reconfig-interop.md). Raft-retired node IDs cannot be reused; replacements require a new ID. Mixed Java/Zig membership is unsupported, and the full upstream `ReconfigTest` suite remains open. |
| Path quotas and `zookeeper.limitSize` | **Unsupported** | `/zookeeper/quota` enforcement is not implemented. |

## Authentication and transport security

| Feature | Status | Boundary |
| --- | --- | --- |
| `world`, `auth`, `digest`, IPv4 `ip` ACL identities | **Implemented** | Includes IPv4 CIDR masks and ACL enforcement on reads, writes, watches, and administrative deletes. |
| SASL opcode, no configured mechanism | **Implemented** | Empty/nonempty and repeated tokens receive ZooKeeper 3.9.5's `OK`, zxid `0`, and a Jute null response token (not empty). No authenticated identity is added; ACLs remain enforced. Native tests and `SaslInteropTest` cover the baseline and safe rejection of null/malformed/truncated/oversized requests. |
| Full SASL authentication / DIGEST-MD5 | **Unsupported** | No SASL mechanism, JAAS configuration, or SASL identity is supported (FAN-10). An `OK` SASL opcode reply is not authentication success. |
| X.509 client identity | **Unsupported** | No secure client port or certificate authentication. |
| `super` digest | **Unsupported** | No administrative bypass identity. |
| TLS client port | **Unsupported** | Client connections are plaintext TCP. |
| TLS or authenticated Raft transport | **Unsupported** | Peer transport has no TLS or quorum authentication. Deploy only on a trusted network boundary. |

The no-mechanism SASL behavior follows Apache ZooKeeper 3.9.5
`ZooKeeperServer.processSasl`: without a SASL server, a non-null request token
gets a null challenge and no auth identity. Null request tokens close the
connection safely (upstream accesses their length before its exception handler).
`ZooKeeperSaslClient.createSaslToken` treats a null server challenge during an
incomplete exchange as authentication failure, not successful negotiation.
This baseline does not enable SASL-required configurations or secure transport.

## Server roles and ensemble compatibility

| Feature | Status | Boundary |
| --- | --- | --- |
| Single-node and voting Raft ensembles | **Implemented** | Real quorum tests cover persistence, replication, partition, and restart; local sandbox execution may require an unrestricted `io_uring` seccomp profile. |
| Observer membership in reconfiguration | **Partial** | Fresh `observer` members map to Raft learners. Java 3.9.5 interop verifies bulk and observer-only incremental fresh-ID addition, catch-up, cross-client reads/writes, non-voting behavior during sustained voter loss, and recovery. Final configurations without a participant are rejected. Same-ID role transitions are checked against private persisted membership metadata, even if the public config node is edited or deleted, and fail with `BadArguments` without changing config bytes/version; role-aware reconciliation and full upstream Observer serving semantics are unsupported. Historical v6 log replay preserves old behavior; v7 requires coordinated Zig upgrades (no mixed-version ensemble or downgrade after v7 writes). |
| Read-only mode during partition | **Unsupported** | Connect framing negotiates the field, but the server does not enter upstream read-only mode and `isro` remains false. |
| Mixed Java/Zig ensemble | **Unsupported** | Java servers speak ZAB peer protocols; Zig servers speak `raftz`. They cannot form one ensemble. |
| Rolling migration between Java and Zig servers | **Unsupported** | Migration requires an offline one-way import into a new Zig ensemble. |

## zxid semantics

Client-visible zxids are monotonic signed 64-bit values, but they are not ZAB
zxids:

- a fresh Zig data store uses `client_zxid = raft_index`;
- after importing an upstream store at zxid `source_zxid`, it uses
  `client_zxid = source_zxid + raft_index`;
- every operation inside one successful `multi` shares the enclosing Raft
  entry's zxid;
- the imported offset is persisted as `zxid_base` and overflow is rejected.

This preserves ordering, watch zxid progression, and CAS/stat relationships
used by clients. It does **not** preserve ZooKeeper's epoch/counter bit
interpretation, and the high/low 32-bit fields must not be decoded as a ZAB
leader epoch and counter. Operational tools that only compare zxids are
supported; tools that infer ZAB terms from their bit layout are not.

## Persistence and migration

| Feature | Status | Boundary |
| --- | --- | --- |
| Import Apache ZooKeeper 3.9.5 `dataDir` | **Implemented** | One-shot import on first start reads 3.9.5 snapshots and transaction logs, preserves data/stat/ACL/session state, and establishes the zxid offset. |
| Separate upstream `dataLogDir` input | **Implemented** | `--import-zookeeper-log-dir` supplies the transaction-log directory. |
| Import into a non-empty Zig store | **Unsupported** | Import is an initial migration operation, not merge or replication. |
| Export upstream snapshot/transaction logs | **Unsupported** | Migration back to Java ZooKeeper is not available. |
| Direct RocksDB portability to Java ZooKeeper | **Unsupported** | The Zig RocksDB schema is private and not an upstream persistence format. |
| Offline full-directory backup | **Partial** | A same-version, same-identity single-node and fixed-three-voter drill verifies stopped-directory copying, data/Stat/ACL/config recovery, sequence counters, new writes, and restored majority availability. Online backups, partial-ensemble/mixed-time disaster recovery, and rollback fencing guarantees remain open; see [runbook and limits](docs/backup-restore.md). |
| Crash recovery and snapshots inside a Zig ensemble | **Implemented** | Durable applied indexes, RocksDB state, Raft WAL/snapshots, and restart paths are native to the Zig/Raft deployment. |

Take backups before import and retain the original upstream `dataDir`; the
migration is intentionally one-way.

## Operations and configuration

| Feature | Status | Boundary |
| --- | --- | --- |
| Client connection admission | **Implemented** | Optional total client-socket cap via `--client-max-connections` / `zookeeper-zig.clientMaxConnections`; `0` defaults to unlimited. Includes pre-handshake/4lw sockets; subprocess tests verify rejection and slot recovery. Not upstream per-IP `maxClientCnxns`; AdminServer/Raft and rate limits are outside this cap. Opening input has a separate default 10-second absolute receive deadline (`--client-handshake-timeout-ms`, `0` disables); partial Connect/4lw input cannot extend it. Established sessions and server-side session establishment are unaffected. |
| Four-letter commands | **Verified** | The ZooKeeper 3.9.5 command set, whitelist rejection, and `stmk` payload are exercised through upstream `FourLetterWordMain` in CI, with native output tests. |
| AdminServer `/commands` HTTP endpoints | **Partial** | Plaintext and JSON command responses are implemented when `--admin-listen` is set. XML and JSONP are not implemented, and the server is disabled by default instead of listening on port 8080. Complete HTTP headers are limited to 4096 bytes and a fixed 10-second receive deadline; invalid/incomplete input is closed before command dispatch. Admin sockets default to a configurable total cap of 64 (`--admin-max-connections`, `0` disables); response writes have a separate fixed 10-second deadline. Command execution and response allocation are not bounded by these limits. |
| Prometheus-style `mntr` consumers | **Implemented** | Upstream-compatible metric keys are emitted; exporter-specific certification is not claimed. |
| `zoo.cfg` loading | **Partial** | `dataDir`/`myid`, client endpoints, `server.N`, `dynamicConfigFile`, 4lw, reconfiguration, and AdminServer settings are loaded. The quorum port maps to Raft; election ports are unused. Initial observers, hostnames/IPv6, continuations, and enabled TLS/SASL settings are rejected. CLI flags can override file values; mutable membership requires an explicit stable cluster ID. |
| Dynamic `zoo.cfg.dynamic` management | **Partial** | Existing dynamic files are read at startup. Client reconfiguration updates replicated `/zookeeper/config`; it does not rewrite upstream configuration files. |

## Verification and test boundary

The TLA+ models verify session fencing, cleanup fencing, multi publication,
read-index consequences, and watch delivery under the assumptions documented
in `tla/README.md`. They do not prove `raftz` itself correct. ThreadSanitizer
covers project-owned worker-pool, watch-manager, async-client, and TCP
transport concurrency paths; Raft, grpc-lite, and RocksDB keep their upstream
verification and concurrency-testing boundaries.
