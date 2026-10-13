# CI unit-test failure investigation

CI run [38335213767](https://github.com/fanyang89/zookeeper-zig/actions/runs/38335213767)
at `79d16f3` failed only the Unit tests job. Format, Lint, Build,
ThreadSanitizer, Fuzz smoke, TLA+, and both interoperability jobs passed.
The same three failures reproduced locally before this fix.

## Causes and fixes

- Both real quorum tests timed out after snapshot installation. The earlier
  `applying snapshot` / `ZooKeeper Raft driver stopped: Fatal` messages identify
  the cause: `ensureConfigBaseline` inserted `/zookeeper` without updating the
  root's child count. Snapshot restoration correctly rejected the inconsistent
  tree. Update parent counts atomically with baseline creation, including the
  existing `/zookeeper`, missing config case. Do not relax snapshot validation
  or increase test timeouts. Bootstrap does not represent a client mutation;
  existing cversion, sequence counters, and zxids remain unchanged.
- The TCP CRUD test expected only `app` under `/`. Assert both `app` and the
  initialized `zookeeper` system node instead.
- Snapshot inspection also exposed omission of the private reconfiguration
  baseline. Include the private metadata record and restore it verbatim,
  rejecting duplicate records. Never reconstruct it from public config data:
  that data can be edited independently of membership.

A native regression covers fresh and pre-existing system parents, repeated
bootstrap, strict snapshot restore, and authoritative role rejection after a
snapshot containing poisoned public config data.

## Local validation

- Native test suite: 144/144 passed.
- CI's system-RocksDB test configuration: four consecutive 144/144 passes.
- Format check, ReleaseSafe lint, and system-RocksDB fuzz smoke passed.

These are local results, not a rerun of the remote workflow. The original
failed GitHub run remains unchanged until a new revision is tested there.

## Existing-data boundary

This fix prevents inconsistent baseline creation; it does not automatically
repair stores or snapshots already containing incorrect child counts. Snapshots
without the private metadata record remain readable if otherwise valid, but
missing authoritative membership is not reconstructed from public data during
restore. New snapshots containing that record are rejected by older binaries;
use coordinated upgrades and retain pre-upgrade backups, not rolling downgrade.
The replicated command format and historical command replay are unchanged.
