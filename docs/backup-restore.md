# Offline backup and restore: initial production boundary

## Supported drill

The current automated drill covers **one voting node**, the **same executable
version**, and the **same cluster ID, node ID, and peer endpoints**. All source
processes are stopped while files are copied. Recovery uses a different data
directory; the original instance remains offline. This is a bounded recovery
exercise, not a supported online backup API or a multi-node disaster-recovery
procedure.

Run:

```sh
mise run test-backup-restore
# CI configuration:
zig build test-backup-restore -Dsystem-rocksdb=true --summary all
```

Python 3 and the Linux server runtime are required. The test uses temporary
directories and loopback ports; it does not touch deployed data or services.

The drill verifies:

1. Persistent parent/child data, an updated data version, a non-default ACL,
   a sequential node, and `/zookeeper/config` exist before backup.
2. After process exit, the complete directory is copied. Relative filenames and
   SHA-256 hashes match; WAL, RocksDB, and any snapshots/metadata stay together.
3. The original is restarted and acknowledges additional writes. It is then
   stopped and its directory moved out of the original path.
4. The backup is copied to a fresh directory and the same identity is started
   there. Original data, full node Stat, ACL, and config are preserved. Later
   source writes are absent, and sequential allocation resumes from the
   backup's counter. New writes succeed.
5. The backup artifact remains byte-for-byte unchanged after recovery.

The fixture stops the source with SIGTERM and waits for exit (with a bounded
kill fallback); it does not claim a graceful application shutdown protocol.
The acknowledged writes rely on the server's durable commit path.

## Operator procedure for the bounded single-node case

### Capture

- Schedule downtime and fence clients. Stop automatic restarts and verify that
  the server process has exited and no process is writing the data directory.
  Port closure alone is not proof that all writers have stopped.
- Record the exact binary/build revision, configuration, cluster ID, node ID,
  advertised endpoints, filesystem ownership, and backup time. Store secrets
  with appropriate access controls, not in public manifests or logs.
- Copy the **entire** configured `dataDir` to a new backup location, including
  Raft WAL, snapshot/membership files, and `state.rocksdb`. Do not copy just SST
  files, omit hidden files, or merge files from different capture times.
- Ensure that external mounts or symlinks do not leave required data outside
  the capture. Preserve permissions and ownership. Generate a relative-path
  file inventory with SHA-256 checksums and verify the copy before restarting.
- Retain an immutable, access-controlled off-host copy and its build/config
  records. Local file hashes alone are not protection against loss of the host,
  tampering with both data and manifest, or unreliable storage durability.

### Restore

- Keep the original identity offline and clients fenced throughout validation.
  Never run the original and restored copies concurrently in the same network.
- Verify the artifact inventory and checksums. Restore a copy to a new empty
  directory; never overwrite a live directory or start directly on the sole
  backup. Preserve the original artifact for subsequent attempts.
- Start the recorded version with the recorded cluster/node identity and peer
  endpoints, pointing `--data-dir` to the restored directory. Do not improvise
  new membership, reuse this as a clone, or downgrade snapshot formats.
- Verify service availability, application records, ACLs, Stat/version values,
  and a controlled new write/read. Reopen client traffic only after application
  owners have accepted the recovery point and any downstream reconciliation.
- If recovery fails, stop the restored instance and retain logs/artifacts for
  diagnosis; do not automatically delete data or bootstrap an empty cluster.

## Production gates still open

- Online checkpoint/export and consistent multi-node backup/recovery.
- Forced Raft snapshot creation and restore coverage in this subprocess drill;
  its small workload primarily exercises WAL/RocksDB directory recovery.
- Different hosts/endpoints, membership replacement, and version changes.
- Session resumption, ephemeral nodes, watch restoration, and external fencing
  tokens across an offline rollback.
- Measured RPO/RTO at production data sizes and failure conditions.
- Backup artifact packaging, durable publication, retention, encryption, and
  deployment automation.

**Rollback loses writes after the backup and can reuse sequence numbers,
versions, and zxid ranges relative to the discarded history.** This is unsafe to
use transparently for locks or fencing without application-level reconciliation.
Do not infer zero data loss, globally monotonic fencing tokens across restore,
or recoverability of a quorum by copying one node into multiple identities.
Java ZooKeeper cannot consume this native directory; export back to Java remains
unsupported.
