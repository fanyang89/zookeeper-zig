# Fatal Raft driver handling

A terminal `Raftor.tick` error marks the quorum driver failed and stops Raft.
The client server observes that flag every 100 ms using a cancelable health
watch alongside its accept loop. On failure it cancels the accept task, closes
the listener, cancels client connections, and returns `RaftDriverFailed`.
The executable propagates that error after cleanup, exiting unsuccessfully.
When enabled, the AdminServer task is canceled by the executable's cleanup.

The 100 ms interval is detection cadence, not a hard process-exit deadline:
thread scheduling and connection/worker/Raft cleanup may take longer. External
service supervision should also enforce a shutdown deadline.

Election, startup without a leader, and temporary loss of quorum do not set
this terminal flag and do not trigger a restart. Restarting on those conditions
can make a recoverable outage worse.

## Health checks and supervision

- Do not use an open TCP port or `ruok` as readiness. `ruok` retains its upstream
  liveness semantics; it does not prove quorum availability.
- State-dependent 4lw/AdminServer output is marked not serving once the driver
  fails, even if the cached Raft status still names a leader. This is not a new
  HTTP readiness endpoint or a guarantee that every read-index will succeed.
- Alert on `ZooKeeper Raft driver stopped` and repeated unsuccessful exits.
  Keep logs from before the failure, particularly snapshot and storage errors.
- For systemd, configure `Restart=on-failure`, a nonzero `RestartSec`, and start
  rate limits. In Kubernetes, retain restart backoff and use a readiness check
  that reflects service availability, not merely process existence.
- A restart does not repair corrupt snapshots, invalid persisted state, full
  disks, or failed storage. Repeated failure requires diagnosis or a tested
  restore/replacement procedure; do not delete the data directory automatically.

No service manager or cluster configuration is installed by this change.

## Verification boundary

The native regression starts the real client accept loop without an elected
leader, keeps idle TCP clients connected across multiple health polls, then
injects the same atomic terminal flag set by the driver failure path. It checks
not-serving output, bounded return of `RaftDriverFailed`, and a closed listening
port.

The Linux subprocess regression `tests/faults/storage_failure.py` starts the real
executable, confirms a persistent write and read, opens idle clients, then uses
`prlimit(RLIMIT_FSIZE)` to make subsequent regular-file writes fail with `EFBIG`.
`SIGXFSZ` is ignored in the child so signal termination cannot satisfy the test.
The parent remains unrestricted, and no filesystem is filled or remounted.

Both AdminServer-disabled and AdminServer-enabled scenarios require:

- no successful acknowledgement of the write attempted under the fault;
- a logged `WalWriteFailed`, followed by `RaftDriverFailed` and a positive
  (non-signal) process exit status within the deadline;
- refusal of new connections to the client and enabled admin ports;
- a fresh unrestricted process, using the same identity and untouched data
  directory, reading the acknowledged pre-fault value and accepting new writes.

Run with `mise run test-storage-failure` or
`zig build test-storage-failure -Dsystem-rocksdb=true --summary all`. Python 3
and Linux `prlimit` support are required. CI runs it in the Unit tests job.
The former behavior at `3239589` fails this regression by remaining alive past
the exit deadline; the driver-exit implementation passes with both bundled and
system RocksDB.

This is a real syscall failure but not a physical disk-failure simulation. It
covers WAL write failure, not targeted RocksDB fsync/compaction errors, ENOSPC,
EIO, or corrupted media. Restart is performed by the test parent, not systemd or
Kubernetes; deployment-specific backoff and restart-policy testing remain open.
It also does not establish backup/restore capability, multi-node recovery, or
any outcome guarantee for unacknowledged operations.
