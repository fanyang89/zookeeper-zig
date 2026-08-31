# Jepsen tests

This suite checks ZooKeeper Zig with Jepsen 0.3.13 and the official Apache
ZooKeeper 3.9.5 Java client.

The smoke workload models one linearizable compare-and-set register. Concurrent
clients issue reads, unconditional writes, and versioned compare-and-set
operations while the nemesis repeatedly kills and restarts one member of a
three-node quorum. Knossos checks the resulting history for linearizability.

## Run

```sh
mise run test-jepsen-smoke
```

The default runner uses local Leiningen when available, then Docker or Podman.
The container runner uses host networking and builds the local
`zookeeper-zig-jepsen:0.3` image from `Dockerfile`. Its Noble-based
Clojure/Leiningen base image is digest-pinned, and Git plus gnuplot are
installed for Jepsen reports. Local runs use Aliyun mirrors for Ubuntu packages
and Maven Central artifacts by default; Clojars remains the source for Jepsen
artifacts. GitHub Actions uses the official upstream repositories. Set
`JEPSEN_MAVEN_MIRROR=central` and `JEPSEN_USE_ALIYUN_MIRRORS=false` to select
the same behavior locally. Override the runtime or use a prebuilt image with
`JEPSEN_RUNNER` and `JEPSEN_IMAGE`.

Useful settings:

```sh
JEPSEN_TIME_LIMIT=120 \
JEPSEN_CONCURRENCY=12 \
JEPSEN_TEST_COUNT=3 \
mise run test-jepsen-smoke
```

`JEPSEN_NODES` must contain an odd number of at least three logical node names.
Each run allocates loopback ports dynamically and gives every server an
independent data directory. No SSH or root privileges are required for this
process-failure workload. The container runs with its seccomp filter disabled
because grpc-lite's libxev backend requires the `io_uring` syscalls blocked by
Docker's default profile; only trusted test binaries should run in this image.

Jepsen histories and checker output are written under `tests/jepsen/store/`.
Server data and logs are deleted after successful runs. Failed runs preserve
server state under `tests/jepsen/target/` and print the tail of each node log.

GitHub Actions runs one 30-second smoke test for matching pushes and pull
requests. The nightly schedule runs three 120-second tests. Manual runs accept
workload duration and test-count inputs, and always upload Jepsen reports plus
preserved failure logs.

This smoke suite injects process failures only. Network partitions, pauses,
session-expiration workloads, and multi-key workloads belong in later suites.
