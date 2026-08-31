# Jepsen tests

This suite checks ZooKeeper Zig with Jepsen 0.3.13 and the official Apache
ZooKeeper 3.9.5 Java client.

## Coverage

The default smoke test runs a single linearizable compare-and-set register.
Concurrent clients issue reads, unconditional writes, and versioned
compare-and-set operations while the nemesis repeatedly kills and restarts one
member of a three-node quorum. Knossos checks the history for linearizability.

The full suite adds:

- `presence`: a linearizable presence register backed by `exists`, persistent
  `create`, and versioned `delete`;
- `independent-register`: independent CAS registers on multiple znodes, checked
  separately with `jepsen.independent`;
- `set`: concurrent unique child creation and `getChildren` reads, checked by
  Jepsen's linearizable full-set checker;
- `pause-one`: repeated `SIGSTOP`/`SIGCONT` of one server process while the
  register workload runs.

The full suite runs the four workloads with `kill-one`, plus the register with
`pause-one`. Network partitions, transactions, and watches are excluded because
the local loopback harness cannot isolate per-node traffic and the server does
not yet dispatch `multi` or watch operations.

## Run

Run the short register smoke test:

```sh
mise run test-jepsen-smoke
```

Run the full workload and nemesis suite:

```sh
mise run test-jepsen
```

Run one configuration directly through the smoke task:

```sh
JEPSEN_TIME_LIMIT=60 \
JEPSEN_WORKLOAD=presence \
JEPSEN_NEMESIS=pause-one \
mise run test-jepsen-smoke
```

Supported workloads are `register`, `presence`, `independent-register`, and
`set`. Supported nemeses are `kill-one` and `pause-one`. The independent
register requires concurrency to be divisible by three.

The default runner uses local Leiningen when available, then Docker or Podman.
The container runner uses host networking and builds the local
`zookeeper-zig-jepsen:0.3` image from `Dockerfile`. Its Noble-based
Clojure/Leiningen base image is digest-pinned, and Git plus gnuplot are
installed for Jepsen reports. Local runs use Aliyun mirrors for Ubuntu packages
and Maven Central artifacts plus the TUNA Clojars mirror by default. GitHub
Actions uses the official upstream repositories. Set
`JEPSEN_MAVEN_MIRROR=central` and `JEPSEN_USE_ALIYUN_MIRRORS=false` to select
the same behavior locally. Override the runtime or use a prebuilt image with
`JEPSEN_RUNNER` and `JEPSEN_IMAGE`.

For local proxy-based development, set `JEPSEN_PROXY` (or the standard
`http_proxy`/`https_proxy` variables). The runner forwards it to Leiningen and
the runtime container; Docker image builds use host networking so a loopback
proxy remains reachable:

```sh
JEPSEN_PROXY=http://127.0.0.1:7890 mise run test-jepsen-smoke
```

Useful settings:

```sh
JEPSEN_TIME_LIMIT=120 \
JEPSEN_CONCURRENCY=12 \
JEPSEN_TEST_COUNT=3 \
mise run test-jepsen
```

Set `JEPSEN_COMMAND=test-all` when invoking `run.sh` through another build
entry point. `JEPSEN_NODES` must contain an odd number of at least three logical
node names. Each run allocates loopback ports dynamically and gives every
server an independent data directory. No SSH or root privileges are required
for these process-failure workloads. The container runs with its seccomp filter
disabled because grpc-lite's libxev backend requires the `io_uring` syscalls
blocked by Docker's default profile; only trusted test binaries should run in
this image.

Jepsen histories and checker output are written under `tests/jepsen/store/`.
Server data and logs are deleted after successful runs. Failed runs preserve
server state under `tests/jepsen/target/` and print the tail of each node log.

GitHub Actions runs one 30-second register smoke test for matching pushes and
pull requests. The nightly schedule runs the full suite with 120 seconds per
configuration. Manual runs can select smoke or full coverage and configure the
workload duration and test count. Every run uploads Jepsen reports and preserved
failure logs.
