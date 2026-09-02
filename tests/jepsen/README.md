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
- `unique-ids`: persistent sequential creates checked for duplicate IDs;
- `counter`: atomic sequential increments checked against concurrent reads;
- `total-queue`: unique enqueues and atomic multi-based dequeues, followed by a
  final drain checked for lost or unexpected values;
- `linear-queue`: the same queue operations checked for linearizability with an
  unordered queue model;
- `pause-one`: repeated `SIGSTOP`/`SIGCONT` of one server process;
- `partition-one`: isolation of one Docker-backed member from both Raft peers
  while clients retain access to every node;
- `kill-all` and `pause-all`: full-cluster crash and suspension followed by
  recovery.

The register workload and one-versus-majority partition shape from
[`jepsen-io/zookeeper`](https://github.com/jepsen-io/zookeeper) are covered by
the register workload and `partition-one`. The `unique-ids`, `counter`, and
all-node fault scenarios are clean-room adaptations of the
[ClickHouse Keeper Jepsen suite](https://github.com/ClickHouse/ClickHouse/tree/master/tests/jepsen.clickhouse/src/jepsen/clickhouse/keeper).
The queue workloads use the Keeper suite's version-checked write-multi pattern
to serialize concurrent removals. `partition-one` gives every server its own
Docker network namespace and uses container-local firewall rules to isolate
Raft peers without blocking Jepsen clients. Storage corruption and dedicated
watch workloads remain excluded.

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

Supported workloads are `register`, `presence`, `independent-register`, `set`,
`unique-ids`, `counter`, `total-queue`, and `linear-queue`. Supported nemeses are
`kill-one`, `pause-one`, `partition-one`, `kill-all`, and `pause-all`. The
independent register requires concurrency to be divisible by three.

The default runner uses local Leiningen when available, then Docker or Podman.
The container runner uses host networking and builds the local
`zookeeper-zig-jepsen:0.4` image from `Dockerfile`. Its Noble-based
Clojure/Leiningen base image is digest-pinned, and Docker CLI, Git, and gnuplot
are installed for orchestration and reports. Local runs use Aliyun mirrors for
Ubuntu packages and Maven Central artifacts plus the TUNA Clojars mirror by
default. GitHub
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
node names. Process-failure runs allocate loopback ports dynamically and give
every server an independent data directory without SSH or root privileges.
`partition-one` builds `Dockerfile.node`, starts one sibling container per
server on a private bridge, and requires access to the Docker socket. Node
containers receive only `NET_ADMIN` plus an unconfined seccomp profile; the
firewall blocks peer container addresses while retaining client access. Docker
socket access is host-equivalent, so partition tests must run only trusted code.
The unconfined profile is required because grpc-lite's libxev backend uses
`io_uring` syscalls blocked by Docker's default profile.

Jepsen histories and checker output are written under `tests/jepsen/store/`.
Server data and logs are deleted after successful runs. Failed runs preserve
server state, node logs, and Docker command failures under
`tests/jepsen/target/` for artifact upload without dumping node logs to the
console. Generate a standalone local HTML index with:

```sh
cd tests/jepsen
lein run -m zookeeper-zig.report store report
```

The report is written to `tests/jepsen/report/index.html` and links to each
run's timeline, checker result, history, log, and performance graphs.

GitHub Actions runs one 30-second register smoke test for matching pushes and
pull requests without exposing the Docker socket. The nightly schedule and
trusted manual full runs execute the complete suite, including `partition-one`,
with Docker socket access and 120 seconds per configuration by default. Manual
runs can configure the workload duration and test count. Every run uploads
Jepsen reports and preserved failure logs. Nightly and trusted manual full runs
also deploy the latest full-suite HTML report to GitHub Pages and show an
`Open HTML report` link in the deployment job summary. Smoke runs do not deploy
to Pages, so they cannot replace the full-suite report. Set the repository's
Pages source to **GitHub Actions** once before the first deployment.
