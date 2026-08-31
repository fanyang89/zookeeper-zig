#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 PATH_TO_ZOOKEEPER_SERVER [JEPSEN_ARGS...]" >&2
    exit 2
fi

server=$(realpath "$1")
shift
test_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(realpath "$test_dir/../..")
run_root="$test_dir/target/run.$(date +%s).$$"
binary="$run_root/bin/zookeeper-quorum-server"
mkdir -p "$(dirname "$binary")"
cp "$server" "$binary"
chmod +x "$binary"

cleanup() {
    status=$?
    if [[ $status -eq 0 ]]; then
        rm -rf "$run_root"
    else
        echo "Jepsen run data preserved at: $run_root" >&2
        find "$run_root" -type f -name 'node-*.log' \
            -exec sh -c 'echo "--- $1 ---"; tail -n 200 "$1"' _ {} \; \
            2>/dev/null >&2 || true
    fi
}
trap cleanup EXIT

nodes=${JEPSEN_NODES:-n1,n2,n3}
time_limit=${JEPSEN_TIME_LIMIT:-30}
concurrency=${JEPSEN_CONCURRENCY:-6}
test_count=${JEPSEN_TEST_COUNT:-1}
runner=${JEPSEN_RUNNER:-auto}
maven_mirror=${JEPSEN_MAVEN_MIRROR:-aliyun}
lein_args=(lein)
case "$maven_mirror" in
    aliyun)
        lein_args+=(with-profile +aliyun)
        ;;
    central | none)
        ;;
    *)
        echo "unsupported JEPSEN_MAVEN_MIRROR: $maven_mirror" >&2
        exit 2
        ;;
esac
common_args=(
    test
    --no-ssh
    --nodes "$nodes"
    --time-limit "$time_limit"
    --concurrency "$concurrency"
    --test-count "$test_count"
    "$@"
)

run_host() {
    ZOOKEEPER_ZIG_SERVER="$binary" \
    ZOOKEEPER_ZIG_RUN_DIR="$run_root/data" \
        "${lein_args[@]}" run "${common_args[@]}"
}

run_container() {
    local runtime=$1
    local default_image=zookeeper-zig-jepsen:0.3
    local image=${JEPSEN_IMAGE:-$default_image}
    local cache_root=${JEPSEN_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/zookeeper-zig/jepsen}
    local container_binary=/workspace/${binary#"$repo/"}
    local container_run_root=/workspace/${run_root#"$repo/"}
    mkdir -p "$cache_root"
    if [[ -z ${JEPSEN_IMAGE:-} ]]; then
        "$runtime" build --tag "$image" --file "$test_dir/Dockerfile" "$test_dir"
    fi
    "$runtime" run --rm --init --network host \
        --security-opt seccomp=unconfined \
        --user "$(id -u):$(id -g)" \
        -e HOME=/tmp/jepsen-home \
        -e LEIN_HOME=/tmp/jepsen-home/.lein \
        -e ZOOKEEPER_ZIG_SERVER="$container_binary" \
        -e ZOOKEEPER_ZIG_RUN_DIR="$container_run_root/data" \
        -v "$cache_root:/tmp/jepsen-home" \
        -v "$repo:/workspace" \
        -w /workspace/tests/jepsen \
        "$image" "${lein_args[@]}" run "${common_args[@]}"
}

case "$runner" in
    host)
        command -v lein >/dev/null || {
            echo "JEPSEN_RUNNER=host requires Leiningen" >&2
            exit 2
        }
        run_host
        ;;
    docker | podman)
        command -v "$runner" >/dev/null || {
            echo "$runner is not installed" >&2
            exit 2
        }
        run_container "$runner"
        ;;
    auto)
        if command -v lein >/dev/null; then
            run_host
        elif command -v docker >/dev/null && docker info >/dev/null 2>&1; then
            run_container docker
        elif command -v podman >/dev/null; then
            run_container podman
        else
            echo "Jepsen requires Leiningen, Docker, or Podman" >&2
            exit 2
        fi
        ;;
    *)
        echo "unsupported JEPSEN_RUNNER: $runner" >&2
        exit 2
        ;;
esac
