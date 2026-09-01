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
docker_log="$run_root/docker-command-failures.log"
mkdir -p "$(dirname "$binary")"
cp "$server" "$binary"
chmod +x "$binary"

docker_cleanup_command() {
    local command_timeout=${JEPSEN_DOCKER_CLEANUP_TIMEOUT:-60}
    timeout --kill-after=5s "${command_timeout}s" docker "$@"
}

cleanup() {
    status=$?
    docker_cleanup_safe=true
    if [[ ${needs_docker_nodes:-false} == true ]] && command -v docker >/dev/null; then
        node_containers=()
        if container_output=$(docker_cleanup_command ps --all --quiet \
            --filter "label=zookeeper-zig.jepsen.run=${run_id:-unknown}"); then
            while IFS= read -r container; do
                [[ -n $container ]] && node_containers+=("$container")
            done <<<"$container_output"
        else
            echo "Failed to discover Docker node containers" >&2
            docker_cleanup_safe=false
            status=1
        fi
        if ((${#node_containers[@]})); then
            mkdir -p "$run_root/data"
            for container in "${node_containers[@]}"; do
                if ! docker_cleanup_command logs "$container" \
                    >"$run_root/node-docker-$container.log" 2>&1; then
                    docker_cleanup_safe=false
                fi
                if ! docker_cleanup_command cp "$container:/var/lib/zookeeper-zig" \
                    "$run_root/data/$container" >/dev/null 2>&1; then
                    docker_cleanup_safe=false
                fi
            done
            if [[ $docker_cleanup_safe == true ]]; then
                if ! docker_cleanup_command rm --force "${node_containers[@]}" \
                    >/dev/null 2>&1; then
                    echo "Failed to remove Docker node containers" >&2
                    docker_cleanup_safe=false
                    status=1
                fi
            else
                echo "Docker artifacts could not be preserved; leaving containers intact" >&2
                status=1
            fi
        fi
        node_networks=()
        if network_output=$(docker_cleanup_command network ls --quiet \
            --filter "label=zookeeper-zig.jepsen.run=${run_id:-unknown}"); then
            while IFS= read -r network; do
                [[ -n $network ]] && node_networks+=("$network")
            done <<<"$network_output"
        else
            echo "Failed to discover Docker node networks" >&2
            docker_cleanup_safe=false
            status=1
        fi
        if [[ $docker_cleanup_safe == true ]] && ((${#node_networks[@]})); then
            for network in "${node_networks[@]}"; do
                attached_containers=()
                if attached_output=$(docker_cleanup_command network inspect --format \
                    '{{range .Containers}}{{.Name}}{{"\n"}}{{end}}' "$network"); then
                    while IFS= read -r container; do
                        [[ -n $container ]] && attached_containers+=("$container")
                    done <<<"$attached_output"
                else
                    echo "Failed to inspect Docker node network $network" >&2
                    status=1
                    continue
                fi
                for container in "${attached_containers[@]}"; do
                    if ! docker_cleanup_command network disconnect --force \
                        "$network" "$container" >/dev/null 2>&1; then
                        echo "Failed to disconnect $container from $network" >&2
                        status=1
                    fi
                done
            done
            if ! docker_cleanup_command network rm "${node_networks[@]}" \
                >/dev/null 2>&1; then
                echo "Failed to remove Docker node networks" >&2
                status=1
            fi
        fi
    fi
    if [[ $status -eq 0 ]]; then
        rm -rf "$run_root"
    else
        echo "Jepsen failure logs preserved for artifact upload: $run_root" >&2
    fi
    trap - EXIT
    exit "$status"
}
trap cleanup EXIT

nodes=${JEPSEN_NODES:-n1,n2,n3}
time_limit=${JEPSEN_TIME_LIMIT:-30}
concurrency=${JEPSEN_CONCURRENCY:-6}
test_count=${JEPSEN_TEST_COUNT:-1}
command=${JEPSEN_COMMAND:-test}
runner=${JEPSEN_RUNNER:-auto}
maven_mirror=${JEPSEN_MAVEN_MIRROR:-aliyun}
node_image=${JEPSEN_NODE_IMAGE:-zookeeper-zig-jepsen-node:0.1}
run_id=$(basename "$run_root")
forwarded_args=("$@")
needs_docker_nodes=false
if [[ $command == test-all || ${JEPSEN_NEMESIS:-} == partition-one ]]; then
    needs_docker_nodes=true
fi
for ((index = 0; index < ${#forwarded_args[@]}; index++)); do
    argument=${forwarded_args[index]}
    if [[ $argument == --nemesis=partition-one ]]; then
        needs_docker_nodes=true
    elif [[ $argument == --nemesis ]] \
        && ((index + 1 < ${#forwarded_args[@]})) \
        && [[ ${forwarded_args[$((index + 1))]} == partition-one ]]; then
        needs_docker_nodes=true
    fi
done
use_aliyun_mirrors=${JEPSEN_USE_ALIYUN_MIRRORS:-true}
proxy=${JEPSEN_PROXY:-${https_proxy:-${HTTPS_PROXY:-${http_proxy:-${HTTP_PROXY:-}}}}}
if [[ -n $proxy ]]; then
    export http_proxy="$proxy"
    export https_proxy="$proxy"
    export HTTP_PROXY="$proxy"
    export HTTPS_PROXY="$proxy"
fi
case "$use_aliyun_mirrors" in
    true | false)
        ;;
    *)
        echo "unsupported JEPSEN_USE_ALIYUN_MIRRORS: $use_aliyun_mirrors" >&2
        exit 2
        ;;
esac
case "$command" in
    test | test-all)
        ;;
    *)
        echo "unsupported JEPSEN_COMMAND: $command" >&2
        exit 2
        ;;
esac
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
    "$command"
    --no-ssh
    --nodes "$nodes"
    --time-limit "$time_limit"
    --concurrency "$concurrency"
    --test-count "$test_count"
)
if [[ -n ${JEPSEN_WORKLOAD:-} ]]; then
    common_args+=(--workload "$JEPSEN_WORKLOAD")
fi
if [[ -n ${JEPSEN_NEMESIS:-} ]]; then
    common_args+=(--nemesis "$JEPSEN_NEMESIS")
fi
common_args+=("${forwarded_args[@]}")

prepare_node_image() {
    if [[ $needs_docker_nodes != true ]]; then
        return
    fi
    command -v docker >/dev/null || {
        echo "partition-one requires the Docker CLI" >&2
        exit 2
    }
    command -v timeout >/dev/null || {
        echo "partition-one requires GNU timeout" >&2
        exit 2
    }
    docker info >/dev/null
    if [[ -z ${JEPSEN_NODE_IMAGE:-} ]]; then
        local proxy_args=()
        if [[ -n $proxy ]]; then
            proxy_args+=(
                --network host
                --build-arg "http_proxy=$proxy"
                --build-arg "https_proxy=$proxy"
                --build-arg "HTTP_PROXY=$proxy"
                --build-arg "HTTPS_PROXY=$proxy"
            )
        fi
        docker build \
            "${proxy_args[@]}" \
            --tag "$node_image" \
            --file "$test_dir/Dockerfile.node" \
            "$test_dir"
    fi
}

run_host() (
    cd "$test_dir"
    ZOOKEEPER_ZIG_SERVER="$binary" \
    ZOOKEEPER_ZIG_RUN_DIR="$run_root/data" \
    ZOOKEEPER_ZIG_DOCKER_LOG="$docker_log" \
    ZOOKEEPER_ZIG_NODE_IMAGE="$node_image" \
    ZOOKEEPER_ZIG_RUN_ID="$run_id" \
        "${lein_args[@]}" run "${common_args[@]}"
)

run_container() {
    local runtime=$1
    local default_image=zookeeper-zig-jepsen:0.4
    local image=${JEPSEN_IMAGE:-$default_image}
    local cache_root=${JEPSEN_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/zookeeper-zig/jepsen}
    local container_binary=/workspace/${binary#"$repo/"}
    local container_run_root=/workspace/${run_root#"$repo/"}
    mkdir -p "$cache_root"
    local build_proxy_args=()
    local run_proxy_args=()
    if [[ -n $proxy ]]; then
        build_proxy_args+=(
            --network host
            --build-arg "http_proxy=$proxy"
            --build-arg "https_proxy=$proxy"
            --build-arg "HTTP_PROXY=$proxy"
            --build-arg "HTTPS_PROXY=$proxy"
        )
        run_proxy_args+=(
            -e "http_proxy=$proxy"
            -e "https_proxy=$proxy"
            -e "HTTP_PROXY=$proxy"
            -e "HTTPS_PROXY=$proxy"
        )
    fi
    if [[ -z ${JEPSEN_IMAGE:-} ]]; then
        "$runtime" build \
            "${build_proxy_args[@]}" \
            --build-arg "USE_ALIYUN_MIRRORS=$use_aliyun_mirrors" \
            --tag "$image" \
            --file "$test_dir/Dockerfile" \
            "$test_dir"
    fi
    local docker_node_args=()
    if [[ $needs_docker_nodes == true ]]; then
        if [[ $runtime != docker ]]; then
            echo "partition-one requires Docker rather than $runtime" >&2
            exit 2
        fi
        [[ -S /var/run/docker.sock ]] || {
            echo "partition-one requires /var/run/docker.sock" >&2
            exit 2
        }
        docker_node_args+=(
            --group-add "$(stat -c %g /var/run/docker.sock)"
            -v /var/run/docker.sock:/var/run/docker.sock
            -e ZOOKEEPER_ZIG_NODE_IMAGE="$node_image"
            -e ZOOKEEPER_ZIG_RUN_ID="$run_id"
        )
    fi
    "$runtime" run --rm --init --network host \
        --security-opt seccomp=unconfined \
        --user "$(id -u):$(id -g)" \
        -e HOME=/tmp/jepsen-home \
        -e LEIN_HOME=/tmp/jepsen-home/.lein \
        -e ZOOKEEPER_ZIG_SERVER="$container_binary" \
        -e ZOOKEEPER_ZIG_RUN_DIR="$container_run_root/data" \
        -e ZOOKEEPER_ZIG_DOCKER_LOG="$container_run_root/docker-command-failures.log" \
        "${docker_node_args[@]}" \
        "${run_proxy_args[@]}" \
        -v "$cache_root:/tmp/jepsen-home" \
        -v "$repo:/workspace" \
        -w /workspace/tests/jepsen \
        "$image" "${lein_args[@]}" run "${common_args[@]}"
}

if [[ $runner == auto ]]; then
    if command -v lein >/dev/null; then
        runner=host
    elif command -v docker >/dev/null && docker info >/dev/null 2>&1; then
        runner=docker
    elif command -v podman >/dev/null; then
        runner=podman
    else
        echo "Jepsen requires Leiningen, Docker, or Podman" >&2
        exit 2
    fi
fi

prepare_node_image

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
    *)
        echo "unsupported JEPSEN_RUNNER: $runner" >&2
        exit 2
        ;;
esac
