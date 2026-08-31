#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 PATH_TO_ZOOKEEPER_SERVER" >&2
    exit 2
fi

server=$(realpath "$1")
test_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
version=${ZOOKEEPER_VERSION:-3.9.5}

case "$version" in
    3.9.5)
        release_tag=release-3.9.5
        release_commit=293c895a8d966a3ecb92872be4a1daf87d725da2
        ;;
    *)
        echo "unsupported upstream test version: $version" >&2
        echo "add its verified release tag, commit, and adapter validation first" >&2
        exit 2
        ;;
esac

cache_root=${ZOOKEEPER_INTEROP_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/zookeeper-zig}
source_repo=${ZOOKEEPER_SOURCE_DIR:-$cache_root/apache-zookeeper-$version}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/zookeeper-upstream-interop.XXXXXX")
worktree=$work_dir/zookeeper
worktree_added=false
maven_pid=

cleanup() {
    status=$?
    if [[ -n "$maven_pid" ]]; then
        kill -TERM -- "-$maven_pid" 2>/dev/null || true
    fi
    if [[ $status -ne 0 && -d "$worktree" ]]; then
        echo "--- upstream Surefire reports ---" >&2
        find "$worktree/zookeeper-server/target/surefire-reports" \
            -type f -name '*.txt' -exec tail -n 120 {} \; 2>/dev/null >&2 || true
        echo "--- Zig server logs ---" >&2
        find "$worktree/zookeeper-server/target" \
            -type f \( -name 'zig-server.log' -o -name 'node-*.log' \) \
            -exec sh -c 'echo "# $1"; cat "$1"' _ {} \; \
            2>/dev/null >&2 || true
    fi
    if [[ ${ZOOKEEPER_KEEP_WORKTREE:-false} == true ]]; then
        echo "preserved upstream worktree: $worktree" >&2
    else
        if [[ "$worktree_added" == true ]]; then
            git -C "$source_repo" worktree remove --force "$worktree" >/dev/null 2>&1 || true
        fi
        rm -rf "$work_dir"
    fi
}
trap cleanup EXIT

if [[ -z ${ZOOKEEPER_SOURCE_DIR:-} && ! -d "$source_repo/.git" ]]; then
    mkdir -p "$cache_root"
    git clone --depth 1 --branch "$release_tag" \
        https://github.com/apache/zookeeper.git "$source_repo"
fi

if [[ ! -d "$source_repo/.git" ]]; then
    echo "ZooKeeper source repository not found: $source_repo" >&2
    exit 2
fi
if ! git -C "$source_repo" cat-file -e "$release_commit^{commit}" 2>/dev/null; then
    echo "ZooKeeper source repository does not contain $release_tag ($release_commit)" >&2
    exit 2
fi
actual_commit=$(git -C "$source_repo" rev-parse "$release_commit^{commit}")
if [[ "$actual_commit" != "$release_commit" ]]; then
    echo "unexpected ZooKeeper release commit: $actual_commit" >&2
    exit 2
fi

git -C "$source_repo" worktree add --detach "$worktree" "$release_commit" >/dev/null
worktree_added=true
python3 "$test_dir/patch_client_base.py" \
    "$worktree/zookeeper-server/src/test/java/org/apache/zookeeper/test/ClientBase.java"
overlay_target="$worktree/zookeeper-server/src/test/java/top/fuis/zookeeperzig/interop"
mkdir -p "$overlay_target"
cp "$test_dir/overlay/src/test/java/top/fuis/zookeeperzig/interop/"*.java \
    "$overlay_target/"

async_methods=(
    testAsyncCreate
    testAsyncCreate2
    testAsyncCreateThree
    testAsyncCreateFailure_NodeExists
    testAsyncCreateFailure_NoNode
    testAsyncCreateFailure_NoChildForEphemeral
    testAsyncCreate2Failure_NodeExists
    testAsyncCreate2Failure_NoNode
    testAsyncCreate2Failure_NoChildForEphemeral
    testAsyncDelete
    testAsyncDeleteFailure_NoNode
    testAsyncDeleteFailure_BadVersion
    testAsyncDeleteFailure_NotEmpty
    testAsyncSync
    testAsyncSetACL
    testAsyncSetACLFailure_NoNode
    testAsyncSetACLFailure_BadVersion
    testAsyncSetData
    testAsyncSetDataFailure_NoNode
    testAsyncSetDataFailure_BadVersion
    testAsyncExists
    testAsyncExistsFailure_NoNode
    testAsyncGetACL
    testAsyncGetACLFailure_NoNode
    testAsyncGetChildrenEmpty
    testAsyncGetChildrenSingle
    testAsyncGetChildrenTwo
    testAsyncGetChildrenFailure_NoNode
    testAsyncGetChildren2Empty
    testAsyncGetChildren2Single
    testAsyncGetChildren2Two
    testAsyncGetChildren2Failure_NoNode
    testAsyncGetData
    testAsyncGetDataFailure_NoNode
)
client_methods=(
    testTestability
    testACLs
    testNullAuthId
    testSequentialNodeNames
    testSequentialNodeData
    testLargeNodeData
    testDeleteWithChildren
    testClientCleanup
    testNonExistingOpCode
    testTryWithResources
    testCXidRollover
)
join_by_plus() {
    local IFS=+
    echo "$*"
}
default_test_selector="AsyncOpsTest#$(join_by_plus "${async_methods[@]}"),ClientTest#$(join_by_plus "${client_methods[@]}"),ExtendedTypesInteropTest,QuorumFailoverInteropTest"
test_selector=${ZOOKEEPER_TEST_SELECTOR:-$default_test_selector}

maven_settings_args=()
if [[ -n ${MAVEN_SETTINGS:-} ]]; then
    maven_settings_args=(--settings "$MAVEN_SETTINGS")
else
    case ${ZOOKEEPER_MAVEN_MIRROR:-aliyun} in
        aliyun)
            maven_settings_args=(--settings "$test_dir/maven-settings.aliyun.xml")
            ;;
        central | none)
            ;;
        *)
            echo "unsupported Maven mirror: $ZOOKEEPER_MAVEN_MIRROR" >&2
            exit 2
            ;;
    esac
fi

setsid mvn --batch-mode --no-transfer-progress \
    "${maven_settings_args[@]}" \
    -f "$worktree/pom.xml" \
    -pl zookeeper-server -am \
    -Dtest="$test_selector" \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -Dsurefire-forkcount=1 \
    -Dzookeeper.sasl.client=false \
    -Dzookeeper.zig.server="$server" \
    -Dzookeeper.zig.printServerLog="${ZOOKEEPER_PRINT_SERVER_LOG:-false}" \
    test &
maven_pid=$!
set +e
wait "$maven_pid"
status=$?
set -e
kill -TERM -- "-$maven_pid" 2>/dev/null || true
maven_pid=
exit "$status"
