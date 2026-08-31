#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 PATH_TO_ZOOKEEPER_SERVER" >&2
    exit 2
fi

test_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
server_tests=(
    CreateTest
    'StatTest#testBasic+testDataSizeChange'
    NullDataTest
    ACLRootTest
    ClientRetryTest
    'ClientHammerTest#testHammerBasic'
    'SessionTimeoutTest#testSessionRestore+testSessionSurviveServerRestart'
)
join_by_comma() {
    local IFS=,
    echo "$*"
}

export ZOOKEEPER_TEST_SELECTOR="${ZOOKEEPER_SERVER_TEST_SELECTOR:-$(join_by_comma "${server_tests[@]}")}"
exec "$test_dir/run.sh" "$1"
