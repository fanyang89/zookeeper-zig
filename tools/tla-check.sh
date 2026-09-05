#!/usr/bin/env bash
# Runs TLC bounded model checking for the TLA+ specifications in tla/.
#
# The TLA+ toolchain is pinned by release tag and SHA-256; the jar is cached
# under ~/.cache/zookeeper-zig and re-verified on every run.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo/tla"

version=1.7.0
url="https://github.com/tlaplus/tlaplus/releases/download/v${version}/tla2tools.jar"
expected=8cce75caa1e59d0b0483bb8fb881ba33825edce8b2d98aba59d66ce685dd3d1a
cache="${XDG_CACHE_HOME:-$HOME/.cache}/zookeeper-zig/tla2tools-${version}.jar"

mkdir -p "$(dirname "$cache")"
if [[ ! -f $cache ]]; then
    curl -sSL -o "$cache" "$url"
fi
echo "${expected}  ${cache}" | sha256sum -c - >/dev/null || {
    echo "tla2tools.jar checksum mismatch: ${cache}" >&2
    exit 1
}

workers=$(nproc)
run() {
    local spec=$1
    local cfg=$2
    echo "=== TLC ${spec} ==="
    java -cp "$cache" tlc2.TLC -nowarning -workers "$workers" -config "$cfg" "$spec"
}

run SessionTracker.tla MCSessionTracker.cfg
run ContainerCleanup.tla MCContainerCleanup.cfg
run MultiAtomicity.tla MCMultiAtomicity.cfg
run LinearizableRead.tla MCLinearizableRead.cfg
run WatchDelivery.tla MCWatchDelivery.cfg
