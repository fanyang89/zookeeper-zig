#!/usr/bin/env bash
# Symbolically checks the session tracker with bounds larger than the TLC
# configuration. The upstream Apalache release is pinned and verified.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
version=0.62.2
archive_sha256=765f610537281a0f25b8c30f2554f19523e2859c824e80e62276653ee23c10e2
url="https://github.com/apalache-mc/apalache/releases/download/v${version}/apalache-${version}.tgz"
cache_root="${XDG_CACHE_HOME:-$HOME/.cache}/zookeeper-zig"
archive="${cache_root}/apalache-${version}.tgz"
home="${cache_root}/apalache-${version}"

mkdir -p "$cache_root"
if [[ ! -f $archive ]]; then
    curl -fsSL --retry 3 --retry-delay 2 -o "${archive}.tmp" "$url"
    mv "${archive}.tmp" "$archive"
fi
if ! echo "${archive_sha256}  ${archive}" | sha256sum -c - >/dev/null; then
    echo "Apalache archive checksum mismatch: ${archive}" >&2
    exit 1
fi

installed_sha256=$(cat "${home}/.archive-sha256" 2>/dev/null || true)
if [[ ! -x ${home}/bin/apalache-mc || $installed_sha256 != "$archive_sha256" ]]; then
    rm -rf "${home}.tmp"
    mkdir -p "${home}.tmp"
    tar -xzf "$archive" -C "${home}.tmp" --strip-components=1
    echo "$archive_sha256" >"${home}.tmp/.archive-sha256"
    rm -rf "$home"
    mv "${home}.tmp" "$home"
fi

scratch_root=${TMPDIR:-$HOME/tmp}
mkdir -p "$scratch_root"
scratch=$(mktemp -d "${scratch_root}/zookeeper-zig-apalache.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
log="${scratch}/apalache.log"

cd "$repo/tla"
echo "=== Apalache SessionTracker.tla (length 25) ==="
if ! "$home/bin/apalache-mc" \
    --out-dir="${scratch}/out" \
    check \
    --config=MCSessionTrackerApalache.cfg \
    --length=25 \
    --no-deadlock \
    SessionTracker.tla >"$log" 2>&1
then
    cat "$log" >&2
    exit 1
fi

grep -E 'PASS #[0-9]+:|The outcome is:|Checker reports|EXITCODE:' "$log"
