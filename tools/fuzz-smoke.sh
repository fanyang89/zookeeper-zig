#!/usr/bin/env bash
# Bounded deterministic fuzz smoke test for the Apache ZooKeeper import
# parser. Uses fixed seeds so a failure in CI reproduces locally.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 PATH_TO_ZQ_FUZZ_IMPORT" >&2
    exit 2
fi

fuzzer=$(realpath "$1")
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

"$fuzzer" --target snapshot --iterations 3000 --seed 0xC0FFEE
"$fuzzer" --target txnlog --iterations 3000 --seed 0xBEEF
"$fuzzer" --target snappy --iterations 3000 --seed 0x5EED
"$fuzzer" --target gzip --iterations 3000 --seed 0xD00D

# Replay the committed regression corpus.
if compgen -G "tests/fuzz/corpus/import/*/*.bin" > /dev/null; then
    "$fuzzer" --replay tests/fuzz/corpus/import/*/*.bin
fi
