#!/usr/bin/env bash
# Bounded deterministic fuzz smoke tests. Uses fixed seeds so a failure in
# CI reproduces locally.
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 PATH_TO_ZQ_FUZZ_IMPORT PATH_TO_ZQ_FUZZ_WIRE" >&2
    exit 2
fi

import_fuzzer=$(realpath "$1")
wire_fuzzer=$(realpath "$2")
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

"$import_fuzzer" --target snapshot --iterations 3000 --seed 0xC0FFEE
"$import_fuzzer" --target txnlog --iterations 3000 --seed 0xBEEF
"$import_fuzzer" --target snappy --iterations 3000 --seed 0x5EED
"$import_fuzzer" --target gzip --iterations 3000 --seed 0xD00D
"$wire_fuzzer" --target frame --iterations 3000 --seed 0xF1A7
"$wire_fuzzer" --target jute-record --iterations 3000 --seed 0x0FF5E7

# Replay the committed regression corpus.
if compgen -G "tests/fuzz/corpus/*/*/*.bin" > /dev/null; then
    "$import_fuzzer" --replay tests/fuzz/corpus/import/*/*.bin
    "$wire_fuzzer" --replay tests/fuzz/corpus/wire/*/*.bin
fi
