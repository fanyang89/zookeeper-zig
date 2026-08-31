#!/usr/bin/env bash
set -euo pipefail

export JEPSEN_COMMAND=test-all
exec "$(dirname "${BASH_SOURCE[0]}")/run.sh" "$@"
