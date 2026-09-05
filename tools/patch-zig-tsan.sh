#!/usr/bin/env bash
# Work around Zig 0.16.0's vendored libtsan including obsolete Linux SCC
# declarations. The code is unused, but prevents the runtime from compiling
# on systems whose selected libc header set does not expose linux/scc.h.
set -euo pipefail

if [[ -n ${ZIG_LIB_DIR:-} ]]; then
    lib_dir=$ZIG_LIB_DIR
else
    lib_dir=$(zig env | sed -n 's/.*\.lib_dir = "\([^"]*\)".*/\1/p')
fi
if [[ -z $lib_dir ]]; then
    echo "could not determine Zig lib directory" >&2
    exit 1
fi

source_file="${lib_dir}/libtsan/sanitizer_common/sanitizer_platform_limits_posix.cpp"
if [[ ! -f $source_file ]]; then
    echo "Zig libtsan source not found: ${source_file}" >&2
    exit 1
fi

include_count=$(grep -c '^#include <linux/scc\.h>$' "$source_file" || true)
modem_count=$(grep -Ec '^[[:space:]]*unsigned struct_scc_modem_sz = sizeof\(struct scc_modem\);$' "$source_file" || true)
stat_count=$(grep -Ec '^[[:space:]]*unsigned struct_scc_stat_sz = sizeof\(struct scc_stat\);$' "$source_file" || true)

if ((include_count == 0 && modem_count == 0 && stat_count == 0)); then
    echo "Zig libtsan SCC workaround already applied: ${source_file}"
    exit 0
fi
if ((include_count != 1 || modem_count != 1 || stat_count != 1)); then
    echo "unexpected Zig libtsan SCC source shape; refusing partial patch" >&2
    echo "include=${include_count} modem=${modem_count} stat=${stat_count}" >&2
    exit 1
fi
if [[ ! -w $source_file || ! -w $(dirname "$source_file") ]]; then
    echo "Zig libtsan source is not writable: ${source_file}" >&2
    exit 1
fi

temporary="${source_file}.tmp.$$"
trap 'rm -f "$temporary"' EXIT
sed \
    -e '/^#include <linux\/scc\.h>$/d' \
    -e '/^[[:space:]]*unsigned struct_scc_modem_sz = sizeof(struct scc_modem);$/d' \
    -e '/^[[:space:]]*unsigned struct_scc_stat_sz = sizeof(struct scc_stat);$/d' \
    "$source_file" >"$temporary"
chmod --reference="$source_file" "$temporary"
mv "$temporary" "$source_file"
trap - EXIT

echo "Applied Zig libtsan SCC workaround: ${source_file}"
