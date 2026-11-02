#!/usr/bin/env bash
# H-0 / H16: ADB and FlowR (CE and EE workspaces) must resolve the shared Arrow /
# DataFusion / Lance stack to the same versions. Fails, listing the differences.
#
#   tests/stack-versions.sh            (flowr-system next to ADB)
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEV="$(dirname "$ROOT")"
LOCKS=("$ROOT/Cargo.lock" "$DEV/flowr-system/flowr-ce/Cargo.lock" "$DEV/flowr-system/flowr/Cargo.lock")
CRATES=(arrow arrow-array arrow-schema arrow-flight arrow-ipc arrow-cast datafusion lance lance-index)

versions() { # lockfile crate -> the versions it resolves (space-separated)
    awk -v c="$2" '$0 == "name = \"" c "\"" { getline; gsub(/version = |"/, ""); print }' "$1" | sort -u | tr '\n' ' '
}

bad=0
for c in "${CRATES[@]}"; do
    seen=""
    for l in "${LOCKS[@]}"; do
        [ -f "$l" ] || { echo "missing $l"; exit 2; }
        v=$(versions "$l" "$c")
        [ -z "$v" ] && continue          # a workspace that doesn't use the crate
        if [ -z "$seen" ]; then seen="$v"; elif [ "$v" != "$seen" ]; then
            echo "MISMATCH $c: $seen vs $v ($l)"; bad=1
        fi
    done
    printf '%-14s %s\n' "$c" "${seen:-—}"
done
exit $bad
