#!/usr/bin/env bash
# Every ADB gate in one run (MVP plan W1) — run before a commit and before an image.
#
#   tests/check-all.sh            all gates (~15 min in the dev profile)
#   tests/check-all.sh --quick    Rust tests + AQL conformance + FlowR integration only
#
# Needs: cargo, node, the AQL repo next to ADB (../AQL), flowr-system next to ADB
# (../flowr-system) for the FlowR gates, and a copy of an A3T platform LanceDB for the
# platform differential (ADB_CHECK_LANCEDB, default tests/fixtures/a3t-platform.lance;
# skipped if missing). Build output goes to $BUILD_DIR (the PVC), never the CIFS home.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEV="$(dirname "$ROOT")"
BUILD_DIR="${BUILD_DIR:-/var/tmp/a3t-build}"
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$BUILD_DIR/target-adb}"
FLOWR_TARGET="${FLOWR_TARGET:-$BUILD_DIR/target-flowr-ce}"
LANCEDB="${ADB_CHECK_LANCEDB:-$ROOT/tests/fixtures/a3t-platform.lance}"
QUICK=0; [ "${1:-}" = "--quick" ] && QUICK=1
# The embedding model (W4), so the gates run relevance recall too (fetched once).
export ADB_EMBED_MODEL_DIR="${ADB_EMBED_MODEL_DIR:-$("$ROOT/tests/fetch-model.sh")}"

# W10: every ADB the gates start checks each hot-cache answer against Lance, with
# rules that keep every memory type, small enough that buckets are evicted from.
export ADB_CACHE="${ADB_CACHE:-shadow}"
if [ -z "${ADB_CACHE_RULES:-}" ]; then
    ADB_CACHE_RULES=$(cat <<'JSON'
{"max_bytes": "64MB", "rules": [
  {"memory": "working", "keep": {"last": 5}},
  {"memory": "episodic", "keep": {"last": 5}},
  {"memory": "semantic", "keep": {"last": 20}},
  {"memory": "procedural", "keep": "all"},
  {"memory": "tools", "keep": "all"}]}
JSON
)
fi
export ADB_CACHE_RULES

pass=(); fail=()
step() {
    local name="$1"; shift
    local log="$BUILD_DIR/check-$(echo "$name" | tr ' /' '__').log"
    printf '%-34s' "$name"
    local t0=$SECONDS
    if "$@" >"$log" 2>&1; then
        echo "✓  $((SECONDS - t0)) s"; pass+=("$name")
    else
        echo "✗  $((SECONDS - t0)) s — $log"; fail+=("$name"); tail -15 "$log" | sed 's/^/      /'
    fi
}

[ -x "$HOME/bin/build-budget.sh" ] && "$HOME/bin/build-budget.sh" >/dev/null
scratch="$(mktemp -d -p "$BUILD_DIR")"
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$scratch"' EXIT

ADB="$CARGO_TARGET_DIR/debug/adb"
FLOWR="$FLOWR_TARGET/debug/flowr-ce"

step "stack versions (H16)" "$ROOT/tests/stack-versions.sh"
step "build adb" bash -c "cd '$ROOT' && cargo build -p adb-cli"
step "rust tests (workspace)" bash -c "cd '$ROOT' && cargo test --workspace --no-fail-fast"
step "AQL conformance (all suites)" bash -c "
    port=\$(python3 -c 'import socket; s=socket.socket(); s.bind((\"127.0.0.1\",0)); print(s.getsockname()[1])')
    ADB_DATA_DIR='$scratch/conf' RUST_LOG=warn '$ADB' serve-http --port \$port --flight-port 0 & pid=\$!
    for i in \$(seq 1 100); do curl -sf http://127.0.0.1:\$port/health >/dev/null && break; sleep 0.2; done
    cd '$DEV/AQL/tests' && node run-tests.mjs --url http://127.0.0.1:\$port | tee /dev/stderr | grep -q 'Failed: 0'
    r=\$?; kill \$pid; exit \$r"
if [ -d "$DEV/flowr-system/flowr-ce" ]; then
    step "build flowr-ce (adb-platform)" bash -c "cd '$DEV/flowr-system/flowr-ce' && CARGO_TARGET_DIR='$FLOWR_TARGET' cargo build -p flowr-ce --features adb-platform"
    step "FlowR integration (+ crash)" node "$ROOT/tests/flowr-integration.mjs" --adb "$ADB" --adb-data-dir "$scratch/fi" --flowr "$FLOWR"
fi
if [ $QUICK = 0 ]; then
    step "crash durability (20 kills)" node "$ROOT/tests/crash-durability.mjs" --adb "$ADB" --rounds 20
    step "subscriptions crash (10 kills)" node "$ROOT/tests/subscriptions-crash.mjs" --adb "$ADB" --rounds 10
    step "recall eval (W4)" node "$ROOT/tests/recall-eval.mjs" --adb "$ADB" --model "$ADB_EMBED_MODEL_DIR"
    if [ -d "$DEV/flowr-system/flowr-ce" ]; then
        step "engine crash (FlowR runs, ADB killed 3x)" node "$ROOT/tests/engine-crash.mjs" --adb "$ADB" --flowr "$FLOWR" --runs 20 --kills 3
    step "topic trigger (2 FlowR)" node "$ROOT/tests/topic-trigger.mjs" --adb "$ADB" --flowr "$FLOWR"
        step "runtime e2e (shared registry)" node "$ROOT/tests/runtime-e2e.mjs" --adb "$ADB" --flowr "$FLOWR"
        if [ -d "$LANCEDB" ]; then
            step "platform differential" node "$ROOT/tests/platform-differential.mjs" --adb "$ADB" --flowr "$FLOWR" \
                --lancedb "$LANCEDB" --flows "$DEV/flowr-system/a3t/backend/flows"
        else
            echo "platform differential             – skipped (no $LANCEDB)"
        fi
    fi
fi

echo
echo "${#pass[@]} passed, ${#fail[@]} failed"
[ ${#fail[@]} -eq 0 ]
