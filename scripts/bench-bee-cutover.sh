#!/usr/bin/env bash
# scripts/bench-bee-cutover.sh
#
# Benchmark harness for the bee 2.7 → 2.8 BZZ-handshake cutover.
# Scenarios B1-B5 from PLAN-bee-2.8.md §10; one scenario per invocation,
# emits one CSV line per run to stdout (and the run log to stderr).
#
# Usage:
#   scripts/bench-bee-cutover.sh --scenario {b1|b2|b3|b4|b5} \
#       [--build {0.4.1|0.5.0}] [--runs N] [--data-dir DIR]
#
# Env knobs:
#   READY_PEERS    Peers required before B2-B5 start (default 50).
#   PEER_TIMEOUT   Cap for the wait-for-peers loop in B1 only (default 600).
#   GET_TIMEOUT    Cap for a single antctl get / upload (default 120).
#
# Build selection:
#   - `--build 0.4.1` checks out the pre-upgrade tree (git stash + checkout
#     to origin/main if 0.4.1 is in working tree) and builds release.
#   - `--build 0.5.0` builds the current working tree, expected to be
#     post-upgrade. The harness never modifies Cargo.toml itself — bump
#     the workspace version before invoking 0.5.0.
#
# All runs talk to Swarm mainnet from whatever host invokes the script.
# Production deployment caveat: if `antd.service` is active on this host,
# B1 (cold-start) will conflict with it on the default ports / data dir.
# Pass `--data-dir /tmp/antd-bench` (the default) to keep the harness off
# the production state.

set -euo pipefail

SCENARIO=""
BUILD="0.5.0"
RUNS=3
DATA_DIR="/tmp/antd-bench"
READY_PEERS="${READY_PEERS:-50}"
PEER_TIMEOUT="${PEER_TIMEOUT:-600}"
GET_TIMEOUT="${GET_TIMEOUT:-120}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenario) SCENARIO="$2"; shift 2 ;;
        --build)    BUILD="$2";    shift 2 ;;
        --runs)     RUNS="$2";     shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            echo "unknown arg: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$SCENARIO" ]]; then
    echo "missing --scenario {b1..b5}" >&2
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOCKET="$DATA_DIR/antd.sock"
LOG_FILE="$DATA_DIR/antd.log"

# Binaries: ANTD_BIN / ANTCTL_BIN env overrides skip the rebuild and
# point the harness at pre-built release binaries. Useful for the
# 0.4.1 vs 0.5.0 comparison, where we want both builds materialised
# side-by-side rather than the harness clobbering one with the other.
ANTD_BIN="${ANTD_BIN:-}"
ANTCTL_BIN="${ANTCTL_BIN:-}"

ANTD_PID=""
cleanup() {
    local code=$?
    if [[ -n "$ANTD_PID" ]] && kill -0 "$ANTD_PID" 2>/dev/null; then
        kill -TERM "$ANTD_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            kill -0 "$ANTD_PID" 2>/dev/null || break
            sleep 1
        done
        kill -KILL "$ANTD_PID" 2>/dev/null || true
    fi
    exit $code
}
trap cleanup EXIT INT TERM

build_binary() {
    if [[ -n "$ANTD_BIN" && -n "$ANTCTL_BIN" ]]; then
        # Pre-built binaries supplied; skip cargo entirely.
        return 0
    fi
    case "$BUILD" in
        0.5.0)
            (cd "$REPO_ROOT" && cargo build --release -p antd -p antctl) >&2
            ;;
        0.4.1)
            # Build from origin/main with the bee-2.8 cutover changes
            # stashed. Operator can also pre-stage a `git worktree` and
            # pass ANTD_BIN / ANTCTL_BIN env vars to skip this branch.
            (cd "$REPO_ROOT" && git stash -u --keep-index || true) >&2
            (cd "$REPO_ROOT" && cargo build --release -p antd -p antctl) >&2
            ;;
        *)
            echo "unknown --build $BUILD (want 0.4.1 or 0.5.0)" >&2
            exit 2
            ;;
    esac
    ANTD_BIN="$REPO_ROOT/target/release/antd"
    ANTCTL_BIN="$ANTCTL_BIN"
}

start_daemon() {
    rm -f "$DATA_DIR/antd.sock" "$LOG_FILE"
    mkdir -p "$DATA_DIR"
    # Use a non-default API address so we don't fight `antd.service` for
    # port 1633 on production hosts; the bench daemon uses its own data
    # dir and control socket so antctl finds it via `--data-dir`.
    "$ANTD_BIN" \
        --data-dir "$DATA_DIR" \
        --api-addr "${BENCH_API_ADDR:-127.0.0.1:11633}" \
        --reset-peerstore \
        --log-level info >"$LOG_FILE" 2>&1 &
    ANTD_PID=$!
    for _ in $(seq 1 30); do
        [[ -S "$SOCKET" ]] && return 0
        sleep 0.5
    done
    echo "antd never opened $SOCKET" >&2
    tail -n 50 "$LOG_FILE" >&2 || true
    return 1
}

stop_daemon() {
    if [[ -n "$ANTD_PID" ]] && kill -0 "$ANTD_PID" 2>/dev/null; then
        kill -TERM "$ANTD_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            kill -0 "$ANTD_PID" 2>/dev/null || break
            sleep 1
        done
    fi
    ANTD_PID=""
}

# Returns "routing_size connected" from antctl status --json.
peers_now() {
    local snap
    snap="$("$ANTCTL_BIN" --data-dir "$DATA_DIR" --json status 2>/dev/null || true)"
    if [[ -z "$snap" ]]; then
        echo "0 0"
        return
    fi
    printf '%s' "$snap" | python3 -c '
import json, sys
try:
    snap = json.load(sys.stdin)
except Exception:
    print("0 0"); sys.exit(0)
peers = snap.get("peers", {}) or {}
routing = peers.get("routing", {}) or {}
print("%d %d" % (routing.get("size", 0), peers.get("connected", 0)))
'
}

wait_for_peers() {
    local target="$1"
    local cap="$2"
    local started; started=$(date +%s)
    while :; do
        read -r routing connected < <(peers_now)
        if [[ "${routing:-0}" -ge "$target" ]]; then
            local elapsed=$(( $(date +%s) - started ))
            echo "$elapsed"
            return 0
        fi
        if [[ $(( $(date +%s) - started )) -ge "$cap" ]]; then
            echo "TIMEOUT"
            return 1
        fi
        sleep 1
    done
}

scenario_b1() {
    # Time to 100 peers (cold start). One PEER_TIMEOUT cap per run total,
    # not per-threshold (the old per-threshold cap meant a 0.4.1 baseline
    # took 3 × PEER_TIMEOUT per run — 30 min for the full sweep).
    for run in $(seq 1 "$RUNS"); do
        rm -rf "$DATA_DIR"
        start_daemon
        local started; started=$(date +%s)
        local t_first="" t_50="" t_100=""
        while :; do
            read -r routing _connected < <(peers_now)
            local elapsed=$(( $(date +%s) - started ))
            if [[ -z "$t_first" && "${routing:-0}" -ge 1   ]]; then t_first=$elapsed; fi
            if [[ -z "$t_50"   && "${routing:-0}" -ge 50  ]]; then t_50=$elapsed;   fi
            if [[ -z "$t_100"  && "${routing:-0}" -ge 100 ]]; then t_100=$elapsed;  fi
            if [[ -n "$t_100" ]]; then break; fi
            if [[ $elapsed -ge "$PEER_TIMEOUT" ]]; then break; fi
            sleep 1
        done
        echo "b1,$BUILD,$run,${t_first:-TIMEOUT},${t_50:-TIMEOUT},${t_100:-TIMEOUT}"
        stop_daemon
    done
}

do_upload() {
    # Upload a file with `upload start --detach` then poll for terminal
    # status. Echoes `ok ms=<wall> ref=<hex>` on success, or
    # `FAIL state=<status> err=<msg>` on failure.
    local src="$1"
    local t0; t0=$(date +%s%N)
    local out job_id
    out="$("$ANTCTL_BIN" --data-dir "$DATA_DIR" --json upload start --detach "$src" 2>/dev/null)"
    job_id="$(printf '%s' "$out" | python3 -c '
import json, sys
try: d = json.load(sys.stdin)
except: sys.exit(0)
print(d.get("job_id") or d.get("id") or "")
' 2>/dev/null)"
    if [[ -z "$job_id" ]]; then
        echo "FAIL state=no-job-id err=$out"
        return 1
    fi
    while :; do
        local s state ref err
        s="$("$ANTCTL_BIN" --data-dir "$DATA_DIR" --json upload status "$job_id" 2>/dev/null || true)"
        if [[ -z "$s" ]]; then sleep 2; continue; fi
        state="$(printf '%s' "$s" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",""))' 2>/dev/null)"
        ref="$(printf   '%s' "$s" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("reference") or "")' 2>/dev/null)"
        err="$(printf   '%s' "$s" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("last_error") or "")' 2>/dev/null)"
        case "$state" in
            completed)
                local t1; t1=$(date +%s%N)
                echo "ok ms=$(( (t1 - t0) / 1000000 )) ref=$ref"
                return 0
                ;;
            failed|cancelled)
                echo "FAIL state=$state ref=$ref err=$err"
                return 1
                ;;
        esac
        sleep 2
    done
}

scenario_b2() {
    # Upload 1 MiB, repeated. Same source file each run — Swarm is
    # content-addressed so the reference is stable; the wall-clock
    # captures push pipeline behaviour, not network thrash.
    start_daemon
    wait_for_peers "$READY_PEERS" "$PEER_TIMEOUT" >/dev/null || {
        echo "b2,$BUILD,*,WARMUP_TIMEOUT"
        return 1
    }
    local src=/tmp/bench-1m.bin
    [[ -f "$src" ]] || dd if=/dev/urandom of="$src" bs=1M count=1 status=none
    for run in $(seq 1 "$RUNS"); do
        echo "b2,$BUILD,$run,$(do_upload "$src")"
    done
    stop_daemon
}

scenario_b3() {
    # Download 1 MiB. Requires a known reference: env BZZ_REF=<hex>.
    # Restart the daemon between runs to get a cold libp2p + cold chunk
    # cache — otherwise runs 2/3 just measure local memory access.
    if [[ -z "${BZZ_REF:-}" ]]; then
        echo "b3 needs BZZ_REF=<hex> (upload via b2 first)" >&2
        return 2
    fi
    for run in $(seq 1 "$RUNS"); do
        rm -rf "$DATA_DIR"
        start_daemon
        wait_for_peers "$READY_PEERS" "$PEER_TIMEOUT" >/dev/null || {
            echo "b3,$BUILD,$run,WARMUP_TIMEOUT"
            stop_daemon
            continue
        }
        rm -f /tmp/bench-1m.out
        local started; started=$(date +%s%N)
        if ! "$ANTCTL_BIN" --data-dir "$DATA_DIR" \
                get "bzz://$BZZ_REF" -o /tmp/bench-1m.out >/dev/null 2>&1; then
            echo "b3,$BUILD,$run,FAIL"
            stop_daemon
            continue
        fi
        local ended; ended=$(date +%s%N)
        local bytes; bytes=$(stat -c '%s' /tmp/bench-1m.out 2>/dev/null || echo 0)
        echo "b3,$BUILD,$run,$(( (ended - started) / 1000000 ))ms,${bytes}B"
        stop_daemon
    done
}

scenario_b4() {
    # 8x concurrent downloads. Requires BZZ_REFS=<comma-separated hex>.
    # Spawn each get as a backgrounded subshell so we can capture per-task
    # latency + success/fail separately and print a useful breakdown
    # rather than just the aggregate (which is dominated by whichever ref
    # is slowest or unfetchable).
    if [[ -z "${BZZ_REFS:-}" ]]; then
        echo "b4 needs BZZ_REFS=<csv hex> (upload 8 files via b2 first)" >&2
        return 2
    fi
    start_daemon
    wait_for_peers "$READY_PEERS" "$PEER_TIMEOUT" >/dev/null || {
        echo "b4,$BUILD,*,WARMUP_TIMEOUT"
        return 1
    }
    for run in $(seq 1 "$RUNS"); do
        rm -f "$DATA_DIR/bench-b4-$run-"*.t "$DATA_DIR/bench-b4-$run-"*.out
        local started; started=$(date +%s%N)
        local pids=() i=0
        for ref in $(echo "$BZZ_REFS" | tr ',' ' '); do
            i=$((i + 1))
            (
                local t0; t0=$(date +%s%N)
                if "$ANTCTL_BIN" --data-dir "$DATA_DIR" \
                        get "bzz://$ref" -o "$DATA_DIR/bench-b4-$run-$i.out" >/dev/null 2>&1; then
                    local t1; t1=$(date +%s%N)
                    echo "ok $(( (t1 - t0) / 1000000 ))" > "$DATA_DIR/bench-b4-$run-$i.t"
                else
                    echo "fail" > "$DATA_DIR/bench-b4-$run-$i.t"
                fi
            ) &
            pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p"; done
        local ended; ended=$(date +%s%N)
        local agg=$(( (ended - started) / 1000000 ))
        local ok; ok=$(grep -lc '^ok' "$DATA_DIR/bench-b4-$run-"*.t 2>/dev/null | wc -l)
        local per; per=$(grep -h '^ok' "$DATA_DIR/bench-b4-$run-"*.t 2>/dev/null | awk '{print $2}' | sort -n | tr '\n' ',' | sed 's/,$//')
        echo "b4,$BUILD,$run,aggregate_ms=$agg,ok=$ok/$i,per_task_ms=$per"
    done
    stop_daemon
}

scenario_b5() {
    # Upload a large file. Default size is 500 MiB, set $B5_MB to
    # override (e.g. 100 to dodge the bee 2.8 pushsync 60s/chunk
    # exhaust-retries failure mode that surfaces at ~250 MiB+ on the
    # current mainnet storer load).
    start_daemon
    wait_for_peers "$READY_PEERS" "$PEER_TIMEOUT" >/dev/null || {
        echo "b5,$BUILD,*,WARMUP_TIMEOUT"
        return 1
    }
    local size="${B5_MB:-500}"
    local src="/tmp/bench-${size}m.bin"
    [[ -f "$src" ]] || dd if=/dev/urandom of="$src" bs=1M count="$size" status=none
    for run in $(seq 1 "$RUNS"); do
        local r; r="$(do_upload "$src")"
        local ms; ms="$(echo "$r" | grep -oE 'ms=[0-9]+' | head -1 | cut -d= -f2)"
        if [[ -n "$ms" ]]; then
            local mbps; mbps=$(python3 -c "print(round($size * 1000 / $ms, 2))" 2>/dev/null || echo "?")
            echo "b5,$BUILD,$run,$r,mbps=$mbps"
        else
            echo "b5,$BUILD,$run,$r"
        fi
    done
    stop_daemon
}

build_binary

case "$SCENARIO" in
    b1) scenario_b1 ;;
    b2) scenario_b2 ;;
    b3) scenario_b3 ;;
    b4) scenario_b4 ;;
    b5) scenario_b5 ;;
    *) echo "unknown scenario: $SCENARIO" >&2; exit 2 ;;
esac
