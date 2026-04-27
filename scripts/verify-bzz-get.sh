#!/usr/bin/env bash
# scripts/verify-bzz-get.sh
#
# Two-in-one harness for `antctl get bzz://...`:
#
#   * Live verify (default). Spin up antd against mainnet, wait for
#     enough BZZ peers, run the get, and report what came back. Useful
#     when something in the retrieval / handshake / peer-discovery
#     stack changes and you want a quick "does the URL still resolve
#     end-to-end?" check.
#   * Fixture refresh. With `RECORD_DIR=...` set, the daemon is built
#     in debug mode and run with `--record-chunks <dir>` so every
#     chunk it fetches lands on disk. The integration test
#     `replays_mainnet_bzz_fixture` (in
#     `crates/ant-retrieval/tests/bzz_fixture.rs`) replays those
#     chunks offline; this is how you regenerate them when the
#     captured URL changes.
#
# This is a dev-loop tool, not a CI gate. Mainnet timing is too noisy
# for unattended runs; on a laptop expect anywhere between 10 s and
# 60 s for the routing table to fill up.
#
# Customise the URL / wait budget via env vars:
#
#   BZZ_URL=bzz://<ref>/<path> ./scripts/verify-bzz-get.sh
#   READY_PEERS=12 PEER_TIMEOUT=120 ./scripts/verify-bzz-get.sh
#   RECORD_DIR=$(pwd)/crates/ant-retrieval/tests/fixtures/bzz-... \
#       ./scripts/verify-bzz-get.sh             # debug build + record

set -euo pipefail

BZZ_URL="${BZZ_URL:-bzz://ab77201f6541a9ceafb98a46c643273cfa397a87798273dd17feb2aa366ce2e6/13/4358/2645.png}"
DATA_DIR="${DATA_DIR:-/tmp/antd-verify-bzz}"
OUT_PATH="${OUT_PATH:-/tmp/antctl-verify-bzz.bin}"
READY_PEERS="${READY_PEERS:-8}"
PEER_TIMEOUT="${PEER_TIMEOUT:-90}"
GET_TIMEOUT="${GET_TIMEOUT:-90}"
RECORD_DIR="${RECORD_DIR:-}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="$DATA_DIR/antd.log"
SOCKET="$DATA_DIR/antd.sock"

cleanup() {
    local code=$?
    if [[ -n "${ANTD_PID:-}" ]] && kill -0 "$ANTD_PID" 2>/dev/null; then
        kill -TERM "$ANTD_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            if ! kill -0 "$ANTD_PID" 2>/dev/null; then break; fi
            sleep 1
        done
        kill -KILL "$ANTD_PID" 2>/dev/null || true
    fi
    exit $code
}
trap cleanup EXIT INT TERM

# `--record-chunks` is only compiled into debug builds (gated on
# `cfg(debug_assertions)`), so when capturing a fixture we need the
# debug profile. The release build is faster otherwise and is what we
# default to for plain smoke runs.
if [[ -n "$RECORD_DIR" ]]; then
    BUILD_PROFILE="debug"
    BUILD_FLAGS=()
else
    BUILD_PROFILE="release"
    BUILD_FLAGS=(--release)
fi
echo "==> building antd + antctl ($BUILD_PROFILE)"
( cd "$REPO_ROOT" && cargo build "${BUILD_FLAGS[@]}" -p antd -p antctl >&2 )

ANTD="$REPO_ROOT/target/$BUILD_PROFILE/antd"
ANTCTL="$REPO_ROOT/target/$BUILD_PROFILE/antctl"

mkdir -p "$DATA_DIR"
rm -f "$DATA_DIR/peers.json" "$LOG_FILE"
rm -f "$DATA_DIR/antd.sock"

echo "==> starting antd at $DATA_DIR (log: $LOG_FILE)"
ANTD_ARGS=(--data-dir "$DATA_DIR" --reset-peerstore --log-level info)
if [[ -n "$RECORD_DIR" ]]; then
    mkdir -p "$RECORD_DIR"
    ANTD_ARGS+=(--record-chunks "$RECORD_DIR")
    echo "==> recording every retrieved chunk to $RECORD_DIR"
fi
"$ANTD" "${ANTD_ARGS[@]}" >"$LOG_FILE" 2>&1 &
ANTD_PID=$!
echo "==> antd pid: $ANTD_PID"

# Wait for the control socket to appear so antctl status can connect.
for _ in $(seq 1 20); do
    if [[ -S "$SOCKET" ]]; then break; fi
    sleep 0.5
done
if [[ ! -S "$SOCKET" ]]; then
    echo "!! control socket never appeared at $SOCKET" >&2
    tail -n 100 "$LOG_FILE" >&2 || true
    exit 2
fi

echo "==> waiting up to ${PEER_TIMEOUT}s for >= ${READY_PEERS} BZZ peers"
deadline=$(( $(date +%s) + PEER_TIMEOUT ))
# Pretty-printed multi-line JSON beats sed; pipe to python3 (always present
# on the supported OS matrix) for proper field extraction. Note: we
# intentionally use `python3 -c` rather than `python3 - <<EOF`, because
# the latter binds stdin to the heredoc and would silently swallow our
# piped JSON.
PY_EXTRACT='import json, sys
try:
    snap = json.load(sys.stdin)
except Exception:
    print("0 0"); sys.exit(0)
peers = snap.get("peers", {}) or {}
routing = peers.get("routing", {}) or {}
print("%d %d" % (routing.get("size", 0), peers.get("connected", 0)))'
extract_status() {
    python3 -c "$PY_EXTRACT"
}
while :; do
    snap="$("$ANTCTL" --data-dir "$DATA_DIR" status --json 2>/dev/null || true)"
    if [[ -n "$snap" ]]; then
        read -r routing_size connected < <(printf '%s' "$snap" | extract_status)
    else
        routing_size=0; connected=0
    fi
    printf "    routing=%s connected=%s\n" "$routing_size" "$connected"
    if [[ "${routing_size:-0}" -ge "$READY_PEERS" ]]; then break; fi
    if [[ $(date +%s) -ge $deadline ]]; then
        echo "!! timed out after ${PEER_TIMEOUT}s with routing=${routing_size:-0} connected=${connected:-0}" >&2
        tail -n 80 "$LOG_FILE" >&2 || true
        exit 3
    fi
    sleep 2
done

rm -f "$OUT_PATH"
echo
echo "==> antctl get $BZZ_URL -o $OUT_PATH"
# macOS doesn't ship `timeout(1)`. Pick the first available wrapper, else
# run unguarded — the daemon already enforces TREE_COMMAND_TIMEOUT (60 s)
# server-side, so the only thing the wall-clock guard buys us is an
# early bail on a wedged stream/socket.
if command -v timeout >/dev/null 2>&1; then
    TIMEOUT_CMD=(timeout "$GET_TIMEOUT")
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_CMD=(gtimeout "$GET_TIMEOUT")
else
    TIMEOUT_CMD=()
fi
set +e
GET_OUT="$("${TIMEOUT_CMD[@]}" "$ANTCTL" --data-dir "$DATA_DIR" get \
    --allow-degraded-redundancy \
    "$BZZ_URL" -o "$OUT_PATH" 2>&1)"
GET_RC=$?
set -e
echo "    rc=$GET_RC"
printf '%s\n' "$GET_OUT"

echo
echo "==> result"
if [[ $GET_RC -eq 0 && -f "$OUT_PATH" ]]; then
    BYTES=$(wc -c <"$OUT_PATH" | tr -d ' ')
    echo "    wrote $BYTES bytes to $OUT_PATH"
    if command -v file >/dev/null 2>&1; then
        file "$OUT_PATH"
    fi
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$OUT_PATH"
    fi
else
    echo "!! get failed (rc=$GET_RC); see daemon tail below" >&2
fi

echo
echo "==> antd tail (last 50 lines)"
tail -n 50 "$LOG_FILE" || true

exit $GET_RC
