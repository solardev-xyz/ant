#!/usr/bin/env bash
#
# Orchestrate an end-to-end upload reliability test against a freshly
# started, isolated `antd`:
#
#   1. (optionally) create a dedicated postage batch on Gnosis,
#   2. start `antd` in a throwaway data dir with outbound SWAP wired up
#      when a chequebook is supplied (required for ~100 MB+ uploads),
#   3. wait until the node has handshaked enough BZZ peers,
#   4. run scripts/upload_integration_test.py (upload → gateway round-trip),
#   5. always tear the node down and surface the harness exit code.
#
# This is what .github/workflows/upload-integration.yml invokes, but it is
# self-contained so you can run it locally too:
#
#   GNOSIS_RPC_URL=… WALLET_PRIVATE_KEY=… ANT_SIZES_MB=1,10 \
#     scripts/run_upload_integration.sh
#
# Required env:
#   GNOSIS_RPC_URL        Gnosis RPC endpoint.
#   WALLET_PRIVATE_KEY    funds the postage batch + (if set) signs cheques.
#
# Key optional env:
#   ANT_BIN_DIR           dir holding antd + antctl   (default: target/release)
#   ANT_BATCH             reuse this batch instead of creating one.
#   ANT_BATCH_DEPTH       depth for a created batch    (default: 20)
#   ANT_BATCH_AMOUNT      PLUR/chunk for a created batch (default: 1200000000;
#                         must stay above the chain's current minimum or
#                         createBatch reverts — ~1.2e9 gives a multi-day TTL)
#   ANT_CHEQUEBOOK        chequebook addr → enables SWAP (needed for 100 MB).
#   ANT_SIZES_MB          sizes to test               (default: 1,10)
#   ANT_GATEWAY           verification gateway        (default: https://vibing.at/ant)
#   ANT_PEER_THRESHOLD    peers to wait for           (default: 50)
#   ANT_PEER_TIMEOUT      seconds to wait for peers   (default: 300)
#   ANT_API_PORT          local node HTTP API port    (default: 1733)

set -euo pipefail

ANT_BIN_DIR="${ANT_BIN_DIR:-target/release}"
ANT_BATCH_DEPTH="${ANT_BATCH_DEPTH:-20}"
ANT_BATCH_AMOUNT="${ANT_BATCH_AMOUNT:-1200000000}"
ANT_SIZES_MB="${ANT_SIZES_MB:-1,10}"
ANT_GATEWAY="${ANT_GATEWAY:-https://vibing.at/ant}"
ANT_PEER_THRESHOLD="${ANT_PEER_THRESHOLD:-50}"
ANT_PEER_TIMEOUT="${ANT_PEER_TIMEOUT:-300}"
ANT_API_PORT="${ANT_API_PORT:-1733}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANTD="${ANT_BIN_DIR}/antd"
ANTCTL="${ANT_BIN_DIR}/antctl"

die() { echo "error: $*" >&2; exit 1; }

[[ -n "${GNOSIS_RPC_URL:-}" ]] || die "GNOSIS_RPC_URL is required"
[[ -n "${WALLET_PRIVATE_KEY:-}" ]] || die "WALLET_PRIVATE_KEY is required"
[[ -x "$ANTD" ]] || die "antd not found/executable at $ANTD (set ANT_BIN_DIR)"
[[ -x "$ANTCTL" ]] || die "antctl not found/executable at $ANTCTL (set ANT_BIN_DIR)"

DATA_DIR="$(mktemp -d -t antd-it-XXXXXX)"
SOCK="${DATA_DIR}/antd.sock"
NODE_LOG="${DATA_DIR}/antd.log"
ANTD_PID=""

cleanup() {
  if [[ -n "$ANTD_PID" ]] && kill -0 "$ANTD_PID" 2>/dev/null; then
    echo "--- stopping antd (pid $ANTD_PID) ---"
    kill "$ANTD_PID" 2>/dev/null || true
    wait "$ANTD_PID" 2>/dev/null || true
  fi
  if [[ -f "$NODE_LOG" ]]; then
    echo "--- antd log (last 40 lines) ---"
    tail -n 40 "$NODE_LOG" || true
  fi
  rm -rf "$DATA_DIR" 2>/dev/null || true
}
trap cleanup EXIT

# --- postage batch -------------------------------------------------------
if [[ -z "${ANT_BATCH:-}" ]]; then
  echo "=== creating postage batch (depth=$ANT_BATCH_DEPTH amount=$ANT_BATCH_AMOUNT) ==="
  CREATE_JSON="$(POSTAGE_OWNER_KEY="$WALLET_PRIVATE_KEY" "$ANTCTL" postage create \
    --gnosis-rpc-url "$GNOSIS_RPC_URL" \
    --owner-key "$WALLET_PRIVATE_KEY" \
    --depth "$ANT_BATCH_DEPTH" \
    --amount-per-chunk "$ANT_BATCH_AMOUNT" \
    --json)"
  echo "$CREATE_JSON"
  # `antctl … --json` prints progress lines before the final JSON object, so
  # scan all JSON objects and pull `batch_id` (not, e.g., an approve tx hash).
  ANT_BATCH="$(printf '%s' "$CREATE_JSON" | python3 -c '
import sys, json
dec = json.JSONDecoder()
s = sys.stdin.read()
i, bid = 0, ""
while i < len(s):
    while i < len(s) and s[i] in " \t\r\n":
        i += 1
    if i >= len(s):
        break
    try:
        obj, end = dec.raw_decode(s, i)
        i = end
        if isinstance(obj, dict) and obj.get("batch_id"):
            bid = obj["batch_id"]
    except json.JSONDecodeError:
        i += 1
print(bid)')"
  [[ -n "$ANT_BATCH" ]] || die "could not parse created batch id"
fi
echo "using batch: $ANT_BATCH"

# --- start antd ----------------------------------------------------------
SWAP_ARGS=()
if [[ -n "${ANT_CHEQUEBOOK:-}" ]]; then
  echo "outbound SWAP enabled (chequebook=$ANT_CHEQUEBOOK)"
  SWAP_ARGS=(--chequebook "$ANT_CHEQUEBOOK" --swap-key "$WALLET_PRIVATE_KEY")
fi

echo "=== starting antd (data-dir=$DATA_DIR api=127.0.0.1:$ANT_API_PORT) ==="
RUST_LOG="${RUST_LOG:-info}" "$ANTD" \
  --data-dir "$DATA_DIR" \
  --api-addr "127.0.0.1:${ANT_API_PORT}" \
  --control-socket "$SOCK" \
  --gnosis-rpc-url "$GNOSIS_RPC_URL" \
  --gnosis-logs-rpc-url "" \
  --postage-batch "$ANT_BATCH" \
  --postage-owner-key "$WALLET_PRIVATE_KEY" \
  "${SWAP_ARGS[@]}" \
  > "$NODE_LOG" 2>&1 &
ANTD_PID=$!
echo "antd pid $ANTD_PID"

# --- wait for peers ------------------------------------------------------
echo "=== waiting for >= $ANT_PEER_THRESHOLD BZZ peers (timeout ${ANT_PEER_TIMEOUT}s) ==="
deadline=$(( $(date +%s) + ANT_PEER_TIMEOUT ))
peers=0
while (( $(date +%s) < deadline )); do
  if ! kill -0 "$ANTD_PID" 2>/dev/null; then
    die "antd exited before reaching peer threshold (see log above)"
  fi
  resp="$(curl -fsS -m 5 "http://127.0.0.1:${ANT_API_PORT}/peers" 2>/dev/null || true)"
  peers="$(printf '%s' "$resp" | python3 -c 'import sys,json
try: print(len(json.load(sys.stdin).get("peers",[])))
except Exception: print(0)' 2>/dev/null || true)"
  peers="${peers//[!0-9]/}"   # keep digits only — never feed junk to (( ))
  peers="${peers:-0}"
  if (( peers >= ANT_PEER_THRESHOLD )); then
    echo "peer set ready: $peers peers"
    break
  fi
  sleep 5
done
if (( peers < ANT_PEER_THRESHOLD )); then
  die "only $peers peers after ${ANT_PEER_TIMEOUT}s (needed $ANT_PEER_THRESHOLD)"
fi

# --- run the upload matrix ----------------------------------------------
echo "=== running upload integration harness ==="
ANT_ANTCTL="$ANTCTL" \
ANT_SOCKET="$SOCK" \
ANT_GATEWAY="$ANT_GATEWAY" \
ANT_BATCH="$ANT_BATCH" \
ANT_SIZES_MB="$ANT_SIZES_MB" \
  python3 "${SCRIPT_DIR}/upload_integration_test.py"
HARNESS_RC=$?

echo "harness exit code: $HARNESS_RC"
exit "$HARNESS_RC"
