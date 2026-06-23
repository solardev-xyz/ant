#!/usr/bin/env bash
#
# Spin up an isolated AntDrive-style upload-reliability test rig and run the
# availability probe against it:
#
#   * 1 uploader node  — funded with the EXISTING postage batch (its per-bucket
#                        counter .bin is seeded from the production node so we
#                        never collide stamp indices), no chequebook (mobile
#                        AntDrive relies on pseudosettle, not SWAP).
#   * N verifier nodes — fresh identities => distinct overlays / neighbourhoods,
#                        pure retrieval (no postage). These are the "corners".
#
# All nodes use throwaway data dirs + high ports + their own control sockets, so
# the production `antd-ant` on :1633 / ~/.antd is never touched. Always torn down.
#
# Required env (or sourced from ./.env):
#   GNOSIS_RPC_URL, WALLET_PRIVATE_KEY, ANT_BATCH (0x..64hex, the batch to reuse)
#
# Key optional env:
#   ANT_BIN_DIR        (default target/release)
#   ANT_VERIFIER_COUNT (default 4)
#   ANT_BASE_PORT      (default 1810; uploader=BASE, verifiers=BASE+1..)
#   ANT_UP_THRESHOLD   uploader peer threshold     (default 50)
#   ANT_VER_THRESHOLD  verifier peer threshold     (default 30)
#   ANT_PEER_TIMEOUT   seconds to wait for peers   (default 300)
#   ANT_TARGET_PEERS   per-node target peers       (default 100)
#   ANT_SIZES, ANT_RUNS, ANT_SCHEDULE, ANT_GATEWAY, ANT_USE_PUBLIC  -> probe
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
[[ -f .env ]] && set -a && . ./.env && set +a || true

ANT_BIN_DIR="${ANT_BIN_DIR:-target/release}"
ANTD="${ANT_BIN_DIR}/antd"
ANTCTL="${ANT_BIN_DIR}/antctl"
ANT_VERIFIER_COUNT="${ANT_VERIFIER_COUNT:-4}"
ANT_BASE_PORT="${ANT_BASE_PORT:-1810}"
ANT_UP_THRESHOLD="${ANT_UP_THRESHOLD:-50}"
ANT_VER_THRESHOLD="${ANT_VER_THRESHOLD:-30}"
ANT_PEER_TIMEOUT="${ANT_PEER_TIMEOUT:-300}"
ANT_TARGET_PEERS="${ANT_TARGET_PEERS:-100}"
PROD_DATA_DIR="${PROD_DATA_DIR:-$HOME/.antd}"

die() { echo "error: $*" >&2; exit 1; }
[[ -x "$ANTD" ]]   || die "antd not executable at $ANTD"
[[ -x "$ANTCTL" ]] || die "antctl not executable at $ANTCTL"
[[ -n "${GNOSIS_RPC_URL:-}" ]] || die "GNOSIS_RPC_URL required"
[[ -n "${WALLET_PRIVATE_KEY:-}" ]] || die "WALLET_PRIVATE_KEY required"
[[ -n "${ANT_BATCH:-}" ]] || die "ANT_BATCH required (0x..)"

BATCH_NO0X="${ANT_BATCH#0x}"
RIG_DIR="$(mktemp -d -t ant-rig-XXXXXX)"
declare -a PIDS=()
declare -a LOGS=()

cleanup() {
  echo "=== tearing down rig ==="
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && { kill "$pid" 2>/dev/null || true; }
  done
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null || true
  done
  rm -rf "$RIG_DIR" 2>/dev/null || true
}
trap cleanup EXIT

wait_peers() {  # $1 = api port, $2 = threshold, $3 = label, $4 = pid
  local port="$1" thr="$2" label="$3" pid="$4"
  local deadline=$(( $(date +%s) + ANT_PEER_TIMEOUT )) peers=0
  while (( $(date +%s) < deadline )); do
    kill -0 "$pid" 2>/dev/null || die "$label exited before reaching peers"
    local resp; resp="$(curl -fsS -m 5 "http://127.0.0.1:${port}/peers" 2>/dev/null || true)"
    peers="$(printf '%s' "$resp" | python3 -c 'import sys,json
try: print(len(json.load(sys.stdin).get("peers",[])))
except Exception: print(0)' 2>/dev/null || echo 0)"
    peers="${peers//[!0-9]/}"; peers="${peers:-0}"
    (( peers >= thr )) && { echo "  $label ready: $peers peers"; return 0; }
    sleep 4
  done
  die "$label only $peers peers after ${ANT_PEER_TIMEOUT}s (needed $thr)"
}

overlay_of() {  # $1 = control socket
  "$ANTCTL" --socket "$1" status 2>/dev/null | awk '/Overlay:/{print $2}'
}

# --- uploader -------------------------------------------------------------
UP_DD="${RIG_DIR}/uploader"; mkdir -p "${UP_DD}/postage"
UP_PORT="$ANT_BASE_PORT"; UP_SOCK="${UP_DD}/antd.sock"; UP_LOG="${UP_DD}/antd.log"
# When REUSING a batch that another node already stamped, seed bucket counters
# from that node's .bin so we continue its stamp indices instead of re-issuing
# already-used (bucket,index) pairs. For a FRESH batch (the default here) zero
# counters are correct, so seeding is opt-in via ANT_SEED_POSTAGE=1.
if [[ "${ANT_SEED_POSTAGE:-0}" == "1" && -f "${PROD_DATA_DIR}/postage/${BATCH_NO0X}.bin" ]]; then
  cp "${PROD_DATA_DIR}/postage/${BATCH_NO0X}.bin" "${UP_DD}/postage/${BATCH_NO0X}.bin"
  echo "seeded uploader postage state from ${PROD_DATA_DIR}"
else
  echo "fresh batch: uploader starts with zero bucket counters"
fi
# Optional outbound SWAP: set ANT_CHEQUEBOOK to a factory-registered
# chequebook whose issuer() == EOA(ANT_SWAP_KEY) (defaults to the postage
# owner key). Needed for large uploads (~100 MB+) where pseudosettle's free
# allowance would otherwise throttle the push. Leave unset to rely on
# pseudosettle only (the mobile AntDrive default).
SWAP_ARGS=()
if [[ -n "${ANT_CHEQUEBOOK:-}" ]]; then
  echo "outbound SWAP enabled (chequebook=$ANT_CHEQUEBOOK)"
  SWAP_ARGS=(--chequebook "$ANT_CHEQUEBOOK" --swap-key "${ANT_SWAP_KEY:-$WALLET_PRIVATE_KEY}")
fi
echo "=== starting uploader (port $UP_PORT) ==="
RUST_LOG="${RUST_LOG:-info,ant_node::uploads=debug}" "$ANTD" \
  --data-dir "$UP_DD" \
  --api-addr "127.0.0.1:${UP_PORT}" \
  --control-socket "$UP_SOCK" \
  --target-peers "$ANT_TARGET_PEERS" \
  --gnosis-rpc-url "$GNOSIS_RPC_URL" \
  --postage-batch "$ANT_BATCH" \
  --postage-owner-key "$WALLET_PRIVATE_KEY" \
  "${SWAP_ARGS[@]}" \
  > "$UP_LOG" 2>&1 &
UP_PID=$!; PIDS+=("$UP_PID"); LOGS+=("$UP_LOG")
echo "uploader pid $UP_PID  log $UP_LOG"

# --- verifiers ------------------------------------------------------------
declare -a VER_URLS=() VER_LABELS=()
for i in $(seq 1 "$ANT_VERIFIER_COUNT"); do
  VD="${RIG_DIR}/verifier${i}"; mkdir -p "$VD"
  VP=$(( ANT_BASE_PORT + i )); VS="${VD}/antd.sock"; VL="${VD}/antd.log"
  echo "=== starting verifier${i} (port $VP) ==="
  RUST_LOG="${RUST_LOG:-info}" "$ANTD" \
    --data-dir "$VD" \
    --api-addr "127.0.0.1:${VP}" \
    --control-socket "$VS" \
    --target-peers "$ANT_TARGET_PEERS" \
    > "$VL" 2>&1 &
  VPID=$!; PIDS+=("$VPID"); LOGS+=("$VL")
  VER_URLS+=("http://127.0.0.1:${VP}")
  echo "verifier${i} pid $VPID  log $VL"
done

# --- wait for peers -------------------------------------------------------
echo "=== waiting for peer sets ==="
wait_peers "$UP_PORT" "$ANT_UP_THRESHOLD" "uploader" "$UP_PID"
for i in $(seq 1 "$ANT_VERIFIER_COUNT"); do
  VP=$(( ANT_BASE_PORT + i ))
  # PID for verifier i is at index i in PIDS (0 = uploader).
  wait_peers "$VP" "$ANT_VER_THRESHOLD" "verifier${i}" "${PIDS[$i]}"
done

# --- record overlays (the "corners") -------------------------------------
echo "=== node overlays (neighbourhoods) ==="
UP_OVL="$(overlay_of "$UP_SOCK")"; echo "  uploader : $UP_OVL"
for i in $(seq 1 "$ANT_VERIFIER_COUNT"); do
  VS="${RIG_DIR}/verifier${i}/antd.sock"
  OVL="$(overlay_of "$VS")"
  VER_LABELS+=("v${i}:${OVL:2:6}")
  echo "  verifier${i}: $OVL"
done

# --- run the probe --------------------------------------------------------
JOINED_URLS="$(IFS=,; echo "${VER_URLS[*]}")"
JOINED_LABELS="$(IFS=,; echo "${VER_LABELS[*]}")"
OUT="${ANT_OUT:-${REPO_DIR}/scripts/reliability_report.json}"
echo "=== running availability probe ==="
ANT_ANTCTL="$ANTCTL" \
ANT_UPLOADER_SOCKET="$UP_SOCK" \
ANT_BATCH="$ANT_BATCH" \
ANT_VERIFIERS="$JOINED_URLS" \
ANT_VERIFIER_LABELS="$JOINED_LABELS" \
ANT_OUT="$OUT" \
  python3 "${REPO_DIR}/scripts/reliability_probe.py"
RC=$?
echo "probe exit code: $RC"
echo "uploader log tail:"; tail -n 25 "$UP_LOG" || true
exit "$RC"
