#!/usr/bin/env bash
set -euo pipefail
# Head-to-head disk chunk throughput (litter-ally.eth asset tree) for:
#   * ant:  examples/litter_disk_cache_bench (`put_batch` populate, parallel `get`)
#   * bee:  pkg/storer/cmd/litterbench (Sharky + LevelDB, batched populate)
#
# Prerequisites:
#   * LITTER_BENCH_DIR populated (same layout as verify-bzz-get litter download)
#   * `go`, bee checkout at BEE_SRC (see run_bee_sharky_litter_bench.sh)
#   * ant release example built: cargo build --release -p ant-retrieval --example litter_disk_cache_bench

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export LITTER_BENCH_DIR="${LITTER_BENCH_DIR:-/tmp/litter_bench}"
export PATH="${PATH:-}"

echo "=== ant (DiskChunkCache) ==="
( cd "$ROOT" && cargo build --release --example litter_disk_cache_bench -p ant-retrieval >/dev/null )
"$ROOT/target/release/examples/litter_disk_cache_bench"

echo
echo "=== bee (chunkstore) ==="
BEE_SRC="${BEE_SRC:-/tmp/bee}"
BENCH_GO="${ROOT}/scripts/bee_sharky_litter_bench/main.go"
if [[ ! -f "${BEE_SRC}/go.mod" ]]; then
	echo "run scripts/run_bee_sharky_litter_bench.sh once to clone bee into ${BEE_SRC}" >&2
	exit 1
fi
mkdir -p "${BEE_SRC}/pkg/storer/cmd/litterbench"
cp "${BENCH_GO}" "${BEE_SRC}/pkg/storer/cmd/litterbench/main.go"
if ! command -v go >/dev/null 2>&1; then
	echo "go not on PATH" >&2
	exit 1
fi
( cd "$BEE_SRC" && GOTOOLCHAIN="${GOTOOLCHAIN:-auto}" go run ./pkg/storer/cmd/litterbench )
