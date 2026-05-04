#!/usr/bin/env bash
set -euo pipefail
# Runs the bee chunkstore (sharky + LevelDB retrieval index) benchmark on the
# same litter-ally.eth asset tree as `litter_disk_cache_bench`.
#
#   BEE_SRC=/path/to/bee \
#   LITTER_BENCH_DIR=/tmp/litter_bench \
#   ./scripts/run_bee_sharky_litter_bench.sh

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BEE_SRC="${BEE_SRC:-/tmp/bee}"
BENCH_DIR="${ROOT}/scripts/bee_sharky_litter_bench"
export PATH="${PATH:-}"

if [[ ! -f "${BEE_SRC}/go.mod" ]]; then
	echo "==> cloning bee into ${BEE_SRC}" >&2
	git clone --depth 1 --branch v2.7.1 https://github.com/ethersphere/bee.git "${BEE_SRC}"
fi

mkdir -p "${BEE_SRC}/pkg/storer/cmd/litterbench"
cp "${BENCH_DIR}/main.go" "${BEE_SRC}/pkg/storer/cmd/litterbench/main.go"

if ! command -v go >/dev/null 2>&1; then
	echo "go not found on PATH; install Go 1.23+ (toolchain auto-downloads 1.25.x for bee)" >&2
	exit 1
fi

cd "${BEE_SRC}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-auto}"
exec go run ./pkg/storer/cmd/litterbench
