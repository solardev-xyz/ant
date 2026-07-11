#!/usr/bin/env bash
# Overnight baseline backfill: DoD #1 wants ≥5 runs per matrix cell;
# the 256/512 baseline cells ran n=1 under the batch-1 budget cap.
# Batch 2 (mutable, user-funded) now funds the remaining 4+4 runs on
# the pre-lab snapshot binary (perf/state/antd-baseline-bin — predates
# every experiment, so no env-off toggles needed). Expect every run to
# stall-abort like the originals; each takes ~55–95 min and cleans up
# its /tmp dir on exit.
set -uo pipefail
cd "$(dirname "$0")/.."
B=./target/release/perf_bench
BIN=perf/state/antd-baseline-bin

for i in 1 2 3 4; do
  echo "=== backfill baseline-256 extra run $i/4 ==="
  $B upload --size-mib 256 --runs 1 --batch 2 --label baseline256bf \
    --antd-bin $BIN --stall-abort-secs 2100 --run-timeout-secs 10800 || true
done
for i in 1 2 3 4; do
  echo "=== backfill baseline-512 extra run $i/4 ==="
  $B upload --size-mib 512 --runs 1 --batch 2 --label baseline512bf \
    --antd-bin $BIN --stall-abort-secs 2100 --run-timeout-secs 14400 || true
done
echo "=== BACKFILL DONE ==="
./target/release/perf_bench status
