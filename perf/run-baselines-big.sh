#!/usr/bin/env bash
# Big baseline cells, n=1 each (budget-adjusted plan in PERF-LAB.md):
# 256 MiB expected to complete (0.3.14-era precedent, but that was
# with cheques); 512 MiB expected to stall — the stall-abort guard
# (35 min without byte progress) bounds the run and the result JSON +
# antd log capture the failure signature for Experiment 1.
set -euo pipefail
cd "$(dirname "$0")/.."
B=./target/release/perf_bench

echo "=== 256 MiB x1 ==="
$B upload --size-mib 256 --runs 1 --label baseline --stall-abort-secs 2100 --run-timeout-secs 14400

echo "=== 512 MiB x1 ==="
$B upload --size-mib 512 --runs 1 --label baseline --stall-abort-secs 2100 --run-timeout-secs 21600

echo "=== big cells DONE ==="
./target/release/perf_bench status
