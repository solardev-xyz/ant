#!/usr/bin/env bash
# Baseline battery, cheap cells only (see PERF-LAB.md "Budget-adjusted
# plan"). Serialized: only one antd at a time (upload runs share the
# postage counters; the rest just share the port range).
set -euo pipefail
cd "$(dirname "$0")/.."
B=./target/release/perf_bench

echo "=== [1/6] warmup x5 ==="
$B warmup --runs 5 --peers-target 30 --timeout-secs 300 --label baseline

echo "=== [2/6] upload 1 MiB x5 ==="
$B upload --size-mib 1 --runs 5 --label baseline

echo "=== [3/6] upload 32 MiB x5 ==="
$B upload --size-mib 32 --runs 5 --label baseline

echo "=== [4/6] feed-setup (head 0/10/100) ==="
if [ ! -f perf/state/feeds.json ]; then
  $B feed-setup --counts 1,11,101
else
  echo "feeds.json exists — skipping setup"
fi

echo "=== [5/6] feed x5 ==="
$B feed --runs 5 --label baseline

echo "=== [6/6] download x5 (self-uploaded 1 MiB cells + third-party WAV) ==="
{
  grep -h '"reference"' perf/results/baseline-upload-1mib-*.json 2>/dev/null \
    | sed 's/.*"0x\([0-9a-f]*\)".*/\1\/payload-1mib.bin/' | sort -u | head -3
  # 55 MiB WAV live on mainnet since ~2026-05 (PLAN.md F.6), uploaded
  # by the production node — genuine cold third-party retrieval for
  # this fresh-keyed benchmark node.
  echo "c4f8a45301b57d0e36f0f5348ed371aee42ea0b9fe9b3caaf26015d652eedc40/tracks/01%20arrival.wav"
} > perf/state/download-refs.txt
cat perf/state/download-refs.txt
$B download --refs-file perf/state/download-refs.txt --kind bzz --runs 5 --label baseline

echo "=== baseline cheap battery DONE ==="
