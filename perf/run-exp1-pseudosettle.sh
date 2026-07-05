#!/usr/bin/env bash
# Experiment 1 A/B: cheque-less push-side pseudosettle
# (ANT_PUSH_PSEUDOSETTLE=1) vs baseline, interleaved arms, one build.
#
# Phase 1: 5 interleaved pairs at 32 MiB (the throughput-decay cell).
# Phase 2 (only if arm B wins or ties phase 1): 1× 256 MiB on arm B
#          to compare against the committed baseline decay curve.
# Phase 3 (the stall test) is run separately once phases 1–2 read out.
set -euo pipefail
cd "$(dirname "$0")/.."
B=./target/release/perf_bench

# Budget note (2026-07-05 17:00Z): utilization 0.469 after the big
# baselines; ~100K chunks to the 0.6 warn line. 3 pairs at 32 MiB
# (49K) + one 256 MiB treated run (66K) is the affordable decisive
# set; anything bigger waits for a user-funded batch.
echo "=== exp1 phase 1: 32 MiB x3 interleaved A/B ==="
$B upload --size-mib 32 --runs 3 \
  --label exp1-off \
  --label-b exp1-pseudosettle --env-b ANT_PUSH_PSEUDOSETTLE=1

echo "=== exp1 phase 2: 256 MiB x1 arm B ==="
$B upload --size-mib 256 --runs 1 \
  --label exp1-pseudosettle-256 --env ANT_PUSH_PSEUDOSETTLE=1 \
  --stall-abort-secs 2100 --run-timeout-secs 14400

./target/release/perf_bench status
echo "=== exp1 A/B DONE ==="
