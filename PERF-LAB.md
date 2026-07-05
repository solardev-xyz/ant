# PERF-LAB — ant mainnet performance lab notebook

Measure-first optimization campaign on branch `perf-lab` (from
`conformance-harness`). Goal spec: `/home/florian/claude/goal.md`.
Every experiment: Hypothesis → Change → Method → Results → Decision
(KEEP / REVERT), decided on measured mainnet numbers only.

## STATUS

- **Current experiment**: #0 — harness bring-up (perf_bench)
- **Phase**: benchmarking (shakedown)
- **Next action**: evaluate the 1 MiB shakedown result
  (`perf/results/shakedown-upload-1mib-*`), fix any harness issues,
  then start the budget-adjusted baseline plan below.
- **In-flight hypotheses**: none yet.
- **Long-running processes**: possibly a `perf_bench upload … --label
  shakedown` (check `pgrep -af 'antd|perf_bench'`).

### ⚠ Batch budget vs. the full matrix (2026-07-05 arithmetic)

Lab issuer seeded 2026-07-05: fill floor 2 (offset), max 3, ratio
0.094, worst-case remaining 1.9 M chunks. Stochastic reality: for
random addresses over 65 536 buckets, max-bucket fill ≈ mean +
sqrt(2·mean·ln 65536). The **0.8 hard-stop** (fill 25.6) is reached at
≈ 600 K more chunks ≈ **2.3 GiB of uploads**; the 0.6 warn line at
≈ 370 K ≈ 1.4 GiB.

The goal's full protocol needs far more than that: baselines 5×256 MiB
(330 K) + 5×512 MiB attempts (~5×64 K = 320 K even failing at
~250 MiB) + A/B experiment runs + final 256 MiB before/after + 3
consecutive successful 512 MiB runs (400 K) ≈ **1.3–1.5 M chunks ≈ 2×
the whole remaining budget**. A new/topped-up/diluted batch is an
on-chain spending decision = **user-only** (goal stop-conditions).

**Budget-adjusted plan** (autonomous, keeps every verdict honest but
buys headroom for the experiments — flag to the user for a second
batch to run the full-fat matrix):

1. Free/cheap baselines first: warmup ×5 (0 slots), download ×5
   (0 slots), feed-setup + feed ×5 (~120 slots), upload 1 MiB ×5
   (~1.3 K), 32 MiB ×5 (~41 K).
2. Big cells at n=1 for now: 256 MiB ×1 (~66 K), 512 MiB ×1 (expected
   stall ~250 MiB, ~64 K, aborted by the stall guard) — enough to
   characterize the stall; more repeats only if budget allows at end
   or user funds a batch.
3. Experiment A/B verdicts at 8 MiB ×5/arm (~20 K per experiment)
   unless the technique specifically needs bigger files; 32 MiB
   confirmation only for the winner(s).
4. The 3×512 MiB success requirement (~400 K chunks) is **not
   fundable** from this batch after the above — needs the user.
   Recommend: second depth-21+ batch (or dilute to 22+).

### Operational invariants (read before touching anything)

- **Lab issuer state** (`perf/state/postage/<batch>.bin`, gitignored but
  PRECIOUS — losing it means re-issuing already-used stamp indices on an
  IMMUTABLE batch): every upload-capable antd gets this dir symlinked
  into its temp data dir. Only ONE upload-capable antd may run at a
  time (concurrent daemons would fork the counters). Download/feed/
  warm-up daemons run ultra-light (no batch, no key) and are safe to
  parallelize with each other, but never bind the same port.
- Seeding: merged per-bucket max across surviving smoke-run stores
  + flat +2 offset (insurance against runs whose state was lost;
  earlier smoke runs each started counters at 0 in throwaway dirs).
- **Batch budget**: depth-21 immutable batch ⇒ 65 536 buckets × 32
  slots. Utilization ratio = fullest-bucket fill / 32. perf_bench
  refuses upload runs at ratio ≥ 0.8 (goal stop-condition; user
  decision required) and warns at ≥ 0.6. Random 4 KiB chunks ⇒
  ~1 MiB ≈ 260 slots; stochastic bucket overflow makes the practical
  ceiling ~2.4–3 GiB of uploads from zero counters, NOT 8 GiB. Budget
  every experiment before running it; prefer 1/32 MiB cells for A/B
  verdicts and reserve 256/512 MiB for final confirmation runs.
- **Ports**: perf_bench uses 3633–3641 (rotating). mainnet_smoke owns
  2733/2833/2834; antd default 1633 is never used by the lab.
- **antd env per run**: no `GNOSIS_RPC_URL`, no `STORAGE_STAMP_BATCH_ID`
  (issuer loads from the postage-dir scan); upload runs get
  `STORAGE_STAMP_PRIVATE_KEY` only. Politer to the public RPC and
  faster to boot.
- **Results**: one JSON per run under `perf/results/` (committed).
  antd logs land next to them as `*.antd.log` (gitignored). A result
  that isn't committed doesn't exist.

## Environment

- Machine: Linux (user florian), residential-class connectivity.
- ant repo: `/home/florian/claude/ant`, branch `perf-lab`.
- Reference bee checkout: `/home/florian/claude/bee` (buildable).
- Technique reference: `/home/florian/claude/hoverfly` (PERFORMANCE.md
  read in full 2026-07-05; its measured levers, in its environment:
  in-flight cap 2.6×, substream upgrades 1.8×, identify-push ~180× on
  session fill, AOR sort +28%, patient outer retry budget = the
  512 MiB-class stall fix).
- Batch: `SWARM_BATCH_ID` in `ant/.env` (depth 21, immutable,
  batch id starts 1decad4e).

## Benchmark matrix (baselines = task 2)

| Cell | Tool | Runs |
|---|---|---|
| Upload 1 / 32 / 256 / 512 MiB | `perf_bench upload --size-mib N --runs 5` | ≥5 each |
| Cold download (self-uploaded + third-party) + TTFB | `perf_bench download` | ≥5 |
| Feed head resolution, head index 0 / 10 / 100, cold+warm | `perf_bench feed` | ≥5 |
| Session warm-up (daemon start → N usable peers) | `perf_bench warmup` | ≥5 |

Methodology guards: fresh random content per run (seed recorded);
medians + spreads; A/B arms interleaved in one invocation
(`--antd-bin-b`); wall-clock window + peer count recorded in every
result JSON; utilization checked before/after every upload run.

## Baselines

_(pending — filled by task 2)_

## Experiments

_(template)_

### Experiment N: name

- **Hypothesis**:
- **Change** (commits):
- **Method** (exact commands, sizes, run counts, time window, peers):
- **Results**:
- **Decision**: KEEP / REVERT —

## Experiment queue

1. Slow-storer resilience cluster (the 512 MiB stall fix)
2. Per-peer latency-aware in-flight cap
3. Concurrent substream upgrades (check libp2p-0.56 first)
4. Identify-push session accelerator (check ant's handshake flow first)
5. Storage-radius receipt routing
6. Feed head-finding cost model

Additions from reading hoverfly (candidates, only if queue 1–6 close
with budget left): wide dial-fill window decoupled from pool size;
address-space-spread peer selection; peerlist freshness (skip-cache
TTL tuning).

## Dead ends (do not re-run — hoverfly measured these)

- Single shared libp2p swarm for all sessions → collapses to zero
  throughput under concurrent substream load.
- Multi-process / multi-worker uploads from one IP → regression.
- Push buffer > ~128 × multiplier → accounting-mutex pileup collapse.
- SWAP cheque issuance at one-shot scales → no measurable effect.
- Light-client structural note: co-located full bee keeps ~131 stable
  kademlia neighbours; a light client's pool churns. Goal is closing
  the gap + eliminating stalls, not beating bee.
