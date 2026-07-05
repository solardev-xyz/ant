# PERF-LAB — ant mainnet performance lab notebook

Measure-first optimization campaign on branch `perf-lab` (from
`conformance-harness`). Goal spec: `/home/florian/claude/goal.md`.
Every experiment: Hypothesis → Change → Method → Results → Decision
(KEEP / REVERT), decided on measured mainnet numbers only.

## STATUS

- **Current experiment**: #0 — baselines (cheap cells)
- **Phase**: benchmarking
- **Next action**: when `perf/run-baselines-cheap.sh` finishes
  (warmup ×5 → upload 1 MiB ×5 → 32 MiB ×5 → feed-setup → feed ×5 →
  download ×5), transcribe median tables into "Baselines" below,
  commit results, open the PR. Then run 256 MiB ×1 and 512 MiB ×1
  (stall characterization) overnight. Meanwhile: Experiment 1 recon
  (PLAN.md GGUF stall history around lines 700–930).
- **In-flight hypotheses**: none yet.
- **Long-running processes**: `perf/run-baselines-cheap.sh` in the
  background (spawns one antd at a time on ports 3633–3641; kills
  each after its run). Shakedown result already committed:
  1 MiB = 102 KiB/s, clean histogram.

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

### Recon notes (2026-07-05, code reading ahead of the experiments)

- **Ant's push path today** (`ant-retrieval::fetcher::push_stamped_chunk`):
  closest-first with a 5 s preemptive hedge, per-chunk skip list,
  `MAX_PUSH_ERRORS = 32`, shallow receipts accepted after
  `MAX_SHALLOW_ATTEMPTS = 12` + neighbourhood-deepening dials. Job
  manager (`ant-node::uploads`): `MAX_PUSH_CONCURRENCY = 32`,
  `PUSH_TIMEOUT = 120 s`, unbounded requeue (never fails on
  transients) + `stalled` flag; the bounded "exhausted N retries"
  error is only the *heal/re-push* path (`PER_CHUNK_RETRY_BUDGET =
  8`, was 5 in the goal-era logs).
- **Exp 2 is applicable**: no per-peer in-flight cap anywhere on the
  push path — 32 concurrent chunks can stack on the same closest
  peers. No latency EWMA either.
- **Exp 3 is applicable**: stock `libp2p-stream 0.4.0-alpha` (which
  ant uses for every protocol stream, incl. pushsync) has a single
  `pending_upgrade` slot per connection — outbound substream upgrades
  are serialized exactly as hoverfly found; their keyed-upgrades patch
  (`hoverfly/src/protocols/stream_pool/handler.rs`) is the reference.
- **Exp 4 likely already implemented**: `ant-p2p::behaviour` has a
  wait-for-identify → open-BZZ pipeline, identify-push usefulness
  heuristics, and external-address plumbing ("Opening our BZZ stream
  before identify has round-tripped makes bee stall for the full
  10 s, then disconnect" — their words). Baseline warm-up is
  consistent with it working: 30–75 BZZ-handshaked peers within
  ~2 s of process start, 5/5 runs. Experiment reduces to verifying
  fresh-session push latency shows no ~10 s idle-identify tail.
- **Exp 6 baseline**: ant's `resolve_sequence_feed` already does
  exponential bracket + binary search + parallel look-ahead
  (phases 1–3). Hoverfly's edge, if any, is concurrent k-ary
  narrowing + anchor re-probing against false-absents on cold pools.
- **Settlement config**: the lab (like the smoke) has NO chequebook —
  push settlement hook (`PushsyncSettlement`, SWAP cheques) is
  disabled; the daemon relies on pseudosettle/tolerance. Production
  runs cheques + pseudosettle. The goal's "reliably fails at
  ~250 MiB" symptom (PLAN.md phase 7b, GGUF post-mortem) predates
  phases 7b–7d; the current branch already completed a 256 MiB
  mainnet upload once (0.3.14-era, with cheques). Tonight's 256/512
  baselines establish what the *cheque-less* lab config does today.
- **Extra queue candidate** (from hoverfly "What hoverfly is"):
  ant advertises `full_node = false` in the BZZ handshake
  (`handshake_outbound`); hoverfly measured light advertisement as a
  5–6× throughput regression (450 K vs 4.5 M PLUR/s per-peer refresh)
  and ships `full_node = true`. Cheap flag experiment IF budget
  remains after the six.

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
