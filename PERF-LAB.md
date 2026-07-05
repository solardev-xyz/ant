# PERF-LAB — ant mainnet performance lab notebook

Measure-first optimization campaign on branch `perf-lab` (from
`conformance-harness`). Goal spec: `/home/florian/claude/goal.md`.
Every experiment: Hypothesis → Change → Method → Results → Decision
(KEEP / REVERT), decided on measured mainnet numbers only.

## STATUS

- **Current experiment**: #1 (push pseudosettle) — waiting for the
  512 MiB baseline to release the postage lock
- **Phase**: benchmarking (512 baseline) / exp 1 implemented
- **Next action**: when `perf/run-baselines-big.sh` exits (512 will
  grind, freeze, stall-abort ≈ like the 256 run), transcribe the 512
  row into Baselines, then IMMEDIATELY run
  `perf/run-exp1-pseudosettle.sh` (32 MiB ×5 interleaved A/B, then
  256 MiB ×1 on the treated arm). Verdict criteria in the exp 1
  section. After exp 1: exp 2 A/B (`ANT_PUSH_INFLIGHT_CAP=4` alone,
  then + `ANT_PUSH_CONCURRENCY=128`), then exp 3
  (`ANT_SUBSTREAM_UPGRADE_CAP=64`).
- **Scoreboard**: exp 4 CLOSED (no change needed — measured), exp
  6(a) CLOSED (KEEP, 0/27 → 27/27). Exps 1/2/3 implemented +
  committed, env-gated off, unmeasured. Exp 5 and 6(b) not started.
- **In-flight hypotheses**: unsettled push debt is the stall (256
  evidence conclusive; exp 1 tests the fix). Exp 2's cap should
  compose with higher job width (the 2026-05 "256 collapses" predates
  per-peer caps).
- **Long-running processes**: `perf/run-baselines-big.sh` — 512 MiB
  run on port 3634, data dir `/tmp/ant-perf-upload-3634-1783265452`,
  started 15:30:52Z. Only upload-capable daemon (postage lock).
- **Binaries**: `target/release/antd` = HEAD (all experiments
  env-gated off; feed fix active). `perf/state/antd-baseline-bin` =
  pre-experiment snapshot for A/B control arms.

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
(`--antd-bin-b` / `--env-b`); wall-clock window + peer count recorded
in every result JSON; utilization checked before/after every upload
run.

**Bee reference uploads are OFF the table on this batch**: bee keeps
its own stamperstore and would issue `(bucket, index)` from 0 on the
same immutable batch — colliding with the lab issuer. A bee upload
reference row needs a second batch (user decision). Bee *download*
reference (ultra-light, no stamps) remains feasible and is planned
alongside the final gate's smoke run.

## Baselines

Pre-experiment code = commit `7d2880f` ancestry (branch point
`cc9ba84`). Config: cheque-less light node (pseudosettle only), fresh
data dir per run, shared lab issuer, ports 3633–3641. Peer counts and
wall-clock windows per run in the JSONs.

### Upload (antctl job path, fresh random content)

| size | outcome | median KiB/s (spread) | median secs | requeues |
|---|---|---|---|---|
| 1 MiB ×5 | 5/5 ok | 152.3 (35.8–232.7) | 7 (4–29) | 0 |
| 32 MiB ×5 | 5/5 ok | **489.2 (470.8–534.7)** | 67 (61–70) | 0 |
| 256 MiB ×1 | **stall-aborted** | 82.4 effective | 3180 | 676 |
| 512 MiB ×1 | **terminated at 50.7 %** | 51.2 effective | 5193 | 21 550 |

**256 MiB**: decay 459 → 79 KiB/s over 18 min, then tail FROZEN 34 min
at 65 833/66 054 chunks (bytes 100 %; the 221 post-data tree/manifest
chunks unlandable) → stall-abort. 159 801 hard failures ≈ 3.7 failed
attempts per chunk.

**512 MiB**: decay per-5-min window 395 → 180 → 109 → 24 → … → 0,
with brief churn-reset revivals (0/0/0/3/12/71/0 KiB/s); crept at
~14 KiB/s around the 235–260 MiB mark — the goal's "fails at
~250 MiB+" profile exactly. Terminated deliberately at 87 min /
50.7 % (5 h of grind ahead, ~65 K stamps, zero information value):
**856 813 hard failures + 857 871 same-peer retries for 66 482 landed
chunks ≈ 13 failed attempts per chunk.** Both runs: 0 literal
"overdraft", 0 timeouts — it's all bee-side connection kills
(`io: connection is closed` / `unexpected EOF`), i.e. debt
enforcement at SO_LINGER=0.

Window 2026-07-05T14:10–14:17Z, 64–100 peers at start. 1 MiB is
startup/manifest dominated (run 1 at 35.8 was the fresh-daemon cold
outlier). Caveat: the log-derived outcome histograms for these runs
read an empty file (stdout-capture bug, fixed in the harness after
this battery) — requeue counts (0 across all runs) come from the job
view and are trustworthy; deeper per-chunk histograms start with the
256 MiB run.

### Download (cold = fresh daemon; warm = same daemon, 2nd fetch)

| content | ok | cold TTFB s | cold KiB/s | warm KiB/s |
|---|---|---|---|---|
| self 1 MiB a (3a92…) ×5 | 5/5 | 0.14 (0.04–0.25) | 1858 (944–3665) | ~145 000 |
| self 1 MiB b (3bdd…) ×5 | 5/5 | 0.21 (0.13–0.73) | 893 (591–1332) | ~208 000 |
| self 1 MiB c (50c1…) ×5 | 5/5 | 0.12 (0.08–0.77) | 923 (569–1775) | ~203 000 |
| third-party 55 MiB WAV (c4f8…) ×5 | 5/5 | 0.37 (0.18–2.78) | **685 (650–716)** | ~325 000 |

Self-uploaded content (pushed by a *different* daemon minutes
earlier) is 100 % cold-retrievable — upload propagation is healthy.
Warm numbers are the local disk/mem cache, recorded for completeness.

### Feed head resolution — **BROKEN cold (0/45), root-caused**

All 45 cold+warm-pool resolutions across head 0/10/100 returned 404
in 7–10 s. Debug trace (`/tmp/ant-diag-*/antd.log`, 2026-07-05
14:35Z): the sequence finder itself SUCCEEDS ("feed resolved
latest_index=0 reference=531b9cee… probed=11") — the SOC is
network-retrievable — but ant then re-fetches the update's wrapped
CAC **by address from the network** ("fetch root chunk 531b9cee…:
all peers failed … storage: not found"). A wrapped feed-update chunk
only exists *inside* the SOC payload; nobody pushes it standalone.
Bee reads the wrapped chunk straight out of the SOC (its GetWrappedChunk
path), which is why the smoke's bee cross-check passes while ant's own
cold resolution 404s (the uploader daemon masks this via local cache).
→ Experiment 6 scope: use the wrapped chunk carried by the SOC; also
note the finder burns ~7 s at head 0 on a cold pool (800 ms
speculative-probe timeouts folding into "transient miss").

### Session warm-up (process spawn → N BZZ-handshaked peers)

| api-up s | to 30 peers s | peers @end |
|---|---|---|
| 1.51 (1.26–1.76) | 0.25 (0.25–1.76) | 95 (88–97) |

30+ BZZ-handshaked peers ~0.3–1.8 s after the API binds, 5/5 runs —
ant's wait-for-identify pipeline + parallel bootstrap already deliver
what hoverfly's identify-push bought them (their pre-fix figure was
minutes for a 128-session fill). Experiment 4 will verify per-fresh-
session push latencies rather than handshake counts.

## Experiments

### Experiment 1: cheque-less push-side pseudosettle (the stall fix)

- **Hypothesis**: ant's large-upload stall is unsettled push debt.
  Without a chequebook there is NO settlement on the push path (the
  only `PushsyncSettlement` impl was the SWAP service; the
  pseudosettle driver only walks retrieval-side `Accounting` rows —
  ant's own startup log says "outbound SWAP settlement DISABLED —
  uploads will stall after ~20 K chunks"). Bee debits every pushed
  chunk in the same per-peer ledger and, past
  threshold+tolerance, kills connections (SO_LINGER=0 ⇒ everything
  surfaces as resets, never "overdraft"). Mirroring push debits into
  the shared `Accounting` — so the existing pseudosettle driver
  time-settles upload peers exactly like bee's own light nodes do —
  should remove the decay/stall. Free: no cheques, no chain, no BZZ.
- **Baseline evidence (2026-07-05, committed 256 MiB run)**:
  throughput decayed 370 → ~145 KiB/s within the run; then the tail
  FROZE at 65 833/66 054 chunks (bytes 100 %, last 221 tree/manifest
  chunks unlandable for 30+ min → stall-abort). Failure taxonomy from
  the (fixed) log capture: 200 730 × `io: connection is closed`,
  34 335 × `io: unexpected end of file`, 5 475 × dial `no addresses`,
  0 × literal "overdraft", 2 904 shallow receipts. ≈3.7 failed
  attempts per landed chunk. Same-peer transient retry fired 118 141×
  (the 150 ms retry is nearly always doomed — the connection is gone).
- **Change**: `ant-p2p::push_pseudosettle::PushPseudosettle`
  (implements `PushsyncSettlement` over the shared `Accounting`,
  firing the driver's HotHint via the existing threshold logic),
  wired as fallback in `push_with_stamp`/`push_soc_with_stamp` when
  no chequebook is configured. Env-gated `ANT_PUSH_PSEUDOSETTLE=1`
  for the A/B (one build, both arms). Commits: _(pending)_
- **Method**: `perf/run-exp1-pseudosettle.sh` — 32 MiB ×3 interleaved
  pairs (trimmed from 5 for batch budget), then 256 MiB ×1 treated vs
  the same-day baseline 256 curve. Window 17:00–18:04Z, 60–100 peers.
- **Results**:
  - 32 MiB (throughput a wash, as expected — the debt wall barely
    bites at this size): off = 511/515/492 KiB/s; treated = 533/394
    (n=2; run 1 lost to a harness poll bug, fixed in-session). Hard
    failures leaned treated: 275/796 vs 1001/1001/1347.
  - **256 MiB (decisive)**: baseline collapsed 459→79→**0** KiB/s by
    min 18 and froze 221 chunks short for 34 min. Treated run held a
    **sustained ~190 KiB/s plateau** (per-min: 540, 471, 352, 270,
    then 178–230 for 17 straight minutes — the pseudosettle
    refresh-limited steady state) until bytes hit 100 %, with hard
    failures **159 801 → 86 474**, requeues **676 → 187**, and the
    frozen tail **221 → 1 chunk**. That last chunk ground for 35 min
    against ~3 connection-killing candidates (its neighbourhood
    plausibly blocklisted us during the run's own first-minute burst,
    540 KiB/s ≫ per-peer refresh budgets) → stall-abort.
- **Decision**: **KEEP** (`ANT_PUSH_PSEUDOSETTLE` default remains env
  opt-in until exp 2 lands, then flip on). Removes the systemic debt
  collapse; not yet sufficient alone for full-file completion — the
  residual single-chunk hang is the per-peer *spend-rate* problem
  (we outrun refresh budgets early and earn escalating blocklists),
  which is exactly Experiment 2's cap. The two are one mechanism:
  settle what you spend (1) + never outspend a peer's refresh (2).

### Experiment 2: per-peer latency-aware in-flight cap — **KEEP (provisional n=1 at 256 MiB; small-size confirmation pending)**

- **Hypothesis**: without a per-peer cap, 32 concurrent chunks stack
  on the same closest peers; even with pseudosettle (exp 1), the
  early burst outruns per-peer refresh budgets (450 K PLUR/s light)
  → overdraft-class offenses → escalating storer blocklists → the
  residual tail hang. Cap concurrent pushes per peer (latency-aware:
  fast <200 ms ⇒ 2×base, slow ≥2 s ⇒ base/2) to spread spend across
  refresh budgets. Hoverfly's biggest lever (2.6×, overdrafts 18→0%).
- **Change**: `ant-retrieval::push_load::PushLoadTracker` shared via
  `SwarmState`, at-cap peers dropped from `next_push_peer` ranking
  (soft fall-through), begin/end + latency EWMA hooks in the dispatch
  macro. `ANT_PUSH_INFLIGHT_CAP=<base>`; `ANT_PUSH_CONCURRENCY=<n>`
  exists for the width re-test (not yet exercised).
- **Method**: 256 MiB ×1 with `ANT_PUSH_PSEUDOSETTLE=1,
  ANT_PUSH_INFLIGHT_CAP=4` (window 17:59–18:27Z), controls = the two
  same-day 256 runs (baseline; exp1-only).
- **Results** (256 MiB, same-day):
  | arm | outcome | effective KiB/s | data phase | hard failures | tail |
  |---|---|---|---|---|---|
  | baseline | stall-aborted | 82.4 | collapse →0 by min 18 | 159 801 | 221 frozen ∞ |
  | + pseudosettle | stall-aborted | 82.9 | ~190 plateau, 100 % @ ~min 18 | 86 474 | 1 frozen ∞ |
  | + pseudosettle + cap4 | **completed** | **160.8** | 564→255, 100 % @ ~min 12 (~355 KiB/s) | **53 325** | 518 tree/manifest chunks landed in ~15 min |
- **Decision**: **KEEP** (with exp 1; the two are one mechanism —
  settle what you spend + never outspend a peer's refresh). First
  full 256 MiB completion of the lab; reference
  `0xaf4d0d7a…`. Effective 161 KiB/s ≈ 1.95× the committed baseline
  cell (82.4, which never completed). Residual sore spot: the
  post-data tree/manifest tail still grinds (~15 min) — candidates:
  interleave tree pushes with data (job-manager change), or exp 5's
  AOR-aware candidate ordering. Pending: 8 MiB A/B for a clean
  interleaved small-size confirmation + uniform-vs-latency-aware cap
  comparison when budget allows.

### Experiment 4: identify-push session accelerator — **NO CHANGE NEEDED (closed on measurements)**

- **Hypothesis** (hoverfly, transport.rs `prep_connection`): without
  actively pushing identify (+`add_external_address(observed)`) after
  connect and awaiting the ack before the BZZ handshake, bee waits out
  libp2p's ~7–10 s idle-identify interval per session (hoverfly
  pre-fix: 128-session fill ≈ 15 min; post-fix ≈ 5 s).
- **Check first** (the goal mandates it): ant already implements the
  mechanism — `ant-p2p::behaviour` has an explicit
  wait-for-identify → open-BZZ pipeline (`PendingPeer::identify_started`,
  "Opening our BZZ stream before identify has round-tripped makes bee
  stall for the full 10 s, then disconnect"), plus
  `our_identify_useful_for_bee` push heuristics and external-address
  promotion from observed addrs.
- **Measurements** (committed baseline results):
  - Session warm-up ×5: 30+ BZZ-handshaked peers 0.25–1.76 s after
    API-up; ~95 peers steady state within the same few seconds. No
    multi-second per-session tail exists at all.
  - Cold downloads on fresh daemons (fresh connections, first-ever
    streams): TTFB medians 0.12–0.37 s across 4 refs ×5 runs — a
    ~7–10 s idle-identify wait would be unmissable here.
- **Decision**: **NO CHANGE** (kept as-is). The technique's win is
  already banked in ant's connection pipeline; there is nothing left
  to accelerate at the session-establishment layer. Closed without a
  code change — the numbers say the ceiling is elsewhere (accounting
  and per-peer scheduling, see exps 1–2).

### Experiment 6(a): serve feed wrapped CAC from the SOC — **KEEP**

- **Hypothesis**: cold feed resolution 404s because ant re-fetches a
  v2 update's wrapped CAC by address from the network, where it never
  exists standalone (bee serves it straight from the SOC).
- **Change**: `fetcher.put_recovered(inner_cac_addr, wire[97..])` at
  `decode_sequence_update` success in the resolver — one hook covers
  GetFeed, the /bzz feed-manifest walk, and traversal. Unflagged
  (read-path bug fix); regression test
  `get_feed_seeds_wrapped_v2_cac_into_cache`. Commit `19caab5`-era
  (exp code commit).
- **Method**: `perf_bench feed --runs 3` per arm, arm A = pre-fix
  binary snapshot (`perf/state/antd-baseline-bin`, from the running
  baseline process), arm B = fixed build; same topics (heads
  0/10/100), back-to-back windows 16:15–16:20Z, fresh daemon per run,
  ports 3733/3743. Interleaving waived: outcome is categorical and
  the wrapped CACs can never become network-retrievable (only the
  SOCs replicate), so time drift can't flip arm A.
- **Results**: arm A **0/27 ok** (median attempt 10.6 s, all 404
  after successful head resolution); arm B **27/27 ok**, resolved
  indices exact (0 / 0xa / 0x64), cold medians **1.61 / 2.48 /
  4.33 s** for heads 0/10/100, warm ≈ cold.
- **Decision**: **KEEP.** 0 % → 100 % cold feed resolution; also
  removes ~6–9 s of doomed network fetch per resolution. Note for
  6(b): the remaining latency is quantised at multiples of the 800 ms
  speculative-probe deadline (2×/3×/5× for heads 0/10/100) — the
  finder's phases serialise deadline windows; that's the next lever,
  together with warm-path hint caching (warm == cold today).

_(further experiments use the same template)_

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
