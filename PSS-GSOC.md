# PSS & GSOC on ant — light-node messaging

Status notebook for adding Swarm's two messaging conventions — **PSS**
(Postal Service over Swarm: encrypted, targeted trojan-chunk messages)
and **GSOC** (Graffiti Single-Owner Chunk: unencrypted broadcast to a
neighborhood) — to the ant light client **without turning ant into a full
node**.

Branch: `pss-gsoc` (off `bug-hunt`, so the conformance/perf harness comes
along). Ground truth: bee `@32a4a1ff` (v2.8.1-rc4) and `@ethersphere/
bee-js` 12.2.2.

---

## The idea in one paragraph

Sending is light-node-native in bee itself: both PSS and GSOC senders
just mine a chunk address into a target neighborhood, stamp it, and
pushsync it — no reserve, no full-node role. Receiving is the part bee
reserves for full nodes, but the restriction is **topological, not
cryptographic**: bee's gsoc/pss dispatch has no full-node check; light
nodes miss messages only because nobody *pushes* neighborhood chunks to
them. A light node can instead **pull** those chunks from the full nodes
that already store them, using bee's own `pullsync` protocol — whose
server has no peer-mode gate — and run the exact same dispatch. That's
the "lurker" receive path.

---

## What's built and verified

### Send — DONE

| Piece | Where | Verified |
|---|---|---|
| PSS trojan wrap/unwrap/mine | `ant-crypto/src/pss.rs` | ant unwraps **all 60** bee(Go) golden chunks; bee's Go `pss.Unwrap` accepts **3/3** ant-produced chunks; DH + cipher pinned to bee golden vectors |
| GSOC mine + SOC authoring | `ant-crypto/src/gsoc.rs` | `gsoc_mine` + `build_gsoc_chunk` match **tool-verified bee-js 12.2.2** vectors (owner, SOC address, upload body); output self-validates via `soc_valid` |
| `POST /pss/send/{topic}/{targets}` | `ant-gateway/src/retrieval.rs` | endpoint wraps off-runtime (`spawn_blocking`) + stamps + pushes via the existing `PushChunk`; bee-shaped route/params |
| GSOC send | existing `POST /soc/{owner}/{id}` | bee-js `gsocSend` is a plain SOC upload; ant already accepts pre-signed SOCs, and our authored wire matches bee-js byte-for-byte |

Interop is bidirectional and pinned by golden vectors on both sides — see
`ant-crypto/tests/trojan_interop.rs` (unwraps `trojan_gen.jsonl`; emits
`ant_pss_out.jsonl` for `go run ./cmd/trojan-vectors check`). Honest
caveat: the bee→ant direction (60 golden chunks) runs in CI; the
ant→bee direction is **manual** — the emit test is env-gated
(`ANT_EMIT_PSS=1`) and the Go `check` step is run by hand, so the 3/3
result is not re-verified automatically by the repo.

**A light ant node can, today, send a PSS message to any bee full node
and a GSOC update to any GSOC listener.**

### Receive — working (GSOC + PSS verified live; hardened 2026-07-13)

The full path is wired and passed mainnet e2e (below). An external
review then found real defects behind the green runs — a GSOC
subscription only ever delivered its *first* update, and a closed-but-
quiet WebSocket leaked its lurker — all fixed in the hardening pass
(see **Hardening pass** below). "Verified" here means the listed runs
passed, not that the path was defect-free.

| Piece | Where | Verified |
|---|---|---|
| pullsync 1.4.0 **client** | `ant-p2p/src/pullsync.rs` | codec pinned to bee's `cmd/pullvec` golden protobuf bytes; **live on mainnet** (Exp-1) |
| message dispatch (lurker brain) | `ant-p2p/src/messaging.rs` | `classify()` decodes real GSOC + PSS chunks and rejects spoofs |
| lurker driver (residency + live loop) | `ant-p2p/src/lurker.rs` | **GSOC e2e PASS on mainnet** (bee-js → ant → pull → WS → bee-js) |
| `LurkerSubscribe` control cmd | `ant-control`, `behaviour.rs` | streaming ack → WS fan-out |
| WS `/gsoc/subscribe`, `/pss/subscribe` | `ant-gateway/src/subscribe.rs` | bee-js `gsocSubscribe`/`pssSubscribe` compatible |
| `antctl pullsync` probe | `antctl`, `ControlCommand::PullsyncProbe` | Exp-1 tool |

`pullsync::sync_once(bin, start, want)` runs one page (Get → Offer →
Want → Delivery), with a `want` closure so a lurker requests only the
addresses it watches. **The near-zero-delivery-bandwidth property is
GSOC-only** (exact watched addresses): PSS mode must download every
candidate CAC in the bin window to attempt unwrap, exactly like a bee
full node's `TryUnwrap`. `messaging::classify()` turns a delivered
chunk + `WatchState` into a `DecodedMessage` — content-trust holds for
GSOC (SOC self-binding re-verified) and, since the hardening pass, for
everything: `accept_delivery` rejects any delivery whose bytes don't
hash to its address. `lurker::run` ties them together with
neighborhood residency.

---

## Exp 1 — mainnet pullsync viability: CONFIRMED (2026-07-12)

**A light ant node drove full pullsync against a live mainnet full node.**
Wired `ControlCommand::PullsyncProbe` (peer picked via
`routing.closest_peer(target)`, bin = the peer's proximity order to the
target) + `antctl pullsync <target>`. Ran against a fresh mainnet daemon
(overlay `d7a2…`, 100 peers):

- **cursors**: `cursors_ok=true` in **84 ms** — peer returned its 32 bin
  cursors + reserve epoch.
- **sync page** (bin 7, historical `start=1`): `sync_ok=true`, **5/5**
  then **50/62** chunks delivered (Get→Offer→Want→Delivery) in ~1.0–1.1 s.
- Reproducible across peers (`d63cea…`, `d62ba4…`).

Nuance that shapes the lurker: a full node's reserve only holds its
**deep** neighborhood, so pulling a *shallow* bin (a target far from any
connected peer) correctly long-blocks on an empty interval — which is
also why a live `start=cursor+1` pull parks until a new chunk arrives.
The lurker must therefore reside in / target the **deep neighborhood bin
a covering peer actually stores**, which is exactly the case that worked.
**The single biggest risk (would mainnet bees serve a NAT'd light peer?)
is retired.**

## GSOC receive e2e — PASS on mainnet (2026-07-12)

The full receive path is wired and proven live. `ControlCommand::
LurkerSubscribe` spawns `ant_p2p::lurker::run`, which resides in the
target neighborhood (dials toward it, waits for the deepest covering peer
via the existing neighborhood-dial primitive), pulls its bin live
(`sync_once(bin, start=cursor+1)`, bounded 20 s per round so it re-dials
and re-picks deeper peers), classifies each delivered chunk, and streams
decoded payloads over `GET /gsoc/subscribe/{address}` /
`GET /pss/subscribe/{topic}` WebSockets as binary frames.

**End-to-end, driven entirely by bee-js against a live ant node:**
`bee.gsocMine` + `bee.gsocSend` → ant `POST /soc` → pushed into the
neighborhood → ant lurker pulls it back from a real full node via
pullsync → `classify()` decodes → WS → `bee.gsocSubscribe` received the
**exact payload**. The residency step was load-bearing: the lurker
deepened from bin 5 (a shallow *forwarder*) to bin 13 (an actual
*storer*) before the message came through.

**PSS send** verified live too: `POST /pss/send/{topic}/{targets}` → HTTP
`201`; combined with the offline proof that bee's Go `pss.Unwrap` accepts
ant-produced trojan chunks, ant→bee PSS is fully validated.

## PSS receive e2e — PASS on mainnet (2026-07-12)

The first PSS-receive loopback missed: `makeMaxTarget` mines a 16-bit
prefix, so the chunk's storers sit at bin ~14–16 — deeper than a light
node's steady covering peers (bin ~13). The chunk was network-retrievable
(retrieval dials to the storer) but our single steady peer wasn't a
storer, so pullsync missed it. Three changes in `lurker.rs` closed it:

- **Multi-peer**: pull concurrently from the N=5 closest covering peers
  (a fresh chunk lands on the nearest storer and replicates outward —
  several peers catch it whoever got it first).
- **Bin window**: for PSS (address not exact, unlike GSOC) pull a few
  deeper bins past the base bin.
- **Deep residency**: `reside()` dials toward the target for its full 15 s
  budget instead of stopping at the first plateau, pulling the actual
  storers into our connection set — like the retrieval path's
  dial-and-await-deeper.

**Verified e2e**: bee-js `pssSubscribe` + `pssSend` (topic-broadcast, no
recipient) → ant `/pss/send` → ant lurker (now reaching **bin 14–15**)
pulls the trojan chunk → topic-derived unwrap → WS → bee-js received the
exact payload.

## Rendezvous many-to-many — PASS on mainnet (2026-07-12)

The payoff: a **group channel with no central server**. A "room" is a
shared PSS topic broadcast into a neighborhood; every participant lurks it
and every participant can send. Demo (`perf/pss-gsoc-demos/
rendezvous_demo.mjs`): one bee-js client against one ant node sent three
serially-spaced messages into a shared room; the lurking subscription
received all 3/3 — multi-*message* reception through one node.

## Multi-node multi-sender rendezvous — PASS on mainnet (2026-07-13)

`perf/pss-gsoc-demos/multi_node_rendezvous.mjs`: **two independent ant
nodes** (fresh overlays `1b63…` and `cb8c…`), one shared room hosted in
node A's neighborhood. Senders alternate across both nodes (Alice@A,
Bob@B, Carol@A, Dave@B — node B pushes into a foreign neighborhood);
subscriber on A lurks natively, subscriber on B lurks A's neighborhood
via the `?neighborhood=` override from a completely different overlay.
**Enforced result: subscriber A received 4/4**, ~1 s per message.

What this proves, precisely (a round-3 review correctly flagged the
earlier "many-to-many" wording as overstated): senders on **two
independent nodes** both deliver into a shared room that **one reliable
subscriber** reads in full. That is multi-node multi-*sender* → single
reliable receiver — a many-to-**one** receive. It does *not* yet prove
symmetric many-to-**many**, which needs a **second reliable
subscriber** (a second node co-resident in the room's neighborhood via
overlay mining) — not standable with the current two-node / one-batch
setup. Subscriber B here lurks a *foreign* neighborhood from a distant
overlay (the ~bin-12 unreliable case documented below), so its count is
an **observation only**; it happened to get 4/4 in the recorded run — a
first positive data point for foreign-neighborhood rooms, not a
guarantee, and explicitly not the basis for the PASS. Operational note:
the two nodes MUST stamp with different batches or two independent
issuers allocate colliding stamp indices in the room's bucket (this run
reused
one batch across both; distinct batches are the clean setup).

Two fixes made multi-message reception reliable: **continuous pullers**
(the driver no longer aborts its pull tasks to re-dial, which had left a
gap that dropped messages) and spacing sends so each lands cleanly.

A room in a *foreign* neighborhood (owned by nobody, e.g.
`keccak("room:"+name)`) additionally needs each participant to reside
*there* — `GET /pss/subscribe/{topic}?neighborhood=<overlay>` targets it,
but a light node far from an arbitrary neighborhood only reaches ~bin 12
and reception is unreliable at that depth; a room in (or near) a
participant's own neighborhood is reliably reachable. Mining participant
overlays into a shared room neighborhood is the path to arbitrary rooms.

## Hardening pass (2026-07-13) — two external review rounds

> Round 2 (a second reviewer) found seven more issues in the round-1
> fixes; all are now addressed in the "Round 2 hardening" section below.
> This section documents round 1.

## Round 1 — external review

An independent adversarial review of the branch confirmed the crypto
port (ECDH stripping, cipher, parity, EIP-191, `topic mod n` — no
defects) but found seven real receive-path/API issues. All are fixed:

1. **GSOC delivered only the first update, ever** (High). The dedup set
   keyed on chunk address alone; GSOC reuses one stable address per
   room, so update #2+ was dropped — and a spoofed first delivery could
   poison the address. Fix: dedup on `(address, keccak(data))` in a
   bounded oldest-first-eviction set (`lurker::Seen`); the old
   clear-the-whole-set-at-cap behavior is gone too. The mainnet e2e
   never caught this because it sent one update per subscription.
2. **A closed-but-quiet WebSocket leaked its lurker** (High). The
   node-side forwarder only noticed the dead subscriber while relaying
   a message; with none arriving it blocked forever and the lurker (and
   its pull streams) ran on. Fix: `forward_lurker_messages` selects on
   `ack.closed()`; regression test
   `lurker_forwarder_stops_when_quiet_subscriber_leaves`.
3. **Subscribe/restart message gaps** (High). The old blocking 15 s
   reside phase meant WS-open ≠ pulling; and a died puller's
   replacement re-read the *current* cursor, skipping the outage. Fix:
   pullers start immediately and coverage tops up on every
   connectivity change (`peers.changed()`-driven); per-`(peer, bin)`
   resume positions survive puller restarts; fresh pullers start
   `PULL_BACKLOG` behind the cursor. Semantics are now explicitly
   **at-least-once with bounded replay** near subscribe/handover —
   content-keyed dedup collapses cross-peer duplicates.
4. **`/pss/send` was an unauthenticated CPU-exhaustion path** (High).
   Mining (up to ~2^24 BMT hashes on all cores for a 3-byte target) ran
   before any batch check, uncancellable, unbounded concurrency. Fix:
   batch usability pre-checked via `PostageList` *before* mining;
   global 2-permit mining semaphore (503 when saturated); targets
   deduped and capped at 64; `pss::wrap_cancellable` stops miner
   threads on client disconnect or timeout.
5. **Deliveries weren't bound to offers or content-validated** (Med).
   `accept_delivery` now rejects unsolicited/duplicate addresses,
   requires the 113-byte stamp, and verifies CAC BMT / SOC self-binding
   — a peer can no longer replay a captured chunk under fresh
   addresses to bypass dedup.
6. **Unbounded lurker fan-out** (Med, partially addressed). Node-side
   admission cap: at most 8 concurrent lurker subscriptions
   (`MAX_LURKER_SUBSCRIPTIONS`); at the cap the subscription is
   refused. Global bandwidth budgets and a shared watch registry that
   fans one pull out to N subscribers remain open (below).
7. **Ex-covering-peer pullers accumulated** (Med). The driver now keeps
   a desired `(peer, bin)` set and retires obsolete pullers only once
   replacement coverage is fully live — bounded puller set, no
   coverage gap during handover.

Also surfaced by the review and fixed here: two gateway tests were
already red on the branch (`pss_send_falls_through_to_501` asserted the
pre-implementation 501; the `/stamps` fixture forgot `usable: true`,
which serde's `default_true` can't supply on an in-process channel) —
neither we nor the reviewer had run the gateway suite. The full
workspace is green again; `cargo fmt`/`clippy -D warnings` clean.

### The second same-address bug (found live, 2026-07-13)

Re-verifying the dedup fix on mainnet (`gsoc_two_updates_diag.mjs`:
send two different payloads to ONE GSOC address, require both via the
subscription) still failed — update #2 was 201-acked but **never
arrived at any storer**. Page-level pullsync logs showed all five
covering peers offering+delivering update #1 within ~2 s and then
*zero* offers for #2. Root cause was ours, in `ant-postage`:
`sign_stamp_bytes` returned the byte-identical cached stamp for a
known address (same index, same timestamp), and bee's reserve rejects
a same-`(batch, index)` put whose timestamp is not strictly newer
(`ErrOverwriteNewerChunk`) — receipt green, chunk dropped,
network-wide. Bee's own stamper reuses the slot but refreshes the
timestamp and re-signs (`pkg/postage/stamper.go`); ant now does
exactly that. After the fix the two-update test **passes live**
(update #2 delivered over the WS ~2 s after push).

Two morals worth pinning: (1) the client-side dedup bug (finding #1)
and this server-acceptance bug were stacked — fixing either alone
still loses every GSOC update after the first, and only a live
two-update test could prove the pair closed; (2) the first diagnostic
falsely blamed the receive side because `GET /chunks` has no cache
bypass and happily served our own pre-push local copy as "network"
state — a gateway parity gap (bee honours `swarm-cache: false`) still
open below.

### Post-hardening mainnet verification (2026-07-13, batch 5)

| Test | Result |
|---|---|
| `gsoc_e2e.mjs` (single update) | ✅ PASS |
| `gsoc_two_updates.mjs` (same-address regression) | ✅ PASS after the re-stamp fix (failed 3× before it — see below) |
| `pss_e2e.mjs` (topic broadcast) | ✅ PASS |
| `rendezvous_demo.mjs` (3 msgs, one node) | ✅ PASS |
| `multi_node_rendezvous.mjs` (2 nodes, 4 senders alternating → 1 reliable receiver) | ✅ PASS — multi-node multi-sender; A 4/4 (enforced); far-node B 4/4 (observed). Many-to-**one**; symmetric many-to-many still TODO |

Lab note: debug-build keccak made 2-byte-target mining take ~30 s
(measured) and intermittently trip the request timeout; the workspace
`Cargo.toml` now pins `opt-level = 3` for `sha3`/`keccak`/`ant-crypto`
in the dev profile so lab daemons mine at production speed (~2 s
mine+push).

### Shared lurker registry (2026-07-13)

`ant_p2p::lurker_registry` now keys one pull pipeline per target
neighborhood: the first subscriber spawns it, later ones attach, a
dispatcher fans each decoded message out to exactly the matching
subscribers, and the lurker follows the live **union** watch (shared
`RwLock<WatchState>`, re-read per round — joins/leaves never restart
pullers or lose resume positions). Budgets: ≤8 distinct neighborhoods,
≤64 subscribers per neighborhood, ≤256 union-watch items (each PSS
topic is per-candidate-chunk ECDH work, so it's capped too), and a
subscriber 64 messages behind sheds instead of head-of-line-blocking
the rest. Attaching to an existing neighborhood is cheap *to pull*
(shared stream) but not free — it still adds a fan-out channel, a
per-message routing pass, and unwrap work — hence the caps. Supersedes
the interim per-subscription semaphore and closes the shared-fan-out
half of review finding #6; **global** (cross-neighborhood) bandwidth
budgets remain open.

## Round 2 hardening (2026-07-13) — second external review

A second reviewer found seven issues in the round-1 fixes; all
addressed:

1. **Epoch-blind resume (High)** — saved pull positions were keyed
   `(peer, bin) → start` with no reserve epoch. A peer that wipes its
   reserve resets its cursors, so an old (high) position would park the
   puller above every new binID forever. Now positions are
   `(peer, bin) → (epoch, start)` and the driver discards a position
   whose epoch doesn't match the freshly-fetched cursor's — bee's
   puller drops saved intervals on epoch change for the same reason.
2. **Sequential cursor fetch / no timeout (Med)** — coverage setup
   awaited `get_cursors` peer-by-peer with no bound, so one silent
   closest peer stalled all later pullers. Now fetches run concurrently
   (`join_all`) each under a 5 s `CURSOR_FETCH_TIMEOUT`.
3. **Handover didn't wait for live coverage (Med)** — a replacement
   puller was considered "covering" the instant it was `spawn`ed, so
   obsolete pullers could be retired before the replacement opened a
   stream. Now a puller is marked **ready** only after its first
   completed pull round, and the handover retires old coverage only
   once every desired `(peer, bin)` is ready.
4. **Deliveries not bound to the full offered identity (Med)** — the
   wanted set was address-only and delivery validation only checked
   stamp length. Now `sync_once` retains the offered `(batchID,
   stampHash)` per address and `accept_delivery` recomputes
   `keccak(stamp)` and requires it to match one offered identity —
   bee's own `(address, batchID, stampHash)` binding.
5. **Non-monotonic re-stamp timestamp (Med)** — the re-stamp used the
   raw wall clock; equal clock resolution or a backward step would
   re-create the `ErrOverwriteNewerChunk` rejection it fixes. Now
   `ts = max(now, prev + 1)`, guaranteeing strictly-newer.
6. **Unbounded sidecar growth (part of #5)** — every re-stamp appended
   and fsynced a new `.stamps` record, growing the file without bound
   for a busy GSOC room and slowing startup scans. Re-stamps now update
   the in-memory stamp only (the persisted slot record is unchanged);
   the sidecar stays one record per address.
7. **Unbounded same-neighborhood subscriptions + mining threads
   (High)** — see the registry caps above (subscribers/union-watch),
   and mining now claims worker threads from a **process-wide budget**
   (`ant-crypto`) so aggregate mining across all `/pss/send` jobs never
   exceeds the machine's core count — two jobs of 16 workers can no
   longer oversubscribe every core. Authentication of `/pss/send`
   (still unauthenticated, like bee's) remains a deployment concern.

Not a full fix, disclosed: `/pss/send` is still unauthenticated (matches
bee); the mining budget + 2-job semaphore + cancellation bound the CPU
cost but a flood of valid-batch requests can still occupy the miner.
Rate-limiting / auth is a gateway-deployment decision.

## Round 3 hardening (2026-07-13) — third external review

A third reviewer found four issues in the round-2 fixes; all addressed:

1. **Quiet bins stalled handover (Med)** — readiness was signalled only
   after a *completed* pull page, but bee's server holds the Offer
   indefinitely on a quiet bin (and our 20 s round timeout just reopens
   the stream), so a legitimately-covering puller on a quiet bin never
   became "ready", `all_ready` stayed false, and obsolete pullers
   accumulated during churn. Readiness now fires inside `sync_once`
   right after the `Get` is sent (stream open, request accepted),
   **before** the blocking Offer read. Added `HANDOVER_MAX_OVERLAP`
   (60 s) as a backstop so a permanently-wedged replacement peer can't
   pin obsolete coverage forever.
2. **Re-stamp monotonicity lost across restart (Med)** — round 2 made
   re-stamps update only the in-memory timestamp, so a restart reloaded
   the *original* stamp; under a clock rollback the next re-stamp could
   re-emit a timestamp the network already saw. The `.stamps` sidecar
   now stores exactly one fixed-size record per address and a re-stamp
   overwrites it **in place** (`overwrite_stamp_record`, tracked by
   per-address byte offset) — no file growth *and* the latest timestamp
   persists. New `restamp_timestamp_survives_restart` regression test.
3. **Mining cap bypassed under contention (Med)** — a zero-slot job
   still spawned one worker, so N concurrent callers could run
   `C + (N-1)` threads and the process-wide invariant was false. The
   budget is now a proper counting semaphore (`MiningPool`,
   condvar-based): a job with no free slot **waits** (cancellation-aware,
   50 ms poll) rather than fabricating a worker, so aggregate threads
   never exceed `C`. A cancelled/timed-out request returns
   `PssMiningCancelled` from the wait.
4. **Demo proved many-to-one, not many-to-many (Low)** — corrected the
   claim (see "Multi-node multi-sender rendezvous" above): the demo
   enforces multi-node multi-*sender* → one reliable receiver; symmetric
   many-to-many (two reliable subscribers) is renamed as still-TODO,
   not claimed.

## What remains (optional)

1. **Arbitrary-neighborhood rooms**: reliable only when participants are
   resident near the room (mine overlays into it, or pick rooms in
   well-connected regions). The `?neighborhood=` override is in place; the
   residency depth a light node can reach far from home is the limit.
2. **`GET /addresses.pssPublicKey`** + a persisted `pss.key` for
   *directed* PSS to the node's own key (topic-broadcast already works
   without one).
3. **GSOC same-address rooms**: a pull-based reader sees only the latest
   write (the reserve keeps one SOC per address); per-message unique ids
   avoid this. PSS topic-broadcast is the cleaner many-to-many vehicle.
4. **Tuning**: residency budget / covering-peer count / bin window are
   conservative defaults; expose as env knobs for busy rooms.
5. **Global bandwidth budgets**: the registry (done, above) gives
   shared fan-out and a neighborhood cap; per-node stream/bandwidth
   budgets across all pipelines remain.
6. **Automate ant→bee interop**: run `trojan-vectors check` in CI (or a
   make target) so the reverse direction stops being a manual result.
7. **`GET /chunks` cache bypass**: honour bee's `swarm-cache: false`
   header (`ControlCommand::GetChunk` needs a `bypass_cache` flag) —
   without it a node can't read the *network's* view of a chunk it
   itself pushed, which actively misled the two-update diagnosis.
8. **Symmetric many-to-many demo**: mine two node overlays into one
   shared room neighborhood so **both** subscribers are reliable, and
   enforce both in the demo — the true many-to-many proof the current
   many-to-one `multi_node_rendezvous.mjs` doesn't reach. Needs a second
   on-chain postage batch.

## Demo scripts

`perf/pss-gsoc-demos/` (run against a live `antd` with a usable batch;
bee-js 12.2.2): `gsoc_e2e.mjs` (bee-js gsocSend → ant lurker →
gsocSubscribe), `pss_e2e.mjs` (bee-js pssSend/pssSubscribe topic-
broadcast), `rendezvous_demo.mjs` (multi-sender group room).

---

## Key facts pinned during the build (so we don't relearn them)

- **bee's gsoc/pss dispatch has no full-node gate** — the wall is
  topology, not code (`bee/pkg/pullsync/pullsync.go` handler, no peer-mode
  check; `bee/pkg/gsoc/gsoc.go` handler fires on any incoming SOC).
- **PSS trojan quirks reproduced** (all in `pss.rs`): ECDH shared key is
  `keccak256(minimal_be(x) ‖ topic)` with Go-style leading-zero stripping;
  cipher is `keccak256(keccak256(key ‖ le32(ctr)))` over 32-byte segments;
  ciphertext padded to 4032 with **raw random** (not keystream); hint =
  `keccak256(sk ‖ topic)[:8]` hashed as the BMT span; byte-28 nonce parity
  mined but ignored on read (bee's `|`-bug always lifts 0x03 — we lift
  0x02, identical since ECDH is x-only); topic-derived broadcast key =
  `topic mod n`.
- **GSOC mining is uncapped proximity** — bee-js counts raw leading bits
  (0..256), *not* `swarm.Proximity` (which clamps at MaxPO). Reusing the
  capped one would desync sender and receiver.
- **SOC signatures are EIP-191** over `keccak256(id ‖ cac_addr)`, v∈{27,28}
  — matches ant's existing `soc_valid` (its real `blog.swarm.eth` fixture
  passes), so GSOC authoring reuses `sign_handshake_data`.
- **pullsync live = poll loop**, not a subscription: repeated
  `Get(bin, start=prev_topmost+1)`; the server blocks until a new chunk
  arrives. One page per stream; each stream does the libp2p headers
  preamble first. Delivery placeholders (empty address) still consume a
  want slot. Stamp = 113 bytes; server rate-limits at 250 chunks/s/peer.

## Runtime knobs (planned)

- `POST /pss/send` honours `Swarm-Postage-Batch-Id` (required) and
  `?recipient=<pubkey hex>` (optional; absent ⇒ topic-derived broadcast).
- Lurker residency + WS subscriptions will be driven by the subscribe
  endpoints; a resident-neighborhood override belongs in `antd` config
  next to `overlay_nonce` (the overlay is already minable into a chosen
  neighborhood via the persisted nonce).
