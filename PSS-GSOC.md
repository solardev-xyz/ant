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
`ant_pss_out.jsonl` for `go run ./cmd/trojan-vectors check`).

**A light ant node can, today, send a PSS message to any bee full node
and a GSOC update to any GSOC listener.**

### Receive — DONE (GSOC proven live)

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
addresses it watches (Delivery bandwidth ≈ 0 when nothing matches).
`messaging::classify()` turns a delivered chunk + `WatchState` into a
`DecodedMessage`, trusting content not the delivering peer.
`lurker::run` ties them together with neighborhood residency.

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
`201` (mine + push) in ~6 s; combined with the offline proof that bee's
Go `pss.Unwrap` accepts ant-produced trojan chunks, ant→bee PSS is fully
validated.

## What remains

1. **PSS *receive* depth alignment.** GSOC receive is exact because the
   watch target *is* the chunk's address. PSS reception targets the
   node's overlay, but `makeMaxTarget` mines a 2-byte (16-bit) prefix —
   deeper than the lurker's typical bin-12/13 reach — and the chunk's
   closest storer isn't necessarily the peer we pull from (replication to
   it lags). So a mainnet PSS-receive loopback didn't land in the pulled
   bin within the test window. Options to close it: reside deeper / pull a
   small bin-window around the target, pull from several covering peers,
   or accept PSS-receive only when the node sits deep in its own
   neighborhood. The mechanism is proven (GSOC); this is a targeting
   refinement, not a new capability.
2. **`GET /addresses.pssPublicKey`** + a persisted `pss.key` for
   *directed* PSS to the node's own key (topic-broadcast already works
   without one). Deferred so we don't advertise a key before directed
   receive is exercised.
3. **Rendezvous-neighborhood many-to-many** demo (all app clients lurk a
   topic-derived neighborhood; real full nodes there store and serve — no
   designated full node anywhere). Per-message unique SOC ids sidestep
   GSOC's same-address reserve-overwrite caveat. The primitives are all in
   place; this is an app-level composition + demo.

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
