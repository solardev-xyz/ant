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

### Receive primitives — DONE

| Piece | Where | Verified |
|---|---|---|
| pullsync 1.4.0 **client** | `ant-p2p/src/pullsync.rs` | codec pinned to bee's `cmd/pullvec` golden protobuf bytes (Get/Ack/Offer/Want, LSB-first bitvector, framing) — 7 tests |
| message dispatch (lurker brain) | `ant-p2p/src/messaging.rs` | `classify()` decodes real GSOC + PSS chunks end-to-end and rejects spoofed addresses — 4 tests |

`pullsync::sync_once(bin, start, want)` runs one page (Get → Offer →
Want → Delivery), with a `want` closure so a lurker requests only the
addresses it watches (Delivery bandwidth ≈ 0 when nothing matches).
`messaging::classify()` turns a delivered chunk + `WatchState` (watched
GSOC addresses, registered PSS topics + secret) into a `DecodedMessage`,
trusting content not the delivering peer.

---

## What remains — the live receive wiring

The two receive primitives are complete and unit-tested; what's left is
integrating them into the running node and exposing them over HTTP. None
of it changes the crypto or the wire codec.

1. **Exp 1 — mainnet pullsync viability probe** *(de-risks everything
   below)*. Add a `ControlCommand::Pullsync{Cursors,Sync}` handled in the
   `behaviour.rs` command loop (it already holds a cloneable `Control`;
   pick a peer via `state.routing.closest_peer(target)`), then a small
   conformance probe that boots the stack, connects to mainnet, and calls
   `get_cursors` + one `sync_once`. Confirms a light-advertised peer can
   hold a pullsync stream and measures offer latency. **Open question the
   probe answers:** do mainnet bees serve pullsync to a NAT'd light peer
   in practice (the code says yes; the network sometimes disagrees).

2. **Lurker driver** (`behaviour.rs`). A task that, given a `WatchState`:
   - keeps 2–3 peers resident in the target neighborhood (extend the
     existing `dial_toward_target`/`dial_and_await_deeper` from a
     transient upload burst into a "stay resident near X" mode);
   - per covering peer, runs the live loop `sync_once(bin, start=cursor+1)`
     forever, advancing `start = topmost+1` (the server long-blocks, so it
     parks rather than busy-spins);
   - feeds each `DeliveredChunk` to `messaging::classify()` and pushes
     `DecodedMessage`s onto a broadcast channel, deduping by address.

3. **WS subscribe endpoints** (`ant-gateway`). `GET /pss/subscribe/{topic}`
   and `GET /gsoc/subscribe/{address}` — clone the working
   `chunk_stream.rs` WebSocket handler; registering a subscription updates
   the node's `WatchState` (and sets/mines the neighborhood to reside in),
   and the handler streams matching `DecodedMessage` payloads as binary
   frames. bee-compatible, so bee-js `pssSubscribe`/`gsocSubscribe` work
   unmodified.

4. **`GET /addresses.pssPublicKey`**. Expose the node's PSS key once
   receive works (deferred deliberately — advertising a receive key that
   can't yet decrypt would mislead senders). Needs a persisted `pss.key`
   like bee's dedicated key.

5. **e2e on mainnet**: ant→bee PSS (bee full node as oracle), bee-js→ant
   GSOC, and the **rendezvous-neighborhood** many-to-many test (all app
   clients lurk a topic-derived neighborhood; real full nodes there store
   and serve — no designated full node anywhere). Per-message unique SOC
   ids sidestep GSOC's same-address reserve-overwrite caveat.

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
