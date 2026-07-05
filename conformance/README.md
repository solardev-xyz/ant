# Conformance harness

Bee-conformance testing for ant at four levels, each using canonical
bee (the sibling `../bee` checkout) as the oracle. Run them in this
order when validating changes; only the mainnet smoke touches the real
network or needs credentials.

## Tier 1 — bee-generated vectors (offline, in CI)

- [`vectorgen/`](vectorgen/) — Go generator compiled **against the bee
  checkout** (go.mod `replace ../../../bee`), so every expected value is
  produced by bee's own code: CAC addresses, SOC signatures + addresses,
  feed identifiers, signed feed-update chunks, postage stamp signatures,
  dispersed-replica addresses, Reed-Solomon parity tables (plain *and*
  encrypted), complete RS-encoded files (every chunk bee's real pipeline
  emits at levels 1–4), and swarm-encrypt vectors (`encryption.json`):
  byte-exact chunk-encryption cases with *injected* keys (bee's
  `pkg/encryption` primitives — the pipeline itself hardwires
  `crypto/rand`, so whole encrypted trees can't be made deterministic)
  plus frozen encrypted trees from bee's real pipeline, incl. an
  encrypted+RS-level-1 composition, that ant's decrypting joiner must
  reproduce.
- [`vectors/`](vectors/) — generated JSON, checked in so tests run
  without Go or the bee checkout.
- Consuming tests: `crates/ant-conformance` (`tests/vectors.rs`,
  `tests/rs.rs`, `tests/rs_encode.rs`, `tests/encryption.rs`) assert ant
  is **byte-exact**.

```sh
cargo test -p ant-conformance            # runs tiers 1 + 2 below
# regenerate after bumping the bee checkout:
cd conformance/vectorgen && go run . -out ../vectors
```

## Tier 2 — differential HTTP runner (offline)

Fires identical HTTP scenarios at two backends and diffs normalized
observations (status, swarm-*/content headers, bodies with volatile
fields masked):

- **oracle**: [`beemock/`](beemock/) — bee's *real* `pkg/api` service
  over in-memory mocks (what `bee dev` used to be). Build once:
  `cd conformance/beemock && go build -o beemock .`
- **subject**: MemNode (`crates/ant-conformance/src/memnode.rs`) — ant's
  production router + retrieval/feed code over an in-memory store.
- Encrypted uploads (`swarm-encrypt: true`) are compared by *shape*:
  per-chunk keys are random, so the 128-hex references (and etags that
  quote them) are masked shape-preserving while decrypted bodies must
  still be byte-identical.
- Intentional differences live in [`divergences.json`](divergences.json)
  with explanatory notes; **any unregistered divergence fails the
  test**. When you change gateway behavior, either make it match bee or
  register it with a precise note.

Scenarios (`crates/ant-conformance/tests/differential.rs`): `bytes`,
`chunks`, `bzz-file`, `bzz-collection`, `encryption`, `soc`,
`feeds-timeline`, `tags`, `stewardship-envelope`, `redundancy`,
`pins` (bee `pkg/api/pin.go` lifecycle + `swarm-pin` upload header,
incl. an encrypted-reference pin), `range-mechanics` (Go
`http.ServeContent` parity on `/bytes`, `/bzz`, and an encrypted
reference: clamping, suffix/open-ended ranges, both 416 shapes,
`multipart/byteranges` with the boundary normalized by the harness,
HEAD ignoring Range), `stamps-buckets` (`GET /stamps/{id}/buckets`),
`batches` (`GET /batches`; ant's no-event-sync subset is a registered
divergence), and `settlements`.

`GET /pins/check` is **not** in the differential: bee's
`pinIntegrityHandler` needs a real `storer.PinIntegrity` (transactional
store + sharky) that the api mocks can't provide — beemock would panic.
Its NDJSON shape is pinned by `crates/ant-gateway/tests/pins.rs`
instead.

The `/chunks/stream` WebSocket upload (bee `pkg/api/chunk_stream.go`)
is exercised by `crates/ant-conformance/tests/chunk_stream.rs`, which
drives both backends over a live WebSocket (the differential Step model
is plain HTTP): same frames in, bee's empty-binary acks out, pre-signed
stamp mode, tag interaction, and bee's exact close codes/reasons on
protocol errors.

The differential test (`crates/ant-conformance/tests/differential.rs`)
runs as part of `cargo test -p ant-conformance` and **skips with a
notice if the beemock binary is missing** (e.g. CI without Go).

## Tier 2.5 — bee-js drop-in pack (offline)

[`beejs/`](beejs/) drives **unmodified bee-js v12** against both
backends and asserts every operation succeeds and every
content-addressed reference is byte-identical across them:

```sh
cargo build -p ant-conformance --bin memgateway
cd conformance/beejs && npm install
MEMGATEWAY_BIN=../../target/debug/memgateway npm test
```

## Tier 3 — mainnet cross-client smoke (opt-in, credentialed, WRITES TO SWARM)

`crates/ant-conformance/src/bin/mainnet_smoke.rs` spawns a **real antd
light node on Swarm mainnet**, uploads real content stamped with a real
postage batch, exercises read-your-own-writes and the live feed flow,
then boots a local **bee ultra-light node** (download-only, unfunded)
that must retrieve everything ant pushed from the actual network.

Requirements — `<repo>/.env` (gitignored; never commit it):

```
GNOSIS_RPC_URL=https://…            # any Gnosis RPC endpoint
SWARM_BATCH_ID=<64 hex>             # funded postage batch
SWARM_OWNER_PRIVATE_KEY=0x<64 hex>  # batch owner key; also signs test feed SOCs
```

Plus binaries: `cargo build --release -p antd`, and a bee binary from
the sibling checkout (`cd ../bee && go build -o /tmp/bee-bin ./cmd/bee`;
point the smoke at it with `BEE_BIN=/tmp/bee-bin` — default path is a
session-specific scratch location).

```sh
cargo run --release -p ant-conformance --bin mainnet_smoke
```

What it writes to the network per run: one small unique text file (a
few chunks + manifest), two sequence-feed update SOCs under a
run-unique topic, their replica/parity chunks at the default redundancy
level, and one envelope stamp issuance. **Costs: stamp slots from the
batch only — no on-chain transactions, no gas.** Content is
content-addressed and unique per run (timestamped), so runs don't
collide. Batch health can be checked with
`antctl postage show --batch-id $SWARM_BATCH_ID`.

Expected result: `18/18 checks passed` (peer warm-up, upload,
immediate self-read, encrypted `/bytes` + `/bzz` uploads with 128-hex
references and self-reads, feed updates 0/1 with immediate latest
reads, stewardship, envelope, bee ultra-light cross-retrieval of the
file and feed, and — the real cross-client encryption proof — bee
decrypting ant's encrypted `/bytes` upload and walking ant's encrypted
mantaray manifest via `/bzz`). Takes ~3–10 minutes depending on network
conditions; ant uses API port 2733, bee 2833/2834.

## Known doc-vs-code drift captured by the vectors

Bee's PARANOID redundancy level is 89 parities / **39** max data shards
per 128-slot node (`GetParities(128) == 89`), while docs.ethswarm.org
describes it as 90/38. The vectors follow the implementation. Bee also
resolves ambiguous v1-length feed payloads nondeterministically (see
`divergences.json`), and defaults every upload to redundancy MEDIUM(1).
