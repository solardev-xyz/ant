# ant-gateway

Bee-shaped HTTP gateway in front of `antd`. Lets unmodified bee
consumers — `bee-js`, `curl`, browser proxies — drive read-only
browsing of Swarm mainnet content against `antd` on
`127.0.0.1:1633`.

This crate is the smallest cut of bee's HTTP API that gets the
ultra-light read path working end-to-end (PLAN.md "Tier A"). Anything
write-shaped, SWAP-shaped, or stamp-shaped lives behind the catch-all
`501 {code:501,message:"not implemented in ant"}` until a future
milestone wires it up. The compatibility contract — endpoint matrix,
JSON shapes, error envelope — is normative in [PLAN.md Appendix C][appx-c],
and the work breakdown is in [PLAN.md Appendix D][appx-d].

[appx-c]: ../../PLAN.md
[appx-d]: ../../PLAN.md

## Supported endpoints

| Path                         | Method     | Tier | Notes                                                                |
| ---------------------------- | ---------- | ---- | -------------------------------------------------------------------- |
| `/health`                    | GET        | A    | `{status, version, apiVersion}`. Constant after bind.                |
| `/readiness`                 | GET        | A    | 200 once a BZZ peer is handshaked, 503 otherwise.                    |
| `/node`                      | GET        | A    | `beeMode: "ultra-light"`, swap/chequebook flagged off.               |
| `/addresses`                 | GET        | A    | Static identity — overlay (bare hex), ethereum (`0x…`), publicKey.   |
| `/peers`                     | GET        | A    | BZZ-handshaked peers by overlay; pre-handshake peers omitted.        |
| `/topology`                  | GET        | A    | Bee `kademlia.Snapshot`-shaped: 32-bin map plus summary fields.      |
| `/wallet`                    | GET        | A    | Stubbed zero balances on Gnosis (chain ID 100).                      |
| `/stamps`                    | GET        | A    | Stubbed empty list — no postage in Tier A.                           |
| `/chequebook/address`        | GET        | A    | All-zero address — bee's "feature absent" sentinel.                  |
| `/chequebook/balance`        | GET        | A    | Stubbed zero `totalBalance` / `availableBalance`.                    |
| `/chunks/{addr}`             | GET / HEAD | A    | Returns wire bytes (`span ‖ payload`), matching `chunkstore.Get`.    |
| `/bytes/{addr}`              | GET / HEAD | A    | Joins the chunk tree; honors a single-range `Range` header.          |
| `/bzz/{addr}`                | GET / HEAD | A    | Manifest root — resolves `website-index-document` if present.        |
| `/bzz/{addr}/{*path}`        | GET / HEAD | A    | Walks the manifest, joins data; `Content-Type` from manifest meta.   |
| _everything else_            | _any_      | —    | `501 {code:501, message:"not implemented in ant"}`.                  |

### What's intentionally not here

- Tier-B writes (`POST /bytes`, `POST /bzz`, `POST /chunks`, real
  stamps / SWAP / wallet / feeds / SOCs / tags / pins).
- Web3 v3 keystore (`keys/swarm.key`) — PLAN.md D.3.2.
- ENS resolution (`/bzz/<ens>/...` — currently treated as a literal
  reference; bee-js's reachability checks accept the 400 today).
- Streaming joiner output. The joiner materialises full file bodies
  in memory before handing them to axum. Per-request body size is
  capped at `retrieval::GATEWAY_MAX_FILE_BYTES` (1 GiB) — high enough
  for any realistic web/media payload, low enough that a malicious
  manifest claiming a multi-terabyte span gets rejected before
  allocation. The CLI (`antctl get`) keeps the much tighter
  `ant_retrieval::DEFAULT_MAX_FILE_BYTES` (32 MiB) since its audience
  is interactive use, not a browser.
- ENS / `resolver-options`, bee-style YAML config, `antd init`, the
  `antd start` subcommand shim. `antd`'s existing CLI is unchanged.

## How to run

The gateway is wired into `antd` and starts after the node is up.
Defaults match bee's own (`127.0.0.1:1633`), so unmodified bee-js
clients work without configuration:

```sh
# build
cargo build --release -p antd

# run with the gateway on the default address
./target/release/antd --data-dir /tmp/ant-smoke

# or pick a different port — pass through to bee-js as $BEE_API_URL
./target/release/antd --data-dir /tmp/ant-smoke --api-addr 127.0.0.1:11633

# headless deployments (no HTTP, only the antctl UDS)
./target/release/antd --data-dir /tmp/ant-smoke --no-http-api
```

Smoke tests once peers are connected:

```sh
curl -fsS  http://127.0.0.1:1633/health
curl -fsS  http://127.0.0.1:1633/peers | jq '.peers | length'
curl -fsSI http://127.0.0.1:1633/bzz/<root>/<path>
curl -i    http://127.0.0.1:1633/transactions   # 501, not implemented
```

## Embedding

Embedders that want the bee API surface depend on `ant-gateway` directly:

```rust,no_run
use ant_gateway::{Gateway, GatewayHandle};

# async fn run(handle: GatewayHandle) -> anyhow::Result<()> {
let addr = "127.0.0.1:1633".parse()?;
Gateway::serve(handle, addr).await?;
# Ok(()) }
```

Embedders that don't want the HTTP surface (most importantly the
`ant-ffi` mobile artefact) compile this crate with default features
disabled — the `http-api` feature is the only thing that pulls in
`axum` / `hyper` / `tower`, so the entire HTTP closure drops out.

## Tests

```sh
cargo test -p ant-gateway
```

Status / stub / fallback tests build the production router via the
`testkit` module and drive it through `tower::ServiceExt::oneshot` —
no socket. Retrieval tests reuse the on-disk chunk fixture in
`crates/ant-retrieval/tests/fixtures/bzz-ab7720-13-4358-2645/` (the
same one `bzz_fixture.rs` locks the joiner / mantaray walker against),
spinning up an in-process fake node loop that resolves the
`ControlCommand`s the gateway dispatches.
