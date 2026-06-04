//! Runtime handle the gateway uses to talk to the node loop.
//!
//! The gateway owns no node state itself; everything it needs is passed
//! in via `GatewayHandle`:
//!
//! - `status`: the same `watch::Receiver<StatusSnapshot>` `antctl status`
//!   reads from. Cheap to clone, so each request can call `.borrow()`.
//! - `commands`: the single-producer channel into the node loop, used to
//!   dispatch retrieval requests (`GetChunkRaw`, `GetBytes`, `GetBzz`).
//! - `identity`: the static bits resolved once at `antd` startup
//!   (overlay / ethereum address / libp2p public key). The gateway
//!   never derives these itself — keeping the identity loader in `antd`
//!   means the Web3 v3 keystore (PLAN.md D.3.2) can land later without
//!   touching `ant-gateway`.

use ant_control::{ControlCommand, GatewayActivity, StatusSnapshot};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crate::chain::ChainContext;
use crate::cors::CorsConfig;
use crate::tags::TagRegistry;

/// Static identity surface used by `/addresses`. All fields are owned
/// strings so cloning the handle is cheap and the gateway can drop the
/// original without affecting in-flight requests.
#[derive(Debug, Clone)]
pub struct GatewayIdentity {
    /// Swarm overlay as bare lowercase hex (no `0x` prefix), matching
    /// bee's `swarm.Address.MarshalJSON`.
    pub overlay_hex: String,
    /// 20-byte Ethereum address as `0x`-prefixed lowercase hex.
    pub ethereum_hex: String,
    /// Compressed secp256k1 public key (33 bytes) as bare lowercase hex.
    pub public_key_hex: String,
    /// libp2p peer-id, used internally for diagnostics / topology rows
    /// — not part of the bee `/addresses` payload.
    pub peer_id: String,
}

/// Live wiring the gateway needs to serve every Tier-A endpoint.
///
/// Cloning is cheap: the `watch::Receiver`, `mpsc::Sender`, and `Arc`
/// fields are all reference-counted handles. Each request handler clones
/// the bits it cares about (status snapshot for status, commands sender
/// for retrieval) so axum's per-request handler runs without borrowing
/// the shared state.
#[derive(Clone)]
pub struct GatewayHandle {
    /// Daemon agent string (e.g. `"antd/0.1.0"`). Surfaced via
    /// `/health.version`. Wrapped in `Arc` so the cloned handle in
    /// every request handler doesn't realloc.
    pub agent: Arc<String>,
    /// Pinned bee API version we claim wire-compatibility against.
    /// Surfaced via `/health.apiVersion`. See PLAN.md §C.5.
    pub api_version: Arc<String>,
    /// Static identity bits.
    pub identity: Arc<GatewayIdentity>,
    /// Live status snapshot maintained by the node loop. Cloning the
    /// receiver is constant-time; `.borrow()` returns a guard with no
    /// allocation.
    pub status: watch::Receiver<StatusSnapshot>,
    /// Dispatch channel into the node loop for retrieval and
    /// peerstore commands.
    pub commands: mpsc::Sender<ControlCommand>,
    /// Live registry the gateway writes every in-flight retrieval
    /// request into. The same `Arc` is held by the daemon's status
    /// publisher, which snapshots it for `StatusSnapshot::retrieval`
    /// so `antop` can render the Retrieval tab.
    pub activity: Arc<GatewayActivity>,
    /// Whether the node is running in **light** mode (uploads + SWAP
    /// settlement configured) versus **ultra-light** (read-only). Set
    /// by `antd` at startup from the postage/chequebook config. Drives
    /// `GET /node.beeMode`, which Freedom's `checkSwarmPreFlight` gates
    /// every publish/feed/SOC write on — an `ultra-light` answer makes
    /// Freedom refuse to publish at all (PLAN.md J.4.1).
    pub light_mode: bool,
    /// In-process upload-tag registry backing `POST /tags` and
    /// `GET /tags/{uid}`. bee auto-creates a tag on every `POST /bzz`
    /// / `POST /bytes` and returns its uid in the `Swarm-Tag-Uid`
    /// response header; bee-js then polls `GET /tags/{uid}` for upload
    /// progress (PLAN.md J.2.4 / C3). Because the gateway's uploads are
    /// synchronous (every chunk is pushsynced before the response),
    /// the tag is marked fully synced as soon as it's created.
    pub tags: Arc<TagRegistry>,
    /// CORS policy applied to every response. Driven by bee's
    /// `cors-allowed-origins` config (PLAN.md J.4.8): Freedom sets it to
    /// `"null"` because its dweb pages `fetch`/upload from an opaque
    /// (`null`) origin. With the default (deny) config no CORS headers
    /// are emitted, matching a bee node started without the option.
    pub cors: Arc<CorsConfig>,
    /// Optional live Gnosis chain reader + addresses backing
    /// `/wallet`, `/chequebook/*`, `/status`, `/chainstate` (PLAN.md J.5
    /// A2/A3/D1/D2). `None` on a node started without an RPC endpoint:
    /// balances then report the bee zero-stub and the chain-state
    /// endpoints fall to `501`, matching bee with no configured backend.
    pub chain: Option<Arc<ChainContext>>,
}
