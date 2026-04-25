//! Wire types shared by the daemon and `antctl`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Bumped whenever the wire format changes in a non-additive way.
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Client → daemon request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    /// Current daemon status snapshot.
    Status,
    /// Daemon agent string + control-protocol version.
    Version,
    /// Drop the on-disk peerstore (`<data-dir>/peers.json`) and the in-memory
    /// dedup set without disconnecting current peers. The next restart will
    /// bootstrap fresh from bootnodes.
    PeersReset,
    /// Retrieve the chunk at `reference` from the network. The 32-byte
    /// reference is a `0x`-prefixed hex string. The daemon answers with
    /// [`Response::Bytes`] carrying the verified chunk payload (without
    /// the span prefix).
    Get { reference: String },
}

/// Daemon → client response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Response {
    Status(Box<StatusSnapshot>),
    Version(VersionInfo),
    /// Generic acknowledgement for commands that have no structured payload
    /// (e.g. `PeersReset`). `message` carries a human-readable summary of
    /// what the daemon did.
    Ok {
        message: String,
    },
    /// Hex-encoded chunk payload returned by [`Request::Get`]. Hex (rather
    /// than base64 or raw binary) keeps the wire format newline-delimited
    /// JSON for now; we'll revisit when multi-chunk fetches need streaming.
    Bytes {
        /// `0x`-prefixed lowercase hex of the payload bytes.
        hex: String,
    },
    Error {
        message: String,
    },
}

/// Everything `antctl status` wants to show.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub agent: String,
    pub protocol_version: u32,
    pub network_id: u64,
    pub pid: u32,
    pub started_at_unix: u64,
    pub identity: IdentityInfo,
    pub peers: PeerInfo,
    pub listeners: Vec<String>,
    pub control_socket: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentityInfo {
    pub eth_address: String,
    pub overlay: String,
    pub peer_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerInfo {
    pub connected: u32,
    #[serde(default)]
    pub node_limit: u32,
    #[serde(default)]
    pub connected_peers: Vec<PeerConnectionInfo>,
    /// Unified peer rows for UIs (e.g. `antctl top`); includes dialing / pipeline
    /// states not present in `connected_peers` alone. Empty on older daemons.
    #[serde(default)]
    pub peer_pipeline: Vec<PeerPipelineEntry>,
    pub last_handshake: Option<HandshakeReport>,
    /// Monotonic time from process start to first BZZ peer (seconds, e.g. for `antctl top`).
    #[serde(default)]
    pub time_to_first_peer_s: Option<f64>,
    /// Monotonic time from process start to reaching `node_limit` BZZ peers.
    #[serde(default)]
    pub time_to_node_limit_s: Option<f64>,
    /// Forwarding-Kademlia routing table summary (per-bin peer counts).
    /// Empty `bins` on older daemons.
    #[serde(default)]
    pub routing: RoutingInfo,
}

/// Snapshot of the forwarding-Kademlia routing table: how many BZZ peers
/// we currently have, indexed by proximity order to our overlay. `bins[i]`
/// is the count of peers in PO bin `i` (0 = farthest, 31 = closest).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutingInfo {
    /// Local Swarm overlay as a `0x`-prefixed hex string. Empty when the
    /// daemon has no peers and hasn't computed it yet.
    #[serde(default)]
    pub base_overlay: String,
    /// Total entries in the routing table (== number of BZZ peers we have).
    #[serde(default)]
    pub size: u32,
    /// Per-bin peer counts. Always 32 elements when populated.
    #[serde(default)]
    pub bins: Vec<u32>,
}

/// Connection / pipeline state for a peer, aligned with the daemon’s swarm view.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PeerConnectionState {
    #[default]
    Ready,
    Dialing,
    Identifying,
    Handshaking,
    Failed,
    Closing,
}

/// One row in `PeerInfo::peer_pipeline` (full `peer_id`; clients may shorten for display).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerPipelineEntry {
    pub peer_id: String,
    pub state: PeerConnectionState,
    /// Best-effort remote endpoint for UI (e.g. `192.0.2.1` or a DNS name).
    #[serde(default)]
    pub ip: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerConnectionInfo {
    pub peer_id: String,
    pub direction: String,
    pub address: String,
    pub connected_at_unix: u64,
    #[serde(default)]
    pub agent_version: String,
    #[serde(default)]
    pub bzz_overlay: Option<String>,
    #[serde(default)]
    pub full_node: Option<bool>,
    #[serde(default)]
    pub last_bzz_at_unix: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeReport {
    pub remote_overlay: String,
    pub remote_peer_id: String,
    pub agent_version: String,
    pub full_node: bool,
    pub at_unix: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VersionInfo {
    pub agent: String,
    pub protocol_version: u32,
}
