//! Wire types shared by the daemon and `antctl`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Bumped whenever the wire format changes in a non-additive way.
///
/// v2 introduced streaming progress on `Get`/`GetBytes`/`GetBzz`: when the
/// client sends `progress: true`, the daemon may emit zero or more
/// [`Response::Progress`] messages on the same connection before the
/// terminal response. Older clients leave `progress` at its default
/// (`false`) and continue to see exactly one response, preserving the
/// v1 single-shot behaviour. The new `bypass_cache` request flag is
/// equally additive — old daemons simply ignore the field.
pub const PROTOCOL_VERSION: u32 = 2;

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
    ///
    /// `bypass_cache` skips the daemon's shared in-memory chunk cache
    /// for this request only: the daemon mints a fresh per-request
    /// cache so intra-request retries still benefit from caching, but
    /// no chunk hits or writes touch the long-lived cache. Useful for
    /// timing the cold path or reproducing a transient retrieval.
    /// `progress` opts the request into streaming
    /// [`Response::Progress`] updates emitted at a regular cadence
    /// while the chunk tree is fetched.
    Get {
        reference: String,
        #[serde(default)]
        bypass_cache: bool,
        #[serde(default)]
        progress: bool,
    },
    /// Retrieve a file via the bzz read-path: walks the manifest at
    /// `reference`, resolves `path` (empty triggers `website-index-document`),
    /// then joins the resulting chunk tree into the file's raw bytes.
    /// Daemon answers with [`Response::BzzBytes`].
    ///
    /// `allow_degraded_redundancy`: opt in to decoding files whose root
    /// chunk carries a non-zero Reed-Solomon redundancy level. The
    /// daemon masks the level byte and walks the chunk tree without
    /// running RS recovery — bytes come back if every data chunk is
    /// reachable, otherwise the request fails as it would today.
    /// Defaults to `false` so the normal path remains strict.
    /// See [`Request::Get`] for `bypass_cache` and `progress`.
    GetBzz {
        reference: String,
        #[serde(default)]
        path: String,
        #[serde(default)]
        allow_degraded_redundancy: bool,
        #[serde(default)]
        bypass_cache: bool,
        #[serde(default)]
        progress: bool,
    },
    /// Retrieve a multi-chunk raw byte tree by joining its chunk tree.
    /// `reference` points at the root chunk of a `/bytes/` tree. Daemon
    /// answers with [`Response::Bytes`] (no content-type, no manifest
    /// metadata). See [`Request::Get`] for `bypass_cache` and `progress`.
    GetBytes {
        reference: String,
        #[serde(default)]
        bypass_cache: bool,
        #[serde(default)]
        progress: bool,
    },
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
    /// Hex-encoded chunk payload returned by [`Request::Get`] /
    /// [`Request::GetBytes`]. Hex (rather than base64 or raw binary) keeps
    /// the wire format newline-delimited JSON for now; we'll revisit when
    /// multi-chunk fetches need streaming.
    Bytes {
        /// `0x`-prefixed lowercase hex of the payload bytes.
        hex: String,
    },
    /// Hex-encoded file payload returned by [`Request::GetBzz`], plus the
    /// `Content-Type` metadata read from the matching mantaray value
    /// node (when the manifest carried one).
    BzzBytes {
        hex: String,
        #[serde(default)]
        content_type: Option<String>,
        /// Best-effort filename derived from the resolved path or
        /// manifest metadata; useful for `antctl get -o`.
        #[serde(default)]
        filename: Option<String>,
    },
    /// Streaming progress emitted by `Get` / `GetBytes` / `GetBzz`
    /// when the client opted in via `progress: true`. May be sent
    /// zero or more times before a terminal `Bytes` / `BzzBytes` /
    /// `Error` reply on the same connection. Clients that didn't ask
    /// for progress will never receive this variant; old clients can
    /// ignore it.
    Progress(GetProgress),
    Error {
        message: String,
    },
}

/// One streaming progress sample for an in-flight `Get*` request.
///
/// All counters are cumulative since the request started. Spans of `0`
/// for `total_*_estimate` mean "not yet known" — the daemon doesn't
/// learn the file size until it has fetched the data root chunk and
/// inspected its 8-byte span prefix, so the first few samples on a
/// large file may report only `chunks_done` / `bytes_done`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetProgress {
    /// Wall-clock milliseconds since the daemon accepted the request.
    pub elapsed_ms: u64,
    /// Number of chunks delivered to the joiner so far (cache hits +
    /// successful network fetches).
    pub chunks_done: u64,
    /// Best-effort estimate of total chunks for the request, derived
    /// from the data root's span. `0` until the data root has been
    /// fetched (and for single-chunk files where the root *is* the
    /// data, the first sample carries `chunks_done == 1` and
    /// `total_chunks_estimate == 1`).
    pub total_chunks_estimate: u64,
    /// Bytes of chunk wire data fetched so far (`span (8) || payload`
    /// for each delivered chunk).
    pub bytes_done: u64,
    /// File-body byte count reported by the data root's span. `0`
    /// until known. For `Get` and `GetBytes` this is the file size;
    /// for `GetBzz` it's the size of the resolved file body, not the
    /// manifest.
    pub total_bytes_estimate: u64,
    /// Cache hits served without going to the network. Useful for
    /// distinguishing "fast retrieval from a warm cache" from "fast
    /// retrieval from a single very-close peer".
    pub cache_hits: u64,
    /// Distinct peers we successfully retrieved at least one chunk
    /// from during this request. `0` means everything seen so far
    /// came from cache.
    pub peers_used: u32,
    /// Echoes the request's `bypass_cache` flag so a client that
    /// forgot which mode it asked for can reconcile when rendering.
    pub bypass_cache: bool,
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
    /// Wall-clock milliseconds from the first dial attempt (or, for inbound
    /// peers, from `ConnectionEstablished`) to the moment the BZZ handshake
    /// completed and the peer entered `Ready`. `None` for peers that
    /// haven't reached `Ready` yet, and for peers tracked by older
    /// daemons that didn't record this. Reset on disconnect, so a
    /// reconnect produces a fresh measurement.
    #[serde(default)]
    pub ready_in_ms: Option<u64>,
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
