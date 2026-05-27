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
///
/// v3 added the `Upload*` request set: long-lived daemon-resident
/// upload jobs that stream `UploadProgress` over a control-socket
/// follow connection and survive daemon restarts via on-disk
/// manifests. v2 daemons reject `Upload*` with the standard
/// "bad request" envelope, so an old `antctl` against a new
/// daemon and vice versa are both observably wrong instead of
/// silently misbehaving. The optional `raw` flag on `UploadStart`
/// (and the `raw` field echoed back on `UploadJobView`) is
/// additive — old daemons leave it at the `#[serde(default)]`
/// `false` value, which preserves the original "always wrap in a
/// single-file mantaray manifest" behaviour.
pub const PROTOCOL_VERSION: u32 = 3;

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
    /// Create a new upload job for a file at `source_path`. The daemon
    /// reads the file from disk (the control socket is `0o600` and
    /// owned by the same user, so no further auth is needed); the
    /// CLI's job is just to hand over the path. Returns immediately
    /// with [`Response::UploadStarted`] carrying the assigned
    /// `job_id`; the actual splitting + pushsync happens on a daemon
    /// background task whose progress is observable via
    /// [`Request::UploadFollow`].
    ///
    /// `batch_id`: optional per-job postage batch override (32-byte
    /// hex). `None` means "use the daemon's configured default
    /// (`--postage-batch`)".
    /// `name` / `content_type`: Mantaray manifest entry metadata.
    /// `name` defaults to the source file's basename;
    /// `content_type` defaults to `application/octet-stream`.
    /// `raw` skips the trailing single-file mantaray manifest; the
    /// completed job's `reference` is the data root chunk address
    /// itself (a `/bytes/<ref>` reference, not a `/bzz/<ref>` one).
    /// Saves 1-2 chunks of postage and a final round-trip; loses
    /// the embedded filename + content-type, so consumers must
    /// know the type out-of-band. Defaults to `false` so the
    /// reference returned is bzz-compatible.
    UploadStart {
        source_path: String,
        #[serde(default)]
        batch_id: Option<String>,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        content_type: Option<String>,
        #[serde(default)]
        raw: bool,
    },
    /// Snapshot every known upload job (running, paused, completed,
    /// cancelled, failed). Daemon answers with [`Response::UploadList`].
    UploadList,
    /// Snapshot one upload job by id (or by unique 8-hex-char prefix —
    /// the daemon resolves prefixes for ergonomic CLI use). Daemon
    /// answers with [`Response::UploadJob`] or [`Response::Error`].
    UploadStatus { job_id: String },
    /// Soft-stop a running upload. Idempotent on already-paused jobs.
    /// Daemon answers with [`Response::UploadJob`] reflecting the new
    /// state.
    UploadPause { job_id: String },
    /// Bring a paused or failed upload back to running. Spawns a
    /// fresh driver task; the previous driver has already exited.
    UploadResume { job_id: String },
    /// Hard-stop an upload. Already-pushed chunks are left in the
    /// network (Swarm GCs them at postage TTL expiry — there's no
    /// unpush). Idempotent.
    UploadCancel { job_id: String },
    /// Subscribe to live upload progress. Daemon emits zero or more
    /// [`Response::UploadProgress`] frames until the job reaches a
    /// terminal status, then closes with [`Response::UploadJob`]
    /// (the terminal snapshot) and EOF. Multiple followers per job
    /// are supported; each gets its own broadcast.
    UploadFollow { job_id: String },
    /// Snapshot of the daemon's local postage stamp issuer:
    /// theoretical capacity, indices issued so far, per-bucket
    /// fill min/max, and the conservative "worst-case remaining
    /// chunks" budget that bounds the size of the next upload.
    /// Returns [`Response::PostageStatus`]. Pure local read — no
    /// chain RPC. Old daemons reject this with the standard "bad
    /// request" envelope.
    PostageStatus,
    /// Insert a CAC-validated chunk directly into the daemon's
    /// disk chunk cache, without stamping or pushsync. Used by
    /// `antctl pin` to make a previously-uploaded file
    /// retrievable through *this* node's HTTP gateway without
    /// waiting for bee-side neighbourhood replication.
    ///
    /// `wire_hex` is the chunk's full wire bytes (`span (8 LE) ||
    /// payload`, ≤ 8 + 4096 bytes), `0x`-prefixed lowercase hex.
    /// The daemon recomputes the chunk address from the wire
    /// (CAC) and rejects mismatched payloads — callers cannot
    /// poison the disk cache by putting wrong-address bytes.
    ///
    /// Returns [`Response::Ok`] on success; the message carries
    /// the resulting chunk address. Old daemons reject this with
    /// the standard "bad request" envelope.
    PutChunkLocal {
        /// `0x`-prefixed lowercase hex of the chunk wire bytes.
        wire_hex: String,
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
    /// Bee `chunkAddressResponse` after `POST /chunks`.
    ChunkUploaded {
        reference: String,
    },
    /// Successful [`Request::UploadStart`]. The daemon has written
    /// the on-disk job manifest and spawned the driver task.
    UploadStarted {
        job_id: String,
    },
    /// Snapshot of one upload job. Returned by `UploadStatus`,
    /// `UploadPause`, `UploadResume`, `UploadCancel`, and as the
    /// terminal frame on a `UploadFollow` connection.
    UploadJob(UploadJobView),
    /// All known upload jobs at the time of the request. Returned
    /// by `UploadList`.
    ///
    /// Wrapped in a struct variant rather than a newtype around the
    /// vec because `#[serde(tag = "result")]` cannot inject the tag
    /// into a JSON array (serde fails with "cannot serialize tagged
    /// newtype variant ... containing a sequence" at runtime). The
    /// extra `jobs` key on the wire is the price of keeping the
    /// outer enum internally tagged.
    UploadList {
        jobs: Vec<UploadJobView>,
    },
    /// Streaming progress frame on a `UploadFollow` connection.
    /// Emitted at most once per state change; old daemons never send
    /// this variant.
    UploadProgress(UploadJobView),
    /// Local postage stamp issuer snapshot. Returned by
    /// [`Request::PostageStatus`].
    PostageStatus(PostageStatusView),
}

/// Wire shape of one upload job (status snapshot). Mirrors
/// `ant_node::UploadJobInfo` but is defined here so `ant-control`
/// stays free of an `ant-node` dependency.
///
/// Field semantics:
///
/// - `bytes_pushed` / `source_size`: drives the byte progress bar.
/// - `chunks_pushed` / `chunks_total`: drives the chunk progress
///   bar; `chunks_total` is `None` for very-old daemons that
///   didn't pre-compute the estimate.
/// - `reference`: only set once `status == "completed"`; the value
///   is the lowercase `0x`-prefixed mantaray manifest root the
///   uploader would publish (e.g. as a `bzz://` URL). If the job
///   was started with `raw = true`, this is instead the data root
///   chunk address (a `/bytes/<ref>` reference).
/// - `raw`: echoes the request flag so consumers can pick the
///   right URL prefix (`bzz://` vs `bytes://`) without an extra
///   round-trip. Old daemons leave it at the `#[serde(default)]`
///   `false`, matching the historical always-manifest behaviour.
/// - `last_error`: most recent transient or terminal driver error,
///   useful for debugging stalled jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadJobView {
    pub job_id: String,
    pub source_path: String,
    pub source_size: u64,
    #[serde(default)]
    pub batch_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub raw: bool,
    pub status: String,
    pub bytes_pushed: u64,
    pub chunks_pushed: u64,
    #[serde(default)]
    pub chunks_total: Option<u64>,
    pub created_at_unix: u64,
    pub last_update_unix: u64,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub reference: Option<String>,
}

/// Snapshot of the daemon's local postage stamp issuer.
///
/// `enabled = false` means the daemon was started without a postage
/// batch (no `--postage-batch` / `STORAGE_STAMP_BATCH_ID`); in that
/// case every other field is at its default and the renderer should
/// label the batch as "uploads disabled". When `enabled = true`:
///
/// - `total_capacity_chunks` is the theoretical ceiling
///   (`2^batch_depth`). Reachable only with perfectly-even chunk
///   routing across buckets — almost never the practical limit.
/// - `worst_case_remaining_chunks` is the conservative budget: room
///   left in the *currently fullest* bucket, scaled to a global
///   count. Use this to decide whether a planned upload of N
///   chunks is guaranteed to fit. For an immutable batch this is
///   the right answer; for a mutable batch the upload still
///   "fits" past this budget, but at the cost of overwriting older
///   stamps (their chunks become un-stampable for resync).
/// - `bucket_fill_min` / `bucket_fill_max` characterise the
///   distribution. A large `max - min` gap signals lopsided
///   routing: usually fine for random uploads, worth investigating
///   if you've been pinning many addresses with a shared prefix.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostageStatusView {
    pub enabled: bool,
    /// `0x`-prefixed hex of the configured batch id. Empty when
    /// `enabled = false`.
    #[serde(default)]
    pub batch_id: String,
    #[serde(default)]
    pub batch_depth: u8,
    #[serde(default)]
    pub bucket_depth: u8,
    #[serde(default)]
    pub immutable: bool,
    #[serde(default)]
    pub bucket_count: u64,
    #[serde(default)]
    pub bucket_capacity: u32,
    #[serde(default)]
    pub total_capacity_chunks: u64,
    #[serde(default)]
    pub issued_chunks: u64,
    #[serde(default)]
    pub bucket_fill_min: u32,
    #[serde(default)]
    pub bucket_fill_max: u32,
    /// Sum of free indices across all buckets (optimistic budget).
    #[serde(default)]
    pub remaining_total_chunks: u64,
    /// `(bucket_capacity − bucket_fill_max) × bucket_count`.
    /// Pessimistic upper bound on the next upload (see the
    /// type-level doc). 0 when the batch is empty.
    #[serde(default)]
    pub worst_case_remaining_chunks: u64,
}

/// One streaming progress sample for an in-flight `Get*` request.
///
/// All counters are cumulative since the request started. Spans of `0`
/// for `total_*_estimate` mean "not yet known" — the daemon doesn't
/// learn the file size until it has fetched the data root chunk and
/// inspected its 8-byte span prefix, so the first few samples on a
/// large file may report only `chunks_done` / `bytes_done`.
///
/// `in_flight` (added in v3 of this crate) is the only non-cumulative
/// field — it's an instantaneous count of chunk fetches the request
/// has dispatched but not yet seen a delivery for. Old daemons leave
/// it at `0`; a `0` value on a new daemon is also legitimate (cache
/// hits and idle moments).
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
    /// Chunks the request currently has dispatched (acquired the
    /// retrieval semaphore but not yet returned a delivery). Cache
    /// hits don't increment this; an idle stretch reads `0` even on
    /// a healthy connection. Old daemons leave this at the
    /// `#[serde(default)]` zero value.
    #[serde(default)]
    pub in_flight: u32,
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
    /// Externally-advertised multiaddrs the daemon has registered with
    /// libp2p-identify, with the source that produced each one
    /// (operator-supplied, listener auto-promotion, observed-from-peer,
    /// or `UPnP`). Older daemons leave this at the `#[serde(default)]`
    /// empty value; new clients should fall back to `listeners` when it
    /// is empty. Order is insertion-stable so the UI can show a
    /// deterministic history.
    #[serde(default)]
    pub external_addresses: Vec<ExternalAddressInfo>,
    pub control_socket: String,
    /// Data-plane snapshot: chunk cache fill, in-flight retrievals,
    /// cumulative download counters, and the list of currently active
    /// gateway HTTP requests. Populated by the daemon on every status
    /// update; surfaced as the `Retrieval` tab in `antop`. Old
    /// daemons leave this at the `#[serde(default)]` zero value.
    #[serde(default)]
    pub retrieval: RetrievalInfo,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentityInfo {
    pub eth_address: String,
    pub overlay: String,
    pub peer_id: String,
}

/// One externally-advertised multiaddr, with metadata about how the
/// daemon learned it. UIs typically render the `source` column verbatim
/// so operators can tell at a glance whether their address came from a
/// CLI flag, listener auto-promotion, a peer's identify push, or the
/// `UPnP` behaviour talking to a home router.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExternalAddressInfo {
    /// Multiaddr we advertise via libp2p-identify (e.g.
    /// `/ip4/203.0.113.42/tcp/1634`).
    pub addr: String,
    /// Free-form source identifier. Stable values produced by `antd`:
    /// `manual` (`--external-address`), `listener` (auto-promoted
    /// non-loopback listener), `observed` (from a remote's identify
    /// `observed_addr`), `upnp` (mapped via libp2p-upnp / IGD). Future
    /// daemons may add new sources, so UIs should treat unknown values
    /// as opaque labels.
    pub source: String,
    /// Wall-clock unix timestamp at which the address was registered.
    /// Useful for "added 12 s ago" rendering.
    pub added_at_unix: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerInfo {
    pub connected: u32,
    #[serde(default)]
    pub node_limit: u32,
    #[serde(default)]
    pub connected_peers: Vec<PeerConnectionInfo>,
    /// Unified peer rows for UIs (e.g. `antop`); includes dialing / pipeline
    /// states not present in `connected_peers` alone. Empty on older daemons.
    #[serde(default)]
    pub peer_pipeline: Vec<PeerPipelineEntry>,
    pub last_handshake: Option<HandshakeReport>,
    /// Monotonic time from process start to first BZZ peer (seconds, e.g. for `antop`).
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

/// Snapshot of the in-memory chunk cache.
///
/// `capacity` reflects `ant_retrieval::DEFAULT_CACHE_CAPACITY` (or
/// whatever the daemon was configured with) and is constant for the
/// lifetime of the process; `used` is the live LRU fill, refreshed
/// on every status tick.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CacheInfo {
    pub used: u32,
    pub capacity: u32,
}

/// Snapshot of the persistent (SQLite-backed) chunk cache.
///
/// All-zero / `enabled = false` when the daemon was started with
/// `--no-disk-cache` (or with no disk cache wired up at all). Sizes
/// are tracked in *bytes* — unlike the in-memory tier the disk
/// cache's eviction key is total bytes, not slot count, so the row
/// count is informational and the byte total is what drives the
/// "fullness" gauge in `antop`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskCacheInfo {
    /// Whether the daemon has a persistent cache attached. When
    /// `false`, every other field is zero / empty and `antop` should
    /// label the row as `disabled` rather than render
    /// `0/0 (0.0%)`.
    pub enabled: bool,
    /// Live byte total mirrored from `SUM(size)`. May lag by a
    /// single in-flight write but never diverges by more than one
    /// chunk.
    pub used_bytes: u64,
    /// Configured hard cap (`--disk-cache-max-gb` × 1 GiB).
    pub capacity_bytes: u64,
    /// Cumulative tier-2 hits since process start. A subset of
    /// `RetrievalInfo::chunks_fetched_total` and disjoint from
    /// `mem_hits_total` — a chunk that lifts from disk into memory
    /// counts toward `disk_hits_total` only on the lifting fetch;
    /// subsequent fetches from the warm in-memory entry count toward
    /// `mem_hits_total`.
    pub hits_total: u64,
    /// Live row count mirrored from `COUNT(*) FROM chunks`. Combined
    /// with `used_bytes` this exposes the on-disk mean chunk size
    /// (useful for sanity-checking against the 4 KiB BMT chunk size).
    /// Reports `0` for the ~30 s after a cold start while the backfill
    /// `COUNT(*)` is still running — see `DiskChunkCache::open` for
    /// why we don't block startup on that scan.
    #[serde(default)]
    pub chunks: u64,
    /// Absolute on-disk path of the `chunks.sqlite` file. Empty when
    /// `enabled = false`. Surfaced so `antop` operators don't have to
    /// guess between the default `~/.antd/chunks.sqlite` and whatever
    /// `--disk-cache-path` the daemon was started with.
    #[serde(default)]
    pub path: String,
    /// Number of dedicated read-worker threads servicing `get`
    /// requests. Reported so an operator can correlate sustained
    /// retrieval queue depth against the worker pool size without
    /// digging into the binary.
    #[serde(default)]
    pub read_workers: u32,
}

/// Data-plane snapshot read by the `Retrieval` tab in `antop`.
///
/// All numeric fields are cumulative since process start except
/// `cache.used`, `in_flight`, and the per-request entries in
/// `gateway_requests`, which are instantaneous. Bandwidth is *not*
/// reported as a derived rate here — the daemon publishes raw
/// cumulative `bytes_fetched_total` and `chunks_fetched_total`, and
/// the consumer (`antop`) computes whatever smoothing /
/// peak-tracking it wants from successive snapshots. That keeps the
/// daemon free of UI-tier policy and makes the rate trivially
/// reproducible from the wire log.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetrievalInfo {
    pub cache: CacheInfo,
    /// Chunk fetches the daemon currently has on the wire across all
    /// in-flight requests (sum of `RoutingFetcher::FuturesUnordered`
    /// pools that are past their semaphore acquire). Bounded above by
    /// `in_flight_capacity`.
    pub in_flight: u32,
    /// Process-wide cap on concurrent retrievals
    /// (`RETRIEVAL_INFLIGHT_CAP` in `ant-p2p`).
    pub in_flight_capacity: u32,
    /// Cumulative chunk count delivered to joiners since process
    /// start. Includes both network deliveries and cache hits.
    pub chunks_fetched_total: u64,
    /// Cumulative wire bytes (`span (8) || payload`) delivered to
    /// joiners. Used by `antop` to compute instantaneous
    /// bandwidth as `(b2 - b1) / dt`.
    pub bytes_fetched_total: u64,
    /// Cumulative cache hits (subset of `chunks_fetched_total`).
    /// Equal to `mem_hits_total + disk_hits_total`; kept on the
    /// snapshot for backward compatibility with consumers that
    /// don't yet split memory and disk hits.
    pub cache_hits_total: u64,
    /// Cumulative tier-1 (in-memory LRU) hits. Disjoint from
    /// `disk_hits_total`.
    #[serde(default)]
    pub mem_hits_total: u64,
    /// Persistent (`SQLite`) chunk-cache snapshot. `enabled = false`
    /// when the daemon was started without a disk cache.
    #[serde(default)]
    pub disk: DiskCacheInfo,
    /// Snapshot of currently-active gateway HTTP requests. Empty when
    /// the gateway is idle or disabled.
    #[serde(default)]
    pub gateway_requests: Vec<GatewayRequestInfo>,
}

/// One in-flight gateway request as surfaced by `StatusSnapshot::retrieval`.
///
/// `path` is a short human-readable label — usually `<short_ref>` for
/// `/bytes` and `/chunks` and `<short_ref>/<path>` for `/bzz`. The
/// daemon truncates the reference to its leading 8 hex chars to keep
/// the table column width sane in `antop`; clients that need the
/// full reference can correlate via the gateway access log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayRequestInfo {
    pub kind: GatewayRequestKind,
    pub path: String,
    pub started_at_unix: u64,
    /// Chunks delivered to the joiner so far for this request (network +
    /// cache). `0` until the first chunk lands; common for fresh
    /// requests where the root hasn't been fetched yet.
    pub chunks_done: u64,
    /// Best-effort total derived from the data root's span. `0` until
    /// the daemon has fetched the root chunk.
    pub total_chunks_estimate: u64,
    /// Live count of chunk fetches the request has dispatched but not
    /// yet seen a delivery for. Mirrors
    /// [`GetProgress::in_flight`] for this request.
    pub chunks_in_flight: u32,
    pub bytes_done: u64,
}

/// Coarse classification of the gateway endpoint so `antop` can
/// label each row. Mapped from the axum handler that registered the
/// active request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayRequestKind {
    /// `GET/HEAD /bytes/{addr}` — raw chunk-tree streaming.
    Bytes,
    /// `GET/HEAD /bzz/{addr}` or `/bzz/{addr}/{path}` — manifest-aware
    /// streaming.
    Bzz,
    /// `GET/HEAD /chunks/{addr}` — single-chunk wire fetch.
    Chunk,
    /// `GET/HEAD/POST /soc/{owner}/{id}` — single-owner-chunk wire
    /// fetch or upload. Distinct from [`Self::Chunk`] so operators can
    /// tell SOC traffic apart from CAC traffic in `antctl top`.
    Soc,
    /// `GET /feeds/{owner}/{topic}` — sequence-feed resolution.
    Feed,
    /// `GET /v0/manifest/{addr}` — Ant-specific manifest enumeration.
    Manifest,
}

impl GatewayRequestKind {
    /// Short label shown in the `antop` Retrieval tab. Up to 5
    /// ASCII characters so the table's `Kind` column stays narrow
    /// while still being self-explanatory: each label is the bee /
    /// Ant endpoint name (`/bytes`, `/bzz`, `/chunks`, `/soc`,
    /// `/feeds`, `/v0/manifest`) either spelled in full or trimmed
    /// to the same root.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Bytes => "Bytes",
            Self::Bzz => "BZZ",
            Self::Chunk => "Chunk",
            Self::Soc => "SOC",
            Self::Feed => "Feed",
            Self::Manifest => "Manif",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Catch the "tagged newtype variant containing a sequence" trap
    /// before it reaches a live socket: serde's internally-tagged enum
    /// representation cannot inject the discriminator field into a
    /// JSON array, so any `Response::Foo(Vec<_>)` newtype variant
    /// panics at the first write. Round-tripping every collection-
    /// shaped variant here keeps the wire format honest.
    #[test]
    fn response_upload_list_roundtrips() {
        let view = UploadJobView {
            job_id: "deadbeef".into(),
            source_path: "/tmp/x".into(),
            source_size: 1,
            batch_id: None,
            name: None,
            content_type: None,
            raw: false,
            status: "running".into(),
            bytes_pushed: 0,
            chunks_pushed: 0,
            chunks_total: None,
            created_at_unix: 0,
            last_update_unix: 0,
            last_error: None,
            reference: None,
        };
        for jobs in [Vec::new(), vec![view]] {
            let resp = Response::UploadList { jobs: jobs.clone() };
            let wire = serde_json::to_string(&resp).expect("serialize");
            assert!(wire.contains("\"result\":\"upload_list\""), "{wire}");
            let back: Response = serde_json::from_str(&wire).expect("deserialize");
            match back {
                Response::UploadList { jobs: round } => {
                    assert_eq!(round.len(), jobs.len());
                }
                other => panic!("wrong variant: {other:?}"),
            }
        }
    }
}
