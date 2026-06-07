//! Control-plane command + ack types shared between the daemon's node
//! loop and every transport that drives it.
//!
//! These are plain data + tokio channel handles — deliberately free of
//! any OS-socket dependency so the crate (and its consumers `ant-node`,
//! `ant-gateway`, `ant-ffi`, `antd`) compile on every platform. The Unix
//! domain-socket transport that *carries* them lives in the `server`
//! module, which is `#[cfg(unix)]`-gated; Windows builds get the types
//! without the socket I/O.

use crate::protocol::{GetProgress, PostageStatusView, UploadJobView};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

/// Buffer depth for the streaming ack channel. Sized big enough to
/// hold a couple of progress emissions plus the terminal ack without
/// the producer ever blocking, and small enough that a stalled writer
/// is observable in memory pressure rather than runaway buffering.
const STREAM_ACK_CAPACITY: usize = 16;

/// A mutating or network-touching command issued by a client and forwarded
/// to the node loop.
///
/// The node loop owns state that needs a single-writer path (the peerstore,
/// the swarm), so the transport does not touch it directly; instead it relays
/// a `ControlCommand` through an `mpsc::Sender` and awaits the ack(s) on the
/// embedded reply channel. Non-streaming commands (peerstore reset, single-
/// chunk retrieve) carry a `oneshot` — exactly one ack lands and the
/// channel closes. Streaming-capable commands (`GetBytes`, `GetBzz`)
/// carry an `mpsc::Sender` that produces zero or more
/// [`ControlAck::Progress`] samples followed by exactly one terminal
/// variant (`Bytes` / `BzzBytes` / `Error`).
#[derive(Debug)]
pub enum ControlCommand {
    /// Drop the on-disk peerstore snapshot and clear the in-memory dedup
    /// state. Does not disconnect current peers.
    ResetPeerstore { ack: oneshot::Sender<ControlAck> },
    /// Retrieve a chunk by 32-byte reference. The node loop picks the
    /// closest BZZ peer, runs `ant_retrieval::retrieve_chunk`, and acks
    /// with the verified payload bytes (or an error string).
    GetChunk {
        reference: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
    /// Like `GetChunk`, but the ack carries the chunk's full **wire
    /// bytes** (`span (8 LE) || payload`) instead of just the payload.
    /// Used by `ant-gateway` to serve `/chunks/{addr}` in a bee-shaped
    /// way (bee's `chunkstore.Get` returns `Chunk.Data()` which is the
    /// wire form). Routing / peer-pick / CAC verification logic is
    /// identical to `GetChunk`; the only difference is what the node
    /// loop puts in `ControlAck::Bytes::data`.
    GetChunkRaw {
        reference: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
    /// Walk the manifest at `reference`, resolve `path`, then join the
    /// resulting chunk tree into a single `Vec<u8>`. Acks with
    /// [`ControlAck::BzzBytes`] including any `Content-Type` metadata
    /// recovered from the manifest. When `allow_degraded_redundancy`
    /// is set the joiner masks any non-zero RS level byte off the
    /// file's root span and decodes without RS recovery.
    /// `bypass_cache` swaps the daemon's shared chunk cache for a
    /// fresh per-request one (intra-request retries still benefit
    /// from caching). `progress` opts the request into streaming
    /// [`ControlAck::Progress`] emissions. `max_bytes` overrides the
    /// joiner's per-request size cap; `None` means "use the joiner's
    /// `DEFAULT_MAX_FILE_BYTES` (32 MiB)" — appropriate for `antctl get`
    /// where allocating gigabytes from a malformed root chunk would be
    /// a footgun. The HTTP gateway raises this so real bzz sites with
    /// videos / archives don't 502 at the joiner step.
    GetBzz {
        reference: [u8; 32],
        path: String,
        allow_degraded_redundancy: bool,
        bypass_cache: bool,
        progress: bool,
        max_bytes: Option<u64>,
        ack: mpsc::Sender<ControlAck>,
    },
    /// List the paths and metadata in a mantaray manifest. Gateway-only
    /// helper for the `/manifest/{addr}` inspection endpoint.
    ListBzz {
        reference: [u8; 32],
        bypass_cache: bool,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Join the multi-chunk tree rooted at `reference` (a `/bytes/` ref,
    /// no manifest) and ack with the joined file bytes. See
    /// [`ControlCommand::GetBzz`] for `bypass_cache`, `progress`, and
    /// `max_bytes`.
    GetBytes {
        reference: [u8; 32],
        bypass_cache: bool,
        progress: bool,
        max_bytes: Option<u64>,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Gateway-only streaming `/bytes/{ref}` path. Sends
    /// [`ControlAck::BytesStreamStart`], zero or more
    /// [`ControlAck::BytesChunk`] messages, then [`ControlAck::StreamDone`]
    /// or [`ControlAck::Error`]. The JSON control socket does not expose
    /// this command; it exists so `ant-gateway` can behave like Bee and
    /// write the HTTP body while the joiner is still retrieving.
    ///
    /// `range` is interpreted against the file body (the joined bytes,
    /// not chunk wire bytes). `None` streams the whole file. The
    /// daemon clamps `range.end_inclusive` to `total_bytes - 1`.
    /// `head_only` short-circuits the body retrieval entirely: the
    /// daemon emits a `BytesStreamStart` with the file size and goes
    /// straight to `StreamDone`. This is what backs `HEAD /bytes/{addr}`
    /// without joining the chunk tree.
    StreamBytes {
        reference: [u8; 32],
        bypass_cache: bool,
        max_bytes: Option<u64>,
        range: Option<StreamRange>,
        head_only: bool,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Gateway-only streaming `/bzz/{ref}/{path}` path. Resolves the
    /// manifest, then streams the file body the same way `StreamBytes`
    /// does. Sends [`ControlAck::BzzStreamStart`] (with content type +
    /// filename + total size), zero or more
    /// [`ControlAck::BytesChunk`] messages, then
    /// [`ControlAck::StreamDone`] or [`ControlAck::Error`].
    ///
    /// `range` and `head_only` behave identically to `StreamBytes`.
    /// `head_only` is what backs `HEAD /bzz/{ref}/{path}`: the daemon
    /// resolves the manifest, fetches the data root chunk to learn
    /// the size, returns metadata + total span, and never joins.
    StreamBzz {
        reference: [u8; 32],
        path: String,
        allow_degraded_redundancy: bool,
        bypass_cache: bool,
        max_bytes: Option<u64>,
        range: Option<StreamRange>,
        head_only: bool,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Gateway `POST /chunks`: stamp locally (postage issuer) then pushsync.
    /// `batch_id` selects which registered postage batch issues the
    /// stamp; the node loop rejects the push if no issuer is registered
    /// for it (`ControlAck::Error "batch … not usable"`).
    PushChunk {
        wire: Vec<u8>,
        batch_id: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
    /// Gateway `POST /soc/{owner}/{id}`: stamp the SOC at `address` and
    /// pushsync the wire bytes. `address` is `keccak256(id || owner)`,
    /// already validated by the gateway via `soc_valid(&address, &wire)`;
    /// `wire` is the SOC's `swarm.Chunk.Data()` form (`id || sig ||
    /// inner_cac`). The node stamps directly at `address` rather than
    /// rederiving via BMT, since SOC addresses are not BMT-addressable.
    PushSoc {
        address: [u8; 32],
        wire: Vec<u8>,
        batch_id: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
    /// Register (or refresh) a postage batch with the running node so it
    /// can stamp uploads against it without a restart. Sent by the
    /// gateway right after a successful on-chain `createBatch`
    /// (`POST /stamps/{amount}/{depth}`) and after a `dilute`. The node
    /// opens (or reopens) the issuer's persistent `.bin` at
    /// `<data-dir>/postage/<batch_id>.bin` and inserts it into the live
    /// issuer registry. Idempotent: re-registering an existing batch
    /// reopens the same counters; a higher `depth` bumps the live
    /// issuer's depth in place (dilute). `bucket_depth` must equal the
    /// value baked into the `createBatch` transaction (bee uses 16) or
    /// the resulting stamps won't validate.
    RegisterBatch {
        batch_id: [u8; 32],
        depth: u8,
        bucket_depth: u8,
        immutable: bool,
        ack: oneshot::Sender<ControlAck>,
    },
    /// Install (or replace) the outbound SWAP settlement subsystem on
    /// the running node, so pushsync starts emitting cheques without a
    /// restart. Sent by `ant-ffi` right after it deploys / rediscovers
    /// the node's chequebook on first storage purchase: at that point
    /// the node wallet is funded and a factory-registered chequebook
    /// exists, but the swarm loop came up (at `ant_init`) before either
    /// was true. Without this the freshly bought batch would upload at
    /// most ~20 K chunks across the peer set before bee's accounting
    /// locks us out, and settlement would only take effect on the next
    /// app launch (when init reloads the persisted chequebook).
    ///
    /// `swap_secret` is the 32-byte secp256k1 secret of the
    /// chequebook's issuer EOA (the node identity key for an
    /// auto-deployed chequebook). Idempotent: re-installing the same
    /// `chequebook` is a no-op that re-acks `Ok`.
    EnablePushsyncSwap {
        chequebook: [u8; 20],
        swap_secret: [u8; 32],
        chain_id: u64,
        outbound_ledger_path: String,
        ack: oneshot::Sender<ControlAck>,
    },
    /// Create a new upload job. The node loop forwards to its
    /// `UploadManager`, which writes the persistent manifest, spawns
    /// the driver task, and acks with the assigned job id.
    UploadStart {
        source_path: PathBuf,
        batch_id: Option<String>,
        name: Option<String>,
        content_type: Option<String>,
        /// Skip the trailing single-file mantaray manifest. The
        /// completed job's `reference` is the data root chunk
        /// address (a `/bytes/<ref>` reference). See the
        /// [`crate::Request::UploadStart`] doc for the trade-off.
        raw: bool,
        ack: oneshot::Sender<ControlAck>,
    },
    /// Snapshot every known upload job. Cheap (returns the in-memory
    /// table; no I/O).
    UploadList { ack: oneshot::Sender<ControlAck> },
    /// Snapshot one upload job by id (or unique 8-hex-char prefix).
    UploadStatus {
        job_id: String,
        ack: oneshot::Sender<ControlAck>,
    },
    /// Soft-stop, hard-stop, or restart an upload job. Each variant
    /// acks with a fresh [`UploadJobView`] reflecting the new state.
    UploadPause {
        job_id: String,
        ack: oneshot::Sender<ControlAck>,
    },
    UploadResume {
        job_id: String,
        ack: oneshot::Sender<ControlAck>,
    },
    UploadCancel {
        job_id: String,
        ack: oneshot::Sender<ControlAck>,
    },
    /// Subscribe to live upload progress. Sends zero or more
    /// [`ControlAck::UploadProgress`] frames followed by a terminal
    /// [`ControlAck::UploadJob`] / [`ControlAck::Error`] when the job
    /// reaches a terminal status (or an error happens).
    UploadFollow {
        job_id: String,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Snapshot of the local postage stamp issuer (capacity, fill,
    /// per-bucket extremes). Pure local read — no chain RPC, no
    /// network round-trip. With multiple registered batches this returns
    /// the first one (back-compat for `antctl postage status`); use
    /// [`ControlCommand::PostageList`] to enumerate every batch.
    PostageStatus { ack: oneshot::Sender<ControlAck> },
    /// Snapshot of **every** registered postage batch. Backs the
    /// gateway's `GET /stamps`, which bee-js / Freedom poll to learn the
    /// set of usable batches. Pure local read.
    PostageList { ack: oneshot::Sender<ControlAck> },
    /// Write a single CAC chunk straight into the local disk chunk
    /// cache (no postage stamp, no pushsync). Backs `antctl pin`,
    /// which lets an operator make a previously-uploaded file
    /// retrievable through this node's HTTP gateway without
    /// waiting for bee-side neighbourhood replication.
    ///
    /// `wire` is the full chunk wire bytes (`span (8 LE) ||
    /// payload`); the handler recomputes the address (BMT-with-span)
    /// and rejects payloads whose hash doesn't match.
    PutChunkLocal {
        wire: Vec<u8>,
        ack: oneshot::Sender<ControlAck>,
    },
    /// Resolve a sequence feed `(owner, topic)` to its latest update.
    /// Backs the gateway's `GET /feeds/{owner}/{topic}` endpoint. The
    /// handler uses the same `RoutingFetcher` machinery `GetBytes` does,
    /// so the lookup is bounded by `ant_retrieval::feed`'s scan limits
    /// and tolerates transient peer flakiness. Epoch feeds aren't a
    /// supported variant: the gateway short-circuits `?type=epoch` with
    /// `501 Not Implemented` before sending the command, so the wire
    /// here is sequence-only.
    GetFeed {
        owner: [u8; 20],
        topic: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
}

/// Inclusive byte range used by [`ControlCommand::StreamBytes`] and
/// [`ControlCommand::StreamBzz`]. Mirrors
/// [`ant_retrieval::ByteRange`] but kept local so `ant-control` doesn't
/// take a build-graph dependency on the retrieval crate.
#[derive(Debug, Clone, Copy)]
pub struct StreamRange {
    pub start: u64,
    pub end_inclusive: u64,
}

/// Node-loop reply to a [`ControlCommand`]. Serialized back to the client as
/// `Response::Ok`, `Response::Bytes`, `Response::BzzBytes`,
/// `Response::Progress`, or `Response::Error` depending on the variant.
#[derive(Debug, Clone)]
pub enum ControlAck {
    Ok {
        message: String,
    },
    Bytes {
        data: Vec<u8>,
    },
    BzzBytes {
        data: Vec<u8>,
        content_type: Option<String>,
        filename: Option<String>,
    },
    Manifest {
        entries: Vec<ManifestEntryInfo>,
    },
    /// Streaming progress sample for a `GetBytes` / `GetBzz` request.
    /// Producer can fire as many of these as it likes; the dispatcher
    /// keeps writing them until a terminal variant arrives or the
    /// channel closes.
    Progress(GetProgress),
    BytesStreamStart {
        total_bytes: u64,
    },
    /// Streaming-bzz prologue: emitted by `StreamBzz` once the manifest
    /// has resolved and the data root chunk has been fetched (so the
    /// total span is known). Followed by zero or more `BytesChunk`s
    /// and a terminal `StreamDone` / `Error`.
    BzzStreamStart {
        total_bytes: u64,
        content_type: Option<String>,
        filename: Option<String>,
    },
    BytesChunk {
        data: Vec<u8>,
    },
    StreamDone,
    /// Successful `PushChunk`; reference is lowercase `0x` + 64 nibbles (bee-compatible).
    ChunkUploaded {
        reference: String,
    },
    /// Successful `UploadStart`.
    UploadStarted {
        job_id: String,
    },
    /// Snapshot of one upload job (terminal ack on
    /// `UploadStatus`/`UploadPause`/`UploadResume`/`UploadCancel`,
    /// terminal frame on `UploadFollow`).
    UploadJob(UploadJobView),
    /// Snapshot of every upload job (terminal ack on `UploadList`).
    UploadList(Vec<UploadJobView>),
    /// Streaming progress frame for `UploadFollow`. Non-terminal.
    UploadProgress(UploadJobView),
    /// Local postage stamp issuer snapshot. Terminal ack on
    /// `PostageStatus`.
    PostageStatus(PostageStatusView),
    /// Snapshot of every registered postage batch. Terminal ack on
    /// `PostageList`.
    PostageList(Vec<PostageStatusView>),
    /// Successful feed resolution: the SOC `id`
    /// (`keccak256(topic ‖ index_be8)`) of the resolved update, the
    /// reference its CAC payload points at, the sequence index it lived
    /// at, and the v1 timestamp embedded in that CAC payload. The `id`
    /// is what bee surfaces in the `swarm-feed-index` HTTP header on
    /// `GET /feeds/{owner}/{topic}`. Carrying it through the ack keeps
    /// the gateway from recomputing `keccak256` for the header.
    FeedResolved {
        id: [u8; 32],
        reference: [u8; 32],
        index: u64,
        ts: u64,
    },
    /// Empty-feed signal from `GetFeed`. Distinct from
    /// [`Self::Error`] so the gateway can map it to `404 Not Found`
    /// without substring-matching an error message.
    FeedNotFound,
    /// The node loop accepted the command but isn't ready to serve it
    /// (most commonly: zero connected peers). Distinct from
    /// [`Self::Error`] so the gateway can map it to `503 Service
    /// Unavailable` rather than the `502 Bad Gateway` it returns for
    /// genuine I/O failures.
    NotReady {
        message: String,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntryInfo {
    pub path: String,
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    /// File size in bytes derived from the data root chunk's BMT span.
    /// `None` means the entry is metadata-only (no data reference) or
    /// the data root chunk could not be fetched while listing the
    /// manifest. Skipped from the JSON wire when absent so older
    /// clients keep deserializing newer responses unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

impl ControlAck {
    pub(crate) const fn is_terminal(&self) -> bool {
        !matches!(
            self,
            Self::Progress(_)
                | Self::BytesStreamStart { .. }
                | Self::BzzStreamStart { .. }
                | Self::BytesChunk { .. }
                | Self::UploadProgress(_)
        )
    }
}

/// Build the `mpsc` ack channel handed to streaming-capable
/// `ControlCommand`s. Public so the node loop's tests can wire one up
/// without reaching into the server module's internals.
#[must_use]
pub fn streaming_ack_channel() -> (mpsc::Sender<ControlAck>, mpsc::Receiver<ControlAck>) {
    mpsc::channel(STREAM_ACK_CAPACITY)
}
