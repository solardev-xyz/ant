//! Hand-written `extern "C"` FFI surface for the iOS download smoke test
//! (PLAN.md § 9, "iOS download smoke test (pre-FFI)").
//!
//! Four entry points — deliberately small, deliberately C-shaped, no
//! `UniFFI`, no `.udl`, no codegen. The full mobile artefact in Phase 4
//! replaces this with a UniFFI-generated `.xcframework`; this crate is
//! expected to be deleted (or promoted) when that lands.
//!
//! * [`ant_init`] spawns a Tokio runtime, drives the `ant-node` loop in
//!   the background, and returns an opaque handle.
//! * [`ant_download`] takes a reference string (`<64-hex>`,
//!   `bytes://<hex>`, or `bzz://<hex>[/path]`), drives the retrieval
//!   through the existing `ControlCommand` channel, and hands back an
//!   allocated byte buffer.
//! * [`ant_free_buffer`] / [`ant_free_string`] free buffers owned by
//!   this crate.
//! * [`ant_shutdown`] drops the handle and cleans up the runtime.
//!
//! **Thread-safety:** the handle itself is `Send + Sync`, so the host
//! app can call `ant_download` from any thread. Concurrent
//! `ant_download` calls are allowed and share the node's cache /
//! retrieval pipeline; the mpsc command channel serialises dispatch.

#[cfg(feature = "jni")]
mod jni;
mod manifest;
mod stream;

use ant_control::{
    ControlAck, ControlCommand, GetProgress, IdentityInfo, PeerInfo, RetrievalInfo, StatusSnapshot,
};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, random_overlay_nonce,
    random_secp256k1_secret, OVERLAY_NONCE_LEN, SECP256K1_SECRET_LEN,
};
use ant_node::{run_node, NodeConfig};
use ant_retrieval::DiskChunkCache;
use k256::ecdsa::SigningKey;
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use stream::AntStream;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch, Notify};

/// Overall deadline for a single `ant_download` call, covering both
/// cold-start peer warmup and the actual chunk retrieval. 10 minutes
/// is comfortably above mainnet-from-scratch observations (~5-20 s
/// warmup, then 50–200 KiB/s until the hot chunk set settles) and
/// lets the iOS smoke test pull sub-gigabyte bzz payloads end-to-end
/// without hand-tuning per file. Phase-4's `UniFFI` surface will expose
/// this so hosts can pick their own.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_mins(10);

/// How often to retry a `GetBytes` / `GetBzz` that bounces off an
/// empty BZZ peer set. iOS cold starts frequently hit this: the node
/// loop is up, TCP+Noise have dialed dozens of peers, but no one has
/// finished the BZZ handshake yet. Letting the outer timeout cover
/// the whole warmup window is friendlier than forcing the Swift
/// side to poll.
const NO_PEERS_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// On-disk `SQLite` chunk cache cap for the embedded node. The whole
/// point of running a Swarm node on-device is to amortise fetches
/// across app launches; without a persistent cache every cold start
/// re-walks the manifest tree from the network and re-pays the
/// per-chunk latency, which is what makes iOS feel "always slow" to
/// users who actually re-open the same content. 512 MiB is the size we
/// can comfortably commit to on a phone (sandboxed app caches survive
/// app restarts but get reaped by the OS under memory pressure;
/// half a gigabyte is well under that threshold on every device
/// shipped in the last five years), and covers thousands of small
/// media files or a couple of long-form videos.
const DISK_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Per-request joiner size ceiling. `ant_retrieval::DEFAULT_MAX_FILE_BYTES`
/// is 32 MiB, which is the right cap for interactive `antctl get` but
/// trips on realistic bzz payloads (photos, short videos, archives).
/// Match `ant-gateway`'s 1 GiB ceiling here — it keeps the
/// "malformed span claims 10 TiB" safety net intact while letting
/// browser-shaped files through. The Phase-4 artefact will expose
/// this through the `UniFFI` surface so hosts can tune it per app.
const MAX_DOWNLOAD_BYTES: u64 = 1024 * 1024 * 1024;

/// Opaque handle returned to the Swift side. Lives for as long as the
/// embedded node loop is running.
pub struct AntHandle {
    /// Held to keep the runtime alive for the lifetime of the handle.
    /// Dropping the runtime aborts every spawned task, so `ant_shutdown`
    /// does all the cleanup we need.
    runtime: Runtime,
    cmd_tx: mpsc::Sender<ControlCommand>,
    /// Watch channel the node loop writes into on every status tick.
    /// The FFI reads `status.peers.connected` through `ant_peer_count`
    /// so the host app can render a live peer counter without polling
    /// `antctl`-style control commands.
    status_rx: watch::Receiver<StatusSnapshot>,
    /// Most recent `ControlAck::Progress` sample captured inside
    /// `attempt_download`. Swift polls this through
    /// `ant_download_progress` to render the progress bar + throughput
    /// without needing a callback bridge across the FFI boundary.
    progress: Mutex<DownloadProgressState>,
    /// Cooperative cancel flag for an in-flight `ant_download`. Flipped
    /// to `true` by `ant_cancel_download`; checked at every ack recv
    /// and the no-peers backoff in `download_inner`. Reset to `false`
    /// at the start of each new download so a cancel request can only
    /// ever affect the download it was aimed at.
    cancel_flag: AtomicBool,
    /// Notifier woken alongside `cancel_flag` so a download that's
    /// blocked inside `ack_rx.recv()` or the retry sleep unblocks
    /// immediately instead of waiting up to the next progress tick
    /// (~150 ms) / backoff interval (2 s).
    cancel_notify: Notify,
}

/// Live snapshot of the in-flight download, maintained by the
/// `attempt_download` loop. Lives on the handle (not on a request
/// object) because the FFI intentionally keeps a single-command shape:
/// one `AntHandle`, one `ant_download` at a time is the common path
/// in the smoke test, and a shared progress slot is the minimum
/// plumbing that gives Swift a live view.
#[derive(Clone, Copy, Default)]
struct DownloadProgressState {
    in_progress: bool,
    bytes_done: u64,
    total_bytes: u64,
    chunks_done: u64,
    total_chunks: u64,
    elapsed_ms: u64,
    peers_used: u32,
    in_flight: u32,
    cache_hits: u64,
}

impl DownloadProgressState {
    fn start() -> Self {
        Self {
            in_progress: true,
            ..Default::default()
        }
    }

    const fn apply(&mut self, p: &GetProgress) {
        self.in_progress = true;
        self.bytes_done = p.bytes_done;
        self.total_bytes = p.total_bytes_estimate;
        self.chunks_done = p.chunks_done;
        self.total_chunks = p.total_chunks_estimate;
        self.elapsed_ms = p.elapsed_ms;
        self.peers_used = p.peers_used;
        self.in_flight = p.in_flight;
        self.cache_hits = p.cache_hits;
    }

    const fn finish(&mut self) {
        self.in_progress = false;
        self.in_flight = 0;
    }
}

// ---------------------------------------------------------------------------
// Identity file (a trimmed-down mirror of `antd::IdentityFile`; ant-ffi
// intentionally duplicates this rather than reaching into `antd`'s
// binary crate).
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct IdentityFile {
    signing_key: String,
    overlay_nonce: String,
    libp2p_keypair: Option<String>,
}

#[derive(Debug, thiserror::Error)]
enum FfiError {
    #[error("null pointer")]
    NullPointer,
    #[error("invalid UTF-8 in argument")]
    InvalidUtf8,
    #[error("{0}")]
    Io(String),
    #[error("{0}")]
    Crypto(String),
    #[error("{0}")]
    Runtime(String),
    #[error("{0}")]
    Reference(String),
    #[error("{0}")]
    Download(String),
}

// ---------------------------------------------------------------------------
// ant_init
// ---------------------------------------------------------------------------

/// Spin up an embedded node. `data_dir` is a UTF-8 C string pointing at
/// a writable directory (typically the iOS `Application Support/`
/// sandbox). The directory is created if it doesn't exist.
///
/// Returns a non-null handle on success. On failure, returns null and
/// writes an allocated NUL-terminated error string into `*out_err`;
/// the caller owns that string and must free it with [`ant_free_string`].
///
/// # Safety
///
/// * `data_dir` must be a valid NUL-terminated UTF-8 string.
/// * `out_err` must point at a writable `*mut c_char` slot, or be null
///   to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_init(
    data_dir: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut AntHandle {
    unsafe {
        clear_out_err(out_err);
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<AntHandle, FfiError> {
            let path = cstr_to_path(data_dir)?;
            init_inner(&path)
        }));
        match result {
            Ok(Ok(handle)) => Box::into_raw(Box::new(handle)),
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_init");
                std::ptr::null_mut()
            }
        }
    }
}

fn init_inner(data_dir: &Path) -> Result<AntHandle, FfiError> {
    install_log_subscriber();

    std::fs::create_dir_all(data_dir)
        .map_err(|e| FfiError::Io(format!("create data dir {}: {e}", data_dir.display())))?;

    let id_path = data_dir.join("identity.json");
    let (signing_secret, overlay_nonce, libp2p_keypair) = load_or_create_identity(&id_path)?;

    let vk = *SigningKey::from_bytes((&signing_secret).into())
        .map_err(|e| FfiError::Crypto(format!("invalid signing key: {e}")))?
        .verifying_key();
    let eth = ethereum_address_from_public_key(&vk);
    let overlay = overlay_from_ethereum_address(&eth, 1, &overlay_nonce);
    let peer_id = libp2p_keypair.public().to_peer_id();

    let started_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let initial_snapshot = StatusSnapshot {
        agent: format!("ant-ffi/{}", env!("CARGO_PKG_VERSION")),
        protocol_version: ant_control::PROTOCOL_VERSION,
        network_id: 1,
        pid: std::process::id(),
        started_at_unix,
        identity: IdentityInfo {
            eth_address: format!("0x{}", hex::encode(eth)),
            overlay: format!("0x{}", hex::encode(overlay)),
            peer_id: peer_id.to_string(),
        },
        peers: PeerInfo {
            node_limit: ant_p2p::DEFAULT_TARGET_PEERS as u32,
            ..PeerInfo::default()
        },
        listeners: Vec::new(),
        external_addresses: Vec::new(),
        control_socket: String::new(),
        retrieval: RetrievalInfo::default(),
    };
    let (status_tx, status_rx) = watch::channel(initial_snapshot);

    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(32);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ant-ffi")
        .build()
        .map_err(|e| FfiError::Runtime(format!("build tokio runtime: {e}")))?;

    let bootnodes = ant_p2p::default_mainnet_bootnodes();
    let peerstore_path = Some(data_dir.join("peers.json"));
    let process_start = Instant::now();

    // Persistent chunk cache. On-device retrieval is dominated by
    // per-chunk close-peer latency, so amortising fetches across app
    // launches is the single biggest win we can hand the host: a
    // re-opened photo album / video is served from local SQLite
    // instead of re-walking the manifest tree against a small (NAT'd,
    // ~100 peer) BZZ peer set. A failed open is non-fatal — the node
    // still serves uploads and live retrievals, it just doesn't
    // amortise them.
    let disk_cache_path = data_dir.join("chunks.sqlite");
    let disk_cache = match DiskChunkCache::open(&disk_cache_path, DISK_CACHE_MAX_BYTES) {
        Ok(c) => {
            tracing::info!(
                target: "ant-ffi",
                path = %disk_cache_path.display(),
                max_bytes = DISK_CACHE_MAX_BYTES,
                used_bytes = c.used_bytes(),
                "opened persistent chunk cache",
            );
            Some(Arc::new(c))
        }
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                path = %disk_cache_path.display(),
                "failed to open persistent chunk cache: {e}; falling back to in-memory only",
            );
            None
        }
    };

    let cfg = NodeConfig::mainnet_default(signing_secret, overlay_nonce, bootnodes, libp2p_keypair)
        .with_status(status_tx)
        .with_process_start(process_start)
        .with_peerstore_path(peerstore_path)
        .with_commands(cmd_rx)
        .with_disk_cache(disk_cache);

    runtime.spawn(async move {
        if let Err(e) = run_node(cfg).await {
            tracing::error!(target: "ant-ffi", "node loop exited with error: {e}");
        } else {
            tracing::info!(target: "ant-ffi", "node loop exited cleanly");
        }
    });

    tracing::info!(
        target: "ant-ffi",
        eth = %format!("0x{}", hex::encode(eth)),
        overlay = %format!("0x{}", hex::encode(overlay)),
        "ant-ffi node started",
    );

    Ok(AntHandle {
        runtime,
        cmd_tx,
        status_rx,
        progress: Mutex::new(DownloadProgressState::default()),
        cancel_flag: AtomicBool::new(false),
        cancel_notify: Notify::new(),
    })
}

// ---------------------------------------------------------------------------
// ant_download
// ---------------------------------------------------------------------------

/// Download a reference. `reference` may be:
///
/// * `<64-hex>` — treated as a `/bytes/<hex>` reference (single chunk
///   or multi-chunk byte tree).
/// * `bytes://<64-hex>` — same as bare hex, explicit form.
/// * `bzz://<64-hex>[/path]` — walk the mantaray manifest at `<hex>`,
///   resolve `/path` (or the manifest's `website-index-document`), and
///   join the resulting chunk tree.
///
/// On success, returns a heap-allocated buffer and writes its length
/// into `*out_len`. Free with [`ant_free_buffer`].
///
/// On failure, returns null and writes an allocated NUL-terminated
/// error string into `*out_err`. Free with [`ant_free_string`].
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been passed
///   to [`ant_shutdown`].
/// * `reference` must be a valid NUL-terminated UTF-8 string.
/// * `out_len` and `out_err` must point at writable slots (either may
///   be null to opt out).
#[no_mangle]
pub unsafe extern "C" fn ant_download(
    handle: *mut AntHandle,
    reference: *const c_char,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    unsafe {
        clear_out_err(out_err);
        if !out_len.is_null() {
            *out_len = 0;
        }
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<Vec<u8>, FfiError> {
            let handle = handle.as_ref().ok_or(FfiError::NullPointer)?;
            let reference = cstr_to_str(reference)?;
            download_inner(handle, reference)
        }));
        match result {
            Ok(Ok(bytes)) => {
                let len = bytes.len();
                let mut boxed = bytes.into_boxed_slice();
                let ptr = boxed.as_mut_ptr();
                // Ownership crosses the FFI boundary; freed by `ant_free`.
                #[allow(clippy::mem_forget)]
                std::mem::forget(boxed);
                if !out_len.is_null() {
                    *out_len = len;
                }
                ptr
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_download");
                std::ptr::null_mut()
            }
        }
    }
}

enum ParsedRef {
    Bytes([u8; 32]),
    Bzz { root: [u8; 32], path: String },
}

fn parse_reference(input: &str) -> Result<ParsedRef, FfiError> {
    let trimmed = input.trim();
    if let Some(rest) = trimmed.strip_prefix("bzz://") {
        let (hex_part, path_part) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        Ok(ParsedRef::Bzz {
            root: parse_hex32(hex_part)?,
            path: decode_bzz_path(path_part)?,
        })
    } else if let Some(rest) = trimmed.strip_prefix("bytes://") {
        Ok(ParsedRef::Bytes(parse_hex32(rest)?))
    } else {
        Ok(ParsedRef::Bytes(parse_hex32(trimmed)?))
    }
}

/// Percent-decode a `bzz://` path component the same way `ant-gateway`'s
/// axum extractor does, so manifest entries containing literal spaces /
/// unicode survive a round trip through a URL the user pastes in. A
/// path like `tracks/02%20butterfly.wav` decodes to `tracks/02 butterfly.wav`,
/// which is what the mantaray manifest stores. Invalid UTF-8 in the
/// decoded bytes is rejected loudly rather than masked with the lossy
/// replacement character — a manifest path is an exact byte sequence.
fn decode_bzz_path(raw: &str) -> Result<String, FfiError> {
    percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|e| FfiError::Reference(format!("invalid percent-encoding in bzz path: {e}")))
}

fn parse_hex32(s: &str) -> Result<[u8; 32], FfiError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 64 {
        return Err(FfiError::Reference(format!(
            "reference must be 32 bytes (64 hex chars), got {} chars",
            s.len()
        )));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out)
        .map_err(|e| FfiError::Reference(format!("invalid hex: {e}")))?;
    Ok(out)
}

fn download_inner(handle: &AntHandle, reference: &str) -> Result<Vec<u8>, FfiError> {
    let parsed = parse_reference(reference)?;
    let cmd_tx = handle.cmd_tx.clone();
    // Reset the progress slot AND the cancel flag at the start of
    // each download. A stale `true` here would otherwise instantly
    // abort the next download the moment it starts.
    if let Ok(mut slot) = handle.progress.lock() {
        *slot = DownloadProgressState::start();
    }
    handle.cancel_flag.store(false, Ordering::SeqCst);
    let progress = &handle.progress;
    let cancel_flag = &handle.cancel_flag;
    let cancel_notify = &handle.cancel_notify;
    let out = handle.runtime.block_on(async move {
        let deadline = tokio::time::Instant::now() + DOWNLOAD_TIMEOUT;
        loop {
            match attempt_download(
                &cmd_tx,
                &parsed,
                deadline,
                progress,
                cancel_flag,
                cancel_notify,
            )
            .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(FfiError::Download(msg)) if is_no_peers_error(&msg) => {
                    // The node hasn't completed the BZZ handshake with
                    // any peer yet. Sleep briefly and retry until the
                    // outer deadline — turns the cold-start pain into a
                    // "first download takes a while" experience rather
                    // than a hard failure that forces the Swift side to
                    // poll.
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(FfiError::Download(format!(
                            "no BZZ peers completed handshake within {} seconds",
                            DOWNLOAD_TIMEOUT.as_secs()
                        )));
                    }
                    let wait = NO_PEERS_RETRY_INTERVAL.min(deadline.saturating_duration_since(now));
                    // Wake up early on cancel so the user doesn't have
                    // to stare at a 2 s backoff after hitting Stop.
                    tokio::select! {
                        biased;
                        () = cancel_notify.notified() => {}
                        () = tokio::time::sleep(wait) => {}
                    }
                    if cancel_flag.load(Ordering::SeqCst) {
                        return Err(FfiError::Download("download canceled".to_string()));
                    }
                }
                Err(e) => return Err(e),
            }
        }
    });
    // Success or failure, clear the "in progress" flag so Swift can
    // hide the progress HUD on the next poll without racing on an
    // explicit stop signal.
    if let Ok(mut slot) = handle.progress.lock() {
        slot.finish();
    }
    out
}

fn is_no_peers_error(msg: &str) -> bool {
    msg.contains("no peers available")
}

async fn attempt_download(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    parsed: &ParsedRef,
    deadline: tokio::time::Instant,
    progress_slot: &Mutex<DownloadProgressState>,
    cancel_flag: &AtomicBool,
    cancel_notify: &Notify,
) -> Result<Vec<u8>, FfiError> {
    let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(32);
    let cmd = match parsed {
        ParsedRef::Bytes(root) => ControlCommand::GetBytes {
            reference: *root,
            bypass_cache: false,
            progress: true,
            max_bytes: Some(MAX_DOWNLOAD_BYTES),
            ack: ack_tx,
        },
        ParsedRef::Bzz { root, path } => ControlCommand::GetBzz {
            reference: *root,
            path: path.clone(),
            // Smoke-test app cares about "bytes come back", not
            // "bytes come back Reed-Solomon verified". Masking the
            // redundancy level off the root span lets us read files
            // uploaded with bee's default redundancy without shipping
            // the RS recovery machinery yet (see PLAN.md §E.8).
            allow_degraded_redundancy: true,
            bypass_cache: false,
            progress: true,
            max_bytes: Some(MAX_DOWNLOAD_BYTES),
            ack: ack_tx,
        },
    };
    cmd_tx
        .send(cmd)
        .await
        .map_err(|_| FfiError::Download("node loop is not accepting commands".to_string()))?;

    loop {
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(FfiError::Download("download canceled".to_string()));
        }
        // Race the ack recv against the cancel notifier so a Stop
        // press unblocks us immediately instead of waiting for the
        // next progress tick (~150 ms) or a long idle stretch. The
        // `biased` keyword picks the cancel branch first on every
        // wake so we never miss a signal that landed a microsecond
        // before an ack.
        let next = tokio::select! {
            biased;
            () = cancel_notify.notified() => {
                return Err(FfiError::Download("download canceled".to_string()));
            }
            r = tokio::time::timeout_at(deadline, ack_rx.recv()) => r,
        };
        match next {
            Ok(Some(ControlAck::Bytes { data } | ControlAck::BzzBytes { data, .. })) => {
                return Ok(data)
            }
            Ok(Some(ControlAck::Error { message })) => return Err(FfiError::Download(message)),
            Ok(Some(ControlAck::Progress(p))) => {
                if let Ok(mut slot) = progress_slot.lock() {
                    slot.apply(&p);
                }
            }
            Ok(Some(
                ControlAck::Ok { .. }
                | ControlAck::Manifest { .. }
                | ControlAck::BytesStreamStart { .. }
                | ControlAck::BzzStreamStart { .. }
                | ControlAck::BytesChunk { .. }
                | ControlAck::StreamDone
                | ControlAck::ChunkUploaded { .. }
                | ControlAck::UploadStarted { .. }
                | ControlAck::UploadJob(_)
                | ControlAck::UploadList(_)
                | ControlAck::UploadProgress(_)
                | ControlAck::PostageStatus(_)
                | ControlAck::PostageList(_)
                | ControlAck::FeedResolved { .. }
                | ControlAck::FeedNotFound
                | ControlAck::NotReady { .. },
            )) => {}
            Ok(None) => {
                return Err(FfiError::Download(
                    "node dropped the ack channel without a terminal response".to_string(),
                ))
            }
            Err(_) => {
                return Err(FfiError::Download(format!(
                    "download did not complete within {} seconds",
                    DOWNLOAD_TIMEOUT.as_secs()
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Live node status
// ---------------------------------------------------------------------------

/// Current number of BZZ peers we've completed the handshake with.
/// The node loop updates this on every status tick; the FFI reads the
/// latest snapshot from the shared watch channel without blocking.
///
/// Returns `-1` on a null / poisoned handle (never happens in practice
/// unless the Swift side calls this after `ant_shutdown`).
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_peer_count(handle: *const AntHandle) -> i32 {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return -1;
        };
        let connected = handle.status_rx.borrow().peers.connected;
        i32::try_from(connected).unwrap_or(i32::MAX)
    }
}

/// Snapshot the running node's `agent` string from the live status
/// channel — currently `ant-ffi/<crate-version>` set at
/// [`ant_init`] time. Lets the host display the version that is
/// actually running, rather than e.g. `CFBundleShortVersionString`
/// from the iOS bundle which can drift from the embedded library.
///
/// Returns a freshly-allocated UTF-8 C string the caller must release
/// with [`ant_free_string`], or `NULL` if `handle` is null / the
/// agent string contains an interior NUL (cannot happen in practice
/// since we control the format).
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_agent_string(handle: *const AntHandle) -> *mut c_char {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return std::ptr::null_mut();
        };
        let agent = handle.status_rx.borrow().agent.clone();
        match CString::new(agent) {
            Ok(cs) => cs.into_raw(),
            Err(_) => std::ptr::null_mut(),
        }
    }
}

/// C-ABI mirror of [`DownloadProgressState`]. Kept as `#[repr(C)]` so
/// the Swift side can treat it as a plain POD struct without a
/// bridging header beyond `ant.h`. Field order is locked to the header
/// declaration — do not reorder without regenerating the Swift view.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct AntProgress {
    /// `1` while a download is in-flight, `0` once it finished (or
    /// before the first call). Swift uses this to hide / show the
    /// progress HUD.
    pub in_progress: u8,
    /// Bytes of chunk wire data delivered to the joiner so far.
    pub bytes_done: u64,
    /// Expected file-body size derived from the data root's span.
    /// `0` until the root chunk has been fetched (and for single-chunk
    /// files, `1` chunk / root span bytes right away).
    pub total_bytes: u64,
    /// Number of chunks delivered to the joiner so far (cache hits +
    /// successful network fetches).
    pub chunks_done: u64,
    /// Estimated total chunks from the root span. `0` until known.
    pub total_chunks: u64,
    /// Wall-clock milliseconds since the node accepted the request.
    pub elapsed_ms: u64,
    /// Distinct peers we've fetched at least one chunk from. `0`
    /// means everything came from cache so far.
    pub peers_used: u32,
    /// Chunk fetches currently dispatched but not yet returned (a
    /// proxy for "how busy is the pipe right now").
    pub in_flight: u32,
    /// Cache hits served without going to the network.
    pub cache_hits: u64,
}

/// Request cancellation of any in-flight `ant_download` call on this
/// handle. The flag is checked cooperatively on the next ack loop
/// iteration (≤ one progress tick, i.e. ~150 ms under normal
/// retrieval) and wakes the ack recv / no-peers backoff immediately
/// via the handle's cancel notifier. The in-flight `ant_download`
/// returns with the error string `"download canceled"`.
///
/// Safe to call at any time, including when no download is running
/// (no-op) or from a different thread than the one blocked in
/// `ant_download`.
///
/// Returns `0` on success, `-1` if `handle` is null.
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_cancel_download(handle: *const AntHandle) -> i32 {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return -1;
        };
        handle.cancel_flag.store(true, Ordering::SeqCst);
        handle.cancel_notify.notify_waiters();
        0
    }
}

/// Sample the current download progress. Swift polls this every few
/// hundred milliseconds while a download is running to drive the
/// progress bar and throughput readout.
///
/// Returns `0` on success (and fills `*out`), `-1` on a null handle
/// or a null `out`. Never blocks beyond a short mutex acquisition;
/// the mutex is held only while `attempt_download` copies the latest
/// `ControlAck::Progress` sample in.
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been
///   passed to [`ant_shutdown`].
/// * `out` must point at a writable `AntProgress` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_download_progress(
    handle: *const AntHandle,
    out: *mut AntProgress,
) -> i32 {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return -1;
        };
        let Some(out) = out.as_mut() else {
            return -1;
        };
        let snapshot = match handle.progress.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        };
        *out = AntProgress {
            in_progress: u8::from(snapshot.in_progress),
            bytes_done: snapshot.bytes_done,
            total_bytes: snapshot.total_bytes,
            chunks_done: snapshot.chunks_done,
            total_chunks: snapshot.total_chunks,
            elapsed_ms: snapshot.elapsed_ms,
            peers_used: snapshot.peers_used,
            in_flight: snapshot.in_flight,
            cache_hits: snapshot.cache_hits,
        };
        0
    }
}

// ---------------------------------------------------------------------------
// Manifest listing — feeds the iOS player's "Up next" queue.
// ---------------------------------------------------------------------------

/// List the entries of a mantaray manifest. `reference` is a
/// `bzz://<64-hex>` (or bare 64-hex) string. Returns a heap-allocated
/// NUL-terminated JSON document of the form
/// `{"entries":[{"path":"...","reference":"...","size":N,"content_type":"..."},...]}`,
/// or null + error string on failure.
///
/// Free the returned string with [`ant_free_string`].
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been
///   passed to [`ant_shutdown`].
/// * `reference` must be a valid NUL-terminated UTF-8 string.
/// * `out_err` must point at a writable `*mut c_char` slot, or be null
///   to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_list_bzz(
    handle: *mut AntHandle,
    reference: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        clear_out_err(out_err);
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<String, FfiError> {
            let handle = handle.as_ref().ok_or(FfiError::NullPointer)?;
            let reference = cstr_to_str(reference)?;
            let parsed = parse_reference(reference)?;
            // Manifest listing only makes sense for `bzz://` references.
            // Reject `bytes://` here so callers don't get the gateway's
            // generic "not a manifest" error halfway down the stack.
            let root = match parsed {
                ParsedRef::Bzz { root, .. } => root,
                ParsedRef::Bytes(_) => {
                    return Err(FfiError::Reference(
                        "ant_list_bzz requires a bzz:// reference".into(),
                    ))
                }
            };
            manifest::list_to_json(&handle.runtime.handle().clone(), &handle.cmd_tx, root)
                .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(json)) => {
                let cstring = CString::new(json.replace('\0', "?"))
                    .unwrap_or_else(|_| CString::new("manifest contained NUL byte").unwrap());
                cstring.into_raw()
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_list_bzz");
                std::ptr::null_mut()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming session — backs the AVAssetResourceLoaderDelegate path.
// ---------------------------------------------------------------------------

/// Open a streaming session for `reference` (a `bzz://<hex>/path`
/// string). Sends a `head_only` request to learn the file size and
/// content type, then returns an opaque `AntStream*` the caller passes
/// to [`ant_stream_read`] / [`ant_stream_progress`] / [`ant_stream_close`].
///
/// On success: writes the file size into `*out_total_bytes` (when
/// non-null) and the manifest's `Content-Type` into `*out_content_type`
/// (when non-null and the manifest carried one — caller frees the
/// string with [`ant_free_string`]).
///
/// On failure: returns null and writes an allocated error into
/// `*out_err`.
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must outlive the returned
///   stream (calling [`ant_shutdown`] before [`ant_stream_close`] is
///   undefined behaviour).
/// * `reference` must be a valid NUL-terminated UTF-8 string of the
///   form `bzz://<64-hex>[/path]`.
/// * `out_total_bytes`, `out_content_type`, `out_err` may each be null
///   or point at a writable slot.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_open(
    handle: *mut AntHandle,
    reference: *const c_char,
    out_total_bytes: *mut u64,
    out_content_type: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> *mut AntStream {
    unsafe {
        clear_out_err(out_err);
        if !out_content_type.is_null() {
            *out_content_type = std::ptr::null_mut();
        }
        if !out_total_bytes.is_null() {
            *out_total_bytes = 0;
        }
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<AntStream, FfiError> {
            let handle = handle.as_ref().ok_or(FfiError::NullPointer)?;
            let reference = cstr_to_str(reference)?;
            let parsed = parse_reference(reference)?;
            let (root, path) = match parsed {
                ParsedRef::Bzz { root, path } => (root, path),
                ParsedRef::Bytes(_) => {
                    return Err(FfiError::Reference(
                        "ant_stream_open requires a bzz:// reference".into(),
                    ))
                }
            };
            AntStream::open(
                handle.runtime.handle().clone(),
                handle.cmd_tx.clone(),
                root,
                path,
            )
            .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(stream)) => {
                if !out_total_bytes.is_null() {
                    *out_total_bytes = stream.total_bytes();
                }
                if !out_content_type.is_null() {
                    if let Some(ct) = stream.content_type() {
                        let cstring = CString::new(ct.replace('\0', "?")).unwrap_or_else(|_| {
                            CString::new("content_type contained NUL byte").unwrap()
                        });
                        *out_content_type = cstring.into_raw();
                    }
                }
                Box::into_raw(Box::new(stream))
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_stream_open");
                std::ptr::null_mut()
            }
        }
    }
}

/// Pull `len` bytes starting at `offset` into `dst`. Blocks the calling
/// thread until the bytes are ready (or the per-read timeout fires).
/// Returns the number of bytes actually written into `dst` on success,
/// or `-1` on error (with an allocated error string in `*out_err`).
///
/// Reads past `total_bytes` are truncated to the available tail; the
/// return value reports the truncated length.
///
/// # Safety
///
/// * `stream` must come from [`ant_stream_open`] and must not have been
///   passed to [`ant_stream_close`].
/// * `dst` must point at a writable buffer of at least `len` bytes.
/// * `out_err` may be null to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_read(
    stream: *mut AntStream,
    offset: u64,
    len: usize,
    dst: *mut u8,
    out_err: *mut *mut c_char,
) -> isize {
    unsafe {
        clear_out_err(out_err);
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<usize, FfiError> {
            let stream = stream.as_ref().ok_or(FfiError::NullPointer)?;
            if dst.is_null() && len > 0 {
                return Err(FfiError::NullPointer);
            }
            let dst_slice = std::slice::from_raw_parts_mut(dst, len);
            stream
                .read(offset, len, dst_slice)
                .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(n)) => isize::try_from(n).unwrap_or(isize::MAX),
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                -1
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_stream_read");
                -1
            }
        }
    }
}

/// Pull a contiguous byte range and deliver each joiner-produced
/// chunk to `on_chunk` as it arrives. Unlike [`ant_stream_read`] —
/// which holds the calling thread until the entire `len` bytes have
/// been buffered — this drives a single `StreamBzz` request all the
/// way to its terminal `StreamDone`, invoking the callback for every
/// `BytesChunk` ack along the way. That matches the
/// `AVAssetResourceLoaderDelegate` shape on the Swift side: a long-
/// lived `dataRequest` that wants `respond(with:)` called as soon as
/// each piece of body lands, not after the whole window has buffered.
///
/// `on_chunk(ctx, ptr, len) -> i32`: returning a non-zero value asks
/// the FFI to stop pulling early (e.g. `AVPlayer` canceled the
/// `dataRequest`). The remaining acks are drained for clean shutdown.
/// The callback runs on a tokio runtime worker thread and must be
/// non-blocking and Send-safe.
///
/// Returns the total number of bytes delivered, or `-1` on error
/// (with `*out_err` set when the slot is non-null).
///
/// # Safety
///
/// * `stream` must come from [`ant_stream_open`] and must not have
///   been passed to [`ant_stream_close`].
/// * `on_chunk` must be a valid C ABI function pointer; `ctx` is
///   passed through opaquely and may be null.
/// * The callback receives a non-null `data` pointer and `len > 0`.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_pull(
    stream: *mut AntStream,
    offset: u64,
    len: u64,
    on_chunk: Option<
        unsafe extern "C" fn(ctx: *mut std::ffi::c_void, data: *const u8, len: usize) -> i32,
    >,
    ctx: *mut std::ffi::c_void,
    out_err: *mut *mut c_char,
) -> i64 {
    unsafe {
        clear_out_err(out_err);
        // SAFETY: the FFI shape uses a raw `*mut c_void` ctx that
        // Swift will only pass back into us through `on_chunk`. Once we
        // wrap it in a `usize` it crosses the `catch_unwind` boundary
        // without UnwindSafe complaints, and we cast back inside the
        // callback closure.
        let ctx_addr = ctx as usize;
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<u64, FfiError> {
            let stream = stream.as_ref().ok_or(FfiError::NullPointer)?;
            let cb = on_chunk.ok_or_else(|| FfiError::Download("on_chunk is null".into()))?;
            let mut closure = |chunk: &[u8]| -> bool {
                if chunk.is_empty() {
                    return false;
                }
                let rc = cb(
                    ctx_addr as *mut std::ffi::c_void,
                    chunk.as_ptr(),
                    chunk.len(),
                );
                rc != 0
            };
            stream
                .pull(offset, len, &mut closure)
                .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(n)) => i64::try_from(n).unwrap_or(i64::MAX),
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                -1
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_stream_pull");
                -1
            }
        }
    }
}

/// Sample the cumulative state of a stream (bytes downloaded so far,
/// total file size, last `Progress` chunk/peer counters). Fills `*out`
/// and returns 0 on success; returns `-1` if either pointer is null.
///
/// `bytes_done` is the running sum of bytes the stream has handed back
/// across all `ant_stream_read` calls — i.e. the number of bytes
/// `AVPlayer` has actually consumed for *this* file. The Network tab uses
/// this for the "2.4 MB / 117.5 MB" progress card. The chunk / peer /
/// in-flight numbers reflect the *most recent* range request the node
/// emitted a `Progress` ack for, which is the most useful proxy for
/// "what is the network doing right now" while a stream plays.
///
/// # Safety
///
/// * `stream` must come from [`ant_stream_open`] and must not have been
///   passed to [`ant_stream_close`].
/// * `out` must point at a writable [`AntProgress`] slot.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_progress(
    stream: *const AntStream,
    out: *mut AntProgress,
) -> i32 {
    unsafe {
        let Some(stream) = stream.as_ref() else {
            return -1;
        };
        let Some(out) = out.as_mut() else {
            return -1;
        };
        let (
            bytes_done,
            total_bytes,
            chunks_done,
            total_chunks,
            elapsed_ms,
            peers,
            in_flight,
            cache,
        ) = stream::snapshot_progress(stream);
        *out = AntProgress {
            in_progress: 1,
            bytes_done,
            total_bytes,
            chunks_done,
            total_chunks,
            elapsed_ms,
            peers_used: peers,
            in_flight,
            cache_hits: cache,
        };
        0
    }
}

/// Close a stream and free its handle. After this returns, `stream`
/// must not be used again.
///
/// # Safety
///
/// `stream` must have come from [`ant_stream_open`]. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_close(stream: *mut AntStream) {
    unsafe {
        if stream.is_null() {
            return;
        }
        drop(Box::from_raw(stream));
    }
}

// ---------------------------------------------------------------------------
// Memory management
// ---------------------------------------------------------------------------

/// Free a buffer returned by [`ant_download`]. `len` must be the value
/// the function wrote into `*out_len`; passing a different length is
/// undefined behaviour (we allocate with that exact length).
///
/// # Safety
///
/// See above. Calling with a null pointer is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_free_buffer(ptr: *mut u8, len: usize) {
    unsafe {
        if ptr.is_null() || len == 0 {
            return;
        }
        let slice = std::slice::from_raw_parts_mut(ptr, len);
        drop(Box::from_raw(std::ptr::from_mut::<[u8]>(slice)));
    }
}

/// Free a NUL-terminated string written by the FFI into an `out_err`
/// out-parameter.
///
/// # Safety
///
/// `ptr` must have come from this crate. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_free_string(ptr: *mut c_char) {
    unsafe {
        if ptr.is_null() {
            return;
        }
        drop(CString::from_raw(ptr));
    }
}

/// Shut the embedded node down. Aborts the Tokio runtime and frees the
/// handle. After this returns, `handle` must not be used again.
///
/// # Safety
///
/// `handle` must have come from [`ant_init`]. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_shutdown(handle: *mut AntHandle) {
    unsafe {
        if handle.is_null() {
            return;
        }
        let handle = Box::from_raw(handle);
        // Dropping the runtime aborts every spawned task (including the
        // node loop) and joins blocking threads. We ship it off to a
        // `shutdown_background` call so this FFI entry point never blocks
        // if the node loop is mid-dial and holding a socket open.
        handle.runtime.shutdown_background();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, FfiError> {
    unsafe {
        if ptr.is_null() {
            return Err(FfiError::NullPointer);
        }
        CStr::from_ptr(ptr)
            .to_str()
            .map_err(|_| FfiError::InvalidUtf8)
    }
}

unsafe fn cstr_to_path(ptr: *const c_char) -> Result<PathBuf, FfiError> {
    unsafe { Ok(PathBuf::from(cstr_to_str(ptr)?)) }
}

unsafe fn clear_out_err(out_err: *mut *mut c_char) {
    unsafe {
        if !out_err.is_null() {
            *out_err = std::ptr::null_mut();
        }
    }
}

unsafe fn write_out_err(out_err: *mut *mut c_char, msg: &str) {
    unsafe {
        if out_err.is_null() {
            return;
        }
        let cstring = CString::new(msg.replace('\0', "?"))
            .unwrap_or_else(|_| CString::new("error message contained a NUL byte").unwrap());
        *out_err = cstring.into_raw();
    }
}

fn load_or_create_identity(
    id_path: &Path,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; OVERLAY_NONCE_LEN], Keypair), FfiError> {
    if id_path.exists() {
        let raw = std::fs::read_to_string(id_path)
            .map_err(|e| FfiError::Io(format!("read {}: {e}", id_path.display())))?;
        let id: IdentityFile = serde_json::from_str(&raw)
            .map_err(|e| FfiError::Io(format!("parse identity.json: {e}")))?;
        let mut signing_secret = [0u8; SECP256K1_SECRET_LEN];
        hex::decode_to_slice(&id.signing_key, &mut signing_secret)
            .map_err(|e| FfiError::Io(format!("decode signing_key: {e}")))?;
        let mut overlay_nonce = [0u8; OVERLAY_NONCE_LEN];
        hex::decode_to_slice(&id.overlay_nonce, &mut overlay_nonce)
            .map_err(|e| FfiError::Io(format!("decode overlay_nonce: {e}")))?;
        let kp = if let Some(ref enc) = id.libp2p_keypair {
            let bytes = hex::decode(enc)
                .map_err(|e| FfiError::Io(format!("decode libp2p_keypair: {e}")))?;
            Keypair::from_protobuf_encoding(&bytes)
                .map_err(|e| FfiError::Io(format!("libp2p keypair protobuf: {e}")))?
        } else {
            secp256k1_keypair_from_signing_secret(&signing_secret)?
        };
        return Ok((signing_secret, overlay_nonce, kp));
    }

    let signing_secret = random_secp256k1_secret();
    let overlay_nonce = random_overlay_nonce();
    let kp = secp256k1_keypair_from_signing_secret(&signing_secret)?;

    let id = IdentityFile {
        signing_key: hex::encode(signing_secret),
        overlay_nonce: hex::encode(overlay_nonce),
        libp2p_keypair: Some(hex::encode(
            kp.to_protobuf_encoding()
                .map_err(|e| FfiError::Io(format!("encode libp2p keypair: {e}")))?,
        )),
    };
    let pretty = serde_json::to_string_pretty(&id)
        .map_err(|e| FfiError::Io(format!("serialize identity: {e}")))?;
    std::fs::write(id_path, pretty)
        .map_err(|e| FfiError::Io(format!("write {}: {e}", id_path.display())))?;
    Ok((signing_secret, overlay_nonce, kp))
}

fn secp256k1_keypair_from_signing_secret(
    secret: &[u8; SECP256K1_SECRET_LEN],
) -> Result<Keypair, FfiError> {
    let mut sk_copy = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut sk_copy)
        .map_err(|e| FfiError::Crypto(format!("libp2p secp256k1 secret: {e}")))?;
    let kp = identity::secp256k1::Keypair::from(sk);
    Ok(Keypair::from(kp))
}

// ---------------------------------------------------------------------------
// Logging
//
// iOS surfaces process stderr through os_log, so a plain
// tracing_subscriber::fmt() pointed at stderr shows up in the Xcode
// console unmodified. Android does *not* — release-build stderr is
// silently discarded — so on `target_os = "android"` we pipe through
// __android_log_write instead, which routes to logcat under the tag
// "ant-ffi". Both branches stay behind a `Once` so a Kotlin /
// SwiftUI host that calls ant_init twice doesn't stack two
// subscribers.
//
// The proper mobile artefact (PLAN.md § 11) replaces both with a
// `set_log_sink` callback so the host owns sink lifecycle; the smoke
// tests don't need that yet.
// ---------------------------------------------------------------------------

fn install_log_subscriber() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("ANT_LOG")
            .or_else(|_| std::env::var("RUST_LOG"))
            .unwrap_or_else(|_| "info,ant_p2p=info,ant_retrieval=info".to_string());
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_writer(default_log_writer())
            .try_init();
    });
}

#[cfg(not(target_os = "android"))]
fn default_log_writer() -> impl for<'a> tracing_subscriber::fmt::MakeWriter<'a> + 'static {
    std::io::stderr
}

/// Logcat-backed writer. `tracing_subscriber::fmt` calls
/// `make_writer()` once per event, then writes the formatted line in
/// a single `write` call followed by a `flush`. Each `write` is one
/// fully-formed line, so we hand it straight to `__android_log_write`
/// after stripping the trailing newline (logcat appends its own).
#[cfg(target_os = "android")]
fn default_log_writer() -> android_log::MakeWriter {
    android_log::MakeWriter
}

#[cfg(target_os = "android")]
mod android_log {
    use std::ffi::CString;
    use std::io::{self, Write};
    use std::os::raw::c_char;

    /// `android_LogPriority::ANDROID_LOG_INFO`. Hard-coded rather than
    /// pulled in via `android_log_sys` to keep the Android branch
    /// dependency-free.
    const ANDROID_LOG_INFO: i32 = 4;

    /// Tag passed to logcat; `adb logcat ant-ffi:V *:S` filters on
    /// just our output.
    const LOG_TAG: &str = "ant-ffi";

    // Linked from the NDK's `liblog.so`, which is part of the standard
    // Android system libraries — every NDK toolchain ships it and
    // every Android process can link against it without extra Gradle
    // wiring.
    #[link(name = "log")]
    extern "C" {
        fn __android_log_write(prio: i32, tag: *const c_char, text: *const c_char) -> i32;
    }

    pub struct LogcatWriter;

    impl Write for LogcatWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            // tracing_subscriber::fmt always writes valid UTF-8, but
            // be defensive against any future formatter that chunks
            // mid-codepoint.
            let s = std::str::from_utf8(buf).unwrap_or("(non-utf8 log line)");
            let trimmed = s.trim_end_matches(|c| c == '\r' || c == '\n');
            if !trimmed.is_empty() {
                if let (Ok(tag), Ok(msg)) = (CString::new(LOG_TAG), CString::new(trimmed)) {
                    unsafe { __android_log_write(ANDROID_LOG_INFO, tag.as_ptr(), msg.as_ptr()) };
                }
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    pub struct MakeWriter;

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeWriter {
        type Writer = LogcatWriter;
        fn make_writer(&'a self) -> Self::Writer {
            LogcatWriter
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reference_accepts_bare_hex() {
        let hex = "a".repeat(64);
        match parse_reference(&hex) {
            Ok(ParsedRef::Bytes(addr)) => assert_eq!(addr, [0xaa; 32]),
            other => panic!("expected ParsedRef::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_accepts_0x_prefix() {
        let mut hex = "0x".to_string();
        hex.push_str(&"b".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bytes(addr)) => assert_eq!(addr, [0xbb; 32]),
            other => panic!("expected ParsedRef::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_accepts_bytes_scheme() {
        let hex = format!("bytes://{}", "c".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bytes(addr)) => assert_eq!(addr, [0xcc; 32]),
            other => panic!("expected ParsedRef::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_accepts_bzz_with_path() {
        let hex = format!("bzz://{}/index.html", "d".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bzz { root, path }) => {
                assert_eq!(root, [0xdd; 32]);
                assert_eq!(path, "index.html");
            }
            other => panic!("expected ParsedRef::Bzz, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_percent_decodes_bzz_path() {
        let hex = format!("bzz://{}/tracks/02%20butterfly.wav", "e".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bzz { root, path }) => {
                assert_eq!(root, [0xee; 32]);
                assert_eq!(path, "tracks/02 butterfly.wav");
            }
            other => panic!("expected ParsedRef::Bzz, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_decodes_unicode_bzz_path() {
        let hex = format!("bzz://{}/caf%C3%A9/men%C3%BC.txt", "f".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bzz { path, .. }) => {
                assert_eq!(path, "café/menü.txt");
            }
            other => panic!("expected ParsedRef::Bzz, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_rejects_invalid_percent_utf8() {
        let hex = format!("bzz://{}/bad%FFpath", "1".repeat(64));
        match parse_reference(&hex) {
            Err(FfiError::Reference(msg)) => {
                assert!(msg.contains("percent-encoding"), "got {msg:?}");
            }
            other => panic!("expected FfiError::Reference, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_rejects_bad_length() {
        let err = parse_reference("abc").unwrap_err();
        assert!(matches!(err, FfiError::Reference(_)), "got {err:?}");
    }

    // ParsedRef doesn't derive Debug because [u8; 32] wouldn't print
    // usefully; provide it locally for the test diagnostics above.
    impl std::fmt::Debug for ParsedRef {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Bytes(root) => write!(f, "Bytes(0x{})", hex::encode(root)),
                Self::Bzz { root, path } => {
                    write!(f, "Bzz(0x{}, {path:?})", hex::encode(root))
                }
            }
        }
    }
}
