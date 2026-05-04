//! Hand-written `extern "C"` FFI surface for the iOS download smoke test
//! (PLAN.md § 9, "iOS download smoke test (pre-FFI)").
//!
//! Four entry points — deliberately small, deliberately C-shaped, no
//! UniFFI, no `.udl`, no codegen. The full mobile artefact in Phase 4
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

use ant_control::{ControlAck, ControlCommand, GetProgress, IdentityInfo, PeerInfo, StatusSnapshot};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, random_overlay_nonce,
    random_secp256k1_secret, OVERLAY_NONCE_LEN, SECP256K1_SECRET_LEN,
};
use ant_node::{run_node, NodeConfig};
use k256::ecdsa::SigningKey;
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch, Notify};

/// Overall deadline for a single `ant_download` call, covering both
/// cold-start peer warmup and the actual chunk retrieval. 10 minutes
/// is comfortably above mainnet-from-scratch observations (~5-20 s
/// warmup, then 50–200 KiB/s until the hot chunk set settles) and
/// lets the iOS smoke test pull sub-gigabyte bzz payloads end-to-end
/// without hand-tuning per file. Phase-4's UniFFI surface will expose
/// this so hosts can pick their own.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);

/// How often to retry a `GetBytes` / `GetBzz` that bounces off an
/// empty BZZ peer set. iOS cold starts frequently hit this: the node
/// loop is up, TCP+Noise have dialed dozens of peers, but no one has
/// finished the BZZ handshake yet. Letting the outer timeout cover
/// the whole warmup window is friendlier than forcing the Swift
/// side to poll.
const NO_PEERS_RETRY_INTERVAL: Duration = Duration::from_millis(2000);

/// Per-request joiner size ceiling. `ant_retrieval::DEFAULT_MAX_FILE_BYTES`
/// is 32 MiB, which is the right cap for interactive `antctl get` but
/// trips on realistic bzz payloads (photos, short videos, archives).
/// Match `ant-gateway`'s 1 GiB ceiling here — it keeps the
/// "malformed span claims 10 TiB" safety net intact while letting
/// browser-shaped files through. The Phase-4 artefact will expose
/// this through the UniFFI surface so hosts can tune it per app.
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

    fn apply(&mut self, p: &GetProgress) {
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

    fn finish(&mut self) {
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
        .map(|d| d.as_secs())
        .unwrap_or(0);
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
        control_socket: String::new(),
        retrieval: Default::default(),
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

    let cfg = NodeConfig::mainnet_default(signing_secret, overlay_nonce, bootnodes, libp2p_keypair)
        .with_status(status_tx)
        .with_process_start(process_start)
        .with_peerstore_path(peerstore_path)
        .with_commands(cmd_rx);

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
            path: path_part.to_string(),
        })
    } else if let Some(rest) = trimmed.strip_prefix("bytes://") {
        Ok(ParsedRef::Bytes(parse_hex32(rest)?))
    } else {
        Ok(ParsedRef::Bytes(parse_hex32(trimmed)?))
    }
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
                    let wait =
                        NO_PEERS_RETRY_INTERVAL.min(deadline.saturating_duration_since(now));
                    // Wake up early on cancel so the user doesn't have
                    // to stare at a 2 s backoff after hitting Stop.
                    tokio::select! {
                        biased;
                        _ = cancel_notify.notified() => {}
                        _ = tokio::time::sleep(wait) => {}
                    }
                    if cancel_flag.load(Ordering::SeqCst) {
                        return Err(FfiError::Download("download canceled".to_string()));
                    }
                    continue;
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
            _ = cancel_notify.notified() => {
                return Err(FfiError::Download("download canceled".to_string()));
            }
            r = tokio::time::timeout_at(deadline, ack_rx.recv()) => r,
        };
        match next {
            Ok(Some(ControlAck::Bytes { data })) | Ok(Some(ControlAck::BzzBytes { data, .. })) => {
                return Ok(data)
            }
            Ok(Some(ControlAck::Error { message })) => return Err(FfiError::Download(message)),
            Ok(Some(ControlAck::Progress(p))) => {
                if let Ok(mut slot) = progress_slot.lock() {
                    slot.apply(&p);
                }
                continue;
            }
            Ok(Some(ControlAck::Ok { .. }))
            | Ok(Some(ControlAck::Manifest { .. }))
            | Ok(Some(ControlAck::BytesStreamStart { .. }))
            | Ok(Some(ControlAck::BzzStreamStart { .. }))
            | Ok(Some(ControlAck::BytesChunk { .. }))
            | Ok(Some(ControlAck::StreamDone)) => continue,
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
    let Some(handle) = handle.as_ref() else {
        return -1;
    };
    let connected = handle.status_rx.borrow().peers.connected;
    i32::try_from(connected).unwrap_or(i32::MAX)
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
    let Some(handle) = handle.as_ref() else {
        return -1;
    };
    handle.cancel_flag.store(true, Ordering::SeqCst);
    handle.cancel_notify.notify_waiters();
    0
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
        in_progress: if snapshot.in_progress { 1 } else { 0 },
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
    if ptr.is_null() || len == 0 {
        return;
    }
    let slice = std::slice::from_raw_parts_mut(ptr, len);
    drop(Box::from_raw(slice as *mut [u8]));
}

/// Free a NUL-terminated string written by the FFI into an `out_err`
/// out-parameter.
///
/// # Safety
///
/// `ptr` must have come from this crate. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    drop(CString::from_raw(ptr));
}

/// Shut the embedded node down. Aborts the Tokio runtime and frees the
/// handle. After this returns, `handle` must not be used again.
///
/// # Safety
///
/// `handle` must have come from [`ant_init`]. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_shutdown(handle: *mut AntHandle) {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, FfiError> {
    if ptr.is_null() {
        return Err(FfiError::NullPointer);
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map_err(|_| FfiError::InvalidUtf8)
}

unsafe fn cstr_to_path(ptr: *const c_char) -> Result<PathBuf, FfiError> {
    Ok(PathBuf::from(cstr_to_str(ptr)?))
}

unsafe fn clear_out_err(out_err: *mut *mut c_char) {
    if !out_err.is_null() {
        *out_err = std::ptr::null_mut();
    }
}

unsafe fn write_out_err(out_err: *mut *mut c_char, msg: &str) {
    if out_err.is_null() {
        return;
    }
    let cstring = CString::new(msg.replace('\0', "?"))
        .unwrap_or_else(|_| CString::new("error message contained a NUL byte").unwrap());
    *out_err = cstring.into_raw();
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
// console unmodified. The proper mobile artefact (PLAN.md § 11)
// replaces this with a `set_log_sink` callback; the smoke test doesn't
// need that yet.
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
            .with_writer(std::io::stderr)
            .try_init();
    });
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
    fn parse_reference_rejects_bad_length() {
        let err = parse_reference("abc").unwrap_err();
        assert!(matches!(err, FfiError::Reference(_)), "got {err:?}");
    }

    // ParsedRef doesn't derive Debug because [u8; 32] wouldn't print
    // usefully; provide it locally for the test diagnostics above.
    impl std::fmt::Debug for ParsedRef {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ParsedRef::Bytes(root) => write!(f, "Bytes(0x{})", hex::encode(root)),
                ParsedRef::Bzz { root, path } => {
                    write!(f, "Bzz(0x{}, {path:?})", hex::encode(root))
                }
            }
        }
    }
}
