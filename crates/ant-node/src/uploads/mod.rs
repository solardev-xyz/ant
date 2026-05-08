//! `antctl upload` job manager.
//!
//! Responsible for the long-lived state of in-flight, paused,
//! completed, and cancelled uploads. Each job is one daemon-side
//! task that walks a source file, splits it incrementally with
//! [`StreamingSplitter`], and pushsyncs every produced chunk to the
//! network through the existing [`ControlCommand::PushChunk`]
//! pipeline (so postage stamping reuses
//! `ant_p2p::UploadRuntime::issuer` exactly the way `POST /chunks`
//! does today).
//!
//! ## Key design points
//!
//! - **Streaming.** The source is mmap'd via [`memmap2::Mmap`] so a
//!   7 GiB file doesn't double-allocate; the kernel pages it in/out
//!   as we walk it. Resident memory is `levels × BRANCHES × 32 B +
//!   in-flight push window` ≈ tens of KB for any file size.
//!
//! - **Resume.** [`StreamingSplitter`] is deterministic (same input
//!   ⇒ same chunk addresses), so on resume we re-mmap, re-stream
//!   from byte 0, recompute every address, and skip pushsync for
//!   the first `chunks_pushed` chunks. No per-chunk receipts on
//!   disk, no double-stamping (the postage issuer is only consulted
//!   when we actually dispatch a `PushChunk`).
//!
//! - **Pause / cancel.** A driver checks
//!   [`JobHandle::pause_requested`] / [`JobHandle::cancel_requested`]
//!   between every chunk dispatch and every chunk completion. On
//!   pause it drains in-flight pushes, checkpoints state, and parks
//!   on a [`Notify`]. Cancel returns immediately and lets the
//!   future call drop in-flight work.
//!
//! - **Persistence.** [`UploadJobInfo`] is rewritten atomically
//!   (tmp → fsync → rename) every `CHECKPOINT_INTERVAL_CHUNKS`
//!   pushes plus on every state transition. A daemon crash mid-push
//!   loses at most this many chunks of progress (we just re-push
//!   them on the next start; pushsync is idempotent at the
//!   network level, so duplicate pushes for already-replicated
//!   chunks are a wasted RTT but not a correctness bug).
//!
//! - **Concurrency.** Bounded at [`MAX_PUSH_CONCURRENCY`] in-flight
//!   pushes per job (matches the gateway's parallel push width).
//!   Multiple jobs share the daemon's process-wide retrieval /
//!   pushsync semaphores, so two simultaneous uploads don't double
//!   the load on neighbours.

pub mod state;

use ant_control::{ControlAck, ControlCommand, UploadJobView};
use ant_retrieval::{
    build_single_file_manifest, ManifestWriteError, SplitChunk, StreamingSplitter,
};
// `ManifestWriteError` is re-exported from `ant-retrieval`'s root.
use futures::stream::{FuturesUnordered, StreamExt};
use memmap2::Mmap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tracing::{debug, info, warn};

pub use state::{UploadJobInfo, UploadStatus};

/// Bee's leaf chunk size — kept here as a private constant so the
/// driver doesn't have to depend on `ant-crypto` for one number.
/// Re-exported from `ant_retrieval` if outside callers need it.
const CHUNK_SIZE: usize = 4096;

/// Maximum simultaneous in-flight `PushChunk` futures per job. Same
/// value as the gateway's `MAX_PUSH_CONCURRENCY` so a single upload
/// can saturate the daemon's pushsync capacity without unfairly
/// dwarfing concurrent reads.
const MAX_PUSH_CONCURRENCY: usize = 32;

/// Re-checkpoint the on-disk job manifest every N successful pushes.
/// Chosen so a crash loses ≤ ~250 chunks (~1 MB at 4 KiB chunks)
/// without thrashing the disk: at peak push throughput (~5 K
/// chunks/s on a fast LAN) this is one fsync per 50 ms.
const CHECKPOINT_INTERVAL_CHUNKS: u64 = 256;

/// Cap on outstanding `PushChunk` ack timeout. Mirrors what the
/// gateway uses for `upload_bzz`.
const PUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Per-chunk retry budget before the job moves to `Failed`. Pushsync
/// against a freshly-handshaked peer set occasionally fires a
/// transient "no peers available" or "stamp signature rejected"
/// error; a small fixed retry hides those from the operator without
/// wallpapering over a real protocol failure.
const PER_CHUNK_RETRY_BUDGET: u32 = 5;

/// Future returned by [`UploadManager::dispatch_push`]: a chunk-emit
/// index (so the ack log can advance the in-order cursor) plus the
/// outcome of the pushsync round-trip.
type PushFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = (u64, Result<(), UploadError>)> + Send>>;

#[derive(Debug, Error)]
pub enum UploadError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest: {0}")]
    Manifest(#[from] ManifestWriteError),
    #[error(
        "source file changed since job was created (size {expected_size}, found {actual_size})"
    )]
    SourceChanged {
        expected_size: u64,
        actual_size: u64,
    },
    #[error("source file modified since job was created (mtime {expected_mtime_ms}, found {actual_mtime_ms})")]
    SourceMtimeChanged {
        expected_mtime_ms: u64,
        actual_mtime_ms: u64,
    },
    #[error("daemon node loop is no longer accepting commands")]
    DaemonGone,
    #[error("push chunk failed: {0}")]
    PushFailed(String),
    #[error("upload not found: {0}")]
    NotFound(String),
    #[error("upload in unexpected state: {0:?}")]
    BadState(UploadStatus),
}

/// Owned, daemon-resident job state plus its driver controls.
struct JobHandle {
    info: Mutex<UploadJobInfo>,
    /// Last persisted snapshot of `info`, broadcast to every
    /// `follow`er.
    progress: watch::Sender<UploadJobInfo>,
    /// Set by `pause`; the driver checks it between chunks. Never
    /// reset — instead, `resume` spawns a fresh driver task.
    pause_requested: AtomicBool,
    /// Set by `cancel`; same observation cadence as `pause`. Once
    /// set, the driver aborts at the next opportunity.
    cancel_requested: AtomicBool,
    /// Wakes a paused driver when `resume` is called. Only the
    /// resume path uses it; the driver itself calls `notified()`
    /// inside its run loop after observing `pause_requested`.
    notify: Notify,
}

impl JobHandle {
    fn snapshot(&self) -> UploadJobInfo {
        self.info.lock().expect("upload mutex poisoned").clone()
    }
}

/// Public, cheaply-cloneable handle to the daemon-side upload
/// system. Owned by the swarm event loop; dispatched into from each
/// `ControlCommand::Upload*` variant.
#[derive(Clone)]
pub struct UploadManager {
    inner: Arc<UploadManagerInner>,
}

struct UploadManagerInner {
    state_dir: PathBuf,
    cmd_tx: mpsc::Sender<ControlCommand>,
    jobs: Mutex<HashMap<String, Arc<JobHandle>>>,
    id_counter: AtomicU64,
}

impl UploadManager {
    /// Build a new manager rooted at `state_dir` (typically
    /// `<data-dir>/uploads`). Creates the directory if it doesn't
    /// exist. `cmd_tx` is the same control-command channel the
    /// HTTP gateway and `antctl` socket use; the manager dispatches
    /// `PushChunk` commands through it so the existing postage
    /// stamping + pushsync pipeline is reused unchanged.
    pub fn new(state_dir: PathBuf, cmd_tx: mpsc::Sender<ControlCommand>) -> std::io::Result<Self> {
        std::fs::create_dir_all(&state_dir)?;
        Ok(Self {
            inner: Arc::new(UploadManagerInner {
                state_dir,
                cmd_tx,
                jobs: Mutex::new(HashMap::new()),
                id_counter: AtomicU64::new(0),
            }),
        })
    }

    /// State-dir path so the daemon can log it / surface it in
    /// `antctl status`.
    pub fn state_dir(&self) -> &Path {
        &self.inner.state_dir
    }

    /// Create a new job for `source_path` with optional manifest
    /// metadata. Writes the persistent manifest, registers the job,
    /// spawns the driver, and returns the assigned job id. Does
    /// *not* wait for the first chunk to push.
    ///
    /// `raw = true` skips the trailing single-file mantaray
    /// manifest. The completed job's `reference` is the data root
    /// chunk address (a `/bytes/<ref>` reference) instead of the
    /// manifest root. Saves 1-2 chunks of postage and a final
    /// round-trip but loses the embedded filename + content-type;
    /// consumers must know the type out-of-band. `raw = false`
    /// keeps the historical bzz-compatible behaviour.
    pub async fn start(
        &self,
        source_path: PathBuf,
        batch_id: Option<String>,
        name: Option<String>,
        content_type: Option<String>,
        raw: bool,
    ) -> Result<String, UploadError> {
        let metadata = std::fs::metadata(&source_path)?;
        if !metadata.is_file() {
            return Err(UploadError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("not a regular file: {}", source_path.display()),
            )));
        }
        let source_size = metadata.len();
        let source_mtime_unix_ms = mtime_unix_ms(&metadata);

        let job_id = self.mint_id();
        let now_s = unix_seconds();
        let info = UploadJobInfo {
            job_id: job_id.clone(),
            source_path: source_path.clone(),
            source_size,
            source_mtime_unix_ms,
            batch_id,
            name: name.or_else(|| {
                source_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(str::to_string)
            }),
            content_type,
            raw,
            status: UploadStatus::Pending,
            bytes_pushed: 0,
            chunks_pushed: 0,
            chunks_total: Some(estimate_chunk_count(source_size, raw)),
            created_at_unix: now_s,
            last_update_unix: now_s,
            last_error: None,
            reference: None,
        };
        info.save(&UploadJobInfo::manifest_path(
            &self.inner.state_dir,
            &job_id,
        ))?;

        let (progress_tx, _progress_rx) = watch::channel(info.clone());
        let handle = Arc::new(JobHandle {
            info: Mutex::new(info),
            progress: progress_tx,
            pause_requested: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            notify: Notify::new(),
        });
        self.inner
            .jobs
            .lock()
            .expect("uploads jobs mutex poisoned")
            .insert(job_id.clone(), handle.clone());

        info!(
            target: "ant_node::uploads",
            job_id, source = %source_path.display(), source_size,
            "started upload",
        );
        self.spawn_driver(handle);
        Ok(job_id)
    }

    /// Look up a job by id (or by unique 8-hex-char prefix, which
    /// the operator can shorten to). Returns `NotFound` if absent.
    fn resolve(&self, id_or_prefix: &str) -> Result<Arc<JobHandle>, UploadError> {
        let jobs = self.inner.jobs.lock().expect("uploads jobs mutex poisoned");
        if let Some(h) = jobs.get(id_or_prefix) {
            return Ok(h.clone());
        }
        let mut hits = jobs
            .iter()
            .filter(|(k, _)| k.starts_with(id_or_prefix))
            .map(|(_, v)| v.clone());
        let first = hits.next();
        if hits.next().is_some() {
            return Err(UploadError::NotFound(format!(
                "ambiguous prefix {id_or_prefix}",
            )));
        }
        first.ok_or_else(|| UploadError::NotFound(id_or_prefix.to_string()))
    }

    /// Snapshot every known job. Stable insertion order is not
    /// guaranteed (HashMap), so callers that want a deterministic
    /// list sort by `created_at_unix`.
    pub fn list(&self) -> Vec<UploadJobInfo> {
        self.inner
            .jobs
            .lock()
            .expect("uploads jobs mutex poisoned")
            .values()
            .map(|h| h.snapshot())
            .collect()
    }

    /// Snapshot one job.
    pub fn status(&self, id: &str) -> Result<UploadJobInfo, UploadError> {
        Ok(self.resolve(id)?.snapshot())
    }

    /// Subscribe to live progress updates for `id`. Returns a
    /// [`watch::Receiver`] whose initial value is the current
    /// snapshot. Caller drops it when done following.
    pub fn subscribe(&self, id: &str) -> Result<watch::Receiver<UploadJobInfo>, UploadError> {
        let handle = self.resolve(id)?;
        Ok(handle.progress.subscribe())
    }

    /// Soft-stop a `Running` (or `Pending`) job. Idempotent on
    /// already-paused jobs; errors on terminal states so the CLI
    /// surfaces the operator's mistake.
    pub fn pause(&self, id: &str) -> Result<UploadJobInfo, UploadError> {
        let handle = self.resolve(id)?;
        let snap = {
            let mut info = handle.info.lock().expect("upload mutex poisoned");
            match info.status {
                UploadStatus::Running | UploadStatus::Pending => {
                    handle.pause_requested.store(true, Ordering::SeqCst);
                    info.status = UploadStatus::Paused;
                    info.last_update_unix = unix_seconds();
                    info.clone()
                }
                UploadStatus::Paused => info.clone(),
                other => return Err(UploadError::BadState(other)),
            }
        };
        let _ = handle.progress.send(snap.clone());
        snap.save(&UploadJobInfo::manifest_path(&self.inner.state_dir, id))?;
        Ok(snap)
    }

    /// Bring a `Paused` (or `Failed`) job back to `Running`. Spawns
    /// a fresh driver task; the previous driver has already
    /// exited.
    pub fn resume(&self, id: &str) -> Result<UploadJobInfo, UploadError> {
        let handle = self.resolve(id)?;
        let snap = {
            let mut info = handle.info.lock().expect("upload mutex poisoned");
            match info.status {
                UploadStatus::Paused | UploadStatus::Failed => {
                    handle.pause_requested.store(false, Ordering::SeqCst);
                    handle.cancel_requested.store(false, Ordering::SeqCst);
                    info.status = UploadStatus::Running;
                    info.last_error = None;
                    info.last_update_unix = unix_seconds();
                    info.clone()
                }
                UploadStatus::Running | UploadStatus::Pending => info.clone(),
                other => return Err(UploadError::BadState(other)),
            }
        };
        let _ = handle.progress.send(snap.clone());
        snap.save(&UploadJobInfo::manifest_path(&self.inner.state_dir, id))?;
        self.spawn_driver(handle);
        Ok(snap)
    }

    /// Hard-stop a job. Already-pushed chunks are left in the
    /// network (Swarm GCs them at postage TTL expiry — we can't
    /// unpush). Idempotent.
    pub fn cancel(&self, id: &str) -> Result<UploadJobInfo, UploadError> {
        let handle = self.resolve(id)?;
        let snap = {
            let mut info = handle.info.lock().expect("upload mutex poisoned");
            handle.cancel_requested.store(true, Ordering::SeqCst);
            handle.pause_requested.store(false, Ordering::SeqCst);
            handle.notify.notify_waiters();
            if !info.status.is_terminal() {
                info.status = UploadStatus::Cancelled;
                info.last_update_unix = unix_seconds();
            }
            info.clone()
        };
        let _ = handle.progress.send(snap.clone());
        snap.save(&UploadJobInfo::manifest_path(&self.inner.state_dir, id))?;
        Ok(snap)
    }

    /// On daemon boot, scan the state directory for persisted jobs
    /// and re-register them. Jobs in `Running` state are restarted
    /// (the previous daemon was killed mid-push); jobs in
    /// `Paused`/`Failed`/`Cancelled`/`Completed` are loaded into
    /// memory but not restarted, so `antctl upload list` still
    /// shows them.
    ///
    /// `auto_resume` controls whether `Running`/`Pending` jobs
    /// found on disk get a fresh driver task. Operators who want
    /// to inspect state before re-engaging the network can pass
    /// `false` (mapped from `--no-resume-uploads`).
    pub fn rehydrate_from_disk(&self, auto_resume: bool) -> std::io::Result<usize> {
        let dir = &self.inner.state_dir;
        let mut count = 0;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let info = match UploadJobInfo::load(&path) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        target: "ant_node::uploads",
                        path = %path.display(),
                        "skipping unreadable job manifest: {e}",
                    );
                    continue;
                }
            };
            let job_id = info.job_id.clone();
            let needs_restart =
                matches!(info.status, UploadStatus::Running | UploadStatus::Pending,);
            let (progress_tx, _rx) = watch::channel(info.clone());
            let handle = Arc::new(JobHandle {
                info: Mutex::new(info),
                progress: progress_tx,
                pause_requested: AtomicBool::new(false),
                cancel_requested: AtomicBool::new(false),
                notify: Notify::new(),
            });
            self.inner
                .jobs
                .lock()
                .expect("uploads jobs mutex poisoned")
                .insert(job_id.clone(), handle.clone());
            count += 1;
            if needs_restart && auto_resume {
                info!(
                    target: "ant_node::uploads",
                    job_id, "auto-resuming upload after daemon restart",
                );
                self.spawn_driver(handle);
            } else if needs_restart {
                // Park as Paused so the operator can inspect via
                // `antctl upload list` and `resume` explicitly.
                let snap = {
                    let h = self
                        .inner
                        .jobs
                        .lock()
                        .expect("uploads jobs mutex poisoned")
                        .get(&job_id)
                        .cloned()
                        .expect("just inserted");
                    let mut info = h.info.lock().expect("upload mutex poisoned");
                    info.status = UploadStatus::Paused;
                    info.last_update_unix = unix_seconds();
                    info.clone()
                };
                let _ = handle.progress.send(snap.clone());
                let _ = snap.save(&UploadJobInfo::manifest_path(
                    &self.inner.state_dir,
                    &job_id,
                ));
            }
        }
        Ok(count)
    }

    fn mint_id(&self) -> String {
        let counter = self.inner.id_counter.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        // 8 bytes of identity: low 40 bits of nanos XOR with the
        // counter (so two starts in the same nanosecond don't
        // collide). Hex-encoded for `antctl upload <id>` ergonomics.
        let raw = (nanos & 0x000000FF_FFFFFFFF) ^ (counter << 40);
        format!("{raw:016x}")
    }

    fn spawn_driver(&self, handle: Arc<JobHandle>) {
        let mgr = self.clone();
        tokio::spawn(async move {
            if let Err(e) = mgr.run_job(handle.clone()).await {
                warn!(
                    target: "ant_node::uploads",
                    "upload driver returned error: {e}",
                );
                // Defensive: any unhandled error from the driver
                // already surfaced by `mark_failed`; this branch
                // exists for the `?` propagation path before the
                // driver's own error handling kicks in.
                mgr.mark_failed(&handle, e.to_string());
            }
        });
    }

    /// Move `info.status` to `Failed` and persist. Called on
    /// terminal driver errors; the operator can `resume` to retry.
    fn mark_failed(&self, handle: &Arc<JobHandle>, message: String) {
        let snap = {
            let mut info = handle.info.lock().expect("upload mutex poisoned");
            info.status = UploadStatus::Failed;
            info.last_error = Some(message);
            info.last_update_unix = unix_seconds();
            info.clone()
        };
        let _ = handle.progress.send(snap.clone());
        let _ = snap.save(&UploadJobInfo::manifest_path(
            &self.inner.state_dir,
            &snap.job_id,
        ));
    }

    /// Drive a single job through to completion (or pause /
    /// cancel). Re-streams the source file from byte 0, recomputing
    /// every chunk address; chunks below the persisted resume
    /// cursor are skipped (no postage stamp consumed, no pushsync
    /// dispatched). All other chunks go through the existing
    /// `ControlCommand::PushChunk` pipeline with bounded
    /// concurrency.
    async fn run_job(&self, handle: Arc<JobHandle>) -> Result<(), UploadError> {
        // Mark Running unconditionally — even Pending jobs flip
        // here so the watch receiver sees the transition.
        {
            let mut info = handle.info.lock().expect("upload mutex poisoned");
            info.status = UploadStatus::Running;
            info.last_update_unix = unix_seconds();
            let _ = handle.progress.send(info.clone());
        }

        let snap = handle.snapshot();
        let source_path = snap.source_path.clone();

        // Re-validate: file must exist, be the same size and mtime
        // we recorded at start. Resume across mtime change would
        // produce a different chunk tree → the persisted reference
        // would not match the actual content.
        let metadata = std::fs::metadata(&source_path)?;
        if metadata.len() != snap.source_size {
            let e = UploadError::SourceChanged {
                expected_size: snap.source_size,
                actual_size: metadata.len(),
            };
            self.mark_failed(&handle, e.to_string());
            return Err(e);
        }
        let live_mtime = mtime_unix_ms(&metadata);
        if live_mtime != snap.source_mtime_unix_ms {
            let e = UploadError::SourceMtimeChanged {
                expected_mtime_ms: snap.source_mtime_unix_ms,
                actual_mtime_ms: live_mtime,
            };
            self.mark_failed(&handle, e.to_string());
            return Err(e);
        }

        // mmap (zero-copy). Empty files are a special case: mmap
        // on a 0-byte file is undefined on some platforms, so we
        // produce the empty leaf via the streaming splitter's
        // `finish` path without ever feeding a leaf.
        let file = std::fs::File::open(&source_path)?;
        let body: Option<Mmap> = if snap.source_size == 0 {
            None
        } else {
            // SAFETY: `body` is immutable for the upload's life; we
            // refuse to resume on mtime change, so the OS will not
            // relocate the file under us. macOS/Linux page faults
            // are tolerated by walking the bytes in 4 KiB strides.
            Some(unsafe { Mmap::map(&file)? })
        };

        let resume_cursor = snap.chunks_pushed;
        let mut splitter = StreamingSplitter::new();
        let mut next_index: u64 = 0;
        let mut ack_log = AckLog::new(resume_cursor);
        // Bounded queue of in-flight pushes. Each entry carries the
        // chunk's emit index so we can advance the in-order
        // checkpoint cursor on completion.
        let mut in_flight: FuturesUnordered<PushFuture> = FuturesUnordered::new();

        let mut bytes_emitted: u64 = 0;
        let mut leaf_iter = LeafIter::new(body.as_deref(), snap.source_size);

        loop {
            // Cancellation check at the top of every loop turn —
            // no chunk dispatch can sneak in after a `cancel` call.
            if handle.cancel_requested.load(Ordering::SeqCst) {
                debug!(
                    target: "ant_node::uploads",
                    job_id = %snap.job_id,
                    "cancel observed; draining in-flight pushes",
                );
                // Drop in_flight; let the spawned futures error
                // out on their own. We don't care about results.
                drop(in_flight);
                return Ok(());
            }
            // Pause check — drain in-flight, persist, park.
            if handle.pause_requested.load(Ordering::SeqCst) {
                self.drain_inflight(&mut in_flight, &mut ack_log, &handle)
                    .await?;
                debug!(
                    target: "ant_node::uploads",
                    job_id = %snap.job_id,
                    chunks_pushed = ack_log.cursor(),
                    "paused; awaiting resume",
                );
                // resume() flips pause_requested to false then
                // spawns a fresh driver task — so this driver
                // exits and is replaced.
                return Ok(());
            }

            // Either: dispatch the next chunk(s), await an
            // in-flight completion, or — if both queues are empty
            // and the splitter is exhausted — break to the finish
            // step.
            if in_flight.len() < MAX_PUSH_CONCURRENCY {
                if let Some(leaf) = leaf_iter.next() {
                    bytes_emitted += leaf.len() as u64;
                    let chunks = splitter.push_leaf(leaf);
                    for chunk in chunks {
                        let i = next_index;
                        next_index += 1;
                        if i < resume_cursor {
                            // Already pushed in a previous run.
                            // No stamp consumed, no command
                            // dispatched. The next-chunk index
                            // bookkeeping is what matters; the
                            // streaming splitter has just
                            // re-derived this address byte-for-byte.
                            continue;
                        }
                        in_flight.push(self.dispatch_push(i, chunk));
                    }
                    continue;
                }
            }

            if let Some((i, res)) = in_flight.next().await {
                match res {
                    Ok(()) => {
                        ack_log.record(i);
                        let cursor = ack_log.cursor();
                        let mut info = handle.info.lock().expect("upload mutex poisoned");
                        if cursor > info.chunks_pushed {
                            info.chunks_pushed = cursor;
                            // bytes_pushed tracks data leaves
                            // pushed (cursor includes intermediates,
                            // so derive from the leaf-bytes
                            // counter we keep on the side).
                            info.bytes_pushed =
                                ack_log.bytes_pushed(snap.source_size).min(snap.source_size);
                            info.last_update_unix = unix_seconds();
                            // Checkpoint to disk every N chunks —
                            // pubsub watchers always see updates.
                            let snap = info.clone();
                            drop(info);
                            let _ = handle.progress.send(snap.clone());
                            if cursor.is_multiple_of(CHECKPOINT_INTERVAL_CHUNKS) {
                                let _ = snap.save(&UploadJobInfo::manifest_path(
                                    &self.inner.state_dir,
                                    &snap.job_id,
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        // Best-effort: surface the failure and exit.
                        // The job stays in `Failed` and the operator
                        // can `resume` to retry from the persisted
                        // cursor.
                        let msg = e.to_string();
                        self.mark_failed(&handle, msg.clone());
                        return Err(UploadError::PushFailed(msg));
                    }
                }
                continue;
            }

            // Nothing in flight, nothing more to dispatch — break
            // to the finish step.
            if leaf_iter.is_done() && in_flight.is_empty() {
                break;
            }
        }
        // Source fully consumed. Cancel/pause check before pushing
        // intermediates is unnecessary because they were emitted
        // inline by `push_leaf` already; only the splitter's tail
        // (root + partial intermediates) remains.
        let (data_root, _total_bytes, tail) = splitter.finish();
        for chunk in tail {
            let i = next_index;
            next_index += 1;
            if i < resume_cursor {
                continue;
            }
            in_flight.push(self.dispatch_push(i, chunk));
        }
        // Drain the tail.
        self.drain_inflight(&mut in_flight, &mut ack_log, &handle)
            .await?;

        // Either wrap the data root in a single-file mantaray
        // manifest (the bzz-compatible default) or finish at the
        // raw data root (saves 1-2 chunks but loses the embedded
        // filename + content-type, so consumers must know the
        // file type out-of-band).
        let info_at_finish = handle.snapshot();
        let final_root: [u8; 32] = if info_at_finish.raw {
            data_root
        } else {
            let filename = info_at_finish
                .name
                .clone()
                .unwrap_or_else(|| "blob.bin".to_string());
            let manifest = build_single_file_manifest(
                &filename,
                info_at_finish.content_type.as_deref(),
                data_root,
            )?;
            for chunk in &manifest.chunks {
                let i = next_index;
                next_index += 1;
                if i < resume_cursor {
                    continue;
                }
                in_flight.push(self.dispatch_push(i, chunk.clone()));
            }
            self.drain_inflight(&mut in_flight, &mut ack_log, &handle)
                .await?;
            manifest.root
        };

        // Job complete. Record the final reference (manifest root,
        // or — for raw jobs — the data root) and flush a final
        // checkpoint. Pause/cancel from here on no-op (job is in
        // a terminal state).
        let snap = {
            let mut info = handle.info.lock().expect("upload mutex poisoned");
            info.status = UploadStatus::Completed;
            info.bytes_pushed = info.source_size;
            info.chunks_pushed = next_index;
            info.reference = Some(format!("0x{}", hex::encode(final_root)));
            info.last_update_unix = unix_seconds();
            info.clone()
        };
        let _ = handle.progress.send(snap.clone());
        snap.save(&UploadJobInfo::manifest_path(
            &self.inner.state_dir,
            &snap.job_id,
        ))?;
        info!(
            target: "ant_node::uploads",
            job_id = %snap.job_id,
            reference = %snap.reference.as_deref().unwrap_or(""),
            chunks = snap.chunks_pushed,
            bytes = snap.bytes_pushed,
            "upload completed",
        );
        // Defensive: silence "unused" lint for `bytes_emitted` —
        // we keep the counter on the side as a debug aid for
        // tracing slow uploads even though `info.bytes_pushed`
        // is the wire-visible field.
        let _ = bytes_emitted;
        Ok(())
    }

    /// Wait for every in-flight push to complete (success or
    /// failure) and update the cursor. Used at pause time, after
    /// the splitter tail is drained, and after the manifest is
    /// pushed.
    async fn drain_inflight(
        &self,
        in_flight: &mut FuturesUnordered<PushFuture>,
        ack_log: &mut AckLog,
        handle: &Arc<JobHandle>,
    ) -> Result<(), UploadError> {
        while let Some((i, res)) = in_flight.next().await {
            match res {
                Ok(()) => {
                    ack_log.record(i);
                    let cursor = ack_log.cursor();
                    let snap = {
                        let mut info = handle.info.lock().expect("upload mutex poisoned");
                        if cursor > info.chunks_pushed {
                            info.chunks_pushed = cursor;
                            info.bytes_pushed =
                                ack_log.bytes_pushed(info.source_size).min(info.source_size);
                            info.last_update_unix = unix_seconds();
                        }
                        info.clone()
                    };
                    let _ = handle.progress.send(snap.clone());
                }
                Err(e) => {
                    let msg = e.to_string();
                    self.mark_failed(handle, msg.clone());
                    return Err(UploadError::PushFailed(msg));
                }
            }
        }
        // Final checkpoint after each drain.
        let snap = handle.snapshot();
        snap.save(&UploadJobInfo::manifest_path(
            &self.inner.state_dir,
            &snap.job_id,
        ))?;
        Ok(())
    }

    /// Build the future that pushes one chunk through the existing
    /// `ControlCommand::PushChunk` pipeline. Boxed so the
    /// `FuturesUnordered` queue can hold heterogeneous push types
    /// (data leaf vs intermediate vs manifest chunk — the wire
    /// shape is identical, but boxing keeps the type uniform).
    fn dispatch_push(&self, index: u64, chunk: SplitChunk) -> PushFuture {
        let cmd_tx = self.inner.cmd_tx.clone();
        Box::pin(async move {
            let mut last_err: Option<String> = None;
            for attempt in 0..PER_CHUNK_RETRY_BUDGET {
                let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
                if cmd_tx
                    .send(ControlCommand::PushChunk {
                        wire: chunk.wire.clone(),
                        ack: ack_tx,
                    })
                    .await
                    .is_err()
                {
                    return (index, Err(UploadError::DaemonGone));
                }
                match tokio::time::timeout(PUSH_TIMEOUT, ack_rx).await {
                    Ok(Ok(ControlAck::ChunkUploaded { .. })) => {
                        return (index, Ok(()));
                    }
                    Ok(Ok(ControlAck::Error { message })) => {
                        last_err = Some(message);
                    }
                    Ok(Ok(other)) => {
                        return (
                            index,
                            Err(UploadError::PushFailed(format!(
                                "unexpected ack: {other:?}",
                            ))),
                        );
                    }
                    Ok(Err(_)) => {
                        return (index, Err(UploadError::DaemonGone));
                    }
                    Err(_) => {
                        last_err =
                            Some(format!("push timed out after {}s", PUSH_TIMEOUT.as_secs(),));
                    }
                }
                // Bounded back-off between retries: 100 ms × 2^n,
                // capped at 4 s. Most transient pushsync failures
                // (no peers ready, stamp signature collision)
                // recover within the first second.
                let backoff_ms = (100u64 << attempt).min(4000);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
            (
                index,
                Err(UploadError::PushFailed(format!(
                    "exhausted {PER_CHUNK_RETRY_BUDGET} retries: {}",
                    last_err.unwrap_or_else(|| "unknown".to_string()),
                ))),
            )
        })
    }
}

/// Iterator over the source file in `CHUNK_SIZE`-byte windows. The
/// last window may be shorter; an empty source produces zero leaves
/// (the splitter's `finish` will emit the canonical empty leaf).
struct LeafIter<'a> {
    body: Option<&'a [u8]>,
    cursor: usize,
    total: usize,
    done: bool,
}

impl<'a> LeafIter<'a> {
    fn new(body: Option<&'a [u8]>, total: u64) -> Self {
        Self {
            body,
            cursor: 0,
            total: total as usize,
            done: total == 0,
        }
    }

    fn is_done(&self) -> bool {
        self.done
    }
}

impl<'a> Iterator for LeafIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let body = self.body?;
        let end = (self.cursor + CHUNK_SIZE).min(self.total);
        let leaf = &body[self.cursor..end];
        self.cursor = end;
        if self.cursor >= self.total {
            self.done = true;
        }
        Some(leaf)
    }
}

/// Tracks which chunk indices have been confirmed pushed. The
/// chunk-emit order is deterministic (a property of
/// [`StreamingSplitter`]) but completion order is not — the cursor
/// advances by the highest contiguous prefix of completed indices.
///
/// On resume the cursor starts at `resume_cursor`; the driver
/// re-derives indices `0..resume_cursor` from the splitter and
/// skips dispatching them, so the first observed completion is
/// always at index `resume_cursor`.
struct AckLog {
    resume_cursor: u64,
    cursor: u64,
    /// Sparse "have we seen this index yet" bitmap relative to
    /// `cursor`. Each bit covers one chunk; the bitmap collapses
    /// when the leading run of set bits advances `cursor`.
    pending: std::collections::BTreeSet<u64>,
}

impl AckLog {
    fn new(resume_cursor: u64) -> Self {
        Self {
            resume_cursor,
            cursor: resume_cursor,
            pending: std::collections::BTreeSet::new(),
        }
    }

    fn record(&mut self, i: u64) {
        if i < self.cursor {
            return;
        }
        self.pending.insert(i);
        // Collapse the leading run.
        while let Some(&first) = self.pending.iter().next() {
            if first == self.cursor {
                self.pending.remove(&first);
                self.cursor += 1;
            } else {
                break;
            }
        }
    }

    fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Best-effort estimate of source bytes covered by the pushed
    /// prefix. Counts each chunk index ≤ source_chunks as
    /// `CHUNK_SIZE` bytes, capping at `source_size`. This is
    /// approximate (intermediates carry no source bytes themselves
    /// but advance the cursor) and is intended for the progress UI
    /// only.
    fn bytes_pushed(&self, source_size: u64) -> u64 {
        // For a depth-D tree, the first ceil(source_size /
        // CHUNK_SIZE) chunks emitted are leaves, but the streaming
        // splitter interleaves intermediates. As a UX-grade
        // estimator, cap at source_size — operators care about
        // "fraction of file uploaded", not chunk-tree pedantry.
        let approx = (self.cursor.saturating_sub(self.resume_cursor)) * CHUNK_SIZE as u64
            + (self.resume_cursor * CHUNK_SIZE as u64);
        approx.min(source_size)
    }
}

/// Estimate the chunk count for a file of `source_size` bytes.
///
/// Includes data leaves + every intermediate level, plus the
/// trailing single-file mantaray manifest root when `raw` is
/// `false` (the default). Used to prefill `chunks_total` so the
/// progress bar can show a percentage from chunk #1.
///
/// The estimate is slightly conservative: very small files
/// produce richer mantaray nodes that occasionally spill across
/// 2-3 chunks (extra metadata fork + value fork). The driver
/// always reports the true count via `chunks_pushed` once the
/// upload completes, so a slight under-count here only affects
/// the UX percentage during the last few hundred milliseconds of
/// the run.
pub fn estimate_chunk_count(source_size: u64, raw: bool) -> u64 {
    if source_size == 0 {
        // An empty raw upload still has a single (empty) data
        // leaf; an empty manifested upload has the leaf + a
        // manifest root.
        return if raw { 1 } else { 2 };
    }
    let leaves = source_size.div_ceil(CHUNK_SIZE as u64);
    let mut total: u64 = leaves;
    let mut level = leaves;
    while level > 1 {
        level = level.div_ceil(ant_retrieval::BRANCHES as u64);
        total += level;
    }
    if raw {
        total
    } else {
        // +1 single-file mantaray root.
        total + 1
    }
}

/// Wire-shape conversion: `UploadJobInfo` → `UploadJobView`. Used by
/// the command interceptor to turn a daemon-side snapshot into the
/// JSON shape `antctl` expects on the wire. Kept as a free function
/// so `ant-control` doesn't take a build-graph dependency on
/// `ant-node`.
pub fn to_view(info: UploadJobInfo) -> UploadJobView {
    UploadJobView {
        job_id: info.job_id,
        source_path: info.source_path.display().to_string(),
        source_size: info.source_size,
        batch_id: info.batch_id,
        name: info.name,
        content_type: info.content_type,
        raw: info.raw,
        status: match info.status {
            UploadStatus::Pending => "pending".into(),
            UploadStatus::Running => "running".into(),
            UploadStatus::Paused => "paused".into(),
            UploadStatus::Completed => "completed".into(),
            UploadStatus::Cancelled => "cancelled".into(),
            UploadStatus::Failed => "failed".into(),
        },
        bytes_pushed: info.bytes_pushed,
        chunks_pushed: info.chunks_pushed,
        chunks_total: info.chunks_total,
        created_at_unix: info.created_at_unix,
        last_update_unix: info.last_update_unix,
        last_error: info.last_error,
        reference: info.reference,
    }
}

/// Drive a `ControlCommand::UploadFollow` connection to completion.
/// Subscribes to the job's progress watch, emits one
/// [`ControlAck::UploadProgress`] per state change, and finishes
/// with a terminal [`ControlAck::UploadJob`] (the final snapshot)
/// when the job reaches a terminal status. If the job vanishes
/// mid-stream (cancelled and removed from the table — not done
/// today, but defensively handled), emits an `Error` ack instead.
pub async fn run_follow(mgr: UploadManager, job_id: String, ack: mpsc::Sender<ControlAck>) {
    let mut rx = match mgr.subscribe(&job_id) {
        Ok(rx) => rx,
        Err(e) => {
            let _ = ack
                .send(ControlAck::Error {
                    message: e.to_string(),
                })
                .await;
            return;
        }
    };
    // Always ship the current snapshot first so a follower that
    // attaches mid-flight gets immediate state.
    let snap0 = rx.borrow().clone();
    let terminal = snap0.status.is_terminal();
    let frame = if terminal {
        ControlAck::UploadJob(to_view(snap0))
    } else {
        ControlAck::UploadProgress(to_view(snap0))
    };
    if ack.send(frame).await.is_err() {
        return;
    }
    if terminal {
        return;
    }
    while rx.changed().await.is_ok() {
        let snap = rx.borrow().clone();
        let terminal = snap.status.is_terminal();
        let frame = if terminal {
            ControlAck::UploadJob(to_view(snap))
        } else {
            ControlAck::UploadProgress(to_view(snap))
        };
        if ack.send(frame).await.is_err() {
            return;
        }
        if terminal {
            return;
        }
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn mtime_unix_ms(meta: &std::fs::Metadata) -> u64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_control::ControlCommand;
    use ant_crypto::cac_valid;
    use std::collections::HashSet;
    use std::io::Write as _;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// Counts of pushes a fake node loop has accepted.
    #[derive(Default)]
    struct PushStats {
        /// Distinct chunk addresses (deduped). Lower than
        /// `total_dispatches` for payloads with content that
        /// collides at chunk boundaries.
        unique_addresses: HashSet<[u8; 32]>,
        /// Every accepted PushChunk, including duplicates produced
        /// by content collisions.
        total_dispatches: u64,
    }

    /// Spawn a fake "node loop" that consumes `PushChunk` commands
    /// and acks them as if they were really pushed to a peer.
    /// Validates each chunk's CAC so a regression in the splitter
    /// trips the test.
    fn spawn_fake_pushsync() -> (mpsc::Sender<ControlCommand>, Arc<Mutex<PushStats>>) {
        let (tx, mut rx) = mpsc::channel::<ControlCommand>(64);
        let stats: Arc<Mutex<PushStats>> = Arc::new(Mutex::new(PushStats::default()));
        let stats_for_task = stats.clone();
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    ControlCommand::PushChunk { wire, ack } => {
                        // Recompute the address from the wire bytes
                        // (span ‖ payload) and accept only CAC-valid
                        // chunks — this is the same cross-check the
                        // real `PushChunk` handler does inline.
                        let mut span = [0u8; 8];
                        span.copy_from_slice(&wire[..8]);
                        let addr =
                            ant_crypto::bmt::bmt_hash_with_span(&span, &wire[8..]).expect("BMT");
                        assert!(cac_valid(&addr, &wire), "invalid CAC chunk submitted");
                        {
                            let mut s = stats_for_task.lock().expect("pushed mutex");
                            s.unique_addresses.insert(addr);
                            s.total_dispatches += 1;
                        }
                        let _ = ack.send(ControlAck::ChunkUploaded {
                            reference: format!("0x{}", hex::encode(addr)),
                        });
                    }
                    other => panic!("unexpected control command in upload tests: {other:?}"),
                }
            }
        });
        (tx, stats)
    }

    fn write_temp_file(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        f.write_all(bytes).expect("write");
        f.flush().expect("flush");
        f
    }

    /// End-to-end happy path: a multi-MiB file ends in `Completed`
    /// with a non-empty manifest reference, and every chunk the
    /// driver dispatched matches a CAC-valid wire chunk.
    #[tokio::test]
    async fn upload_completes_multi_mib_file() {
        let len = 2_500_000usize;
        let payload: Vec<u8> = (0..len).map(|i| ((i * 31) % 251) as u8).collect();
        let f = write_temp_file(&payload);
        let state_dir = tempfile::tempdir().expect("tempdir");
        let (tx, stats) = spawn_fake_pushsync();
        let mgr = UploadManager::new(state_dir.path().to_path_buf(), tx).expect("manager");

        let id = mgr
            .start(f.path().to_path_buf(), None, None, None, false)
            .await
            .expect("start");

        let mut rx = mgr.subscribe(&id).expect("subscribe");
        loop {
            let snap = rx.borrow().clone();
            if snap.status.is_terminal() {
                assert_eq!(snap.status, UploadStatus::Completed);
                let r = snap.reference.expect("manifest reference");
                assert!(r.starts_with("0x"));
                assert_eq!(r.len(), 2 + 64);
                break;
            }
            // Wait for the next state change. `recv` errors when
            // the sender drops; that shouldn't happen mid-test.
            rx.changed().await.expect("watch");
        }
        // Sanity on dispatch count: leaves + ceil(leaves /
        // BRANCHES) intermediates + 1 root + 1 manifest. Every
        // dispatch is one PushChunk regardless of whether the
        // address turns out to be a duplicate of a previous push
        // (some payload patterns produce content collisions
        // between leaves; pushsync's idempotency makes that fine).
        let leaves = len.div_ceil(CHUNK_SIZE) as u64;
        let estimate = estimate_chunk_count(len as u64, false);
        let s = stats.lock().expect("stats");
        assert!(
            s.total_dispatches >= leaves,
            "expected ≥ {leaves} dispatches, got {}",
            s.total_dispatches,
        );
        // Estimate is "data + intermediates + 1 manifest"; real
        // manifests for single-file uploads sometimes spill across
        // 2-3 chunks (extra metadata fork + value fork). Accept up
        // to 4 manifest chunks of slack so a richer manifest
        // payload doesn't fail this guard.
        assert!(
            (estimate..=estimate + 4).contains(&s.total_dispatches),
            "dispatches {} not in estimate window {}..={}",
            s.total_dispatches,
            estimate,
            estimate + 4,
        );
    }

    /// `raw = true` skips the trailing single-file mantaray
    /// manifest. The completed job's `reference` is the data root
    /// chunk address (deterministic from the splitter), and the
    /// total dispatch count matches the no-manifest estimate.
    #[tokio::test]
    async fn raw_upload_skips_manifest() {
        // Chosen to span multiple leaves + an intermediate so the
        // splitter exercises the full tree code path, not just a
        // single-chunk file.
        let len = 600_000usize;
        let payload: Vec<u8> = (0..len).map(|i| ((i * 7) % 251) as u8).collect();
        let f = write_temp_file(&payload);
        let state_dir = tempfile::tempdir().expect("tempdir");
        let (tx, stats) = spawn_fake_pushsync();
        let mgr = UploadManager::new(state_dir.path().to_path_buf(), tx).expect("manager");

        let id = mgr
            .start(f.path().to_path_buf(), None, None, None, true)
            .await
            .expect("start");

        let mut rx = mgr.subscribe(&id).expect("subscribe");
        let final_snap = loop {
            let snap = rx.borrow().clone();
            if snap.status.is_terminal() {
                break snap;
            }
            rx.changed().await.expect("watch");
        };
        assert_eq!(final_snap.status, UploadStatus::Completed);
        assert!(final_snap.raw, "info.raw should round-trip through finish");
        let r = final_snap
            .reference
            .as_deref()
            .expect("data-root reference");
        assert!(r.starts_with("0x"));
        assert_eq!(r.len(), 2 + 64);

        // Independently re-derive the data root via the same
        // streaming splitter and assert the raw upload's reference
        // matches it byte-for-byte. Catches accidental "we still
        // returned the manifest root" regressions.
        let mut splitter = StreamingSplitter::new();
        let mut idx = 0usize;
        while idx < payload.len() {
            let end = (idx + CHUNK_SIZE).min(payload.len());
            let _ = splitter.push_leaf(&payload[idx..end]);
            idx = end;
        }
        let (data_root, _bytes, _tail) = splitter.finish();
        assert_eq!(r, format!("0x{}", hex::encode(data_root)));

        // Dispatch count should match the no-manifest estimate
        // exactly — there is no extra metadata fork to spill into
        // 2-3 chunks here.
        let estimate = estimate_chunk_count(len as u64, true);
        let s = stats.lock().expect("stats");
        assert_eq!(
            s.total_dispatches, estimate,
            "raw upload pushed {} chunks; expected {}",
            s.total_dispatches, estimate,
        );
    }

    /// Resume math: pre-seed an on-disk manifest with
    /// `chunks_pushed = N`, call `rehydrate`, and check the driver
    /// emits exactly `total - N` push commands. Proves the
    /// skip-first-N optimisation is wired and that re-pushing
    /// already-acknowledged chunks doesn't happen.
    #[tokio::test]
    async fn resume_skips_already_pushed_chunks() {
        let len = 250_000usize;
        let payload: Vec<u8> = (0..len).map(|i| ((i * 17) % 251) as u8).collect();
        let f = write_temp_file(&payload);
        let state_dir = tempfile::tempdir().expect("tempdir");

        // First pass: complete the upload normally.
        let (tx1, stats1) = spawn_fake_pushsync();
        let mgr1 = UploadManager::new(state_dir.path().to_path_buf(), tx1).expect("manager");
        let id = mgr1
            .start(f.path().to_path_buf(), None, None, None, false)
            .await
            .expect("start");
        let mut rx = mgr1.subscribe(&id).expect("subscribe");
        let final_snap = loop {
            let snap = rx.borrow().clone();
            if snap.status.is_terminal() {
                break snap;
            }
            rx.changed().await.expect("watch");
        };
        let total_chunks = final_snap.chunks_pushed;
        let pushed1_count = stats1.lock().expect("stats").total_dispatches;
        assert!(pushed1_count > 0);
        // Drop the manager to release the watch sender, simulating
        // a daemon shutdown after a successful upload.
        drop(mgr1);

        // Manually rewind the on-disk manifest to mid-flight: cut
        // chunks_pushed in half, status back to Running. This is
        // the "we crashed mid-push" scenario.
        let manifest_path = state_dir.path().join(format!("{id}.json"));
        let mut info = UploadJobInfo::load(&manifest_path).expect("load");
        info.chunks_pushed = total_chunks / 2;
        info.status = UploadStatus::Running;
        info.reference = None;
        info.save(&manifest_path).expect("save rewound");

        // Second pass: rehydrate, auto-resume, expect to push
        // exactly `total_chunks - chunks_pushed` further commands.
        let (tx2, stats2) = spawn_fake_pushsync();
        let mgr2 = UploadManager::new(state_dir.path().to_path_buf(), tx2).expect("manager");
        let restored = mgr2.rehydrate_from_disk(true).expect("rehydrate");
        assert_eq!(restored, 1);

        let mut rx2 = mgr2.subscribe(&id).expect("subscribe");
        loop {
            let snap = rx2.borrow().clone();
            if snap.status.is_terminal() {
                assert_eq!(snap.status, UploadStatus::Completed);
                break;
            }
            rx2.changed().await.expect("watch");
        }
        let pushed2_count = stats2.lock().expect("stats").total_dispatches;
        assert_eq!(
            pushed2_count,
            total_chunks - (total_chunks / 2),
            "resume should push exactly the chunks past the cursor",
        );
    }

    /// `cancel` flips status and stops the driver. Already-pushed
    /// chunks stay in `pushed`; we just check the job ends in
    /// `Cancelled`.
    #[tokio::test]
    async fn cancel_stops_running_job() {
        let len = 8 * 1024 * 1024usize;
        let payload: Vec<u8> = vec![0xAB; len];
        let f = write_temp_file(&payload);
        let state_dir = tempfile::tempdir().expect("tempdir");
        let (tx, _stats) = spawn_fake_pushsync();
        let mgr = UploadManager::new(state_dir.path().to_path_buf(), tx).expect("manager");

        let id = mgr
            .start(f.path().to_path_buf(), None, None, None, false)
            .await
            .expect("start");

        // Give the driver a moment to dispatch a few chunks, then
        // cancel.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let snap = mgr.cancel(&id).expect("cancel");
        assert_eq!(snap.status, UploadStatus::Cancelled);

        // Re-read from disk to confirm the cancel was persisted.
        let manifest_path = state_dir.path().join(format!("{id}.json"));
        let on_disk = UploadJobInfo::load(&manifest_path).expect("load");
        assert_eq!(on_disk.status, UploadStatus::Cancelled);
    }

    /// A source-file mtime change after the job was started causes
    /// resume to refuse and mark the job `Failed`. Important
    /// safety property: silently re-uploading mismatched bytes
    /// would produce a manifest that doesn't match what the user
    /// thinks they uploaded.
    #[tokio::test]
    async fn resume_refuses_on_mtime_change() {
        let len = 8000usize;
        let payload: Vec<u8> = vec![0x42; len];
        let f = write_temp_file(&payload);
        let state_dir = tempfile::tempdir().expect("tempdir");

        // Pre-stage a manifest that points at the file but with an
        // older mtime than the actual file.
        let job_id = "0123456789abcdef".to_string();
        let info = UploadJobInfo {
            job_id: job_id.clone(),
            source_path: f.path().to_path_buf(),
            source_size: len as u64,
            // Older than the file's true mtime.
            source_mtime_unix_ms: 1,
            batch_id: None,
            name: Some("blob.bin".into()),
            content_type: None,
            raw: false,
            status: UploadStatus::Running,
            bytes_pushed: 0,
            chunks_pushed: 0,
            chunks_total: Some(estimate_chunk_count(len as u64, false)),
            created_at_unix: 0,
            last_update_unix: 0,
            last_error: None,
            reference: None,
        };
        info.save(&UploadJobInfo::manifest_path(state_dir.path(), &job_id))
            .expect("save");

        let (tx, _stats) = spawn_fake_pushsync();
        let mgr = UploadManager::new(state_dir.path().to_path_buf(), tx).expect("manager");
        mgr.rehydrate_from_disk(true).expect("rehydrate");

        let mut rx = mgr.subscribe(&job_id).expect("subscribe");
        loop {
            let snap = rx.borrow().clone();
            if snap.status.is_terminal() {
                assert_eq!(snap.status, UploadStatus::Failed);
                let err = snap.last_error.expect("last_error set");
                assert!(
                    err.contains("modified") || err.contains("mtime"),
                    "expected mtime-mismatch message, got: {err}",
                );
                break;
            }
            rx.changed().await.expect("watch");
        }
    }
}
