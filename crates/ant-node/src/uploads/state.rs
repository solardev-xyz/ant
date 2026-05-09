//! On-disk state for an in-flight or completed upload job.
//!
//! Each job is one `<job_id>.json` under `<data-dir>/uploads/`. The
//! manager rewrites the file atomically (tmp → fsync → rename) on every
//! checkpoint so a crash mid-write can never produce a half-readable
//! manifest. This is the same idiom `ant_postage::StampIssuer` uses for
//! its bucket counters.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Lifecycle status of an upload job.
///
/// State machine:
///
/// ```text
///   pending ─► running ─► completed
///                │  ▲
///                ▼  │
///              paused
///                │
///                ▼
///             cancelled / failed
/// ```
///
/// `Failed` is the terminal-error state and carries the last error
/// message. `Cancelled` is operator-initiated. Both are kept on disk
/// so `antctl upload list` can show recent history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadStatus {
    /// Job manifest written but the driver task hasn't yet
    /// dispatched its first chunk. Transient: callers will rarely
    /// observe this directly because `start` returns after the first
    /// status flush.
    Pending,
    /// Driver task is actively dispatching pushsyncs.
    Running,
    /// Operator-initiated soft stop. The driver has drained
    /// in-flight pushes and parked on a notify; `resume` brings it
    /// back to `Running`.
    Paused,
    /// All data + manifest chunks pushed. `reference` carries the
    /// manifest root.
    Completed,
    /// Operator-initiated hard stop. Already-pushed chunks are left
    /// in the network (Swarm will GC them when the postage TTL
    /// expires; we can't unpush).
    Cancelled,
    /// Driver hit an unrecoverable error. `last_error` carries the
    /// message. Operator can inspect, then either `cancel` (to clean
    /// up) or `resume` (to retry from the last checkpoint).
    Failed,
}

impl UploadStatus {
    /// Status implies the driver should stop holding resources.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

/// Persistent + live job state. The same struct serialises to disk
/// (manifest at `<data-dir>/uploads/<job_id>.json`) and to the wire
/// (`Response::UploadJob` from the control socket), so adding a field
/// only requires `#[serde(default)]` for backward compatibility with
/// older daemons / clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadJobInfo {
    /// 16-hex-char identifier, opaque to the operator. Stable across
    /// restarts.
    pub job_id: String,
    /// Absolute path to the source file on the daemon's filesystem.
    /// The daemon mmaps this — it must stay in place and unchanged
    /// for the upload's life. Resume refuses if the size or mtime
    /// changed.
    pub source_path: PathBuf,
    /// File body size in bytes at job creation. Cross-checked
    /// against the live `metadata().len()` on resume.
    pub source_size: u64,
    /// Source file's modification time at job creation, in
    /// milliseconds since the Unix epoch. Resume refuses on
    /// mismatch.
    pub source_mtime_unix_ms: u64,
    /// Optional postage batch override. `None` ⇒ use the daemon's
    /// configured default (`--postage-batch`).
    #[serde(default)]
    pub batch_id: Option<String>,
    /// Filename to use inside the single-file mantaray manifest.
    /// Defaults to the source file's basename.
    #[serde(default)]
    pub name: Option<String>,
    /// Optional content-type metadata for the manifest entry.
    /// Defaults to `application/octet-stream` (matches bee).
    #[serde(default)]
    pub content_type: Option<String>,
    /// When `true`, the driver skips the trailing single-file
    /// mantaray manifest and finishes with `reference` set to the
    /// data root chunk address (a `/bytes/<ref>` reference). When
    /// `false` (the default — and the value old persisted
    /// manifests get via `#[serde(default)]`), the original
    /// always-wrap-in-manifest behaviour applies.
    #[serde(default)]
    pub raw: bool,
    /// Lifecycle.
    pub status: UploadStatus,
    /// Cumulative source bytes whose covering leaf has been
    /// successfully pushed. Equal to `chunks_pushed × CHUNK_SIZE`
    /// for the in-progress prefix; `source_size` once complete.
    /// Used by the progress UI for the byte progress bar.
    pub bytes_pushed: u64,
    /// Cumulative chunks (leaves + intermediates + manifest)
    /// successfully pushed. Used as the resume cursor: on restart
    /// the driver re-streams from byte 0, recomputes every chunk
    /// address (deterministic by construction), and skips pushsync
    /// for the first `chunks_pushed` of them.
    pub chunks_pushed: u64,
    /// Best-effort estimate of total chunks (data + intermediates +
    /// manifest). Computed at job start from `source_size`. `None`
    /// for very-old persisted jobs that didn't record it.
    #[serde(default)]
    pub chunks_total: Option<u64>,
    /// Wall-clock unix seconds at `start`.
    pub created_at_unix: u64,
    /// Wall-clock unix seconds of the last status mutation. Updated
    /// on every checkpoint flush so `antctl upload list` can show
    /// "last seen 12 s ago".
    pub last_update_unix: u64,
    /// Most recent driver error (transient or terminal). Cleared on
    /// `resume`; preserved on `pause`.
    #[serde(default)]
    pub last_error: Option<String>,
    /// Final manifest root reference (`0x` + 64 hex). Set once
    /// `status` flips to `Completed`.
    #[serde(default)]
    pub reference: Option<String>,
}

impl UploadJobInfo {
    /// Path of the on-disk manifest for `state_dir/<job_id>.json`.
    #[must_use]
    pub fn manifest_path(state_dir: &Path, job_id: &str) -> PathBuf {
        state_dir.join(format!("{job_id}.json"))
    }

    /// Read a persisted manifest from disk. Returns `Err` for I/O
    /// failures; an empty / missing file maps to `Err`. Callers can
    /// treat `NotFound` as "job not found" and surface it to the
    /// client.
    pub fn load(path: &Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Crash-safe write: write to `<path>.tmp`, fsync, rename over
    /// `<path>`, then best-effort fsync the directory so the rename
    /// is durable across a power loss.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?;
        let tmp = path.with_extension("json.tmp");
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        if let Some(parent) = path.parent() {
            if let Ok(d) = std::fs::File::open(parent) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }
}
