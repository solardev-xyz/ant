//! Upload-tag registry — bee-shaped `POST /tags` + `GET /tags/{uid}`.
//!
//! Bee assigns every upload a *tag*: a small counter object tracking
//! how many chunks of the upload have been split, seen, stored, sent,
//! and synced. bee-js surfaces it as `UploadResult.tagUid` (read from
//! the `Swarm-Tag-Uid` response header) and polls `GET /tags/{uid}` to
//! drive an upload-progress UI (PLAN.md J.2.4). Freedom's
//! `getUploadStatus` reads `split / seen / stored / sent / synced /
//! uid` and renders the progress bar from `sent / split`.
//!
//! `antd`'s upload path is **synchronous**: `POST /bzz` / `POST /bytes`
//! pushsync every chunk to the network before sending the HTTP
//! response. By the time bee-js polls the tag, the upload is already
//! complete. So a tag is created in the "all done" state the moment the
//! upload finishes — `split == seen == stored == sent == synced ==
//! total`. A freshly created tag (via `POST /tags`, before any upload
//! is attached) starts at zero, matching bee.

use std::collections::HashMap;
use std::sync::Mutex;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::error::json_error;
use crate::handle::GatewayHandle;
use crate::status::format_rfc3339_now;

/// One upload tag. Counters are cumulative chunk counts, matching bee's
/// `tags.Tag`. `address` is the upload's root reference once known.
#[derive(Debug, Clone, Default)]
struct Tag {
    uid: u32,
    total: u64,
    split: u64,
    seen: u64,
    stored: u64,
    sent: u64,
    synced: u64,
    address: Option<[u8; 32]>,
    started_at: String,
}

/// bee-shaped `GET /tags/{uid}` / `POST /tags` response body. Field
/// names and casing match bee's `tags.Tag` JSON exactly so bee-js
/// decodes it without a shim.
#[derive(Debug, Serialize)]
pub struct TagView {
    pub uid: u32,
    pub total: u64,
    pub split: u64,
    pub seen: u64,
    pub stored: u64,
    pub sent: u64,
    pub synced: u64,
    /// Root reference as bare lowercase hex, or empty before it's
    /// known. Bee emits an all-zero address for a not-yet-uploaded
    /// tag; bee-js tolerates either.
    pub address: String,
    #[serde(rename = "startedAt")]
    pub started_at: String,
}

impl Tag {
    fn view(&self) -> TagView {
        TagView {
            uid: self.uid,
            total: self.total,
            split: self.split,
            seen: self.seen,
            stored: self.stored,
            sent: self.sent,
            synced: self.synced,
            address: self.address.map(hex::encode).unwrap_or_default(),
            started_at: self.started_at.clone(),
        }
    }
}

/// Thread-safe in-process tag store. Cheap: a `Mutex<HashMap>` only
/// touched on upload start/finish and tag polls, neither of which is
/// hot. Uids are monotonic per process start (bee restarts also reset
/// uids, so no client depends on cross-restart stability).
#[derive(Debug, Default)]
pub struct TagRegistry {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    next_uid: u32,
    tags: HashMap<u32, Tag>,
}

impl TagRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                // Bee uids start at 1; 0 is "no tag".
                next_uid: 1,
                tags: HashMap::new(),
            }),
        }
    }

    /// Create an empty tag and return its uid. Mirrors `POST /tags`.
    pub fn create(&self) -> u32 {
        let mut inner = self.inner.lock().expect("tag registry poisoned");
        let uid = inner.next_uid.max(1);
        inner.next_uid = uid.wrapping_add(1).max(1);
        inner.tags.insert(
            uid,
            Tag {
                uid,
                started_at: format_rfc3339_now(),
                ..Tag::default()
            },
        );
        uid
    }

    /// Create a tag already marked fully synced for a synchronous
    /// upload of `total` chunks rooted at `address`. Returns the new
    /// uid for the `Swarm-Tag-Uid` response header.
    pub fn create_completed(&self, total: u64, address: [u8; 32]) -> u32 {
        let uid = self.create();
        self.complete(uid, total, address);
        uid
    }

    /// Mark an existing tag as fully synced. No-op if the uid is
    /// unknown (the client may have uploaded without first creating a
    /// tag, in which case `create_completed` is used instead).
    pub fn complete(&self, uid: u32, total: u64, address: [u8; 32]) {
        let mut inner = self.inner.lock().expect("tag registry poisoned");
        if let Some(tag) = inner.tags.get_mut(&uid) {
            tag.total = total;
            tag.split = total;
            tag.seen = total;
            tag.stored = total;
            tag.sent = total;
            tag.synced = total;
            tag.address = Some(address);
        }
    }

    /// Snapshot a tag for `GET /tags/{uid}`.
    pub fn get(&self, uid: u32) -> Option<TagView> {
        let inner = self.inner.lock().expect("tag registry poisoned");
        inner.tags.get(&uid).map(Tag::view)
    }
}

/// `POST /tags`. Creates a fresh (empty) tag and returns it bee-shaped
/// with status 201. bee-js calls this before a tracked upload; the
/// upload then references the returned uid via the `Swarm-Tag` header.
pub async fn create_tag(State(handle): State<GatewayHandle>) -> Response {
    let uid = handle.tags.create();
    match handle.tags.get(uid) {
        Some(view) => (StatusCode::CREATED, Json(view)).into_response(),
        // Unreachable: we just created it. Defensive only.
        None => json_error(StatusCode::INTERNAL_SERVER_ERROR, "tag vanished after create"),
    }
}

/// `GET /tags/{uid}`. Returns the tag's counters bee-shaped, or `404`
/// if the uid is unknown (bee returns 404 for a missing tag, which
/// bee-js surfaces as a clean error rather than a crash).
pub async fn get_tag(State(handle): State<GatewayHandle>, Path(uid): Path<u32>) -> Response {
    match handle.tags.get(uid) {
        Some(view) => Json(view).into_response(),
        None => json_error(StatusCode::NOT_FOUND, "tag not found"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_get_roundtrips() {
        let reg = TagRegistry::new();
        let uid = reg.create();
        assert_eq!(uid, 1);
        let view = reg.get(uid).expect("tag exists");
        assert_eq!(view.uid, 1);
        assert_eq!(view.split, 0);
        assert!(!view.started_at.is_empty());
    }

    #[test]
    fn completed_tag_reports_full_progress() {
        let reg = TagRegistry::new();
        let addr = [7u8; 32];
        let uid = reg.create_completed(42, addr);
        let view = reg.get(uid).expect("tag exists");
        assert_eq!(view.total, 42);
        assert_eq!(view.split, 42);
        assert_eq!(view.synced, 42);
        assert_eq!(view.sent, 42);
        assert_eq!(view.address, hex::encode(addr));
    }

    #[test]
    fn unknown_uid_is_none() {
        let reg = TagRegistry::new();
        assert!(reg.get(999).is_none());
    }
}
