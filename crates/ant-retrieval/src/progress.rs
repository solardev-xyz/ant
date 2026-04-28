//! Per-request progress accounting for `Get*` retrievals.
//!
//! [`ProgressTracker`] is a small bag of atomics + a peer set that
//! [`crate::RoutingFetcher`] writes to on every successful chunk fetch
//! (network or cache hit). The daemon spawns a periodic emitter that
//! reads [`ProgressTracker::snapshot`] and ships the resulting
//! [`ProgressSample`] over the control socket as a streaming
//! `Response::Progress` line; the snapshot is also derived once per
//! request from the data root's span (file size) so the client can
//! render `chunks_done / chunks_total` and `bytes_done / bytes_total`
//! gauges without polling the joiner directly.
//!
//! Hot path is "increment on every chunk delivery", so per-counter
//! state is `AtomicU64::Relaxed` — we never order writes against
//! anything else and the consumer only ever reads a coarse snapshot.
//! The unique-peer set has its own `Mutex` (writers only) so the
//! "did this come from a peer or the cache?" attribution stays
//! correct even when sibling fetches finish in arbitrary orders.

use libp2p::PeerId;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// One read of a [`ProgressTracker`]. All counters are cumulative
/// since `ProgressTracker::new` was called. `total_*_estimate` are
/// `0` for "not yet known" — the daemon doesn't learn the file size
/// until it has fetched the data root chunk and inspected its span.
#[derive(Debug, Clone, Default)]
pub struct ProgressSample {
    pub chunks_done: u64,
    pub total_chunks_estimate: u64,
    pub bytes_done: u64,
    pub total_bytes_estimate: u64,
    pub cache_hits: u64,
    pub peers_used: u32,
    pub bypass_cache: bool,
}

/// Cheap, lock-free-on-the-hot-path counter struct shared between the
/// fetcher (writer) and the progress emitter (reader). One instance
/// per `Get*` request, wrapped in `Arc` so both sides hold a clone.
pub struct ProgressTracker {
    chunks_done: AtomicU64,
    bytes_done: AtomicU64,
    cache_hits: AtomicU64,
    /// File-body size from the data root's span (cached as an atomic
    /// for the same reason as the rest: we don't need to lock to
    /// publish "we just learned the total"). `0` until set.
    total_bytes: AtomicU64,
    /// Distinct peers we've actually downloaded chunks from. Set
    /// rather than `Atomic*` because we need cardinality, not a
    /// running count of insertions. The lock is held only across
    /// `HashSet::insert` and never across an `.await`, so there's no
    /// contention with the joiner's per-chunk fan-out.
    peers: Mutex<HashSet<PeerId>>,
    bypass_cache: bool,
}

impl ProgressTracker {
    /// Build a fresh tracker. `bypass_cache` is just plumbed through
    /// to the snapshot for client-side rendering; the tracker doesn't
    /// itself care whether the cache is bypassed.
    pub fn new(bypass_cache: bool) -> Self {
        Self {
            chunks_done: AtomicU64::new(0),
            bytes_done: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            peers: Mutex::new(HashSet::new()),
            bypass_cache,
        }
    }

    /// Record one delivered chunk. `peer = Some(p)` for a successful
    /// network fetch (counted toward the unique-peer set);
    /// `peer = None` for a cache hit. `bytes` is the wire size
    /// (`span (8) || payload`), matching what
    /// `RoutingFetcher::fetch` returns to the joiner.
    pub fn record_chunk(&self, peer: Option<PeerId>, bytes: u64) {
        self.chunks_done.fetch_add(1, Ordering::Relaxed);
        self.bytes_done.fetch_add(bytes, Ordering::Relaxed);
        match peer {
            Some(p) => {
                self.peers
                    .lock()
                    .expect("progress mutex poisoned")
                    .insert(p);
            }
            None => {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Publish the file-body byte count once the daemon has fetched
    /// the data root chunk. Idempotent — the daemon may call it more
    /// than once on the manifest path (manifest root span first,
    /// data root span second) and we keep the latest value.
    pub fn set_total_bytes(&self, total: u64) {
        self.total_bytes.store(total, Ordering::Relaxed);
    }

    /// Cumulative read for the emitter. Cheap; safe to call as often
    /// as the emitter ticks (typically every 150 ms).
    pub fn snapshot(&self) -> ProgressSample {
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        ProgressSample {
            chunks_done: self.chunks_done.load(Ordering::Relaxed),
            total_chunks_estimate: estimate_total_chunks(total_bytes),
            bytes_done: self.bytes_done.load(Ordering::Relaxed),
            total_bytes_estimate: total_bytes,
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            peers_used: self.peers.lock().expect("progress mutex poisoned").len() as u32,
            bypass_cache: self.bypass_cache,
        }
    }
}

/// Estimate how many chunks a balanced k-ary tree (branching factor
/// 128, leaf size 4096) needs to encode `span_bytes` of file data.
/// Returns `0` when the span is unknown so the consumer can render
/// "?" instead of a misleading `1`. Mirrors the layout
/// [`crate::joiner`] reconstructs against, so the estimate matches
/// the actual fetch count for sane inputs.
pub fn estimate_total_chunks(span_bytes: u64) -> u64 {
    if span_bytes == 0 {
        return 0;
    }
    const LEAF_SIZE: u64 = 4096;
    const BRANCHING: u64 = 128;
    let leaves = span_bytes.div_ceil(LEAF_SIZE).max(1);
    if leaves == 1 {
        return 1;
    }
    let mut total = leaves;
    let mut level = leaves;
    while level > 1 {
        level = level.div_ceil(BRANCHING);
        total += level;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_total_chunks_small_files() {
        assert_eq!(estimate_total_chunks(0), 0, "unknown file size yields 0");
        assert_eq!(estimate_total_chunks(1), 1, "1 byte fits in a single leaf");
        assert_eq!(estimate_total_chunks(4096), 1, "exactly one chunk");
        // 4097 bytes = 2 leaves + 1 root.
        assert_eq!(estimate_total_chunks(4097), 3);
        // 128 leaves + 1 root.
        assert_eq!(estimate_total_chunks(128 * 4096), 129);
        // 129 leaves require an extra intermediate level.
        assert_eq!(
            estimate_total_chunks(129 * 4096),
            // 129 leaves + 2 intermediates (covering 128 + 1) + 1 root.
            129 + 2 + 1
        );
    }

    #[test]
    fn record_chunk_distinguishes_cache_and_network() {
        let t = ProgressTracker::new(false);
        let p1 = PeerId::random();
        let p2 = PeerId::random();
        t.record_chunk(Some(p1), 4104);
        t.record_chunk(None, 4104); // cache hit
        t.record_chunk(Some(p1), 4104); // same peer, no new uniques
        t.record_chunk(Some(p2), 4104);

        let snap = t.snapshot();
        assert_eq!(snap.chunks_done, 4);
        assert_eq!(snap.bytes_done, 4 * 4104);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.peers_used, 2);
        assert_eq!(snap.total_bytes_estimate, 0, "not yet known");
        assert_eq!(snap.total_chunks_estimate, 0);
        assert!(!snap.bypass_cache);
    }

    #[test]
    fn set_total_bytes_drives_estimate() {
        let t = ProgressTracker::new(true);
        t.set_total_bytes(4097);
        let snap = t.snapshot();
        assert_eq!(snap.total_bytes_estimate, 4097);
        assert_eq!(snap.total_chunks_estimate, 3);
        assert!(snap.bypass_cache);
    }
}
