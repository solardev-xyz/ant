//! Process-wide retrieval counters.
//!
//! [`RetrievalCounters`] is a tiny `AtomicU64` bag the daemon clones
//! into every [`crate::RoutingFetcher`] it builds. Each successful
//! chunk delivery (network or cache) bumps the counters once, and the
//! status publisher reads cumulative totals from them when populating
//! [`StatusSnapshot::retrieval`] for `antctl top`.
//!
//! Only the *winning* fetch is counted toward `bytes_fetched_total` —
//! losing hedge dispatches that the drain task pulls to completion
//! consume real bytes on the wire, but counting them would inflate
//! the user-facing bandwidth gauge with bytes nobody asked for. The
//! winner-only accounting keeps the gauge aligned with the work the
//! daemon actually delivered.
//!
//! Cumulative-counter design (rather than a windowed average kept in
//! the daemon) is deliberate: the consumer (`antctl top`) already polls
//! at a known cadence, so it can derive instantaneous throughput by
//! diff'ing two consecutive snapshots and apply whatever smoothing /
//! peak-tracking it wants without burning daemon CPU on a sliding
//! window we'd then have to wire through the wire format.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative chunk / byte counters shared across every fetcher in
/// one daemon process.
#[derive(Debug, Default)]
pub struct RetrievalCounters {
    chunks_fetched: AtomicU64,
    bytes_fetched: AtomicU64,
    cache_hits: AtomicU64,
}

/// Plain-old-data snapshot of [`RetrievalCounters`]. Cheaper to
/// thread into `StatusSnapshot` than locking each atomic individually
/// at the read site.
#[derive(Debug, Clone, Copy, Default)]
pub struct RetrievalCountersSnapshot {
    pub chunks_fetched: u64,
    pub bytes_fetched: u64,
    pub cache_hits: u64,
}

impl RetrievalCounters {
    /// Build a fresh zero-initialised counter set. The daemon constructs
    /// exactly one of these and clones the `Arc` into every fetcher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one chunk delivered to the joiner. `bytes` is the wire
    /// size (`span (8) || payload`); `from_cache = true` means the
    /// chunk came out of the in-memory cache rather than from the
    /// network. Bytes are still counted for cache hits — the caller
    /// can subtract `cache_hits * avg_size` if they need network-only
    /// throughput.
    pub fn record_chunk(&self, bytes: u64, from_cache: bool) {
        self.chunks_fetched.fetch_add(1, Ordering::Relaxed);
        self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
        if from_cache {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read all counters in one shot. Called by the status publisher
    /// on its periodic tick.
    pub fn snapshot(&self) -> RetrievalCountersSnapshot {
        RetrievalCountersSnapshot {
            chunks_fetched: self.chunks_fetched.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_chunk_distinguishes_cache_and_network() {
        let c = RetrievalCounters::new();
        c.record_chunk(4104, false);
        c.record_chunk(4104, true);
        c.record_chunk(4104, false);
        let s = c.snapshot();
        assert_eq!(s.chunks_fetched, 3);
        assert_eq!(s.bytes_fetched, 3 * 4104);
        assert_eq!(s.cache_hits, 1);
    }
}
