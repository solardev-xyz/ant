//! Per-peer pushsync load tracking: concurrent-push caps + latency EWMA.
//!
//! Perf-lab Experiment 2 (hoverfly's biggest measured lever, 2.6× on
//! their stack). Mechanism: bee's per-peer accounting credits refresh
//! at a fixed PLUR/s rate; stacking N concurrent pushes on the same
//! closest peer overdraws its ghost balance and every push beyond the
//! refresh budget is answered with overdraft/RST — throughput then
//! degrades to refresh-rate-per-saturated-peer. Capping concurrent
//! pushes per peer forces the dispatcher to fan out to lower-PO peers
//! instead, spreading debt across many refresh budgets.
//!
//! The cap is latency-aware (hoverfly measured a uniform raised cap
//! REGRESSING vs the latency-split one, yamux contention on slow
//! peers): fast peers (EWMA < 200 ms) get `2×base`, slow peers
//! (EWMA ≥ 2 s) get `base/2` (min 1), fresh/medium peers get `base`.
//!
//! Enabled via `ANT_PUSH_INFLIGHT_CAP=<base>` (0/unset = disabled) so
//! a single build serves both arms of the A/B benchmark.

use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// EWMA smoothing factor (weight of the newest sample).
const EWMA_ALPHA: f64 = 0.3;
/// Latency below which a peer counts as fast (cap ×2).
const FAST_THRESHOLD: Duration = Duration::from_millis(200);
/// Latency at/above which a peer counts as slow (cap ÷2).
const SLOW_THRESHOLD: Duration = Duration::from_secs(2);
/// Fold-cap for outcome latencies: a 45 s timeout should mark a peer
/// slow, not poison the EWMA so badly it never recovers.
const LATENCY_FOLD_CAP: Duration = Duration::from_secs(10);

#[derive(Default)]
struct PeerLoad {
    inflight: u32,
    /// Microseconds; 0 = no observation yet.
    ewma_micros: u64,
}

/// Process-wide per-peer pushsync load book. One instance lives in the
/// swarm state and is shared by every `RoutingFetcher` the daemon
/// builds (the fetchers themselves are per-request).
pub struct PushLoadTracker {
    base_cap: u32,
    peers: Mutex<HashMap<PeerId, PeerLoad>>,
    /// Times the soft fall-through re-admitted an at-cap peer because
    /// every ranked candidate was saturated (diagnostic; a high value
    /// means the pool is too small for the concurrency).
    saturated_fallthroughs: AtomicU64,
}

impl PushLoadTracker {
    #[must_use]
    pub fn new(base_cap: u32) -> Self {
        Self {
            base_cap: base_cap.max(1),
            peers: Mutex::new(HashMap::new()),
            saturated_fallthroughs: AtomicU64::new(0),
        }
    }

    /// Read `ANT_PUSH_INFLIGHT_CAP`; `None` when unset/0/unparsable
    /// (feature disabled — the baseline arm).
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("ANT_PUSH_INFLIGHT_CAP").ok()?;
        let base: u32 = raw.trim().parse().ok()?;
        (base > 0).then(|| Self::new(base))
    }

    /// Latency-aware cap for `peer` right now.
    fn cap_for(&self, load: &PeerLoad) -> u32 {
        if load.ewma_micros == 0 {
            return self.base_cap;
        }
        let ewma = Duration::from_micros(load.ewma_micros);
        if ewma < FAST_THRESHOLD {
            self.base_cap * 2
        } else if ewma >= SLOW_THRESHOLD {
            (self.base_cap / 2).max(1)
        } else {
            self.base_cap
        }
    }

    /// Is `peer` at or over its current cap?
    #[must_use]
    pub fn at_cap(&self, peer: &PeerId) -> bool {
        let peers = self.peers.lock().expect("push load mutex");
        peers
            .get(peer)
            .is_some_and(|l| l.inflight >= self.cap_for(l))
    }

    /// Record a dispatch to `peer`.
    pub fn begin(&self, peer: PeerId) {
        let mut peers = self.peers.lock().expect("push load mutex");
        peers.entry(peer).or_default().inflight += 1;
    }

    /// Record completion of a push to `peer` (any outcome). `latency`
    /// is folded into the EWMA, capped at [`LATENCY_FOLD_CAP`] so
    /// timeouts mark the peer slow without permanently poisoning it.
    pub fn end(&self, peer: &PeerId, latency: Duration) {
        let mut peers = self.peers.lock().expect("push load mutex");
        if let Some(l) = peers.get_mut(peer) {
            l.inflight = l.inflight.saturating_sub(1);
            let sample = latency.min(LATENCY_FOLD_CAP).as_micros() as u64;
            l.ewma_micros = if l.ewma_micros == 0 {
                sample
            } else {
                let e = l.ewma_micros as f64;
                (e + EWMA_ALPHA * (sample as f64 - e)) as u64
            };
        }
    }

    /// Note that the ranking had to re-admit at-cap peers (all ranked
    /// candidates saturated).
    pub fn note_saturated_fallthrough(&self) {
        self.saturated_fallthroughs.fetch_add(1, Ordering::Relaxed);
    }

    /// Diagnostic counter for logs / experiment writeups.
    #[must_use]
    pub fn saturated_fallthroughs(&self) -> u64 {
        self.saturated_fallthroughs.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_scale_with_latency() {
        let t = PushLoadTracker::new(4);
        let peer = PeerId::random();
        // Fresh peer: base cap.
        for _ in 0..4 {
            assert!(!t.at_cap(&peer));
            t.begin(peer);
        }
        assert!(t.at_cap(&peer));
        // Fast completions: cap doubles.
        for _ in 0..4 {
            t.end(&peer, Duration::from_millis(50));
        }
        for _ in 0..8 {
            assert!(!t.at_cap(&peer), "fast peer should take 2x base");
            t.begin(peer);
        }
        assert!(t.at_cap(&peer));
        for _ in 0..8 {
            t.end(&peer, Duration::from_secs(5));
        }
        // Slow completions dragged the EWMA over 2 s: cap halves.
        t.begin(peer);
        t.begin(peer);
        assert!(t.at_cap(&peer), "slow peer should cap at base/2");
    }

    #[test]
    fn timeout_fold_is_capped() {
        let t = PushLoadTracker::new(4);
        let peer = PeerId::random();
        t.begin(peer);
        t.end(&peer, Duration::from_secs(45));
        let peers = t.peers.lock().unwrap();
        assert_eq!(
            peers.get(&peer).unwrap().ewma_micros,
            LATENCY_FOLD_CAP.as_micros() as u64
        );
    }
}
