//! GSOC/PSS lurker driver — the persistent receive loop.
//!
//! Given a [`WatchState`] (watched GSOC addresses, plus registered PSS
//! topics and secret) and a target neighborhood, the lurker keeps peers
//! resident near the target, pulls the neighborhood bin live from a peer
//! (see [`crate::pullsync`]), classifies each delivered chunk
//! ([`crate::messaging::classify`]), and forwards decoded messages to a
//! subscriber channel. It runs until the subscriber drops its receiver.
//!
//! Bandwidth shape:
//!
//! - **GSOC** is precise — we watch exact SOC addresses, so the `want`
//!   bitvector requests *only* those (delivery bandwidth ≈ 0 until an
//!   update lands).
//! - **PSS** cannot be identified from the offered address (the trojan
//!   address is mined, not derivable), so when PSS is enabled the lurker
//!   must download candidate CACs and attempt unwrap — exactly what a bee
//!   full node does when it `TryUnwrap`s every passing chunk. Callers
//!   that only need GSOC leave `pss_secret` unset and pay nothing.

use crate::messaging::{classify, DecodedMessage, WatchState};
use crate::pullsync::{self, OfferedChunk};
use crate::routing::{proximity, Overlay};
use ant_crypto::keccak256;
use libp2p::PeerId;
use libp2p_stream::Control;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// Cap on the dedup set; beyond it the **oldest** entries are evicted
/// (never the whole set — a wholesale clear would re-deliver everything
/// a busy bin re-offers). An evicted entry can at worst re-deliver one
/// old message.
const SEEN_CAP: usize = 8192;
/// Bound on a single blocking live-sync round. The server long-blocks on
/// an empty bin, so we cap each round to periodically re-dial toward the
/// neighborhood and re-pick a deeper covering peer.
const SYNC_ROUND_TIMEOUT: Duration = Duration::from_secs(20);
/// How long the driver waits after a connectivity change before topping
/// up coverage — coalesces a burst of peer churn into one pass.
const TOPUP_DEBOUNCE: Duration = Duration::from_millis(700);
/// Fallback tick: how often the driver re-dials and re-picks covering
/// peers even when connectivity is quiet.
const RE_RESIDE_INTERVAL: Duration = Duration::from_secs(30);
/// How far behind a peer's cursor a **fresh** (peer, bin) puller starts.
/// A light node takes seconds to dial the neighborhood's storers into
/// its connection set; a message that landed on a storer just before we
/// read its cursor would live below `cursor + 1` and be skipped forever.
/// Pulling a short backlog closes that window at the cost of bounded
/// replay near subscribe/handover time — the seen-set dedups across
/// peers, so delivery is at-least-once, not N-times. Replaced pullers
/// don't use this: they resume exactly where their predecessor stopped.
const PULL_BACKLOG: u64 = 8;
/// Number of closest connected peers to pull from concurrently. A
/// freshly-pushed chunk lands on the storer(s) nearest its address and
/// replicates outward; pulling several covering peers catches it
/// regardless of which one got (or replicated) it first — the difference
/// between reliable and flaky reception on a light node.
const COVERING_PEERS: usize = 5;
/// Extra deeper bins to pull past the base neighborhood bin. GSOC's watch
/// target *is* the chunk address, so its base bin is exact; PSS mines a
/// short overlay prefix, so the chunk can sit a few bins deeper on a given
/// peer — this window covers that without pulling the whole reserve.
const PSS_BIN_WINDOW: u8 = 3;
/// Highest proximity-order bin.
const MAX_BIN: u8 = 31;

/// A live lurker subscription: the target neighborhood and what to watch.
pub struct LurkerConfig {
    /// Overlay whose neighborhood we reside in and pull.
    pub target: [u8; 32],
    /// What to decode (GSOC addresses, PSS topics + secret).
    pub watch: WatchState,
}

/// Bounded delivered-chunk dedup, keyed by `(address, keccak(data))`.
///
/// The key must include the content: GSOC deliberately reuses one stable
/// SOC address for every update, so keying by address alone would drop
/// every update after the first — and would let an invalid first
/// delivery poison the address for the real one. Including the address
/// keeps distinct watched SOCs distinct even if two ever carried equal
/// bytes. Eviction is oldest-first, never wholesale.
struct Seen {
    set: HashSet<[u8; 64]>,
    order: VecDeque<[u8; 64]>,
}

impl Seen {
    fn new() -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Record `(address, content)`; `false` if this exact version was
    /// already seen (another covering peer usually delivers it too).
    fn insert(&mut self, address: &[u8; 32], data: &[u8]) -> bool {
        let mut key = [0u8; 64];
        key[..32].copy_from_slice(address);
        key[32..].copy_from_slice(&keccak256(data));
        if !self.set.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > SEEN_CAP {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}

/// Run the lurker until `out` is closed (subscriber gone) or the peer
/// source ends. Emits every decoded GSOC/PSS message for the watch set.
///
/// `neighborhood_dial`, when present, is pinged each round with the
/// target so the swarm keeps peers resident in that neighborhood (the
/// dial primitive from the retrieval path). `peers` is the live
/// connected-peer snapshot the swarm publishes.
pub async fn run(
    control: Control,
    mut peers: watch::Receiver<Vec<(PeerId, Overlay)>>,
    neighborhood_dial: Option<mpsc::Sender<[u8; 32]>>,
    config: LurkerConfig,
    out: mpsc::Sender<DecodedMessage>,
) {
    let LurkerConfig { target, watch } = config;
    if watch.is_empty() {
        return;
    }
    let watch = Arc::new(watch);
    let seen: Arc<Mutex<Seen>> = Arc::new(Mutex::new(Seen::new()));
    // Last position each (peer, bin) puller synced to, shared with the
    // pullers. A replacement puller resumes where its predecessor
    // stopped instead of jumping to the peer's *current* cursor — which
    // would silently skip everything that arrived during the outage.
    let positions: Arc<Mutex<HashMap<(PeerId, u8), u64>>> = Arc::new(Mutex::new(HashMap::new()));
    // Deeper bins only matter for PSS (unknown address); GSOC's base bin
    // is exact.
    let window = if watch.pss_topics.is_empty() {
        0
    } else {
        PSS_BIN_WINDOW
    };

    // One long-lived puller per (peer, bin). Pullers run **continuously** —
    // we top up coverage for newly-connected peers on each tick but never
    // abort a live puller (except in the coordinated handover below, and
    // only once its replacement is live), so a message is never dropped
    // in a gap while the driver re-dials (an earlier design
    // aborted-and-restarted every tick and lost messages that arrived
    // during the re-reside window).
    let mut active: HashMap<(PeerId, u8), tokio::task::JoinHandle<()>> = HashMap::new();

    loop {
        if out.is_closed() {
            break;
        }
        // Keep dialing toward the target every pass. Pullers start
        // immediately on whatever peers are already connected (no
        // blocking reside phase — the first pullers matter for messages
        // arriving *now*), and coverage deepens as closer peers connect:
        // each connectivity change re-runs this top-up within
        // `TOPUP_DEBOUNCE`.
        if let Some(dial) = &neighborhood_dial {
            let _ = dial.try_send(target);
        }
        // Drop handles for pullers that ended (peer dropped / stream died).
        active.retain(|_, h| !h.is_finished());

        // Ensure a puller for each covering (peer, bin); add only the
        // missing ones so existing pullers keep running uninterrupted.
        let mut desired: HashSet<(PeerId, u8)> = HashSet::new();
        for (peer_id, peer_overlay) in closest_n(&mut peers, &target, COVERING_PEERS) {
            let base_bin = proximity(&peer_overlay, &target);
            let top_bin = base_bin.saturating_add(window).min(MAX_BIN);
            for bin in base_bin..=top_bin {
                desired.insert((peer_id, bin));
            }
            if (base_bin..=top_bin).all(|b| active.contains_key(&(peer_id, b))) {
                continue; // already fully covered
            }
            let Ok(cursors) = pullsync::get_cursors(&mut control.clone(), peer_id).await else {
                continue;
            };
            for bin in base_bin..=top_bin {
                if active.contains_key(&(peer_id, bin)) {
                    continue;
                }
                // Resume a replaced puller exactly where it stopped; a
                // fresh (peer, bin) starts a short backlog behind the
                // cursor (see PULL_BACKLOG).
                let resume = positions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&(peer_id, bin))
                    .copied();
                let start = resume.unwrap_or_else(|| {
                    cursors
                        .cursors
                        .get(bin as usize)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(1)
                        .saturating_sub(PULL_BACKLOG)
                        .max(1)
                });
                tracing::info!(
                    target: "ant_p2p::lurker",
                    peer = %peer_id, bin, start, resumed = resume.is_some(),
                    "lurker pulling neighborhood bin",
                );
                let handle = tokio::spawn(pull_bin(
                    control.clone(),
                    peer_id,
                    bin,
                    start,
                    Arc::clone(&watch),
                    Arc::clone(&seen),
                    Arc::clone(&positions),
                    out.clone(),
                ));
                active.insert((peer_id, bin), handle);
            }
        }

        // Coordinated handover: a peer that fell out of the covering set
        // keeps its pullers until every desired (peer, bin) is live, then
        // they're retired — churn never gaps coverage, but the puller set
        // can't grow without bound as closest-peers turn over either.
        if !desired.is_empty() && desired.iter().all(|k| active.contains_key(k)) {
            active.retain(|k, h| {
                if desired.contains(k) {
                    true
                } else {
                    h.abort();
                    false
                }
            });
        }

        // Wait for connectivity to change (top up new closer peers right
        // away) or the fallback tick. The pullers keep running
        // throughout — no gap.
        tokio::select! {
            () = out.closed() => break,
            changed = peers.changed() => {
                if changed.is_err() {
                    break; // peer source gone: node shutting down
                }
                tokio::time::sleep(TOPUP_DEBOUNCE).await;
            }
            () = tokio::time::sleep(RE_RESIDE_INTERVAL) => {}
        }
    }
    for (_, h) in active {
        h.abort();
    }
}

/// Live-pull one bin from one peer, forwarding decoded messages. Runs
/// until the peer's stream errors (peer likely gone → the driver drops
/// this puller) or the subscriber leaves. A round timeout is *not* fatal:
/// the server long-blocks on a quiet bin, so a timeout just means "no new
/// chunk yet" and we re-open at the same `start`.
#[allow(clippy::too_many_arguments)]
async fn pull_bin(
    mut control: Control,
    peer_id: PeerId,
    bin: u8,
    mut start: u64,
    watch: Arc<WatchState>,
    seen: Arc<Mutex<Seen>>,
    positions: Arc<Mutex<HashMap<(PeerId, u8), u64>>>,
    out: mpsc::Sender<DecodedMessage>,
) {
    loop {
        if out.is_closed() {
            return;
        }
        let round = pullsync::sync_once(&mut control, peer_id, bin, start, |o: &OfferedChunk| {
            want(o, &watch)
        });
        let page = match tokio::time::timeout(SYNC_ROUND_TIMEOUT, round).await {
            Ok(Ok(page)) => page,
            Ok(Err(_)) => return, // stream/peer error → drop this puller
            Err(_) => continue,   // long-block timeout → re-open at same start
        };
        for chunk in &page.chunks {
            // Dedup one *content version* — the same chunk arrives from
            // several covering peers, but a GSOC update reuses its
            // address with new content and must still go through.
            if !seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(&chunk.address, &chunk.data)
            {
                continue;
            }
            if let Some(msg) = classify(&chunk.address, &chunk.data, &watch) {
                tracing::info!(
                    target: "ant_p2p::lurker",
                    peer = %peer_id, bin, "lurker decoded a message",
                );
                if out.send(msg).await.is_err() {
                    return;
                }
            }
        }
        start = page.topmost.saturating_add(1);
        // Publish how far this (peer, bin) got so a replacement puller
        // resumes here rather than skipping the gap.
        positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((peer_id, bin), start);
    }
}

/// The `n` connected peers whose overlays are closest to `target`,
/// deepest first. Marks the snapshot seen (`borrow_and_update`) so the
/// driver's `changed()` wait really waits for the *next* change.
fn closest_n(
    peers: &mut watch::Receiver<Vec<(PeerId, Overlay)>>,
    target: &[u8; 32],
    n: usize,
) -> Vec<(PeerId, Overlay)> {
    let mut v: Vec<(PeerId, Overlay)> = peers.borrow_and_update().clone();
    // Deepest proximity first (descending).
    v.sort_by_key(|(_, ov)| std::cmp::Reverse(proximity(ov, target)));
    v.truncate(n);
    v
}

/// Decide whether to request a chunk's delivery. GSOC is precise (exact
/// watched address); PSS needs the chunk body to attempt unwrap, so any
/// chunk is a candidate when PSS is enabled.
fn want(offered: &OfferedChunk, watch: &WatchState) -> bool {
    if watch.gsoc_addresses.contains(&offered.address) {
        return true;
    }
    // PSS: the trojan address isn't derivable, so any chunk is a candidate
    // once a topic is registered (topic-broadcast needs no node secret).
    !watch.pss_topics.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overlay(first: u8) -> Overlay {
        let mut o = [0u8; 32];
        o[0] = first;
        o
    }

    #[test]
    fn closest_n_orders_deepest_first_and_marks_seen() {
        let target = overlay(0xff);
        let (tx, mut rx) = watch::channel(vec![
            (PeerId::random(), overlay(0x00)),
            (PeerId::random(), overlay(0xf0)), // shares top nibble → closest
            (PeerId::random(), overlay(0x80)),
        ]);
        let expect_closest = rx.borrow()[1].0;
        let picked = closest_n(&mut rx, &target, 2);
        assert_eq!(picked.len(), 2);
        assert_eq!(picked[0].1, overlay(0xf0));
        assert_eq!(picked[0].0, expect_closest);
        assert_eq!(picked[1].1, overlay(0x80));
        // borrow_and_update marked the snapshot seen.
        assert!(!rx.has_changed().unwrap());
        drop(tx);
    }

    #[test]
    fn seen_passes_gsoc_updates_and_dedups_exact_versions() {
        let mut seen = Seen::new();
        let addr = [0xaau8; 32];
        // First GSOC update at the stable address.
        assert!(seen.insert(&addr, b"update-1"));
        // The same version re-delivered by another covering peer: deduped.
        assert!(!seen.insert(&addr, b"update-1"));
        // A NEW update reusing the same address must pass — this is the
        // whole point of keying on (address, content), not address alone.
        assert!(seen.insert(&addr, b"update-2"));
        // An invalid/spoofed delivery must not poison the address for a
        // later legitimate version.
        assert!(seen.insert(&addr, b"garbage"));
        assert!(seen.insert(&addr, b"update-3"));
    }

    #[test]
    fn seen_evicts_oldest_first_not_wholesale() {
        let mut seen = Seen::new();
        let addr_for = |i: usize| {
            let mut a = [0u8; 32];
            a[..8].copy_from_slice(&(i as u64).to_le_bytes());
            a
        };
        for i in 0..=SEEN_CAP {
            assert!(seen.insert(&addr_for(i), b"x"));
        }
        // Only the single oldest entry was evicted; a recent one is
        // still deduped (a wholesale clear would forget it).
        assert!(!seen.insert(&addr_for(SEEN_CAP), b"x"));
        assert!(!seen.insert(&addr_for(1), b"x"));
        assert!(seen.insert(&addr_for(0), b"x"), "oldest was evicted");
        assert_eq!(seen.order.len(), seen.set.len());
        assert!(seen.set.len() <= SEEN_CAP + 1);
    }

    #[test]
    fn want_is_precise_for_gsoc_and_broad_for_pss() {
        let addr = [0x11u8; 32];
        let gsoc_only = WatchState {
            gsoc_addresses: HashSet::from([addr]),
            ..Default::default()
        };
        let offered = OfferedChunk {
            address: addr,
            batch_id: [0u8; 32],
            stamp_hash: [0u8; 32],
        };
        let other = OfferedChunk {
            address: [0x22u8; 32],
            batch_id: [0u8; 32],
            stamp_hash: [0u8; 32],
        };
        // GSOC-only: want the watched address, ignore others.
        assert!(want(&offered, &gsoc_only));
        assert!(!want(&other, &gsoc_only));

        // PSS enabled (even broadcast, no secret): any chunk is a
        // candidate — must download to attempt unwrap.
        let with_pss = WatchState {
            pss_topics: vec![[1u8; 32]],
            pss_secret: None,
            ..Default::default()
        };
        assert!(want(&other, &with_pss));
    }
}
