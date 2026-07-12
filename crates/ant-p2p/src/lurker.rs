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
use libp2p::PeerId;
use libp2p_stream::Control;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// Cap on the dedup set before it's cleared (bounds memory on a busy
/// bin; a cleared entry can at worst re-deliver one message).
const SEEN_CAP: usize = 8192;
/// Bound on a single blocking live-sync round. The server long-blocks on
/// an empty bin, so we cap each round to periodically re-dial toward the
/// neighborhood and re-pick a deeper covering peer.
const SYNC_ROUND_TIMEOUT: Duration = Duration::from_secs(20);
/// Time budget spent dialing toward the target neighborhood and waiting
/// for deeper covering peers to connect before each pull. A light node
/// isn't resident in an arbitrary neighborhood, so — like the retrieval
/// path's dial-and-await-deeper — we spend real time pulling the actual
/// storers (which sit at the network storage radius, ~bin 11–14) into our
/// connection set before pulling.
const RESIDE_BUDGET: Duration = Duration::from_secs(15);
/// One reside wait step.
const RESIDE_STEP: Duration = Duration::from_millis(700);
/// How often the driver re-dials, re-picks covering peers, and restarts
/// its pull tasks so it tracks peer churn.
const RE_RESIDE_INTERVAL: Duration = Duration::from_secs(30);
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
    let seen: Arc<Mutex<HashSet<[u8; 32]>>> = Arc::new(Mutex::new(HashSet::new()));
    // Deeper bins only matter for PSS (unknown address); GSOC's base bin
    // is exact.
    let window = if watch.pss_topics.is_empty() {
        0
    } else {
        PSS_BIN_WINDOW
    };

    // One long-lived puller per (peer, bin). Pullers run **continuously** —
    // we top up coverage for newly-connected peers on each tick but never
    // abort a live puller, so a message is never dropped in a gap while the
    // driver re-dials (an earlier design aborted-and-restarted every tick
    // and lost messages that arrived during the re-reside window).
    let mut active: std::collections::HashMap<(PeerId, u8), tokio::task::JoinHandle<()>> =
        std::collections::HashMap::new();

    // Dial deep into the neighborhood once up front so the first pullers
    // hit real storers (a light node isn't resident by default).
    reside(&mut peers, neighborhood_dial.as_ref(), &out, &target).await;

    loop {
        if out.is_closed() {
            break;
        }
        // Drop handles for pullers that ended (peer dropped / stream died).
        active.retain(|_, h| !h.is_finished());

        // Ensure a puller for each covering (peer, bin); add only the
        // missing ones so existing pullers keep running uninterrupted.
        for (peer_id, peer_overlay) in closest_n(&peers, &target, COVERING_PEERS) {
            let base_bin = proximity(&peer_overlay, &target);
            let top_bin = base_bin.saturating_add(window).min(MAX_BIN);
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
                let start = cursors
                    .cursors
                    .get(bin as usize)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(1);
                tracing::info!(
                    target: "ant_p2p::lurker",
                    peer = %peer_id, bin, start, "lurker pulling neighborhood bin",
                );
                let handle = tokio::spawn(pull_bin(
                    control.clone(),
                    peer_id,
                    bin,
                    start,
                    Arc::clone(&watch),
                    Arc::clone(&seen),
                    out.clone(),
                ));
                active.insert((peer_id, bin), handle);
            }
        }

        // Hold residency and wait before the next top-up. The pullers keep
        // running throughout — no gap.
        tokio::select! {
            () = out.closed() => break,
            () = tokio::time::sleep(RE_RESIDE_INTERVAL) => {}
        }
        if let Some(dial) = &neighborhood_dial {
            let _ = dial.try_send(target);
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
async fn pull_bin(
    mut control: Control,
    peer_id: PeerId,
    bin: u8,
    mut start: u64,
    watch: Arc<WatchState>,
    seen: Arc<Mutex<HashSet<[u8; 32]>>>,
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
            {
                let mut s = seen
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !s.insert(chunk.address) {
                    continue;
                }
                if s.len() > SEEN_CAP {
                    s.clear();
                    s.insert(chunk.address);
                }
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
    }
}

/// The `n` connected peers whose overlays are closest to `target`,
/// deepest first.
fn closest_n(
    peers: &watch::Receiver<Vec<(PeerId, Overlay)>>,
    target: &[u8; 32],
    n: usize,
) -> Vec<(PeerId, Overlay)> {
    let mut v: Vec<(PeerId, Overlay)> = peers.borrow().clone();
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

/// Dial toward `target` for the full [`RESIDE_BUDGET`], pulling the
/// neighborhood's actual storers into our connection set before we pull.
/// A light node isn't resident in an arbitrary neighborhood, and the
/// storers sit at the network storage radius — so, like the retrieval
/// path, we keep re-issuing the neighborhood dial the whole time rather
/// than stopping at the first plateau (deep peers can take several
/// seconds and multiple dial rounds to connect). Returns early only if
/// we hit the deepest possible bin or the subscriber leaves.
async fn reside(
    peers: &mut watch::Receiver<Vec<(PeerId, Overlay)>>,
    neighborhood_dial: Option<&mpsc::Sender<[u8; 32]>>,
    out: &mpsc::Sender<DecodedMessage>,
    target: &[u8; 32],
) {
    let start = tokio::time::Instant::now();
    while start.elapsed() < RESIDE_BUDGET && !out.is_closed() {
        if let Some(dial) = neighborhood_dial {
            let _ = dial.try_send(*target);
        }
        if current_proximity(peers, target) >= MAX_BIN {
            return; // can't get any deeper
        }
        tokio::select! {
            () = out.closed() => return,
            r = peers.changed() => if r.is_err() { return; },
            () = tokio::time::sleep(RESIDE_STEP) => {}
        }
    }
}

/// Highest proximity to `target` among connected peers (0 if none).
fn current_proximity(peers: &watch::Receiver<Vec<(PeerId, Overlay)>>, target: &[u8; 32]) -> u8 {
    closest_connected(peers, target).map_or(0, |(_, ov)| proximity(&ov, target))
}

/// The connected peer whose overlay is closest to `target`.
fn closest_connected(
    peers: &watch::Receiver<Vec<(PeerId, Overlay)>>,
    target: &[u8; 32],
) -> Option<(PeerId, Overlay)> {
    peers
        .borrow()
        .iter()
        .min_by(|(_, a), (_, b)| {
            // Larger proximity to target = closer.
            proximity(b, target).cmp(&proximity(a, target))
        })
        .copied()
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
    fn closest_connected_picks_highest_proximity() {
        let target = overlay(0xff);
        let (tx, rx) = watch::channel(vec![
            (PeerId::random(), overlay(0x00)),
            (PeerId::random(), overlay(0xf0)), // shares top nibble → closest
            (PeerId::random(), overlay(0x80)),
        ]);
        let (pid, ov) = closest_connected(&rx, &target).unwrap();
        assert_eq!(ov, overlay(0xf0));
        // and the peer id matches the 0xf0 entry
        assert_eq!(pid, rx.borrow()[1].0);
        drop(tx);
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
