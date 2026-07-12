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
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// How long to wait for peers/cursors before retrying when the
/// neighborhood is momentarily uncovered.
const RETRY_BACKOFF: Duration = Duration::from_millis(1500);
/// Cap on the dedup set before it's cleared (bounds memory on a busy
/// bin; a cleared entry can at worst re-deliver one message).
const SEEN_CAP: usize = 8192;

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
    mut control: Control,
    mut peers: watch::Receiver<Vec<(PeerId, Overlay)>>,
    neighborhood_dial: Option<mpsc::Sender<[u8; 32]>>,
    config: LurkerConfig,
    out: mpsc::Sender<DecodedMessage>,
) {
    let LurkerConfig { target, watch } = config;
    if watch.is_empty() {
        return;
    }
    let mut seen: HashSet<[u8; 32]> = HashSet::new();

    loop {
        if out.is_closed() {
            return;
        }
        // Keep peers resident near the target neighborhood.
        if let Some(dial) = &neighborhood_dial {
            let _ = dial.try_send(target);
        }

        let Some((peer_id, peer_overlay)) = closest_connected(&peers, &target) else {
            // No covering peer yet; wait for the peer set to change.
            if wait_or_closed(&mut peers, &out).await {
                return;
            }
            continue;
        };
        let bin = proximity(&peer_overlay, &target);

        // Cursor for this bin → start live from the next binID.
        let mut start = if let Ok(c) = pullsync::get_cursors(&mut control, peer_id).await {
            c.cursors
                .get(bin as usize)
                .copied()
                .unwrap_or(0)
                .saturating_add(1)
        } else {
            tokio::time::sleep(RETRY_BACKOFF).await;
            continue;
        };

        // Pull this peer's bin until it drops or errors, then re-pick.
        loop {
            if out.is_closed() {
                return;
            }
            // A peer/stream error breaks out to re-pick a covering peer.
            let Ok(page) =
                pullsync::sync_once(&mut control, peer_id, bin, start, |o: &OfferedChunk| {
                    want(o, &watch)
                })
                .await
            else {
                break;
            };
            for chunk in &page.chunks {
                if !seen.insert(chunk.address) {
                    continue;
                }
                if seen.len() > SEEN_CAP {
                    seen.clear();
                    seen.insert(chunk.address);
                }
                if let Some(msg) = classify(&chunk.address, &chunk.data, &watch) {
                    if out.send(msg).await.is_err() {
                        return; // subscriber gone
                    }
                }
            }
            // Advance. If the peer is still the closest, keep pulling;
            // otherwise fall back out to re-pick.
            start = page.topmost.saturating_add(1);
            if !still_closest(&peers, &target, &peer_id) {
                break;
            }
        }
    }
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

/// Is `peer` still the closest connected peer to `target`?
fn still_closest(
    peers: &watch::Receiver<Vec<(PeerId, Overlay)>>,
    target: &[u8; 32],
    peer: &PeerId,
) -> bool {
    closest_connected(peers, target).is_some_and(|(p, _)| &p == peer)
}

/// Wait for the peer set to change or the subscriber to drop. Returns
/// `true` if the lurker should stop (channel closed).
async fn wait_or_closed(
    peers: &mut watch::Receiver<Vec<(PeerId, Overlay)>>,
    out: &mpsc::Sender<DecodedMessage>,
) -> bool {
    tokio::select! {
        () = out.closed() => true,
        r = peers.changed() => r.is_err(),
        () = tokio::time::sleep(RETRY_BACKOFF) => false,
    }
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
