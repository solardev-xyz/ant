//! Routing-aware [`ChunkFetcher`] for production use.
//!
//! Wraps a `libp2p_stream::Control` and a static snapshot of
//! `(PeerId, overlay)` peers — the closest BZZ peers we have to *any*
//! target — and exposes a fetch method that:
//!
//! 1. picks the peer whose overlay XORs with the requested chunk address
//!    to the smallest big-endian value (forwarding-Kademlia "closest");
//! 2. opens a `/swarm/retrieval/1.4.0/retrieval` stream, runs the bee
//!    headers handshake, sends a [`crate::PROTOCOL_RETRIEVAL`] request,
//!    and reads the delivery;
//! 3. on failure, falls back to the next-closest peer up to a small
//!    bounded retry count, so a single bad peer doesn't tank a whole
//!    file fetch.
//!
//! The snapshot is taken at command time by the node loop; new BZZ
//! handshakes during the fetch don't show up here, but no in-flight
//! manifest walk is so long-lived that it matters in practice. Re-issuing
//! the command is cheap.

use crate::{retrieve_chunk, ChunkFetcher, RetrievalError};
use async_trait::async_trait;
use libp2p::PeerId;
use libp2p_stream::Control;
use std::cmp::Ordering;
use std::error::Error as StdError;
use tracing::debug;

/// 32-byte Swarm overlay. Duplicated locally rather than re-exported from
/// `ant_p2p::routing` to keep `ant-retrieval` free of a circular dep.
pub type Overlay = [u8; 32];

/// Per-fetch retry budget. With 100 BZZ peers in the table the closest
/// 8 will, on average, all be willing to forward — so 6 attempts is
/// plenty before declaring the chunk unavailable. Keep small to avoid
/// runaway latency on a manifest walk.
const FALLBACK_PEERS: usize = 6;

/// Stateful per-call fetcher. Owns a clone of `Control` and the peer
/// snapshot; `&mut self` lets us memoise across the call (e.g. blacklist
/// peers that have already failed), even though right now we just track a
/// per-call skip list.
pub struct RoutingFetcher {
    control: Control,
    peers: Vec<(PeerId, Overlay)>,
    /// Peers that have failed at least once during this fetch. Used to
    /// rotate through candidates on retry without picking the same dud
    /// peer twice.
    blacklist: Vec<PeerId>,
}

impl RoutingFetcher {
    /// Build a fetcher around a `libp2p` stream control and a peer
    /// snapshot. The snapshot is *not* refreshed across the fetcher's
    /// lifetime; for a manifest walk + joiner that takes seconds this
    /// matches what bee does (it freezes a peer set per request to avoid
    /// surprising re-routing mid-walk).
    pub fn new(control: Control, peers: Vec<(PeerId, Overlay)>) -> Self {
        Self {
            control,
            peers,
            blacklist: Vec::new(),
        }
    }

    /// `(peer, overlay)` ordered by ascending XOR distance to `target`,
    /// excluding any peer in [`Self::blacklist`].
    fn ranked(&self, target: &Overlay) -> Vec<(PeerId, Overlay)> {
        let mut ranked: Vec<(PeerId, Overlay)> = self
            .peers
            .iter()
            .filter(|(p, _)| !self.blacklist.contains(p))
            .copied()
            .collect();
        ranked.sort_by(|(_, a), (_, b)| {
            for i in 0..32 {
                let da = a[i] ^ target[i];
                let db = b[i] ^ target[i];
                match da.cmp(&db) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            Ordering::Equal
        });
        ranked
    }
}

#[async_trait]
impl ChunkFetcher for RoutingFetcher {
    async fn fetch(&mut self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        let candidates = self.ranked(&addr);
        if candidates.is_empty() {
            return Err("no BZZ peers available".into());
        }
        let mut last_err: Option<RetrievalError> = None;
        for (peer, _overlay) in candidates.into_iter().take(FALLBACK_PEERS) {
            match retrieve_chunk(&mut self.control, peer, addr).await {
                Ok(chunk) => {
                    // Caller wants `span (8 LE) || payload`, exactly what
                    // arrives on the wire. `retrieve_chunk` already
                    // CAC-validates so we can hand bytes back without a
                    // re-check.
                    let mut wire = Vec::with_capacity(8 + chunk.payload().len());
                    wire.extend_from_slice(&chunk.span_bytes());
                    wire.extend_from_slice(chunk.payload());
                    return Ok(wire);
                }
                Err(e) => {
                    debug!(
                        target: "ant_retrieval::fetcher",
                        %peer,
                        chunk = %hex::encode(addr),
                        "fetch failed, blacklisting peer: {e}",
                    );
                    self.blacklist.push(peer);
                    last_err = Some(e);
                }
            }
        }
        Err(format!(
            "all peers failed for chunk {} (last: {})",
            hex::encode(addr),
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no candidates".into())
        )
        .into())
    }
}
