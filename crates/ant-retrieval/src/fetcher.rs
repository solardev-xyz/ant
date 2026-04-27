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

use crate::progress::ProgressTracker;
use crate::{retrieve_chunk, ChunkFetcher, InMemoryChunkCache, RetrievalError};
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::PeerId;
use libp2p_stream::Control;
use std::cmp::Ordering;
use std::error::Error as StdError;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, trace, warn};

/// 32-byte Swarm overlay. Duplicated locally rather than re-exported from
/// `ant_p2p::routing` to keep `ant-retrieval` free of a circular dep.
pub type Overlay = [u8; 32];

/// Per-fetch retry budget. With 100 BZZ peers in the table the closest
/// dozen or so will, on average, all be willing to forward, so a
/// budget of 8 distinct peer attempts is comfortable headroom for one
/// chunk before we declare it unavailable for this attempt. The outer
/// retry wrapper in `ant-p2p` carries the resulting blacklist forward
/// so subsequent attempts genuinely start at a different slice of
/// peers (rather than re-hammering the same closest 8).
const FALLBACK_PEERS: usize = 8;

/// Hedging width. We may have at most this many in-flight retrieval
/// streams for a single chunk at once; whichever returns a valid
/// delivery first wins, the rest get cancelled. K=2 hits the sweet
/// spot between tail-latency reduction and wasted bandwidth on
/// duplicated chunks.
const HEDGE_FANOUT: usize = 2;

/// How long we wait on the first peer before firing a hedge. Most
/// chunks resolve in well under this on the happy path (typical fast
/// peer is 100–300 ms end-to-end), so the second request is only
/// fired when the first peer is genuinely tail-slow. This avoids
/// hammering bee peers — and getting disconnected for it — on every
/// fetch, while still capping tail latency at roughly
/// `HEDGE_DELAY + min_hedge_response`. If the first peer *errors*
/// before the timer fires we hedge immediately rather than waiting.
const HEDGE_DELAY: Duration = Duration::from_millis(250);

/// Stateful per-call fetcher. Owns a clone of `Control` and the peer
/// snapshot. The blacklist is behind a `Mutex` because the joiner now
/// fans out sibling fetches concurrently against the same `&self`
/// fetcher, so two failing fetches can race to record the same bad peer.
/// We never hold the lock across an `.await`, so a `std::sync::Mutex` is
/// fine and avoids the `tokio::sync::Mutex` overhead.
pub struct RoutingFetcher {
    control: Control,
    peers: Vec<(PeerId, Overlay)>,
    /// Peers that have failed at least once during this fetch. Used to
    /// rotate through candidates on retry without picking the same dud
    /// peer twice.
    blacklist: Mutex<Vec<PeerId>>,
    /// Optional shared chunk cache. When set, every `fetch` consults
    /// the cache before going to the network and writes back on
    /// success. The daemon holds one cache shared across requests so
    /// re-fetches (within a retry attempt or across `antctl get`
    /// invocations) skip the network entirely.
    cache: Option<Arc<InMemoryChunkCache>>,
    /// When set, every CAC-validated chunk is dumped to
    /// `<dir>/<hex_addr>.bin` (wire bytes: `span || payload`) just
    /// before being returned to the caller. Used by `antd
    /// --record-chunks <dir>` to capture an offline fixture of the
    /// chunks involved in a successful `antctl get`. Best-effort: a
    /// failed write logs a warning but does not break the fetch.
    record_dir: Option<PathBuf>,
    /// Optional shared progress counters. When set, every chunk
    /// returned from this fetcher (network or cache) increments
    /// `chunks_done` / `bytes_done` and — for network fetches —
    /// adds the source peer to the unique-peer set. The daemon
    /// reads from it on a timer to emit `Response::Progress` lines.
    progress: Option<Arc<ProgressTracker>>,
}

impl RoutingFetcher {
    /// Build a fetcher around a `libp2p` stream control and a peer
    /// snapshot. The snapshot is *not* refreshed across the fetcher's
    /// lifetime; for a manifest walk + joiner that takes seconds this
    /// matches what bee does (it freezes a peer set per request to avoid
    /// surprising re-routing mid-walk).
    pub fn new(control: Control, peers: Vec<(PeerId, Overlay)>) -> Self {
        Self::with_blacklist(control, peers, Vec::new())
    }

    /// Build a fetcher pre-populated with peers we already know are
    /// hopeless for this request. Used by the daemon's outer retry
    /// loop to *carry the blacklist forward* across attempts: peers
    /// that failed to deliver chunk(s) on attempt N get skipped on
    /// attempt N+1, so each retry genuinely explores a fresh slice
    /// of the ranked candidate list rather than re-asking the same
    /// closest peers.
    pub fn with_blacklist(
        control: Control,
        peers: Vec<(PeerId, Overlay)>,
        blacklist: Vec<PeerId>,
    ) -> Self {
        Self {
            control,
            peers,
            blacklist: Mutex::new(blacklist),
            cache: None,
            record_dir: None,
            progress: None,
        }
    }

    /// Attach a shared chunk cache to this fetcher. Chainable so call
    /// sites can write
    /// `RoutingFetcher::with_blacklist(..).with_cache(cache.clone())`
    /// without juggling a third constructor variant. Passing the same
    /// `Arc<InMemoryChunkCache>` to every fetcher built for the same
    /// daemon process is what makes the cache persist across retry
    /// attempts and across `antctl get` invocations.
    pub fn with_cache(mut self, cache: Arc<InMemoryChunkCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Dump every successfully-fetched chunk to `dir` as
    /// `<dir>/<hex_addr>.bin` (raw wire bytes: 8-byte LE span ||
    /// payload). Combined with `MapFetcher::from_dir` this lets the
    /// caller replay a real `antctl get` offline. Set by `antd
    /// --record-chunks <dir>` (debug builds only); a `None` value (the
    /// default) is a no-op. The directory is *not* created here — the
    /// caller is responsible for that.
    pub fn with_record_dir(mut self, dir: Option<PathBuf>) -> Self {
        self.record_dir = dir;
        self
    }

    /// Hook a [`ProgressTracker`] into this fetcher. Every chunk the
    /// fetcher hands back (cache hit or network success) updates the
    /// tracker; the daemon's progress emitter reads it on a timer.
    /// Pass the same `Arc<ProgressTracker>` to every fetcher built
    /// for the same `Get*` request — the retry-loop wrapper in
    /// `ant-p2p` already does this.
    pub fn with_progress(mut self, tracker: Arc<ProgressTracker>) -> Self {
        self.progress = Some(tracker);
        self
    }

    /// Snapshot of the peers this fetcher has marked as failed during
    /// its lifetime. Cheap clone (the blacklist is `Vec<PeerId>` and
    /// `PeerId` is `Copy`-cheap). Caller hands this into
    /// [`RoutingFetcher::with_blacklist`] when constructing the next
    /// attempt's fetcher.
    pub fn blacklist_snapshot(&self) -> Vec<PeerId> {
        self.blacklist
            .lock()
            .expect("blacklist mutex poisoned")
            .clone()
    }

    /// `(peer, overlay)` ordered by ascending XOR distance to `target`,
    /// excluding any peer currently in the blacklist. The blacklist is
    /// snapshotted under the lock and dropped before we await anything.
    fn ranked(&self, target: &Overlay) -> Vec<(PeerId, Overlay)> {
        let blacklist = self.blacklist.lock().expect("blacklist mutex poisoned");
        let mut ranked: Vec<(PeerId, Overlay)> = self
            .peers
            .iter()
            .filter(|(p, _)| !blacklist.contains(p))
            .copied()
            .collect();
        drop(blacklist);
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

    fn blacklist_peer(&self, peer: PeerId) {
        self.blacklist
            .lock()
            .expect("blacklist mutex poisoned")
            .push(peer);
    }
}

#[async_trait]
impl ChunkFetcher for RoutingFetcher {
    async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        // Cache hit short-circuits the entire network path. `retrieve_chunk`
        // CAC-validates before we ever cache, so cached entries are trusted
        // without re-hashing.
        if let Some(cache) = self.cache.as_ref() {
            if let Some(bytes) = cache.get(&addr) {
                trace!(
                    target: "ant_retrieval::fetcher",
                    chunk = %hex::encode(addr),
                    "cache hit",
                );
                if let Some(dir) = self.record_dir.as_ref() {
                    record_chunk(dir, &addr, &bytes);
                }
                if let Some(tracker) = self.progress.as_ref() {
                    tracker.record_chunk(None, bytes.len() as u64);
                }
                return Ok(bytes);
            }
        }

        let candidates = self.ranked(&addr);
        if candidates.is_empty() {
            return Err("no BZZ peers available".into());
        }

        // Hedged retrieval with a delay. We walk the ranked candidate
        // list (capped at `FALLBACK_PEERS`) and keep up to
        // `HEDGE_FANOUT` requests in flight at once. The first request
        // is fired immediately. If it hasn't completed within
        // `HEDGE_DELAY`, we add a hedge request to the next-closest
        // peer. If a request errors, we backfill with the next peer
        // straight away (don't wait on the timer — keep race breadth).
        // The first peer to return a valid chunk wins; the rest get
        // dropped, which cancels their libp2p streams.
        let mut iter = candidates.into_iter().take(FALLBACK_PEERS);
        let mut in_flight = FuturesUnordered::new();
        // The initial `None` is observably dead — the only path that
        // reads it goes through the `Err` arm below, which always
        // overwrites it — but the type annotation on first use needs a
        // value here.
        #[allow(unused_assignments)]
        let mut last_err: Option<RetrievalError> = None;

        // Each in-flight fetch gets its own `Control` clone; libp2p's
        // stream control multiplexes over the existing yamux session
        // so this is cheap (no transport-level handshake), and lets
        // hedge attempts truly run in parallel rather than queuing
        // behind a single shared `&mut Control`.
        let make_fut = |peer: PeerId| {
            let mut control = self.control.clone();
            async move {
                let r = retrieve_chunk(&mut control, peer, addr).await;
                (peer, r)
            }
        };

        match iter.next() {
            Some((peer, _)) => in_flight.push(make_fut(peer)),
            None => return Err("no BZZ peers available".into()),
        }

        let mut hedge_timer = Box::pin(tokio::time::sleep(HEDGE_DELAY));

        loop {
            tokio::select! {
                Some((peer, result)) = in_flight.next() => {
                    match result {
                        Ok(chunk) => {
                            // Drop the rest — `FuturesUnordered`'s drop
                            // runs each remaining future's destructor,
                            // which cancels its libp2p stream. Bee on
                            // the other side just sees a stream reset.
                            drop(in_flight);
                            // Caller wants `span (8 LE) || payload`,
                            // exactly what arrives on the wire.
                            // `retrieve_chunk` already CAC-validates so
                            // we can hand bytes back without a re-check.
                            let mut wire = Vec::with_capacity(8 + chunk.payload().len());
                            wire.extend_from_slice(&chunk.span_bytes());
                            wire.extend_from_slice(chunk.payload());
                            // Write-through to the cache so the next
                            // fetcher (next retry attempt, or a future
                            // `antctl get` of the same reference)
                            // skips the network for this chunk. The
                            // clone is one 4 KiB memcpy per success.
                            if let Some(cache) = self.cache.as_ref() {
                                cache.put(addr, wire.clone());
                            }
                            if let Some(dir) = self.record_dir.as_ref() {
                                record_chunk(dir, &addr, &wire);
                            }
                            if let Some(tracker) = self.progress.as_ref() {
                                tracker.record_chunk(Some(peer), wire.len() as u64);
                            }
                            return Ok(wire);
                        }
                        Err(e) => {
                            debug!(
                                target: "ant_retrieval::fetcher",
                                %peer,
                                chunk = %hex::encode(addr),
                                "fetch failed, blacklisting peer: {e}",
                            );
                            self.blacklist_peer(peer);
                            last_err = Some(e);
                            // Eagerly backfill with the next candidate
                            // so the race stays alive. Reset the hedge
                            // timer so the new request gets a fair shot
                            // before we layer another hedge on top.
                            if let Some((peer, _)) = iter.next() {
                                in_flight.push(make_fut(peer));
                                hedge_timer = Box::pin(tokio::time::sleep(HEDGE_DELAY));
                            } else if in_flight.is_empty() {
                                break;
                            }
                        }
                    }
                }
                _ = &mut hedge_timer, if in_flight.len() < HEDGE_FANOUT => {
                    // First peer is taking a while; fire a hedge to the
                    // next-closest candidate, if any.
                    if let Some((peer, _)) = iter.next() {
                        in_flight.push(make_fut(peer));
                    }
                    hedge_timer = Box::pin(tokio::time::sleep(HEDGE_DELAY));
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

/// Best-effort dump of a CAC-validated chunk's wire bytes to
/// `<dir>/<hex_addr>.bin`. Used by the daemon's `--record-chunks`
/// flag to capture a fixture of every chunk a successful `antctl
/// get` touched. Skips if the file already exists (chunks are
/// content-addressed, so the bytes can't differ); a write error
/// only logs a warning so a full disk doesn't poison a live
/// retrieval.
fn record_chunk(dir: &std::path::Path, addr: &[u8; 32], wire: &[u8]) {
    let path = dir.join(format!("{}.bin", hex::encode(addr)));
    if path.exists() {
        return;
    }
    if let Err(e) = std::fs::write(&path, wire) {
        warn!(
            target: "ant_retrieval::fetcher",
            chunk = %hex::encode(addr),
            error = %e,
            path = %path.display(),
            "record-chunks write failed",
        );
    } else {
        trace!(
            target: "ant_retrieval::fetcher",
            chunk = %hex::encode(addr),
            bytes = wire.len(),
            "recorded chunk",
        );
    }
}
