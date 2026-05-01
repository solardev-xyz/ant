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
use crate::{retrieve_chunk, ChunkFetcher, InMemoryChunkCache, RetrievalError, RetrievedChunk};
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::PeerId;
use libp2p_stream::Control;
use std::cmp::Ordering;
use std::error::Error as StdError;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch, Semaphore};
use tracing::{debug, trace, warn};

/// 32-byte Swarm overlay. Duplicated locally rather than re-exported from
/// `ant_p2p::routing` to keep `ant-retrieval` free of a circular dep.
pub type Overlay = [u8; 32];

/// Per-chunk error budget, matching bee `maxOriginErrors` in
/// `pkg/retrieval/retrieval.go`. Bee's origin path tolerates up to 32
/// peer errors (Remote / Timeout / Io / OpenStream) before giving up
/// on a chunk; we mirror that exactly so behaviour is comparable
/// chunk-for-chunk. Beyond ~32 candidates the chunk is almost
/// certainly not retrievable from our connected set anyway.
const MAX_ORIGIN_ERRORS: usize = 32;

/// Idle hedge interval. If the active retrieval stream hasn't returned
/// a delivery within this window we dispatch a single backup peer in
/// parallel. Set deliberately well above bee's per-chunk timeout's
/// median (typical `retrieve-chunk` round-trip is 100–800 ms) so this
/// only fires on genuinely tail-slow chunks — every preemptive dispatch
/// either accumulates *applied* debt (if we then cancel after read) or
/// *ghost* debt (if we cancel before read) on bee, both of which count
/// against `lightDisconnectLimit`. Bee's own retrieval uses a 1 s
/// preemptive ticker, but bee is also serving its own neighbourhood
/// where per-chunk RTT is much shorter; for an origin client streaming
/// from forwarders we eat fewer ghost debits by waiting longer before
/// hedging. Hedging is still bounded by the 32-attempt error budget
/// for that single chunk.
const HEDGE_DELAY: Duration = Duration::from_secs(4);

/// Stateful per-call fetcher. Owns a clone of `Control` and a live
/// peer-snapshot subscription via `tokio::sync::watch::Receiver`.
/// Reading from the watch on every `ranked()` call is what keeps the
/// per-chunk peer pool aligned with the swarm's *current* set of BZZ
/// peers. A frozen `Vec<(PeerId, Overlay)>` (the previous design) would
/// drift over the seconds-to-minutes of a multi-MiB fetch and leave us
/// repeatedly trying to open streams on long-dead libp2p connections,
/// surfacing as `oneshot canceled` / `connection is closed` /
/// `Dial error: no addresses for peer` in the retry loop's error
/// messages. The blacklist is still per-fetcher (single retrieval
/// attempt) so misbehaving peers don't get re-tried for sibling chunks
/// of the same request, and it sits behind a `Mutex` because the
/// joiner fans out sibling fetches concurrently against the same
/// `&self` fetcher. We never hold the lock across an `.await`, so a
/// `std::sync::Mutex` is fine and avoids the `tokio::sync::Mutex`
/// overhead.
pub struct RoutingFetcher {
    control: Control,
    /// Live `(PeerId, Overlay)` snapshot of the BZZ peer set. Subscribed
    /// from the swarm's `peers_watch` in `ant-p2p`; tests can hand in a
    /// fixed-value receiver via `RoutingFetcher::with_static_peers`.
    peers_rx: watch::Receiver<Vec<(PeerId, Overlay)>>,
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
    /// Optional process-wide cap on concurrent `retrieve_chunk`
    /// invocations. The daemon constructs **one** semaphore in
    /// `ant-p2p` and clones it into every fetcher built for any
    /// `GetBytes` / `GetBzz` request, so the cap applies *across*
    /// concurrent requests, not per-request. Without this, two
    /// browser-driven `bzz://` fetches happily race ~64 retrieval
    /// streams (8-wide joiner × 2-wide hedge × 2 files × 2 retries
    /// stacking) — bee-side queueing then makes individual chunks
    /// time out at our 20 s envelope even though the *same chunks*
    /// fetched in 90–200 ms in isolation when probed via
    /// `/chunks/<addr>`. The semaphore tames that stampede; absent
    /// (i.e. `None`), every fetch runs unbounded — appropriate for
    /// unit tests and the (single-request) `antctl get` path.
    inflight_limit: Option<Arc<Semaphore>>,
    /// Per-request cap layered in front of `inflight_limit`. Futures
    /// acquire this semaphore before they enter the process-wide queue,
    /// which prevents one large joiner from filling the global FIFO with
    /// hundreds of descendant chunk fetches while another HTTP request is
    /// still trying to fetch its root or first ordered subtree.
    request_inflight_limit: Option<Arc<Semaphore>>,
    /// Notification channel into the pseudosettle driver. When set, every
    /// successful chunk fetch sends the source peer's id; the driver
    /// uses that to schedule periodic
    /// `/swarm/pseudosettle/1.0.0/pseudosettle` refreshes and keep our
    /// per-peer debt below bee's light-mode `disconnectLimit`. Omitted in
    /// unit tests (no real bee on the other end means there's no debt to
    /// settle).
    payment_notify: Option<mpsc::Sender<PeerId>>,
}

impl RoutingFetcher {
    /// Build a fetcher around a `libp2p` stream control and a *live*
    /// peer-snapshot subscription. Every chunk fetch consults
    /// `peers_rx.borrow()` afresh, so peers that disconnect mid-fetch
    /// drop out of the candidate pool the moment the swarm's
    /// `ConnectionClosed` handler fires (rather than sitting in a frozen
    /// list and giving us "connection is closed" errors over and over).
    /// Bee's retrieval works the same way: its skip-list is per-request
    /// and the candidate set is always the *current* forwarding-Kademlia
    /// table.
    ///
    /// The blacklist starts empty — each `RoutingFetcher` is a single
    /// retrieval attempt, so misbehaving peers don't get re-tried for
    /// sibling chunks of the same request, but they're not banned
    /// across attempts either. The retry wrapper in `ant-p2p` constructs
    /// a fresh fetcher per attempt to keep that scoping honest.
    pub fn new(control: Control, peers_rx: watch::Receiver<Vec<(PeerId, Overlay)>>) -> Self {
        Self {
            control,
            peers_rx,
            blacklist: Mutex::new(Vec::new()),
            cache: None,
            record_dir: None,
            progress: None,
            inflight_limit: None,
            request_inflight_limit: None,
            payment_notify: None,
        }
    }

    /// Test helper: wrap a fixed peer list in a watch channel. The
    /// returned fetcher behaves identically to the production one but
    /// never sees a peer-set update — appropriate for unit tests where
    /// we drive a `MapFetcher`-style fixture rather than a live swarm.
    /// The `Sender` is dropped immediately; `watch::Receiver::borrow`
    /// keeps returning the seeded value indefinitely afterwards.
    #[cfg(test)]
    pub fn with_static_peers(control: Control, peers: Vec<(PeerId, Overlay)>) -> Self {
        let (_tx, rx) = watch::channel(peers);
        Self::new(control, rx)
    }

    /// Attach a shared chunk cache to this fetcher. Chainable so call
    /// sites can write `RoutingFetcher::new(..).with_cache(cache.clone())`
    /// without juggling a second constructor variant. Passing the same
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

    /// Cap concurrent in-flight `retrieve_chunk` calls across every
    /// fetcher that shares this `Arc<Semaphore>`. Pass the same
    /// `Arc<Semaphore>` to every fetcher built within one daemon
    /// process — see `inflight_limit` on the struct for why.
    pub fn with_inflight_limit(mut self, sem: Arc<Semaphore>) -> Self {
        self.inflight_limit = Some(sem);
        self
    }

    /// Cap concurrent in-flight `retrieve_chunk` calls for one
    /// user-visible tree join. This is deliberately separate from the
    /// process-wide cap: `ant-p2p` creates one fetcher per `GetBytes` /
    /// `GetBzz` command, so this keeps concurrent browser downloads fair
    /// while still allowing the daemon as a whole to use the network.
    pub fn with_request_inflight_limit(mut self, limit: usize) -> Self {
        self.request_inflight_limit = Some(Arc::new(Semaphore::new(limit.max(1))));
        self
    }

    /// Wire this fetcher to the daemon's pseudosettle driver. On every
    /// successful chunk fetch, the source peer's id is sent on
    /// `notify_tx`; the driver uses that as a heartbeat to schedule
    /// `/swarm/pseudosettle/1.0.0/pseudosettle` refreshes. Without this,
    /// debt accumulates on every peer that serves us until bee's
    /// `disconnectLimit` kicks in (~7-30 chunks per peer in light mode)
    /// — see the 0.3.0 streaming-regression appendix in `PLAN.md` for the
    /// full analysis. The channel is bounded; if it backs up the fetcher
    /// drops the notification rather than blocking the hot path.
    pub fn with_payment_notify(mut self, notify_tx: mpsc::Sender<PeerId>) -> Self {
        self.payment_notify = Some(notify_tx);
        self
    }

    /// `(peer, overlay)` ordered by ascending XOR distance to `target`,
    /// excluding any peer currently in the blacklist. The blacklist is
    /// snapshotted under the lock and dropped before we await anything.
    /// The peer pool itself is read fresh from the watch on every call
    /// — that's the load-bearing line for "peer disappears mid-fetch":
    /// the next sibling chunk's `ranked()` call automatically excludes
    /// the now-disconnected peer because the swarm's
    /// `ConnectionClosed` handler already published a watch update.
    fn ranked(&self, target: &Overlay) -> Vec<(PeerId, Overlay)> {
        let blacklist = self.blacklist.lock().expect("blacklist mutex poisoned");
        let live_peers = self.peers_rx.borrow();
        let mut ranked: Vec<(PeerId, Overlay)> = live_peers
            .iter()
            .filter(|(p, _)| !blacklist.contains(p))
            .copied()
            .collect();
        drop(live_peers);
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

        // Bee-shaped retrieval, lightly tuned for the origin / forwarder
        // split (`bee/pkg/retrieval/retrieval.go`):
        //
        //  - dispatch the closest unasked peer immediately;
        //  - if the chunk hasn't returned within `HEDGE_DELAY`, dispatch
        //    one more peer in parallel and reset the timer (so a really
        //    pathological chunk can build up 2-3 racers, but only after
        //    several seconds of silence on each); whichever returns the
        //    delivery first wins and the remaining streams are dropped;
        //  - on per-stream error, backfill with the next-closest peer
        //    immediately (no timer wait — the racer pool just lost a
        //    slot and we want it filled before the consumer notices);
        //  - the per-chunk skip set keeps growing until either someone
        //    answers, the candidate pool is empty, or `errors_left`
        //    drops to zero.
        //
        // Why we wait `HEDGE_DELAY` instead of bee's `1 s` preemptive
        // ticker: bee dispatches another peer every second on the
        // origin path so a slow forwarder doesn't stall the chunk —
        // but every cancelled inflight stream lands either as an
        // *applied debit* (we read past their write) or a *ghost
        // overdraw* (we don't read, so bee's `debitAction.Cleanup`
        // bumps `accountingPeer.ghostBalance` by the chunk price; cf.
        // `bee/pkg/accounting/accounting.go::debitAction.Cleanup`).
        // Either kind of debit counts against bee's
        // `lightDisconnectLimit` (≈1.69M units). The previous design
        // (1 s preemptive on every chunk that took >1 s) showed up as
        // a peer-set collapse from 100 → 37 over a 4-track media
        // benchmark; widening the hedge window to several seconds
        // dramatically reduces those redundant dispatches without
        // losing the safety net.
        //
        // We deliberately do not implement the `proximity >= radius`
        // multiplex-forward branch from bee's code — that's for nodes
        // that ARE in the chunk's neighbourhood (storage nodes pushing
        // to neighbours). As a light origin we always walk closest-first
        // through forwarder peers.
        let mut asked: Vec<PeerId> = Vec::new();
        let mut errors_left = MAX_ORIGIN_ERRORS;
        let mut last_err: Option<RetrievalError> = None;
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut hedge_timer = Box::pin(tokio::time::sleep(HEDGE_DELAY));

        let make_fut = |peer: PeerId| {
            let mut control = self.control.clone();
            let sem = self.inflight_limit.clone();
            let request_sem = self.request_inflight_limit.clone();
            async move {
                let _request_permit = match request_sem {
                    Some(s) => Some(
                        s.acquire_owned()
                            .await
                            .expect("request retrieval semaphore closed"),
                    ),
                    None => None,
                };
                let _permit = match sem {
                    Some(s) => Some(s.acquire_owned().await.expect("retrieval semaphore closed")),
                    None => None,
                };
                let r = retrieve_chunk(&mut control, peer, addr).await;
                (peer, r)
            }
        };

        // Pick the next-closest live peer that we haven't asked yet for
        // THIS chunk. Reads `peers_rx` afresh on every call so peers
        // that disconnect mid-fetch automatically drop out of the
        // candidate pool, and a peer that gets blacklisted (CAC
        // mismatch, malformed framing) by a sibling chunk's fetcher
        // also disappears here.
        let pick_next = |asked: &Vec<PeerId>| -> Option<PeerId> {
            let ranked = self.ranked(&addr);
            ranked.into_iter().find_map(|(peer, _)| {
                if asked.contains(&peer) {
                    None
                } else {
                    Some(peer)
                }
            })
        };

        // Initial dispatch.
        match pick_next(&asked) {
            Some(peer) => {
                asked.push(peer);
                in_flight.push(make_fut(peer));
            }
            None => return Err("no BZZ peers available".into()),
        }

        loop {
            // Loop exit when we've burned the error budget AND have
            // nothing else racing. Mirrors bee's `for errorsLeft > 0`
            // outer loop with the "continue if inflight" inner check.
            if errors_left == 0 && in_flight.is_empty() {
                break;
            }
            // No more candidates and nothing inflight → can't possibly
            // succeed. This is bee's `topology.ErrNotFound` arm: it
            // returns immediately with the underlying error rather
            // than ticking against a frozen candidate list forever.
            if in_flight.is_empty() && pick_next(&asked).is_none() {
                break;
            }

            tokio::select! {
                biased;
                Some((peer, result)) = in_flight.next() => {
                    match result {
                        Ok(chunk) => {
                            // Cancel-tolerant hedging. Instead of
                            // `drop(in_flight)`, hand the remaining
                            // futures to a detached drain task that
                            // reads each loser's delivery message to
                            // completion before letting the future drop.
                            //
                            // Why: bee's retrieval handler in
                            // `pkg/retrieval/retrieval.go::handler`
                            // calls `accounting.PrepareDebit` *before*
                            // `WriteMsgWithContext(&Delivery{...})`,
                            // and only then `debit.Apply()`. If we drop
                            // the future mid-write, bee's write fails,
                            // `Apply()` doesn't run, and bee's deferred
                            // `debit.Cleanup()` increments
                            // `accountingPeer.ghostBalance` by the chunk
                            // price — which counts against
                            // `lightDisconnectLimit` exactly like real
                            // debt does, but pseudosettle does NOT clear
                            // ghostBalance. The result was a peer-set
                            // collapse from 100 → 36 over a four-track
                            // benchmark even with HEDGE_DELAY = 4 s.
                            //
                            // By draining the loser to completion we
                            // turn the cancellation into a real applied
                            // debit on bee's side. Pseudosettle clears
                            // those, and the peer set stays warm.
                            //
                            // The drain task also write-throughs cached
                            // wire bytes (free, CAC-validated) and
                            // notifies pseudosettle for every success,
                            // so hot peers stay debt-cleared even when
                            // they lose the race.
                            spawn_drain_losers(
                                in_flight,
                                addr,
                                self.cache.clone(),
                                self.record_dir.clone(),
                                self.payment_notify.clone(),
                            );
                            let mut wire = Vec::with_capacity(8 + chunk.payload().len());
                            wire.extend_from_slice(&chunk.span_bytes());
                            wire.extend_from_slice(chunk.payload());
                            if let Some(cache) = self.cache.as_ref() {
                                cache.put(addr, wire.clone());
                            }
                            if let Some(dir) = self.record_dir.as_ref() {
                                record_chunk(dir, &addr, &wire);
                            }
                            if let Some(tracker) = self.progress.as_ref() {
                                tracker.record_chunk(Some(peer), wire.len() as u64);
                            }
                            // Heartbeat into the pseudosettle driver. We
                            // use `try_send` to keep the hot path
                            // strictly non-blocking: if the driver
                            // hasn't drained the bounded channel yet,
                            // missing one notification just delays the
                            // next refresh by at most one driver tick
                            // (~1 s) and is harmless.
                            if let Some(notify) = self.payment_notify.as_ref() {
                                let _ = notify.try_send(peer);
                            }
                            return Ok(wire);
                        }
                        Err(e) => {
                            let blacklist = is_peer_fatal(&e);
                            debug!(
                                target: "ant_retrieval::fetcher",
                                %peer,
                                chunk = %hex::encode(addr),
                                blacklist,
                                inflight = in_flight.len(),
                                errors_left,
                                "fetch failed: {e}",
                            );
                            if blacklist {
                                self.blacklist_peer(peer);
                            }
                            last_err = Some(e);
                            errors_left = errors_left.saturating_sub(1);
                            if errors_left == 0 {
                                continue;
                            }
                            // Backfill immediately on error: don't wait
                            // for the next preemptive tick. Mirrors bee's
                            // `retry()` call inside the error arm.
                            if let Some(peer) = pick_next(&asked) {
                                asked.push(peer);
                                in_flight.push(make_fut(peer));
                            }
                        }
                    }
                }
                _ = &mut hedge_timer, if errors_left > 0 => {
                    // Tail-slow chunk: layer one more peer onto the race.
                    // Reset the timer so the next hedge needs another
                    // full HEDGE_DELAY of silence before firing — we
                    // don't want a single stuck chunk to spin up 32
                    // hedges in a tight loop.
                    if let Some(peer) = pick_next(&asked) {
                        asked.push(peer);
                        in_flight.push(make_fut(peer));
                    }
                    hedge_timer = Box::pin(tokio::time::sleep(HEDGE_DELAY));
                }
            }
        }

        Err(format!(
            "all peers failed for chunk {} after {} attempts (last: {})",
            hex::encode(addr),
            asked.len(),
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no candidates".into())
        )
        .into())
    }
}

/// Classify a retrieval failure as "this peer is broken" (true) vs
/// "this peer just couldn't help with this one chunk" (false). The
/// fetcher only adds peers to the cross-chunk blacklist for the
/// former; the latter group stays in the candidate pool for sibling
/// chunks of the same request, which dramatically widens the effective
/// peer set on multi-chunk file fetches where a peer's chunk coverage
/// is sparse but not non-existent.
///
/// `Remote(_)` is the load-bearing case here: bee returns
/// `storage: not found` whenever its own forwarding attempt couldn't
/// locate the chunk within its retry budget — that's a property of
/// the chunk's locality, not a property of the peer's health, so
/// banning the peer for the rest of the request is wasteful.
/// `Timeout` likewise often reflects a deep forwarding hop that
/// stalled, not the local peer being broken.
///
/// `Io(_)` is *also* non-fatal, and this is the load-bearing addition
/// that fixed the "WAV files won't load" production regression: bee
/// signals "I don't have this chunk" by closing the libp2p_stream
/// without writing the protobuf reply. Our reader then surfaces
/// `UnexpectedEof` (most common) or `BrokenPipe` / `ConnectionReset`
/// (when the close races our read), all of which surface here as
/// `Io(_)`. Treating those as peer-fatal blacklisted ~3 peers per
/// chunk on a multi-MiB media file: we observed 292 distinct peers
/// blacklisted in a single 90-second `/bytes/` fetch (against a
/// connected peer set of ~100), at which point sibling chunks had
/// no candidates left and the joiner deadlocked into the request
/// timeout. If a peer's libp2p *connection* (not just one stream)
/// genuinely dies, libp2p emits a `peer disconnected` event and the
/// `peers_watch` removes them from the candidate pool — that's the
/// proper signal, not an Io error from a single stream.
///
/// `OpenStream(_)` is likewise kept per-chunk. Treating it as fatal was
/// faster for isolated single-file fetches, but live gateway verification
/// with four parallel WAVs showed the shared request blacklist poisoning
/// otherwise healthy file attempts: enough sibling chunks raced through
/// stale address-book entries that later chunks exhausted their candidate
/// set and returned `all peers failed`. The `peers_watch` remains the
/// source of truth for peer liveness; one stream-open failure is not
/// enough to ban a peer for every other chunk in the file.
///
/// We still blacklist on framing errors (`MessageTooLarge`,
/// `BadPayloadSize`, protobuf decode failures) and CAC mismatches: those
/// say the peer is misbehaving or speaking a protocol we can't decode,
/// and there's no reason to expect a different chunk fetch against the
/// same peer to behave any differently.
fn is_peer_fatal(err: &RetrievalError) -> bool {
    match err {
        RetrievalError::Remote(_) => false,
        RetrievalError::Timeout(_) => false,
        RetrievalError::Io(_) => false,
        RetrievalError::OpenStream(_) => false,
        RetrievalError::ProstEncode(_)
        | RetrievalError::ProstDecode(_)
        | RetrievalError::MessageTooLarge { .. }
        | RetrievalError::InvalidChunk
        | RetrievalError::BadPayloadSize(_) => true,
    }
}

/// Detached drain of the losing in-flight fetches after a winner has
/// returned. See the comment at the call site for the full ghost-balance
/// rationale; the short version is that bee's retrieval handler debits
/// us *only* once `WriteMsgWithContext` succeeds, so we let each loser
/// run to completion on a background task instead of cancelling its
/// libp2p stream mid-write.
///
/// Side effects performed by the drain task on each loser that returns
/// a CAC-valid chunk:
///   - **Cache write-through.** The loser already paid for the bytes;
///     caching them is free and a sibling fetch (or a future request
///     for the same chunk) skips the network entirely.
///   - **Pseudosettle notify.** The chunk price was applied as a real
///     debit on bee's accounting, so the pseudosettle driver needs to
///     know we owe this peer. Without this notify, debt would still
///     accumulate and we'd just trade ghost-overdraw blocklists for
///     `lightDisconnectLimit` blocklists.
///   - **`record_chunk`** if recording is enabled.
///
/// Errors from losing fetches are silently discarded — the winner has
/// already returned so there's nothing useful for the caller to do
/// with them.
///
/// The spawned task is detached: we don't await it, and dropping the
/// `JoinHandle` (returned implicitly by `tokio::spawn` and ignored
/// here) does not cancel the task. The loser permits on the per-request
/// and process-wide retrieval semaphores stay held until the drain
/// task finishes, which is exactly the back-pressure we want — sibling
/// chunks of the same request keep waiting until losers finish, so we
/// don't pile on the network while old hedges are still resolving.
fn spawn_drain_losers<S>(
    in_flight: S,
    addr: [u8; 32],
    cache: Option<Arc<InMemoryChunkCache>>,
    record_dir: Option<PathBuf>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
) where
    S: futures::stream::Stream<Item = (PeerId, Result<RetrievedChunk, RetrievalError>)>
        + Send
        + 'static,
{
    tokio::spawn(async move {
        let mut s = Box::pin(in_flight);
        while let Some((peer, result)) = s.next().await {
            match result {
                Ok(chunk) => {
                    let mut wire = Vec::with_capacity(8 + chunk.payload().len());
                    wire.extend_from_slice(&chunk.span_bytes());
                    wire.extend_from_slice(chunk.payload());
                    if let Some(cache) = cache.as_ref() {
                        cache.put(addr, wire.clone());
                    }
                    if let Some(dir) = record_dir.as_ref() {
                        record_chunk(dir, &addr, &wire);
                    }
                    // We deliberately do NOT call
                    // `progress.record_chunk(...)`: the winner already
                    // counted this chunk against the request's totals
                    // and we don't want this loser to double-count the
                    // bytes-served gauge.
                    if let Some(notify) = payment_notify.as_ref() {
                        let _ = notify.try_send(peer);
                    }
                    trace!(
                        target: "ant_retrieval::fetcher",
                        %peer,
                        chunk = %hex::encode(addr),
                        "drained losing hedge to applied debit",
                    );
                }
                Err(e) => {
                    trace!(
                        target: "ant_retrieval::fetcher",
                        %peer,
                        chunk = %hex::encode(addr),
                        "drained losing hedge errored: {e}",
                    );
                }
            }
        }
    });
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// `is_peer_fatal` is the load-bearing classifier: it decides
    /// whether a per-chunk failure spreads into a request-wide
    /// blacklist or stays scoped to the current chunk fetch. Getting
    /// this wrong is exactly the failure mode we hit in production
    /// before this change — `Remote("not found")` from one chunk took
    /// the peer out of the running for *all* sibling chunks of the
    /// same file fetch, even though the peer was perfectly capable of
    /// serving them.
    ///
    /// The matrix below pins the policy variant-by-variant so an
    /// accidental edit to the `is_peer_fatal` arms surfaces here
    /// instead of in a flaky retrieval regression.
    #[test]
    fn peer_fatal_classifier_matrix() {
        assert!(
            !is_peer_fatal(&RetrievalError::Remote("storage: not found".into())),
            "remote 'not found' must NOT poison the peer for sibling chunks",
        );
        assert!(
            !is_peer_fatal(&RetrievalError::Timeout(Duration::from_secs(20))),
            "single timeout must NOT poison the peer for sibling chunks",
        );

        assert!(
            !is_peer_fatal(&RetrievalError::OpenStream("dial failed".into())),
            "stream-open failure must NOT poison the peer for sibling chunks",
        );
        assert!(
            is_peer_fatal(&RetrievalError::InvalidChunk),
            "CAC mismatch: peer is misbehaving",
        );
        assert!(
            is_peer_fatal(&RetrievalError::BadPayloadSize(42)),
            "out-of-range payload: peer is misbehaving",
        );
        assert!(
            is_peer_fatal(&RetrievalError::MessageTooLarge {
                got: 1 << 20,
                cap: 16 << 10
            }),
            "oversized message: peer is misbehaving or compromised",
        );
        // Bee signals "I don't have this chunk" by closing the
        // libp2p_stream without writing a reply. Our reader then
        // surfaces those closes as `Io(UnexpectedEof)` (most common)
        // or `Io(BrokenPipe)` / `Io(ConnectionReset)` (when the close
        // races a read). Pre-fix we treated all of those as peer-fatal
        // and burned ~3 peers per chunk on a multi-MiB file; the
        // joiner then deadlocked into its request timeout because
        // sibling chunks had no candidates left. The matrix below
        // pins the post-fix policy: an `Io(_)` from any kind is
        // a per-chunk signal, never a per-peer one.
        for kind in [
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
        ] {
            assert!(
                !is_peer_fatal(&RetrievalError::Io(std::io::Error::new(kind, "stream closed"))),
                "Io({kind:?}) is bee's 'no chunk' signal, must NOT poison the peer for sibling chunks",
            );
        }
    }

    /// Pin the consumer side of the live-peers fix: every `ranked()`
    /// call must reflect the *current* contents of the `peers_watch`,
    /// not a snapshot frozen at fetcher-construction time. The pre-fix
    /// regression — using a `Vec<(PeerId, Overlay)>` cloned once and
    /// stored on `Self` — survived months of testing because no unit
    /// test exercised the consumer at all (the publisher side, in
    /// `ant-p2p::behaviour::tests::publish_peers_reflects_admit_and_forget`,
    /// was added later but doesn't observably catch a fetcher that
    /// just ignored the watch). This test closes that gap: it
    /// constructs the fetcher with one peer, asserts `ranked()` sees
    /// it, swaps the channel value to a different peer set, and asserts
    /// the same `ranked()` call now reflects the new set. If the
    /// fetcher ever regresses to a stored snapshot the second
    /// assertion is what fails.
    #[test]
    fn ranked_reflects_live_watch_updates() {
        use libp2p::identity::Keypair;

        // Two arbitrary, distinct peers. The actual XOR-distance
        // ordering doesn't matter for this test; we only assert which
        // peers `ranked()` *includes*.
        let p1 = Keypair::generate_ed25519().public().to_peer_id();
        let o1 = [0xaa_u8; 32];
        let p2 = Keypair::generate_ed25519().public().to_peer_id();
        let o2 = [0xbb_u8; 32];

        let (tx, rx) = watch::channel(vec![(p1, o1)]);

        // We need a `Control` to construct a `RoutingFetcher` even
        // though `ranked()` never touches it. `libp2p_stream::Behaviour`
        // hands one out without requiring a swarm to be running.
        let behaviour = libp2p_stream::Behaviour::default();
        let control = behaviour.new_control();

        let fetcher = RoutingFetcher::new(control, rx);
        let target = [0u8; 32];

        let ranked_before = fetcher.ranked(&target);
        assert_eq!(
            ranked_before,
            vec![(p1, o1)],
            "ranked() must surface the initial watch value",
        );

        // Republish a different peer set. Production calls this from
        // `SwarmState::publish_peers` on every admit / forget.
        tx.send_replace(vec![(p2, o2)]);

        let ranked_after = fetcher.ranked(&target);
        assert_eq!(
            ranked_after,
            vec![(p2, o2)],
            "ranked() must read the watch on every call, not at construction time",
        );
    }

    /// Pin the contract that `with_inflight_limit` actually parks the
    /// fetch path on the supplied semaphore. The whole point of the
    /// process-wide cap is to keep concurrent `bzz://` requests from
    /// stampeding bee with ~60 simultaneous retrieval streams; if a
    /// future refactor accidentally drops the permit-acquire (or
    /// acquires it *after* `retrieve_chunk` runs) the saturation
    /// regression returns silently — failing tests would have to
    /// actually drive a multi-MiB fetch over the wire to notice. This
    /// test catches that without leaving the unit-test boundary: with
    /// the only permit externally held, a `fetch` call must never
    /// reach `retrieve_chunk` (which would resolve near-instantly to a
    /// transport error against the dummy `Control`) and must therefore
    /// still be running after a generous park window.
    #[tokio::test]
    async fn fetch_parks_when_inflight_cap_exhausted() {
        use libp2p::identity::Keypair;
        use std::time::Duration;

        let sem = Arc::new(Semaphore::new(1));
        // Hold the only permit so any `fetch` task that respects the
        // limit must park at `acquire_owned()`.
        let hold = sem.clone().acquire_owned().await.unwrap();

        let p = Keypair::generate_ed25519().public().to_peer_id();
        let o = [0u8; 32];
        let (_tx, rx) = watch::channel(vec![(p, o)]);
        let behaviour = libp2p_stream::Behaviour::default();
        let control = behaviour.new_control();
        let fetcher = RoutingFetcher::new(control, rx).with_inflight_limit(sem.clone());

        let h = tokio::spawn(async move {
            let _ = fetcher.fetch([0u8; 32]).await;
        });

        // 100 ms is well past the 250 ms hedge timer's first fire too —
        // even after the loop has tried to schedule a second peer, every
        // future is parked at the semaphore.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !h.is_finished(),
            "fetch must be parked at the inflight-permit acquire while the cap is exhausted",
        );
        assert_eq!(
            sem.available_permits(),
            0,
            "the held permit must still be the only outstanding one",
        );

        // Cleanup — drop our holder, then abort the spawned task so
        // the test doesn't depend on a real swarm being available to
        // satisfy `retrieve_chunk`.
        drop(hold);
        h.abort();
    }
}
