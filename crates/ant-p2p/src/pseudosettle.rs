//! Outbound bee `/swarm/pseudosettle/1.0.0/pseudosettle` driver.
//!
//! # Why this exists
//!
//! Bee tracks per-peer **debt** for every chunk it serves us. The disconnect
//! threshold for a light-mode peer (we declare `full_node: false` in the
//! handshake) is `lightDisconnectLimit ≈ 1.69M units` on a default-config
//! mainnet node. At a typical chunk price of `~150_000 units`, that's about
//! **11 chunks per peer before bee rejects further requests with
//! `ErrDisconnectThresholdExceeded`** and blocklists our overlay locally.
//!
//! For a one-shot small file the burst stays under the limit, but a streaming
//! `/bzz` of a 50 MiB media file fans out to ~15k chunk fetches — many of
//! them concentrated on the few peers closest to the file's neighbourhood.
//! Without a way to reset that debt, those hot peers reject us long before
//! the download finishes; without `pseudosettle` the only recovery is to
//! redial fresh peers, which is the peer-set thrash we observed during the
//! 0.3.0 streaming regression.
//!
//! # The protocol (bee 2.7.x)
//!
//! `pseudosettle` is bee's free **time-based debt refresh** mechanism. The
//! exchange is symmetric (either peer can dial), single round-trip, no
//! negotiation:
//!
//! ```text
//! dialer → listener : varint + Headers pb (empty)        // bee headers preamble
//! listener → dialer : varint + Headers pb (empty)
//! dialer → listener : varint + Payment{ amount: bytes }  // big-endian big.Int
//! listener → dialer : varint + PaymentAck{ amount, timestamp }
//! ```
//!
//! The listener (bee, here) clamps `amount` to
//! `min(attempted, lightRefreshRate * (now - lastTimestamp), our_debt)` and
//! returns the accepted figure plus its current Unix timestamp in seconds.
//! That's all that's needed to clear our debt up to the refresh-rate budget
//! since the previous successful settle.
//!
//! See `bee/pkg/settlement/pseudosettle/pseudosettle.go::Pay` for the bee
//! dialer reference and `::handler` for the listener.
//!
//! # What this module does
//!
//! Three pieces:
//!
//! 1. **Inbound drain** ([`run_inbound`]). Bee's `pseudosettle.Protocol`
//!    declares a `ConnectIn`/`ConnectOut` callback that runs `init`, which in
//!    turn registers us in their per-peer `s.peers` map *before any
//!    pseudosettle stream is opened*. So bee never actually dials us for
//!    pseudosettle in normal operation (we're the consumer, not the
//!    forwarder). On the off chance that some peer does, we drain and
//!    NAK-with-zero so they don't get billed for nothing. This is purely
//!    defensive; the steady-state rate of inbound pseudosettle is ~zero.
//!
//! 2. **Outbound refresh** ([`refresh_peer`]). Opens the pseudosettle
//!    stream, exchanges headers, sends a `Payment` with our intended
//!    refresh amount, reads the `PaymentAck`. Returns the accepted amount
//!    and the bee-side timestamp, or an error on stream / framing failure.
//!
//! 3. **Driver task** ([`run_driver`]). A background tokio task. The
//!    fetcher hot path notifies it of every peer we successfully fetched a
//!    chunk from via an mpsc channel (bounded; drops on backpressure).
//!    The driver tracks the last refresh time per peer and, every
//!    [`REFRESH_TICK`], walks the active set and dispatches a
//!    [`refresh_peer`] for any peer whose elapsed time since last refresh
//!    is at least [`MIN_REFRESH_INTERVAL`]. Refreshes run concurrently on
//!    spawned tasks but are bounded by [`MAX_INFLIGHT_REFRESHES`] so a
//!    burst of new peers can't starve the libp2p control of stream slots.

use crate::sinks::{HEADERS_MAX, STREAM_TIMEOUT};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::StreamExt;
use libp2p::{PeerId, StreamProtocol};
use libp2p_stream::{Control, IncomingStreams};
use libp2p_swarm::Stream;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Semaphore};
use tracing::{debug, info, trace, warn};

/// bee `pkg/settlement/pseudosettle`. One protocol, one stream version,
/// matches bee 2.6 → 2.7.x.
pub const PROTOCOL_PSEUDOSETTLE: &str = "/swarm/pseudosettle/1.0.0/pseudosettle";

/// Bee's `lightRefreshRate` in accounting units per second
/// (`refreshRate / lightFactor` with `refreshRate = 4_500_000` and
/// `lightFactor = 10`). Sets the upper bound on how much debt one
/// pseudosettle can clear, since bee clamps `amount` to
/// `lightRefreshRate * elapsed_seconds`. We send `amount = lightRefreshRate
/// * MAX_REFRESH_WINDOW_SECS`; bee will then clamp to the actual elapsed.
pub const LIGHT_REFRESH_RATE_UNITS_PER_SEC: u64 = 450_000;

/// How often [`run_driver`] wakes to scan active peers. Bee's per-peer
/// allowance grows at `LIGHT_REFRESH_RATE_UNITS_PER_SEC` per second, so a
/// 1 s tick is enough resolution to keep up with sustained streaming
/// without burning CPU on idle peers. Aligns with bee's `sequencerResolution
/// = 1 s` granularity in the blocker, too.
const REFRESH_TICK: Duration = Duration::from_millis(1000);

/// Minimum spacing between successive pseudosettle calls to the same peer.
/// Bee's `peerAllowance` returns `ErrSettlementTooSoon` when two settles
/// land in the same wall-clock second on its side, which both wastes a
/// stream and increments their error metric. 2 s leaves a safe margin for
/// network jitter and clock drift between us and the peer.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum window we ever ask bee to clear in a single pseudosettle.
/// Mostly cosmetic — bee clamps internally — but keeps the wire amount
/// from looking absurd in their debug logs and avoids accidentally hitting
/// any future server-side sanity check.
const MAX_REFRESH_WINDOW_SECS: u64 = 60;

/// Concurrent outbound pseudosettle attempts. Each one opens a fresh
/// libp2p stream and waits up to [`STREAM_TIMEOUT`] for the round trip,
/// so a hard cap protects the libp2p control's stream queue from a
/// gateway burst that touches hundreds of new peers in a few seconds.
const MAX_INFLIGHT_REFRESHES: usize = 16;

/// Bound on the "I just fetched from X" channel into the driver. Drops
/// on backpressure (the driver is a small fixed work queue; missing a
/// notification only delays the next refresh by one tick).
pub const NOTIFY_CHANNEL_CAP: usize = 1024;

/// Bee's `pseudosettle.proto::Payment { bytes Amount = 1 }`.
#[derive(Clone, PartialEq, Message)]
struct PaymentPb {
    #[prost(bytes = "vec", tag = "1")]
    amount: Vec<u8>,
}

/// Bee's `pseudosettle.proto::PaymentAck { bytes Amount = 1; int64 Timestamp = 2 }`.
#[derive(Clone, PartialEq, Message)]
struct PaymentAckPb {
    #[prost(bytes = "vec", tag = "1")]
    amount: Vec<u8>,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

/// Bee's `internal/headers/pb.Headers` is a `repeated Header` — but we never
/// set any so the encoded message is always zero bytes long, and a single
/// 0 length-prefix byte on the wire suffices.
async fn write_empty_headers<W: AsyncWriteExt + Unpin>(w: &mut W) -> std::io::Result<()> {
    w.write_all(&[0u8]).await?;
    w.flush().await?;
    Ok(())
}

/// Read a length-delimited message off the stream. Mirrors the helper in
/// `sinks.rs` but lives here to keep the pseudosettle driver self-contained.
async fn read_delimited<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max: usize,
) -> std::io::Result<Vec<u8>> {
    let len = read_varint_len(r).await?;
    if len > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("message too large: {len} bytes (cap {max})"),
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_varint_len<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<usize> {
    let mut byte = [0u8; 1];
    let mut acc: Vec<u8> = Vec::with_capacity(10);
    loop {
        r.read_exact(&mut byte).await?;
        acc.push(byte[0]);
        match unsigned_varint::decode::u64(&acc) {
            Ok((v, [])) => {
                return usize::try_from(v).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "varint overflow")
                });
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid varint framing",
                ));
            }
            Err(unsigned_varint::decode::Error::Insufficient) => {
                if acc.len() > 10 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "varint too long",
                    ));
                }
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("varint: {e}"),
                ));
            }
        }
    }
}

async fn write_delimited<W, M>(w: &mut W, msg: &M) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    M: Message,
{
    let mut buf = Vec::with_capacity(msg.encoded_len() + 10);
    msg.encode_length_delimited(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

/// Outcome of one pseudosettle round trip.
#[derive(Debug, Clone, Copy)]
pub struct RefreshOk {
    /// Amount bee accepted (`<= attempted`). At most
    /// `lightRefreshRate * elapsed`, often less when our debt with this
    /// peer is below the time-based budget.
    pub accepted: u64,
    /// Bee-side Unix timestamp from the `PaymentAck`. Returned for
    /// optional logging; the driver uses local wall-clock to schedule the
    /// next attempt.
    #[allow(dead_code)]
    pub timestamp: i64,
}

/// Open a pseudosettle stream to `peer`, refresh up to
/// `LIGHT_REFRESH_RATE_UNITS_PER_SEC * MAX_REFRESH_WINDOW_SECS` units of
/// debt. Returns the accepted amount on success.
pub async fn refresh_peer(control: &mut Control, peer: PeerId) -> std::io::Result<RefreshOk> {
    let proto = StreamProtocol::new(PROTOCOL_PSEUDOSETTLE);
    let mut stream = control
        .open_stream(peer, proto)
        .await
        .map_err(|e| std::io::Error::other(format!("open stream: {e}")))?;

    // bee-headers preamble: dialer writes first, then reads. We never set
    // headers; bee responds with the same.
    write_empty_headers(&mut stream)
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("write headers: {e}")))?;
    let _their_headers = read_delimited(&mut stream, HEADERS_MAX)
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("read headers: {e}")))?;

    // We send the maximum amount we'd refresh in any one interval. Bee
    // clamps to `min(attempted, lightRefreshRate * elapsed, peer_debt)`
    // and returns the accepted figure, so over-asking is safe and never
    // wasteful.
    let amount: u64 = LIGHT_REFRESH_RATE_UNITS_PER_SEC.saturating_mul(MAX_REFRESH_WINDOW_SECS);
    let amount_bytes = big_int_be_bytes(amount);
    let payment = PaymentPb {
        amount: amount_bytes,
    };
    write_delimited(&mut stream, &payment)
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("write payment: {e}")))?;

    let ack_bytes = read_delimited(&mut stream, 256)
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("read ack: {e}")))?;
    let ack = PaymentAckPb::decode(ack_bytes.as_slice())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("decode ack: {e}")))?;
    let accepted = parse_be_u64(&ack.amount).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "PaymentAck.amount overflows u64",
        )
    })?;

    // Half-close so bee's `stream.FullClose()` returns promptly. Drop on
    // error path is fine — yamux sends a Reset, bee handles it.
    let _ = stream.close().await;

    Ok(RefreshOk {
        accepted,
        timestamp: ack.timestamp,
    })
}

/// Encode a non-negative `u64` as the big-endian unsigned bytes that
/// Go's `big.Int.SetBytes` decodes back to the same value. Strips
/// leading zeroes, so `0 → []` and `1 → [0x01]`.
fn big_int_be_bytes(v: u64) -> Vec<u8> {
    if v == 0 {
        return Vec::new();
    }
    let buf = v.to_be_bytes();
    let first_nonzero = buf.iter().position(|b| *b != 0).unwrap_or(buf.len() - 1);
    buf[first_nonzero..].to_vec()
}

/// Decode big-endian unsigned bytes as a `u64`. Returns `None` if longer
/// than 8 bytes (i.e. the value exceeds `u64::MAX`). Bee always sends
/// values bounded by `lightRefreshRate * MAX_REFRESH_WINDOW_SECS`, well
/// inside `u64`, so this never realistically overflows.
fn parse_be_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.len() > 8 {
        return None;
    }
    let mut padded = [0u8; 8];
    padded[8 - bytes.len()..].copy_from_slice(bytes);
    Some(u64::from_be_bytes(padded))
}

/// Inbound drain. We never expect to see traffic here in steady state
/// (bee only opens this stream when it would *receive* a refresh from us,
/// and as a light node we never originate a refresh inbound), but if
/// some peer does we read the Payment, ack zero, and close. NAK-with-zero
/// keeps their accounting consistent — they treat zero acceptance the
/// same as "settlement too soon" and don't disconnect.
pub async fn run_inbound(mut incoming: IncomingStreams) {
    while let Some((peer_id, stream)) = incoming.next().await {
        tokio::spawn(async move {
            match tokio::time::timeout(STREAM_TIMEOUT, drain_inbound(stream)).await {
                Ok(Ok(())) => trace!(
                    target: "ant_p2p::pseudosettle",
                    %peer_id,
                    "inbound drained",
                ),
                Ok(Err(e)) => debug!(
                    target: "ant_p2p::pseudosettle",
                    %peer_id,
                    "inbound drain failed: {e}",
                ),
                Err(_) => warn!(
                    target: "ant_p2p::pseudosettle",
                    %peer_id,
                    "inbound stream timed out after {}s",
                    STREAM_TIMEOUT.as_secs(),
                ),
            }
        });
    }
}

async fn drain_inbound(mut stream: Stream) -> std::io::Result<()> {
    // Headers preamble (we're the listener for inbound, so read first).
    let _their_headers = read_delimited(&mut stream, HEADERS_MAX).await?;
    write_empty_headers(&mut stream).await?;
    // Read the Payment so bee's writer unblocks; we don't actually
    // credit anything — we have no accounting state of our own, so the
    // refund is a no-op on our side.
    let _payment = read_delimited(&mut stream, 256).await?;
    // ACK zero with the current timestamp: bee's dialer-side check
    // accepts a zero amount as "we wouldn't take any of this", marks the
    // settlement attempt as concluded, and moves on without flagging us.
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let ack = PaymentAckPb {
        amount: Vec::new(),
        timestamp,
    };
    write_delimited(&mut stream, &ack).await?;
    let _ = stream.close().await;
    Ok(())
}

/// Per-peer state held by the driver. Tracking the last refresh
/// success at all means we can space refreshes correctly even when the
/// fetcher streams hundreds of "I just used peer X" notifications in
/// quick succession.
#[derive(Debug, Clone, Copy)]
struct PeerState {
    /// When `run_driver` last attempted a refresh for this peer. Used to
    /// avoid hammering bee with same-second pseudosettles
    /// (`ErrSettlementTooSoon`).
    last_refresh: Option<Instant>,
    /// Last time the fetcher said it succeeded with this peer. Drives
    /// pruning: peers we haven't used in a while drop out of the active
    /// set and stop generating refresh attempts.
    last_used: Instant,
}

/// Aggregate counters surfaced periodically by the driver so an
/// operator can confirm pseudosettle is functioning without turning on
/// per-peer trace logging. Cumulative since process start.
#[derive(Debug, Default, Clone, Copy)]
struct DriverMetrics {
    /// Refresh round trips that bee accepted (any non-error reply,
    /// including `accepted = 0` when our debt was already zero).
    ok: u64,
    /// Refreshes that bee acknowledged with a non-zero `accepted`.
    /// Useful as a cheap signal of "pseudosettle is actually clearing
    /// debt, not just NAKing".
    ok_nonzero: u64,
    /// Sum of `accepted` units across all successful round trips.
    units_accepted: u128,
    /// Refreshes that errored before reaching a `PaymentAck` — most
    /// commonly because the peer disconnected between notify and dial.
    failed: u64,
    /// Refreshes that hit [`STREAM_TIMEOUT`] before completing.
    timed_out: u64,
}

/// Run the driver loop forever. Reads from `notify_rx` (peers we've
/// just fetched chunks from) and periodically (every [`REFRESH_TICK`])
/// dispatches a [`refresh_peer`] to any peer whose last refresh is
/// older than [`MIN_REFRESH_INTERVAL`].
///
/// `peers_rx` carries the routing-table snapshot — the peers we've
/// completed the BZZ handshake with and can actually open substreams
/// to. The driver consults it every tick and skips refreshes for
/// peers that are no longer in the snapshot, so long-lived gateway
/// load (where the routing set churns through thousands of peers per
/// hour) doesn't burn the whole `MAX_INFLIGHT_REFRESHES` budget on
/// `no addresses for peer` errors.
pub async fn run_driver(
    control: Control,
    mut notify_rx: mpsc::Receiver<PeerId>,
    peers_rx: watch::Receiver<Vec<(PeerId, [u8; 32])>>,
) {
    let mut state: HashMap<PeerId, PeerState> = HashMap::new();
    let semaphore = Arc::new(Semaphore::new(MAX_INFLIGHT_REFRESHES));
    let mut tick = tokio::time::interval(REFRESH_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let metrics = Arc::new(std::sync::Mutex::new(DriverMetrics::default()));
    // Log a summary roughly once per minute. At ~1 refresh / peer / 2 s
    // and ~150 active peers during a busy fetch, this is ~4500 events
    // per summary — granular enough to spot a regression, sparse enough
    // to keep the log file readable.
    let summary_every = Duration::from_secs(60);
    let mut last_summary = Instant::now();
    let mut last_metrics = DriverMetrics::default();

    loop {
        tokio::select! {
            biased;
            recv = notify_rx.recv() => {
                let Some(peer) = recv else {
                    debug!(target: "ant_p2p::pseudosettle", "notify channel closed; driver exiting");
                    return;
                };
                let now = Instant::now();
                state
                    .entry(peer)
                    .and_modify(|s| s.last_used = now)
                    .or_insert(PeerState {
                        last_refresh: None,
                        last_used: now,
                    });
            }
            _ = tick.tick() => {
                let now = Instant::now();
                // Drain the notify queue opportunistically — if the
                // fetcher is hot and the receive future above keeps
                // losing the select race, this catches up so we don't
                // starve out new peers indefinitely.
                while let Ok(peer) = notify_rx.try_recv() {
                    state
                        .entry(peer)
                        .and_modify(|s| s.last_used = now)
                        .or_insert(PeerState {
                            last_refresh: None,
                            last_used: now,
                        });
                }
                // Snapshot the live routing set once per tick. Reading
                // the watch is cheap (Arc clone of the inner Vec), and
                // collecting into a `HashSet<PeerId>` makes the
                // per-peer membership check below O(1).
                let live: HashSet<PeerId> = peers_rx
                    .borrow()
                    .iter()
                    .map(|(p, _)| *p)
                    .collect();

                // Drop peers we haven't seen on the routing watch for
                // a tick. Without this filter the gateway-style load
                // (BZZ-streaming a big file pulls chunks from peers
                // that subsequently disconnect) leaves thousands of
                // stale entries that never refresh successfully and
                // starve the live ones out of the in-flight budget.
                state.retain(|peer, s| {
                    if live.contains(peer) {
                        return true;
                    }
                    // Brief grace period so a peer that flapped the
                    // connection between the fetcher's notify and
                    // this tick still gets one shot at a refresh —
                    // libp2p frequently closes and re-opens the same
                    // connection in <100 ms during yamux idle resets.
                    now.duration_since(s.last_used) < Duration::from_secs(5)
                });

                // Walk active peers and dispatch refreshes for those
                // that are past `MIN_REFRESH_INTERVAL`. Spawn detached
                // tasks gated by the semaphore: a refresh that takes a
                // long time mustn't stall the driver's tick.
                for (peer, s) in state.iter_mut() {
                    if !live.contains(peer) {
                        continue;
                    }
                    let due = match s.last_refresh {
                        None => true,
                        Some(last) => now.duration_since(last) >= MIN_REFRESH_INTERVAL,
                    };
                    if !due {
                        continue;
                    }
                    s.last_refresh = Some(now);
                    let peer = *peer;
                    let mut control = control.clone();
                    let sem = semaphore.clone();
                    let metrics = metrics.clone();
                    tokio::spawn(async move {
                        let Ok(_permit) = sem.acquire_owned().await else {
                            return;
                        };
                        let result = tokio::time::timeout(
                            STREAM_TIMEOUT,
                            refresh_peer(&mut control, peer),
                        )
                        .await;
                        let mut m = metrics.lock().expect("metrics mutex poisoned");
                        match result {
                            Ok(Ok(ok)) => {
                                m.ok += 1;
                                if ok.accepted > 0 {
                                    m.ok_nonzero += 1;
                                    m.units_accepted = m
                                        .units_accepted
                                        .saturating_add(ok.accepted as u128);
                                }
                                trace!(
                                    target: "ant_p2p::pseudosettle",
                                    %peer,
                                    accepted = ok.accepted,
                                    timestamp = ok.timestamp,
                                    "refresh ok",
                                );
                            }
                            Ok(Err(e)) => {
                                m.failed += 1;
                                debug!(
                                    target: "ant_p2p::pseudosettle",
                                    %peer,
                                    "refresh failed: {e}",
                                );
                            }
                            Err(_) => {
                                m.timed_out += 1;
                                debug!(
                                    target: "ant_p2p::pseudosettle",
                                    %peer,
                                    "refresh timed out after {}s",
                                    STREAM_TIMEOUT.as_secs(),
                                );
                            }
                        }
                    });
                }

                // Periodic summary. Only emit when we've actually done
                // something this window, otherwise an idle daemon would
                // fill its log with empty status lines.
                if now.duration_since(last_summary) >= summary_every {
                    let snap = *metrics.lock().expect("metrics mutex poisoned");
                    let delta_ok = snap.ok.saturating_sub(last_metrics.ok);
                    let delta_failed = snap.failed.saturating_sub(last_metrics.failed);
                    let delta_timed_out = snap.timed_out.saturating_sub(last_metrics.timed_out);
                    let delta_units = snap
                        .units_accepted
                        .saturating_sub(last_metrics.units_accepted);
                    if delta_ok + delta_failed + delta_timed_out > 0 {
                        info!(
                            target: "ant_p2p::pseudosettle",
                            active_peers = state.len(),
                            ok = delta_ok,
                            failed = delta_failed,
                            timed_out = delta_timed_out,
                            units_accepted = delta_units as u64,
                            "refresh summary (last {}s)",
                            summary_every.as_secs(),
                        );
                    }
                    last_summary = now;
                    last_metrics = snap;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn big_int_be_round_trip() {
        for v in [0u64, 1, 0xff, 0x100, 12345, 450_000, u32::MAX as u64, u64::MAX] {
            let bytes = big_int_be_bytes(v);
            let parsed = parse_be_u64(&bytes).unwrap();
            assert_eq!(parsed, v, "round trip {v}");
            // bee's encoding has no leading zeroes (or empty for zero).
            if v != 0 {
                assert_ne!(bytes[0], 0, "leading zero for {v}");
            } else {
                assert!(bytes.is_empty(), "zero must encode empty");
            }
        }
    }

    #[test]
    fn parse_be_u64_rejects_oversize() {
        assert!(parse_be_u64(&[0u8; 9]).is_none());
        assert_eq!(parse_be_u64(&[]), Some(0));
        assert_eq!(parse_be_u64(&[0x01]), Some(1));
        assert_eq!(parse_be_u64(&[0x01, 0x00]), Some(256));
    }
}
