//! Inbound-stream sinks for bee protocols that would otherwise cause an
//! immediate post-handshake disconnect.
//!
//! After the BZZ handshake, bee runs two things that can tear our connection
//! down if we're not prepared:
//!
//! 1. **`ConnectIn`** callbacks on each registered p2p protocol (libp2p-go
//!    `p2p.ProtocolSpec`). Only `pricing` and `pseudosettle` expose one in
//!    bee 2.7.1, and of those only [`pricing`] opens a stream back to us —
//!    `/swarm/pricing/1.0.0/pricing` — and returns an error if the stream
//!    can't be negotiated. Bee's stream handler catches that as
//!    `Disconnect(overlay, "failed to process inbound connection notifier")`.
//!
//! 2. **Kademlia's `Announce`** then calls
//!    `discovery.BroadcastPeers(ctx, peer, addrs...)` on every new peer,
//!    which dials `/swarm/hive/1.1.0/peers` to us. On failure bee calls
//!    `Disconnect(peer, "failed broadcasting to peer")`.
//!
//! For pricing we drain and close — that's all bee wants. For hive we parse
//! the protobuf (`Peers { repeated BzzAddress }`) and forward each underlay
//! multiaddr to the swarm loop via an mpsc channel. That turns bee's
//! bookkeeping chatter into free peer discovery: bootnodes broadcast their
//! neighbourhood to us the moment our BZZ handshake finishes.
//!
//! # The bee-headers framing
//!
//! `bee` wraps every protocol stream (except `/swarm/handshake/...`) with a
//! mandatory pair of length-delimited `pb.Headers` messages before the
//! actual protocol payload. The dialer (bee, when it calls
//! `streamer.NewStream` and then writes pricing / hive payload) goes:
//!
//! ```text
//! dialer → listener : varint + Headers pb        // bee sendHeaders
//! listener → dialer : varint + Headers pb        // our response
//! dialer → listener : varint + <protocol body>   // e.g. pricing.AnnouncePaymentThreshold
//! ```
//!
//! Headers are typed `map<string,bytes>`; bee passes `nil` for pricing and
//! hive so the encoded `pb.Headers` is zero bytes long. We respond with the
//! same empty message. Dropping the stream before responding causes bee's
//! `sendHeaders` to see `io.EOF` on its read, which bubbles up as
//! `ConnectIn` failure on pricing and triggers an immediate disconnect.
//!
//! [`pricing`]: https://github.com/ethersphere/bee/blob/v2.7.1/pkg/pricing/pricing.go
//! [`bee-headers`]: https://github.com/ethersphere/bee/blob/v2.7.1/pkg/p2p/libp2p/headers.go

use crate::dial::extract_peer_id;
use crate::underlay::deserialize_underlays;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::StreamExt;
use libp2p::{Multiaddr, PeerId, StreamProtocol};
use libp2p_stream::{Control, IncomingStreams};
use libp2p_swarm::Stream;
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

/// bee `pkg/pricing`, sends a single `AnnouncePaymentThreshold` (<= ~256 bits).
pub const PROTOCOL_PRICING: &str = "/swarm/pricing/1.0.0/pricing";
/// bee `pkg/hive`, sends a `Peers{ repeated BzzAddress }` with up to 30 entries.
pub const PROTOCOL_HIVE_PEERS: &str = "/swarm/hive/1.1.0/peers";

/// Tight bound for the pricing payload — it's a single big.Int.
const PRICING_MAX: usize = 1024;
/// Hive batches up to 30 peers × (overlay + underlay + sig + nonce) per stream.
/// Round up to a comfortable cap so we don't reject valid gossip from bee.
const HIVE_MAX: usize = 128 * 1024;
/// bee caps its own headers exchange at `defaultHeadersRWTimeout = 10s`. We
/// never send non-trivial headers so a much tighter cap is safe here — but
/// giving bee the same 8 KiB window it uses lets us stay compatible with
/// tracing, settlement, and any future non-empty headers payload.
const HEADERS_MAX: usize = 8 * 1024;
/// Per-stream wall-clock cap; bee's `pricing.AnnouncePaymentThreshold` uses
/// a 5 s context and its headers exchange a 10 s one, so 15 s is enough to
/// survive both without letting a stuck stream leak forever.
const STREAM_TIMEOUT: Duration = Duration::from_secs(15);

/// A decoded hive `BzzAddress` reduced to the fields the swarm loop needs
/// to dial the peer. The overlay is carried through so the swarm loop can
/// correlate hive hints with handshake outcomes in future work.
#[derive(Debug, Clone)]
pub struct PeerHint {
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
    #[allow(dead_code)]
    pub overlay: [u8; 32],
}

/// hive.proto `message Peers { repeated BzzAddress peers = 1; }`.
#[derive(Clone, PartialEq, prost::Message)]
struct PeersPb {
    #[prost(message, repeated, tag = "1")]
    peers: Vec<BzzAddressPb>,
}

/// hive.proto `message BzzAddress { bytes Underlay=1; Signature=2; Overlay=3; Nonce=4; }`.
/// Underlay is `bzz.SerializeUnderlays(addrs)` — either raw multiaddr bytes
/// (single address, backward-compat) or the 0x99-prefixed length-delimited list.
/// Signature and Nonce are required on the wire (both empty is a decode error
/// for bee) but we ignore them: we don't verify overlays for hints.
#[derive(Clone, PartialEq, prost::Message)]
struct BzzAddressPb {
    #[prost(bytes = "vec", tag = "1")]
    underlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    signature: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    overlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    nonce: Vec<u8>,
}

/// Handles on the `IncomingStreams` we need to keep alive while the node runs.
#[must_use = "register() must be paired with a spawn() call to drive the sinks"]
pub struct Registrations {
    pricing: IncomingStreams,
    hive: IncomingStreams,
    hint_tx: mpsc::Sender<PeerHint>,
}

/// Claim every bee protocol we need to silently drain. Must run before the
/// first outbound handshake completes: if bee's `ConnectIn` races ahead of
/// our registration, the first peer gets kicked and we start the backoff
/// loop for nothing.
pub fn register(control: &mut Control, hint_tx: mpsc::Sender<PeerHint>) -> Registrations {
    let pricing = control
        .accept(StreamProtocol::new(PROTOCOL_PRICING))
        .expect("pricing protocol registered exactly once");
    let hive = control
        .accept(StreamProtocol::new(PROTOCOL_HIVE_PEERS))
        .expect("hive peers protocol registered exactly once");
    Registrations {
        pricing,
        hive,
        hint_tx,
    }
}

/// Drive both sinks forever on the current runtime.
pub fn spawn(reg: Registrations) {
    tokio::spawn(run_pricing(reg.pricing));
    tokio::spawn(run_hive(reg.hive, reg.hint_tx));
}

async fn run_pricing(mut incoming: IncomingStreams) {
    while let Some((peer_id, stream)) = incoming.next().await {
        tokio::spawn(async move {
            let res = tokio::time::timeout(STREAM_TIMEOUT, drain_stream(stream, PRICING_MAX)).await;
            match res {
                Ok(Ok(d)) => trace!(
                    target: "ant_p2p::pricing",
                    %peer_id, headers = d.headers, body = d.body.len(), "drained"
                ),
                Ok(Err(e)) => debug!(target: "ant_p2p::pricing", %peer_id, "drain failed: {e}"),
                Err(_) => warn!(
                    target: "ant_p2p::pricing",
                    %peer_id,
                    "stream timed out after {}s",
                    STREAM_TIMEOUT.as_secs(),
                ),
            }
        });
    }
}

async fn run_hive(mut incoming: IncomingStreams, hint_tx: mpsc::Sender<PeerHint>) {
    while let Some((peer_id, stream)) = incoming.next().await {
        let tx = hint_tx.clone();
        tokio::spawn(async move {
            let res = tokio::time::timeout(STREAM_TIMEOUT, drain_stream(stream, HIVE_MAX)).await;
            match res {
                Ok(Ok(d)) => {
                    let hints = decode_hive_peers(&d.body);
                    trace!(
                        target: "ant_p2p::hive",
                        %peer_id,
                        headers = d.headers,
                        body = d.body.len(),
                        peers = hints.len(),
                        "drained",
                    );
                    for hint in hints {
                        // Bounded channel — drop on backpressure rather than
                        // stalling the hive handler (and blocking bee).
                        if let Err(mpsc::error::TrySendError::Full(dropped)) = tx.try_send(hint) {
                            trace!(
                                target: "ant_p2p::hive",
                                %peer_id,
                                dropped_peer = %dropped.peer_id,
                                "hint channel full",
                            );
                        }
                    }
                }
                Ok(Err(e)) => debug!(target: "ant_p2p::hive", %peer_id, "drain failed: {e}"),
                Err(_) => warn!(
                    target: "ant_p2p::hive",
                    %peer_id,
                    "stream timed out after {}s",
                    STREAM_TIMEOUT.as_secs(),
                ),
            }
        });
    }
}

/// Decode a hive `Peers` payload into `PeerHint`s. Entries without a
/// `/p2p/<peer_id>` component in their underlay are dropped: libp2p's `Dial`
/// needs a peer id to multiplex concurrent dials, and bee's hive
/// `broadcastPeers` always includes one, so a missing id is a corrupted peer.
fn decode_hive_peers(body: &[u8]) -> Vec<PeerHint> {
    let Ok(msg) = PeersPb::decode(body) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(msg.peers.len());
    for p in msg.peers {
        let Ok(addrs) = deserialize_underlays(&p.underlay) else {
            continue;
        };
        // All underlays of a single BzzAddress describe the same peer, so
        // pull the peer id from the first one that has it.
        let Some(peer_id) = addrs.iter().find_map(extract_peer_id) else {
            continue;
        };
        let mut overlay = [0u8; 32];
        if p.overlay.len() == 32 {
            overlay.copy_from_slice(&p.overlay);
        }
        out.push(PeerHint {
            peer_id,
            addrs,
            overlay,
        });
    }
    out
}

#[derive(Debug)]
struct Drained {
    headers: usize,
    body: Vec<u8>,
}

async fn drain_stream(mut stream: Stream, max: usize) -> std::io::Result<Drained> {
    let headers = read_delimited(&mut stream, HEADERS_MAX).await?;
    // Bee sends its headers first, then blocks reading ours inside
    // `sendHeaders`. An empty `pb.Headers` encodes to 0 bytes, so a varint
    // length of 0 is enough to make bee's `ReadMsgWithContext` succeed and
    // unblock the write of the protocol body.
    write_empty_delimited(&mut stream).await?;

    let body = read_delimited(&mut stream, max).await?;

    // Half-close so bee's `stream.FullClose()` (`CloseWrite` +
    // `io.Copy(io.Discard, stream)`) unblocks promptly. Pricing uses
    // FullClose; hive uses `stream.Close()` which also waits for our
    // close-write via yamux flow control, so this is required on both.
    let _ = stream.close().await;

    // Drain anything bee sends before its own close and return cleanly on
    // either EOF or an already-reset stream — yamux reports the latter as
    // `UnexpectedEof` on macOS / `ConnectionReset` on Linux when the peer
    // resets the stream right after its final write.
    let mut tail = [0u8; 64];
    loop {
        match stream.read(&mut tail).await {
            Ok(0) => {
                return Ok(Drained {
                    headers: headers.len(),
                    body,
                });
            }
            Ok(_) => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                return Ok(Drained {
                    headers: headers.len(),
                    body,
                });
            }
            Err(e) => return Err(e),
        }
    }
}

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

async fn write_empty_delimited<W: AsyncWriteExt + Unpin>(w: &mut W) -> std::io::Result<()> {
    // varint(0) — single zero byte, length prefix for an empty `pb.Headers`.
    w.write_all(&[0u8]).await?;
    w.flush().await?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::underlay::serialize_underlays;

    #[test]
    fn decodes_single_peer_hive_message() {
        let addr: Multiaddr =
            "/ip4/1.2.3.4/tcp/1634/p2p/QmP9b7MxjyEfrJrch5jUThmuFaGzvUPpWEJewCpx5Ln6i8"
                .parse()
                .unwrap();
        let underlay = serialize_underlays(std::slice::from_ref(&addr));
        let msg = PeersPb {
            peers: vec![BzzAddressPb {
                underlay,
                signature: vec![0u8; 65],
                overlay: vec![0xab; 32],
                nonce: vec![0xcd; 32],
            }],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let hints = decode_hive_peers(&buf);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![addr]);
        assert_eq!(hints[0].overlay, [0xab; 32]);
    }

    #[test]
    fn decodes_multi_underlay_peer() {
        let a4: Multiaddr =
            "/ip4/1.2.3.4/tcp/1634/p2p/QmTxX73q8dDiVbmXU7GqMNwG3gWmjSFECuMoCsTW4xp6CK"
                .parse()
                .unwrap();
        let a6: Multiaddr =
            "/ip6/2001:db8::1/tcp/1634/p2p/QmTxX73q8dDiVbmXU7GqMNwG3gWmjSFECuMoCsTW4xp6CK"
                .parse()
                .unwrap();
        let underlay = serialize_underlays(&[a4.clone(), a6.clone()]);
        let msg = PeersPb {
            peers: vec![BzzAddressPb {
                underlay,
                signature: vec![],
                overlay: vec![0u8; 32],
                nonce: vec![],
            }],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let hints = decode_hive_peers(&buf);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![a4, a6]);
    }

    #[test]
    fn drops_peers_without_p2p_id() {
        let bare: Multiaddr = "/ip4/1.2.3.4/tcp/1634".parse().unwrap();
        let underlay = serialize_underlays(&[bare]);
        let msg = PeersPb {
            peers: vec![BzzAddressPb {
                underlay,
                signature: vec![],
                overlay: vec![0u8; 32],
                nonce: vec![],
            }],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert!(decode_hive_peers(&buf).is_empty());
    }
}
