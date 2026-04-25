//! Swarm BZZ handshake v14 (`bee` `pkg/p2p/libp2p/internal/handshake`), length-delimited protobuf.

use crate::underlay::{deserialize_underlays, serialize_underlays, UnderlayError};
use ant_crypto::{
    overlay_from_ethereum_address, sign_handshake_data, verify_bzz_address_signature, CryptoError,
    OVERLAY_NONCE_LEN, SECP256K1_SECRET_LEN,
};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use k256::ecdsa::SigningKey;
use libp2p::core::multiaddr::Protocol;
use libp2p::identity::PeerId;
use libp2p::multiaddr::Multiaddr;
use prost::Message;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::debug;

pub const PROTOCOL_HANDSHAKE: &str = "/swarm/handshake/14.0.0/handshake";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_PROTO_MESSAGE: usize = 128 * 1024;

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("protobuf: {0}")]
    Prost(#[from] prost::DecodeError),
    #[error("protobuf encode: {0}")]
    ProstEncode(#[from] prost::EncodeError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("underlay: {0}")]
    Underlay(#[from] UnderlayError),
    #[error("crypto: {0}")]
    Crypto(#[from] CryptoError),
    #[error("ecdsa: {0}")]
    Ecdsa(#[from] k256::ecdsa::Error),
    #[error("network id mismatch")]
    NetworkId,
    #[error("expected full node")]
    NotFullNode,
    #[error("invalid syn: observed peer id does not match ours")]
    SynPeerId,
    #[error("missing syn or ack in SynAck")]
    IncompleteSynAck,
    #[error("missing fields in ack")]
    IncompleteAck,
    #[error("handshake timed out")]
    Timeout,
    #[error("message too large")]
    MessageTooLarge,
}

#[derive(Clone, Debug)]
pub struct HandshakeInfo {
    pub remote_overlay: [u8; 32],
    pub remote_full_node: bool,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Syn {
    #[prost(bytes = "vec", tag = "1")]
    pub observed_underlay: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BzzAddress {
    #[prost(bytes = "vec", tag = "1")]
    pub underlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub signature: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub overlay: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ack {
    #[prost(message, optional, tag = "1")]
    pub address: Option<BzzAddress>,
    #[prost(uint64, tag = "2")]
    pub network_id: u64,
    #[prost(bool, tag = "3")]
    pub full_node: bool,
    #[prost(bytes = "vec", tag = "4")]
    pub nonce: Vec<u8>,
    #[prost(string, tag = "99")]
    pub welcome_message: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SynAck {
    #[prost(message, optional, tag = "1")]
    pub syn: Option<Syn>,
    #[prost(message, optional, tag = "2")]
    pub ack: Option<Ack>,
}

async fn write_delimited(
    w: &mut (impl AsyncWriteExt + Unpin),
    msg: &impl Message,
) -> Result<(), HandshakeError> {
    let mut buf = Vec::new();
    msg.encode_length_delimited(&mut buf)?;
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

async fn read_delimited(r: &mut (impl AsyncReadExt + Unpin)) -> Result<Vec<u8>, HandshakeError> {
    let len = read_varint_len(r).await?;
    if len > MAX_PROTO_MESSAGE {
        return Err(HandshakeError::MessageTooLarge);
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_varint_len(r: &mut (impl AsyncReadExt + Unpin)) -> Result<usize, HandshakeError> {
    let mut b = [0u8; 1];
    let mut buf = Vec::with_capacity(8);
    loop {
        r.read_exact(&mut b).await?;
        buf.push(b[0]);
        match unsigned_varint::decode::u64(&buf) {
            Ok((v, [])) => return Ok(v as usize),
            Ok(_) => {
                return Err(HandshakeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid varint framing",
                )));
            }
            Err(unsigned_varint::decode::Error::Insufficient) => {
                if buf.len() > 10 {
                    return Err(HandshakeError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "varint too long",
                    )));
                }
            }
            Err(e) => {
                return Err(HandshakeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("varint: {e}"),
                )));
            }
        }
    }
}

/// Outbound handshake (we are the dialer). `remote_peer_addrs` are dial addresses for the **remote** peer.
#[allow(clippy::too_many_arguments)]
pub async fn handshake_outbound<S>(
    mut stream: S,
    local_peer_id: PeerId,
    remote_peer_id: PeerId,
    signing_secret: &[u8; SECP256K1_SECRET_LEN],
    overlay_nonce: &[u8; OVERLAY_NONCE_LEN],
    network_id: u64,
    remote_peer_addrs: &[Multiaddr],
    local_listen_addrs: Vec<Multiaddr>,
) -> Result<HandshakeInfo, HandshakeError>
where
    S: AsyncReadExt + AsyncWriteExt + Send + Unpin,
{
    tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        handshake_outbound_inner(
            &mut stream,
            local_peer_id,
            remote_peer_id,
            signing_secret,
            overlay_nonce,
            network_id,
            remote_peer_addrs,
            local_listen_addrs,
        ),
    )
    .await
    .map_err(|_| HandshakeError::Timeout)?
}

#[allow(clippy::too_many_arguments)]
async fn handshake_outbound_inner(
    stream: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin),
    local_peer_id: PeerId,
    remote_peer_id: PeerId,
    signing_secret: &[u8; SECP256K1_SECRET_LEN],
    overlay_nonce: &[u8; OVERLAY_NONCE_LEN],
    network_id: u64,
    remote_peer_addrs: &[Multiaddr],
    local_listen_addrs: Vec<Multiaddr>,
) -> Result<HandshakeInfo, HandshakeError> {
    let sk = SigningKey::from_bytes(signing_secret.into())?;
    let vk = k256::ecdsa::VerifyingKey::from(&sk);
    let eth = ant_crypto::ethereum_address_from_public_key(&vk);
    let overlay: [u8; 32] = overlay_from_ethereum_address(&eth, network_id, overlay_nonce);

    let remote_mas = build_full_peer_maddrs(remote_peer_addrs, remote_peer_id)?;
    let syn = Syn {
        observed_underlay: serialize_underlays(&remote_mas),
    };
    let t0 = Instant::now();
    write_delimited(stream, &syn).await?;
    debug!(target: "ant_p2p::handshake", elapsed_ms = t0.elapsed().as_millis() as u64, "syn sent");

    let buf = read_delimited(stream).await?;
    debug!(target: "ant_p2p::handshake", elapsed_ms = t0.elapsed().as_millis() as u64, bytes = buf.len(), "syn-ack received");
    let syn_ack = SynAck::decode(&buf[..])?;
    let resp_syn = syn_ack
        .syn
        .as_ref()
        .ok_or(HandshakeError::IncompleteSynAck)?;
    let resp_ack = syn_ack
        .ack
        .as_ref()
        .ok_or(HandshakeError::IncompleteSynAck)?;

    let observed = deserialize_underlays(&resp_syn.observed_underlay)?;
    for o in &observed {
        let info = o.iter().find_map(|p| match p {
            Protocol::P2p(pid) => Some(pid),
            _ => None,
        });
        if info != Some(local_peer_id) {
            return Err(HandshakeError::SynPeerId);
        }
    }

    let mut advertisable: Vec<Multiaddr> = observed
        .into_iter()
        .map(resolve_advertisable)
        .collect::<Vec<_>>();
    for a in local_listen_addrs {
        if !is_loopback(&a) {
            advertisable.push(build_full_ma(&a, local_peer_id)?);
        }
    }
    sort_dedupe_maddrs(&mut advertisable);

    let underlay_bytes = serialize_underlays(&advertisable);
    let sign_data = ant_crypto::handshake_sign_data(&underlay_bytes, &overlay, network_id);
    let sig = sign_handshake_data(signing_secret, &sign_data)?;

    if resp_ack.network_id != network_id {
        return Err(HandshakeError::NetworkId);
    }
    if !resp_ack.full_node {
        return Err(HandshakeError::NotFullNode);
    }

    let addr = resp_ack
        .address
        .as_ref()
        .ok_or(HandshakeError::IncompleteAck)?;
    if addr.overlay.len() != 32 || addr.signature.len() != 65 {
        return Err(HandshakeError::IncompleteAck);
    }
    let mut remote_overlay = [0u8; 32];
    remote_overlay.copy_from_slice(&addr.overlay);
    let mut remote_nonce = [0u8; OVERLAY_NONCE_LEN];
    if resp_ack.nonce.len() != OVERLAY_NONCE_LEN {
        return Err(HandshakeError::IncompleteAck);
    }
    remote_nonce.copy_from_slice(&resp_ack.nonce);
    let mut remote_sig = [0u8; 65];
    remote_sig.copy_from_slice(&addr.signature);

    verify_bzz_address_signature(
        &addr.underlay,
        &remote_overlay,
        &remote_sig,
        &remote_nonce,
        network_id,
        true,
    )?;

    if !resp_ack.welcome_message.is_empty() {
        debug!(
            target: "ant_p2p::handshake",
            welcome = %resp_ack.welcome_message,
            "remote welcome message",
        );
    }

    let our_ack = Ack {
        address: Some(BzzAddress {
            underlay: underlay_bytes,
            signature: sig.to_vec(),
            overlay: overlay.to_vec(),
        }),
        network_id,
        full_node: false,
        nonce: overlay_nonce.to_vec(),
        welcome_message: String::new(),
    };
    write_delimited(stream, &our_ack).await?;
    // Half-close the write side so bee's `handshakeStream.FullClose()`
    // gets a clean `io.EOF` on its `Read([]byte{0})`. Dropping the stream
    // handle instead makes yamux send a Reset, which bee interprets as a
    // protocol error and disconnects us with
    // `"could not fully close stream on handshake"` ~80 ms after the Ack.
    stream.close().await?;
    debug!(target: "ant_p2p::handshake", elapsed_ms = t0.elapsed().as_millis() as u64, "ack sent");

    Ok(HandshakeInfo {
        remote_overlay,
        remote_full_node: resp_ack.full_node,
    })
}

fn resolve_advertisable(observed: Multiaddr) -> Multiaddr {
    observed
}

fn is_loopback(addr: &Multiaddr) -> bool {
    addr.iter()
        .any(|p| matches!(p, Protocol::Ip4(ip) if ip.is_loopback()))
        || addr
            .iter()
            .any(|p| matches!(p, Protocol::Ip6(ip) if ip.is_loopback()))
}

fn build_full_ma(addr: &Multiaddr, peer: PeerId) -> Result<Multiaddr, HandshakeError> {
    if addr.iter().any(|p| matches!(p, Protocol::P2p(_))) {
        return Ok(addr.clone());
    }
    format!("{addr}/p2p/{peer}")
        .parse()
        .map_err(|e| HandshakeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))
}

fn build_full_peer_maddrs(
    addrs: &[Multiaddr],
    remote: PeerId,
) -> Result<Vec<Multiaddr>, HandshakeError> {
    let mut v = Vec::new();
    for a in addrs {
        v.push(build_full_ma(a, remote)?);
    }
    if v.is_empty() {
        return Err(HandshakeError::IncompleteAck);
    }
    Ok(v)
}

fn sort_dedupe_maddrs(addrs: &mut Vec<Multiaddr>) {
    addrs.sort_by_key(|a| a.to_string());
    addrs.dedup_by(|a, b| a == b);
}
