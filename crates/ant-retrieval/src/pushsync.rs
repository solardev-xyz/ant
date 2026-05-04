//! Pushsync dialer (`/swarm/pushsync/1.3.1/pushsync`) — bee `pkg/pushsync`.

use futures::io::AsyncWriteExt;
use libp2p::{PeerId, StreamProtocol};
use libp2p_stream::Control;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, trace};

use crate::{
    read_delimited, write_delimited, PbHeaders, HEADERS_MAX, RETRIEVE_TIMEOUT, RetrievalError,
};

/// Bee `pkg/pushsync/pushsync.go` pinned protocol id.
pub const PROTOCOL_PUSH: &str = "/swarm/pushsync/1.3.1/pushsync";

const RECEIPT_MAX: usize = 4 * 1024;

#[derive(Debug, Error)]
pub enum PushSyncError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protobuf encode: {0}")]
    ProstEncode(#[from] prost::EncodeError),
    #[error("protobuf decode: {0}")]
    ProstDecode(#[from] prost::DecodeError),
    #[error("open pushsync stream: {0}")]
    OpenStream(String),
    #[error("timed out pushsync after {0:?}")]
    Timeout(Duration),
    #[error("{0}")]
    Remote(String),
    #[error("receipt mismatch")]
    ReceiptMismatch,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct PbPushDelivery {
    #[prost(bytes = "vec", tag = "1")]
    pub(crate) address: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub(crate) data: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub(crate) stamp: Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct PbPushReceipt {
    #[prost(bytes = "vec", tag = "1")]
    pub(crate) address: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub(crate) signature: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub(crate) nonce: Vec<u8>,
    #[prost(string, tag = "4")]
    pub(crate) err: String,
    #[prost(uint32, tag = "5")]
    #[allow(dead_code)]
    pub(crate) storage_radius: u32,
}

/// Push one CAC chunk to a single peer — headers + protobuf `Delivery` + read `Receipt`.
///
/// Returns `Ok(())` when the peer ACKed the chunk address; the receipt
/// signature is currently dropped because we don't yet keep cheques.
pub async fn push_chunk_to_peer(
    control: &mut Control,
    peer: PeerId,
    chunk_addr: [u8; 32],
    wire_span_payload: &[u8],
    stamp_binary: &[u8; ant_postage::STAMP_SIZE],
) -> Result<(), PushSyncError> {
    tokio::time::timeout(
        RETRIEVE_TIMEOUT,
        push_chunk_inner(control, peer, chunk_addr, wire_span_payload, stamp_binary),
    )
    .await
    .map_err(|_| PushSyncError::Timeout(RETRIEVE_TIMEOUT))?
}

async fn push_chunk_inner(
    control: &mut Control,
    peer: PeerId,
    chunk_addr: [u8; 32],
    wire_span_payload: &[u8],
    stamp_binary: &[u8; ant_postage::STAMP_SIZE],
) -> Result<(), PushSyncError> {
    let proto = StreamProtocol::new(PROTOCOL_PUSH);
    let mut stream = control
        .open_stream(peer, proto)
        .await
        .map_err(|e| PushSyncError::OpenStream(e.to_string()))?;

    write_delimited(&mut stream, &PbHeaders::default())
        .await
        .map_err(map_retrieval)?;
    let _their_headers = read_delimited::<PbHeaders>(&mut stream, HEADERS_MAX)
        .await
        .map_err(map_retrieval)?;

    trace!(target: "ant_pushsync", %peer, addr=%hex::encode(chunk_addr), "bee headers exchanged (push)");

    let delivery = PbPushDelivery {
        address: chunk_addr.to_vec(),
        data: wire_span_payload.to_vec(),
        stamp: stamp_binary.to_vec(),
    };

    write_delimited(&mut stream, &delivery)
        .await
        .map_err(map_retrieval)?;
    trace!(target: "ant_pushsync", %peer, "delivery sent");

    let receipt = read_delimited::<PbPushReceipt>(&mut stream, RECEIPT_MAX)
        .await
        .map_err(map_retrieval)?;

    let _ = stream.close().await;

    if !receipt.err.is_empty() {
        debug!(target: "ant_pushsync", %peer, err=%receipt.err, "pushsync remote error");
        return Err(PushSyncError::Remote(receipt.err.clone()));
    }

    let rep_addr: [u8; 32] = receipt
        .address
        .as_slice()
        .try_into()
        .map_err(|_| PushSyncError::ReceiptMismatch)?;
    if rep_addr != chunk_addr {
        return Err(PushSyncError::ReceiptMismatch);
    }

    Ok(())
}

fn map_retrieval(e: RetrievalError) -> PushSyncError {
    match e {
        RetrievalError::Io(io) => PushSyncError::Io(io),
        RetrievalError::ProstEncode(pe) => PushSyncError::ProstEncode(pe),
        RetrievalError::ProstDecode(pe) => PushSyncError::ProstDecode(pe),
        RetrievalError::MessageTooLarge { got, cap } => PushSyncError::Remote(format!(
            "message too large: {got} (cap {cap})"
        )),
        RetrievalError::OpenStream(s) => PushSyncError::OpenStream(s),
        RetrievalError::Timeout(d) => PushSyncError::Timeout(d),
        RetrievalError::Remote(s) => PushSyncError::Remote(s),
        RetrievalError::InvalidChunk => PushSyncError::Remote("invalid chunk hint".into()),
        RetrievalError::BadPayloadSize(_) => PushSyncError::Remote("bad payload size".into()),
    }
}
