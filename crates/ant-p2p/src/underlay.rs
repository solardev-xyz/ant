//! Serialize / deserialize underlay multiaddrs (Bee `bzz.SerializeUnderlays`).

use libp2p::multiaddr::{Error as MultiaddrError, Multiaddr};
use unsigned_varint::decode::Error as VarintError;

const UNDERLAY_LIST_PREFIX: u8 = 0x99;

#[derive(Debug, thiserror::Error)]
pub enum UnderlayError {
    #[error("empty underlay bytes")]
    Empty,
    #[error("invalid multiaddr: {0}")]
    Multiaddr(#[from] MultiaddrError),
    #[error("varint: {0}")]
    Varint(#[from] VarintError),
}

/// Single-address backward-compatible encoding uses raw multiaddr bytes.
pub fn serialize_underlays(addrs: &[Multiaddr]) -> Vec<u8> {
    if addrs.len() == 1 {
        return addrs[0].to_vec();
    }
    let mut buf = Vec::new();
    buf.push(UNDERLAY_LIST_PREFIX);
    for addr in addrs {
        let b = addr.to_vec();
        let mut enc = unsigned_varint::encode::u64_buffer();
        let v = unsigned_varint::encode::u64(b.len() as u64, &mut enc);
        buf.extend_from_slice(v);
        buf.extend_from_slice(&b);
    }
    buf
}

pub fn deserialize_underlays(data: &[u8]) -> Result<Vec<Multiaddr>, UnderlayError> {
    if data.is_empty() {
        return Err(UnderlayError::Empty);
    }
    if data[0] == UNDERLAY_LIST_PREFIX {
        return deserialize_list(&data[1..]);
    }
    Ok(vec![Multiaddr::try_from(data.to_vec())?])
}

fn deserialize_list(data: &[u8]) -> Result<Vec<Multiaddr>, UnderlayError> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < data.len() {
        let slice = &data[i..];
        let (len, rest) = unsigned_varint::decode::u64(slice)?;
        let used = slice.len() - rest.len();
        i += used;
        let len = len as usize;
        if data.len() < i + len {
            return Err(UnderlayError::Empty);
        }
        out.push(Multiaddr::try_from(data[i..i + len].to_vec())?);
        i += len;
    }
    Ok(out)
}
