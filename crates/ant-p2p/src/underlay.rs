//! Serialize / deserialize underlay multiaddrs (Bee `bzz.SerializeUnderlays`).
//!
//! Bee 2.8 caps the number and serialized size of advertised underlays
//! per peer (`maxUnderlaysPerPeer = 20`, `maxUnderlayBytes = 2048`) and
//! rejects out-of-bound payloads up-front — see
//! [pkg/bzz/transport.go](../bee/pkg/bzz/transport.go) and
//! [pkg/bzz/underlay.go](../bee/pkg/bzz/underlay.go). We mirror both
//! limits on send and receive so a misbehaving peer can't blow our
//! buffer and we don't get silently rejected once our own listen-set
//! grows beyond the cap.

use libp2p::core::multiaddr::Protocol;
use libp2p::multiaddr::{Error as MultiaddrError, Multiaddr};
use std::net::{Ipv4Addr, Ipv6Addr};
use unsigned_varint::decode::Error as VarintError;

const UNDERLAY_LIST_PREFIX: u8 = 0x99;

/// Maximum number of underlay addresses we will pack into a single
/// serialized record. Matches bee 2.8 `maxUnderlaysPerPeer`.
pub const MAX_UNDERLAYS_PER_PEER: usize = 20;

/// Maximum size of the serialized underlay payload. Matches bee 2.8
/// `maxUnderlayBytes` — chosen there as ~10× the typical multiaddr
/// budget so the cap rarely bites in practice.
pub const MAX_UNDERLAY_BYTES: usize = 2048;

#[derive(Debug, thiserror::Error)]
pub enum UnderlayError {
    #[error("empty underlay bytes")]
    Empty,
    #[error("invalid multiaddr: {0}")]
    Multiaddr(#[from] MultiaddrError),
    #[error("varint: {0}")]
    Varint(#[from] VarintError),
    #[error("underlay count {0} exceeds cap of {MAX_UNDERLAYS_PER_PEER}")]
    CountExceeded(usize),
    #[error("underlay bytes {0} exceed cap of {MAX_UNDERLAY_BYTES}")]
    ByteSizeExceeded(usize),
}

/// Sort + truncate `addrs` to fit inside bee 2.8's count and byte
/// caps, prioritising IPv4 public TCP > IPv4 public WS/WSS > private
/// > loopback / non-IPv4. Mirrors `bzz.SortUnderlaysByPriority` +
/// `bzz.TruncateUnderlays`. Returns the truncated list in priority
/// order; callers should pass this into [`serialize_underlays`].
#[must_use]
pub fn truncate_underlays(addrs: &[Multiaddr]) -> Vec<Multiaddr> {
    let mut sorted: Vec<Multiaddr> = addrs.to_vec();
    sorted.sort_by_key(underlay_score);

    let mut out: Vec<Multiaddr> = Vec::with_capacity(sorted.len().min(MAX_UNDERLAYS_PER_PEER));
    // Account for the 0x99 list-prefix byte that `serialize_underlays`
    // emits when there are 2+ entries. Single-entry records use the
    // backward-compat raw multiaddr encoding (no prefix); we
    // pre-charge the prefix here as an upper bound — slightly
    // conservative for the single-entry case, exactly right for
    // multi-entry records.
    let mut total = 1_usize;
    for addr in sorted {
        if out.len() >= MAX_UNDERLAYS_PER_PEER {
            break;
        }
        let b = addr.to_vec();
        let mut enc = unsigned_varint::encode::u64_buffer();
        let v = unsigned_varint::encode::u64(b.len() as u64, &mut enc);
        let size = v.len() + b.len();
        if total + size > MAX_UNDERLAY_BYTES {
            break;
        }
        total += size;
        out.push(addr);
    }
    out
}

/// Lower score = higher priority. Matches bee 2.8 `underlayScore`:
/// - non-IPv4 +100
/// - loopback +20, else private +10
/// - transport priority: TCP 0, WS 1, WSS 2, else 3
fn underlay_score(addr: &Multiaddr) -> i32 {
    let mut score = 0;
    let mut has_ip4 = false;
    let mut is_loopback = false;
    let mut is_private = false;
    for p in addr.iter() {
        match p {
            Protocol::Ip4(ip) => {
                has_ip4 = true;
                if ip.is_loopback() {
                    is_loopback = true;
                } else if is_private_ipv4(ip) {
                    is_private = true;
                }
            }
            Protocol::Ip6(ip) => {
                if ip.is_loopback() {
                    is_loopback = true;
                } else if is_private_ipv6(ip) {
                    is_private = true;
                }
            }
            _ => {}
        }
    }
    if !has_ip4 {
        score += 100;
    }
    if is_loopback {
        score += 20;
    } else if is_private {
        score += 10;
    }
    score += transport_priority(addr);
    score
}

fn transport_priority(addr: &Multiaddr) -> i32 {
    let mut tcp = false;
    let mut ws = false;
    let mut wss = false;
    for p in addr.iter() {
        match p {
            Protocol::Tcp(_) => tcp = true,
            Protocol::Ws(_) => ws = true,
            Protocol::Wss(_) => wss = true,
            _ => {}
        }
    }
    if wss {
        2
    } else if ws {
        1
    } else if tcp {
        0
    } else {
        3
    }
}

fn is_private_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_private() || ip.is_link_local()
}

fn is_private_ipv6(ip: Ipv6Addr) -> bool {
    // RFC 4193 ULA (fc00::/7) — matches `manet.IsPrivateAddr`.
    let octets = ip.octets();
    (octets[0] & 0xfe) == 0xfc
}

/// Single-address backward-compatible encoding uses raw multiaddr bytes.
///
/// Returns the serialized form unconditionally; the cap checks live in
/// [`serialize_underlays_checked`]. Existing callers that rely on the
/// infallible signature don't change behaviour because we already feed
/// them small, hand-curated address sets.
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

/// Like [`serialize_underlays`] but returns an explicit error when the
/// count / byte caps would be exceeded. Prefer this on the handshake
/// hot path so we surface the cap locally instead of letting bee 2.8
/// reject the payload mid-handshake with `ErrUnderlayCountExceeded` /
/// `ErrUnderlayByteSizeExceeded`.
pub fn serialize_underlays_checked(addrs: &[Multiaddr]) -> Result<Vec<u8>, UnderlayError> {
    if addrs.len() > MAX_UNDERLAYS_PER_PEER {
        return Err(UnderlayError::CountExceeded(addrs.len()));
    }
    let out = serialize_underlays(addrs);
    if out.len() > MAX_UNDERLAY_BYTES {
        return Err(UnderlayError::ByteSizeExceeded(out.len()));
    }
    Ok(out)
}

pub fn deserialize_underlays(data: &[u8]) -> Result<Vec<Multiaddr>, UnderlayError> {
    if data.is_empty() {
        return Err(UnderlayError::Empty);
    }
    if data.len() > MAX_UNDERLAY_BYTES {
        return Err(UnderlayError::ByteSizeExceeded(data.len()));
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
        if out.len() >= MAX_UNDERLAYS_PER_PEER {
            return Err(UnderlayError::CountExceeded(out.len() + 1));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ma(s: &str) -> Multiaddr {
        s.parse().unwrap()
    }

    #[test]
    fn priority_orders_ipv4_public_tcp_first() {
        let public = ma("/ip4/1.2.3.4/tcp/1634");
        let private = ma("/ip4/10.0.0.1/tcp/1634");
        let loopback = ma("/ip4/127.0.0.1/tcp/1634");
        let ipv6 = ma("/ip6/2001:db8::1/tcp/1634");
        let wss = ma("/ip4/1.2.3.4/tcp/1634/wss");
        assert!(underlay_score(&public) < underlay_score(&private));
        assert!(underlay_score(&private) < underlay_score(&loopback));
        assert!(underlay_score(&public) < underlay_score(&ipv6));
        assert!(underlay_score(&public) < underlay_score(&wss));
    }

    #[test]
    fn truncate_drops_low_priority_when_over_cap() {
        let mut addrs: Vec<Multiaddr> =
            (0..MAX_UNDERLAYS_PER_PEER + 5).map(|i| ma(&format!("/ip6/::1/tcp/{i}"))).collect();
        // One IPv4 public TCP entry should always survive truncation.
        let must_keep = ma("/ip4/1.2.3.4/tcp/1634");
        addrs.push(must_keep.clone());
        let truncated = truncate_underlays(&addrs);
        assert!(truncated.len() <= MAX_UNDERLAYS_PER_PEER);
        assert_eq!(truncated[0], must_keep);
    }

    #[test]
    fn serialize_checked_rejects_oversize_count() {
        let addrs: Vec<Multiaddr> =
            (0..MAX_UNDERLAYS_PER_PEER + 1).map(|i| ma(&format!("/ip4/127.0.0.1/tcp/{i}"))).collect();
        let err = serialize_underlays_checked(&addrs).unwrap_err();
        matches!(err, UnderlayError::CountExceeded(_));
    }

    #[test]
    fn deserialize_rejects_oversize_payload() {
        let oversize = vec![0u8; MAX_UNDERLAY_BYTES + 1];
        let err = deserialize_underlays(&oversize).unwrap_err();
        matches!(err, UnderlayError::ByteSizeExceeded(_));
    }

    #[test]
    fn deserialize_rejects_too_many_entries() {
        let mut buf = vec![UNDERLAY_LIST_PREFIX];
        let addr = ma("/ip4/127.0.0.1/tcp/1");
        let b = addr.to_vec();
        let mut enc = unsigned_varint::encode::u64_buffer();
        let v = unsigned_varint::encode::u64(b.len() as u64, &mut enc).to_vec();
        for _ in 0..(MAX_UNDERLAYS_PER_PEER + 1) {
            buf.extend_from_slice(&v);
            buf.extend_from_slice(&b);
        }
        if buf.len() > MAX_UNDERLAY_BYTES {
            // Test would conflate two limits; keep the byte budget below
            // the size cap so the count cap is the one that fires.
            buf.truncate(MAX_UNDERLAY_BYTES);
        }
        let err = deserialize_underlays(&buf).unwrap_err();
        matches!(err, UnderlayError::CountExceeded(_));
    }
}
