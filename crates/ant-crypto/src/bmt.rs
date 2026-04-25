//! Binary Merkle Tree (BMT) hash and Content-Addressed Chunk (CAC) validation,
//! matching `bee` `pkg/bmt` and `pkg/cac` byte-for-byte.
//!
//! A Swarm chunk is up to [`CHUNK_SIZE`] bytes of payload prefixed by an
//! 8-byte little-endian span. Its address is the **BMT hash with span prefix**:
//!
//! ```text
//! addr = keccak256( span_le_u64 || bmt_root(payload padded to 4096 bytes) )
//! ```
//!
//! `bmt_root` is the root of a balanced binary keccak-256 Merkle tree over
//! exactly 128 leaf segments of 32 bytes each. The payload is right-padded
//! with zero bytes to fill the 4096-byte capacity; the tree shape is fixed
//! regardless of the actual payload length, which is why the *length* (the
//! span) is mixed in only at the very last step. This is the
//! "BMT hash with metadata" construction described in the Swarm book.
//!
//! Test vectors are taken straight from `bee` `pkg/cac/cac_test.go`
//! (`TestNew`, `TestValid`) so any divergence from upstream surfaces here.
//!
//! # Why iterative?
//!
//! `bee`'s implementation hashes sections in goroutines and joins them with
//! channels for throughput. A swarm light client retrieves chunks one at a
//! time off the network and verifies them on the request path; the cost
//! that matters is the 30 ms it takes to read the chunk over the wire, not
//! the 10 µs it takes to hash 4 KiB. Doing it level-by-level on a single
//! thread is small, obviously correct, and avoids dragging `tokio` into
//! `ant-crypto`.

use sha3::{Digest, Keccak256};

/// Size of a single BMT segment / hash output (`swarm.SectionSize`).
pub const SEGMENT_SIZE: usize = 32;
/// Number of leaf segments per chunk (`swarm.Branches` / `swarm.BmtBranches`).
pub const BRANCHES: usize = 128;
/// Maximum payload size of a single chunk in bytes (`swarm.ChunkSize`).
pub const CHUNK_SIZE: usize = SEGMENT_SIZE * BRANCHES;
/// Size of the little-endian span prefix on every chunk (`swarm.SpanSize`).
pub const SPAN_SIZE: usize = 8;

/// BMT root of `payload` (right-padded to [`CHUNK_SIZE`] with zeros).
///
/// Returns the 32-byte root of the balanced binary keccak-256 Merkle tree
/// over 128 leaf segments. Does **not** mix in the span — call
/// [`bmt_hash_with_span`] for the chunk address.
///
/// `payload.len()` must be `<= CHUNK_SIZE`. Returns `None` otherwise.
pub fn bmt_root(payload: &[u8]) -> Option<[u8; SEGMENT_SIZE]> {
    if payload.len() > CHUNK_SIZE {
        return None;
    }
    // Pad to the fixed 4 KiB capacity. The shape of the tree never changes;
    // missing leaves are zero-filled before hashing the leaf pairs, so the
    // empty subtree hashes propagate up naturally without a precomputed
    // `zerohashes` table (which is what `bee` keeps for performance).
    let mut buf = [0u8; CHUNK_SIZE];
    buf[..payload.len()].copy_from_slice(payload);

    // Level 0: 128 leaves, each a single 32-byte segment of `buf`.
    // Level 1 = keccak(seg_2i || seg_2i+1) for i in 0..64. We collapse
    // `buf` in place: at each level we have `len` 32-byte nodes laid out
    // contiguously, and we hash adjacent pairs back into the same buffer
    // until a single 32-byte root remains.
    let mut nodes = buf.to_vec();
    let mut len = BRANCHES;
    while len > 1 {
        let half = len / 2;
        for i in 0..half {
            let l = i * 2 * SEGMENT_SIZE;
            let r = l + SEGMENT_SIZE;
            let pair = &nodes[l..r + SEGMENT_SIZE];
            let mut h = Keccak256::new();
            h.update(pair);
            let digest = h.finalize();
            nodes[i * SEGMENT_SIZE..(i + 1) * SEGMENT_SIZE].copy_from_slice(&digest);
        }
        len = half;
    }

    let mut root = [0u8; SEGMENT_SIZE];
    root.copy_from_slice(&nodes[..SEGMENT_SIZE]);
    Some(root)
}

/// Full Swarm chunk address: `keccak256(span || bmt_root(payload))`.
///
/// `span` is the 8-byte little-endian length prefix that precedes the
/// payload on the wire. For a content-addressed chunk that bee built with
/// `cac.New(payload)`, `span = u64::to_le_bytes(payload.len() as u64)`.
pub fn bmt_hash_with_span(span: &[u8; SPAN_SIZE], payload: &[u8]) -> Option<[u8; SEGMENT_SIZE]> {
    let root = bmt_root(payload)?;
    let mut h = Keccak256::new();
    h.update(span);
    h.update(root);
    let digest = h.finalize();
    let mut out = [0u8; SEGMENT_SIZE];
    out.copy_from_slice(&digest);
    Some(out)
}

/// Validate a content-addressed chunk: the wire bytes are
/// `span (8 LE) || payload (<= 4096)`, the address is the BMT hash with span.
///
/// Mirrors `bee` `pkg/cac.Valid`: returns `true` iff `data` parses, the
/// payload size is within bounds, and the recomputed hash matches `address`.
pub fn cac_valid(address: &[u8; SEGMENT_SIZE], data: &[u8]) -> bool {
    if data.len() < SPAN_SIZE || data.len() > SPAN_SIZE + CHUNK_SIZE {
        return false;
    }
    let mut span = [0u8; SPAN_SIZE];
    span.copy_from_slice(&data[..SPAN_SIZE]);
    let payload = &data[SPAN_SIZE..];
    match bmt_hash_with_span(&span, payload) {
        Some(h) => h == *address,
        None => false,
    }
}

/// Build a content-addressed chunk envelope from a raw payload, the way
/// `bee` `cac.New` does: returns `(address, span_then_data)`.
///
/// Convenience helper for tests; production code receives the chunk bytes
/// from the network and only needs [`cac_valid`].
pub fn cac_new(payload: &[u8]) -> Option<([u8; SEGMENT_SIZE], Vec<u8>)> {
    if payload.len() > CHUNK_SIZE {
        return None;
    }
    let span = (payload.len() as u64).to_le_bytes();
    let addr = bmt_hash_with_span(&span, payload)?;
    let mut wire = Vec::with_capacity(SPAN_SIZE + payload.len());
    wire.extend_from_slice(&span);
    wire.extend_from_slice(payload);
    Some((addr, wire))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex32(s: &str) -> [u8; SEGMENT_SIZE] {
        let mut out = [0u8; SEGMENT_SIZE];
        hex::decode_to_slice(s, &mut out).unwrap();
        out
    }

    /// `bee/pkg/cac/cac_test.go::TestValid` — `"foo"` hashes to a fixed address.
    /// If this fails, our BMT diverges from upstream and nothing else in the
    /// retrieval path will work.
    #[test]
    fn bee_vector_foo() {
        let want = hex32("2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48");
        let (addr, _wire) = cac_new(b"foo").unwrap();
        assert_eq!(addr, want);
    }

    /// `bee/pkg/cac/cac_test.go::TestNew` — `"greaterthanspan"` (15 bytes,
    /// strictly larger than the 8-byte span). Catches off-by-one in span
    /// handling that `"foo"` (3 bytes, shorter than span) wouldn't reveal.
    #[test]
    fn bee_vector_greaterthanspan() {
        let want = hex32("27913f1bdb6e8e52cbd5a5fd4ab577c857287edf6969b41efe926b51de0f4f23");
        let (addr, _wire) = cac_new(b"greaterthanspan").unwrap();
        assert_eq!(addr, want);
    }

    /// `cac::valid` round-trips on the same vectors.
    #[test]
    fn valid_round_trip() {
        for input in [b"foo".as_slice(), b"greaterthanspan".as_slice()] {
            let (addr, wire) = cac_new(input).unwrap();
            assert!(cac_valid(&addr, &wire));
        }
    }

    /// Tampering invalidates the chunk. Flipping a single byte in either
    /// the address or the data must trip `cac_valid`.
    #[test]
    fn valid_rejects_tamper() {
        let (mut addr, mut wire) = cac_new(b"Digital Freedom Now").unwrap();
        addr[0] ^= 0x01;
        assert!(!cac_valid(&addr, &wire));

        let (addr, _) = cac_new(b"Digital Freedom Now").unwrap();
        wire[SPAN_SIZE] ^= 0x01;
        assert!(!cac_valid(&addr, &wire));
    }

    /// A full 4 KiB payload — exercises the "no padding" path where every
    /// leaf is filled by real data. Self-consistency: `cac_new` and
    /// `cac_valid` agree.
    #[test]
    fn valid_full_chunk() {
        let payload = vec![0xab; CHUNK_SIZE];
        let (addr, wire) = cac_new(&payload).unwrap();
        assert!(cac_valid(&addr, &wire));
    }

    /// Oversize payload is refused before hashing; we never construct a
    /// chunk we couldn't put on the wire.
    #[test]
    fn rejects_oversize() {
        assert!(cac_new(&vec![0u8; CHUNK_SIZE + 1]).is_none());
        let mut wire = vec![0u8; SPAN_SIZE + CHUNK_SIZE + 1];
        wire[0] = 1; // any non-zero span
        assert!(!cac_valid(&[0u8; SEGMENT_SIZE], &wire));
    }
}
