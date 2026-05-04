//! File splitter: bytes → balanced k-ary chunk tree.
//!
//! The inverse of [`crate::joiner`]. Given a byte slice, produces every
//! content-addressed chunk needed to represent the file on Swarm: leaf
//! chunks (4 KiB of payload each) and intermediate "join" chunks (lists
//! of up to 128 child references) all the way up to a single root.
//!
//! Wire shape matches `bee/pkg/file/splitter`:
//!
//! - **Leaf chunk** = `span (8 LE) || payload (≤ 4096)`. Span equals the
//!   payload length, so a leaf is identical to whatever
//!   [`ant_crypto::cac_new`] would produce.
//! - **Intermediate chunk** = `span (8 LE) || refs`, where `refs` is the
//!   concatenation of N ∈ [2, 128] child addresses (32 bytes each) and
//!   `span` is the *total file-data bytes covered by the entire subtree*
//!   rooted at this chunk — not the chunk's own payload length.
//!
//! The tree is left-leaning: leaves are filled to 4 KiB before starting a
//! new one; intermediates at level k are filled to 128 children before a
//! new sibling is opened. This matches what bee's splitter emits and is
//! also exactly what [`crate::joiner::join`] expects on the way back.
//!
//! Returned vectors are in *push order*: data leaves first, then each
//! level of intermediates bottom-up, with the root last. Callers that
//! upload via pushsync can simply iterate; callers that need the root
//! address explicitly read [`SplitResult::root`].

use ant_crypto::{bmt_hash_with_span, cac_new, CHUNK_SIZE, SPAN_SIZE};

use crate::ChunkAddr;

/// Maximum number of child references packed into one intermediate chunk.
/// Equal to bee's `swarm.Branches` (4096 / 32).
pub const BRANCHES: usize = CHUNK_SIZE / 32;

/// One chunk produced by the splitter, ready to push: address + wire bytes
/// (`span (8 LE) || payload`).
#[derive(Debug, Clone)]
pub struct SplitChunk {
    pub address: ChunkAddr,
    pub wire: Vec<u8>,
}

/// Result of splitting a byte buffer.
#[derive(Debug, Clone)]
pub struct SplitResult {
    /// Address of the file's root chunk. Hand this to a joiner / mantaray
    /// "entry" field to fetch the file back.
    pub root: ChunkAddr,
    /// Total file bytes (matches the root chunk's span).
    pub total_bytes: u64,
    /// Every chunk needed to reconstruct the file. The last entry is the
    /// root; data leaves come first, intermediate levels next.
    pub chunks: Vec<SplitChunk>,
}

/// Split `payload` into a balanced chunk tree.
///
/// Empty input is treated as a single 0-byte leaf — the same chunk a
/// `cac_new(b"")` produces. The resulting `root` is the well-known
/// empty-content reference and the file has zero bytes.
///
/// # Panics
///
/// Never; all chunk constructions stay within [`CHUNK_SIZE`] by
/// construction. The internal `cac_new` / `bmt_hash_with_span` callers
/// `unwrap()` only after enforcing payload length ≤ `CHUNK_SIZE`.
pub fn split_bytes(payload: &[u8]) -> SplitResult {
    if payload.len() <= CHUNK_SIZE {
        let (root, wire) = cac_new(payload).expect("payload length already ≤ CHUNK_SIZE");
        return SplitResult {
            root,
            total_bytes: payload.len() as u64,
            chunks: vec![SplitChunk {
                address: root,
                wire,
            }],
        };
    }

    let mut chunks: Vec<SplitChunk> = Vec::new();

    // Level 0: data leaves. Each carries up to CHUNK_SIZE bytes and its
    // span equals its own payload length.
    let mut level: Vec<(ChunkAddr, u64)> = payload
        .chunks(CHUNK_SIZE)
        .map(|leaf| {
            let (addr, wire) = cac_new(leaf).expect("leaf ≤ CHUNK_SIZE by construction");
            chunks.push(SplitChunk {
                address: addr,
                wire,
            });
            (addr, leaf.len() as u64)
        })
        .collect();

    // Levels 1..R: pack the previous level into intermediate chunks of up
    // to BRANCHES children each. Repeat until the level collapses to one
    // chunk — that's the root.
    while level.len() > 1 {
        let mut next: Vec<(ChunkAddr, u64)> = Vec::with_capacity(level.len().div_ceil(BRANCHES));
        for group in level.chunks(BRANCHES) {
            let mut intermediate_payload = Vec::with_capacity(group.len() * 32);
            let mut subtree_span: u64 = 0;
            for (child_addr, child_span) in group {
                intermediate_payload.extend_from_slice(child_addr);
                subtree_span += child_span;
            }
            let span_le = subtree_span.to_le_bytes();
            let addr = bmt_hash_with_span(&span_le, &intermediate_payload)
                .expect("intermediate payload ≤ CHUNK_SIZE (BRANCHES * 32)");
            let mut wire = Vec::with_capacity(SPAN_SIZE + intermediate_payload.len());
            wire.extend_from_slice(&span_le);
            wire.extend_from_slice(&intermediate_payload);
            chunks.push(SplitChunk {
                address: addr,
                wire,
            });
            next.push((addr, subtree_span));
        }
        level = next;
    }

    let (root, total_bytes) = level[0];
    SplitResult {
        root,
        total_bytes,
        chunks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::joiner::{join, JoinError};
    use crate::ChunkFetcher;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::error::Error;

    /// HashMap-backed fetcher so we can round-trip splitter output through
    /// the existing joiner in unit tests, byte-for-byte.
    struct MapFetcher {
        chunks: HashMap<[u8; 32], Vec<u8>>,
    }

    impl MapFetcher {
        fn from_split(result: &SplitResult) -> Self {
            let mut m = HashMap::new();
            for c in &result.chunks {
                m.insert(c.address, c.wire.clone());
            }
            Self { chunks: m }
        }
    }

    #[async_trait]
    impl ChunkFetcher for MapFetcher {
        async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
            self.chunks
                .get(&addr)
                .cloned()
                .ok_or_else(|| -> Box<dyn Error + Send + Sync> {
                    format!("missing chunk {}", hex::encode(addr)).into()
                })
        }
    }

    async fn round_trip(payload: &[u8]) -> Result<Vec<u8>, JoinError> {
        let result = split_bytes(payload);
        let fetcher = MapFetcher::from_split(&result);
        let root_chunk = fetcher.chunks.get(&result.root).expect("root in fetcher");
        join(&fetcher, root_chunk, 64 * 1024 * 1024).await
    }

    /// Tiny payload (< CHUNK_SIZE): one leaf, root == leaf.
    #[tokio::test]
    async fn single_leaf_round_trip() {
        let payload = b"hello swarm".to_vec();
        let result = split_bytes(&payload);
        assert_eq!(result.chunks.len(), 1);
        assert_eq!(result.total_bytes, payload.len() as u64);
        assert_eq!(result.root, result.chunks[0].address);
        let back = round_trip(&payload).await.unwrap();
        assert_eq!(back, payload);
    }

    /// Empty input: still produces one valid leaf — bee accepts zero-byte
    /// files and `cac_new(b"")` is a defined chunk.
    #[tokio::test]
    async fn empty_round_trip() {
        let result = split_bytes(b"");
        assert_eq!(result.chunks.len(), 1);
        assert_eq!(result.total_bytes, 0);
        let back = round_trip(b"").await.unwrap();
        assert!(back.is_empty());
    }

    /// 5 KiB → root has 2 leaf children, first 4 KiB and second 1 KiB.
    /// This is the simplest case where intermediate-vs-leaf logic kicks in.
    #[tokio::test]
    async fn two_leaf_root_round_trip() {
        let payload: Vec<u8> = (0..5120).map(|i| (i % 251) as u8).collect();
        let result = split_bytes(&payload);
        assert_eq!(result.total_bytes, 5120);
        // 2 leaves + 1 intermediate root
        assert_eq!(result.chunks.len(), 3);
        let back = round_trip(&payload).await.unwrap();
        assert_eq!(back, payload);
    }

    /// 4 KiB exactly: still one leaf (boundary).
    #[tokio::test]
    async fn exact_chunk_boundary() {
        let payload: Vec<u8> = (0..CHUNK_SIZE as u32).map(|i| (i % 251) as u8).collect();
        let result = split_bytes(&payload);
        assert_eq!(result.chunks.len(), 1);
        assert_eq!(result.total_bytes, CHUNK_SIZE as u64);
        let back = round_trip(&payload).await.unwrap();
        assert_eq!(back, payload);
    }

    /// Multi-MiB payload triggers two-level fan-out (256 leaves → 2
    /// intermediates → 1 root). Verifies the iterative level builder
    /// closes out properly when the deepest level isn't full.
    #[tokio::test]
    async fn multi_level_fanout_round_trip() {
        let len = 1_000_000usize;
        let payload: Vec<u8> = (0..len).map(|i| ((i * 31) % 251) as u8).collect();
        let result = split_bytes(&payload);
        assert_eq!(result.total_bytes, len as u64);
        // 245 leaves (ceil(1_000_000 / 4096)) + 2 intermediates (groups of
        // 128 + 117 leaves) + 1 root = 248 chunks.
        let leaves = len.div_ceil(CHUNK_SIZE);
        let level1 = leaves.div_ceil(BRANCHES);
        let expected = leaves + level1 + 1;
        assert_eq!(result.chunks.len(), expected);
        let back = round_trip(&payload).await.unwrap();
        assert_eq!(back, payload);
    }

    /// Boundary that bee's splitter explicitly tests: 524288 bytes (128
    /// full leaves). Should produce 128 leaves + 1 root, no second level.
    #[tokio::test]
    async fn full_first_intermediate() {
        let len = CHUNK_SIZE * BRANCHES; // 4096 * 128 = 524288
        let payload: Vec<u8> = (0..len).map(|i| ((i * 7) % 251) as u8).collect();
        let result = split_bytes(&payload);
        assert_eq!(result.chunks.len(), BRANCHES + 1);
        assert_eq!(result.total_bytes, len as u64);
        let back = round_trip(&payload).await.unwrap();
        assert_eq!(back, payload);
    }
}
