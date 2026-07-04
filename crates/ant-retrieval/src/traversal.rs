//! Content-tree traversal — parity with bee `pkg/traversal`.
//!
//! Given a root reference, [`traverse_chunk_addresses`] yields the
//! address of **every** chunk making up the content rooted there:
//!
//! * a valid SOC root is just the single chunk (bee: `soc.Valid` ⇒
//!   `iterFn(addr)` and stop);
//! * a single-chunk root that parses as a mantaray manifest is walked
//!   fork by fork: each manifest node's own chunk tree is enumerated
//!   and every value entry's file tree is enumerated in turn (bee:
//!   `manifest.IterateAddresses` with `processBytes` on node references
//!   *and* entries). Feed manifests are **not** dereferenced — like
//!   bee, the traversal covers the manifest's own metadata nodes, not
//!   whatever content the feed currently points at;
//! * anything else is a plain data span tree: the root, every interior
//!   node, and every leaf (bee: `joiner.IterateChunkAddresses`).
//!
//! Interior/manifest nodes are fetched through the supplied
//! [`ChunkFetcher`] (they must be readable to learn their children);
//! data leaves are *listed but not fetched* — callers that need to
//! prove leaf retrievability (stewardship `isRetrievable`) or re-push
//! leaves (stewardship re-upload) fetch each returned address
//! themselves. Addresses are deduplicated, in discovery order.

use std::collections::HashSet;

use thiserror::Error;

use crate::joiner::{enumerate_chunk_tree, JoinError, JoinOptions};
use crate::mantaray::{ManifestError, Node};
use crate::{join_with_options, ChunkFetcher};
use ant_crypto::{soc_valid, CHUNK_SIZE, SPAN_SIZE};

#[derive(Debug, Error)]
pub enum TraversalError {
    #[error("traversal: {0}")]
    Join(#[from] JoinError),
    #[error("traversal: manifest walk: {0}")]
    Manifest(#[from] ManifestError),
    #[error("traversal: fetch {addr}: {source}")]
    Fetch {
        addr: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Enumerate every chunk address of the content tree rooted at `root`.
/// `max_bytes` bounds each joined file span the same way the joiner's
/// byte cap does (pass a generous ceiling; bee's traversal is uncapped).
pub async fn traverse_chunk_addresses(
    fetcher: &dyn ChunkFetcher,
    root: [u8; 32],
    max_bytes: usize,
) -> Result<Vec<[u8; 32]>, TraversalError> {
    let root_wire = fetcher
        .fetch(root)
        .await
        .map_err(|source| TraversalError::Fetch {
            addr: hex::encode(root),
            source,
        })?;

    // A single-owner chunk has no tree below it (its wrapped CAC is
    // carried inline); bee stops at the SOC address itself.
    if soc_valid(&root, &root_wire) {
        return Ok(vec![root]);
    }

    let mut out = Vec::new();
    let mut emitted = HashSet::new();

    // Manifest heuristic, mirroring bee: only a root whose span fits a
    // single chunk can be a mantaray root node worth probing. Mask the
    // redundancy level off the span's high byte like the joiner does.
    let span = masked_span(&root_wire)?;
    if span <= CHUNK_SIZE as u64 && root_wire.len() >= SPAN_SIZE {
        match Node::unmarshal(&root_wire[SPAN_SIZE..]) {
            Ok(_) => {
                traverse_manifest(fetcher, root, max_bytes, &mut out, &mut emitted).await?;
                return Ok(out);
            }
            // Same fallbacks bee's traversal takes: a version-hash
            // mismatch or too-short blob concludes "not a manifest" and
            // retries plain span-tree processing. Anything else (e.g.
            // an encrypted manifest we can't walk) is a real failure.
            Err(ManifestError::NotAManifest | ManifestError::TooShort(_)) => {}
            Err(e) => return Err(e.into()),
        }
    }

    push_tree(fetcher, root, &root_wire, max_bytes, &mut out, &mut emitted).await?;
    Ok(out)
}

/// Little-endian span with the redundancy-level byte masked off, like
/// `JoinOptions { allow_degraded_redundancy: true }` treats root spans.
fn masked_span(wire: &[u8]) -> Result<u64, TraversalError> {
    let mut span: [u8; SPAN_SIZE] = wire
        .get(..SPAN_SIZE)
        .and_then(|s| s.try_into().ok())
        .ok_or(JoinError::MalformedChunk {
            offset: 0,
            detail: format!("root shorter than span: {} bytes", wire.len()),
        })?;
    span[7] = 0;
    Ok(u64::from_le_bytes(span))
}

/// Enumerate the span tree rooted at `(addr, wire)` into `out`,
/// deduplicating against `emitted`. Interior nodes are fetched; leaves
/// are listed only.
async fn push_tree(
    fetcher: &dyn ChunkFetcher,
    addr: [u8; 32],
    wire: &[u8],
    max_bytes: usize,
    out: &mut Vec<[u8; 32]>,
    emitted: &mut HashSet<[u8; 32]>,
) -> Result<(), TraversalError> {
    let opts = JoinOptions {
        allow_degraded_redundancy: true,
    };
    let inv = enumerate_chunk_tree(fetcher, addr, wire, max_bytes, opts).await?;
    for a in inv.intermediates.into_iter().chain(inv.leaves) {
        if emitted.insert(a) {
            out.push(a);
        }
    }
    Ok(())
}

/// Walk a mantaray trie rooted at the manifest node `root`, emitting
/// every manifest node's own chunk tree and every value entry's file
/// tree, without dereferencing feed metadata. Iterative with a visited
/// set so a (malformed) cyclic manifest can't loop forever.
async fn traverse_manifest(
    fetcher: &dyn ChunkFetcher,
    root: [u8; 32],
    max_bytes: usize,
    out: &mut Vec<[u8; 32]>,
    emitted: &mut HashSet<[u8; 32]>,
) -> Result<(), TraversalError> {
    let opts = JoinOptions {
        allow_degraded_redundancy: true,
    };
    let mut visited = HashSet::new();
    let mut stack = vec![root];
    while let Some(node_ref) = stack.pop() {
        if !visited.insert(node_ref) {
            continue;
        }
        let node_wire = fetcher
            .fetch(node_ref)
            .await
            .map_err(|source| TraversalError::Fetch {
                addr: hex::encode(node_ref),
                source,
            })?;
        // The manifest node is itself a swarm file (usually one chunk);
        // its own chunk tree is part of the content.
        push_tree(fetcher, node_ref, &node_wire, max_bytes, out, emitted).await?;
        let node_bytes = join_with_options(fetcher, &node_wire, max_bytes, opts).await?;
        let node = Node::unmarshal(&node_bytes)?;

        // Value entry: the file data reference (bee skips the all-zero
        // sentinel used by metadata-only nodes, e.g. feed manifests).
        if node.entry.len() == 32 && node.entry.iter().any(|&b| b != 0) {
            let mut entry = [0u8; 32];
            entry.copy_from_slice(&node.entry);
            if !emitted.contains(&entry) {
                let entry_wire =
                    fetcher
                        .fetch(entry)
                        .await
                        .map_err(|source| TraversalError::Fetch {
                            addr: hex::encode(entry),
                            source,
                        })?;
                push_tree(fetcher, entry, &entry_wire, max_bytes, out, emitted).await?;
            }
        }

        for fork in node.forks.values() {
            if fork.child_ref != [0u8; 32] {
                stack.push(fork.child_ref);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest_writer::build_single_file_manifest;
    use crate::splitter::split_bytes;
    use std::collections::HashMap;
    use std::error::Error as StdError;
    use std::sync::Mutex;

    struct MapFetcher {
        chunks: Mutex<HashMap<[u8; 32], Vec<u8>>>,
    }

    impl MapFetcher {
        fn new() -> Self {
            Self {
                chunks: Mutex::new(HashMap::new()),
            }
        }
        fn insert(&self, addr: [u8; 32], wire: Vec<u8>) {
            self.chunks.lock().unwrap().insert(addr, wire);
        }
        fn remove(&self, addr: &[u8; 32]) {
            self.chunks.lock().unwrap().remove(addr);
        }
    }

    #[async_trait::async_trait]
    impl ChunkFetcher for MapFetcher {
        async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
            self.chunks
                .lock()
                .unwrap()
                .get(&addr)
                .cloned()
                .ok_or_else(|| format!("chunk {} not found", hex::encode(addr)).into())
        }
    }

    const CAP: usize = 64 * 1024 * 1024;

    /// A single-chunk `/bytes` upload traverses to exactly its root.
    #[tokio::test]
    async fn single_chunk_bytes() {
        let store = MapFetcher::new();
        let split = split_bytes(b"tiny payload");
        for c in &split.chunks {
            store.insert(c.address, c.wire.clone());
        }
        let addrs = traverse_chunk_addresses(&store, split.root, CAP)
            .await
            .unwrap();
        assert_eq!(addrs, vec![split.root]);
    }

    /// A multi-chunk `/bytes` tree yields the root, every interior
    /// node, and every leaf — exactly the set the splitter produced.
    #[tokio::test]
    async fn multi_chunk_bytes_lists_whole_tree() {
        let store = MapFetcher::new();
        let body = vec![0xabu8; 4096 * 3 + 100];
        let split = split_bytes(&body);
        for c in &split.chunks {
            store.insert(c.address, c.wire.clone());
        }
        let addrs = traverse_chunk_addresses(&store, split.root, CAP)
            .await
            .unwrap();
        let got: HashSet<_> = addrs.iter().copied().collect();
        let want: HashSet<_> = split.chunks.iter().map(|c| c.address).collect();
        assert_eq!(got, want);
        // The repeated-content leaves share an address; the traversal
        // output is deduplicated.
        assert_eq!(addrs.len(), got.len(), "no duplicates");
    }

    /// A single-file mantaray manifest traverses to every manifest node
    /// *and* the file's data tree.
    #[tokio::test]
    async fn manifest_traversal_covers_nodes_and_data() {
        let store = MapFetcher::new();
        let body = vec![0x5au8; 4096 * 2 + 17];
        let split = split_bytes(&body);
        for c in &split.chunks {
            store.insert(c.address, c.wire.clone());
        }
        let manifest = build_single_file_manifest("hello.txt", Some("text/plain"), split.root)
            .expect("manifest");
        for c in &manifest.chunks {
            store.insert(c.address, c.wire.clone());
        }
        let addrs = traverse_chunk_addresses(&store, manifest.root, CAP)
            .await
            .unwrap();
        let got: HashSet<_> = addrs.iter().copied().collect();
        for c in &manifest.chunks {
            assert!(got.contains(&c.address), "manifest node missing");
        }
        for c in &split.chunks {
            assert!(got.contains(&c.address), "data chunk missing");
        }
    }

    /// A missing *interior* node fails the traversal (its children are
    /// unknowable); a missing *leaf* does not — leaves are listed from
    /// their parents without being fetched, and it's the caller's probe
    /// that discovers they're gone.
    #[tokio::test]
    async fn missing_interior_fails_missing_leaf_lists() {
        let store = MapFetcher::new();
        let body = vec![0x11u8; 4096 * 200];
        let split = split_bytes(&body);
        for c in &split.chunks {
            store.insert(c.address, c.wire.clone());
        }
        // Drop one leaf: traversal still lists it.
        let leaf = split.chunks[0].address;
        assert_ne!(leaf, split.root);
        store.remove(&leaf);
        let addrs = traverse_chunk_addresses(&store, split.root, CAP)
            .await
            .unwrap();
        assert!(addrs.contains(&leaf));

        // Drop an interior node: with 200 leaves the tree has a second
        // level, so some non-root chunk declares a span > one chunk.
        // Removing it makes its subtree unenumerable.
        let interior = split
            .chunks
            .iter()
            .find(|c| {
                if c.address == split.root || c.wire.len() < 8 {
                    return false;
                }
                let mut span = [0u8; 8];
                span.copy_from_slice(&c.wire[..8]);
                u64::from_le_bytes(span) > 4096
            })
            .map(|c| c.address)
            .expect("a non-root interior node");
        store.remove(&interior);
        assert!(traverse_chunk_addresses(&store, split.root, CAP)
            .await
            .is_err());
    }

    /// Missing root is an error the callers map to `isRetrievable:
    /// false`.
    #[tokio::test]
    async fn missing_root_errors() {
        let store = MapFetcher::new();
        let err = traverse_chunk_addresses(&store, [0xee; 32], CAP)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }
}
