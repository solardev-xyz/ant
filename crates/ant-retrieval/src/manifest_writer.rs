//! Mantaray v0.2 manifest writer.
//!
//! The inverse of [`crate::mantaray`]. Builds the chunks that bee's
//! `mantaray.NodeMarshalBinary` would emit, given a list of files
//! already split into chunk trees by [`crate::splitter::split_bytes`].
//!
//! Wire shape (de-obfuscated; we always emit the all-zero obfuscation
//! key, which matches what `swarm-cli upload` produces for unencrypted
//! manifests):
//!
//! ```text
//! [0..32]   obfuscation key (zeros)
//! [32..63]  31-byte version hash for "mantaray:0.2"
//! [63]      ref-bytes-size (0x20 if the node has any forks or its own
//!           entry; 0x00 only for empty placeholder leaves)
//! [64..96]  entry (32 bytes: data ref, or zeros for nodes that only
//!           anchor metadata-bearing forks). Omitted when ref-size is 0.
//! [+0..+32] 256-bit forks bitmap; bit `b` set ⇒ a fork starts on byte b
//! [forks…]  ascending bit order; for each set bit:
//!             - 1 byte  type      (2=value, 4=edge, 8=path-separator,
//!                                  16=with-metadata; bitwise-or)
//!             - 1 byte  prefix-len (1..30)
//!             - 30 bytes prefix    (only the first prefix-len matter)
//!             - 32 bytes child reference
//!             - if (type & 16): 2 BE bytes meta length, then JSON
//!               metadata padded with '\n' so that
//!               (meta_len + 2) is a multiple of 32
//! ```
//!
//! The fork-as-a-whole alignment is important: a parser that wants to
//! seek forward fork-by-fork relies on each fork occupying a whole
//! number of 32-byte blocks.
//!
//! Manifests are emitted as a proper recursive trie: every node fits in
//! one CAC chunk, and large collections naturally deepen the tree
//! instead of overflowing a single root chunk. A flat 12-file collection
//! lands in a single root chunk; a 200-file site distributes across
//! ~20–40 chunks (root + per-prefix sub-tries). Path segments longer
//! than [`MAX_FILENAME_BYTES`] are split into chained 30-byte forks
//! transparently — bee's reader walks the chain identically to a
//! single-fork prefix.

use std::collections::{BTreeMap, BTreeSet};

use ant_crypto::{cac_new, CHUNK_SIZE};
use thiserror::Error;

use crate::splitter::SplitChunk;
use crate::{ChunkAddr, MANTARAY_CONTENT_TYPE_KEY, MANTARAY_INDEX_DOC_KEY};

/// 31 leading bytes of `keccak256("mantaray:0.2")`. Bee writes only the
/// first 31 bytes on the wire (`versionHashSize = 31`).
const VERSION_HASH_31: [u8; 31] = [
    0x57, 0x68, 0xb3, 0xb6, 0xa7, 0xdb, 0x56, 0xd2, 0x1d, 0x1a, 0xbf, 0xf4, 0x0d, 0x41, 0xce, 0xbf,
    0xc8, 0x34, 0x48, 0xfe, 0xd8, 0xd7, 0xe9, 0xb0, 0x6e, 0xc0, 0xd3, 0xb0, 0x73, 0xf2, 0x8f,
];

const NODE_TYPE_VALUE: u8 = 2;
const NODE_TYPE_PATH_SEPARATOR: u8 = 8;
const NODE_TYPE_WITH_METADATA: u8 = 16;

/// Bee's hard cap on per-fork prefix length: 30 bytes (the on-wire
/// prefix slot is a fixed 30-byte zero-padded field). Path segments
/// longer than this are emitted as chained 30-byte forks with
/// synthetic single-fork intermediate nodes — bee's reader walks the
/// chain identically to a flat prefix.
pub const MAX_FILENAME_BYTES: usize = 30;

/// One file to add to a collection manifest.
#[derive(Debug, Clone)]
pub struct ManifestFile {
    /// Path inside the manifest (no leading slash). May contain slashes
    /// to nest into subdirectories. Each path segment must be ≤
    /// [`MAX_FILENAME_BYTES`] bytes.
    pub path: String,
    /// MIME type written into the file fork's metadata. `None` falls
    /// back to `application/octet-stream`, matching bee's default.
    pub content_type: Option<String>,
    /// Address of the file's root chunk — usually
    /// [`crate::splitter::SplitResult::root`].
    pub data_ref: ChunkAddr,
}

/// Result of building a manifest: the manifest root reference, plus
/// every chunk that has to be pushed to Swarm to make the manifest
/// retrievable. The root chunk is always the *last* entry.
#[derive(Debug, Clone)]
pub struct ManifestWriteResult {
    pub root: ChunkAddr,
    pub chunks: Vec<SplitChunk>,
}

#[derive(Debug, Error)]
pub enum ManifestWriteError {
    /// A path segment exceeds the 30-byte fork-prefix limit. Today bee
    /// uses chained forks for long names; we don't yet emit those.
    #[error("path segment too long: {segment} bytes (cap {cap})")]
    SegmentTooLong { segment: usize, cap: usize },
    /// Empty path was passed in a multi-file collection. The single-file
    /// helper assigns its own filename, so this only ever fires for the
    /// collection variant.
    #[error("empty path is not allowed in a manifest")]
    EmptyPath,
    /// The serialised root manifest exceeds [`CHUNK_SIZE`]. Today this
    /// happens only for collections with very many or very long names;
    /// the proper fix is to split the manifest itself across an
    /// intermediate join chunk, which is future work.
    #[error("manifest root payload {size} bytes exceeds chunk capacity {cap}; multi-chunk manifests not yet supported")]
    ManifestTooBig { size: usize, cap: usize },
}

/// Build a single-file manifest with a `website-index-document` pointing
/// at `filename`, so both `bzz://<root>/<filename>` and the bare
/// `bzz://<root>/` resolve to the same data. Mirrors what
/// `swarm-cli upload <file>` produces.
///
/// `data_ref` is the file's root chunk address, typically the `root`
/// field of a [`crate::splitter::SplitResult`]. The file's *data* chunks
/// are **not** included in the returned `chunks`; the caller is
/// responsible for pushing those (the splitter already returns them).
pub fn build_single_file_manifest(
    filename: &str,
    content_type: Option<&str>,
    data_ref: ChunkAddr,
) -> Result<ManifestWriteResult, ManifestWriteError> {
    build_collection_manifest(
        &[ManifestFile {
            path: filename.to_string(),
            content_type: content_type.map(str::to_string),
            data_ref,
        }],
        Some(filename),
    )
}

/// Build a manifest covering many files. `files` is a list of distinct
/// paths and their pre-split data references. If any path equals
/// `index_document` (typically `"index.html"`), the root carries a
/// `website-index-document` metadata pointer so `bzz://<root>/` serves
/// that file.
///
/// The writer constructs a real radix trie: every node serializes to
/// exactly one CAC chunk, and large collections naturally deepen the
/// tree instead of overflowing the root. Path segments longer than
/// [`MAX_FILENAME_BYTES`] are emitted as chained 30-byte forks with
/// synthetic single-fork intermediate nodes — bee's reader walks the
/// chain identically. Returns
/// [`ManifestWriteError::ManifestTooBig`] only in the pathological
/// case where one trie level has so many forks (with metadata) that a
/// single node exceeds [`CHUNK_SIZE`]; in practice this requires
/// 50+ siblings sharing a prefix at the same trie depth.
pub fn build_collection_manifest(
    files: &[ManifestFile],
    index_document: Option<&str>,
) -> Result<ManifestWriteResult, ManifestWriteError> {
    if files.is_empty() {
        return Err(ManifestWriteError::EmptyPath);
    }
    let mut seen_paths: BTreeSet<&str> = BTreeSet::new();
    for f in files {
        if f.path.is_empty() {
            return Err(ManifestWriteError::EmptyPath);
        }
        if !seen_paths.insert(f.path.as_str()) {
            // Duplicate path. We treat it as an empty-path error —
            // there's no clean way to "merge" two distinct data refs
            // at the same logical location.
            return Err(ManifestWriteError::EmptyPath);
        }
    }

    // Build the in-memory trie.
    let mut root = TrieNode::default();
    for f in files {
        let ct = f
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string());
        trie_insert(
            &mut root,
            f.path.as_bytes(),
            f.data_ref,
            file_metadata(&ct, &f.path),
        )?;
    }

    // Index-document anchor on the synthetic '/' fork at the root.
    if let Some(idx) = index_document {
        // bee's bzzUploadHandler always sets the '/' fork's metadata
        // to website-index-document, regardless of whether the named
        // file exists in the manifest. If it doesn't, bee returns 404
        // when bzz://<root>/ is hit; we mirror that exactly.
        let mut idx_node = TrieNode::default();
        idx_node.value = Some(EmptyValue);
        let fork = TrieFork {
            prefix: b"/".to_vec(),
            child: Box::new(idx_node),
            metadata_override: Some(index_doc_metadata(idx)),
            path_separator: true,
        };
        root.forks.insert(b'/', fork);
    }

    // Serialize bottom-up.
    let mut chunks: Vec<SplitChunk> = Vec::new();
    let root_addr = serialize_node(&root, &mut chunks)?;
    Ok(ManifestWriteResult {
        root: root_addr,
        chunks,
    })
}

// --- trie data model + insertion ---

/// Marker for the "/" placeholder leaf used to anchor the
/// website-index-document metadata. Carries no data of its own.
#[derive(Clone)]
struct EmptyValue;

#[derive(Default)]
struct TrieNode {
    /// File path that ends exactly at this node, if any. The data ref
    /// will be written to the node's `entry` slot at serialization
    /// time; the matching metadata lives on the parent fork.
    own_data_ref: Option<ChunkAddr>,
    own_metadata: BTreeMap<String, String>,
    /// Marker present iff this node represents the empty-stub fork
    /// for the website-index-document anchor. Distinct from
    /// `own_data_ref` because the empty stub carries refsize=0x00,
    /// not 0x20+entry.
    value: Option<EmptyValue>,
    /// Outgoing forks keyed by the byte that distinguishes them.
    forks: BTreeMap<u8, TrieFork>,
}

struct TrieFork {
    /// Bytes consumed by following this fork (prefix length 1..=30).
    prefix: Vec<u8>,
    child: Box<TrieNode>,
    /// Override the fork's metadata block at serialization. Only used
    /// for the synthetic '/' index-document fork; everywhere else the
    /// metadata is read from `child.own_metadata`.
    metadata_override: Option<BTreeMap<String, String>>,
    path_separator: bool,
}

/// Insert `path → data_ref` into `node`. Splits or extends forks as
/// needed and chains 30-byte forks for paths whose remaining suffix
/// exceeds the per-fork prefix cap.
fn trie_insert(
    node: &mut TrieNode,
    path: &[u8],
    data_ref: ChunkAddr,
    metadata: BTreeMap<String, String>,
) -> Result<(), ManifestWriteError> {
    if path.is_empty() {
        node.own_data_ref = Some(data_ref);
        node.own_metadata = metadata;
        return Ok(());
    }
    let first = path[0];
    // Take the existing fork (if any) by removing it; we may rebuild
    // it before re-inserting. Borrow-checker friendly.
    if let Some(mut fork) = node.forks.remove(&first) {
        let lcp = longest_common_prefix(
            [fork.prefix.as_slice(), path].iter().copied(),
        );
        if lcp.len() == fork.prefix.len() {
            // Whole existing prefix matches; descend with the
            // remaining tail.
            trie_insert(&mut fork.child, &path[lcp.len()..], data_ref, metadata)?;
            node.forks.insert(first, fork);
        } else {
            // Split: create an intermediate node holding the old
            // child under the suffix it kept, plus the new branch.
            let old_suffix: Vec<u8> = fork.prefix[lcp.len()..].to_vec();
            let mut intermediate = TrieNode::default();
            // Move the old subtree under a fork keyed on
            // old_suffix[0]. Its metadata still hangs off the
            // existing fork's metadata_override (if any) — but
            // because fork.metadata_override is only set for the
            // index-doc placeholder which we never split, this is
            // None and we keep it None on the intermediate fork too.
            let old_path_sep = old_suffix.contains(&b'/');
            intermediate.forks.insert(
                old_suffix[0],
                TrieFork {
                    prefix: old_suffix,
                    child: std::mem::take(&mut fork.child),
                    metadata_override: fork.metadata_override.take(),
                    path_separator: old_path_sep,
                },
            );
            // Insert the new tail into the intermediate.
            trie_insert(
                &mut intermediate,
                &path[lcp.len()..],
                data_ref,
                metadata,
            )?;
            // Replace the original fork with one truncated to LCP.
            let lcp_path_sep = lcp.contains(&b'/');
            node.forks.insert(
                first,
                TrieFork {
                    prefix: lcp,
                    child: Box::new(intermediate),
                    metadata_override: None,
                    path_separator: lcp_path_sep,
                },
            );
        }
        Ok(())
    } else {
        // No fork starts with this byte yet; create one. If `path` is
        // longer than the per-fork cap, chain through synthetic
        // intermediates so each fork carries at most 30 bytes.
        let (head, tail) = path.split_at(path.len().min(MAX_FILENAME_BYTES));
        if tail.is_empty() {
            let mut leaf = TrieNode::default();
            leaf.own_data_ref = Some(data_ref);
            leaf.own_metadata = metadata;
            node.forks.insert(
                first,
                TrieFork {
                    prefix: head.to_vec(),
                    child: Box::new(leaf),
                    metadata_override: None,
                    path_separator: head.contains(&b'/'),
                },
            );
        } else {
            // Build a child intermediate that holds the tail, then
            // recurse into it with the same data_ref/metadata.
            let mut intermediate = TrieNode::default();
            trie_insert(&mut intermediate, tail, data_ref, metadata)?;
            node.forks.insert(
                first,
                TrieFork {
                    prefix: head.to_vec(),
                    child: Box::new(intermediate),
                    metadata_override: None,
                    path_separator: head.contains(&b'/'),
                },
            );
        }
        Ok(())
    }
}

// --- serialization ---

/// Recursively serialize `node` and all descendants. Returns the
/// node's CAC address; appends every produced chunk to `out` in
/// post-order (children before parents) so callers that push chunks
/// sequentially can rely on dependents already being on the
/// network when each chunk is pushed. The caller still needs to push
/// the *entire* slice — bee's pushsync doesn't backtrack.
fn serialize_node(
    node: &TrieNode,
    out: &mut Vec<SplitChunk>,
) -> Result<ChunkAddr, ManifestWriteError> {
    // Resolve each fork's child address first.
    let mut child_refs: BTreeMap<u8, ChunkAddr> = BTreeMap::new();
    for (&k, fork) in &node.forks {
        let child_addr = serialize_node(&fork.child, out)?;
        child_refs.insert(k, child_addr);
    }

    let payload = serialize_node_payload(node, &child_refs)?;
    let (addr, wire) = cac_payload(&payload);
    out.push(SplitChunk {
        address: addr,
        wire,
    });
    Ok(addr)
}

/// Serialize one node into its on-wire bytes (de-obfuscated).
fn serialize_node_payload(
    node: &TrieNode,
    child_refs: &BTreeMap<u8, ChunkAddr>,
) -> Result<Vec<u8>, ManifestWriteError> {
    let mut p = Vec::with_capacity(128);
    p.extend_from_slice(&[0u8; 32]); // obf key
    p.extend_from_slice(&VERSION_HASH_31);

    // refsize: 0x20 if the node has any forks (forks store 32-byte
    // child refs) or its own data_ref; 0x00 only for the empty
    // index-doc placeholder leaf.
    let needs_ref = node.own_data_ref.is_some() || !node.forks.is_empty();
    if needs_ref {
        p.push(0x20);
        match node.own_data_ref {
            Some(ref a) => p.extend_from_slice(a),
            None => p.extend_from_slice(&[0u8; 32]),
        }
    } else {
        p.push(0x00);
    }

    // Forks bitmap.
    let mut bitmap = [0u8; 32];
    for byte in node.forks.keys() {
        bitmap[(*byte as usize) / 8] |= 1 << (*byte % 8);
    }
    p.extend_from_slice(&bitmap);

    // Forks in ascending order (BTreeMap preserves this).
    for (k, fork) in &node.forks {
        // Resolve the metadata: explicit override (only set on the
        // index-doc placeholder) wins; otherwise read from the child
        // node's own_metadata if it carries a file value. A purely
        // structural intermediate has empty metadata.
        let meta = if let Some(m) = &fork.metadata_override {
            m.clone()
        } else if fork.child.own_data_ref.is_some() {
            fork.child.own_metadata.clone()
        } else {
            BTreeMap::new()
        };
        let child_ref = *child_refs
            .get(k)
            .expect("child_refs populated for every fork above");
        let bytes = serialize_fork_to_bytes(
            &fork.prefix,
            &child_ref,
            &meta,
            fork.path_separator,
        )?;
        p.extend_from_slice(&bytes);
    }

    if p.len() > CHUNK_SIZE {
        return Err(ManifestWriteError::ManifestTooBig {
            size: p.len(),
            cap: CHUNK_SIZE,
        });
    }
    Ok(p)
}

fn serialize_fork_to_bytes(
    prefix: &[u8],
    child_ref: &ChunkAddr,
    metadata: &BTreeMap<String, String>,
    path_separator: bool,
) -> Result<Vec<u8>, ManifestWriteError> {
    if prefix.is_empty() || prefix.len() > MAX_FILENAME_BYTES {
        return Err(ManifestWriteError::SegmentTooLong {
            segment: prefix.len(),
            cap: MAX_FILENAME_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(64);
    let mut node_type = NODE_TYPE_VALUE;
    if !metadata.is_empty() {
        node_type |= NODE_TYPE_WITH_METADATA;
    }
    if path_separator {
        node_type |= NODE_TYPE_PATH_SEPARATOR;
    }
    bytes.push(node_type);
    bytes.push(prefix.len() as u8);
    bytes.extend_from_slice(prefix);
    bytes.extend(std::iter::repeat(0u8).take(MAX_FILENAME_BYTES - prefix.len()));
    bytes.extend_from_slice(child_ref);
    if !metadata.is_empty() {
        let meta_json = encode_metadata(metadata);
        let pad = match (meta_json.len() + 2) % 32 {
            0 => 0,
            r => 32 - r,
        };
        let mut padded = meta_json.into_bytes();
        padded.extend(std::iter::repeat(b'\n').take(pad));
        let meta_len = padded.len() as u16;
        bytes.extend_from_slice(&meta_len.to_be_bytes());
        bytes.extend_from_slice(&padded);
    }
    Ok(bytes)
}

// --- internal helpers ---

/// Stable JSON encoding for mantaray fork metadata: flat string→string
/// object, BTreeMap order, no whitespace, escape `"` and `\`. Bee
/// itself uses `encoding/json` which produces the same wire bytes for
/// the keys and values we ever emit (no nested objects, no Unicode
/// surrogates, no NUL bytes).
fn encode_metadata(meta: &BTreeMap<String, String>) -> String {
    let mut out = String::from("{");
    let mut first = true;
    for (k, v) in meta {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        push_json_escaped(&mut out, k);
        out.push_str("\":\"");
        push_json_escaped(&mut out, v);
        out.push('"');
    }
    out.push('}');
    out
}

fn push_json_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                write!(out, "\\u{:04x}", c as u32).expect("writing to String");
            }
            c => out.push(c),
        }
    }
}

fn index_doc_metadata(filename: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(MANTARAY_INDEX_DOC_KEY.to_string(), filename.to_string());
    m
}

fn file_metadata(content_type: &str, filename: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(
        MANTARAY_CONTENT_TYPE_KEY.to_string(),
        content_type.to_string(),
    );
    m.insert("Filename".to_string(), filename.to_string());
    m
}

/// CAC-wrap a manifest node payload (always one chunk; payload size is
/// already validated against [`CHUNK_SIZE`] in [`serialize_node_payload`]).
fn cac_payload(payload: &[u8]) -> (ChunkAddr, Vec<u8>) {
    cac_new(payload).expect("manifest node payload pre-validated against CHUNK_SIZE")
}

fn longest_common_prefix<'a, I>(it: I) -> Vec<u8>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut iter = it.into_iter();
    let first = match iter.next() {
        Some(s) => s.to_vec(),
        None => return Vec::new(),
    };
    let mut max_len = first.len();
    for s in iter {
        let mut common = 0;
        for (a, b) in first.iter().zip(s.iter()) {
            if a == b {
                common += 1;
            } else {
                break;
            }
        }
        max_len = max_len.min(common);
        if max_len == 0 {
            return Vec::new();
        }
    }
    first[..max_len].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mantaray::{list_manifest, lookup_path};
    use crate::ChunkFetcher;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::error::Error;

    struct MapFetcher {
        chunks: HashMap<[u8; 32], Vec<u8>>,
    }

    impl MapFetcher {
        fn new() -> Self {
            Self {
                chunks: HashMap::new(),
            }
        }
        fn ingest(&mut self, c: &SplitChunk) {
            self.chunks.insert(c.address, c.wire.clone());
        }
    }

    #[async_trait]
    impl ChunkFetcher for MapFetcher {
        async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
            self.chunks
                .get(&addr)
                .cloned()
                .ok_or_else(|| -> Box<dyn Error + Send + Sync> {
                    format!("missing {}", hex::encode(addr)).into()
                })
        }
    }

    /// End-to-end: split + manifest a small text file, walk the
    /// manifest with the existing reader, expect to recover the file's
    /// data ref + content-type. This is the same shape the gateway
    /// will exercise for `POST /bzz?name=hello.txt`.
    #[tokio::test]
    async fn single_file_round_trip() {
        let body = b"uploaded via ant\n".to_vec();
        let split = crate::splitter::split_bytes(&body);
        let manifest = build_single_file_manifest(
            "hello.txt",
            Some("text/plain; charset=utf-8"),
            split.root,
        )
        .unwrap();

        let mut fetcher = MapFetcher::new();
        for c in &split.chunks {
            fetcher.ingest(c);
        }
        for c in &manifest.chunks {
            fetcher.ingest(c);
        }

        // /bzz/<root>/hello.txt → should resolve to the file body's data ref.
        let lookup = lookup_path(&fetcher, manifest.root, "hello.txt")
            .await
            .unwrap();
        assert_eq!(lookup.data_ref, split.root);
        assert_eq!(
            lookup.content_type.as_deref(),
            Some("text/plain; charset=utf-8"),
        );
        assert_eq!(
            lookup.metadata.get("Filename").map(String::as_str),
            Some("hello.txt"),
        );

        // Index-document fallback: bare /bzz/<root>/ resolves to the same.
        let idx = lookup_path(&fetcher, manifest.root, "").await.unwrap();
        assert_eq!(idx.data_ref, split.root);
    }

    /// Multi-file collection with an index-document. Verify each path
    /// resolves to its own data ref and the bare path returns the index.
    #[tokio::test]
    async fn collection_round_trip() {
        let mut fetcher = MapFetcher::new();
        let mut files: Vec<ManifestFile> = Vec::new();
        let bodies = [
            ("index.html", "text/html", &b"<html>hi</html>"[..]),
            ("style.css", "text/css", &b"body{}"[..]),
            ("script.js", "application/javascript", &b"alert(1)"[..]),
        ];
        let mut data_refs: HashMap<&str, [u8; 32]> = HashMap::new();
        for (name, ct, body) in &bodies {
            let split = crate::splitter::split_bytes(body);
            for c in &split.chunks {
                fetcher.ingest(c);
            }
            data_refs.insert(name, split.root);
            files.push(ManifestFile {
                path: name.to_string(),
                content_type: Some(ct.to_string()),
                data_ref: split.root,
            });
        }
        let manifest =
            build_collection_manifest(&files, Some("index.html")).expect("build manifest");
        for c in &manifest.chunks {
            fetcher.ingest(c);
        }

        for (name, ct, _body) in &bodies {
            let r = lookup_path(&fetcher, manifest.root, name).await.unwrap();
            assert_eq!(&r.data_ref, data_refs.get(name).unwrap(), "name {name}");
            assert_eq!(r.content_type.as_deref(), Some(*ct));
        }
        // Index-document fallback returns the index file's data.
        let r = lookup_path(&fetcher, manifest.root, "").await.unwrap();
        assert_eq!(r.data_ref, *data_refs.get("index.html").unwrap());

        // Listing should surface every path (note: list_manifest returns
        // metadata-only entries too — index-document fork shows up).
        let listing = list_manifest(&fetcher, manifest.root).await.unwrap();
        let paths: BTreeSet<&str> = listing.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains("index.html"));
        assert!(paths.contains("style.css"));
        assert!(paths.contains("script.js"));
    }

    /// Filename with a slash gets the path-separator type bit set on its
    /// fork — bee's listing format uses this to render `dir/` listings.
    /// We just check the round-trip still resolves cleanly.
    #[tokio::test]
    async fn nested_path_round_trip() {
        let body = b"sub-page".to_vec();
        let split = crate::splitter::split_bytes(&body);
        let manifest = build_collection_manifest(
            &[ManifestFile {
                path: "blog/post".to_string(),
                content_type: Some("text/html".to_string()),
                data_ref: split.root,
            }],
            None,
        )
        .unwrap();
        let mut fetcher = MapFetcher::new();
        for c in &split.chunks {
            fetcher.ingest(c);
        }
        for c in &manifest.chunks {
            fetcher.ingest(c);
        }
        let r = lookup_path(&fetcher, manifest.root, "blog/post").await.unwrap();
        assert_eq!(r.data_ref, split.root);
    }

    /// Path segments longer than the 30-byte fork prefix cap are
    /// chained transparently. End-to-end: write a 90-character
    /// filename, then walk it back via the reader.
    #[tokio::test]
    async fn long_filename_chains_and_round_trips() {
        let long = "x".repeat(90);
        let body = b"long-name body".to_vec();
        let split = crate::splitter::split_bytes(&body);
        let manifest =
            build_single_file_manifest(&long, Some("text/plain"), split.root).unwrap();
        let mut fetcher = MapFetcher::new();
        for c in &split.chunks {
            fetcher.ingest(c);
        }
        for c in &manifest.chunks {
            fetcher.ingest(c);
        }
        let r = lookup_path(&fetcher, manifest.root, &long).await.unwrap();
        assert_eq!(r.data_ref, split.root);
        assert_eq!(r.content_type.as_deref(), Some("text/plain"));
    }

    /// Every chunk emitted by the trie writer must be a valid CAC so
    /// bee gateways accept it on push. Quick structural check across a
    /// few-file collection (root + per-fork sub-tries).
    #[test]
    fn every_emitted_chunk_is_valid_cac() {
        let files: Vec<ManifestFile> = ["a.txt", "b.txt", "ab.txt"]
            .iter()
            .map(|p| ManifestFile {
                path: (*p).to_string(),
                content_type: Some("text/plain".to_string()),
                data_ref: [0u8; 32],
            })
            .collect();
        let manifest = build_collection_manifest(&files, None).unwrap();
        for c in &manifest.chunks {
            assert!(
                ant_crypto::cac_valid(&c.address, &c.wire),
                "chunk {} not a valid CAC",
                hex::encode(c.address),
            );
        }
        // Root must be the last chunk emitted (post-order).
        assert_eq!(manifest.chunks.last().unwrap().address, manifest.root);
    }

    /// Many shared-prefix files trigger an actual multi-chunk
    /// manifest: the trie deepens and the writer emits ≥ 2 manifest
    /// chunks. Verifies every file resolves and that the writer no
    /// longer errors with `ManifestTooBig` on collections that the
    /// old flat encoder couldn't fit.
    #[tokio::test]
    async fn deep_collection_emits_multiple_chunks_and_round_trips() {
        // 64 files, all sharing the prefix "blog/posts/" then a
        // 5-char unique suffix. Long enough metadata (Filename) that
        // packing them all into one fork-bitmap would overflow a
        // single 4 KiB chunk under the old writer.
        let mut files: Vec<ManifestFile> = Vec::new();
        let mut data_refs: HashMap<String, [u8; 32]> = HashMap::new();
        let mut fetcher = MapFetcher::new();
        for i in 0..64u8 {
            let path = format!("blog/posts/post-{:02x}", i);
            let body = format!("body-{i}").into_bytes();
            let split = crate::splitter::split_bytes(&body);
            for c in &split.chunks {
                fetcher.ingest(c);
            }
            data_refs.insert(path.clone(), split.root);
            files.push(ManifestFile {
                path,
                content_type: Some("text/plain".to_string()),
                data_ref: split.root,
            });
        }
        let manifest = build_collection_manifest(&files, None).unwrap();
        for c in &manifest.chunks {
            fetcher.ingest(c);
        }
        // The trie should have deepened — expect at least 2 manifest
        // chunks (root + at least one sub-trie node).
        assert!(
            manifest.chunks.len() >= 2,
            "deep collection should emit multiple manifest chunks; got {}",
            manifest.chunks.len(),
        );

        // Every file path resolves to its own data ref.
        for (path, want_ref) in &data_refs {
            let r = lookup_path(&fetcher, manifest.root, path).await.unwrap();
            assert_eq!(&r.data_ref, want_ref, "path {path}");
        }
    }

    /// Insertion order must not change the manifest root. The trie
    /// builder sorts forks by byte at every node (BTreeMap), so two
    /// permutations of the same file set hash to the same root.
    #[test]
    fn insertion_order_does_not_change_root() {
        let make_file = |name: &str, ref_byte: u8| ManifestFile {
            path: name.to_string(),
            content_type: Some("text/plain".to_string()),
            data_ref: [ref_byte; 32],
        };
        let forward = vec![
            make_file("apple", 1),
            make_file("apricot", 2),
            make_file("banana", 3),
            make_file("blueberry", 4),
            make_file("cherry", 5),
        ];
        let reversed: Vec<ManifestFile> = forward.iter().rev().cloned().collect();

        let m1 = build_collection_manifest(&forward, None).unwrap();
        let m2 = build_collection_manifest(&reversed, None).unwrap();
        assert_eq!(m1.root, m2.root);
    }

    /// Adding a file whose path is a prefix of another already in the
    /// manifest puts the prefix file's value at the parent node and
    /// the longer path under a new fork. Walks both back correctly.
    #[tokio::test]
    async fn prefix_overlap_round_trip() {
        let mut fetcher = MapFetcher::new();
        let body_a = b"abc".to_vec();
        let body_b = b"abc/d".to_vec();
        let split_a = crate::splitter::split_bytes(&body_a);
        let split_b = crate::splitter::split_bytes(&body_b);
        for c in &split_a.chunks {
            fetcher.ingest(c);
        }
        for c in &split_b.chunks {
            fetcher.ingest(c);
        }

        let manifest = build_collection_manifest(
            &[
                ManifestFile {
                    path: "abc".to_string(),
                    content_type: Some("text/plain".to_string()),
                    data_ref: split_a.root,
                },
                ManifestFile {
                    path: "abc/d".to_string(),
                    content_type: Some("text/html".to_string()),
                    data_ref: split_b.root,
                },
            ],
            None,
        )
        .unwrap();
        for c in &manifest.chunks {
            fetcher.ingest(c);
        }

        let ra = lookup_path(&fetcher, manifest.root, "abc").await.unwrap();
        assert_eq!(ra.data_ref, split_a.root);
        assert_eq!(ra.content_type.as_deref(), Some("text/plain"));
        let rb = lookup_path(&fetcher, manifest.root, "abc/d").await.unwrap();
        assert_eq!(rb.data_ref, split_b.root);
        assert_eq!(rb.content_type.as_deref(), Some("text/html"));
    }

    /// Duplicate paths in the input list are rejected — there's no
    /// reasonable resolution if the same logical path points at two
    /// different data refs.
    #[test]
    fn duplicate_paths_rejected() {
        let f = ManifestFile {
            path: "dup.txt".to_string(),
            content_type: None,
            data_ref: [0u8; 32],
        };
        let err = build_collection_manifest(&[f.clone(), f], None).unwrap_err();
        assert!(matches!(err, ManifestWriteError::EmptyPath));
    }

    /// JSON encoder produces strict, deterministic output for the keys
    /// bee actually uses. Important because byte-equal output keeps
    /// bee-shaped fixtures stable.
    #[test]
    fn metadata_encoding_is_stable() {
        let mut m = BTreeMap::new();
        m.insert("Content-Type".to_string(), "text/plain".to_string());
        m.insert("Filename".to_string(), "hello.txt".to_string());
        let s = encode_metadata(&m);
        assert_eq!(
            s,
            r#"{"Content-Type":"text/plain","Filename":"hello.txt"}"#
        );
    }
}
