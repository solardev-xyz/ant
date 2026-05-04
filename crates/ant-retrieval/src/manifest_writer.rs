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
//! All produced manifests fit in a single CAC chunk today. For a
//! ≤ 4 KiB total payload (typical for ≤ ~12 files at default content
//! types), the manifest *is* its own root and there is no second
//! join level — bee's gateway handles this fine.

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

/// Bee's hard cap on per-fork prefix length. A path segment longer than
/// 30 bytes has to be split into multiple chained forks, each carrying a
/// 30-byte prefix and a 1-fork synthetic intermediate node. We don't
/// support path segments longer than this here yet — return an error
/// instead of producing a manifest bee can't read. Caller can pre-split
/// long names if they need them (almost no one does).
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
    if filename.is_empty() {
        return Err(ManifestWriteError::EmptyPath);
    }
    if filename.as_bytes().len() > MAX_FILENAME_BYTES {
        return Err(ManifestWriteError::SegmentTooLong {
            segment: filename.as_bytes().len(),
            cap: MAX_FILENAME_BYTES,
        });
    }

    let ct = content_type
        .map(str::to_string)
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let mut chunks: Vec<SplitChunk> = Vec::with_capacity(3);

    // Empty stub the `/` metadata-only fork points at. Bee's
    // `m.Add("/", NewEntry(zero, …))` call in fileUploadHandler creates
    // this — a node with no entry and no forks, just a placeholder for
    // the parent fork's metadata.
    let slash_stub_payload = empty_stub_payload();
    let (slash_stub_addr, slash_stub_wire) = cac_payload(&slash_stub_payload);
    chunks.push(SplitChunk {
        address: slash_stub_addr,
        wire: slash_stub_wire,
    });

    // The file value leaf: ref-size 0x20, entry = data_ref, no forks.
    let file_leaf_payload = value_leaf_payload(data_ref);
    let (file_leaf_addr, file_leaf_wire) = cac_payload(&file_leaf_payload);
    chunks.push(SplitChunk {
        address: file_leaf_addr,
        wire: file_leaf_wire,
    });

    // Root: 2 forks.
    let mut root_forks: BTreeMap<u8, ForkDescriptor> = BTreeMap::new();
    root_forks.insert(
        b'/',
        ForkDescriptor {
            prefix: b"/".to_vec(),
            child_ref: slash_stub_addr,
            metadata: index_doc_metadata(filename),
            path_separator: true,
        },
    );
    let first_byte = filename.as_bytes()[0];
    root_forks.insert(
        first_byte,
        ForkDescriptor {
            prefix: filename.as_bytes().to_vec(),
            child_ref: file_leaf_addr,
            metadata: file_metadata(&ct, filename),
            path_separator: false,
        },
    );

    let root_payload = node_payload(/* entry */ None, &root_forks)?;
    let (root_addr, root_wire) = cac_payload(&root_payload);
    chunks.push(SplitChunk {
        address: root_addr,
        wire: root_wire,
    });

    Ok(ManifestWriteResult {
        root: root_addr,
        chunks,
    })
}

/// Build a manifest covering many files. `files` is a list of distinct
/// paths and their pre-split data references. If any path equals
/// `index_document` (typically `"index.html"`), the root carries a
/// `website-index-document` metadata pointer so `bzz://<root>/` serves
/// that file.
///
/// The current implementation produces single-chunk manifests only:
/// for very large collections (~ 60+ files with long names) the root
/// would overflow [`CHUNK_SIZE`] and we return
/// [`ManifestWriteError::ManifestTooBig`]. Splitting the manifest
/// across chunks is future work.
pub fn build_collection_manifest(
    files: &[ManifestFile],
    index_document: Option<&str>,
) -> Result<ManifestWriteResult, ManifestWriteError> {
    if files.is_empty() {
        return Err(ManifestWriteError::EmptyPath);
    }
    for f in files {
        if f.path.is_empty() {
            return Err(ManifestWriteError::EmptyPath);
        }
        for segment in f.path.split('/') {
            if segment.as_bytes().len() > MAX_FILENAME_BYTES {
                return Err(ManifestWriteError::SegmentTooLong {
                    segment: segment.as_bytes().len(),
                    cap: MAX_FILENAME_BYTES,
                });
            }
        }
    }

    // For the v1 collection writer we emit a flat root: every file gets
    // a top-level fork keyed on its first path byte with `prefix = full
    // path`. This is exactly what bee's reader expects and produces in
    // its simplest form — it walks the prefix, finds the fork, descends
    // into the value leaf. Two files whose paths start with the same
    // byte require a real trie split (longest common prefix), handled
    // below for the common case (paths sharing more than the first
    // byte just collide and we error out — collection users almost
    // always have distinct first letters or distinct top-level dirs).
    let mut chunks: Vec<SplitChunk> = Vec::new();

    // Map of (full path prefix) → fork descriptor at the root level.
    let mut root_forks: BTreeMap<u8, ForkDescriptor> = BTreeMap::new();

    // Group files by their first byte so we can detect collisions and
    // produce a useful error rather than silently overwriting.
    let mut by_first: BTreeMap<u8, Vec<&ManifestFile>> = BTreeMap::new();
    for f in files {
        by_first.entry(f.path.as_bytes()[0]).or_default().push(f);
    }
    let mut seen_paths: BTreeSet<&str> = BTreeSet::new();
    for f in files {
        if !seen_paths.insert(f.path.as_str()) {
            return Err(ManifestWriteError::SegmentTooLong {
                segment: f.path.as_bytes().len(),
                cap: MAX_FILENAME_BYTES,
            });
        }
    }

    for (first_byte, group) in by_first {
        if group.len() == 1 {
            let f = group[0];
            let ct = f
                .content_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string());
            let leaf_payload = value_leaf_payload(f.data_ref);
            let (leaf_addr, leaf_wire) = cac_payload(&leaf_payload);
            chunks.push(SplitChunk {
                address: leaf_addr,
                wire: leaf_wire,
            });
            root_forks.insert(
                first_byte,
                ForkDescriptor {
                    prefix: f.path.as_bytes().to_vec(),
                    child_ref: leaf_addr,
                    metadata: file_metadata(&ct, &f.path),
                    path_separator: f.path.contains('/'),
                },
            );
        } else {
            // Multiple files share a first byte: build a one-level
            // sub-trie. Find the longest common prefix among these
            // siblings, emit a fork with that prefix pointing at a
            // sub-node with one fork per descendant on the byte
            // immediately after the LCP.
            let lcp = longest_common_prefix(group.iter().map(|f| f.path.as_bytes()));
            if lcp.len() > MAX_FILENAME_BYTES {
                return Err(ManifestWriteError::SegmentTooLong {
                    segment: lcp.len(),
                    cap: MAX_FILENAME_BYTES,
                });
            }

            let mut child_forks: BTreeMap<u8, ForkDescriptor> = BTreeMap::new();
            let mut child_index_doc: Option<&str> = None;
            for f in &group {
                let suffix = &f.path.as_bytes()[lcp.len()..];
                if suffix.is_empty() {
                    return Err(ManifestWriteError::EmptyPath);
                }
                if suffix.len() > MAX_FILENAME_BYTES {
                    return Err(ManifestWriteError::SegmentTooLong {
                        segment: suffix.len(),
                        cap: MAX_FILENAME_BYTES,
                    });
                }
                let ct = f
                    .content_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".to_string());
                let leaf_payload = value_leaf_payload(f.data_ref);
                let (leaf_addr, leaf_wire) = cac_payload(&leaf_payload);
                chunks.push(SplitChunk {
                    address: leaf_addr,
                    wire: leaf_wire,
                });
                if child_forks
                    .insert(
                        suffix[0],
                        ForkDescriptor {
                            prefix: suffix.to_vec(),
                            child_ref: leaf_addr,
                            metadata: file_metadata(&ct, &f.path),
                            path_separator: f.path[lcp.len()..].contains('/'),
                        },
                    )
                    .is_some()
                {
                    return Err(ManifestWriteError::SegmentTooLong {
                        segment: suffix.len(),
                        cap: MAX_FILENAME_BYTES,
                    });
                }
                if Some(f.path.as_str()) == index_document {
                    child_index_doc = Some(f.path.as_str());
                }
            }
            let child_node_payload = node_payload(/* entry */ None, &child_forks)?;
            let (child_node_addr, child_node_wire) = cac_payload(&child_node_payload);
            chunks.push(SplitChunk {
                address: child_node_addr,
                wire: child_node_wire,
            });

            let mut fork_meta = BTreeMap::new();
            if lcp.contains(&b'/') {
                // bee marks intermediate dir-spanning forks as path-separator;
                // it doesn't change retrieval, but it's how bee's lister
                // formats listings.
            }
            // Per-fork index-doc metadata is the bee convention only on the
            // root '/' fork; nested forks just carry Content-Type+Filename
            // on their leaves.
            let _ = child_index_doc;
            root_forks.insert(
                first_byte,
                ForkDescriptor {
                    prefix: lcp.clone(),
                    child_ref: child_node_addr,
                    metadata: fork_meta_drop(&mut fork_meta),
                    path_separator: lcp.contains(&b'/'),
                },
            );
        }
    }

    // Index-document anchor on the synthetic '/' fork at the root.
    if let Some(idx) = index_document {
        if !files.iter().any(|f| f.path == idx) {
            // Caller asked for an index doc that isn't in the file list.
            // We still emit it; bee will then 404 when it tries to
            // dereference the missing file. Quietly tolerate, like bee.
        }
        let stub_payload = empty_stub_payload();
        let (stub_addr, stub_wire) = cac_payload(&stub_payload);
        chunks.push(SplitChunk {
            address: stub_addr,
            wire: stub_wire,
        });
        root_forks.insert(
            b'/',
            ForkDescriptor {
                prefix: b"/".to_vec(),
                child_ref: stub_addr,
                metadata: index_doc_metadata(idx),
                path_separator: true,
            },
        );
    }

    let root_payload = node_payload(/* entry */ None, &root_forks)?;
    let (root_addr, root_wire) = cac_payload(&root_payload);
    chunks.push(SplitChunk {
        address: root_addr,
        wire: root_wire,
    });
    Ok(ManifestWriteResult {
        root: root_addr,
        chunks,
    })
}

// --- internal helpers ---

/// One fork as it will be serialised into a parent node's body.
struct ForkDescriptor {
    prefix: Vec<u8>,
    child_ref: ChunkAddr,
    metadata: BTreeMap<String, String>,
    path_separator: bool,
}

/// Empty-stub manifest node: bee's "no value here, no forks here, just a
/// placeholder address" — the same chunk that any well-known empty
/// stub hashes to. Used as the child of the root `/` fork.
fn empty_stub_payload() -> Vec<u8> {
    // 32 bytes obfkey + 31 bytes vhash + 1 byte ref-size(0) + 32 bytes
    // forks-bitmap (all zero). No entry slot when ref-size is 0.
    let mut p = Vec::with_capacity(32 + 31 + 1 + 32);
    p.extend_from_slice(&[0u8; 32]);
    p.extend_from_slice(&VERSION_HASH_31);
    p.push(0x00);
    p.extend_from_slice(&[0u8; 32]);
    p
}

/// Value-bearing leaf node: ref-size 0x20, entry = `data_ref`, no forks.
fn value_leaf_payload(data_ref: ChunkAddr) -> Vec<u8> {
    let mut p = Vec::with_capacity(32 + 31 + 1 + 32 + 32);
    p.extend_from_slice(&[0u8; 32]);
    p.extend_from_slice(&VERSION_HASH_31);
    p.push(0x20);
    p.extend_from_slice(&data_ref);
    p.extend_from_slice(&[0u8; 32]);
    p
}

/// Serialise a manifest node from an optional own entry and its set of
/// outgoing forks. Returns the de-obfuscated payload bytes; the caller
/// wraps them in a CAC chunk via [`cac_payload`].
fn node_payload(
    entry: Option<ChunkAddr>,
    forks: &BTreeMap<u8, ForkDescriptor>,
) -> Result<Vec<u8>, ManifestWriteError> {
    let mut p = Vec::with_capacity(128);
    p.extend_from_slice(&[0u8; 32]); // obf key
    p.extend_from_slice(&VERSION_HASH_31);

    // ref-size: 0x20 if any forks exist (forks reuse it for child_ref
    // size) or if the node has its own entry. 0x00 only for an empty
    // stub leaf.
    let needs_ref = entry.is_some() || !forks.is_empty();
    if needs_ref {
        p.push(0x20);
        match entry {
            Some(addr) => p.extend_from_slice(&addr),
            None => p.extend_from_slice(&[0u8; 32]),
        }
    } else {
        p.push(0x00);
    }

    // Forks bitmap.
    let mut bitmap = [0u8; 32];
    for byte in forks.keys() {
        bitmap[(*byte as usize) / 8] |= 1 << (*byte % 8);
    }
    p.extend_from_slice(&bitmap);

    // Forks in ascending order of first byte (BTreeMap preserves this).
    for fork in forks.values() {
        let bytes = serialize_fork(fork)?;
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

/// Serialise one fork: type(1) + prefix-len(1) + prefix(30) +
/// child_ref(32) + (optional meta-len(2) + meta(N, '\n'-padded so the
/// whole fork is a multiple of 32 bytes)).
fn serialize_fork(fork: &ForkDescriptor) -> Result<Vec<u8>, ManifestWriteError> {
    if fork.prefix.is_empty() || fork.prefix.len() > MAX_FILENAME_BYTES {
        return Err(ManifestWriteError::SegmentTooLong {
            segment: fork.prefix.len(),
            cap: MAX_FILENAME_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(64);
    let mut node_type = NODE_TYPE_VALUE; // every fork we emit points at
                                         // a value-or-edge node; bee sets
                                         // the bit on the parent fork
                                         // not the child's own node_type
                                         // header.
    if !fork.metadata.is_empty() {
        node_type |= NODE_TYPE_WITH_METADATA;
    }
    if fork.path_separator {
        node_type |= NODE_TYPE_PATH_SEPARATOR;
    }
    bytes.push(node_type);
    bytes.push(fork.prefix.len() as u8);
    bytes.extend_from_slice(&fork.prefix);
    bytes.extend(std::iter::repeat(0u8).take(30 - fork.prefix.len()));
    bytes.extend_from_slice(&fork.child_ref);
    if !fork.metadata.is_empty() {
        let meta_json = encode_metadata(&fork.metadata);
        // Pad with '\n' so that (meta_size + 2) is a multiple of 32 —
        // this keeps the entire fork (1+1+30+32+2+meta = 66+meta) at
        // a 32-byte multiple, which bee's parser walks fork-by-fork.
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

fn fork_meta_drop(meta: &mut BTreeMap<String, String>) -> BTreeMap<String, String> {
    std::mem::take(meta)
}

/// CAC-wrap a manifest node payload (always one chunk; payload size is
/// already validated against [`CHUNK_SIZE`] in [`node_payload`]).
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

    /// Path segment over 30 bytes is rejected — we don't emit chained
    /// forks yet.
    #[test]
    fn long_filename_errors() {
        let long = "x".repeat(31);
        let err = build_single_file_manifest(&long, None, [0u8; 32]).unwrap_err();
        assert!(matches!(err, ManifestWriteError::SegmentTooLong { .. }));
    }

    /// The empty stub address must be a valid CAC chunk so that bee
    /// gateways will actually accept it on push. Cheap sanity check
    /// that the bytes we ship form a valid leaf.
    #[test]
    fn empty_stub_is_valid_cac() {
        let payload = empty_stub_payload();
        let (addr, wire) = cac_payload(&payload);
        assert!(ant_crypto::cac_valid(&addr, &wire));
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
