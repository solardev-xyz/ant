//! Mantaray manifest decoder and path lookup.
//!
//! Bee's binary manifest format. A manifest reference points at the **root
//! node** of a trie, where each edge carries a 1..30 byte path prefix and
//! each node optionally carries a value (a chunk reference to file data)
//! and arbitrary string metadata.
//!
//! `swarm-cli upload <file>` produces a manifest with one fork keyed on
//! the file's name, plus a `website-index-document` metadata entry on the
//! root pointing at that name (so `bzz://<ref>/` resolves to the file).
//! `swarm-cli upload <dir>` produces one fork per top-level entry, no
//! `index-document` unless the directory has an `index.html` and you
//! pass `--index-document=index.html`.
//!
//! # Wire format ([`bee/pkg/manifest/mantaray/marshal.go`])
//!
//! After XOR-decoding the body (every 32-byte block past the first XORed
//! with the obfuscation key, which lives unencrypted in bytes 0..32):
//!
//! ```text
//! [0..32]   obfuscation key (XOR pad; all-zero on unencrypted manifests)
//! [32..63]  31-byte version hash
//!           - "mantaray:0.2" → 5768b3...28f7b   (current)
//!           - "mantaray:0.1" → 025184...a9b7   (legacy, also handled)
//! [63]      ref-bytes-size (32 = unencrypted, 64 = encrypted; we only
//!           accept 32)
//! [64..64+R] entry: data reference for *this* node (zeroed if no entry)
//! [+0..+32] forks index — 256-bit bitmap, bit `b` set ⇒ a fork starts on
//!           input byte `b`
//! [forks…]  in ascending bit order, for each set bit:
//!             - 1 byte node type (1=value, 2=value+, 4=edge, 8=path-sep,
//!               16=with-metadata; bitwise-or)
//!             - 1 byte prefix length (1..30)
//!             - 30 bytes prefix (only first prefixLen bytes meaningful)
//!             - R bytes child reference
//!             - if (type & 16): 2 BE bytes metadata length, then JSON
//!               metadata (padded with '\n' to a multiple of 32)
//! ```
//!
//! # Path lookup
//!
//! Walk the root: at each node, find the fork whose first byte matches the
//! current head of `path`, check the fork's prefix is itself a prefix of
//! `path`, advance, recurse. When `path` is exhausted return the current
//! node's `entry` and any `Content-Type` metadata. Mirrors
//! `mantaray.Node.LookupNode`.
//!
//! Empty path is **not** an error — if the root has metadata key
//! `website-index-document`, we treat that as the path. This matches what
//! `bee/pkg/api/bzz.go` does when serving `bzz://<ref>/`.

use crate::ChunkFetcher;
use crate::{cac_valid, join, JoinError, DEFAULT_MAX_FILE_BYTES};
use std::collections::HashMap;
use thiserror::Error;
use tracing::{debug, trace};

/// Metadata key on a node carrying its MIME type. Set on file value
/// nodes by `swarm-cli upload`.
pub const MANTARAY_CONTENT_TYPE_KEY: &str = "Content-Type";
/// Metadata key on a directory's root naming the default file to serve
/// for an empty path.
pub const MANTARAY_INDEX_DOC_KEY: &str = "website-index-document";
/// Metadata key on a directory's root naming the file to serve when no
/// other path matches (404 fallback). We honour it after the index doc.
pub const MANTARAY_ERROR_DOC_KEY: &str = "website-error-document";

const OBFUSCATION_KEY_SIZE: usize = 32;
const VERSION_HASH_SIZE: usize = 31;
const NODE_HEADER_SIZE: usize = OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE + 1;
const FORK_PREFIX_MAX: usize = 30;
const FORK_PRE_REF_SIZE: usize = 32; // type(1) + prefixLen(1) + prefix(30)
const FORK_METADATA_SIZE_BYTES: usize = 2;
const NODE_TYPE_VALUE: u8 = 2;
const NODE_TYPE_EDGE: u8 = 4;
const NODE_TYPE_WITH_METADATA: u8 = 16;

const VERSION_02_HASH_HEX: &str =
    "5768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f7b";
const VERSION_01_HASH_HEX: &str =
    "025184789d63635766d78c41900196b57d7400875ebe4d9b5d1e76bd9652a9b7";

#[derive(Debug, Error)]
pub enum ManifestError {
    /// The path being looked up doesn't exist in the manifest.
    #[error("path '{path}' not found")]
    NotFound { path: String },
    /// Manifest node bytes are too short for the declared layout.
    #[error("manifest node too short: {0}")]
    TooShort(String),
    /// Version hash on the node is neither mantaray:0.1 nor mantaray:0.2.
    #[error("unsupported manifest version: {0}")]
    UnsupportedVersion(String),
    /// Reference size is 64 (encrypted manifest); we only handle 32.
    #[error("encrypted manifests not supported")]
    UnsupportedEncryption,
    /// Ref length is something other than 0, 32, or 64.
    #[error("invalid manifest reference length: {0}")]
    InvalidRefLength(u8),
    /// Metadata blob isn't valid UTF-8 / JSON.
    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),
    /// Walking past a value-only node when the path still has bytes left.
    /// Typically signals a broken path like `/index.html/extra`.
    #[error("path '{path}' descends past leaf")]
    DescendPastLeaf { path: String },
    /// The reference points at file data instead of a manifest. Bee's
    /// own bzz handler treats this as a 404 too; we propagate it as a
    /// distinct variant so callers can fall back to raw bytes if they
    /// want to.
    #[error("reference is not a mantaray manifest")]
    NotAManifest,
    /// Underlying chunk-fetch failed while loading a node.
    #[error("fetch manifest node: {0}")]
    Fetch(#[from] JoinError),
}

/// One match in the manifest trie.
#[derive(Debug, Clone)]
pub struct LookupResult {
    /// 32-byte reference to the file data — feed this to [`crate::join`]
    /// (after fetching the chunk it points to) to read the file contents.
    pub data_ref: [u8; 32],
    /// `Content-Type` metadata at the matched value node, if present.
    pub content_type: Option<String>,
    /// All metadata at the matched value node. Caller may inspect for
    /// `Filename` etc.
    pub metadata: HashMap<String, String>,
}

/// Walk a mantaray manifest and resolve `path` to the file it points at.
///
/// `fetcher` is used to load every node in the trie. `root_addr` is the
/// chunk reference the user gave us (usually a bzz reference). `path` is
/// what comes after the reference in `bzz://<ref>/<path>` — leading slash
/// optional. An empty path triggers the `website-index-document` lookup
/// stored on the manifest's "/" fork (this is what `swarm-cli upload <file>`
/// produces and what bee's `bzzDownloadHandler` looks up; see
/// `bee/pkg/api/bzz.go`).
pub async fn lookup_path(
    fetcher: &dyn ChunkFetcher,
    root_addr: [u8; 32],
    path: &str,
) -> Result<LookupResult, ManifestError> {
    let path = path.trim_start_matches('/');

    let root_node = load_node(fetcher, root_addr).await?;
    debug!(
        target: "ant_retrieval::mantaray",
        root = %hex::encode(root_addr),
        forks = root_node.forks.len(),
        "loaded mantaray root",
    );

    // Empty-path resolution. Bee stores `website-index-document` as
    // metadata on the **fork at byte '/'**, not on the root node itself.
    // (Marshalled as `m.Add("/", NewEntry(zero, {website-index-document:
    // ...}))` in `bee/pkg/api/bzz.go::fileUploadHandler`.) Read it
    // directly off the root's "/" fork; if absent, fall back to looking
    // the empty path up as a value on the root.
    let effective_path: String = if path.is_empty() {
        match root_node
            .forks
            .get(&b'/')
            .and_then(|f| f.metadata.get(MANTARAY_INDEX_DOC_KEY))
        {
            Some(idx) => idx.clone(),
            None => String::new(),
        }
    } else {
        path.to_string()
    };

    if effective_path.is_empty() {
        // No index document and no path: the root would have to be a
        // self-value node, which `bzzDownloadHandler` treats as 404. Be
        // explicit so the user gets a friendlier message than "fork at
        // 0x00 not found".
        return Err(ManifestError::NotFound {
            path: "(root)".into(),
        });
    }

    walk(
        fetcher,
        root_node,
        effective_path.as_bytes(),
        &effective_path,
    )
    .await
}

/// Recursive walker. Loads nodes lazily from `fetcher` as it descends.
/// At each fork hop, reads the file's actual data reference from the
/// child node's *own* `entry` bytes (set by bee when `m.Add("hello.txt",
/// entry, …)` ultimately marshals the child) and merges fork-level
/// metadata (Content-Type, Filename) onto that result.
fn walk<'a>(
    fetcher: &'a dyn ChunkFetcher,
    node: Node,
    remaining: &'a [u8],
    full_path: &'a str,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<LookupResult, ManifestError>> + Send + 'a>,
> {
    Box::pin(async move {
        if remaining.is_empty() {
            // Land on this node. The data reference is the node's `entry`
            // (must be 32 bytes, non-zero); the file's metadata
            // (Content-Type, Filename) was already merged in by the
            // caller from the fork that pointed here.
            if node.entry.len() != 32 {
                return Err(ManifestError::InvalidRefLength(node.entry.len() as u8));
            }
            let mut data_ref = [0u8; 32];
            data_ref.copy_from_slice(&node.entry);
            if data_ref == [0u8; 32] {
                // A zero entry is bee's "no value here" sentinel — used
                // on the synthetic "/" fork that holds website metadata
                // but not a file. Calling `Lookup("/")` on a real bee
                // node will return NotFound; we mirror that.
                return Err(ManifestError::NotFound {
                    path: full_path.to_string(),
                });
            }
            let content_type = node.metadata.get(MANTARAY_CONTENT_TYPE_KEY).cloned();
            return Ok(LookupResult {
                data_ref,
                content_type,
                metadata: node.metadata,
            });
        }

        // Find a fork whose first byte matches.
        let head = remaining[0];
        let fork = match node.forks.get(&head) {
            Some(f) => f.clone(),
            None => {
                return Err(ManifestError::NotFound {
                    path: full_path.to_string(),
                });
            }
        };

        // The fork's prefix must be a prefix of `remaining`.
        if remaining.len() < fork.prefix.len()
            || remaining[..fork.prefix.len()] != fork.prefix[..]
        {
            return Err(ManifestError::NotFound {
                path: full_path.to_string(),
            });
        }
        let next_remaining = &remaining[fork.prefix.len()..];
        trace!(
            target: "ant_retrieval::mantaray",
            consumed = %String::from_utf8_lossy(&fork.prefix),
            child_ref = %hex::encode(fork.child_ref),
            "descending into fork",
        );
        let mut child_node = load_node(fetcher, fork.child_ref).await?;
        // Merge fork-level metadata onto the child. In bee's wire format
        // (mantaray 0.2) per-file Content-Type / Filename are stored on
        // the parent fork, not on the child node's own bytes — yet bee's
        // `LookupNode` returns them as if they were on the child via Go
        // pointer-embedding tricks. We do it explicitly.
        for (k, v) in fork.metadata {
            child_node.metadata.entry(k).or_insert(v);
        }
        walk(fetcher, child_node, next_remaining, full_path).await
    })
}

/// Fetch a manifest node by reference and unmarshal it.
async fn load_node(
    fetcher: &dyn ChunkFetcher,
    addr: [u8; 32],
) -> Result<Node, ManifestError> {
    // The node is itself a Swarm file: fetch the root chunk, then join.
    // For tiny manifests (< 4 KiB) the root chunk's payload is already
    // the entire serialised node.
    let root_chunk = fetcher
        .fetch(addr)
        .await
        .map_err(|e| ManifestError::Fetch(JoinError::FetchChunk {
            addr: hex::encode(addr),
            source: e,
        }))?;
    // Defensive: cac_valid should already have run inside `fetch`, but
    // re-check so a buggy custom fetcher can't hand us garbage.
    if !cac_valid(&addr, &root_chunk) {
        return Err(ManifestError::Fetch(JoinError::FetchChunk {
            addr: hex::encode(addr),
            source: "chunk failed CAC validation".into(),
        }));
    }
    let bytes = join(fetcher, &root_chunk, DEFAULT_MAX_FILE_BYTES).await?;
    Node::unmarshal(&bytes)
}

/// In-memory representation of a mantaray node after unmarshalling.
#[derive(Debug, Clone)]
struct Node {
    /// Bee-side bitfield (value/edge/with-metadata). We parse it for
    /// fidelity with the on-wire format and to keep diagnostics readable;
    /// fork lookup is currently driven by `entry`/`forks` directly.
    #[allow(dead_code)]
    node_type: u8,
    entry: Vec<u8>,
    metadata: HashMap<String, String>,
    forks: HashMap<u8, Fork>,
}

#[derive(Debug, Clone)]
struct Fork {
    prefix: Vec<u8>,
    child_ref: [u8; 32],
    metadata: HashMap<String, String>,
}

impl Node {
    /// Parse a fully-joined manifest node. The first 32 bytes are the
    /// obfuscation key (treated literally), the rest is XORed with that
    /// key as a 32-byte rolling pad before parsing.
    fn unmarshal(input: &[u8]) -> Result<Self, ManifestError> {
        if input.len() < NODE_HEADER_SIZE {
            return Err(ManifestError::TooShort(format!(
                "{} bytes < header {}",
                input.len(),
                NODE_HEADER_SIZE
            )));
        }
        // De-obfuscate. The key lives in [0..32] in the clear.
        let mut data = input.to_vec();
        let key: [u8; 32] = data[..32].try_into().unwrap();
        for chunk in data[OBFUSCATION_KEY_SIZE..].chunks_mut(OBFUSCATION_KEY_SIZE) {
            for (i, b) in chunk.iter_mut().enumerate() {
                *b ^= key[i % OBFUSCATION_KEY_SIZE];
            }
        }
        Self::parse_decoded(&data)
    }

    fn parse_decoded(data: &[u8]) -> Result<Self, ManifestError> {
        let version_hash = &data[OBFUSCATION_KEY_SIZE..OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE];
        // The Go constants are the full 32-byte keccak256 of "mantaray:0.2"
        // / "mantaray:0.1"; bee writes only the first 31 bytes on the wire
        // (see `versionHashSize = 31` and `copy(*bytes, b)` in
        // `mantaray.initVersion`). Truncate so the comparison matches.
        let v01_full = hex::decode(VERSION_01_HASH_HEX).expect("const");
        let v02_full = hex::decode(VERSION_02_HASH_HEX).expect("const");
        let v01 = &v01_full[..VERSION_HASH_SIZE];
        let v02 = &v02_full[..VERSION_HASH_SIZE];
        let is_v02 = version_hash == v02;
        let is_v01 = version_hash == v01;
        if !is_v02 && !is_v01 {
            // Most often this means the user passed a `/bytes/`
            // reference (raw file) where a `/bzz/` reference was
            // expected. Log the head so a debug-level run can tell
            // that case apart from a corrupt manifest.
            debug!(
                target: "ant_retrieval::mantaray",
                len = data.len(),
                version_hash_31 = %hex::encode(version_hash),
                "mantaray: version-hash mismatch, returning NotAManifest",
            );
            return Err(ManifestError::NotAManifest);
        }
        let ref_size = data[OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE];
        if ref_size == 64 {
            return Err(ManifestError::UnsupportedEncryption);
        }
        if ref_size != 32 && ref_size != 0 {
            return Err(ManifestError::InvalidRefLength(ref_size));
        }
        let ref_size = ref_size as usize;

        let mut offset = NODE_HEADER_SIZE;
        // Entry: `ref_size` bytes (zeroed for nodes without an entry).
        if data.len() < offset + ref_size {
            return Err(ManifestError::TooShort(format!(
                "entry: have {}, need {}",
                data.len(),
                offset + ref_size
            )));
        }
        let entry_bytes = data[offset..offset + ref_size].to_vec();
        offset += ref_size;

        // 32-byte fork-index bitmap.
        if data.len() < offset + 32 {
            return Err(ManifestError::TooShort("forks index".into()));
        }
        let index = data[offset..offset + 32].to_vec();
        offset += 32;

        // Determine if this node holds an entry: bee's marshaller doesn't
        // round-trip the bare value-flag, so we infer it from the entry
        // bytes being non-zero. A directory node with no own value has
        // an all-zero entry slot.
        let has_entry = entry_bytes.iter().any(|&b| b != 0);
        let mut node_type: u8 = 0;
        if has_entry {
            node_type |= NODE_TYPE_VALUE;
        }
        // Edge: any fork present.
        if index.iter().any(|&b| b != 0) {
            node_type |= NODE_TYPE_EDGE;
        }

        let mut forks = HashMap::new();
        // Walk the bitmap in ascending bit order. Bee writes forks in the
        // same order during marshal so we follow suit.
        for byte in 0u16..256u16 {
            let b = byte as u8;
            let bit_set = (index[(b / 8) as usize] >> (b % 8)) & 1 != 0;
            if !bit_set {
                continue;
            }

            // Per-fork header: type(1) + prefixLen(1) + 30 prefix bytes.
            if is_v01 {
                if data.len() < offset + FORK_PRE_REF_SIZE + ref_size {
                    return Err(ManifestError::TooShort(format!(
                        "fork {b:02x}: need {}, have {}",
                        offset + FORK_PRE_REF_SIZE + ref_size,
                        data.len()
                    )));
                }
                let f_type = data[offset];
                let prefix_len = data[offset + 1] as usize;
                if prefix_len == 0 || prefix_len > FORK_PREFIX_MAX {
                    return Err(ManifestError::TooShort(format!(
                        "fork {b:02x} prefix_len {prefix_len} out of range"
                    )));
                }
                let prefix = data[offset + 2..offset + 2 + prefix_len].to_vec();
                let mut child_ref = [0u8; 32];
                child_ref.copy_from_slice(
                    &data[offset + FORK_PRE_REF_SIZE..offset + FORK_PRE_REF_SIZE + ref_size],
                );
                forks.insert(
                    b,
                    Fork {
                        prefix,
                        child_ref,
                        metadata: HashMap::new(),
                    },
                );
                let _ = f_type;
                offset += FORK_PRE_REF_SIZE + ref_size;
            } else {
                // mantaray:0.2
                if data.len() < offset + 1 {
                    return Err(ManifestError::TooShort("fork type byte".into()));
                }
                let f_type = data[offset];
                let mut fork_size = FORK_PRE_REF_SIZE + ref_size;
                let has_meta = (f_type & NODE_TYPE_WITH_METADATA) != 0;
                let mut meta_size = 0usize;
                if has_meta {
                    if data.len() < offset + fork_size + FORK_METADATA_SIZE_BYTES {
                        return Err(ManifestError::TooShort("fork metadata size".into()));
                    }
                    meta_size = u16::from_be_bytes([
                        data[offset + fork_size],
                        data[offset + fork_size + 1],
                    ]) as usize;
                    fork_size += FORK_METADATA_SIZE_BYTES + meta_size;
                }
                if data.len() < offset + fork_size {
                    return Err(ManifestError::TooShort(format!(
                        "fork body: need {}, have {}",
                        offset + fork_size,
                        data.len()
                    )));
                }
                let prefix_len = data[offset + 1] as usize;
                if prefix_len == 0 || prefix_len > FORK_PREFIX_MAX {
                    return Err(ManifestError::TooShort(format!(
                        "fork {b:02x} prefix_len {prefix_len} out of range"
                    )));
                }
                let prefix = data[offset + 2..offset + 2 + prefix_len].to_vec();
                let mut child_ref = [0u8; 32];
                child_ref.copy_from_slice(
                    &data[offset + FORK_PRE_REF_SIZE..offset + FORK_PRE_REF_SIZE + ref_size],
                );
                let mut fork_meta = HashMap::new();
                if has_meta && meta_size > 0 {
                    let meta_start = offset + FORK_PRE_REF_SIZE + ref_size + FORK_METADATA_SIZE_BYTES;
                    let meta_bytes = &data[meta_start..meta_start + meta_size];
                    fork_meta = parse_metadata(meta_bytes)?;
                    // Forks set with-metadata flag on themselves; some of
                    // the keys (e.g. `Content-Type`) are bee-specific,
                    // others (`website-index-document`) belong on the
                    // *parent* node. Treat them as fork-level for now;
                    // walk() lifts them onto the child during traversal.
                }
                forks.insert(
                    b,
                    Fork {
                        prefix,
                        child_ref,
                        metadata: fork_meta,
                    },
                );
                offset += fork_size;
            }
        }

        // Per the bee marshaller, root-level metadata (the kind that
        // holds `website-index-document`) is encoded on the **fork that
        // covers the root's path-separator key** in v0.2. In practice,
        // for `swarm-cli upload <file>`, the metadata goes on the value
        // fork; we already pulled that into fork_meta. The root node
        // itself doesn't carry metadata directly in this serialiser.
        Ok(Node {
            node_type,
            entry: entry_bytes,
            metadata: HashMap::new(),
            forks,
        })
    }
}

/// Parse JSON metadata after stripping bee's `\n` padding. Mantaray pads
/// the metadata blob to a multiple of 32 with newlines so each metadata
/// region aligns to the obfuscation block size.
fn parse_metadata(raw: &[u8]) -> Result<HashMap<String, String>, ManifestError> {
    let s = std::str::from_utf8(raw).map_err(|e| ManifestError::InvalidMetadata(e.to_string()))?;
    let s = s.trim_end_matches('\n');
    if s.is_empty() {
        return Ok(HashMap::new());
    }
    // Tiny bespoke JSON object parser: bee writes a flat
    // `map[string]string`, so the expected shape is `{"k":"v",...}` with
    // no nesting. Avoids pulling in serde_json for a five-line use case
    // — and crucially preserves whatever ordering the producer chose
    // without imposing one.
    parse_flat_json_object(s)
}

/// Parse a flat JSON object `{"k":"v",...}` into a string map. Tolerant
/// of standard JSON escape sequences inside strings; bee never emits
/// non-ASCII keys, but values may contain spaces or punctuation.
fn parse_flat_json_object(s: &str) -> Result<HashMap<String, String>, ManifestError> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut out = HashMap::new();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'{' {
        return Err(ManifestError::InvalidMetadata(format!(
            "expected '{{', got {:?}",
            &s[..s.len().min(40)]
        )));
    }
    i += 1;
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b'}' {
            return Ok(out);
        }
        let key = parse_json_string(bytes, &mut i)?;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b':' {
            return Err(ManifestError::InvalidMetadata("missing ':'".into()));
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let value = parse_json_string(bytes, &mut i)?;
        out.insert(key, value);
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b',' {
            i += 1;
            continue;
        }
        if i < bytes.len() && bytes[i] == b'}' {
            return Ok(out);
        }
        return Err(ManifestError::InvalidMetadata("expected ',' or '}'".into()));
    }
}

/// Parse a single JSON-quoted string starting at `*i`. Advances `*i`
/// past the closing quote. Handles `\"`, `\\`, `\/`, `\n`, `\t`, `\r`,
/// `\b`, `\f`, and `\uXXXX` (BMP only — bee never writes anything else).
fn parse_json_string(bytes: &[u8], i: &mut usize) -> Result<String, ManifestError> {
    if *i >= bytes.len() || bytes[*i] != b'"' {
        return Err(ManifestError::InvalidMetadata("expected '\"'".into()));
    }
    *i += 1;
    let mut out = String::new();
    while *i < bytes.len() {
        let c = bytes[*i];
        if c == b'"' {
            *i += 1;
            return Ok(out);
        }
        if c == b'\\' {
            *i += 1;
            if *i >= bytes.len() {
                return Err(ManifestError::InvalidMetadata("dangling escape".into()));
            }
            match bytes[*i] {
                b'"' => out.push('"'),
                b'\\' => out.push('\\'),
                b'/' => out.push('/'),
                b'n' => out.push('\n'),
                b't' => out.push('\t'),
                b'r' => out.push('\r'),
                b'b' => out.push('\u{0008}'),
                b'f' => out.push('\u{000c}'),
                b'u' => {
                    if *i + 4 >= bytes.len() {
                        return Err(ManifestError::InvalidMetadata("\\u truncated".into()));
                    }
                    let hex = std::str::from_utf8(&bytes[*i + 1..*i + 5])
                        .map_err(|e| ManifestError::InvalidMetadata(e.to_string()))?;
                    let cp = u32::from_str_radix(hex, 16)
                        .map_err(|e| ManifestError::InvalidMetadata(e.to_string()))?;
                    if let Some(ch) = char::from_u32(cp) {
                        out.push(ch);
                    }
                    *i += 4;
                }
                other => {
                    return Err(ManifestError::InvalidMetadata(format!(
                        "bad escape \\{}",
                        other as char
                    )))
                }
            }
            *i += 1;
        } else {
            // Raw UTF-8 bytes pass through. Reading char-by-char from
            // `bytes` would be cheaper but mantaray metadata is tiny so
            // we eat the cost of the helper.
            let s = std::str::from_utf8(&bytes[*i..]).map_err(|e| {
                ManifestError::InvalidMetadata(format!("non-utf8 in metadata: {e}"))
            })?;
            let ch = s.chars().next().unwrap();
            out.push(ch);
            *i += ch.len_utf8();
        }
    }
    Err(ManifestError::InvalidMetadata("unterminated string".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChunkFetcher;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::error::Error;

    /// HashMap-backed fetcher mirroring `joiner::tests::MapFetcher`.
    struct MapFetcher {
        chunks: HashMap<[u8; 32], Vec<u8>>,
    }

    impl MapFetcher {
        fn new() -> Self {
            Self {
                chunks: HashMap::new(),
            }
        }
        fn insert(&mut self, addr: [u8; 32], wire: Vec<u8>) {
            self.chunks.insert(addr, wire);
        }
    }

    #[async_trait]
    impl ChunkFetcher for MapFetcher {
        async fn fetch(
            &self,
            addr: [u8; 32],
        ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
            self.chunks
                .get(&addr)
                .cloned()
                .ok_or_else(|| -> Box<dyn Error + Send + Sync> {
                    format!("missing chunk {}", hex::encode(addr)).into()
                })
        }
    }

    /// Pre-recorded wire bytes from a real `swarm-cli upload` of an
    /// 85-byte text file via the public Bee gateway (April 2026). Root
    /// reference is `b6c8b1b6...`. Captured to make sure the v0.2
    /// version-hash truncation, fork-metadata extraction and the
    /// website-index-document fallback all stay in sync with bee.
    const ROOT_HEX: &str = "a00100000000000000000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000100000000000000000000000000000000000012012f00000000000000000000000000000000000000000000000000000000000cc878d32c96126d47f63fbe391114ee1438cd521146fc975dea1546d302b6c0003e7b22776562736974652d696e6465782d646f63756d656e74223a2268656c6c6f2e747874227d0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a120968656c6c6f2e7478740000000000000000000000000000000000000000000ad6c856f417b0805250d41a46559c9402a9251f8bc6acd65540fe0e68b45d41005e7b22436f6e74656e742d54797065223a22746578742f706c61696e3b20636861727365743d7574662d38222c2246696c656e616d65223a2268656c6c6f2e747874227d0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a";
    const CHILD_SLASH_HEX: &str = "600000000000000000000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f000000000000000000000000000000000000000000000000000000000000000000";
    const CHILD_H_HEX: &str = "800000000000000000000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f202f63d85b71d63176df24ea98df065d4de18163fcbc9f75e6d23f4327f33bca820000000000000000000000000000000000000000000000000000000000000000";
    const ROOT_ADDR_HEX: &str =
        "b6c8b1b632dac74382f7f0fb9ac44d4885b4b894d2b794e57fd1c43f773f78a2";
    const CHILD_SLASH_ADDR_HEX: &str =
        "0cc878d32c96126d47f63fbe391114ee1438cd521146fc975dea1546d302b6c0";
    const CHILD_H_ADDR_HEX: &str =
        "0ad6c856f417b0805250d41a46559c9402a9251f8bc6acd65540fe0e68b45d41";
    const FILE_DATA_REF_HEX: &str =
        "2f63d85b71d63176df24ea98df065d4de18163fcbc9f75e6d23f4327f33bca82";

    /// Build a fetcher pre-populated with the three on-wire chunks from
    /// the gateway capture above. Tests can then walk the manifest as
    /// if `antd` had fetched these chunks itself.
    fn gateway_fetcher() -> MapFetcher {
        let mut f = MapFetcher::new();
        f.insert(
            decode_addr(ROOT_ADDR_HEX),
            hex::decode(ROOT_HEX).unwrap(),
        );
        f.insert(
            decode_addr(CHILD_SLASH_ADDR_HEX),
            hex::decode(CHILD_SLASH_HEX).unwrap(),
        );
        f.insert(
            decode_addr(CHILD_H_ADDR_HEX),
            hex::decode(CHILD_H_HEX).unwrap(),
        );
        f
    }

    fn decode_addr(s: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out).unwrap();
        out
    }

    /// Empty path: should resolve via `website-index-document` on the
    /// "/" fork, descend into the "hello.txt" fork, and return the
    /// file's data ref + Content-Type.
    #[tokio::test]
    async fn lookup_empty_path_uses_index_document() {
        let fetcher = gateway_fetcher();
        let root = decode_addr(ROOT_ADDR_HEX);
        let res = lookup_path(&fetcher, root, "").await.unwrap();
        assert_eq!(hex::encode(res.data_ref), FILE_DATA_REF_HEX);
        assert_eq!(
            res.content_type.as_deref(),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(res.metadata.get("Filename").map(|s| s.as_str()), Some("hello.txt"));
    }

    /// Explicit path matches the same file via the `h` fork.
    #[tokio::test]
    async fn lookup_explicit_path() {
        let fetcher = gateway_fetcher();
        let root = decode_addr(ROOT_ADDR_HEX);
        let res = lookup_path(&fetcher, root, "hello.txt").await.unwrap();
        assert_eq!(hex::encode(res.data_ref), FILE_DATA_REF_HEX);
        assert_eq!(
            res.content_type.as_deref(),
            Some("text/plain; charset=utf-8")
        );
    }

    /// Non-existent path returns a clear `NotFound`.
    #[tokio::test]
    async fn lookup_unknown_path_not_found() {
        let fetcher = gateway_fetcher();
        let root = decode_addr(ROOT_ADDR_HEX);
        let err = lookup_path(&fetcher, root, "missing.txt")
            .await
            .unwrap_err();
        assert!(matches!(err, ManifestError::NotFound { .. }));
    }

    /// A `/bytes/`-style raw chunk passed in by mistake should yield
    /// `NotAManifest` (so the caller can guide the user to `--bytes`
    /// or `antctl get` for a single chunk). Use a payload long enough
    /// to clear the header-size check so we hit the version-hash
    /// comparison rather than `TooShort`.
    #[tokio::test]
    async fn lookup_non_manifest_chunk_distinct_error() {
        let payload = vec![0xab; 200];
        let (addr, wire) = ant_crypto::cac_new(&payload).unwrap();
        let mut fetcher = MapFetcher::new();
        fetcher.insert(addr, wire);
        let err = lookup_path(&fetcher, addr, "anything").await.unwrap_err();
        assert!(
            matches!(err, ManifestError::NotAManifest),
            "expected NotAManifest, got {err:?}",
        );
    }

    /// JSON escape parser: covers every escape mantaray emits.
    #[test]
    fn json_string_round_trip() {
        let mut i = 0;
        let s = b"\"hello world\"";
        assert_eq!(parse_json_string(s, &mut i).unwrap(), "hello world");
        assert_eq!(i, s.len());

        let mut i = 0;
        let s = b"\"a\\nb\"";
        assert_eq!(parse_json_string(s, &mut i).unwrap(), "a\nb");
    }

    /// Flat JSON object parser: handles the shapes bee actually writes
    /// (Content-Type strings, filenames with spaces).
    #[test]
    fn flat_json_object_basic() {
        let m = parse_flat_json_object("{\"Content-Type\":\"text/plain; charset=utf-8\",\"Filename\":\"hello.txt\"}").unwrap();
        assert_eq!(m.get("Content-Type").unwrap(), "text/plain; charset=utf-8");
        assert_eq!(m.get("Filename").unwrap(), "hello.txt");
        assert_eq!(m.len(), 2);
    }

    /// Trailing newline padding (bee pads metadata blocks to 32 bytes
    /// with '\n' before serialising).
    #[test]
    fn flat_json_object_strip_padding() {
        let raw = b"{\"k\":\"v\"}\n\n\n\n";
        let m = parse_metadata(raw).unwrap();
        assert_eq!(m.get("k").unwrap(), "v");
    }

    /// Empty padded metadata = no entries, no error.
    #[test]
    fn flat_json_object_empty_padding() {
        let raw = b"\n\n\n";
        let m = parse_metadata(raw).unwrap();
        assert!(m.is_empty());
    }
}
