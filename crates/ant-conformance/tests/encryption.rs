//! Swarm content-encryption conformance against bee.
//!
//! `conformance/vectors/encryption.json` carries two tiers:
//!
//! - **`chunkCases`** — bee's `pkg/encryption` primitives run with an
//!   *injected* key and zero ciphertext padding, so they are fully
//!   deterministic. Ant's encryptor must be byte-exact (keystream,
//!   span/data counter split, stored-form address) and its decryptor
//!   must invert them.
//! - **`treeCases`** — whole encrypted trees frozen from bee's *real*
//!   pipeline (`builder.NewPipelineBuilder(…, encrypt=true, rLevel)`).
//!   The chunk keys were random at generation time (bee hardwires
//!   `crypto/rand` in its `chunkEncrypter`, so cross-implementation
//!   reference equality cannot be asserted) but are fixed in the JSON;
//!   ant's decrypting joiner must reproduce the exact payload from the
//!   64-byte root reference. Three cases compose encryption with
//!   Reed-Solomon levels 1, 2 and 4, pinning the encrypted erasure
//!   geometry (`GetMaxEncShards` / `GetEncParities`) on the read path —
//!   and, mirroring the unencrypted `rs.rs` suite, that ant *recovers*
//!   deleted data chunks of those trees byte-for-byte from bee's
//!   ciphertext-space parities, falls back to the dispersed replicas of
//!   a lost encrypted root, fails promptly on over-loss, and serves a
//!   degraded encrypted tree through the production gateway router.
//!
//! Ant's *write* path (random keys) can't be compared byte-for-byte
//! against these trees; its structural equivalence is proven by the
//! `enc_split` unit tests (split → `join_encrypted` round-trips at
//! every level, with and without losses) and cross-client by the
//! mainnet smoke (ant-encrypted upload fetched and decrypted by a
//! canonical bee node).

use ant_conformance::{
    load, spawn_mem_gateway, unhex, unhex_arr, EncryptionVectors, MemChunkStore, MemGatewayConfig,
};
use ant_crypto::{decrypt_chunk, encrypt_chunk_unpadded, keccak256, CHUNK_SIZE, SPAN_SIZE};
use ant_retrieval::{join_encrypted, rs, ChunkFetcher, JoinError};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::time::Duration;

/// Deterministic payload stream, mirroring `det()` in
/// `conformance/vectorgen/main.go`.
fn det(seed: &str, n: usize) -> Vec<u8> {
    let mut cur = keccak256(seed.as_bytes());
    let mut out = Vec::with_capacity(n + 32);
    while out.len() < n {
        out.extend_from_slice(&cur);
        cur = keccak256(&cur);
    }
    out.truncate(n);
    out
}

struct MapFetcher {
    chunks: HashMap<[u8; 32], Vec<u8>>,
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

/// Ant's chunk encryptor reproduces bee's `pkg/encryption` byte-for-byte
/// under an injected key: encrypted span (initCtr 128), encrypted data
/// (initCtr 0), and the BMT address of the zero-padded stored form.
#[test]
fn chunk_encryption_is_byte_exact() {
    let vectors: EncryptionVectors = load("encryption.json");
    assert!(!vectors.chunk_cases.is_empty());
    for case in &vectors.chunk_cases {
        let key: [u8; 32] = unhex_arr("keyHex", &case.key_hex);
        let payload = unhex("payloadHex", &case.payload_hex);
        let plain_span = unhex("plainSpanHex", &case.plain_span_hex);

        let mut plain_wire = plain_span.clone();
        plain_wire.extend_from_slice(&payload);
        let enc = encrypt_chunk_unpadded(&plain_wire, &key);
        assert_eq!(
            hex::encode(&enc[..SPAN_SIZE]),
            case.enc_span_hex,
            "{}: encrypted span",
            case.name
        );
        assert_eq!(
            hex::encode(&enc[SPAN_SIZE..]),
            case.enc_data_hex,
            "{}: encrypted data",
            case.name
        );

        // Zero-padded stored form + address, as the splitter stores it.
        let mut stored = enc;
        stored.resize(SPAN_SIZE + ant_crypto::CHUNK_SIZE, 0);
        assert_eq!(
            hex::encode(&stored),
            case.stored_wire_hex,
            "{}: stored wire",
            case.name
        );
        let span: [u8; SPAN_SIZE] = stored[..SPAN_SIZE].try_into().unwrap();
        let addr = ant_crypto::bmt_hash_with_span(&span, &stored[SPAN_SIZE..]).unwrap();
        assert_eq!(
            hex::encode(addr),
            case.address_hex,
            "{}: address",
            case.name
        );
    }
}

/// Ant's *splitter* under an injected key stores a single-leaf file
/// exactly as bee's primitives dictate: the leaf-chunk vectors double
/// as whole-tree vectors for one-chunk files (root ref = address ‖ key,
/// stored wire = the padded encrypted chunk).
#[test]
fn splitter_single_leaf_matches_vectors() {
    let vectors: EncryptionVectors = load("encryption.json");
    for case in vectors
        .chunk_cases
        .iter()
        .filter(|c| c.name.starts_with("leaf"))
    {
        let key: [u8; 32] = unhex_arr("keyHex", &case.key_hex);
        let payload = unhex("payloadHex", &case.payload_hex);
        let result = ant_retrieval::split_bytes_encrypted_with_keys(&payload, 0, || key);
        assert_eq!(result.chunks.len(), 1, "{}", case.name);
        assert_eq!(
            hex::encode(&result.chunks[0].wire),
            case.stored_wire_hex,
            "{}: stored wire",
            case.name
        );
        assert_eq!(
            hex::encode(result.chunks[0].address),
            case.address_hex,
            "{}: address",
            case.name
        );
        assert_eq!(hex::encode(&result.root_ref[..32]), case.address_hex);
        assert_eq!(hex::encode(&result.root_ref[32..]), case.key_hex);
    }
}

/// Ant's decryptor inverts bee's stored form (leaf cases only —
/// `decrypt_chunk` truncates to the decoded span; the intermediate
/// case's payload is reference-list-shaped and covered below via the
/// whole-tree joins).
#[test]
fn chunk_decryption_inverts_bee() {
    let vectors: EncryptionVectors = load("encryption.json");
    for case in &vectors.chunk_cases {
        let key: [u8; 32] = unhex_arr("keyHex", &case.key_hex);
        let stored = unhex("storedWireHex", &case.stored_wire_hex);
        let payload = unhex("payloadHex", &case.payload_hex);
        let plain_span = unhex("plainSpanHex", &case.plain_span_hex);

        let (span, data) = ant_crypto::decrypt_chunk_parts(&stored, &key).unwrap();
        assert_eq!(span.to_vec(), plain_span, "{}: decrypted span", case.name);
        assert_eq!(
            &data[..payload.len()],
            payload.as_slice(),
            "{}: decrypted payload",
            case.name
        );

        // The leaf cases also decode end-to-end through decrypt_chunk.
        if case.name.starts_with("leaf") {
            let dec = decrypt_chunk(&stored, &key).unwrap();
            assert_eq!(&dec[..SPAN_SIZE], plain_span.as_slice());
            assert_eq!(&dec[SPAN_SIZE..], payload.as_slice());
        }
    }
}

/// `join_encrypted` reproduces the exact payload of every tree bee's
/// real encrypted pipeline emitted, including the encrypted+RS-level-1
/// composition.
#[tokio::test]
async fn joins_bee_encrypted_trees() {
    let vectors: EncryptionVectors = load("encryption.json");
    assert!(!vectors.tree_cases.is_empty());
    for case in &vectors.tree_cases {
        let root_ref: [u8; 64] = unhex_arr("rootRefHex", &case.root_ref_hex);
        let fetcher = MapFetcher {
            chunks: case
                .chunks
                .iter()
                .map(|c| {
                    (
                        unhex_arr::<32>("addressHex", &c.address_hex),
                        unhex("dataHex", &c.data_hex),
                    )
                })
                .collect(),
        };
        let back = join_encrypted(&fetcher, root_ref, 1 << 30)
            .await
            .unwrap_or_else(|e| panic!("{}: join failed: {e}", case.name));
        assert_eq!(
            back,
            det(&format!("enc/{}", case.name), case.file_size),
            "{}: payload mismatch",
            case.name
        );
    }
}

/// Parsed encrypted+RS tree case: the 64-byte root ref, every chunk
/// wire by address, and the root's dispersed replica SOC wires.
struct EncCase {
    name: String,
    level: u8,
    file_size: usize,
    root_ref: [u8; 64],
    chunks: HashMap<[u8; 32], Vec<u8>>,
    replicas: Vec<([u8; 32], Vec<u8>)>,
}

fn parse_redundant_cases() -> Vec<EncCase> {
    let vectors: EncryptionVectors = load("encryption.json");
    let cases: Vec<EncCase> = vectors
        .tree_cases
        .iter()
        .filter(|c| c.level > 0)
        .map(|c| EncCase {
            name: c.name.clone(),
            level: c.level,
            file_size: c.file_size,
            root_ref: unhex_arr("rootRefHex", &c.root_ref_hex),
            chunks: c
                .chunks
                .iter()
                .map(|ch| {
                    (
                        unhex_arr::<32>("addressHex", &ch.address_hex),
                        unhex("dataHex", &ch.data_hex),
                    )
                })
                .collect(),
            replicas: c
                .root_replicas
                .iter()
                .map(|ch| {
                    (
                        unhex_arr::<32>("addressHex", &ch.address_hex),
                        unhex("dataHex", &ch.data_hex),
                    )
                })
                .collect(),
        })
        .collect();
    let levels: HashSet<u8> = cases.iter().map(|c| c.level).collect();
    assert!(
        levels.contains(&1) && levels.contains(&2) && levels.contains(&4),
        "expected encrypted+RS cases at levels 1, 2 and 4"
    );
    cases
}

/// One intermediate node of an encrypted tree: the 32-byte address
/// halves of its 64-byte data references (in ref order) and its parity
/// count — derived exactly like the joiner derives them, by decrypting
/// the node and reading `reference_count_enc` off its decoded span.
struct EncNode {
    data: Vec<[u8; 32]>,
    parity: usize,
}

fn collect_enc_nodes(
    chunks: &HashMap<[u8; 32], Vec<u8>>,
    node_ref: [u8; 64],
    out: &mut Vec<EncNode>,
) {
    let addr: [u8; 32] = node_ref[..32].try_into().unwrap();
    let key: [u8; 32] = node_ref[32..].try_into().unwrap();
    let (span_raw, payload) = ant_crypto::decrypt_chunk_parts(&chunks[&addr], &key)
        .unwrap_or_else(|e| panic!("decrypt node {}: {e}", hex::encode(addr)));
    let (level, plain) = rs::decode_span(span_raw);
    let span = u64::from_le_bytes(plain);
    if span <= CHUNK_SIZE as u64 {
        return; // leaf
    }
    let (shards, parity) = rs::reference_count_enc(span, level);
    let mut data = Vec::with_capacity(shards);
    for i in 0..shards {
        let child: [u8; 64] = payload[i * 64..(i + 1) * 64].try_into().unwrap();
        data.push(child[..32].try_into().unwrap());
        collect_enc_nodes(chunks, child, out);
    }
    out.push(EncNode { data, parity });
}

/// Deterministic pick of `k` shard indices out of `0..shards`, always
/// including the first and the last shard (mirrors `rs.rs`).
fn pick_indices(shards: usize, k: usize) -> Vec<usize> {
    assert!(k >= 1 && k <= shards);
    if k == 1 {
        return vec![0];
    }
    let mut picked: Vec<usize> = (0..k).map(|i| i * (shards - 1) / (k - 1)).collect();
    picked.dedup();
    picked
}

/// Store seeded with every chunk of the case except `skip`.
fn store_without(case: &EncCase, skip: &HashSet<[u8; 32]>) -> MemChunkStore {
    let store = MemChunkStore::default();
    for (addr, wire) in &case.chunks {
        if !skip.contains(addr) {
            store.insert(*addr, wire.clone());
        }
    }
    store
}

fn expected_payload(case: &EncCase) -> Vec<u8> {
    det(&format!("enc/{}", case.name), case.file_size)
}

/// Recovery — delete up to `parityCnt` data chunks per intermediate
/// node (first and last shard always included) from each encrypted+RS
/// tree bee's pipeline froze, and `join_encrypted` must still return
/// the exact payload by reconstructing the missing *ciphertext* wires
/// from the parities. Every recovered wire — fed back through
/// `put_recovered` — must byte-equal bee's original stored chunk, which
/// pins the ciphertext-space RS matrix (parities over encrypted wires,
/// no plaintext-span truncation) against bee's encoder.
#[tokio::test]
async fn recovery_restores_deleted_encrypted_data_chunks_byte_exact() {
    for case in parse_redundant_cases() {
        let mut nodes = Vec::new();
        collect_enc_nodes(&case.chunks, case.root_ref, &mut nodes);
        assert!(nodes.len() >= 2, "{}: want a multi-node tree", case.name);

        let mut deleted: HashSet<[u8; 32]> = HashSet::new();
        for node in &nodes {
            assert!(node.parity > 0, "{}: node without parities", case.name);
            let k = node.parity.min(node.data.len());
            for i in pick_indices(node.data.len(), k) {
                deleted.insert(node.data[i]);
            }
        }
        assert!(!deleted.is_empty());

        let store = store_without(&case, &deleted);
        let out = join_encrypted(&store, case.root_ref, 1 << 30)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "{}: join with {} deleted data chunks failed: {e}",
                    case.name,
                    deleted.len()
                )
            });
        assert_eq!(
            out,
            expected_payload(&case),
            "{}: payload mismatch after recovery",
            case.name
        );

        // `put_recovered` fed every reconstructed ciphertext wire back
        // into the store; each must be byte-identical to what bee's
        // encrypted pipeline uploaded.
        for addr in &deleted {
            let recovered = store.get(addr).unwrap_or_else(|| {
                panic!(
                    "{}: recovered chunk {} was not stored",
                    case.name,
                    hex::encode(addr)
                )
            });
            assert_eq!(
                &recovered,
                &case.chunks[addr],
                "{}: recovered ciphertext {} differs from bee's original",
                case.name,
                hex::encode(addr)
            );
        }
    }
}

/// Over-loss — deleting `parityCnt + 1` data chunks from one node must
/// fail with a clean, terminal `Recovery` error, quickly (the sweep
/// already re-tried every sibling; nothing is retried after it).
#[tokio::test]
async fn encrypted_over_loss_fails_promptly() {
    let cases = parse_redundant_cases();
    let case = cases
        .iter()
        .find(|c| c.level == 1)
        .expect("level-1 encrypted case present");
    let mut nodes = Vec::new();
    collect_enc_nodes(&case.chunks, case.root_ref, &mut nodes);
    let node = nodes
        .iter()
        .find(|n| n.data.len() > n.parity + 1)
        .expect("node with more data shards than parities");
    let deleted: HashSet<[u8; 32]> = node.data.iter().take(node.parity + 1).copied().collect();

    let store = store_without(case, &deleted);
    let err = tokio::time::timeout(
        Duration::from_mins(1),
        join_encrypted(&store, case.root_ref, 1 << 30),
    )
    .await
    .expect("over-loss join must fail promptly, not hang")
    .expect_err("join with parityCnt + 1 losses must fail");
    assert!(
        matches!(err, JoinError::Recovery { .. }),
        "expected a Recovery error, got: {err:?}"
    );
}

/// Root loss — delete the encrypted root chunk (by its 32-byte address
/// half), keep bee's dispersed replica SOCs, and the read must succeed:
/// the replica wraps the *encrypted* root chunk, which the joiner
/// validates against the address half and then decrypts with the key
/// half of the 64-byte reference.
#[tokio::test]
async fn encrypted_root_loss_recovers_via_dispersed_replicas() {
    for case in parse_redundant_cases() {
        let root_addr: [u8; 32] = case.root_ref[..32].try_into().unwrap();
        let mut skip = HashSet::new();
        skip.insert(root_addr);
        let store = store_without(&case, &skip);
        for (soc_addr, wire) in &case.replicas {
            store.insert(*soc_addr, wire.clone());
        }
        assert!(store.get(&root_addr).is_none());

        let out = join_encrypted(&store, case.root_ref, 1 << 30)
            .await
            .unwrap_or_else(|e| panic!("{}: replica-fallback join failed: {e}", case.name));
        assert_eq!(
            out,
            expected_payload(&case),
            "{}: payload mismatch after replica fallback",
            case.name
        );
        // The recovered root was cached under its address half.
        assert_eq!(
            store.get(&root_addr).as_ref(),
            Some(&case.chunks[&root_addr]),
            "{}: replica-unwrapped root differs from the original",
            case.name
        );
    }
}

/// End-to-end through the production gateway router over the in-memory
/// node: an encrypted redundant tree seeded minus data chunks (and
/// minus its root, replicas remaining), fetched as
/// `GET /bytes/{128-hex}` — must be a 200 with the exact body.
#[tokio::test]
async fn gateway_bytes_serves_encrypted_redundant_file_with_losses() {
    let cases = parse_redundant_cases();
    let case = cases
        .iter()
        .find(|c| c.level == 2)
        .expect("level-2 encrypted case present");

    let mut nodes = Vec::new();
    collect_enc_nodes(&case.chunks, case.root_ref, &mut nodes);
    let node = &nodes[0];
    let root_addr: [u8; 32] = case.root_ref[..32].try_into().unwrap();
    let mut skip: HashSet<[u8; 32]> =
        pick_indices(node.data.len(), node.parity.min(node.data.len()))
            .into_iter()
            .map(|i| node.data[i])
            .collect();
    skip.insert(root_addr);

    let gateway = spawn_mem_gateway(MemGatewayConfig::default()).await;
    for (addr, wire) in &case.chunks {
        if !skip.contains(addr) {
            gateway.store().insert(*addr, wire.clone());
        }
    }
    for (soc_addr, wire) in &case.replicas {
        gateway.store().insert(*soc_addr, wire.clone());
    }

    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/bytes/{}",
            gateway.base_url(),
            hex::encode(case.root_ref)
        ))
        .send()
        .await
        .expect("GET /bytes");
    assert_eq!(resp.status(), 200, "gateway must serve the degraded file");
    let body = resp.bytes().await.expect("body");
    assert_eq!(body.len(), case.file_size);
    assert_eq!(
        &body[..],
        &expected_payload(case)[..],
        "gateway body mismatch"
    );
    gateway.shutdown().await;
}

/// The dispersed replicas of an encrypted+RS root are the same
/// derivable SOCs ant mints — proven by validating bee's frozen
/// replica wires against ant's replica addresses for the root's
/// 32-byte address part.
#[test]
fn encrypted_root_replicas_match_ant_derivation() {
    let vectors: EncryptionVectors = load("encryption.json");
    for case in vectors.tree_cases.iter().filter(|c| c.level > 0) {
        let root_ref: [u8; 64] = unhex_arr("rootRefHex", &case.root_ref_hex);
        let root_addr: [u8; 32] = root_ref[..32].try_into().unwrap();
        let derived = ant_retrieval::replica_addresses(&root_addr, case.level);
        assert_eq!(derived.len(), case.root_replicas.len(), "{}", case.name);
        let derived_hex: Vec<String> = derived.iter().map(hex::encode).collect();
        for replica in &case.root_replicas {
            assert!(
                derived_hex.contains(&replica.address_hex),
                "{}: replica {} not derivable from the root address",
                case.name,
                replica.address_hex
            );
            let addr: [u8; 32] = unhex_arr("addressHex", &replica.address_hex);
            let wire = unhex("dataHex", &replica.data_hex);
            assert!(
                ant_crypto::soc_valid(&addr, &wire),
                "{}: replica SOC must self-validate",
                case.name
            );
        }
    }
}
