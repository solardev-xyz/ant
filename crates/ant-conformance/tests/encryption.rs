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
//!   64-byte root reference. One case composes encryption with
//!   Reed-Solomon level 1, pinning the encrypted erasure geometry
//!   (`GetMaxEncShards` / `GetEncParities`) on the read path.
//!
//! Ant's *write* path (random keys) can't be compared byte-for-byte
//! against these trees; its structural equivalence is proven by the
//! `enc_split` unit tests (split → `join_encrypted` round-trips at
//! every level) and cross-client by the mainnet smoke (ant-encrypted
//! upload fetched and decrypted by a canonical bee node).

use ant_conformance::{load, unhex, unhex_arr, EncryptionVectors};
use ant_crypto::{decrypt_chunk, encrypt_chunk_unpadded, keccak256, SPAN_SIZE};
use ant_retrieval::{join_encrypted, ChunkFetcher};
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;

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
