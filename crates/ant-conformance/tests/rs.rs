//! Reed-Solomon read-path conformance against bee-generated fixtures.
//!
//! `conformance/vectors/rs_files.json` holds five files encoded by
//! bee's **real** upload pipeline (`builder.NewPipelineBuilder`) at
//! `swarm-redundancy-level` 1–4, including every emitted chunk (data +
//! intermediate + parity) and the dispersed replica SOCs of each root.
//! These tests prove, against those fixtures, that ant:
//!
//! 1. joins redundant trees at all (spans carry level bits, parity refs
//!    must be skipped, the branching factor shrinks to `maxShards`);
//! 2. recovers deleted data chunks from bee's parities **byte-for-byte**
//!    (i.e. the `reed-solomon-erasure` crate is bit-compatible with
//!    bee's `klauspost/reedsolomon` encoder);
//! 3. falls back to the dispersed replicas when the root chunk is gone;
//! 4. fails promptly (no hang, no garbage) when more than `parityCnt`
//!    data chunks of a node are lost;
//! 5. serves all of the above through the production gateway router
//!    while accepting bee's redundancy request headers.

use ant_conformance::{load, spawn_mem_gateway, MemChunkStore, MemGatewayConfig, RsFileVectors};
use ant_crypto::keccak256;
use ant_retrieval::{join, rs, JoinError, DEFAULT_MAX_FILE_BYTES};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Deterministic payload stream, mirroring `det()` in
/// `conformance/vectorgen/main.go`: a keccak256 chain seeded with the
/// label, concatenated until `n` bytes are available.
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

struct Case {
    name: String,
    level: u8,
    file_size: usize,
    root: [u8; 32],
    /// Every chunk bee emitted, by address.
    chunks: HashMap<[u8; 32], Vec<u8>>,
    /// Dispersed replica SOC wires, by SOC address.
    replicas: Vec<([u8; 32], Vec<u8>)>,
}

fn parse_cases() -> Vec<Case> {
    let vectors: RsFileVectors = load("rs_files.json");
    assert_eq!(vectors.cases.len(), 5, "expected the five pinned cases");
    vectors
        .cases
        .iter()
        .map(|c| Case {
            name: c.name.clone(),
            level: c.level,
            file_size: c.file_size,
            root: ant_conformance::unhex_arr::<32>("rootHex", &c.root_hex),
            chunks: c
                .chunks
                .iter()
                .map(|ch| {
                    (
                        ant_conformance::unhex_arr::<32>("addressHex", &ch.address_hex),
                        ant_conformance::unhex("dataHex", &ch.data_hex),
                    )
                })
                .collect(),
            replicas: c
                .root_replicas
                .iter()
                .map(|ch| {
                    (
                        ant_conformance::unhex_arr::<32>("addressHex", &ch.address_hex),
                        ant_conformance::unhex("dataHex", &ch.data_hex),
                    )
                })
                .collect(),
        })
        .collect()
}

/// Store seeded with every chunk of the case except `skip`.
fn store_without(case: &Case, skip: &HashSet<[u8; 32]>) -> MemChunkStore {
    let store = MemChunkStore::default();
    for (addr, wire) in &case.chunks {
        if !skip.contains(addr) {
            store.insert(*addr, wire.clone());
        }
    }
    store
}

/// One intermediate node of the tree: its data-shard addresses (in ref
/// order) and its parity count, derived exactly like the joiner derives
/// them — from the node's own decoded span + level.
struct Node {
    data: Vec<[u8; 32]>,
    parity: usize,
}

fn collect_nodes(chunks: &HashMap<[u8; 32], Vec<u8>>, addr: [u8; 32], out: &mut Vec<Node>) {
    let wire = &chunks[&addr];
    let span_bytes: [u8; 8] = wire[..8].try_into().unwrap();
    let (level, plain) = rs::decode_span(span_bytes);
    let span = u64::from_le_bytes(plain);
    let payload = &wire[8..];
    if span <= payload.len() as u64 {
        return; // leaf
    }
    let refs = payload.len() / 32;
    let (shards, parity) = if level == 0 {
        (refs, 0)
    } else {
        rs::reference_count(span, level)
    };
    assert_eq!(
        shards + parity,
        refs,
        "ref layout mismatch on intermediate node {}",
        hex::encode(addr)
    );
    let data: Vec<[u8; 32]> = (0..shards)
        .map(|i| payload[i * 32..(i + 1) * 32].try_into().unwrap())
        .collect();
    for child in &data {
        collect_nodes(chunks, *child, out);
    }
    out.push(Node { data, parity });
}

/// Deterministic pick of `k` shard indices out of `0..shards`,
/// always including the first and the last shard.
fn pick_indices(shards: usize, k: usize) -> Vec<usize> {
    assert!(k >= 1 && k <= shards);
    if k == 1 {
        return vec![0];
    }
    let mut picked: Vec<usize> = (0..k).map(|i| i * (shards - 1) / (k - 1)).collect();
    picked.dedup();
    picked
}

/// Test 1 — no loss: with every chunk present, ant's joiner reads all
/// five bee-encoded redundant trees back to the exact `det` payload.
/// This alone pins the level-bit span decoding, the data/parity ref
/// split and the `maxShards` branching geometry.
#[tokio::test]
async fn no_loss_join_returns_exact_payload() {
    for case in parse_cases() {
        let store = store_without(&case, &HashSet::new());
        let root_wire = case.chunks[&case.root].clone();
        let out = join(&store, &root_wire, DEFAULT_MAX_FILE_BYTES)
            .await
            .unwrap_or_else(|e| panic!("{}: no-loss join failed: {e}", case.name));
        assert_eq!(out.len(), case.file_size, "{}: length", case.name);
        assert_eq!(
            out,
            det(&format!("rs/{}", case.name), case.file_size),
            "{}: payload mismatch",
            case.name
        );
    }
}

/// Test 2 — recovery: delete up to `parityCnt` data chunks per
/// intermediate node (first and last shard always included; for
/// paranoid-50k that is *all 13* data shards of the root) and join.
/// The bytes must still be exact, and every recovered chunk — leaf and
/// intermediate alike — must equal bee's original wire bytes, which is
/// the byte-compatibility proof for the RS matrix and for ant's
/// recovered-shard truncation (bee's `lastLen` equivalent).
#[tokio::test]
async fn recovery_restores_deleted_data_chunks_byte_exact() {
    for case in parse_cases() {
        let mut nodes = Vec::new();
        collect_nodes(&case.chunks, case.root, &mut nodes);
        assert!(!nodes.is_empty(), "{}: no intermediate nodes", case.name);
        if case.name == "medium-600k" {
            // The multi-node case: root + two mid-level intermediates.
            assert_eq!(nodes.len(), 3, "medium-600k must have 3 nodes");
        }

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
        let root_wire = case.chunks[&case.root].clone();
        let out = join(&store, &root_wire, DEFAULT_MAX_FILE_BYTES)
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
            det(&format!("rs/{}", case.name), case.file_size),
            "{}: payload mismatch after recovery",
            case.name
        );

        // `put_recovered` fed every reconstructed chunk back into the
        // store; each must be byte-identical to what bee uploaded.
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
                "{}: recovered chunk {} differs from bee's original",
                case.name,
                hex::encode(addr)
            );
        }
    }
}

/// Test 3 — root loss: delete the root chunk, keep the dispersed
/// replicas, and read through the replica fallback. Also pins ant's
/// replica-address derivation against the exact SOC set bee's
/// `replicas.NewPutter` produced.
#[tokio::test]
async fn root_loss_recovers_via_dispersed_replicas() {
    for case in parse_cases() {
        // ant's derived replica set must equal bee's uploaded set.
        let derived: HashSet<[u8; 32]> = rs::replica_addresses(&case.root, case.level)
            .into_iter()
            .collect();
        let uploaded: HashSet<[u8; 32]> = case.replicas.iter().map(|(a, _)| *a).collect();
        assert_eq!(
            derived.len(),
            rs::REPLICA_COUNTS[case.level as usize],
            "{}: replica count",
            case.name
        );
        assert_eq!(derived, uploaded, "{}: replica address set", case.name);

        let mut skip = HashSet::new();
        skip.insert(case.root);
        let store = store_without(&case, &skip);
        for (soc_addr, wire) in &case.replicas {
            store.insert(*soc_addr, wire.clone());
        }
        assert!(store.get(&case.root).is_none());

        let root_wire = ant_retrieval::fetch_root_with_replicas(&store, case.root)
            .await
            .unwrap_or_else(|e| panic!("{}: replica fallback failed: {e}", case.name));
        assert_eq!(
            &root_wire, &case.chunks[&case.root],
            "{}: replica-unwrapped root differs from the original root chunk",
            case.name
        );

        let out = join(&store, &root_wire, DEFAULT_MAX_FILE_BYTES)
            .await
            .unwrap_or_else(|e| panic!("{}: join after replica fallback failed: {e}", case.name));
        assert_eq!(
            out,
            det(&format!("rs/{}", case.name), case.file_size),
            "{}: payload mismatch after replica fallback",
            case.name
        );
    }
}

/// Test 4 — over-loss: deleting `parityCnt + 1` data chunks from one
/// node must fail with a clean, terminal `Recovery` error — quickly
/// (recovery failures are not retried), with no partial garbage.
#[tokio::test]
async fn over_loss_fails_promptly() {
    let cases = parse_cases();
    let case = cases
        .iter()
        .find(|c| c.name == "medium-150k")
        .expect("medium-150k vector present");
    let mut nodes = Vec::new();
    collect_nodes(&case.chunks, case.root, &mut nodes);
    // Single-level tree: the root is the only intermediate node.
    assert_eq!(nodes.len(), 1);
    let node = &nodes[0];
    let lost = node.parity + 1;
    assert!(lost <= node.data.len());
    let deleted: HashSet<[u8; 32]> = node.data.iter().take(lost).copied().collect();

    let store = store_without(case, &deleted);
    let root_wire = case.chunks[&case.root].clone();
    let err = tokio::time::timeout(
        Duration::from_mins(1),
        join(&store, &root_wire, DEFAULT_MAX_FILE_BYTES),
    )
    .await
    .expect("over-loss join must fail promptly, not hang")
    .expect_err("join with parityCnt + 1 losses must fail");
    assert!(
        matches!(err, JoinError::Recovery { .. }),
        "expected a Recovery error, got: {err:?}"
    );
}

/// Test 5 — end-to-end through the production gateway router over the
/// in-memory node: chunk loss *and* root loss at once, plus bee's
/// redundancy request headers (which ant accepts and ignores — the
/// DATA-equivalent strategy with recovery is always on).
#[tokio::test]
async fn gateway_bytes_serves_redundant_file_with_losses() {
    let cases = parse_cases();
    let case = cases
        .iter()
        .find(|c| c.name == "strong-150k")
        .expect("strong-150k vector present");

    let mut nodes = Vec::new();
    collect_nodes(&case.chunks, case.root, &mut nodes);
    let node = &nodes[0];
    // Lose the root (replicas remain) and parityCnt data chunks.
    let mut skip: HashSet<[u8; 32]> = node.data.iter().take(node.parity).copied().collect();
    skip.insert(case.root);

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
            hex::encode(case.root)
        ))
        .header("swarm-redundancy-strategy", "3")
        .header("swarm-redundancy-fallback-mode", "true")
        .header("swarm-chunk-retrieval-timeout", "30s")
        .send()
        .await
        .expect("GET /bytes");
    assert_eq!(resp.status(), 200, "gateway must serve the degraded file");
    let body = resp.bytes().await.expect("body");
    assert_eq!(body.len(), case.file_size);
    assert_eq!(
        &body[..],
        &det(&format!("rs/{}", case.name), case.file_size)[..],
        "gateway body mismatch"
    );
    gateway.shutdown().await;
}
