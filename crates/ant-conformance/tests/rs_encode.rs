//! Reed-Solomon **upload-side** conformance against bee-generated
//! fixtures — the mirror image of `tests/rs.rs`.
//!
//! `conformance/vectors/rs_files.json` holds five files encoded by
//! bee's real upload pipeline (`builder.NewPipelineBuilder`) at
//! `swarm-redundancy-level` 1–4, with **every** chunk the pipeline
//! emitted (data + intermediate + parity + the root's dispersed
//! replica SOCs) plus a separately-minted `rootReplicas` list. These
//! tests prove that ant's encoder (`ant_retrieval::rs_encode`), fed
//! the same payload and level, reproduces bee **exactly**:
//!
//! 1. the same root reference;
//! 2. the same chunk address set — every leaf, intermediate, parity
//!    chunk and replica SOC bee uploads, nothing more, nothing less;
//! 3. byte-identical wire data for every CAC chunk;
//! 4. byte-identical replica SOC wires (which also pins the RFC 6979
//!    determinism of the replica signature against go-ethereum's).
//!
//! If this passes, a redundant upload made through ant is
//! indistinguishable on the network from one made through bee.

use ant_conformance::{load, unhex, unhex_arr, RsFileVectors};
use ant_crypto::keccak256;
use ant_retrieval::rs_encode::{replica_chunks, split_bytes_with_redundancy};
use std::collections::{HashMap, HashSet};

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

/// Bee's replica putter wraps the root data zero-padded to 4104 bytes
/// (`swarm.ChunkWithSpanSize`) — an artifact of its erasure buffer that
/// the in-pipeline replica fixtures reflect. Ant wraps the exact wire.
const SOC_INNER_PADDED: usize = 32 + 65 + 4104;

#[test]
fn encoder_reproduces_bee_root_and_full_chunk_set() {
    let vectors: RsFileVectors = load("rs_files.json");
    assert_eq!(vectors.cases.len(), 5, "expected the five pinned cases");

    for case in &vectors.cases {
        let payload = det(&format!("rs/{}", case.name), case.file_size);
        let result = split_bytes_with_redundancy(&payload, case.level);

        // 1. Root reference equality — the headline.
        let expected_root = unhex_arr::<32>("rootHex", &case.root_hex);
        assert_eq!(
            result.root, expected_root,
            "{}: root reference differs from bee's",
            case.name,
        );
        assert_eq!(result.total_bytes, case.file_size as u64, "{}", case.name);

        let replicas = replica_chunks(&result.root, &result.root_wire, case.level);

        // 2. Address-set equality: bee's pipeline putter received the
        // tree chunks *and* the root's replica SOCs; ant must produce
        // exactly that set.
        let bee_chunks: HashMap<[u8; 32], Vec<u8>> = case
            .chunks
            .iter()
            .map(|c| {
                (
                    unhex_arr::<32>("addressHex", &c.address_hex),
                    unhex("dataHex", &c.data_hex),
                )
            })
            .collect();
        let bee_replicas: HashMap<[u8; 32], Vec<u8>> = case
            .root_replicas
            .iter()
            .map(|c| {
                (
                    unhex_arr::<32>("addressHex", &c.address_hex),
                    unhex("dataHex", &c.data_hex),
                )
            })
            .collect();

        let ours: HashSet<[u8; 32]> = result
            .chunks
            .iter()
            .chain(&replicas)
            .map(|c| c.address)
            .collect();
        let bees: HashSet<[u8; 32]> = bee_chunks.keys().copied().collect();
        if let Some(addr) = bees.difference(&ours).next() {
            panic!(
                "{}: bee uploaded {} but ant's encoder did not produce it",
                case.name,
                hex::encode(addr),
            );
        }
        if let Some(addr) = ours.difference(&bees).next() {
            panic!(
                "{}: ant produced {} which bee never uploaded",
                case.name,
                hex::encode(addr),
            );
        }
        assert_eq!(
            result.chunks.len() + replicas.len(),
            case.chunks.len(),
            "{}: chunk count (duplicates?)",
            case.name,
        );

        // 3. Wire equality for every CAC chunk (leaves, intermediates,
        // parities).
        for chunk in &result.chunks {
            assert_eq!(
                bee_chunks.get(&chunk.address),
                Some(&chunk.wire),
                "{}: chunk {} wire differs from bee's",
                case.name,
                hex::encode(chunk.address),
            );
        }

        // 4. Replica SOC wires: byte-identical to the unpadded fixture
        // list (same id, same deterministic signature, same inner
        // root), and identical to bee's in-pipeline padded form modulo
        // the trailing zero padding.
        assert_eq!(replicas.len(), case.root_replicas.len(), "{}", case.name);
        for replica in &replicas {
            assert_eq!(
                bee_replicas.get(&replica.address),
                Some(&replica.wire),
                "{}: replica SOC {} wire differs from bee's (signature or inner mismatch)",
                case.name,
                hex::encode(replica.address),
            );
            let padded = &bee_chunks[&replica.address];
            assert_eq!(padded.len(), SOC_INNER_PADDED, "{}", case.name);
            assert_eq!(
                &padded[..replica.wire.len()],
                replica.wire.as_slice(),
                "{}: replica differs from bee's padded in-pipeline form",
                case.name,
            );
            assert!(
                padded[replica.wire.len()..].iter().all(|&b| b == 0),
                "{}: bee's in-pipeline replica padding is not all zeros",
                case.name,
            );
        }
    }
}
