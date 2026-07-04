//! Bit-exactness tests against bee-generated vectors.
//!
//! Every expected value in `conformance/vectors/*.json` was produced by
//! the canonical Go implementation (see `conformance/vectorgen/`); these
//! tests prove ant derives identical bytes.

use ant_conformance::{load, unhex, unhex_arr};
use ant_crypto::bmt::{cac_new, cac_valid};
use ant_crypto::soc::soc_valid;
use ant_crypto::{
    ethereum_address_from_public_key, keccak256, recover_public_key, sign_handshake_data,
};
use ant_postage::postage_sign_digest;
use ant_retrieval::feed::{sequence_update_address, sequence_update_id, Feed, FeedType};

#[test]
fn cac_addresses_match_bee() {
    let vectors: ant_conformance::CacVectors = load("cac.json");
    assert!(!vectors.cases.is_empty());
    for case in &vectors.cases {
        let payload = unhex("payloadHex", &case.payload_hex);
        let (addr, wire) =
            cac_new(&payload).unwrap_or_else(|| panic!("cac_new failed for vector {}", case.name));
        assert_eq!(
            hex::encode(addr),
            case.address_hex,
            "cac address mismatch for {}",
            case.name
        );
        assert_eq!(
            hex::encode(&wire),
            case.chunk_data_hex,
            "cac wire bytes mismatch for {}",
            case.name
        );
        assert_eq!(
            hex::encode(&wire[..8]),
            case.span_hex,
            "cac span mismatch for {}",
            case.name
        );
        assert!(cac_valid(&addr, &wire), "cac_valid rejected {}", case.name);
    }
}

#[test]
fn soc_signing_and_addressing_match_bee() {
    let vectors: ant_conformance::SocVectors = load("soc.json");
    assert!(!vectors.cases.is_empty());
    for case in &vectors.cases {
        let secret = unhex_arr::<32>("secretHex", &case.secret_hex);
        let owner = unhex_arr::<20>("ownerHex", &case.owner_hex);
        let id = unhex_arr::<32>("idHex", &case.id_hex);
        let payload = unhex("payloadHex", &case.payload_hex);

        let (cac_addr, cac_wire) = cac_new(&payload).expect("cac_new");
        assert_eq!(
            hex::encode(cac_addr),
            case.cac_address_hex,
            "wrapped cac address mismatch for {}",
            case.name
        );

        // Digest bee signs (before the Ethereum message prefix):
        // keccak256(id || cac address).
        let mut digest_input = Vec::with_capacity(64);
        digest_input.extend_from_slice(&id);
        digest_input.extend_from_slice(&cac_addr);
        let digest = keccak256(&digest_input);
        assert_eq!(
            hex::encode(digest),
            case.digest_hex,
            "soc sign digest mismatch for {}",
            case.name
        );

        // bee's crypto.Signer.Sign wraps the digest in the Ethereum
        // message prefix; ant's sign_handshake_data does the same and
        // must be byte-identical (RFC 6979 deterministic nonces).
        let sig = sign_handshake_data(&secret, &digest).expect("sign");
        assert_eq!(
            hex::encode(sig),
            case.signature_hex,
            "soc signature mismatch for {}",
            case.name
        );

        // Owner recovery round-trip.
        let vk = recover_public_key(&sig, &digest).expect("recover");
        assert_eq!(
            hex::encode(ethereum_address_from_public_key(&vk)),
            case.owner_hex,
            "recovered owner mismatch for {}",
            case.name
        );

        // SOC address = keccak256(id || owner).
        let mut addr_input = Vec::with_capacity(52);
        addr_input.extend_from_slice(&id);
        addr_input.extend_from_slice(&owner);
        let soc_addr = keccak256(&addr_input);
        assert_eq!(
            hex::encode(soc_addr),
            case.soc_address_hex,
            "soc address mismatch for {}",
            case.name
        );

        // Full wire layout: id || sig || span || payload.
        let mut wire = Vec::with_capacity(32 + 65 + cac_wire.len());
        wire.extend_from_slice(&id);
        wire.extend_from_slice(&sig);
        wire.extend_from_slice(&cac_wire);
        assert_eq!(
            hex::encode(&wire),
            case.chunk_data_hex,
            "soc wire bytes mismatch for {}",
            case.name
        );
        assert!(
            soc_valid(&soc_addr, &wire),
            "soc_valid rejected bee-signed chunk {}",
            case.name
        );

        // Tampered payload must not validate.
        let mut bad = wire.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0x01;
        assert!(
            !soc_valid(&soc_addr, &bad),
            "soc_valid accepted tampered chunk {}",
            case.name
        );
    }
}

#[test]
fn sequence_feed_ids_match_bee() {
    let vectors: ant_conformance::FeedSequenceVectors = load("feed_sequence.json");
    assert!(!vectors.cases.is_empty());
    for case in &vectors.cases {
        let topic = unhex_arr::<32>("topicHex", &case.topic_hex);
        let owner = unhex_arr::<20>("ownerHex", &case.owner_hex);
        let index: u64 = case.index.parse().expect("index");

        let id = sequence_update_id(&topic, index);
        assert_eq!(
            hex::encode(id),
            case.id_hex,
            "feed id mismatch for topic {} index {}",
            case.topic_hex,
            case.index
        );

        let feed = Feed {
            owner,
            topic,
            kind: FeedType::Sequence,
        };
        let addr = sequence_update_address(&feed, index);
        assert_eq!(
            hex::encode(addr),
            case.soc_address_hex,
            "feed soc address mismatch for topic {} index {}",
            case.topic_hex,
            case.index
        );
    }
}

#[test]
fn epoch_feed_id_derivation_matches_bee() {
    // ant does not implement epoch feeds yet (FeedType::Sequence only);
    // these vectors pin the derivation for when it does. Until then,
    // verify the documented formula — indexBytes = keccak256(BE8(start)
    // || level), id = keccak256(topic || indexBytes) — reproduces bee's
    // output using ant's keccak.
    let vectors: ant_conformance::FeedEpochVectors = load("feed_epoch.json");
    assert!(!vectors.cases.is_empty());
    for case in &vectors.cases {
        let topic = unhex_arr::<32>("topicHex", &case.topic_hex);
        let owner = unhex_arr::<20>("ownerHex", &case.owner_hex);
        let start: u64 = case.start.parse().expect("start");

        let mut index_input = Vec::with_capacity(9);
        index_input.extend_from_slice(&start.to_be_bytes());
        index_input.push(case.level);
        let index_bytes = keccak256(&index_input);
        assert_eq!(hex::encode(index_bytes), case.index_bytes_hex);

        let mut id_input = Vec::with_capacity(64);
        id_input.extend_from_slice(&topic);
        id_input.extend_from_slice(&index_bytes);
        let id = keccak256(&id_input);
        assert_eq!(hex::encode(id), case.id_hex);

        let mut addr_input = Vec::with_capacity(52);
        addr_input.extend_from_slice(&id);
        addr_input.extend_from_slice(&owner);
        assert_eq!(hex::encode(keccak256(&addr_input)), case.soc_address_hex);
    }
}

#[test]
fn feed_update_chunks_match_bee() {
    let vectors: ant_conformance::FeedUpdateVectors = load("feed_updates.json");
    assert!(!vectors.cases.is_empty());
    for case in &vectors.cases {
        let secret = unhex_arr::<32>("secretHex", &case.secret_hex);
        let owner = unhex_arr::<20>("ownerHex", &case.owner_hex);
        let topic = unhex_arr::<32>("topicHex", &case.topic_hex);
        let index: u64 = case.index.parse().expect("index");
        let payload = unhex("payloadHex", &case.payload_hex);

        let id = sequence_update_id(&topic, index);
        assert_eq!(hex::encode(id), case.id_hex, "id mismatch ({})", case.kind);

        let feed = Feed {
            owner,
            topic,
            kind: FeedType::Sequence,
        };
        let soc_addr = sequence_update_address(&feed, index);
        assert_eq!(
            hex::encode(soc_addr),
            case.soc_address_hex,
            "soc address mismatch ({})",
            case.kind
        );

        let (cac_addr, cac_wire) = cac_new(&payload).expect("cac_new");
        let mut digest_input = Vec::with_capacity(64);
        digest_input.extend_from_slice(&id);
        digest_input.extend_from_slice(&cac_addr);
        let sig = sign_handshake_data(&secret, &keccak256(&digest_input)).expect("sign");
        assert_eq!(
            hex::encode(sig),
            case.signature_hex,
            "update signature mismatch ({})",
            case.kind
        );

        let mut wire = Vec::with_capacity(32 + 65 + cac_wire.len());
        wire.extend_from_slice(&id);
        wire.extend_from_slice(&sig);
        wire.extend_from_slice(&cac_wire);
        assert_eq!(
            hex::encode(&wire),
            case.chunk_data_hex,
            "update wire mismatch ({})",
            case.kind
        );
        assert!(soc_valid(&soc_addr, &wire), "soc_valid ({})", case.kind);

        // v1-detection heuristic bee uses (getter.go isV1Length): the
        // wrapped chunk's total data length is 48 (unencrypted ref) or
        // 80 (encrypted ref).
        match case.kind.as_str() {
            "v1" => assert_eq!(cac_wire.len(), 48, "v1 wrapped length"),
            "v1-enc" => assert_eq!(cac_wire.len(), 80, "v1-enc wrapped length"),
            _ => {}
        }
    }
}

#[test]
fn stamp_signing_matches_bee() {
    let vectors: ant_conformance::StampVectors = load("stamps.json");
    assert!(!vectors.cases.is_empty());
    for case in &vectors.cases {
        let secret = unhex_arr::<32>("secretHex", &case.secret_hex);
        let chunk_addr = unhex_arr::<32>("chunkAddrHex", &case.chunk_addr_hex);
        let batch_id = unhex_arr::<32>("batchIdHex", &case.batch_id_hex);
        let index = unhex_arr::<8>("indexHex", &case.index_hex);
        let ts = unhex_arr::<8>("timestampHex", &case.timestamp_hex);

        let digest = postage_sign_digest(&chunk_addr, &batch_id, &index, &ts);
        assert_eq!(
            hex::encode(digest),
            case.digest_hex,
            "stamp digest mismatch for {}",
            case.name
        );

        let sig = sign_handshake_data(&secret, &digest).expect("sign");
        assert_eq!(
            hex::encode(sig),
            case.signature_hex,
            "stamp signature mismatch for {}",
            case.name
        );

        let vk = recover_public_key(&sig, &digest).expect("recover");
        assert_eq!(
            hex::encode(ethereum_address_from_public_key(&vk)),
            case.owner_hex,
            "stamp owner recovery mismatch for {}",
            case.name
        );

        // Wire layout: batchID || index || ts || sig (113 bytes).
        let mut stamp = Vec::with_capacity(113);
        stamp.extend_from_slice(&batch_id);
        stamp.extend_from_slice(&index);
        stamp.extend_from_slice(&ts);
        stamp.extend_from_slice(&sig);
        assert_eq!(
            hex::encode(&stamp),
            case.stamp_hex,
            "stamp wire mismatch for {}",
            case.name
        );
    }
}

#[test]
fn replica_addresses_match_bee() {
    // ant does not implement dispersed replicas yet; pin the address
    // derivation — id = chunk address with id[0] = entropy, soc address
    // = keccak256(id || ReplicasOwner) — for the future implementation.
    let vectors: ant_conformance::ReplicaVectors = load("replicas.json");
    assert!(!vectors.cases.is_empty());
    assert_eq!(vectors.replica_counts, vec![0, 2, 4, 8, 16]);
    let base = unhex_arr::<32>("baseAddressHex", &vectors.base_address_hex);
    let owner = unhex("replicasOwnerHex", &vectors.replicas_owner_hex);
    assert_eq!(
        vectors.replicas_owner_hex,
        "dc5b20847f43d67928f49cd4f85d696b5a7617b5"
    );
    for case in &vectors.cases {
        let mut id = base;
        id[0] = case.entropy;
        assert_eq!(hex::encode(id), case.id_hex, "replica id derivation");
        let mut addr_input = Vec::with_capacity(52);
        addr_input.extend_from_slice(&id);
        addr_input.extend_from_slice(&owner);
        assert_eq!(
            hex::encode(keccak256(&addr_input)),
            case.soc_address_hex,
            "replica soc address for entropy {}",
            case.entropy
        );
    }
}

#[test]
fn redundancy_tables_parse_and_match_documented_shape() {
    // ant does not implement Reed-Solomon recovery yet; these pins keep
    // the bee parity tables available and assert the documented level
    // parameters so a future decoder tests against the same numbers.
    let vectors: ant_conformance::RedundancyVectors = load("redundancy.json");
    assert_eq!(vectors.levels.len(), 5);
    // PARANOID is 39 data shards in bee's code (GetParities(128) == 89),
    // although docs.ethswarm.org describes it as 90 parities / 38 data —
    // the vectors follow the implementation, not the docs.
    let max_shards: Vec<u32> = vectors.levels.iter().map(|l| l.max_shards).collect();
    assert_eq!(max_shards, vec![128, 119, 107, 97, 39]);
    let replica_counts: Vec<u32> = vectors.levels.iter().map(|l| l.replica_count).collect();
    assert_eq!(replica_counts, vec![0, 2, 4, 8, 16]);
    for level in &vectors.levels {
        assert_eq!(level.parities.len(), 128, "level {}", level.name);
        assert_eq!(level.enc_parities.len(), 64, "level {}", level.name);
    }
}
