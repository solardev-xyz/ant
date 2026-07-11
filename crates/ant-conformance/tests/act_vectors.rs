//! ACT (access control) conformance against bee-generated vectors
//! (`conformance/vectors/act.json`, produced by bee's own
//! `pkg/accesscontrol` — see `conformance/vectorgen/act.go`).
//!
//! Deterministic tiers assert ant's primitives byte-exact: the ECDH
//! session KDF, access-key wrap, reference encryption, the ACT
//! key-value store's simple-manifest JSON + chunk address, and
//! multi-epoch history manifest roots (which also proves ant's
//! rebuild-from-entries produces bee's incremental load-add-store
//! bytes). The frozen tiers replay full `accesscontrol.Controller`
//! outputs (random access keys) that ant must resolve from the chunks
//! alone — publisher-side and grantee-side.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;

use ant_conformance::{load, unhex, unhex_arr, ActVectors};
use ant_crypto::act as actc;
use ant_retrieval::act as acts;
use ant_retrieval::ChunkFetcher;
use async_trait::async_trait;

struct MapFetcher {
    chunks: HashMap<[u8; 32], Vec<u8>>,
}

impl MapFetcher {
    fn from_entries(entries: &[ant_conformance::RsChunkEntry]) -> Self {
        let mut chunks = HashMap::new();
        for e in entries {
            chunks.insert(
                unhex_arr::<32>("addressHex", &e.address_hex),
                unhex("dataHex", &e.data_hex),
            );
        }
        Self { chunks }
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

#[test]
fn session_kdf_matches_bee() {
    let v: ActVectors = load("act.json");
    assert!(!v.kdf_cases.is_empty());
    for case in &v.kdf_cases {
        let secret: [u8; 32] = unhex_arr("secretHex", &case.secret_hex);
        let pub_bytes = unhex("counterpartyPubHex", &case.counterparty_pub_hex);
        let public = actc::parse_public_key(&pub_bytes).expect("vector public key");
        let (lookup, akdk) =
            actc::lookup_and_ak_decryption_keys(&secret, &public).expect("derive keys");
        assert_eq!(hex::encode(lookup), case.lookup_key_hex, "{}", case.name);
        assert_eq!(hex::encode(akdk), case.akdk_hex, "{}", case.name);
    }
    // ECDH symmetry: publisher->grantee and grantee->publisher agree.
    assert_eq!(v.kdf_cases[1].lookup_key_hex, v.kdf_cases[2].lookup_key_hex);
    assert_eq!(v.kdf_cases[1].akdk_hex, v.kdf_cases[2].akdk_hex);
}

#[test]
fn wrap_and_reference_encryption_match_bee() {
    let v: ActVectors = load("act.json");
    assert!(!v.ref_cases.is_empty());
    for case in &v.ref_cases {
        let key: [u8; 32] = unhex_arr("accessKeyHex", &case.access_key_hex);
        let reference = unhex("refHex", &case.ref_hex);
        let enc = actc::transform_reference(&reference, &key);
        assert_eq!(hex::encode(&enc), case.encrypted_hex, "{}", case.name);
        // XOR stream: decrypting round-trips.
        assert_eq!(actc::transform_reference(&enc, &key), reference);
    }
}

#[test]
fn kvs_manifest_bytes_and_root_match_bee() {
    let v: ActVectors = load("act.json");
    assert!(!v.kvs_cases.is_empty());
    for case in &v.kvs_cases {
        let mut kvs = acts::ActKvs::new();
        for row in &case.rows {
            kvs.put(
                &unhex("keyHex", &row.key_hex),
                &unhex("valueHex", &row.value_hex),
            );
        }
        let bytes = kvs.to_manifest_bytes();
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            case.manifest_json,
            "{}: simple-manifest JSON must be byte-identical to Go's json.Marshal",
            case.name
        );
        let split = ant_retrieval::split_bytes(&bytes);
        assert_eq!(hex::encode(split.root), case.root_hex, "{}", case.name);
        // Every chunk bee stored must come out of ant's splitter too.
        let ours: HashMap<String, Vec<u8>> = split
            .chunks
            .iter()
            .map(|c| (hex::encode(c.address), c.wire.clone()))
            .collect();
        for chunk in &case.chunks {
            assert_eq!(
                ours.get(&chunk.address_hex).map(hex::encode),
                Some(chunk.data_hex.clone()),
                "{}: chunk {}",
                case.name,
                chunk.address_hex
            );
        }
    }
}

#[tokio::test]
async fn history_manifest_roots_match_bee() {
    let v: ActVectors = load("act.json");
    assert!(!v.history_cases.is_empty());
    for case in &v.history_cases {
        // Rebuild the history epoch by epoch: ant's canonical rebuild
        // must hit bee's incremental (load-add-store) root each time.
        let mut entries: Vec<acts::HistoryEntry> = Vec::new();
        for step in &case.steps {
            let manifest = acts::history_with_epoch(
                &entries,
                acts::history_key(step.timestamp),
                unhex_arr("actRefHex", &step.act_ref_hex),
                step.metadata.clone(),
            )
            .expect("append epoch");
            assert_eq!(
                hex::encode(manifest.root),
                step.root_hex,
                "{}: root after t={}",
                case.name,
                step.timestamp
            );
            // Every node chunk must byte-match one bee stored.
            let bee: HashMap<&str, &str> = case
                .chunks
                .iter()
                .map(|c| (c.address_hex.as_str(), c.data_hex.as_str()))
                .collect();
            for chunk in &manifest.chunks {
                let addr = hex::encode(chunk.address);
                assert_eq!(
                    bee.get(addr.as_str()).copied(),
                    Some(hex::encode(&chunk.wire)).as_deref(),
                    "{}: node chunk {addr}",
                    case.name
                );
            }
            entries.push(acts::HistoryEntry {
                key: acts::history_key(step.timestamp),
                reference: unhex_arr("actRefHex", &step.act_ref_hex),
                metadata: step.metadata.clone(),
            });
            entries.sort_by(|a, b| a.key.cmp(&b.key));
        }

        // Read side: walk bee's stored final history and check both the
        // recovered entries and the Lookup truth table.
        let fetcher = MapFetcher::from_entries(&case.chunks);
        let final_root: [u8; 32] =
            unhex_arr("rootHex", &case.steps.last().expect("steps").root_hex);
        let walked = acts::history_entries(&fetcher, final_root)
            .await
            .expect("walk bee history");
        assert_eq!(walked.len(), case.steps.len(), "{}", case.name);
        for (w, e) in walked.iter().zip(entries.iter()) {
            assert_eq!(w.key, e.key, "{}", case.name);
            assert_eq!(w.reference, e.reference, "{}", case.name);
            assert_eq!(
                w.metadata,
                e.metadata.clone().into_iter().collect::<BTreeMap<_, _>>(),
                "{}",
                case.name
            );
        }
        for lookup in &case.lookups {
            match acts::history_lookup(&walked, lookup.timestamp) {
                Ok(entry) => {
                    assert!(
                        lookup.error.is_empty(),
                        "{}: t={}",
                        case.name,
                        lookup.timestamp
                    );
                    assert_eq!(
                        hex::encode(entry.reference),
                        lookup.act_ref_hex,
                        "{}: t={}",
                        case.name,
                        lookup.timestamp
                    );
                }
                Err(e) => {
                    assert!(
                        !lookup.error.is_empty(),
                        "{}: t={} unexpectedly failed: {e}",
                        case.name,
                        lookup.timestamp
                    );
                    // Bee's error strings are pinned by ant's ActError
                    // Display impls.
                    assert_eq!(
                        e.to_string(),
                        lookup.error,
                        "{}: t={}",
                        case.name,
                        lookup.timestamp
                    );
                }
            }
        }
    }
}

/// Resolve an access key exactly like the gateway's download path:
/// walk the history, pick the epoch, load the kvs, unwrap our row.
async fn resolve_access_key(
    fetcher: &dyn ChunkFetcher,
    secret: &[u8; 32],
    counterparty: &k256::PublicKey,
    history_root: [u8; 32],
    timestamp: i64,
) -> [u8; 32] {
    let entries = acts::history_entries(fetcher, history_root)
        .await
        .expect("walk history");
    let entry = acts::history_lookup(&entries, timestamp).expect("epoch for timestamp");
    let kvs = acts::ActKvs::load(fetcher, entry.reference)
        .await
        .expect("load kvs");
    let (lookup, akdk) =
        actc::lookup_and_ak_decryption_keys(secret, counterparty).expect("derive keys");
    let sealed: [u8; 32] = kvs
        .get(&lookup)
        .expect("grantee row present")
        .as_slice()
        .try_into()
        .expect("sealed access key is 32 bytes");
    actc::seal_access_key(&sealed, &akdk)
}

#[tokio::test]
async fn frozen_upload_flow_resolves_as_publisher() {
    let v: ActVectors = load("act.json");
    let flow = &v.upload_flow;
    let secret: [u8; 32] = unhex_arr("publisherSecretHex", &flow.publisher_secret_hex);
    let publisher = actc::public_key_of(&secret).expect("publisher key");
    let fetcher = MapFetcher::from_entries(&flow.chunks);

    let history_root: [u8; 32] = unhex_arr("historyRefHex", &flow.history_ref_hex);
    // Far-future timestamp resolves the newest (only) epoch.
    let access_key = resolve_access_key(&fetcher, &secret, &publisher, history_root, 1 << 62).await;

    let encrypted = unhex("encryptedRefHex", &flow.encrypted_ref_hex);
    let decrypted = actc::transform_reference(&encrypted, &access_key);
    assert_eq!(hex::encode(decrypted), flow.content_ref_hex);
}

#[tokio::test]
async fn frozen_grantee_flow_resolves_as_grantee_and_publisher() {
    let v: ActVectors = load("act.json");
    let flow = &v.grantee_flow;
    let publisher_secret: [u8; 32] = unhex_arr("publisherSecretHex", &flow.publisher_secret_hex);
    let publisher = actc::public_key_of(&publisher_secret).expect("publisher key");
    let fetcher = MapFetcher::from_entries(&flow.chunks);
    let history_root: [u8; 32] = unhex_arr("historyRefHex", &flow.history_ref_hex);

    // Publisher side: decrypt the grantee-list reference and read the
    // list bee stored (bee's `Get`), asserting order.
    let key = actc::session_keys(&publisher_secret, &publisher, &[actc::NONCE_AK_DECRYPT])
        .expect("publisher wrap key")[0];
    let egrantee = unhex("egranteeRefHex", &flow.egrantee_ref_hex);
    let grantee_ref = actc::transform_reference(&egrantee, &key);
    assert_eq!(grantee_ref.len(), 64, "grantee lists store encrypted");
    let data = acts::load_encrypted(&fetcher, grantee_ref.as_slice().try_into().unwrap())
        .await
        .expect("join grantee list");
    let grantees = acts::deserialize_grantees(&data);
    let got: Vec<String> = grantees
        .iter()
        .map(|pk| hex::encode(actc::compress_public_key(pk)))
        .collect();
    assert_eq!(got, flow.grantee_pubs_hex);

    // Grantee side: each grantee secret unwraps the same access key by
    // its own ECDH row and decrypts the content reference.
    let encrypted = unhex("encryptedRefHex", &flow.encrypted_ref_hex);
    for secret_hex in &flow.grantee_secrets_hex {
        let grantee_secret: [u8; 32] = unhex_arr("granteeSecretsHex", secret_hex);
        let access_key =
            resolve_access_key(&fetcher, &grantee_secret, &publisher, history_root, 1 << 62).await;
        let decrypted = actc::transform_reference(&encrypted, &access_key);
        assert_eq!(hex::encode(decrypted), flow.content_ref_hex);
    }

    // Publisher can decrypt too (self-row).
    let access_key = resolve_access_key(
        &fetcher,
        &publisher_secret,
        &publisher,
        history_root,
        1 << 62,
    )
    .await;
    assert_eq!(
        hex::encode(actc::transform_reference(&encrypted, &access_key)),
        flow.content_ref_hex
    );
}
