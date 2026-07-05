//! Differential matrix for sequence-feed lookup semantics: ant's
//! 4-phase finder (`ant_retrieval::feed`) vs bee's `asyncFinder`
//! (`bee/pkg/feeds/sequence`), compared through the real HTTP surface
//! of both backends (`GET /feeds/{owner}/{topic}`) with beemock — bee's
//! actual API layer — as the oracle.
//!
//! For each update PATTERN (dense, gappy, sparse, deep) the test signs
//! real v1 sequence updates with the scenario secret (the
//! tests/vectors.rs SOC pattern), writes them to BOTH backends via
//! `POST /soc`, then compares a set of QUERIES: plain latest, `?after`
//! below/at/above the head, `?at` (which bee parses but ignores for
//! sequence feeds), `?type=epoch` (ignored entirely), the
//! `swarm-only-root-chunk` header, and the malformed-parameter 400
//! shapes. Status, body, and the swarm-feed-* / soc-signature headers
//! must all match.
//!
//! Divergences that are intentional (bee's asyncFinder is racy on
//! gapped feeds — see the registry notes) live in
//! `conformance/divergences.json` under scenario `feeds-matrix`; a diff
//! matching a registry entry is reported KNOWN and does not fail.
//!
//! Requires the beemock binary (skips with a notice when absent, like
//! tests/differential.rs).

use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

// --- beemock child process (same contract as tests/differential.rs) ---------

struct Beemock {
    child: Child,
    base: String,
    batch_id: String,
}

impl Drop for Beemock {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn beemock_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("BEEMOCK_BIN") {
        let p = PathBuf::from(p);
        return p.exists().then_some(p);
    }
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/beemock/beemock");
    p.exists().then_some(p)
}

#[derive(Deserialize)]
struct BeemockHello {
    listening: String,
    #[serde(rename = "batchId")]
    batch_id: String,
}

async fn spawn_beemock(bin: &PathBuf) -> Beemock {
    // Offset by 7 vs the other conformance test binaries so parallel
    // runs in one `cargo test -p ant-conformance` don't collide.
    let port = 20000 + ((std::process::id() + 7) % 10000) as u16;
    let mut child = Command::new(bin)
        .arg("-addr")
        .arg(format!("127.0.0.1:{port}"))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn beemock");
    let stdout = child.stdout.take().expect("beemock stdout");
    let hello_line = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufReader::new(stdout).read_line(&mut line).ok();
        line
    })
    .await
    .expect("join stdout reader");
    let hello: BeemockHello =
        serde_json::from_str(hello_line.trim()).expect("parse beemock hello line");
    let base = format!("http://{}", hello.listening);

    let client = reqwest::Client::new();
    for _ in 0..100 {
        if let Ok(r) = client.get(format!("{base}/health")).send().await {
            if r.status().is_success() {
                return Beemock {
                    child,
                    base,
                    batch_id: hello.batch_id,
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("beemock did not become healthy at {base}");
}

// --- divergence registry (same file + shape as tests/differential.rs) -------

#[derive(Deserialize)]
struct DivergenceEntry {
    scenario: String,
    step: String,
    aspect: String,
    note: String,
}

fn load_registry() -> Vec<DivergenceEntry> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/divergences.json");
    let blob = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("read divergence registry {}: {e}", path.display()));
    serde_json::from_slice(&blob).expect("parse divergences.json")
}

// --- feed identity + update signing ------------------------------------------

fn matrix_secret() -> [u8; 32] {
    ant_crypto::keccak256(b"ant-conformance/feeds-matrix/key")
}

fn matrix_owner() -> [u8; 20] {
    let sk = k256::ecdsa::SigningKey::from_bytes(&matrix_secret().into()).expect("valid secret");
    ant_crypto::ethereum_address_from_public_key(sk.verifying_key())
}

fn pattern_topic(name: &str) -> [u8; 32] {
    ant_crypto::keccak256(format!("feeds-matrix/topic/{name}").as_bytes())
}

/// Build the `POST /soc/{owner}/{id}?sig=…` request parts for a v1
/// sequence update at `index` wrapping `content_ref`: payload =
/// `ts(8 BE) ‖ ref(32)`, signed with the matrix secret exactly like
/// bee's `feeds.NewUpdate` (the tests/vectors.rs SOC pattern).
fn soc_update_request(topic: &[u8; 32], index: u64, content_ref: [u8; 32]) -> (String, Vec<u8>) {
    let secret = matrix_secret();
    let owner_hex = hex::encode(matrix_owner());
    let id = ant_retrieval::sequence_update_id(topic, index);
    let mut payload = (1_720_000_000u64 + index).to_be_bytes().to_vec();
    payload.extend_from_slice(&content_ref);
    let (cac_addr, cac_wire) = ant_crypto::cac_new(&payload).expect("payload fits a chunk");
    let mut digest_input = Vec::with_capacity(64);
    digest_input.extend_from_slice(&id);
    digest_input.extend_from_slice(&cac_addr);
    let digest = ant_crypto::keccak256(&digest_input);
    let sig = ant_crypto::sign_handshake_data(&secret, &digest).expect("sign feed update");
    (
        format!(
            "/soc/{owner_hex}/{}?sig={}",
            hex::encode(id),
            hex::encode(sig)
        ),
        cac_wire,
    )
}

// --- observation --------------------------------------------------------------

/// Headers compared on every feed response.
const COMPARED_HEADERS: &[&str] = &[
    "content-type",
    "etag",
    "swarm-feed-index",
    "swarm-feed-index-next",
    "swarm-feed-resolved-version",
    "swarm-soc-signature",
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct Obs {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

/// GET the feed with a bounded wall-clock. Bee's asyncFinder can HANG
/// on gapped feeds (see the `feeds-matrix` registry notes); a timeout
/// is folded into a synthetic status-0 observation so the run keeps
/// going and the diff is register-able.
async fn feed_get(client: &reqwest::Client, base: &str, path_and_query: &str) -> Obs {
    let url = format!("{base}{path_and_query}");
    let resp = match client
        .get(&url)
        .timeout(Duration::from_secs(8))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) if e.is_timeout() => {
            return Obs {
                status: 0,
                headers: BTreeMap::new(),
                body: "<timeout: no response within 8s>".into(),
            };
        }
        Err(e) => panic!("GET {url}: {e}"),
    };
    let status = resp.status().as_u16();
    let mut headers = BTreeMap::new();
    for name in COMPARED_HEADERS {
        if let Some(v) = resp.headers().get(*name) {
            headers.insert(
                (*name).to_string(),
                v.to_str().unwrap_or("<non-ascii>").to_string(),
            );
        }
    }
    let raw = match resp.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => format!("<body read error: {e}>").into_bytes(),
    };
    // JSON bodies compare semantically (bee's jsonhttp appends "\n\n").
    let body = if headers
        .get("content-type")
        .is_some_and(|ct| ct.contains("json"))
    {
        match serde_json::from_slice::<Value>(&raw) {
            Ok(v) => v.to_string(),
            Err(_) => hex::encode(&raw),
        }
    } else {
        String::from_utf8_lossy(&raw).into_owned()
    };
    Obs {
        status,
        headers,
        body,
    }
}

// --- matrix -------------------------------------------------------------------

struct Pattern {
    name: &'static str,
    indices: Vec<u64>,
    /// Query/header variants probed against this pattern, appended to
    /// the base `/feeds/{owner}/{topic}` URL. `("", None)` is the plain
    /// latest lookup; the `Option` is a `(header, value)` pair.
    queries: Vec<(&'static str, Option<(&'static str, &'static str)>)>,
}

fn patterns() -> Vec<Pattern> {
    let latest = vec![("", None)];
    vec![
        Pattern {
            name: "single-0",
            indices: vec![0],
            queries: vec![
                ("", None),
                ("?after=0", None),
                ("?after=1", None),
                // Root-chunk branch: body is the wrapped chunk verbatim.
                ("", Some(("swarm-only-root-chunk", "true"))),
                ("", Some(("swarm-only-root-chunk", "maybe"))),
            ],
        },
        Pattern {
            name: "dense-0-1-2",
            indices: vec![0, 1, 2],
            queries: vec![
                ("", None),
                ("?after=0", None),
                ("?after=1", None),
                ("?after=2", None),
                ("?after=3", None),
                // `at` parses but is IGNORED for sequence feeds.
                ("?at=1", None),
                ("?at=-1", None),
                ("?at=99999999999", None),
                ("?at=1&after=1", None),
                // `type` is never parsed by bee's GET handler.
                ("?type=epoch", None),
                // Malformed params: bee's mapStructure 400 shapes.
                ("?at=abc", None),
                ("?after=abc", None),
                ("?after=-1", None),
                ("?after=18446744073709551616", None),
                ("?at=9223372036854775808", None),
            ],
        },
        // Gap right after 0: bee's probe lattice (base+2^l-1 =
        // 1,3,7,…) never sees index 2 and stops at 0.
        Pattern {
            name: "gap-0-2",
            indices: vec![0, 2],
            queries: latest.clone(),
        },
        // Gap at 1..2 with 3 present: 3 IS on bee's lattice, which
        // makes the asyncFinder racy (found-at-3 vs not-found-at-1
        // arrival order) — it either answers index 0 or hangs.
        Pattern {
            name: "gap-0-3",
            indices: vec![0, 3],
            queries: vec![("", None), ("?after=3", None)],
        },
        // Gap at 2 with 3 present, bridged by 1: both finders reach 3
        // (bee via its base+2^l-1 lattice, ant via the exponential
        // bracket) — an agreeing gap case pinned as a regression.
        Pattern {
            name: "bridge-0-1-3",
            indices: vec![0, 1, 3],
            queries: latest.clone(),
        },
        // Dense prefix 0..3, gap at 4, orphan at 5: bee's second
        // window (base=3, level=2) probes 4 and 6 — never 5 — and
        // deterministically answers 3; ant's binary search bisects
        // (3,7) and lands exactly on 5. Registered: bee's answer is a
        // probe-lattice artifact ({0,1,2,3,6} would flip it to 6, or
        // hang, depending on goroutine arrival order).
        Pattern {
            name: "lattice-0-1-2-3-5",
            indices: vec![0, 1, 2, 3, 5],
            queries: latest.clone(),
        },
        // No index 0: the anchor probe misses ⇒ "no update found",
        // even though updates exist; `?after=1` re-anchors and finds 2.
        Pattern {
            name: "no-index-0",
            indices: vec![1, 2],
            queries: vec![("", None), ("?after=1", None)],
        },
        // Crosses bee's DefaultLevels=8 first-window boundary.
        Pattern {
            name: "dense-0-8",
            indices: (0..=8).collect(),
            queries: latest.clone(),
        },
        Pattern {
            name: "deep-0-33",
            indices: (0..=33).collect(),
            queries: vec![
                ("", None),
                ("?after=33", None),
                ("?after=34", None),
                ("?after=16", None),
            ],
        },
        Pattern {
            name: "only-5",
            indices: vec![5],
            queries: vec![("", None), ("?after=5", None)],
        },
    ]
}

struct Diff {
    step: String,
    aspect: String,
    ant: String,
    bee: String,
}

fn compare(step: &str, ant: &Obs, bee: &Obs) -> Vec<Diff> {
    let mut diffs = Vec::new();
    if ant.status != bee.status {
        diffs.push(Diff {
            step: step.to_string(),
            aspect: "status".into(),
            ant: ant.status.to_string(),
            bee: bee.status.to_string(),
        });
    }
    let keys: std::collections::BTreeSet<&String> =
        ant.headers.keys().chain(bee.headers.keys()).collect();
    for key in keys {
        let a = ant.headers.get(key);
        let b = bee.headers.get(key);
        if a != b {
            diffs.push(Diff {
                step: step.to_string(),
                aspect: format!("header:{key}"),
                ant: a.cloned().unwrap_or_else(|| "<absent>".into()),
                bee: b.cloned().unwrap_or_else(|| "<absent>".into()),
            });
        }
    }
    if ant.body != bee.body {
        diffs.push(Diff {
            step: step.to_string(),
            aspect: "body".into(),
            ant: ant.body.clone(),
            bee: bee.body.clone(),
        });
    }
    diffs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feeds_matrix_ant_vs_bee() {
    let Some(bin) = beemock_bin() else {
        eprintln!(
            "SKIP feeds_matrix_ant_vs_bee: beemock binary not found; \
             build it with `cd conformance/beemock && go build -o beemock .` \
             or set BEEMOCK_BIN"
        );
        return;
    };

    let bee = spawn_beemock(&bin).await;
    let ant_gw =
        ant_conformance::spawn_mem_gateway(ant_conformance::MemGatewayConfig::default()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build http client");

    let owner_hex = hex::encode(matrix_owner());
    let registry = load_registry();
    let mut known: Vec<(Diff, String)> = Vec::new();
    let mut failures: Vec<Diff> = Vec::new();
    let mut ok_steps = 0usize;

    for pattern in patterns() {
        let topic = pattern_topic(pattern.name);
        let topic_hex = hex::encode(topic);

        // Stage the update chain on both backends: content chunk via
        // /bytes (content-addressed ⇒ identical refs), then the signed
        // SOC update.
        for &i in &pattern.indices {
            let body = format!("feeds-matrix {} update {i}", pattern.name);
            let mut refs = Vec::new();
            for base in [&ant_gw.base_url(), &bee.base] {
                let resp = client
                    .post(format!("{base}/bytes"))
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", &bee.batch_id)
                    .body(body.clone())
                    .send()
                    .await
                    .expect("upload content");
                assert_eq!(resp.status().as_u16(), 201, "content upload on {base}");
                let raw = resp.bytes().await.expect("upload response body");
                let v: Value = serde_json::from_slice(&raw).expect("upload response json");
                refs.push(v["reference"].as_str().expect("reference").to_string());
            }
            assert_eq!(refs[0], refs[1], "content refs must match");
            let content_ref: [u8; 32] = hex::decode(&refs[0])
                .expect("hex ref")
                .try_into()
                .expect("32-byte ref");

            let (path, wire) = soc_update_request(&topic, i, content_ref);
            for base in [&ant_gw.base_url(), &bee.base] {
                let resp = client
                    .post(format!("{base}{path}"))
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", &bee.batch_id)
                    .body(wire.clone())
                    .send()
                    .await
                    .expect("soc upload");
                assert_eq!(
                    resp.status().as_u16(),
                    201,
                    "SOC write {}[{i}] on {base}",
                    pattern.name
                );
            }
        }

        for (query, header) in &pattern.queries {
            let path = format!("/feeds/{owner_hex}/{topic_hex}{query}");
            let step = match header {
                Some((h, v)) => format!("{}{query}+{h}={v}", pattern.name),
                None => format!("{}{query}", pattern.name),
            };
            let get = |base: String| {
                let client = &client;
                let path = &path;
                async move {
                    let url_path = path.clone();
                    match header {
                        Some((h, v)) => {
                            // feed_get with an extra header: inline here.
                            let url = format!("{base}{url_path}");
                            let resp = match client
                                .get(&url)
                                .header(*h, *v)
                                .timeout(Duration::from_secs(8))
                                .send()
                                .await
                            {
                                Ok(r) => r,
                                Err(e) if e.is_timeout() => {
                                    return Obs {
                                        status: 0,
                                        headers: BTreeMap::new(),
                                        body: "<timeout: no response within 8s>".into(),
                                    };
                                }
                                Err(e) => panic!("GET {url}: {e}"),
                            };
                            let status = resp.status().as_u16();
                            let mut headers = BTreeMap::new();
                            for name in COMPARED_HEADERS {
                                if let Some(v) = resp.headers().get(*name) {
                                    headers.insert(
                                        (*name).to_string(),
                                        v.to_str().unwrap_or("<non-ascii>").to_string(),
                                    );
                                }
                            }
                            let raw = resp.bytes().await.map(|b| b.to_vec()).unwrap_or_default();
                            let body = if headers
                                .get("content-type")
                                .is_some_and(|ct| ct.contains("json"))
                            {
                                serde_json::from_slice::<Value>(&raw)
                                    .map_or_else(|_| hex::encode(&raw), |v| v.to_string())
                            } else {
                                String::from_utf8_lossy(&raw).into_owned()
                            };
                            Obs {
                                status,
                                headers,
                                body,
                            }
                        }
                        None => feed_get(client, &base, &url_path).await,
                    }
                }
            };
            let ant_obs = get(ant_gw.base_url()).await;
            let bee_obs = get(bee.base.clone()).await;
            let diffs = compare(&step, &ant_obs, &bee_obs);
            if diffs.is_empty() {
                ok_steps += 1;
            }
            for d in diffs {
                let entry = registry.iter().find(|e| {
                    e.scenario == "feeds-matrix" && e.step == d.step && e.aspect == d.aspect
                });
                match entry {
                    Some(e) => known.push((d, e.note.clone())),
                    None => failures.push(d),
                }
            }
        }
    }

    ant_gw.shutdown().await;

    println!("\n=== feeds matrix: {ok_steps} steps identical ===");
    if !known.is_empty() {
        println!("--- known divergences ({}) ---", known.len());
        for (d, note) in &known {
            println!(
                "KNOWN {} [{}] ant={} bee={} ({note})",
                d.step, d.aspect, d.ant, d.bee
            );
        }
    }
    if !failures.is_empty() {
        println!("--- UNREGISTERED divergences ({}) ---", failures.len());
        for d in &failures {
            println!(
                "DIFF {} [{}]\n  ant: {}\n  bee: {}",
                d.step, d.aspect, d.ant, d.bee
            );
        }
    }
    assert!(
        failures.is_empty(),
        "{} unregistered feed divergence(s) between ant and bee — fix ant's finder or \
         (if bee is demonstrably broken) register them in conformance/divergences.json",
        failures.len()
    );
}
