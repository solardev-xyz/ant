//! Tier-3 mainnet cross-client interop smoke.
//!
//! Runs a REAL ant light node against Swarm mainnet with a funded
//! postage batch, uploads content, exercises read-your-own-writes and
//! the live feed flow (the swarm-kit repro), then boots a local **bee
//! ultra-light node** (download-only, unfunded) as an independent
//! cross-client verifier: everything ant pushed must be retrievable
//! and correctly interpreted by canonical bee from the real network.
//!
//! Opt-in and credentialed. Reads `<repo>/.env` (and process env, which
//! wins) for:
//!
//! ```text
//! GNOSIS_RPC_URL=…
//! SWARM_BATCH_ID=<64 hex>
//! SWARM_OWNER_PRIVATE_KEY=0x<64 hex>   # batch owner; also signs feed SOCs
//! ```
//!
//! Also needs binaries: `target/release/antd` (cargo build --release -p
//! antd) and a bee binary (env `BEE_BIN`, default `../bee-bin` next to
//! the repo scratchpad — pass `BEE_BIN` explicitly if in doubt). Costs: stamp slots from the batch; no
//! on-chain transactions.
//!
//! ```sh
//! cargo run --release -p ant-conformance --bin mainnet-smoke
//! ```

use ant_crypto::{keccak256, sign_handshake_data};
use ant_retrieval::feed::sequence_update_id;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ANT_API: &str = "127.0.0.1:2733";
const BEE_API: &str = "127.0.0.1:2833";

struct ChildGuard(Child, &'static str);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        eprintln!("stopping {}", self.1);
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn load_env() -> HashMap<String, String> {
    let mut vars = HashMap::new();
    if let Ok(body) = std::fs::read_to_string(repo_root().join(".env")) {
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                vars.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    for (k, v) in std::env::vars() {
        vars.insert(k, v);
    }
    vars
}

struct Check {
    name: &'static str,
    ok: bool,
    detail: String,
}

struct Harness {
    client: reqwest::Client,
    checks: Vec<Check>,
}

impl Harness {
    fn record(&mut self, name: &'static str, ok: bool, detail: impl Into<String>) {
        let detail = detail.into();
        println!("  [{}] {name}{}", if ok { "ok  " } else { "FAIL" }, {
            if detail.is_empty() {
                String::new()
            } else {
                format!(" — {detail}")
            }
        });
        self.checks.push(Check { name, ok, detail });
    }

    async fn wait_http_ok(&self, url: &str, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(r) = self.client.get(url).send().await {
                if r.status().is_success() {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        false
    }

    /// Poll `url` until it returns 200 with `expected` as the body.
    async fn wait_for_body(&self, url: &str, expected: &[u8], timeout: Duration) -> (bool, String) {
        let start = Instant::now();
        let mut last = String::from("no response");
        while start.elapsed() < timeout {
            if let Ok(r) = self.client.get(url).send().await {
                let status = r.status().as_u16();
                if let Ok(body) = r.bytes().await {
                    if status == 200 && body.as_ref() == expected {
                        return (true, format!("after {:.0?}", start.elapsed()));
                    }
                    last = format!("status {status}, {} bytes", body.len());
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        (false, format!("timed out ({timeout:?}); last: {last}"))
    }
}

#[tokio::main]
async fn main() {
    let env = load_env();
    let Some(rpc) = env.get("GNOSIS_RPC_URL") else {
        eprintln!("SKIP: GNOSIS_RPC_URL not set (populate <repo>/.env)");
        std::process::exit(2);
    };
    let Some(batch) = env.get("SWARM_BATCH_ID") else {
        eprintln!("SKIP: SWARM_BATCH_ID not set");
        std::process::exit(2);
    };
    let Some(key) = env.get("SWARM_OWNER_PRIVATE_KEY") else {
        eprintln!("SKIP: SWARM_OWNER_PRIVATE_KEY not set");
        std::process::exit(2);
    };
    let key_hex = key.trim_start_matches("0x").to_string();
    let secret: [u8; 32] = hex::decode(&key_hex)
        .expect("key hex")
        .try_into()
        .expect("key length");
    let owner = {
        use k256::ecdsa::SigningKey;
        let sk = SigningKey::from_bytes((&secret).into()).expect("key scalar");
        ant_crypto::ethereum_address_from_public_key(&sk.verifying_key().clone())
    };
    let owner_hex = hex::encode(owner);

    let root = repo_root();
    let antd_bin = root.join("target/release/antd");
    assert!(
        antd_bin.exists(),
        "build antd first: cargo build --release -p antd"
    );
    let bee_bin = env.get("BEE_BIN").map_or_else(
        || PathBuf::from("/tmp/claude-1000/-home-florian-claude/68db7b94-adad-46ac-952b-9188c9d5cec2/scratchpad/bee-bin"),
        PathBuf::from,
    );

    let mut h = Harness {
        client: reqwest::Client::new(),
        checks: Vec::new(),
    };

    // Unique-per-run content so re-runs push fresh chunks.
    let run_nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // ---- phase 1: start antd against mainnet --------------------------------
    println!(
        "phase 1: starting antd (mainnet light node, batch {}…)",
        &batch[..8]
    );
    let ant_dir = std::env::temp_dir().join(format!("ant-mainnet-smoke-{run_nonce}"));
    std::fs::create_dir_all(&ant_dir).unwrap();
    let antd = ChildGuard(
        Command::new(&antd_bin)
            .arg("--data-dir")
            .arg(&ant_dir)
            .arg("--api-addr")
            .arg(ANT_API)
            .arg("--log-level")
            .arg("warn")
            .env("GNOSIS_RPC_URL", rpc)
            .env("STORAGE_STAMP_BATCH_ID", batch)
            .env("STORAGE_STAMP_PRIVATE_KEY", &key_hex)
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(ant_dir.join("antd.log"))
                    .map_or_else(|_| Stdio::null(), Stdio::from),
            )
            .spawn()
            .expect("spawn antd"),
        "antd",
    );
    let ant = format!("http://{ANT_API}");
    assert!(
        h.wait_http_ok(&format!("{ant}/health"), Duration::from_mins(1))
            .await,
        "antd api did not come up (see {}/antd.log)",
        ant_dir.display()
    );

    // Wait for a working peer set before pushing.
    let mut peers = 0usize;
    let start = Instant::now();
    while start.elapsed() < Duration::from_mins(2) {
        if let Ok(r) = h.client.get(format!("{ant}/peers")).send().await {
            if let Ok(body) = r.bytes().await {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
                    peers = v["peers"].as_array().map_or(0, Vec::len);
                    if peers >= 30 {
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    h.record("antd peers warmed", peers >= 30, format!("{peers} peers"));

    // Since v0.5.35 the HTTP API binds BEFORE chain init resolves, so
    // /health answers while the startup batch registration is still in
    // flight and an immediate upload 503s with "uploads not
    // configured". Wait until GET /stamps lists the configured batch
    // as usable before pushing anything.
    let batch_ready = {
        let want = batch.trim_start_matches("0x").to_lowercase();
        let start = Instant::now();
        let mut ready = false;
        while start.elapsed() < Duration::from_mins(3) {
            if let Ok(r) = h.client.get(format!("{ant}/stamps")).send().await {
                if let Ok(body) = r.bytes().await {
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
                        ready = v["stamps"].as_array().is_some_and(|a| {
                            a.iter().any(|s| {
                                s["batchID"].as_str().is_some_and(|b| b == want)
                                    && s["usable"].as_bool().unwrap_or(false)
                            })
                        });
                        if ready {
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        ready
    };
    h.record("postage batch registered (chain init)", batch_ready, "");

    // ---- phase 2: upload + read-your-own-writes -----------------------------
    println!("phase 2: upload via ant + immediate self-reads");
    let file_body =
        format!("ant tier-3 mainnet smoke {run_nonce}\ncross-client interop test file\n");
    let file_ref = match h
        .client
        .post(format!("{ant}/bzz?name=smoke-{run_nonce}.txt"))
        .header("content-type", "text/plain")
        .header("swarm-postage-batch-id", batch)
        .body(file_body.clone())
        .send()
        .await
    {
        Ok(r) if r.status() == 201 => {
            let body = r.bytes().await.expect("upload body");
            let v: serde_json::Value = serde_json::from_slice(&body).expect("upload json");
            let reference = v["reference"].as_str().expect("reference").to_string();
            h.record("bzz upload via ant", true, reference.clone());
            reference
        }
        Ok(r) => {
            let status = r.status().as_u16();
            let body = r.text().await.unwrap_or_default();
            h.record(
                "bzz upload via ant",
                false,
                format!("status {status}: {body}"),
            );
            finish(h);
            return;
        }
        Err(e) => {
            h.record("bzz upload via ant", false, e.to_string());
            finish(h);
            return;
        }
    };

    // Immediate self-read (read-your-own-writes through the real node).
    let (ok, detail) = h
        .wait_for_body(
            &format!("{ant}/bzz/{file_ref}/"),
            file_body.as_bytes(),
            Duration::from_secs(15),
        )
        .await;
    h.record("immediate self-read of upload", ok, detail);

    // ---- phase 2b: encrypted uploads (swarm-encrypt: true) -------------------
    // Encrypted references are random per upload, so cross-backend
    // reference equality can't exist; the proof is the round-trip —
    // and, in phase 5, canonical bee decrypting ant-encrypted content
    // fetched from the real network (including ant's encrypted
    // mantaray manifest on the /bzz path).
    println!("phase 2b: encrypted uploads via ant + self-reads");
    let enc_bytes_body = format!("ant mainnet smoke encrypted bytes {run_nonce}\n").repeat(300);
    let enc_bytes_ref = match h
        .client
        .post(format!("{ant}/bytes"))
        .header("content-type", "application/octet-stream")
        .header("swarm-postage-batch-id", batch)
        .header("swarm-encrypt", "true")
        .body(enc_bytes_body.clone())
        .send()
        .await
    {
        Ok(r) if r.status() == 201 => {
            let body = r.bytes().await.expect("upload body");
            let v: serde_json::Value = serde_json::from_slice(&body).expect("upload json");
            let reference = v["reference"].as_str().unwrap_or_default().to_string();
            let ok = reference.len() == 128;
            h.record(
                "encrypted bytes upload via ant (128-hex ref)",
                ok,
                format!(
                    "{}… len={}",
                    &reference[..16.min(reference.len())],
                    reference.len()
                ),
            );
            ok.then_some(reference)
        }
        Ok(r) => {
            let status = r.status().as_u16();
            let body = r.text().await.unwrap_or_default();
            h.record(
                "encrypted bytes upload via ant (128-hex ref)",
                false,
                format!("status {status}: {body}"),
            );
            None
        }
        Err(e) => {
            h.record(
                "encrypted bytes upload via ant (128-hex ref)",
                false,
                e.to_string(),
            );
            None
        }
    };
    if let Some(r) = &enc_bytes_ref {
        let (ok, detail) = h
            .wait_for_body(
                &format!("{ant}/bytes/{r}"),
                enc_bytes_body.as_bytes(),
                Duration::from_secs(15),
            )
            .await;
        h.record("self-read of encrypted bytes", ok, detail);
    }

    let enc_file_body =
        format!("ant mainnet smoke encrypted file {run_nonce}\nvia encrypted mantaray manifest\n");
    let enc_file_ref = match h
        .client
        .post(format!("{ant}/bzz?name=smoke-enc-{run_nonce}.txt"))
        .header("content-type", "text/plain")
        .header("swarm-postage-batch-id", batch)
        .header("swarm-encrypt", "true")
        .body(enc_file_body.clone())
        .send()
        .await
    {
        Ok(r) if r.status() == 201 => {
            let body = r.bytes().await.expect("upload body");
            let v: serde_json::Value = serde_json::from_slice(&body).expect("upload json");
            let reference = v["reference"].as_str().unwrap_or_default().to_string();
            let ok = reference.len() == 128;
            h.record(
                "encrypted bzz upload via ant (128-hex ref)",
                ok,
                format!(
                    "{}… len={}",
                    &reference[..16.min(reference.len())],
                    reference.len()
                ),
            );
            ok.then_some(reference)
        }
        Ok(r) => {
            let status = r.status().as_u16();
            let body = r.text().await.unwrap_or_default();
            h.record(
                "encrypted bzz upload via ant (128-hex ref)",
                false,
                format!("status {status}: {body}"),
            );
            None
        }
        Err(e) => {
            h.record(
                "encrypted bzz upload via ant (128-hex ref)",
                false,
                e.to_string(),
            );
            None
        }
    };
    if let Some(r) = &enc_file_ref {
        let (ok, detail) = h
            .wait_for_body(
                &format!("{ant}/bzz/{r}/"),
                enc_file_body.as_bytes(),
                Duration::from_secs(15),
            )
            .await;
        h.record("self-read of encrypted bzz upload", ok, detail);
    }

    // ---- phase 3: live feed flow (the swarm-kit repro) -----------------------
    println!("phase 3: feed write + immediate latest reads (swarm-kit repro)");
    let topic = keccak256(format!("ant-conformance/mainnet-smoke/{run_nonce}").as_bytes());
    let topic_hex = hex::encode(topic);

    let update_client = h.client.clone();
    let post_update = |idx: u64, payload: Vec<u8>| {
        let client = update_client.clone();
        let ant = ant.clone();
        let batch = batch.clone();
        let id = sequence_update_id(&topic, idx);
        let (cac_addr, cac_wire) = ant_crypto::cac_new(&payload).expect("cac");
        let mut digest_input = Vec::with_capacity(64);
        digest_input.extend_from_slice(&id);
        digest_input.extend_from_slice(&cac_addr);
        let sig = sign_handshake_data(&secret, &keccak256(&digest_input)).expect("sign");
        let owner_hex = owner_hex.clone();
        async move {
            let r = client
                .post(format!(
                    "{ant}/soc/{owner_hex}/{}?sig={}",
                    hex::encode(id),
                    hex::encode(sig)
                ))
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", batch)
                .body(cac_wire)
                .send()
                .await;
            match r {
                Ok(r) if r.status() == 201 => Ok(()),
                Ok(r) => Err(format!("status {}", r.status())),
                Err(e) => Err(e.to_string()),
            }
        }
    };

    let upd0 = format!("mainnet feed update 0 @{run_nonce}").into_bytes();
    let upd1 = format!("mainnet feed update 1 @{run_nonce}").into_bytes();

    match post_update(0, upd0.clone()).await {
        Ok(()) => h.record("feed update 0 via ant /soc", true, ""),
        Err(e) => h.record("feed update 0 via ant /soc", false, e),
    }
    let (ok, detail) = h
        .wait_for_body(
            &format!("{ant}/feeds/{owner_hex}/{topic_hex}"),
            &upd0,
            Duration::from_secs(20),
        )
        .await;
    h.record("immediate latest == update 0", ok, detail);

    match post_update(1, upd1.clone()).await {
        Ok(()) => h.record("feed update 1 via ant /soc", true, ""),
        Err(e) => h.record("feed update 1 via ant /soc", false, e),
    }
    let (ok, detail) = h
        .wait_for_body(
            &format!("{ant}/feeds/{owner_hex}/{topic_hex}"),
            &upd1,
            Duration::from_secs(20),
        )
        .await;
    h.record("immediate latest == update 1", ok, detail);

    // ---- phase 4: stewardship + envelope on the live node --------------------
    println!("phase 4: stewardship + envelope");
    match h
        .client
        .get(format!("{ant}/stewardship/{file_ref}"))
        .send()
        .await
    {
        Ok(r) => {
            let status = r.status().as_u16();
            let body = r.text().await.unwrap_or_default();
            h.record(
                "stewardship isRetrievable",
                status == 200 && body.contains("true"),
                format!("status {status}: {body}"),
            );
        }
        Err(e) => h.record("stewardship isRetrievable", false, e.to_string()),
    }
    match h
        .client
        .post(format!("{ant}/envelope/{file_ref}"))
        .header("swarm-postage-batch-id", batch)
        .send()
        .await
    {
        Ok(r) => {
            let status = r.status().as_u16();
            h.record(
                "envelope for uploaded ref",
                status == 201,
                format!("status {status}"),
            );
        }
        Err(e) => h.record("envelope for uploaded ref", false, e.to_string()),
    }

    // ---- phase 5: independent verification via bee ultra-light ---------------
    println!("phase 5: bee ultra-light cross-verification (this takes a few minutes)");
    if !bee_bin.exists() {
        h.record(
            "bee binary present",
            false,
            format!("{} missing (set BEE_BIN)", bee_bin.display()),
        );
        finish(h);
        return;
    }
    let bee_dir = std::env::temp_dir().join(format!("bee-mainnet-smoke-{run_nonce}"));
    std::fs::create_dir_all(&bee_dir).unwrap();
    let bee_proc = ChildGuard(
        Command::new(&bee_bin)
            .arg("start")
            .arg("--data-dir")
            .arg(&bee_dir)
            .arg("--password")
            .arg("mainnet-smoke")
            .arg("--api-addr")
            .arg(BEE_API)
            .arg("--p2p-addr")
            .arg(":2834")
            .arg("--swap-enable=false")
            .arg("--full-node=false")
            .arg("--verbosity")
            .arg("2")
            .stdout(
                std::fs::File::create(bee_dir.join("bee.log"))
                    .map_or_else(|_| Stdio::null(), Stdio::from),
            )
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn bee"),
        "bee",
    );
    let bee = format!("http://{BEE_API}");
    let bee_up = h
        .wait_http_ok(&format!("{bee}/health"), Duration::from_mins(2))
        .await;
    h.record("bee ultra-light api up", bee_up, "");
    if bee_up {
        // bee needs peers + our chunks need to have propagated to their
        // storers; poll generously.
        let (ok, detail) = h
            .wait_for_body(
                &format!("{bee}/bzz/{file_ref}/"),
                file_body.as_bytes(),
                Duration::from_mins(5),
            )
            .await;
        h.record("bee retrieves ant's bzz upload", ok, detail);

        let (ok, detail) = h
            .wait_for_body(
                &format!("{bee}/feeds/{owner_hex}/{topic_hex}"),
                &upd1,
                Duration::from_mins(3),
            )
            .await;
        h.record("bee resolves ant's feed (latest == update 1)", ok, detail);

        // Real cross-client encryption proof: canonical bee fetches
        // ant-encrypted content from the mainnet and decrypts it with
        // the key carried in the 128-hex reference — including walking
        // ant's *encrypted mantaray manifest* on the /bzz path.
        if let Some(r) = &enc_bytes_ref {
            let (ok, detail) = h
                .wait_for_body(
                    &format!("{bee}/bytes/{r}"),
                    enc_bytes_body.as_bytes(),
                    Duration::from_mins(3),
                )
                .await;
            h.record("bee decrypts ant's encrypted /bytes upload", ok, detail);
        } else {
            h.record(
                "bee decrypts ant's encrypted /bytes upload",
                false,
                "skipped: encrypted bytes upload failed",
            );
        }
        if let Some(r) = &enc_file_ref {
            let (ok, detail) = h
                .wait_for_body(
                    &format!("{bee}/bzz/{r}/"),
                    enc_file_body.as_bytes(),
                    Duration::from_mins(3),
                )
                .await;
            h.record(
                "bee walks ant's encrypted manifest (bzz round-trip)",
                ok,
                detail,
            );
        } else {
            h.record(
                "bee walks ant's encrypted manifest (bzz round-trip)",
                false,
                "skipped: encrypted bzz upload failed",
            );
        }

        // Keep the guard alive until here.
        drop(bee_proc);
    }

    drop(antd);
    finish(h);
}

fn finish(h: Harness) {
    let failed: Vec<&Check> = h.checks.iter().filter(|c| !c.ok).collect();
    println!(
        "\n=== mainnet smoke: {}/{} checks passed ===",
        h.checks.len() - failed.len(),
        h.checks.len()
    );
    for c in &failed {
        println!("FAILED: {} — {}", c.name, c.detail);
    }
    std::process::exit(i32::from(!failed.is_empty()));
}
