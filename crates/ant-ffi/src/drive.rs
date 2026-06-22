//! Upload, storage-plan, and account helpers backing the `AntDrive` demo
//! app's FFI surface.
//!
//! The download/stream entry points in [`crate`] cover the "read" half
//! of a Swarm light node; this module covers the "write" + "account"
//! half `AntDrive` needs:
//!
//! * **Uploads** drive the same daemon-resident [`UploadManager`] jobs
//!   `antctl upload` uses (start / list / status / pause / resume /
//!   cancel) through the node's control-command channel. The actual
//!   chunking + postage stamping + pushsync happens on the manager's
//!   own task; these helpers only kick jobs off and read their state.
//! * **Storage plan** maps the node's local postage-stamp issuer to a
//!   Dropbox-style "how much room do I have" view (`PostageStatus`).
//!   With the `chain` feature, `connect_batch` / `discover` read the
//!   batch the account owns on Gnosis and register it so uploads can
//!   stamp against it.
//! * **Account** exposes the node identity (address / overlay / peer
//!   id) and the raw signing key for a "back up your account" flow.
//!
//! Everything returns a JSON string (or a plain string / error) so the
//! Swift side decodes one shape per call instead of marshalling a
//! length-prefixed array of variable-size C structs.

use ant_control::{ControlAck, ControlCommand};
use ant_postage::StampIssuer;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::AntHandle;

/// Upper bound on a single control round-trip. Upload start / list /
/// status / postage reads are all local daemon ops that complete in
/// milliseconds; the timeout only guards against a wedged node loop.
const OP_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound for a propagation check. Unlike the other control ops this
/// is a network operation — it fetches every interior node plus a large
/// sample of data leaves over the live network — so it legitimately runs
/// for many seconds on a big file. A generous ceiling keeps it from being
/// cut short (which surfaced as a missing verdict / silent failure) while
/// still guarding against a wedged loop. Raised now that a full check is
/// uncapped (every leaf, however large the file); the user can also cancel
/// a long check from the detail view, so a high ceiling is safe.
const VERIFY_TIMEOUT: Duration = Duration::from_mins(30);

#[derive(Debug, thiserror::Error)]
pub(crate) enum DriveError {
    #[error("{0}")]
    Op(String),
}

/// Reload every postage batch persisted under `<data_dir>/postage/*.bin`
/// into a fresh issuer registry, so a user's storage plan survives an
/// app restart without re-reading the chain. Mirrors `antd`'s startup
/// reload. Unreadable stores are skipped with a warning rather than
/// failing the whole node bring-up.
pub(crate) fn reload_persisted_issuers(
    postage_dir: &std::path::Path,
) -> HashMap<[u8; 32], StampIssuer> {
    let mut issuers = HashMap::new();
    if !postage_dir.is_dir() {
        return issuers;
    }
    let entries = match std::fs::read_dir(postage_dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target: "ant-ffi", dir = %postage_dir.display(), "scan postage dir: {e}");
            return issuers;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("bin") {
            continue;
        }
        match StampIssuer::open_existing(path.clone()) {
            Ok(iss) => {
                let id = *iss.batch_id();
                tracing::info!(
                    target: "ant-ffi",
                    batch = %format!("0x{}", hex::encode(id)),
                    "reloaded persisted postage batch",
                );
                issuers.insert(id, iss);
            }
            Err(e) => tracing::warn!(
                target: "ant-ffi",
                store = %path.display(),
                "skipping unreadable postage store: {e}",
            ),
        }
    }
    issuers
}

// ---------------------------------------------------------------------------
// Uploads
// ---------------------------------------------------------------------------

pub(crate) fn upload_start(
    h: &AntHandle,
    source_path: PathBuf,
    batch_id: Option<String>,
    name: Option<String>,
    content_type: Option<String>,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(
            &cmd_tx,
            ControlCommand::UploadStart {
                source_path,
                batch_id,
                name,
                content_type,
                raw: false,
                ack: ack_tx,
            },
        )
        .await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::UploadStarted { job_id } => Ok(job_id),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

pub(crate) fn upload_list(h: &AntHandle) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(&cmd_tx, ControlCommand::UploadList { ack: ack_tx }).await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::UploadList(jobs) => to_json(&Jobs { jobs }),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

/// Shared body for the single-job commands that all ack with
/// [`ControlAck::UploadJob`]: status, pause, resume, cancel.
pub(crate) fn upload_job_command(
    h: &AntHandle,
    job_id: String,
    kind: JobCommand,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        let cmd = match kind {
            JobCommand::Status => ControlCommand::UploadStatus {
                job_id,
                ack: ack_tx,
            },
            JobCommand::Pause => ControlCommand::UploadPause {
                job_id,
                ack: ack_tx,
            },
            JobCommand::Resume => ControlCommand::UploadResume {
                job_id,
                ack: ack_tx,
            },
            JobCommand::Cancel => ControlCommand::UploadCancel {
                job_id,
                ack: ack_tx,
            },
        };
        send(&cmd_tx, cmd).await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::UploadJob(view) => to_json(&view),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

/// "Push again" with streamed progress: re-push a completed job's missing
/// chunks on the same job, handing each `{"phase":...,"checked"?,"total"?}`
/// progress line to `on_progress` as it arrives, then return the updated
/// job JSON. Mirrors [`verify_propagation_progress`]: the callback fires on
/// the FFI runtime thread inline with the blocking call.
pub(crate) fn upload_repush_progress(
    h: &AntHandle,
    job_id: String,
    mut on_progress: impl FnMut(&str),
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        let (prog_tx, mut prog_rx) = mpsc::unbounded_channel::<String>();
        send(
            &cmd_tx,
            ControlCommand::UploadRepush {
                job_id,
                progress: Some(prog_tx),
                ack: ack_tx,
            },
        )
        .await?;

        let finish = |ack: Result<ControlAck, oneshot::error::RecvError>| match ack {
            Ok(ControlAck::UploadJob(view)) => to_json(&view),
            Ok(ControlAck::NotReady { message } | ControlAck::Error { message }) => {
                Err(DriveError::Op(message))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(_) => Err(DriveError::Op("node dropped the ack channel".into())),
        };

        // Heal can run a few read-back/re-push rounds; reuse the generous
        // verification timeout as an upper bound.
        let mut ack_rx = ack_rx;
        let deadline = tokio::time::sleep(VERIFY_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                ack = &mut ack_rx => return finish(ack),
                line = prog_rx.recv() => {
                    match line {
                        Some(l) => on_progress(&l),
                        None => return finish((&mut ack_rx).await),
                    }
                }
                () = &mut deadline => {
                    return Err(DriveError::Op("operation timed out".into()));
                }
            }
        }
    })
}

#[derive(Clone, Copy)]
pub(crate) enum JobCommand {
    Status,
    Pause,
    Resume,
    Cancel,
}

// ---------------------------------------------------------------------------
// Storage plan
// ---------------------------------------------------------------------------

pub(crate) fn storage_status(h: &AntHandle) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime
        .block_on(async move { postage_status_json(&cmd_tx).await })
}

/// Read-back propagation check for an uploaded `reference` (the root
/// chunk / manifest address). Asks the node loop to probe up to
/// `probes` distinct closest peers with a network-only retrieval and
/// returns the JSON `{reference, retrievable, sources, probes}` the
/// handler builds. `sources` counts how many distinct entry peers
/// served the chunk back — a route-diversity / replication estimate —
/// so the app can show a "verified retrievable" badge instead of the
/// bare "online" state, which only reflects that we *attempted* the
/// push.
pub(crate) fn verify_propagation(
    h: &AntHandle,
    reference: [u8; 32],
    samples: u8,
    probes: u8,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(
            &cmd_tx,
            ControlCommand::VerifyPropagation {
                reference,
                samples,
                probes,
                progress: None,
                cancel: None,
                ack: ack_tx,
            },
        )
        .await?;
        match recv_oneshot_within(ack_rx, VERIFY_TIMEOUT).await? {
            ControlAck::Ok { message } => Ok(message),
            ControlAck::NotReady { message } | ControlAck::Error { message } => {
                Err(DriveError::Op(message))
            }
            other => Err(unexpected(&other)),
        }
    })
}

/// Like [`verify_propagation`], but streams incremental progress: each
/// `{"phase":...,"checked"?,"total"?}` JSON line the node emits is handed
/// to `on_progress` as it arrives, before the final verdict JSON is
/// returned. The callback runs on the FFI runtime thread inline with the
/// blocking call, so the caller (Swift) keeps it cheap (a channel hop).
pub(crate) fn verify_propagation_progress(
    h: &AntHandle,
    reference: [u8; 32],
    samples: u8,
    probes: u8,
    mut on_progress: impl FnMut(&str),
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    // Fresh cancel flag for this check: clear any prior request so a cancel
    // can only ever abort the check it was aimed at. Shared with the node's
    // verify task via the command.
    h.verify_cancel
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let cancel = h.verify_cancel.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        let (prog_tx, mut prog_rx) = mpsc::unbounded_channel::<String>();
        send(
            &cmd_tx,
            ControlCommand::VerifyPropagation {
                reference,
                samples,
                probes,
                progress: Some(prog_tx),
                cancel: Some(cancel),
                ack: ack_tx,
            },
        )
        .await?;

        let finish = |ack: Result<ControlAck, oneshot::error::RecvError>| match ack {
            Ok(ControlAck::Ok { message }) => Ok(message),
            Ok(ControlAck::NotReady { message } | ControlAck::Error { message }) => {
                Err(DriveError::Op(message))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(_) => Err(DriveError::Op("node dropped the ack channel".into())),
        };

        // Drain progress until the terminal ack lands (or we time out).
        // Bias toward the ack so completion isn't delayed behind a backlog
        // of progress lines.
        let mut ack_rx = ack_rx;
        let deadline = tokio::time::sleep(VERIFY_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                ack = &mut ack_rx => return finish(ack),
                line = prog_rx.recv() => {
                    match line {
                        Some(l) => on_progress(&l),
                        // Progress channel closed (verify finished); the ack
                        // is next — await it directly rather than re-looping
                        // on a now-closed receiver.
                        None => return finish((&mut ack_rx).await),
                    }
                }
                () = &mut deadline => {
                    return Err(DriveError::Op("operation timed out".into()));
                }
            }
        }
    })
}

async fn postage_status_json(cmd_tx: &mpsc::Sender<ControlCommand>) -> Result<String, DriveError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    send(cmd_tx, ControlCommand::PostageStatus { ack: ack_tx }).await?;
    match recv_oneshot(ack_rx).await? {
        ControlAck::PostageStatus(view) => to_json(&view),
        ControlAck::Error { message } => Err(DriveError::Op(message)),
        other => Err(unexpected(&other)),
    }
}

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

pub(crate) fn account_info(h: &AntHandle) -> Result<String, DriveError> {
    let snap = h.status_rx.borrow();
    to_json(&AccountInfo {
        eth_address: format!("0x{}", hex::encode(h.eth)),
        overlay: snap.identity.overlay.clone(),
        peer_id: snap.identity.peer_id.clone(),
        agent: snap.agent.clone(),
    })
}

pub(crate) fn account_export_key(h: &AntHandle) -> String {
    hex::encode(h.signing_secret)
}

// ---------------------------------------------------------------------------
// On-chain "connect storage" — only with the `chain` feature.
// ---------------------------------------------------------------------------

/// Register the postage batch `batch_hex` (owned by this account on
/// Gnosis) so uploads can stamp against it. One `eth_call` for the
/// batch metadata, then a `RegisterBatch` into the live issuer
/// registry. Returns the refreshed [`storage_status`] JSON.
#[cfg(feature = "chain")]
pub(crate) fn storage_connect_batch(
    h: &AntHandle,
    rpc: String,
    batch_hex: String,
) -> Result<String, DriveError> {
    let batch_id = parse_batch_id(&batch_hex)?;
    let cmd_tx = h.cmd_tx.clone();
    let eth = h.eth;
    let secret = h.signing_secret;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        let chain = ant_chain::ChainClient::new(rpc);
        let meta =
            ant_chain::fetch_postage_batch_meta(&chain, ant_chain::GNOSIS_POSTAGE_STAMP, &batch_id)
                .await
                .map_err(|e| DriveError::Op(format!("read storage plan from chain: {e}")))?;
        if meta.batch_owner_eth != eth {
            return Err(DriveError::Op(format!(
                "this storage plan belongs to 0x{}, not your account (0x{})",
                hex::encode(meta.batch_owner_eth),
                hex::encode(eth),
            )));
        }
        register_batch(
            &cmd_tx,
            batch_id,
            meta.depth,
            meta.bucket_depth,
            meta.immutable,
        )
        .await?;
        // Connecting a plan must also turn on outbound settlement —
        // otherwise uploads stamp fine but never propagate (bee freezes
        // an unpaying node out past its payment threshold). Best-effort:
        // a thin wallet or flaky RPC just leaves settlement off, which
        // the Storage UI surfaces via `settlement_status`.
        ensure_settlement_best_effort(&cmd_tx, &chain, secret, &data_dir, eth).await;
        postage_status_json(&cmd_tx).await
    })
}

/// Auto-discover every funded postage batch this account owns on Gnosis
/// (a log scan from the xBZZ deploy block) and register each one.
/// Returns `{"registered":[...],"status":<plan>}`.
#[cfg(feature = "chain")]
pub(crate) fn storage_discover(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    let eth = h.eth;
    let secret = h.signing_secret;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        let chain = ant_chain::ChainClient::new(rpc);
        let found = ant_chain::discover::discover_owned_batches(
            &chain,
            ant_chain::GNOSIS_POSTAGE_STAMP,
            ant_chain::GNOSIS_BZZ_TOKEN,
            &eth,
            ant_chain::discover::GNOSIS_XBZZ_DEPLOY_BLOCK,
        )
        .await
        .map_err(|e| DriveError::Op(format!("search the chain for your storage: {e}")))?;
        let mut registered = Vec::new();
        for b in &found {
            register_batch(&cmd_tx, b.batch_id, b.depth, b.bucket_depth, b.immutable).await?;
            registered.push(format!("0x{}", hex::encode(b.batch_id)));
        }
        // If we connected at least one plan, make sure outbound
        // settlement is on so uploads against it actually reach the
        // network. Best-effort; never fails discovery.
        if !registered.is_empty() {
            ensure_settlement_best_effort(&cmd_tx, &chain, secret, &data_dir, eth).await;
        }
        let status = postage_status_json(&cmd_tx).await?;
        // `status` is already a JSON document; splice it in raw.
        Ok(format!(
            "{{\"registered\":{},\"status\":{}}}",
            serde_json::to_string(&registered)
                .map_err(|e| DriveError::Op(format!("encode: {e}")))?,
            status,
        ))
    })
}

#[cfg(feature = "chain")]
async fn register_batch(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    batch_id: [u8; 32],
    depth: u8,
    bucket_depth: u8,
    immutable: bool,
) -> Result<(), DriveError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    send(
        cmd_tx,
        ControlCommand::RegisterBatch {
            batch_id,
            depth,
            bucket_depth,
            immutable,
            ack: ack_tx,
        },
    )
    .await?;
    match recv_oneshot(ack_rx).await? {
        ControlAck::Ok { .. } => Ok(()),
        ControlAck::Error { message } => Err(DriveError::Op(message)),
        other => Err(unexpected(&other)),
    }
}

/// Gnosis block time in seconds — postage price is denominated
/// per-chunk *per block*, so a storage duration in days converts to a
/// per-chunk balance through the block count.
#[cfg(feature = "chain")]
const GNOSIS_BLOCK_SECS: u128 = 5;

/// Gnosis chain id, re-exported from the crate root so the on-chain
/// helpers here and the node bring-up in `lib.rs` share one constant.
#[cfg(feature = "chain")]
use crate::GNOSIS_CHAIN_ID;

/// Postage collision-bucket depth (bee's constant). Every `createBatch`
/// uses it and the registered issuer must match.
#[cfg(feature = "chain")]
const POSTAGE_BUCKET_DEPTH: u8 = 16;

/// xBZZ has 16 decimals; one whole xBZZ is `10^16` PLUR.
#[cfg(feature = "chain")]
const PLUR_PER_BZZ: u128 = 10_000_000_000_000_000;

/// Swarm chunk size in bytes (capacity math).
#[cfg(feature = "chain")]
const BYTES_PER_CHUNK: u64 = 4096;

/// xDAI (the Gnosis native gas token) has 18 decimals.
#[cfg(feature = "chain")]
const WEI_PER_XDAI: u128 = 1_000_000_000_000_000_000;

/// Gas reserve (wei) we keep aside / ask the user to fund on top of the
/// swap input, covering the up-to-four txs a first xDAI buy submits
/// (helper deploy + swap + approve + createBatch) at Gnosis gas prices.
/// 0.01 xDAI is several times the real cost.
#[cfg(feature = "chain")]
const GAS_RESERVE_WEI: u128 = 10_000_000_000_000_000;

/// Slippage + fee buffer applied to the fair-value swap input: we send
/// `fair × 105 / 100` xDAI so the 0.3% pool fee and a few percent of
/// price impact still clear the `amountOutMin` floor. Any excess simply
/// becomes a little extra xBZZ in the account.
#[cfg(feature = "chain")]
const SWAP_BUFFER_NUM: u128 = 105;
#[cfg(feature = "chain")]
const SWAP_BUFFER_DEN: u128 = 100;

/// Price a storage plan: read the current postage price + the account's
/// xBZZ / xDAI balances from Gnosis and compute what a `depth`-sized
/// plan lasting `days` costs. Returns everything the "payment
/// information" screen needs to show cost vs. the account's funds. No
/// transaction is sent.
#[cfg(feature = "chain")]
pub(crate) fn storage_quote(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    days: u64,
) -> Result<String, DriveError> {
    let eth = h.eth;
    h.runtime.block_on(async move {
        let client = ant_chain::ChainClient::new(rpc);
        let price = client
            .postage_last_price(ant_chain::GNOSIS_POSTAGE_STAMP)
            .await
            .map_err(|e| DriveError::Op(format!("read storage price: {e}")))?;
        // Balances are best-effort: a flaky RPC shouldn't block showing a
        // quote, so default to 0 ("you need to add funds").
        let bzz = client
            .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, &eth)
            .await
            .unwrap_or(0);
        let xdai = client.eth_get_balance_lower128(&eth).await.unwrap_or(0);

        let blocks = (u128::from(days) * 86_400 / GNOSIS_BLOCK_SECS).max(1);
        // A batch needs a non-zero per-chunk balance; if the RPC reports a
        // zero price, fall back to 1 PLUR/chunk/block so the plan is valid.
        let amount_per_chunk = price.max(1).saturating_mul(blocks);
        let total_plur = amount_per_chunk.saturating_mul(1u128 << depth);
        let capacity_bytes = (1u64 << depth).saturating_mul(BYTES_PER_CHUNK);

        // xDAI-only flow: the user funds plain xDAI, the node swaps the
        // shortfall into xBZZ. Total to hold = swap input + gas reserve.
        let needed_bzz = total_plur.saturating_sub(bzz);
        let swap_input = if needed_bzz > 0 {
            buffered_swap_input(&client, needed_bzz).await?
        } else {
            0
        };
        let xdai_required = swap_input.saturating_add(GAS_RESERVE_WEI);
        let xdai_to_send = xdai_required.saturating_sub(xdai);

        to_json(&Quote {
            depth,
            days,
            amount_per_chunk: amount_per_chunk.to_string(),
            total_cost_plur: total_plur.to_string(),
            total_cost_bzz: format_bzz(total_plur),
            capacity_bytes,
            account_bzz: bzz.to_string(),
            account_bzz_display: format_bzz(bzz),
            account_xdai: xdai.to_string(),
            account_xdai_display: format_native(xdai),
            needed_bzz: needed_bzz.to_string(),
            needed_bzz_display: format_bzz(needed_bzz),
            xdai_required: xdai_required.to_string(),
            xdai_required_display: format_native(xdai_required),
            xdai_to_send: xdai_to_send.to_string(),
            xdai_to_send_display: format_native(xdai_to_send),
            sufficient_funds: xdai >= xdai_required,
        })
    })
}

/// Fair-value xDAI (wei) to swap for `needed_bzz` PLUR of xBZZ, plus a
/// fee/slippage buffer. Reads the pool's live `sqrtPriceX96`: the raw
/// price `(sqrtPriceX96/2^96)^2` is WXDAI-wei per BZZ-plur (token1 has
/// 18 decimals, token0 16), so `plur × price` is the WXDAI to send.
#[cfg(feature = "chain")]
async fn buffered_swap_input(
    client: &ant_chain::ChainClient,
    needed_bzz: u128,
) -> Result<u128, DriveError> {
    use primitive_types::U512;
    let word = client
        .pool_sqrt_price_x96(ant_chain::GNOSIS_BZZ_WXDAI_POOL)
        .await
        .map_err(|e| DriveError::Op(format!("read swap price: {e}")))?;
    let sp = U512::from_big_endian(&word);
    let fair = sp
        .checked_mul(sp)
        .and_then(|v| v.checked_mul(U512::from(needed_bzz)))
        .map(|v| v >> 192)
        .ok_or_else(|| DriveError::Op("swap price math overflowed".into()))?;
    let fair = u512_to_u128(fair)?;
    Ok(fair.saturating_mul(SWAP_BUFFER_NUM) / SWAP_BUFFER_DEN)
}

/// Narrow a `U512` into a `u128`, erroring if it doesn't fit (a swap
/// input above 2^128 wei would be a nonsensical multi-billion-xDAI buy).
#[cfg(feature = "chain")]
fn u512_to_u128(v: primitive_types::U512) -> Result<u128, DriveError> {
    if v > primitive_types::U512::from(u128::MAX) {
        return Err(DriveError::Op("swap amount too large".into()));
    }
    Ok(v.low_u128())
}

/// Buy and activate a storage plan: `approve` the postage contract for
/// `amount_per_chunk × 2^depth` xBZZ, submit `createBatch`, pull the new
/// batch id from the receipt, and register it with the running node so
/// uploads can stamp against it immediately. Returns the refreshed
/// [`storage_status`] JSON.
///
/// This submits two real Gnosis transactions and spends real funds, so
/// the app gates it behind an explicit confirmation.
#[cfg(feature = "chain")]
pub(crate) fn storage_buy(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    amount_per_chunk: String,
    immutable: bool,
) -> Result<String, DriveError> {
    let amount: u128 = amount_per_chunk
        .trim()
        .parse()
        .map_err(|_| DriveError::Op("invalid plan price".into()))?;
    if amount == 0 {
        return Err(DriveError::Op(
            "plan price must be greater than zero".into(),
        ));
    }
    let cmd_tx = h.cmd_tx.clone();
    let secret = h.signing_secret;
    let owner = h.eth;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        use primitive_types::U256;

        let client = ant_chain::ChainClient::new(rpc);
        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("wallet: {e}")))?;
        let postage = parse_addr(ant_chain::GNOSIS_POSTAGE_STAMP)?;
        let bzz = parse_addr(ant_chain::GNOSIS_BZZ_TOKEN)?;

        let amount_u256 = U256::from(amount);
        let total = amount_u256
            .checked_mul(U256::one() << u32::from(depth))
            .ok_or_else(|| DriveError::Op("plan cost overflows".into()))?;

        wallet
            .approve_bzz(&client, &bzz, &postage, total)
            .await
            .map_err(|e| DriveError::Op(format!("authorise payment: {e}")))?;
        let nonce = ant_crypto::random_overlay_nonce();
        let receipt = wallet
            .create_batch(
                &client,
                &postage,
                &owner,
                amount_u256,
                depth,
                POSTAGE_BUCKET_DEPTH,
                &nonce,
                immutable,
            )
            .await
            .map_err(|e| DriveError::Op(format!("buy storage: {e}")))?;
        let batch_id = ant_chain::tx::extract_created_batch_id(&receipt)
            .ok_or_else(|| DriveError::Op("storage purchase receipt had no batch".into()))?;
        register_batch(&cmd_tx, batch_id, depth, POSTAGE_BUCKET_DEPTH, immutable).await?;
        // Now that the wallet is funded and a batch exists, make sure
        // outbound settlement is on so the upload actually reaches the
        // network (best-effort; never fails the purchase).
        ensure_settlement(&cmd_tx, &client, &wallet, &data_dir, secret, owner).await;
        postage_status_json(&cmd_tx).await
    })
}

/// Buy and activate a storage plan funding **only with xDAI**: the node
/// swaps the xBZZ shortfall through the on-chain helper, then runs the
/// same `approve` + `createBatch` flow as [`storage_buy`].
///
/// Submits up to four real Gnosis transactions (one-time helper deploy,
/// swap, approve, createBatch) and spends real funds, so the app gates
/// it behind explicit confirmation.
#[cfg(feature = "chain")]
pub(crate) fn storage_buy_xdai(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    amount_per_chunk: String,
    immutable: bool,
) -> Result<String, DriveError> {
    let amount: u128 = amount_per_chunk
        .trim()
        .parse()
        .map_err(|_| DriveError::Op("invalid plan price".into()))?;
    if amount == 0 {
        return Err(DriveError::Op(
            "plan price must be greater than zero".into(),
        ));
    }
    let cmd_tx = h.cmd_tx.clone();
    let secret = h.signing_secret;
    let owner = h.eth;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        use primitive_types::U256;

        let client = ant_chain::ChainClient::new(rpc);
        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("wallet: {e}")))?;
        let postage = parse_addr(ant_chain::GNOSIS_POSTAGE_STAMP)?;
        let bzz = parse_addr(ant_chain::GNOSIS_BZZ_TOKEN)?;

        let total_plur = amount
            .checked_mul(1u128 << depth)
            .ok_or_else(|| DriveError::Op("plan cost overflows".into()))?;

        // 1) Top up xBZZ by swapping xDAI for the shortfall, if any.
        let have_bzz = client
            .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, &owner)
            .await
            .unwrap_or(0);
        let needed_bzz = total_plur.saturating_sub(have_bzz);
        if needed_bzz > 0 {
            let xdai = client
                .eth_get_balance_lower128(&owner)
                .await
                .map_err(|e| DriveError::Op(format!("read xDAI balance: {e}")))?;
            let swap_input = buffered_swap_input(&client, needed_bzz).await?;
            let required = swap_input.saturating_add(GAS_RESERVE_WEI);
            if xdai < required {
                return Err(DriveError::Op(format!(
                    "not enough xDAI: send {} more xDAI to your account, then try again",
                    format_native(required - xdai)
                )));
            }
            let helper = wallet
                .ensure_swap_helper(&client)
                .await
                .map_err(|e| DriveError::Op(format!("prepare swap: {e}")))?;
            wallet
                .swap_xdai_for_bzz(
                    &client,
                    &helper,
                    &owner,
                    U256::from(swap_input),
                    U256::from(needed_bzz),
                )
                .await
                .map_err(|e| DriveError::Op(format!("swap xDAI for xBZZ: {e}")))?;
        }

        // 2) Authorise + create the batch, identical to the funded path.
        let amount_u256 = U256::from(amount);
        let total = amount_u256
            .checked_mul(U256::one() << u32::from(depth))
            .ok_or_else(|| DriveError::Op("plan cost overflows".into()))?;
        wallet
            .approve_bzz(&client, &bzz, &postage, total)
            .await
            .map_err(|e| DriveError::Op(format!("authorise payment: {e}")))?;
        let nonce = ant_crypto::random_overlay_nonce();
        let receipt = wallet
            .create_batch(
                &client,
                &postage,
                &owner,
                amount_u256,
                depth,
                POSTAGE_BUCKET_DEPTH,
                &nonce,
                immutable,
            )
            .await
            .map_err(|e| DriveError::Op(format!("buy storage: {e}")))?;
        let batch_id = ant_chain::tx::extract_created_batch_id(&receipt)
            .ok_or_else(|| DriveError::Op("storage purchase receipt had no batch".into()))?;
        register_batch(&cmd_tx, batch_id, depth, POSTAGE_BUCKET_DEPTH, immutable).await?;
        // Now that the wallet is funded and a batch exists, make sure
        // outbound settlement is on so the upload actually reaches the
        // network (best-effort; never fails the purchase).
        ensure_settlement(&cmd_tx, &client, &wallet, &data_dir, secret, owner).await;
        postage_status_json(&cmd_tx).await
    })
}

/// Ensure this node has a factory-registered chequebook and switch on
/// outbound SWAP settlement for the *running* node, so the batch the
/// user just bought can actually be pushed to the network. Without a
/// chequebook, bee's accounting locks us out after ~20 K chunks across
/// the peer set and the upload stalls — the user sees "uploaded" but
/// the content never propagates.
///
/// Resolution order mirrors `antd`:
///   1. **Persisted / startup** — a chequebook we deployed on an
///      earlier run (already enabled at `ant_init`); re-send the enable
///      command (idempotent) so a chequebook deployed *this* session is
///      also covered.
///   2. **Rediscover** — a chequebook this node EOA already owns
///      on-chain (e.g. a reinstalled app with the same backed-up key);
///      adopt + persist it rather than stranding its balance.
///   3. **Auto-deploy** — first run with a funded wallet: deploy a
///      fresh factory-registered chequebook (issuer = node EOA),
///      persist it, leave it unfunded (bee still accepts the cheques;
///      it's cashed once the user deposits BZZ).
///
/// Best-effort: never fails the surrounding storage purchase. A thin
/// wallet (no spare xDAI for the one-time deploy) or flaky RPC just
/// logs a warning; settlement enables on the next buy or app launch.
/// The node wallet both pays gas and is the issuer, so no external key
/// is ever introduced.
#[cfg(feature = "chain")]
pub(crate) async fn ensure_settlement(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    swap_secret: [u8; 32],
    node_eth: [u8; 20],
) {
    let chequebook = match resolve_or_deploy_chequebook(client, wallet, data_dir, node_eth).await {
        Ok(Some(cb)) => cb,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                "could not enable network settlement (uploads still work for a while, \
                 then stall until a chequebook exists): {e}",
            );
            return;
        }
    };

    let (ack_tx, ack_rx) = oneshot::channel();
    if send(
        cmd_tx,
        ControlCommand::EnablePushsyncSwap {
            chequebook,
            swap_secret,
            chain_id: GNOSIS_CHAIN_ID,
            outbound_ledger_path: data_dir
                .join("pushsync_outbound.json")
                .to_string_lossy()
                .into_owned(),
            ack: ack_tx,
        },
    )
    .await
    .is_err()
    {
        return;
    }
    match recv_oneshot(ack_rx).await {
        Ok(ControlAck::Ok { message }) => {
            tracing::info!(target: "ant-ffi", "{message}");
        }
        Ok(ControlAck::Error { message }) => {
            tracing::warn!(target: "ant-ffi", "enable settlement: {message}");
        }
        _ => {}
    }
}

/// Build the node wallet and run [`ensure_settlement`] — the wrapper the
/// discover / connect flows use so they share the buy flow's settlement
/// bootstrap. Best-effort: a wallet-init failure (bad key) just logs and
/// leaves settlement off; the Storage UI then shows the "settlement not
/// set up" state via [`settlement_status`].
#[cfg(feature = "chain")]
async fn ensure_settlement_best_effort(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    secret: [u8; 32],
    data_dir: &std::path::Path,
    node_eth: [u8; 20],
) {
    match ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID) {
        Ok(wallet) => ensure_settlement(cmd_tx, client, &wallet, data_dir, secret, node_eth).await,
        Err(e) => tracing::warn!(target: "ant-ffi", "settlement skipped (wallet init): {e}"),
    }
}

/// Whether outbound SWAP settlement is active for this device, as a JSON
/// object `{"enabled":bool,"chequebook":"0x…"|null}`. Settlement is the
/// thing that lets uploads actually propagate (bee charges the uploader
/// for every pushed chunk and freezes out a node that can't pay), so the
/// Storage tab reads this to warn the user when a connected plan still
/// won't upload reliably. Local + cheap: it just reads the persisted
/// `chequebook.json` association (written when a chequebook was
/// deployed), no chain round-trip.
#[cfg(feature = "chain")]
pub(crate) fn settlement_status(h: &AntHandle) -> Result<String, DriveError> {
    let path = h.data_dir.join("chequebook.json");
    let (enabled, chequebook) = match ant_chain::chequebook_store::load_persisted_chequebook(&path)
    {
        Ok(Some(cb)) => (true, Some(format!("0x{}", hex::encode(cb)))),
        _ => (false, None),
    };
    to_json(&SettlementStatus {
        enabled,
        chequebook,
    })
}

/// Reuse / rediscover / deploy a chequebook for `node_eth`, persisting
/// the association so future launches reload it directly. Returns the
/// 20-byte chequebook address, or `None` when the wallet can't afford
/// the one-time deploy (a soft skip, not an error). The persist /
/// rediscover / deploy mechanics are shared with `antd` via
/// [`ant_chain::chequebook_store`]; only the resolution *order* (no
/// operator-flag branches) and the unfunded deploy policy live here.
#[cfg(feature = "chain")]
async fn resolve_or_deploy_chequebook(
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    node_eth: [u8; 20],
) -> Result<Option<[u8; 20]>, DriveError> {
    use ant_chain::chequebook_store::{self, ChequebookError, ChequebookFile};

    let persist_path = data_dir.join("chequebook.json");

    // 1. Already known (persisted from a prior run / this session).
    if let Some(cb) =
        chequebook_store::load_persisted_chequebook(&persist_path).map_err(map_cb_err)?
    {
        return Ok(Some(cb));
    }

    // 2. Rediscover a chequebook this node EOA already owns on-chain
    //    (reinstall with a restored key). Adopt + persist it.
    match ant_chain::discover::discover_owned_chequebook(
        client,
        &ant_chain::chequebook::GNOSIS_CHEQUEBOOK_FACTORY,
        ant_chain::GNOSIS_POSTAGE_STAMP,
        ant_chain::GNOSIS_BZZ_TOKEN,
        &node_eth,
        ant_chain::discover::GNOSIS_XBZZ_DEPLOY_BLOCK,
    )
    .await
    {
        Ok(Some(cb)) => {
            if let Err(e) = chequebook_store::persist_chequebook(
                &persist_path,
                &ChequebookFile::rediscovered(&cb, &node_eth),
            ) {
                tracing::warn!(target: "ant-ffi", "persist rediscovered chequebook: {e}");
            }
            tracing::info!(
                target: "ant-ffi",
                chequebook = %format!("0x{}", hex::encode(cb)),
                "rediscovered node-owned chequebook on-chain; adopting it",
            );
            return Ok(Some(cb));
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(target: "ant-ffi", "chequebook rediscovery scan failed: {e}"),
    }

    // 3. Auto-deploy, unfunded (deposit 0): bee still accepts the
    //    cheques, and not draining the user's xBZZ keeps their whole
    //    balance available for the postage batch they came to buy.
    //    Insufficient gas is a soft skip — settlement turns on once the
    //    wallet has a little more xDAI.
    match chequebook_store::auto_deploy_chequebook(client, wallet, &node_eth, 0, &persist_path)
        .await
    {
        Ok(cb) => Ok(Some(cb)),
        Err(ChequebookError::InsufficientGas { need, .. }) => {
            tracing::warn!(
                target: "ant-ffi",
                "not enough spare xDAI to deploy a chequebook (need ~{need} wei); \
                 network settlement will turn on after you add a little more xDAI and buy again",
            );
            Ok(None)
        }
        Err(e) => Err(map_cb_err(e)),
    }
}

/// Map a shared-chequebook-store error into the drive op error.
#[cfg(feature = "chain")]
fn map_cb_err(e: ant_chain::chequebook_store::ChequebookError) -> DriveError {
    DriveError::Op(e.to_string())
}

/// Render a PLUR amount as a short xBZZ decimal string (4 dp).
#[cfg(feature = "chain")]
fn format_bzz(plur: u128) -> String {
    let whole = plur / PLUR_PER_BZZ;
    let frac = (plur % PLUR_PER_BZZ) / (PLUR_PER_BZZ / 10_000); // 4 decimals
    format!("{whole}.{frac:04}")
}

/// Render a wei amount as a short xDAI decimal string (4 dp).
#[cfg(feature = "chain")]
fn format_native(wei: u128) -> String {
    let whole = wei / WEI_PER_XDAI;
    let frac = (wei % WEI_PER_XDAI) / (WEI_PER_XDAI / 10_000); // 4 decimals
    format!("{whole}.{frac:04}")
}

#[cfg(feature = "chain")]
fn parse_addr(s: &str) -> Result<[u8; 20], DriveError> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out)
        .map_err(|e| DriveError::Op(format!("invalid contract address: {e}")))?;
    Ok(out)
}

#[cfg(feature = "chain")]
fn parse_batch_id(s: &str) -> Result<[u8; 32], DriveError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 64 {
        return Err(DriveError::Op(format!(
            "storage id must be 32 bytes (64 hex chars), got {}",
            s.len()
        )));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out)
        .map_err(|e| DriveError::Op(format!("invalid storage id: {e}")))?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Wire shapes + helpers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Jobs {
    jobs: Vec<ant_control::UploadJobView>,
}

#[derive(Serialize)]
struct AccountInfo {
    eth_address: String,
    overlay: String,
    peer_id: String,
    agent: String,
}

/// Wire shape for [`settlement_status`].
#[cfg(feature = "chain")]
#[derive(Serialize)]
struct SettlementStatus {
    /// `true` once a chequebook is deployed + persisted, i.e. outbound
    /// pushsync cheques can be issued and uploads will propagate.
    enabled: bool,
    /// `0x`-prefixed chequebook contract address when enabled.
    chequebook: Option<String>,
}

#[cfg(feature = "chain")]
#[derive(Serialize)]
struct Quote {
    depth: u8,
    days: u64,
    amount_per_chunk: String,
    total_cost_plur: String,
    total_cost_bzz: String,
    capacity_bytes: u64,
    account_bzz: String,
    account_bzz_display: String,
    account_xdai: String,
    account_xdai_display: String,
    /// xBZZ still needed for the plan (PLUR), i.e. cost minus balance.
    needed_bzz: String,
    needed_bzz_display: String,
    /// Total xDAI the account must hold to activate via auto-swap: the
    /// (buffered) swap input plus a gas reserve.
    xdai_required: String,
    xdai_required_display: String,
    /// Additional xDAI the user still needs to send (`required − balance`).
    xdai_to_send: String,
    xdai_to_send_display: String,
    sufficient_funds: bool,
}

async fn send(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    cmd: ControlCommand,
) -> Result<(), DriveError> {
    cmd_tx
        .send(cmd)
        .await
        .map_err(|_| DriveError::Op("node loop is not accepting commands".into()))
}

async fn recv_oneshot(rx: oneshot::Receiver<ControlAck>) -> Result<ControlAck, DriveError> {
    recv_oneshot_within(rx, OP_TIMEOUT).await
}

async fn recv_oneshot_within(
    rx: oneshot::Receiver<ControlAck>,
    timeout: Duration,
) -> Result<ControlAck, DriveError> {
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(ack)) => Ok(ack),
        Ok(Err(_)) => Err(DriveError::Op("node dropped the ack channel".into())),
        Err(_) => Err(DriveError::Op("operation timed out".into())),
    }
}

fn to_json<T: Serialize>(v: &T) -> Result<String, DriveError> {
    serde_json::to_string(v).map_err(|e| DriveError::Op(format!("encode response: {e}")))
}

fn unexpected(ack: &ControlAck) -> DriveError {
    DriveError::Op(format!("unexpected node response: {ack:?}"))
}
