//! `antctl` — command-line client for the `antd` daemon.
//!
//! Speaks to the daemon over its Unix-domain control socket (NDJSON,
//! `ant-control::Request` / `Response`).

use ant_control::{
    request_streaming, request_sync, request_upload_follow, GatewayRequestInfo, GetProgress,
    PeerConnectionInfo, PeerConnectionState, Request, Response, RetrievalInfo, StatusSnapshot,
    UploadJobView,
};
use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use crossterm::{
    cursor::{Hide, Show},
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    style::ResetColor,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect as TuiRect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Row, Table, Tabs},
    Frame, Terminal,
};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(
    name = "antctl",
    version,
    about = "Control and inspect a running antd daemon"
)]
struct Opt {
    /// Path to the daemon's control socket. Defaults to `<data-dir>/antd.sock`.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,

    /// Data directory, used to locate the default control socket.
    #[arg(long, global = true, default_value = "~/.antd")]
    data_dir: PathBuf,

    /// Emit JSON instead of a human-readable report.
    #[arg(long, global = true, default_value_t = false)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Show live daemon status: identity, peers, listeners, uptime.
    Status,
    /// Auto-refreshing daemon status board.
    Top {
        /// Refresh interval in milliseconds.
        #[arg(long = "interval-ms", default_value_t = 200)]
        interval_ms: u64,
    },
    /// Show the daemon agent string and control-protocol version.
    Version,
    /// Inspect or manage the daemon's peer set.
    Peers {
        #[command(subcommand)]
        command: PeersCommand,
    },
    /// Retrieve content from Swarm.
    #[command(long_about = "\
Retrieve content from Swarm.

The reference may be one of:

  <hex>                  Single 32-byte content-addressed chunk
                         (bee's /bytes/<ref> for short files <= 4 KiB).
  bytes://<hex>          Walk a multi-chunk byte tree rooted at <hex>
                         (bee's /bytes/<ref> for longer files). Same
                         shape as --bytes.
  bzz://<hex>[/path]     Walk the mantaray manifest at <hex>, resolve
                         <path> (or the website-index-document), then
                         join the resulting chunk tree. Same shape as
                         --bzz [--bzz-path]. Path may be multi-segment.

Examples:

  antctl get <chunk-hex>                      # raw 4 KiB chunk
  antctl get bytes://<file-hex> -o blob.bin   # multi-chunk file
  antctl get bzz://<root>/index.html          # website index
  antctl get bzz://<root>/images/logo.png     # nested file
  antctl get bzz://<root>/13/4358/2645.png    # tile pyramid leaf")]
    /// Postage batch lifecycle on Gnosis: createBatch, topUp,
    /// increaseDepth, list (read-only). All variants talk directly to
    /// the chain — `antd` does not need to be running.
    Postage {
        #[command(subcommand)]
        command: PostageCommand,
    },
    /// Chequebook contract diagnostics on Gnosis: read state,
    /// dry-run / submit `cashChequeBeneficiary`. Talks directly to
    /// the chain — `antd` does not need to be running.
    Chequebook {
        #[command(subcommand)]
        command: ChequebookCommand,
    },
    /// Upload a file to Swarm via the daemon's pushsync path.
    /// Default invocation (`antctl upload <path>`) starts a new
    /// job and follows it until the manifest reference is known —
    /// Ctrl+C detaches (the upload keeps running on `antd`); a
    /// second Ctrl+C cancels.
    ///
    /// Subcommands `list / status / pause / resume / cancel /
    /// follow` operate on existing jobs. Job ids are 16 hex chars;
    /// any unique 8-hex prefix also resolves.
    Upload {
        #[command(subcommand)]
        command: UploadCommand,
    },
    Get {
        /// Reference. See above for accepted forms.
        reference: String,
        /// Path to write the bytes to. Defaults to stdout.
        #[arg(long, short = 'o')]
        out: Option<PathBuf>,
        /// Force `bytes` (multi-chunk join) mode regardless of the
        /// reference's URL scheme.
        #[arg(long, conflicts_with = "bzz")]
        bytes: bool,
        /// Force `bzz` (manifest walk + join) mode regardless of the
        /// reference's URL scheme. Pair with `--bzz-path` to pick a
        /// non-default file inside a directory manifest.
        #[arg(long, conflicts_with = "bytes")]
        bzz: bool,
        /// Path component of a bzz reference, e.g. `index.html` or
        /// `images/logo.png` or `13/4358/2645.png`. Empty triggers
        /// `website-index-document`.
        #[arg(long, requires = "bzz")]
        bzz_path: Option<String>,
        /// Decode bzz files whose root span carries a non-zero
        /// Reed-Solomon redundancy level. The daemon masks the level
        /// byte off and walks the chunk tree without RS recovery, so
        /// every data chunk must still be reachable. Default: off
        /// (the daemon errors with `redundancy / erasure coding not
        /// supported`).
        #[arg(long)]
        allow_degraded_redundancy: bool,
        /// Before issuing the request, poll `antctl status` until the
        /// daemon has a completed BZZ handshake AND at least N libp2p
        /// peers connected. Avoids the `no peers available; wait for
        /// handshakes to complete` error when the user fires `get`
        /// against a freshly-started daemon. `0` disables the wait
        /// (request goes out immediately).
        #[arg(long, default_value_t = 4)]
        wait_peers: u32,
        /// Maximum time to spend waiting for `--wait-peers` before
        /// giving up and issuing the request anyway. The daemon will
        /// then return its own error if peers really are zero. In
        /// seconds.
        #[arg(long, default_value_t = 30)]
        wait_timeout: u64,
        /// Skip the daemon's shared in-memory chunk cache for this
        /// request only. The daemon mints a fresh per-request cache,
        /// so intra-request retries still benefit from caching but no
        /// chunk hits or writes touch the long-lived cache. Useful
        /// for timing the cold path or reproducing a transient
        /// retrieval issue.
        #[arg(long)]
        bypass_cache: bool,
        /// Suppress the in-place progress line on stderr. By default
        /// `antctl get` renders a one-line status (chunks, bytes,
        /// rate, peer count) that updates every ~150 ms while the
        /// daemon walks the chunk tree. The line is also auto-hidden
        /// for non-TTY stderr, when `--json` is set, and for
        /// single-chunk fetches where there's nothing to track.
        #[arg(long)]
        no_progress: bool,
        /// Progress renderer to use when stderr is a TTY. `line` is the
        /// compact default; `visual` draws a retro block map from the
        /// aggregate chunk counters the daemon streams today.
        #[arg(long, value_enum, default_value = "line")]
        progress_style: ProgressStyle,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProgressStyle {
    Line,
    Visual,
}

#[derive(Subcommand, Debug)]
enum PeersCommand {
    /// Drop the on-disk peerstore (`<data-dir>/peers.json`) and clear the
    /// in-memory dedup set. Existing connections stay up; the next restart
    /// will bootstrap fresh through the bootnodes.
    Reset,
}

#[derive(Subcommand, Debug)]
enum UploadCommand {
    /// Start a new upload job. Without `--detach`, the command also
    /// follows the job and prints a live progress line until the
    /// upload completes; press Ctrl+C to detach (the job keeps
    /// running in the daemon — use `antctl upload follow <id>` to
    /// reattach later, or `cancel <id>` to abort).
    Start {
        /// Source file. Must be a regular file the daemon can read.
        path: PathBuf,
        /// Optional postage batch override (`0x` + 64 hex). Defaults
        /// to the daemon's `--postage-batch` if absent.
        #[arg(long)]
        batch: Option<String>,
        /// Filename to embed in the single-file mantaray manifest.
        /// Defaults to the source file's basename. Ignored when
        /// `--raw` is set.
        #[arg(long)]
        name: Option<String>,
        /// MIME type for the manifest entry. Defaults to
        /// `application/octet-stream`. Ignored when `--raw` is
        /// set.
        #[arg(long)]
        content_type: Option<String>,
        /// Skip the trailing single-file mantaray manifest. The
        /// returned reference is the raw data root chunk address
        /// — fetch it with `/bytes/<ref>` (not `/bzz/<ref>`).
        /// Saves 1-2 chunks of postage and a final round-trip;
        /// loses the embedded filename + content-type, so the
        /// downloader has to know the file type out-of-band. Use
        /// for content-addressed blob storage where the consumer
        /// is your own code; leave off when humans / browsers
        /// will fetch the result.
        #[arg(long)]
        raw: bool,
        /// Skip the postage pre-flight check that compares the
        /// file's estimated chunk count against the daemon's
        /// worst-case remaining-chunk budget (`antctl postage
        /// status`). Useful for scripts that have already done
        /// their own accounting, or when the operator
        /// intentionally wants to top up / dilute mid-upload. The
        /// pre-flight is a stderr warning, not a hard block, so
        /// most callers can leave this off.
        #[arg(long)]
        no_preflight: bool,
        /// Return immediately after the daemon registers the job.
        /// Use `antctl upload follow <id>` to attach to its
        /// progress later. Without this flag the command blocks
        /// and prints live progress.
        #[arg(long)]
        detach: bool,
    },
    /// List every known upload job (running, paused, completed,
    /// cancelled, failed). Output is sorted by creation time with
    /// the oldest first.
    List,
    /// Snapshot one job by id (or unique 8-hex prefix).
    Status {
        /// Job id, or any unique prefix.
        job_id: String,
    },
    /// Stream live progress for a job until it reaches a terminal
    /// status (or the user disconnects with Ctrl+C).
    Follow {
        /// Job id, or any unique prefix.
        job_id: String,
    },
    /// Soft-stop a running job. The driver drains in-flight pushes,
    /// checkpoints state, and parks. Use `resume` to bring it back.
    Pause {
        /// Job id, or any unique prefix.
        job_id: String,
    },
    /// Bring a paused or failed job back to running. Spawns a
    /// fresh driver task; any error stored in `last_error` is
    /// cleared.
    Resume {
        /// Job id, or any unique prefix.
        job_id: String,
    },
    /// Hard-stop a job. Already-pushed chunks stay in the network
    /// (Swarm GCs them at postage TTL expiry — there's no unpush);
    /// the daemon updates the on-disk manifest to `cancelled`.
    Cancel {
        /// Job id, or any unique prefix.
        job_id: String,
    },
}

/// Common chain-write flags shared by every `postage` write subcommand.
/// All can be supplied via env so the operator doesn't have to repeat
/// secrets on every invocation: `GNOSIS_RPC_URL`, `POSTAGE_OWNER_KEY`
/// (or the bee-shaped alias `STORAGE_STAMP_PRIVATE_KEY`),
/// `POSTAGE_CONTRACT`. Any flag passed on the command line wins over
/// the env var.
#[derive(clap::Args, Debug)]
struct PostageChainArgs {
    /// Gnosis RPC URL. Reads `GNOSIS_RPC_URL` if absent.
    #[arg(long, env = "GNOSIS_RPC_URL")]
    gnosis_rpc_url: String,
    /// Owner private key (32-byte hex, optional `0x` prefix). Reads
    /// `POSTAGE_OWNER_KEY` or `STORAGE_STAMP_PRIVATE_KEY` if absent.
    /// Used to sign the on-chain transaction.
    #[arg(long, env = "POSTAGE_OWNER_KEY")]
    owner_key: String,
    /// Address of the deployed `PostageStamp` contract on Gnosis. Reads
    /// `POSTAGE_CONTRACT` if absent. Default is the Swarm mainnet
    /// stamp contract.
    #[arg(
        long,
        env = "POSTAGE_CONTRACT",
        default_value = "0x45a1502382541Cd610CC9068e88727426b696293"
    )]
    postage_contract: String,
    /// Address of the BZZ ERC-20 token on Gnosis. Reads
    /// `BZZ_TOKEN_CONTRACT` if absent. Default is mainnet xBZZ.
    #[arg(
        long,
        env = "BZZ_TOKEN_CONTRACT",
        default_value = "0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da"
    )]
    bzz_token: String,
    /// Per-tx confirmation timeout in seconds. Gnosis blocks land
    /// every ~5 s; 90 s comfortably covers a busy-mempool wait.
    #[arg(long, default_value_t = 90)]
    wait_secs: u64,
    /// Gas price override in gwei. Defaults to 2 gwei (above the
    /// current Gnosis floor) — bump for congestion.
    #[arg(long)]
    gas_price_gwei: Option<u64>,
}

#[derive(Subcommand, Debug)]
enum PostageCommand {
    /// Create a new postage batch. Approves BZZ for the `PostageStamp`
    /// contract first, then issues `createBatch`. Prints the new
    /// batch id (extracted from the `BatchCreated` event) on success.
    Create {
        #[command(flatten)]
        chain: PostageChainArgs,
        /// Batch depth. The batch can stamp 2^depth chunks total
        /// before the buckets fill (or wrap around, if mutable).
        /// Bee's minimum is 17; 22 is a sensible default for a
        /// small site (~16 M chunks ≈ 64 GiB).
        #[arg(long)]
        depth: u8,
        /// Bucket depth. Bee fixes this at 16 in practice; leave at
        /// the default unless you know why you're changing it.
        #[arg(long, default_value_t = 16)]
        bucket_depth: u8,
        /// Initial balance per chunk, in PLUR (BZZ wei, 1 BZZ = 1e16
        /// PLUR). Bee uses cumulative-paid-out semantics — this is
        /// what each of the 2^depth buckets is funded with.
        #[arg(long)]
        amount_per_chunk: u128,
        /// Mark the batch immutable. Immutable batches refuse new
        /// stamps once a bucket fills (instead of wrapping around
        /// and overwriting old indices); pick this for archival
        /// uploads, not for ephemeral content.
        #[arg(long)]
        immutable: bool,
        /// Random 32-byte salt used to derive the batch id. Defaults
        /// to a fresh random value; pass an explicit hex string for
        /// reproducible test deployments.
        #[arg(long)]
        salt: Option<String>,
    },
    /// Top up an existing batch by adding `amount_per_chunk` more
    /// PLUR per bucket — extends the batch's lifetime without
    /// changing its capacity.
    TopUp {
        #[command(flatten)]
        chain: PostageChainArgs,
        /// Batch id (32-byte hex, optional `0x` prefix).
        #[arg(long)]
        batch_id: String,
        /// Per-chunk top-up amount in PLUR. Total cost =
        /// `amount_per_chunk × 2^depth`.
        #[arg(long)]
        amount_per_chunk: u128,
    },
    /// Dilute an existing batch by raising its depth. Doubles
    /// capacity per +1 of `new_depth` and halves the per-chunk
    /// balance accordingly. The batch id stays the same — every
    /// stamp issued so far remains valid.
    Dilute {
        #[command(flatten)]
        chain: PostageChainArgs,
        /// Batch id (32-byte hex, optional `0x` prefix).
        #[arg(long)]
        batch_id: String,
        /// New batch depth. Must be > current depth.
        #[arg(long)]
        new_depth: u8,
    },
    /// Read-only: show on-chain metadata for a batch (depth, bucket
    /// depth, immutable flag, owner address). Doesn't require a key.
    Show {
        /// Gnosis RPC URL.
        #[arg(long, env = "GNOSIS_RPC_URL")]
        gnosis_rpc_url: String,
        /// `PostageStamp` contract address.
        #[arg(
            long,
            env = "POSTAGE_CONTRACT",
            default_value = "0x45a1502382541Cd610CC9068e88727426b696293"
        )]
        postage_contract: String,
        /// Batch id (32-byte hex, optional `0x` prefix).
        #[arg(long)]
        batch_id: String,
    },
    /// Read-only: print the wallet's xDAI and BZZ balances. Useful as
    /// a pre-flight sanity check before `create` / `topup`.
    Balance {
        /// Gnosis RPC URL.
        #[arg(long, env = "GNOSIS_RPC_URL")]
        gnosis_rpc_url: String,
        /// Wallet to check. If omitted, derives from
        /// `POSTAGE_OWNER_KEY`. At least one of `--address` or
        /// `POSTAGE_OWNER_KEY` env must be set.
        #[arg(long)]
        address: Option<String>,
        /// BZZ token contract.
        #[arg(
            long,
            env = "BZZ_TOKEN_CONTRACT",
            default_value = "0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da"
        )]
        bzz_token: String,
    },
    /// Live snapshot of the daemon's local postage stamp issuer:
    /// theoretical capacity, indices issued so far, per-bucket fill
    /// extremes, and the conservative "worst-case remaining chunks"
    /// budget that bounds the size of the next upload. Pure local
    /// read against `antd` — no on-chain RPC, so the chain flags
    /// (`--gnosis-rpc-url`, `--postage-contract`, key envs) are not
    /// required. Honours the top-level `--socket` / `--data-dir`
    /// flags like every other socket-talking subcommand.
    Status,
}

#[derive(Subcommand, Debug)]
enum ChequebookCommand {
    /// Read-only: print the chequebook's `issuer()`, `balance()`,
    /// `totalPaidOut()`, `liquidBalance()`, and `paidOut(<beneficiary>)`
    /// snapshots — useful as a pre-flight before `cash-self`.
    Show {
        /// Gnosis RPC URL.
        #[arg(long, env = "GNOSIS_RPC_URL")]
        gnosis_rpc_url: String,
        /// Chequebook contract address.
        #[arg(long, env = "CHEQUEBOOK_ADDRESS")]
        chequebook: String,
        /// Beneficiary address to read `paidOut(...)` for. Optional;
        /// defaults to `WALLET_ADDRESS` env if set.
        #[arg(long, env = "WALLET_ADDRESS")]
        beneficiary: Option<String>,
    },
    /// Deploy a brand-new chequebook through the official Swarm
    /// chequebook factory (`SimpleSwapFactory` at
    /// `0xC2d5A532cf69AA9A1378737D8ccDEF884B6E7420` on Gnosis
    /// mainnet), so bee peers will accept cheques drawn on it.
    ///
    /// **Why this exists:** bee's `chequeStore.ReceiveCheque`
    /// silently rejects cheques whose chequebook isn't registered
    /// with the factory (it calls `factory.deployedContracts(addr)`
    /// before recovering the signature). A `SimpleSwap` contract
    /// deployed independently of the factory will have all its
    /// cheques dropped by mainnet bee, and pushsync will stall.
    ///
    /// The wallet (`--wallet-key`) pays gas; the *issuer* baked
    /// into the chequebook is `--issuer-address`. They can be the
    /// same EOA (simple) or different (cold-storage issuer key,
    /// hot wallet pays gas). After deploy, optionally fund the
    /// chequebook with `--initial-deposit-plur` (a plain
    /// ERC-20 transfer of BZZ).
    Deploy {
        /// Gnosis RPC URL.
        #[arg(long, env = "GNOSIS_RPC_URL")]
        gnosis_rpc_url: String,
        /// Wallet private key — pays gas + (optionally) the initial
        /// BZZ deposit. Defaults to `WALLET_PRIVATE_KEY`.
        #[arg(long, env = "WALLET_PRIVATE_KEY")]
        wallet_key: String,
        /// Issuer EOA address to bake into the chequebook. Bee
        /// recovers cheque signatures and compares to this — only
        /// the holder of the matching private key can sign valid
        /// cheques. Defaults to `STORAGE_STAMP_OWNER_ADDRESS` so
        /// the chequebook lines up with the existing postage
        /// owner without operator gymnastics.
        #[arg(long, env = "STORAGE_STAMP_OWNER_ADDRESS")]
        issuer_address: String,
        /// Hard-deposit timeout, in seconds. Bee uses 86 400 (24 h)
        /// as its default in `pkg/node/devnode.go`. Increase if
        /// you plan to use hard deposits and want the lock-up
        /// window longer; leave at default for normal SWAP usage.
        #[arg(long, default_value_t = ant_chain::chequebook::DEFAULT_HARD_DEPOSIT_TIMEOUT_SECS)]
        hard_deposit_timeout_secs: u64,
        /// Optional initial BZZ deposit, in PLUR (1 BZZ = 1e16
        /// PLUR). If set, after a successful deploy we transfer
        /// this amount from the wallet to the new chequebook.
        #[arg(long)]
        initial_deposit_plur: Option<u128>,
        /// Optional CREATE2 salt (32-byte hex). If unset, generated
        /// from `OsRng` so a fresh deploy never collides with an
        /// existing one. Specify explicitly to make the deploy
        /// deterministic across operator runs.
        #[arg(long)]
        salt: Option<String>,
        /// Override the factory address (only useful on devnets;
        /// mainnet uses the pinned address). Defaults to the
        /// Gnosis mainnet factory.
        #[arg(long)]
        factory: Option<String>,
        /// Per-tx confirmation timeout (seconds).
        #[arg(long, default_value_t = 120)]
        wait_secs: u64,
        /// Gas price override (gwei). Defaults to 2 gwei.
        #[arg(long)]
        gas_price_gwei: Option<u64>,
        /// If set, append/replace `CHEQUEBOOK_ADDRESS=0x…` in this
        /// `.env` file so the next `antd` restart picks up the
        /// new chequebook. Other lines are left untouched.
        #[arg(long)]
        write_env_file: Option<PathBuf>,
    },
    /// Verify a chequebook is registered with the Swarm chequebook
    /// factory. Cheap (one `eth_call`) and offline-safe — useful
    /// for diagnosing a pushsync stall without involving
    /// gnosisscan.
    Verify {
        /// Gnosis RPC URL.
        #[arg(long, env = "GNOSIS_RPC_URL")]
        gnosis_rpc_url: String,
        /// Chequebook contract address.
        #[arg(long, env = "CHEQUEBOOK_ADDRESS")]
        chequebook: String,
        /// Override the factory address (mainnet uses the pinned
        /// `SimpleSwapFactory`).
        #[arg(long)]
        factory: Option<String>,
    },
    /// Tier-1 SWAP self-test: sign a tiny EIP-712 cheque with the
    /// chequebook's issuer key, beneficiary = our own
    /// `WALLET_PRIVATE_KEY` address, then either `eth_call` (default)
    /// or `eth_sendRawTransaction` (`--submit`) `cashChequeBeneficiary`
    /// against the chequebook contract.
    ///
    /// Proves end-to-end that our EIP-712 digest, ECDSA recid, and
    /// signature shape are bit-compatible with bee's chequebook
    /// contract: a passing call → recover yields exactly `issuer()`.
    /// Costs ~$0.0001 in xDAI gas at `--submit`; the dry-run is free.
    CashSelf {
        /// Gnosis RPC URL.
        #[arg(long, env = "GNOSIS_RPC_URL")]
        gnosis_rpc_url: String,
        /// Chequebook contract address.
        #[arg(long, env = "CHEQUEBOOK_ADDRESS")]
        chequebook: String,
        /// Issuer private key (32-byte hex). Must derive to the
        /// address `chequebook.issuer()` returns. Reads
        /// `STORAGE_STAMP_PRIVATE_KEY` if absent.
        #[arg(long, env = "STORAGE_STAMP_PRIVATE_KEY")]
        issuer_key: String,
        /// Beneficiary private key (32-byte hex). Must derive to an
        /// EOA we control, because the chequebook contract bakes
        /// `msg.sender` (the submitter) into the EIP-712 digest as
        /// the beneficiary. Reads `WALLET_PRIVATE_KEY` if absent.
        #[arg(long, env = "WALLET_PRIVATE_KEY")]
        beneficiary_key: String,
        /// Optional recipient address (where the tiny BZZ payout
        /// would land). Defaults to the beneficiary itself.
        #[arg(long)]
        recipient: Option<String>,
        /// Cumulative payout to claim, in PLUR (1 BZZ = 1e16 PLUR).
        /// Default `1` is the smallest possible cheque. `cash-self`
        /// auto-bumps this to `paidOut[beneficiary] + 1` so the
        /// chequebook never rejects the cheque as already-cashed.
        #[arg(long, default_value_t = 1u128)]
        payout_plur: u128,
        /// Submit the signed tx instead of just `eth_call`-ing.
        /// `eth_call` is enough to prove signature correctness; only
        /// pass `--submit` to also exercise on-chain state changes.
        #[arg(long)]
        submit: bool,
        /// Per-tx confirmation timeout (seconds), only meaningful
        /// with `--submit`.
        #[arg(long, default_value_t = 90)]
        wait_secs: u64,
        /// Gas price override (gwei). Defaults to 2 gwei.
        #[arg(long)]
        gas_price_gwei: Option<u64>,
    },
}

fn main() -> Result<()> {
    let opt = Opt::parse();
    let socket = resolve_socket(&opt);

    match opt.command {
        Command::Status => {
            let snap = fetch_status(&socket)?;
            if opt.json {
                println!("{}", serde_json::to_string_pretty(&snap)?);
            } else {
                print_status(&snap);
            }
        }
        Command::Top { interval_ms } => {
            if opt.json {
                bail!("--json is not supported for `top`; use `antctl status --json` for machine-readable output");
            }
            run_top(&socket, interval_ms)?;
        }
        Command::Version => {
            let resp = request_sync(&socket, &Request::Version)
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            match resp {
                Response::Version(v) => {
                    if opt.json {
                        println!("{}", serde_json::to_string_pretty(&v)?);
                    } else {
                        println!(
                            "client: antctl/{}\nserver: {} (control protocol v{})",
                            env!("CARGO_PKG_VERSION"),
                            v.agent,
                            v.protocol_version,
                        );
                    }
                }
                Response::Error { message } => bail!("antd: {message}"),
                other => bail!("unexpected response: {other:?}"),
            }
        }
        Command::Peers {
            command: PeersCommand::Reset,
        } => {
            let resp = request_sync(&socket, &Request::PeersReset)
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            match resp {
                Response::Ok { message } => {
                    if opt.json {
                        println!("{}", serde_json::json!({ "ok": true, "message": message }));
                    } else {
                        println!("{message}");
                    }
                }
                Response::Error { message } => bail!("antd: {message}"),
                other => bail!("unexpected response: {other:?}"),
            }
        }
        Command::Postage { command } => match command {
            // `Status` doesn't need a tokio runtime or chain RPC —
            // it talks to the local daemon socket synchronously,
            // exactly like `antctl status`. The global `--socket`
            // / `--data-dir` flags resolved into `socket` above
            // are reused here.
            PostageCommand::Status => {
                run_postage_status(&socket, opt.json)?;
            }
            other => {
                run_postage(other, opt.json)?;
            }
        },
        Command::Chequebook { command } => {
            run_chequebook(command, opt.json)?;
        }
        Command::Upload { command } => {
            run_upload(&socket, command, opt.json)?;
        }
        Command::Get {
            reference,
            out,
            bytes,
            bzz,
            bzz_path,
            allow_degraded_redundancy,
            wait_peers,
            wait_timeout,
            bypass_cache,
            no_progress,
            progress_style,
        } => {
            if wait_peers > 0 {
                wait_for_peers(&socket, wait_peers, Duration::from_secs(wait_timeout));
            }
            let mode = FetchMode::from_flags(bytes, bzz, bzz_path.as_deref());
            // Single-chunk fetches return a single line; there's
            // nothing to stream, so don't bother asking the daemon.
            // For multi-chunk fetches, render a status line on stderr
            // unless the user opted out, the user wants JSON, or
            // stderr isn't a TTY (we'd otherwise mangle log capture).
            // URL schemes on the reference (`bzz://`, `bytes://`)
            // override the explicit flags, so peek at the reference
            // too — otherwise `antctl get bzz://...` would silently
            // skip progress because `mode` is `Chunk`.
            let multi_chunk = !matches!(mode, FetchMode::Chunk)
                || reference.starts_with("bzz://")
                || reference.starts_with("bytes://");
            let show_progress = !no_progress
                && !opt.json
                && multi_chunk
                && std::io::IsTerminal::is_terminal(&std::io::stderr());
            run_get(
                &socket,
                &reference,
                out.as_deref(),
                opt.json,
                mode,
                allow_degraded_redundancy,
                bypass_cache,
                show_progress,
                progress_style,
            )?;
        }
    }
    Ok(())
}

/// One of the three retrieval shapes `antctl get` knows how to dispatch.
/// Computed once from the command-line flags / URL scheme so the
/// downstream logic can stay flat.
#[derive(Debug, Clone)]
enum FetchMode {
    /// Single content-addressed chunk; daemon answers `Response::Bytes`.
    Chunk,
    /// Multi-chunk join; daemon answers `Response::Bytes` with the
    /// joined file body.
    Bytes,
    /// Manifest walk + join; daemon answers `Response::BzzBytes` with
    /// the joined body plus optional content-type / filename.
    Bzz { path: String },
}

impl FetchMode {
    /// Resolve a fetch mode from CLI flags. The reference itself is
    /// inspected separately by [`parse_reference_with_mode`] so a URL
    /// scheme on the reference can override these defaults.
    fn from_flags(bytes: bool, bzz: bool, bzz_path: Option<&str>) -> Self {
        if bzz {
            Self::Bzz {
                path: bzz_path.unwrap_or("").to_string(),
            }
        } else if bytes {
            Self::Bytes
        } else {
            Self::Chunk
        }
    }
}

/// Final destination for the bytes pulled back from the daemon. Stdout
/// for raw bytes, optionally `--out` for a fixed path, or — if the
/// daemon hands us a `Filename` from manifest metadata and the user
/// passed `--out <dir>` rather than a file — `<dir>/<Filename>`.
fn write_output(
    out: Option<&Path>,
    filename_hint: Option<&str>,
    data: &[u8],
) -> Result<Option<PathBuf>> {
    use std::io::Write;
    if let Some(p) = out {
        // If `out` is an existing directory, append the manifest filename
        // (or fall back to "data.bin" if there isn't one). This lets
        // callers do `antctl get bzz://<ref>/ -o ./` and end up with
        // `./index.html` automatically.
        let target = if p.is_dir() {
            let name = filename_hint.unwrap_or("data.bin");
            p.join(name)
        } else {
            p.to_path_buf()
        };
        std::fs::write(&target, data).with_context(|| format!("write to {}", target.display()))?;
        return Ok(Some(target));
    }
    io::stdout().lock().write_all(data)?;
    Ok(None)
}

/// Run a `postage` subcommand. We construct a fresh tokio runtime here
/// rather than threading async through `main` because every other
/// antctl command is synchronous; postage is the lone async path
/// today.
fn run_postage(cmd: PostageCommand, json: bool) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build postage runtime")?;
    rt.block_on(async move { run_postage_async(cmd, json).await })
}

/// Sibling of [`run_postage`] for the `chequebook` subcommand. Same
/// rationale: a fresh single-thread runtime is cheaper than threading
/// async through every antctl call site.
fn run_chequebook(cmd: ChequebookCommand, json: bool) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build chequebook runtime")?;
    rt.block_on(async move { run_chequebook_async(cmd, json).await })
}

async fn run_chequebook_async(cmd: ChequebookCommand, json: bool) -> Result<()> {
    use ant_chain::chequebook::{
        cash_cheque_beneficiary_calldata, cheque_digest, chequebook_balance_selector,
        chequebook_issuer_selector, chequebook_liquid_balance_selector,
        chequebook_paid_out_calldata, chequebook_total_paid_out_selector,
        extract_deployed_chequebook, factory_deployed_contracts_calldata,
        factory_erc20_address_calldata, random_chequebook_salt, sign_cheque, Cheque,
        GNOSIS_BZZ_TOKEN_BYTES, GNOSIS_CHEQUEBOOK_FACTORY,
    };
    use ant_chain::tx::{Wallet, GNOSIS_CHAIN_ID};
    use ant_chain::ChainClient;
    use primitive_types::U256;
    use std::time::Duration;

    match cmd {
        ChequebookCommand::Show {
            gnosis_rpc_url,
            chequebook,
            beneficiary,
        } => {
            let cb = parse_addr(&chequebook)?;
            let client = ChainClient::new(gnosis_rpc_url);
            let issuer = read_address(&client, &cb, &chequebook_issuer_selector()).await?;
            let balance = read_u256(&client, &cb, &chequebook_balance_selector()).await?;
            let total_paid_out =
                read_u256(&client, &cb, &chequebook_total_paid_out_selector()).await?;
            let liquid = read_u256(&client, &cb, &chequebook_liquid_balance_selector()).await?;
            let (paid_out, beneficiary_addr) = match beneficiary {
                Some(b) => {
                    let addr = parse_addr(&b)?;
                    let calldata = chequebook_paid_out_calldata(&addr);
                    let v = client
                        .eth_call(
                            &format!("0x{}", hex::encode(cb)),
                            &format!("0x{}", hex::encode(&calldata)),
                        )
                        .await
                        .context("paidOut")?;
                    let w = ant_chain_padded_last_word(&v)?;
                    (Some(U256::from_big_endian(&w)), Some(addr))
                }
                None => (None, None),
            };

            if json {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "chequebook".into(),
                    serde_json::Value::String(hex_addr(&cb)),
                );
                obj.insert(
                    "issuer".into(),
                    serde_json::Value::String(hex_addr(&issuer)),
                );
                obj.insert(
                    "balance_plur".into(),
                    serde_json::Value::String(balance.to_string()),
                );
                obj.insert(
                    "total_paid_out_plur".into(),
                    serde_json::Value::String(total_paid_out.to_string()),
                );
                obj.insert(
                    "liquid_balance_plur".into(),
                    serde_json::Value::String(liquid.to_string()),
                );
                if let Some(addr) = beneficiary_addr {
                    obj.insert(
                        "beneficiary".into(),
                        serde_json::Value::String(hex_addr(&addr)),
                    );
                    obj.insert(
                        "paid_out_plur".into(),
                        serde_json::Value::String(paid_out.unwrap().to_string()),
                    );
                }
                println!("{}", serde_json::Value::Object(obj));
            } else {
                println!("chequebook         {}", hex_addr(&cb));
                println!("  issuer           {}", hex_addr(&issuer));
                println!("  balance          {balance} PLUR");
                println!("  total_paid_out   {total_paid_out} PLUR");
                println!("  liquid_balance   {liquid} PLUR");
                if let Some(addr) = beneficiary_addr {
                    println!("  beneficiary      {}", hex_addr(&addr));
                    println!("  paid_out         {} PLUR", paid_out.unwrap());
                }
            }
        }
        ChequebookCommand::CashSelf {
            gnosis_rpc_url,
            chequebook,
            issuer_key,
            beneficiary_key,
            recipient,
            payout_plur,
            submit,
            wait_secs,
            gas_price_gwei,
        } => {
            let cb = parse_addr(&chequebook)?;
            let issuer_secret = parse_secret(&issuer_key)?;
            let beneficiary_secret = parse_secret(&beneficiary_key)?;

            let client = ChainClient::new(gnosis_rpc_url);

            let issuer_wallet =
                Wallet::new(issuer_secret, GNOSIS_CHAIN_ID).context("derive issuer wallet")?;
            let issuer_addr = *issuer_wallet.address();

            let mut beneficiary_wallet = Wallet::new(beneficiary_secret, GNOSIS_CHAIN_ID)
                .context("derive beneficiary wallet")?
                .wait_for(Duration::from_secs(wait_secs));
            if let Some(g) = gas_price_gwei {
                beneficiary_wallet.default_gas_price_wei = g.saturating_mul(1_000_000_000);
            }
            let beneficiary_addr = *beneficiary_wallet.address();
            let recipient_addr = match recipient {
                Some(s) => parse_addr(&s)?,
                None => beneficiary_addr,
            };

            // Verify on-chain state matches what we expect: issuer
            // key derives to chequebook.issuer(), and we know the
            // current paidOut[beneficiary] so we can build a cheque
            // the contract won't reject as already-cashed.
            let onchain_issuer = read_address(&client, &cb, &chequebook_issuer_selector()).await?;
            if onchain_issuer != issuer_addr {
                bail!(
                    "issuer mismatch: chequebook.issuer() = {}, but --issuer-key derives to {}",
                    hex_addr(&onchain_issuer),
                    hex_addr(&issuer_addr),
                );
            }
            let liquid = read_u256(&client, &cb, &chequebook_liquid_balance_selector()).await?;
            let paid_calldata = chequebook_paid_out_calldata(&beneficiary_addr);
            let v = client
                .eth_call(
                    &format!("0x{}", hex::encode(cb)),
                    &format!("0x{}", hex::encode(&paid_calldata)),
                )
                .await
                .context("paidOut")?;
            let already_paid = U256::from_big_endian(&ant_chain_padded_last_word(&v)?);

            // Make the cumulative_payout strictly greater than what
            // the contract has already paid this beneficiary, so
            // `payout = cumulative - paidOut > 0`. User picks the
            // step size; default `1 PLUR` for the cheapest possible
            // cheque.
            let cumulative = already_paid + U256::from(payout_plur);

            let cheque = Cheque {
                chequebook: cb,
                beneficiary: beneficiary_addr,
                cumulative_payout: cumulative,
            };
            let signed =
                sign_cheque(&issuer_secret, &cheque, GNOSIS_CHAIN_ID).context("sign cheque")?;
            let signature = signed.signature;

            let local_digest = cheque_digest(&cheque, GNOSIS_CHAIN_ID);
            eprintln!(
                "chequebook        {}\n\
                 issuer            {} (matches on-chain)\n\
                 beneficiary       {}\n\
                 recipient         {}\n\
                 paid_out (now)    {} PLUR\n\
                 cumulative_payout {} PLUR  (cheque diff = {} PLUR)\n\
                 liquid_balance    {} PLUR\n\
                 local digest      0x{}",
                hex_addr(&cb),
                hex_addr(&issuer_addr),
                hex_addr(&beneficiary_addr),
                hex_addr(&recipient_addr),
                already_paid,
                cumulative,
                payout_plur,
                liquid,
                hex::encode(local_digest),
            );

            // Dry-run via eth_call. Distinguishes "invalid signature"
            // (our wire format is wrong) from "liquid balance not
            // sufficient" (signature OK, the chequebook just hasn't
            // got the BZZ on-hand to actually pay out).
            let calldata =
                cash_cheque_beneficiary_calldata(&recipient_addr, cumulative, &signature);
            let dry_run = client
                .eth_call_from(&beneficiary_addr, &cb, &calldata)
                .await;

            let dry_status = match dry_run {
                Ok(_) => "ok".to_string(),
                Err(e) => format!("revert: {e}"),
            };

            let signature_ok = !dry_status.contains("invalid issuer signature")
                && !dry_status.contains("invalid signature");
            eprintln!(
                "dry-run           {}\n\
                 signature         {}",
                if dry_status == "ok" {
                    "ok (eth_call returned without revert)".to_string()
                } else {
                    dry_status.clone()
                },
                if signature_ok {
                    "✓ accepted by chequebook contract"
                } else {
                    "✗ contract rejected with `invalid issuer signature`"
                },
            );

            let mut tx_hash: Option<[u8; 32]> = None;
            let mut block_number: Option<u64> = None;
            if submit {
                if !signature_ok {
                    bail!("refusing to --submit a cheque the dry-run already rejected");
                }
                eprintln!("submitting cashChequeBeneficiary…");
                let rcpt = beneficiary_wallet
                    .cash_cheque_beneficiary(&client, &cb, &recipient_addr, cumulative, &signature)
                    .await
                    .context("cashChequeBeneficiary")?;
                eprintln!(
                    "tx confirmed in block {} (hash 0x{})",
                    rcpt.block_number,
                    hex::encode(rcpt.tx_hash),
                );
                tx_hash = Some(rcpt.tx_hash);
                block_number = Some(rcpt.block_number);
            }

            if json {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "chequebook".into(),
                    serde_json::Value::String(hex_addr(&cb)),
                );
                obj.insert(
                    "issuer".into(),
                    serde_json::Value::String(hex_addr(&issuer_addr)),
                );
                obj.insert(
                    "beneficiary".into(),
                    serde_json::Value::String(hex_addr(&beneficiary_addr)),
                );
                obj.insert(
                    "recipient".into(),
                    serde_json::Value::String(hex_addr(&recipient_addr)),
                );
                obj.insert(
                    "cumulative_payout_plur".into(),
                    serde_json::Value::String(cumulative.to_string()),
                );
                obj.insert(
                    "paid_out_before_plur".into(),
                    serde_json::Value::String(already_paid.to_string()),
                );
                obj.insert("dry_run".into(), serde_json::Value::String(dry_status));
                obj.insert(
                    "signature_accepted".into(),
                    serde_json::Value::Bool(signature_ok),
                );
                if let Some(h) = tx_hash {
                    obj.insert(
                        "tx".into(),
                        serde_json::Value::String(format!("0x{}", hex::encode(h))),
                    );
                }
                if let Some(b) = block_number {
                    obj.insert("block".into(), serde_json::Value::Number(b.into()));
                }
                println!("{}", serde_json::Value::Object(obj));
            } else {
                println!(
                    "result: {}",
                    if signature_ok {
                        "PASS — chequebook contract recovered the issuer from our EIP-712 cheque"
                    } else {
                        "FAIL — see `dry-run` line above"
                    },
                );
                if let Some(h) = tx_hash {
                    println!("on-chain tx: 0x{}", hex::encode(h));
                }
            }
        }
        ChequebookCommand::Verify {
            gnosis_rpc_url,
            chequebook,
            factory,
        } => {
            let cb = parse_addr(&chequebook)?;
            let factory_addr = match factory {
                Some(s) => parse_addr(&s)?,
                None => GNOSIS_CHEQUEBOOK_FACTORY,
            };
            let client = ChainClient::new(gnosis_rpc_url);
            let calldata = factory_deployed_contracts_calldata(&cb);
            let v = client
                .eth_call(
                    &format!("0x{}", hex::encode(factory_addr)),
                    &format!("0x{}", hex::encode(&calldata)),
                )
                .await
                .context("factory.deployedContracts")?;
            let word = ant_chain_padded_last_word(&v)?;
            let registered = word.iter().any(|&b| b != 0);

            if json {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "chequebook".into(),
                    serde_json::Value::String(hex_addr(&cb)),
                );
                obj.insert(
                    "factory".into(),
                    serde_json::Value::String(hex_addr(&factory_addr)),
                );
                obj.insert("registered".into(), serde_json::Value::Bool(registered));
                println!("{}", serde_json::Value::Object(obj));
            } else {
                println!("chequebook  {}", hex_addr(&cb));
                println!("factory     {}", hex_addr(&factory_addr));
                println!(
                    "registered  {}",
                    if registered {
                        "yes — bee will accept cheques drawn on this chequebook"
                    } else {
                        "NO — bee silently rejects cheques from this chequebook; deploy a new one with `antctl chequebook deploy`"
                    }
                );
            }
        }
        ChequebookCommand::Deploy {
            gnosis_rpc_url,
            wallet_key,
            issuer_address,
            hard_deposit_timeout_secs,
            initial_deposit_plur,
            salt,
            factory,
            wait_secs,
            gas_price_gwei,
            write_env_file,
        } => {
            let wallet_secret = parse_secret(&wallet_key)?;
            let issuer_addr = parse_addr(&issuer_address)?;
            let factory_addr = match factory {
                Some(s) => parse_addr(&s)?,
                None => GNOSIS_CHEQUEBOOK_FACTORY,
            };
            let salt_bytes: [u8; 32] = match salt {
                Some(s) => {
                    let raw = hex::decode(s.trim_start_matches("0x"))
                        .context("--salt must be 32-byte hex")?;
                    if raw.len() != 32 {
                        bail!("--salt must be exactly 32 bytes (64 hex chars)");
                    }
                    let mut out = [0u8; 32];
                    out.copy_from_slice(&raw);
                    out
                }
                None => random_chequebook_salt(),
            };

            let client = ChainClient::new(gnosis_rpc_url);
            let mut wallet = Wallet::new(wallet_secret, GNOSIS_CHAIN_ID)
                .context("derive wallet from --wallet-key")?
                .wait_for(Duration::from_secs(wait_secs));
            if let Some(g) = gas_price_gwei {
                wallet.default_gas_price_wei = g.saturating_mul(1_000_000_000);
            }
            let wallet_addr = *wallet.address();

            // Sanity: factory's pinned BZZ token must match what we
            // expect. Catches `--factory` typos before we sign anything.
            let bzz_ret = client
                .eth_call(
                    &format!("0x{}", hex::encode(factory_addr)),
                    &format!("0x{}", hex::encode(factory_erc20_address_calldata())),
                )
                .await
                .context("factory.ERC20Address()")?;
            let bzz_word = ant_chain_padded_last_word(&bzz_ret)?;
            let mut bzz_addr = [0u8; 20];
            bzz_addr.copy_from_slice(&bzz_word[12..32]);
            if bzz_addr != GNOSIS_BZZ_TOKEN_BYTES {
                bail!(
                    "factory at {} returns ERC20Address={}, expected mainnet BZZ {}; refusing to deploy against the wrong factory",
                    hex_addr(&factory_addr),
                    hex_addr(&bzz_addr),
                    hex_addr(&GNOSIS_BZZ_TOKEN_BYTES),
                );
            }

            eprintln!(
                "deploying chequebook…\n\
                 factory             {}\n\
                 wallet (gas payer)  {}\n\
                 issuer              {}\n\
                 hard_deposit_to     {}s\n\
                 salt                0x{}",
                hex_addr(&factory_addr),
                hex_addr(&wallet_addr),
                hex_addr(&issuer_addr),
                hard_deposit_timeout_secs,
                hex::encode(salt_bytes),
            );

            let receipt = wallet
                .deploy_chequebook(
                    &client,
                    &factory_addr,
                    &issuer_addr,
                    U256::from(hard_deposit_timeout_secs),
                    &salt_bytes,
                )
                .await
                .context("factory.deploySimpleSwap")?;

            let Some(cb) = extract_deployed_chequebook(&receipt) else {
                bail!(
                    "factory tx 0x{} confirmed in block {} but emitted no SimpleSwapDeployed log; logs were {:?}",
                    hex::encode(receipt.tx_hash),
                    receipt.block_number,
                    receipt.logs,
                );
            };

            // Verify factory.deployedContracts(new_addr) == true
            // before declaring success — paranoia, but cheap.
            let verify_calldata = factory_deployed_contracts_calldata(&cb);
            let v = client
                .eth_call(
                    &format!("0x{}", hex::encode(factory_addr)),
                    &format!("0x{}", hex::encode(&verify_calldata)),
                )
                .await
                .context("post-deploy factory.deployedContracts")?;
            let word = ant_chain_padded_last_word(&v)?;
            let registered = word.iter().any(|&b| b != 0);
            if !registered {
                bail!(
                    "deploy succeeded (chequebook = {}) but factory.deployedContracts returned false; this should be impossible — refuse to proceed",
                    hex_addr(&cb),
                );
            }

            // Optional initial deposit: plain ERC-20 transfer of
            // BZZ from wallet to chequebook. The chequebook's
            // `balance()` view is `BZZ.balanceOf(swap)`, so this
            // is functionally a chequebook deposit.
            let mut deposit_tx: Option<[u8; 32]> = None;
            let mut deposit_block: Option<u64> = None;
            if let Some(deposit) = initial_deposit_plur {
                if deposit > 0 {
                    eprintln!(
                        "depositing {} PLUR ({} BZZ) into the chequebook via ERC-20 transfer…",
                        deposit,
                        deposit / 10_000_000_000_000_000u128,
                    );
                    let rcpt = wallet
                        .erc20_transfer(&client, &GNOSIS_BZZ_TOKEN_BYTES, &cb, U256::from(deposit))
                        .await
                        .context("BZZ.transfer to new chequebook")?;
                    deposit_tx = Some(rcpt.tx_hash);
                    deposit_block = Some(rcpt.block_number);
                }
            }

            // If the operator asked us to update an .env file,
            // splice the new CHEQUEBOOK_ADDRESS in. Match the
            // `KEY=VALUE` shape that bash sources cleanly.
            if let Some(path) = write_env_file.as_ref() {
                update_env_file(path, "CHEQUEBOOK_ADDRESS", &hex_addr(&cb))
                    .with_context(|| format!("update {}", path.display()))?;
            }

            if json {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "chequebook".into(),
                    serde_json::Value::String(hex_addr(&cb)),
                );
                obj.insert(
                    "factory".into(),
                    serde_json::Value::String(hex_addr(&factory_addr)),
                );
                obj.insert(
                    "issuer".into(),
                    serde_json::Value::String(hex_addr(&issuer_addr)),
                );
                obj.insert(
                    "wallet".into(),
                    serde_json::Value::String(hex_addr(&wallet_addr)),
                );
                obj.insert(
                    "factory_verified".into(),
                    serde_json::Value::Bool(registered),
                );
                obj.insert(
                    "salt".into(),
                    serde_json::Value::String(format!("0x{}", hex::encode(salt_bytes))),
                );
                obj.insert(
                    "tx_hash".into(),
                    serde_json::Value::String(format!("0x{}", hex::encode(receipt.tx_hash))),
                );
                obj.insert(
                    "block".into(),
                    serde_json::Value::Number(receipt.block_number.into()),
                );
                if let Some(d) = initial_deposit_plur {
                    obj.insert(
                        "deposit_plur".into(),
                        serde_json::Value::String(d.to_string()),
                    );
                }
                if let Some(h) = deposit_tx {
                    obj.insert(
                        "deposit_tx".into(),
                        serde_json::Value::String(format!("0x{}", hex::encode(h))),
                    );
                }
                if let Some(b) = deposit_block {
                    obj.insert("deposit_block".into(), serde_json::Value::Number(b.into()));
                }
                if let Some(p) = write_env_file.as_ref() {
                    obj.insert(
                        "env_file".into(),
                        serde_json::Value::String(p.display().to_string()),
                    );
                }
                println!("{}", serde_json::Value::Object(obj));
            } else {
                println!("chequebook       {}", hex_addr(&cb));
                println!("factory          {}", hex_addr(&factory_addr));
                println!(
                    "issuer           {}  (factory-baked)",
                    hex_addr(&issuer_addr)
                );
                println!("wallet           {}  (paid gas)", hex_addr(&wallet_addr));
                println!("factory-verified {registered}");
                println!("salt             0x{}", hex::encode(salt_bytes));
                println!(
                    "deploy tx        0x{}  (block {})",
                    hex::encode(receipt.tx_hash),
                    receipt.block_number,
                );
                if let Some(d) = initial_deposit_plur {
                    println!("deposit          {d} PLUR");
                    if let (Some(h), Some(b)) = (deposit_tx, deposit_block) {
                        println!("deposit tx       0x{}  (block {})", hex::encode(h), b,);
                    }
                }
                if let Some(p) = write_env_file.as_ref() {
                    println!(
                        "wrote CHEQUEBOOK_ADDRESS=0x{} to {}",
                        hex::encode(cb),
                        p.display()
                    );
                }
                println!();
                println!("next step: restart antd so it picks up the new chequebook.");
            }
        }
    }
    Ok(())
}

/// Append-or-replace `key=value` in a `.env`-style file. Touches
/// only the line that starts with `key=`; preserves comments,
/// blank lines, and ordering of unrelated keys. The new value is
/// written un-quoted (callers are responsible for picking values
/// that don't need quoting — Ethereum addresses are pure hex, so
/// they always work).
fn update_env_file(path: &std::path::Path, key: &str, value: &str) -> Result<()> {
    let body = std::fs::read_to_string(path).unwrap_or_default();
    let mut lines: Vec<String> = body.lines().map(std::string::ToString::to_string).collect();
    let prefix = format!("{key}=");
    let new_line = format!("{key}={value}");
    let mut replaced = false;
    for line in &mut lines {
        let trimmed = line.trim_start();
        if trimmed.starts_with(&prefix) {
            line.clone_from(&new_line);
            replaced = true;
            break;
        }
    }
    if !replaced {
        lines.push(new_line);
    }
    let mut out = lines.join("\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }
    std::fs::write(path, out).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

async fn read_address(
    client: &ant_chain::ChainClient,
    contract: &[u8; 20],
    selector: &[u8; 4],
) -> Result<[u8; 20]> {
    let v = client
        .eth_call(
            &format!("0x{}", hex::encode(contract)),
            &format!("0x{}", hex::encode(selector)),
        )
        .await
        .with_context(|| format!("eth_call selector 0x{}", hex::encode(selector)))?;
    let word = ant_chain_padded_last_word(&v)?;
    let mut out = [0u8; 20];
    out.copy_from_slice(&word[12..32]);
    Ok(out)
}

async fn read_u256(
    client: &ant_chain::ChainClient,
    contract: &[u8; 20],
    selector: &[u8; 4],
) -> Result<primitive_types::U256> {
    let v = client
        .eth_call(
            &format!("0x{}", hex::encode(contract)),
            &format!("0x{}", hex::encode(selector)),
        )
        .await
        .with_context(|| format!("eth_call selector 0x{}", hex::encode(selector)))?;
    let word = ant_chain_padded_last_word(&v)?;
    Ok(primitive_types::U256::from_big_endian(&word))
}

/// Local copy of `padded_last_word` — `ant_chain` keeps its version
/// crate-private. Same shape: pad/extract the trailing 32-byte word
/// from an `eth_call` ABI return blob.
fn ant_chain_padded_last_word(data: &[u8]) -> Result<[u8; 32]> {
    if data.len() < 32 {
        bail!("ABI return shorter than 32 bytes");
    }
    let mut w = [0u8; 32];
    w.copy_from_slice(&data[data.len() - 32..]);
    Ok(w)
}

async fn run_postage_async(cmd: PostageCommand, json: bool) -> Result<()> {
    use ant_chain::tx::{extract_created_batch_id, Wallet, GNOSIS_CHAIN_ID};
    use ant_chain::{fetch_postage_batch_meta, ChainClient};
    use primitive_types::U256;
    use std::time::Duration;

    match cmd {
        PostageCommand::Create {
            chain,
            depth,
            bucket_depth,
            amount_per_chunk,
            immutable,
            salt,
        } => {
            if depth <= bucket_depth {
                bail!(
                    "depth ({depth}) must be > bucket_depth ({bucket_depth}); \
                     2^(depth-bucket_depth) is the per-bucket capacity",
                );
            }
            let secret = parse_secret(&chain.owner_key)?;
            let postage = parse_addr(&chain.postage_contract)?;
            let token = parse_addr(&chain.bzz_token)?;
            let salt_bytes = match salt {
                Some(h) => parse_word32(&h)?,
                None => random_word32(),
            };

            let client = ChainClient::new(chain.gnosis_rpc_url);
            let mut wallet = Wallet::new(secret, GNOSIS_CHAIN_ID)
                .context("derive wallet")?
                .wait_for(Duration::from_secs(chain.wait_secs));
            if let Some(g) = chain.gas_price_gwei {
                wallet.default_gas_price_wei = g.saturating_mul(1_000_000_000);
            }
            let owner = *wallet.address();

            // Total cost = amount_per_chunk * 2^depth.
            let total = U256::from(amount_per_chunk)
                .checked_mul(U256::from(1u128) << depth)
                .ok_or_else(|| anyhow::anyhow!("amount_per_chunk * 2^depth overflows U256"))?;

            eprintln!(
                "owner={} depth={} bucket_depth={} amount/chunk={} total={} immutable={}",
                hex_addr(&owner),
                depth,
                bucket_depth,
                amount_per_chunk,
                total,
                immutable,
            );

            eprintln!("approving BZZ for postage contract {}…", hex_addr(&postage));
            let approve_rcpt = wallet
                .approve_bzz(&client, &token, &postage, total)
                .await
                .context("approve BZZ")?;
            eprintln!(
                "approve confirmed in block {} (tx {})",
                approve_rcpt.block_number,
                hex::encode(approve_rcpt.tx_hash),
            );

            eprintln!("calling createBatch…");
            let create_rcpt = wallet
                .create_batch(
                    &client,
                    &postage,
                    &owner,
                    U256::from(amount_per_chunk),
                    depth,
                    bucket_depth,
                    &salt_bytes,
                    immutable,
                )
                .await
                .context("createBatch")?;
            let batch_id = extract_created_batch_id(&create_rcpt).ok_or_else(|| {
                anyhow::anyhow!(
                    "createBatch confirmed (tx {}) but no BatchCreated event found in receipt",
                    hex::encode(create_rcpt.tx_hash),
                )
            })?;

            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "batch_id": format!("0x{}", hex::encode(batch_id)),
                        "owner": hex_addr(&owner),
                        "depth": depth,
                        "bucket_depth": bucket_depth,
                        "amount_per_chunk": amount_per_chunk.to_string(),
                        "total": total.to_string(),
                        "immutable": immutable,
                        "approve_tx": format!("0x{}", hex::encode(approve_rcpt.tx_hash)),
                        "create_tx": format!("0x{}", hex::encode(create_rcpt.tx_hash)),
                        "block": create_rcpt.block_number,
                    }),
                );
            } else {
                println!("batch created: 0x{}", hex::encode(batch_id));
                println!("  approve tx: 0x{}", hex::encode(approve_rcpt.tx_hash));
                println!("  create  tx: 0x{}", hex::encode(create_rcpt.tx_hash));
                println!("  block:      {}", create_rcpt.block_number);
            }
        }
        PostageCommand::TopUp {
            chain,
            batch_id,
            amount_per_chunk,
        } => {
            let secret = parse_secret(&chain.owner_key)?;
            let postage = parse_addr(&chain.postage_contract)?;
            let token = parse_addr(&chain.bzz_token)?;
            let id = parse_word32(&batch_id)?;

            let client = ChainClient::new(chain.gnosis_rpc_url);
            let mut wallet = Wallet::new(secret, GNOSIS_CHAIN_ID)
                .context("derive wallet")?
                .wait_for(Duration::from_secs(chain.wait_secs));
            if let Some(g) = chain.gas_price_gwei {
                wallet.default_gas_price_wei = g.saturating_mul(1_000_000_000);
            }

            // Need to know the depth to compute `total` for the approve.
            let meta = fetch_postage_batch_meta(&client, &chain.postage_contract, &id)
                .await
                .context("fetch batch meta")?;
            let total = U256::from(amount_per_chunk)
                .checked_mul(U256::from(1u128) << meta.depth)
                .ok_or_else(|| anyhow::anyhow!("amount_per_chunk * 2^depth overflows U256"))?;

            eprintln!(
                "topup batch=0x{} depth={} amount/chunk={} total={}",
                hex::encode(id),
                meta.depth,
                amount_per_chunk,
                total,
            );

            eprintln!("approving BZZ…");
            let approve_rcpt = wallet
                .approve_bzz(&client, &token, &postage, total)
                .await
                .context("approve BZZ")?;
            eprintln!("calling topUp…");
            let rcpt = wallet
                .top_up(&client, &postage, &id, U256::from(amount_per_chunk))
                .await
                .context("topUp")?;

            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "batch_id": format!("0x{}", hex::encode(id)),
                        "amount_per_chunk": amount_per_chunk.to_string(),
                        "total": total.to_string(),
                        "approve_tx": format!("0x{}", hex::encode(approve_rcpt.tx_hash)),
                        "topup_tx": format!("0x{}", hex::encode(rcpt.tx_hash)),
                        "block": rcpt.block_number,
                    }),
                );
            } else {
                println!("topUp confirmed: 0x{}", hex::encode(rcpt.tx_hash));
                println!("  approve tx: 0x{}", hex::encode(approve_rcpt.tx_hash));
                println!("  block:      {}", rcpt.block_number);
            }
        }
        PostageCommand::Dilute {
            chain,
            batch_id,
            new_depth,
        } => {
            let secret = parse_secret(&chain.owner_key)?;
            let postage = parse_addr(&chain.postage_contract)?;
            let id = parse_word32(&batch_id)?;

            let client = ChainClient::new(chain.gnosis_rpc_url);
            let meta = fetch_postage_batch_meta(&client, &chain.postage_contract, &id)
                .await
                .context("fetch batch meta")?;
            if new_depth <= meta.depth {
                bail!(
                    "new_depth ({new_depth}) must be > current depth ({})",
                    meta.depth,
                );
            }

            let mut wallet = Wallet::new(secret, GNOSIS_CHAIN_ID)
                .context("derive wallet")?
                .wait_for(Duration::from_secs(chain.wait_secs));
            if let Some(g) = chain.gas_price_gwei {
                wallet.default_gas_price_wei = g.saturating_mul(1_000_000_000);
            }

            eprintln!(
                "dilute batch=0x{} {}→{}",
                hex::encode(id),
                meta.depth,
                new_depth,
            );

            let rcpt = wallet
                .increase_depth(&client, &postage, &id, new_depth)
                .await
                .context("increaseDepth")?;

            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "batch_id": format!("0x{}", hex::encode(id)),
                        "old_depth": meta.depth,
                        "new_depth": new_depth,
                        "tx": format!("0x{}", hex::encode(rcpt.tx_hash)),
                        "block": rcpt.block_number,
                    }),
                );
            } else {
                println!("increaseDepth confirmed: 0x{}", hex::encode(rcpt.tx_hash));
                println!("  block: {}", rcpt.block_number);
            }
        }
        PostageCommand::Show {
            gnosis_rpc_url,
            postage_contract,
            batch_id,
        } => {
            let id = parse_word32(&batch_id)?;
            let client = ChainClient::new(gnosis_rpc_url);
            let meta = fetch_postage_batch_meta(&client, &postage_contract, &id)
                .await
                .context("fetch batch meta")?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "batch_id": format!("0x{}", hex::encode(id)),
                        "depth": meta.depth,
                        "bucket_depth": meta.bucket_depth,
                        "immutable": meta.immutable,
                        "owner": hex_addr(&meta.batch_owner_eth),
                    }),
                );
            } else {
                println!("batch     0x{}", hex::encode(id));
                println!("  depth        {}", meta.depth);
                println!("  bucket_depth {}", meta.bucket_depth);
                println!("  immutable    {}", meta.immutable);
                println!("  owner        {}", hex_addr(&meta.batch_owner_eth));
            }
        }
        PostageCommand::Balance {
            gnosis_rpc_url,
            address,
            bzz_token,
        } => {
            let addr = if let Some(s) = address {
                parse_addr(&s)?
            } else {
                let key = std::env::var("POSTAGE_OWNER_KEY")
                    .or_else(|_| std::env::var("STORAGE_STAMP_PRIVATE_KEY"))
                    .context(
                        "no --address and POSTAGE_OWNER_KEY / STORAGE_STAMP_PRIVATE_KEY \
                         not set",
                    )?;
                let secret = parse_secret(&key)?;
                let wallet = Wallet::new(secret, GNOSIS_CHAIN_ID).context("derive wallet")?;
                *wallet.address()
            };
            let client = ChainClient::new(gnosis_rpc_url);
            let xdai = client
                .eth_get_balance_lower128(&addr)
                .await
                .context("eth_getBalance")?;
            let bzz = client
                .erc20_balance_of_lower128(&bzz_token, &addr)
                .await
                .context("balanceOf BZZ")?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "address": hex_addr(&addr),
                        "xdai_wei": xdai.to_string(),
                        "bzz_plur": bzz.to_string(),
                    }),
                );
            } else {
                println!("address  {}", hex_addr(&addr));
                println!("  xDAI   {} wei  ({})", xdai, format_eth_18(xdai));
                println!("  BZZ    {} PLUR ({})", bzz, format_eth_16(bzz));
            }
        }
        PostageCommand::Status => {
            // Routed via `main` -> `run_postage_status` so we don't
            // pay the cost of building a tokio runtime + chain
            // context for a pure local socket round-trip. This arm
            // keeps the match exhaustive but is unreachable in
            // practice.
            unreachable!("PostageCommand::Status is dispatched in main, not here");
        }
    }
    Ok(())
}

/// Local control-socket call: snapshot the daemon's stamp issuer
/// state and pretty-print it (or emit JSON). No chain RPC.
fn run_postage_status(socket: &Path, json: bool) -> Result<()> {
    let resp = request_sync(socket, &Request::PostageStatus)
        .with_context(|| format!("talk to antd at {}", socket.display()))?;
    let view = match resp {
        Response::PostageStatus(v) => v,
        Response::Error { message } => bail!("antd: {message}"),
        other => bail!("unexpected response: {other:?}"),
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&view).context("serialise postage status")?,
        );
        return Ok(());
    }
    print_postage_status(&view);
    Ok(())
}

/// Render a `PostageStatusView` as a human-readable block. Matches
/// the column-aligned style of the upload status block so the two
/// look like siblings in CLI sessions.
fn print_postage_status(v: &ant_control::PostageStatusView) {
    if !v.enabled {
        println!("uploads disabled");
        println!("  reason  no postage batch configured on antd");
        println!("          set --postage-batch / STORAGE_STAMP_BATCH_ID and restart");
        return;
    }

    let total_bytes = v.total_capacity_chunks.saturating_mul(CHUNK_SIZE_BYTES);
    let issued_bytes = v.issued_chunks.saturating_mul(CHUNK_SIZE_BYTES);
    let remaining_bytes = v.remaining_total_chunks.saturating_mul(CHUNK_SIZE_BYTES);
    let worst_case_bytes = v
        .worst_case_remaining_chunks
        .saturating_mul(CHUNK_SIZE_BYTES);
    let issued_pct = pct(v.issued_chunks, v.total_capacity_chunks);
    let bucket_max_pct = pct(u64::from(v.bucket_fill_max), u64::from(v.bucket_capacity));

    println!("batch         {}", v.batch_id);
    println!(
        "  depth       {}  bucket_depth {}  immutable {}",
        v.batch_depth, v.bucket_depth, v.immutable,
    );
    println!(
        "  capacity    {} chunks  ({})",
        v.total_capacity_chunks,
        format_bytes(total_bytes),
    );
    println!(
        "  issued      {} chunks  ({})  {:.1}%",
        v.issued_chunks,
        format_bytes(issued_bytes),
        issued_pct,
    );
    println!(
        "  free        {} chunks  ({})  optimistic, even routing",
        v.remaining_total_chunks,
        format_bytes(remaining_bytes),
    );
    println!(
        "  buckets     {} total  cap {}  fullest {} ({:.1}%)  emptiest {}",
        v.bucket_count, v.bucket_capacity, v.bucket_fill_max, bucket_max_pct, v.bucket_fill_min,
    );
    println!(
        "  upload room {} chunks  ({})  worst-case (next bucket-collision)",
        v.worst_case_remaining_chunks,
        format_bytes(worst_case_bytes),
    );
    if v.immutable {
        println!("  note        immutable batch — stamps are rejected once a bucket fills");
    } else {
        println!(
            "  note        mutable batch — past worst-case room, new stamps wrap and overwrite"
        );
        println!("              older indices, leaving their chunks un-restampable for resync");
    }
}

/// Swarm chunk size (data + span). Used to convert
/// `PostageStatusView` chunk counts into byte estimates for human
/// rendering. Mirrors `ant_retrieval::CHUNK_SIZE`; duplicated here
/// to keep `antctl` free of an `ant-retrieval` build dep.
const CHUNK_SIZE_BYTES: u64 = 4096;

fn pct(num: u64, denom: u64) -> f64 {
    if denom == 0 {
        0.0
    } else {
        (num as f64) * 100.0 / (denom as f64)
    }
}

/// Postage pre-flight: snapshot the daemon's stamp issuer state and
/// compare an estimated chunk count for `source_size` against the
/// conservative worst-case bucket budget. Bails on "uploads
/// disabled"; warns (stderr) but does not block when the estimate
/// exceeds the budget.
///
/// Why it's a warning, not a block: the operator can legitimately
/// want to top up / dilute a batch mid-upload (`antctl postage
/// topup` / `dilute`), or knowingly accept some chunks landing on a
/// fresher batch in a future re-upload. The pre-flight is here to
/// make the most common surprise — "I just kicked off a 7 GiB
/// upload that's going to fail at 5 GiB" — observable before any
/// chunk leaves the daemon, not to gate the operator's intent.
fn preflight_postage(socket: &Path, source_size: u64, raw: bool) -> Result<()> {
    let resp = match request_sync(socket, &Request::PostageStatus) {
        Ok(r) => r,
        Err(e) => {
            // Older daemons that don't know `PostageStatus` answer
            // with `Error{message:"bad request: …"}`, which
            // request_sync surfaces as Ok(Response::Error). A
            // transport-level error is a different case and almost
            // certainly means the upload itself will fail next; we
            // warn and let the caller proceed so the real error
            // surfaces below.
            eprintln!("warning: postage pre-flight skipped — control socket: {e}");
            return Ok(());
        }
    };
    let view = match resp {
        Response::PostageStatus(v) => v,
        Response::Error { message } => {
            // Old daemon, or status temporarily unavailable.
            eprintln!("warning: postage pre-flight skipped — antd: {message}");
            return Ok(());
        }
        other => bail!("unexpected response to PostageStatus: {other:?}"),
    };
    if !view.enabled {
        bail!(
            "uploads are disabled on this daemon: no postage batch configured. \
             Start antd with --postage-batch (or set STORAGE_STAMP_BATCH_ID) and try again",
        );
    }
    let estimate = estimate_chunk_count(source_size, raw);
    if estimate <= view.worst_case_remaining_chunks {
        return Ok(());
    }
    let estimate_bytes = estimate.saturating_mul(CHUNK_SIZE_BYTES);
    let budget_bytes = view
        .worst_case_remaining_chunks
        .saturating_mul(CHUNK_SIZE_BYTES);
    let policy = if view.immutable {
        "this immutable batch will reject the next stamp once a single bucket fills, \
         so the upload may FAIL mid-flight"
    } else {
        "this mutable batch will start overwriting older indices once a single bucket fills, \
         leaving previously-stamped chunks un-restampable for resync"
    };
    eprintln!(
        "warning: estimated upload ({} chunks ≈ {}) exceeds the daemon's worst-case \
         remaining-chunks budget ({} chunks ≈ {}). {policy}.\n         \
         pass --no-preflight to silence this warning, or run `antctl postage topup` / `dilute` \
         to extend the batch.",
        estimate,
        format_bytes(estimate_bytes),
        view.worst_case_remaining_chunks,
        format_bytes(budget_bytes),
    );
    Ok(())
}

/// Local copy of `ant_node::uploads::estimate_chunk_count`.
/// Duplicated here so `antctl` doesn't take a build-graph
/// dependency on `ant-node` / `ant-retrieval` just for the
/// pre-flight chunk count. Must stay byte-compatible with the
/// daemon's estimator; the unit test below pins both formulas to
/// the same canonical inputs.
const fn estimate_chunk_count(source_size: u64, raw: bool) -> u64 {
    /// Mantaray fan-out matching `ant_retrieval::BRANCHES`. Hard
    /// constant in the splitter; would only change as part of a
    /// chunk-format break that bumps the protocol version.
    const BRANCHES: u64 = 128;
    if source_size == 0 {
        return if raw { 1 } else { 2 };
    }
    let leaves = source_size.div_ceil(CHUNK_SIZE_BYTES);
    let mut total = leaves;
    let mut level = leaves;
    while level > 1 {
        level = level.div_ceil(BRANCHES);
        total += level;
    }
    if raw {
        total
    } else {
        total + 1
    }
}

#[cfg(test)]
mod estimate_chunk_count_tests {
    use super::*;

    /// Pin a few canonical sizes so a divergence from the daemon's
    /// formula is caught at CI time rather than in a real upload.
    /// Mirrors the test fixtures used by the `ant-node::uploads`
    /// path.
    #[test]
    fn matches_daemon_formula() {
        // Empty file: one leaf chunk + one manifest chunk, or just
        // one leaf in raw mode.
        assert_eq!(estimate_chunk_count(0, false), 2);
        assert_eq!(estimate_chunk_count(0, true), 1);

        // Single full leaf, no intermediate.
        assert_eq!(estimate_chunk_count(4096, false), 2);
        assert_eq!(estimate_chunk_count(4096, true), 1);

        // 2 leaves: fits in one intermediate fork; with manifest
        // adds 1.
        assert_eq!(estimate_chunk_count(8192, false), 4);
        assert_eq!(estimate_chunk_count(8192, true), 3);

        // 7 GiB: 1 835 008 leaves, 14 336 + 112 + 1 intermediates,
        // + 1 manifest = 1 849 458; raw drops the manifest.
        let size_7gib: u64 = 7 * 1024 * 1024 * 1024;
        let raw = estimate_chunk_count(size_7gib, true);
        let with_manifest = estimate_chunk_count(size_7gib, false);
        assert_eq!(with_manifest, raw + 1);
        assert_eq!(raw, 1_835_008 + 14_336 + 112 + 1);
    }
}

fn parse_addr(s: &str) -> Result<[u8; 20]> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).with_context(|| format!("address hex: {s}"))?;
    if bytes.len() != 20 {
        bail!(
            "address must be 20 bytes (40 hex chars); got {}",
            bytes.len()
        );
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn parse_word32(s: &str) -> Result<[u8; 32]> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).with_context(|| format!("word32 hex: {s}"))?;
    if bytes.len() != 32 {
        bail!("expected 32 bytes (64 hex chars), got {}", bytes.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn parse_secret(s: &str) -> Result<[u8; 32]> {
    let s = s.trim();
    parse_word32(s).context("invalid private key")
}

fn random_word32() -> [u8; 32] {
    use std::time::SystemTime;
    let mut out = [0u8; 32];
    // Mix nanoseconds + a fresh OS-random tail so two `create`
    // calls in the same second still produce distinct salts.
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    out[..16].copy_from_slice(&nanos.to_be_bytes()[..16]);
    let _ = getrandom_fill(&mut out[16..]);
    out
}

fn getrandom_fill(buf: &mut [u8]) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::Read;
    let mut f = File::open("/dev/urandom")?;
    f.read_exact(buf)?;
    Ok(())
}

fn hex_addr(a: &[u8; 20]) -> String {
    format!("0x{}", hex::encode(a))
}

/// Format an 18-decimal token amount (xDAI / ETH) with up to 6
/// fractional digits, trimming trailing zeros.
fn format_eth_18(plur: u128) -> String {
    let denom: u128 = 1_000_000_000_000_000_000; // 1e18
    let whole = plur / denom;
    let frac = plur % denom;
    if frac == 0 {
        format!("{whole} xDAI")
    } else {
        let frac_str = format!("{frac:018}");
        let trimmed = frac_str.trim_end_matches('0');
        let trimmed = if trimmed.len() > 6 {
            &trimmed[..6]
        } else {
            trimmed
        };
        format!("{whole}.{trimmed} xDAI")
    }
}

/// Format a 16-decimal BZZ amount (1 BZZ = 1e16 PLUR).
fn format_eth_16(plur: u128) -> String {
    let denom: u128 = 10_000_000_000_000_000; // 1e16
    let whole = plur / denom;
    let frac = plur % denom;
    if frac == 0 {
        format!("{whole} BZZ")
    } else {
        let frac_str = format!("{frac:016}");
        let trimmed = frac_str.trim_end_matches('0');
        let trimmed = if trimmed.len() > 6 {
            &trimmed[..6]
        } else {
            trimmed
        };
        format!("{whole}.{trimmed} BZZ")
    }
}

/// Percent-decode a `bzz://<hex>/<path>` URL path component the same
/// way `ant-gateway`'s axum extractor does, so a manifest entry with a
/// literal space (e.g. `02 butterfly.wav`) is still reachable via the
/// shell-friendly URL form `02%20butterfly.wav`. Only applied to the
/// `bzz://` URL branch — `--bzz-path <p>` is a literal argument the
/// caller already typed in raw form, so a `%20` there is preserved.
fn decode_bzz_url_path(raw: &str) -> Result<String> {
    percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|e| anyhow!("invalid percent-encoding in bzz path: {e}"))
}

/// Resolve a CLI reference + mode into the daemon-side `Request`. URL
/// schemes (`bzz://…`, `bytes://…`) on the reference take precedence
/// over the explicit `--bzz` / `--bytes` flags; the flags fill in the
/// gap when the user hands over a bare hex reference.
fn parse_reference_with_mode(
    reference: &str,
    mode: FetchMode,
    allow_degraded_redundancy: bool,
    bypass_cache: bool,
    progress: bool,
) -> Result<Request> {
    if let Some(rest) = reference.strip_prefix("bzz://") {
        // Split optional path (after first '/').
        let (hex_part, path_part) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        return Ok(Request::GetBzz {
            reference: hex_part.to_string(),
            path: decode_bzz_url_path(path_part)?,
            allow_degraded_redundancy,
            bypass_cache,
            progress,
        });
    }
    if let Some(rest) = reference.strip_prefix("bytes://") {
        return Ok(Request::GetBytes {
            reference: rest.to_string(),
            bypass_cache,
            progress,
        });
    }
    Ok(match mode {
        FetchMode::Chunk => Request::Get {
            reference: reference.to_string(),
            bypass_cache,
            progress,
        },
        FetchMode::Bytes => Request::GetBytes {
            reference: reference.to_string(),
            bypass_cache,
            progress,
        },
        FetchMode::Bzz { path } => Request::GetBzz {
            reference: reference.to_string(),
            path,
            allow_degraded_redundancy,
            bypass_cache,
            progress,
        },
    })
}

/// Fetch a chunk / file and write it to `out` (or stdout). With `--json`,
/// prints a structured envelope on stdout regardless of mode, since
/// shells generally won't render binary cleanly.
///
/// `show_progress` opts the request into streaming
/// `Response::Progress` updates and renders an in-place stderr status
/// line. The caller decides whether progress makes sense (TTY check,
/// `--json`, single-chunk fetches, etc.) so this function can stay
/// focused on plumbing.
// Eight args is one over clippy's preferred ceiling, but every one of
// them is a CLI-driven knob with a distinct meaning, and bundling them
// into a struct just for clippy would obscure the call site below.
#[allow(clippy::too_many_arguments)]
fn run_get(
    socket: &Path,
    reference: &str,
    out: Option<&Path>,
    json: bool,
    mode: FetchMode,
    allow_degraded_redundancy: bool,
    bypass_cache: bool,
    show_progress: bool,
    progress_style: ProgressStyle,
) -> Result<()> {
    let req = parse_reference_with_mode(
        reference,
        mode,
        allow_degraded_redundancy,
        bypass_cache,
        show_progress,
    )?;

    let (resp, last_progress) = if show_progress {
        let mut renderer = ProgressRenderer::new(bypass_cache, progress_style);
        let resp = request_streaming(socket, &req, |p| renderer.render(p))
            .with_context(|| format!("talk to antd at {}", socket.display()))?;
        let last = renderer.finish();
        (resp, last)
    } else {
        (
            request_sync(socket, &req)
                .with_context(|| format!("talk to antd at {}", socket.display()))?,
            None,
        )
    };

    // Unify the three shapes into (data, content_type, filename).
    let (data, content_type, filename) = match resp {
        Response::Bytes { hex } => (decode_hex_payload(&hex)?, None, None),
        Response::BzzBytes {
            hex,
            content_type,
            filename,
        } => (decode_hex_payload(&hex)?, content_type, filename),
        Response::Error { message } => bail!("antd: {message}"),
        other => bail!("unexpected response: {other:?}"),
    };

    let written = write_output(out, filename.as_deref(), &data)?;

    if json {
        let mut payload = serde_json::json!({
            "ok": true,
            "reference": reference,
            "bytes": data.len(),
        });
        if let Some(ct) = &content_type {
            payload["content_type"] = serde_json::Value::String(ct.clone());
        }
        if let Some(fname) = &filename {
            payload["filename"] = serde_json::Value::String(fname.clone());
        }
        if let Some(p) = &written {
            payload["out"] = serde_json::Value::String(p.display().to_string());
        } else {
            payload["hex"] = serde_json::Value::String(format!("0x{}", hex::encode(&data)));
        }
        println!("{payload}");
    } else if let Some(p) = written {
        let mut msg = format!("wrote {} bytes to {}", data.len(), p.display());
        if let Some(ct) = &content_type {
            write!(msg, " ({ct})").unwrap();
        }
        eprintln!("{msg}");
    } else if let Some(ct) = &content_type {
        // Stdout got the body; surface the content type on stderr so
        // shell pipelines like `antctl get bzz://<ref>/ | head` still
        // know what they got without polluting stdout.
        eprintln!("Content-Type: {ct}");
    }

    // Post-download stats summary. Only meaningful when streaming
    // progress was active (multi-chunk fetch on a TTY without
    // `--json` / `--no-progress`); otherwise we have nothing to
    // report and the existing one-line `wrote N bytes …` carries
    // its own narrative.
    if !json {
        if let Some(sample) = last_progress {
            eprintln!("{}", format_stats_summary(&sample, bypass_cache));
        }
    }
    Ok(())
}

/// EMA smoothing factor for the per-second download-rate readout.
/// 0.30 is a middle ground: visibly responsive when the rate jumps but
/// not so jumpy that a single fast/slow sample dominates the line.
const PROGRESS_EMA_ALPHA: f64 = 0.30;

/// Stateful renderer for `antctl get`'s per-line stderr progress
/// output. Holds the last sample we showed so we can compute an
/// instantaneous rate (with EMA smoothing) between consecutive
/// `GetProgress` events, plus the column count of the last line so
/// [`finish`](Self::finish) can erase it cleanly with `\r` + spaces.
struct ProgressRenderer {
    /// Selected stderr renderer for this request.
    style: ProgressStyle,
    /// Smoothed bytes/sec across the request. `None` until we have
    /// at least two samples to interpolate between.
    ema_bps: Option<f64>,
    /// Width of the most recently printed line, in display columns,
    /// so `finish` can wipe it with the right number of spaces.
    last_line_width: usize,
    /// Most recent sample received, so [`finish`](Self::finish) can
    /// hand it back to the caller for a post-download summary line.
    /// `None` if no progress event ever landed (single-chunk fetch
    /// or a request that errored before the first emitter tick).
    last_sample: Option<GetProgress>,
    /// Cached `bypass_cache` so we can prefix `[no-cache] ` even on
    /// the very first sample (where the in-flight `GetProgress` may
    /// still echo `false` if the daemon hasn't initialised the
    /// tracker yet — shouldn't happen in practice but guards us).
    bypass_cache_hint: bool,
}

impl ProgressRenderer {
    const fn new(bypass_cache: bool, style: ProgressStyle) -> Self {
        Self {
            style,
            ema_bps: None,
            last_line_width: 0,
            last_sample: None,
            bypass_cache_hint: bypass_cache,
        }
    }

    /// Update the rate EMA and re-render the in-place status line.
    /// Called once per `Response::Progress` from the streaming client.
    fn render(&mut self, p: &GetProgress) {
        if let Some(prev) = &self.last_sample {
            let dt_ms = p.elapsed_ms.saturating_sub(prev.elapsed_ms);
            let dbytes = p.bytes_done.saturating_sub(prev.bytes_done);
            // Skip the EMA update for samples that produced no
            // progress (e.g. emitter ticked while the joiner was
            // stuck on a slow peer). Keeping the EMA stable is
            // friendlier than letting it trend toward zero on every
            // stalled tick.
            if dt_ms > 0 && dbytes > 0 {
                let bps = (dbytes as f64) * 1000.0 / dt_ms as f64;
                self.ema_bps = Some(match self.ema_bps {
                    Some(prev) => prev.mul_add(1.0 - PROGRESS_EMA_ALPHA, bps * PROGRESS_EMA_ALPHA),
                    None => bps,
                });
            }
        }
        let previous = self.last_sample.as_ref();
        // `\r` returns to column 0; pad with spaces to overwrite any
        // trailing characters from the previous (longer) line, then
        // write the new content. Flush so the user sees updates even
        // when stderr is line-buffered.
        use std::io::Write as _;
        let mut err = std::io::stderr().lock();
        match self.style {
            ProgressStyle::Line => {
                let line = format_progress_line(p, self.ema_bps, self.bypass_cache_hint);
                let new_width = display_width(&line);
                let padding = self.last_line_width.saturating_sub(new_width);
                let _ = write!(err, "\r\x1b[2K{line}{}", " ".repeat(padding));
                self.last_line_width = new_width;
            }
            ProgressStyle::Visual => {
                let (bar, stats) = format_visual_progress_lines_for_width(
                    p,
                    previous,
                    self.ema_bps,
                    self.bypass_cache_hint,
                    terminal_width(),
                );
                let _ = write!(err, "\r\x1b[2K{bar}\n\r\x1b[2K{stats}\x1b[1A\r");
                self.last_line_width = 0;
            }
        }
        let _ = err.flush();
        self.last_sample = Some(p.clone());
    }

    /// Erase the in-place status line so subsequent stderr output
    /// (e.g. `wrote N bytes to <path>`) starts on a fresh column,
    /// and hand the most recent sample back to the caller for the
    /// post-download stats summary.
    fn finish(self) -> Option<GetProgress> {
        if self.last_sample.is_some() {
            use std::io::Write as _;
            let mut err = std::io::stderr().lock();
            match self.style {
                ProgressStyle::Line => {
                    let _ = write!(err, "\r\x1b[2K\r");
                }
                ProgressStyle::Visual => {
                    let _ = write!(err, "\r\x1b[2K\n\r\x1b[2K\x1b[1A\r");
                }
            }
            let _ = err.flush();
        }
        self.last_sample
    }
}

/// Render one `GetProgress` sample as a one-line stderr status. The
/// renderer trims to a "best summary" view rather than dumping every
/// counter; users wanting a structured feed can always re-run with
/// `--json` (which doesn't take this path).
fn format_progress_line(p: &GetProgress, ema_bps: Option<f64>, bypass_cache: bool) -> String {
    let chunks = if p.total_chunks_estimate > 0 {
        format!("{}/{}", p.chunks_done, p.total_chunks_estimate)
    } else {
        format!("{}/?", p.chunks_done)
    };
    let bytes = if p.total_bytes_estimate > 0 {
        format!(
            "{}/{}",
            format_bytes(p.bytes_done),
            format_bytes(p.total_bytes_estimate),
        )
    } else {
        format!("{}/?", format_bytes(p.bytes_done))
    };
    let rate = match ema_bps {
        Some(bps) if bps > 0.0 => format!("{}/s", format_bytes(bps as u64)),
        _ => "—/s".to_string(),
    };
    let elapsed = format_progress_elapsed(p.elapsed_ms);
    // Show the source mix even when the user didn't pass
    // `--bypass-cache`, so a populated cache doesn't "look slow"
    // simply because the joiner is hitting it instead of peers.
    let sources = format!(
        "{} peer{}{}",
        p.peers_used,
        if p.peers_used == 1 { "" } else { "s" },
        if p.cache_hits > 0 {
            format!(", {} cache", p.cache_hits)
        } else {
            String::new()
        },
    );
    let cache_tag = if bypass_cache { "[no-cache] " } else { "" };
    format!("{cache_tag}↓ {chunks} chunks  {bytes}  {rate}  {sources}  {elapsed}",)
}

/// Render a compact, defrag-style block map plus a separate stats line
/// from aggregate progress counters. The current control protocol does
/// not report exact chunk indexes, so the map visualizes
/// completed-vs-pending volume rather than claiming a physical chunk
/// layout.
fn format_visual_progress_lines_for_width(
    p: &GetProgress,
    previous: Option<&GetProgress>,
    ema_bps: Option<f64>,
    bypass_cache: bool,
    terminal_width: usize,
) -> (String, String) {
    const MAX_BLOCKS: usize = 72;
    let percent = if p.total_chunks_estimate > 0 {
        let capped = p.chunks_done.min(p.total_chunks_estimate);
        format!(
            "{:>5.1}%",
            capped as f64 * 100.0 / p.total_chunks_estimate as f64
        )
    } else {
        " ?.?%".to_string()
    };
    let chunks = if p.total_chunks_estimate > 0 {
        format!("{}/{}", p.chunks_done, p.total_chunks_estimate)
    } else {
        format!("{}/?", p.chunks_done)
    };
    let rate = match ema_bps {
        Some(bps) if bps > 0.0 => format!("{}/s", format_bytes(bps as u64)),
        _ => "--/s".to_string(),
    };
    let cache_tag = if bypass_cache { "[no-cache] " } else { "" };
    let stats = format!(
        "{cache_tag}{percent}  {chunks}ch  {rate}  {}p/{}c  {}",
        p.peers_used,
        p.cache_hits,
        format_progress_elapsed(p.elapsed_ms),
    );
    let blocks = terminal_width.saturating_sub(2).min(MAX_BLOCKS);

    let total = visual_total_chunks(p);
    let done_blocks = visual_done_blocks(p.chunks_done, total, blocks);
    let previous_done_blocks = previous
        .map_or(0, |prev| {
            visual_done_blocks(prev.chunks_done, total, blocks)
        })
        .min(done_blocks);
    let cache_blocks =
        visual_done_blocks(p.cache_hits.min(p.chunks_done), total, blocks).min(done_blocks);
    let scan = if blocks > 0 {
        ((p.elapsed_ms / 150) as usize) % blocks
    } else {
        0
    };

    let mut map = String::with_capacity(blocks);
    for i in 0..blocks {
        let ch = if i < done_blocks {
            if i >= previous_done_blocks && previous.is_some() {
                '▓'
            } else if i < cache_blocks {
                '▣'
            } else {
                '█'
            }
        } else if p.total_chunks_estimate == 0 && i == scan {
            '▒'
        } else {
            '░'
        };
        map.push(ch);
    }

    (format!("[{map}]"), stats)
}

fn visual_total_chunks(p: &GetProgress) -> u64 {
    if p.total_chunks_estimate > 0 {
        p.total_chunks_estimate
    } else {
        // Before the root span is known, reserve some pending space so
        // early samples still look like an in-flight map instead of a
        // completed tiny file.
        p.chunks_done.saturating_add(16).max(16)
    }
}

fn visual_done_blocks(chunks_done: u64, total_chunks: u64, blocks: usize) -> usize {
    if chunks_done == 0 || total_chunks == 0 || blocks == 0 {
        return 0;
    }
    let capped = chunks_done.min(total_chunks);
    ((capped * blocks as u64).div_ceil(total_chunks)) as usize
}

/// Single-line post-download summary printed once the in-place
/// progress line has been wiped. Pulls every interesting counter out
/// of the final `GetProgress` so the user can see, at a glance, how
/// the fetch went without scrolling back through live frames.
///
/// Format: `stats: 5 chunks (18.2KiB) in 312ms · avg 58.4KiB/s · 3 peers · 2 cache · cold`.
/// `cold` / `warm` is dropped when the daemon's cache wasn't
/// involved either way (no peers and no hits — should only happen on
/// a no-op fetch).
fn format_stats_summary(p: &GetProgress, bypass_cache_hint: bool) -> String {
    let chunks = if p.total_chunks_estimate > 0 && p.total_chunks_estimate != p.chunks_done {
        format!("{}/{} chunks", p.chunks_done, p.total_chunks_estimate)
    } else {
        format!(
            "{} chunk{}",
            p.chunks_done,
            if p.chunks_done == 1 { "" } else { "s" },
        )
    };
    let bytes = format_bytes(p.bytes_done);
    let elapsed = format_progress_elapsed(p.elapsed_ms);
    // Use cumulative average rather than the live EMA: the EMA was
    // useful while the user could read it, but the post-download
    // summary wants the deterministic "this is what just happened"
    // number — total bytes ÷ total seconds.
    let avg = if p.elapsed_ms > 0 {
        let bps = (p.bytes_done as f64) * 1000.0 / p.elapsed_ms as f64;
        format!("avg {}/s", format_bytes(bps as u64))
    } else {
        "avg —/s".to_string()
    };
    let peers = format!(
        "{} peer{}",
        p.peers_used,
        if p.peers_used == 1 { "" } else { "s" },
    );
    let cache = format!("{} cache", p.cache_hits);
    // Trust `bypass_cache` from the sample; fall back to the caller's
    // hint in case an old daemon ever lands a sample with the field
    // unset (defensive — shouldn't happen on v2).
    let mode = if p.bypass_cache || bypass_cache_hint {
        " · cold"
    } else if p.cache_hits > 0 || p.peers_used > 0 {
        " · warm"
    } else {
        ""
    };
    format!("stats: {chunks} ({bytes}) in {elapsed} · {avg} · {peers} · {cache}{mode}")
}

fn format_progress_elapsed(ms: u64) -> String {
    let secs = ms / 1000;
    let tenths = (ms % 1000) / 100;
    if secs >= 60 {
        let m = secs / 60;
        let s = secs % 60;
        format!("{m}m{s:02}s")
    } else {
        format!("{secs}.{tenths}s")
    }
}

/// Drive the `antctl upload` subcommand tree against the daemon's
/// control socket. Each variant maps to one or two `Request::Upload*`
/// round-trips; the default `start` path tail-follows the new job
/// until it reaches a terminal status (or the operator detaches with
/// Ctrl+C — the upload survives in the daemon).
fn run_upload(socket: &Path, command: UploadCommand, json: bool) -> Result<()> {
    match command {
        UploadCommand::Start {
            path,
            batch,
            name,
            content_type,
            raw,
            no_preflight,
            detach,
        } => {
            let abs_path = path
                .canonicalize()
                .with_context(|| format!("source path {}", path.display()))?;
            // Pre-flight: ask the daemon for the local stamp
            // issuer state and compare the file's chunk estimate
            // against the worst-case bucket budget. Bails early on
            // "uploads disabled"; otherwise warns (never blocks)
            // when the upload would exceed the conservative
            // budget. `--no-preflight` skips the check entirely.
            if !no_preflight {
                let metadata = std::fs::metadata(&abs_path)
                    .with_context(|| format!("stat source {}", abs_path.display()))?;
                preflight_postage(socket, metadata.len(), raw)?;
            }
            let req = Request::UploadStart {
                source_path: abs_path.display().to_string(),
                batch_id: batch,
                name,
                content_type,
                raw,
            };
            let resp = request_sync(socket, &req)
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            let job_id = match resp {
                Response::UploadStarted { job_id } => job_id,
                Response::Error { message } => bail!("antd: {message}"),
                other => bail!("unexpected response: {other:?}"),
            };
            if json {
                println!("{}", serde_json::json!({"ok": true, "job_id": job_id}));
            } else {
                eprintln!("upload {job_id} started");
            }
            if !detach {
                follow_upload(socket, &job_id, json)?;
            }
        }
        UploadCommand::List => {
            let resp = request_sync(socket, &Request::UploadList)
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            let mut items = match resp {
                Response::UploadList { jobs } => jobs,
                Response::Error { message } => bail!("antd: {message}"),
                other => bail!("unexpected response: {other:?}"),
            };
            items.sort_by_key(|j| j.created_at_unix);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&items).context("serialise upload list")?,
                );
            } else if items.is_empty() {
                println!("no upload jobs");
            } else {
                print_upload_table(&items);
            }
        }
        UploadCommand::Status { job_id } => {
            let resp = request_sync(socket, &Request::UploadStatus { job_id })
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            print_upload_response(resp, json)?;
        }
        UploadCommand::Pause { job_id } => {
            let resp = request_sync(socket, &Request::UploadPause { job_id })
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            print_upload_response(resp, json)?;
        }
        UploadCommand::Resume { job_id } => {
            let resp = request_sync(socket, &Request::UploadResume { job_id })
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            print_upload_response(resp, json)?;
        }
        UploadCommand::Cancel { job_id } => {
            let resp = request_sync(socket, &Request::UploadCancel { job_id })
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            print_upload_response(resp, json)?;
        }
        UploadCommand::Follow { job_id } => {
            follow_upload(socket, &job_id, json)?;
        }
    }
    Ok(())
}

fn print_upload_response(resp: Response, json: bool) -> Result<()> {
    match resp {
        Response::UploadJob(view) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&view).context("serialise upload job")?,
                );
            } else {
                println!("{}", format_upload_view_block(&view));
            }
        }
        Response::Error { message } => bail!("antd: {message}"),
        other => bail!("unexpected response: {other:?}"),
    }
    Ok(())
}

/// Tail-follow one upload job. Renders a one-line stderr status that
/// updates on every progress frame; on terminal status (`completed`,
/// `cancelled`, `failed`) clears the line and prints the final
/// snapshot on stdout. Returns immediately on `EOF` (server closed
/// the connection — same as a clean terminal) or on a
/// [`Response::Error`].
fn follow_upload(socket: &Path, job_id: &str, json: bool) -> Result<()> {
    let req = Request::UploadFollow {
        job_id: job_id.to_string(),
    };
    let show_progress = !json && std::io::IsTerminal::is_terminal(&std::io::stderr());
    let mut renderer = UploadProgressRenderer::default();
    let terminal = request_upload_follow(socket, &req, |view| {
        if show_progress {
            renderer.render(view);
        }
    })
    .with_context(|| format!("talk to antd at {}", socket.display()))?;
    if show_progress {
        renderer.finish();
    }
    match terminal {
        Response::UploadJob(view) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&view).context("serialise upload job")?,
                );
            } else {
                println!("{}", format_upload_view_block(&view));
            }
            Ok(())
        }
        Response::Error { message } => bail!("antd: {message}"),
        other => bail!("unexpected response in follow: {other:?}"),
    }
}

#[derive(Default)]
struct UploadProgressRenderer {
    last_line_width: usize,
    started: bool,
}

impl UploadProgressRenderer {
    fn render(&mut self, view: &UploadJobView) {
        use std::io::Write as _;
        let line = format_upload_progress_line(view);
        let new_width = display_width(&line);
        let padding = self.last_line_width.saturating_sub(new_width);
        let mut err = std::io::stderr().lock();
        let _ = write!(err, "\r\x1b[2K{line}{}", " ".repeat(padding));
        let _ = err.flush();
        self.last_line_width = new_width;
        self.started = true;
    }

    fn finish(&mut self) {
        if !self.started {
            return;
        }
        use std::io::Write as _;
        let mut err = std::io::stderr().lock();
        let _ = write!(err, "\r\x1b[2K\r");
        let _ = err.flush();
    }
}

fn format_upload_progress_line(view: &UploadJobView) -> String {
    let chunks = match view.chunks_total {
        Some(t) if t > 0 => format!("{}/{}", view.chunks_pushed, t),
        _ => format!("{}/?", view.chunks_pushed),
    };
    let bytes = if view.source_size > 0 {
        format!(
            "{}/{}",
            format_bytes(view.bytes_pushed),
            format_bytes(view.source_size),
        )
    } else {
        format!("{}/?", format_bytes(view.bytes_pushed))
    };
    let percent = if view.source_size > 0 {
        format!(
            " {:>5.1}%",
            (view.bytes_pushed as f64 * 100.0 / view.source_size as f64).min(100.0),
        )
    } else {
        String::new()
    };
    format!(
        "↑ [{}]{percent}  {chunks} chunks  {bytes}",
        format_upload_status_label(&view.status),
    )
}

fn print_upload_table(items: &[UploadJobView]) {
    println!(
        "{:<16}  {:<10}  {:>10}  {:<48}  PATH",
        "JOB", "STATUS", "PROGRESS", "REFERENCE",
    );
    for view in items {
        let percent = if view.source_size > 0 {
            format!(
                "{:>5.1}%",
                (view.bytes_pushed as f64 * 100.0 / view.source_size as f64).min(100.0),
            )
        } else {
            "  ?.?%".to_string()
        };
        let reference = view.reference.as_deref().unwrap_or("-").to_string();
        let path = view.source_path.as_str();
        println!(
            "{:<16}  {:<10}  {:>10}  {:<48}  {}",
            view.job_id,
            format_upload_status_label(&view.status),
            percent,
            reference,
            path,
        );
    }
}

fn format_upload_view_block(view: &UploadJobView) -> String {
    let mut out = String::new();
    writeln!(out, "job_id:    {}", view.job_id).unwrap();
    writeln!(
        out,
        "status:    {}",
        format_upload_status_label(&view.status)
    )
    .unwrap();
    writeln!(out, "source:    {}", view.source_path).unwrap();
    writeln!(
        out,
        "size:      {} ({} bytes)",
        format_bytes(view.source_size),
        view.source_size,
    )
    .unwrap();
    if view.source_size > 0 {
        writeln!(
            out,
            "progress:  {} / {} ({:.1}%)",
            format_bytes(view.bytes_pushed),
            format_bytes(view.source_size),
            (view.bytes_pushed as f64 * 100.0 / view.source_size as f64).min(100.0),
        )
        .unwrap();
    }
    if let Some(t) = view.chunks_total {
        writeln!(out, "chunks:    {} / {}", view.chunks_pushed, t).unwrap();
    } else {
        writeln!(out, "chunks:    {}", view.chunks_pushed).unwrap();
    }
    writeln!(
        out,
        "mode:      {}",
        if view.raw {
            "raw (/bytes)"
        } else {
            "manifest (/bzz)"
        }
    )
    .unwrap();
    if let Some(b) = &view.batch_id {
        writeln!(out, "batch:     {b}").unwrap();
    }
    if let Some(n) = &view.name {
        writeln!(out, "name:      {n}").unwrap();
    }
    if let Some(c) = &view.content_type {
        writeln!(out, "type:      {c}").unwrap();
    }
    if let Some(r) = &view.reference {
        writeln!(out, "reference: {r}").unwrap();
        let scheme = if view.raw { "bytes" } else { "bzz" };
        writeln!(out, "url:       {scheme}://{}", r.trim_start_matches("0x")).unwrap();
    }
    if let Some(e) = &view.last_error {
        writeln!(out, "error:     {e}").unwrap();
    }
    // Drop the trailing newline.
    if out.ends_with('\n') {
        out.pop();
    }
    out
}

const fn format_upload_status_label(s: &str) -> &str {
    s
}

/// Human-readable byte size: SI-ish powers of 1024 with a single decimal
/// for non-byte units. Kept inline to avoid a `humansize`-style dep just
/// for one stderr line.
fn format_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if n >= GIB {
        format!("{:.1}GiB", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.1}MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.1}KiB", n as f64 / KIB as f64)
    } else {
        format!("{n}B")
    }
}

/// Approximate display width in columns. Good enough for the progress
/// line, which is plain ASCII plus a few well-known progress glyphs
/// (`↓`, `█`, `░`); we treat each `char` as one column.
fn display_width(s: &str) -> usize {
    s.chars().count()
}

fn terminal_width() -> usize {
    terminal::size()
        .map(|(width, _)| width as usize)
        .unwrap_or(80)
        .max(20)
}

/// Decode a `0x`-prefixed (or bare) hex string from the daemon's
/// `Response::Bytes`. The daemon controls the format; we still validate
/// because a corrupted socket would otherwise silently drop bytes.
fn decode_hex_payload(s: &str) -> Result<Vec<u8>> {
    let stripped = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    hex::decode(stripped).context("decode response hex")
}

fn fetch_status(socket: &Path) -> Result<StatusSnapshot> {
    let resp = request_sync(socket, &Request::Status)
        .with_context(|| format!("talk to antd at {}", socket.display()))?;
    match resp {
        Response::Status(snap) => Ok(*snap),
        Response::Error { message } => bail!("antd: {message}"),
        other => bail!("unexpected response: {other:?}"),
    }
}

/// Block until the daemon reports `min_peers` libp2p peers AND at least
/// one completed BZZ handshake, or `timeout` elapses. Best-effort: any
/// transport hiccup is logged and ignored — the actual `Get` request
/// will surface a real error if the daemon stays unreachable.
///
/// Distinct from `peers.routing.size` (BZZ-handshaked peers in the
/// forwarding-Kademlia table): `connected + last_handshake.is_some()`
/// matches the matrix the daemon's `run_get_*` paths actually trip on
/// (`peers.is_empty()` from `routing.snapshot()`), so the bar is set
/// where the daemon needs it, not higher.
fn wait_for_peers(socket: &Path, min_peers: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut printed_waiting = false;
    loop {
        match fetch_status(socket) {
            Ok(snap) => {
                if snap.peers.last_handshake.is_some() && snap.peers.connected >= min_peers {
                    if printed_waiting {
                        eprintln!(
                            "antctl: routing ready ({} peers, {} connected)",
                            snap.peers.routing.size, snap.peers.connected,
                        );
                    }
                    return;
                }
                if !printed_waiting {
                    eprintln!(
                        "antctl: waiting for {} peers (currently {}, routing={})",
                        min_peers, snap.peers.connected, snap.peers.routing.size,
                    );
                    printed_waiting = true;
                }
            }
            Err(e) => {
                eprintln!("antctl: status poll failed (will retry): {e:#}");
            }
        }
        if Instant::now() >= deadline {
            eprintln!(
                "antctl: --wait-peers={min_peers} not reached in {timeout:?}; issuing request anyway",
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn run_top(socket: &Path, interval_ms: u64) -> Result<()> {
    let interval_ms = interval_ms.max(1);
    let refresh = Duration::from_millis(interval_ms);
    let mut terminal = TopTerminal::new()?;
    let mut next_refresh = Instant::now();
    let mut last_status: Option<StatusSnapshot> = None;
    let mut last_error: Option<String> = None;
    let mut page = TopPage::Nodes;
    let mut selection = NodeSelection::default();
    let mut bandwidth = BandwidthTracker::new();

    loop {
        if Instant::now() >= next_refresh {
            match fetch_status(socket) {
                Ok(snap) => {
                    bandwidth.observe(&snap.retrieval, Instant::now());
                    let idx = selection.current_index(&snap);
                    render_top(
                        terminal.terminal_mut(),
                        &snap,
                        socket,
                        interval_ms,
                        page,
                        idx,
                        &bandwidth,
                    )?;
                    last_status = Some(snap);
                    last_error = None;
                }
                Err(e) => {
                    let error = e.to_string();
                    render_top_error(terminal.terminal_mut(), socket, interval_ms, page, &error)?;
                    last_status = None;
                    last_error = Some(error);
                }
            }
            next_refresh = Instant::now() + refresh;
        }

        let timeout = next_refresh
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(200));
        if event::poll(timeout)? {
            let event = event::read()?;
            if should_quit(&event) {
                break;
            }
            let redraw = if let Some(next_page) = page.after_event(&event) {
                page = next_page;
                true
            } else {
                handle_selection_event(&event, &mut selection, last_status.as_ref())
                    || matches!(event, Event::Resize(_, _))
            };
            if redraw {
                if let Some(snap) = &last_status {
                    let idx = selection.current_index(snap);
                    render_top(
                        terminal.terminal_mut(),
                        snap,
                        socket,
                        interval_ms,
                        page,
                        idx,
                        &bandwidth,
                    )?;
                } else if let Some(error) = &last_error {
                    render_top_error(terminal.terminal_mut(), socket, interval_ms, page, error)?;
                }
            }
        }
    }
    Ok(())
}

/// Rolling bandwidth estimate derived from successive
/// `RetrievalInfo` snapshots.
///
/// The daemon publishes only cumulative `bytes_fetched_total`; the
/// instantaneous rate is `(b2 - b1) / (t2 - t1)`. We smooth that with
/// a simple EMA so a 100 ms cache-warm spike or a one-off lull
/// doesn't make the gauge flap, and remember the all-time peak so
/// the user can spot how close the current run came to saturating
/// the connection.
///
/// Counters reset to zero on daemon restart (the `Arc<RetrievalCounters>`
/// is fresh per process); we detect that as a backwards delta and
/// reset the EMA rather than reporting a giant negative rate.
struct BandwidthTracker {
    last_sample: Option<(u64, Instant)>,
    /// Smoothed bytes/sec. `None` until at least two samples have
    /// been observed and a non-zero delta has been computed.
    ema_bps: Option<f64>,
    /// All-time peak over a single sample window. Useful for "what's
    /// the best this daemon ever managed?" without scrolling the log.
    peak_bps: f64,
    peak_chunks: u64,
}

impl BandwidthTracker {
    /// EMA smoothing factor. 0.3 keeps the gauge responsive (a real
    /// step up to a new rate stabilises in ~3 samples) without
    /// jittering visibly between consecutive reads of an active
    /// stream.
    const ALPHA: f64 = 0.3;

    const fn new() -> Self {
        Self {
            last_sample: None,
            ema_bps: None,
            peak_bps: 0.0,
            peak_chunks: 0,
        }
    }

    /// Fold one fresh snapshot into the rolling state. Cheap; safe
    /// to call on every refresh tick.
    fn observe(&mut self, info: &RetrievalInfo, now: Instant) {
        let bytes = info.bytes_fetched_total;
        if info.chunks_fetched_total > self.peak_chunks {
            self.peak_chunks = info.chunks_fetched_total;
        }
        let prev = self.last_sample;
        self.last_sample = Some((bytes, now));
        let Some((prev_bytes, prev_at)) = prev else {
            return;
        };
        // Daemon restart or counter wraparound: drop the EMA so we
        // don't sample a wildly negative delta as legitimate.
        if bytes < prev_bytes {
            self.ema_bps = None;
            return;
        }
        let dt = now.saturating_duration_since(prev_at).as_secs_f64();
        if dt <= 0.0 {
            return;
        }
        let rate = (bytes - prev_bytes) as f64 / dt;
        self.ema_bps = Some(match self.ema_bps {
            Some(prev) => Self::ALPHA.mul_add(rate, (1.0 - Self::ALPHA) * prev),
            None => rate,
        });
        if rate > self.peak_bps {
            self.peak_bps = rate;
        }
    }

    const fn current_bps(&self) -> Option<f64> {
        self.ema_bps
    }

    const fn peak_bps(&self) -> f64 {
        self.peak_bps
    }
}

fn clamp_selection(selected: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        selected.min(len - 1)
    }
}

/// Rows shown in the Nodes table (pipeline view when the daemon populates it).
const fn peer_list_len(s: &StatusSnapshot) -> usize {
    if s.peers.peer_pipeline.is_empty() {
        s.peers.connected_peers.len()
    } else {
        s.peers.peer_pipeline.len()
    }
}

/// Position-pinned selection: the cursor sticks to the row index the user
/// navigated to, regardless of how peers churn underneath. Default is row 0
/// — when the user hasn't moved yet the cursor stays glued to the top, so
/// the visible header row never drifts as new peers stream in. Once the
/// user presses Up/Down the index becomes "sticky" on that line: peers
/// added or removed below don't move it, peers added or removed above
/// don't either (we honour the index as a coordinate, not an anchor on a
/// specific peer). The index is only clamped if the list shrinks past it.
#[derive(Default)]
struct NodeSelection {
    index: usize,
}

impl NodeSelection {
    fn current_index(&self, s: &StatusSnapshot) -> usize {
        let len = peer_list_len(s);
        if len == 0 {
            0
        } else {
            self.index.min(len - 1)
        }
    }

    fn move_by(&mut self, s: &StatusSnapshot, delta: isize) {
        let len = peer_list_len(s);
        if len == 0 {
            self.index = 0;
            return;
        }
        let cur = self.current_index(s) as isize;
        let next = (cur + delta).clamp(0, len as isize - 1) as usize;
        self.index = next;
    }
}

fn short_peer_id(id: &str) -> String {
    let c = id.chars().count();
    if c <= 9 {
        return id.to_string();
    }
    let pre: String = id.chars().take(4).collect();
    let suf: String = id
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{pre}…{suf}")
}

const fn pipeline_state_label(st: PeerConnectionState) -> &'static str {
    match st {
        PeerConnectionState::Dialing => "Dialing",
        PeerConnectionState::Identifying => "Identifying",
        PeerConnectionState::Handshaking => "Handshaking",
        PeerConnectionState::Ready => "Ready",
        PeerConnectionState::Failed => "Failed",
        PeerConnectionState::Closing => "Closing",
    }
}

/// Format a `ready_in_ms` value for the Nodes table. `<1 s` is shown
/// in milliseconds (`312 ms`), `<100 s` in tenths of a second
/// (`3.4 s`), and longer durations in whole seconds (`120 s`). The
/// resulting strings are at most 7 characters, so they fit the
/// 8-column `ready in` field with breathing room.
fn format_ready_in_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms} ms")
    } else if ms < 100_000 {
        let secs = ms as f64 / 1_000.0;
        format!("{secs:.1} s")
    } else {
        format!("{} s", ms / 1_000)
    }
}

/// Sort key for `antctl top`'s Nodes table: slowest `ready_in_ms` first,
/// peers that haven't reached `Ready` yet pushed to the bottom in the
/// daemon's existing arrival order. Returns indices into
/// `s.peers.peer_pipeline`; both the table and the detail panel must
/// route through this so the cursor row and the details stay in sync.
fn pipeline_view_order(s: &StatusSnapshot) -> Vec<usize> {
    let mut order: Vec<usize> = (0..s.peers.peer_pipeline.len()).collect();
    order.sort_by(|&a, &b| {
        let pa = &s.peers.peer_pipeline[a];
        let pb = &s.peers.peer_pipeline[b];
        match (pa.ready_in_ms, pb.ready_in_ms) {
            (Some(x), Some(y)) => y.cmp(&x).then(a.cmp(&b)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.cmp(&b),
        }
    });
    order
}

fn ready_count_for_gauge(s: &StatusSnapshot) -> u32 {
    if s.peers.peer_pipeline.is_empty() {
        s.peers.connected_peers.len() as u32
    } else {
        s.peers
            .peer_pipeline
            .iter()
            .filter(|p| p.state == PeerConnectionState::Ready)
            .count() as u32
    }
}

/// Returns true if the event was consumed and the caller should redraw.
fn handle_selection_event(
    event: &Event,
    selection: &mut NodeSelection,
    status: Option<&StatusSnapshot>,
) -> bool {
    let Some(snap) = status else { return false };
    if peer_list_len(snap) == 0 {
        return false;
    }
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
            KeyCode::Up => {
                selection.move_by(snap, -1);
                true
            }
            KeyCode::Down => {
                selection.move_by(snap, 1);
                true
            }
            _ => false,
        },
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TopPage {
    Nodes,
    Retrieval,
    Status,
}

impl TopPage {
    const ALL: [Self; 3] = [Self::Nodes, Self::Retrieval, Self::Status];

    fn index(self) -> usize {
        Self::ALL.iter().position(|p| *p == self).unwrap_or(0)
    }

    const fn title(self) -> &'static str {
        match self {
            Self::Nodes => "Nodes",
            Self::Retrieval => "Retrieval",
            Self::Status => "Status",
        }
    }

    fn previous(self) -> Self {
        let idx = self.index();
        Self::ALL[(idx + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    fn next(self) -> Self {
        let idx = self.index();
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    fn after_event(self, event: &Event) -> Option<Self> {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Left => Some(self.previous()),
                KeyCode::Right => Some(self.next()),
                _ => None,
            },
            _ => None,
        }
    }
}

struct TopTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TopTerminal {
    fn new() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(e) = execute!(stdout, EnterAlternateScreen, Hide) {
            let _ = terminal::disable_raw_mode();
            return Err(e.into());
        }
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal })
    }

    const fn terminal_mut(&mut self) -> &mut Terminal<CrosstermBackend<Stdout>> {
        &mut self.terminal
    }
}

impl Drop for TopTerminal {
    fn drop(&mut self) {
        let _ = execute!(
            self.terminal.backend_mut(),
            Show,
            LeaveAlternateScreen,
            ResetColor
        );
        let _ = terminal::disable_raw_mode();
    }
}

fn should_quit(event: &Event) -> bool {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            matches!(key.code, KeyCode::Char('q' | 'Q') | KeyCode::Esc)
                || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        }
        _ => false,
    }
}

fn resolve_socket(opt: &Opt) -> PathBuf {
    if let Some(p) = &opt.socket {
        return expand_tilde(p);
    }
    let dir = expand_tilde(&opt.data_dir);
    dir.join("antd.sock")
}

fn expand_tilde(p: &Path) -> PathBuf {
    if let Some(s) = p.to_str() {
        if let Some(rest) = s.strip_prefix("~/") {
            return dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(rest);
        }
    }
    p.to_path_buf()
}

fn print_status(s: &StatusSnapshot) {
    let now = current_unix();
    let uptime = now.saturating_sub(s.started_at_unix);

    println!(
        "{}  (pid {}, up {})",
        s.agent,
        s.pid,
        format_duration(uptime),
    );
    println!();
    println!("Network:");
    println!(
        "  Network ID:  {}{}",
        s.network_id,
        if s.network_id == 1 { " (mainnet)" } else { "" },
    );
    println!("  ETH:         {}", s.identity.eth_address);
    println!("  Overlay:     {}", s.identity.overlay);
    println!("  Peer ID:     {}", s.identity.peer_id);
    if s.listeners.is_empty() {
        println!("  Listeners:   (none yet)");
    } else {
        println!("  Listeners:");
        for l in &s.listeners {
            println!("    - {l}");
        }
    }
    if s.external_addresses.is_empty() {
        println!("  Externals:   (none yet)");
    } else {
        println!("  Externals:");
        for e in &s.external_addresses {
            let age = now.saturating_sub(e.added_at_unix);
            println!(
                "    - {} (source: {}, {} ago)",
                e.addr,
                e.source,
                format_duration(age),
            );
        }
    }
    println!();
    println!("Peers:");
    println!("  Connected:   {}", s.peers.connected);
    if let Some(h) = &s.peers.last_handshake {
        let age = now.saturating_sub(h.at_unix);
        let agent = if h.agent_version.is_empty() {
            "(unknown agent)".to_string()
        } else {
            h.agent_version.clone()
        };
        println!(
            "  Last BZZ:    {} ({}, {} ago){}",
            h.remote_overlay,
            agent,
            format_duration(age),
            if h.full_node { ", full-node" } else { "" },
        );
    } else {
        println!("  Last BZZ:    (none yet)");
    }
    println!();
    println!("Control:");
    println!("  Socket:      {}", s.control_socket);
}

#[allow(clippy::too_many_arguments)]
fn render_top(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    s: &StatusSnapshot,
    socket: &Path,
    interval_ms: u64,
    page: TopPage,
    selected_node: usize,
    bandwidth: &BandwidthTracker,
) -> Result<()> {
    let now = current_unix();
    let uptime = now.saturating_sub(s.started_at_unix);
    let ctx = TopFrameContext {
        status: s,
        socket,
        interval_ms,
        page,
        selected_node,
        now,
        uptime,
        bandwidth,
    };
    terminal.draw(|frame| {
        draw_top_frame(frame, &ctx);
    })?;
    Ok(())
}

struct TopFrameContext<'a> {
    status: &'a StatusSnapshot,
    socket: &'a Path,
    interval_ms: u64,
    page: TopPage,
    selected_node: usize,
    now: u64,
    uptime: u64,
    bandwidth: &'a BandwidthTracker,
}

fn render_top_error(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    socket: &Path,
    interval_ms: u64,
    page: TopPage,
    error: &str,
) -> Result<()> {
    terminal.draw(|frame| {
        draw_error_frame(frame, socket, interval_ms, page, error);
    })?;
    Ok(())
}

fn draw_top_frame(frame: &mut Frame, ctx: &TopFrameContext<'_>) {
    let area = frame.area();
    if area.width < 50 || area.height < 16 {
        draw_too_small(frame, area);
        return;
    }

    let [header, tabs, body, footer] = vertical_chunks(
        area,
        [
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ],
    );
    draw_header(
        frame,
        header,
        &format!(
            "antctl top | {} | pid {} | up {} ",
            ctx.status.agent,
            ctx.status.pid,
            format_duration(ctx.uptime)
        ),
    );
    draw_bar(frame, footer, " q: quit ");
    draw_tabs(frame, tabs, ctx.page);

    let network_id = format!(
        "{}{}",
        ctx.status.network_id,
        if ctx.status.network_id == 1 {
            " (mainnet)"
        } else {
            ""
        }
    );
    match ctx.page {
        TopPage::Nodes => {
            draw_nodes_page(frame, body, ctx.status, ctx.now, ctx.selected_node);
        }
        TopPage::Retrieval => {
            draw_retrieval_page(frame, body, ctx);
        }
        TopPage::Status => {
            let mut rows = vec![
                kv("Process", ""),
                kv("Agent", &ctx.status.agent),
                kv("PID", &ctx.status.pid.to_string()),
                kv("Uptime", &format_duration(ctx.uptime)),
                kv("Refresh", &format!("{}ms", ctx.interval_ms)),
                kv("Updated", &ctx.now.to_string()),
                kv("", ""),
                kv("Network", ""),
                kv("Network", &network_id),
                kv("ETH", &ctx.status.identity.eth_address),
                kv("Overlay", &ctx.status.identity.overlay),
                kv("Peer ID", &ctx.status.identity.peer_id),
                kv("Connected", &ctx.status.peers.connected.to_string()),
                kv("Listeners", &ctx.status.listeners.len().to_string()),
            ];
            // One row per advertised external address, attributed to its
            // source. Operators debugging a 10 s handshake stall use this
            // panel to confirm whether their `--external-address` /
            // listener / UPnP path actually produced something. Empty
            // collection means we have nothing advertised yet, which is
            // a useful (and bad) signal in itself.
            if ctx.status.external_addresses.is_empty() {
                rows.push(kv("Externals", "none"));
            } else {
                rows.push(kv("Externals", ""));
                for e in &ctx.status.external_addresses {
                    let age = ctx.now.saturating_sub(e.added_at_unix);
                    let value = format!("{} [{}, {} ago]", e.addr, e.source, format_duration(age));
                    rows.push(kv("  Addr", &value));
                }
            }
            rows.extend([
                kv("", ""),
                kv("Control", ""),
                kv("Socket", &ctx.status.control_socket),
                kv("Requested", &ctx.socket.display().to_string()),
                kv("Protocol", &format!("v{}", ctx.status.protocol_version)),
            ]);
            draw_panel(frame, body, ctx.page.title(), &rows);
        }
    }
}

fn draw_error_frame(
    frame: &mut Frame,
    socket: &Path,
    interval_ms: u64,
    page: TopPage,
    error: &str,
) {
    let area = frame.area();
    if area.width < 50 || area.height < 16 {
        draw_too_small(frame, area);
        return;
    }
    let [header, tabs, body, footer] = vertical_chunks(
        area,
        [
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ],
    );
    draw_header(
        frame,
        header,
        &format!("antctl top | disconnected | refresh {interval_ms}ms "),
    );
    draw_bar(frame, footer, " q: quit ");
    draw_tabs(frame, tabs, page);
    match page {
        TopPage::Nodes => draw_disconnected_nodes_page(frame, body, error),
        TopPage::Retrieval | TopPage::Status => {
            let rows = [
                kv("Daemon", "disconnected"),
                kv("Socket", &socket.display().to_string()),
                kv("Error", error),
                kv("Retry", "polling"),
                kv("Refresh", &format!("{interval_ms}ms")),
            ];
            draw_panel(frame, body, page.title(), &rows);
        }
    }
}

fn draw_too_small(frame: &mut Frame, area: TuiRect) {
    let text = Paragraph::new(
        "Terminal too small for antctl top.\nUse at least 50 columns x 16 rows. Press q to quit.",
    )
    .style(Style::default().bg(Color::Blue).fg(Color::Yellow));
    frame.render_widget(text, area);
}

fn draw_bar(frame: &mut Frame, area: TuiRect, text: &str) {
    frame.render_widget(
        Paragraph::new(fit(text, area.width as usize)).style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        area,
    );
}

fn draw_header(frame: &mut Frame, area: TuiRect, text: &str) {
    let prefix = " Ant | ";
    let text_width = area.width.saturating_sub(prefix.chars().count() as u16) as usize;
    let line = Line::from(vec![
        Span::styled(
            prefix,
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            fit(text, text_width),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(Color::Blue)),
        area,
    );
}

fn draw_tabs(frame: &mut Frame, area: TuiRect, page: TopPage) {
    let titles: Vec<Line> = TopPage::ALL
        .iter()
        .map(|p| Line::from(format!(" {} ", p.title())))
        .collect();
    let tabs = Tabs::new(titles)
        .select(page.index())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Plain)
                .border_style(Style::default().fg(Color::Cyan))
                .style(Style::default().bg(Color::Blue)),
        )
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .highlight_style(
            Style::default()
                .bg(Color::Cyan)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, area);
}

fn draw_panel(frame: &mut Frame, area: TuiRect, title: &str, rows: &[PanelRow]) {
    let lines: Vec<Line> = rows
        .iter()
        .map(|row| {
            Line::from(vec![
                Span::styled(
                    fit(&row.key, 10),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
                Span::styled(row.value.clone(), Style::default().fg(row.color)),
            ])
        })
        .collect();
    let block = Block::default()
        .title(format!(" {title} "))
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue));
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(Color::Blue).fg(Color::White))
            .block(block),
        area,
    );
}

/// Lay out the Retrieval tab: a 7-row metrics panel at the top
/// (cache, in-flight, bandwidth) and the gateway-requests table
/// underneath. The panel needs a fixed height so the table reflows
/// gracefully when the terminal is short — without it, ratatui would
/// hand the table all the slack and clip the metrics readout.
fn draw_retrieval_page(frame: &mut Frame, area: TuiRect, ctx: &TopFrameContext<'_>) {
    let r = &ctx.status.retrieval;
    let [metrics_area, requests_area] = vertical_chunks(
        area,
        // 9 rows = top border (1) + 6 panel rows + bottom border (1) +
        // 1 padding. The two cache rows (Mem / Disk) plus In-flight,
        // Bandwidth, Totals, Requests give us six.
        [Constraint::Length(9), Constraint::Min(0)],
    );
    draw_retrieval_metrics(frame, metrics_area, r, ctx.bandwidth);
    draw_gateway_requests(frame, requests_area, r, ctx.now);
}

/// Top-of-tab metrics readout: per-tier cache fill, in-flight count,
/// and bandwidth. Uses the same `draw_panel` helper as the Status tab
/// so the colour palette matches.
///
/// The disk-cache row degrades gracefully: when the daemon was
/// started without a persistent cache (`--no-disk-cache` or open
/// failure) the row reads `disabled` rather than `0/0 (0.0%)`. That
/// way operators can tell at a glance whether the second tier is
/// even wired up — important when diagnosing "I configured a disk
/// cache and nothing got faster on restart".
fn draw_retrieval_metrics(
    frame: &mut Frame,
    area: TuiRect,
    r: &RetrievalInfo,
    bandwidth: &BandwidthTracker,
) {
    let mem_cache = format!(
        "{}/{} chunks ({})  · {} hits",
        r.cache.used,
        r.cache.capacity,
        format_percentage(u64::from(r.cache.used), u64::from(r.cache.capacity)),
        format_count(r.mem_hits_total),
    );
    let disk_cache = if r.disk.enabled {
        format!(
            "{} / {} ({})  · {} hits",
            format_bytes(r.disk.used_bytes),
            format_bytes(r.disk.capacity_bytes),
            format_percentage(r.disk.used_bytes, r.disk.capacity_bytes),
            format_count(r.disk.hits_total),
        )
    } else {
        "disabled".to_string()
    };
    let in_flight = format!("{}/{} chunks", r.in_flight, r.in_flight_capacity);
    let totals = format!(
        "{} fetched · {} ({} cache hits)",
        format_count(r.chunks_fetched_total),
        format_bytes(r.bytes_fetched_total),
        format_count(r.cache_hits_total),
    );
    let bw_now = bandwidth
        .current_bps()
        .map_or_else(|| "—".to_string(), format_byte_rate);
    let bw_peak = if bandwidth.peak_bps() > 0.0 {
        format_byte_rate(bandwidth.peak_bps())
    } else {
        "—".to_string()
    };
    let bandwidth_row = format!("{bw_now}  (peak {bw_peak})");

    let rows = vec![
        kv("Mem cache", &mem_cache),
        kv("Disk cache", &disk_cache),
        kv("In-flight", &in_flight),
        kv("Bandwidth", &bandwidth_row),
        kv("Totals", &totals),
        kv("Requests", &r.gateway_requests.len().to_string()),
    ];
    draw_panel(frame, area, "Retrieval", &rows);
}

/// Minimum widths for the fixed columns of the gateway-requests
/// table. The `Path` column is rendered with a `Min` constraint
/// (see [`gateway_constraints`]) so it absorbs all leftover
/// horizontal space — long bzz paths are visible in full on a wide
/// terminal but the table still renders cleanly at the 50-column
/// floor enforced by `draw_top_frame`.
///
/// Each width is `max(header, an upper bound on the longest cell
/// ever rendered)`:
///
/// * `Kind` — up to 5-char label (`Bytes` / `BZZ` / `Chunk` /
///   `Manif`); 5 fits all of them and the header.
/// * `Age` — `format_duration` emits `<60s` / `Mm Ss` / `Hh Mm Ss`;
///   8 fits up to ~99 hours of uptime before clipping.
/// * `Done` — `chunks_done/total_chunks` with up to ~6-digit values
///   each, plus the `/` separator (~14).
/// * `Infl` — `chunks_in_flight` is bounded by
///   `RETRIEVAL_REQUEST_INFLIGHT_CAP` (≤3 digits today).
/// * `Bytes` — `format_bytes` always emits `<7 chars` (`9.9GiB`).
const GATEWAY_FIXED_WIDTHS: [u16; 5] = [5, 8, 14, 6, 12];
/// Floor on the `Path` column so the header stays legible even when
/// every other column is fully expanded. Anything beyond this is
/// growth space the column claims via `Constraint::Min`.
const GATEWAY_PATH_MIN_WIDTH: u16 = 22;
const GATEWAY_HEADER: [&str; 6] = ["Kind", "Path", "Age", "Done", "Infl", "Bytes"];

/// Build the per-column constraint set. `Path` gets `Min` so any
/// horizontal slack the table area has after the fixed columns and
/// inter-column spacing goes to it; the other five columns stay at
/// their constant `Length` so numbers don't bounce around as the
/// terminal resizes.
const fn gateway_constraints() -> [Constraint; 6] {
    [
        Constraint::Length(GATEWAY_FIXED_WIDTHS[0]),
        Constraint::Min(GATEWAY_PATH_MIN_WIDTH),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[1]),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[2]),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[3]),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[4]),
    ]
}

fn draw_gateway_requests(frame: &mut Frame, area: TuiRect, r: &RetrievalInfo, now: u64) {
    let block = Block::default()
        .title(" Gateway requests ")
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue));

    let header = Row::new(GATEWAY_HEADER.to_vec()).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    // Trim to whatever fits in the available rows. The body grows
    // top-down (oldest request first); when more requests arrive
    // than fit we drop the newest ones rather than older ones —
    // older requests are the ones likely to need attention if
    // they're hung.
    let visible_rows = area.height.saturating_sub(3).max(1) as usize;
    let cells: Vec<Vec<String>> = if r.gateway_requests.is_empty() {
        vec![vec![
            "—".to_string(),
            "(idle)".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ]]
    } else {
        r.gateway_requests
            .iter()
            .take(visible_rows)
            .map(|req| gateway_request_cells(req, now))
            .collect()
    };

    let rows: Vec<Row> = cells.into_iter().map(Row::new).collect();
    let table = Table::new(rows, gateway_constraints())
        .header(header)
        .column_spacing(2)
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .block(block);
    frame.render_widget(table, area);
}

/// Build the cell strings for one row. The `Path` cell is *not*
/// truncated here — ratatui clips it to the rendered column width,
/// which lets the column expand to show the full path on wide
/// terminals.
fn gateway_request_cells(req: &GatewayRequestInfo, now: u64) -> Vec<String> {
    let age_secs = now.saturating_sub(req.started_at_unix);
    let done = if req.total_chunks_estimate > 0 {
        format!("{}/{}", req.chunks_done, req.total_chunks_estimate)
    } else {
        format!("{}", req.chunks_done)
    };
    vec![
        req.kind.label().to_string(),
        req.path.clone(),
        format_duration(age_secs),
        done,
        req.chunks_in_flight.to_string(),
        format_bytes(req.bytes_done),
    ]
}

/// Compact "X / max" percentage shown next to the cache fill in the
/// Retrieval metrics panel. Returns `0%` for `max == 0` rather than
/// dividing by zero (defensive — capacity is always non-zero in the
/// daemon, but a misbehaving fixture or future config could ship a
/// `0` value).
fn format_percentage(used: u64, max: u64) -> String {
    if max == 0 {
        return "0%".to_string();
    }
    let pct = (used as f64 / max as f64) * 100.0;
    format!("{pct:.0}%")
}

/// SI-ish chunk count: `1234` → `1.2K`, `1_234_567` → `1.2M`. Keeps
/// the totals column from sprawling when a busy daemon has fetched
/// millions of chunks across uptime.
fn format_count(n: u64) -> String {
    const K: u64 = 1_000;
    const M: u64 = 1_000_000;
    const G: u64 = 1_000_000_000;
    if n >= G {
        format!("{:.1}G", n as f64 / G as f64)
    } else if n >= M {
        format!("{:.1}M", n as f64 / M as f64)
    } else if n >= K {
        format!("{:.1}K", n as f64 / K as f64)
    } else {
        n.to_string()
    }
}

/// Bytes-per-second pretty printer for the bandwidth gauge.
/// Mirrors `format_bytes` but always emits `…/s` — even at 0 B/s
/// to make it obvious the field is a rate.
fn format_byte_rate(bps: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    if bps >= GIB {
        format!("{:.2} GiB/s", bps / GIB)
    } else if bps >= MIB {
        format!("{:.2} MiB/s", bps / MIB)
    } else if bps >= KIB {
        format!("{:.1} KiB/s", bps / KIB)
    } else {
        format!("{bps:.0} B/s")
    }
}

fn draw_nodes_page(
    frame: &mut Frame,
    area: TuiRect,
    s: &StatusSnapshot,
    now: u64,
    selected: usize,
) {
    let [progress_area, table_area, detail_area] = vertical_chunks(
        area,
        [
            Constraint::Length(4),
            Constraint::Percentage(58),
            Constraint::Percentage(42),
        ],
    );
    draw_connected_progress(frame, progress_area, s);
    draw_nodes_table(frame, table_area, s, selected);
    draw_node_details(frame, detail_area, s, now, selected);
}

/// `Connected` progress bar + cold-start timing line (one bordered box, `antctl top`).
fn draw_connected_progress(frame: &mut Frame, area: TuiRect, s: &StatusSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default().bg(Color::Blue));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [bar_area, time_area] =
        vertical_chunks(inner, [Constraint::Length(1), Constraint::Length(1)]);

    let limit = s.peers.node_limit.max(1);
    let connected = ready_count_for_gauge(s).min(limit);
    let ratio = f64::from(connected) / f64::from(limit);
    draw_gauge_line(frame, bar_area, &progress_label(connected, limit), ratio);

    let m = peer_milestone_line(s);
    let time_line = if m.is_empty() {
        Line::default()
    } else {
        Line::from(Span::styled(
            format!(" {m} "),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ))
    };
    frame.render_widget(
        Paragraph::new(time_line).style(Style::default().bg(Color::Blue).fg(Color::White)),
        time_area,
    );
}

/// Populated as milestones occur (`antd` p2p loop); only non-empty `Option`s are shown.
fn peer_milestone_line(s: &StatusSnapshot) -> String {
    let n = s.peers.node_limit.max(1);
    let mut parts = Vec::new();
    if let Some(t) = s.peers.time_to_first_peer_s {
        parts.push(format!("Time to first peer: {t:.3}s"));
    }
    if let Some(t) = s.peers.time_to_node_limit_s {
        parts.push(format!("Time to {n} nodes: {t:.3}s"));
    }
    parts.join("  ")
}

fn draw_gauge_line(frame: &mut Frame, area: TuiRect, label: &str, ratio: f64) {
    let prefix = format!(" Connected: {label} ");
    let bar_width = area.width.saturating_sub(prefix.chars().count() as u16 + 3) as usize;
    let filled = ((bar_width as f64 * ratio).round() as usize).min(bar_width);
    let empty = bar_width - filled;
    let line = Line::from(vec![
        Span::styled(
            prefix,
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "[",
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "█".repeat(filled),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            " ".repeat(empty),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "] ",
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(Color::Blue)),
        area,
    );
}

fn progress_label(connected: u32, limit: u32) -> String {
    let width = limit.to_string().len();
    format!("{connected:0width$}/{limit}")
}

fn draw_disconnected_nodes_page(frame: &mut Frame, area: TuiRect, error: &str) {
    let [progress_area, table_area, detail_area] = vertical_chunks(
        area,
        [
            Constraint::Length(4),
            Constraint::Percentage(58),
            Constraint::Percentage(42),
        ],
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default().bg(Color::Blue));
    let inner = block.inner(progress_area);
    frame.render_widget(block, progress_area);
    let [bar_area, time_area] =
        vertical_chunks(inner, [Constraint::Length(1), Constraint::Length(1)]);
    draw_gauge_line(frame, bar_area, "000/???", 0.0);
    let waiting = Line::from(Span::styled(
        " (waiting for antd) ",
        Style::default()
            .bg(Color::Blue)
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(
        Paragraph::new(waiting).style(Style::default().bg(Color::Blue).fg(Color::White)),
        time_area,
    );
    draw_empty_nodes_table(frame, table_area);
    let rows = [
        kv("Daemon", "disconnected"),
        kv("Nodes", "(waiting for antd)"),
        kv("Error", error),
    ];
    draw_panel(frame, detail_area, "Details", &rows);
}

const NODES_HEADER: [&str; 7] = [
    "Peer ID", "State", "Ready in", "IP", "Type", "Agent", "Version",
];
/// Gap between adjacent columns; `column_spacing(2)` so each column gets
/// exactly two spaces of breathing room before the next one starts.
const NODES_COLUMN_SPACING: u16 = 2;
/// Fixed column widths picked once at startup so the table doesn't reflow
/// between frames. Each width is `max(header, an upper bound on the longest
/// cell ever rendered)`:
///
/// * `Peer ID` — `short_peer_id` always emits `XXXX…YYYY` (9 chars); 10
///   leaves room for the `(none yet)` placeholder when the daemon has no
///   peers yet.
/// * `State` — longest pipeline label is `Identifying`/`Handshaking` (11).
/// * `Ready in` — `format_ready_in_ms` emits `<999 ms` (3 ascii digits +
///   space + unit) or `<99.9 s` (4 ascii chars + space + unit), so 8
///   covers both plus the header.
/// * `IP` — fits IPv4 `255.255.255.255` (15); rarer IPv6 / DNS values clip.
/// * `Type` — `light` is the longest of `full` / `light` / `-` (5).
/// * `Agent` — `bee` is 3; allow up to 8 for future clients without reflow.
/// * `Version` — `10.20.30` (8) covers any plausible semver core.
const NODES_COLUMN_WIDTHS: [u16; 7] = [10, 11, 8, 15, 5, 8, 8];

fn nodes_table_block() -> Block<'static> {
    Block::default()
        .title(" Nodes ")
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue))
}

fn nodes_header_row() -> Row<'static> {
    Row::new(NODES_HEADER.to_vec()).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )
}

const fn node_kind_label(full: Option<bool>) -> &'static str {
    match full {
        Some(true) => "full",
        Some(false) => "light",
        None => "-",
    }
}

/// Split a libp2p Identify `agent_version` into a short name and a clean
/// semver. Bee advertises `bee/2.7.1-61fab37b go1.25.2 linux/amd64`, so we
/// take the part before the first `/` as the agent name and the leading
/// numeric-dot run after it as the version (dropping the `-<gitsha>` and any
/// `go<x>/<arch>` trailers).
fn parse_agent_version(raw: &str) -> (String, String) {
    if raw.is_empty() {
        return (String::new(), String::new());
    }
    let (name, rest) = match raw.split_once('/') {
        Some((n, r)) => (n.trim(), r),
        None => return (raw.trim().to_string(), String::new()),
    };
    let version_field = rest.split_whitespace().next().unwrap_or("");
    let version: String = version_field
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let version = version.trim_end_matches('.').to_string();
    let version = if version.is_empty() {
        version_field.to_string()
    } else {
        version
    };
    (name.to_string(), version)
}

fn dash_if_empty(s: &str) -> String {
    if s.is_empty() {
        "-".to_string()
    } else {
        s.to_string()
    }
}

const fn nodes_constraints() -> [Constraint; 7] {
    [
        Constraint::Length(NODES_COLUMN_WIDTHS[0]),
        Constraint::Length(NODES_COLUMN_WIDTHS[1]),
        Constraint::Length(NODES_COLUMN_WIDTHS[2]),
        Constraint::Length(NODES_COLUMN_WIDTHS[3]),
        Constraint::Length(NODES_COLUMN_WIDTHS[4]),
        Constraint::Length(NODES_COLUMN_WIDTHS[5]),
        Constraint::Length(NODES_COLUMN_WIDTHS[6]),
    ]
}

fn draw_empty_nodes_table(frame: &mut Frame, area: TuiRect) {
    let rows = vec![Row::new(vec![
        "(waiting…)".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
    ])];
    let table = Table::new(rows, nodes_constraints())
        .header(nodes_header_row())
        .column_spacing(NODES_COLUMN_SPACING)
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .block(nodes_table_block());
    frame.render_widget(table, area);
}

fn draw_nodes_table(frame: &mut Frame, area: TuiRect, s: &StatusSnapshot, selected: usize) {
    let visible_rows = area.height.saturating_sub(3).max(1) as usize;
    let total = peer_list_len(s);
    let selected = clamp_selection(selected, total);
    let start = if total <= visible_rows {
        0
    } else if selected >= visible_rows {
        selected + 1 - visible_rows
    } else {
        0
    };
    let end = (start + visible_rows).min(total);

    // Pipeline rows carry only peer_id / state / ip; type and agent live on
    // `connected_peers`, so index by id once and look up per row.
    let by_id: HashMap<&str, &PeerConnectionInfo> = s
        .peers
        .connected_peers
        .iter()
        .map(|p| (p.peer_id.as_str(), p))
        .collect();

    let cells: Vec<Vec<String>> = if total == 0 {
        vec![vec![
            "(none yet)".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ]]
    } else if !s.peers.peer_pipeline.is_empty() {
        let order = pipeline_view_order(s);
        (start..end)
            .map(|idx| {
                let p = &s.peers.peer_pipeline[order[idx]];
                let conn = by_id.get(p.peer_id.as_str()).copied();
                let (agent, version) =
                    parse_agent_version(conn.map_or("", |c| c.agent_version.as_str()));
                let ready_in = match p.ready_in_ms {
                    Some(ms) => format_ready_in_ms(ms),
                    None => "-".to_string(),
                };
                vec![
                    short_peer_id(&p.peer_id),
                    pipeline_state_label(p.state).to_string(),
                    ready_in,
                    dash_if_empty(&p.ip),
                    node_kind_label(conn.and_then(|c| c.full_node)).to_string(),
                    dash_if_empty(&agent),
                    dash_if_empty(&version),
                ]
            })
            .collect()
    } else {
        (start..end)
            .map(|idx| {
                let peer = &s.peers.connected_peers[idx];
                let status = if peer.bzz_overlay.is_some() {
                    "Ready"
                } else {
                    "Handshaking"
                };
                let (agent, version) = parse_agent_version(&peer.agent_version);
                vec![
                    short_peer_id(&peer.peer_id),
                    status.to_string(),
                    "-".to_string(),
                    status_addr_ip(peer),
                    node_kind_label(peer.full_node).to_string(),
                    dash_if_empty(&agent),
                    dash_if_empty(&version),
                ]
            })
            .collect()
    };

    let highlight = Style::default()
        .bg(Color::Cyan)
        .fg(Color::Black)
        .add_modifier(Modifier::BOLD);

    let rows: Vec<Row> = cells
        .into_iter()
        .enumerate()
        .map(|(i, c)| {
            // `i` indexes into the visible window; offset by `start` to map
            // back onto the absolute selection cursor.
            let row = Row::new(c);
            if total > 0 && start + i == selected {
                row.style(highlight)
            } else {
                row
            }
        })
        .collect();

    let table = Table::new(rows, nodes_constraints())
        .header(nodes_header_row())
        .column_spacing(NODES_COLUMN_SPACING)
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .block(nodes_table_block());

    frame.render_widget(table, area);
}

fn draw_node_details(
    frame: &mut Frame,
    area: TuiRect,
    s: &StatusSnapshot,
    now: u64,
    selected: usize,
) {
    let n = peer_list_len(s);
    let selected = clamp_selection(selected, n);
    let rows: Vec<PanelRow> = if n == 0 {
        vec![kv("Node", "(none selected)")]
    } else if !s.peers.peer_pipeline.is_empty() {
        let order = pipeline_view_order(s);
        let row_idx = order.get(selected).copied();
        match row_idx.and_then(|i| s.peers.peer_pipeline.get(i)) {
            Some(row) => {
                let full = s
                    .peers
                    .connected_peers
                    .iter()
                    .find(|p| p.peer_id == row.peer_id);
                let mut rows = vec![
                    kv("Peer ID", &row.peer_id),
                    kv("State", pipeline_state_label(row.state)),
                    kv(
                        "Ready in",
                        &row.ready_in_ms
                            .map_or_else(|| "-".to_string(), format_ready_in_ms),
                    ),
                    kv(
                        "IP",
                        if row.ip.is_empty() {
                            "-"
                        } else {
                            row.ip.as_str()
                        },
                    ),
                ];
                if let Some(peer) = full {
                    rows.extend(detail_rows_from_connection(peer, now));
                }
                rows
            }
            None => vec![kv("Node", "(none selected)")],
        }
    } else {
        match s.peers.connected_peers.get(selected) {
            Some(peer) => {
                let mut r = vec![
                    kv("Peer ID", &peer.peer_id),
                    kv(
                        "State",
                        if peer.bzz_overlay.is_some() {
                            "Ready"
                        } else {
                            "Handshaking"
                        },
                    ),
                    kv("IP", &status_addr_ip(peer)),
                ];
                r.extend(detail_rows_from_connection(peer, now));
                r
            }
            None => vec![kv("Node", "(none selected)")],
        }
    };
    draw_panel(frame, area, "Details", &rows);
}

fn status_addr_ip(peer: &PeerConnectionInfo) -> String {
    extract_ip_from_multiaddr_str(&peer.address)
}

fn extract_ip_from_multiaddr_str(a: &str) -> String {
    const PREFIXES: &[&str] = &["/ip4/", "/ip6/", "/dns4/", "/dns6/", "/dnsaddr/"];
    let earliest = PREFIXES
        .iter()
        .filter_map(|p| a.find(p).map(|i| (i, *p)))
        .min_by_key(|&(i, _)| i);
    if let Some((start, prefix)) = earliest {
        let after = &a[start + prefix.len()..];
        let end = after.find('/').unwrap_or(after.len());
        if end > 0 {
            return after[..end].to_string();
        }
    }
    "-".to_string()
}

fn detail_rows_from_connection(peer: &PeerConnectionInfo, now: u64) -> Vec<PanelRow> {
    let mut rows = vec![
        kv("Direction", &peer.direction),
        kv("Address", &peer.address),
        kv("Agent", unknown_if_empty(&peer.agent_version)),
        kv(
            "Connected",
            &format_duration(now.saturating_sub(peer.connected_at_unix)),
        ),
    ];
    if let Some(overlay) = &peer.bzz_overlay {
        rows.push(kv("Overlay", overlay));
    }
    if let Some(full_node) = peer.full_node {
        rows.push(kv(
            "Kind",
            if full_node { "full-node" } else { "light-node" },
        ));
    }
    if let Some(at) = peer.last_bzz_at_unix {
        rows.push(kv(
            "BZZ age",
            &format!("{} ago", format_duration(now.saturating_sub(at))),
        ));
    }
    rows
}

const fn unknown_if_empty(value: &str) -> &str {
    if value.is_empty() {
        "(unknown)"
    } else {
        value
    }
}

fn vertical_chunks<const N: usize>(area: TuiRect, constraints: [Constraint; N]) -> [TuiRect; N] {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area)
        .as_ref()
        .try_into()
        .expect("layout produced requested number of chunks")
}

struct PanelRow {
    key: String,
    value: String,
    color: Color,
}

fn kv(key: &str, value: &str) -> PanelRow {
    PanelRow {
        key: key.to_string(),
        value: value.to_string(),
        color: Color::White,
    }
}

fn fit(text: &str, width: usize) -> String {
    let mut out: String = text.chars().take(width).collect();
    if out.chars().count() < width {
        out.push_str(&" ".repeat(width - out.chars().count()));
    } else if text.chars().count() > width && width > 0 {
        out.pop();
        out.push('~');
    }
    out
}

fn current_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn format_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(chunks: u64, total_chunks: u64, bytes: u64, total_bytes: u64) -> GetProgress {
        GetProgress {
            elapsed_ms: 250,
            chunks_done: chunks,
            total_chunks_estimate: total_chunks,
            bytes_done: bytes,
            total_bytes_estimate: total_bytes,
            cache_hits: 0,
            peers_used: 0,
            bypass_cache: false,
            in_flight: 0,
        }
    }

    #[test]
    fn stats_summary_formats_completed_cold_fetch() {
        let mut s = sample(5, 5, 18 * 1024, 18 * 1024);
        s.peers_used = 3;
        s.bypass_cache = true;
        let line = format_stats_summary(&s, true);
        assert_eq!(
            line,
            "stats: 5 chunks (18.0KiB) in 0.2s · avg 72.0KiB/s · 3 peers · 0 cache · cold",
        );
    }

    #[test]
    fn stats_summary_formats_warm_cache_hit() {
        let mut s = sample(3, 3, 12 * 1024, 12 * 1024);
        s.cache_hits = 3;
        let line = format_stats_summary(&s, false);
        assert!(line.contains("0 peers"), "got: {line}");
        assert!(line.contains("3 cache"), "got: {line}");
        assert!(line.ends_with("warm"), "got: {line}");
    }

    #[test]
    fn stats_summary_renders_partial_progress_on_error() {
        let s = sample(2, 9, 8 * 1024, 36 * 1024);
        let line = format_stats_summary(&s, false);
        assert!(line.starts_with("stats: 2/9 chunks"), "got: {line}");
    }

    #[test]
    fn progress_line_marks_unknown_total_with_question_mark() {
        let s = sample(1, 0, 4 * 1024, 0);
        let line = format_progress_line(&s, None, false);
        assert!(line.contains("1/?"), "got: {line}");
        assert!(line.contains("4.0KiB/?"), "got: {line}");
    }

    #[test]
    fn visual_progress_line_draws_block_map() {
        let previous = sample(3, 10, 12 * 1024, 40 * 1024);
        let mut current = sample(5, 10, 20 * 1024, 40 * 1024);
        current.peers_used = 2;
        current.cache_hits = 1;

        let (bar, stats) = format_visual_progress_lines_for_width(
            &current,
            Some(&previous),
            Some(8.0 * 1024.0),
            false,
            80,
        );
        assert!(bar.contains('▣'), "got: {bar}");
        assert!(bar.contains('▓'), "got: {bar}");
        assert!(bar.contains('░'), "got: {bar}");
        assert!(stats.contains("50.0%"), "got: {stats}");
        assert!(stats.contains("5/10ch"), "got: {stats}");
        assert!(stats.contains("2p/1c"), "got: {stats}");
    }

    #[test]
    fn visual_progress_line_handles_unknown_total() {
        let s = sample(1, 0, 4 * 1024, 0);
        let (bar, stats) = format_visual_progress_lines_for_width(&s, None, None, true, 80);
        assert!(bar.starts_with('['), "got: {bar}");
        assert!(stats.starts_with("[no-cache] "), "got: {stats}");
        assert!(stats.contains("?.?%"), "got: {stats}");
        assert!(stats.contains("1/?ch"), "got: {stats}");
    }

    /// Render the Retrieval-tab metrics panel against a `TestBackend`
    /// and assert the disk-cache row reflects the wire snapshot. The
    /// regression we're guarding against here: a refactor that drops
    /// the `Disk cache` row, hides the byte / hit gauges, or — most
    /// subtly — silently uses `cache.used` (chunk slots) instead of
    /// `disk.used_bytes` when formatting the disk fill. Driving the
    /// real renderer (rather than a string-only helper) keeps that
    /// honest because the layout, label, and value formatting all
    /// live in `draw_retrieval_metrics`.
    #[test]
    fn retrieval_metrics_panel_shows_disk_cache_row_when_enabled() {
        use ant_control::{CacheInfo, DiskCacheInfo, RetrievalInfo};
        use ratatui::{backend::TestBackend, Terminal};

        let info = RetrievalInfo {
            cache: CacheInfo {
                used: 4096,
                capacity: 8192,
            },
            in_flight: 12,
            in_flight_capacity: 256,
            chunks_fetched_total: 1_700_000,
            bytes_fetched_total: 6_800_000_000,
            cache_hits_total: 1_700_000,
            mem_hits_total: 1_213_000,
            disk: DiskCacheInfo {
                enabled: true,
                used_bytes: 13_200_000_000,      // ~12.3 GiB
                capacity_bytes: 107_374_182_400, // 100 GiB
                hits_total: 487_000,
            },
            gateway_requests: Vec::new(),
        };
        let bw = BandwidthTracker::new();

        let backend = TestBackend::new(120, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = TuiRect {
                    x: 0,
                    y: 0,
                    width: 120,
                    height: 9,
                };
                draw_retrieval_metrics(frame, area, &info, &bw);
            })
            .unwrap();

        let buf = terminal.backend().buffer().clone();
        let rendered: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol().chars().next().unwrap_or(' '))
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Both rows render with their tier-specific labels and values.
        // We assert on substrings (not exact frames) so the test isn't
        // brittle against ratatui's whitespace-padding tweaks.
        assert!(
            rendered.contains("Mem cache"),
            "Mem cache row missing in:\n{rendered}",
        );
        assert!(
            rendered.contains("4096/8192 chunks"),
            "memory fill not rendered correctly in:\n{rendered}",
        );
        assert!(
            rendered.contains("1.2M hits"),
            "memory hit count not rendered in:\n{rendered}",
        );
        assert!(
            rendered.contains("Disk cache"),
            "Disk cache row missing in:\n{rendered}",
        );
        assert!(
            rendered.contains("12.3GiB / 100.0GiB"),
            "disk byte fill not rendered correctly in:\n{rendered}",
        );
        assert!(
            rendered.contains("487.0K hits"),
            "disk hit count not rendered in:\n{rendered}",
        );
        // Sanity: we shouldn't have the disabled label when the
        // cache is wired up. Catches a regression that flips the
        // ternary backwards.
        assert!(
            !rendered.contains("disabled"),
            "wired-up cache must not render the disabled label:\n{rendered}",
        );
    }

    /// Mirror of the previous test for the `--no-disk-cache` path:
    /// `enabled = false` must produce a clean `disabled` label rather
    /// than rendering `0B / 0B (0%)`. This is the operator's "is
    /// tier 2 wired up at all?" signal.
    #[test]
    fn retrieval_metrics_panel_labels_disabled_disk_cache() {
        use ant_control::{CacheInfo, DiskCacheInfo, RetrievalInfo};
        use ratatui::{backend::TestBackend, Terminal};

        let info = RetrievalInfo {
            cache: CacheInfo {
                used: 0,
                capacity: 8192,
            },
            in_flight: 0,
            in_flight_capacity: 256,
            chunks_fetched_total: 0,
            bytes_fetched_total: 0,
            cache_hits_total: 0,
            mem_hits_total: 0,
            disk: DiskCacheInfo::default(), // enabled = false
            gateway_requests: Vec::new(),
        };
        let bw = BandwidthTracker::new();

        let backend = TestBackend::new(120, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = TuiRect {
                    x: 0,
                    y: 0,
                    width: 120,
                    height: 9,
                };
                draw_retrieval_metrics(frame, area, &info, &bw);
            })
            .unwrap();

        let buf = terminal.backend().buffer().clone();
        let rendered: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol().chars().next().unwrap_or(' '))
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            rendered.contains("Disk cache"),
            "Disk cache row missing in:\n{rendered}",
        );
        assert!(
            rendered.contains("disabled"),
            "disabled disk cache must render the 'disabled' label:\n{rendered}",
        );
        // The byte gauge must NOT render when the cache is off —
        // a `0B / 0B` row would mislead operators into thinking
        // the cache exists but is empty.
        assert!(
            !rendered.contains("0B / 0B"),
            "disabled cache must not render a fake 0B/0B fill:\n{rendered}",
        );
    }

    #[test]
    fn parse_addr_round_trip() {
        let want = [0xab; 20];
        let s = format!("0x{}", hex::encode(want));
        assert_eq!(super::parse_addr(&s).unwrap(), want);
        // No-prefix variant.
        assert_eq!(super::parse_addr(&hex::encode(want)).unwrap(), want);
    }

    #[test]
    fn parse_addr_rejects_short_input() {
        assert!(super::parse_addr("0x1234").is_err());
    }

    #[test]
    fn parse_word32_round_trip() {
        let want = [0xcd; 32];
        let s = format!("0x{}", hex::encode(want));
        assert_eq!(super::parse_word32(&s).unwrap(), want);
    }

    #[test]
    fn random_word32_is_unique() {
        let a = super::random_word32();
        let b = super::random_word32();
        assert_ne!(
            a, b,
            "random_word32 collisions imply both nanos+urandom failed"
        );
    }

    #[test]
    fn format_eth_18_handles_round_amounts() {
        // 1 xDAI exactly.
        assert_eq!(super::format_eth_18(1_000_000_000_000_000_000), "1 xDAI");
        // 0.5 xDAI.
        assert_eq!(super::format_eth_18(500_000_000_000_000_000), "0.5 xDAI",);
        // Truncates to 6 fractional digits, no rounding.
        assert_eq!(
            super::format_eth_18(123_456_789_012_345_678),
            "0.123456 xDAI",
        );
    }

    #[test]
    fn format_upload_status_known_states() {
        assert_eq!(super::format_upload_status_label("running"), "running");
        assert_eq!(super::format_upload_status_label("completed"), "completed");
        assert_eq!(super::format_upload_status_label("paused"), "paused");
        // Unknown values fall through unchanged so a future status
        // string from a newer daemon doesn't render as `?`.
        assert_eq!(super::format_upload_status_label("future"), "future");
    }

    #[test]
    fn format_eth_16_handles_bzz() {
        // 1 BZZ.
        assert_eq!(super::format_eth_16(10_000_000_000_000_000), "1 BZZ");
        // The live storage-stamp wallet had 42_849_794_328_341_404 PLUR.
        // Expected display: 4.284979 BZZ (truncated, no rounding).
        assert_eq!(super::format_eth_16(42_849_794_328_341_404), "4.284979 BZZ",);
    }
}
