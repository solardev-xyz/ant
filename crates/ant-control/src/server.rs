//! Tokio-based UDS server. Reads one NDJSON request, writes one or more
//! NDJSON responses, closes.
//!
//! The wire is single-shot for almost every request: one [`Request`] line
//! in, one [`Response`] line out, EOF. The exception is a `Get*` request
//! with `progress: true` — the daemon may emit zero or more
//! [`Response::Progress`] lines before the terminal response, all on the
//! same connection. Old clients that don't ask for progress only ever
//! see the terminal line, so the v1 single-shot framing is preserved.

use crate::protocol::{
    GetProgress, Request, Response, StatusSnapshot, VersionInfo, PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// How long to wait for the node loop to acknowledge a near-instant
/// command (e.g. `PeersReset`). If the node loop is wedged we don't want
/// to hang the socket handler indefinitely.
const FAST_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap for commands that may take a network round-trip (retrieval). Sized
/// to comfortably cover `ant_retrieval::retrieve_chunk`'s own 10 s
/// timeout plus per-attempt overhead, without hanging the socket forever
/// if the node loop wedges.
const NETWORK_COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
/// Cap for commands that walk a chunk tree (joiner / mantaray): every
/// node and every leaf is one round-trip. A 1 MB file is ~256 leaves +
/// 2-3 intermediates; a manifest with one fork adds one more fetch on
/// top. Real-world chunk fetches usually complete in <1 s, but pad for
/// the unlucky path.
const TREE_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
/// Buffer depth for the streaming ack channel. Sized big enough to
/// hold a couple of progress emissions plus the terminal ack without
/// the producer ever blocking, and small enough that a stalled writer
/// is observable in memory pressure rather than runaway buffering.
const STREAM_ACK_CAPACITY: usize = 16;

/// A mutating or network-touching command issued by a client and forwarded
/// to the node loop.
///
/// The node loop owns state that needs a single-writer path (the peerstore,
/// the swarm), so `serve()` does not touch it directly; instead it relays a
/// `ControlCommand` through an `mpsc::Sender` and awaits the ack(s) on the
/// embedded reply channel. Non-streaming commands (peerstore reset, single-
/// chunk retrieve) carry a `oneshot` — exactly one ack lands and the
/// channel closes. Streaming-capable commands (`GetBytes`, `GetBzz`)
/// carry an `mpsc::Sender` that produces zero or more
/// [`ControlAck::Progress`] samples followed by exactly one terminal
/// variant (`Bytes` / `BzzBytes` / `Error`).
#[derive(Debug)]
pub enum ControlCommand {
    /// Drop the on-disk peerstore snapshot and clear the in-memory dedup
    /// state. Does not disconnect current peers.
    ResetPeerstore { ack: oneshot::Sender<ControlAck> },
    /// Retrieve a chunk by 32-byte reference. The node loop picks the
    /// closest BZZ peer, runs `ant_retrieval::retrieve_chunk`, and acks
    /// with the verified payload bytes (or an error string).
    GetChunk {
        reference: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
    /// Like `GetChunk`, but the ack carries the chunk's full **wire
    /// bytes** (`span (8 LE) || payload`) instead of just the payload.
    /// Used by `ant-gateway` to serve `/chunks/{addr}` in a bee-shaped
    /// way (bee's `chunkstore.Get` returns `Chunk.Data()` which is the
    /// wire form). Routing / peer-pick / CAC verification logic is
    /// identical to `GetChunk`; the only difference is what the node
    /// loop puts in `ControlAck::Bytes::data`.
    GetChunkRaw {
        reference: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
    /// Walk the manifest at `reference`, resolve `path`, then join the
    /// resulting chunk tree into a single `Vec<u8>`. Acks with
    /// [`ControlAck::BzzBytes`] including any `Content-Type` metadata
    /// recovered from the manifest. When `allow_degraded_redundancy`
    /// is set the joiner masks any non-zero RS level byte off the
    /// file's root span and decodes without RS recovery.
    /// `bypass_cache` swaps the daemon's shared chunk cache for a
    /// fresh per-request one (intra-request retries still benefit
    /// from caching). `progress` opts the request into streaming
    /// [`ControlAck::Progress`] emissions. `max_bytes` overrides the
    /// joiner's per-request size cap; `None` means "use the joiner's
    /// `DEFAULT_MAX_FILE_BYTES` (32 MiB)" — appropriate for `antctl get`
    /// where allocating gigabytes from a malformed root chunk would be
    /// a footgun. The HTTP gateway raises this so real bzz sites with
    /// videos / archives don't 502 at the joiner step.
    GetBzz {
        reference: [u8; 32],
        path: String,
        allow_degraded_redundancy: bool,
        bypass_cache: bool,
        progress: bool,
        max_bytes: Option<u64>,
        ack: mpsc::Sender<ControlAck>,
    },
    /// List the paths and metadata in a mantaray manifest. Gateway-only
    /// helper for the `/manifest/{addr}` inspection endpoint.
    ListBzz {
        reference: [u8; 32],
        bypass_cache: bool,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Join the multi-chunk tree rooted at `reference` (a `/bytes/` ref,
    /// no manifest) and ack with the joined file bytes. See
    /// [`ControlCommand::GetBzz`] for `bypass_cache`, `progress`, and
    /// `max_bytes`.
    GetBytes {
        reference: [u8; 32],
        bypass_cache: bool,
        progress: bool,
        max_bytes: Option<u64>,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Gateway-only streaming `/bytes/{ref}` path. Sends
    /// [`ControlAck::BytesStreamStart`], zero or more
    /// [`ControlAck::BytesChunk`] messages, then [`ControlAck::StreamDone`]
    /// or [`ControlAck::Error`]. The JSON control socket does not expose
    /// this command; it exists so `ant-gateway` can behave like Bee and
    /// write the HTTP body while the joiner is still retrieving.
    ///
    /// `range` is interpreted against the file body (the joined bytes,
    /// not chunk wire bytes). `None` streams the whole file. The
    /// daemon clamps `range.end_inclusive` to `total_bytes - 1`.
    /// `head_only` short-circuits the body retrieval entirely: the
    /// daemon emits a `BytesStreamStart` with the file size and goes
    /// straight to `StreamDone`. This is what backs `HEAD /bytes/{addr}`
    /// without joining the chunk tree.
    StreamBytes {
        reference: [u8; 32],
        bypass_cache: bool,
        max_bytes: Option<u64>,
        range: Option<StreamRange>,
        head_only: bool,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Gateway-only streaming `/bzz/{ref}/{path}` path. Resolves the
    /// manifest, then streams the file body the same way `StreamBytes`
    /// does. Sends [`ControlAck::BzzStreamStart`] (with content type +
    /// filename + total size), zero or more
    /// [`ControlAck::BytesChunk`] messages, then
    /// [`ControlAck::StreamDone`] or [`ControlAck::Error`].
    ///
    /// `range` and `head_only` behave identically to `StreamBytes`.
    /// `head_only` is what backs `HEAD /bzz/{ref}/{path}`: the daemon
    /// resolves the manifest, fetches the data root chunk to learn
    /// the size, returns metadata + total span, and never joins.
    StreamBzz {
        reference: [u8; 32],
        path: String,
        allow_degraded_redundancy: bool,
        bypass_cache: bool,
        max_bytes: Option<u64>,
        range: Option<StreamRange>,
        head_only: bool,
        ack: mpsc::Sender<ControlAck>,
    },
    /// Gateway `POST /chunks`: stamp locally (postage issuer) then pushsync.
    PushChunk {
        wire: Vec<u8>,
        ack: oneshot::Sender<ControlAck>,
    },
}

/// Inclusive byte range used by [`ControlCommand::StreamBytes`] and
/// [`ControlCommand::StreamBzz`]. Mirrors
/// [`ant_retrieval::ByteRange`] but kept local so `ant-control` doesn't
/// take a build-graph dependency on the retrieval crate.
#[derive(Debug, Clone, Copy)]
pub struct StreamRange {
    pub start: u64,
    pub end_inclusive: u64,
}

/// Node-loop reply to a [`ControlCommand`]. Serialized back to the client as
/// `Response::Ok`, `Response::Bytes`, `Response::BzzBytes`,
/// `Response::Progress`, or `Response::Error` depending on the variant.
#[derive(Debug, Clone)]
pub enum ControlAck {
    Ok {
        message: String,
    },
    Bytes {
        data: Vec<u8>,
    },
    BzzBytes {
        data: Vec<u8>,
        content_type: Option<String>,
        filename: Option<String>,
    },
    Manifest {
        entries: Vec<ManifestEntryInfo>,
    },
    /// Streaming progress sample for a `GetBytes` / `GetBzz` request.
    /// Producer can fire as many of these as it likes; the dispatcher
    /// keeps writing them until a terminal variant arrives or the
    /// channel closes.
    Progress(GetProgress),
    BytesStreamStart {
        total_bytes: u64,
    },
    /// Streaming-bzz prologue: emitted by `StreamBzz` once the manifest
    /// has resolved and the data root chunk has been fetched (so the
    /// total span is known). Followed by zero or more `BytesChunk`s
    /// and a terminal `StreamDone` / `Error`.
    BzzStreamStart {
        total_bytes: u64,
        content_type: Option<String>,
        filename: Option<String>,
    },
    BytesChunk {
        data: Vec<u8>,
    },
    StreamDone,
    /// Successful `PushChunk`; reference is lowercase `0x` + 64 nibbles (bee-compatible).
    ChunkUploaded {
        reference: String,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntryInfo {
    pub path: String,
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl ControlAck {
    fn is_terminal(&self) -> bool {
        !matches!(
            self,
            ControlAck::Progress(_)
                | ControlAck::BytesStreamStart { .. }
                | ControlAck::BzzStreamStart { .. }
                | ControlAck::BytesChunk { .. }
        )
    }
}

/// Build the `mpsc` ack channel handed to streaming-capable
/// `ControlCommand`s. Public so the node loop's tests can wire one up
/// without reaching into the server module's internals.
pub fn streaming_ack_channel() -> (mpsc::Sender<ControlAck>, mpsc::Receiver<ControlAck>) {
    mpsc::channel(STREAM_ACK_CAPACITY)
}

/// Serve control requests on `socket_path` until the task is cancelled.
///
/// `status_rx` is the live status view maintained by the node loop; each
/// request call `.borrow()` on it and replies with the current snapshot.
///
/// `command_tx` is the single-producer channel into the node loop for
/// mutating requests (e.g. `Request::PeersReset`). `None` means mutating
/// commands will reply with `Response::Error`.
///
/// `agent` is the daemon agent string (e.g. `"antd/0.1.0"`), returned by
/// `Request::Version` and overriding `snapshot.agent` in case the sender
/// shipped an older value.
pub async fn serve(
    socket_path: PathBuf,
    agent: String,
    status_rx: watch::Receiver<StatusSnapshot>,
    command_tx: Option<mpsc::Sender<ControlCommand>>,
) -> Result<(), ServerError> {
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = UnixListener::bind(&socket_path)?;
    restrict_socket_permissions(&socket_path)?;

    debug!(target: "ant_control", "control socket listening at {}", socket_path.display());

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let rx = status_rx.clone();
                let agent = agent.clone();
                let cmd_tx = command_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, agent, rx, cmd_tx).await {
                        warn!(target: "ant_control", "control request failed: {e}");
                    }
                });
            }
            Err(e) => {
                warn!(target: "ant_control", "accept failed: {e}");
            }
        }
    }
}

async fn handle_connection(
    stream: UnixStream,
    agent: String,
    status_rx: watch::Receiver<StatusSnapshot>,
    command_tx: Option<mpsc::Sender<ControlCommand>>,
) -> Result<(), ServerError> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).take(MAX_REQUEST_BYTES as u64);
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(());
    }
    let trimmed = line.trim_end_matches(['\r', '\n']);

    match serde_json::from_str::<Request>(trimmed) {
        Ok(Request::Status) => {
            let snap = status_rx.borrow().clone();
            write_response(&mut write_half, &Response::Status(Box::new(snap))).await?;
        }
        Ok(Request::Version) => {
            write_response(
                &mut write_half,
                &Response::Version(VersionInfo {
                    agent,
                    protocol_version: PROTOCOL_VERSION,
                }),
            )
            .await?;
        }
        Ok(Request::PeersReset) => {
            let response = dispatch_oneshot(command_tx.as_ref(), FAST_COMMAND_TIMEOUT, |ack| {
                ControlCommand::ResetPeerstore { ack }
            })
            .await;
            write_response(&mut write_half, &response).await?;
        }
        Ok(Request::Get {
            reference,
            bypass_cache: _,
            progress: _,
        }) => match parse_reference(&reference) {
            Ok(addr) => {
                let response =
                    dispatch_oneshot(command_tx.as_ref(), NETWORK_COMMAND_TIMEOUT, |ack| {
                        ControlCommand::GetChunk {
                            reference: addr,
                            ack,
                        }
                    })
                    .await;
                write_response(&mut write_half, &response).await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad reference: {e}"),
                    },
                )
                .await?;
            }
        },
        Ok(Request::GetBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache,
            progress,
        }) => match parse_reference(&reference) {
            Ok(addr) => {
                dispatch_streaming(
                    &mut write_half,
                    command_tx.as_ref(),
                    TREE_COMMAND_TIMEOUT,
                    |ack| ControlCommand::GetBzz {
                        reference: addr,
                        path,
                        allow_degraded_redundancy,
                        bypass_cache,
                        progress,
                        // CLI keeps the conservative joiner default; the
                        // HTTP gateway opts in to a higher ceiling for
                        // real-site downloads.
                        max_bytes: None,
                        ack,
                    },
                )
                .await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad reference: {e}"),
                    },
                )
                .await?;
            }
        },
        Ok(Request::GetBytes {
            reference,
            bypass_cache,
            progress,
        }) => match parse_reference(&reference) {
            Ok(addr) => {
                dispatch_streaming(
                    &mut write_half,
                    command_tx.as_ref(),
                    TREE_COMMAND_TIMEOUT,
                    |ack| ControlCommand::GetBytes {
                        reference: addr,
                        bypass_cache,
                        progress,
                        max_bytes: None,
                        ack,
                    },
                )
                .await?;
            }
            Err(e) => {
                write_response(
                    &mut write_half,
                    &Response::Error {
                        message: format!("bad reference: {e}"),
                    },
                )
                .await?;
            }
        },
        Err(e) => {
            write_response(
                &mut write_half,
                &Response::Error {
                    message: format!("bad request: {e}"),
                },
            )
            .await?;
        }
    }

    write_half.flush().await?;
    write_half.shutdown().await?;
    Ok(())
}

async fn write_response<W>(write_half: &mut W, response: &Response) -> Result<(), ServerError>
where
    W: AsyncWriteExt + Unpin,
{
    let mut out = serde_json::to_vec(response).expect("serialize Response");
    out.push(b'\n');
    write_half.write_all(&out).await?;
    Ok(())
}

/// Forward a non-streaming command to the node loop and block until it
/// acks (or the timeout fires). Returns a `Response` ready to serialize
/// back to the client.
async fn dispatch_oneshot<F>(
    command_tx: Option<&mpsc::Sender<ControlCommand>>,
    timeout: Duration,
    build: F,
) -> Response
where
    F: FnOnce(oneshot::Sender<ControlAck>) -> ControlCommand,
{
    let Some(tx) = command_tx else {
        return Response::Error {
            message: "daemon has no control-command channel wired up".to_string(),
        };
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if tx.send(build(ack_tx)).await.is_err() {
        return Response::Error {
            message: "daemon node loop is no longer accepting commands".to_string(),
        };
    }
    match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack_to_response(ack),
        Ok(Err(_)) => Response::Error {
            message: "daemon dropped the command without replying".to_string(),
        },
        Err(_) => Response::Error {
            message: format!("daemon did not ack within {} seconds", timeout.as_secs()),
        },
    }
}

/// Forward a streaming-capable command to the node loop, write each
/// non-terminal `Progress` ack as its own NDJSON line, and finish with
/// the terminal response. The `timeout` resets every time a fresh ack
/// lands so a long-but-progressing fetch isn't killed for taking longer
/// than the per-message bound.
async fn dispatch_streaming<W, F>(
    write_half: &mut W,
    command_tx: Option<&mpsc::Sender<ControlCommand>>,
    timeout: Duration,
    build: F,
) -> Result<(), ServerError>
where
    W: AsyncWriteExt + Unpin,
    F: FnOnce(mpsc::Sender<ControlAck>) -> ControlCommand,
{
    let Some(tx) = command_tx else {
        let response = Response::Error {
            message: "daemon has no control-command channel wired up".to_string(),
        };
        return write_response(write_half, &response).await;
    };
    let (ack_tx, mut ack_rx) = streaming_ack_channel();
    if tx.send(build(ack_tx)).await.is_err() {
        let response = Response::Error {
            message: "daemon node loop is no longer accepting commands".to_string(),
        };
        return write_response(write_half, &response).await;
    }
    loop {
        match tokio::time::timeout(timeout, ack_rx.recv()).await {
            Ok(Some(ack)) => {
                let is_terminal = ack.is_terminal();
                let response = ack_to_response(ack);
                write_response(write_half, &response).await?;
                if is_terminal {
                    return Ok(());
                }
            }
            Ok(None) => {
                let response = Response::Error {
                    message: "daemon dropped the command without replying".to_string(),
                };
                return write_response(write_half, &response).await;
            }
            Err(_) => {
                let response = Response::Error {
                    message: format!(
                        "daemon went silent for more than {} seconds",
                        timeout.as_secs()
                    ),
                };
                return write_response(write_half, &response).await;
            }
        }
    }
}

fn ack_to_response(ack: ControlAck) -> Response {
    match ack {
        ControlAck::Ok { message } => Response::Ok { message },
        ControlAck::Bytes { data } => Response::Bytes {
            hex: format!("0x{}", hex::encode(data)),
        },
        ControlAck::BzzBytes {
            data,
            content_type,
            filename,
        } => Response::BzzBytes {
            hex: format!("0x{}", hex::encode(data)),
            content_type,
            filename,
        },
        ControlAck::Manifest { .. } => Response::Error {
            message: "manifest listing is only supported by ant-gateway".to_string(),
        },
        ControlAck::Progress(p) => Response::Progress(p),
        ControlAck::BytesStreamStart { .. }
        | ControlAck::BzzStreamStart { .. }
        | ControlAck::BytesChunk { .. } => Response::Error {
            message: "streaming byte bodies are only supported by ant-gateway".to_string(),
        },
        ControlAck::StreamDone => Response::Ok {
            message: "stream complete".to_string(),
        },
        ControlAck::ChunkUploaded { reference } => Response::ChunkUploaded { reference },
        ControlAck::Error { message } => Response::Error { message },
    }
}

/// Parse a 32-byte chunk reference. Accepts `0x`-prefixed or bare hex.
/// Lower- or upper-case is fine. Anything else is rejected before the
/// command hits the node loop so the caller gets a tidy error.
fn parse_reference(s: &str) -> Result<[u8; 32], String> {
    let stripped = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if stripped.len() != 64 {
        return Err(format!(
            "reference must be 32 bytes (64 hex chars); got {}",
            stripped.len()
        ));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(stripped, &mut out).map_err(|e| format!("invalid hex: {e}"))?;
    Ok(out)
}

#[cfg(unix)]
fn restrict_socket_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_socket_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}
