//! Tokio-based UDS server. Reads one NDJSON request, writes one NDJSON response, closes.

use crate::protocol::{Request, Response, StatusSnapshot, VersionInfo, PROTOCOL_VERSION};
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

/// A mutating or network-touching command issued by a client and forwarded
/// to the node loop.
///
/// The node loop owns state that needs a single-writer path (the peerstore,
/// the swarm), so `serve()` does not touch it directly; instead it relays a
/// `ControlCommand` through an `mpsc::Sender` and awaits an ack on the
/// embedded `oneshot`.
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
    /// Walk the manifest at `reference`, resolve `path`, then join the
    /// resulting chunk tree into a single `Vec<u8>`. Acks with
    /// [`ControlAck::BzzBytes`] including any `Content-Type` metadata
    /// recovered from the manifest. When `allow_degraded_redundancy`
    /// is set the joiner masks any non-zero RS level byte off the
    /// file's root span and decodes without RS recovery.
    GetBzz {
        reference: [u8; 32],
        path: String,
        allow_degraded_redundancy: bool,
        ack: oneshot::Sender<ControlAck>,
    },
    /// Join the multi-chunk tree rooted at `reference` (a `/bytes/` ref,
    /// no manifest) and ack with the joined file bytes.
    GetBytes {
        reference: [u8; 32],
        ack: oneshot::Sender<ControlAck>,
    },
}

/// Node-loop reply to a [`ControlCommand`]. Serialized back to the client as
/// `Response::Ok`, `Response::Bytes`, `Response::BzzBytes`, or
/// `Response::Error` depending on the variant.
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
    Error {
        message: String,
    },
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

    let response = match serde_json::from_str::<Request>(trimmed) {
        Ok(Request::Status) => {
            let snap = status_rx.borrow().clone();
            Response::Status(Box::new(snap))
        }
        Ok(Request::Version) => Response::Version(VersionInfo {
            agent,
            protocol_version: PROTOCOL_VERSION,
        }),
        Ok(Request::PeersReset) => {
            dispatch_command(
                command_tx.as_ref(),
                FAST_COMMAND_TIMEOUT,
                |ack| ControlCommand::ResetPeerstore { ack },
            )
            .await
        }
        Ok(Request::Get { reference }) => match parse_reference(&reference) {
            Ok(addr) => {
                dispatch_command(
                    command_tx.as_ref(),
                    NETWORK_COMMAND_TIMEOUT,
                    |ack| ControlCommand::GetChunk {
                        reference: addr,
                        ack,
                    },
                )
                .await
            }
            Err(e) => Response::Error {
                message: format!("bad reference: {e}"),
            },
        },
        Ok(Request::GetBzz {
            reference,
            path,
            allow_degraded_redundancy,
        }) => match parse_reference(&reference) {
            Ok(addr) => {
                dispatch_command(
                    command_tx.as_ref(),
                    TREE_COMMAND_TIMEOUT,
                    |ack| ControlCommand::GetBzz {
                        reference: addr,
                        path,
                        allow_degraded_redundancy,
                        ack,
                    },
                )
                .await
            }
            Err(e) => Response::Error {
                message: format!("bad reference: {e}"),
            },
        },
        Ok(Request::GetBytes { reference }) => match parse_reference(&reference) {
            Ok(addr) => {
                dispatch_command(
                    command_tx.as_ref(),
                    TREE_COMMAND_TIMEOUT,
                    |ack| ControlCommand::GetBytes {
                        reference: addr,
                        ack,
                    },
                )
                .await
            }
            Err(e) => Response::Error {
                message: format!("bad reference: {e}"),
            },
        },
        Err(e) => Response::Error {
            message: format!("bad request: {e}"),
        },
    };

    let mut out = serde_json::to_vec(&response).expect("serialize Response");
    out.push(b'\n');
    write_half.write_all(&out).await?;
    write_half.flush().await?;
    write_half.shutdown().await?;
    Ok(())
}

/// Forward a mutating command to the node loop and block until it acks (or
/// the timeout fires). Returns a `Response` ready to serialize back to the
/// client.
async fn dispatch_command<F>(
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
        Ok(Ok(ControlAck::Ok { message })) => Response::Ok { message },
        Ok(Ok(ControlAck::Bytes { data })) => Response::Bytes {
            hex: format!("0x{}", hex::encode(data)),
        },
        Ok(Ok(ControlAck::BzzBytes {
            data,
            content_type,
            filename,
        })) => Response::BzzBytes {
            hex: format!("0x{}", hex::encode(data)),
            content_type,
            filename,
        },
        Ok(Ok(ControlAck::Error { message })) => Response::Error { message },
        Ok(Err(_)) => Response::Error {
            message: "daemon dropped the command without replying".to_string(),
        },
        Err(_) => Response::Error {
            message: format!("daemon did not ack within {} seconds", timeout.as_secs()),
        },
    }
}

/// Parse a 32-byte chunk reference. Accepts `0x`-prefixed or bare hex.
/// Lower- or upper-case is fine. Anything else is rejected before the
/// command hits the node loop so the caller gets a tidy error.
fn parse_reference(s: &str) -> Result<[u8; 32], String> {
    let stripped = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
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
