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
/// How long to wait for the node loop to acknowledge a mutating command before
/// replying to the client with a timeout error. Commands like `PeersReset` are
/// near-instant, but if the node loop is wedged we don't want to hang the
/// socket handler indefinitely.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// A mutating command issued by a client and forwarded to the node loop.
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
}

/// Node-loop reply to a [`ControlCommand`]. Serialized back to the client as
/// `Response::Ok` or `Response::Error`.
#[derive(Debug, Clone)]
pub enum ControlAck {
    Ok { message: String },
    Error { message: String },
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
            dispatch_command(command_tx.as_ref(), |ack| ControlCommand::ResetPeerstore {
                ack,
            })
            .await
        }
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
    match tokio::time::timeout(COMMAND_TIMEOUT, ack_rx).await {
        Ok(Ok(ControlAck::Ok { message })) => Response::Ok { message },
        Ok(Ok(ControlAck::Error { message })) => Response::Error { message },
        Ok(Err(_)) => Response::Error {
            message: "daemon dropped the command without replying".to_string(),
        },
        Err(_) => Response::Error {
            message: format!(
                "daemon did not ack within {} seconds",
                COMMAND_TIMEOUT.as_secs()
            ),
        },
    }
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
