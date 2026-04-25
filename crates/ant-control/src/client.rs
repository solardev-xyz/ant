//! Blocking std-only client for `antctl`. No tokio runtime needed.

use crate::protocol::{Request, Response};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connect {path}: {source}")]
    Connect {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("empty response from daemon")]
    EmptyResponse,
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Round-trip one request to the daemon and return its response.
pub fn request_sync(socket_path: &Path, request: &Request) -> Result<Response, ClientError> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| ClientError::Connect {
        path: socket_path.display().to_string(),
        source: e,
    })?;
    stream.set_read_timeout(Some(DEFAULT_TIMEOUT))?;
    stream.set_write_timeout(Some(DEFAULT_TIMEOUT))?;

    let mut line = serde_json::to_vec(request)?;
    line.push(b'\n');
    stream.write_all(&line)?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response)?;
    if response.trim().is_empty() {
        return Err(ClientError::EmptyResponse);
    }
    Ok(serde_json::from_str(response.trim())?)
}
