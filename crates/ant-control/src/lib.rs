//! Control-plane protocol and transports for `antd` ↔ `antctl`.
//!
//! Transport: newline-delimited JSON over a Unix domain socket. One request,
//! one response, then close. Keeping the wire format framing-trivial lets us
//! debug with `nc -U ~/.antd/antd.sock` and evolve the payload independently.

mod protocol;

#[cfg(unix)]
mod client;
#[cfg(unix)]
mod server;

pub use protocol::{
    GetProgress, HandshakeReport, IdentityInfo, PeerConnectionInfo, PeerConnectionState, PeerInfo,
    PeerPipelineEntry, ProtocolError, Request, Response, RoutingInfo, StatusSnapshot, VersionInfo,
    PROTOCOL_VERSION,
};

#[cfg(unix)]
pub use client::{request_streaming, request_sync, ClientError, StreamEvent};
#[cfg(unix)]
pub use server::{
    serve, streaming_ack_channel, ControlAck, ControlCommand, ManifestEntryInfo, ServerError,
};
