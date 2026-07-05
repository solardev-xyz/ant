//! Control-plane protocol and transports for `antd` ↔ `antctl`.
//!
//! Transport: newline-delimited JSON over a Unix domain socket. One request,
//! one response, then close. Keeping the wire format framing-trivial lets us
//! debug with `nc -U ~/.antd/antd.sock` and evolve the payload independently.

mod activity;
mod command;
mod protocol;

#[cfg(unix)]
mod client;
#[cfg(unix)]
mod server;

pub use activity::{ActiveRequestGuard, GatewayActivity, GatewayActivityHandle};
// Command/ack types + the streaming ack channel are platform-neutral
// (plain data + tokio channels); they live outside the `#[cfg(unix)]`
// transport so `ant-node` / `ant-gateway` / `ant-ffi` / `antd` build on
// Windows too. Only the Unix domain-socket transport (`serve`, client)
// is gated.
pub use command::{
    streaming_ack_channel, ControlAck, ControlCommand, ManifestEntryInfo, PinCheckStat,
    PostageBucketsView, StreamRange,
};
pub use protocol::{
    AccountingSnapshotView, CacheInfo, DiskCacheInfo, ExternalAddressInfo, GatewayRequestInfo,
    GatewayRequestKind, GetProgress, HandshakeReport, IdentityInfo, LastChequeView,
    PeerAccountingView, PeerConnectionInfo, PeerConnectionState, PeerInfo, PeerPipelineEntry,
    PostageStatusView, ProtocolError, Request, Response, RetrievalInfo, RoutingInfo,
    StatusSnapshot, UploadJobView, VersionInfo, PROTOCOL_VERSION,
};

#[cfg(unix)]
pub use client::{
    request_streaming, request_sync, request_upload_follow, ClientError, StreamEvent,
};
#[cfg(unix)]
pub use server::{serve, ServerError};
