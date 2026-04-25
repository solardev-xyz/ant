//! libp2p host (TCP + DNS, Noise, Yamux, Identify, Ping) and Swarm `/swarm/handshake/14.0.0/handshake`.

mod behaviour;
mod dial;
pub mod dnsaddr;
mod handshake;
mod peerstore;
pub mod routing;
mod sinks;
mod underlay;

pub use behaviour::{run, RunConfig, RunError, DEFAULT_TARGET_PEERS};
pub use handshake::{HandshakeError, HandshakeInfo, PROTOCOL_HANDSHAKE};
pub use routing::{proximity, RoutingTable, NUM_BINS, OVERLAY_LEN};

use libp2p::multiaddr::Multiaddr;
use std::time::Duration;

/// Default mainnet bootnodes, matching upstream `bee` >= 2.7
/// (`/dnsaddr/mainnet.ethswarm.org` is the only published entry).
pub fn default_mainnet_bootnodes() -> Vec<Multiaddr> {
    ["/dnsaddr/mainnet.ethswarm.org"]
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect()
}

/// Default backoff range after a failed dial / handshake.
pub fn default_backoff() -> Duration {
    Duration::from_secs(5)
}
