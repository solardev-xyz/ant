//! Standalone in-memory ant gateway for out-of-process conformance
//! consumers — the bee-js pack (`conformance/beejs/`) drives unmodified
//! bee-js against this endpoint and against beemock, comparing both.
//!
//! Prints a single JSON hello line (same shape as beemock) and serves
//! until killed:
//!
//! ```sh
//! cargo run -p ant-conformance --bin memgateway
//! {"listening":"127.0.0.1:PORT"}
//! ```

use ant_conformance::{spawn_mem_gateway, MemGatewayConfig};

#[tokio::main]
async fn main() {
    let gw = spawn_mem_gateway(MemGatewayConfig::default()).await;
    println!("{{\"listening\":\"{}\"}}", gw.addr());
    // Unbuffer stdout so spawning harnesses see the hello immediately.
    use std::io::Write;
    std::io::stdout().flush().ok();
    std::future::pending::<()>().await;
}
