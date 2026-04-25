//! Tokio libp2p swarm: dial bootnodes, open `/swarm/handshake/14.0.0/handshake` via `libp2p_stream`, Identify + Ping.

use crate::dial::{bootstrap_dial_opts, endpoint_host, first_multiaddr_per_peer};
use crate::dnsaddr;
use crate::handshake::{handshake_outbound, HandshakeError, HandshakeInfo, PROTOCOL_HANDSHAKE};
use crate::peerstore::PeerStore;
use crate::routing::{Overlay, RoutingTable};
use crate::sinks;
use ant_control::{
    ControlAck, ControlCommand, HandshakeReport, PeerConnectionInfo, PeerConnectionState,
    PeerPipelineEntry, RoutingInfo, StatusSnapshot,
};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, OVERLAY_NONCE_LEN,
    SECP256K1_SECRET_LEN,
};
use k256::ecdsa::{SigningKey, VerifyingKey};
use futures::StreamExt;
use libp2p::core::connection::ConnectedPoint;
use libp2p::identify;
use libp2p::identity::Keypair;
use libp2p::multiaddr::Multiaddr;
use libp2p::ping;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{DialError, SwarmEvent};
use libp2p::{dns, noise, tcp, yamux, PeerId, StreamProtocol, Swarm, SwarmBuilder};
use libp2p_stream::Control;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, trace, warn};

const LISTEN: &str = "/ip4/0.0.0.0/tcp/0";
const AGENT: &str = concat!("antd/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Error)]
pub enum RunError {
    #[error("transport: {0}")]
    Transport(#[from] std::io::Error),
    #[error("handshake: {0}")]
    Handshake(#[from] crate::handshake::HandshakeError),
    #[error("behaviour: {0}")]
    Behaviour(#[from] libp2p::BehaviourBuilderError),
}

pub struct RunConfig {
    pub signing_secret: [u8; SECP256K1_SECRET_LEN],
    pub overlay_nonce: [u8; OVERLAY_NONCE_LEN],
    pub network_id: u64,
    pub bootnodes: Vec<Multiaddr>,
    pub libp2p_keypair: Keypair,
    /// Addresses to advertise via identify as externally reachable. Bee's
    /// inbound handshake stalls for 10 s if its peerstore has no public
    /// multiaddr for us, so operators behind NAT should set at least one
    /// dialable address here.
    pub external_addrs: Vec<Multiaddr>,
    /// Live status view maintained by the swarm loop; read by the control
    /// socket server.
    pub status: Option<watch::Sender<StatusSnapshot>>,
    /// Peer-set target. The swarm loop will keep dialing hive-discovered
    /// peers until this many BZZ-handshake-complete peers are online.
    pub target_peers: usize,
    /// Path to the on-disk peer snapshot. Loaded at startup to warm the
    /// dial pipeline and avoid the bootnode hop on every restart, then
    /// flushed periodically and on shutdown. `None` disables persistence.
    pub peerstore_path: Option<PathBuf>,
    /// Live mutation channel fed by the control-socket server. `None` means
    /// commands like `antctl peers reset` will fail with
    /// `"daemon has no control-command channel wired up"`.
    pub commands: Option<mpsc::Receiver<ControlCommand>>,
    /// Baseline for cold-start elapsed metrics (`time_to_first_peer_s`, etc.).
    pub process_start: std::time::Instant,
}

#[derive(libp2p::swarm::NetworkBehaviour)]
#[behaviour(prelude = "libp2p_swarm::derive_prelude")]
pub struct AntBehaviour {
    stream: libp2p_stream::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

const MIN_BACKOFF: Duration = Duration::from_secs(2);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// Bee's inbound handshake handler waits for the peerstore to be populated
/// via libp2p-identify (a 10 s timeout). Opening our BZZ stream before
/// identify has round-tripped makes bee stall for the full 10 s, then disconnect.
/// This cap is the longest we'll wait before opening the BZZ stream anyway.
const HANDSHAKE_IDENTIFY_WAIT: Duration = Duration::from_secs(3);
/// Hive sinks → swarm-loop channel depth. Bee sends Peers messages with up to
/// 30 entries per stream, and multiple bootnodes gossip concurrently; a few
/// hundred slots is enough to avoid backpressure without being wasteful.
const HINT_CHAN_CAP: usize = 512;
/// Per-loop iteration cap on how many hint dials we fan out. Keeps the
/// ConnectionEstablished → handshake pipeline from being drowned in parallel
/// dials when a fresh bootnode dumps its full neighbourhood at once.
const HINT_DIAL_BATCH: usize = 4;
/// Default target peer count if `RunConfig::target_peers` is left at zero.
/// Bee's mainnet neighbourhood typically fans out to a few hundred peers
/// once kademlia stabilises; 100 keeps us connected to a useful slice of
/// that fan-out while staying cheap on CPU/FDs and well under the raised
/// `RLIMIT_NOFILE` we bump to at `antd` startup.
pub const DEFAULT_TARGET_PEERS: usize = 100;
/// Wall-clock cadence for flushing the peerstore snapshot to disk. Trades
/// off "how much state can we lose on a SIGKILL" against IO churn under
/// peer churn. 30 s strikes a balance: a fresh restart still warms within
/// seconds even if the last 30 s of additions are gone.
const PEERSTORE_FLUSH_INTERVAL: Duration = Duration::from_secs(30);

fn is_loopback_multiaddr(addr: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    addr.iter().any(|p| match p {
        Protocol::Ip4(ip) => ip.is_loopback(),
        Protocol::Ip6(ip) => ip.is_loopback(),
        _ => false,
    })
}

/// Bee's libp2p-go peerstore filters out unroutable addresses (loopback,
/// RFC1918, link-local) when the peer is connected over a public transport.
/// Before we promote an observed address to external we need it to pass the
/// same filter, otherwise we just re-publish what bee will throw away.
fn is_globally_routable_multiaddr(addr: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    let mut has_ip = false;
    let ok = addr.iter().all(|p| match p {
        Protocol::Ip4(ip) => {
            has_ip = true;
            !ip.is_loopback()
                && !ip.is_private()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_broadcast()
                && !ip.is_documentation()
        }
        Protocol::Ip6(ip) => {
            has_ip = true;
            !ip.is_loopback() && !ip.is_unspecified()
        }
        _ => true,
    });
    has_ip && ok
}

/// Everything `drive_handshake` needs, lifted out of `RunConfig` so it can be
/// cloned into spawned per-peer tasks without touching the main loop's `cfg`.
#[derive(Clone)]
struct HandshakeParams {
    local_peer_id: PeerId,
    signing_secret: [u8; SECP256K1_SECRET_LEN],
    overlay_nonce: [u8; OVERLAY_NONCE_LEN],
    network_id: u64,
}

/// Result of the full per-peer pipeline: wait-for-identify → open BZZ stream
/// → run handshake. Emitted to the main loop over an mpsc channel.
enum DriveOutcome {
    Ok(HandshakeInfo),
    OpenFailed(String),
    HandshakeFailed(HandshakeError),
}

/// Per-peer state tracked while they're moving through the pipeline. The
/// dialed `Multiaddr` is carried through both phases so we can persist it to
/// the peerstore on a successful handshake without going back to libp2p for
/// connection metadata after the fact.
enum PendingPhase {
    /// Dialed or accepted; waiting for libp2p-identify to round-trip so bee's
    /// inbound handshake handler finds us in its peerstore.
    AwaitingIdentify {
        sig: oneshot::Sender<()>,
        addr: Multiaddr,
        identify_started: Instant,
        /// `Some` when we were the dialer and recorded a start in `dial_started`.
        dial_ms: Option<u64>,
    },
    /// Identify sent; the spawned task is opening the BZZ stream and running
    /// the handshake. Kept for cleanup on disconnect plus the dialed addr we
    /// hand to the peerstore on success.
    Handshaking {
        addr: Multiaddr,
        handshake_started: Instant,
        dial_ms: Option<u64>,
        identifying_ms: u64,
    },
}

impl PendingPhase {
    fn dialed_addr(&self) -> Multiaddr {
        match self {
            PendingPhase::AwaitingIdentify { addr, .. } => addr.clone(),
            PendingPhase::Handshaking { addr, .. } => addr.clone(),
        }
    }
}

/// Swarm-loop accounting: which peers we're mid-handshake with, which have
/// completed, and the dial backlog from hive gossip.
struct PeerFailure {
    peer: PeerId,
    at: Instant,
}

struct SwarmState {
    /// Peers we've dialed but haven't yet seen a `ConnectionEstablished` or
    /// `OutgoingConnectionError` for. Counting these against `target_peers`
    /// is what stops us from firing 4 dials per event-loop iteration and
    /// ballooning the peer set past the cap on fast connections.
    dialing: HashSet<PeerId>,
    /// Wall time when `dialing.insert` happened (outbound dials only).
    dial_started: HashMap<PeerId, Instant>,
    /// A representative dial address for `antctl` (first hint / bootnode multiaddr).
    dial_hint_addr: HashMap<PeerId, Multiaddr>,
    /// `disconnect_peer_id` was called; connection not fully torn down yet.
    closing: HashSet<PeerId>,
    /// Recent dial or handshake failures (pruned in [`sync_peer_pipeline`]).
    failed: Vec<PeerFailure>,
    pending: HashMap<PeerId, PendingPhase>,
    bzz_peers: HashSet<PeerId>,
    /// Forwarding-Kademlia routing table populated as each outbound BZZ
    /// handshake completes; consumed by the retrieval client to pick which
    /// peer to ask for a given chunk. Mirrors `bzz_peers` but carries the
    /// overlay alongside the peer-id and indexes by proximity order.
    routing: RoutingTable,
    /// All peer ids we've ever had a hint for, whether enqueued or already
    /// dialed. Stops hive's Peers broadcasts from queueing the same peer
    /// repeatedly as every bootnode gossips its neighbourhood to us.
    seen_hints: HashSet<PeerId>,
    hint_queue: VecDeque<sinks::PeerHint>,
    target_peers: usize,
    /// Order in which peers first appeared in the pipeline. Used as the sort
    /// key in [`build_peer_pipeline_entries`] so new peers are appended to
    /// the bottom of the rendered table instead of jumping around as their
    /// state advances. Pruned to whatever's currently in the pipeline on
    /// every rebuild so it can't grow unbounded across long sessions.
    peer_order: HashMap<PeerId, u64>,
    next_peer_order: u64,
}

impl SwarmState {
    fn new(target_peers: usize, base_overlay: Overlay) -> Self {
        Self {
            dialing: HashSet::new(),
            dial_started: HashMap::new(),
            dial_hint_addr: HashMap::new(),
            closing: HashSet::new(),
            failed: Vec::new(),
            pending: HashMap::new(),
            bzz_peers: HashSet::new(),
            routing: RoutingTable::new(base_overlay),
            seen_hints: HashSet::new(),
            hint_queue: VecDeque::new(),
            target_peers,
            peer_order: HashMap::new(),
            next_peer_order: 0,
        }
    }

    fn peer_set_size(&self) -> usize {
        self.bzz_peers.len()
    }

    /// Everything we're counting against `target_peers`: live dials, peers
    /// mid-handshake, and completed peers.
    fn pipeline_size(&self) -> usize {
        self.dialing.len() + self.pending.len() + self.bzz_peers.len()
    }

    fn enqueue_hint(&mut self, hint: sinks::PeerHint, local_peer_id: PeerId) {
        if hint.peer_id == local_peer_id
            || self.bzz_peers.contains(&hint.peer_id)
            || self.pending.contains_key(&hint.peer_id)
            || self.dialing.contains(&hint.peer_id)
            || !self.seen_hints.insert(hint.peer_id)
        {
            return;
        }
        self.hint_queue.push_back(hint);
    }
}

fn record_swarm_failure(state: &mut SwarmState, peer: PeerId) {
    state.failed.retain(|f| f.peer != peer);
    state.failed.push(PeerFailure {
        peer,
        at: Instant::now(),
    });
    const MAX_FAIL: usize = 64;
    if state.failed.len() > MAX_FAIL {
        let drain = state.failed.len() - MAX_FAIL;
        state.failed.drain(0..drain);
    }
}

fn classify_pipeline_state(
    peer: PeerId,
    state: &SwarmState,
    conn: Option<&PeerConnectionInfo>,
) -> PeerConnectionState {
    if state.closing.contains(&peer) {
        return PeerConnectionState::Closing;
    }
    if state.bzz_peers.contains(&peer) {
        return PeerConnectionState::Ready;
    }
    if let Some(phase) = state.pending.get(&peer) {
        return match phase {
            PendingPhase::AwaitingIdentify { .. } => PeerConnectionState::Identifying,
            PendingPhase::Handshaking { .. } => PeerConnectionState::Handshaking,
        };
    }
    if state.dialing.contains(&peer) {
        return PeerConnectionState::Dialing;
    }
    if let Some(c) = conn {
        return if c.bzz_overlay.is_some() {
            PeerConnectionState::Ready
        } else {
            PeerConnectionState::Handshaking
        };
    }
    if state.failed.iter().any(|f| f.peer == peer) {
        return PeerConnectionState::Failed;
    }
    PeerConnectionState::Ready
}

fn ip_for_pipeline_row(
    peer: PeerId,
    state: &SwarmState,
    conn: Option<&PeerConnectionInfo>,
) -> String {
    if let Some(c) = conn {
        if let Ok(ma) = c.address.parse::<Multiaddr>() {
            let h = endpoint_host(&ma);
            if !h.is_empty() {
                return h;
            }
        }
    }
    if let Some(phase) = state.pending.get(&peer) {
        return endpoint_host(&phase.dialed_addr());
    }
    if let Some(ma) = state.dial_hint_addr.get(&peer) {
        return endpoint_host(ma);
    }
    String::new()
}

fn build_peer_pipeline_entries(
    state: &mut SwarmState,
    connected: &[PeerConnectionInfo],
) -> Vec<PeerPipelineEntry> {
    let mut by_id: HashMap<PeerId, &PeerConnectionInfo> = HashMap::new();
    for c in connected {
        if let Ok(p) = c.peer_id.parse::<PeerId>() {
            by_id.insert(p, c);
        }
    }
    let mut all: HashSet<PeerId> = HashSet::new();
    for p in &state.closing {
        all.insert(*p);
    }
    for f in &state.failed {
        all.insert(f.peer);
    }
    for p in &state.dialing {
        all.insert(*p);
    }
    for p in state.pending.keys() {
        all.insert(*p);
    }
    for p in &state.bzz_peers {
        all.insert(*p);
    }
    for p in by_id.keys() {
        all.insert(*p);
    }

    // Drop ordering for peers that have left the pipeline so the map can't
    // grow unbounded over a long-running daemon. Done before assigning new
    // sequence numbers so a peer that disappears and reappears gets sent to
    // the bottom of the list (treated as a new peer).
    state.peer_order.retain(|p, _| all.contains(p));

    let mut rows: Vec<(u64, PeerPipelineEntry)> = Vec::with_capacity(all.len());
    for peer in all {
        let order = *state.peer_order.entry(peer).or_insert_with(|| {
            let n = state.next_peer_order;
            state.next_peer_order = state.next_peer_order.wrapping_add(1);
            n
        });
        let conn = by_id.get(&peer).copied();
        let st = classify_pipeline_state(peer, state, conn);
        let ip = ip_for_pipeline_row(peer, state, conn);
        rows.push((
            order,
            PeerPipelineEntry {
                peer_id: peer.to_string(),
                state: st,
                ip,
            },
        ));
    }
    rows.sort_by_key(|(order, _)| *order);
    rows.into_iter().map(|(_, e)| e).collect()
}

/// Time between full peer-pipeline rebuilds. Bootstrap fires hundreds of
/// swarm events per second; without a floor here we'd rebuild + serialize a
/// 200+ row pipeline (and grab the watch's write lock) on every event,
/// starving `ant_control::serve` enough that `antctl top` reads time out.
const PIPELINE_SYNC_INTERVAL: Duration = Duration::from_millis(100);

fn sync_peer_pipeline(status: &Option<watch::Sender<StatusSnapshot>>, state: &mut SwarmState) {
    const FAIL_TTL: Duration = Duration::from_secs(60);
    state.failed.retain(|f| f.at.elapsed() < FAIL_TTL);
    let Some(tx) = status else { return };
    let routing = routing_snapshot(&state.routing);
    tx.send_modify(|st| {
        st.peers.peer_pipeline = build_peer_pipeline_entries(state, &st.peers.connected_peers);
        st.peers.routing = routing.clone();
    });
}

/// Build a `RoutingInfo` that mirrors the live routing table for the
/// control snapshot. Cheap (32 entries) so we recompute on every pipeline
/// sync rather than try to track deltas.
fn routing_snapshot(table: &RoutingTable) -> RoutingInfo {
    let counts = table.bin_counts();
    RoutingInfo {
        base_overlay: format!("0x{}", hex::encode(table.base())),
        size: table.len() as u32,
        bins: counts.iter().map(|c| *c as u32).collect(),
    }
}

/// Push a routing-only update to the status snapshot. Used right after
/// `routing.admit` so `antctl top` reflects new peers without waiting for
/// the next pipeline-sync tick.
fn sync_routing_snapshot(status: &Option<watch::Sender<StatusSnapshot>>, table: &RoutingTable) {
    let Some(tx) = status else { return };
    let routing = routing_snapshot(table);
    tx.send_modify(|st| st.peers.routing = routing);
}

/// Compute our own Swarm overlay from the BZZ signing secret + nonce. This
/// is the same derivation the BZZ handshake uses, just lifted out so the
/// routing table can pin its base address before the first peer connects.
fn local_overlay_from_secret(
    signing_secret: &[u8; SECP256K1_SECRET_LEN],
    overlay_nonce: &[u8; OVERLAY_NONCE_LEN],
    network_id: u64,
) -> Overlay {
    let sk = SigningKey::from_bytes(signing_secret.into())
        .expect("signing_secret was validated at startup");
    let vk = VerifyingKey::from(&sk);
    let eth = ethereum_address_from_public_key(&vk);
    overlay_from_ethereum_address(&eth, network_id, overlay_nonce)
}

/// Run forever. Keeps a running handshake pipeline (capacity =
/// `target_peers`) fed by two sources: the static bootnode list (bootstrap
/// only) and live `/swarm/hive/1.1.0/peers` gossip from every connected
/// bee. Each successful BZZ handshake usually triggers a hive stream in
/// response, so the peer set fans out geometrically until we hit
/// `target_peers` and the dialer stops pulling from the hint queue.
pub async fn run(mut cfg: RunConfig) -> Result<(), RunError> {
    let local_peer_id = cfg.libp2p_keypair.public().to_peer_id();
    let mut swarm = build_swarm(cfg.libp2p_keypair.clone())?;
    let mut commands = cfg.commands.take();
    let listen: Multiaddr = LISTEN.parse().unwrap();
    swarm
        .listen_on(listen)
        .map_err(|e| std::io::Error::other(format!("listen_on: {e}")))?;

    // Advertise any user-supplied external addresses so bee's peerstore sees
    // a public multiaddr for us. Without this bee's inbound handshake handler
    // waits 10 s in `peerMultiaddrs` because libp2p-go filters out the RFC1918
    // listener addresses we'd otherwise publish via identify.
    for addr in &cfg.external_addrs {
        swarm.add_external_address(addr.clone());
    }

    let mut control = swarm.behaviour().stream.new_control();
    let mut peer_agents: HashMap<PeerId, String> = HashMap::new();

    // Must register the post-handshake sinks BEFORE the first bootnode
    // handshake completes. bee's `ConnectIn` opens
    // `/swarm/pricing/1.0.0/pricing` and kademlia's `Announce` opens
    // `/swarm/hive/1.1.0/peers` immediately after BZZ handshake; rejecting
    // either triggers an instant `Disconnect`.
    let (hint_tx, mut hint_rx) = mpsc::channel::<sinks::PeerHint>(HINT_CHAN_CAP);
    sinks::spawn(sinks::register(&mut control, hint_tx));

    let target_peers = if cfg.target_peers == 0 {
        DEFAULT_TARGET_PEERS
    } else {
        cfg.target_peers
    };
    if let Some(s) = &cfg.status {
        s.send_modify(|st| st.peers.node_limit = target_peers as u32);
    }
    let base_overlay = local_overlay_from_secret(
        &cfg.signing_secret,
        &cfg.overlay_nonce,
        cfg.network_id,
    );
    let mut state = SwarmState::new(target_peers, base_overlay);
    let mut peerstore = match cfg.peerstore_path.clone() {
        Some(p) => PeerStore::load(p),
        None => PeerStore::disabled(),
    };
    // Warm the dial pipeline from the on-disk snapshot before the bootnode
    // dial fires. The bootnode dial still runs as a fallback — if every
    // stored peer is dead, the bootnodes carry us back into the network.
    let warm = peerstore.warm_hints();
    if !warm.is_empty() {
        info!(
            target: "ant_p2p",
            "warming pipeline from peerstore ({} entries)",
            warm.len(),
        );
        for hint in warm {
            state.enqueue_hint(hint, local_peer_id);
        }
    }
    let hs_params = HandshakeParams {
        local_peer_id,
        signing_secret: cfg.signing_secret,
        overlay_nonce: cfg.overlay_nonce,
        network_id: cfg.network_id,
    };
    let (result_tx, mut result_rx) = mpsc::channel::<(PeerId, DriveOutcome)>(64);

    let mut backoff = MIN_BACKOFF;
    let mut last_bootstrap_at = Instant::now();
    bootstrap_dial(&mut swarm, &cfg.bootnodes, &mut state).await;

    let mut flush_timer = tokio::time::interval(PEERSTORE_FLUSH_INTERVAL);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick — we have nothing to flush at startup.
    flush_timer.tick().await;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut last_pipeline_sync = Instant::now()
        .checked_sub(PIPELINE_SYNC_INTERVAL)
        .unwrap_or_else(Instant::now);

    loop {
        fill_pipeline_from_hints(&mut swarm, &mut state);
        maybe_rebootstrap(
            &mut swarm,
            &cfg.bootnodes,
            &mut state,
            &mut backoff,
            &mut last_bootstrap_at,
        )
        .await;
        if last_pipeline_sync.elapsed() >= PIPELINE_SYNC_INTERVAL {
            sync_peer_pipeline(&cfg.status, &mut state);
            last_pipeline_sync = Instant::now();
        }

        tokio::select! {
            ev = swarm.select_next_some() => {
                observe_event(&cfg.status, &mut peer_agents, &ev);
                handle_swarm_event(
                    &mut swarm,
                    &mut state,
                    &mut control,
                    &hs_params,
                    &result_tx,
                    &mut peerstore,
                    ev,
                );
            }
            Some(hint) = hint_rx.recv() => {
                state.enqueue_hint(hint, local_peer_id);
            }
            Some((peer, outcome)) = result_rx.recv() => {
                handle_drive_outcome(
                    &cfg,
                    &mut swarm,
                    &mut state,
                    &peer_agents,
                    &mut backoff,
                    &mut peerstore,
                    peer,
                    outcome,
                );
            }
            _ = flush_timer.tick() => {
                peerstore.flush();
            }
            Some(cmd) = recv_command(commands.as_mut()) => {
                handle_control_command(&mut state, &mut peerstore, &control, cmd);
            }
            _ = &mut shutdown => {
                info!(target: "ant_p2p", "shutdown signal; flushing peerstore");
                peerstore.flush();
                return Ok(());
            }
        }
    }
}

/// `tokio::select!` wrapper that makes the command channel inert when the
/// daemon was launched without one (`cfg.commands = None`). Returning a
/// `Future<Output = Option<_>>` that never yields in the `None` case lets the
/// select arm compile under the same shape as the other arms.
async fn recv_command(
    commands: Option<&mut mpsc::Receiver<ControlCommand>>,
) -> Option<ControlCommand> {
    match commands {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Apply a control-socket command against the node loop's single-writer
/// state. Keep the match exhaustive so a newly-added variant on the wire is
/// noisily rejected instead of silently dropped.
fn handle_control_command(
    state: &mut SwarmState,
    peerstore: &mut PeerStore,
    control: &Control,
    cmd: ControlCommand,
) {
    match cmd {
        ControlCommand::ResetPeerstore { ack } => {
            let evicted = peerstore.clear();
            // Also drop the dedup set so any peer we previously discarded as
            // "already seen" can re-enter the hint queue next time a bee
            // gossips us its table.
            state.seen_hints.clear();
            state.hint_queue.clear();
            let msg = format!(
                "peerstore reset: dropped {evicted} on-disk entries; current connections unchanged",
            );
            info!(target: "ant_p2p", "{msg}");
            let _ = ack.send(ControlAck::Ok { message: msg });
        }
        ControlCommand::GetChunk { reference, ack } => {
            // Pick the best peer up-front (synchronously, no allocations
            // beyond the existing routing entries) and hand the actual
            // libp2p round-trip off to a spawned task. Keeps the swarm
            // event loop free for new connections / hive gossip while the
            // retrieve is in flight.
            let candidate = state.routing.closest_peer(&reference, &[]);
            let Some((peer, _overlay)) = candidate else {
                let _ = ack.send(ControlAck::Error {
                    message: "no peers available; wait for handshakes to complete".to_string(),
                });
                return;
            };
            let mut control = control.clone();
            tokio::spawn(async move {
                let res = ant_retrieval::retrieve_chunk(&mut control, peer, reference).await;
                let reply = match res {
                    Ok(chunk) => {
                        debug!(
                            target: "ant_p2p",
                            %peer,
                            payload_bytes = chunk.payload().len(),
                            "retrieval ok",
                        );
                        ControlAck::Bytes {
                            data: chunk.payload().to_vec(),
                        }
                    }
                    Err(e) => {
                        debug!(target: "ant_p2p", %peer, "retrieval failed: {e}");
                        ControlAck::Error {
                            message: format!("retrieval from {peer}: {e}"),
                        }
                    }
                };
                let _ = ack.send(reply);
            });
        }
        ControlCommand::GetBytes { reference, ack } => {
            // Multi-chunk raw bytes: snapshot peers, build a routing-aware
            // fetcher, and drive the joiner inside a spawned task. The
            // joiner does its own per-chunk fetch + CAC verify; we only
            // need to feed it the root chunk.
            let peers = state.routing.snapshot();
            if peers.is_empty() {
                let _ = ack.send(ControlAck::Error {
                    message: "no peers available; wait for handshakes to complete".to_string(),
                });
                return;
            }
            let control = control.clone();
            tokio::spawn(async move {
                let reply = run_get_bytes(control, peers, reference).await;
                let _ = ack.send(reply);
            });
        }
        ControlCommand::GetBzz {
            reference,
            path,
            ack,
        } => {
            let peers = state.routing.snapshot();
            if peers.is_empty() {
                let _ = ack.send(ControlAck::Error {
                    message: "no peers available; wait for handshakes to complete".to_string(),
                });
                return;
            }
            let control = control.clone();
            tokio::spawn(async move {
                let reply = run_get_bzz(control, peers, reference, path).await;
                let _ = ack.send(reply);
            });
        }
    }
}

/// Spawned task body for `Request::GetBytes`. Fetches the root chunk via
/// `RoutingFetcher`, then hands it to `ant_retrieval::join`. Pulled out
/// of `handle_control_command` so the latter stays small and the async
/// pieces are exercise-able without spinning up a swarm.
async fn run_get_bytes(
    control: Control,
    peers: Vec<(libp2p::PeerId, [u8; 32])>,
    reference: [u8; 32],
) -> ControlAck {
    use ant_retrieval::{join, ChunkFetcher, RoutingFetcher, DEFAULT_MAX_FILE_BYTES};
    let mut fetcher = RoutingFetcher::new(control, peers);
    let root = match fetcher.fetch(reference).await {
        Ok(b) => b,
        Err(e) => {
            return ControlAck::Error {
                message: format!("fetch root chunk {}: {e}", hex::encode(reference)),
            }
        }
    };
    match join(&mut fetcher, &root, DEFAULT_MAX_FILE_BYTES).await {
        Ok(data) => {
            debug!(
                target: "ant_p2p",
                root = %hex::encode(reference),
                bytes = data.len(),
                "joiner produced file",
            );
            ControlAck::Bytes { data }
        }
        Err(e) => ControlAck::Error {
            message: format!("join {}: {e}", hex::encode(reference)),
        },
    }
}

/// Spawned task body for `Request::GetBzz`. Walks the manifest at
/// `reference`, resolves `path`, then runs the joiner against the
/// resolved data ref.
async fn run_get_bzz(
    control: Control,
    peers: Vec<(libp2p::PeerId, [u8; 32])>,
    reference: [u8; 32],
    path: String,
) -> ControlAck {
    use ant_retrieval::{
        join, lookup_path, ChunkFetcher, ManifestError, RoutingFetcher, DEFAULT_MAX_FILE_BYTES,
    };

    let mut fetcher = RoutingFetcher::new(control, peers);
    let lookup = match lookup_path(&mut fetcher, reference, &path).await {
        Ok(r) => r,
        Err(ManifestError::NotAManifest) => {
            return ControlAck::Error {
                message: format!(
                    "{} is not a mantaray manifest; try `antctl get` (raw chunk) or fetch via /bytes",
                    hex::encode(reference)
                ),
            }
        }
        Err(e) => {
            return ControlAck::Error {
                message: format!("manifest lookup '{path}': {e}"),
            }
        }
    };
    let data_ref = lookup.data_ref;
    debug!(
        target: "ant_p2p",
        manifest = %hex::encode(reference),
        path = %path,
        data_ref = %hex::encode(data_ref),
        content_type = ?lookup.content_type,
        "manifest resolved",
    );

    // Now fetch the file body itself. Reuse the same fetcher so the
    // blacklist / peer ordering carries over from the manifest walk.
    let root = match fetcher.fetch(data_ref).await {
        Ok(b) => b,
        Err(e) => {
            return ControlAck::Error {
                message: format!("fetch data root {}: {e}", hex::encode(data_ref)),
            }
        }
    };
    let data = match join(&mut fetcher, &root, DEFAULT_MAX_FILE_BYTES).await {
        Ok(d) => d,
        Err(e) => {
            return ControlAck::Error {
                message: format!("join data {}: {e}", hex::encode(data_ref)),
            }
        }
    };
    let filename = lookup
        .metadata
        .get("Filename")
        .cloned()
        .or_else(|| derive_filename_from_path(&path));
    ControlAck::BzzBytes {
        data,
        content_type: lookup.content_type,
        filename,
    }
}

/// Heuristic fallback: if mantaray didn't carry a `Filename` metadata
/// key (some uploaders skip it), use the last segment of the request
/// path so `antctl get -o` can still pick a sensible default.
fn derive_filename_from_path(path: &str) -> Option<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    trimmed.rsplit('/').next().map(|s| s.to_string())
}

/// Drain the hint queue into live libp2p dials until we hit either the peer
/// target or the per-iteration batch cap. Bounded dial fan-out keeps us from
/// swamping the local TCP stack when a bootnode gossips 30 peers at once.
fn fill_pipeline_from_hints(swarm: &mut Swarm<AntBehaviour>, state: &mut SwarmState) {
    let mut dialed = 0usize;
    while dialed < HINT_DIAL_BATCH && state.pipeline_size() < state.target_peers {
        let Some(hint) = state.hint_queue.pop_front() else {
            break;
        };
        if state.bzz_peers.contains(&hint.peer_id)
            || state.pending.contains_key(&hint.peer_id)
            || state.dialing.contains(&hint.peer_id)
        {
            continue;
        }
        if swarm.is_connected(&hint.peer_id) {
            continue;
        }
        let opts = DialOpts::peer_id(hint.peer_id)
            .addresses(hint.addrs.clone())
            .build();
        let peer_id = hint.peer_id;
        match swarm.dial(opts) {
            Ok(()) => {
                state.dialing.insert(peer_id);
                state.dial_started.insert(peer_id, Instant::now());
                state.failed.retain(|f| f.peer != peer_id);
                if let Some(a) = hint.addrs.first() {
                    state.dial_hint_addr.insert(peer_id, a.clone());
                }
                dialed += 1;
                debug!(
                    target: "ant_p2p",
                    peer = %peer_id,
                    addrs = hint.addrs.len(),
                    "dialing hive hint",
                );
            }
            Err(DialError::DialPeerConditionFalse(_)) => {}
            Err(e) => {
                debug!(target: "ant_p2p", peer = %peer_id, "hint dial error: {e}");
            }
        }
    }
}

/// Fire a fresh bootstrap dial when we've fully drained: no BZZ-handshaked
/// peers, nothing mid-pipeline, nothing queued. Uses exponential backoff so a
/// cold DNS + dead bootnode region doesn't turn into a tight retry loop.
async fn maybe_rebootstrap(
    swarm: &mut Swarm<AntBehaviour>,
    bootnodes: &[Multiaddr],
    state: &mut SwarmState,
    backoff: &mut Duration,
    last_at: &mut Instant,
) {
    if bootnodes.is_empty()
        || !state.bzz_peers.is_empty()
        || !state.pending.is_empty()
        || !state.dialing.is_empty()
        || !state.hint_queue.is_empty()
    {
        return;
    }
    if last_at.elapsed() < *backoff {
        return;
    }
    *last_at = Instant::now();
    bootstrap_dial(swarm, bootnodes, state).await;
    *backoff = bump_backoff(*backoff);
}

fn bump_backoff(backoff: Duration) -> Duration {
    (backoff * 2).min(MAX_BACKOFF)
}

/// Route a `SwarmEvent` into the pipeline. Everything that mutates state
/// lives here; the status snapshot has already been updated via
/// `observe_event` by the time we're called.
fn handle_swarm_event(
    swarm: &mut Swarm<AntBehaviour>,
    state: &mut SwarmState,
    control: &mut Control,
    hs_params: &HandshakeParams,
    result_tx: &mpsc::Sender<(PeerId, DriveOutcome)>,
    peerstore: &mut PeerStore,
    ev: SwarmEvent<AntBehaviourEvent>,
) {
    match ev {
        SwarmEvent::NewListenAddr { address, .. } if !is_loopback_multiaddr(&address) => {
            // Advertise non-loopback listeners as external addresses so that
            // our identify message carries usable `listen_addrs`. Without this
            // bee's inbound handshake handler waits 10 s in `peerMultiaddrs`
            // for our peerstore entry to populate.
            swarm.add_external_address(address);
        }
        // rust-libp2p's identify surfaces each remote's `observed_addr` as
        // a candidate here. Promote globally-routable ones to confirmed
        // external addresses so the next outbound identify carries our
        // NAT-punched public multiaddr and bee's peerstore accepts it.
        SwarmEvent::NewExternalAddrCandidate { address }
            if is_globally_routable_multiaddr(&address) =>
        {
            debug!(
                target: "ant_p2p",
                %address,
                "promoting observed addr to external",
            );
            swarm.add_external_address(address);
        }
        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            state.dialing.remove(&peer_id);
            state.dial_hint_addr.remove(&peer_id);
            state.failed.retain(|f| f.peer != peer_id);
            // Only dialer connections need our outbound BZZ handshake — bee
            // initiates the handshake over the listener direction itself.
            let ConnectedPoint::Dialer { address, .. } = endpoint else {
                return;
            };
            if state.bzz_peers.contains(&peer_id) || state.pending.contains_key(&peer_id) {
                // Duplicate connection (race with a hint or a bootnode retry);
                // keep the existing pipeline entry and let the new connection
                // get GC'd by libp2p's inactivity timeout if nothing uses it.
                return;
            }
            let (tx, rx) = oneshot::channel();
            let dial_ms = state
                .dial_started
                .remove(&peer_id)
                .map(|t| t.elapsed().as_millis() as u64);
            state.pending.insert(
                peer_id,
                PendingPhase::AwaitingIdentify {
                    sig: tx,
                    addr: address.clone(),
                    identify_started: Instant::now(),
                    dial_ms,
                },
            );
            let listen_addrs: Vec<Multiaddr> = swarm.listeners().cloned().collect();
            spawn_handshake_driver(
                peer_id,
                address,
                listen_addrs,
                control.clone(),
                hs_params.clone(),
                rx,
                result_tx.clone(),
            );
        }
        SwarmEvent::Behaviour(AntBehaviourEvent::Identify(identify::Event::Sent {
            peer_id,
            ..
        })) => {
            // Bee's inbound handshake calls `peerMultiaddrs` and blocks up to
            // 10 s on the peerstore being populated. `Event::Sent` is our
            // signal that we've pushed our identify to the remote, so it's
            // now safe to open the BZZ stream.
            if let Some(PendingPhase::AwaitingIdentify {
                sig,
                addr,
                identify_started,
                dial_ms,
            }) = state.pending.remove(&peer_id)
            {
                let identifying_ms = identify_started.elapsed().as_millis() as u64;
                state.pending.insert(
                    peer_id,
                    PendingPhase::Handshaking {
                        addr,
                        handshake_started: Instant::now(),
                        dial_ms,
                        identifying_ms,
                    },
                );
                let _ = sig.send(());
                trace!(target: "ant_p2p", %peer_id, "identify sent; unblocking handshake");
            }
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            state.closing.remove(&peer_id);
            state.dial_hint_addr.remove(&peer_id);
            let was_pending = state.pending.remove(&peer_id).is_some();
            let was_bzz = state.bzz_peers.remove(&peer_id);
            // Drop the routing entry alongside `bzz_peers`; otherwise the
            // forwarder would keep picking a peer we have no live
            // connection to.
            state.routing.forget(&peer_id);
            if was_bzz {
                info!(
                    target: "ant_p2p",
                    %peer_id,
                    "peer disconnected; peer set size={}",
                    state.peer_set_size(),
                );
            } else if was_pending {
                match cause {
                    Some(err) => debug!(
                        target: "ant_p2p",
                        %peer_id,
                        "connection closed during handshake: {err}",
                    ),
                    None => debug!(
                        target: "ant_p2p",
                        %peer_id,
                        "connection closed during handshake",
                    ),
                }
            }
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            if let Some(p) = peer_id {
                // `dial_started` is set by the two paths that *we* drive
                // (bootstrap_dial, fill_pipeline_from_hints). Any other
                // outbound dial — e.g. a libp2p-internal redial or a stray
                // address book attempt — would otherwise pollute the
                // pipeline with `Failed` rows the operator never asked for.
                let was_tracked = state.dial_started.remove(&p).is_some();
                state.dialing.remove(&p);
                state.dial_hint_addr.remove(&p);
                if was_tracked {
                    record_swarm_failure(state, p);
                }
                // Stored entries that go dud get evicted after
                // [`MAX_FAIL_COUNT`] consecutive failures; the peerstore
                // already noops for unknown peers.
                peerstore.record_failure(&p);
            }
            // Bootnode dial failures are noisy during rotation; demote to
            // debug once we're past the cold start (peer set > 0).
            if state.bzz_peers.is_empty() && state.pending.is_empty() {
                warn!(target: "ant_p2p", peer = ?peer_id, "dial error: {error}");
            } else {
                debug!(target: "ant_p2p", peer = ?peer_id, "dial error: {error}");
            }
        }
        _ => {}
    }
}

/// Finalize the per-peer pipeline result. `Ok` promotes the peer to
/// `bzz_peers`; anything else disconnects, which in turn drives
/// `ConnectionClosed` cleanup.
#[allow(clippy::too_many_arguments)]
fn handle_drive_outcome(
    cfg: &RunConfig,
    swarm: &mut Swarm<AntBehaviour>,
    state: &mut SwarmState,
    peer_agents: &HashMap<PeerId, String>,
    backoff: &mut Duration,
    peerstore: &mut PeerStore,
    peer: PeerId,
    outcome: DriveOutcome,
) {
    // Pull the dialed addr out of pending before we drop the entry: it's the
    // multiaddr we want to persist for redial. Entry may already be gone if
    // ConnectionClosed beat us here (e.g. bee dropped us mid-handshake).
    let pending_removed = state.pending.remove(&peer);
    let dialed_addr = pending_removed.as_ref().map(PendingPhase::dialed_addr);
    let pipeline_ms = match &pending_removed {
        Some(PendingPhase::Handshaking {
            handshake_started,
            dial_ms,
            identifying_ms,
            ..
        }) => Some(OutboundPipelineMs {
            dial_ms: *dial_ms,
            identifying_ms: *identifying_ms,
            handshake_ms: handshake_started.elapsed().as_millis() as u64,
        }),
        _ => None,
    };
    match outcome {
        DriveOutcome::Ok(info) => {
            let first_bzz_in_session = state.bzz_peers.is_empty();
            state.bzz_peers.insert(peer);
            // The remote_overlay was just verified by `handshake_outbound`
            // (signature recovers to an Ethereum address whose overlay
            // matches the declared one), so admitting it is safe.
            state.routing.admit(peer, info.remote_overlay);
            let peer_set_size = state.peer_set_size();
            state.failed.retain(|f| f.peer != peer);
            *backoff = MIN_BACKOFF;
            log_handshake_ok(&info, peer_set_size, pipeline_ms);
            record_peer_session_milestones(
                &cfg.status,
                cfg.process_start,
                first_bzz_in_session,
                peer_set_size,
                state.target_peers,
            );
            record_handshake(&cfg.status, peer, &info, peer_agents.get(&peer).cloned());
            sync_routing_snapshot(&cfg.status, &state.routing);
            // Persist the working dial address. We only ever record outbound
            // peers — for inbound connections we don't yet have a guaranteed-
            // reachable multiaddr, so we let those become hints via hive
            // gossip on the next start instead.
            let addrs = dialed_addr.map(|a| vec![a]).unwrap_or_default();
            peerstore.record_success(peer, addrs, info.remote_overlay);
        }
        DriveOutcome::OpenFailed(e) => {
            debug!(target: "ant_p2p", %peer, "open handshake stream: {e}");
            mark_for_disconnect(state, swarm, peer);
            record_swarm_failure(state, peer);
            peerstore.record_failure(&peer);
        }
        DriveOutcome::HandshakeFailed(e) => {
            warn!(target: "ant_p2p", %peer, "bzz handshake failed: {e}");
            mark_for_disconnect(state, swarm, peer);
            record_swarm_failure(state, peer);
            peerstore.record_failure(&peer);
        }
    }
}

/// Tear down a peer and mark it `Closing` for the duration of the wind-down.
/// `disconnect_peer_id` returns `Err` if there is no live connection (e.g.
/// remote already closed); in that case `ConnectionClosed` won't fire and we
/// must clear `closing` ourselves to avoid a sticky pipeline row.
fn mark_for_disconnect(state: &mut SwarmState, swarm: &mut Swarm<AntBehaviour>, peer: PeerId) {
    state.closing.insert(peer);
    if swarm.disconnect_peer_id(peer).is_err() {
        state.closing.remove(&peer);
    }
}

/// Drive one peer end-to-end on a spawned tokio task. Keeping this out of
/// the main loop lets us handshake `target_peers` in parallel while the loop
/// is free to accept new events, hive hints, and result messages.
fn spawn_handshake_driver(
    peer_id: PeerId,
    dial_addr: Multiaddr,
    listen_addrs: Vec<Multiaddr>,
    mut control: Control,
    params: HandshakeParams,
    identify_signal: oneshot::Receiver<()>,
    result_tx: mpsc::Sender<(PeerId, DriveOutcome)>,
) {
    tokio::spawn(async move {
        // Wait for identify to round-trip, but cap the wait so a broken peer
        // can't hold the slot forever. On timeout we open anyway — bee's
        // 10 s peerstore wait is a lot more tolerant than our 3 s cap, so the
        // handshake will still usually succeed.
        let _ = tokio::time::timeout(HANDSHAKE_IDENTIFY_WAIT, identify_signal).await;

        let proto = StreamProtocol::new(PROTOCOL_HANDSHAKE);
        let stream = match control.open_stream(peer_id, proto).await {
            Ok(s) => s,
            Err(e) => {
                let _ = result_tx
                    .send((peer_id, DriveOutcome::OpenFailed(e.to_string())))
                    .await;
                return;
            }
        };

        let dial_addrs = vec![dial_addr];
        match handshake_outbound(
            stream,
            params.local_peer_id,
            peer_id,
            &params.signing_secret,
            &params.overlay_nonce,
            params.network_id,
            &dial_addrs,
            listen_addrs,
        )
        .await
        {
            Ok(info) => {
                let _ = result_tx.send((peer_id, DriveOutcome::Ok(info))).await;
            }
            Err(e) => {
                let _ = result_tx
                    .send((peer_id, DriveOutcome::HandshakeFailed(e)))
                    .await;
            }
        }
    });
}

/// Side-channel: keep the status snapshot and the `peer_agents` map in sync
/// without changing the flow of the main event loop.
fn observe_event(
    status: &Option<watch::Sender<StatusSnapshot>>,
    peer_agents: &mut HashMap<PeerId, String>,
    ev: &SwarmEvent<AntBehaviourEvent>,
) {
    match ev {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(target: "ant_p2p", "listening on {address}");
            if let Some(s) = status {
                let ma = address.to_string();
                s.send_modify(|st| {
                    if !st.listeners.iter().any(|l| l == &ma) {
                        st.listeners.push(ma);
                    }
                });
            }
        }
        SwarmEvent::ExpiredListenAddr { address, .. } => {
            if let Some(s) = status {
                let ma = address.to_string();
                s.send_modify(|st| st.listeners.retain(|l| l != &ma));
            }
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            endpoint,
            num_established,
            ..
        } => {
            if let Some(s) = status {
                let pid = *peer_id;
                let peer_id = pid.to_string();
                let (direction, address) = peer_endpoint(endpoint);
                let connected_at_unix = unix_now();
                s.send_modify(|st| {
                    let already_listed = st
                        .peers
                        .connected_peers
                        .iter()
                        .any(|p| p.peer_id == peer_id);
                    if !already_listed {
                        st.peers.connected_peers.insert(
                            0,
                            PeerConnectionInfo {
                                peer_id,
                                direction,
                                address,
                                connected_at_unix,
                                agent_version: String::new(),
                                bzz_overlay: None,
                                full_node: None,
                                last_bzz_at_unix: None,
                            },
                        );
                    } else if num_established.get() == 1 {
                        if let Some(peer) = st
                            .peers
                            .connected_peers
                            .iter_mut()
                            .find(|p| p.peer_id == peer_id)
                        {
                            peer.direction = direction;
                            peer.address = address;
                            peer.connected_at_unix = connected_at_unix;
                        }
                    }
                    st.peers.connected = st.peers.connected_peers.len() as u32;
                });
                tracing::trace!(target: "ant_p2p", %pid, "status: connection opened");
            }
        }
        SwarmEvent::ConnectionClosed {
            peer_id,
            num_established,
            ..
        } => {
            if *num_established == 0 {
                peer_agents.remove(peer_id);
            }
            if let Some(s) = status {
                let pid = peer_id.to_string();
                s.send_modify(|st| {
                    if *num_established == 0 {
                        st.peers.connected_peers.retain(|p| p.peer_id != pid);
                    }
                    st.peers.connected = st.peers.connected_peers.len() as u32;
                });
            }
        }
        SwarmEvent::Behaviour(AntBehaviourEvent::Identify(identify::Event::Received {
            peer_id,
            info,
            ..
        })) => {
            let agent = info.agent_version.clone();
            info!(target: "ant_p2p", %peer_id, "libp2p connected agent={agent:?}");
            peer_agents.insert(*peer_id, agent);
            if let Some(s) = status {
                let pid = peer_id.to_string();
                let agent = info.agent_version.clone();
                s.send_modify(|st| {
                    if let Some(peer) = st
                        .peers
                        .connected_peers
                        .iter_mut()
                        .find(|p| p.peer_id == pid)
                    {
                        peer.agent_version = agent;
                    }
                });
            }
        }
        SwarmEvent::Behaviour(AntBehaviourEvent::Ping(e)) => {
            tracing::trace!(target: "ant_p2p", "ping: {e:?}");
        }
        _ => {}
    }
}

fn peer_endpoint(endpoint: &ConnectedPoint) -> (String, String) {
    match endpoint {
        ConnectedPoint::Dialer { address, .. } => ("outbound".to_string(), address.to_string()),
        ConnectedPoint::Listener { send_back_addr, .. } => {
            ("inbound".to_string(), send_back_addr.to_string())
        }
    }
}

/// Record one-shot session timings for UIs (e.g. `antctl top`).
fn record_peer_session_milestones(
    status: &Option<watch::Sender<StatusSnapshot>>,
    process_start: std::time::Instant,
    first_bzz_in_session: bool,
    peer_set_size: usize,
    target_peers: usize,
) {
    let Some(s) = status else { return };
    let elapsed = process_start.elapsed().as_secs_f64();
    s.send_modify(|st| {
        if first_bzz_in_session {
            st.peers.time_to_first_peer_s.get_or_insert(elapsed);
        }
        // Guard against the (currently unreachable but cheap to defend against)
        // case where a future caller wires `RunConfig::target_peers = 0`
        // directly without going through `state.target_peers` normalisation.
        if target_peers > 0 && peer_set_size >= target_peers {
            st.peers.time_to_node_limit_s.get_or_insert(elapsed);
        }
    });
}

fn record_handshake(
    status: &Option<watch::Sender<StatusSnapshot>>,
    remote: PeerId,
    info: &HandshakeInfo,
    agent: Option<String>,
) {
    let Some(s) = status else { return };
    let now = unix_now();
    let report = HandshakeReport {
        remote_overlay: format!("0x{}", hex::encode(info.remote_overlay)),
        remote_peer_id: remote.to_string(),
        agent_version: agent.unwrap_or_default(),
        full_node: info.remote_full_node,
        at_unix: now,
    };
    s.send_modify(|st| {
        if let Some(peer) = st
            .peers
            .connected_peers
            .iter_mut()
            .find(|p| p.peer_id == report.remote_peer_id)
        {
            peer.bzz_overlay = Some(report.remote_overlay.clone());
            peer.full_node = Some(report.full_node);
            peer.last_bzz_at_unix = Some(report.at_unix);
            if peer.agent_version.is_empty() {
                peer.agent_version = report.agent_version.clone();
            }
        }
        st.peers.last_handshake = Some(report);
    });
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Sub-second breakdown for a completed **outbound** (dialer) pipeline.
struct OutboundPipelineMs {
    dial_ms: Option<u64>,
    identifying_ms: u64,
    handshake_ms: u64,
}

fn log_handshake_ok(
    info: &HandshakeInfo,
    peer_set_size: usize,
    pipeline_ms: Option<OutboundPipelineMs>,
) {
    let ov = hex::encode(info.remote_overlay);
    let prefix = ov.chars().take(6).collect::<String>();
    let suffix = ov.chars().rev().take(2).collect::<String>();
    info!(
        target: "ant_p2p",
        "bzz handshake ok overlay={prefix}…{suffix} full_node={} peer_set_size={peer_set_size}",
        info.remote_full_node,
    );
    if let Some(p) = pipeline_ms {
        let dial = p
            .dial_ms
            .map(|d| d.to_string())
            .unwrap_or_else(|| "n/a".to_string());
        let total_ms = p
            .dial_ms
            .map(|d| d + p.identifying_ms + p.handshake_ms)
            .unwrap_or(p.identifying_ms + p.handshake_ms);
        debug!(
            target: "ant_p2p",
            "outbound peer_pipeline_ms peer_set_size={peer_set_size} dial_ms={dial} \
             identifying_ms={} handshake_ms={} total_ms={total_ms}",
            p.identifying_ms,
            p.handshake_ms,
        );
    }
}

/// Expand every `/dnsaddr/` entry into concrete peer multiaddrs, group by
/// PeerId, then hand each group to libp2p as a single [`DialOpts`] with
/// IPv6-first ordering and a concurrency factor sized to the address count.
/// Swarm's `mainnet.ethswarm.org` TXT tree fans out into several regional
/// bootnodes; dialing them concurrently makes bootstrap tolerant of any
/// single region being unreachable.
async fn bootstrap_dial(
    swarm: &mut Swarm<AntBehaviour>,
    bootnodes: &[Multiaddr],
    state: &mut SwarmState,
) {
    if bootnodes.is_empty() {
        return;
    }
    let resolved = dnsaddr::resolve_all(bootnodes.iter().cloned()).await;
    let first_for_peer = first_multiaddr_per_peer(&resolved);
    let opts_list = bootstrap_dial_opts(resolved);
    if opts_list.is_empty() {
        warn!(
            target: "ant_p2p",
            "no dialable bootnode addresses after /dnsaddr/ resolution"
        );
        return;
    }
    // Skip peers we're already connected to (common on retry: bee ejects our
    // BZZ peer but keeps the other regional connections open, which would
    // otherwise produce noisy `PeerCondition::Disconnected` dial-cancelled
    // warnings).
    let filtered: Vec<_> = opts_list
        .into_iter()
        .filter(|opts| match opts.get_peer_id() {
            Some(p) => !swarm.is_connected(&p) && !state.dialing.contains(&p),
            None => true,
        })
        .collect();
    let total = filtered.len();
    let mut actually_dialed = 0usize;
    for opts in filtered {
        let peer = opts.get_peer_id();
        match swarm.dial(opts) {
            Ok(()) => {
                actually_dialed += 1;
                if let Some(p) = peer {
                    state.dialing.insert(p);
                    state.dial_started.insert(p, Instant::now());
                    state.failed.retain(|f| f.peer != p);
                    if let Some(a) = first_for_peer.get(&p) {
                        state.dial_hint_addr.insert(p, a.clone());
                    }
                    info!(target: "ant_p2p", peer=%p, "dialing bootnode");
                }
            }
            // Benign: the peer reconnected (or its dial is still pending)
            // between our `is_connected` check and libp2p's own bookkeeping.
            // Happens on hot retry loops when bee drops-then-reaccepts us.
            Err(DialError::DialPeerConditionFalse(_)) => {
                tracing::debug!(
                    target: "ant_p2p",
                    peer = ?peer,
                    "dial skipped: already connected or dial in progress",
                );
            }
            Err(e) => warn!(target: "ant_p2p", "dial error: {e}"),
        }
    }
    if actually_dialed > 0 {
        info!(target: "ant_p2p", "bootstrap dial fanout={actually_dialed}/{total}");
    }
}

/// Build a DNS resolver config from `/etc/resolv.conf` (system) but strip any
/// local search domains. `/dnsaddr/*.ethswarm.org` must resolve globally; a
/// captive-portal search suffix would turn it into a name that only exists on
/// the local network. Falls back to Cloudflare (1.1.1.1) if system config is
/// unreadable (common on Android / minimal containers).
fn resolver_config() -> dns::ResolverConfig {
    match hickory_resolver::system_conf::read_system_conf() {
        Ok((cfg, _)) => {
            dns::ResolverConfig::from_parts(None, Vec::new(), cfg.name_servers().to_vec())
        }
        Err(_) => dns::ResolverConfig::cloudflare(),
    }
}

fn build_swarm(keypair: Keypair) -> Result<Swarm<AntBehaviour>, RunError> {
    // `push_listen_addr_updates` triggers an identify-push whenever our
    // listener set changes. That matters for bee: if a later listener arrives
    // (e.g. port reopens after network change) bee learns it mid-connection
    // rather than on the next 5-minute identify interval. External address
    // additions don't trigger push in libp2p-identify 0.47, so the first
    // connection to a fresh bee still eats the 10 s `peerMultiaddrs` wait.
    let id_cfg = identify::Config::new(AGENT.to_string(), keypair.public().clone())
        .with_agent_version(AGENT.to_string())
        .with_push_listen_addr_updates(true);
    let behaviour = AntBehaviour {
        stream: libp2p_stream::Behaviour::default(),
        identify: identify::Behaviour::new(id_cfg),
        ping: ping::Behaviour::new(ping::Config::new()),
    };

    let swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| std::io::Error::other(format!("tcp/noise/yamux: {e}")))?
        .with_dns_config(resolver_config(), dns::ResolverOpts::default())
        .with_behaviour(|_| behaviour)
        .expect("infallible behaviour")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(u64::MAX)))
        .build();
    Ok(swarm)
}
