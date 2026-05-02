//! Tokio libp2p swarm: dial bootnodes, open `/swarm/handshake/14.0.0/handshake` via `libp2p_stream`, Identify + Ping.

use crate::dial::{bootstrap_dial_opts, endpoint_host, first_multiaddr_per_peer};
use crate::dnsaddr;
use crate::handshake::{handshake_outbound, HandshakeError, HandshakeInfo, PROTOCOL_HANDSHAKE};
use crate::peerstore::PeerStore;
use crate::routing::{Overlay, RoutingTable};
use crate::sinks;
use ant_control::{
    CacheInfo, ControlAck, ControlCommand, GatewayActivity, GetProgress, HandshakeReport,
    PeerConnectionInfo, PeerConnectionState, PeerPipelineEntry, RetrievalInfo, RoutingInfo,
    StatusSnapshot, StreamRange,
};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, OVERLAY_NONCE_LEN,
    SECP256K1_SECRET_LEN,
};
use ant_retrieval::{ProgressTracker, RetrievalCounters, DEFAULT_CACHE_CAPACITY};
use futures::StreamExt;
use k256::ecdsa::{SigningKey, VerifyingKey};
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
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
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
    /// When true, scope the in-memory chunk cache to a single
    /// `antctl get` invocation: a fresh `InMemoryChunkCache` is built
    /// for each request and dropped when the request finishes. Intra-
    /// request retries still benefit from the cache (they share the
    /// per-request `Arc`), but a second `antctl get` starts cold.
    /// Useful for reproducing transient retrieval failures and
    /// timing the cold path; off in production.
    pub per_request_chunk_cache: bool,
    /// When set, every chunk fetched by a `GetBytes` / `GetBzz`
    /// request is dumped to `<dir>/<hex_addr>.bin` (raw wire bytes:
    /// 8-byte LE span || payload). Combined with the integration-test
    /// helper that loads a directory of such chunks into a
    /// `MapFetcher`, this lets us snapshot a successful mainnet
    /// retrieval and replay it offline as a regression fixture. The
    /// daemon only honours the flag in debug builds (the `antd` CLI
    /// hides it otherwise); the directory must already exist.
    pub chunk_record_dir: Option<PathBuf>,
    /// Live registry of currently-active gateway HTTP requests,
    /// shared with `ant-gateway`. The swarm loop reads a snapshot
    /// from it on every status tick to populate
    /// `StatusSnapshot::retrieval.gateway_requests` so `antctl top`
    /// can render the Retrieval tab. `None` when the gateway is
    /// disabled (e.g. `antd --no-http-api`).
    pub gateway_activity: Option<Arc<GatewayActivity>>,
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
    /// Total cold-start time (dial + identify + handshake, in ms) for each
    /// peer that has reached `Ready`. Surfaced as `ready_in_ms` on the
    /// pipeline rows so `antctl top` can show the column and rank slow
    /// peers. Reset on `ConnectionClosed` together with `bzz_peers`, so a
    /// reconnect always reports the new pipeline's timing rather than a
    /// stale one.
    ready_in_ms: HashMap<PeerId, u64>,
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
    /// Process-wide chunk cache, when enabled. `Some(cache)` means
    /// every `GetBytes` / `GetBzz` clones this single `Arc`, so chunks
    /// fetched on request N are reusable for request N+1. `None`
    /// means each request builds its own fresh `InMemoryChunkCache`
    /// (intra-request retries still cache-hit; consecutive
    /// `antctl get` calls start cold). Toggled by
    /// `RunConfig::per_request_chunk_cache`.
    chunk_cache: Option<Arc<ant_retrieval::InMemoryChunkCache>>,
    /// Optional fixture-capture directory; cloned into every
    /// `RoutingFetcher` we build for a `GetBytes` / `GetBzz` request,
    /// so each chunk fetched (or served from the cache) gets dumped
    /// to disk. Set from `RunConfig::chunk_record_dir`.
    chunk_record_dir: Option<PathBuf>,
    /// Live `(PeerId, Overlay)` snapshot of the routing table, refreshed
    /// every time we admit or forget a peer. Spawned `GetBytes` /
    /// `GetBzz` tasks subscribe to this watch and re-read it at the
    /// start of every retry attempt — that's how we avoid pinning a
    /// minute-stale peer list across a 3+ minute multi-chunk fetch.
    /// Bee peers churn aggressively (libp2p connections regularly
    /// open and close within a few seconds during normal swarm
    /// activity), and a snapshot frozen at command time would leave
    /// the fetcher trying to open streams on long-dead connections —
    /// which surfaces in the logs as `io: <peer>/N: connection is closed`
    /// or `oneshot canceled` from the libp2p stream-open path. The
    /// retry loop's whole point is to re-roll against the *current*
    /// network state, not the network state from before the fetch
    /// started.
    peers_watch: watch::Sender<Vec<(PeerId, Overlay)>>,
    /// Process-wide cap on concurrent `retrieve_chunk` calls. Cloned
    /// into every `RoutingFetcher` we build for any `GetBytes` /
    /// `GetBzz` request, so the cap applies across concurrent
    /// requests rather than per-request. The cap is deliberately below
    /// the unbounded browser stampede (~60 simultaneous retrieval
    /// streams in the four-WAV repro), but high enough that several
    /// large gateway downloads can make progress at once.
    retrieval_inflight: Arc<Semaphore>,
    /// Notification channel into the pseudosettle driver. Cloned into
    /// every `RoutingFetcher` so successful chunk fetches feed the
    /// driver's per-peer activity tracker. Set lazily by the run loop
    /// after `mpsc::channel` is created (the channel sender doesn't
    /// outlive the driver task, and the run loop owns both).
    payment_notify: Option<mpsc::Sender<PeerId>>,
    /// Shared client-side accounting mirror. Built once and cloned
    /// into every fetcher and into the pseudosettle driver. The
    /// fetcher uses it for admission control on dispatch
    /// (`Accounting::try_reserve`); the driver uses it to credit
    /// peers back on every accepted refresh
    /// (`Accounting::credit`); the swarm calls
    /// `Accounting::forget` from `ConnectionClosed` so peer state
    /// matches bee's `notifyPeerConnect` reset on the remote end.
    accounting: Option<Arc<ant_retrieval::accounting::Accounting>>,
    /// Process-wide retrieval counters. Cloned into every
    /// `RoutingFetcher` we build; read on every status tick to
    /// populate the cumulative `bytes_fetched_total` /
    /// `chunks_fetched_total` fields in `StatusSnapshot::retrieval`.
    retrieval_counters: Arc<RetrievalCounters>,
    /// Optional live registry of in-flight gateway HTTP requests.
    /// `None` when `--no-http-api` was passed; `Some` when the
    /// gateway is wired up. Read by the status publisher for the
    /// Retrieval tab in `antctl top`.
    gateway_activity: Option<Arc<GatewayActivity>>,
}

impl SwarmState {
    fn new(
        target_peers: usize,
        base_overlay: Overlay,
        per_request_chunk_cache: bool,
        chunk_record_dir: Option<PathBuf>,
        gateway_activity: Option<Arc<GatewayActivity>>,
    ) -> Self {
        let chunk_cache = if per_request_chunk_cache {
            None
        } else {
            Some(Arc::new(
                ant_retrieval::InMemoryChunkCache::with_default_capacity(),
            ))
        };
        Self {
            dialing: HashSet::new(),
            dial_started: HashMap::new(),
            dial_hint_addr: HashMap::new(),
            closing: HashSet::new(),
            failed: Vec::new(),
            pending: HashMap::new(),
            bzz_peers: HashSet::new(),
            ready_in_ms: HashMap::new(),
            routing: RoutingTable::new(base_overlay),
            seen_hints: HashSet::new(),
            hint_queue: VecDeque::new(),
            target_peers,
            peer_order: HashMap::new(),
            next_peer_order: 0,
            chunk_cache,
            chunk_record_dir,
            peers_watch: watch::channel(Vec::new()).0,
            retrieval_inflight: Arc::new(Semaphore::new(RETRIEVAL_INFLIGHT_CAP)),
            payment_notify: None,
            accounting: None,
            retrieval_counters: Arc::new(RetrievalCounters::new()),
            gateway_activity,
        }
    }

    /// Republish the current routing-table snapshot on `peers_watch`.
    /// Call after every `routing.admit` / `routing.forget`. Cheap: the
    /// snapshot is at most a few hundred `(PeerId, [u8; 32])` pairs and
    /// `watch::Sender::send_replace` does a single move + wake of any
    /// borrow-blocked readers (there aren't any during normal operation
    /// — the spawned retry tasks read on attempt boundaries, not
    /// continuously).
    fn publish_peers(&self) {
        let snapshot = self.routing.snapshot();
        self.peers_watch.send_replace(snapshot);
    }

    /// Resolve the chunk cache for one user-visible `GetBytes` / `GetBzz`
    /// command: clone the shared daemon cache if it's enabled, or
    /// mint a brand-new cache for this request only. Either way the
    /// returned `Arc` is what the request's retry loop hands to every
    /// `RoutingFetcher` it builds, so intra-request retries always
    /// share a cache and only inter-request reuse depends on the flag.
    ///
    /// `bypass_cache` overrides the daemon-wide setting for this one
    /// request: a fresh `InMemoryChunkCache` is always returned, so
    /// no chunk hits or writes touch the long-lived cache.
    fn cache_for_request(&self, bypass_cache: bool) -> Arc<ant_retrieval::InMemoryChunkCache> {
        if bypass_cache {
            return Arc::new(ant_retrieval::InMemoryChunkCache::with_default_capacity());
        }
        match &self.chunk_cache {
            Some(c) => c.clone(),
            None => Arc::new(ant_retrieval::InMemoryChunkCache::with_default_capacity()),
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
        let ready_in_ms = state.ready_in_ms.get(&peer).copied();
        rows.push((
            order,
            PeerPipelineEntry {
                peer_id: peer.to_string(),
                state: st,
                ip,
                ready_in_ms,
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
    let retrieval = build_retrieval_info(state);
    tx.send_modify(|st| {
        st.peers.peer_pipeline = build_peer_pipeline_entries(state, &st.peers.connected_peers);
        st.peers.routing = routing.clone();
        st.retrieval = retrieval.clone();
    });
}

/// Read the live data-plane snapshot for `StatusSnapshot::retrieval`.
/// Cheap: each load is a single relaxed atomic; the gateway-activity
/// snapshot grabs one Mutex per call but the entry count is small
/// (typically ≤ a few dozen) so it never shows up in profiles.
fn build_retrieval_info(state: &SwarmState) -> RetrievalInfo {
    let cache_used = state
        .chunk_cache
        .as_ref()
        .map(|c| c.len() as u32)
        .unwrap_or(0);
    let in_flight = (RETRIEVAL_INFLIGHT_CAP - state.retrieval_inflight.available_permits()) as u32;
    let counters = state.retrieval_counters.snapshot();
    let gateway_requests = state
        .gateway_activity
        .as_ref()
        .map(|a| a.snapshot())
        .unwrap_or_default();
    RetrievalInfo {
        cache: CacheInfo {
            used: cache_used,
            capacity: DEFAULT_CACHE_CAPACITY as u32,
        },
        in_flight,
        in_flight_capacity: RETRIEVAL_INFLIGHT_CAP as u32,
        chunks_fetched_total: counters.chunks_fetched,
        bytes_fetched_total: counters.bytes_fetched,
        cache_hits_total: counters.cache_hits,
        gateway_requests,
    }
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
    let base_overlay =
        local_overlay_from_secret(&cfg.signing_secret, &cfg.overlay_nonce, cfg.network_id);
    let mut state = SwarmState::new(
        target_peers,
        base_overlay,
        cfg.per_request_chunk_cache,
        cfg.chunk_record_dir.clone(),
        cfg.gateway_activity.clone(),
    );

    // Pseudosettle driver: keeps our per-peer debt below bee's
    // light-mode `disconnectLimit` by periodically opening
    // `/swarm/pseudosettle/1.0.0/pseudosettle` and asking bee to clear
    // the time-based allowance worth. The fetcher feeds this driver
    // via `payment_notify` (see [`RoutingFetcher::with_payment_notify`]).
    // The driver also subscribes to `peers_watch` so it skips
    // refreshes for peers we no longer have a direct connection to —
    // critical for gateway loads where the routing set churns through
    // thousands of peers per hour.
    //
    // The shared `Accounting` mirror is the same one we hand to every
    // `RoutingFetcher` we build below; it gives the fetcher's hot path
    // admission control (refuse to dispatch to peers near
    // `lightDisconnectLimit`) and the driver a way to credit peers
    // back as bee accepts our refresh attempts. The driver also
    // listens for `HotHint`s on a separate channel so a debt crossing
    // can leapfrog the periodic walk.
    let (payment_notify_for_state, payment_notify_rx) =
        mpsc::channel::<PeerId>(crate::pseudosettle::NOTIFY_CHANNEL_CAP);
    let (hot_hint_tx, hot_hint_rx) = mpsc::channel::<ant_retrieval::accounting::HotHint>(
        crate::pseudosettle::HOT_HINT_CHANNEL_CAP,
    );
    let accounting = Arc::new(
        ant_retrieval::accounting::Accounting::new().with_hot_hint(hot_hint_tx),
    );
    tokio::spawn(crate::pseudosettle::run_driver(
        control.clone(),
        payment_notify_rx,
        hot_hint_rx,
        state.peers_watch.subscribe(),
        Some(accounting.clone()),
    ));
    state.payment_notify = Some(payment_notify_for_state);
    state.accounting = Some(accounting);
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
    // Floor on how often the status snapshot is republished even when
    // the swarm is fully idle. `sync_peer_pipeline` already runs from
    // the loop top on every wakeup, gated by `PIPELINE_SYNC_INTERVAL`;
    // this timer just guarantees `antctl top` keeps seeing a live
    // `RetrievalInfo` (cache fill, in-flight count, gateway-request
    // list) even when no swarm events fire. Cadence chosen to match
    // `antctl top --interval`'s 1 s default while still letting
    // operators run with sub-second polling without seeing stale data.
    let mut status_pulse = tokio::time::interval(Duration::from_millis(250));
    status_pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    status_pulse.tick().await;
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
            _ = status_pulse.tick() => {
                // No-op arm: the loop-top `sync_peer_pipeline` call
                // does the actual work. We just need *some* event to
                // wake the select up regularly so the snapshot stays
                // live during gateway-only / fully-idle periods.
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
        ControlCommand::GetChunkRaw { reference, ack } => {
            // Same routing / fetch path as `GetChunk`, but ack carries
            // the wire bytes (`span || payload`) so `ant-gateway` can
            // serve `/chunks/{addr}` byte-for-byte the way bee does.
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
                            wire_bytes = chunk.data.len(),
                            "retrieval ok (raw)",
                        );
                        ControlAck::Bytes { data: chunk.data }
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
        ControlCommand::GetBytes {
            reference,
            bypass_cache,
            progress,
            max_bytes,
            ack,
        } => {
            // Multi-chunk raw bytes: subscribe to the live peer-snapshot
            // watch, build a routing-aware fetcher per attempt against
            // the *current* set of BZZ peers, and drive the joiner
            // inside a spawned task. The joiner does its own per-chunk
            // fetch + CAC verify; we only need to feed it the root
            // chunk. Subscribing (rather than snapshotting at command
            // time) is what keeps the retry loop honest across long
            // multi-MiB fetches: by attempt N our original peer list
            // would be 30+ seconds stale and full of disconnected
            // peers.
            let peers_rx = state.peers_watch.subscribe();
            if peers_rx.borrow().is_empty() {
                send_terminal_ack(
                    &ack,
                    ControlAck::Error {
                        message: "no peers available; wait for handshakes to complete".to_string(),
                    },
                );
                return;
            }
            let control = control.clone();
            let cache = state.cache_for_request(bypass_cache);
            let record_dir = state.chunk_record_dir.clone();
            let inflight_limit = state.retrieval_inflight.clone();
            let payment_notify = state.payment_notify.clone();
            let accounting = state.accounting.clone();
            let counters = state.retrieval_counters.clone();
            let tracker = Arc::new(ProgressTracker::new(bypass_cache));
            let started = Instant::now();
            let emitter = if progress {
                Some(spawn_progress_emitter(
                    tracker.clone(),
                    ack.clone(),
                    started,
                ))
            } else {
                None
            };
            let tracker_for_run = tracker.clone();
            tokio::spawn(async move {
                let reply = run_get_bytes(
                    control,
                    peers_rx,
                    cache,
                    record_dir,
                    inflight_limit,
                    payment_notify,
                    accounting,
                    counters,
                    tracker_for_run,
                    reference,
                    max_bytes,
                )
                .await;
                if let Some(handle) = emitter {
                    handle.abort();
                }
                let _ = ack.send(reply).await;
            });
        }
        ControlCommand::StreamBytes {
            reference,
            bypass_cache,
            max_bytes,
            range,
            head_only,
            ack,
        } => {
            let peers_rx = state.peers_watch.subscribe();
            if peers_rx.borrow().is_empty() {
                send_terminal_ack(
                    &ack,
                    ControlAck::Error {
                        message: "no peers available; wait for handshakes to complete".to_string(),
                    },
                );
                return;
            }
            let control = control.clone();
            let cache = state.cache_for_request(bypass_cache);
            let record_dir = state.chunk_record_dir.clone();
            let inflight_limit = state.retrieval_inflight.clone();
            let payment_notify = state.payment_notify.clone();
            let accounting = state.accounting.clone();
            let counters = state.retrieval_counters.clone();
            let tracker = Arc::new(ProgressTracker::new(bypass_cache));
            // Always spawn the progress emitter for streaming requests:
            // the gateway uses the periodic `Progress` acks to update
            // its `GatewayActivity` entry so `antctl top`'s Retrieval
            // tab reflects live per-request chunks/bytes/in-flight
            // counts. Unlike `GetBytes` (where the client opts in via
            // the `progress` flag), streaming has no "consumer doesn't
            // care" mode — the gateway always wants the updates and a
            // 150 ms tick is well below the rate at which `BytesChunk`
            // acks already flow.
            let started = Instant::now();
            let emitter = spawn_progress_emitter(tracker.clone(), ack.clone(), started);
            let tracker_for_run = tracker.clone();
            tokio::spawn(async move {
                run_stream_bytes(
                    control,
                    peers_rx,
                    cache,
                    record_dir,
                    inflight_limit,
                    payment_notify,
                    accounting,
                    counters,
                    tracker_for_run,
                    reference,
                    max_bytes,
                    range,
                    head_only,
                    ack,
                )
                .await;
                emitter.abort();
            });
        }
        ControlCommand::StreamBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache,
            max_bytes,
            range,
            head_only,
            ack,
        } => {
            let peers_rx = state.peers_watch.subscribe();
            if peers_rx.borrow().is_empty() {
                send_terminal_ack(
                    &ack,
                    ControlAck::Error {
                        message: "no peers available; wait for handshakes to complete".to_string(),
                    },
                );
                return;
            }
            let control = control.clone();
            let cache = state.cache_for_request(bypass_cache);
            let record_dir = state.chunk_record_dir.clone();
            let inflight_limit = state.retrieval_inflight.clone();
            let payment_notify = state.payment_notify.clone();
            let accounting = state.accounting.clone();
            let counters = state.retrieval_counters.clone();
            let tracker = Arc::new(ProgressTracker::new(bypass_cache));
            // Same rationale as the StreamBytes arm above: gateway-side
            // activity rendering depends on the periodic `Progress`
            // acks, so the streaming path always runs the emitter.
            let started = Instant::now();
            let emitter = spawn_progress_emitter(tracker.clone(), ack.clone(), started);
            let tracker_for_run = tracker.clone();
            tokio::spawn(async move {
                run_stream_bzz(
                    control,
                    peers_rx,
                    cache,
                    record_dir,
                    inflight_limit,
                    payment_notify,
                    accounting,
                    counters,
                    tracker_for_run,
                    reference,
                    path,
                    allow_degraded_redundancy,
                    max_bytes,
                    range,
                    head_only,
                    ack,
                )
                .await;
                emitter.abort();
            });
        }
        ControlCommand::GetBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache,
            progress,
            max_bytes,
            ack,
        } => {
            // Same live-snapshot reasoning as `GetBytes` above.
            let peers_rx = state.peers_watch.subscribe();
            if peers_rx.borrow().is_empty() {
                send_terminal_ack(
                    &ack,
                    ControlAck::Error {
                        message: "no peers available; wait for handshakes to complete".to_string(),
                    },
                );
                return;
            }
            let control = control.clone();
            let cache = state.cache_for_request(bypass_cache);
            let record_dir = state.chunk_record_dir.clone();
            let inflight_limit = state.retrieval_inflight.clone();
            let payment_notify = state.payment_notify.clone();
            let accounting = state.accounting.clone();
            let counters = state.retrieval_counters.clone();
            let tracker = Arc::new(ProgressTracker::new(bypass_cache));
            let started = Instant::now();
            let emitter = if progress {
                Some(spawn_progress_emitter(
                    tracker.clone(),
                    ack.clone(),
                    started,
                ))
            } else {
                None
            };
            let tracker_for_run = tracker.clone();
            tokio::spawn(async move {
                let reply = run_get_bzz(
                    control,
                    peers_rx,
                    cache,
                    record_dir,
                    inflight_limit,
                    payment_notify,
                    accounting,
                    counters,
                    tracker_for_run,
                    reference,
                    path,
                    allow_degraded_redundancy,
                    max_bytes,
                )
                .await;
                if let Some(handle) = emitter {
                    handle.abort();
                }
                let _ = ack.send(reply).await;
            });
        }
        ControlCommand::ListBzz {
            reference,
            bypass_cache,
            ack,
        } => {
            let peers_rx = state.peers_watch.subscribe();
            if peers_rx.borrow().is_empty() {
                send_terminal_ack(
                    &ack,
                    ControlAck::Error {
                        message: "no peers available; wait for handshakes to complete".to_string(),
                    },
                );
                return;
            }
            let control = control.clone();
            let cache = state.cache_for_request(bypass_cache);
            let record_dir = state.chunk_record_dir.clone();
            let inflight_limit = state.retrieval_inflight.clone();
            let payment_notify = state.payment_notify.clone();
            let accounting = state.accounting.clone();
            let counters = state.retrieval_counters.clone();
            tokio::spawn(async move {
                let reply = run_list_bzz(
                    control,
                    peers_rx,
                    cache,
                    record_dir,
                    inflight_limit,
                    payment_notify,
                    accounting,
                    counters,
                    reference,
                )
                .await;
                let _ = ack.send(reply).await;
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_stream_bytes(
    control: Control,
    peers_rx: watch::Receiver<Vec<(libp2p::PeerId, [u8; 32])>>,
    cache: Arc<ant_retrieval::InMemoryChunkCache>,
    record_dir: Option<PathBuf>,
    inflight_limit: Arc<Semaphore>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
    accounting: Option<Arc<ant_retrieval::accounting::Accounting>>,
    counters: Arc<RetrievalCounters>,
    tracker: Arc<ProgressTracker>,
    reference: [u8; 32],
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    head_only: bool,
    ack: mpsc::Sender<ControlAck>,
) {
    use ant_retrieval::ChunkFetcher;

    let mut builder = ant_retrieval::RoutingFetcher::new(control, peers_rx)
        .with_cache(cache)
        .with_record_dir(record_dir)
        .with_progress(tracker.clone())
        .with_inflight_limit(inflight_limit)
        .with_counters(counters)
        .with_request_inflight_limit(RETRIEVAL_REQUEST_INFLIGHT_CAP);
    if let Some(tx) = payment_notify {
        builder = builder.with_payment_notify(tx);
    }
    if let Some(acc) = accounting {
        builder = builder.with_accounting(acc);
    }
    let fetcher = builder;

    let root = match fetcher.fetch(reference).await {
        Ok(root) => root,
        Err(e) => {
            let _ = ack
                .send(ControlAck::Error {
                    message: format!("fetch root chunk {}: {e}", hex::encode(reference)),
                })
                .await;
            return;
        }
    };
    // `/bytes` does not pass `allow_degraded_redundancy`; if the root
    // span carries an RS level the joiner will reject the file later.
    // We still mask before reporting `total_bytes` so the gateway's
    // `Content-Length` reflects the real file size when (eventually) we
    // wire RS recovery in.
    let Some(total_bytes) = root_span_to_u64_masked(&root) else {
        let _ = ack
            .send(ControlAck::Error {
                message: format!("root chunk {} shorter than span", hex::encode(reference)),
            })
            .await;
        return;
    };
    tracker.set_total_bytes(total_bytes);
    if ack
        .send(ControlAck::BytesStreamStart { total_bytes })
        .await
        .is_err()
    {
        return;
    }

    if head_only {
        let _ = ack.send(ControlAck::StreamDone).await;
        return;
    }

    stream_root_chunk_with_range(&fetcher, root, total_bytes, max_bytes, range, reference, ack)
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_stream_bzz(
    control: Control,
    peers_rx: watch::Receiver<Vec<(libp2p::PeerId, [u8; 32])>>,
    cache: Arc<ant_retrieval::InMemoryChunkCache>,
    record_dir: Option<PathBuf>,
    inflight_limit: Arc<Semaphore>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
    accounting: Option<Arc<ant_retrieval::accounting::Accounting>>,
    counters: Arc<RetrievalCounters>,
    tracker: Arc<ProgressTracker>,
    reference: [u8; 32],
    path: String,
    allow_degraded_redundancy: bool,
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    head_only: bool,
    ack: mpsc::Sender<ControlAck>,
) {
    use ant_retrieval::{lookup_path, ChunkFetcher, ManifestError};

    // Reuse the same retry envelope as `run_get_bzz` for the manifest
    // walk. Once the data root has landed and we've emitted
    // `BzzStreamStart`, we cannot restart the response — `join_to_sender`
    // (and its range-aware sibling) handle in-place subtree retries
    // themselves, so the consumer sees a continuous stream.
    let mut last_error = None;
    for attempt in 1..=MAX_FETCH_ATTEMPTS {
        if peers_rx.borrow().is_empty() {
            let _ = ack
                .send(ControlAck::Error {
                    message: "no peers available; wait for handshakes to complete".to_string(),
                })
                .await;
            return;
        }
        let mut builder = ant_retrieval::RoutingFetcher::new(control.clone(), peers_rx.clone())
            .with_cache(cache.clone())
            .with_record_dir(record_dir.clone())
            .with_progress(tracker.clone())
            .with_inflight_limit(inflight_limit.clone())
            .with_counters(counters.clone())
            .with_request_inflight_limit(RETRIEVAL_REQUEST_INFLIGHT_CAP);
        if let Some(tx) = payment_notify.clone() {
            builder = builder.with_payment_notify(tx);
        }
        if let Some(acc) = accounting.clone() {
            builder = builder.with_accounting(acc);
        }
        let fetcher = builder;

        let lookup = match lookup_path(&fetcher, reference, &path).await {
            Ok(r) => r,
            Err(ManifestError::NotAManifest) => {
                let _ = ack
                    .send(ControlAck::Error {
                        message: format!(
                            "{} is not a mantaray manifest; try /bytes",
                            hex::encode(reference)
                        ),
                    })
                    .await;
                return;
            }
            Err(e) if is_manifest_transient(&e) && attempt < MAX_FETCH_ATTEMPTS => {
                last_error = Some(format!("manifest lookup '{path}': {e}"));
                let backoff = RETRY_BACKOFF_BASE * attempt as u32;
                warn!(
                    target: "ant_p2p",
                    attempt,
                    next_in_ms = backoff.as_millis() as u64,
                    "stream_bzz manifest lookup failed, retrying: {e}",
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
            Err(e) => {
                let _ = ack
                    .send(ControlAck::Error {
                        message: format!("manifest lookup '{path}': {e}"),
                    })
                    .await;
                return;
            }
        };

        let data_ref = lookup.data_ref;
        debug!(
            target: "ant_p2p",
            manifest = %hex::encode(reference),
            path = %path,
            data_ref = %hex::encode(data_ref),
            content_type = ?lookup.content_type,
            "stream_bzz manifest resolved",
        );

        let root = match fetcher.fetch(data_ref).await {
            Ok(r) => r,
            Err(e) if attempt < MAX_FETCH_ATTEMPTS => {
                last_error = Some(format!("fetch data root {}: {e}", hex::encode(data_ref)));
                let backoff = RETRY_BACKOFF_BASE * attempt as u32;
                warn!(
                    target: "ant_p2p",
                    attempt,
                    next_in_ms = backoff.as_millis() as u64,
                    "stream_bzz data root fetch failed, retrying: {e}",
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
            Err(e) => {
                let _ = ack
                    .send(ControlAck::Error {
                        message: format!("fetch data root {}: {e}", hex::encode(data_ref)),
                    })
                    .await;
                return;
            }
        };

        let Some(total_bytes) = root_span_to_u64_masked(&root) else {
            let _ = ack
                .send(ControlAck::Error {
                    message: format!("data root {} shorter than span", hex::encode(data_ref)),
                })
                .await;
            return;
        };
        tracker.set_total_bytes(total_bytes);

        let filename = lookup
            .metadata
            .get("Filename")
            .cloned()
            .or_else(|| derive_filename_from_path(&path));
        if ack
            .send(ControlAck::BzzStreamStart {
                total_bytes,
                content_type: lookup.content_type,
                filename,
            })
            .await
            .is_err()
        {
            return;
        }

        if head_only {
            let _ = ack.send(ControlAck::StreamDone).await;
            return;
        }

        // Once `BzzStreamStart` has been emitted we own the stream;
        // any further failure terminates with `Error` on the same
        // body channel. The joiner already wraps each subtree in its
        // own retry loop, so transient chunk misses don't leak out.
        let join_options = ant_retrieval::JoinOptions {
            allow_degraded_redundancy,
        };
        stream_root_chunk_inner(
            &fetcher,
            root,
            total_bytes,
            max_bytes,
            range,
            data_ref,
            join_options,
            ack,
        )
        .await;
        return;
    }
    let _ = ack
        .send(ControlAck::Error {
            message: last_error.unwrap_or_else(|| "stream_bzz exhausted retries".to_string()),
        })
        .await;
}

/// Drive a streaming joiner from an already-fetched root chunk and emit
/// `BytesChunk` acks until completion. Used by both `StreamBytes` (which
/// has already sent its `BytesStreamStart`) and `StreamBzz` (which has
/// already sent its `BzzStreamStart`).
async fn stream_root_chunk_with_range(
    fetcher: &ant_retrieval::RoutingFetcher,
    root: Vec<u8>,
    total_bytes: u64,
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    reference: [u8; 32],
    ack: mpsc::Sender<ControlAck>,
) {
    stream_root_chunk_inner(
        fetcher,
        root,
        total_bytes,
        max_bytes,
        range,
        reference,
        ant_retrieval::JoinOptions::default(),
        ack,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn stream_root_chunk_inner(
    fetcher: &ant_retrieval::RoutingFetcher,
    root: Vec<u8>,
    total_bytes: u64,
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    reference: [u8; 32],
    options: ant_retrieval::JoinOptions,
    ack: mpsc::Sender<ControlAck>,
) {
    // 64 × ~4 KiB ≈ 256 KiB of decoded body buffered between the
    // joiner and the control reply pump. This matches bee's small-file
    // lookahead window (`langos` with `smallFileBufferSize` = 8 ×
    // 32 KiB) so a slow-draining HTTP consumer doesn't immediately
    // back-pressure into the joiner's per-child pipes. Combined with
    // the joiner's `STREAM_PIPE_DEPTH` per-child buffers, a healthy
    // download keeps roughly 2-3 MiB of decoded body ahead of the
    // gateway at any given moment.
    let (chunk_tx, mut chunk_rx) = mpsc::channel::<Vec<u8>>(64);
    let cap = clamp_max_bytes(max_bytes);
    let body_range = range.and_then(|r| {
        ant_retrieval::ByteRange::clamp(r.start, r.end_inclusive, total_bytes)
    });
    if range.is_some() && body_range.is_none() {
        let _ = ack
            .send(ControlAck::Error {
                message: "range not satisfiable for resource".to_string(),
            })
            .await;
        return;
    }
    let mut join_fut = Box::pin(ant_retrieval::join_to_sender_range(
        fetcher,
        &root,
        cap,
        options,
        body_range,
        chunk_tx,
    ));
    let mut join_result = None;
    loop {
        tokio::select! {
            result = &mut join_fut, if join_result.is_none() => {
                join_result = Some(result);
            }
            maybe_chunk = chunk_rx.recv() => {
                match maybe_chunk {
                    Some(data) => {
                        if ack.send(ControlAck::BytesChunk { data }).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        match join_result.unwrap_or(Ok(())) {
                            Ok(()) => {
                                let _ = ack.send(ControlAck::StreamDone).await;
                            }
                            Err(e) => {
                                let _ = ack
                                    .send(ControlAck::Error {
                                        message: format!("join {}: {e}", hex::encode(reference)),
                                    })
                                    .await;
                            }
                        }
                        return;
                    }
                }
            }
        }
    }
}

/// How often the progress emitter wakes up and pushes a sample over
/// the ack channel. 150 ms is fast enough to drive a smoothly-updating
/// stderr status line on the client without flooding the wire (or
/// blocking the joiner on a stalled writer).
const PROGRESS_TICK: Duration = Duration::from_millis(150);

/// Convert a tracker snapshot + the request's start instant into the
/// wire-shape `GetProgress`. The tracker itself doesn't track
/// elapsed time (it's a pure counter struct), so we mix it in here.
fn build_progress(tracker: &ProgressTracker, started: Instant) -> GetProgress {
    let sample = tracker.snapshot();
    GetProgress {
        elapsed_ms: started.elapsed().as_millis() as u64,
        chunks_done: sample.chunks_done,
        total_chunks_estimate: sample.total_chunks_estimate,
        bytes_done: sample.bytes_done,
        total_bytes_estimate: sample.total_bytes_estimate,
        cache_hits: sample.cache_hits,
        peers_used: sample.peers_used,
        bypass_cache: sample.bypass_cache,
        in_flight: sample.in_flight,
    }
}

/// Spawn a background ticker that emits one `ControlAck::Progress`
/// per `PROGRESS_TICK` until either the ack channel closes (the
/// connection went away) or the parent task aborts the handle. The
/// emitter exits on the first failed send so a disconnected client
/// doesn't keep us spinning forever.
fn spawn_progress_emitter(
    tracker: Arc<ProgressTracker>,
    ack: mpsc::Sender<ControlAck>,
    started: Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROGRESS_TICK);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first fire — counters are all zero so
        // there's nothing useful to send before the first chunk lands.
        interval.tick().await;
        loop {
            interval.tick().await;
            let progress = build_progress(&tracker, started);
            if ack.send(ControlAck::Progress(progress)).await.is_err() {
                return;
            }
        }
    })
}

/// Best-effort terminal ack send for the rare cases where we know
/// we're going to error out before spawning the worker task (e.g.
/// no peers available). The mpsc channel was minted with capacity
/// 16 so this never actually blocks; spawning a tiny task keeps
/// `handle_control_command` itself synchronous.
fn send_terminal_ack(ack: &mpsc::Sender<ControlAck>, reply: ControlAck) {
    let ack = ack.clone();
    tokio::spawn(async move {
        let _ = ack.send(reply).await;
    });
}

/// Max number of full pipeline attempts (lookup_path + fetch + join)
/// per user-visible `get` command. Successful chunks are written through
/// to the daemon cache, so later attempts usually skip straight to the
/// sparse tail chunks that still need a working peer. Live gateway
/// verification against four concurrent 44 MiB WAVs still saw late
/// single-chunk misses after five attempts; ten attempts gives those
/// tail chunks more peer-set churn windows without re-downloading the
/// already joined prefix.
const MAX_FETCH_ATTEMPTS: usize = 10;

/// Process-wide cap on concurrent `retrieve_chunk` calls (see
/// `SwarmState::retrieval_inflight`). Bee's retrieval has no
/// equivalent — it spawns one goroutine per chunk fetch and lets the
/// Go runtime sort it out. We keep a soft cap so a stuck network
/// can't chew through unbounded yamux streams; 256 is well above
/// what 4–8 concurrent media downloads ever drive at once but caps
/// pathological loops. The per-request cap is what does the actual
/// fairness work between requests.
const RETRIEVAL_INFLIGHT_CAP: usize = 256;

/// Per-user-request cap layered in front of the process-wide semaphore.
/// Sized to roughly match bee's effective chunk-level parallelism for
/// a single file (one langos lookahead window of ~256 KiB ≈ 64 chunks)
/// while keeping per-peer chunk rate inside the headroom that the
/// pseudosettle driver can clear. Going higher (e.g. 128) makes a
/// fresh peer set drain faster on a single file but punches through
/// the per-peer debt allowance for sustained multi-file streaming —
/// the hottest peers see ≥10× their share when chunk addresses cluster
/// around a small neighbourhood, and the pseudosettle refresh budget
/// per peer (`450k units/s × MIN_REFRESH_INTERVAL` = 900k units / 2 s)
/// caps the steady-state per-peer fetch rate at ~12 chunks / 2 s.
const RETRIEVAL_REQUEST_INFLIGHT_CAP: usize = 64;

/// Multiplier base for inter-attempt backoff. Attempt N waits
/// `RETRY_BACKOFF_BASE * N` before retrying — small enough not to
/// noticeably stretch the user's wait, large enough that we don't
/// re-flood the same overloaded forwarders the moment they shed us.
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(500);

/// One pipeline attempt's outcome on failure. `Transient` failures
/// (chunk fetch errors from peer / network flakes) should trigger a
/// retry; `Permanent` failures (corrupt data, "not a manifest", path
/// not found, file-too-large, …) won't get better with another try
/// and surface to the user immediately.
enum AttemptError {
    Transient(String),
    Permanent(String),
}

/// Did this `JoinError` come from a chunk-fetch failure? The other
/// variants (oversize span, malformed intermediate, RS / encryption)
/// describe data-shape problems that would re-fail identically on a
/// retry, so we mark them permanent.
fn is_join_transient(e: &ant_retrieval::JoinError) -> bool {
    matches!(e, ant_retrieval::JoinError::FetchChunk { .. })
}

/// Did this `ManifestError` ultimately come from a chunk-fetch failure
/// during the trie walk? The wire/encoding variants (bad version hash,
/// invalid metadata, descend-past-leaf, …) and the lookup-miss are all
/// terminal.
fn is_manifest_transient(e: &ant_retrieval::ManifestError) -> bool {
    matches!(
        e,
        ant_retrieval::ManifestError::Fetch(inner) if is_join_transient(inner)
    )
}

/// Spawned task body for `Request::GetBytes`. Fetches the root chunk via
/// `RoutingFetcher`, then hands it to `ant_retrieval::join`. Pulled out
/// of `handle_control_command` so the latter stays small and the async
/// pieces are exercise-able without spinning up a swarm.
///
/// Wraps the actual pipeline in a retry loop: a fresh `RoutingFetcher`
/// per attempt with a **clean blacklist** *and* a fresh peer snapshot
/// pulled from `peers_rx` at attempt-start. So peers that were
/// transiently unreachable on attempt N (dropped libp2p connection,
/// tail-slow forward, momentarily no dial address) drop out of the
/// candidate list automatically once `ConnectionClosed` fires —
/// instead of sitting in a frozen list and giving us "connection is
/// closed" / "oneshot canceled" errors over and over until we exhaust
/// `MAX_FETCH_ATTEMPTS`. Bee's retrieval works the same way: skip-lists
/// are scoped to a single retrieval, never to the whole client session,
/// and the candidate set comes from the live forwarding-Kademlia table.
///
/// This is the core of the fix for "browser stalls on a multi-MiB media
/// file": both the blacklist carry-forward *and* the frozen peer snapshot
/// pushed us into "no BZZ peers available" or "connection is closed"
/// by attempt 2 — every peer we'd ever flubbed got banned permanently
/// for the rest of the request, and even un-flubbed-but-disconnected
/// peers stayed in the rotation forever.
// Eight args is over clippy's preferred ceiling, but bundling shared
// retrieval plumbing with request-scoped knobs into a struct just for
// one call site hides more than it helps.
#[allow(clippy::too_many_arguments)]
async fn run_get_bytes(
    control: Control,
    peers_rx: watch::Receiver<Vec<(libp2p::PeerId, [u8; 32])>>,
    cache: Arc<ant_retrieval::InMemoryChunkCache>,
    record_dir: Option<PathBuf>,
    inflight_limit: Arc<Semaphore>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
    accounting: Option<Arc<ant_retrieval::accounting::Accounting>>,
    counters: Arc<RetrievalCounters>,
    tracker: Arc<ProgressTracker>,
    reference: [u8; 32],
    max_bytes: Option<u64>,
) -> ControlAck {
    for attempt in 1..=MAX_FETCH_ATTEMPTS {
        if peers_rx.borrow().is_empty() {
            return ControlAck::Error {
                message: "no peers available; wait for handshakes to complete".to_string(),
            };
        }
        // Pass the watch receiver itself, not a snapshot: the fetcher's
        // per-chunk `ranked()` call rereads it so peers that disconnect
        // during the attempt drop out of subsequent chunks' candidate
        // pools immediately.
        let mut builder = ant_retrieval::RoutingFetcher::new(control.clone(), peers_rx.clone())
            .with_cache(cache.clone())
            .with_record_dir(record_dir.clone())
            .with_progress(tracker.clone())
            .with_inflight_limit(inflight_limit.clone())
            .with_counters(counters.clone())
            .with_request_inflight_limit(RETRIEVAL_REQUEST_INFLIGHT_CAP);
        if let Some(tx) = payment_notify.clone() {
            builder = builder.with_payment_notify(tx);
        }
        if let Some(acc) = accounting.clone() {
            builder = builder.with_accounting(acc);
        }
        let fetcher = builder;
        match try_get_bytes(&fetcher, &tracker, reference, max_bytes).await {
            Ok(data) => {
                debug!(
                    target: "ant_p2p",
                    root = %hex::encode(reference),
                    bytes = data.len(),
                    attempt,
                    "joiner produced file",
                );
                return ControlAck::Bytes { data };
            }
            Err(AttemptError::Permanent(message)) => {
                return ControlAck::Error { message };
            }
            Err(AttemptError::Transient(message)) if attempt < MAX_FETCH_ATTEMPTS => {
                let backoff = RETRY_BACKOFF_BASE * attempt as u32;
                warn!(
                    target: "ant_p2p",
                    attempt,
                    next_in_ms = backoff.as_millis() as u64,
                    "get_bytes failed, retrying: {message}",
                );
                tokio::time::sleep(backoff).await;
            }
            Err(AttemptError::Transient(message)) => {
                return ControlAck::Error { message };
            }
        }
    }
    unreachable!("retry loop exits via return on the final attempt");
}

/// One attempt at the get-bytes pipeline. Splits per-error-site
/// classification away from the retry policy in `run_get_bytes`.
async fn try_get_bytes(
    fetcher: &ant_retrieval::RoutingFetcher,
    tracker: &ProgressTracker,
    reference: [u8; 32],
    max_bytes: Option<u64>,
) -> Result<Vec<u8>, AttemptError> {
    use ant_retrieval::{join, ChunkFetcher};

    let root = fetcher.fetch(reference).await.map_err(|e| {
        AttemptError::Transient(format!("fetch root chunk {}: {e}", hex::encode(reference)))
    })?;
    // The root chunk's span is the total file size (in bytes) of the
    // BMT-built tree below it, so we can finalize the progress totals
    // as soon as the root lands and the rest of the joiner can drive
    // the chunks_done counter against a real denominator.
    if let Some(total) = root_span_to_u64(&root) {
        tracker.set_total_bytes(total);
    }
    let cap = clamp_max_bytes(max_bytes);
    join(fetcher, &root, cap).await.map_err(|e| {
        let message = format!("join {}: {e}", hex::encode(reference));
        if is_join_transient(&e) {
            AttemptError::Transient(message)
        } else {
            AttemptError::Permanent(message)
        }
    })
}

/// Decode the 8-byte little-endian BMT span prefix from a wire-format
/// CAC chunk (`span || payload`) into a `u64` byte length. For an
/// intermediate chunk this is the total span its subtree covers; for
/// the root it's the final file size. Returns `None` if `wire` is
/// somehow shorter than the span prefix (shouldn't happen with
/// CAC-verified output, but cheap to guard).
fn root_span_to_u64(wire: &[u8]) -> Option<u64> {
    let span: [u8; 8] = wire.get(..8)?.try_into().ok()?;
    Some(u64::from_le_bytes(span))
}

/// Like [`root_span_to_u64`] but masks the redundancy level byte off the
/// span before decoding. Bee stores the RS level in the high byte of the
/// chunk's 8-byte span (top of the range, since a real file's span fits
/// in 56 bits / 72 PB). Reads that go through the joiner with
/// `allow_degraded_redundancy` set already mask, but the streaming path
/// also reports the file's total span over the wire (`BytesStreamStart`,
/// `BzzStreamStart`), so we need the masked value there too — otherwise
/// the gateway emits a `Content-Length` of `0x82_<...>` instead of the
/// real file size, and HEAD / Content-Range responses look like the
/// file is 9 exabytes long.
fn root_span_to_u64_masked(wire: &[u8]) -> Option<u64> {
    let mut span: [u8; 8] = wire.get(..8)?.try_into().ok()?;
    span[7] = 0;
    Some(u64::from_le_bytes(span))
}

#[allow(clippy::too_many_arguments)]
async fn run_list_bzz(
    control: Control,
    peers_rx: watch::Receiver<Vec<(libp2p::PeerId, [u8; 32])>>,
    cache: Arc<ant_retrieval::InMemoryChunkCache>,
    record_dir: Option<PathBuf>,
    inflight_limit: Arc<Semaphore>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
    accounting: Option<Arc<ant_retrieval::accounting::Accounting>>,
    counters: Arc<RetrievalCounters>,
    reference: [u8; 32],
) -> ControlAck {
    if peers_rx.borrow().is_empty() {
        return ControlAck::Error {
            message: "no peers available; wait for handshakes to complete".to_string(),
        };
    }

    let mut builder = ant_retrieval::RoutingFetcher::new(control, peers_rx)
        .with_cache(cache)
        .with_record_dir(record_dir)
        .with_inflight_limit(inflight_limit)
        .with_counters(counters)
        .with_request_inflight_limit(RETRIEVAL_REQUEST_INFLIGHT_CAP);
    if let Some(tx) = payment_notify {
        builder = builder.with_payment_notify(tx);
    }
    if let Some(acc) = accounting {
        builder = builder.with_accounting(acc);
    }
    let fetcher = builder;

    match ant_retrieval::list_manifest(&fetcher, reference).await {
        Ok(entries) => ControlAck::Manifest {
            entries: entries
                .into_iter()
                .map(|entry| ant_control::ManifestEntryInfo {
                    path: entry.path,
                    reference: entry.reference.map(hex::encode),
                    metadata: entry.metadata,
                })
                .collect(),
        },
        Err(e) => ControlAck::Error {
            message: format!("list manifest {}: {e}", hex::encode(reference)),
        },
    }
}

/// Resolve the optional per-request `max_bytes` override into the
/// `usize` the joiner expects. `None` falls back to the conservative
/// `DEFAULT_MAX_FILE_BYTES` (32 MiB), keeping `antctl get` safe by
/// default. The HTTP gateway raises this so real bzz sites with media
/// payloads don't 502 at the joiner step. Saturating cast keeps a
/// hypothetical `u64` value larger than `usize::MAX` — only possible on
/// 32-bit targets — from wrapping to a tiny cap silently.
fn clamp_max_bytes(max_bytes: Option<u64>) -> usize {
    use ant_retrieval::DEFAULT_MAX_FILE_BYTES;
    match max_bytes {
        Some(n) => usize::try_from(n).unwrap_or(usize::MAX),
        None => DEFAULT_MAX_FILE_BYTES,
    }
}

/// Spawned task body for `Request::GetBzz`. Walks the manifest at
/// `reference`, resolves `path`, then runs the joiner against the
/// resolved data ref. Wrapped in the same retry loop as
/// [`run_get_bytes`].
// Nine args is over clippy's preferred ceiling, but the alternative
// (bundling shared retrieval plumbing with request-scoped knobs into a
// struct just for one call site) hides more than it helps.
#[allow(clippy::too_many_arguments)]
async fn run_get_bzz(
    control: Control,
    peers_rx: watch::Receiver<Vec<(libp2p::PeerId, [u8; 32])>>,
    cache: Arc<ant_retrieval::InMemoryChunkCache>,
    record_dir: Option<PathBuf>,
    inflight_limit: Arc<Semaphore>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
    accounting: Option<Arc<ant_retrieval::accounting::Accounting>>,
    counters: Arc<RetrievalCounters>,
    tracker: Arc<ProgressTracker>,
    reference: [u8; 32],
    path: String,
    allow_degraded_redundancy: bool,
    max_bytes: Option<u64>,
) -> ControlAck {
    for attempt in 1..=MAX_FETCH_ATTEMPTS {
        if peers_rx.borrow().is_empty() {
            return ControlAck::Error {
                message: "no peers available; wait for handshakes to complete".to_string(),
            };
        }
        // Pass the watch receiver itself: see `run_get_bytes` for
        // rationale on per-chunk peer freshness.
        let mut builder = ant_retrieval::RoutingFetcher::new(control.clone(), peers_rx.clone())
            .with_cache(cache.clone())
            .with_record_dir(record_dir.clone())
            .with_progress(tracker.clone())
            .with_inflight_limit(inflight_limit.clone())
            .with_counters(counters.clone())
            .with_request_inflight_limit(RETRIEVAL_REQUEST_INFLIGHT_CAP);
        if let Some(tx) = payment_notify.clone() {
            builder = builder.with_payment_notify(tx);
        }
        if let Some(acc) = accounting.clone() {
            builder = builder.with_accounting(acc);
        }
        let fetcher = builder;
        match try_get_bzz(
            &fetcher,
            &tracker,
            reference,
            &path,
            allow_degraded_redundancy,
            max_bytes,
        )
        .await
        {
            Ok((data, content_type, filename)) => {
                return ControlAck::BzzBytes {
                    data,
                    content_type,
                    filename,
                };
            }
            Err(AttemptError::Permanent(message)) => {
                return ControlAck::Error { message };
            }
            Err(AttemptError::Transient(message)) if attempt < MAX_FETCH_ATTEMPTS => {
                let backoff = RETRY_BACKOFF_BASE * attempt as u32;
                warn!(
                    target: "ant_p2p",
                    attempt,
                    next_in_ms = backoff.as_millis() as u64,
                    "get_bzz failed, retrying: {message}",
                );
                tokio::time::sleep(backoff).await;
            }
            Err(AttemptError::Transient(message)) => {
                return ControlAck::Error { message };
            }
        }
    }
    unreachable!("retry loop exits via return on the final attempt");
}

/// One attempt at the get-bzz pipeline. Returns the resolved
/// `(data, content_type, filename)` tuple on success.
#[allow(clippy::too_many_arguments)]
async fn try_get_bzz(
    fetcher: &ant_retrieval::RoutingFetcher,
    tracker: &ProgressTracker,
    reference: [u8; 32],
    path: &str,
    allow_degraded_redundancy: bool,
    max_bytes: Option<u64>,
) -> Result<(Vec<u8>, Option<String>, Option<String>), AttemptError> {
    use ant_retrieval::{join_with_options, lookup_path, ChunkFetcher, JoinOptions, ManifestError};

    let lookup = match lookup_path(fetcher, reference, path).await {
        Ok(r) => r,
        Err(ManifestError::NotAManifest) => {
            return Err(AttemptError::Permanent(format!(
                "{} is not a mantaray manifest; try `antctl get` (raw chunk) or fetch via /bytes",
                hex::encode(reference)
            )));
        }
        Err(e) => {
            let message = format!("manifest lookup '{path}': {e}");
            return Err(if is_manifest_transient(&e) {
                AttemptError::Transient(message)
            } else {
                AttemptError::Permanent(message)
            });
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

    // Reuse the same fetcher so the blacklist / peer ordering carries
    // over from the manifest walk into the file-body fetch.
    let root = fetcher.fetch(data_ref).await.map_err(|e| {
        AttemptError::Transient(format!("fetch data root {}: {e}", hex::encode(data_ref)))
    })?;
    // Manifest chunks already showed up in the tracker as raw bytes;
    // we only learn the *file*'s size once the data root lands. Set
    // it now so the client's progress bar gets a real denominator
    // for the second half of the request.
    if let Some(total) = root_span_to_u64(&root) {
        tracker.set_total_bytes(total);
    }
    let join_options = JoinOptions {
        allow_degraded_redundancy,
    };
    let cap = clamp_max_bytes(max_bytes);
    let data = join_with_options(fetcher, &root, cap, join_options)
        .await
        .map_err(|e| {
            let message = format!("join data {}: {e}", hex::encode(data_ref));
            if is_join_transient(&e) {
                AttemptError::Transient(message)
            } else {
                AttemptError::Permanent(message)
            }
        })?;

    let filename = lookup
        .metadata
        .get("Filename")
        .cloned()
        .or_else(|| derive_filename_from_path(path));
    Ok((data, lookup.content_type, filename))
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
            state.ready_in_ms.remove(&peer_id);
            // Drop the routing entry alongside `bzz_peers`; otherwise the
            // forwarder would keep picking a peer we have no live
            // connection to.
            state.routing.forget(&peer_id);
            // Drop the accounting mirror for this peer too: bee's
            // `notifyPeerConnect` resets the `accountingPeer` on
            // reconnect, so our balance must reset alongside.
            // Without this, a peer that flapped the connection would
            // come back in our peerstore with stale debt and we'd
            // refuse to dispatch to it for ~1 s for no reason.
            if let Some(acc) = state.accounting.as_ref() {
                acc.forget(&peer_id);
            }
            state.publish_peers();
            if was_bzz {
                info!(
                    target: "ant_p2p",
                    %peer_id,
                    cause = ?cause,
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
            if let Some(p) = pipeline_ms.as_ref() {
                let total = p.dial_ms.unwrap_or(0) + p.identifying_ms + p.handshake_ms;
                state.ready_in_ms.insert(peer, total);
            }
            // The remote_overlay was just verified by `handshake_outbound`
            // (signature recovers to an Ethereum address whose overlay
            // matches the declared one), so admitting it is safe.
            state.routing.admit(peer, info.remote_overlay);
            state.publish_peers();
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

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    fn pid() -> PeerId {
        Keypair::generate_ed25519().public().to_peer_id()
    }

    /// Pin the contract that `publish_peers` actually republishes the
    /// current routing-table snapshot on `peers_watch`. The retry loop
    /// in `run_get_bytes` / `run_get_bzz` reads from this watch on every
    /// attempt, so silently breaking publication (e.g. by forgetting to
    /// call it after an admit / forget) reverts us to the frozen-snapshot
    /// regression that produced the "io: <peer>/N: connection is closed"
    /// 502s on multi-MiB media files.
    #[test]
    fn publish_peers_reflects_admit_and_forget() {
        let mut state = SwarmState::new(32, [0u8; 32], false, None, None);
        let mut rx = state.peers_watch.subscribe();
        // Initial state: empty.
        assert!(rx.borrow_and_update().is_empty());

        let p1 = pid();
        let o1 = [1u8; 32];
        state.routing.admit(p1, o1);
        state.publish_peers();
        let v = rx.borrow_and_update().clone();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0], (p1, o1));

        let p2 = pid();
        let o2 = [2u8; 32];
        state.routing.admit(p2, o2);
        state.publish_peers();
        let v = rx.borrow_and_update().clone();
        assert_eq!(v.len(), 2, "two peers visible after second admit");
        assert!(v.contains(&(p1, o1)));
        assert!(v.contains(&(p2, o2)));

        state.routing.forget(&p1);
        state.publish_peers();
        let v = rx.borrow_and_update().clone();
        assert_eq!(v.len(), 1, "only the un-forgotten peer remains");
        assert_eq!(v[0], (p2, o2));
    }
}
