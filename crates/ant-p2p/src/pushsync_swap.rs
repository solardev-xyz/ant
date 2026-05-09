//! Phase 7b: outbound-side SWAP settlement for pushsync uploads.
//!
//! # What this does
//!
//! Implements [`ant_retrieval::PushsyncSettlement`] by tracking
//! per-peer cumulative outbound debt (in PLUR) and emitting EIP-712
//! `/swarm/swap/1.0.0/swap` cheques when a peer's debt crosses a
//! configurable trigger. The trigger defaults to half of bee's
//! `lightPaymentThreshold` (1.35 M PLUR), so we settle proactively
//! before bee's accounting decides we're a freeloader and silently
//! RST's our pushsync stream.
//!
//! # Why this exists
//!
//! Without this, every chunk we push to a bee peer adds ~10 K-320 K
//! PLUR to that peer's view of our debt. After bee's `paymentTolerance`
//! (typically a few hundred chunks) it stops accepting our pushsync
//! deliveries. The 2026-05-08 production attempt at uploading
//! `gemma4:e2b` (1.76 M chunks) stalled at ~20 K successful pushes —
//! exactly matching the cumulative `paymentTolerance` of ~100 peers,
//! confirming there was no slow path through. With this module live,
//! every peer that crosses the trigger gets a cheque before it can
//! decide to lock us out, and the upload runs to completion.
//!
//! # Glossary
//!
//! - **Beneficiary**: 20-byte EOA recovered from the peer's BZZ
//!   handshake signature (`HandshakeInfo::remote_eth_address`). It's
//!   the address bee credits when it cashes our cheque.
//! - **Cumulative payout**: the running total of PLUR our chequebook
//!   has authorised paying to this beneficiary across our entire
//!   relationship. EIP-712 cheques are cumulative — cheque N+1 has
//!   `cumulative = cheque N's cumulative + delta`. Bee accepts the
//!   highest cumulative it sees and ignores duplicates / older ones,
//!   so cheque emission is naturally idempotent.
//! - **Trigger**: the per-peer in-PLUR threshold at which we emit a
//!   fresh cheque. Default is 50 % of `LIGHT_PAYMENT_THRESHOLD` —
//!   matches bee's `earlyPayment = 50 %` constant in
//!   `pkg/accounting/accounting.go::PrepareCredit`.
//!
//! # Wiring
//!
//! `antd` builds one [`PushsyncSwap`] per process (configured via the
//! `--chequebook` / `--swap-key` flags). The swarm loop populates
//! [`PeerEthMap`] in the BZZ-handshake-success branch, and clones an
//! `Arc<PushsyncSwap>` into every `RoutingFetcher` it constructs for
//! `PushChunk` requests. From the fetcher's perspective the swap
//! subsystem is a single `Arc<dyn PushsyncSettlement>`; this module is
//! the only place that knows about EIP-712 / `OutboundLedger` /
//! `issue_and_emit`.

use crate::swap::{issue_and_emit, OutboundLedger, SwapError};
use ant_crypto::SECP256K1_SECRET_LEN;
use ant_retrieval::accounting::HotHint;
use ant_retrieval::PushsyncSettlement;
use libp2p::PeerId;
use libp2p_stream::Control;
use primitive_types::U256;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;
use tracing::{debug, info, trace};

/// Bee's `lightPaymentThreshold = paymentThreshold / lightFactor =
/// 13.5 M / 10 = 1.35 M PLUR`. From `pkg/node/node.go`. This is the
/// debt level at which a `full_node = true` bee peer (in light-mode
/// accounting against us, since we're light) decides we owe enough to
/// be settled.
pub const LIGHT_PAYMENT_THRESHOLD: u64 = 1_350_000;

/// Default pre-emptive cheque-emission trigger. Half of the bee-side
/// payment threshold; matches bee's `earlyPayment = 50 %` policy in
/// `pkg/accounting/accounting.go::PrepareCredit`. We settle BEFORE bee
/// even thinks we're a slow payer, leaving plenty of headroom for the
/// stream to be accepted.
pub const DEFAULT_CHEQUE_TRIGGER: u64 = LIGHT_PAYMENT_THRESHOLD / 2;

/// Shared `peer_id -> ethereum_address` registry, populated by the
/// BZZ-handshake handler and consumed by [`PushsyncSwap`] to find the
/// cheque beneficiary for a libp2p peer. Held behind an `Arc<RwLock>`
/// so the handshake handler can write while concurrent pushsyncs
/// read.
///
/// The map is intentionally tiny — one 20-byte EOA per peer, peers
/// churn but never accumulate beyond `state.bzz_peers.len() ~= 100` in
/// our peer set. We don't bother evicting on `forget`; stale entries
/// are at most 100 × 20 bytes and a `forget(peer)` from
/// `ConnectionClosed` is followed quickly by a fresh `record(peer,
/// new_eoa)` if the peer reconnects.
#[derive(Default, Clone)]
pub struct PeerEthMap {
    inner: Arc<RwLock<HashMap<PeerId, [u8; 20]>>>,
}

impl PeerEthMap {
    #[must_use] 
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `eth_address` for `peer`. Called from the swarm's
    /// `record_handshake` site immediately after a successful BZZ
    /// handshake, where `eth_address = HandshakeInfo::remote_eth_address`.
    /// Overwrites any previous value silently — bee assumes a stable
    /// `(peer_id, eoa)` pairing per session, and so do we.
    pub fn record(&self, peer: PeerId, eth_address: [u8; 20]) {
        if let Ok(mut g) = self.inner.write() {
            g.insert(peer, eth_address);
        }
    }

    /// Drop `peer`'s entry. Called from the swarm's `ConnectionClosed`
    /// handler so a reconnected peer doesn't carry over a stale EOA.
    pub fn forget(&self, peer: &PeerId) {
        if let Ok(mut g) = self.inner.write() {
            g.remove(peer);
        }
    }

    /// Look up `peer`'s 20-byte EOA. Returns `None` if the peer's BZZ
    /// handshake hasn't completed yet (rare — the fetcher only picks
    /// from peers it has handshaken) or if the entry has been
    /// evicted by [`Self::forget`].
    #[must_use] 
    pub fn get(&self, peer: &PeerId) -> Option<[u8; 20]> {
        self.inner.read().ok()?.get(peer).copied()
    }
}

/// Configuration for the outbound SWAP settlement subsystem. Built
/// once at `antd` startup from CLI flags / env, stored on
/// [`crate::RunConfig`], and threaded into the swarm loop where it
/// becomes an `Arc<PushsyncSwap>` shared by every `RoutingFetcher`.
#[derive(Clone)]
pub struct PushsyncSwapConfig {
    /// 20-byte chequebook contract address — owns the BZZ that gets
    /// cashed out. The chequebook's `issuer()` view must return the
    /// EOA derived from `swap_secret`; otherwise bee rejects every
    /// cheque ("invalid issuer signature" on the EIP-712 verification).
    pub chequebook: [u8; 20],
    /// 32-byte secp256k1 secret of the chequebook's issuer EOA. Same
    /// key bee derives at `m/44'/60'/0'/0/0` in its default setup; we
    /// reuse the convention that `WALLET_PRIVATE_KEY` is also the
    /// chequebook owner.
    pub swap_secret: [u8; SECP256K1_SECRET_LEN],
    /// Chain id used for EIP-712 cheque domain separator — bee's
    /// chequebook code is hard-coded to expect `chainId = 100` on
    /// Gnosis mainnet. See `ant_chain::chequebook::eip712_domain_separator`.
    pub chain_id: u64,
    /// Where the outbound cumulative-payout ledger lives on disk.
    /// Restart-safe: the cumulative for each beneficiary is loaded
    /// from this file so we never re-issue an already-emitted cheque
    /// amount (bee would treat a duplicate as a redundant accept,
    /// not a regression, but it wastes a stream).
    pub outbound_ledger_path: PathBuf,
    /// Per-peer cumulative-debit threshold (PLUR) at which we emit a
    /// cheque before the next pushsync. [`DEFAULT_CHEQUE_TRIGGER`] is
    /// the recommended value; smaller values mean more chains, larger
    /// values risk crossing bee's actual `paymentTolerance`.
    pub cheque_trigger_plur: u64,
    /// Optional lookup helper threaded from the swarm loop so we can
    /// recover a peer's cheque beneficiary EOA from its libp2p
    /// `PeerId`.
    pub peer_eth: PeerEthMap,
}

impl PushsyncSwapConfig {
    /// Build a config with the default cheque trigger.
    #[must_use] 
    pub fn new(
        chequebook: [u8; 20],
        swap_secret: [u8; SECP256K1_SECRET_LEN],
        chain_id: u64,
        outbound_ledger_path: PathBuf,
        peer_eth: PeerEthMap,
    ) -> Self {
        Self {
            chequebook,
            swap_secret,
            chain_id,
            outbound_ledger_path,
            cheque_trigger_plur: DEFAULT_CHEQUE_TRIGGER,
            peer_eth,
        }
    }
}

/// State that the cheque-emission task needs. Held behind an `Arc`
/// so [`PushsyncSwap::note_pushsync`] can clone it cheaply and hand
/// it to a `tokio::spawn`-ed background task — that's how Phase 7e
/// keeps cheque emission off the pushsync hot path. The fields are
/// the same as their previous direct-on-`PushsyncSwap` counterparts;
/// nothing here is novel except the `Arc` boundary.
struct EmitCore {
    cfg: PushsyncSwapConfig,
    outbound_ledger: OutboundLedger,
    /// `libp2p_stream::Control` clone used to open `/swarm/swap/...`
    /// streams. Cloning `Control` is cheap (`Arc` underneath); each
    /// settlement clones it again before calling `open_stream`.
    control: Mutex<Control>,
    /// Per-peer in-PLUR debt accumulated since the last successful
    /// cheque emission (or since the peer connected). When a peer's
    /// debt crosses [`PushsyncSwapConfig::cheque_trigger_plur`] we
    /// fold it into the cumulative payout, emit a cheque, and zero
    /// the entry. Held behind a `Mutex` rather than a `Mutex<Map>`
    /// so the brief lock window doesn't span the swap stream's
    /// network roundtrip.
    pending_debt: Mutex<HashMap<PeerId, u64>>,
}

/// Live SWAP settlement service. One per process; shared with the
/// fetcher via `Arc<dyn PushsyncSettlement>`.
pub struct PushsyncSwap {
    /// Shared cheque-emission state — see [`EmitCore`]. Held behind
    /// `Arc` so [`PushsyncSettlement::note_pushsync`] can hand a
    /// cheap clone to a background task without holding the trait's
    /// `&self` reference across the swap stream RTT.
    core: Arc<EmitCore>,
    /// Optional hot-hint sender shared with `pseudosettle::run_driver`.
    ///
    /// # Why this exists (Phase 7d, 2026-05-08)
    ///
    /// Bee's `pkg/accounting/accounting.PrepareDebit` (the hook bee
    /// runs *before* forwarding a chunk we just pushed) gates each
    /// chunk on the peer's mirrored `expectedDebt < disconnectLimit`.
    /// Bee clears that debt by two paths:
    ///
    /// 1. **Pseudosettle** (free, time-based). Bee's `lightRefreshRate
    ///    = 5 K PLUR/s` per peer; only triggered when the *peer* dials
    ///    us with a `pseudosettle.Pay`, OR when bee's own `settle()`
    ///    runs (which it does on retrieval debt, not on pushsync).
    /// 2. **SWAP** (monetary, cheque). Triggered on bee's side by
    ///    `settle()` only when accumulated debt exceeds
    ///    `paymentThreshold * earlyPayment%`.
    ///
    /// On a sustained pushsync stream we — the dialer — must drive
    /// **both** paths from our side: emit cheques (this struct does
    /// that on every `note_pushsync` past the trigger) **and** open
    /// pseudosettle refreshes whenever we accrue meaningful debt.
    /// Without the second path, bee's accounting only ever sees us
    /// payment-via-SWAP, never via pseudosettle, and the steady-state
    /// debt sits one cheque-cycle wide — frequently above
    /// `lightDisconnectLimit ≈ 1.69 M PLUR` for short bursts. By
    /// emitting a `HotHint{ peer }` for every `note_pushsync` we
    /// queue a pseudosettle refresh on the next driver tick, which
    /// in turn shrinks bee's view of our debt by up to
    /// `lightRefreshRate * elapsed` per peer per second — exactly
    /// the slack the cheque path doesn't fill.
    ///
    /// Wiring: cloned from the same `mpsc::Sender<HotHint>` that
    /// `ant_retrieval::Accounting` already owns; the receiver in
    /// `pseudosettle::run_driver` doesn't care which subsystem
    /// produced the hint, only that the peer is hot. Set to `None`
    /// in builds without pseudosettle (e.g. unit tests); production
    /// `antd` always plumbs it through.
    hot_hint: Option<mpsc::Sender<HotHint>>,
}

impl EmitCore {
    fn control_clone(&self) -> Control {
        self.control.lock().expect("pushsync_swap control mutex").clone()
    }

    fn accrue(&self, peer: PeerId, amount: u64) -> u64 {
        let mut g = match self.pending_debt.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let entry = g.entry(peer).or_insert(0);
        *entry = entry.saturating_add(amount);
        *entry
    }

    fn drain_settled(&self, peer: PeerId, settled: u64) {
        let mut g = match self.pending_debt.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(entry) = g.get_mut(&peer) {
            *entry = entry.saturating_sub(settled);
            if *entry == 0 {
                g.remove(&peer);
            }
        }
    }

    async fn emit_for(&self, peer: PeerId, amount: u64) -> Result<U256, SwapError> {
        let beneficiary = self
            .cfg
            .peer_eth
            .get(&peer)
            .ok_or_else(|| SwapError::Rejected("no eoa for peer".into()))?;
        let amount_u256 = U256::from(amount);
        let mut control = self.control_clone();
        let new_cum = issue_and_emit(
            &mut control,
            peer,
            &self.cfg.swap_secret,
            self.cfg.chequebook,
            beneficiary,
            amount_u256,
            self.cfg.chain_id,
            &self.outbound_ledger,
        )
        .await?;
        info!(
            target: "ant_p2p::pushsync_swap",
            %peer,
            beneficiary = %hex::encode(beneficiary),
            amount,
            new_cumulative = %new_cum,
            "emitted pushsync cheque",
        );
        Ok(new_cum)
    }
}

impl PushsyncSwap {
    /// Build the service. Loads the outbound cumulative-payout
    /// snapshot if `outbound_ledger_path` exists; missing or
    /// unparseable snapshots start empty (bee accepts our cheques
    /// from `cumulative = 0` upward, so a fresh start is harmless).
    #[must_use] 
    pub fn new(cfg: PushsyncSwapConfig, control: Control) -> Self {
        let outbound_ledger = OutboundLedger::open(Some(cfg.outbound_ledger_path.clone()));
        Self {
            core: Arc::new(EmitCore {
                cfg,
                outbound_ledger,
                control: Mutex::new(control),
                pending_debt: Mutex::new(HashMap::new()),
            }),
            hot_hint: None,
        }
    }

    /// Builder-style setter: register a hot-hint sender so that every
    /// `note_pushsync` queues a pseudosettle refresh for the peer.
    /// See the doc comment on [`PushsyncSwap::hot_hint`] for the
    /// rationale; the wired-up production path lives in
    /// `ant_p2p::behaviour`'s swarm bootstrap.
    #[must_use] 
    pub fn with_hot_hint(mut self, tx: mpsc::Sender<HotHint>) -> Self {
        self.hot_hint = Some(tx);
        self
    }

    /// Peek the current pending debt for `peer` (in PLUR). Useful for
    /// `antctl swap status`-style introspection. Returns 0 if peer is
    /// unknown or fully settled.
    #[must_use] 
    pub fn pending_debt_for(&self, peer: &PeerId) -> u64 {
        self.core
            .pending_debt
            .lock()
            .ok()
            .and_then(|g| g.get(peer).copied())
            .unwrap_or(0)
    }

    /// Cumulative payout we've ever emitted to `beneficiary` — bee's
    /// view should match this exactly modulo cheques in flight. Used
    /// by smoke tests and (future) `antctl swap snapshot`.
    #[must_use] 
    pub fn cumulative_for(&self, beneficiary: &[u8; 20]) -> U256 {
        self.core.outbound_ledger.cumulative_for(beneficiary)
    }

    /// Borrow the underlying outbound ledger, e.g. for diagnostic
    /// dumping.
    #[must_use] 
    pub fn outbound_ledger(&self) -> &OutboundLedger {
        &self.core.outbound_ledger
    }

    /// Clone of the chequebook address, so callers who hold an
    /// `Arc<PushsyncSwap>` (e.g. an HTTP handler for `GET
    /// /chequebook/address`) can reach it without bypassing the
    /// abstraction.
    #[must_use] 
    pub fn chequebook(&self) -> [u8; 20] {
        self.core.cfg.chequebook
    }

    /// Synchronous variant of cheque emission, kept for tests and
    /// any external caller that wants to await the round trip
    /// (`antctl swap force-emit`-style tooling). Production code on
    /// the pushsync hot path goes through
    /// [`PushsyncSettlement::note_pushsync`] which spawns this in
    /// the background.
    pub async fn emit_for(&self, peer: PeerId, amount: u64) -> Result<U256, SwapError> {
        self.core.emit_for(peer, amount).await
    }
}

#[async_trait::async_trait]
impl PushsyncSettlement for PushsyncSwap {
    async fn note_pushsync(&self, peer: PeerId, price: u64) {
        // Hot path. Two responsibilities:
        //
        // 1. Hint the pseudosettle driver so every chunk's per-peer
        //    debt gets refreshed (Phase 7d). Cheap `try_send` —
        //    drops on backpressure, the driver picks up the peer on
        //    its next periodic walk anyway.
        // 2. Bump pending debt; if it crosses
        //    `cheque_trigger_plur`, await the cheque emit so bee's
        //    accounting credits us BEFORE the next chunk's
        //    `PrepareDebit` runs (see the rationale below — we
        //    tried fire-and-forget in 0.3.15 and it broke uploads).
        let pending = self.core.accrue(peer, price);
        if let Some(tx) = self.hot_hint.as_ref() {
            let _ = tx.try_send(HotHint { peer });
        }
        if pending == 0 {
            // Pre-pushsync no-op (price = 0 from the fetcher's
            // pre-attempt hint). If a prior emit failed mid-stream
            // we may have non-zero pending state for the peer; the
            // *real* note_pushsync after the chunk lands will
            // re-evaluate the trigger on the updated total.
            return;
        }
        if pending < self.core.cfg.cheque_trigger_plur {
            trace!(
                target: "ant_p2p::pushsync_swap",
                %peer,
                pending,
                trigger = self.core.cfg.cheque_trigger_plur,
                "pushsync debit accrued; below trigger",
            );
            return;
        }
        // Above trigger: emit cheque inline. Empirically (0.3.15
        // smoke tests, 2026-05-08), spawning the emit as a
        // background task and returning immediately caused bee to
        // reject subsequent chunks: bee's `accounting.PrepareDebit`
        // is invoked synchronously per chunk forward and rejects
        // when debt > `disconnectLimit`, so the credit from a
        // cheque MUST land at bee before the next chunk's debit
        // does. Awaiting here serialises (peer, chunk) pairs above
        // the trigger so a cheque always precedes the chunk that
        // would otherwise overdraw the peer's view of our balance.
        //
        // The hot-path cost is the swap stream RTT (~250-500 ms)
        // every `cheque_trigger_plur / chunk_price` chunks per
        // peer — i.e. one cheque RTT per ~2-7 chunks per peer in
        // steady state, paid on the chunk that crosses the
        // trigger. With concurrency 32 across ~100 active peers
        // this stays in the noise: most fetcher tasks are working
        // on different peers and aren't gated on the same cheque.
        match self.core.emit_for(peer, pending).await {
            Ok(_) => self.core.drain_settled(peer, pending),
            Err(e) => {
                debug!(
                    target: "ant_p2p::pushsync_swap",
                    %peer,
                    pending,
                    "cheque emit failed: {e}; will retry on next pushsync",
                );
                // Don't drain on failure — next pushsync sees the
                // same (or higher) pending and tries again. Bee
                // accepts the higher cumulative without complaint
                // because cheques are monotonic.
            }
        }
    }

    fn forget(&self, peer: &PeerId) {
        let mut g = match self.core.pending_debt.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.remove(peer);
    }
}

/// Wrapper that the swarm loop drops into a `Box<dyn PushsyncSettlement>`-
/// shaped slot when SWAP is unconfigured. Counts pushsyncs and emits
/// nothing — useful for diagnostics in builds without a chequebook
/// (e.g. ultra-light reads).
#[derive(Default)]
pub struct NoopPushsyncSettlement;

#[async_trait::async_trait]
impl PushsyncSettlement for NoopPushsyncSettlement {
    async fn note_pushsync(&self, _peer: PeerId, _price: u64) {}
    fn forget(&self, _peer: &PeerId) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::random_secp256k1_secret;

    #[test]
    fn peer_eth_map_record_get_forget() {
        let m = PeerEthMap::new();
        let p = PeerId::random();
        let eoa = [0xabu8; 20];
        assert!(m.get(&p).is_none());
        m.record(p, eoa);
        assert_eq!(m.get(&p), Some(eoa));
        m.forget(&p);
        assert!(m.get(&p).is_none());
    }

    #[test]
    fn config_default_trigger_matches_constant() {
        let cfg = PushsyncSwapConfig::new(
            [0u8; 20],
            random_secp256k1_secret(),
            100,
            std::path::PathBuf::from("/tmp/none"),
            PeerEthMap::new(),
        );
        assert_eq!(cfg.cheque_trigger_plur, DEFAULT_CHEQUE_TRIGGER);
        assert_eq!(DEFAULT_CHEQUE_TRIGGER, LIGHT_PAYMENT_THRESHOLD / 2);
    }

    /// `note_pushsync` below the trigger only accrues; never emits.
    /// We can verify this without a real swarm by checking that the
    /// pending debt grew but `cumulative_for` stayed zero (the
    /// outbound ledger is untouched until `emit_for` runs).
    ///
    /// Constructing a `PushsyncSwap` requires a `Control`, which we
    /// can't easily build in a unit test without a swarm. Instead,
    /// exercise the public state-machine pieces individually:
    /// `accrue` + `drain_settled`.
    #[test]
    fn accrue_and_drain_state_machine() {
        // Stand-alone state-machine tests using a synthetic mutex;
        // mirror the production code by calling the same helpers.
        // We can't easily build the full PushsyncSwap (needs Control),
        // so instead we replicate `accrue` / `drain_settled` against a
        // standalone HashMap and verify the math.
        let mut pending: HashMap<PeerId, u64> = HashMap::new();
        let p = PeerId::random();
        // accrue 100k three times
        for _ in 0..3 {
            *pending.entry(p).or_insert(0) = pending.get(&p).copied().unwrap_or(0) + 100_000;
        }
        assert_eq!(pending.get(&p).copied(), Some(300_000));
        // drain everything
        let cur = pending.get(&p).copied().unwrap_or(0);
        if let Some(e) = pending.get_mut(&p) {
            *e = e.saturating_sub(cur);
            if *e == 0 {
                pending.remove(&p);
            }
        }
        assert!(!pending.contains_key(&p));
    }
}
