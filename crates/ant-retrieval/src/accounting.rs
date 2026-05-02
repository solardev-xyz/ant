//! Client-side accounting mirror for bee retrieval.
//!
//! # Why this exists
//!
//! Bee's `pkg/retrieval/retrieval.go::RetrieveChunk` calls
//! `accounting.PrepareCredit(peer, price)` *before* dispatching a
//! request. The check fails with `ErrOverdraft` if the dispatch
//! would push our expected debt with that peer over the
//! `paymentThreshold + 1s × refreshRate` envelope, at which point
//! bee silently picks the next-closest peer instead and re-tries the
//! original 600 ms later (after one `lightRefreshRate` worth of
//! allowance has accumulated). The dispatched request therefore
//! never lands on a peer that's about to disconnect us — the worst
//! case is an extra 600 ms of latency for a single saturated chunk.
//!
//! Without this mirror, we instead **find out** that a peer is over
//! its disconnect limit by being RST'd at the TCP layer (bee's
//! `accounting.go::debitAction.Apply()::if nextBalance >=
//! disconnectLimit { Blocklist }`). A 4-track bench against
//! production saw the 112 MiB file drop ~52 peers and finish in
//! 312 s vs bee's 195 s on the identical file under identical
//! handshake terms. The 117 s gap is almost entirely
//! "dispatch-then-find-out" overhead: we hedge harder, the hedge
//! lands on a saturated peer, the peer RSTs us, we cancel +
//! re-dispatch, peer set degrades, retry budget drains.
//!
//! # What this does
//!
//! [`Accounting`] tracks per-peer state mirroring bee's
//! `accountingPeer` (just the four fields that matter for
//! admission):
//!
//! - `balance`: real debt accumulated on this peer since the last
//!   accepted pseudosettle, in chunk-price units.
//! - `reserved`: prospective debt for in-flight retrievals (added
//!   on `try_reserve`, cleared on `apply` or guard drop).
//! - `last_refresh`: when the pseudosettle driver last credited
//!   this peer with an accepted refresh, so the same time-elapsed
//!   allowance bee uses can be added to the overdraft envelope.
//! - `last_used`: last successful chunk fetch from this peer, used
//!   only to age out idle peers when the gateway processes
//!   thousands of distinct peers per hour.
//!
//! [`Accounting::try_reserve`] returns a [`DebitGuard`] (RAII) on
//! success; on failure (`balance + reserved + price` >
//! `OVERDRAFT_LIMIT`) it returns `None`, and the fetcher must skip
//! this peer with a 600 ms TTL, mirroring bee's
//! `skip.Add(chunkAddr, peer, overDraftRefresh)`.
//!
//! On success the [`DebitGuard`] is moved into the per-chunk fetch
//! future. Either `apply()` is called when the chunk arrives (debit
//! moves from reserved into balance, hot hint sent if balance just
//! crossed [`HOT_DEBT_THRESHOLD`]) or the guard is dropped without
//! `apply()` (reserved is released — the chunk fetch failed and the
//! request never reached the peer's `PrepareDebit`, so bee's
//! `creditAction.Cleanup` cleared its reserve too without touching
//! ghost balance).
//!
//! [`Accounting::credit`] is the pseudosettle ack callback: when
//! the driver receives a `PaymentAck { accepted, timestamp }`,
//! call `credit(peer, accepted)` to drop our balance by that
//! amount. `last_refresh` is bumped to `Instant::now()` so the
//! envelope's `1 s × refreshRate` term reopens.
//!
//! # Sizing
//!
//! - Chunk price at typical proximity (PO 8): ~240 k units.
//! - `lightDisconnectLimit` (the line bee enforces on us when we
//!   declare `full_node = false`): 1.69 M units.
//! - `lightRefreshRate`: 450 k units / sec.
//!
//! [`OVERDRAFT_LIMIT`] is set to `lightDisconnectLimit + 1 s ×
//! lightRefreshRate ≈ 2.14 M units` — that's the exact ceiling bee
//! enforces in `debitAction.Apply` (line 1357), the boundary at
//! which a peer disconnects us. We refuse to dispatch any request
//! that would cross it.
//!
//! [`HOT_DEBT_THRESHOLD`] is set at 50 % of the disconnect limit
//! (≈ 850 k units), matching bee's `earlyPayment = 50 %` in
//! `accounting.go::PrepareCredit`. Crossing it triggers a hot hint
//! into the pseudosettle driver, which dispatches a refresh on the
//! next driver tick (100 ms) instead of waiting for the 1 s
//! periodic walk.
//!
//! # Conservatism
//!
//! Our mirror is intentionally conservative: we only ever
//! *increase* `balance` on `apply()`, so the worst-case error in
//! our envelope check is "we think this peer is more loaded than
//! it really is" → we skip them for 600 ms → tiny per-chunk latency
//! penalty. The opposite error ("we think the peer has more
//! headroom than it does") would let us blow past
//! `lightDisconnectLimit`, which is exactly the regression we're
//! trying to prevent. Cancellations release the reserve (no debit
//! ever happened on bee's side either, see
//! `creditAction.Cleanup`); only successful applies stay on the
//! books until pseudosettle clears them.

use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::fetcher::Overlay;

type SharedBalances = Arc<Mutex<HashMap<PeerId, PeerBalance>>>;

/// Bee's `lightDisconnectLimit` for a peer that declares itself
/// `full_node = false` in the BZZ handshake. Computed by bee as
/// `(100 + paymentTolerance) % * lightPaymentThreshold` =
/// `1.25 * (paymentThreshold / lightFactor)` =
/// `1.25 * (13.5 M / 10)` = 1.6875 M units.
///
/// See `bee/pkg/accounting/accounting.go::NotifyPeerConnect` for
/// the wiring and `pkg/node/node.go` for the
/// `paymentThreshold = 13.5M` and `lightFactor = 10` constants.
pub const LIGHT_DISCONNECT_LIMIT: u64 = 1_687_500;

/// Bee's per-peer light-mode refresh allowance. From `pkg/node/node.go`:
/// `lightRefreshRate = refreshRate / lightFactor = 4.5 M / 10`.
/// Bee's `debitAction.Apply` allows up to `1 s × refreshRate`
/// of "in-flight" allowance on top of the disconnect limit, capped
/// at one second's worth (`min(elapsed_seconds, 1)`).
pub const LIGHT_REFRESH_RATE_PER_SEC: u64 = 450_000;

/// Combined ceiling we refuse to cross. Bee's `debitAction.Apply`
/// blocklists at `disconnectLimit + min(int64(elapsed_seconds), 1)
/// * refreshRate`, which is *either* `disconnectLimit` (when
/// elapsed < 1 s) *or* `disconnectLimit + refreshRate` (when
/// elapsed ≥ 1 s) — bee's `min` operates on `int64` second
/// granularity, no fractional seconds. See
/// `bee/pkg/accounting/accounting.go:1346`:
///
/// ```text
/// timeElapsedInSeconds := min(timeNow().Unix() - refreshReceivedTimestamp, 1)
/// ```
///
/// We pick the *lower* of the two boundaries — `disconnectLimit`
/// alone, no allowance — to leave headroom for the
/// `min(elapsed, 1) * refreshRate` we don't precisely model. With
/// 100 peers each consuming ≥ 4 chunks/sec (24-s download of a
/// 44 MiB file), the difference between 1.687 M and 2.137 M is
/// roughly one second of saturation, which a single
/// pseudosettle round-trip can clear. Anything beyond is
/// indistinguishable from a stall on bee's end.
pub const OVERDRAFT_LIMIT: u64 = LIGHT_DISCONNECT_LIMIT;

/// Threshold at which the fetcher hints to the pseudosettle driver
/// that a peer's debt is approaching the limit and a refresh
/// should fire on the next tick rather than waiting for the
/// periodic walk.
///
/// 50 % of `LIGHT_DISCONNECT_LIMIT` matches bee's
/// `earlyPayment = 50 %` constant in `pkg/accounting/accounting.go`.
pub const HOT_DEBT_THRESHOLD: u64 = LIGHT_DISCONNECT_LIMIT / 2;

/// Per-chunk skip TTL for peers that fail [`Accounting::try_reserve`].
/// Mirrors bee's `overDraftRefresh = time.Millisecond * 600` in
/// `pkg/retrieval/retrieval.go`. The fetcher's per-chunk skip
/// list lifts the entry after this elapses, so the same peer
/// becomes available again on the next preemptive tick once
/// pseudosettle has had a chance to clear its debt.
pub const OVERDRAFT_REFRESH: Duration = Duration::from_millis(600);

/// Per-peer mirror of bee's `accountingPeer`, restricted to the
/// fields that affect admission control. We don't track
/// `ghostBalance`, `paymentThresholdForPeer`, or any of bee's
/// pricing-protocol state — those are the receiver's concern
/// (they decide when to disconnect us), and the only number we
/// can act on locally is "what does the receiver think we owe
/// them right now."
#[derive(Debug)]
struct PeerBalance {
    balance: u64,
    reserved: u64,
    last_refresh: Option<Instant>,
    last_used: Instant,
}

impl Default for PeerBalance {
    fn default() -> Self {
        Self {
            balance: 0,
            reserved: 0,
            last_refresh: None,
            last_used: Instant::now(),
        }
    }
}

/// Shared accounting state.
///
/// Held behind an `Arc<Mutex<_>>` so all sibling chunk fetches of
/// a single user request observe the same balance per peer. The
/// joiner fans out concurrent fetches against the same fetcher
/// (`&self`), so admission decisions on chunk N must see the
/// reservations made by chunks N-1, N-2, ... still in flight.
///
/// We never hold the lock across an `.await`; the `try_reserve`
/// path is purely synchronous, and `apply` / `credit` /
/// `cleanup_reserved` are likewise.
pub struct Accounting {
    peers: SharedBalances,
    /// Channel into the pseudosettle driver. Each event is a
    /// peer that has just crossed [`HOT_DEBT_THRESHOLD`] and
    /// needs an out-of-band refresh ASAP. The driver coalesces
    /// duplicates and respects the 1.1 s minimum spacing on
    /// bee's side.
    hot_hint: Option<mpsc::Sender<HotHint>>,
}

/// Hint payload sent from the fetcher hot path into the
/// pseudosettle driver. Currently just carries the peer id, but
/// is wrapped in a struct so future fields (e.g., observed debt)
/// can be added without touching every call site.
#[derive(Debug, Clone, Copy)]
pub struct HotHint {
    pub peer: PeerId,
}

impl Default for Accounting {
    fn default() -> Self {
        Self::new()
    }
}

impl Accounting {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            hot_hint: None,
        }
    }

    pub fn with_hot_hint(mut self, tx: mpsc::Sender<HotHint>) -> Self {
        self.hot_hint = Some(tx);
        self
    }

    /// Compute the chunk price for a peer at a given chunk address.
    /// Mirrors `bee/pkg/pricer/pricer.go::PeerPrice`:
    ///
    /// ```text
    /// price = (MaxPO - proximity(peer, chunk) + 1) * basePrice
    /// ```
    ///
    /// where `MaxPO = 31` (256-bit address space, byte boundary)
    /// and `basePrice = 10_000`. Closer peers (higher proximity)
    /// charge less; far peers charge more, because the far peer's
    /// forwarding chain is longer and more nodes earn along the
    /// way.
    pub fn peer_price(peer_overlay: &Overlay, chunk_addr: &[u8; 32]) -> u64 {
        const MAX_PO: u64 = 31;
        const BASE_PRICE: u64 = 10_000;
        (MAX_PO - proximity(peer_overlay, chunk_addr) + 1) * BASE_PRICE
    }

    /// Try to reserve `price` against `peer` for chunk `chunk_addr`.
    ///
    /// Returns `Some(DebitGuard)` if the dispatch is admissible —
    /// i.e., `balance + reserved + price <= OVERDRAFT_LIMIT` after
    /// adding back any `lightRefreshRate × elapsed` allowance that
    /// has accrued since the last successful refresh.
    ///
    /// Returns `None` if the dispatch would cross the limit; the
    /// caller is expected to skip this peer for
    /// [`OVERDRAFT_REFRESH`] and try the next-closest one. This
    /// matches bee's
    /// `pkg/retrieval/retrieval.go::case ErrOverdraft` arm.
    pub fn try_reserve(&self, peer: PeerId, price: u64) -> Option<DebitGuard> {
        let mut peers = self.peers.lock().ok()?;
        let now = Instant::now();
        let entry = peers.entry(peer).or_insert_with(|| PeerBalance {
            balance: 0,
            reserved: 0,
            last_refresh: None,
            last_used: now,
        });

        // Mirror bee's `min(timeNow().Unix() - refreshReceivedTimestamp, 1)`
        // exactly: the allowance is binary on a one-second boundary,
        // not interpolated. Bee grants the full
        // `lightRefreshRate` only after a full second has elapsed
        // since the last *accepted* refresh.
        //
        // For never-refreshed peers (`last_refresh = None`) we
        // intentionally use the smaller, zero-allowance branch so a
        // burst of dispatches at startup doesn't pile reservations
        // up to bee's max line before pseudosettle has had a chance
        // to land.
        let allowance = match entry.last_refresh {
            Some(last) if now.duration_since(last) >= Duration::from_secs(1) => {
                LIGHT_REFRESH_RATE_PER_SEC
            }
            _ => 0,
        };
        let limit = OVERDRAFT_LIMIT.saturating_add(allowance);
        let next = entry.balance.saturating_add(entry.reserved).saturating_add(price);
        if next > limit {
            return None;
        }
        entry.reserved = entry.reserved.saturating_add(price);
        entry.last_used = now;
        Some(DebitGuard {
            peer,
            price,
            applied: false,
            balances: self.peers.clone(),
            hot_hint: self.hot_hint.clone(),
        })
    }

    /// Credit a peer with the bee-side accepted refresh amount.
    /// Subtracts `accepted` from `balance` (saturating) and bumps
    /// `last_refresh` to `Instant::now()`, opening the per-second
    /// allowance window again.
    pub fn credit(&self, peer: PeerId, accepted: u64) {
        let Ok(mut peers) = self.peers.lock() else { return };
        let entry = peers.entry(peer).or_default();
        entry.balance = entry.balance.saturating_sub(accepted);
        entry.last_refresh = Some(Instant::now());
    }

    /// Drop all per-peer state for `peer`. Called when the swarm
    /// `ConnectionClosed` event fires (peers reconnect with a
    /// fresh balance on bee's side too — bee's
    /// `notifyPeerConnect` resets the `accountingPeer`, so we
    /// should match).
    pub fn forget(&self, peer: &PeerId) {
        let Ok(mut peers) = self.peers.lock() else { return };
        peers.remove(peer);
    }

    /// Snapshot a peer's balance for diagnostic logging. Returns
    /// `(balance, reserved)`. Cheap (single lock acquisition).
    pub fn debug_snapshot(&self, peer: &PeerId) -> Option<(u64, u64)> {
        let peers = self.peers.lock().ok()?;
        peers.get(peer).map(|b| (b.balance, b.reserved))
    }
}

/// RAII guard returned by [`Accounting::try_reserve`]. Wraps the
/// reserved debit; either [`DebitGuard::apply`] is called (debit
/// moves from `reserved` into `balance`) or the guard drops with
/// `applied = false` (reserved is released without touching
/// balance).
///
/// The guard does NOT hold the `peers` lock — the `Mutex` is
/// re-acquired in `apply`/`Drop` for the brief moment of
/// state mutation.
pub struct DebitGuard {
    peer: PeerId,
    price: u64,
    applied: bool,
    balances: SharedBalances,
    hot_hint: Option<mpsc::Sender<HotHint>>,
}

impl DebitGuard {
    /// Mark the debit as applied. Moves `price` from `reserved`
    /// into `balance`. If the new `balance` crosses
    /// [`HOT_DEBT_THRESHOLD`], fire a [`HotHint`] into the
    /// pseudosettle driver so it can dispatch a refresh on the
    /// next 100 ms tick rather than waiting for the periodic
    /// walk.
    pub fn apply(mut self) {
        let crossed = {
            let Ok(mut peers) = self.balances.lock() else {
                return;
            };
            let entry = peers.entry(self.peer).or_default();
            entry.reserved = entry.reserved.saturating_sub(self.price);
            let prev_balance = entry.balance;
            entry.balance = entry.balance.saturating_add(self.price);
            prev_balance < HOT_DEBT_THRESHOLD && entry.balance >= HOT_DEBT_THRESHOLD
        };
        self.applied = true;
        if crossed {
            if let Some(tx) = &self.hot_hint {
                let _ = tx.try_send(HotHint { peer: self.peer });
            }
        }
    }

    /// Peer this guard is for. Useful for logging.
    pub fn peer(&self) -> PeerId {
        self.peer
    }
}

impl Drop for DebitGuard {
    fn drop(&mut self) {
        if self.applied {
            return;
        }
        // Cancellation path: release the reservation without
        // touching balance. Mirrors bee's
        // `creditAction.Cleanup` (line 430 of
        // `pkg/accounting/accounting.go`): if the action wasn't
        // applied, just decrement the per-peer reserve. No
        // ghost-balance increment on the *credit* side; the
        // ghost-balance penalty is on bee's *debit* side and is
        // the receiver's concern, not ours.
        let Ok(mut peers) = self.balances.lock() else {
            return;
        };
        if let Some(entry) = peers.get_mut(&self.peer) {
            entry.reserved = entry.reserved.saturating_sub(self.price);
        }
    }
}

/// XOR-distance proximity order between two 32-byte addresses,
/// matching `bee/pkg/swarm/swarm.go::Proximity`. Returns the
/// number of leading zero bits in `a XOR b`, capped at `MAX_PO =
/// 31` (we don't bother with the full 256 because chunk price
/// uses `MAX_PO - po + 1` and any `po >= MAX_PO` collapses to
/// the same minimum price anyway).
fn proximity(a: &[u8; 32], b: &[u8; 32]) -> u64 {
    const MAX_PO: u64 = 31;
    let mut po = 0u64;
    for i in 0..32 {
        let x = a[i] ^ b[i];
        if x == 0 {
            po = po.saturating_add(8);
            if po >= MAX_PO {
                return MAX_PO;
            }
            continue;
        }
        po = po.saturating_add(x.leading_zeros() as u64);
        return po.min(MAX_PO);
    }
    MAX_PO
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> [u8; 32] {
        let mut a = [0u8; 32];
        a[0] = b;
        a
    }

    #[test]
    fn proximity_identical_addrs_is_max_po() {
        let a = addr(0xab);
        assert_eq!(proximity(&a, &a), 31);
    }

    #[test]
    fn proximity_first_byte_differs() {
        let a = addr(0b10000000);
        let b = addr(0b00000000);
        assert_eq!(proximity(&a, &b), 0);
    }

    #[test]
    fn proximity_capped_at_max_po() {
        // 8 leading zero bytes = 64 matching bits, far above MAX_PO = 31.
        let mut a = [0u8; 32];
        a[8] = 0x80;
        let b = [0u8; 32];
        assert_eq!(proximity(&a, &b), 31);
    }

    #[test]
    fn peer_price_higher_for_far_peer() {
        let chunk = addr(0xff);
        let close = addr(0xff);
        let far = addr(0x00);
        let close_price = Accounting::peer_price(&close, &chunk);
        let far_price = Accounting::peer_price(&far, &chunk);
        assert!(far_price > close_price);
        // (MAX_PO - po + 1) * BASE_PRICE = (31 - 31 + 1) * 10_000 = 10_000.
        assert_eq!(close_price, 10_000);
        // (MAX_PO - po + 1) * BASE_PRICE = (31 -  0 + 1) * 10_000 = 320_000.
        assert_eq!(far_price, 320_000);
    }

    #[test]
    fn try_reserve_admits_under_limit() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 240_000);
        assert!(g.is_some());
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (0, 240_000));
        drop(g);
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (0, 0), "guard drop should release the reservation");
    }

    #[test]
    fn try_reserve_rejects_over_limit() {
        let acc = Accounting::new();
        let p = PeerId::random();
        // Each call before any successful refresh gets a full 1 s
        // allowance: limit = OVERDRAFT_LIMIT + 450 k = 2.5875 M.
        // 11 chunks × 240 k = 2.64 M crosses it.
        let mut guards = Vec::new();
        for _ in 0..15 {
            if let Some(g) = acc.try_reserve(p, 240_000) {
                guards.push(g);
            }
        }
        assert!(
            !guards.is_empty(),
            "first reservation should always succeed",
        );
        assert!(
            guards.len() < 15,
            "got {} reservations, expected the cap to bite before 15",
            guards.len(),
        );
    }

    #[test]
    fn apply_moves_reserved_to_balance() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 240_000).unwrap();
        g.apply();
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (240_000, 0));
    }

    #[test]
    fn credit_reduces_balance() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 500_000).unwrap();
        g.apply();
        acc.credit(p, 300_000);
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (200_000, 0));
    }

    #[test]
    fn forget_removes_peer_state() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 240_000).unwrap();
        g.apply();
        assert!(acc.debug_snapshot(&p).is_some());
        acc.forget(&p);
        assert!(acc.debug_snapshot(&p).is_none());
    }

    #[test]
    fn hot_hint_fires_when_crossing_threshold() {
        let (tx, mut rx) = mpsc::channel::<HotHint>(8);
        let acc = Accounting::new().with_hot_hint(tx);
        let p = PeerId::random();
        // First 3 chunks of 240 k = 720 k, just under HOT_DEBT_THRESHOLD = 843 k.
        for _ in 0..3 {
            acc.try_reserve(p, 240_000).unwrap().apply();
        }
        assert!(rx.try_recv().is_err(), "should not fire below threshold");
        // Fourth chunk crosses the threshold (3 × 240 k + 240 k = 960 k > 843 k).
        acc.try_reserve(p, 240_000).unwrap().apply();
        let hint = rx.try_recv().expect("hot hint should fire");
        assert_eq!(hint.peer, p);
    }
}
