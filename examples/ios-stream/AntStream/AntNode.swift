import Foundation
import Network
import SwiftUI

/// Swift wrapper around the hand-written C API in `crates/ant-ffi/`.
///
/// Same shape as `examples/ios-drive`'s wrapper — one opaque
/// `AntHandle*` for the app's lifetime, FFI calls on detached tasks,
/// decoded models published back on the main actor — with two AntStream
/// differences:
///
/// * The account key comes from ``AccountKeystore`` (Keychain / Secure
///   Enclave) and is passed in through `ant_init_with_identity`, so the
///   library never writes `identity.json` into the app container.
/// * After startup we bring up the in-process bee-shaped HTTP gateway
///   (`ant_start_gateway`, `light_mode`) on localhost. That is the
///   surface the publisher (#67) and the HLS viewer (#66) will push
///   segments and feed updates through.
@MainActor
final class AntNode: ObservableObject {
    enum Status: Equatable {
        case idle
        case starting
        case ready
        case failed(String)

        var label: String {
            switch self {
            case .idle: return "Idle"
            case .starting: return "Starting…"
            case .ready: return "Connected"
            case .failed(let m): return "Offline: \(m)"
            }
        }

        var isReady: Bool { if case .ready = self { return true }; return false }
    }

    @Published private(set) var status: Status = .idle
    @Published private(set) var peerCount: Int = 0
    @Published private(set) var plan: StoragePlan?
    @Published private(set) var account: AccountInfo?
    @Published private(set) var settlement: SettlementInfo?
    /// Remaining lifetime of the connected plan. Fetched on demand
    /// (`refreshValidity`) since it needs a chain RPC; `nil` until then.
    @Published private(set) var validity: StorageValidity?
    /// `true` once the in-process gateway is listening on
    /// ``gatewayURL``. The publisher and viewer tracks talk to it over
    /// HTTP rather than through more C entry points.
    @Published private(set) var gatewayUp: Bool = false
    /// `true` while the device has no usable network path
    /// (`NWPathMonitor`). The node is suspended for the duration.
    @Published private(set) var isOffline: Bool = false
    /// How the account key is protected at rest, for the Storage tab.
    @Published private(set) var keyProtection: AccountKeystore.Protection?

    /// Where the in-process bee gateway listens. Fixed rather than
    /// ephemeral so the publisher/viewer can be pointed at it without
    /// plumbing the port around.
    static let gatewayAddress = "127.0.0.1:1633"
    var gatewayURL: URL? { URL(string: "http://\(Self.gatewayAddress)") }

    private var handle: OpaquePointer?
    private var peerPollTask: Task<Void, Never>?
    private var pathMonitor: NWPathMonitor?
    /// Serialises the node's lifecycle transitions (`start`, `shutdown`,
    /// `restoreAccount`), so exactly one is ever in flight.
    ///
    /// `status` cannot do that job on its own: `restoreAccount` has to
    /// force it back to `.idle` — a `.failed` node is exactly the case
    /// Restore exists for — which would let a second `start()` slip past
    /// the idle guard while the first is still inside
    /// `ant_init_with_identity`. Two inits over one data dir race in
    /// `bind_account_state` (each parking the other's postage /
    /// chequebook / SWAP state, worst case hitting its "refusing to
    /// start: both exist" abort), leave whichever node lost the `handle`
    /// assignment running unshut-down over state now scoped to the other
    /// account, and fight over the gateway's fixed port.
    private var lifecycle: Task<Void, Never>?
    /// Gnosis RPC handed to the gateway so its `/wallet` and `/stamps`
    /// surfaces read real on-chain state (the `chain` build feature).
    private var gnosisRpc: String = AntNode.defaultRpc

    static let defaultRpc = "https://rpc.gnosischain.com"

    // MARK: - Lifecycle

    /// Run `body` once every lifecycle transition queued ahead of it has
    /// finished. Code already running *inside* a transition must call the
    /// `perform…` variants directly — coming back through here would make
    /// it wait on the queue entry it is.
    private func serialized(_ body: @escaping @MainActor () async -> Void) async {
        let previous = lifecycle
        let task = Task { @MainActor in
            await previous?.value
            await body()
        }
        lifecycle = task
        await task.value
    }

    func start(rpc: String? = nil) async {
        await serialized { await self.performStart(rpc: rpc) }
    }

    private func performStart(rpc: String?) async {
        guard case .idle = status, handle == nil else { return }
        status = .starting
        if let rpc, !rpc.trimmingCharacters(in: .whitespaces).isEmpty {
            gnosisRpc = rpc
        }

        let dataDir = Self.resolveDataDir()
        Self.seedPeerstoreIfNeeded(in: dataDir)
        // Defence in depth: if any build ever left a plaintext key in the
        // container, pull it into the Keychain and delete the file.
        _ = AccountKeystore.migrateLegacyIdentityFile(at: dataDir.appendingPathComponent("identity.json"))

        let identity: String
        do {
            // Minting is only ever right on a genuine first launch. An
            // empty Keychain on a data dir that already ran an account
            // means the key was lost, not that there never was one — most
            // sharply when another device in the iCloud circle turned the
            // sync toggle off and the deletion propagated here. Creating
            // a fresh account there would bury the funded one under a
            // stranger's marker; failing sends the user to Restore.
            identity = try AccountKeystore.loadOrCreateIdentity(
                allowCreate: !Self.hasPriorAccount(in: dataDir))
        } catch {
            // An unreadable stored key (e.g. the enclave wrapping key is
            // gone) must not be a dead end: the Storage tab's Restore
            // flow still works from here and re-runs `start()`.
            keyProtection = AccountKeystore.currentProtection()
            status = .failed(error.localizedDescription)
            return
        }
        keyProtection = AccountKeystore.currentProtection()

        let path = dataDir.path
        let raw: OpaquePointer? = await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let h = path.withCString { cpath in
                identity.withCString { cid in
                    ant_init_with_identity(cpath, nil, cid, &errPtr)
                }
            }
            if h == nil, let errPtr { ant_free_string(errPtr) }
            return h
        }.value

        guard let raw else {
            status = .failed("could not start")
            return
        }
        handle = raw
        status = .ready
        startPollingPeers()
        startNetworkMonitor()
        await startGateway()
        await refreshAll()
    }

    func shutdown() async {
        await serialized { await self.performShutdown() }
    }

    private func performShutdown() async {
        guard let h = handle else { return }
        handle = nil
        peerPollTask?.cancel()
        pathMonitor?.cancel()
        pathMonitor = nil
        peerCount = 0
        gatewayUp = false
        await Task.detached(priority: .userInitiated) {
            _ = ant_stop_gateway(h)
            ant_shutdown(h)
        }.value
        status = .idle
    }

    /// Bring up the in-process bee-shaped HTTP gateway in `light_mode`
    /// (publish / feed / SOC writes allowed — exactly what broadcasting
    /// needs; ultra-light is read-only). Idempotent node-side.
    @discardableResult
    func startGateway() async -> Bool {
        guard let h = handle else { return false }
        let addr = Self.gatewayAddress
        let rpc = gnosisRpc
        let ok = await Task.detached(priority: .userInitiated) { () -> Bool in
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let ok = addr.withCString { caddr in
                rpc.withCString { crpc in
                    ant_start_gateway(h, caddr, true, crpc, &errPtr)
                }
            }
            if !ok, let errPtr { ant_free_string(errPtr) }
            return ok
        }.value
        gatewayUp = ok
        return ok
    }

    /// System-suspend before the app loses execution (backgrounding) or
    /// the network disappears. Returns once node state is checkpointed
    /// (bounded node-side at ~5 s), so run it inside a
    /// `beginBackgroundTask` window.
    func suspend() async {
        guard let h = handle else { return }
        await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            if ant_suspend(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
        }.value
    }

    /// Undo `suspend()` on foreground / network-restored transitions.
    /// `ant_resume` re-warms the peer connections (after a long
    /// suspension the sockets are half-open) and `ant_wake` restarts the
    /// work the suspension paused.
    func wake() async {
        guard let h = handle else { return }
        await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            if ant_resume(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
            var wakeErr: UnsafeMutablePointer<CChar>? = nil
            if ant_wake(h, &wakeErr) != 0, let wakeErr { ant_free_string(wakeErr) }
        }.value
        // `ant_resume` recovers the swarm only: if the OS also tore down
        // the gateway's localhost listener it has to be rebound
        // separately (see ant.h). Re-binding a live gateway is a no-op
        // success, so this is safe on every foreground.
        await restartGateway()
    }

    private func restartGateway() async {
        guard let h = handle else { return }
        await Task.detached(priority: .userInitiated) { _ = ant_stop_gateway(h) }.value
        await startGateway()
    }

    /// Watch the device's network path. Offline ⇒ suspend (retries
    /// against a dead network only burn battery); back online ⇒ wake.
    private func startNetworkMonitor() {
        guard pathMonitor == nil else { return }
        let monitor = NWPathMonitor()
        monitor.pathUpdateHandler = { [weak self] netPath in
            let offline = netPath.status != .satisfied
            Task { @MainActor [weak self] in
                guard let self, self.isOffline != offline else { return }
                self.isOffline = offline
                if offline {
                    await self.suspend()
                } else {
                    await self.wake()
                }
            }
        }
        monitor.start(queue: DispatchQueue(label: "antstream.netpath"))
        pathMonitor = monitor
    }

    // MARK: - Storage

    /// True once a storage plan is connected — gates broadcasting.
    var hasStorage: Bool { plan?.enabled == true }

    /// Everything a first launch has to get through before the device can
    /// go live. Drives the Broadcast tab's readiness checklist.
    var isReadyToBroadcast: Bool {
        status.isReady && hasStorage && settlement?.enabled == true && gatewayUp
    }

    /// Price a plan (no transaction). Returns the payment information.
    func quoteStorage(rpc: String, depth: UInt8, days: UInt64) async throws -> StorageQuote {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "price plan") { errPtr in
            rpc.withCString { ant_storage_quote(h, $0, depth, days, errPtr) }
        }
        guard let q = StreamDecoder.quote(from: json) else {
            throw AntError.op("could not read plan price")
        }
        return q
    }

    /// Buy + activate a plan funding only with xDAI: the node swaps the
    /// xBZZ shortfall on-chain itself, then buys the plan. Spends real funds.
    func buyStorage(rpc: String, depth: UInt8, amountPerChunk: String, immutable: Bool) async throws {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "activate storage") { errPtr in
            rpc.withCString { crpc in
                amountPerChunk.withCString { camt in
                    ant_storage_buy_xdai(h, crpc, depth, camt, immutable ? 1 : 0, errPtr)
                }
            }
        }
        if let p = StreamDecoder.plan(from: json) { plan = p }
        await refreshAll()
    }

    /// Price extending the connected plan's lifetime by `days`.
    func quoteTopUp(rpc: String, days: UInt64) async throws -> StorageQuote {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "price extension") { errPtr in
            rpc.withCString { ant_storage_topup_quote(h, $0, days, errPtr) }
        }
        guard let q = StreamDecoder.quote(from: json) else {
            throw AntError.op("could not read extension price")
        }
        return q
    }

    /// Extend the connected plan's lifetime, funding only with xDAI.
    /// Spends real funds. Publishes the refreshed validity.
    func topUpStorage(rpc: String, amountPerChunk: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "extend storage") { errPtr in
            rpc.withCString { crpc in
                amountPerChunk.withCString { camt in
                    ant_storage_topup_xdai(h, crpc, camt, errPtr)
                }
            }
        }
        if let v = StreamDecoder.validity(from: json) { validity = v }
        await refreshAll()
    }

    /// Auto-discover every storage plan the account owns on Gnosis. Also
    /// the retry path for the one-time chequebook deploy, so a plan that
    /// activated without settlement can be repaired without `antctl`.
    func discoverStorage(rpc: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        _ = try await Self.string(name: "find storage") { errPtr in
            rpc.withCString { ant_storage_discover(h, $0, errPtr) }
        }
        await refreshAll()
    }

    // MARK: - Account

    func exportKey() async throws -> String {
        guard let h = handle else { throw AntError.notReady }
        return try await Self.string(name: "export key") { errPtr in
            ant_account_export_key(h, errPtr)
        }
    }

    /// Move the stored account key between iCloud-backed and device-bound
    /// protection. Takes effect at rest immediately; the running node is
    /// unaffected (it already holds the key in memory). Turning it off
    /// also removes the key from the user's *other* devices, so callers
    /// confirm first — see `AccountKeystore.setICloudBackup`.
    func setICloudBackup(_ enabled: Bool) throws {
        try AccountKeystore.setICloudBackup(enabled)
        keyProtection = AccountKeystore.currentProtection()
    }

    /// Replace the stored account key with a backed-up one. The node has
    /// to restart to adopt it, so we tear it down and start again.
    ///
    /// The restart is also what re-scopes the node's on-disk state to the
    /// restored account: `ant_init_with_identity` parks the previous
    /// account's postage batches, chequebook association and SWAP ledgers
    /// under `<data dir>/accounts/<its address>/` and swaps in whatever
    /// the restored account left behind (see `bind_account_state` in
    /// `ant-ffi`). Without that the checklist would keep reporting the
    /// old account's plan as active while every stamp the restored key
    /// signs over it is rejected by peers.
    ///
    /// Restoring is one transition, run on the lifecycle queue: a Restore
    /// tapped while the launch `start()` is still inside
    /// `ant_init_with_identity` has to wait for it rather than race a
    /// second init over the same data dir.
    func restoreAccount(fromKey key: String) async throws {
        // Validate before queueing so a typo comes back straight away
        // instead of behind an in-flight start. Nothing is written yet.
        _ = try AccountKeystore.identity(fromAccountKey: key)
        var failure: Error?
        await serialized {
            do {
                _ = try AccountKeystore.restore(fromAccountKey: key)
            } catch {
                failure = error
                return
            }
            await self.performShutdown()
            // `performShutdown()` is a no-op for a node that never came
            // up, so reset the status by hand — otherwise the idle guard
            // would strand a `.failed` node (exactly the case Restore
            // exists for). Safe only because the queue guarantees no
            // other start is in flight.
            self.status = .idle
            await self.performStart(rpc: nil)
            if case .failed(let m) = self.status { failure = AntError.op(m) }
        }
        if let failure { throw failure }
    }

    // MARK: - Refresh

    func refreshAll() async {
        await refreshPlan()
        await refreshAccount()
        await refreshSettlement()
    }

    func refreshPlan() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "storage status", { errPtr in
            ant_storage_status(h, errPtr)
        }) {
            let decoded = StreamDecoder.plan(from: json)
            if decoded != plan { plan = decoded }
        }
    }

    func refreshAccount() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "account info", { errPtr in
            ant_account_info(h, errPtr)
        }) {
            let decoded = StreamDecoder.account(from: json)
            if decoded != account { account = decoded }
        }
    }

    /// Fetch the connected plan's remaining lifetime from chain. Needs a
    /// Gnosis RPC URL; a no-op when none is set or no plan is connected.
    func refreshValidity(rpc: String) async {
        guard let h = handle, !rpc.isEmpty, hasStorage else { return }
        if let json = try? await Self.string(name: "storage validity", { errPtr in
            rpc.withCString { ant_storage_validity(h, $0, errPtr) }
        }), let v = StreamDecoder.validity(from: json) {
            validity = v
        }
    }

    func refreshSettlement() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "settlement status", { errPtr in
            ant_storage_settlement_status(h, errPtr)
        }) {
            let decoded = StreamDecoder.settlement(from: json)
            if decoded != settlement { settlement = decoded }
        }
    }

    // MARK: - Polling

    private func startPollingPeers() {
        peerPollTask?.cancel()
        guard let h = handle else { return }
        peerPollTask = Task { [weak self] in
            while !Task.isCancelled {
                let count = Int(ant_peer_count(h))
                await MainActor.run {
                    guard let self, self.handle == h else { return }
                    // Publish only real changes: an unconditional
                    // re-assign fires objectWillChange every second and
                    // re-renders every observing view.
                    let clamped = max(0, count)
                    if self.peerCount != clamped { self.peerCount = clamped }
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }

    // MARK: - FFI string helper

    /// Run an FFI call that returns an owned C string + writes an error
    /// string into its `out_err` slot. Frees both. Hops onto a detached
    /// task so the (potentially blocking) Rust call never stalls the UI.
    private static func string(
        name: String,
        _ body: @escaping (UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> UnsafeMutablePointer<CChar>?
    ) async throws -> String {
        try await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            if let raw = body(&errPtr) {
                defer { ant_free_string(raw) }
                return String(cString: raw)
            }
            let msg = errPtr.flatMap { String(cString: $0) } ?? "\(name) failed"
            if let errPtr { ant_free_string(errPtr) }
            throw AntError.op(msg)
        }.value
    }

    // MARK: - Data dir / peer seed

    private static func resolveDataDir() -> URL {
        let fm = FileManager.default
        let base = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                                appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
        let dir = base.appendingPathComponent("antstream", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    /// Has an account already run against this data dir? `ant-ffi` writes
    /// `account.json` (the public address only) on every start, so its
    /// presence means an earlier account's postage / chequebook / SWAP
    /// state is sitting at the canonical paths. A reinstall takes the
    /// container with it, so this is `false` on a real first launch.
    private static func hasPriorAccount(in dataDir: URL) -> Bool {
        FileManager.default.fileExists(
            atPath: dataDir.appendingPathComponent("account.json").path)
    }

    private static func seedPeerstoreIfNeeded(in dataDir: URL) {
        let dest = dataDir.appendingPathComponent("peers.json")
        if FileManager.default.fileExists(atPath: dest.path),
           let data = try? Data(contentsOf: dest),
           let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           let peers = obj["peers"] as? [Any], !peers.isEmpty {
            return
        }
        guard let bundled = Bundle.main.url(forResource: "peers.seed", withExtension: "json") else { return }
        try? FileManager.default.removeItem(at: dest)
        try? FileManager.default.copyItem(at: bundled, to: dest)
    }
}

enum AntError: LocalizedError {
    case notReady
    case op(String)

    var errorDescription: String? {
        switch self {
        case .notReady: return "AntStream is still starting up"
        case .op(let m): return m
        }
    }
}
