import Foundation
import SwiftUI

/// Swift wrapper around the hand-written C API in `crates/ant-ffi/`.
///
/// Holds a single opaque `AntHandle*` for the app's lifetime. `start()`
/// spins up the embedded node; the upload / storage / account methods
/// drive the FFI on detached tasks (the Rust side blocks on its own
/// Tokio runtime) and publish decoded models back on the main actor for
/// SwiftUI to observe.
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
    @Published private(set) var jobs: [UploadJob] = []
    @Published private(set) var plan: StoragePlan?
    @Published private(set) var account: AccountInfo?
    @Published private(set) var settlement: SettlementInfo?
    /// Read-back propagation results, keyed by upload job id. Populated
    /// on demand by `verifyPropagation(_:)`; `nil` for a job means "not
    /// checked yet".
    @Published private(set) var propagation: [String: PropagationInfo] = [:]
    /// Job ids with an in-flight propagation check, so the UI can show a
    /// spinner and avoid firing duplicate probes.
    @Published private(set) var verifying: Set<String> = []

    private var handle: OpaquePointer?
    private var peerPollTask: Task<Void, Never>?
    private var refreshTask: Task<Void, Never>?

    /// Disk-backed propagation verdicts, keyed by the Swarm reference
    /// (content-addressed, so stable across restarts). Lets a verified
    /// file stay verified across launches instead of re-probing the
    /// network every cold start. Only *positive* verdicts are stored, so a
    /// not-yet-propagated file is re-checked next launch rather than being
    /// pinned to a stale "missing" result.
    private var verdictCache: [String: CachedVerdict] = [:]
    private static let verdictTTL: TimeInterval = 24 * 60 * 60

    private struct CachedVerdict: Codable {
        let info: PropagationInfo
        let checkedAtUnix: UInt64
    }

    // MARK: - Lifecycle

    func start() async {
        guard case .idle = status else { return }
        status = .starting

        let dataDir = Self.resolveDataDir()
        Self.seedPeerstoreIfNeeded(in: dataDir)
        let path = dataDir.path

        let raw: OpaquePointer? = await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let h = path.withCString { ant_init($0, &errPtr) }
            if h == nil, let errPtr { ant_free_string(errPtr) }
            return h
        }.value

        guard let raw else {
            status = .failed("could not start")
            return
        }
        handle = raw
        status = .ready
        loadVerdictCache()
        startPollingPeers()
        await refreshAll()
        startAutoRefresh()
    }

    func shutdown() async {
        guard let h = handle else { return }
        handle = nil
        peerPollTask?.cancel()
        refreshTask?.cancel()
        peerCount = 0
        await Task.detached(priority: .userInitiated) { ant_shutdown(h) }.value
        status = .idle
    }

    // MARK: - Downloads (import by link)

    /// Fetch a shared link's bytes. Used by "Add from link" to import a
    /// file someone shared into the user's library view.
    func download(reference: String) async throws -> Data {
        guard let h = handle else { throw AntError.notReady }
        return try await Task.detached(priority: .userInitiated) {
            var len = 0
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let body = reference.withCString { ant_download(h, $0, &len, &errPtr) }
            if let body {
                let data = Data(bytes: body, count: len)
                ant_free_buffer(body, len)
                return data
            }
            let msg = errPtr.flatMap { String(cString: $0) } ?? "download failed"
            if let errPtr { ant_free_string(errPtr) }
            throw AntError.op(msg)
        }.value
    }

    // MARK: - Uploads

    /// Upload a file. Stamps against the connected storage plan (its
    /// batch id), so a plan must exist first.
    @discardableResult
    func startUpload(path: URL, name: String?, contentType: String?) async throws -> UploadJob? {
        guard let h = handle else { throw AntError.notReady }
        let batch = plan?.enabled == true ? plan?.batchId : nil
        let p = path.path
        let json = try await Self.string(name: "upload start") { errPtr in
            p.withCString { cpath in
                Self.withOptCString(batch) { cbatch in
                    Self.withOptCString(name) { cname in
                        Self.withOptCString(contentType) { cct in
                            ant_upload_start(h, cpath, cbatch, cname, cct, errPtr)
                        }
                    }
                }
            }
        }
        // `json` here is just the job id; refresh to pull the full row.
        _ = json
        await refreshJobs()
        return jobs.first
    }

    func pauseUpload(_ jobId: String) async { await jobControl(jobId, ant_upload_pause) }
    func resumeUpload(_ jobId: String) async { await jobControl(jobId, ant_upload_resume) }
    func cancelUpload(_ jobId: String) async { await jobControl(jobId, ant_upload_cancel) }

    private func jobControl(
        _ jobId: String,
        _ fn: @escaping (OpaquePointer?, UnsafePointer<CChar>?, UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> UnsafeMutablePointer<CChar>?
    ) async {
        guard let h = handle else { return }
        _ = try? await Self.string(name: "job control") { errPtr in
            jobId.withCString { fn(h, $0, errPtr) }
        }
        await refreshJobs()
    }

    // MARK: - Storage

    /// Connect a storage plan the account already owns on Gnosis, by id.
    func connectStorage(rpc: String, batchId: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "connect storage") { errPtr in
            rpc.withCString { crpc in
                batchId.withCString { cbatch in
                    ant_storage_connect_batch(h, crpc, cbatch, errPtr)
                }
            }
        }
        if let p = DriveDecoder.plan(from: json) { plan = p }
        await refreshAll()
    }

    /// True once a storage plan is connected — gates uploads.
    var hasStorage: Bool { plan?.enabled == true }

    /// Price a plan (no transaction). Returns the payment information.
    func quoteStorage(rpc: String, depth: UInt8, days: UInt64) async throws -> StorageQuote {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "price plan") { errPtr in
            rpc.withCString { ant_storage_quote(h, $0, depth, days, errPtr) }
        }
        guard let q = DriveDecoder.quote(from: json) else {
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
        if let p = DriveDecoder.plan(from: json) { plan = p }
        await refreshAll()
    }

    /// Auto-discover every storage plan the account owns on Gnosis.
    func discoverStorage(rpc: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        _ = try await Self.string(name: "find storage") { errPtr in
            rpc.withCString { ant_storage_discover(h, $0, errPtr) }
        }
        await refreshAll()
    }

    // MARK: - Propagation verification

    /// Check that a completed upload actually propagated by resolving its
    /// manifest, enumerating the file's chunk tree, and checking the real
    /// data leaves over the network — bypassing our local copy. Stores the
    /// result in `propagation[job.id]` for the file row. `samples == 0`
    /// (the default) checks *every* leaf — the only verdict strong enough
    /// to catch a partially-propagated file; a non-zero value spot-checks
    /// that many evenly-spread leaves. `probes` is how many distinct
    /// closest peers to ask per chunk (route diversity / replication,
    /// clamped 1…8).
    @discardableResult
    func verifyPropagation(_ job: UploadJob, samples: UInt8 = 0, probes: UInt8 = 2) async -> PropagationInfo? {
        guard let h = handle, let reference = job.reference, !reference.isEmpty else { return nil }
        if verifying.contains(job.id) { return propagation[job.id] }
        verifying.insert(job.id)
        defer { verifying.remove(job.id) }
        guard let json = try? await Self.string(name: "verify propagation", { errPtr in
            reference.withCString { ant_storage_verify_propagation(h, $0, samples, probes, errPtr) }
        }) else { return nil }
        let info = DriveDecoder.propagation(from: json)
        if let info {
            propagation[job.id] = info
            // Persist only a clean, fully-retrievable verdict. A partial or
            // not-yet-propagated result is left out so the next launch
            // re-probes rather than trusting a stale "missing".
            if info.retrievable, info.error == nil {
                verdictCache[Self.normalizedRef(reference)] =
                    CachedVerdict(info: info, checkedAtUnix: UInt64(Date().timeIntervalSince1970))
                saveVerdictCache()
            }
        }
        return info
    }

    /// Populate a completed row's verdict, preferring the disk cache: a
    /// fresh (<24h) cached verdict is reused without touching the network;
    /// otherwise the file is probed once. Drives the per-row auto-verify.
    func ensureVerified(_ job: UploadJob) async {
        guard job.isDone, let reference = job.reference, !reference.isEmpty,
              peerCount > 0 else { return }
        if propagation[job.id] != nil { return }
        if let cached = freshVerdict(for: reference) {
            propagation[job.id] = cached
            return
        }
        await verifyPropagation(job)
    }

    /// A cached verdict for `reference` if one exists and is younger than
    /// the TTL, else `nil` (missing or expired → re-probe).
    private func freshVerdict(for reference: String) -> PropagationInfo? {
        guard let entry = verdictCache[Self.normalizedRef(reference)] else { return nil }
        let age = Date().timeIntervalSince1970 - Double(entry.checkedAtUnix)
        return age < Self.verdictTTL ? entry.info : nil
    }

    /// Drop every completed file's cached + in-memory verdict and re-probe
    /// the network, bypassing the TTL. Wired to pull-to-refresh so a manual
    /// refresh always re-checks. Probes run concurrently in the background
    /// so the gesture returns promptly while rows show their own spinners.
    private func forceReverifyCompleted() {
        let completed = jobs.filter { $0.isDone && ($0.reference?.isEmpty == false) }
        guard !completed.isEmpty else { return }
        for job in completed {
            propagation[job.id] = nil
            if let ref = job.reference { verdictCache[Self.normalizedRef(ref)] = nil }
        }
        saveVerdictCache()
        for job in completed {
            Task { _ = await verifyPropagation(job) }
        }
    }

    private static func normalizedRef(_ r: String) -> String {
        (r.hasPrefix("0x") ? String(r.dropFirst(2)) : r).lowercased()
    }

    private static func verdictCacheURL() -> URL {
        resolveDataDir().appendingPathComponent("verify-cache.json")
    }

    private func loadVerdictCache() {
        guard let data = try? Data(contentsOf: Self.verdictCacheURL()),
              let decoded = try? JSONDecoder().decode([String: CachedVerdict].self, from: data)
        else { return }
        verdictCache = decoded
    }

    private func saveVerdictCache() {
        guard let data = try? JSONEncoder().encode(verdictCache) else { return }
        try? data.write(to: Self.verdictCacheURL(), options: .atomic)
    }

    // MARK: - Account

    func exportKey() async throws -> String {
        guard let h = handle else { throw AntError.notReady }
        return try await Self.string(name: "export key") { errPtr in
            ant_account_export_key(h, errPtr)
        }
    }

    // MARK: - Refresh

    func refreshAll() async {
        await refreshJobs()
        await refreshPlan()
        await refreshAccount()
        await refreshSettlement()
    }

    /// Pull-to-refresh: refresh everything, then force a fresh propagation
    /// re-check of every completed file (ignoring the 24h cache).
    func refreshAndReverify() async {
        await refreshAll()
        forceReverifyCompleted()
    }

    func refreshJobs() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "list uploads", { errPtr in
            ant_upload_list(h, errPtr)
        }) {
            jobs = DriveDecoder.jobs(from: json).sorted { $0.createdAtUnix > $1.createdAtUnix }
        }
    }

    func refreshPlan() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "storage status", { errPtr in
            ant_storage_status(h, errPtr)
        }) {
            plan = DriveDecoder.plan(from: json)
        }
    }

    func refreshAccount() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "account info", { errPtr in
            ant_account_info(h, errPtr)
        }) {
            account = DriveDecoder.account(from: json)
        }
    }

    func refreshSettlement() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "settlement status", { errPtr in
            ant_storage_settlement_status(h, errPtr)
        }) {
            settlement = DriveDecoder.settlement(from: json)
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
                    self.peerCount = max(0, count)
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }

    /// While any upload is in flight, refresh the job list ~once a second
    /// so the Files tab shows live progress.
    private func startAutoRefresh() {
        refreshTask?.cancel()
        refreshTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_500_000_000)
                guard let self else { return }
                let active = await MainActor.run { self.jobs.contains { $0.isActive } }
                if active { await self.refreshJobs() }
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

    private static func withOptCString<R>(_ s: String?, _ body: (UnsafePointer<CChar>?) -> R) -> R {
        if let s { return s.withCString { body($0) } }
        return body(nil)
    }

    // MARK: - Data dir / peer seed

    private static func resolveDataDir() -> URL {
        let fm = FileManager.default
        let base = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                                appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
        let dir = base.appendingPathComponent("antdrive", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
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
        case .notReady: return "AntDrive is still starting up"
        case .op(let m): return m
        }
    }
}
