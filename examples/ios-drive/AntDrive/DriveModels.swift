import Foundation

/// Swift mirrors of the JSON the FFI returns. Field names match the
/// `serde`-serialised Rust structs (`UploadJobView`, `PostageStatusView`,
/// and the account info object) exactly, so plain `Codable` decoding
/// works without custom keys.

/// One upload job, as returned by `ant_upload_list` / `ant_upload_status`.
struct UploadJob: Codable, Identifiable, Equatable {
    let jobId: String
    let sourcePath: String
    let sourceSize: UInt64
    let name: String?
    let contentType: String?
    let status: String
    let bytesPushed: UInt64
    let chunksPushed: UInt64
    let chunksTotal: UInt64?
    let createdAtUnix: UInt64
    let lastUpdateUnix: UInt64
    let lastError: String?
    let reference: String?

    var id: String { jobId }

    enum CodingKeys: String, CodingKey {
        case jobId = "job_id"
        case sourcePath = "source_path"
        case sourceSize = "source_size"
        case name
        case contentType = "content_type"
        case status
        case bytesPushed = "bytes_pushed"
        case chunksPushed = "chunks_pushed"
        case chunksTotal = "chunks_total"
        case createdAtUnix = "created_at_unix"
        case lastUpdateUnix = "last_update_unix"
        case lastError = "last_error"
        case reference
    }

    /// Best display name: the manifest name, else the source file's
    /// basename, else the job id.
    var displayName: String {
        if let name, !name.isEmpty { return name }
        let base = (sourcePath as NSString).lastPathComponent
        return base.isEmpty ? jobId : base
    }

    /// 0…1 upload progress. `nil` until the total chunk count is known.
    var fraction: Double? {
        guard let total = chunksTotal, total > 0 else { return nil }
        return min(1.0, Double(chunksPushed) / Double(total))
    }

    var isActive: Bool { status == "running" || status == "queued" }
    var isPaused: Bool { status == "paused" }
    var isDone: Bool { status == "completed" }
    var isFailed: Bool { status == "failed" }

    /// A friendly, non-Swarm status line for the row.
    var statusLabel: String {
        switch status {
        case "completed": return "Online"
        case "running": return fraction.map { "Uploading \(Int($0 * 100))%" } ?? "Uploading…"
        case "queued": return "Waiting…"
        case "paused": return "Paused"
        case "failed": return "Upload failed"
        case "cancelled": return "Cancelled"
        default: return status.capitalized
        }
    }
}

private struct UploadJobList: Codable {
    let jobs: [UploadJob]
}

/// The local storage plan, as returned by `ant_storage_status`.
struct StoragePlan: Codable, Equatable {
    let enabled: Bool
    let batchId: String
    let batchDepth: UInt8
    let immutable: Bool
    let totalCapacityChunks: UInt64
    let issuedChunks: UInt64
    let worstCaseRemainingChunks: UInt64

    enum CodingKeys: String, CodingKey {
        case enabled
        case batchId = "batch_id"
        case batchDepth = "batch_depth"
        case immutable
        case totalCapacityChunks = "total_capacity_chunks"
        case issuedChunks = "issued_chunks"
        case worstCaseRemainingChunks = "worst_case_remaining_chunks"
    }

    /// Swarm chunks are 4 KiB. Translate the chunk-denominated capacity
    /// figures into bytes so the UI can show a familiar "GB" meter.
    static let bytesPerChunk: UInt64 = 4096

    var totalBytes: UInt64 { totalCapacityChunks * Self.bytesPerChunk }
    var usedBytes: UInt64 { issuedChunks * Self.bytesPerChunk }
    /// Free space the user actually has: total capacity minus what's
    /// been issued. This is the batch's true `remaining_total`
    /// (`Σ free indices`), not `worstCaseRemainingChunks` — the latter
    /// is a pessimistic pre-flight budget (room left in the *fullest*
    /// bucket, scaled) that collapses to 0 as soon as a single bucket
    /// saturates, which is misleading as a free-space meter.
    var freeBytes: UInt64 { totalBytes - usedBytes }

    var usedFraction: Double {
        guard totalBytes > 0 else { return 0 }
        return min(1.0, Double(usedBytes) / Double(totalBytes))
    }

    /// "Running low" heuristic: less than 10% of the plan's capacity
    /// left, consistent with the free-space figure and meter bar.
    var isLow: Bool {
        guard totalCapacityChunks > 0 else { return false }
        return Double(issuedChunks) / Double(totalCapacityChunks) > 0.90
    }
}

/// Outbound-settlement status, as returned by
/// `ant_storage_settlement_status`. `enabled` is what lets uploads
/// actually reach the network: without a deployed chequebook the node
/// can't pay peers for pushsync and they stop accepting its chunks, so
/// files look "uploaded" locally but never propagate.
struct SettlementInfo: Codable, Equatable {
    let enabled: Bool
    let chequebook: String?
}

/// Deep read-back propagation result, as returned by
/// `ant_storage_verify_propagation`. The daemon resolves the manifest,
/// enumerates the file's chunk tree, fetches every interior node
/// network-only, then probes a sample of the real data leaves across
/// distinct closest peers. This verifies the *actual data chunks* are
/// retrievable from the network — a much stronger signal than a job's
/// "Online" status (which only means the upload was *attempted*) or a
/// root-only reachability check.
struct PropagationInfo: Codable, Equatable {
    let reference: String
    let retrievable: Bool
    /// Total chunks in the file's data tree (leaves + interior nodes).
    let totalChunks: UInt32
    let leafChunks: UInt32
    let intermediateChunks: UInt32
    /// Interior nodes (all) + sampled leaves actually probed.
    let checkedChunks: UInt32
    let retrievableChunks: UInt32
    let sampledLeaves: UInt32
    /// Minimum distinct-route count across sampled leaves — a
    /// replication floor.
    let sources: UInt32
    /// Present only when verification couldn't complete (e.g. the
    /// manifest or data root wasn't retrievable).
    let error: String?

    enum CodingKeys: String, CodingKey {
        case reference
        case retrievable
        case totalChunks = "total_chunks"
        case leafChunks = "leaf_chunks"
        case intermediateChunks = "intermediate_chunks"
        case checkedChunks = "checked_chunks"
        case retrievableChunks = "retrievable_chunks"
        case sampledLeaves = "sampled_leaves"
        case sources
        case error
    }

    /// Short, friendly verdict for a file row badge.
    var label: String {
        if !retrievable {
            if totalChunks == 0 { return "Not found on network" }
            let missing = checkedChunks - retrievableChunks
            return "\(missing) of \(checkedChunks) chunks missing"
        }
        let suffix = sources > 1 ? " · \(sources)×" : ""
        if checkedChunks >= totalChunks {
            return "Verified · all \(totalChunks) chunks\(suffix)"
        }
        return "Verified · \(checkedChunks)/\(totalChunks) chunks\(suffix)"
    }
}

/// Account identity, as returned by `ant_account_info`.
struct AccountInfo: Codable, Equatable {
    let ethAddress: String
    let overlay: String
    let peerId: String
    let agent: String

    enum CodingKeys: String, CodingKey {
        case ethAddress = "eth_address"
        case overlay
        case peerId = "peer_id"
        case agent
    }

    /// `0x1234… abcd` short form for display.
    var shortAddress: String {
        let a = ethAddress
        guard a.count > 12 else { return a }
        return "\(a.prefix(8))…\(a.suffix(4))"
    }
}

/// A storage plan the user can pick in Get Started. Maps a friendly
/// size/duration to the on-chain `depth` + `days` the FFI prices.
struct StoragePlanTier: Identifiable, Equatable {
    let id: String
    let title: String
    let subtitle: String
    let depth: UInt8
    let days: UInt64
    let recommended: Bool

    /// Theoretical capacity (2^depth chunks × 4 KiB), for display.
    var capacityBytes: UInt64 { (UInt64(1) << depth) &* StoragePlan.bytesPerChunk }

    /// Friendly duration label ("1 day" / "1 year").
    var durationLabel: String {
        switch days {
        case 1: return "1 day"
        case 2...30: return "\(days) days"
        case 31...364: return "\(days / 30) months"
        default: return days == 365 ? "1 year" : "\(days / 365) years"
        }
    }

    static let all: [StoragePlanTier] = [
        // Smallest valid batch (depth 17, the postage minimum) for just a
        // day — cheapest possible, so it's the one to use when testing the
        // real buy flow without spending much.
        StoragePlanTier(id: "test", title: "Test",
                        subtitle: "Smallest plan, for trying it out",
                        depth: 17, days: 1, recommended: false),
        StoragePlanTier(id: "starter", title: "Starter",
                        subtitle: "For a few documents and photos",
                        depth: 20, days: 365, recommended: false),
        StoragePlanTier(id: "standard", title: "Standard",
                        subtitle: "For everyday files and backups",
                        depth: 22, days: 365, recommended: true),
        StoragePlanTier(id: "plus", title: "Plus",
                        subtitle: "For large libraries and media",
                        depth: 24, days: 365, recommended: false),
    ]
}

/// Payment information for a plan, as returned by `ant_storage_quote`.
struct StorageQuote: Codable, Equatable {
    let depth: UInt8
    let days: UInt64
    let amountPerChunk: String
    let totalCostBzz: String
    let capacityBytes: UInt64
    let accountBzzDisplay: String
    let accountXdai: String
    let accountXdaiDisplay: String
    let neededBzzDisplay: String
    let xdaiRequiredDisplay: String
    let xdaiToSendDisplay: String
    let sufficientFunds: Bool

    enum CodingKeys: String, CodingKey {
        case depth
        case days
        case amountPerChunk = "amount_per_chunk"
        case totalCostBzz = "total_cost_bzz"
        case capacityBytes = "capacity_bytes"
        case accountBzzDisplay = "account_bzz_display"
        case accountXdai = "account_xdai"
        case accountXdaiDisplay = "account_xdai_display"
        case neededBzzDisplay = "needed_bzz_display"
        case xdaiRequiredDisplay = "xdai_required_display"
        case xdaiToSendDisplay = "xdai_to_send_display"
        case sufficientFunds = "sufficient_funds"
    }
}

enum DriveDecoder {
    static func jobs(from json: String) -> [UploadJob] {
        guard let data = json.data(using: .utf8),
              let list = try? JSONDecoder().decode(UploadJobList.self, from: data)
        else { return [] }
        return list.jobs
    }

    static func job(from json: String) -> UploadJob? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(UploadJob.self, from: data)
    }

    static func plan(from json: String) -> StoragePlan? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(StoragePlan.self, from: data)
    }

    static func account(from json: String) -> AccountInfo? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(AccountInfo.self, from: data)
    }

    static func settlement(from json: String) -> SettlementInfo? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(SettlementInfo.self, from: data)
    }

    static func propagation(from json: String) -> PropagationInfo? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(PropagationInfo.self, from: data)
    }

    static func quote(from json: String) -> StorageQuote? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(StorageQuote.self, from: data)
    }
}

/// Fetches the live xBZZ→USD spot price so plan costs can be shown in a
/// familiar currency. Best-effort: if the lookup fails the UI just omits
/// the USD figure rather than blocking.
enum PriceOracle {
    private struct CoinGecko: Decodable {
        let swarm: Price?
        enum CodingKeys: String, CodingKey { case swarm = "swarm-bzz" }
        struct Price: Decodable { let usd: Double }
    }

    static func bzzUsd() async -> Double? {
        let url = URL(string: "https://api.coingecko.com/api/v3/simple/price?ids=swarm-bzz&vs_currencies=usd")!
        guard let (data, _) = try? await URLSession.shared.data(from: url),
              let decoded = try? JSONDecoder().decode(CoinGecko.self, from: data)
        else { return nil }
        return decoded.swarm?.usd
    }

    /// Format a USD amount, scaling precision for tiny values so a
    /// fraction-of-a-cent test plan still shows something meaningful.
    static func formatUsd(_ value: Double) -> String {
        if value > 0, value < 0.01 { return String(format: "$%.4f", value) }
        return String(format: "$%.2f", value)
    }
}
