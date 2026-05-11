import Foundation
import SwiftUI

/// Swift wrapper around the hand-written C API in `crates/ant-ffi/`.
///
/// Holds a single `AntHandle*` for the lifetime of the app. `start()`
/// spins up the embedded node on a background task; `download(...)`
/// blocks on a cooperative Swift task while the Rust side runs the
/// retrieval on its own Tokio runtime. The FFI is thread-safe, so
/// multiple concurrent `download` calls share the node's chunk cache
/// and retrieval pipeline.
///
/// `AntHandle` is an opaque forward-declared struct in C
/// (`typedef struct AntHandle AntHandle;`) — Swift imports that as
/// `OpaquePointer`, so that is what we carry around here.
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
            case .ready: return "Ready"
            case .failed(let msg): return "Failed: \(msg)"
            }
        }

        var color: Color {
            switch self {
            case .idle: return .secondary
            case .starting: return .orange
            case .ready: return .green
            case .failed: return .red
            }
        }

        var isReady: Bool {
            if case .ready = self { return true }
            return false
        }
    }

    @Published private(set) var status: Status = .idle
    @Published private(set) var peerCount: Int = 0
    /// Live progress view of the in-flight `download(reference:)` call,
    /// updated ~4×/s from a poll loop. `nil` whenever no download is
    /// running (or the last one finished and its state was cleared).
    @Published private(set) var downloadProgress: DownloadProgress? = nil

    private var handle: OpaquePointer?
    private var peerPollTask: Task<Void, Never>?
    private var progressPollTask: Task<Void, Never>?

    func start() async {
        guard case .idle = status else { return }
        status = .starting

        let dataDir = Self.resolveDataDir()
        let path = dataDir.path

        let result: InitOutcome = await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let raw: OpaquePointer? = path.withCString { cpath in
                ant_init(cpath, &errPtr)
            }
            if let raw = raw {
                return .ready(raw)
            }
            let msg = errPtr.flatMap { String(cString: $0) } ?? "unknown error"
            if let errPtr = errPtr {
                ant_free_string(errPtr)
            }
            return .failed(msg)
        }.value

        switch result {
        case .ready(let h):
            handle = h
            status = .ready
            startPollingPeers(handle: h)
        case .failed(let msg):
            status = .failed(msg)
        }
    }

    func shutdown() async {
        guard let h = handle else { return }
        handle = nil
        peerPollTask?.cancel()
        peerPollTask = nil
        progressPollTask?.cancel()
        progressPollTask = nil
        peerCount = 0
        downloadProgress = nil
        await Task.detached(priority: .userInitiated) {
            ant_shutdown(h)
        }.value
        status = .idle
    }

    /// Poll `ant_peer_count` 5×/s so the UI row tracks the BZZ
    /// handshake progress. `watch::Receiver` is not friendly to a
    /// C-ABI boundary, so polling is the simplest stable shape — and
    /// cheap: each call is a `RwLock` read of the shared snapshot.
    private func startPollingPeers(handle: OpaquePointer) {
        peerPollTask?.cancel()
        peerPollTask = Task { [weak self] in
            while !Task.isCancelled {
                let count = Int(ant_peer_count(handle))
                await MainActor.run {
                    guard let self = self, self.handle == handle else { return }
                    self.peerCount = max(0, count)
                }
                try? await Task.sleep(nanoseconds: 200_000_000)
            }
        }
    }

    /// Abort the in-flight `download(reference:)`. No-op when no
    /// download is running. The racing `download` call throws
    /// `AntError.download("download canceled")` so the view can
    /// surface it (or swallow it, if it already knows the user hit
    /// Pause or Cancel).
    ///
    /// Caveat: this only stops our Swift/FFI wait loop. The embedded
    /// node's `run_get_bzz` task is not cross-request cancellable
    /// today — it keeps fetching remaining chunks in the background
    /// and populates the in-memory chunk cache. That's what makes
    /// "Pause → Download" feel like a resume. A full cancellation
    /// token through `ControlCommand` is a follow-up (PLAN §11).
    func cancelDownload() {
        guard let h = handle else { return }
        ant_cancel_download(h)
    }

    func download(reference: String) async throws -> Data {
        guard let h = handle else {
            throw AntError.notReady
        }
        startPollingProgress(handle: h)
        defer {
            progressPollTask?.cancel()
            progressPollTask = nil
            // Leave the final sample visible for ~400 ms so the UI
            // can freeze on "100 %" instead of snapping back to nothing
            // the instant ant_download returns.
            Task { [weak self] in
                try? await Task.sleep(nanoseconds: 400_000_000)
                await MainActor.run {
                    guard let self = self, self.progressPollTask == nil else { return }
                    self.downloadProgress = nil
                }
            }
        }
        return try await Task.detached(priority: .userInitiated) {
            var len: Int = 0
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let body = reference.withCString { cref in
                ant_download(h, cref, &len, &errPtr)
            }
            if let body = body {
                // Copy into Swift-managed storage so the caller can
                // drop the Rust-owned buffer immediately.
                let data = Data(bytes: body, count: len)
                ant_free_buffer(body, len)
                return data
            }
            let msg = errPtr.flatMap { String(cString: $0) } ?? "unknown download error"
            if let errPtr = errPtr {
                ant_free_string(errPtr)
            }
            throw AntError.download(msg)
        }.value
    }

    /// Start a background poll that reads `ant_download_progress` 4×/s,
    /// derives a smoothed throughput, and publishes the result on the
    /// main actor. Cancelled in `download(reference:)`'s defer.
    private func startPollingProgress(handle: OpaquePointer) {
        progressPollTask?.cancel()
        // Start with a fresh zero-progress sample so the UI can render
        // the HUD immediately (chunks_done=0, total=0) rather than
        // waiting for the first tick — otherwise the first ~150 ms
        // after hitting Download shows no feedback at all.
        downloadProgress = DownloadProgress(
            bytesDone: 0,
            totalBytes: 0,
            chunksDone: 0,
            totalChunks: 0,
            elapsedMs: 0,
            peersUsed: 0,
            inFlight: 0,
            cacheHits: 0,
            throughputBytesPerSec: 0
        )
        progressPollTask = Task { [weak self] in
            // Sliding-window mean over the last `windowSeconds` of
            // (timestamp, bytes_done) samples. Compared to the EMA we
            // had before (alpha = 0.3 → ~830 ms time constant), a
            // straight 3 s window is a) much calmer when chunks land
            // in `buffer_unordered` bursts and b) easier to reason
            // about: the readout literally is "average MiB/s across
            // the last 3 s", which is what users expect a download
            // speedometer to mean. The chunks / peers / in-flight
            // counters keep updating at the full 250 ms cadence
            // because the window only smooths the rate, not the rest.
            let windowSeconds: TimeInterval = 3.0
            var samples: [(at: Date, bytes: UInt64)] = []
            var progress = AntProgress()
            while !Task.isCancelled {
                let rc = ant_download_progress(handle, &progress)
                if rc == 0 {
                    let now = Date()
                    samples.append((now, progress.bytes_done))
                    let cutoff = now.addingTimeInterval(-windowSeconds)
                    if let firstFresh = samples.firstIndex(where: { $0.at >= cutoff }),
                       firstFresh > 1 {
                        // Keep one stale sample so the window has a
                        // baseline at the trailing edge — without it,
                        // dropping every "too old" entry would leave
                        // us computing rate over only the latest few
                        // hundred ms again.
                        samples.removeFirst(firstFresh - 1)
                    }
                    let bytesPerSec: Double
                    if let oldest = samples.first, samples.count >= 2 {
                        let dt = now.timeIntervalSince(oldest.at)
                        if dt > 0.25 {
                            let delta = progress.bytes_done >= oldest.bytes
                                ? Double(progress.bytes_done - oldest.bytes)
                                : 0
                            bytesPerSec = delta / dt
                        } else {
                            bytesPerSec = 0
                        }
                    } else {
                        bytesPerSec = 0
                    }
                    let snapshot = DownloadProgress(
                        bytesDone: progress.bytes_done,
                        totalBytes: progress.total_bytes,
                        chunksDone: progress.chunks_done,
                        totalChunks: progress.total_chunks,
                        elapsedMs: progress.elapsed_ms,
                        peersUsed: progress.peers_used,
                        inFlight: progress.in_flight,
                        cacheHits: progress.cache_hits,
                        throughputBytesPerSec: bytesPerSec
                    )
                    await MainActor.run {
                        guard let self = self, self.handle == handle else { return }
                        self.downloadProgress = snapshot
                    }
                }
                try? await Task.sleep(nanoseconds: 250_000_000)
            }
        }
    }

    private static func resolveDataDir() -> URL {
        let fm = FileManager.default
        let base = (try? fm.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )) ?? fm.temporaryDirectory
        let dir = base.appendingPathComponent("ant", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }
}

private enum InitOutcome {
    case ready(OpaquePointer)
    case failed(String)
}

/// Swift-friendly value type mirroring `AntProgress` plus a smoothed
/// throughput field derived on the Swift side (the Rust FFI doesn't
/// compute rates — it just ships the raw counters).
struct DownloadProgress: Equatable {
    var bytesDone: UInt64
    var totalBytes: UInt64
    var chunksDone: UInt64
    var totalChunks: UInt64
    var elapsedMs: UInt64
    var peersUsed: UInt32
    var inFlight: UInt32
    var cacheHits: UInt64
    /// Exponential-moving-average of `delta_bytes / delta_time` over
    /// the poll samples. `0` until we have at least two samples.
    var throughputBytesPerSec: Double

    /// `0.0 ... 1.0` progress fraction, or `nil` while the data root
    /// hasn't landed yet (which is when `totalBytes == 0`).
    var fraction: Double? {
        guard totalBytes > 0 else { return nil }
        return min(1.0, Double(bytesDone) / Double(totalBytes))
    }
}

enum AntError: LocalizedError {
    case notReady
    case download(String)

    var errorDescription: String? {
        switch self {
        case .notReady: return "Node is not ready"
        case .download(let msg): return msg
        }
    }
}
