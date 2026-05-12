import Foundation

/// Swift wrapper around an `AntStream*` from the C FFI. Carries the
/// total file size + content type that `ant_stream_open` reported, plus
/// a serial dispatch queue for the `ant_stream_read` calls AVPlayer's
/// resource loader fires off.
///
/// The session keeps the AntStream* alive for its lifetime and frees
/// it in `deinit`. AntNode (the owner of the AntHandle) must outlive
/// the session — our caller enforces this by holding the session as a
/// per-track property on `PlayerEngine` and only ever creating sessions
/// while the node is `.ready`.
final class AntStreamSession: @unchecked Sendable {
    let totalBytes: UInt64
    let contentType: String?
    let bzzReference: String

    private let handle: OpaquePointer
    private let stream: OpaquePointer
    private let readQueue: DispatchQueue
    private let lock = NSLock()
    private var closed = false

    init?(handle: OpaquePointer, bzzReference: String) {
        var totalBytes: UInt64 = 0
        var contentTypePtr: UnsafeMutablePointer<CChar>? = nil
        var errPtr: UnsafeMutablePointer<CChar>? = nil

        let rawStream: OpaquePointer? = bzzReference.withCString { cref in
            ant_stream_open(handle, cref, &totalBytes, &contentTypePtr, &errPtr)
        }
        guard let stream = rawStream else {
            if let errPtr = errPtr {
                let msg = String(cString: errPtr)
                ant_free_string(errPtr)
                NSLog("ant_stream_open failed: %@", msg)
            }
            return nil
        }
        self.handle = handle
        self.stream = stream
        self.totalBytes = totalBytes
        self.bzzReference = bzzReference
        if let ctPtr = contentTypePtr {
            self.contentType = String(cString: ctPtr)
            ant_free_string(ctPtr)
        } else {
            self.contentType = nil
        }
        self.readQueue = DispatchQueue(
            label: "at.vibing.ant.streamsession.\(UUID().uuidString)",
            qos: .userInitiated
        )
    }

    deinit {
        close()
    }

    func close() {
        lock.lock()
        defer { lock.unlock() }
        if closed { return }
        ant_stream_close(stream)
        closed = true
    }

    /// Blocking read; safe to call from any non-main thread. Returns
    /// the bytes that were actually written (≤ requested) or throws.
    func read(offset: UInt64, length: Int) throws -> Data {
        lock.lock()
        let isClosed = closed
        lock.unlock()
        if isClosed { throw AntStreamError.closed }
        if length <= 0 { return Data() }

        var buffer = [UInt8](repeating: 0, count: length)
        var errPtr: UnsafeMutablePointer<CChar>? = nil
        let written = buffer.withUnsafeMutableBufferPointer { ptr -> Int in
            let n = ant_stream_read(stream, offset, length, ptr.baseAddress, &errPtr)
            return n
        }
        if written < 0 {
            let msg: String
            if let errPtr = errPtr {
                msg = String(cString: errPtr)
                ant_free_string(errPtr)
            } else {
                msg = "unknown stream error"
            }
            throw AntStreamError.read(msg)
        }
        if written < length {
            buffer.removeLast(length - written)
        }
        return Data(buffer)
    }

    /// Async wrapper that hops onto the per-session serial queue so
    /// AVAssetResourceLoaderDelegate callbacks (which already arrive on
    /// our delegate queue) don't all pile up on the same FFI thread.
    func read(offset: UInt64, length: Int) async throws -> Data {
        try await withCheckedThrowingContinuation { cont in
            readQueue.async {
                do {
                    let data = try self.read(offset: offset, length: length)
                    cont.resume(returning: data)
                } catch {
                    cont.resume(throwing: error)
                }
            }
        }
    }

    /// Pull a contiguous byte range, invoking `onChunk` with each
    /// joiner-produced chunk as it lands. One long-running StreamBzz
    /// services the entire range — the manifest walk + data-root
    /// fetch + joiner setup happen exactly once for the whole pull,
    /// not once per AVPlayer slice. Crucial for keeping AVPlayer's
    /// `dataRequest` alive: with the per-slice `read` model, AVPlayer
    /// canceled the toEnd request after ~10 s because the first
    /// 256 KiB took 13 s to land.
    ///
    /// `onChunk` is called from a dispatched FFI worker thread; if
    /// the caller wants to interact with main-actor state it must
    /// hop. Returning `true` from the closure asks the FFI to stop
    /// pulling early (e.g. AVPlayer canceled the dataRequest).
    ///
    /// Returns the total number of bytes delivered to `onChunk`.
    func pull(
        offset: UInt64,
        length: UInt64,
        onChunk: @escaping (UnsafeBufferPointer<UInt8>) -> Bool
    ) throws -> UInt64 {
        lock.lock()
        let isClosed = closed
        lock.unlock()
        if isClosed { throw AntStreamError.closed }
        if length == 0 { return 0 }

        // The C ABI shape can't carry an arbitrary Swift closure across
        // the boundary, so we box the closure on the heap, pass its
        // address as `ctx`, and reconstruct it inside the C callback.
        // The pointer is held alive by `Unmanaged.retain` until the
        // pull completes and we balance with `release` below.
        final class Box {
            var fn: (UnsafeBufferPointer<UInt8>) -> Bool
            init(fn: @escaping (UnsafeBufferPointer<UInt8>) -> Bool) { self.fn = fn }
        }
        let box = Box(fn: onChunk)
        let unmanaged = Unmanaged.passRetained(box)
        defer { unmanaged.release() }
        let ctx = UnsafeMutableRawPointer(unmanaged.toOpaque())

        let cb: AntStreamChunkCb = { ctx, data, len in
            guard let ctx = ctx, let data = data else { return 0 }
            let box = Unmanaged<Box>.fromOpaque(ctx).takeUnretainedValue()
            let buf = UnsafeBufferPointer<UInt8>(start: data, count: len)
            return box.fn(buf) ? 1 : 0
        }

        var errPtr: UnsafeMutablePointer<CChar>? = nil
        let delivered = ant_stream_pull(stream, offset, length, cb, ctx, &errPtr)
        if delivered < 0 {
            let msg: String
            if let errPtr = errPtr {
                msg = String(cString: errPtr)
                ant_free_string(errPtr)
            } else {
                msg = "unknown stream pull error"
            }
            throw AntStreamError.read(msg)
        }
        return UInt64(delivered)
    }

    /// Snapshot the cumulative stream progress (bytes downloaded so
    /// far, total size, last per-request peer/in-flight stats). Used
    /// by the Network tab; cheap to call.
    func progressSnapshot() -> StreamProgress {
        var raw = AntProgress()
        let rc = ant_stream_progress(stream, &raw)
        if rc != 0 {
            return StreamProgress(
                bytesDownloaded: 0,
                totalBytes: totalBytes,
                chunksDone: 0,
                totalChunks: 0,
                elapsedMs: 0,
                peersUsed: 0,
                inFlight: 0,
                cacheHits: 0
            )
        }
        return StreamProgress(
            bytesDownloaded: raw.bytes_done,
            totalBytes: max(raw.total_bytes, totalBytes),
            chunksDone: raw.chunks_done,
            totalChunks: raw.total_chunks,
            elapsedMs: raw.elapsed_ms,
            peersUsed: raw.peers_used,
            inFlight: raw.in_flight,
            cacheHits: raw.cache_hits
        )
    }
}

struct StreamProgress: Equatable {
    var bytesDownloaded: UInt64
    var totalBytes: UInt64
    var chunksDone: UInt64
    var totalChunks: UInt64
    var elapsedMs: UInt64
    var peersUsed: UInt32
    var inFlight: UInt32
    var cacheHits: UInt64

    var fraction: Double {
        guard totalBytes > 0 else { return 0 }
        return min(1.0, Double(bytesDownloaded) / Double(totalBytes))
    }
}

enum AntStreamError: LocalizedError {
    case closed
    case read(String)

    var errorDescription: String? {
        switch self {
        case .closed: return "stream is closed"
        case .read(let msg): return msg
        }
    }
}
