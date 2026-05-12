import AVFoundation
import Foundation

/// Bridges AVPlayer's byte-range loading to an `AntStreamSession`.
///
/// We mint a custom URL scheme (`ant-asset://`) so AVURLAsset routes
/// every range request through our delegate. The first request always
/// asks for `contentInformationRequest` first; we synthesise that from
/// the session's `totalBytes` + `contentType`. Subsequent requests
/// carry `dataRequest.requestedOffset` / `requestedLength`, which we
/// satisfy by reading the session in chunked passes (so AVPlayer can
/// see partial data while a slow read finishes).
final class AntStreamLoader: NSObject, AVAssetResourceLoaderDelegate {
    static let scheme = "ant-asset"

    private let session: AntStreamSession
    private let queue: DispatchQueue
    private var activeRequests = Set<RequestBox>()
    private let activeLock = NSLock()

    init(session: AntStreamSession) {
        self.session = session
        // Concurrent: AVPlayer routinely fires multiple dataRequests in
        // flight at once (a small head sniff, the toEnd= long pull, and
        // a tail-end probe to read the WAV trailer / mp4 moov are
        // typical). With a serial queue the toEnd pull would block the
        // tail probe behind it for the entire file, which AVPlayer
        // interprets as "the resource isn't actually byte-range
        // capable" and gives up.
        self.queue = DispatchQueue(
            label: "at.vibing.ant.streamloader.\(UUID().uuidString)",
            qos: .userInitiated,
            attributes: .concurrent
        )
        super.init()
    }

    deinit {
        cancelAll()
    }

    /// Wrap the bzz reference in an `ant-asset://` URL so AVURLAsset
    /// routes through us. The reference itself is opaque to the URL —
    /// we already know which session is bound to which loader.
    static func assetURL(for reference: String) -> URL {
        var components = URLComponents()
        components.scheme = scheme
        components.host = "stream"
        components.path = "/" + UUID().uuidString
        components.queryItems = [URLQueryItem(name: "ref", value: reference)]
        return components.url!
    }

    func resourceLoader(
        _ resourceLoader: AVAssetResourceLoader,
        shouldWaitForLoadingOfRequestedResource loadingRequest: AVAssetResourceLoadingRequest
    ) -> Bool {
        if let info = loadingRequest.contentInformationRequest {
            let mime = session.contentType ?? "audio/wav"
            // AVAssetResourceLoader's `contentType` field expects a UTI
            // (`com.microsoft.waveform-audio`), not a MIME string —
            // AVPlayer keys off the UTI when picking a reader and
            // silently cancels the dataRequest after ~10 s otherwise.
            info.contentType = AntStreamLoader.uti(forMimeType: mime) ?? "public.wav"
            info.contentLength = Int64(session.totalBytes)
            info.isByteRangeAccessSupported = true
        }
        guard let dataRequest = loadingRequest.dataRequest else {
            loadingRequest.finishLoading()
            return true
        }
        let box = RequestBox(loadingRequest: loadingRequest)
        addActive(box)
        let offset = UInt64(max(0, dataRequest.currentOffset))
        let totalBytes = session.totalBytes
        let requested: Int
        if dataRequest.requestsAllDataToEndOfResource {
            let remaining = totalBytes > offset ? totalBytes - offset : 0
            requested = Int(min(remaining, UInt64(Int.max)))
        } else {
            requested = dataRequest.requestedLength
        }
        queue.async { [weak self] in
            self?.fulfil(box: box, offset: offset, requested: requested)
        }
        return true
    }

    /// AVAssetResourceLoader prefers UTI-style content types
    /// (`public.wav`) over MIME (`audio/wav`). Falling back to a UTI
    /// is what AVPlayer actually keys off when picking a reader; with
    /// a raw MIME string AVPlayer often refuses to start parsing the
    /// stream and silently cancels the dataRequest after ~10 s.
    private static func uti(forMimeType mime: String) -> String? {
        switch mime.lowercased() {
        case "audio/wav", "audio/wave", "audio/x-wav": return "com.microsoft.waveform-audio"
        case "audio/mp3", "audio/mpeg", "audio/mpeg3", "audio/x-mpeg-3": return "public.mp3"
        case "audio/aac", "audio/x-aac", "audio/mp4", "audio/m4a": return "public.mpeg-4-audio"
        case "audio/flac", "audio/x-flac": return "org.xiph.flac"
        case "audio/ogg", "application/ogg": return "org.xiph.ogg-vorbis"
        case "audio/opus": return "org.xiph.opus"
        default: return nil
        }
    }

    func resourceLoader(
        _ resourceLoader: AVAssetResourceLoader,
        didCancel loadingRequest: AVAssetResourceLoadingRequest
    ) {
        activeLock.lock()
        defer { activeLock.unlock() }
        for box in activeRequests where box.loadingRequest == loadingRequest {
            box.cancelled = true
        }
    }

    private func fulfil(box: RequestBox, offset: UInt64, requested: Int) {
        defer { removeActive(box) }
        let length = UInt64(max(0, requested))
        do {
            _ = try session.pull(offset: offset, length: length) { buffer in
                if box.cancelled { return true }
                let data = Data(buffer: buffer)
                box.loadingRequest.dataRequest?.respond(with: data)
                return false
            }
        } catch {
            NSLog("[ant-loader] pull error offset=%llu: %@", offset, "\(error)")
            if box.cancelled { return }
            box.loadingRequest.finishLoading(with: error)
            return
        }
        if !box.cancelled {
            box.loadingRequest.finishLoading()
        }
    }

    private func addActive(_ box: RequestBox) {
        activeLock.lock()
        activeRequests.insert(box)
        activeLock.unlock()
    }

    private func removeActive(_ box: RequestBox) {
        activeLock.lock()
        activeRequests.remove(box)
        activeLock.unlock()
    }

    private func cancelAll() {
        activeLock.lock()
        for box in activeRequests { box.cancelled = true }
        activeLock.unlock()
    }

    private final class RequestBox: Hashable {
        let loadingRequest: AVAssetResourceLoadingRequest
        var cancelled: Bool = false
        init(loadingRequest: AVAssetResourceLoadingRequest) {
            self.loadingRequest = loadingRequest
        }
        static func == (lhs: RequestBox, rhs: RequestBox) -> Bool {
            lhs === rhs
        }
        func hash(into hasher: inout Hasher) {
            hasher.combine(ObjectIdentifier(self))
        }
    }
}
