import Foundation

/// JSON model returned by `ant_list_bzz`. Mirror of `ManifestWire` /
/// `ManifestEntryWire` in `crates/ant-ffi/src/manifest.rs`.
struct Manifest: Decodable, Equatable {
    let entries: [Entry]

    struct Entry: Decodable, Equatable, Identifiable {
        var id: String { path }
        let path: String
        let reference: String?
        let size: UInt64?
        let contentType: String?

        enum CodingKeys: String, CodingKey {
            case path, reference, size
            case contentType = "content_type"
        }
    }
}

/// A playable item, derived from one manifest entry. The Liquid Glass
/// player UI binds to these directly; the heavy lifting (resolving the
/// reference into a stream of bytes) lives in `PlayerEngine`.
struct Track: Hashable, Identifiable {
    let id: String
    let manifestRoot: String
    let path: String
    let title: String
    let filename: String
    let sizeBytes: UInt64?
    let contentType: String?
    let fileExtension: String

    /// `bzz://<root>/<percent-encoded-path>`. Used as the lookup key into
    /// `ant_stream_open` and as the asset URL we hand to AVPlayer (with
    /// the scheme rewritten to a custom one so the resource loader
    /// delegate gets called).
    var bzzReference: String {
        let encoded = path
            .split(separator: "/", omittingEmptySubsequences: false)
            .map { component -> String in
                let s = String(component)
                let allowed = CharacterSet.urlPathAllowed
                return s.addingPercentEncoding(withAllowedCharacters: allowed) ?? s
            }
            .joined(separator: "/")
        return "bzz://\(manifestRoot)/\(encoded)"
    }

    /// "FLAC", "WAV", "MP3"… for the title row in the screenshot.
    var formatLabel: String {
        let trimmed = fileExtension.uppercased()
        switch trimmed {
        case "M4A": return "AAC"
        default: return trimmed
        }
    }

    init(manifestRoot: String, entry: Manifest.Entry) {
        let lastComponent = (entry.path as NSString).lastPathComponent
        let ext = (lastComponent as NSString).pathExtension
        let stem = (lastComponent as NSString).deletingPathExtension
        self.id = "\(manifestRoot)/\(entry.path)"
        self.manifestRoot = manifestRoot
        self.path = entry.path
        self.title = Track.prettify(stem: stem)
        self.filename = lastComponent
        self.sizeBytes = entry.size
        self.contentType = entry.contentType
        self.fileExtension = ext
    }

    /// "01 starlight" → "Starlight". Drops a leading numeric track
    /// prefix and the dash/space separator most discographies ship with.
    private static func prettify(stem: String) -> String {
        var s = stem
        if let space = s.firstIndex(of: " "),
           s.prefix(upTo: space).allSatisfy({ $0.isNumber }) {
            s = String(s[s.index(after: space)...])
        }
        if s.hasPrefix("-") || s.hasPrefix("_") { s.removeFirst() }
        s = s.replacingOccurrences(of: "_", with: " ")
        return s.prefix(1).uppercased() + s.dropFirst()
    }

    static let audioExtensions: Set<String> = [
        "wav", "flac", "mp3", "m4a", "aac", "ogg", "opus", "alac",
    ]

    /// Filter entries to "things AVFoundation can play directly". WAVs
    /// dominate the litter-ally test set; FLAC/MP3/AAC come along for
    /// free because AVAsset sniffs by container, not extension.
    ///
    /// Currently unused — the player ships with a hard-coded
    /// `Track.litterAlly` queue so a cold launch can autoplay the first
    /// track without paying for a manifest walk first. Kept here so a
    /// future settings sheet can offer "load any bzz:// reference"
    /// without touching the FFI.
    static func playableAudio(from manifest: Manifest, root: String) -> [Track] {
        manifest.entries
            .filter { entry in
                let ext = ((entry.path as NSString).pathExtension).lowercased()
                if audioExtensions.contains(ext) { return true }
                if let ct = entry.contentType?.lowercased(), ct.hasPrefix("audio/") {
                    return true
                }
                return false
            }
            .sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending }
            .map { Track(manifestRoot: root, entry: $0) }
    }
}

// MARK: - litter-ally.eth — hard-coded playlist

extension Track {
    /// `bzz://` root for the litter-ally.eth manifest. Same hash the
    /// `0.2.0` smoke tests + Plan §"Disk cache throughput" benches use,
    /// so anything we play here is also under exercise from the Rust
    /// retrieval suite.
    static let litterAllyRoot: String =
        "c4f8a45301b57d0e36f0f5348ed371aee42ea0b9fe9b3caaf26015d652eedc40"

    /// Album cover served by the same manifest. ~60 KB JPEG; fetched
    /// once at startup and shared across every track in the queue.
    static let litterAllyCoverReference: String =
        "bzz://\(litterAllyRoot)/cover.jpg"

    /// 12 WAV masters from the litter-ally.eth `tracks/` folder. File
    /// names + sizes captured directly from the live `litter-ally.eth.limo`
    /// gateway (the same gateway that backs the bee-side smoke); we
    /// hard-code rather than walking the mantaray manifest because:
    ///
    ///   1. Cold-start latency. The manifest walk sits on the BZZ
    ///      handshake until we have peers, which is 5–30 s of
    ///      "Loading queue…" before the user can press play.
    ///   2. Autoplay UX. With a hard-coded list we can hand the first
    ///      track to AVPlayer the instant the node reports ready and
    ///      let the resource loader handle the cold-start retries
    ///      transparently.
    ///   3. Fewer failure modes. `ant_list_bzz` succeeds 99.9 % of the
    ///      time but a transient network blip on cold start would put
    ///      the UI into a `.failed` state with no recovery.
    ///
    /// Indices match the JavaScript playlist on litter-ally.eth itself.
    /// Note that "07 sallarom.wav" is intentionally a different track
    /// from "03 sallarom.wav" — the album re-uses the title.
    static let litterAlly: [Track] = [
        litterAllyTrack(filename: "01 arrival.wav",     size: 54_958_120),
        litterAllyTrack(filename: "02 butterfly.wav",   size: 46_064_680),
        litterAllyTrack(filename: "03 sallarom.wav",    size: 61_455_400),
        litterAllyTrack(filename: "04 unruly.wav",      size: 72_960_040),
        litterAllyTrack(filename: "05 high five.wav",   size: 36_879_400),
        litterAllyTrack(filename: "06 flutterby.wav",   size: 117_542_440),
        litterAllyTrack(filename: "07 sallarom.wav",    size: 58_682_920),
        litterAllyTrack(filename: "08 reihna.wav",      size: 95_377_960),
        litterAllyTrack(filename: "09 melancholy.wav",  size: 71_900_200),
        litterAllyTrack(filename: "10 ruly.wav",        size: 62_453_800),
        litterAllyTrack(filename: "11 eleven.wav",      size: 47_424_040),
        litterAllyTrack(filename: "12 phiway.wav",      size: 86_361_640),
    ]

    private static func litterAllyTrack(filename: String, size: UInt64) -> Track {
        let entry = Manifest.Entry(
            path: "tracks/\(filename)",
            reference: nil,
            size: size,
            contentType: "audio/wav"
        )
        return Track(manifestRoot: litterAllyRoot, entry: entry)
    }
}
