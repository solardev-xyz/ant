import AVFoundation
import Combine
import Foundation
import MediaPlayer
import SwiftUI

/// The brains behind the Player + Network tabs.
///
/// Owns a single `AVPlayer`, the play queue, the current-track stream
/// session, and the lock-screen / control-center wiring. Keeps a
/// rolling 60 s window of throughput samples so the Network tab can
/// render the sparkline straight off `@Published` state.
@MainActor
final class PlayerEngine: ObservableObject {
    @Published private(set) var queue: [Track]
    @Published private(set) var currentIndex: Int = 0
    @Published private(set) var isPlaying: Bool = false
    @Published private(set) var currentTime: TimeInterval = 0
    /// Target position while a seek is in flight and AVPlayer is still
    /// fetching bytes for the new offset. The scrubber reads
    /// [`displayTime`] so the thumb does not snap back to the pre-seek
    /// clock reported by the periodic time observer.
    @Published private(set) var pendingSeekTime: TimeInterval? = nil
    @Published private(set) var duration: TimeInterval = 0
    @Published private(set) var playbackStatus: PlaybackStatus = .waitingForPeers
    @Published private(set) var activeSession: AntStreamSession? = nil
    @Published private(set) var streamProgress: StreamProgress? = nil
    @Published private(set) var throughputBytesPerSec: Double = 0
    @Published private(set) var throughputHistory: [ThroughputSample] = []
    @Published private(set) var peakThroughputBytesPerSec: Double = 0
    /// Decoded album cover for the current playlist. Resolved once at
    /// startup via [`coverArtReference`] and shared across all tracks
    /// — the litter-ally manifest publishes a single `cover.jpg` for
    /// the whole album. `nil` while the fetch is in flight or if it
    /// fails (the artwork view falls back to the SF symbol then).
    @Published private(set) var coverArt: UIImage? = nil
    /// `bzz://<root>/cover.jpg` for the queue's album. Hard-coded
    /// alongside the queue today; a future "load any bzz://" sheet
    /// would replace this when the playlist is rebuilt.
    let coverArtReference: String?

    private var didStartFirstTrack = false
    private let node: AntNode
    private let player = AVPlayer()
    private var playerItem: AVPlayerItem?
    private var loader: AntStreamLoader?
    private var asset: AVURLAsset?
    private var timeObserverToken: Any?
    private var statusObservation: NSKeyValueObservation?
    private var rateObservation: NSKeyValueObservation?
    private var endObserver: NSObjectProtocol?
    private var progressTask: Task<Void, Never>?
    private var bytesSamples: [(at: Date, bytes: UInt64)] = []
    private var resumePlaybackAfterSeek = false
    private var seekGeneration: UInt64 = 0
    private var bufferObservation: NSKeyValueObservation?
    private var seekResumeTask: Task<Void, Never>?

    /// Player-engine state machine. Hard-coded playlist means we never
    /// need a "manifest loading" / "manifest failed" state — the queue
    /// is populated synchronously at init. The only thing we wait on
    /// is the embedded node finishing its BZZ handshakes; once at
    /// least one peer replies, the resource loader's first range read
    /// stops returning "no peers available" and AVPlayer gets bytes.
    enum PlaybackStatus: Equatable {
        case waitingForPeers
        case ready
        case failed(String)
    }

    var currentTrack: Track? {
        guard !queue.isEmpty, queue.indices.contains(currentIndex) else { return nil }
        return queue[currentIndex]
    }

    /// Scrubber / elapsed label: live clock unless a seek is pending.
    var displayTime: TimeInterval {
        pendingSeekTime ?? currentTime
    }

    init(
        node: AntNode,
        queue: [Track] = Track.litterAlly,
        coverArtReference: String? = Track.litterAllyCoverReference
    ) {
        self.node = node
        self.queue = queue
        self.coverArtReference = coverArtReference
        configureAudioSession()
        configureRemoteCommands()
        observePlayer()
        startProgressPoll()
    }

    deinit {
        if let token = timeObserverToken {
            player.removeTimeObserver(token)
        }
        if let endObserver = endObserver {
            NotificationCenter.default.removeObserver(endObserver)
        }
        progressTask?.cancel()
    }

    // MARK: – playback bootstrap

    /// Hand the first track to AVPlayer once the embedded node is up.
    /// The resource loader handles the cold-start "no peers yet" case
    /// internally, so we don't need to gate on a peer count threshold
    /// — the moment the node returns from `start()`, we can issue the
    /// first stream open and let the FFI's retry envelope swallow any
    /// transient failures.
    func startFirstTrackIfNeeded() async {
        guard !didStartFirstTrack else { return }
        guard !queue.isEmpty else { return }
        didStartFirstTrack = true
        playbackStatus = .ready
        // Fire the cover fetch in parallel with the first stream open
        // so the UI gets artwork as soon as the network warms up,
        // without delaying the audio path waiting for it.
        if coverArt == nil, let ref = coverArtReference {
            Task { [weak self] in await self?.loadCoverArt(reference: ref) }
        }
        await prepareCurrent(autoPlay: true)
    }

    /// Best-effort album-art fetch. ~60 KB JPEG, off the main path —
    /// failure just leaves the SF-symbol placeholder visible. Updates
    /// `coverArt` (and the `MPNowPlayingInfoCenter` artwork) on
    /// success so lock-screen / Control Center pick up the same
    /// image.
    private func loadCoverArt(reference: String) async {
        do {
            let data = try await node.downloadQuiet(reference: reference)
            guard let image = UIImage(data: data) else {
                NSLog("[player] cover.jpg decode failed (%d B)", data.count)
                return
            }
            self.coverArt = image
            updateNowPlaying()
        } catch {
            NSLog("[player] cover fetch failed: %@", "\(error)")
        }
    }

    // MARK: – transport

    func togglePlay() {
        if isPlaying {
            resumePlaybackAfterSeek = false
            player.pause()
        } else {
            player.play()
        }
    }

    func play() { player.play() }
    func pause() {
        resumePlaybackAfterSeek = false
        player.pause()
    }

    func next() {
        guard !queue.isEmpty else { return }
        let nextIndex = (currentIndex + 1) % queue.count
        select(index: nextIndex, autoPlay: true)
    }

    func previous() {
        guard !queue.isEmpty else { return }
        if currentTime > 3 {
            seek(to: 0)
            return
        }
        let prev = currentIndex == 0 ? queue.count - 1 : currentIndex - 1
        select(index: prev, autoPlay: true)
    }

    func select(index: Int, autoPlay: Bool) {
        guard queue.indices.contains(index) else { return }
        currentIndex = index
        Task { await prepareCurrent(autoPlay: autoPlay) }
    }

    func seek(to seconds: TimeInterval) {
        guard duration > 0 else { return }
        let clamped = max(0, min(seconds, duration))
        let target = CMTime(seconds: clamped, preferredTimescale: 600)

        cancelSeekWait()
        seekGeneration &+= 1
        let generation = seekGeneration
        resumePlaybackAfterSeek = isPlaying
        player.pause()
        pendingSeekTime = clamped
        currentTime = clamped
        updateNowPlaying()

        player.seek(to: target, toleranceBefore: .zero, toleranceAfter: .zero) { [weak self] finished in
            Task { @MainActor in
                guard let self, self.seekGeneration == generation else { return }
                guard finished else {
                    self.pendingSeekTime = nil
                    self.resumePlaybackAfterSeek = false
                    return
                }
                self.waitForBufferThenFinishSeek(generation: generation)
            }
        }
    }

    // MARK: – internals

    private func cancelSeekWait() {
        seekResumeTask?.cancel()
        seekResumeTask = nil
        bufferObservation?.invalidate()
        bufferObservation = nil
    }

    private func waitForBufferThenFinishSeek(generation: UInt64) {
        guard seekGeneration == generation else { return }
        guard let item = player.currentItem else {
            finishSeekPlayback(generation: generation)
            return
        }
        if item.isPlaybackLikelyToKeepUp {
            finishSeekPlayback(generation: generation)
            return
        }
        bufferObservation = item.observe(\.isPlaybackLikelyToKeepUp, options: [.new]) { [weak self] item, _ in
            guard item.isPlaybackLikelyToKeepUp else { return }
            Task { @MainActor in self?.finishSeekPlayback(generation: generation) }
        }
        seekResumeTask?.cancel()
        seekResumeTask = Task { [weak self] in
            let pollInterval: UInt64 = 250_000_000
            let maxPolls = 120
            for _ in 0..<maxPolls {
                try? await Task.sleep(nanoseconds: pollInterval)
                guard !Task.isCancelled else { return }
                let ready = await MainActor.run { () -> Bool in
                    guard let self else { return true }
                    if self.seekGeneration != generation { return true }
                    if self.pendingSeekTime == nil { return true }
                    return self.player.currentItem?.isPlaybackLikelyToKeepUp == true
                }
                if ready {
                    await MainActor.run { self?.finishSeekPlayback(generation: generation) }
                    return
                }
            }
            await MainActor.run { self?.finishSeekPlayback(generation: generation) }
        }
    }

    private func finishSeekPlayback(generation: UInt64) {
        guard seekGeneration == generation, pendingSeekTime != nil else { return }
        cancelSeekWait()
        if let target = pendingSeekTime {
            currentTime = target
        }
        pendingSeekTime = nil
        updateNowPlaying()
        if resumePlaybackAfterSeek {
            player.play()
        }
        resumePlaybackAfterSeek = false
    }

    private func prepareCurrent(autoPlay: Bool) async {
        guard let track = currentTrack else { return }
        cancelSeekWait()
        pendingSeekTime = nil
        resumePlaybackAfterSeek = false
        player.pause()
        // The periodic time observer and the `rate` KVO live on the
        // AVPlayer instance, not on individual items, so they survive
        // `replaceCurrentItem` and we install them exactly once in
        // `observePlayer()`. Tearing them down here was the bug
        // behind "the slider stops moving after the first track" —
        // without a re-add, `currentTime` never ticked and `isPlaying`
        // never flipped, which made the pause button look dead too.
        statusObservation?.invalidate()
        statusObservation = nil
        if let endObserver = endObserver {
            NotificationCenter.default.removeObserver(endObserver)
            self.endObserver = nil
        }
        playerItem = nil
        asset = nil
        loader = nil
        activeSession = nil
        streamProgress = nil
        throughputBytesPerSec = 0
        currentTime = 0
        duration = 0
        bytesSamples.removeAll(keepingCapacity: true)

        do {
            let session = try await node.openStream(reference: track.bzzReference)
            self.activeSession = session
            let url = AntStreamLoader.assetURL(for: track.bzzReference)
            let loader = AntStreamLoader(session: session)
            let asset = AVURLAsset(url: url)
            asset.resourceLoader.setDelegate(loader, queue: .global(qos: .userInitiated))
            self.loader = loader
            self.asset = asset
            let item = AVPlayerItem(asset: asset)
            self.playerItem = item
            attachItemObservers(item: item)
            player.replaceCurrentItem(with: item)
            updateNowPlaying()
            if autoPlay {
                player.play()
            }
        } catch {
            playbackStatus = .failed("stream open: \(error)")
        }
    }

    private func attachItemObservers(item: AVPlayerItem) {
        statusObservation = item.observe(\.status, options: [.new]) { [weak self] item, _ in
            guard let self else { return }
            Task { @MainActor in
                if item.status == .readyToPlay {
                    self.duration = item.duration.seconds.isFinite ? item.duration.seconds : 0
                    self.updateNowPlaying()
                }
            }
        }
        endObserver = NotificationCenter.default.addObserver(
            forName: .AVPlayerItemDidPlayToEndTime,
            object: item,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor in self?.next() }
        }
    }

    private func observePlayer() {
        let interval = CMTime(seconds: 0.25, preferredTimescale: 4)
        timeObserverToken = player.addPeriodicTimeObserver(forInterval: interval, queue: .main) { [weak self] time in
            // The .main queue we passed in guarantees this fires on the
            // main thread, but Swift's actor checker can't see that —
            // assumeIsolated lets us touch our @Published state here
            // without a Task hop that would lose a tick of UI updates.
            MainActor.assumeIsolated {
                guard let self else { return }
                if self.pendingSeekTime == nil {
                    self.currentTime = time.seconds.isFinite ? time.seconds : 0
                }
                if let item = self.player.currentItem,
                   item.duration.seconds.isFinite,
                   item.duration.seconds > 0,
                   self.duration != item.duration.seconds {
                    self.duration = item.duration.seconds
                }
                self.updateNowPlaying()
            }
        }
        rateObservation = player.observe(\.rate, options: [.new]) { [weak self] player, _ in
            Task { @MainActor in self?.isPlaying = player.rate > 0 }
        }
    }

    private func configureAudioSession() {
        do {
            let session = AVAudioSession.sharedInstance()
            try session.setCategory(.playback, mode: .default, options: [])
            try session.setActive(true)
        } catch {
            NSLog("audio session: %@", "\(error)")
        }
    }

    private func configureRemoteCommands() {
        let center = MPRemoteCommandCenter.shared()
        center.playCommand.addTarget { [weak self] _ in
            self?.play()
            return .success
        }
        center.pauseCommand.addTarget { [weak self] _ in
            self?.pause()
            return .success
        }
        center.togglePlayPauseCommand.addTarget { [weak self] _ in
            self?.togglePlay()
            return .success
        }
        center.nextTrackCommand.addTarget { [weak self] _ in
            self?.next()
            return .success
        }
        center.previousTrackCommand.addTarget { [weak self] _ in
            self?.previous()
            return .success
        }
        center.changePlaybackPositionCommand.addTarget { [weak self] event in
            guard let positionEvent = event as? MPChangePlaybackPositionCommandEvent else {
                return .commandFailed
            }
            self?.seek(to: positionEvent.positionTime)
            return .success
        }
    }

    private func updateNowPlaying() {
        guard let track = currentTrack else { return }
        var info: [String: Any] = [:]
        info[MPMediaItemPropertyTitle] = track.title
        info[MPMediaItemPropertyAlbumTitle] = "Swarm"
        info[MPMediaItemPropertyPlaybackDuration] = duration
        info[MPNowPlayingInfoPropertyElapsedPlaybackTime] = displayTime
        info[MPNowPlayingInfoPropertyPlaybackRate] = player.rate
        if let image = coverArt {
            let artwork = MPMediaItemArtwork(boundsSize: image.size) { _ in image }
            info[MPMediaItemPropertyArtwork] = artwork
        }
        MPNowPlayingInfoCenter.default().nowPlayingInfo = info
    }

    private func startProgressPoll() {
        progressTask?.cancel()
        progressTask = Task { [weak self] in
            let windowSeconds: TimeInterval = 3.0
            let historyWindow: TimeInterval = 60.0
            while !Task.isCancelled {
                if let session = await MainActor.run(body: { self?.activeSession }) {
                    let snapshot = await Task.detached { session.progressSnapshot() }.value
                    await MainActor.run {
                        guard let self = self else { return }
                        self.streamProgress = snapshot
                        let now = Date()
                        self.bytesSamples.append((now, snapshot.bytesDownloaded))
                        let cutoff = now.addingTimeInterval(-windowSeconds)
                        if let firstFresh = self.bytesSamples.firstIndex(where: { $0.at >= cutoff }),
                           firstFresh > 1 {
                            self.bytesSamples.removeFirst(firstFresh - 1)
                        }
                        let bps: Double
                        if let oldest = self.bytesSamples.first, self.bytesSamples.count >= 2 {
                            let dt = now.timeIntervalSince(oldest.at)
                            if dt > 0.25 {
                                let delta = snapshot.bytesDownloaded >= oldest.bytes
                                    ? Double(snapshot.bytesDownloaded - oldest.bytes)
                                    : 0
                                bps = delta / dt
                            } else {
                                bps = 0
                            }
                        } else {
                            bps = 0
                        }
                        self.throughputBytesPerSec = bps
                        self.peakThroughputBytesPerSec = max(self.peakThroughputBytesPerSec, bps)
                        self.throughputHistory.append(ThroughputSample(at: now, bytesPerSec: bps))
                        let historyCutoff = now.addingTimeInterval(-historyWindow)
                        if let firstFresh = self.throughputHistory.firstIndex(where: { $0.at >= historyCutoff }),
                           firstFresh > 0 {
                            self.throughputHistory.removeFirst(firstFresh)
                        }
                    }
                }
                try? await Task.sleep(nanoseconds: 250_000_000)
            }
        }
    }
}

struct ThroughputSample: Identifiable, Equatable {
    let id = UUID()
    let at: Date
    let bytesPerSec: Double
}
