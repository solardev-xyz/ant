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
    /// Pure scrubber-position / seek / track-switch state machine. All the
    /// "what second is the thumb at" rules (and the ordering guards that
    /// stop stale AVPlayer callbacks from moving it) live here and are
    /// unit-tested in `PlayerCore`. The engine only feeds it AVPlayer
    /// events and mirrors AVPlayer side effects back out.
    @Published private(set) var position = PlaybackPosition()
    @Published private(set) var duration: TimeInterval = 0
    /// True while playback is intended (the user hasn't paused) but the
    /// player can't render because the buffer ran dry — i.e. AVPlayer's
    /// `timeControlStatus` is `.waitingToPlayAtSpecifiedRate` or the
    /// item reported an empty playback buffer. Drives the buffering UI.
    @Published private(set) var isBuffering: Bool = false
    /// Seconds of contiguous media buffered ahead of [`currentTime`].
    /// Read straight from the item's `loadedTimeRanges`; shown next to
    /// the buffering indicator so the user can see the runway filling.
    @Published private(set) var bufferedAhead: TimeInterval = 0
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
    private var timeControlObservation: NSKeyValueObservation?
    private var loadedRangesObservation: NSKeyValueObservation?
    private var bufferEmptyObservation: NSKeyValueObservation?
    private var endObserver: NSObjectProtocol?
    private var stallObserver: NSObjectProtocol?
    private var progressTask: Task<Void, Never>?
    private var bytesSamples: [(at: Date, bytes: UInt64)] = []
    private var bufferObservation: NSKeyValueObservation?
    private var seekResumeTask: Task<Void, Never>?
    /// The user's transport intent, independent of AVPlayer's momentary
    /// `timeControlStatus`. With `automaticallyWaitsToMinimizeStalling`
    /// off, a dry buffer mid-track lands the player in `.paused` (not
    /// `.waitingToPlay`) and it will not resume on its own — which is
    /// indistinguishable from a user pause by status alone. This flag is
    /// the disambiguator: `.paused` while `intendsToPlay` means "stalled,
    /// still buffering", so we keep the pause button + buffering UI and
    /// auto-resume once the buffer recovers.
    private var intendsToPlay = false
    /// Polls the buffer back to health after a mid-track stall and calls
    /// `play()` again (the player won't, with auto-wait disabled).
    private var stallResumeTask: Task<Void, Never>?

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

    /// Live playhead in seconds (forwards to the position model).
    var currentTime: TimeInterval { position.currentTime }

    /// Scrubber / elapsed label: live clock unless a seek is pending.
    var displayTime: TimeInterval { position.displayTime }

    init(
        node: AntNode,
        queue: [Track] = Track.litterAlly,
        coverArtReference: String? = Track.litterAllyCoverReference
    ) {
        self.node = node
        self.queue = queue
        self.coverArtReference = coverArtReference
        // The ant-network resource loader delivers bytes in slow, bursty
        // passes, which makes AVPlayer's bandwidth estimator compute a
        // huge `timeToLikelyToKeepUp` (~300 s) and a bwInflationFactor of
        // ~3. With the default `automaticallyWaitsToMinimizeStalling`,
        // `play()` parks the item in `.waitingToPlayAtSpecifiedRate`
        // forever: `rate` is 1 (so the button shows "pause") but no audio
        // renders and the clock never advances, even though tens of
        // seconds are already buffered. Starting immediately and tolerating
        // the occasional stall is the right trade-off for this transport.
        player.automaticallyWaitsToMinimizeStalling = false
        Self.resetDebugLog()
        debugLog("init")
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
        if let stallObserver = stallObserver {
            NotificationCenter.default.removeObserver(stallObserver)
        }
        progressTask?.cancel()
        stallResumeTask?.cancel()
        seekResumeTask?.cancel()
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
        debugLog("togglePlay")
        if isPlaying {
            intendsToPlay = false
            stallResumeTask?.cancel()
            position.userPaused()
            player.pause()
        } else {
            intendsToPlay = true
            player.play()
        }
    }

    func play() {
        intendsToPlay = true
        player.play()
    }
    func pause() {
        intendsToPlay = false
        stallResumeTask?.cancel()
        position.userPaused()
        player.pause()
    }

    func next() {
        guard let nextIndex = PlaybackQueue.nextIndex(from: currentIndex, count: queue.count) else { return }
        debugLog("next -> idx=\(nextIndex)")
        select(index: nextIndex, autoPlay: true)
    }

    func previous() {
        // Decide on `displayTime` (the position shown on the scrubber)
        // rather than the raw clock so the restart-vs-previous choice
        // matches what the user sees even mid-seek or while buffering.
        switch backButtonAction(
            displayTime: displayTime,
            restartThreshold: 3,
            currentIndex: currentIndex,
            trackCount: queue.count
        ) {
        case .none:
            return
        case .restartCurrent:
            debugLog("previous -> restart")
            seek(to: 0)
        case .goToTrack(let idx):
            debugLog("previous -> idx=\(idx)")
            select(index: idx, autoPlay: true)
        }
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
        stallResumeTask?.cancel()
        let generation = position.beginSeek(to: clamped, isPlaying: isPlaying)
        debugLog("seek req=\(fmt(clamped)) gen=\(generation)")
        player.pause()
        updateNowPlaying()

        player.seek(to: target, toleranceBefore: .zero, toleranceAfter: .zero) { [weak self] finished in
            Task { @MainActor in
                guard let self, self.position.generation == generation else { return }
                guard finished else {
                    self.position.cancelSeek(token: generation)
                    self.debugLog("seek cancelled gen=\(generation)")
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
        guard position.generation == generation else { return }
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
                    if self.position.generation != generation { return true }
                    if self.position.pendingSeekTime == nil { return true }
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
        // The model rejects stale tokens; bail before touching the shared
        // seek-wait state so a superseded completion can't cancel a newer
        // seek's observers.
        guard position.generation == generation, position.pendingSeekTime != nil else { return }
        cancelSeekWait()
        let resume = position.finishSeek(token: generation)
        debugLog("finishSeek gen=\(generation) resume=\(resume ? 1 : 0)")
        intendsToPlay = resume
        updateNowPlaying()
        if resume {
            player.play()
        }
    }

    private func prepareCurrent(autoPlay: Bool) async {
        guard let track = currentTrack else { return }
        cancelSeekWait()
        stallResumeTask?.cancel()
        intendsToPlay = autoPlay
        position.beginPrepare()
        debugLog("prepare idx=\(currentIndex) '\(track.title)' autoplay=\(autoPlay ? 1 : 0) gen=\(position.generation)")
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
        loadedRangesObservation?.invalidate()
        loadedRangesObservation = nil
        bufferEmptyObservation?.invalidate()
        bufferEmptyObservation = nil
        if let endObserver = endObserver {
            NotificationCenter.default.removeObserver(endObserver)
            self.endObserver = nil
        }
        if let stallObserver = stallObserver {
            NotificationCenter.default.removeObserver(stallObserver)
            self.stallObserver = nil
        }
        playerItem = nil
        asset = nil
        loader = nil
        activeSession = nil
        streamProgress = nil
        throughputBytesPerSec = 0
        duration = 0
        // Loading a new track to play *is* a buffering state — surface it
        // immediately so skipping shows the indicator during the (often
        // multi-second) `openStream` + first-fill, not a frozen play button.
        isBuffering = autoPlay
        isPlaying = autoPlay
        bufferedAhead = 0
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
            // The fresh item starts at 0; `itemAttached` re-zeros the
            // position and re-enables time-observer writes even if the old
            // item's observer stamped a stale time during `openStream`.
            position.itemAttached()
            debugLog("attached idx=\(currentIndex) '\(track.title)'")
            updateNowPlaying()
            if autoPlay {
                player.play()
            }
        } catch {
            position.itemAttached()
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
        loadedRangesObservation = item.observe(\.loadedTimeRanges, options: [.new]) { [weak self] _, _ in
            Task { @MainActor in self?.recomputeBufferedAhead() }
        }
        // `isPlaybackBufferEmpty` flips before `timeControlStatus` settles,
        // so flag buffering eagerly here. Gate on play *intent*, not on
        // `timeControlStatus != .paused`: a real stall reports `.paused`,
        // so the old guard suppressed exactly the case we want to show.
        bufferEmptyObservation = item.observe(\.isPlaybackBufferEmpty, options: [.new]) { [weak self] item, _ in
            Task { @MainActor in
                guard let self else { return }
                if item.isPlaybackBufferEmpty, self.intendsToPlay, self.position.pendingSeekTime == nil {
                    self.isBuffering = true
                    self.resumeAfterStallWhenReady()
                }
                self.recomputeBufferedAhead()
            }
        }
        endObserver = NotificationCenter.default.addObserver(
            forName: .AVPlayerItemDidPlayToEndTime,
            object: item,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor in self?.next() }
        }
        stallObserver = NotificationCenter.default.addObserver(
            forName: .AVPlayerItemPlaybackStalled,
            object: item,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor in
                guard let self, self.intendsToPlay, self.position.pendingSeekTime == nil else { return }
                self.isBuffering = true
                self.resumeAfterStallWhenReady()
            }
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
                // The model drops the tick while preparing a track or
                // seeking, so a stale old-item clock can't move the thumb.
                self.position.observedTime(time.seconds)
                if let item = self.player.currentItem,
                   item.duration.seconds.isFinite,
                   item.duration.seconds > 0,
                   self.duration != item.duration.seconds {
                    self.duration = item.duration.seconds
                }
                self.recomputeBufferedAhead()
                self.updateNowPlaying()
            }
        }
        // `timeControlStatus` (not `rate`) is the signal that separates
        // "playing" from "wants to play but is rebuffering". With
        // `automaticallyWaitsToMinimizeStalling = false` a dry buffer
        // mid-track lands here as `.waitingToPlayAtSpecifiedRate`, which
        // is exactly the buffering case we want to surface.
        timeControlObservation = player.observe(\.timeControlStatus, options: [.new]) { [weak self] _, _ in
            Task { @MainActor in self?.updateTimeControl() }
        }
    }

    /// Map AVPlayer's transport state onto our `isPlaying` / `isBuffering`
    /// pair. Paused means the user stopped; waiting means we intend to
    /// play but the buffer is dry; playing is the happy path.
    private func updateTimeControl() {
        debugLog("timeCtl=\(player.timeControlStatus.rawValue)")
        // An accurate seek we intend to resume from explicitly pauses the
        // player while it repositions and refills the buffer at the new
        // offset (see `seek(to:)`). That self-induced `.paused` must not
        // flip the UI to the play button or hide the buffering badge —
        // from the user's point of view playback is still in progress,
        // it's just rebuffering at the new position.
        if position.isResumingSeek {
            isPlaying = true
            isBuffering = true
            recomputeBufferedAhead()
            updateNowPlaying()
            return
        }
        switch player.timeControlStatus {
        case .paused:
            // With auto-wait off, a dry-buffer stall also reports `.paused`.
            // If the user still intends to play, this is a stall, not a
            // pause: keep the buffering UI up and schedule a resume.
            if intendsToPlay {
                isPlaying = true
                isBuffering = true
                resumeAfterStallWhenReady()
            } else {
                isPlaying = false
                isBuffering = false
            }
        case .waitingToPlayAtSpecifiedRate:
            isPlaying = true
            isBuffering = true
        case .playing:
            isPlaying = true
            isBuffering = false
        @unknown default:
            break
        }
        recomputeBufferedAhead()
        updateNowPlaying()
    }

    /// After a mid-track stall (or the first fill of a freshly-loaded
    /// track) the player won't resume on its own — auto-wait is off. Poll
    /// the item back to health and `play()` again, bailing if the user
    /// pauses, a seek takes over, or the track is swapped out.
    private func resumeAfterStallWhenReady() {
        guard intendsToPlay, position.pendingSeekTime == nil else { return }
        stallResumeTask?.cancel()
        if bufferReadyToResume() {
            player.play()
            return
        }
        stallResumeTask = Task { [weak self] in
            for _ in 0..<1200 {  // ~5 min ceiling at 250 ms cadence
                try? await Task.sleep(nanoseconds: 250_000_000)
                if Task.isCancelled { return }
                let action = await MainActor.run { () -> Bool? in
                    guard let self else { return false }            // stop
                    guard self.intendsToPlay, self.position.pendingSeekTime == nil else { return false }
                    return self.bufferReadyToResume() ? true : nil   // true = play, nil = keep waiting
                }
                guard let action else { continue }
                if action {
                    await MainActor.run {
                        guard let self, self.intendsToPlay, self.position.pendingSeekTime == nil else { return }
                        self.debugLog("stall-resume")
                        self.player.play()
                    }
                }
                return
            }
        }
    }

    /// Seconds of contiguous buffered media ahead of the playhead, read
    /// from the current item's `loadedTimeRanges`.
    private func recomputeBufferedAhead() {
        bufferedAhead = currentBufferedAhead()
    }

    /// Live read of contiguous decoded runway ahead of the playhead.
    private func currentBufferedAhead() -> TimeInterval {
        guard let item = player.currentItem else { return 0 }
        let now = position.displayTime
        var ahead: TimeInterval = 0
        for value in item.loadedTimeRanges {
            let range = value.timeRangeValue
            let start = range.start.seconds
            let end = (range.start + range.duration).seconds
            guard start.isFinite, end.isFinite else { continue }
            if now >= start - 0.5 && now <= end + 0.5 {
                ahead = max(ahead, end - now)
            }
        }
        return max(0, ahead)
    }

    /// Whether there's enough runway to resume after a stall. Deliberately
    /// does NOT use `isPlaybackLikelyToKeepUp`: on the bursty ant transport
    /// AVPlayer's bandwidth estimate keeps that `false` for a very long
    /// time (the same reason `automaticallyWaitsToMinimizeStalling` is
    /// off), which would leave a stalled track frozen indefinitely. We
    /// resume on a concrete decoded cushion instead, or when the buffer has
    /// reached the end of the track.
    private func bufferReadyToResume() -> Bool {
        let ahead = currentBufferedAhead()
        if ahead >= Self.resumeRunwaySeconds { return true }
        if duration > 0 {
            let remaining = duration - position.displayTime
            if remaining > 0, ahead >= remaining - 0.5 { return true }
        }
        return false
    }
    private static let resumeRunwaySeconds: TimeInterval = 3

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

    // MARK: – debug log

    /// Short, greppable position-state trace. Each line carries the event
    /// tag plus a full snapshot of the state that drives the scrubber, so
    /// skip / seek / jump bugs can be diagnosed from the log alone instead
    /// of by screenshotting the simulator. Mirrored to the unified log
    /// (`NSLog`) and appended to `Documents/player-debug.log`, which is
    /// readable via `xcrun simctl get_app_container <udid> <bundle> data`.
    private func debugLog(_ event: String) {
        let pend = position.pendingSeekTime.map { fmt($0) } ?? "-"
        let tag = event.padding(toLength: max(event.count, 24), withPad: " ", startingAt: 0)
        let line = String(
            format: "%@ %@ disp=%@ cur=%@ pend=%@ prep=%d resume=%d intend=%d play=%d buf=%d idx=%d gen=%llu",
            Self.logClock(), tag, fmt(displayTime), fmt(position.currentTime), pend,
            position.isPreparingTrack ? 1 : 0, position.resumeAfterSeek ? 1 : 0,
            intendsToPlay ? 1 : 0, isPlaying ? 1 : 0, isBuffering ? 1 : 0, currentIndex, position.generation)
        NSLog("[player] %@", line)
        Self.appendDebugLine(line)
    }

    private func fmt(_ t: TimeInterval) -> String { String(format: "%.2f", t) }

    /// View-layer trace, written to the same `player-debug.log` as the
    /// engine state. The scrubber bug ("scrub then skip → thumb mid-song")
    /// was invisible to `debugLog` because the engine was correct and only
    /// the *rendered* value was wrong. Logging what the view derives —
    /// `shown` (the thumb/label time), `scrubV`, `scrubbing` — next to the
    /// engine playhead (`engDisp`) makes that divergence greppable instead
    /// of something you have to screenshot. The dedicated `drift` sentinel
    /// (see `PlayerView`) is what fires when not scrubbing yet `shown` ≠
    /// the engine playhead; the other tags are plain state snapshots.
    func viewDebugLog(_ event: String, isScrubbing: Bool, scrubValue: Double, shown: Double) {
        let tag = "view:\(event)"
        let padded = tag.padding(toLength: max(tag.count, 24), withPad: " ", startingAt: 0)
        let line = String(
            format: "%@ %@ shown=%@ scrubV=%@ scrubbing=%d engDisp=%@ dur=%@",
            Self.logClock(), padded, fmt(shown), fmt(scrubValue), isScrubbing ? 1 : 0,
            fmt(displayTime), fmt(duration))
        NSLog("[player] %@", line)
        Self.appendDebugLine(line)
    }

    private static let debugLogURL: URL? = {
        guard let dir = try? FileManager.default.url(
            for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: true
        ) else { return nil }
        return dir.appendingPathComponent("player-debug.log")
    }()

    private static let logStart = Date()
    private static func logClock() -> String {
        String(format: "%8.3f", Date().timeIntervalSince(logStart))
    }

    /// Truncate the log on each launch so a session reads top-to-bottom.
    private static func resetDebugLog() {
        guard let url = debugLogURL else { return }
        try? "=== player-debug.log ===\n".data(using: .utf8)?.write(to: url)
    }

    private static func appendDebugLine(_ line: String) {
        guard let url = debugLogURL, let data = (line + "\n").data(using: .utf8) else { return }
        if let handle = try? FileHandle(forWritingTo: url) {
            defer { try? handle.close() }
            try? handle.seekToEnd()
            try? handle.write(contentsOf: data)
        } else {
            try? data.write(to: url)
        }
    }
}

struct ThroughputSample: Identifiable, Equatable {
    let id = UUID()
    let at: Date
    let bytesPerSec: Double
}
