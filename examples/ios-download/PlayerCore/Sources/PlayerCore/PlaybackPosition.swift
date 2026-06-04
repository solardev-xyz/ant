import Foundation

/// Pure, AVFoundation-free model of the player's scrubber position and the
/// seek / track-switch state machine.
///
/// `PlayerEngine` drives this from AVPlayer callbacks (periodic time
/// observer, seek completion handlers, item-ready notifications) and a
/// background `await openStream` that runs *between* tearing down the old
/// item and attaching the new one. Those callbacks fire on the main actor
/// but in a non-deterministic order, and that ordering is exactly where the
/// "scrubber jumps to a random spot after skip-then-next/prev" bug lives:
///
///   * a periodic-time tick for the *old* item can land after we've reset
///     the clock to 0 for the *new* track, and
///   * a seek that was still in flight when the user pressed next/previous
///     can complete *after* the new track is attached and stamp the old
///     track's seek target onto it.
///
/// Keeping this logic pure (no AVFoundation, no Combine, no main-actor) lets
/// us unit-test every ordering deterministically. The defence is a
/// monotonic `generation` token: every operation that invalidates an
/// in-flight seek — starting a new seek, or switching tracks — bumps it, and
/// stale callbacks carrying an older token are ignored.
public struct PlaybackPosition: Equatable {
    /// Live playhead in seconds, as last observed or seeked to.
    public private(set) var currentTime: TimeInterval = 0

    /// Target position while a seek is in flight. The scrubber reads
    /// `displayTime` so the thumb pins to the seek target instead of
    /// snapping back to the pre-seek clock the time observer keeps
    /// reporting until the new offset buffers.
    public private(set) var pendingSeekTime: TimeInterval? = nil

    /// True from the moment a track switch begins until the fresh item is
    /// attached. While set, periodic time ticks are dropped so the old
    /// item can't stamp its playhead onto the new track.
    public private(set) var isPreparingTrack: Bool = false

    /// Whether playback should resume once the in-flight seek finishes
    /// buffering (i.e. the user was playing when the seek began).
    public private(set) var resumeAfterSeek: Bool = false

    /// Monotonic token. Bumped by every operation that invalidates an
    /// in-flight seek (`beginSeek`, `beginPrepare`). A seek completion or
    /// buffer-wait callback is only honoured if it echoes the current
    /// value.
    public private(set) var generation: UInt64 = 0

    public init() {}

    /// Position shown on the scrubber / elapsed label: the seek target
    /// while seeking, otherwise the live clock.
    public var displayTime: TimeInterval { pendingSeekTime ?? currentTime }

    // MARK: - track switching

    /// Begin switching to a different track. Pins the indicator to 0,
    /// blocks time-observer writes until the new item is attached, and
    /// invalidates any in-flight seek so its late completion cannot move
    /// the freshly selected track.
    public mutating func beginPrepare() {
        generation &+= 1
        isPreparingTrack = true
        pendingSeekTime = nil
        resumeAfterSeek = false
        currentTime = 0
    }

    /// The new `AVPlayerItem` is now the player's current item. A fresh
    /// item always starts at 0, so re-zero and re-enable time-observer
    /// writes. Defensive against an old-item tick having slipped through.
    public mutating func itemAttached() {
        isPreparingTrack = false
        pendingSeekTime = nil
        currentTime = 0
    }

    // MARK: - seeking

    /// Begin a seek to an already-clamped `target`. Returns the generation
    /// token the caller must echo back to `finishSeek`/`cancelSeek` so a
    /// superseded or cross-track completion is ignored.
    @discardableResult
    public mutating func beginSeek(to target: TimeInterval, isPlaying: Bool) -> UInt64 {
        generation &+= 1
        pendingSeekTime = target
        currentTime = target
        resumeAfterSeek = isPlaying
        return generation
    }

    /// The seek for `token` finished and the new offset has buffered.
    /// Honoured only if the token is still current and a seek is actually
    /// pending — otherwise a stale cross-track completion would resurrect
    /// the old target. Returns whether playback should resume.
    @discardableResult
    public mutating func finishSeek(token: UInt64) -> Bool {
        guard token == generation, let target = pendingSeekTime else { return false }
        currentTime = target
        pendingSeekTime = nil
        let resume = resumeAfterSeek
        resumeAfterSeek = false
        return resume
    }

    /// The seek for `token` was cancelled or failed (`finished == false`,
    /// e.g. it was superseded by `replaceCurrentItem`). Clears the pending
    /// state so the time observer can resume reporting the real clock.
    public mutating func cancelSeek(token: UInt64) {
        guard token == generation else { return }
        pendingSeekTime = nil
        resumeAfterSeek = false
    }

    /// True iff the UI should show "playing + buffering" rather than the
    /// transient `.paused` an accurate seek induces. Mirrors the guard
    /// `PlayerEngine.updateTimeControl` applies.
    public var isResumingSeek: Bool {
        pendingSeekTime != nil && resumeAfterSeek
    }

    // MARK: - playback intent

    /// The user explicitly paused. Drop any intent to auto-resume after a
    /// seek so releasing the scrubber doesn't start playback again.
    public mutating func userPaused() {
        resumeAfterSeek = false
    }

    // MARK: - time observation

    /// A periodic time-observer tick. Dropped while preparing a track or
    /// while a seek is pending, so neither a stale old-item clock nor the
    /// pre-seek clock can overwrite the intended position.
    public mutating func observedTime(_ seconds: TimeInterval) {
        guard !isPreparingTrack, pendingSeekTime == nil else { return }
        currentTime = seconds.isFinite ? max(0, seconds) : 0
    }
}

/// The time the scrubber thumb / elapsed label should show.
///
/// The thumb follows the user's finger **only while actively dragging**
/// (`isScrubbing`). The moment the drag ends — and crucially across a
/// track switch — it must fall back to the engine playhead, never to a
/// leftover `scrubValue`. Conflating "has a scrub value" with "is
/// scrubbing" was the bug behind "scrub to 30s, skip to the next song,
/// thumb sits in the middle": the old track's scrub value shadowed the
/// new track's `0`.
public func scrubberTime(
    isScrubbing: Bool,
    scrubValue: TimeInterval,
    enginePlayhead: TimeInterval
) -> TimeInterval {
    isScrubbing ? scrubValue : enginePlayhead
}

/// Pure index arithmetic for the play queue, extracted so the "jump to
/// next / previous track" wrap-around is unit-testable on its own.
public enum PlaybackQueue {
    /// Next index, wrapping back to 0 past the end. Returns `nil` for an
    /// empty queue.
    public static func nextIndex(from current: Int, count: Int) -> Int? {
        guard count > 0 else { return nil }
        return (current + 1) % count
    }

    /// Previous index, wrapping to the last track before 0. Returns `nil`
    /// for an empty queue.
    public static func previousIndex(from current: Int, count: Int) -> Int? {
        guard count > 0 else { return nil }
        return current == 0 ? count - 1 : current - 1
    }
}

/// What pressing the "previous / back" transport button should do, given
/// how far into the current track the user is.
public enum BackAction: Equatable {
    /// Restart the current track from 0 (user is past the restart window).
    case restartCurrent
    /// Move to `index` and prepare it (user is at the very start).
    case goToTrack(Int)
}

/// Decide the back-button behaviour. Past `restartThreshold` seconds, back
/// restarts the current song; within it, back steps to the previous track.
/// `displayTime` (not the raw clock) is used so the decision matches what
/// the user sees even mid-seek or while buffering.
public func backButtonAction(
    displayTime: TimeInterval,
    restartThreshold: TimeInterval,
    currentIndex: Int,
    trackCount: Int
) -> BackAction? {
    guard trackCount > 0 else { return nil }
    if displayTime > restartThreshold {
        return .restartCurrent
    }
    guard let prev = PlaybackQueue.previousIndex(from: currentIndex, count: trackCount) else {
        return nil
    }
    return .goToTrack(prev)
}
