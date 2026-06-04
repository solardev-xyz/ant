import XCTest
@testable import PlayerCore

/// End-to-end coverage of the scrubber-position state machine for every
/// skipping / jumping ordering. Each test replays the exact sequence of
/// calls `PlayerEngine` makes from its AVPlayer callbacks — including the
/// adversarial orderings (stale ticks, late seek completions across a
/// track switch) that produced the "indicator lands in a random spot" bug.
final class PlaybackPositionTests: XCTestCase {

    private let accuracy: TimeInterval = 0.0001

    // MARK: - queue index arithmetic

    func testNextIndexWraps() {
        XCTAssertEqual(PlaybackQueue.nextIndex(from: 0, count: 3), 1)
        XCTAssertEqual(PlaybackQueue.nextIndex(from: 2, count: 3), 0)
        XCTAssertNil(PlaybackQueue.nextIndex(from: 0, count: 0))
    }

    func testPreviousIndexWraps() {
        XCTAssertEqual(PlaybackQueue.previousIndex(from: 2, count: 3), 1)
        XCTAssertEqual(PlaybackQueue.previousIndex(from: 0, count: 3), 2)
        XCTAssertNil(PlaybackQueue.previousIndex(from: 0, count: 0))
    }

    // MARK: - back-button policy (3s restart window)

    func testBackRestartsWhenPastThreshold() {
        let action = backButtonAction(displayTime: 12, restartThreshold: 3, currentIndex: 1, trackCount: 4)
        XCTAssertEqual(action, .restartCurrent)
    }

    func testBackGoesToPreviousWhenWithinThreshold() {
        let action = backButtonAction(displayTime: 1.5, restartThreshold: 3, currentIndex: 2, trackCount: 4)
        XCTAssertEqual(action, .goToTrack(1))
    }

    func testBackWithinThresholdWrapsToLastTrack() {
        let action = backButtonAction(displayTime: 0.5, restartThreshold: 3, currentIndex: 0, trackCount: 4)
        XCTAssertEqual(action, .goToTrack(3))
    }

    func testBackUsesDisplayTimeNotRawClock() {
        // Mid-seek the raw clock may still read the pre-seek value, but the
        // user sees the seek target. Decide on what they see.
        var pos = PlaybackPosition()
        pos.observedTime(1)            // raw clock = 1
        pos.beginSeek(to: 200, isPlaying: true)  // displayTime now 200
        let action = backButtonAction(
            displayTime: pos.displayTime, restartThreshold: 3, currentIndex: 1, trackCount: 4)
        XCTAssertEqual(action, .restartCurrent)
    }

    // MARK: - plain track switch

    func testNextResetsToZeroAndIgnoresStaleOldItemTick() {
        var pos = PlaybackPosition()
        pos.observedTime(42)
        XCTAssertEqual(pos.displayTime, 42, accuracy: accuracy)

        pos.beginPrepare()                       // user pressed next
        XCTAssertEqual(pos.displayTime, 0, accuracy: accuracy)

        pos.observedTime(43)                     // old item's last tick — must be ignored
        XCTAssertEqual(pos.displayTime, 0, accuracy: accuracy)

        pos.itemAttached()                       // new item is current
        pos.observedTime(0.2)                    // fresh track starts ticking from 0
        XCTAssertEqual(pos.displayTime, 0.2, accuracy: accuracy)
    }

    // MARK: - seek then jump (the reported bug)

    func testCompletedSeekThenNextResetsToZero() {
        var pos = PlaybackPosition()
        pos.observedTime(10)
        let tok = pos.beginSeek(to: 200, isPlaying: true)
        XCTAssertTrue(pos.finishSeek(token: tok))
        XCTAssertEqual(pos.currentTime, 200, accuracy: accuracy)

        pos.observedTime(201)                    // playing on past the seek
        pos.beginPrepare()                       // forward pressed
        pos.observedTime(202)                    // stale old-item tick
        pos.itemAttached()
        XCTAssertEqual(pos.displayTime, 0, accuracy: accuracy)
    }

    func testInFlightSeekThenNextIgnoresLateCompletion() {
        var pos = PlaybackPosition()
        pos.observedTime(10)
        let staleToken = pos.beginSeek(to: 200, isPlaying: true)  // seek still buffering
        pos.beginPrepare()                       // forward pressed before it finished

        // The original seek finally completes — on/after the new track. It
        // must NOT stamp 200 onto the fresh track.
        XCTAssertFalse(pos.finishSeek(token: staleToken))
        pos.itemAttached()
        XCTAssertFalse(pos.finishSeek(token: staleToken))  // even later: still ignored
        pos.observedTime(0.3)
        XCTAssertEqual(pos.displayTime, 0.3, accuracy: accuracy)
    }

    func testLateSeekCompletionAfterItemAttachedDoesNotMovePosition() {
        // Exactly the random-position repro: seek within song, jump track,
        // the seek's buffer-wait fires on the new item.
        var pos = PlaybackPosition()
        let staleToken = pos.beginSeek(to: 95, isPlaying: true)
        pos.beginPrepare()
        pos.itemAttached()
        pos.observedTime(2)                      // new track playing
        XCTAssertFalse(pos.finishSeek(token: staleToken))
        XCTAssertEqual(pos.displayTime, 2, accuracy: accuracy)
    }

    // MARK: - seek mechanics

    func testDoubleSeekHonoursOnlyLatest() {
        var pos = PlaybackPosition()
        let first = pos.beginSeek(to: 50, isPlaying: true)
        let second = pos.beginSeek(to: 150, isPlaying: true)
        XCTAssertFalse(pos.finishSeek(token: first))    // superseded
        XCTAssertEqual(pos.displayTime, 150, accuracy: accuracy)
        XCTAssertTrue(pos.finishSeek(token: second))
        XCTAssertEqual(pos.currentTime, 150, accuracy: accuracy)
        XCTAssertNil(pos.pendingSeekTime)
    }

    func testPendingSeekPinsScrubberAgainstTimeTicks() {
        var pos = PlaybackPosition()
        pos.observedTime(10)
        let tok = pos.beginSeek(to: 180, isPlaying: true)
        pos.observedTime(11)                     // pre-seek clock keeps ticking
        pos.observedTime(12)
        XCTAssertEqual(pos.displayTime, 180, accuracy: accuracy)  // thumb stays at target
        pos.finishSeek(token: tok)
        XCTAssertEqual(pos.displayTime, 180, accuracy: accuracy)
    }

    func testSeekResumesPlaybackOnlyWhenWasPlaying() {
        var playing = PlaybackPosition()
        let t1 = playing.beginSeek(to: 30, isPlaying: true)
        XCTAssertTrue(playing.finishSeek(token: t1))

        var paused = PlaybackPosition()
        let t2 = paused.beginSeek(to: 30, isPlaying: false)
        XCTAssertFalse(paused.finishSeek(token: t2))
    }

    func testUserPauseCancelsResumeIntent() {
        var pos = PlaybackPosition()
        let tok = pos.beginSeek(to: 30, isPlaying: true)
        pos.userPaused()
        XCTAssertFalse(pos.finishSeek(token: tok))  // does not auto-resume
    }

    func testCancelledSeekClearsPendingSoClockResumes() {
        var pos = PlaybackPosition()
        let tok = pos.beginSeek(to: 200, isPlaying: true)
        pos.cancelSeek(token: tok)
        XCTAssertNil(pos.pendingSeekTime)
        pos.observedTime(5)                      // observer free to report again
        XCTAssertEqual(pos.displayTime, 5, accuracy: accuracy)
    }

    func testIsResumingSeekReflectsPlayingBufferingState() {
        var pos = PlaybackPosition()
        XCTAssertFalse(pos.isResumingSeek)
        pos.beginSeek(to: 60, isPlaying: true)
        XCTAssertTrue(pos.isResumingSeek)        // show pause + buffering during seek
        pos.finishSeek(token: pos.generation)
        XCTAssertFalse(pos.isResumingSeek)
    }

    // MARK: - full reported scenario

    func testSkipWithinSongThenBackThenForwardAllLandAtZero() {
        // "Skipped within the same song" (in-song seek), then back (restart),
        // then forward (next). Every landing must be a clean 0, never a
        // leftover offset.
        var pos = PlaybackPosition()
        pos.observedTime(120)

        // in-song skip to 0:30
        let s1 = pos.beginSeek(to: 30, isPlaying: true)
        XCTAssertTrue(pos.finishSeek(token: s1))
        pos.observedTime(31)

        // back, past threshold -> restart current (seek to 0)
        let s2 = pos.beginSeek(to: 0, isPlaying: true)
        XCTAssertTrue(pos.finishSeek(token: s2))
        XCTAssertEqual(pos.displayTime, 0, accuracy: accuracy)

        // forward -> next track
        pos.beginPrepare()
        pos.observedTime(99)                     // stale tick from old item
        pos.itemAttached()
        pos.observedTime(0.1)
        XCTAssertEqual(pos.displayTime, 0.1, accuracy: accuracy)
    }

    // MARK: - scrubber display (view-layer leak regression)

    func testScrubberFollowsFingerWhileScrubbing() {
        XCTAssertEqual(
            scrubberTime(isScrubbing: true, scrubValue: 42, enginePlayhead: 10), 42, accuracy: accuracy)
    }

    func testScrubberIgnoresStaleScrubValueWhenNotScrubbing() {
        // The exact reported bug: a leftover scrub value (150 from the old
        // track) must never shadow the engine playhead (0 on the new track).
        XCTAssertEqual(
            scrubberTime(isScrubbing: false, scrubValue: 150, enginePlayhead: 0), 0, accuracy: accuracy)
    }

    func testScrubReleaseThenTrackChangeShowsZero() {
        // Drag to 150, release (isScrubbing false), engine switches track
        // and resets to 0 — the scrubber must read 0 regardless of the
        // stale scrubValue still sitting in view state.
        var pos = PlaybackPosition()
        pos.observedTime(150)
        let scrubValue = 150.0           // value left in @State after the drag
        // ... user releases, then taps next ...
        pos.beginPrepare()
        pos.itemAttached()
        XCTAssertEqual(
            scrubberTime(isScrubbing: false, scrubValue: scrubValue, enginePlayhead: pos.displayTime),
            0, accuracy: accuracy)
    }

    // MARK: - extended skip / seek / jump scenarios

    func testInSongSeeksToManyDifferentPositions() {
        // Scrub all over a single track; each completed seek lands exactly
        // on its target and clears the pending state.
        var e = EngineSim(count: 3)
        e.startPlaying(at: 5)
        for target in [90.0, 12.5, 200.0, 0.0, 175.25, 30.0] {
            e.seek(to: target)
            XCTAssertEqual(e.displayTime, target, accuracy: accuracy, "thumb pins to target mid-seek")
            e.seekCompletes()
            XCTAssertEqual(e.displayTime, target, accuracy: accuracy)
            XCTAssertNil(e.pos.pendingSeekTime)
        }
    }

    func testRapidScrubbingHonoursOnlyFinalTarget() {
        // Dragging the scrubber emits many seeks; only the last release
        // should win, earlier completions are stale.
        var e = EngineSim(count: 3)
        e.startPlaying(at: 10)
        let t1 = e.seek(to: 40)
        let t2 = e.seek(to: 80)
        let t3 = e.seek(to: 123)
        XCTAssertFalse(e.pos.finishSeek(token: t1))
        XCTAssertFalse(e.pos.finishSeek(token: t2))
        XCTAssertTrue(e.pos.finishSeek(token: t3))
        XCTAssertEqual(e.displayTime, 123, accuracy: accuracy)
    }

    func testWalkForwardThroughWholePlaylistResetsEachTime() {
        // Skip next across the whole queue, wrapping back to the start.
        var e = EngineSim(count: 4, startIndex: 0)
        let expectedIndices = [1, 2, 3, 0, 1]
        for expected in expectedIndices {
            e.startPlaying(at: 37)               // play partway in
            e.tapNext()
            e.trackAttached()
            e.startPlaying(at: 0.2)
            XCTAssertEqual(e.index, expected)
            XCTAssertEqual(e.displayTime, 0.2, accuracy: accuracy)
        }
    }

    func testWalkBackwardThroughWholePlaylistResetsEachTime() {
        // Skip backwards (always at the very start, so it steps tracks)
        // and wrap from 0 to the last track.
        var e = EngineSim(count: 4, startIndex: 0)
        let expectedIndices = [3, 2, 1, 0, 3]
        for expected in expectedIndices {
            XCTAssertEqual(e.tapPrevious(), .goToTrack(expected))
            e.trackAttached()
            e.startPlaying(at: 0.1)
            XCTAssertEqual(e.index, expected)
            XCTAssertEqual(e.displayTime, 0.1, accuracy: accuracy)
        }
    }

    func testSeekToEndThenNextLandsAtZero() {
        var e = EngineSim(count: 3, startIndex: 1)
        e.startPlaying(at: 10)
        e.seek(to: 286)                          // scrub to the very end
        e.seekCompletes()
        e.tapNext()
        e.trackAttached()
        XCTAssertEqual(e.index, 2)
        XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)
    }

    func testSeekForwardThenBackwardWithinSong() {
        var e = EngineSim(count: 3)
        e.startPlaying(at: 5)
        e.seek(to: 220); e.seekCompletes()
        XCTAssertEqual(e.displayTime, 220, accuracy: accuracy)
        e.seek(to: 12); e.seekCompletes()        // rewind
        XCTAssertEqual(e.displayTime, 12, accuracy: accuracy)
    }

    func testNextThenSeekInNewSongThenNextAgain() {
        var e = EngineSim(count: 4, startIndex: 0)
        e.startPlaying(at: 50)
        e.tapNext(); e.trackAttached()           // -> idx 1
        e.startPlaying(at: 2)
        e.seek(to: 140); e.seekCompletes()       // scrub inside idx 1
        XCTAssertEqual(e.displayTime, 140, accuracy: accuracy)
        e.tapNext(); e.trackAttached()           // -> idx 2, must reset
        XCTAssertEqual(e.index, 2)
        XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)
    }

    func testNextWhileBackwardSeekStillInFlightIgnoresLateCompletion() {
        // Scrub backward, then hit next before the seek buffers: the late
        // backward-seek completion must not move the new track.
        var e = EngineSim(count: 3, startIndex: 0)
        e.startPlaying(at: 180)
        let stale = e.seek(to: 20)               // rewind, still buffering
        e.tapNext()                              // forward before it lands
        e.trackAttached()
        e.startPlaying(at: 3)
        XCTAssertFalse(e.pos.finishSeek(token: stale))
        XCTAssertEqual(e.index, 1)
        XCTAssertEqual(e.displayTime, 3, accuracy: accuracy)
    }

    func testBackRestartsThenAnotherBackStepsToPreviousTrack() {
        // First back (deep in the song) restarts; a second back (now at 0)
        // steps to the previous track.
        var e = EngineSim(count: 4, startIndex: 2)
        e.startPlaying(at: 95)
        XCTAssertEqual(e.tapPrevious(), .restartCurrent)
        e.seekCompletes()                        // restart seek lands
        XCTAssertEqual(e.index, 2)
        XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)

        XCTAssertEqual(e.tapPrevious(), .goToTrack(1))
        e.trackAttached()
        XCTAssertEqual(e.index, 1)
        XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)
    }

    func testSeekThenPauseThenNextDoesNotAutoResume() {
        var e = EngineSim(count: 3)
        e.startPlaying(at: 10)
        let tok = e.seek(to: 60)
        e.pos.userPaused()                       // user paused mid-seek
        XCTAssertFalse(e.pos.finishSeek(token: tok))   // no auto-resume
        e.tapNext(); e.trackAttached()
        XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)
    }

    func testAlternatingNextPreviousAtTrackStartsStepsCleanly() {
        // next, then immediate back (within window) returns, repeatedly —
        // index oscillates and position is always 0 on a fresh track.
        var e = EngineSim(count: 4, startIndex: 0)
        e.startPlaying(at: 20)

        e.tapNext(); e.trackAttached()           // 0 -> 1
        XCTAssertEqual(e.index, 1)
        e.startPlaying(at: 0.5)                  // barely into idx 1

        XCTAssertEqual(e.tapPrevious(), .goToTrack(0))  // within window -> back to 0
        e.trackAttached()
        XCTAssertEqual(e.index, 0)
        XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)

        e.tapNext(); e.trackAttached()           // 0 -> 1 again
        XCTAssertEqual(e.index, 1)
        XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)
    }

    func testStaleOldItemTicksDuringPrepareNeverLeakIn() {
        // The old track keeps emitting periodic ticks until replaced; none
        // may survive the switch.
        var e = EngineSim(count: 3, startIndex: 0)
        e.startPlaying(at: 240)
        e.tapNext()
        for t in stride(from: 241.0, through: 244.0, by: 0.25) {
            e.pos.observedTime(t)                // stale ticks during openStream
            XCTAssertEqual(e.displayTime, 0, accuracy: accuracy)
        }
        e.trackAttached()
        e.startPlaying(at: 0.25)
        XCTAssertEqual(e.displayTime, 0.25, accuracy: accuracy)
    }
}

/// In-test stand-in for `PlayerEngine`'s play-queue + position wiring, so
/// scenarios read as user gestures ("tap next", "seek", "track attached")
/// while exercising the real `PlaybackPosition` / `PlaybackQueue` logic.
/// Operations that AVPlayer completes asynchronously are split into a
/// "begin" (the gesture) and a "settle" (`trackAttached` / `seekCompletes`)
/// so tests can interleave stale callbacks deterministically.
private struct EngineSim {
    private(set) var index: Int
    let count: Int
    var pos = PlaybackPosition()
    var playing: Bool
    private var lastSeekToken: UInt64 = 0

    init(count: Int, startIndex: Int = 0, playing: Bool = true) {
        self.count = count
        self.index = startIndex
        self.playing = playing
    }

    var displayTime: TimeInterval { pos.displayTime }

    /// Simulate the periodic time observer reporting the playhead.
    mutating func startPlaying(at t: TimeInterval) { pos.observedTime(t) }

    /// User tapped the forward / skip-next button.
    mutating func tapNext() {
        guard let n = PlaybackQueue.nextIndex(from: index, count: count) else { return }
        index = n
        pos.beginPrepare()
    }

    /// User tapped the back / skip-previous button. Returns the decision so
    /// tests can assert restart-vs-previous.
    @discardableResult
    mutating func tapPrevious(threshold: TimeInterval = 3) -> BackAction? {
        let action = backButtonAction(
            displayTime: pos.displayTime,
            restartThreshold: threshold,
            currentIndex: index,
            trackCount: count
        )
        switch action {
        case .restartCurrent:
            seek(to: 0)
        case .goToTrack(let i):
            index = i
            pos.beginPrepare()
        case nil:
            break
        }
        return action
    }

    /// The freshly selected track finished opening and is now current.
    mutating func trackAttached() { pos.itemAttached() }

    /// Begin a seek; returns the token so tests can simulate stale
    /// completions.
    @discardableResult
    mutating func seek(to t: TimeInterval) -> UInt64 {
        lastSeekToken = pos.beginSeek(to: t, isPlaying: playing)
        return lastSeekToken
    }

    /// The most recent seek finished buffering.
    mutating func seekCompletes() { pos.finishSeek(token: lastSeekToken) }
}
