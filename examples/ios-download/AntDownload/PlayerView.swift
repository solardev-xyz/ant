import SwiftUI

struct PlayerView: View {
    @EnvironmentObject var node: AntNode
    @EnvironmentObject var engine: PlayerEngine
    /// True only while the user physically has the thumb down. This is the
    /// single source of truth for "show the finger position instead of the
    /// engine playhead". It must NOT be inferred from `scrubValue` being
    /// non-nil: the Slider's setter can fire during a track-change range
    /// reconciliation and leave a stale value behind, which previously
    /// shadowed the new track's 0 (scrub to 30s → skip → thumb stuck mid
    /// song). Gating on an explicit drag flag keeps `scrubValue` inert
    /// unless a drag is actually in progress.
    @State private var isScrubbing = false
    /// The finger position during a drag. Only consulted while
    /// `isScrubbing`; otherwise the scrubber follows `engine.displayTime`.
    @State private var scrubValue: Double = 0

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .player)

            ScrollView {
                VStack(spacing: 24) {
                    header
                    artwork
                    title
                    scrubber
                    transport
                    upNext
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)
                .padding(.bottom, 120)
                .animation(.easeInOut(duration: 0.25), value: engine.isBuffering)
            }
            .scrollIndicators(.hidden)
        }
        .preferredColorScheme(.dark)
    }

    // MARK: – sections

    private var header: some View {
        GlassEffectContainer(spacing: 16) {
            HStack(spacing: 14) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Player")
                        .font(.system(.title2, design: .rounded).weight(.semibold))
                        .foregroundStyle(.white)
                    Text(agentLabel)
                        .font(.footnote)
                        .foregroundStyle(.white.opacity(0.65))
                }
                Spacer(minLength: 12)
                peerBadge
            }
            .padding(.horizontal, 22)
            .padding(.vertical, 14)
            .frame(maxWidth: .infinity, alignment: .leading)
            .glassEffect(.regular.tint(.white.opacity(0.06)), in: .capsule)
        }
    }

    private var peerBadge: some View {
        HStack(spacing: 8) {
            Circle()
                .fill(node.peerCount > 0 ? Color.green : Color.orange)
                .frame(width: 8, height: 8)
                .shadow(color: (node.peerCount > 0 ? Color.green : Color.orange).opacity(0.8), radius: 6)
            Image(systemName: "globe")
                .font(.footnote)
                .foregroundStyle(.white.opacity(0.85))
            Text("\(node.peerCount)")
                .font(.system(.footnote, design: .rounded).weight(.semibold))
                .monospacedDigit()
                .foregroundStyle(.white)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .glassEffect(.regular.tint(.white.opacity(0.10)), in: .capsule)
    }

    private var artwork: some View {
        ZStack {
            if let cover = engine.coverArt {
                Image(uiImage: cover)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
            } else if let track = engine.currentTrack {
                Image(systemName: artworkIcon(for: track))
                    .font(.system(size: 88, weight: .light))
                    .foregroundStyle(.white.opacity(0.85))
            } else {
                Image(systemName: "waveform")
                    .font(.system(size: 88, weight: .light))
                    .foregroundStyle(.white.opacity(0.6))
            }
        }
        .frame(maxWidth: .infinity)
        .frame(height: 320)
        .clipShape(RoundedRectangle(cornerRadius: 36, style: .continuous))
        .glassEffect(.regular.tint(.white.opacity(0.04)), in: .rect(cornerRadius: 36))
        .padding(.horizontal, 4)
    }

    private var title: some View {
        VStack(spacing: 6) {
            Text(engine.currentTrack?.title ?? statusTitle)
                .font(.system(.title, design: .rounded).weight(.semibold))
                .foregroundStyle(.white)
                .multilineTextAlignment(.center)
                .lineLimit(2)
            HStack(spacing: 8) {
                Text(engine.currentTrack.flatMap { $0.formatLabel.isEmpty ? nil : $0.formatLabel } ?? "—")
                if let bytes = engine.currentTrack?.sizeBytes {
                    Circle().frame(width: 3, height: 3)
                    Text(formatBytes(bytes))
                }
                if let track = engine.currentTrack, !track.fileExtension.isEmpty {
                    Circle().frame(width: 3, height: 3)
                    Text(track.filename)
                        .lineLimit(1)
                        .truncationMode(.middle)
                }
            }
            .font(.system(.footnote, design: .rounded))
            .foregroundStyle(.white.opacity(0.7))
            .padding(.horizontal, 12)
        }
        .padding(.top, 4)
    }

    private var scrubber: some View {
        // The slider is enabled only once we know the track length —
        // before AVPlayer's `readyToPlay` lands, `duration` is 0 and
        // any drag would just snap to 0. SwiftUI's `Slider` doesn't
        // accept a degenerate `0...0` range, so we widen to `0...1`
        // and disable the control. Once `duration > 0`, the binding
        // either echoes the user's drag (`scrubValue`) or the live
        // `engine.currentTime`, never both — which is what stops the
        // 0.25 s time-observer ticks from yanking the thumb back
        // while the user is dragging.
        let hasDuration = engine.duration > 0
        let displayedTime = scrubberTime(
            isScrubbing: isScrubbing, scrubValue: scrubValue, enginePlayhead: engine.displayTime)
        let upper = hasDuration ? engine.duration : 1
        return VStack(spacing: 6) {
            Slider(
                value: Binding(
                    get: { min(displayedTime, upper) },
                    set: { newValue in scrubValue = newValue }
                ),
                in: 0...upper,
                onEditingChanged: { editing in
                    if editing {
                        // Seed the finger position from the live playhead
                        // so the thumb doesn't jump on grab.
                        scrubValue = engine.displayTime
                        isScrubbing = true
                        engine.viewDebugLog(
                            "scrub-begin", isScrubbing: true, scrubValue: scrubValue, shown: scrubValue)
                    } else {
                        isScrubbing = false
                        engine.viewDebugLog(
                            "scrub-end", isScrubbing: false, scrubValue: scrubValue, shown: scrubValue)
                        engine.seek(to: scrubValue)
                        // `PlayerEngine.position.pendingSeekTime` holds the
                        // thumb at the release position until the stream
                        // has buffered at the new offset.
                    }
                }
            )
            .tint(.white.opacity(0.95))
            .disabled(!hasDuration)

            HStack {
                Text(formatDuration(displayedTime))
                Spacer(minLength: 8)
                if engine.isBuffering {
                    HStack(spacing: 6) {
                        ProgressView()
                            .tint(.white)
                            .controlSize(.mini)
                        ProgressView(value: bufferFraction)
                            .progressViewStyle(.linear)
                            .tint(.white)
                            .frame(width: 44)
                            .animation(.easeOut(duration: 0.25), value: bufferFraction)
                        Text(bufferingDetail)
                            .foregroundStyle(.white.opacity(0.85))
                    }
                    .transition(.opacity)
                    Spacer(minLength: 8)
                }
                Text(formatDuration(engine.duration))
            }
            .font(.system(.caption, design: .rounded))
            .monospacedDigit()
            .foregroundStyle(.white.opacity(0.7))
        }
        .padding(.horizontal, 8)
        // A track switch is authoritative: drop any scrub tracking so the
        // thumb follows the new track's playhead (0) instead of a stale
        // drag value left in @State by the Slider's reconciliation.
        .onChange(of: engine.currentIndex) { _, _ in
            isScrubbing = false
            scrubValue = 0
            engine.viewDebugLog(
                "track-change", isScrubbing: false, scrubValue: 0, shown: engine.displayTime)
        }
        // Regression sentinel: while not dragging, the thumb must track the
        // engine playhead. If `displayedTime` ever drifts from it, log a
        // MISMATCH line so a reintroduced stale-scrub leak is diagnosable
        // from the log alone — no screenshot needed.
        .onChange(of: engine.displayTime) { _, _ in
            if !isScrubbing && abs(displayedTime - engine.displayTime) > 0.5 {
                engine.viewDebugLog(
                    "drift", isScrubbing: isScrubbing, scrubValue: scrubValue, shown: displayedTime)
            }
        }
    }

    /// Fill for the little bar next to the spinner, as a 0…1 fraction.
    /// Prefers the live stream *download* progress (`streamProgress.fraction`),
    /// which advances monotonically the instant bytes arrive — including
    /// during the `openStream` / first-fill gap when AVPlayer's
    /// `loadedTimeRanges` (and hence `bufferedAhead`) are still empty.
    /// Falls back to the decoded-runway estimate when no stream stats are
    /// available yet, so the bar always reflects forward progress rather
    /// than sitting at zero.
    private var bufferFraction: Double {
        if let fraction = engine.streamProgress?.fraction, fraction > 0 {
            return min(max(fraction, 0), 1)
        }
        return min(max(engine.bufferedAhead / Self.bufferTargetSeconds, 0), 1)
    }
    private static let bufferTargetSeconds: Double = 10

    /// Compact buffering detail shown between the time labels: how much
    /// runway is already decoded plus the live fetch rate, so a stall
    /// reads as "filling up" rather than "frozen".
    private var bufferingDetail: String {
        var parts: [String] = []
        if engine.bufferedAhead >= 0.5 {
            parts.append(String(format: "%.0fs ready", engine.bufferedAhead))
        }
        if engine.throughputBytesPerSec >= 1 {
            parts.append(formatBytesPerSec(engine.throughputBytesPerSec))
        }
        return parts.isEmpty ? "fetching…" : parts.joined(separator: " · ")
    }

    private var transport: some View {
        GlassEffectContainer(spacing: 18) {
            HStack(spacing: 18) {
                Button(action: { engine.previous() }) {
                    Image(systemName: "backward.end.fill")
                        .font(.title2)
                        .frame(width: 56, height: 56)
                }
                .buttonStyle(.glass)
                .disabled(engine.queue.isEmpty)

                Button(action: { engine.togglePlay() }) {
                    Image(systemName: engine.isPlaying ? "pause.fill" : "play.fill")
                        .font(.title)
                        .foregroundStyle(.white)
                        .frame(width: 76, height: 76)
                }
                .buttonStyle(.glassProminent)
                // Saturated orange picks up the Player palette's warm
                // edge and gives the white pause/play symbol enough
                // contrast to read at a glance. With a `.white` tint
                // the prominent style filled the button white and the
                // symbol vanished into the background.
                .tint(Color(red: 1.0, green: 0.45, blue: 0.20))
                .disabled(engine.currentTrack == nil)

                Button(action: { engine.next() }) {
                    Image(systemName: "forward.end.fill")
                        .font(.title2)
                        .frame(width: 56, height: 56)
                }
                .buttonStyle(.glass)
                .disabled(engine.queue.isEmpty)
            }
        }
        .frame(maxWidth: .infinity)
    }

    private var upNext: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("UP NEXT")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                Spacer()
                if case .waitingForPeers = engine.playbackStatus,
                   node.peerCount == 0 {
                    ProgressView()
                        .tint(.white.opacity(0.7))
                        .controlSize(.mini)
                }
            }
            .padding(.horizontal, 6)

            if case .failed(let msg) = engine.playbackStatus {
                GlassCard {
                    Text(msg)
                        .font(.footnote)
                        .foregroundStyle(.red.opacity(0.9))
                }
            } else {
                queueList
            }
        }
    }

    @ViewBuilder
    private var queueList: some View {
        let upcoming = engine.queue.enumerated()
            .filter { $0.offset != engine.currentIndex }
        VStack(spacing: 10) {
            ForEach(upcoming, id: \.offset) { idx, track in
                Button {
                    engine.select(index: idx, autoPlay: true)
                } label: {
                    HStack(spacing: 14) {
                        Image(systemName: artworkIcon(for: track))
                            .font(.title3)
                            .foregroundStyle(.white.opacity(0.85))
                            .frame(width: 36, height: 36)
                            .glassEffect(.regular.tint(.white.opacity(0.08)), in: .rect(cornerRadius: 10))
                        VStack(alignment: .leading, spacing: 3) {
                            Text(track.title)
                                .font(.system(.body, design: .rounded).weight(.medium))
                                .foregroundStyle(.white)
                                .lineLimit(1)
                            HStack(spacing: 6) {
                                Text(track.formatLabel)
                                if let size = track.sizeBytes {
                                    Circle().frame(width: 2, height: 2)
                                    Text(formatBytes(size))
                                }
                            }
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.65))
                        }
                        Spacer()
                    }
                    .padding(.horizontal, 16)
                    .padding(.vertical, 12)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .glassEffect(.regular.tint(.white.opacity(0.04)), in: .rect(cornerRadius: 22))
                }
                .buttonStyle(.plain)
            }
        }
    }

    // MARK: – helpers

    private var statusTitle: String {
        switch node.status {
        case .idle, .starting:
            return "Starting"
        case .ready:
            switch engine.playbackStatus {
            case .waitingForPeers: return "Waiting for peers"
            case .ready: return "Ready"
            case .failed(let msg): return msg
            }
        case .failed(let msg):
            return msg
        }
    }

    /// Header subtitle. Reads the agent string off the running node's
    /// status snapshot (`ant-ffi/<crate-version>`); while the node is
    /// still initialising we fall back to a generic "starting…" hint
    /// so the row never goes blank on cold start.
    private var agentLabel: String {
        if !node.agentString.isEmpty {
            return "\(node.agentString) · swarm"
        }
        switch node.status {
        case .idle, .starting: return "starting node…"
        case .ready: return "ant · swarm"
        case .failed: return "node failed"
        }
    }

    private func artworkIcon(for track: Track) -> String {
        switch track.fileExtension.lowercased() {
        case "wav": return "waveform"
        case "flac": return "music.note.list"
        case "mp3", "m4a", "aac": return "music.note"
        default: return "music.note"
        }
    }
}
