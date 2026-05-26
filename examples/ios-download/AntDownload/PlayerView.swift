import SwiftUI

struct PlayerView: View {
    @EnvironmentObject var node: AntNode
    @EnvironmentObject var engine: PlayerEngine
    /// Local scrubbing intent. While the user has the thumb down,
    /// `engine.currentTime` keeps ticking in the background but the
    /// slider must show *their* finger position — otherwise the
    /// periodic time observer's writes on the main actor would yank
    /// the thumb back. Mirrors UIKit's "tracking" state. `nil` means
    /// "not scrubbing — show the live time".
    @State private var scrubValue: Double? = nil

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
        let displayedTime = scrubValue ?? engine.displayTime
        let upper = hasDuration ? engine.duration : 1
        return VStack(spacing: 6) {
            Slider(
                value: Binding(
                    get: { min(displayedTime, upper) },
                    set: { newValue in scrubValue = newValue }
                ),
                in: 0...upper,
                onEditingChanged: { editing in
                    if !editing {
                        if let target = scrubValue {
                            engine.seek(to: target)
                        }
                        // `PlayerEngine.pendingSeekTime` holds the thumb
                        // at the release position until the stream has
                        // buffered at the new offset.
                        scrubValue = nil
                    }
                }
            )
            .tint(.white.opacity(0.95))
            .disabled(!hasDuration)

            HStack {
                Text(formatDuration(displayedTime))
                Spacer()
                Text(formatDuration(engine.duration))
            }
            .font(.system(.caption, design: .rounded))
            .monospacedDigit()
            .foregroundStyle(.white.opacity(0.7))
        }
        .padding(.horizontal, 8)
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
