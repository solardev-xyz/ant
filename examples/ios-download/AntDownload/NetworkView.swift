import Charts
import SwiftUI

struct NetworkView: View {
    @EnvironmentObject var node: AntNode
    @EnvironmentObject var engine: PlayerEngine

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .network)

            ScrollView {
                VStack(spacing: 22) {
                    header
                    progressCard
                    speedCard
                    statTiles
                    throughputCard
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)
                .padding(.bottom, 120)
            }
            .scrollIndicators(.hidden)
        }
        .preferredColorScheme(.dark)
    }

    private var header: some View {
        HStack(spacing: 14) {
            VStack(alignment: .leading, spacing: 2) {
                Text("Network")
                    .font(.system(.title2, design: .rounded).weight(.semibold))
                    .foregroundStyle(.white)
                Text(subtitleText)
                    .font(.footnote)
                    .foregroundStyle(.white.opacity(0.65))
                    .lineLimit(1)
            }
            Spacer(minLength: 12)
        }
        .padding(.horizontal, 22)
        .padding(.vertical, 14)
        .frame(maxWidth: .infinity, alignment: .leading)
        .glassEffect(.regular.tint(.white.opacity(0.06)), in: .capsule)
    }

    private var progressCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text("PROGRESS")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))

                HStack(alignment: .lastTextBaseline) {
                    Text(bytesLabel)
                        .font(.system(.title2, design: .rounded).weight(.semibold))
                        .foregroundStyle(.white)
                    Spacer()
                    Text(percentLabel)
                        .font(.system(.title3, design: .rounded).weight(.medium))
                        .foregroundStyle(.white.opacity(0.85))
                        .monospacedDigit()
                }

                ProgressView(value: progressFraction)
                    .progressViewStyle(.linear)
                    .tint(.white.opacity(0.9))
            }
        }
    }

    private var speedCard: some View {
        GlassCard {
            HStack(spacing: 14) {
                Image(systemName: "speedometer")
                    .font(.title)
                    .foregroundStyle(.white.opacity(0.85))
                VStack(alignment: .leading, spacing: 2) {
                    Text(formatBytesPerSec(engine.throughputBytesPerSec))
                        .font(.system(.title2, design: .rounded).weight(.semibold))
                        .foregroundStyle(.white)
                        .monospacedDigit()
                }
                Spacer()
                VStack(alignment: .trailing, spacing: 2) {
                    Text("ETA")
                        .font(.caption)
                        .tracking(1)
                        .foregroundStyle(.white.opacity(0.55))
                    Text(etaLabel)
                        .font(.system(.body, design: .rounded).weight(.medium))
                        .monospacedDigit()
                        .foregroundStyle(.white.opacity(0.85))
                }
            }
        }
    }

    private var statTiles: some View {
        GlassEffectContainer(spacing: 14) {
            HStack(spacing: 14) {
                statTile(
                    value: chunksValue,
                    suffix: chunksSuffix,
                    label: "chunks",
                    icon: "square.grid.2x2"
                )
                statTile(
                    value: "\(node.peerCount)",
                    suffix: nil,
                    label: "peers",
                    icon: "globe"
                )
                statTile(
                    value: "\(engine.streamProgress?.inFlight ?? 0)",
                    suffix: nil,
                    label: "in-flight",
                    icon: "arrow.left.arrow.right"
                )
            }
        }
    }

    private func statTile(value: String, suffix: String?, label: String, icon: String) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Image(systemName: icon)
                .font(.footnote)
                .foregroundStyle(.white.opacity(0.7))
            HStack(alignment: .firstTextBaseline, spacing: 4) {
                Text(value)
                    .font(.system(.title2, design: .rounded).weight(.semibold))
                    .monospacedDigit()
                    .foregroundStyle(.white)
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                if let suffix = suffix {
                    Text(suffix)
                        .font(.caption2)
                        .foregroundStyle(.white.opacity(0.55))
                }
            }
            Text(label)
                .font(.caption)
                .foregroundStyle(.white.opacity(0.6))
        }
        .padding(14)
        .frame(maxWidth: .infinity, alignment: .leading)
        .glassEffect(.regular.tint(.white.opacity(0.05)), in: .rect(cornerRadius: 22))
    }

    private var throughputCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    Text("THROUGHPUT · 60s")
                        .font(.system(.caption, design: .rounded).weight(.semibold))
                        .tracking(2)
                        .foregroundStyle(.white.opacity(0.6))
                    Spacer()
                    Text("peak \(formatBytesPerSec(engine.peakThroughputBytesPerSec))")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                        .monospacedDigit()
                }

                Chart(engine.throughputHistory) { sample in
                    AreaMark(
                        x: .value("t", sample.at),
                        y: .value("rate", sample.bytesPerSec)
                    )
                    .interpolationMethod(.monotone)
                    .foregroundStyle(
                        LinearGradient(
                            colors: [.white.opacity(0.4), .white.opacity(0.0)],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    LineMark(
                        x: .value("t", sample.at),
                        y: .value("rate", sample.bytesPerSec)
                    )
                    .interpolationMethod(.monotone)
                    .foregroundStyle(.white.opacity(0.95))
                    .lineStyle(StrokeStyle(lineWidth: 2))
                }
                .chartYAxis(.hidden)
                .chartXAxis(.hidden)
                .chartXScale(domain: chartDomain)
                .frame(height: 110)
            }
        }
    }

    // MARK: – derived

    private var subtitleText: String {
        if let track = engine.currentTrack {
            return "\(track.title) · \(track.formatLabel)"
        }
        return "No active stream"
    }

    private var bytesLabel: String {
        guard let progress = engine.streamProgress else {
            if let track = engine.currentTrack, let total = track.sizeBytes {
                return "0 B / \(formatBytes(total))"
            }
            return "—"
        }
        return "\(formatBytes(progress.bytesDownloaded)) / \(formatBytes(progress.totalBytes))"
    }

    private var percentLabel: String {
        guard let progress = engine.streamProgress, progress.totalBytes > 0 else {
            return "0 %"
        }
        let pct = Int((progress.fraction * 100).rounded())
        return "\(pct) %"
    }

    private var progressFraction: Double {
        engine.streamProgress?.fraction ?? 0
    }

    private var etaLabel: String {
        guard let progress = engine.streamProgress, progress.totalBytes > progress.bytesDownloaded else {
            return "—"
        }
        let remaining = progress.totalBytes - progress.bytesDownloaded
        return formatEta(remainingBytes: remaining, bytesPerSec: engine.throughputBytesPerSec)
    }

    private var chunksValue: String {
        let done = engine.streamProgress?.chunksDone ?? 0
        return "\(done)"
    }

    private var chunksSuffix: String? {
        guard let total = engine.streamProgress?.totalChunks, total > 0 else { return nil }
        return "/ \(total)"
    }

    private var chartDomain: ClosedRange<Date> {
        let now = Date()
        return now.addingTimeInterval(-60)...now
    }
}
