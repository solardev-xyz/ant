import SwiftUI
import UIKit

/// The publisher throughput bench (issue #67 stage 1) on the device.
///
/// This is the go/no-go instrument: it runs the publisher loop with
/// synthetic segments instead of camera output — same `POST /bzz` per
/// segment, same in-process gateway, same postage batch — and reports
/// what the phone can actually sustain over the network it is on. The
/// measurement core is `ant_bench_*` in `crates/ant-ffi/src/bench.rs`,
/// so the numbers here and the ones the CI harness prints come from one
/// implementation.
///
/// Two things the simulator cannot answer and this screen therefore
/// labels honestly: cellular radio behaviour, and battery/thermal drain.
/// Battery level is unavailable (and always reports -1) off a real
/// device, so a run without it says so in its own notes rather than
/// quietly reporting 0 %.
struct BenchView: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss
    @StateObject private var banner = BannerState()

    /// Renditions the decision is between — the issue asks which
    /// rendition(s) phone-direct broadcast targets, so the picker is
    /// the shortlist, not a free-form number.
    private struct Rendition: Identifiable {
        let name: String
        let kbps: UInt32
        var id: UInt32 { kbps }
    }

    private struct RunLength: Identifiable {
        let name: String
        let seconds: UInt64
        var id: UInt64 { seconds }
    }

    private static let renditions: [Rendition] = [
        Rendition(name: "360p", kbps: 900),
        Rendition(name: "540p", kbps: 1800),
        Rendition(name: "720p", kbps: 3400),
        Rendition(name: "1080p", kbps: 6000),
    ]

    private static let runLengths: [RunLength] = [
        RunLength(name: "2 min", seconds: 120),
        RunLength(name: "10 min", seconds: 600),
        RunLength(name: "30 min", seconds: 1800),
    ]

    @State private var bitrateKbps: UInt32 = 3400
    @State private var durationSeconds: UInt64 = 1800
    @State private var publishToSwarm = true
    @State private var running = false
    @State private var snapshot: BenchSnapshot?
    @State private var report: BenchReport?
    @State private var failure: String?
    @State private var pollTask: Task<Void, Never>?
    /// Battery level when the run started, or `nil` on a device that
    /// doesn't report one (the simulator).
    @State private var batteryAtStart: Float?
    @State private var worstThermal: ProcessInfo.ThermalState = .nominal

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .broadcast)
                ScrollView {
                    VStack(spacing: 18) {
                        introCard
                        if running || snapshot?.running == true {
                            liveCard
                        } else {
                            settingsCard
                        }
                        if let report { resultCard(report) }
                        if let failure { failureCard(failure) }
                    }
                    .padding(.horizontal, 16)
                    .padding(.top, 12)
                    .padding(.bottom, 60)
                }
                .scrollIndicators(.hidden)
            }
            .preferredColorScheme(.dark)
            .overlay(alignment: .top) { BannerView(message: banner.message) }
            .navigationTitle("Throughput bench")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { dismiss() }.disabled(running)
                }
            }
        }
        .onDisappear { pollTask?.cancel() }
    }

    // MARK: cards

    private var introCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text("Publisher throughput")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text(willPublish
                     ? "Publishes synthetic video segments to Swarm exactly the way a broadcast will — one POST per segment through this device's node — and measures what it sustains."
                     : "Measures only the on-device work (splitting each segment into chunks and stamping them). No network, no storage plan needed — this is the CPU ceiling, not the broadcast number.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                Label(environmentLabel, systemImage: "antenna.radiowaves.left.and.right")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))
            }
        }
    }

    private var settingsCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 16) {
                Text("RENDITION")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                Picker("Rendition", selection: $bitrateKbps) {
                    ForEach(Self.renditions) { Text($0.name).tag($0.kbps) }
                }
                .pickerStyle(.segmented)

                Text("RUN LENGTH")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                Picker("Run length", selection: $durationSeconds) {
                    ForEach(Self.runLengths) { Text($0.name).tag($0.seconds) }
                }
                .pickerStyle(.segmented)
                Text("\(bitrateKbps) kbit/s · \(Int(durationSeconds) / 60) min · 2 s segments")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))

                // Bound to `willPublish`, not to the raw preference: a
                // device with no plan cannot publish, and a switch left
                // reading "on" there would contradict both the sentence
                // under it and what the run actually does.
                Toggle(isOn: Binding(get: { willPublish },
                                     set: { publishToSwarm = $0 })) {
                    VStack(alignment: .leading, spacing: 2) {
                        Text("Publish to Swarm")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.white)
                        Text(canPublish
                             ? "Uses your storage plan — a 30 min run at this bitrate stores about \(formatBytes(estimatedBytes))."
                             : "Needs everything on the Broadcast checklist first; without it only the on-device pipeline can be measured.")
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.6))
                    }
                }
                .disabled(!canPublish)
                .tint(.red.opacity(0.7))

                GlassPillButton(title: "Start bench", icon: "gauge.with.needle",
                                tint: .red.opacity(0.35)) { start() }
            }
        }
    }

    private var liveCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                HStack {
                    Text("RUNNING")
                        .font(.system(.caption, design: .rounded).weight(.semibold))
                        .tracking(2)
                        .foregroundStyle(.white.opacity(0.6))
                    Spacer()
                    ProgressView().tint(.white)
                }
                if let s = snapshot {
                    ProgressView(value: s.progressFraction).tint(.white)
                    HStack(alignment: .lastTextBaseline, spacing: 6) {
                        Text(String(format: "%.2f", s.sustainedMbitS))
                            .font(.system(.largeTitle, design: .rounded).weight(.bold))
                            .foregroundStyle(.white)
                        Text("Mbit/s sustained")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                    statRow("Elapsed", "\(Int(s.elapsedS)) s of \(s.configuredDurationS) s")
                    statRow("Segments", "\(s.segmentsOk) ok · \(s.segmentsFailed) failed")
                    statRow("Chunks/s", String(format: "%.1f", s.sustainedChunksS))
                    statRow("Behind live", "\(s.lagMsLast) ms")
                    statRow("Peers", "\(s.peers)")
                } else {
                    Text("Starting…")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                }
                GlassPillButton(title: "Stop and report", icon: "stop.circle") { stop() }
            }
        }
    }

    private func resultCard(_ report: BenchReport) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                Label(report.sustained ? "Sustained the rendition" : "Did not keep up",
                      systemImage: report.sustained ? "checkmark.seal.fill" : "exclamationmark.triangle.fill")
                    .font(.headline)
                    .foregroundStyle(report.sustained ? Color.green : Color.orange)
                statRow("Sustained", String(format: "%.2f Mbit/s", report.sustainedMbitS))
                statRow("Chunks/s", String(format: "%.1f", report.sustainedChunksS))
                statRow("Publish p50/p95",
                        "\(report.publishMsP50) / \(report.publishMsP95) ms")
                statRow("Lag p95 / final",
                        "\(report.lagMsP95) / \(report.lagMsFinal) ms")
                statRow("Segments",
                        "\(report.segmentsOk) ok · \(report.segmentsFailed) failed")
                if let first = report.errors.first {
                    Text(first)
                        .font(.caption)
                        .foregroundStyle(.orange.opacity(0.9))
                }
                Text(report.markdownRow)
                    .font(.system(.caption2, design: .monospaced))
                    .foregroundStyle(.white.opacity(0.7))
                    .textSelection(.enabled)
                GlassPillButton(title: "Copy results table", icon: "doc.on.doc") {
                    UIPasteboard.general.string = report.markdownBlock + endStateNote()
                    banner.flash("Results copied — paste into issue #67")
                }
            }
        }
    }

    private func failureCard(_ message: String) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 8) {
                Label("Bench failed", systemImage: "exclamationmark.triangle.fill")
                    .font(.headline)
                    .foregroundStyle(.orange)
                // The node's own message ("a benchmark is already
                // running", "invalid config: …", a gateway error) says
                // what to do next; a generic string would not.
                Text(message)
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
                    .textSelection(.enabled)
            }
        }
    }

    private func statRow(_ title: String, _ value: String) -> some View {
        HStack {
            Text(title)
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.65))
            Spacer()
            Text(value)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.white)
        }
    }

    // MARK: state

    /// Publishing needs exactly what the Broadcast checklist gates on —
    /// same expression, not a second copy of the conditions — plus the
    /// batch id to stamp with.
    private var canPublish: Bool { node.isReadyToBroadcast && batchId != nil }

    /// What this run will *actually* do. Every surface that describes
    /// the run — the blurb, the switch, the mode the node is started in
    /// — reads this one expression, so none of them can promise a
    /// network measurement the device can't take.
    private var willPublish: Bool { publishToSwarm && canPublish }

    private var batchId: String? {
        guard let plan = node.plan, plan.enabled, !plan.batchId.isEmpty else { return nil }
        return plan.batchId
    }

    private var estimatedBytes: UInt64 {
        UInt64(bitrateKbps) * 1000 * durationSeconds / 8
    }

    private var environmentLabel: String {
        "\(Self.deviceModel) · \(node.networkLabel)"
    }

    /// e.g. `iPhone16,1`. `UIDevice.model` only ever says "iPhone", which
    /// cannot distinguish the two device generations the issue asks to
    /// compare.
    ///
    /// On a simulator `uname` reports the *host* architecture, which
    /// would file a simulator row as if it came off a phone — so the
    /// simulated model is used instead, and marked as simulated.
    private static var deviceModel: String {
        if let simulated = ProcessInfo.processInfo
            .environment["SIMULATOR_MODEL_IDENTIFIER"], !simulated.isEmpty {
            return "Simulator \(simulated)"
        }
        var info = utsname()
        uname(&info)
        let identifier = withUnsafeBytes(of: &info.machine) { raw in
            raw.prefix { $0 != 0 }.map { Character(UnicodeScalar(UInt8($0))) }
        }
        let model = String(identifier)
        return model.isEmpty ? "unknown-device" : model
    }

    // MARK: actions

    private func start() {
        failure = nil
        report = nil
        snapshot = nil
        running = true
        UIDevice.current.isBatteryMonitoringEnabled = true
        // The simulator reports -1: record "unknown" rather than a
        // battery delta that would be fiction in the results table.
        let level = UIDevice.current.batteryLevel
        batteryAtStart = level < 0 ? nil : level
        worstThermal = ProcessInfo.processInfo.thermalState
        // Keep the screen awake: iOS suspends the app on auto-lock and a
        // 30-minute run would end up measuring the sleep timer.
        UIApplication.shared.isIdleTimerDisabled = true

        let batch = willPublish ? batchId : nil
        Task {
            do {
                try await node.startBench(
                    label: environmentLabel,
                    bitrateKbps: bitrateKbps,
                    durationSeconds: durationSeconds,
                    batchId: batch,
                    notes: startingNotes()
                )
                poll()
            } catch {
                running = false
                UIApplication.shared.isIdleTimerDisabled = false
                failure = error.localizedDescription
            }
        }
    }

    private func poll() {
        pollTask?.cancel()
        pollTask = Task { @MainActor in
            while !Task.isCancelled {
                // Battery and thermal are sampled here, on the same
                // cadence as the throughput readout, so a 30-minute run
                // reports the worst thermal state it passed through and
                // not just the state it happened to end in.
                let state = ProcessInfo.processInfo.thermalState
                if state.rawValue > worstThermal.rawValue { worstThermal = state }
                let s = await node.benchProgress()
                snapshot = s
                if s?.running == false { stop(); return }
                try? await Task.sleep(nanoseconds: 2_000_000_000)
            }
        }
    }

    private func stop() {
        pollTask?.cancel()
        pollTask = nil
        Task {
            defer {
                running = false
                UIApplication.shared.isIdleTimerDisabled = false
            }
            do {
                report = try await node.stopBench()
            } catch {
                failure = error.localizedDescription
            }
        }
    }

    /// The context the numbers need to be believable, captured at start
    /// and finished (battery delta, worst thermal state) when the report
    /// is rendered.
    private func startingNotes() -> String {
        var parts = ["iOS \(UIDevice.current.systemVersion)"]
        if let batteryAtStart {
            parts.append("battery \(Int(batteryAtStart * 100)) % at start")
        } else {
            parts.append("battery unavailable (simulator)")
        }
        parts.append("thermal \(Self.thermalName(ProcessInfo.processInfo.thermalState)) at start")
        return parts.joined(separator: ", ")
    }

    /// The half of the battery/thermal picture that only exists once the
    /// run has finished. The `notes` the node echoes were fixed at
    /// start, so the copied block carries the delta separately rather
    /// than silently reporting only the starting state.
    private func endStateNote() -> String {
        var line = "Ended: thermal peak \(Self.thermalName(worstThermal))"
        let level = UIDevice.current.batteryLevel
        if let batteryAtStart, level >= 0 {
            let drop = Int((batteryAtStart - level) * 100)
            line += ", battery \(Int(batteryAtStart * 100)) % → \(Int(level * 100)) % (−\(drop) pts)"
        } else {
            line += ", battery drain not measurable on this device"
        }
        return line + "\n"
    }

    private static func thermalName(_ state: ProcessInfo.ThermalState) -> String {
        switch state {
        case .nominal: return "nominal"
        case .fair: return "fair"
        case .serious: return "serious"
        case .critical: return "critical"
        @unknown default: return "unknown"
        }
    }
}
