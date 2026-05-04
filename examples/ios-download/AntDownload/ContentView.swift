import SwiftUI

struct ContentView: View {
    @EnvironmentObject var node: AntNode
    @State private var reference: String = ""
    @State private var result: DownloadResult? = nil
    @State private var isDownloading: Bool = false
    @State private var errorMessage: String? = nil
    @State private var autoTriggered: Bool = false
    @State private var wasCanceled: Bool = false
    @FocusState private var referenceFieldFocused: Bool
    let autoReference: String?

    init(autoReference: String? = nil) {
        self.autoReference = autoReference
        _reference = State(initialValue: autoReference ?? "")
    }

    var body: some View {
        NavigationStack {
            Form {
                Section("Status") {
                    HStack {
                        Text("Node")
                        Spacer()
                        Text(node.status.label)
                            .foregroundStyle(node.status.color)
                    }
                    HStack {
                        Text("Peers")
                        Spacer()
                        Text("\(node.peerCount)")
                            .foregroundStyle(node.peerCount > 0 ? Color.green : .secondary)
                            .monospacedDigit()
                    }
                }

                Section("Reference") {
                    TextField(
                        "64-hex / bytes://<hex> / bzz://<hex>[/path]",
                        text: $reference,
                        axis: .vertical
                    )
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .font(.system(.body, design: .monospaced))
                    .lineLimit(2...6)
                    .focused($referenceFieldFocused)

                    if isDownloading {
                        // Pause: stop waiting for the in-flight call so
                        // the UI can breathe, but keep the reference
                        // and partial state on screen. The embedded
                        // node's retrieval task keeps running in the
                        // background; chunks land in the in-memory
                        // cache, so pressing Download again resumes
                        // almost instantly.
                        Button {
                            wasCanceled = true
                            node.cancelDownload()
                        } label: {
                            HStack {
                                ProgressView()
                                Text("Pause")
                            }
                        }
                        // Cancel: same FFI abort, plus wipe the UI
                        // back to a fresh slate. The chunk cache
                        // inside the node is untouched (the smoke-test
                        // FFI has no cross-request cancel token yet),
                        // so a user who pastes the same reference
                        // again will still see a fast second run — but
                        // from the screen's point of view they're
                        // starting over.
                        Button(role: .destructive) {
                            wasCanceled = true
                            node.cancelDownload()
                            reference = ""
                            result = nil
                            errorMessage = nil
                        } label: {
                            Text("Cancel")
                        }
                    } else {
                        Button {
                            referenceFieldFocused = false
                            Task { await download() }
                        } label: {
                            Text("Download")
                        }
                        .disabled(reference.isEmpty || !node.status.isReady)
                    }
                }

                if let progress = node.downloadProgress {
                    Section("Progress") {
                        progressSection(progress)
                    }
                }

                if let result = result {
                    Section("Result") {
                        LabeledContent("Bytes", value: "\(result.byteCount)")
                        LabeledContent("Prefix") {
                            Text(result.hexPrefix)
                                .font(.system(.caption, design: .monospaced))
                                .multilineTextAlignment(.trailing)
                        }
                        LabeledContent("Elapsed", value: String(format: "%.2f s", result.elapsed))
                    }
                }

                if let errorMessage = errorMessage {
                    Section("Error") {
                        Text(errorMessage)
                            .foregroundStyle(.red)
                            .font(.system(.footnote, design: .monospaced))
                    }
                }
            }
            .navigationTitle("Ant download")
            .onChange(of: node.status) { newStatus in
                maybeAutoTrigger(status: newStatus)
            }
            .onAppear {
                maybeAutoTrigger(status: node.status)
            }
        }
    }

    private func maybeAutoTrigger(status: AntNode.Status) {
        guard autoReference != nil, !autoTriggered, status.isReady, !isDownloading else {
            return
        }
        autoTriggered = true
        Task { await download() }
    }

    @ViewBuilder
    private func progressSection(_ p: DownloadProgress) -> some View {
        // While we haven't fetched the data root yet `totalBytes` is 0,
        // so there's nothing to pin a determinate bar to. Show an
        // indeterminate spinner with the bytes-done counter instead;
        // once the root lands the bar jumps to a real fraction.
        if let fraction = p.fraction {
            ProgressView(value: fraction) {
                HStack {
                    Text(bytesLabel(done: p.bytesDone, total: p.totalBytes))
                        .monospacedDigit()
                    Spacer()
                    Text("\(Int(fraction * 100)) %")
                        .monospacedDigit()
                        .foregroundStyle(.secondary)
                }
                .font(.system(.footnote, design: .monospaced))
            }
        } else {
            HStack {
                ProgressView()
                Text("Resolving manifest…")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(formatBytes(p.bytesDone))
                    .font(.system(.footnote, design: .monospaced))
                    .monospacedDigit()
            }
        }

        HStack {
            Label(throughputLabel(p.throughputBytesPerSec), systemImage: "speedometer")
                .font(.system(.footnote, design: .monospaced))
                .monospacedDigit()
            Spacer()
            Text(etaLabel(progress: p))
                .font(.system(.footnote, design: .monospaced))
                .monospacedDigit()
                .foregroundStyle(.secondary)
        }

        HStack(spacing: 12) {
            Label("\(p.chunksDone)\(p.totalChunks > 0 ? " / \(p.totalChunks)" : "") chunks",
                  systemImage: "square.grid.2x2")
            Spacer()
            Label("\(p.peersUsed) peer\(p.peersUsed == 1 ? "" : "s")",
                  systemImage: "network")
            Spacer()
            Label("\(p.inFlight) in-flight",
                  systemImage: "arrow.left.arrow.right")
        }
        .font(.caption)
        .foregroundStyle(.secondary)
    }

    private func bytesLabel(done: UInt64, total: UInt64) -> String {
        "\(formatBytes(done)) / \(formatBytes(total))"
    }

    private func formatBytes(_ n: UInt64) -> String {
        ByteCountFormatter.string(fromByteCount: Int64(min(n, UInt64(Int64.max))), countStyle: .file)
    }

    private func throughputLabel(_ bytesPerSec: Double) -> String {
        if bytesPerSec < 1 {
            return "—"
        }
        let s = ByteCountFormatter.string(fromByteCount: Int64(bytesPerSec), countStyle: .file)
        return "\(s)/s"
    }

    private func etaLabel(progress p: DownloadProgress) -> String {
        guard p.throughputBytesPerSec > 1,
              p.totalBytes > p.bytesDone else {
            return ""
        }
        let remaining = Double(p.totalBytes - p.bytesDone)
        let seconds = remaining / p.throughputBytesPerSec
        if seconds.isInfinite || seconds.isNaN || seconds > 60 * 60 * 24 {
            return ""
        }
        if seconds < 60 {
            return "eta \(Int(seconds.rounded())) s"
        }
        let m = Int(seconds / 60)
        let s = Int(seconds.truncatingRemainder(dividingBy: 60))
        return "eta \(m) m \(s) s"
    }

    private func download() async {
        errorMessage = nil
        result = nil
        wasCanceled = false
        isDownloading = true
        defer { isDownloading = false }

        let trimmed = reference.trimmingCharacters(in: .whitespacesAndNewlines)
        let start = Date()
        do {
            let data = try await node.download(reference: trimmed)
            let elapsed = Date().timeIntervalSince(start)
            let prefixLen = min(32, data.count)
            let prefix = data.prefix(prefixLen).map { String(format: "%02x", $0) }.joined()
            result = DownloadResult(
                byteCount: data.count,
                hexPrefix: prefixLen == data.count ? prefix : prefix + "…",
                elapsed: elapsed
            )
        } catch {
            // Silence the error banner when the user asked for it —
            // the Cancel press was the cancel signal and a red
            // "download canceled" row would just be noise. Any other
            // failure still surfaces.
            let msg = "\(error)"
            if !(wasCanceled && msg.contains("download canceled")) {
                errorMessage = msg
            }
        }
    }
}

private struct DownloadResult {
    let byteCount: Int
    let hexPrefix: String
    let elapsed: TimeInterval
}

#Preview {
    ContentView(autoReference: nil).environmentObject(AntNode())
}
