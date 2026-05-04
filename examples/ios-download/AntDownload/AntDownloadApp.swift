import SwiftUI

@main
struct AntDownloadApp: App {
    @StateObject private var node = AntNode()

    /// Optional auto-download reference read from launch arguments.
    /// Usage: `xcrun simctl launch <udid> <bundle> --auto-download bzz://<hex>[/path]`.
    /// When present, the ContentView pre-fills the text field on first
    /// appear and kicks off a download as soon as the node is ready —
    /// handy for CI smoke tests and for driving the simulator without
    /// Accessibility permissions on the host.
    private let autoReference: String? = AntDownloadApp.parseAutoReference()

    var body: some Scene {
        WindowGroup {
            ContentView(autoReference: autoReference)
                .environmentObject(node)
                .task {
                    await node.start()
                }
        }
    }

    private static func parseAutoReference() -> String? {
        // `xcrun simctl launch <udid> <bundle> --auto-download <ref>`
        // also accepts the combined `--auto-download=<ref>` form, plus
        // an `ANT_AUTO_DOWNLOAD` env var (forwarded with `SIMCTL_CHILD_`
        // prefix from the host) as a belt-and-braces fallback.
        let args = CommandLine.arguments
        if let idx = args.firstIndex(of: "--auto-download"), idx + 1 < args.count {
            return args[idx + 1]
        }
        if let combined = args.first(where: { $0.hasPrefix("--auto-download=") }) {
            return String(combined.dropFirst("--auto-download=".count))
        }
        if let env = ProcessInfo.processInfo.environment["ANT_AUTO_DOWNLOAD"], !env.isEmpty {
            return env
        }
        return nil
    }
}
