import SwiftUI

@main
struct AntDownloadApp: App {
    @StateObject private var node = AntNode()
    @StateObject private var engine: PlayerEngine

    init() {
        let n = AntNode()
        _node = StateObject(wrappedValue: n)
        _engine = StateObject(wrappedValue: PlayerEngine(node: n))
    }

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(node)
                .environmentObject(engine)
                .task {
                    await node.start()
                    if ProcessInfo.processInfo.arguments.contains("--resume-smoke") {
                        await runResumeSmoke(node)
                    }
                }
        }
    }
}

/// Headless smoke for `ant_resume` (issue #12), driven by the
/// `--resume-smoke` launch argument. Waits for the embedded node to reach
/// a healthy peer set, calls `ant_resume`, and logs the peer count before
/// and after so a `simctl launch --console` run can confirm the FFI round
/// trip succeeds end-to-end and the swarm stays/recovers healthy. (The
/// real wedge — half-open sockets after an OS reap — only reproduces on a
/// physical device; the simulator validates linking, the command path, and
/// that a live call is a safe no-op.) Lines are tagged `RESUME-SMOKE` for
/// easy grepping alongside the Rust `ant_p2p` resume log.
@MainActor
func runResumeSmoke(_ node: AntNode) async {
    func log(_ s: String) { NSLog("RESUME-SMOKE %@", s) }

    log("waiting for node ready + peers (up to 75s)…")
    for _ in 0..<150 {
        if node.status.isReady && node.peerCount > 0 { break }
        try? await Task.sleep(nanoseconds: 500_000_000)
    }
    log("before: ready=\(node.status.isReady) peers=\(node.peerCount)")

    let result = node.resume()
    log(result)

    // Give the fresh bootstrap dial a few seconds to land.
    try? await Task.sleep(nanoseconds: 6_000_000_000)
    log("after: peers=\(node.peerCount)")

    let ok = result.hasPrefix("resume: ok") && node.peerCount > 0
    log(ok ? "VERDICT pass" : "VERDICT fail")
}
