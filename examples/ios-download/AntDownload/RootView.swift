import SwiftUI

/// Top-level container. Two tabs (Player / Network) inside a `TabView`;
/// iOS 26 renders that tab bar as a floating glass pill at the bottom
/// for free, which is exactly what the screenshot shows.
struct RootView: View {
    @EnvironmentObject var node: AntNode
    @EnvironmentObject var engine: PlayerEngine

    var body: some View {
        TabView {
            Tab("Player", systemImage: "play.fill") {
                PlayerView()
            }
            Tab("Network", systemImage: "waveform.path.ecg") {
                NetworkView()
            }
        }
        .tabBarMinimizeBehavior(.onScrollDown)
        .preferredColorScheme(.dark)
        .task {
            if case .ready = node.status {
                await engine.startFirstTrackIfNeeded()
            }
        }
        .onChange(of: node.status) { _, newStatus in
            if case .ready = newStatus {
                Task { await engine.startFirstTrackIfNeeded() }
            }
        }
    }
}
