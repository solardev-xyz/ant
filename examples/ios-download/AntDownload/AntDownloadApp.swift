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
                }
        }
    }
}
