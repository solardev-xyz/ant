import SwiftUI

/// AntDrive — a Dropbox-style front end for a Swarm light node.
///
/// Two tabs: **Files** (upload / download / share content) and
/// **Storage** (the storage plan, account balance, and account key).
/// Both talk to a single embedded `ant-node` through the C FFI wrapped
/// by `AntNode`.
@main
struct AntDriveApp: App {
    @StateObject private var node = AntNode()

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(node)
                .task { await node.start() }
        }
    }
}
