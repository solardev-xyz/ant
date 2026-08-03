import SwiftUI

/// Two-tab shell: Broadcast (going live) and Storage (plan / account).
/// iOS 26 renders the tab bar as a floating glass pill for free.
struct RootView: View {
    @EnvironmentObject var node: AntNode

    /// Verification-only: launch-argument hook so CI can screenshot the
    /// Storage tab without a tap driver. Removed before merge.
    static let shotArgs = Set(ProcessInfo.processInfo.arguments)

    var body: some View {
        if Self.shotArgs.contains("-antstream-shot-storage") {
            StorageView().preferredColorScheme(.dark)
        } else {
            tabs
        }
    }

    private var tabs: some View {
        TabView {
            Tab("Broadcast", systemImage: "dot.radiowaves.left.and.right") {
                BroadcastView()
            }
            Tab("Storage", systemImage: "externaldrive.fill") {
                StorageView()
            }
        }
        .tabBarMinimizeBehavior(.onScrollDown)
        .preferredColorScheme(.dark)
    }
}
