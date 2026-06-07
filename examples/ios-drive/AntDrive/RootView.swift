import SwiftUI

/// Two-tab shell: Files (content) and Storage (plan / account). iOS 26
/// renders the tab bar as a floating glass pill for free.
struct RootView: View {
    @EnvironmentObject var node: AntNode

    var body: some View {
        TabView {
            Tab("Files", systemImage: "folder.fill") {
                FilesView()
            }
            Tab("Storage", systemImage: "externaldrive.fill") {
                StorageView()
            }
        }
        .tabBarMinimizeBehavior(.onScrollDown)
        .preferredColorScheme(.dark)
    }
}
