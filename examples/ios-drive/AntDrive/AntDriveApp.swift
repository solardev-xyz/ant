import BackgroundTasks
import SwiftUI
import UIKit

/// AntDrive — a Dropbox-style front end for a Swarm light node.
///
/// Two tabs: **Files** (upload / download / share content) and
/// **Storage** (the storage plan, account balance, and account key).
/// Both talk to a single embedded `ant-node` through the C FFI wrapped
/// by `AntNode`.
///
/// Lifecycle: iOS gives the app no execution in the background, so on
/// `.background` we spend the short grace window checkpointing upload
/// state (`ant_suspend`) and — when files still need network work —
/// hand the rest to a `BGProcessingTask` (`antdrive.secure`) that wakes
/// the node, lets interrupted uploads/securing passes finish, and
/// suspends again. `.active` undoes the suspension.
@main
struct AntDriveApp: App {
    @StateObject private var node: AntNode
    @Environment(\.scenePhase) private var scenePhase

    /// BGProcessingTask identifier under which interrupted uploads and
    /// securing passes finish in the background. Must match the
    /// `BGTaskSchedulerPermittedIdentifiers` entry in Info.plist.
    static let secureTaskID = "antdrive.secure"

    init() {
        // Launch registration is mandatory for BGTaskScheduler; the
        // handler captures the same node instance the UI observes.
        let node = AntNode()
        _node = StateObject(wrappedValue: node)
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: Self.secureTaskID, using: nil
        ) { task in
            guard let task = task as? BGProcessingTask else { return }
            Self.handleSecureTask(task, node: node)
        }
    }

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(node)
                .task { await node.start() }
        }
        .onChange(of: scenePhase) { _, phase in
            switch phase {
            case .background:
                Self.enterBackground(node: node)
            case .active:
                Task {
                    await node.wake()
                    await node.refreshAll()
                }
            default:
                break
            }
        }
    }

    /// Spend the backgrounding grace window well: keep the process alive
    /// with a `beginBackgroundTask` while `ant_suspend` drains and
    /// checkpoints the upload drivers, then — if anything still needs
    /// the network to become durable — schedule the securing task so
    /// the system gives us another run later.
    private static func enterBackground(node: AntNode) {
        let app = UIApplication.shared
        var bgTask: UIBackgroundTaskIdentifier = .invalid
        bgTask = app.beginBackgroundTask(withName: "antdrive.suspend") {
            if bgTask != .invalid {
                app.endBackgroundTask(bgTask)
                bgTask = .invalid
            }
        }
        Task {
            await node.suspend()
            if await node.hasUnsettledUploads {
                scheduleSecureTask()
            }
            await MainActor.run {
                if bgTask != .invalid {
                    app.endBackgroundTask(bgTask)
                    bgTask = .invalid
                }
            }
        }
    }

    private static func scheduleSecureTask() {
        let request = BGProcessingTaskRequest(identifier: secureTaskID)
        // Securing is pure network work; no point waking without a path.
        request.requiresNetworkConnectivity = true
        request.requiresExternalPower = false
        // Duplicate submissions replace the pending request — safe on
        // every backgrounding.
        try? BGTaskScheduler.shared.submit(request)
    }

    /// The `antdrive.secure` handler: wake the node, wait for every
    /// upload to finish and verify durable (`heal_verified`), then
    /// suspend again. The expiration handler cancels the wait so the
    /// final suspend still happens inside the system's window.
    private static func handleSecureTask(_ task: BGProcessingTask, node: AntNode) {
        let work = Task {
            await node.wake()
            var settled = false
            while !Task.isCancelled {
                await node.refreshJobs()
                let unsettled = await node.hasUnsettledUploads
                if !unsettled {
                    settled = true
                    break
                }
                try? await Task.sleep(nanoseconds: 3_000_000_000)
            }
            await node.suspend()
            // Ask for another window if we ran out of time mid-secure.
            if !settled { scheduleSecureTask() }
            task.setTaskCompleted(success: settled)
        }
        task.expirationHandler = { work.cancel() }
    }
}
