import CoreTransferable
import PhotosUI
import SwiftUI
import UniformTypeIdentifiers

/// The Files tab — a Dropbox-style list of everything the user has
/// uploaded, with an Add menu (upload a file / add from a share link)
/// and per-file actions (copy Swarm hash, pause/resume/cancel, retry).
struct FilesView: View {
    @EnvironmentObject var node: AntNode

    @State private var showImporter = false
    @State private var showPhotoPicker = false
    @State private var showLinkSheet = false
    @State private var showGetStarted = false
    @State private var photoSelection: [PhotosPickerItem] = []
    @State private var linkText = ""
    @State private var banner: String?
    @State private var showAddOptions = false

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .player)

            ScrollView {
                VStack(spacing: 18) {
                    header
                    if !node.hasStorage {
                        getStartedCard
                    } else if node.jobs.isEmpty {
                        emptyState
                    } else {
                        ForEach(node.jobs) { job in
                            FileRow(job: job, onCopy: copyHash, onPause: pause,
                                    onResume: resume, onCancel: cancel)
                        }
                    }
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)
                .padding(.bottom, 130)
            }
            .scrollIndicators(.hidden)
            .refreshable { await node.refreshAndReverify() }
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { bannerView }
        .fileImporter(isPresented: $showImporter,
                      allowedContentTypes: [.item],
                      allowsMultipleSelection: true) { handleImport($0) }
        .photosPicker(isPresented: $showPhotoPicker,
                      selection: $photoSelection,
                      maxSelectionCount: 10,
                      matching: .any(of: [.images, .videos]))
        .onChange(of: photoSelection) { _, items in handlePhotos(items) }
        .sheet(isPresented: $showLinkSheet) { linkSheet }
        .sheet(isPresented: $showGetStarted) { GetStartedView() }
        .task { await node.refreshAll() }
    }

    // MARK: header

    private var header: some View {
        HStack(spacing: 14) {
            VStack(alignment: .leading, spacing: 2) {
                Text("Files")
                    .font(.system(.largeTitle, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text(node.status.isReady ? "\(node.peerCount) connections" : node.status.label)
                    .font(.footnote)
                    .foregroundStyle(.white.opacity(0.65))
            }
            Spacer()
            addButton
        }
        .padding(.horizontal, 6)
    }

    private var getStartedCard: some View {
        VStack(spacing: 20) {
            Image(systemName: "sparkles")
                .font(.system(size: 46))
                .foregroundStyle(.white)
                .padding(.top, 60)
            VStack(spacing: 8) {
                Text("Get started")
                    .font(.system(.title, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text("Set up a storage plan to start uploading, downloading, and sharing your files.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 24)
            }
            Button { showGetStarted = true } label: {
                Text("Get started")
                    .font(.headline)
                    .foregroundStyle(.white)
                    .padding(.horizontal, 40).padding(.vertical, 15)
                    .glassEffect(.regular.tint(.blue.opacity(0.6)), in: .capsule)
            }
            .buttonStyle(.plain)
        }
        .frame(maxWidth: .infinity)
    }

    private var emptyState: some View {
        VStack(spacing: 14) {
            Image(systemName: "tray.and.arrow.up")
                .font(.system(size: 52))
                .foregroundStyle(.white.opacity(0.5))
            Text("No files yet")
                .font(.title3.weight(.semibold))
                .foregroundStyle(.white)
            Text("Tap + to upload a file or add one from a link.")
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.65))
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
        .padding(.top, 80)
    }

    // MARK: add button + menu

    /// The add button only appears once storage is active — uploads and
    /// link imports require a storage plan, so before that the Get
    /// Started greeter is the only call to action. It lives in the header
    /// row, top-right, aligned with the "Files" headline.
    ///
    /// This is a plain glass `Button` (not a `Menu`): a `Menu` whose label
    /// carries a hand-applied `.glassEffect` flickers on iOS 26 because the
    /// system can't morph the custom glass into the menu. The options are
    /// presented via a `confirmationDialog` instead, which has no glass
    /// morph and keeps the button rock-steady.
    @ViewBuilder private var addButton: some View {
        if node.hasStorage {
            Button { showAddOptions = true } label: {
                Image(systemName: "plus")
                    .font(.title2.weight(.semibold))
                    .foregroundStyle(.white)
                    .frame(width: 48, height: 48)
                    .glassEffect(.regular.tint(.blue.opacity(0.5)), in: .circle)
            }
            .buttonStyle(.plain)
            .confirmationDialog("Add to AntDrive", isPresented: $showAddOptions,
                                titleVisibility: .visible) {
                Button("Upload photos") { showPhotoPicker = true }
                Button("Upload a file") { showImporter = true }
                Button("Add from link") { showLinkSheet = true }
                Button("Cancel", role: .cancel) {}
            }
        }
    }

    private var linkSheet: some View {
        NavigationStack {
            Form {
                Section("Share link") {
                    TextField("bzz://… or paste a link", text: $linkText)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                }
            }
            .navigationTitle("Add from link")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { showLinkSheet = false }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Add") { addFromLink() }
                        .disabled(linkText.trimmingCharacters(in: .whitespaces).isEmpty)
                }
            }
        }
        .presentationDetents([.height(220)])
    }

    @ViewBuilder private var bannerView: some View {
        if let banner {
            Text(banner)
                .font(.footnote.weight(.medium))
                .foregroundStyle(.white)
                .padding(.horizontal, 18).padding(.vertical, 12)
                .glassEffect(.regular.tint(.black.opacity(0.4)), in: .capsule)
                .padding(.top, 8)
                .transition(.move(edge: .top).combined(with: .opacity))
        }
    }

    // MARK: actions

    /// Copy the bare Swarm hash (64-hex, no `0x`, no `bzz://`) straight to
    /// the clipboard in a single tap — no share sheet, no submenu.
    private func copyHash(_ job: UploadJob) {
        guard let ref = job.reference, !ref.isEmpty else { return }
        let hex = ref.hasPrefix("0x") ? String(ref.dropFirst(2)) : ref
        UIPasteboard.general.string = hex
        flash("Copied Swarm hash")
    }

    private func pause(_ job: UploadJob) { Task { await node.pauseUpload(job.jobId) } }
    private func resume(_ job: UploadJob) { Task { await node.resumeUpload(job.jobId) } }
    private func cancel(_ job: UploadJob) { Task { await node.cancelUpload(job.jobId) } }

    private func addFromLink() {
        let ref = linkText.trimmingCharacters(in: .whitespaces)
        showLinkSheet = false
        linkText = ""
        Task {
            do {
                _ = try await node.download(reference: ref)
                flash("Imported from link")
            } catch {
                flash(error.localizedDescription)
            }
        }
    }

    /// Stage each picked photo/video into the sandbox and kick off an
    /// upload, keeping the original iOS filename. `PhotosPickerItem` hands
    /// us out-of-process data (no photo-library permission prompt); loading
    /// it as a file (rather than raw `Data`) lets us recover the source
    /// file's name (e.g. `IMG_4567.HEIC`) so the row and manifest use the
    /// real name instead of a synthetic `Photo-<stamp>` one.
    private func handlePhotos(_ items: [PhotosPickerItem]) {
        guard !items.isEmpty else { return }
        photoSelection = []
        Task {
            for item in items {
                do {
                    guard let picked = try await item.loadTransferable(type: PickedPhoto.self) else {
                        flash("Couldn't read selected item")
                        continue
                    }
                    let name = picked.url.lastPathComponent
                    let ct = UTType(filenameExtension: picked.url.pathExtension)?.preferredMIMEType
                    try await node.startUpload(path: picked.url, name: name, contentType: ct)
                    flash("Uploading \(name)")
                } catch {
                    flash(error.localizedDescription)
                }
            }
        }
    }

    private func handleImport(_ result: Result<[URL], Error>) {
        guard case .success(let urls) = result else { return }
        Task {
            for url in urls {
                do {
                    let local = try copyIntoSandbox(url)
                    let ct = UTType(filenameExtension: url.pathExtension)?.preferredMIMEType
                    try await node.startUpload(path: local,
                                               name: url.lastPathComponent,
                                               contentType: ct)
                    flash("Uploading \(url.lastPathComponent)")
                } catch {
                    flash(error.localizedDescription)
                }
            }
        }
    }

    /// The Rust uploader streams the source via mmap on its own schedule,
    /// so copy the security-scoped picked file into our sandbox first to
    /// give it a stable, always-readable path.
    private func copyIntoSandbox(_ url: URL) throws -> URL {
        let fm = FileManager.default
        let dest = try antdriveImportsDir().appendingPathComponent(url.lastPathComponent)
        let scoped = url.startAccessingSecurityScopedResource()
        defer { if scoped { url.stopAccessingSecurityScopedResource() } }
        if fm.fileExists(atPath: dest.path) { try fm.removeItem(at: dest) }
        try fm.copyItem(at: url, to: dest)
        return dest
    }

    private func flash(_ message: String) {
        withAnimation { banner = message }
        Task {
            try? await Task.sleep(nanoseconds: 2_500_000_000)
            withAnimation { banner = nil }
        }
    }
}

/// A photo/video picked from the library, materialised into the app's
/// sandbox with its original iOS filename. Loading the item as a *file*
/// (rather than raw `Data`) is what gives us the source filename; the
/// importing closure must copy the handed-over file synchronously because
/// the system reclaims it once the closure returns.
private struct PickedPhoto: Transferable {
    let url: URL

    static var transferRepresentation: some TransferRepresentation {
        FileRepresentation(importedContentType: .data) { received in
            let fm = FileManager.default
            let name = received.file.lastPathComponent
            let dest = try antdriveImportsDir().appendingPathComponent(name)
            if fm.fileExists(atPath: dest.path) { try fm.removeItem(at: dest) }
            try fm.copyItem(at: received.file, to: dest)
            return PickedPhoto(url: dest)
        }
    }
}

/// The sandbox directory the Rust uploader streams imports from. Shared by
/// the file importer and the photo picker so both stage files in one place.
func antdriveImportsDir() throws -> URL {
    let fm = FileManager.default
    let base = try fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                          appropriateFor: nil, create: true)
        .appendingPathComponent("antdrive/imports", isDirectory: true)
    try fm.createDirectory(at: base, withIntermediateDirectories: true)
    return base
}

/// One file row: type icon, name, status line, and a trailing control
/// that changes with state (progress ring while uploading, a menu when
/// done, a retry/cancel pair when paused or failed).
private struct FileRow: View {
    @EnvironmentObject var node: AntNode
    let job: UploadJob
    let onCopy: (UploadJob) -> Void
    let onPause: (UploadJob) -> Void
    let onResume: (UploadJob) -> Void
    let onCancel: (UploadJob) -> Void

    var body: some View {
        GlassCard(radius: 22) {
            HStack(spacing: 14) {
                Image(systemName: icon)
                    .font(.title2)
                    .foregroundStyle(.white.opacity(0.9))
                    .frame(width: 30)

                VStack(alignment: .leading, spacing: 3) {
                    Text(job.displayName)
                        .font(.body.weight(.semibold))
                        .foregroundStyle(.white)
                        .lineLimit(1)
                    Text("\(job.statusLabel) · \(formatBytes(job.sourceSize))")
                        .font(.caption)
                        .foregroundStyle(statusColor)
                    if job.isActive, let f = job.fraction {
                        ProgressView(value: f)
                            .tint(.white.opacity(0.9))
                            .padding(.top, 2)
                    }
                    if job.isDone { propagationBadge }
                }
                Spacer(minLength: 8)
                trailing
            }
        }
        // Auto-verify a completed file: reuse a fresh (<24h) disk-cached
        // verdict if we have one, otherwise probe the network once for the
        // real "retrievable" verdict (rather than just the local "Online"
        // state). Verdicts persist across launches, so a verified file
        // isn't re-probed every cold start.
        .task(id: autoVerifyKey) { await node.ensureVerified(job) }
    }

    /// Re-trigger the auto-verify when the job reaches the completed
    /// state *and* once the peer set is healthy enough for the verdict
    /// to mean something — a probe issued against a still-ramping peer
    /// set reports false "missing" and would pin an orange badge for
    /// the session. The status + peer-floor flag are part of the key so
    /// the task re-runs on those transitions.
    private var autoVerifyKey: String {
        "\(job.id)#\(job.status)#\(node.peerCount >= AntNode.verifyPeerFloor ? "peers" : "none")"
    }

    /// Read-back verdict for a completed file: a spinner while probing,
    /// then a green "Verified" or an orange "Not fully available yet"
    /// once the network has been asked. The check resolves the manifest,
    /// walks the chunk tree, and fetches every data chunk network-only —
    /// distinct from the job's "Online" status, which only means the
    /// upload was attempted locally.
    @ViewBuilder private var propagationBadge: some View {
        if node.verifying.contains(job.id) {
            Label("Verifying chunks…", systemImage: "antenna.radiowaves.left.and.right")
                .font(.caption2)
                .foregroundStyle(.white.opacity(0.6))
                .padding(.top, 1)
        } else if let p = node.propagation[job.id] {
            Label(p.label, systemImage: p.retrievable ? "checkmark.seal.fill" : "exclamationmark.triangle.fill")
                .font(.caption2.weight(.medium))
                .foregroundStyle(p.retrievable ? .green.opacity(0.9) : .orange)
                .padding(.top, 1)
        }
    }

    @ViewBuilder private var trailing: some View {
        if job.isActive {
            Button { onPause(job) } label: {
                Image(systemName: "pause.circle.fill")
                    .font(.title2).foregroundStyle(.white.opacity(0.85))
            }
        } else if job.isPaused {
            HStack(spacing: 10) {
                Button { onResume(job) } label: {
                    Image(systemName: "play.circle.fill").font(.title2).foregroundStyle(.white)
                }
                Button { onCancel(job) } label: {
                    Image(systemName: "xmark.circle.fill").font(.title2).foregroundStyle(.white.opacity(0.6))
                }
            }
        } else if job.isDone {
            Button { onCopy(job) } label: {
                Image(systemName: "doc.on.doc")
                    .font(.title3).foregroundStyle(.white.opacity(0.85))
            }
        } else if job.isFailed {
            Button { onResume(job) } label: {
                Image(systemName: "arrow.clockwise.circle.fill")
                    .font(.title2).foregroundStyle(.orange)
            }
        }
    }

    private var statusColor: Color {
        if job.isFailed { return .orange }
        if job.isDone { return .green.opacity(0.9) }
        return .white.opacity(0.6)
    }

    private var icon: String {
        let ct = job.contentType ?? ""
        if ct.hasPrefix("image") { return "photo" }
        if ct.hasPrefix("video") { return "film" }
        if ct.hasPrefix("audio") { return "music.note" }
        if ct.hasPrefix("text") { return "doc.text" }
        if ct.contains("pdf") { return "doc.richtext" }
        if ct.contains("zip") || ct.contains("compressed") { return "archivebox" }
        return "doc"
    }
}

