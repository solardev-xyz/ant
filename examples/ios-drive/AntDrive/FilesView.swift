import PhotosUI
import SwiftUI
import UniformTypeIdentifiers

/// The Files tab — a Dropbox-style list of everything the user has
/// uploaded, with an Add menu (upload a file / add from a share link)
/// and per-file actions (share link, pause/resume/cancel, retry).
struct FilesView: View {
    @EnvironmentObject var node: AntNode

    @State private var showImporter = false
    @State private var showPhotoPicker = false
    @State private var showLinkSheet = false
    @State private var showGetStarted = false
    @State private var photoSelection: [PhotosPickerItem] = []
    @State private var linkText = ""
    @State private var banner: String?
    @State private var shareItem: ShareItem?

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
                            FileRow(job: job, onShare: share, onPause: pause,
                                    onResume: resume, onCancel: cancel)
                        }
                    }
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)
                .padding(.bottom, 130)
            }
            .scrollIndicators(.hidden)
            .refreshable { await node.refreshAll() }
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { bannerView }
        .overlay(alignment: .bottomTrailing) { addButton }
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
        .sheet(item: $shareItem) { item in ShareSheet(text: item.link) }
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
    /// Started greeter is the only call to action.
    @ViewBuilder private var addButton: some View {
        if node.hasStorage {
            Menu {
                Button { showPhotoPicker = true } label: {
                    Label("Upload photos", systemImage: "photo")
                }
                Button { showImporter = true } label: {
                    Label("Upload a file", systemImage: "arrow.up.doc")
                }
                Button { showLinkSheet = true } label: {
                    Label("Add from link", systemImage: "link")
                }
            } label: {
                Image(systemName: "plus")
                    .font(.title2.weight(.semibold))
                    .foregroundStyle(.white)
                    .frame(width: 60, height: 60)
                    .glassEffect(.regular.tint(.blue.opacity(0.5)), in: .circle)
            }
            .padding(.trailing, 22)
            .padding(.bottom, 110)
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

    private func share(_ job: UploadJob) {
        guard let ref = job.reference else { return }
        let hex = ref.hasPrefix("0x") ? String(ref.dropFirst(2)) : ref
        shareItem = ShareItem(link: "bzz://\(hex)")
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

    /// Load each picked photo/video's bytes, stage them in the sandbox,
    /// and kick off an upload. `PhotosPickerItem` hands us out-of-process
    /// data (no photo-library permission prompt), so we materialise a real
    /// file the Rust uploader can mmap.
    private func handlePhotos(_ items: [PhotosPickerItem]) {
        guard !items.isEmpty else { return }
        photoSelection = []
        Task {
            for (index, item) in items.enumerated() {
                do {
                    guard let data = try await item.loadTransferable(type: Data.self) else {
                        flash("Couldn't read selected item")
                        continue
                    }
                    let type = item.supportedContentTypes.first
                    let ext = type?.preferredFilenameExtension ?? "dat"
                    let ct = type?.preferredMIMEType
                    let stamp = Int(Date().timeIntervalSince1970)
                    let name = "Photo-\(stamp)-\(index + 1).\(ext)"
                    let local = try writeIntoSandbox(data: data, name: name)
                    try await node.startUpload(path: local, name: name, contentType: ct)
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
        let base = try fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                              appropriateFor: nil, create: true)
            .appendingPathComponent("antdrive/imports", isDirectory: true)
        try fm.createDirectory(at: base, withIntermediateDirectories: true)
        let dest = base.appendingPathComponent(url.lastPathComponent)
        let scoped = url.startAccessingSecurityScopedResource()
        defer { if scoped { url.stopAccessingSecurityScopedResource() } }
        if fm.fileExists(atPath: dest.path) { try fm.removeItem(at: dest) }
        try fm.copyItem(at: url, to: dest)
        return dest
    }

    /// Write in-memory bytes (from the photo picker) to the same imports
    /// directory so the uploader has a stable path to stream from.
    private func writeIntoSandbox(data: Data, name: String) throws -> URL {
        let fm = FileManager.default
        let base = try fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                              appropriateFor: nil, create: true)
            .appendingPathComponent("antdrive/imports", isDirectory: true)
        try fm.createDirectory(at: base, withIntermediateDirectories: true)
        let dest = base.appendingPathComponent(name)
        try data.write(to: dest, options: .atomic)
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

private struct ShareItem: Identifiable {
    let id = UUID()
    let link: String
}

/// One file row: type icon, name, status line, and a trailing control
/// that changes with state (progress ring while uploading, a menu when
/// done, a retry/cancel pair when paused or failed).
private struct FileRow: View {
    let job: UploadJob
    let onShare: (UploadJob) -> Void
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
                }
                Spacer(minLength: 8)
                trailing
            }
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
            Menu {
                Button { onShare(job) } label: { Label("Copy share link", systemImage: "link") }
            } label: {
                Image(systemName: "square.and.arrow.up")
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

/// Thin UIActivityViewController wrapper so "Copy share link" can hand
/// the bzz link to the system share sheet.
struct ShareSheet: UIViewControllerRepresentable {
    let text: String
    func makeUIViewController(context: Context) -> UIActivityViewController {
        UIActivityViewController(activityItems: [text], applicationActivities: nil)
    }
    func updateUIViewController(_ controller: UIActivityViewController, context: Context) {}
}
