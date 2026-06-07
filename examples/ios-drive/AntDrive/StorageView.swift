import SwiftUI

/// The Storage tab — the account side of AntDrive: how much room the
/// user has (storage plan), the account that pays for it, and the
/// account key to back up. Deliberately free of Swarm jargon: a postage
/// batch is a "storage plan", the node EOA is "your account", the
/// signing key is the "account key".
struct StorageView: View {
    @EnvironmentObject var node: AntNode

    @AppStorage("gnosisRpc") private var rpc = ""
    @State private var showConnect = false
    @State private var showKey = false
    @State private var exportedKey: String?
    @State private var banner: String?
    @State private var busy = false

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .network)

            ScrollView {
                VStack(spacing: 18) {
                    header
                    meterCard
                    if node.plan?.enabled == true {
                        planCard
                    } else {
                        connectCard
                    }
                    accountCard
                    accountKeyCard
                    advancedCard
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
        .sheet(isPresented: $showConnect) { connectSheet }
        .sheet(isPresented: $showKey) { keySheet }
        .task { await node.refreshAll() }
    }

    // MARK: header

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text("Storage")
                    .font(.system(.largeTitle, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text("Your space and account")
                    .font(.footnote)
                    .foregroundStyle(.white.opacity(0.65))
            }
            Spacer()
        }
        .padding(.horizontal, 6)
    }

    // MARK: storage meter

    private var meterCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                Text("STORAGE")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))

                if let plan = node.plan, plan.enabled {
                    HStack(alignment: .lastTextBaseline, spacing: 6) {
                        Text(formatBytes(plan.usedBytes))
                            .font(.system(.title, design: .rounded).weight(.bold))
                            .foregroundStyle(.white)
                        Text("of \(formatBytes(plan.totalBytes)) used")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                    meterBar(fraction: plan.usedFraction, low: plan.isLow)
                    Text("\(formatBytes(plan.freeBytes)) free")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                } else {
                    Text("No storage plan")
                        .font(.system(.title2, design: .rounded).weight(.semibold))
                        .foregroundStyle(.white)
                    Text("Connect a plan to start storing files.")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                    meterBar(fraction: 0, low: false)
                }
            }
        }
    }

    private func meterBar(fraction: Double, low: Bool) -> some View {
        GeometryReader { geo in
            ZStack(alignment: .leading) {
                Capsule().fill(.white.opacity(0.15))
                Capsule()
                    .fill(low ? Color.orange : Color.green)
                    .frame(width: max(6, geo.size.width * fraction))
            }
        }
        .frame(height: 12)
    }

    // MARK: plan

    private var planCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    Label(planStatusText, systemImage: planStatusIcon)
                        .font(.headline)
                        .foregroundStyle(.white)
                    Spacer()
                }
                if let plan = node.plan {
                    Text(plan.immutable ? "Permanent storage" : "Renewable storage")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                }
                HStack(spacing: 12) {
                    pillButton("Add space", icon: "plus.circle") {
                        flash("Adding space needs an on-chain top-up — coming soon")
                    }
                    pillButton("Renew", icon: "arrow.clockwise") {
                        flash("Renew needs an on-chain top-up — coming soon")
                    }
                }
            }
        }
    }

    private var planStatusText: String {
        guard let plan = node.plan else { return "Active" }
        return plan.isLow ? "Running low" : "Active"
    }
    private var planStatusIcon: String {
        (node.plan?.isLow ?? false) ? "exclamationmark.triangle.fill" : "checkmark.seal.fill"
    }

    // MARK: connect (no plan)

    private var connectCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Set up storage")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text("Already have a storage plan on this account? Connect it to start uploading.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                pillButton("Connect storage", icon: "link") { showConnect = true }
            }
        }
    }

    // MARK: account

    private var accountCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("ACCOUNT")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                HStack {
                    Image(systemName: "person.crop.circle.fill")
                        .font(.title)
                        .foregroundStyle(.white.opacity(0.85))
                    VStack(alignment: .leading, spacing: 2) {
                        Text(node.account?.shortAddress ?? "—")
                            .font(.body.weight(.semibold).monospaced())
                            .foregroundStyle(.white)
                        Text("Used to pay for storage")
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.6))
                    }
                    Spacer()
                    if let addr = node.account?.ethAddress {
                        Button { copy(addr); flash("Address copied") } label: {
                            Image(systemName: "doc.on.doc").foregroundStyle(.white.opacity(0.85))
                        }
                    }
                }
                pillButton("Add funds", icon: "plus") {
                    if let addr = node.account?.ethAddress { copy(addr) }
                    flash("Send xDAI + xBZZ to your account address (copied)")
                }
            }
        }
    }

    private var accountKeyCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Account key", systemImage: "key.fill")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text("Your account key keeps your files yours. Back it up somewhere safe — anyone with it controls this account.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                pillButton("Back up account key", icon: "lock.shield") { revealKey() }
            }
        }
    }

    private var advancedCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text("CONNECTION")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                row("Status", node.status.label)
                row("Connections", "\(node.peerCount)")
                if let agent = node.account?.agent { row("Version", agent) }
            }
        }
    }

    private func row(_ label: String, _ value: String) -> some View {
        HStack {
            Text(label).font(.subheadline).foregroundStyle(.white.opacity(0.65))
            Spacer()
            Text(value).font(.subheadline.weight(.medium).monospaced())
                .foregroundStyle(.white).lineLimit(1).truncationMode(.middle)
        }
    }

    // MARK: sheets

    private var connectSheet: some View {
        NavigationStack {
            Form {
                Section("Gnosis RPC URL") {
                    TextField("https://rpc.gnosischain.com", text: $rpc)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .keyboardType(.URL)
                }
                Section {
                    Button {
                        Task { await discover() }
                    } label: {
                        if busy { ProgressView() } else { Text("Find my storage automatically") }
                    }
                    .disabled(busy || rpc.isEmpty)
                } footer: {
                    Text("Searches the chain for any storage plan this account owns. May take a moment.")
                }
            }
            .navigationTitle("Connect storage")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { showConnect = false }
                }
            }
        }
        .presentationDetents([.medium])
    }

    private var keySheet: some View {
        NavigationStack {
            VStack(alignment: .leading, spacing: 16) {
                Label("Keep this secret", systemImage: "exclamationmark.triangle.fill")
                    .font(.headline)
                    .foregroundStyle(.orange)
                Text("This is your account key. Store it in a password manager. Never share it.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Text(exportedKey ?? "—")
                    .font(.footnote.monospaced())
                    .textSelection(.enabled)
                    .padding()
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(.quaternary, in: .rect(cornerRadius: 12))
                Button {
                    if let k = exportedKey { copy(k); flash("Account key copied") }
                } label: {
                    Label("Copy key", systemImage: "doc.on.doc")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                Spacer()
            }
            .padding()
            .navigationTitle("Account key")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { showKey = false }
                }
            }
        }
        .presentationDetents([.medium])
    }

    @ViewBuilder private var bannerView: some View {
        if let banner {
            Text(banner)
                .font(.footnote.weight(.medium))
                .foregroundStyle(.white)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 18).padding(.vertical, 12)
                .glassEffect(.regular.tint(.black.opacity(0.4)), in: .capsule)
                .padding(.top, 8).padding(.horizontal, 24)
                .transition(.move(edge: .top).combined(with: .opacity))
        }
    }

    // MARK: helpers

    private func pillButton(_ title: String, icon: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Label(title, systemImage: icon)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.white)
                .padding(.horizontal, 16).padding(.vertical, 10)
                .glassEffect(.regular.tint(.white.opacity(0.12)), in: .capsule)
        }
    }

    private func discover() async {
        busy = true
        defer { busy = false }
        do {
            try await node.discoverStorage(rpc: rpc)
            showConnect = false
            if node.plan?.enabled == true {
                flash("Storage connected")
            } else {
                flash("No storage plan found for this account")
            }
        } catch {
            flash(error.localizedDescription)
        }
    }

    private func revealKey() {
        Task {
            do {
                exportedKey = try await node.exportKey()
                showKey = true
            } catch {
                flash(error.localizedDescription)
            }
        }
    }

    private func copy(_ s: String) { UIPasteboard.general.string = s }

    private func flash(_ message: String) {
        withAnimation { banner = message }
        Task {
            try? await Task.sleep(nanoseconds: 3_000_000_000)
            withAnimation { banner = nil }
        }
    }
}
