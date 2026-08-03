import SwiftUI

/// The Storage tab — the account side of AntStream: how much room the
/// broadcast has (storage plan), the account that pays for it, and where
/// the account key is kept. Ported from `examples/ios-drive`, with the
/// account-key card rewritten around ``AccountKeystore`` (the key lives in
/// the Keychain / Secure Enclave, not in a file).
///
/// Deliberately free of Swarm jargon: a postage batch is a "storage plan",
/// the node EOA is "your account", the signing key is the "account key".
struct StorageView: View {
    @EnvironmentObject var node: AntNode
    @StateObject private var banner = BannerState()

    @AppStorage("gnosisRpc") private var rpc = ""
    @State private var showConnect = false
    @State private var showExtend = false
    @State private var showKey = false
    @State private var showRestore = false
    @State private var exportedKey: String?
    @State private var busy = false

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .network)

            ScrollView {
                VStack(spacing: 18) {
                    header
                    meterCard
                    if node.plan?.enabled == true {
                        if node.settlement?.enabled == false {
                            settlementWarningCard
                        }
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
            .refreshable {
                await node.refreshAll()
                await node.refreshValidity(rpc: rpc)
            }
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { BannerView(message: banner.message) }
        .sheet(isPresented: $showConnect) { connectSheet }
        .sheet(isPresented: $showExtend) {
            ExtendStorageSheet { message in
                banner.flash(message)
                Task { await node.refreshValidity(rpc: rpc) }
            }
        }
        .sheet(isPresented: $showKey, onDismiss: { exportedKey = nil }) { keySheet }
        .sheet(isPresented: $showRestore) {
            RestoreAccountSheet { message in banner.flash(message) }
        }
        .task {
            await node.refreshAll()
            await node.refreshValidity(rpc: rpc)
        }
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
                    if let v = node.validity, v.enabled {
                        // How long the plan stays valid before the postage
                        // batch expires (derived from on-chain balance).
                        Label {
                            Text("Valid for \(v.durationLabel) · until \(v.expiryDateLabel)")
                        } icon: {
                            Image(systemName: "clock")
                        }
                        .font(.caption)
                        .foregroundStyle(v.remainingSeconds == 0 ? .orange : .white.opacity(0.6))
                    }
                    GlassPillButton(title: "Extend storage", icon: "clock.arrow.circlepath") {
                        showExtend = true
                    }
                } else {
                    Text("No storage plan")
                        .font(.system(.title2, design: .rounded).weight(.semibold))
                        .foregroundStyle(.white)
                    Text("Connect a plan to start broadcasting.")
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

    // MARK: settlement warning

    /// Shown when a plan is connected but outbound settlement is off:
    /// segments will stamp and look published locally but won't reach the
    /// network until the one-time chequebook setup runs, which needs a
    /// little xDAI for gas. Re-running "Find my storage" retries it.
    private var settlementWarningCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Broadcasts won't reach the network yet",
                      systemImage: "exclamationmark.triangle.fill")
                    .font(.headline)
                    .foregroundStyle(.orange)
                Text("Network settlement isn't set up. There's a one-time setup that needs a little xDAI in your account for gas. Add a small amount of xDAI, then run setup again.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
                HStack(spacing: 12) {
                    GlassPillButton(title: "Add xDAI", icon: "plus") {
                        if let addr = node.account?.ethAddress {
                            UIPasteboard.general.string = addr
                        }
                        banner.flash("Send a little xDAI to your account (copied)")
                    }
                    GlassPillButton(title: "Run setup", icon: "arrow.clockwise") {
                        showConnect = true
                    }
                }
            }
        }
    }

    // MARK: connect (no plan)

    private var connectCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Set up storage")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text("Already have a storage plan on this account? Connect it to start broadcasting.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                GlassPillButton(title: "Connect storage", icon: "link") { showConnect = true }
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
                        Button {
                            UIPasteboard.general.string = addr
                            banner.flash("Address copied")
                        } label: {
                            Image(systemName: "doc.on.doc").foregroundStyle(.white.opacity(0.85))
                        }
                    }
                }
            }
        }
    }

    /// Where the account key is kept, and the two things a user can do
    /// about it: back the key up by hand, or switch on iCloud Keychain so
    /// a reinstall recovers the account by itself.
    private var accountKeyCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Account key", systemImage: "key.fill")
                    .font(.headline)
                    .foregroundStyle(.white)

                HStack(spacing: 8) {
                    Image(systemName: node.keyProtection == .secureEnclave
                          ? "lock.shield.fill" : "lock.fill")
                        .foregroundStyle(.green)
                    Text(node.keyProtection?.label ?? "Not stored yet")
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(.white)
                }
                Text(protectionExplanation)
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.65))

                Toggle(isOn: iCloudBinding) {
                    Text("Recover with iCloud Keychain")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.white)
                }
                .tint(.green)

                Text("Your account key keeps your broadcasts yours. Back it up somewhere safe — anyone with it controls this account.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))

                HStack(spacing: 12) {
                    GlassPillButton(title: "Back up key", icon: "square.and.arrow.up") {
                        revealKey()
                    }
                    GlassPillButton(title: "Restore", icon: "arrow.clockwise.icloud") {
                        showRestore = true
                    }
                }
            }
        }
    }

    private var protectionExplanation: String {
        switch node.keyProtection {
        case .secureEnclave:
            return "Encrypted to a key that never leaves this device's Secure Enclave. A reinstall on another device needs your backed-up key."
        case .deviceOnly:
            return "Stored in this device's Keychain. A reinstall on another device needs your backed-up key."
        case .iCloudKeychain:
            return "Synced through iCloud Keychain, so a reinstall or a new device recovers this account automatically."
        case nil:
            return "The account key is created on first launch and never written to the app's files."
        }
    }

    /// Toggling this re-stores the key under the other protection — read
    /// through the old one first, so the account survives either way.
    private var iCloudBinding: Binding<Bool> {
        Binding(
            get: { node.keyProtection?.syncsToICloud == true },
            set: { enabled in
                do {
                    try node.setICloudBackup(enabled)
                    banner.flash(enabled
                                 ? "Account key will recover from iCloud Keychain"
                                 : "Account key is now device-only")
                } catch {
                    banner.flash(error.localizedDescription)
                }
            }
        )
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
                row("Gateway", node.gatewayUp ? AntNode.gatewayAddress : "not running")
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
                    TextField(AntNode.defaultRpc, text: $rpc)
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
                    Text("Searches the chain for any storage plan this account owns, and finishes the one-time settlement setup. May take a moment.")
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
                    if let k = exportedKey {
                        // Expiring, device-local copy: a private key must
                        // not linger on the clipboard or ride Universal
                        // Clipboard to the user's other devices.
                        copySecretToPasteboard(k)
                        banner.flash("Account key copied — clipboard clears in a minute")
                    }
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

    // MARK: helpers

    private func discover() async {
        busy = true
        defer { busy = false }
        do {
            try await node.discoverStorage(rpc: rpc)
            showConnect = false
            if node.plan?.enabled == true {
                banner.flash("Storage connected")
            } else {
                banner.flash("No storage plan found for this account")
            }
        } catch {
            banner.flash(error.localizedDescription)
        }
    }

    private func revealKey() {
        Task {
            do {
                exportedKey = try await node.exportKey()
                showKey = true
            } catch {
                banner.flash(error.localizedDescription)
            }
        }
    }
}

/// "Restore" — the clean recovery path when the Keychain copy is gone
/// (reinstall without iCloud Keychain, or a new device): paste the account
/// key from the backup, store it under the same protection, and restart
/// the node on the restored account. The key is validated by
/// `ant_identity_from_key` *before* anything is written, so a typo can't
/// destroy a working account.
private struct RestoreAccountSheet: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss

    /// Called on success with a banner message for the Storage tab.
    let onDone: (String) -> Void

    @State private var key = ""
    @State private var error: String?
    @State private var busy = false

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    TextField("64 hex characters", text: $key, axis: .vertical)
                        .font(.footnote.monospaced())
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .lineLimit(2...4)
                } header: {
                    Text("Account key")
                } footer: {
                    Text("Paste the key you backed up. This device's current account is replaced — back it up first if you still need it.")
                }

                if let error {
                    Section { Text(error).foregroundStyle(.orange).font(.footnote) }
                }

                Section {
                    Button {
                        Task { await restore() }
                    } label: {
                        if busy { ProgressView() } else { Text("Restore account") }
                    }
                    .disabled(busy || key.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
            }
            .navigationTitle("Restore account")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }.disabled(busy)
                }
            }
        }
        .interactiveDismissDisabled(busy)
        .presentationDetents([.medium])
    }

    private func restore() async {
        busy = true
        error = nil
        defer { busy = false }
        do {
            try await node.restoreAccount(fromKey: key)
            dismiss()
            onDone("Account restored")
        } catch {
            self.error = error.localizedDescription
        }
    }
}

/// The "Extend storage" flow: pick how much longer the connected plan
/// should last, see the all-in xDAI price for topping the batch up, pay
/// (or use existing funds), and extend on-chain. Mirrors the Get Started
/// payment flow — same quote shape, same waiting-for-payment poll and
/// auto-execute when funds land.
private struct ExtendStorageSheet: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss
    @AppStorage("gnosisRpc") private var rpc = ""

    /// Called on success with a banner message for the Storage tab.
    let onDone: (String) -> Void

    /// Durations the user can add to the plan.
    private static let options: [(id: String, label: String, days: UInt64)] = [
        ("m1", "1 month", 30),
        ("m3", "3 months", 90),
        ("m6", "6 months", 180),
        ("y1", "1 year", 365),
    ]

    private enum Step { case pick, payment, extending }

    @State private var step: Step = .pick
    @State private var selectedDays: UInt64?
    @State private var quote: StorageQuote?
    @State private var quotes: [UInt64: StorageQuote] = [:]
    @State private var loadingQuotes = false
    @State private var error: String?
    /// One-shot guard so funds-detected auto-extension fires only once
    /// per payment session (a failure must not re-trigger a loop).
    @State private var didAutoExtend = false

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .network).ignoresSafeArea()
                content
            }
            .navigationTitle("Extend storage")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    if step == .payment {
                        Button("Back") {
                            withAnimation { step = .pick; quote = nil; didAutoExtend = false }
                        }
                    } else if step == .pick {
                        Button("Close") { dismiss() }
                    }
                }
            }
        }
        .preferredColorScheme(.dark)
        .interactiveDismissDisabled(step == .extending)
        .task { await loadQuotes() }
    }

    @ViewBuilder private var content: some View {
        switch step {
        case .pick: pickStep
        case .payment: paymentStep
        case .extending: extendingStep
        }
    }

    // MARK: Step 1 — duration picker

    private var pickStep: some View {
        ScrollView {
            VStack(spacing: 18) {
                VStack(spacing: 8) {
                    Image(systemName: "clock.arrow.circlepath")
                        .font(.system(size: 44))
                        .foregroundStyle(.white)
                    Text("Keep your broadcasts longer")
                        .font(.system(.title2, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                    if let v = node.validity, v.enabled {
                        Text("Your storage is valid until \(v.expiryDateLabel). Extending adds time on top.")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                            .multilineTextAlignment(.center)
                    }
                }
                .padding(.top, 12)

                ForEach(Self.options, id: \.id) { option in
                    optionCard(label: option.label, days: option.days)
                }

                if let error {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.orange)
                        .multilineTextAlignment(.center)
                }
            }
            .padding(20)
        }
    }

    private func optionCard(label: String, days: UInt64) -> some View {
        Button {
            choose(days)
        } label: {
            GlassCard {
                HStack(spacing: 14) {
                    Text("+\(label)")
                        .font(.title3.weight(.bold))
                        .foregroundStyle(.white)
                    Spacer()
                    if let q = quotes[days] {
                        let price = PriceOracle.roundedUpXdai(q.xdaiToSendDisplay)
                        Text(PriceOracle.usdFromXdai(price) ?? "$\(price)")
                            .font(.headline.weight(.bold))
                            .foregroundStyle(.white)
                    } else if loadingQuotes {
                        ProgressView().tint(.white)
                    } else {
                        Image(systemName: "chevron.right").foregroundStyle(.white.opacity(0.6))
                    }
                }
            }
            .contentShape(.rect(cornerRadius: 28))
        }
        .buttonStyle(.plain)
    }

    // MARK: Step 2 — payment

    @ViewBuilder private var paymentStep: some View {
        if let quote {
            VStack(spacing: 22) {
                Spacer(minLength: 0)

                if quote.sufficientFunds {
                    Label("Payment received", systemImage: "checkmark.circle.fill")
                        .font(.title3.weight(.semibold))
                        .foregroundStyle(.green)
                } else {
                    sendCard(quote)
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white)
                        Text("Waiting for payment…")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                }

                Spacer(minLength: 0)

                Button { extend() } label: {
                    Text("Extend")
                        .font(.headline)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 14)
                        .glassEffect(.regular.tint(.blue.opacity(quote.sufficientFunds ? 0.6 : 0.2)),
                                     in: .capsule)
                        .foregroundStyle(.white.opacity(quote.sufficientFunds ? 1 : 0.5))
                }
                .buttonStyle(.plain)
                .disabled(!quote.sufficientFunds)

                if let error {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.orange)
                        .multilineTextAlignment(.center)
                }
            }
            .padding(24)
            .task { await pollForPayment() }
        }
    }

    private func sendCard(_ quote: StorageQuote) -> some View {
        let send = PriceOracle.roundedUpXdai(quote.xdaiToSendDisplay)
        return GlassCard {
            VStack(spacing: 16) {
                VStack(spacing: 2) {
                    Text("Send")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.white.opacity(0.6))
                    Text("\(send) xDAI")
                        .font(.system(.largeTitle, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                }
                if let addr = node.account?.ethAddress {
                    AddressQRCode(address: addr, xdaiAmount: send)
                        .frame(maxWidth: .infinity)
                    Button {
                        UIPasteboard.general.string = addr
                    } label: {
                        HStack {
                            Text(addr)
                                .font(.caption.monospaced())
                                .foregroundStyle(.white)
                                .lineLimit(1).truncationMode(.middle)
                            Spacer()
                            Image(systemName: "doc.on.doc")
                                .foregroundStyle(.white.opacity(0.85))
                        }
                        .padding(12)
                        .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }

    // MARK: Step 3 — extending

    private var extendingStep: some View {
        VStack(spacing: 18) {
            ProgressView().controlSize(.large).tint(.white)
            Text("Extending your storage…")
                .font(.title3.weight(.semibold))
                .foregroundStyle(.white)
            Text("Confirming your payment on the network. This can take up to a minute — please keep the app open.")
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.7))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
    }

    // MARK: actions

    private var activeRpc: String {
        let r = rpc.trimmingCharacters(in: .whitespaces)
        return r.isEmpty ? AntNode.defaultRpc : r
    }

    /// Fetch a quote for every duration so the picker shows each option's
    /// all-in price up front. Best-effort per option.
    private func loadQuotes() async {
        loadingQuotes = true
        defer { loadingQuotes = false }
        let rpcURL = activeRpc
        let stream = node
        await withTaskGroup(of: (UInt64, StorageQuote?).self) { group in
            for option in Self.options {
                group.addTask {
                    let q = try? await stream.quoteTopUp(rpc: rpcURL, days: option.days)
                    return (option.days, q)
                }
            }
            for await (days, q) in group {
                if let q { quotes[days] = q }
            }
        }
        if quotes.isEmpty {
            error = "Couldn't reach the network to price extensions. Try again."
        }
    }

    private func choose(_ days: UInt64) {
        selectedDays = days
        error = nil
        didAutoExtend = false
        if let cached = quotes[days] {
            quote = cached
            withAnimation { step = .payment }
        } else {
            Task {
                do {
                    let q = try await node.quoteTopUp(rpc: activeRpc, days: days)
                    quote = q
                    quotes[days] = q
                    withAnimation { step = .payment }
                } catch {
                    self.error = error.localizedDescription
                }
            }
        }
    }

    /// While the payment step is on screen and unfunded, re-quote every
    /// few seconds; when the incoming transfer lands, extend
    /// automatically — no tap needed.
    private func pollForPayment() async {
        guard let days = selectedDays else { return }
        while !Task.isCancelled {
            if quote?.sufficientFunds == true { autoExtendIfFunded(); return }
            try? await Task.sleep(nanoseconds: 6_000_000_000)
            if Task.isCancelled { return }
            guard let fresh = try? await node.quoteTopUp(rpc: activeRpc, days: days)
            else { continue }
            withAnimation {
                quote = fresh
                quotes[days] = fresh
            }
            if fresh.sufficientFunds { autoExtendIfFunded(); return }
        }
    }

    private func autoExtendIfFunded() {
        guard !didAutoExtend else { return }
        didAutoExtend = true
        extend()
    }

    private func extend() {
        guard let quote else { return }
        error = nil
        withAnimation { step = .extending }
        Task {
            do {
                try await node.topUpStorage(rpc: activeRpc, amountPerChunk: quote.amountPerChunk)
                let until = node.validity.map { " — valid until \($0.expiryDateLabel)" } ?? ""
                dismiss()
                onDone("Storage extended\(until)")
            } catch {
                self.error = error.localizedDescription
                withAnimation { step = .payment }
            }
        }
    }
}
