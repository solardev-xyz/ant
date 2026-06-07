import SwiftUI

/// The Get Started flow: a guided sheet that welcomes the user, lets
/// them pick a storage plan, shows the payment information for that plan
/// (cost vs. their account balance + where to send funds), and activates
/// the storage on-chain. Deliberately Dropbox-flavoured: "plans", not
/// "postage batches"; "activate", not "createBatch".
struct GetStartedView: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss
    @AppStorage("gnosisRpc") private var rpc = ""

    private let defaultRpc = "https://rpc.gnosischain.com"

    private enum Step { case plan, payment, activating }

    @State private var step: Step = .plan
    @State private var selected: StoragePlanTier?
    @State private var quote: StorageQuote?
    @State private var quotes: [String: StorageQuote] = [:]
    @State private var usdRate: Double?
    @State private var loadingQuotes = false
    @State private var loadingQuote = false
    @State private var error: String?

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .network).ignoresSafeArea()
                content
            }
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    if step == .payment {
                        Button("Back") { withAnimation { step = .plan; quote = nil } }
                    } else if step == .plan {
                        Button("Close") { dismiss() }
                    }
                }
            }
        }
        .preferredColorScheme(.dark)
        .interactiveDismissDisabled(step == .activating)
        .task { await loadPlans() }
    }

    @ViewBuilder private var content: some View {
        switch step {
        case .plan: planStep
        case .payment: paymentStep
        case .activating: activatingStep
        }
    }

    // MARK: Step 1 — welcome + plan picker

    private var planStep: some View {
        ScrollView {
            VStack(spacing: 18) {
                VStack(spacing: 8) {
                    Image(systemName: "externaldrive.fill.badge.plus")
                        .font(.system(size: 50))
                        .foregroundStyle(.white)
                    Text("Welcome to AntDrive")
                        .font(.system(.title, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                    Text("Pick a storage plan to start keeping your files safe on the decentralized network.")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 12)

                ForEach(StoragePlanTier.all) { tier in
                    planCard(tier)
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

    private func planCard(_ tier: StoragePlanTier) -> some View {
        Button {
            choose(tier)
        } label: {
            GlassCard {
                HStack(spacing: 14) {
                    VStack(alignment: .leading, spacing: 4) {
                        HStack(spacing: 8) {
                            Text(tier.title)
                                .font(.title3.weight(.bold))
                                .foregroundStyle(.white)
                            if tier.recommended {
                                Text("POPULAR")
                                    .font(.caption2.weight(.bold))
                                    .foregroundStyle(.black)
                                    .padding(.horizontal, 8).padding(.vertical, 3)
                                    .background(.green, in: .capsule)
                            }
                        }
                        Text(tier.subtitle)
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                        Text("\(formatBytes(tier.capacityBytes)) · \(tier.durationLabel)")
                            .font(.caption.weight(.medium))
                            .foregroundStyle(.white.opacity(0.85))
                    }
                    Spacer()
                    priceColumn(for: tier)
                }
            }
        }
        .buttonStyle(.plain)
        .disabled(loadingQuote)
    }

    @ViewBuilder private func priceColumn(for tier: StoragePlanTier) -> some View {
        if let q = quotes[tier.id] {
            VStack(alignment: .trailing, spacing: 2) {
                Text(usdString(forBzz: q.totalCostBzz) ?? "\(q.totalCostBzz) xBZZ")
                    .font(.headline.weight(.bold))
                    .foregroundStyle(.white)
                if usdRate != nil {
                    Text("\(q.totalCostBzz) xBZZ")
                        .font(.caption2)
                        .foregroundStyle(.white.opacity(0.6))
                }
            }
        } else if loadingQuotes {
            ProgressView().tint(.white)
        } else {
            Image(systemName: "chevron.right").foregroundStyle(.white.opacity(0.6))
        }
    }

    // MARK: Step 2 — payment information

    @ViewBuilder private var paymentStep: some View {
        ScrollView {
            VStack(spacing: 18) {
                if let tier = selected, let quote {
                    VStack(spacing: 6) {
                        Text(tier.title)
                            .font(.system(.title2, design: .rounded).weight(.bold))
                            .foregroundStyle(.white)
                        Text("\(formatBytes(quote.capacityBytes)) for \(tier.durationLabel)")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                    .padding(.top, 8)

                    GlassCard {
                        VStack(spacing: 14) {
                            HStack(alignment: .firstTextBaseline) {
                                Text("Price")
                                    .font(.subheadline)
                                    .foregroundStyle(.white.opacity(0.7))
                                Spacer()
                                VStack(alignment: .trailing, spacing: 2) {
                                    Text(usdString(forBzz: quote.totalCostBzz) ?? "\(quote.totalCostBzz) xBZZ")
                                        .font(.title3.weight(.bold))
                                        .foregroundStyle(.white)
                                    if usdRate != nil {
                                        Text("\(quote.totalCostBzz) xBZZ")
                                            .font(.caption)
                                            .foregroundStyle(.white.opacity(0.6))
                                    }
                                }
                            }
                            Divider().overlay(.white.opacity(0.2))
                            payRow("Pay with", "\(quote.xdaiRequiredDisplay) xDAI")
                            payRow("Your xDAI", "\(quote.accountXdaiDisplay) xDAI",
                                   tint: quote.sufficientFunds ? .white : .orange)
                            payRow("Status", quote.sufficientFunds ? "Ready to activate" : "Needs funds",
                                   tint: quote.sufficientFunds ? .green : .orange)
                        }
                    }

                    if !quote.sufficientFunds {
                        fundingCard(quote)
                    }

                    Button { activate() } label: {
                        Text(quote.sufficientFunds ? "Activate storage" : "I've sent xDAI — Activate")
                            .font(.headline)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .glassEffect(.regular.tint(.blue.opacity(0.6)), in: .capsule)
                            .foregroundStyle(.white)
                    }
                    .buttonStyle(.plain)

                    Button("Refresh balance") { reprice() }
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.8))

                    Text("Activating swaps your xDAI to storage credit and registers your plan on the Gnosis network. This can take a minute.")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.55))
                        .multilineTextAlignment(.center)

                    if let error {
                        Text(error)
                            .font(.footnote)
                            .foregroundStyle(.orange)
                            .multilineTextAlignment(.center)
                    }
                }
            }
            .padding(20)
        }
    }

    private func fundingCard(_ quote: StorageQuote) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Add funds to activate", systemImage: "creditcard")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text("Send this much xDAI to your account address on the Gnosis network. AntDrive converts it to storage credit and activates your plan — no other tokens needed.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))

                sendRow(amount: "\(quote.xdaiToSendDisplay) xDAI",
                        usd: usdFromXdai(quote.xdaiToSendDisplay),
                        note: "covers the swap to xBZZ + network fees")

                if let addr = node.account?.ethAddress {
                    Text("Send to your account address")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.white.opacity(0.6))
                        .padding(.top, 2)
                    HStack {
                        Text(addr)
                            .font(.caption.monospaced())
                            .foregroundStyle(.white)
                            .lineLimit(1).truncationMode(.middle)
                        Spacer()
                        Button {
                            UIPasteboard.general.string = addr
                        } label: {
                            Image(systemName: "doc.on.doc").foregroundStyle(.white.opacity(0.85))
                        }
                    }
                }
            }
        }
    }

    /// One "send X — for Y" line in the funding instructions.
    private func sendRow(amount: String, usd: String?, note: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            Image(systemName: "arrow.up.circle.fill")
                .foregroundStyle(.blue)
            VStack(alignment: .leading, spacing: 1) {
                HStack(spacing: 6) {
                    Text(amount)
                        .font(.subheadline.weight(.bold))
                        .foregroundStyle(.white)
                    if let usd {
                        Text("(≈\(usd))")
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.6))
                    }
                }
                Text(note)
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))
            }
            Spacer()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
        .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
    }

    /// xDAI is a USD stablecoin (~$1), so its USD value is ~1:1.
    private func usdFromXdai(_ xdai: String) -> String? {
        guard let amount = Double(xdai) else { return nil }
        return PriceOracle.formatUsd(amount)
    }

    private func payRow(_ label: String, _ value: String, strong: Bool = false, tint: Color = .white) -> some View {
        HStack {
            Text(label)
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.7))
            Spacer()
            Text(value)
                .font(strong ? .title3.weight(.bold) : .subheadline.weight(.semibold))
                .foregroundStyle(tint)
        }
    }

    // MARK: Step 3 — activating

    private var activatingStep: some View {
        VStack(spacing: 18) {
            ProgressView().controlSize(.large).tint(.white)
            Text("Activating your storage…")
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
        return r.isEmpty ? defaultRpc : r
    }

    /// Fetch the USD rate + a quote for every plan so the picker can show
    /// each plan's price up front. Best-effort per plan.
    private func loadPlans() async {
        if rpc.trimmingCharacters(in: .whitespaces).isEmpty { rpc = defaultRpc }
        loadingQuotes = true
        defer { loadingQuotes = false }
        let rpcURL = activeRpc
        let drive = node
        async let rate = PriceOracle.bzzUsd()
        await withTaskGroup(of: (String, StorageQuote?).self) { group in
            for tier in StoragePlanTier.all {
                group.addTask {
                    let q = try? await drive.quoteStorage(rpc: rpcURL, depth: tier.depth, days: tier.days)
                    return (tier.id, q)
                }
            }
            for await (id, q) in group {
                if let q { quotes[id] = q }
            }
        }
        usdRate = await rate
        if quotes.isEmpty {
            error = "Couldn't reach the network to price plans. Pull to retry."
        }
    }

    /// USD string for an xBZZ decimal string, or nil if no rate yet.
    private func usdString(forBzz bzz: String) -> String? {
        guard let rate = usdRate, let amount = Double(bzz) else { return nil }
        return PriceOracle.formatUsd(amount * rate)
    }

    private func choose(_ tier: StoragePlanTier) {
        selected = tier
        error = nil
        if rpc.trimmingCharacters(in: .whitespaces).isEmpty { rpc = defaultRpc }
        // Use the price already fetched for the picker when we have it;
        // otherwise fetch it now.
        if let cached = quotes[tier.id] {
            quote = cached
            withAnimation { step = .payment }
        } else {
            reprice()
        }
    }

    private func reprice() {
        guard let tier = selected else { return }
        loadingQuote = true
        error = nil
        Task {
            defer { loadingQuote = false }
            do {
                let q = try await node.quoteStorage(rpc: activeRpc, depth: tier.depth, days: tier.days)
                quote = q
                quotes[tier.id] = q
                if usdRate == nil { usdRate = await PriceOracle.bzzUsd() }
                withAnimation { step = .payment }
            } catch {
                self.error = error.localizedDescription
            }
        }
    }

    private func activate() {
        guard let tier = selected, let quote else { return }
        error = nil
        withAnimation { step = .activating }
        Task {
            do {
                try await node.buyStorage(rpc: activeRpc, depth: tier.depth,
                                          amountPerChunk: quote.amountPerChunk,
                                          immutable: false)
                if node.hasStorage {
                    dismiss()
                } else {
                    error = "Storage didn't activate — please try again."
                    withAnimation { step = .payment }
                }
            } catch {
                self.error = error.localizedDescription
                withAnimation { step = .payment }
            }
        }
    }
}
