import CoreImage.CIFilterBuiltins
import SwiftUI

/// Shared visual primitives for the Liquid Glass UI.
///
/// iOS 26's `.glassEffect(...)` is the only thing that actually does
/// the lensing / specular work; everything in this file just composes
/// it with the colorful blurred orbs that give the glass something
/// interesting to refract.

/// Multi-orb gradient backdrop. Two palettes — Player and Network —
/// drawn behind the tab content so the glass cards on top get their
/// colour from somewhere. Each orb is a `RadialGradient` over a soft
/// circle, blurred heavily to produce the diffuse colour fields you
/// see in iOS 26's Apple-provided sample apps.
struct LiquidGlassBackground: View {
    enum Palette {
        case player
        case network
    }

    let palette: Palette

    var body: some View {
        GeometryReader { geo in
            ZStack {
                Color.black.ignoresSafeArea()

                ForEach(orbs(for: geo.size).indices, id: \.self) { idx in
                    let orb = orbs(for: geo.size)[idx]
                    Circle()
                        .fill(
                            RadialGradient(
                                colors: [orb.color.opacity(0.85), orb.color.opacity(0.0)],
                                center: .center,
                                startRadius: 0,
                                endRadius: orb.radius
                            )
                        )
                        .frame(width: orb.radius * 2, height: orb.radius * 2)
                        .position(orb.center)
                        .blur(radius: orb.blur)
                }
            }
            .ignoresSafeArea()
        }
    }

    private struct Orb {
        let color: Color
        let center: CGPoint
        let radius: CGFloat
        let blur: CGFloat
    }

    private func orbs(for size: CGSize) -> [Orb] {
        let w = size.width
        let h = size.height
        switch palette {
        case .player:
            return [
                Orb(
                    color: Color(red: 0.95, green: 0.45, blue: 0.20),
                    center: CGPoint(x: w * 0.10, y: h * 0.05),
                    radius: max(w, h) * 0.55,
                    blur: 60
                ),
                Orb(
                    color: Color(red: 0.85, green: 0.20, blue: 0.40),
                    center: CGPoint(x: w * 0.95, y: h * 0.18),
                    radius: max(w, h) * 0.40,
                    blur: 70
                ),
                Orb(
                    color: Color(red: 0.50, green: 0.30, blue: 0.95),
                    center: CGPoint(x: w * 0.20, y: h * 0.55),
                    radius: max(w, h) * 0.55,
                    blur: 80
                ),
                Orb(
                    color: Color(red: 0.95, green: 0.55, blue: 0.65),
                    center: CGPoint(x: w * 0.90, y: h * 0.78),
                    radius: max(w, h) * 0.45,
                    blur: 80
                ),
            ]
        case .network:
            return [
                Orb(
                    color: Color(red: 0.20, green: 0.55, blue: 0.95),
                    center: CGPoint(x: w * 0.15, y: h * 0.10),
                    radius: max(w, h) * 0.45,
                    blur: 70
                ),
                Orb(
                    color: Color(red: 0.30, green: 0.85, blue: 0.55),
                    center: CGPoint(x: w * 0.10, y: h * 0.65),
                    radius: max(w, h) * 0.55,
                    blur: 80
                ),
                Orb(
                    color: Color(red: 0.95, green: 0.55, blue: 0.20),
                    center: CGPoint(x: w * 0.95, y: h * 0.85),
                    radius: max(w, h) * 0.40,
                    blur: 80
                ),
                Orb(
                    color: Color(red: 0.55, green: 0.40, blue: 0.90),
                    center: CGPoint(x: w * 0.85, y: h * 0.30),
                    radius: max(w, h) * 0.40,
                    blur: 80
                ),
            ]
        }
    }
}

/// Helper: render `content` inside a Liquid Glass capsule pill — the
/// shape used for the header rows in the screenshot.
struct GlassPill<Content: View>: View {
    @ViewBuilder var content: Content

    var body: some View {
        content
            .padding(.horizontal, 22)
            .padding(.vertical, 14)
            .frame(maxWidth: .infinity, alignment: .leading)
            .glassEffect(.regular, in: .capsule)
    }
}

/// Helper: render `content` inside a rounded glass card. Default radius
/// is 28 to match the iOS 26 sample apps' card shape.
struct GlassCard<Content: View>: View {
    var radius: CGFloat = 28
    @ViewBuilder var content: Content

    var body: some View {
        content
            .padding(20)
            .frame(maxWidth: .infinity, alignment: .leading)
            .glassEffect(.regular, in: .rect(cornerRadius: radius))
    }
}

/// A QR code for a payment request, rendered on a white tile so any
/// wallet camera can pick it up against the dark glass UI. With an
/// `xdaiAmount` it encodes an EIP-681 URI
/// (`ethereum:<address>@100?value=<wei>`) so a scanning wallet
/// pre-fills the recipient, the amount, and the Gnosis chain; without
/// one it falls back to the plain `0x…` address. The address is always
/// shown as copyable text next to the code for wallets that don't
/// parse the URI form.
struct AddressQRCode: View {
    let address: String
    /// Decimal xDAI amount to request (e.g. "0.15"); nil for address-only.
    var xdaiAmount: String? = nil
    var size: CGFloat = 160

    var body: some View {
        if let image = Self.generate(payload) {
            Image(uiImage: image)
                .interpolation(.none)
                .resizable()
                .scaledToFit()
                .frame(width: size, height: size)
                .padding(10)
                .background(.white, in: .rect(cornerRadius: 12))
        }
    }

    private var payload: String {
        guard let xdaiAmount, let wei = Self.weiString(fromDecimal: xdaiAmount) else {
            return address
        }
        // 100 is the Gnosis chain id; `value` is the native-token amount
        // in wei, per EIP-681.
        return "ethereum:\(address)@100?value=\(wei)"
    }

    /// Convert a decimal token amount ("0.15") to an exact integer wei
    /// string (18 decimals) without floating-point round-off. Returns
    /// nil for malformed input or a zero amount (a zero-value request
    /// would just confuse the wallet).
    static func weiString(fromDecimal s: String) -> String? {
        let parts = s.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count <= 2, !parts.isEmpty else { return nil }
        let whole = parts[0].isEmpty ? "0" : String(parts[0])
        var frac = parts.count == 2 ? String(parts[1]) : ""
        guard whole.allSatisfy(\.isNumber), frac.allSatisfy(\.isNumber),
              frac.count <= 18 else { return nil }
        frac += String(repeating: "0", count: 18 - frac.count)
        let combined = (whole + frac).drop { $0 == "0" }
        return combined.isEmpty ? nil : String(combined)
    }

    private static func generate(_ text: String) -> UIImage? {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(text.utf8)
        filter.correctionLevel = "M"
        guard let output = filter.outputImage else { return nil }
        // Scale the tiny module grid up before rasterising so the PNG is
        // crisp; `.interpolation(.none)` keeps the edges sharp on screen.
        let scaled = output.transformed(by: CGAffineTransform(scaleX: 10, y: 10))
        guard let cg = CIContext().createCGImage(scaled, from: scaled.extent) else { return nil }
        return UIImage(cgImage: cg)
    }
}

/// Format a byte count using Apple's `.file` style. Wrapped here so
/// every call site is consistent.
func formatBytes(_ count: UInt64) -> String {
    ByteCountFormatter.string(fromByteCount: Int64(min(count, UInt64(Int64.max))), countStyle: .file)
}

func formatBytesPerSec(_ bps: Double) -> String {
    if bps < 1 { return "—" }
    return "\(formatBytes(UInt64(bps)))/s"
}

/// Format a Unix timestamp (seconds) as a short local date + time, e.g.
/// "19 Jun 2026 at 14:32". Used by the file detail page.
func formatUnixDate(_ unix: UInt64) -> String {
    guard unix > 0 else { return "—" }
    let date = Date(timeIntervalSince1970: TimeInterval(unix))
    let f = DateFormatter()
    f.dateStyle = .medium
    f.timeStyle = .short
    return f.string(from: date)
}

func formatDuration(_ seconds: TimeInterval) -> String {
    guard seconds.isFinite, !seconds.isNaN, seconds >= 0 else { return "0:00" }
    let total = Int(seconds.rounded())
    let m = total / 60
    let s = total % 60
    return String(format: "%d:%02d", m, s)
}

func formatEta(remainingBytes: UInt64, bytesPerSec: Double) -> String {
    guard bytesPerSec > 1 else { return "—" }
    let seconds = Double(remainingBytes) / bytesPerSec
    if seconds.isInfinite || seconds.isNaN || seconds > 60 * 60 * 24 { return "—" }
    if seconds < 60 { return "\(Int(seconds.rounded())) s" }
    let m = Int(seconds / 60)
    let s = Int(seconds.truncatingRemainder(dividingBy: 60))
    return "\(m) m \(s) s"
}
