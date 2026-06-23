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
