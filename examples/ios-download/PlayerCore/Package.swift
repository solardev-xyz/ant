// swift-tools-version: 5.9
import PackageDescription

// Pure logic extracted from the AntDownload player so the scrubber /
// seek / track-switch state machine can be unit-tested on the host with
// `swift test` — no simulator, no AVFoundation, no ant-ffi. The same
// source file is also compiled directly into the app target (see the
// Xcode project's Sources build phase), so the tests cover the exact code
// the app runs.
let package = Package(
    name: "PlayerCore",
    products: [
        .library(name: "PlayerCore", targets: ["PlayerCore"])
    ],
    targets: [
        .target(name: "PlayerCore"),
        .testTarget(name: "PlayerCoreTests", dependencies: ["PlayerCore"])
    ]
)
