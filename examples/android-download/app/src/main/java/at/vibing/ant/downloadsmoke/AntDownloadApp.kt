package at.vibing.ant.downloadsmoke

import android.app.Application

/**
 * Application-level holder for the singleton [AntNode]. The Activity
 * picks it up through `applicationContext as AntDownloadApp`; that
 * way the embedded node survives configuration changes (rotation,
 * dark-mode toggle) without re-bootstrapping libp2p.
 *
 * Mirrors the iOS pattern where `AntDownloadApp.swift` constructs
 * a single `AntNode` and passes it down through `@StateObject`. The
 * Android Application class is the closest analogue to a SwiftUI
 * `@main` `App` struct.
 */
class AntDownloadApp : Application() {
    /**
     * Created lazily on first access so unit-test runners that don't
     * want the embedded node up don't pay the JNI library-load
     * cost. `start()` is still required before any FFI call works.
     */
    val node: AntNode by lazy { AntNode() }
}
