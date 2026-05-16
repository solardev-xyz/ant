// Top-level settings for the AntDownload Android example app. See
// `examples/android-download/README.md` for context — this is the
// throwaway smoke-test app that mirrors `examples/ios-download/` on
// the Android side, embedding `crates/ant-ffi`'s JNI surface through
// `app/src/main/jniLibs/<abi>/libant_ffi.so` (populated by
// `cargo xtask build-android-*`).
//
// The Gradle build is intentionally self-contained: nothing here
// references the workspace `Cargo.toml` directly. Cross-compilation
// of the Rust slice happens out-of-band via xtask (the iOS Xcode
// project does the same in its "Build Rust" run-script phase). When
// the proper M2 Phase 4 mobile artefact (a `.aar` published from
// `ant-ffi`) lands, this whole subdirectory is expected to be
// deleted.

pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    // PREFER_SETTINGS so individual `app/build.gradle.kts` files can't
    // accidentally reach for `mavenLocal` or other ad-hoc repos and
    // surprise CI; same discipline AOSP samples follow.
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "AntDownload"

include(":app")
