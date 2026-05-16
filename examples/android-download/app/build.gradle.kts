// AntDownload smoke-test app module. Mirrors `examples/ios-download/`
// on the Android side — see PLAN.md § 9 → "Android download smoke
// test (pre-FFI)" for the full design rationale.
//
// The native lib (`crates/ant-ffi/`) is cross-compiled out-of-band
// by `cargo xtask build-android-arm64 / -armv7 / -x86_64`, which
// drops `libant_ffi.so` into `app/src/main/jniLibs/<abi>/`. Gradle
// picks them up at packaging time without any further wiring; we
// don't try to invoke cargo from Gradle because the iOS Xcode
// project has had good luck keeping those build systems orthogonal
// (host build system reports a path; Gradle / xcodebuild consume
// the path).

plugins {
    // AGP 9.x ships with first-class Kotlin support, so we no longer
    // apply `org.jetbrains.kotlin.android` here — the application
    // plugin handles `*.kt` source compilation directly. We *do*
    // still need the compose-compiler plugin since that's a Kotlin
    // compiler extension that AGP doesn't pull in for us.
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.compose)
}

android {
    namespace = "at.vibing.ant.downloadsmoke"
    compileSdk = libs.versions.compileSdk.get().toInt()

    defaultConfig {
        // Same reverse-DNS as the iOS bundle id so logs / store
        // listings read identically across platforms; PLAN.md
        // § 9 → Android smoke spells this out.
        applicationId = "at.vibing.ant.downloadsmoke"
        minSdk = libs.versions.minSdk.get().toInt()
        targetSdk = libs.versions.targetSdk.get().toInt()
        versionCode = 1
        versionName = "0.1.0"

        // Keep release APKs tied to the ABIs cargo-ndk actually
        // produces — declaring a dummy ABI here would make Gradle
        // try to package native code we don't ship. Each entry maps
        // 1:1 to the `cargo xtask build-android-*` recipes in
        // `xtask/src/main.rs`.
        ndk {
            //noinspection ChromeOsAbiSupport
            abiFilters += listOf("arm64-v8a", "armeabi-v7a", "x86_64")
        }
    }

    buildTypes {
        getByName("release") {
            // R8 minify + resource shrinking on. Trims `classes.dex`
            // from ~36 MB uncompressed down to ~7 MB on this app
            // (drops dead lifecycle / coroutine code paths Compose
            // and our own KTX deps don't reference). The keep rules
            // in `proguard-rules.pro` carry the JNI symbols that the
            // dynamic loader resolves at runtime — without them R8
            // would happily strip them as "unused".
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro",
            )
        }
        getByName("debug") {
            // Debug builds never minify — symbols stay readable in
            // `adb logcat` and stack traces remain legible without
            // a mapping file lookup.
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    // AGP 9.x's built-in Kotlin support drops the `kotlinOptions {}`
    // block — `compileOptions.{source,target}Compatibility` is now
    // the single source of truth for both `javac` and `kotlinc`
    // bytecode targets, so explicit Kotlin JVM target wiring would
    // be redundant.

    buildFeatures {
        compose = true
        // BuildConfig isn't read at runtime so we drop it; saves a
        // generated class per build type.
        buildConfig = false
    }

    packaging {
        resources {
            // The libp2p chain pulls in a few duplicate META-INF
            // entries (NOTICE, LICENSE) through transitive deps; pick
            // any single copy at packaging time.
            excludes += setOf(
                "META-INF/AL2.0",
                "META-INF/LGPL2.1",
                "/META-INF/{AL2.0,LGPL2.1}",
            )
        }
        jniLibs {
            // Required for releases targeting modern Android: the
            // native libs ship inside the APK uncompressed so the
            // dynamic linker can mmap them at start-up without an
            // intermediate copy. This is also why we don't compress
            // them in the .gitignore'd jniLibs source tree.
            useLegacyPackaging = false
        }
    }
}

dependencies {
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.lifecycle.viewmodel.compose)
    implementation(libs.androidx.activity.compose)

    val composeBom = platform(libs.androidx.compose.bom)
    implementation(composeBom)
    androidTestImplementation(composeBom)

    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.graphics)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.compose.material3)

    debugImplementation(libs.androidx.compose.ui.tooling)
}
