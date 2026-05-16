// Workspace-level Gradle build. Holds nothing but the plugin
// declarations applied by `app/build.gradle.kts` — keeping the
// surface area thin makes it obvious that this Gradle root has no
// other modules and isn't going to grow into a multi-module project.
// (When the proper `.aar` artefact lands, the whole
// examples/android-download/ tree is expected to be deleted.)

plugins {
    alias(libs.plugins.android.application) apply false
    alias(libs.plugins.kotlin.compose) apply false
}
