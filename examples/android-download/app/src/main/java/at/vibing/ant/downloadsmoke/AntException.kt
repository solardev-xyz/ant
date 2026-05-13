package at.vibing.ant.downloadsmoke

/**
 * Thrown by every JNI export in `crates/ant-ffi/src/jni.rs` on
 * failure. Mirrors the iOS `AntError.download(String)` /
 * `AntError.notReady` pair, but wired through a real Java exception
 * type so Kotlin's `try { ... } catch (e: AntException) { ... }`
 * pattern reads naturally.
 *
 * Pinned to the package `at.vibing.ant.downloadsmoke` because the JNI
 * layer hard-codes the JVM class path
 * (`Lat/vibing/ant/downloadsmoke/AntException;`) — moving it
 * requires updating both sides in lock-step.
 */
class AntException(message: String) : RuntimeException(message)
