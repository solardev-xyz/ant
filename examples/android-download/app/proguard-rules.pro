# Keep rules for the AntDownload smoke-test app.
#
# These rules survive both R8 minification and obfuscation passes —
# `app/build.gradle.kts` enables both for the release build type, so
# anything not explicitly kept here can (and will) be stripped or
# renamed.

# JNI native methods on AntNode are resolved by the dynamic linker
# from the symbol name, not by reflection. R8 doesn't know that
# `at_vibing_ant_downloadsmoke_AntNode_nativeInit` etc. inside
# `libant_ffi.so` need to find a matching Kotlin method, so it would
# rename / strip them without these directives.
-keep class at.vibing.ant.downloadsmoke.AntNode {
    native <methods>;
}
-keepclassmembers class at.vibing.ant.downloadsmoke.AntNode {
    native <methods>;
}

# AntException is constructed *from inside* `crates/ant-ffi/src/jni.rs`
# via JNI's `Env::throw_new(jni_str!("at/vibing/ant/downloadsmoke/AntException"), …)`.
# That class lookup happens by name, again invisibly to R8, so we
# pin both the class and its (Throwable, String) constructor.
-keep class at.vibing.ant.downloadsmoke.AntException {
    public <init>(java.lang.String);
}
