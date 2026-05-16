package at.vibing.ant.downloadsmoke

import android.content.Context
import android.os.SystemClock
import androidx.compose.runtime.Immutable
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File

/**
 * Kotlin wrapper around the JNI surface in `crates/ant-ffi/src/jni.rs`.
 *
 * Sibling to `AntNode.swift` in the iOS smoke test. Holds a single
 * `nativeHandle: Long` for the lifetime of the app, exposes
 * `StateFlow`-backed observables for the status / peer count /
 * agent string / progress fields the UI subscribes to, and runs every
 * blocking JNI call on `Dispatchers.IO` so the main thread is never
 * caught violating Android's `NetworkOnMainThreadException` policy.
 *
 * Lifecycle:
 *
 *  - Construct a single instance in the [AntDownloadApp.onCreate] hook
 *    (or the Application equivalent). Construction is cheap — it only
 *    loads `libant_ffi.so`.
 *  - Call [start] once Compose has the `Context` to hand us a writable
 *    data dir. Subsequent calls are a no-op.
 *  - Call [download] for each retrieval; concurrent calls share the
 *    embedded node's chunk cache and command channel.
 *  - Call [shutdown] when the process is leaving for good (rare in
 *    Android — usually the OS just kills the process).
 *
 * Threading:
 *
 *  - All `external fun` calls are blocking and live on `Dispatchers.IO`.
 *  - StateFlow updates flip back to the main dispatcher implicitly via
 *    `MutableStateFlow.value =`.
 *  - The handle is plain `Long` (`*mut AntHandle` on the Rust side)
 *    and accessed without locking; the Rust state machine is internally
 *    `Send + Sync`.
 */
class AntNode {

    /**
     * Discriminated status for the embedded node. Mirrors the iOS
     * `AntNode.Status` enum — same names, same colours, same legible
     * `label` strings so logcat / Xcode console lines read identically.
     */
    @Immutable
    sealed interface Status {
        val label: String

        data object Idle : Status { override val label: String = "Idle" }
        data object Starting : Status { override val label: String = "Starting…" }
        data object Ready : Status { override val label: String = "Ready" }
        data class Failed(val message: String) : Status {
            override val label: String get() = "Failed: $message"
        }

        val isReady: Boolean get() = this is Ready
    }

    private val _status = MutableStateFlow<Status>(Status.Idle)
    val status: StateFlow<Status> = _status.asStateFlow()

    private val _peerCount = MutableStateFlow(0)
    val peerCount: StateFlow<Int> = _peerCount.asStateFlow()

    private val _agentString = MutableStateFlow("")
    val agentString: StateFlow<String> = _agentString.asStateFlow()

    /**
     * Live progress view of the in-flight `download(reference)` call,
     * updated ~4×/s from a poll job. `null` whenever no download is
     * running (or the last one finished and its state was cleared).
     * Direct mirror of the iOS `@Published downloadProgress`.
     */
    private val _downloadProgress = MutableStateFlow<DownloadProgress?>(null)
    val downloadProgress: StateFlow<DownloadProgress?> = _downloadProgress.asStateFlow()

    /**
     * Raw `*mut AntHandle` cast to `jlong`, populated by [start]'s
     * call to [nativeInit]. Zero whenever the node is not started.
     * Volatile so the polling job sees the post-`shutdown` zero
     * without holding a lock — same threading discipline as the iOS
     * `private var handle: OpaquePointer?` field.
     */
    @Volatile
    private var nativeHandle: Long = 0L

    /**
     * Coroutine scope rooted on a SupervisorJob so a cancelled poll
     * task doesn't take down the rest. Cancelled in [shutdown].
     */
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /**
     * Background poller that reads `nativePeerCount` 5×/s once the
     * node is `Ready`. Direct mirror of the iOS `peerPollTask`.
     */
    private var peerPollJob: Job? = null

    /**
     * Background poller that reads `nativeDownloadProgress` 4×/s while
     * a download is in flight. Started in [download], cancelled in
     * its `finally` block. Mirror of iOS `progressPollTask`.
     */
    private var progressPollJob: Job? = null

    /**
     * Spin up the embedded node. Idempotent: subsequent calls while
     * status != Idle are no-ops, matching iOS `AntNode.start()`.
     *
     * @param context any Context — used solely to look up
     *   `filesDir.absolutePath`, which we hand the FFI as the
     *   data-dir for `identity.json` / `peers.json` persistence.
     */
    suspend fun start(context: Context) {
        if (_status.value !is Status.Idle) return
        _status.value = Status.Starting

        val dataDir: String = withContext(Dispatchers.IO) {
            // Application Support equivalent on Android. Files here
            // survive app upgrades and aren't user-visible. Sized
            // about right for a peer snapshot + identity (~2 KB).
            val dir = File(context.filesDir, "ant")
            dir.mkdirs()
            dir.absolutePath
        }

        val outcome: InitOutcome = withContext(Dispatchers.IO) {
            try {
                val handle = nativeInit(dataDir)
                if (handle == 0L) {
                    InitOutcome.Failed("nativeInit returned 0 without throwing")
                } else {
                    InitOutcome.Ready(handle)
                }
            } catch (e: AntException) {
                InitOutcome.Failed(e.message ?: "unknown error")
            } catch (t: Throwable) {
                InitOutcome.Failed("unexpected: ${t.message ?: t.javaClass.simpleName}")
            }
        }

        when (outcome) {
            is InitOutcome.Ready -> {
                nativeHandle = outcome.handle
                _status.value = Status.Ready
                _agentString.value = readAgent(outcome.handle)
                startPollingPeers()
            }
            is InitOutcome.Failed -> {
                _status.value = Status.Failed(outcome.message)
            }
        }
    }

    /** Tear down the embedded node. Safe to call when not started. */
    suspend fun shutdown() {
        val h = nativeHandle
        if (h == 0L) return
        nativeHandle = 0L
        peerPollJob?.cancel()
        peerPollJob = null
        progressPollJob?.cancel()
        progressPollJob = null
        _peerCount.value = 0
        _agentString.value = ""
        _downloadProgress.value = null
        withContext(Dispatchers.IO) { nativeShutdown(h) }
        _status.value = Status.Idle
    }

    /**
     * Block (on `Dispatchers.IO`) until the requested reference is
     * fully retrieved. Returns the raw bytes; the caller is
     * responsible for whatever rendering / persistence they want
     * to do next. Throws [AntException] on any FFI-reported error,
     * mirroring iOS `AntError.download(String)`.
     *
     * While the call is in flight, [downloadProgress] is populated
     * by a 4 Hz poll job. The slot is cleared back to `null` ~400 ms
     * after `download` returns so the UI can hold the final "100 %"
     * frame instead of snapping back to nothing the instant the FFI
     * call resolves — same hold-time the iOS smoke uses.
     */
    suspend fun download(reference: String): ByteArray {
        val h = nativeHandle
        if (h == 0L) throw AntException("Node is not ready")
        startPollingProgress(h)
        try {
            return withContext(Dispatchers.IO) {
                nativeDownload(h, reference)
            }
        } finally {
            progressPollJob?.cancel()
            progressPollJob = null
            // Hold the final progress sample on screen briefly so a
            // sub-second download (cache hit) doesn't flash a blank
            // card before the result lands. Mirrors the 400 ms hold
            // in `AntNode.swift` (download(reference:)).
            scope.launch {
                delay(400L)
                if (progressPollJob == null) {
                    _downloadProgress.value = null
                }
            }
        }
    }

    /**
     * Request cancellation of the in-flight `download(reference)`. No-op
     * when no download is running. The racing `download` call throws
     * `AntException("download canceled")` on its next ack-loop
     * iteration (≤ ~150 ms under healthy retrieval).
     *
     * Caveat: this only stops our Kotlin/FFI wait loop. The embedded
     * node's `run_get_bzz` task is not cross-request cancellable
     * today — it keeps fetching remaining chunks in the background
     * and populates the in-memory chunk cache. That's what makes a
     * subsequent retry of the same reference feel like a resume.
     * Same caveat as the iOS smoke (`AntNode.cancelDownload()`).
     */
    fun cancelDownload() {
        val h = nativeHandle
        if (h == 0L) return
        nativeCancelDownload(h)
    }

    /**
     * One-shot pull of the running node's `agent` field. The status
     * snapshot fixes this at `nativeInit` time, so polling would
     * burn cycles for no signal. Same shape as iOS
     * `AntNode.readAgentString(handle:)`.
     */
    private fun readAgent(handle: Long): String {
        return try {
            nativeAgentString(handle).orEmpty()
        } catch (t: Throwable) {
            ""
        }
    }

    /**
     * 5 Hz peer-count poll. Cheap (a `RwLock` read of the shared
     * status snapshot inside the embedded node), so the polling
     * cost on a low-end phone is dominated by the JNI transition,
     * not the read itself.
     */
    private fun startPollingPeers() {
        peerPollJob?.cancel()
        peerPollJob = scope.launch {
            while (true) {
                val h = nativeHandle
                if (h == 0L) break
                val count = try {
                    nativePeerCount(h)
                } catch (t: Throwable) {
                    -1
                }
                _peerCount.value = if (count < 0) 0 else count
                delay(200L)
            }
        }
    }

    /**
     * Read `nativeDownloadProgress(handle)` 4×/s, smooth the
     * throughput over a 3-second sliding window, and publish the
     * result on `_downloadProgress`. Direct port of the iOS
     * `AntNode.startPollingProgress(handle:)` loop in
     * `examples/ios-download/AntDownload/AntNode.swift`.
     */
    private fun startPollingProgress(handle: Long) {
        progressPollJob?.cancel()
        // Seed a fresh zero-progress sample so the UI can render the
        // HUD on the *next* recompose rather than waiting up to one
        // poll interval — same pattern as the iOS code.
        _downloadProgress.value = DownloadProgress.empty()
        progressPollJob = scope.launch {
            // 3 s sliding window over (timestamp_ms, bytes_done) pairs.
            // Compared to an EMA, a straight window is much calmer
            // when chunks land in `buffer_unordered` bursts, and the
            // readout literally is "average MiB/s over the last 3 s",
            // which is what users expect a download speedometer to
            // mean. The chunks / peers / in-flight counters keep
            // updating at the full 250 ms cadence because the window
            // only smooths the rate, not the rest.
            val windowMs = 3_000L
            val samples = ArrayDeque<Sample>()
            while (true) {
                val h = nativeHandle
                if (h == 0L) break
                val raw: LongArray? = try {
                    nativeDownloadProgress(h)
                } catch (t: Throwable) {
                    null
                }
                if (raw != null && raw.size >= 9) {
                    val now = SystemClock.elapsedRealtime()
                    val bytesDone = raw[1]
                    samples.addLast(Sample(at = now, bytes = bytesDone))
                    val cutoff = now - windowMs
                    while (samples.size > 1 && samples.first().at < cutoff) {
                        samples.removeFirst()
                    }
                    val bytesPerSec: Double = if (samples.size >= 2) {
                        val oldest = samples.first()
                        val dtMs = (now - oldest.at).coerceAtLeast(0L)
                        if (dtMs >= 250L) {
                            val deltaBytes = (bytesDone - oldest.bytes).coerceAtLeast(0L)
                            deltaBytes.toDouble() * 1_000.0 / dtMs.toDouble()
                        } else 0.0
                    } else 0.0
                    _downloadProgress.value = DownloadProgress(
                        inProgress = raw[0] != 0L,
                        bytesDone = raw[1],
                        totalBytes = raw[2],
                        chunksDone = raw[3],
                        totalChunks = raw[4],
                        elapsedMs = raw[5],
                        peersUsed = raw[6].toInt(),
                        inFlight = raw[7].toInt(),
                        cacheHits = raw[8],
                        throughputBytesPerSec = bytesPerSec,
                    )
                }
                delay(250L)
            }
        }
    }

    private sealed interface InitOutcome {
        data class Ready(val handle: Long) : InitOutcome
        data class Failed(val message: String) : InitOutcome
    }

    private data class Sample(val at: Long, val bytes: Long)

    // -----------------------------------------------------------------
    // JNI surface — mirror of `crates/ant-ffi/src/jni.rs`.
    //
    // Each `external fun` resolves to a `Java_at_vibing_ant_downloadsmoke_AntNode_<name>`
    // symbol exported from `libant_ffi.so` (which lives in
    // `app/src/main/jniLibs/<abi>/`, populated by
    // `cargo xtask build-android-*`). The companion object's static
    // initializer loads the library exactly once per process — the
    // platform-default behaviour of `System.loadLibrary` matches the
    // iOS staticlib model where all symbols are baked into the app
    // binary by the time the first activity is created.
    //
    // `external` methods are static (`@JvmStatic` via companion
    // object) so the JNI symbol mangling lines up with the Rust
    // exports, which are written as `Java_…_AntNode_native…` not
    // `…_AntNode_00024Companion_native…` (the latter would be the
    // mangling for non-static functions on `AntNode.Companion`).
    // -----------------------------------------------------------------

    companion object {
        init {
            // Triggers `dlopen("libant_ffi.so")` against the right
            // ABI subdirectory. Idempotent within a process — calling
            // `loadLibrary` twice with the same name is a documented
            // no-op since Android 4.x.
            System.loadLibrary("ant_ffi")
        }

        @JvmStatic external fun nativeInit(dataDir: String): Long
        @JvmStatic external fun nativeDownload(handle: Long, reference: String): ByteArray
        @JvmStatic external fun nativePeerCount(handle: Long): Int
        @JvmStatic external fun nativeAgentString(handle: Long): String?
        /**
         * Returns a 9-element snapshot of the in-flight download's
         * progress counters, or `null` on a null handle. See the
         * Rust side (`crates/ant-ffi/src/jni.rs`) for the exact
         * field order — the Kotlin layer in [startPollingProgress]
         * reconstructs a [DownloadProgress] from this array.
         */
        @JvmStatic external fun nativeDownloadProgress(handle: Long): LongArray?
        /** Returns 0 on success, -1 on a null handle. */
        @JvmStatic external fun nativeCancelDownload(handle: Long): Int
        @JvmStatic external fun nativeShutdown(handle: Long)
    }
}
