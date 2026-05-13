package at.vibing.ant.downloadsmoke

/**
 * Snapshot of a single in-flight download. Mirrors the iOS
 * `DownloadProgress` struct in `examples/ios-download/AntDownload/AntNode.swift`
 * one-to-one (same field names lowercased, same units), plus a
 * `throughputBytesPerSec` derived on the Kotlin side from a 3-second
 * sliding window the FFI doesn't compute itself.
 *
 * Populated by `AntNode.startPollingProgress` from the 9-element
 * `LongArray` `nativeDownloadProgress(handle)` returns. Field order
 * is locked to the JNI export — see `crates/ant-ffi/src/jni.rs` for
 * the matching table.
 */
data class DownloadProgress(
    /** `true` while the FFI is actively retrieving, `false` once finished. */
    val inProgress: Boolean,
    /** Bytes of chunk wire data delivered to the joiner so far. */
    val bytesDone: Long,
    /**
     * Expected file-body size derived from the data root's span.
     * `0` until the root chunk has been fetched (and for single-chunk
     * files, `1` chunk / root span bytes right away).
     */
    val totalBytes: Long,
    /** Chunks delivered to the joiner (cache hits + network). */
    val chunksDone: Long,
    /** Estimated total chunks from the root span. `0` until known. */
    val totalChunks: Long,
    /** Wall-clock milliseconds since the node accepted the request. */
    val elapsedMs: Long,
    /** Distinct peers we've fetched at least one chunk from. */
    val peersUsed: Int,
    /** Chunk fetches currently dispatched but not yet returned. */
    val inFlight: Int,
    /** Cache hits served without going to the network. */
    val cacheHits: Long,
    /**
     * Smoothed bytes-per-second over the last 3 seconds of poll
     * samples, computed by the Kotlin layer (the FFI ships raw
     * counters only). `0` until at least two samples are available.
     */
    val throughputBytesPerSec: Double,
) {
    /**
     * `0.0 ... 1.0` progress fraction, or `null` while the data root
     * hasn't landed yet (which is when `totalBytes == 0`). Same shape
     * as iOS `DownloadProgress.fraction`.
     */
    val fraction: Double?
        get() = if (totalBytes > 0L) {
            (bytesDone.toDouble() / totalBytes.toDouble()).coerceAtMost(1.0)
        } else null

    companion object {
        /** Zero-everything seed used when a download is just starting. */
        fun empty(): DownloadProgress = DownloadProgress(
            inProgress = true,
            bytesDone = 0,
            totalBytes = 0,
            chunksDone = 0,
            totalChunks = 0,
            elapsedMs = 0,
            peersUsed = 0,
            inFlight = 0,
            cacheHits = 0,
            throughputBytesPerSec = 0.0,
        )
    }
}
