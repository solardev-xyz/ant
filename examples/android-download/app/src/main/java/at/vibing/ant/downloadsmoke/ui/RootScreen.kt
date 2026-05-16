package at.vibing.ant.downloadsmoke.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import at.vibing.ant.downloadsmoke.AntException
import at.vibing.ant.downloadsmoke.AntNode
import at.vibing.ant.downloadsmoke.DownloadProgress
import kotlinx.coroutines.launch
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

/**
 * Single Compose screen for the v1 smoke test. Mirrors the original
 * `ContentView.swift` from the early iOS smoke (peer count + agent
 * + paste-and-download field + last result). The richer
 * Player/Network tab layout from `examples/ios-download/RootView.swift`
 * is deferred to phase D of the Android plan once the streaming and
 * progress JNI exports land.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun RootScreen(node: AntNode) {
    val status by node.status.collectAsState()
    val peerCount by node.peerCount.collectAsState()
    val agent by node.agentString.collectAsState()
    val progress by node.downloadProgress.collectAsState()
    val scope = rememberCoroutineScope()

    var reference by remember { mutableStateOf("") }
    var lastResult by remember { mutableStateOf<DownloadResult?>(null) }
    var isDownloading by remember { mutableStateOf(false) }

    /**
     * Shared launcher for both the manual `Download` button and the
     * one-tap sample buttons. Hoisted into a closure so the sample
     * buttons can fire the download without going through a recompose
     * step on the `reference` state — that keeps the "tap → status
     * spinner" latency at one frame.
     */
    fun launchDownload(ref: String) {
        if (isDownloading) return
        reference = ref
        scope.launch {
            isDownloading = true
            lastResult = runDownload(node, ref)
            isDownloading = false
        }
    }

    Surface(
        modifier = Modifier.fillMaxSize(),
        color = MaterialTheme.colorScheme.background,
    ) {
        Column(modifier = Modifier.fillMaxSize()) {
            TopAppBar(
                title = { Text("AntDownload") },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = Color.Transparent,
                ),
            )

            Column(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(horizontal = 16.dp, vertical = 8.dp),
                verticalArrangement = Arrangement.spacedBy(16.dp),
            ) {
                StatusCard(status = status, peerCount = peerCount, agent = agent)

                HorizontalDivider()

                SamplesRow(
                    enabled = !isDownloading && status is AntNode.Status.Ready,
                    onPick = { launchDownload(it) },
                )

                OutlinedTextField(
                    value = reference,
                    onValueChange = { reference = it.trim() },
                    label = { Text("Reference (hex / bytes:// / bzz://)") },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth(),
                )

                Row(
                    horizontalArrangement = Arrangement.spacedBy(12.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Button(
                        enabled = !isDownloading && status is AntNode.Status.Ready && reference.isNotBlank(),
                        onClick = { launchDownload(reference) },
                    ) {
                        Text(if (isDownloading) "Downloading…" else "Download")
                    }
                    if (isDownloading) {
                        OutlinedButton(onClick = { node.cancelDownload() }) {
                            Text("Cancel")
                        }
                    }
                }

                progress?.let { ProgressPanel(it, peerCount) }

                lastResult?.let { ResultCard(it) }
            }
        }
    }
}

/**
 * One-tap shortcuts for known-good mainnet references. Each sample
 * sets the `Reference` text field *and* immediately fires
 * `nativeDownload(...)`, so a fresh install with no clipboard contents
 * still has a one-tap path to verify the FFI end-to-end.
 *
 * The references mirror the litter-ally manifest the iOS smoke test
 * already exercises (see `examples/ios-download/AntDownload/Manifest.swift`).
 * Keeping the two apps pinned to the same root means a single live-
 * site outage takes both smoke tests offline at once, instead of
 * leaving us guessing which platform's sample drifted.
 */
@Composable
private fun SamplesRow(enabled: Boolean, onPick: (String) -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
        Text(
            text = "Samples",
            style = MaterialTheme.typography.labelMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            for (sample in Samples.all) {
                FilledTonalButton(
                    enabled = enabled,
                    onClick = { onPick(sample.reference) },
                ) {
                    Text(sample.label)
                }
            }
        }
        Text(
            text = "Cold-start: tap a sample after the status pill turns green.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

/**
 * Static list of bzz:// references the smoke app offers as one-tap
 * downloads. Pinned to the same litter-ally manifest root the iOS
 * smoke embeds, so:
 *
 *  - Cold-start sanity (`Cover (≈60 KB)`) lands in well under 30 s
 *    once the BZZ peer set has warmed up.
 *  - Cold-start stress (`Track (≈37 MB)`) keeps the chunks-pulled
 *    counter and the JNI byte-array allocation honest.
 *
 * Everything past `bzz://<root>/` is percent-encoded the same way a
 * pasted URL would be. The Rust `parse_reference` decodes both forms,
 * but the encoded form keeps copy/paste from the app's text field
 * round-trippable through other tools.
 */
private object Samples {
    private const val LITTER_ALLY_ROOT =
        "c4f8a45301b57d0e36f0f5348ed371aee42ea0b9fe9b3caaf26015d652eedc40"

    val all: List<Sample> = listOf(
        Sample(
            label = "Cover (≈60 KB)",
            reference = "bzz://$LITTER_ALLY_ROOT/cover.jpg",
        ),
        // "05 high five.wav" is the smallest of the 12 WAV masters
        // (~37 MB). Big enough to exercise multi-level joiner +
        // multi-peer fanout, small enough to land in well under a
        // minute on a warm node.
        Sample(
            label = "Track (≈37 MB)",
            reference = "bzz://$LITTER_ALLY_ROOT/tracks/05%20high%20five.wav",
        ),
    )
}

private data class Sample(
    val label: String,
    val reference: String,
)

@Composable
private fun StatusCard(status: AntNode.Status, peerCount: Int, agent: String) {
    val pillColor = when (status) {
        is AntNode.Status.Idle -> Color(0xFF8E8E93)
        is AntNode.Status.Starting -> Color(0xFFFF9F0A)
        is AntNode.Status.Ready -> Color(0xFF34C759)
        is AntNode.Status.Failed -> Color(0xFFFF453A)
    }

    Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Box(
                modifier = Modifier
                    .size(10.dp)
                    .clip(CircleShape)
                    .background(pillColor),
            )
            Spacer(Modifier.width(8.dp))
            Text(
                text = status.label,
                color = MaterialTheme.colorScheme.onBackground,
                style = MaterialTheme.typography.titleMedium,
            )
        }
        Row(
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            Text(
                text = "Peers: $peerCount",
                style = MaterialTheme.typography.bodyMedium,
            )
            if (agent.isNotEmpty()) {
                Text(
                    text = agent,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                )
            }
        }
    }
}

/**
 * Live progress dashboard rendered while a download is in flight (and
 * for ~400 ms after it finishes, to hold the final 100 % frame).
 * Mirrors the layout of `examples/ios-download/AntDownload/NetworkView.swift`
 * — the iOS version splits this across `progressCard`, `speedCard`,
 * and `statTiles`. We pack the same content into one Compose surface
 * so the v1 smoke fits on a single phone screen without scrolling.
 *
 * `peerCount` is the live peer count from the watch channel rather
 * than the per-download `peersUsed` field — same semantics the iOS
 * Network tab uses (`engine.peerCount` for the tile, the per-download
 * counter for the chunk-level "in-flight" tile).
 */
@Composable
private fun ProgressPanel(progress: DownloadProgress, peerCount: Int) {
    Surface(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(16.dp),
        color = MaterialTheme.colorScheme.surfaceVariant,
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(14.dp),
        ) {
            ProgressBytesRow(progress)
            LinearProgressIndicator(
                progress = { progress.fraction?.toFloat() ?: 0f },
                modifier = Modifier.fillMaxWidth(),
            )
            SpeedRow(progress)
            StatTilesRow(progress, peerCount)
        }
    }
}

@Composable
private fun ProgressBytesRow(progress: DownloadProgress) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.Bottom,
    ) {
        Text(
            text = bytesLabel(progress),
            style = MaterialTheme.typography.titleMedium,
            fontFamily = FontFamily.Monospace,
        )
        Text(
            text = percentLabel(progress),
            style = MaterialTheme.typography.titleMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontFamily = FontFamily.Monospace,
        )
    }
}

@Composable
private fun SpeedRow(progress: DownloadProgress) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Column {
            Text(
                text = "Speed",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Text(
                text = formatBytesPerSec(progress.throughputBytesPerSec),
                style = MaterialTheme.typography.titleSmall,
                fontFamily = FontFamily.Monospace,
            )
        }
        Column(horizontalAlignment = Alignment.End) {
            Text(
                text = "ETA",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Text(
                text = formatEta(progress),
                style = MaterialTheme.typography.titleSmall,
                fontFamily = FontFamily.Monospace,
            )
        }
    }
}

@Composable
private fun StatTilesRow(progress: DownloadProgress, peerCount: Int) {
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        // chunks tile gets a wider weight because once the joiner
        // resolves the manifest we render "<done> / <total>" in
        // monospace and that's the longest value of the four tiles.
        // Equal weights would clip the suffix on a 5" phone.
        StatTile(
            label = "chunks",
            value = progress.chunksDone.toString(),
            suffix = progress.totalChunks.takeIf { it > 0L }?.let { "/ $it" },
            modifier = Modifier.weight(2f),
        )
        StatTile(
            label = "peers",
            value = peerCount.toString(),
            modifier = Modifier.weight(1f),
        )
        StatTile(
            label = "in-flight",
            value = progress.inFlight.toString(),
            modifier = Modifier.weight(1f),
        )
        StatTile(
            label = "cache",
            value = progress.cacheHits.toString(),
            modifier = Modifier.weight(1f),
        )
    }
}

@Composable
private fun StatTile(
    label: String,
    value: String,
    modifier: Modifier = Modifier,
    suffix: String? = null,
) {
    Surface(
        modifier = modifier,
        shape = RoundedCornerShape(10.dp),
        color = MaterialTheme.colorScheme.surface,
    ) {
        Column(
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 8.dp),
            verticalArrangement = Arrangement.spacedBy(2.dp),
        ) {
            Row(
                verticalAlignment = Alignment.Bottom,
                horizontalArrangement = Arrangement.spacedBy(4.dp),
            ) {
                Text(
                    text = value,
                    style = MaterialTheme.typography.titleMedium,
                    fontFamily = FontFamily.Monospace,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
                if (suffix != null) {
                    Text(
                        text = suffix,
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                }
            }
            Text(
                text = label,
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

@Composable
private fun ResultCard(result: DownloadResult) {
    Surface(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(12.dp),
        // Three result moods: success / canceled / failed. Cancellation
        // is user-initiated and not an error condition, so it gets the
        // same neutral surfaceVariant background as success — only
        // hard FFI failures get the red wash.
        color = when (result) {
            is DownloadResult.Success, is DownloadResult.Cancelled ->
                MaterialTheme.colorScheme.surfaceVariant
            is DownloadResult.Failure -> Color(0x33FF453A)
        },
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp),
        ) {
            when (result) {
                is DownloadResult.Success -> {
                    Text(
                        text = "Got ${result.bytes} bytes in ${result.elapsedMs} ms",
                        style = MaterialTheme.typography.titleSmall,
                    )
                    Text(
                        text = result.hexPrefix,
                        style = MaterialTheme.typography.bodySmall,
                        fontFamily = FontFamily.Monospace,
                        maxLines = 3,
                        overflow = TextOverflow.Ellipsis,
                    )
                }
                is DownloadResult.Cancelled -> {
                    Text(
                        text = "Canceled",
                        style = MaterialTheme.typography.titleSmall,
                    )
                    Text(
                        text = cancelledSubtitle(result),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                is DownloadResult.Failure -> {
                    Text(
                        text = "Download failed",
                        style = MaterialTheme.typography.titleSmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                    Text(
                        text = result.message,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurface,
                    )
                }
            }
        }
    }
}

private fun cancelledSubtitle(result: DownloadResult.Cancelled): String {
    if (result.bytesSoFar <= 0L) return "Stopped before any chunks landed."
    val done = formatBytes(result.bytesSoFar)
    return if (result.totalBytes > 0L) {
        "Stopped after $done / ${formatBytes(result.totalBytes)}."
    } else {
        "Stopped after $done."
    }
}

private suspend fun runDownload(node: AntNode, reference: String): DownloadResult {
    return try {
        var bytes: ByteArray? = null
        val elapsed = measureTimeMillis { bytes = node.download(reference) }
        val data = bytes ?: return DownloadResult.Failure("download returned no bytes")
        DownloadResult.Success(
            bytes = data.size,
            elapsedMs = elapsed,
            hexPrefix = data.toHexPrefix(),
        )
    } catch (e: AntException) {
        val msg = e.message.orEmpty()
        // The Rust side throws this exact string from
        // `download_inner`'s cancel-flag check; matching on it lets
        // us turn a user-initiated cancel into a neutral terminal
        // state instead of a red error card.
        if (msg.contains("download canceled", ignoreCase = true)) {
            val snapshot = node.downloadProgress.value
            DownloadResult.Cancelled(
                bytesSoFar = snapshot?.bytesDone ?: 0L,
                totalBytes = snapshot?.totalBytes ?: 0L,
            )
        } else {
            DownloadResult.Failure(msg.ifEmpty { "unknown error" })
        }
    } catch (t: Throwable) {
        DownloadResult.Failure("unexpected: ${t.message ?: t.javaClass.simpleName}")
    }
}

private sealed interface DownloadResult {
    data class Success(val bytes: Int, val elapsedMs: Long, val hexPrefix: String) : DownloadResult
    /** User-initiated stop. `bytesSoFar` is the last progress sample. */
    data class Cancelled(val bytesSoFar: Long, val totalBytes: Long) : DownloadResult
    data class Failure(val message: String) : DownloadResult
}

/**
 * Render up to the first 32 bytes as lower-case hex, joined in
 * groups of 4 for readability. Matches the iOS `bytesPrefix` shape
 * so the two smoke tests' result panels look the same to the eye.
 */
private fun ByteArray.toHexPrefix(): String {
    val n = minOf(size, 32)
    val sb = StringBuilder(n * 2 + (n / 4))
    for (i in 0 until n) {
        sb.append(HEX[(this[i].toInt() ushr 4) and 0xF])
        sb.append(HEX[this[i].toInt() and 0xF])
        if (i % 4 == 3 && i != n - 1) sb.append(' ')
    }
    if (size > n) sb.append(" …")
    return sb.toString()
}

private val HEX = "0123456789abcdef".toCharArray()

// ---------------------------------------------------------------------------
// Formatting helpers — mirror NetworkView.swift one-to-one in label
// shape ("`72.4 MB / 117.5 MB`", "`1.32 MB/s`", "`0:42`", "`14 / 36`")
// so the two smoke tests' progress dashboards read identically. The
// iOS code uses ByteCountFormatter; we hand-roll the same four-stage
// {B, KB, MB, GB} ladder because Android's Formatter#formatFileSize
// is locale-dependent and gives a different shape on RU / FR locales.
// ---------------------------------------------------------------------------

private fun bytesLabel(progress: DownloadProgress): String {
    return if (progress.totalBytes > 0L) {
        "${formatBytes(progress.bytesDone)} / ${formatBytes(progress.totalBytes)}"
    } else {
        formatBytes(progress.bytesDone)
    }
}

private fun percentLabel(progress: DownloadProgress): String {
    val frac = progress.fraction ?: return "—"
    return "${(frac * 100).roundToInt()} %"
}

private fun formatEta(progress: DownloadProgress): String {
    val total = progress.totalBytes
    val done = progress.bytesDone
    if (total <= 0L || done >= total) return "—"
    val rate = progress.throughputBytesPerSec
    if (rate <= 0.0) return "—"
    val remainingBytes = total - done
    val seconds = (remainingBytes.toDouble() / rate).toLong().coerceAtLeast(0L)
    return formatDuration(seconds)
}

private fun formatDuration(totalSeconds: Long): String {
    val s = totalSeconds % 60
    val m = (totalSeconds / 60) % 60
    val h = totalSeconds / 3600
    return if (h > 0) {
        "%d:%02d:%02d".format(h, m, s)
    } else {
        "%d:%02d".format(m, s)
    }
}

private fun formatBytes(bytes: Long): String {
    if (bytes <= 0L) return "0 B"
    val k = 1024.0
    val mag = bytes.toDouble()
    return when {
        mag < k -> "$bytes B"
        mag < k * k -> "%.1f KB".format(mag / k)
        mag < k * k * k -> "%.1f MB".format(mag / (k * k))
        else -> "%.2f GB".format(mag / (k * k * k))
    }
}

private fun formatBytesPerSec(rate: Double): String {
    if (rate <= 0.0) return "—"
    val k = 1024.0
    return when {
        rate < k -> "%.0f B/s".format(rate)
        rate < k * k -> "%.1f KB/s".format(rate / k)
        rate < k * k * k -> "%.2f MB/s".format(rate / (k * k))
        else -> "%.2f GB/s".format(rate / (k * k * k))
    }
}
