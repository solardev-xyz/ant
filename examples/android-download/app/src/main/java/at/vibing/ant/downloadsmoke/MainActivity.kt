package at.vibing.ant.downloadsmoke

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.lifecycle.lifecycleScope
import at.vibing.ant.downloadsmoke.ui.RootScreen
import kotlinx.coroutines.launch

/**
 * Single-activity host for the Compose UI.
 *
 * Kicks off `AntNode.start(...)` from `lifecycleScope` so the
 * embedded node is racing the first Compose frame; the UI handles
 * the still-`Starting…` state for free via the StateFlow it
 * subscribes to. Mirrors the SwiftUI `.task { await node.start() }`
 * we attach in `AntDownloadApp.swift`.
 */
class MainActivity : ComponentActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val app = applicationContext as AntDownloadApp
        val node = app.node

        lifecycleScope.launch {
            node.start(applicationContext)
        }

        setContent {
            // Compose runs the rest. We pin the colour scheme to
            // dark by default to match the iOS app
            // (`preferredColorScheme(.dark)`); platform setting
            // override is one boolean away if needed.
            val colorScheme = if (isSystemInDarkTheme()) darkColorScheme() else darkColorScheme()
            MaterialTheme(colorScheme = colorScheme) {
                RootScreen(node = node)
            }
        }
    }
}
