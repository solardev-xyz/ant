//! `antctl` — command-line client for the `antd` daemon.
//!
//! Speaks to the daemon over its Unix-domain control socket (NDJSON,
//! `ant-control::Request` / `Response`).

use ant_control::{
    request_streaming, request_sync, GatewayRequestInfo, GetProgress, PeerConnectionInfo,
    PeerConnectionState, Request, Response, RetrievalInfo, StatusSnapshot,
};
use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use crossterm::{
    cursor::{Hide, Show},
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    style::ResetColor,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect as TuiRect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Row, Table, Tabs},
    Frame, Terminal,
};
use std::collections::HashMap;
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(
    name = "antctl",
    version,
    about = "Control and inspect a running antd daemon"
)]
struct Opt {
    /// Path to the daemon's control socket. Defaults to `<data-dir>/antd.sock`.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,

    /// Data directory, used to locate the default control socket.
    #[arg(long, global = true, default_value = "~/.antd")]
    data_dir: PathBuf,

    /// Emit JSON instead of a human-readable report.
    #[arg(long, global = true, default_value_t = false)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Show live daemon status: identity, peers, listeners, uptime.
    Status,
    /// Auto-refreshing daemon status board.
    Top {
        /// Refresh interval in milliseconds.
        #[arg(long = "interval-ms", default_value_t = 200)]
        interval_ms: u64,
    },
    /// Show the daemon agent string and control-protocol version.
    Version,
    /// Inspect or manage the daemon's peer set.
    Peers {
        #[command(subcommand)]
        command: PeersCommand,
    },
    /// Retrieve content from Swarm.
    #[command(long_about = "\
Retrieve content from Swarm.

The reference may be one of:

  <hex>                  Single 32-byte content-addressed chunk
                         (bee's /bytes/<ref> for short files <= 4 KiB).
  bytes://<hex>          Walk a multi-chunk byte tree rooted at <hex>
                         (bee's /bytes/<ref> for longer files). Same
                         shape as --bytes.
  bzz://<hex>[/path]     Walk the mantaray manifest at <hex>, resolve
                         <path> (or the website-index-document), then
                         join the resulting chunk tree. Same shape as
                         --bzz [--bzz-path]. Path may be multi-segment.

Examples:

  antctl get <chunk-hex>                      # raw 4 KiB chunk
  antctl get bytes://<file-hex> -o blob.bin   # multi-chunk file
  antctl get bzz://<root>/index.html          # website index
  antctl get bzz://<root>/images/logo.png     # nested file
  antctl get bzz://<root>/13/4358/2645.png    # tile pyramid leaf")]
    Get {
        /// Reference. See above for accepted forms.
        reference: String,
        /// Path to write the bytes to. Defaults to stdout.
        #[arg(long, short = 'o')]
        out: Option<PathBuf>,
        /// Force `bytes` (multi-chunk join) mode regardless of the
        /// reference's URL scheme.
        #[arg(long, conflicts_with = "bzz")]
        bytes: bool,
        /// Force `bzz` (manifest walk + join) mode regardless of the
        /// reference's URL scheme. Pair with `--bzz-path` to pick a
        /// non-default file inside a directory manifest.
        #[arg(long, conflicts_with = "bytes")]
        bzz: bool,
        /// Path component of a bzz reference, e.g. `index.html` or
        /// `images/logo.png` or `13/4358/2645.png`. Empty triggers
        /// `website-index-document`.
        #[arg(long, requires = "bzz")]
        bzz_path: Option<String>,
        /// Decode bzz files whose root span carries a non-zero
        /// Reed-Solomon redundancy level. The daemon masks the level
        /// byte off and walks the chunk tree without RS recovery, so
        /// every data chunk must still be reachable. Default: off
        /// (the daemon errors with `redundancy / erasure coding not
        /// supported`).
        #[arg(long)]
        allow_degraded_redundancy: bool,
        /// Before issuing the request, poll `antctl status` until the
        /// daemon has a completed BZZ handshake AND at least N libp2p
        /// peers connected. Avoids the `no peers available; wait for
        /// handshakes to complete` error when the user fires `get`
        /// against a freshly-started daemon. `0` disables the wait
        /// (request goes out immediately).
        #[arg(long, default_value_t = 4)]
        wait_peers: u32,
        /// Maximum time to spend waiting for `--wait-peers` before
        /// giving up and issuing the request anyway. The daemon will
        /// then return its own error if peers really are zero. In
        /// seconds.
        #[arg(long, default_value_t = 30)]
        wait_timeout: u64,
        /// Skip the daemon's shared in-memory chunk cache for this
        /// request only. The daemon mints a fresh per-request cache,
        /// so intra-request retries still benefit from caching but no
        /// chunk hits or writes touch the long-lived cache. Useful
        /// for timing the cold path or reproducing a transient
        /// retrieval issue.
        #[arg(long)]
        bypass_cache: bool,
        /// Suppress the in-place progress line on stderr. By default
        /// `antctl get` renders a one-line status (chunks, bytes,
        /// rate, peer count) that updates every ~150 ms while the
        /// daemon walks the chunk tree. The line is also auto-hidden
        /// for non-TTY stderr, when `--json` is set, and for
        /// single-chunk fetches where there's nothing to track.
        #[arg(long)]
        no_progress: bool,
        /// Progress renderer to use when stderr is a TTY. `line` is the
        /// compact default; `visual` draws a retro block map from the
        /// aggregate chunk counters the daemon streams today.
        #[arg(long, value_enum, default_value = "line")]
        progress_style: ProgressStyle,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProgressStyle {
    Line,
    Visual,
}

#[derive(Subcommand, Debug)]
enum PeersCommand {
    /// Drop the on-disk peerstore (`<data-dir>/peers.json`) and clear the
    /// in-memory dedup set. Existing connections stay up; the next restart
    /// will bootstrap fresh through the bootnodes.
    Reset,
}

fn main() -> Result<()> {
    let opt = Opt::parse();
    let socket = resolve_socket(&opt)?;

    match opt.command {
        Command::Status => {
            let snap = fetch_status(&socket)?;
            if opt.json {
                println!("{}", serde_json::to_string_pretty(&snap)?);
            } else {
                print_status(&snap);
            }
        }
        Command::Top { interval_ms } => {
            if opt.json {
                bail!("--json is not supported for `top`; use `antctl status --json` for machine-readable output");
            }
            run_top(&socket, interval_ms)?;
        }
        Command::Version => {
            let resp = request_sync(&socket, &Request::Version)
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            match resp {
                Response::Version(v) => {
                    if opt.json {
                        println!("{}", serde_json::to_string_pretty(&v)?);
                    } else {
                        println!(
                            "client: antctl/{}\nserver: {} (control protocol v{})",
                            env!("CARGO_PKG_VERSION"),
                            v.agent,
                            v.protocol_version,
                        );
                    }
                }
                Response::Error { message } => bail!("antd: {message}"),
                other => bail!("unexpected response: {other:?}"),
            }
        }
        Command::Peers {
            command: PeersCommand::Reset,
        } => {
            let resp = request_sync(&socket, &Request::PeersReset)
                .with_context(|| format!("talk to antd at {}", socket.display()))?;
            match resp {
                Response::Ok { message } => {
                    if opt.json {
                        println!("{}", serde_json::json!({ "ok": true, "message": message }));
                    } else {
                        println!("{message}");
                    }
                }
                Response::Error { message } => bail!("antd: {message}"),
                other => bail!("unexpected response: {other:?}"),
            }
        }
        Command::Get {
            reference,
            out,
            bytes,
            bzz,
            bzz_path,
            allow_degraded_redundancy,
            wait_peers,
            wait_timeout,
            bypass_cache,
            no_progress,
            progress_style,
        } => {
            if wait_peers > 0 {
                wait_for_peers(&socket, wait_peers, Duration::from_secs(wait_timeout));
            }
            let mode = FetchMode::from_flags(bytes, bzz, bzz_path.as_deref());
            // Single-chunk fetches return a single line; there's
            // nothing to stream, so don't bother asking the daemon.
            // For multi-chunk fetches, render a status line on stderr
            // unless the user opted out, the user wants JSON, or
            // stderr isn't a TTY (we'd otherwise mangle log capture).
            // URL schemes on the reference (`bzz://`, `bytes://`)
            // override the explicit flags, so peek at the reference
            // too — otherwise `antctl get bzz://...` would silently
            // skip progress because `mode` is `Chunk`.
            let multi_chunk = !matches!(mode, FetchMode::Chunk)
                || reference.starts_with("bzz://")
                || reference.starts_with("bytes://");
            let show_progress = !no_progress
                && !opt.json
                && multi_chunk
                && std::io::IsTerminal::is_terminal(&std::io::stderr());
            run_get(
                &socket,
                &reference,
                out.as_deref(),
                opt.json,
                mode,
                allow_degraded_redundancy,
                bypass_cache,
                show_progress,
                progress_style,
            )?;
        }
    }
    Ok(())
}

/// One of the three retrieval shapes `antctl get` knows how to dispatch.
/// Computed once from the command-line flags / URL scheme so the
/// downstream logic can stay flat.
#[derive(Debug, Clone)]
enum FetchMode {
    /// Single content-addressed chunk; daemon answers `Response::Bytes`.
    Chunk,
    /// Multi-chunk join; daemon answers `Response::Bytes` with the
    /// joined file body.
    Bytes,
    /// Manifest walk + join; daemon answers `Response::BzzBytes` with
    /// the joined body plus optional content-type / filename.
    Bzz { path: String },
}

impl FetchMode {
    /// Resolve a fetch mode from CLI flags. The reference itself is
    /// inspected separately by [`parse_reference_with_mode`] so a URL
    /// scheme on the reference can override these defaults.
    fn from_flags(bytes: bool, bzz: bool, bzz_path: Option<&str>) -> Self {
        if bzz {
            FetchMode::Bzz {
                path: bzz_path.unwrap_or("").to_string(),
            }
        } else if bytes {
            FetchMode::Bytes
        } else {
            FetchMode::Chunk
        }
    }
}

/// Final destination for the bytes pulled back from the daemon. Stdout
/// for raw bytes, optionally `--out` for a fixed path, or — if the
/// daemon hands us a `Filename` from manifest metadata and the user
/// passed `--out <dir>` rather than a file — `<dir>/<Filename>`.
fn write_output(
    out: Option<&Path>,
    filename_hint: Option<&str>,
    data: &[u8],
) -> Result<Option<PathBuf>> {
    use std::io::Write;
    if let Some(p) = out {
        // If `out` is an existing directory, append the manifest filename
        // (or fall back to "data.bin" if there isn't one). This lets
        // callers do `antctl get bzz://<ref>/ -o ./` and end up with
        // `./index.html` automatically.
        let target = if p.is_dir() {
            let name = filename_hint.unwrap_or("data.bin");
            p.join(name)
        } else {
            p.to_path_buf()
        };
        std::fs::write(&target, data).with_context(|| format!("write to {}", target.display()))?;
        return Ok(Some(target));
    }
    io::stdout().lock().write_all(data)?;
    Ok(None)
}

/// Resolve a CLI reference + mode into the daemon-side `Request`. URL
/// schemes (`bzz://…`, `bytes://…`) on the reference take precedence
/// over the explicit `--bzz` / `--bytes` flags; the flags fill in the
/// gap when the user hands over a bare hex reference.
fn parse_reference_with_mode(
    reference: &str,
    mode: FetchMode,
    allow_degraded_redundancy: bool,
    bypass_cache: bool,
    progress: bool,
) -> Result<Request> {
    if let Some(rest) = reference.strip_prefix("bzz://") {
        // Split optional path (after first '/').
        let (hex_part, path_part) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        return Ok(Request::GetBzz {
            reference: hex_part.to_string(),
            path: path_part.to_string(),
            allow_degraded_redundancy,
            bypass_cache,
            progress,
        });
    }
    if let Some(rest) = reference.strip_prefix("bytes://") {
        return Ok(Request::GetBytes {
            reference: rest.to_string(),
            bypass_cache,
            progress,
        });
    }
    Ok(match mode {
        FetchMode::Chunk => Request::Get {
            reference: reference.to_string(),
            bypass_cache,
            progress,
        },
        FetchMode::Bytes => Request::GetBytes {
            reference: reference.to_string(),
            bypass_cache,
            progress,
        },
        FetchMode::Bzz { path } => Request::GetBzz {
            reference: reference.to_string(),
            path,
            allow_degraded_redundancy,
            bypass_cache,
            progress,
        },
    })
}

/// Fetch a chunk / file and write it to `out` (or stdout). With `--json`,
/// prints a structured envelope on stdout regardless of mode, since
/// shells generally won't render binary cleanly.
///
/// `show_progress` opts the request into streaming
/// `Response::Progress` updates and renders an in-place stderr status
/// line. The caller decides whether progress makes sense (TTY check,
/// `--json`, single-chunk fetches, etc.) so this function can stay
/// focused on plumbing.
// Eight args is one over clippy's preferred ceiling, but every one of
// them is a CLI-driven knob with a distinct meaning, and bundling them
// into a struct just for clippy would obscure the call site below.
#[allow(clippy::too_many_arguments)]
fn run_get(
    socket: &Path,
    reference: &str,
    out: Option<&Path>,
    json: bool,
    mode: FetchMode,
    allow_degraded_redundancy: bool,
    bypass_cache: bool,
    show_progress: bool,
    progress_style: ProgressStyle,
) -> Result<()> {
    let req = parse_reference_with_mode(
        reference,
        mode,
        allow_degraded_redundancy,
        bypass_cache,
        show_progress,
    )?;

    let (resp, last_progress) = if show_progress {
        let mut renderer = ProgressRenderer::new(bypass_cache, progress_style);
        let resp = request_streaming(socket, &req, |p| renderer.render(p))
            .with_context(|| format!("talk to antd at {}", socket.display()))?;
        let last = renderer.finish();
        (resp, last)
    } else {
        (
            request_sync(socket, &req)
                .with_context(|| format!("talk to antd at {}", socket.display()))?,
            None,
        )
    };

    // Unify the three shapes into (data, content_type, filename).
    let (data, content_type, filename) = match resp {
        Response::Bytes { hex } => (decode_hex_payload(&hex)?, None, None),
        Response::BzzBytes {
            hex,
            content_type,
            filename,
        } => (decode_hex_payload(&hex)?, content_type, filename),
        Response::Error { message } => bail!("antd: {message}"),
        other => bail!("unexpected response: {other:?}"),
    };

    let written = write_output(out, filename.as_deref(), &data)?;

    if json {
        let mut payload = serde_json::json!({
            "ok": true,
            "reference": reference,
            "bytes": data.len(),
        });
        if let Some(ct) = &content_type {
            payload["content_type"] = serde_json::Value::String(ct.clone());
        }
        if let Some(fname) = &filename {
            payload["filename"] = serde_json::Value::String(fname.clone());
        }
        if let Some(p) = &written {
            payload["out"] = serde_json::Value::String(p.display().to_string());
        } else {
            payload["hex"] = serde_json::Value::String(format!("0x{}", hex::encode(&data)));
        }
        println!("{payload}");
    } else if let Some(p) = written {
        let mut msg = format!("wrote {} bytes to {}", data.len(), p.display());
        if let Some(ct) = &content_type {
            msg.push_str(&format!(" ({ct})"));
        }
        eprintln!("{msg}");
    } else if let Some(ct) = &content_type {
        // Stdout got the body; surface the content type on stderr so
        // shell pipelines like `antctl get bzz://<ref>/ | head` still
        // know what they got without polluting stdout.
        eprintln!("Content-Type: {ct}");
    }

    // Post-download stats summary. Only meaningful when streaming
    // progress was active (multi-chunk fetch on a TTY without
    // `--json` / `--no-progress`); otherwise we have nothing to
    // report and the existing one-line `wrote N bytes …` carries
    // its own narrative.
    if !json {
        if let Some(sample) = last_progress {
            eprintln!("{}", format_stats_summary(&sample, bypass_cache));
        }
    }
    Ok(())
}

/// EMA smoothing factor for the per-second download-rate readout.
/// 0.30 is a middle ground: visibly responsive when the rate jumps but
/// not so jumpy that a single fast/slow sample dominates the line.
const PROGRESS_EMA_ALPHA: f64 = 0.30;

/// Stateful renderer for `antctl get`'s per-line stderr progress
/// output. Holds the last sample we showed so we can compute an
/// instantaneous rate (with EMA smoothing) between consecutive
/// `GetProgress` events, plus the column count of the last line so
/// [`finish`](Self::finish) can erase it cleanly with `\r` + spaces.
struct ProgressRenderer {
    /// Selected stderr renderer for this request.
    style: ProgressStyle,
    /// Smoothed bytes/sec across the request. `None` until we have
    /// at least two samples to interpolate between.
    ema_bps: Option<f64>,
    /// Width of the most recently printed line, in display columns,
    /// so `finish` can wipe it with the right number of spaces.
    last_line_width: usize,
    /// Most recent sample received, so [`finish`](Self::finish) can
    /// hand it back to the caller for a post-download summary line.
    /// `None` if no progress event ever landed (single-chunk fetch
    /// or a request that errored before the first emitter tick).
    last_sample: Option<GetProgress>,
    /// Cached `bypass_cache` so we can prefix `[no-cache] ` even on
    /// the very first sample (where the in-flight `GetProgress` may
    /// still echo `false` if the daemon hasn't initialised the
    /// tracker yet — shouldn't happen in practice but guards us).
    bypass_cache_hint: bool,
}

impl ProgressRenderer {
    fn new(bypass_cache: bool, style: ProgressStyle) -> Self {
        Self {
            style,
            ema_bps: None,
            last_line_width: 0,
            last_sample: None,
            bypass_cache_hint: bypass_cache,
        }
    }

    /// Update the rate EMA and re-render the in-place status line.
    /// Called once per `Response::Progress` from the streaming client.
    fn render(&mut self, p: &GetProgress) {
        if let Some(prev) = &self.last_sample {
            let dt_ms = p.elapsed_ms.saturating_sub(prev.elapsed_ms);
            let dbytes = p.bytes_done.saturating_sub(prev.bytes_done);
            // Skip the EMA update for samples that produced no
            // progress (e.g. emitter ticked while the joiner was
            // stuck on a slow peer). Keeping the EMA stable is
            // friendlier than letting it trend toward zero on every
            // stalled tick.
            if dt_ms > 0 && dbytes > 0 {
                let bps = (dbytes as f64) * 1000.0 / dt_ms as f64;
                self.ema_bps = Some(match self.ema_bps {
                    Some(prev) => prev * (1.0 - PROGRESS_EMA_ALPHA) + bps * PROGRESS_EMA_ALPHA,
                    None => bps,
                });
            }
        }
        let previous = self.last_sample.as_ref();
        // `\r` returns to column 0; pad with spaces to overwrite any
        // trailing characters from the previous (longer) line, then
        // write the new content. Flush so the user sees updates even
        // when stderr is line-buffered.
        use std::io::Write as _;
        let mut err = std::io::stderr().lock();
        match self.style {
            ProgressStyle::Line => {
                let line = format_progress_line(p, self.ema_bps, self.bypass_cache_hint);
                let new_width = display_width(&line);
                let padding = self.last_line_width.saturating_sub(new_width);
                let _ = write!(err, "\r\x1b[2K{line}{}", " ".repeat(padding));
                self.last_line_width = new_width;
            }
            ProgressStyle::Visual => {
                let (bar, stats) = format_visual_progress_lines_for_width(
                    p,
                    previous,
                    self.ema_bps,
                    self.bypass_cache_hint,
                    terminal_width(),
                );
                let _ = write!(err, "\r\x1b[2K{bar}\n\r\x1b[2K{stats}\x1b[1A\r");
                self.last_line_width = 0;
            }
        }
        let _ = err.flush();
        self.last_sample = Some(p.clone());
    }

    /// Erase the in-place status line so subsequent stderr output
    /// (e.g. `wrote N bytes to <path>`) starts on a fresh column,
    /// and hand the most recent sample back to the caller for the
    /// post-download stats summary.
    fn finish(self) -> Option<GetProgress> {
        if self.last_sample.is_some() {
            use std::io::Write as _;
            let mut err = std::io::stderr().lock();
            match self.style {
                ProgressStyle::Line => {
                    let _ = write!(err, "\r\x1b[2K\r");
                }
                ProgressStyle::Visual => {
                    let _ = write!(err, "\r\x1b[2K\n\r\x1b[2K\x1b[1A\r");
                }
            }
            let _ = err.flush();
        }
        self.last_sample
    }
}

/// Render one `GetProgress` sample as a one-line stderr status. The
/// renderer trims to a "best summary" view rather than dumping every
/// counter; users wanting a structured feed can always re-run with
/// `--json` (which doesn't take this path).
fn format_progress_line(p: &GetProgress, ema_bps: Option<f64>, bypass_cache: bool) -> String {
    let chunks = if p.total_chunks_estimate > 0 {
        format!("{}/{}", p.chunks_done, p.total_chunks_estimate)
    } else {
        format!("{}/?", p.chunks_done)
    };
    let bytes = if p.total_bytes_estimate > 0 {
        format!(
            "{}/{}",
            format_bytes(p.bytes_done),
            format_bytes(p.total_bytes_estimate),
        )
    } else {
        format!("{}/?", format_bytes(p.bytes_done))
    };
    let rate = match ema_bps {
        Some(bps) if bps > 0.0 => format!("{}/s", format_bytes(bps as u64)),
        _ => "—/s".to_string(),
    };
    let elapsed = format_progress_elapsed(p.elapsed_ms);
    // Show the source mix even when the user didn't pass
    // `--bypass-cache`, so a populated cache doesn't "look slow"
    // simply because the joiner is hitting it instead of peers.
    let sources = format!(
        "{} peer{}{}",
        p.peers_used,
        if p.peers_used == 1 { "" } else { "s" },
        if p.cache_hits > 0 {
            format!(", {} cache", p.cache_hits)
        } else {
            String::new()
        },
    );
    let cache_tag = if bypass_cache { "[no-cache] " } else { "" };
    format!("{cache_tag}↓ {chunks} chunks  {bytes}  {rate}  {sources}  {elapsed}",)
}

/// Render a compact, defrag-style block map plus a separate stats line
/// from aggregate progress counters. The current control protocol does
/// not report exact chunk indexes, so the map visualizes
/// completed-vs-pending volume rather than claiming a physical chunk
/// layout.
fn format_visual_progress_lines_for_width(
    p: &GetProgress,
    previous: Option<&GetProgress>,
    ema_bps: Option<f64>,
    bypass_cache: bool,
    terminal_width: usize,
) -> (String, String) {
    const MAX_BLOCKS: usize = 72;
    let percent = if p.total_chunks_estimate > 0 {
        let capped = p.chunks_done.min(p.total_chunks_estimate);
        format!(
            "{:>5.1}%",
            capped as f64 * 100.0 / p.total_chunks_estimate as f64
        )
    } else {
        " ?.?%".to_string()
    };
    let chunks = if p.total_chunks_estimate > 0 {
        format!("{}/{}", p.chunks_done, p.total_chunks_estimate)
    } else {
        format!("{}/?", p.chunks_done)
    };
    let rate = match ema_bps {
        Some(bps) if bps > 0.0 => format!("{}/s", format_bytes(bps as u64)),
        _ => "--/s".to_string(),
    };
    let cache_tag = if bypass_cache { "[no-cache] " } else { "" };
    let stats = format!(
        "{cache_tag}{percent}  {chunks}ch  {rate}  {}p/{}c  {}",
        p.peers_used,
        p.cache_hits,
        format_progress_elapsed(p.elapsed_ms),
    );
    let blocks = terminal_width.saturating_sub(2).min(MAX_BLOCKS);

    let total = visual_total_chunks(p);
    let done_blocks = visual_done_blocks(p.chunks_done, total, blocks);
    let previous_done_blocks = previous
        .map(|prev| visual_done_blocks(prev.chunks_done, total, blocks))
        .unwrap_or(0)
        .min(done_blocks);
    let cache_blocks =
        visual_done_blocks(p.cache_hits.min(p.chunks_done), total, blocks).min(done_blocks);
    let scan = if blocks > 0 {
        ((p.elapsed_ms / 150) as usize) % blocks
    } else {
        0
    };

    let mut map = String::with_capacity(blocks);
    for i in 0..blocks {
        let ch = if i < done_blocks {
            if i >= previous_done_blocks && previous.is_some() {
                '▓'
            } else if i < cache_blocks {
                '▣'
            } else {
                '█'
            }
        } else if p.total_chunks_estimate == 0 && i == scan {
            '▒'
        } else {
            '░'
        };
        map.push(ch);
    }

    (format!("[{map}]"), stats)
}

fn visual_total_chunks(p: &GetProgress) -> u64 {
    if p.total_chunks_estimate > 0 {
        p.total_chunks_estimate
    } else {
        // Before the root span is known, reserve some pending space so
        // early samples still look like an in-flight map instead of a
        // completed tiny file.
        p.chunks_done.saturating_add(16).max(16)
    }
}

fn visual_done_blocks(chunks_done: u64, total_chunks: u64, blocks: usize) -> usize {
    if chunks_done == 0 || total_chunks == 0 || blocks == 0 {
        return 0;
    }
    let capped = chunks_done.min(total_chunks);
    ((capped * blocks as u64).div_ceil(total_chunks)) as usize
}

/// Single-line post-download summary printed once the in-place
/// progress line has been wiped. Pulls every interesting counter out
/// of the final `GetProgress` so the user can see, at a glance, how
/// the fetch went without scrolling back through live frames.
///
/// Format: `stats: 5 chunks (18.2KiB) in 312ms · avg 58.4KiB/s · 3 peers · 2 cache · cold`.
/// `cold` / `warm` is dropped when the daemon's cache wasn't
/// involved either way (no peers and no hits — should only happen on
/// a no-op fetch).
fn format_stats_summary(p: &GetProgress, bypass_cache_hint: bool) -> String {
    let chunks = if p.total_chunks_estimate > 0 && p.total_chunks_estimate != p.chunks_done {
        format!("{}/{} chunks", p.chunks_done, p.total_chunks_estimate)
    } else {
        format!(
            "{} chunk{}",
            p.chunks_done,
            if p.chunks_done == 1 { "" } else { "s" },
        )
    };
    let bytes = format_bytes(p.bytes_done);
    let elapsed = format_progress_elapsed(p.elapsed_ms);
    // Use cumulative average rather than the live EMA: the EMA was
    // useful while the user could read it, but the post-download
    // summary wants the deterministic "this is what just happened"
    // number — total bytes ÷ total seconds.
    let avg = if p.elapsed_ms > 0 {
        let bps = (p.bytes_done as f64) * 1000.0 / p.elapsed_ms as f64;
        format!("avg {}/s", format_bytes(bps as u64))
    } else {
        "avg —/s".to_string()
    };
    let peers = format!(
        "{} peer{}",
        p.peers_used,
        if p.peers_used == 1 { "" } else { "s" },
    );
    let cache = format!("{} cache", p.cache_hits);
    // Trust `bypass_cache` from the sample; fall back to the caller's
    // hint in case an old daemon ever lands a sample with the field
    // unset (defensive — shouldn't happen on v2).
    let mode = if p.bypass_cache || bypass_cache_hint {
        " · cold"
    } else if p.cache_hits > 0 || p.peers_used > 0 {
        " · warm"
    } else {
        ""
    };
    format!("stats: {chunks} ({bytes}) in {elapsed} · {avg} · {peers} · {cache}{mode}")
}

fn format_progress_elapsed(ms: u64) -> String {
    let secs = ms / 1000;
    let tenths = (ms % 1000) / 100;
    if secs >= 60 {
        let m = secs / 60;
        let s = secs % 60;
        format!("{m}m{s:02}s")
    } else {
        format!("{secs}.{tenths}s")
    }
}

/// Human-readable byte size: SI-ish powers of 1024 with a single decimal
/// for non-byte units. Kept inline to avoid a `humansize`-style dep just
/// for one stderr line.
fn format_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if n >= GIB {
        format!("{:.1}GiB", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.1}MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.1}KiB", n as f64 / KIB as f64)
    } else {
        format!("{n}B")
    }
}

/// Approximate display width in columns. Good enough for the progress
/// line, which is plain ASCII plus a few well-known progress glyphs
/// (`↓`, `█`, `░`); we treat each `char` as one column.
fn display_width(s: &str) -> usize {
    s.chars().count()
}

fn terminal_width() -> usize {
    terminal::size()
        .map(|(width, _)| width as usize)
        .unwrap_or(80)
        .max(20)
}

/// Decode a `0x`-prefixed (or bare) hex string from the daemon's
/// `Response::Bytes`. The daemon controls the format; we still validate
/// because a corrupted socket would otherwise silently drop bytes.
fn decode_hex_payload(s: &str) -> Result<Vec<u8>> {
    let stripped = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    hex::decode(stripped).context("decode response hex")
}

fn fetch_status(socket: &Path) -> Result<StatusSnapshot> {
    let resp = request_sync(socket, &Request::Status)
        .with_context(|| format!("talk to antd at {}", socket.display()))?;
    match resp {
        Response::Status(snap) => Ok(*snap),
        Response::Error { message } => bail!("antd: {message}"),
        other => bail!("unexpected response: {other:?}"),
    }
}

/// Block until the daemon reports `min_peers` libp2p peers AND at least
/// one completed BZZ handshake, or `timeout` elapses. Best-effort: any
/// transport hiccup is logged and ignored — the actual `Get` request
/// will surface a real error if the daemon stays unreachable.
///
/// Distinct from `peers.routing.size` (BZZ-handshaked peers in the
/// forwarding-Kademlia table): `connected + last_handshake.is_some()`
/// matches the matrix the daemon's `run_get_*` paths actually trip on
/// (`peers.is_empty()` from `routing.snapshot()`), so the bar is set
/// where the daemon needs it, not higher.
fn wait_for_peers(socket: &Path, min_peers: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut printed_waiting = false;
    loop {
        match fetch_status(socket) {
            Ok(snap) => {
                if snap.peers.last_handshake.is_some() && snap.peers.connected >= min_peers {
                    if printed_waiting {
                        eprintln!(
                            "antctl: routing ready ({} peers, {} connected)",
                            snap.peers.routing.size, snap.peers.connected,
                        );
                    }
                    return;
                }
                if !printed_waiting {
                    eprintln!(
                        "antctl: waiting for {} peers (currently {}, routing={})",
                        min_peers, snap.peers.connected, snap.peers.routing.size,
                    );
                    printed_waiting = true;
                }
            }
            Err(e) => {
                eprintln!("antctl: status poll failed (will retry): {e:#}");
            }
        }
        if Instant::now() >= deadline {
            eprintln!(
                "antctl: --wait-peers={} not reached in {:?}; issuing request anyway",
                min_peers, timeout,
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn run_top(socket: &Path, interval_ms: u64) -> Result<()> {
    let interval_ms = interval_ms.max(1);
    let refresh = Duration::from_millis(interval_ms);
    let mut terminal = TopTerminal::new()?;
    let mut next_refresh = Instant::now();
    let mut last_status: Option<StatusSnapshot> = None;
    let mut last_error: Option<String> = None;
    let mut page = TopPage::Nodes;
    let mut selection = NodeSelection::default();
    let mut bandwidth = BandwidthTracker::new();

    loop {
        if Instant::now() >= next_refresh {
            match fetch_status(socket) {
                Ok(snap) => {
                    bandwidth.observe(&snap.retrieval, Instant::now());
                    let idx = selection.current_index(&snap);
                    render_top(
                        terminal.terminal_mut(),
                        &snap,
                        socket,
                        interval_ms,
                        page,
                        idx,
                        &bandwidth,
                    )?;
                    last_status = Some(snap);
                    last_error = None;
                }
                Err(e) => {
                    let error = e.to_string();
                    render_top_error(terminal.terminal_mut(), socket, interval_ms, page, &error)?;
                    last_status = None;
                    last_error = Some(error);
                }
            }
            next_refresh = Instant::now() + refresh;
        }

        let timeout = next_refresh
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(200));
        if event::poll(timeout)? {
            let event = event::read()?;
            if should_quit(&event) {
                break;
            }
            let redraw = if let Some(next_page) = page.after_event(&event) {
                page = next_page;
                true
            } else {
                handle_selection_event(&event, &mut selection, last_status.as_ref())
                    || matches!(event, Event::Resize(_, _))
            };
            if redraw {
                if let Some(snap) = &last_status {
                    let idx = selection.current_index(snap);
                    render_top(
                        terminal.terminal_mut(),
                        snap,
                        socket,
                        interval_ms,
                        page,
                        idx,
                        &bandwidth,
                    )?;
                } else if let Some(error) = &last_error {
                    render_top_error(terminal.terminal_mut(), socket, interval_ms, page, error)?;
                }
            }
        }
    }
    Ok(())
}

/// Rolling bandwidth estimate derived from successive
/// `RetrievalInfo` snapshots.
///
/// The daemon publishes only cumulative `bytes_fetched_total`; the
/// instantaneous rate is `(b2 - b1) / (t2 - t1)`. We smooth that with
/// a simple EMA so a 100 ms cache-warm spike or a one-off lull
/// doesn't make the gauge flap, and remember the all-time peak so
/// the user can spot how close the current run came to saturating
/// the connection.
///
/// Counters reset to zero on daemon restart (the `Arc<RetrievalCounters>`
/// is fresh per process); we detect that as a backwards delta and
/// reset the EMA rather than reporting a giant negative rate.
struct BandwidthTracker {
    last_sample: Option<(u64, Instant)>,
    /// Smoothed bytes/sec. `None` until at least two samples have
    /// been observed and a non-zero delta has been computed.
    ema_bps: Option<f64>,
    /// All-time peak over a single sample window. Useful for "what's
    /// the best this daemon ever managed?" without scrolling the log.
    peak_bps: f64,
    peak_chunks: u64,
}

impl BandwidthTracker {
    /// EMA smoothing factor. 0.3 keeps the gauge responsive (a real
    /// step up to a new rate stabilises in ~3 samples) without
    /// jittering visibly between consecutive reads of an active
    /// stream.
    const ALPHA: f64 = 0.3;

    fn new() -> Self {
        Self {
            last_sample: None,
            ema_bps: None,
            peak_bps: 0.0,
            peak_chunks: 0,
        }
    }

    /// Fold one fresh snapshot into the rolling state. Cheap; safe
    /// to call on every refresh tick.
    fn observe(&mut self, info: &RetrievalInfo, now: Instant) {
        let bytes = info.bytes_fetched_total;
        if info.chunks_fetched_total > self.peak_chunks {
            self.peak_chunks = info.chunks_fetched_total;
        }
        let prev = self.last_sample;
        self.last_sample = Some((bytes, now));
        let Some((prev_bytes, prev_at)) = prev else {
            return;
        };
        // Daemon restart or counter wraparound: drop the EMA so we
        // don't sample a wildly negative delta as legitimate.
        if bytes < prev_bytes {
            self.ema_bps = None;
            return;
        }
        let dt = now.saturating_duration_since(prev_at).as_secs_f64();
        if dt <= 0.0 {
            return;
        }
        let rate = (bytes - prev_bytes) as f64 / dt;
        self.ema_bps = Some(match self.ema_bps {
            Some(prev) => Self::ALPHA * rate + (1.0 - Self::ALPHA) * prev,
            None => rate,
        });
        if rate > self.peak_bps {
            self.peak_bps = rate;
        }
    }

    fn current_bps(&self) -> Option<f64> {
        self.ema_bps
    }

    fn peak_bps(&self) -> f64 {
        self.peak_bps
    }
}

fn clamp_selection(selected: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        selected.min(len - 1)
    }
}

/// Rows shown in the Nodes table (pipeline view when the daemon populates it).
fn peer_list_len(s: &StatusSnapshot) -> usize {
    if !s.peers.peer_pipeline.is_empty() {
        s.peers.peer_pipeline.len()
    } else {
        s.peers.connected_peers.len()
    }
}

/// Position-pinned selection: the cursor sticks to the row index the user
/// navigated to, regardless of how peers churn underneath. Default is row 0
/// — when the user hasn't moved yet the cursor stays glued to the top, so
/// the visible header row never drifts as new peers stream in. Once the
/// user presses Up/Down the index becomes "sticky" on that line: peers
/// added or removed below don't move it, peers added or removed above
/// don't either (we honour the index as a coordinate, not an anchor on a
/// specific peer). The index is only clamped if the list shrinks past it.
#[derive(Default)]
struct NodeSelection {
    index: usize,
}

impl NodeSelection {
    fn current_index(&self, s: &StatusSnapshot) -> usize {
        let len = peer_list_len(s);
        if len == 0 {
            0
        } else {
            self.index.min(len - 1)
        }
    }

    fn move_by(&mut self, s: &StatusSnapshot, delta: isize) {
        let len = peer_list_len(s);
        if len == 0 {
            self.index = 0;
            return;
        }
        let cur = self.current_index(s) as isize;
        let next = (cur + delta).clamp(0, len as isize - 1) as usize;
        self.index = next;
    }
}

fn short_peer_id(id: &str) -> String {
    let c = id.chars().count();
    if c <= 9 {
        return id.to_string();
    }
    let pre: String = id.chars().take(4).collect();
    let suf: String = id
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{pre}…{suf}")
}

fn pipeline_state_label(st: PeerConnectionState) -> &'static str {
    match st {
        PeerConnectionState::Dialing => "Dialing",
        PeerConnectionState::Identifying => "Identifying",
        PeerConnectionState::Handshaking => "Handshaking",
        PeerConnectionState::Ready => "Ready",
        PeerConnectionState::Failed => "Failed",
        PeerConnectionState::Closing => "Closing",
    }
}

/// Format a `ready_in_ms` value for the Nodes table. `<1 s` is shown
/// in milliseconds (`312 ms`), `<100 s` in tenths of a second
/// (`3.4 s`), and longer durations in whole seconds (`120 s`). The
/// resulting strings are at most 7 characters, so they fit the
/// 8-column `ready in` field with breathing room.
fn format_ready_in_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms} ms")
    } else if ms < 100_000 {
        let secs = ms as f64 / 1_000.0;
        format!("{secs:.1} s")
    } else {
        format!("{} s", ms / 1_000)
    }
}

/// Sort key for `antctl top`'s Nodes table: slowest `ready_in_ms` first,
/// peers that haven't reached `Ready` yet pushed to the bottom in the
/// daemon's existing arrival order. Returns indices into
/// `s.peers.peer_pipeline`; both the table and the detail panel must
/// route through this so the cursor row and the details stay in sync.
fn pipeline_view_order(s: &StatusSnapshot) -> Vec<usize> {
    let mut order: Vec<usize> = (0..s.peers.peer_pipeline.len()).collect();
    order.sort_by(|&a, &b| {
        let pa = &s.peers.peer_pipeline[a];
        let pb = &s.peers.peer_pipeline[b];
        match (pa.ready_in_ms, pb.ready_in_ms) {
            (Some(x), Some(y)) => y.cmp(&x).then(a.cmp(&b)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.cmp(&b),
        }
    });
    order
}

fn ready_count_for_gauge(s: &StatusSnapshot) -> u32 {
    if !s.peers.peer_pipeline.is_empty() {
        s.peers
            .peer_pipeline
            .iter()
            .filter(|p| p.state == PeerConnectionState::Ready)
            .count() as u32
    } else {
        s.peers.connected_peers.len() as u32
    }
}

/// Returns true if the event was consumed and the caller should redraw.
fn handle_selection_event(
    event: &Event,
    selection: &mut NodeSelection,
    status: Option<&StatusSnapshot>,
) -> bool {
    let Some(snap) = status else { return false };
    if peer_list_len(snap) == 0 {
        return false;
    }
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
            KeyCode::Up => {
                selection.move_by(snap, -1);
                true
            }
            KeyCode::Down => {
                selection.move_by(snap, 1);
                true
            }
            _ => false,
        },
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TopPage {
    Nodes,
    Retrieval,
    Status,
}

impl TopPage {
    const ALL: [TopPage; 3] = [TopPage::Nodes, TopPage::Retrieval, TopPage::Status];

    fn index(self) -> usize {
        Self::ALL.iter().position(|p| *p == self).unwrap_or(0)
    }

    fn title(self) -> &'static str {
        match self {
            TopPage::Nodes => "Nodes",
            TopPage::Retrieval => "Retrieval",
            TopPage::Status => "Status",
        }
    }

    fn previous(self) -> Self {
        let idx = self.index();
        Self::ALL[(idx + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    fn next(self) -> Self {
        let idx = self.index();
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    fn after_event(self, event: &Event) -> Option<Self> {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Left => Some(self.previous()),
                KeyCode::Right => Some(self.next()),
                _ => None,
            },
            _ => None,
        }
    }
}

struct TopTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TopTerminal {
    fn new() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(e) = execute!(stdout, EnterAlternateScreen, Hide) {
            let _ = terminal::disable_raw_mode();
            return Err(e.into());
        }
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal })
    }

    fn terminal_mut(&mut self) -> &mut Terminal<CrosstermBackend<Stdout>> {
        &mut self.terminal
    }
}

impl Drop for TopTerminal {
    fn drop(&mut self) {
        let _ = execute!(
            self.terminal.backend_mut(),
            Show,
            LeaveAlternateScreen,
            ResetColor
        );
        let _ = terminal::disable_raw_mode();
    }
}

fn should_quit(event: &Event) -> bool {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            matches!(
                key.code,
                KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc
            ) || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        }
        _ => false,
    }
}

fn resolve_socket(opt: &Opt) -> Result<PathBuf> {
    if let Some(p) = &opt.socket {
        return Ok(expand_tilde(p));
    }
    let dir = expand_tilde(&opt.data_dir);
    Ok(dir.join("antd.sock"))
}

fn expand_tilde(p: &Path) -> PathBuf {
    if let Some(s) = p.to_str() {
        if let Some(rest) = s.strip_prefix("~/") {
            return dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(rest);
        }
    }
    p.to_path_buf()
}

fn print_status(s: &StatusSnapshot) {
    let now = current_unix();
    let uptime = now.saturating_sub(s.started_at_unix);

    println!(
        "{}  (pid {}, up {})",
        s.agent,
        s.pid,
        format_duration(uptime),
    );
    println!();
    println!("Network:");
    println!(
        "  Network ID:  {}{}",
        s.network_id,
        if s.network_id == 1 { " (mainnet)" } else { "" },
    );
    println!("  ETH:         {}", s.identity.eth_address);
    println!("  Overlay:     {}", s.identity.overlay);
    println!("  Peer ID:     {}", s.identity.peer_id);
    if s.listeners.is_empty() {
        println!("  Listeners:   (none yet)");
    } else {
        println!("  Listeners:");
        for l in &s.listeners {
            println!("    - {l}");
        }
    }
    println!();
    println!("Peers:");
    println!("  Connected:   {}", s.peers.connected);
    if let Some(h) = &s.peers.last_handshake {
        let age = now.saturating_sub(h.at_unix);
        let agent = if h.agent_version.is_empty() {
            "(unknown agent)".to_string()
        } else {
            h.agent_version.clone()
        };
        println!(
            "  Last BZZ:    {} ({}, {} ago){}",
            h.remote_overlay,
            agent,
            format_duration(age),
            if h.full_node { ", full-node" } else { "" },
        );
    } else {
        println!("  Last BZZ:    (none yet)");
    }
    println!();
    println!("Control:");
    println!("  Socket:      {}", s.control_socket);
}

#[allow(clippy::too_many_arguments)]
fn render_top(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    s: &StatusSnapshot,
    socket: &Path,
    interval_ms: u64,
    page: TopPage,
    selected_node: usize,
    bandwidth: &BandwidthTracker,
) -> Result<()> {
    let now = current_unix();
    let uptime = now.saturating_sub(s.started_at_unix);
    let ctx = TopFrameContext {
        status: s,
        socket,
        interval_ms,
        page,
        selected_node,
        now,
        uptime,
        bandwidth,
    };
    terminal.draw(|frame| {
        draw_top_frame(frame, &ctx);
    })?;
    Ok(())
}

struct TopFrameContext<'a> {
    status: &'a StatusSnapshot,
    socket: &'a Path,
    interval_ms: u64,
    page: TopPage,
    selected_node: usize,
    now: u64,
    uptime: u64,
    bandwidth: &'a BandwidthTracker,
}

fn render_top_error(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    socket: &Path,
    interval_ms: u64,
    page: TopPage,
    error: &str,
) -> Result<()> {
    terminal.draw(|frame| {
        draw_error_frame(frame, socket, interval_ms, page, error);
    })?;
    Ok(())
}

fn draw_top_frame(frame: &mut Frame, ctx: &TopFrameContext<'_>) {
    let area = frame.area();
    if area.width < 50 || area.height < 16 {
        draw_too_small(frame, area);
        return;
    }

    let [header, tabs, body, footer] = vertical_chunks(
        area,
        [
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ],
    );
    draw_header(
        frame,
        header,
        &format!(
            "antctl top | {} | pid {} | up {} ",
            ctx.status.agent,
            ctx.status.pid,
            format_duration(ctx.uptime)
        ),
    );
    draw_bar(frame, footer, " q: quit ");
    draw_tabs(frame, tabs, ctx.page);

    let network_id = format!(
        "{}{}",
        ctx.status.network_id,
        if ctx.status.network_id == 1 {
            " (mainnet)"
        } else {
            ""
        }
    );
    match ctx.page {
        TopPage::Nodes => {
            draw_nodes_page(frame, body, ctx.status, ctx.now, ctx.selected_node);
        }
        TopPage::Retrieval => {
            draw_retrieval_page(frame, body, ctx);
        }
        TopPage::Status => {
            let rows = vec![
                kv("Process", ""),
                kv("Agent", &ctx.status.agent),
                kv("PID", &ctx.status.pid.to_string()),
                kv("Uptime", &format_duration(ctx.uptime)),
                kv("Refresh", &format!("{}ms", ctx.interval_ms)),
                kv("Updated", &ctx.now.to_string()),
                kv("", ""),
                kv("Network", ""),
                kv("Network", &network_id),
                kv("ETH", &ctx.status.identity.eth_address),
                kv("Overlay", &ctx.status.identity.overlay),
                kv("Peer ID", &ctx.status.identity.peer_id),
                kv("Connected", &ctx.status.peers.connected.to_string()),
                kv("Listeners", &ctx.status.listeners.len().to_string()),
                kv("", ""),
                kv("Control", ""),
                kv("Socket", &ctx.status.control_socket),
                kv("Requested", &ctx.socket.display().to_string()),
                kv("Protocol", &format!("v{}", ctx.status.protocol_version)),
            ];
            draw_panel(frame, body, ctx.page.title(), &rows);
        }
    }
}

fn draw_error_frame(
    frame: &mut Frame,
    socket: &Path,
    interval_ms: u64,
    page: TopPage,
    error: &str,
) {
    let area = frame.area();
    if area.width < 50 || area.height < 16 {
        draw_too_small(frame, area);
        return;
    }
    let [header, tabs, body, footer] = vertical_chunks(
        area,
        [
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ],
    );
    draw_header(
        frame,
        header,
        &format!("antctl top | disconnected | refresh {interval_ms}ms "),
    );
    draw_bar(frame, footer, " q: quit ");
    draw_tabs(frame, tabs, page);
    match page {
        TopPage::Nodes => draw_disconnected_nodes_page(frame, body, error),
        TopPage::Retrieval | TopPage::Status => {
            let rows = [
                kv("Daemon", "disconnected"),
                kv("Socket", &socket.display().to_string()),
                kv("Error", error),
                kv("Retry", "polling"),
                kv("Refresh", &format!("{interval_ms}ms")),
            ];
            draw_panel(frame, body, page.title(), &rows);
        }
    }
}

fn draw_too_small(frame: &mut Frame, area: TuiRect) {
    let text = Paragraph::new(
        "Terminal too small for antctl top.\nUse at least 50 columns x 16 rows. Press q to quit.",
    )
    .style(Style::default().bg(Color::Blue).fg(Color::Yellow));
    frame.render_widget(text, area);
}

fn draw_bar(frame: &mut Frame, area: TuiRect, text: &str) {
    frame.render_widget(
        Paragraph::new(fit(text, area.width as usize)).style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        area,
    );
}

fn draw_header(frame: &mut Frame, area: TuiRect, text: &str) {
    let prefix = " Ant | ";
    let text_width = area.width.saturating_sub(prefix.chars().count() as u16) as usize;
    let line = Line::from(vec![
        Span::styled(
            prefix,
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            fit(text, text_width),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(Color::Blue)),
        area,
    );
}

fn draw_tabs(frame: &mut Frame, area: TuiRect, page: TopPage) {
    let titles: Vec<Line> = TopPage::ALL
        .iter()
        .map(|p| Line::from(format!(" {} ", p.title())))
        .collect();
    let tabs = Tabs::new(titles)
        .select(page.index())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Plain)
                .border_style(Style::default().fg(Color::Cyan))
                .style(Style::default().bg(Color::Blue)),
        )
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .highlight_style(
            Style::default()
                .bg(Color::Cyan)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, area);
}

fn draw_panel(frame: &mut Frame, area: TuiRect, title: &str, rows: &[PanelRow]) {
    let lines: Vec<Line> = rows
        .iter()
        .map(|row| {
            Line::from(vec![
                Span::styled(
                    fit(&row.key, 10),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
                Span::styled(row.value.clone(), Style::default().fg(row.color)),
            ])
        })
        .collect();
    let block = Block::default()
        .title(format!(" {title} "))
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue));
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(Color::Blue).fg(Color::White))
            .block(block),
        area,
    );
}

/// Lay out the Retrieval tab: a 7-row metrics panel at the top
/// (cache, in-flight, bandwidth) and the gateway-requests table
/// underneath. The panel needs a fixed height so the table reflows
/// gracefully when the terminal is short — without it, ratatui would
/// hand the table all the slack and clip the metrics readout.
fn draw_retrieval_page(frame: &mut Frame, area: TuiRect, ctx: &TopFrameContext<'_>) {
    let r = &ctx.status.retrieval;
    let [metrics_area, requests_area] = vertical_chunks(
        area,
        // 9 rows = top border (1) + 6 panel rows + bottom border (1) +
        // 1 padding. The two cache rows (Mem / Disk) plus In-flight,
        // Bandwidth, Totals, Requests give us six.
        [Constraint::Length(9), Constraint::Min(0)],
    );
    draw_retrieval_metrics(frame, metrics_area, r, ctx.bandwidth);
    draw_gateway_requests(frame, requests_area, r, ctx.now);
}

/// Top-of-tab metrics readout: per-tier cache fill, in-flight count,
/// and bandwidth. Uses the same `draw_panel` helper as the Status tab
/// so the colour palette matches.
///
/// The disk-cache row degrades gracefully: when the daemon was
/// started without a persistent cache (`--no-disk-cache` or open
/// failure) the row reads `disabled` rather than `0/0 (0.0%)`. That
/// way operators can tell at a glance whether the second tier is
/// even wired up — important when diagnosing "I configured a disk
/// cache and nothing got faster on restart".
fn draw_retrieval_metrics(
    frame: &mut Frame,
    area: TuiRect,
    r: &RetrievalInfo,
    bandwidth: &BandwidthTracker,
) {
    let mem_cache = format!(
        "{}/{} chunks ({})  · {} hits",
        r.cache.used,
        r.cache.capacity,
        format_percentage(r.cache.used as u64, r.cache.capacity as u64),
        format_count(r.mem_hits_total),
    );
    let disk_cache = if r.disk.enabled {
        format!(
            "{} / {} ({})  · {} hits",
            format_bytes(r.disk.used_bytes),
            format_bytes(r.disk.capacity_bytes),
            format_percentage(r.disk.used_bytes, r.disk.capacity_bytes),
            format_count(r.disk.hits_total),
        )
    } else {
        "disabled".to_string()
    };
    let in_flight = format!("{}/{} chunks", r.in_flight, r.in_flight_capacity);
    let totals = format!(
        "{} fetched · {} ({} cache hits)",
        format_count(r.chunks_fetched_total),
        format_bytes(r.bytes_fetched_total),
        format_count(r.cache_hits_total),
    );
    let bw_now = bandwidth
        .current_bps()
        .map(format_byte_rate)
        .unwrap_or_else(|| "—".to_string());
    let bw_peak = if bandwidth.peak_bps() > 0.0 {
        format_byte_rate(bandwidth.peak_bps())
    } else {
        "—".to_string()
    };
    let bandwidth_row = format!("{bw_now}  (peak {bw_peak})");

    let rows = vec![
        kv("Mem cache", &mem_cache),
        kv("Disk cache", &disk_cache),
        kv("In-flight", &in_flight),
        kv("Bandwidth", &bandwidth_row),
        kv("Totals", &totals),
        kv("Requests", &r.gateway_requests.len().to_string()),
    ];
    draw_panel(frame, area, "Retrieval", &rows);
}

/// Minimum widths for the fixed columns of the gateway-requests
/// table. The `Path` column is rendered with a `Min` constraint
/// (see [`gateway_constraints`]) so it absorbs all leftover
/// horizontal space — long bzz paths are visible in full on a wide
/// terminal but the table still renders cleanly at the 50-column
/// floor enforced by `draw_top_frame`.
///
/// Each width is `max(header, an upper bound on the longest cell
/// ever rendered)`:
///
/// * `Kind` — up to 5-char label (`Bytes` / `BZZ` / `Chunk` /
///   `Manif`); 5 fits all of them and the header.
/// * `Age` — `format_duration` emits `<60s` / `Mm Ss` / `Hh Mm Ss`;
///   8 fits up to ~99 hours of uptime before clipping.
/// * `Done` — `chunks_done/total_chunks` with up to ~6-digit values
///   each, plus the `/` separator (~14).
/// * `Infl` — `chunks_in_flight` is bounded by
///   `RETRIEVAL_REQUEST_INFLIGHT_CAP` (≤3 digits today).
/// * `Bytes` — `format_bytes` always emits `<7 chars` (`9.9GiB`).
const GATEWAY_FIXED_WIDTHS: [u16; 5] = [5, 8, 14, 6, 12];
/// Floor on the `Path` column so the header stays legible even when
/// every other column is fully expanded. Anything beyond this is
/// growth space the column claims via `Constraint::Min`.
const GATEWAY_PATH_MIN_WIDTH: u16 = 22;
const GATEWAY_HEADER: [&str; 6] = ["Kind", "Path", "Age", "Done", "Infl", "Bytes"];

/// Build the per-column constraint set. `Path` gets `Min` so any
/// horizontal slack the table area has after the fixed columns and
/// inter-column spacing goes to it; the other five columns stay at
/// their constant `Length` so numbers don't bounce around as the
/// terminal resizes.
fn gateway_constraints() -> [Constraint; 6] {
    [
        Constraint::Length(GATEWAY_FIXED_WIDTHS[0]),
        Constraint::Min(GATEWAY_PATH_MIN_WIDTH),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[1]),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[2]),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[3]),
        Constraint::Length(GATEWAY_FIXED_WIDTHS[4]),
    ]
}

fn draw_gateway_requests(
    frame: &mut Frame,
    area: TuiRect,
    r: &RetrievalInfo,
    now: u64,
) {
    let block = Block::default()
        .title(" Gateway requests ")
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue));

    let header = Row::new(GATEWAY_HEADER.to_vec()).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    // Trim to whatever fits in the available rows. The body grows
    // top-down (oldest request first); when more requests arrive
    // than fit we drop the newest ones rather than older ones —
    // older requests are the ones likely to need attention if
    // they're hung.
    let visible_rows = area.height.saturating_sub(3).max(1) as usize;
    let cells: Vec<Vec<String>> = if r.gateway_requests.is_empty() {
        vec![vec![
            "—".to_string(),
            "(idle)".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ]]
    } else {
        r.gateway_requests
            .iter()
            .take(visible_rows)
            .map(|req| gateway_request_cells(req, now))
            .collect()
    };

    let rows: Vec<Row> = cells.into_iter().map(Row::new).collect();
    let table = Table::new(rows, gateway_constraints())
        .header(header)
        .column_spacing(2)
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .block(block);
    frame.render_widget(table, area);
}

/// Build the cell strings for one row. The `Path` cell is *not*
/// truncated here — ratatui clips it to the rendered column width,
/// which lets the column expand to show the full path on wide
/// terminals.
fn gateway_request_cells(req: &GatewayRequestInfo, now: u64) -> Vec<String> {
    let age_secs = now.saturating_sub(req.started_at_unix);
    let done = if req.total_chunks_estimate > 0 {
        format!("{}/{}", req.chunks_done, req.total_chunks_estimate)
    } else {
        format!("{}", req.chunks_done)
    };
    vec![
        req.kind.label().to_string(),
        req.path.clone(),
        format_duration(age_secs),
        done,
        req.chunks_in_flight.to_string(),
        format_bytes(req.bytes_done),
    ]
}

/// Compact "X / max" percentage shown next to the cache fill in the
/// Retrieval metrics panel. Returns `0%` for `max == 0` rather than
/// dividing by zero (defensive — capacity is always non-zero in the
/// daemon, but a misbehaving fixture or future config could ship a
/// `0` value).
fn format_percentage(used: u64, max: u64) -> String {
    if max == 0 {
        return "0%".to_string();
    }
    let pct = (used as f64 / max as f64) * 100.0;
    format!("{pct:.0}%")
}

/// SI-ish chunk count: `1234` → `1.2K`, `1_234_567` → `1.2M`. Keeps
/// the totals column from sprawling when a busy daemon has fetched
/// millions of chunks across uptime.
fn format_count(n: u64) -> String {
    const K: u64 = 1_000;
    const M: u64 = 1_000_000;
    const G: u64 = 1_000_000_000;
    if n >= G {
        format!("{:.1}G", n as f64 / G as f64)
    } else if n >= M {
        format!("{:.1}M", n as f64 / M as f64)
    } else if n >= K {
        format!("{:.1}K", n as f64 / K as f64)
    } else {
        n.to_string()
    }
}

/// Bytes-per-second pretty printer for the bandwidth gauge.
/// Mirrors `format_bytes` but always emits `…/s` — even at 0 B/s
/// to make it obvious the field is a rate.
fn format_byte_rate(bps: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    if bps >= GIB {
        format!("{:.2} GiB/s", bps / GIB)
    } else if bps >= MIB {
        format!("{:.2} MiB/s", bps / MIB)
    } else if bps >= KIB {
        format!("{:.1} KiB/s", bps / KIB)
    } else {
        format!("{:.0} B/s", bps)
    }
}

fn draw_nodes_page(
    frame: &mut Frame,
    area: TuiRect,
    s: &StatusSnapshot,
    now: u64,
    selected: usize,
) {
    let [progress_area, table_area, detail_area] = vertical_chunks(
        area,
        [
            Constraint::Length(4),
            Constraint::Percentage(58),
            Constraint::Percentage(42),
        ],
    );
    draw_connected_progress(frame, progress_area, s);
    draw_nodes_table(frame, table_area, s, selected);
    draw_node_details(frame, detail_area, s, now, selected);
}

/// `Connected` progress bar + cold-start timing line (one bordered box, `antctl top`).
fn draw_connected_progress(frame: &mut Frame, area: TuiRect, s: &StatusSnapshot) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default().bg(Color::Blue));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [bar_area, time_area] =
        vertical_chunks(inner, [Constraint::Length(1), Constraint::Length(1)]);

    let limit = s.peers.node_limit.max(1);
    let connected = ready_count_for_gauge(s).min(limit);
    let ratio = connected as f64 / limit as f64;
    draw_gauge_line(frame, bar_area, &progress_label(connected, limit), ratio);

    let m = peer_milestone_line(s);
    let time_line = if m.is_empty() {
        Line::default()
    } else {
        Line::from(Span::styled(
            format!(" {m} "),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ))
    };
    frame.render_widget(
        Paragraph::new(time_line).style(Style::default().bg(Color::Blue).fg(Color::White)),
        time_area,
    );
}

/// Populated as milestones occur (`antd` p2p loop); only non-empty `Option`s are shown.
fn peer_milestone_line(s: &StatusSnapshot) -> String {
    let n = s.peers.node_limit.max(1);
    let mut parts = Vec::new();
    if let Some(t) = s.peers.time_to_first_peer_s {
        parts.push(format!("Time to first peer: {:.3}s", t));
    }
    if let Some(t) = s.peers.time_to_node_limit_s {
        parts.push(format!("Time to {n} nodes: {:.3}s", t));
    }
    parts.join("  ")
}

fn draw_gauge_line(frame: &mut Frame, area: TuiRect, label: &str, ratio: f64) {
    let prefix = format!(" Connected: {label} ");
    let bar_width = area.width.saturating_sub(prefix.chars().count() as u16 + 3) as usize;
    let filled = ((bar_width as f64 * ratio).round() as usize).min(bar_width);
    let empty = bar_width - filled;
    let line = Line::from(vec![
        Span::styled(
            prefix,
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "[",
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "█".repeat(filled),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            " ".repeat(empty),
            Style::default()
                .bg(Color::Blue)
                .fg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "] ",
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(Color::Blue)),
        area,
    );
}

fn progress_label(connected: u32, limit: u32) -> String {
    let width = limit.to_string().len();
    format!("{connected:0width$}/{limit}", width = width)
}

fn draw_disconnected_nodes_page(frame: &mut Frame, area: TuiRect, error: &str) {
    let [progress_area, table_area, detail_area] = vertical_chunks(
        area,
        [
            Constraint::Length(4),
            Constraint::Percentage(58),
            Constraint::Percentage(42),
        ],
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default().bg(Color::Blue));
    let inner = block.inner(progress_area);
    frame.render_widget(block, progress_area);
    let [bar_area, time_area] =
        vertical_chunks(inner, [Constraint::Length(1), Constraint::Length(1)]);
    draw_gauge_line(frame, bar_area, "000/???", 0.0);
    let waiting = Line::from(Span::styled(
        " (waiting for antd) ",
        Style::default()
            .bg(Color::Blue)
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(
        Paragraph::new(waiting).style(Style::default().bg(Color::Blue).fg(Color::White)),
        time_area,
    );
    draw_empty_nodes_table(frame, table_area);
    let rows = [
        kv("Daemon", "disconnected"),
        kv("Nodes", "(waiting for antd)"),
        kv("Error", error),
    ];
    draw_panel(frame, detail_area, "Details", &rows);
}

const NODES_HEADER: [&str; 7] = [
    "Peer ID", "State", "Ready in", "IP", "Type", "Agent", "Version",
];
/// Gap between adjacent columns; `column_spacing(2)` so each column gets
/// exactly two spaces of breathing room before the next one starts.
const NODES_COLUMN_SPACING: u16 = 2;
/// Fixed column widths picked once at startup so the table doesn't reflow
/// between frames. Each width is `max(header, an upper bound on the longest
/// cell ever rendered)`:
///
/// * `Peer ID` — `short_peer_id` always emits `XXXX…YYYY` (9 chars); 10
///   leaves room for the `(none yet)` placeholder when the daemon has no
///   peers yet.
/// * `State` — longest pipeline label is `Identifying`/`Handshaking` (11).
/// * `Ready in` — `format_ready_in_ms` emits `<999 ms` (3 ascii digits +
///   space + unit) or `<99.9 s` (4 ascii chars + space + unit), so 8
///   covers both plus the header.
/// * `IP` — fits IPv4 `255.255.255.255` (15); rarer IPv6 / DNS values clip.
/// * `Type` — `light` is the longest of `full` / `light` / `-` (5).
/// * `Agent` — `bee` is 3; allow up to 8 for future clients without reflow.
/// * `Version` — `10.20.30` (8) covers any plausible semver core.
const NODES_COLUMN_WIDTHS: [u16; 7] = [10, 11, 8, 15, 5, 8, 8];

fn nodes_table_block() -> Block<'static> {
    Block::default()
        .title(" Nodes ")
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue))
}

fn nodes_header_row() -> Row<'static> {
    Row::new(NODES_HEADER.to_vec()).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )
}

fn node_kind_label(full: Option<bool>) -> &'static str {
    match full {
        Some(true) => "full",
        Some(false) => "light",
        None => "-",
    }
}

/// Split a libp2p Identify `agent_version` into a short name and a clean
/// semver. Bee advertises `bee/2.7.1-61fab37b go1.25.2 linux/amd64`, so we
/// take the part before the first `/` as the agent name and the leading
/// numeric-dot run after it as the version (dropping the `-<gitsha>` and any
/// `go<x>/<arch>` trailers).
fn parse_agent_version(raw: &str) -> (String, String) {
    if raw.is_empty() {
        return (String::new(), String::new());
    }
    let (name, rest) = match raw.split_once('/') {
        Some((n, r)) => (n.trim(), r),
        None => return (raw.trim().to_string(), String::new()),
    };
    let version_field = rest.split_whitespace().next().unwrap_or("");
    let version: String = version_field
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let version = version.trim_end_matches('.').to_string();
    let version = if version.is_empty() {
        version_field.to_string()
    } else {
        version
    };
    (name.to_string(), version)
}

fn dash_if_empty(s: &str) -> String {
    if s.is_empty() {
        "-".to_string()
    } else {
        s.to_string()
    }
}

fn nodes_constraints() -> [Constraint; 7] {
    [
        Constraint::Length(NODES_COLUMN_WIDTHS[0]),
        Constraint::Length(NODES_COLUMN_WIDTHS[1]),
        Constraint::Length(NODES_COLUMN_WIDTHS[2]),
        Constraint::Length(NODES_COLUMN_WIDTHS[3]),
        Constraint::Length(NODES_COLUMN_WIDTHS[4]),
        Constraint::Length(NODES_COLUMN_WIDTHS[5]),
        Constraint::Length(NODES_COLUMN_WIDTHS[6]),
    ]
}

fn draw_empty_nodes_table(frame: &mut Frame, area: TuiRect) {
    let rows = vec![Row::new(vec![
        "(waiting…)".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
    ])];
    let table = Table::new(rows, nodes_constraints())
        .header(nodes_header_row())
        .column_spacing(NODES_COLUMN_SPACING)
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .block(nodes_table_block());
    frame.render_widget(table, area);
}

fn draw_nodes_table(frame: &mut Frame, area: TuiRect, s: &StatusSnapshot, selected: usize) {
    let visible_rows = area.height.saturating_sub(3).max(1) as usize;
    let total = peer_list_len(s);
    let selected = clamp_selection(selected, total);
    let start = if total <= visible_rows {
        0
    } else if selected >= visible_rows {
        selected + 1 - visible_rows
    } else {
        0
    };
    let end = (start + visible_rows).min(total);

    // Pipeline rows carry only peer_id / state / ip; type and agent live on
    // `connected_peers`, so index by id once and look up per row.
    let by_id: HashMap<&str, &PeerConnectionInfo> = s
        .peers
        .connected_peers
        .iter()
        .map(|p| (p.peer_id.as_str(), p))
        .collect();

    let cells: Vec<Vec<String>> = if total == 0 {
        vec![vec![
            "(none yet)".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ]]
    } else if !s.peers.peer_pipeline.is_empty() {
        let order = pipeline_view_order(s);
        (start..end)
            .map(|idx| {
                let p = &s.peers.peer_pipeline[order[idx]];
                let conn = by_id.get(p.peer_id.as_str()).copied();
                let (agent, version) =
                    parse_agent_version(conn.map(|c| c.agent_version.as_str()).unwrap_or(""));
                let ready_in = match p.ready_in_ms {
                    Some(ms) => format_ready_in_ms(ms),
                    None => "-".to_string(),
                };
                vec![
                    short_peer_id(&p.peer_id),
                    pipeline_state_label(p.state).to_string(),
                    ready_in,
                    dash_if_empty(&p.ip),
                    node_kind_label(conn.and_then(|c| c.full_node)).to_string(),
                    dash_if_empty(&agent),
                    dash_if_empty(&version),
                ]
            })
            .collect()
    } else {
        (start..end)
            .map(|idx| {
                let peer = &s.peers.connected_peers[idx];
                let status = if peer.bzz_overlay.is_some() {
                    "Ready"
                } else {
                    "Handshaking"
                };
                let (agent, version) = parse_agent_version(&peer.agent_version);
                vec![
                    short_peer_id(&peer.peer_id),
                    status.to_string(),
                    "-".to_string(),
                    status_addr_ip(peer),
                    node_kind_label(peer.full_node).to_string(),
                    dash_if_empty(&agent),
                    dash_if_empty(&version),
                ]
            })
            .collect()
    };

    let highlight = Style::default()
        .bg(Color::Cyan)
        .fg(Color::Black)
        .add_modifier(Modifier::BOLD);

    let rows: Vec<Row> = cells
        .into_iter()
        .enumerate()
        .map(|(i, c)| {
            // `i` indexes into the visible window; offset by `start` to map
            // back onto the absolute selection cursor.
            let row = Row::new(c);
            if total > 0 && start + i == selected {
                row.style(highlight)
            } else {
                row
            }
        })
        .collect();

    let table = Table::new(rows, nodes_constraints())
        .header(nodes_header_row())
        .column_spacing(NODES_COLUMN_SPACING)
        .style(Style::default().bg(Color::Blue).fg(Color::White))
        .block(nodes_table_block());

    frame.render_widget(table, area);
}

fn draw_node_details(
    frame: &mut Frame,
    area: TuiRect,
    s: &StatusSnapshot,
    now: u64,
    selected: usize,
) {
    let n = peer_list_len(s);
    let selected = clamp_selection(selected, n);
    let rows: Vec<PanelRow> = if n == 0 {
        vec![kv("Node", "(none selected)")]
    } else if !s.peers.peer_pipeline.is_empty() {
        let order = pipeline_view_order(s);
        let row_idx = order.get(selected).copied();
        match row_idx.and_then(|i| s.peers.peer_pipeline.get(i)) {
            Some(row) => {
                let full = s
                    .peers
                    .connected_peers
                    .iter()
                    .find(|p| p.peer_id == row.peer_id);
                let mut rows = vec![
                    kv("Peer ID", &row.peer_id),
                    kv("State", pipeline_state_label(row.state)),
                    kv(
                        "Ready in",
                        &row.ready_in_ms
                            .map(format_ready_in_ms)
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    kv(
                        "IP",
                        if row.ip.is_empty() {
                            "-"
                        } else {
                            row.ip.as_str()
                        },
                    ),
                ];
                if let Some(peer) = full {
                    rows.extend(detail_rows_from_connection(peer, now));
                }
                rows
            }
            None => vec![kv("Node", "(none selected)")],
        }
    } else {
        match s.peers.connected_peers.get(selected) {
            Some(peer) => {
                let mut r = vec![
                    kv("Peer ID", &peer.peer_id),
                    kv(
                        "State",
                        if peer.bzz_overlay.is_some() {
                            "Ready"
                        } else {
                            "Handshaking"
                        },
                    ),
                    kv("IP", &status_addr_ip(peer)),
                ];
                r.extend(detail_rows_from_connection(peer, now));
                r
            }
            None => vec![kv("Node", "(none selected)")],
        }
    };
    draw_panel(frame, area, "Details", &rows);
}

fn status_addr_ip(peer: &PeerConnectionInfo) -> String {
    extract_ip_from_multiaddr_str(&peer.address)
}

fn extract_ip_from_multiaddr_str(a: &str) -> String {
    const PREFIXES: &[&str] = &["/ip4/", "/ip6/", "/dns4/", "/dns6/", "/dnsaddr/"];
    let earliest = PREFIXES
        .iter()
        .filter_map(|p| a.find(p).map(|i| (i, *p)))
        .min_by_key(|&(i, _)| i);
    if let Some((start, prefix)) = earliest {
        let after = &a[start + prefix.len()..];
        let end = after.find('/').unwrap_or(after.len());
        if end > 0 {
            return after[..end].to_string();
        }
    }
    "-".to_string()
}

fn detail_rows_from_connection(peer: &PeerConnectionInfo, now: u64) -> Vec<PanelRow> {
    let mut rows = vec![
        kv("Direction", &peer.direction),
        kv("Address", &peer.address),
        kv("Agent", unknown_if_empty(&peer.agent_version)),
        kv(
            "Connected",
            &format_duration(now.saturating_sub(peer.connected_at_unix)),
        ),
    ];
    if let Some(overlay) = &peer.bzz_overlay {
        rows.push(kv("Overlay", overlay));
    }
    if let Some(full_node) = peer.full_node {
        rows.push(kv(
            "Kind",
            if full_node { "full-node" } else { "light-node" },
        ));
    }
    if let Some(at) = peer.last_bzz_at_unix {
        rows.push(kv(
            "BZZ age",
            &format!("{} ago", format_duration(now.saturating_sub(at))),
        ));
    }
    rows
}

fn unknown_if_empty(value: &str) -> &str {
    if value.is_empty() {
        "(unknown)"
    } else {
        value
    }
}

fn vertical_chunks<const N: usize>(area: TuiRect, constraints: [Constraint; N]) -> [TuiRect; N] {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area)
        .as_ref()
        .try_into()
        .expect("layout produced requested number of chunks")
}

struct PanelRow {
    key: String,
    value: String,
    color: Color,
}

fn kv(key: &str, value: &str) -> PanelRow {
    PanelRow {
        key: key.to_string(),
        value: value.to_string(),
        color: Color::White,
    }
}

fn fit(text: &str, width: usize) -> String {
    let mut out: String = text.chars().take(width).collect();
    if out.chars().count() < width {
        out.push_str(&" ".repeat(width - out.chars().count()));
    } else if text.chars().count() > width && width > 0 {
        out.pop();
        out.push('~');
    }
    out
}

fn current_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn format_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(chunks: u64, total_chunks: u64, bytes: u64, total_bytes: u64) -> GetProgress {
        GetProgress {
            elapsed_ms: 250,
            chunks_done: chunks,
            total_chunks_estimate: total_chunks,
            bytes_done: bytes,
            total_bytes_estimate: total_bytes,
            cache_hits: 0,
            peers_used: 0,
            bypass_cache: false,
            in_flight: 0,
        }
    }

    #[test]
    fn stats_summary_formats_completed_cold_fetch() {
        let mut s = sample(5, 5, 18 * 1024, 18 * 1024);
        s.peers_used = 3;
        s.bypass_cache = true;
        let line = format_stats_summary(&s, true);
        assert_eq!(
            line,
            "stats: 5 chunks (18.0KiB) in 0.2s · avg 72.0KiB/s · 3 peers · 0 cache · cold",
        );
    }

    #[test]
    fn stats_summary_formats_warm_cache_hit() {
        let mut s = sample(3, 3, 12 * 1024, 12 * 1024);
        s.cache_hits = 3;
        let line = format_stats_summary(&s, false);
        assert!(line.contains("0 peers"), "got: {line}");
        assert!(line.contains("3 cache"), "got: {line}");
        assert!(line.ends_with("warm"), "got: {line}");
    }

    #[test]
    fn stats_summary_renders_partial_progress_on_error() {
        let s = sample(2, 9, 8 * 1024, 36 * 1024);
        let line = format_stats_summary(&s, false);
        assert!(line.starts_with("stats: 2/9 chunks"), "got: {line}");
    }

    #[test]
    fn progress_line_marks_unknown_total_with_question_mark() {
        let s = sample(1, 0, 4 * 1024, 0);
        let line = format_progress_line(&s, None, false);
        assert!(line.contains("1/?"), "got: {line}");
        assert!(line.contains("4.0KiB/?"), "got: {line}");
    }

    #[test]
    fn visual_progress_line_draws_block_map() {
        let previous = sample(3, 10, 12 * 1024, 40 * 1024);
        let mut current = sample(5, 10, 20 * 1024, 40 * 1024);
        current.peers_used = 2;
        current.cache_hits = 1;

        let (bar, stats) = format_visual_progress_lines_for_width(
            &current,
            Some(&previous),
            Some(8.0 * 1024.0),
            false,
            80,
        );
        assert!(bar.contains('▣'), "got: {bar}");
        assert!(bar.contains('▓'), "got: {bar}");
        assert!(bar.contains('░'), "got: {bar}");
        assert!(stats.contains("50.0%"), "got: {stats}");
        assert!(stats.contains("5/10ch"), "got: {stats}");
        assert!(stats.contains("2p/1c"), "got: {stats}");
    }

    #[test]
    fn visual_progress_line_handles_unknown_total() {
        let s = sample(1, 0, 4 * 1024, 0);
        let (bar, stats) = format_visual_progress_lines_for_width(&s, None, None, true, 80);
        assert!(bar.starts_with('['), "got: {bar}");
        assert!(stats.starts_with("[no-cache] "), "got: {stats}");
        assert!(stats.contains("?.?%"), "got: {stats}");
        assert!(stats.contains("1/?ch"), "got: {stats}");
    }

    /// Render the Retrieval-tab metrics panel against a `TestBackend`
    /// and assert the disk-cache row reflects the wire snapshot. The
    /// regression we're guarding against here: a refactor that drops
    /// the `Disk cache` row, hides the byte / hit gauges, or — most
    /// subtly — silently uses `cache.used` (chunk slots) instead of
    /// `disk.used_bytes` when formatting the disk fill. Driving the
    /// real renderer (rather than a string-only helper) keeps that
    /// honest because the layout, label, and value formatting all
    /// live in `draw_retrieval_metrics`.
    #[test]
    fn retrieval_metrics_panel_shows_disk_cache_row_when_enabled() {
        use ant_control::{CacheInfo, DiskCacheInfo, RetrievalInfo};
        use ratatui::{backend::TestBackend, Terminal};

        let info = RetrievalInfo {
            cache: CacheInfo {
                used: 4096,
                capacity: 8192,
            },
            in_flight: 12,
            in_flight_capacity: 256,
            chunks_fetched_total: 1_700_000,
            bytes_fetched_total: 6_800_000_000,
            cache_hits_total: 1_700_000,
            mem_hits_total: 1_213_000,
            disk: DiskCacheInfo {
                enabled: true,
                used_bytes: 13_200_000_000,         // ~12.3 GiB
                capacity_bytes: 107_374_182_400,    // 100 GiB
                hits_total: 487_000,
            },
            gateway_requests: Vec::new(),
        };
        let bw = BandwidthTracker::new();

        let backend = TestBackend::new(120, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = TuiRect {
                    x: 0,
                    y: 0,
                    width: 120,
                    height: 9,
                };
                draw_retrieval_metrics(frame, area, &info, &bw);
            })
            .unwrap();

        let buf = terminal.backend().buffer().clone();
        let rendered: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| {
                        buf[(x, y)]
                            .symbol()
                            .chars()
                            .next()
                            .unwrap_or(' ')
                    })
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Both rows render with their tier-specific labels and values.
        // We assert on substrings (not exact frames) so the test isn't
        // brittle against ratatui's whitespace-padding tweaks.
        assert!(
            rendered.contains("Mem cache"),
            "Mem cache row missing in:\n{rendered}",
        );
        assert!(
            rendered.contains("4096/8192 chunks"),
            "memory fill not rendered correctly in:\n{rendered}",
        );
        assert!(
            rendered.contains("1.2M hits"),
            "memory hit count not rendered in:\n{rendered}",
        );
        assert!(
            rendered.contains("Disk cache"),
            "Disk cache row missing in:\n{rendered}",
        );
        assert!(
            rendered.contains("12.3GiB / 100.0GiB"),
            "disk byte fill not rendered correctly in:\n{rendered}",
        );
        assert!(
            rendered.contains("487.0K hits"),
            "disk hit count not rendered in:\n{rendered}",
        );
        // Sanity: we shouldn't have the disabled label when the
        // cache is wired up. Catches a regression that flips the
        // ternary backwards.
        assert!(
            !rendered.contains("disabled"),
            "wired-up cache must not render the disabled label:\n{rendered}",
        );
    }

    /// Mirror of the previous test for the `--no-disk-cache` path:
    /// `enabled = false` must produce a clean `disabled` label rather
    /// than rendering `0B / 0B (0%)`. This is the operator's "is
    /// tier 2 wired up at all?" signal.
    #[test]
    fn retrieval_metrics_panel_labels_disabled_disk_cache() {
        use ant_control::{CacheInfo, DiskCacheInfo, RetrievalInfo};
        use ratatui::{backend::TestBackend, Terminal};

        let info = RetrievalInfo {
            cache: CacheInfo {
                used: 0,
                capacity: 8192,
            },
            in_flight: 0,
            in_flight_capacity: 256,
            chunks_fetched_total: 0,
            bytes_fetched_total: 0,
            cache_hits_total: 0,
            mem_hits_total: 0,
            disk: DiskCacheInfo::default(), // enabled = false
            gateway_requests: Vec::new(),
        };
        let bw = BandwidthTracker::new();

        let backend = TestBackend::new(120, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = TuiRect {
                    x: 0,
                    y: 0,
                    width: 120,
                    height: 9,
                };
                draw_retrieval_metrics(frame, area, &info, &bw);
            })
            .unwrap();

        let buf = terminal.backend().buffer().clone();
        let rendered: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| {
                        buf[(x, y)]
                            .symbol()
                            .chars()
                            .next()
                            .unwrap_or(' ')
                    })
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            rendered.contains("Disk cache"),
            "Disk cache row missing in:\n{rendered}",
        );
        assert!(
            rendered.contains("disabled"),
            "disabled disk cache must render the 'disabled' label:\n{rendered}",
        );
        // The byte gauge must NOT render when the cache is off —
        // a `0B / 0B` row would mislead operators into thinking
        // the cache exists but is empty.
        assert!(
            !rendered.contains("0B / 0B"),
            "disabled cache must not render a fake 0B/0B fill:\n{rendered}",
        );
    }
}
