//! `antctl` — command-line client for the `antd` daemon.
//!
//! Speaks to the daemon over its Unix-domain control socket (NDJSON,
//! `ant-control::Request` / `Response`).

use ant_control::{request_sync, Request, Response, StatusSnapshot};
use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
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
                        println!(
                            "{}",
                            serde_json::json!({ "ok": true, "message": message })
                        );
                    } else {
                        println!("{message}");
                    }
                }
                Response::Error { message } => bail!("antd: {message}"),
                other => bail!("unexpected response: {other:?}"),
            }
        }
    }
    Ok(())
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

fn run_top(socket: &Path, interval_ms: u64) -> Result<()> {
    let interval_ms = interval_ms.max(1);
    let refresh = Duration::from_millis(interval_ms);
    let mut terminal = TopTerminal::new()?;
    let mut next_refresh = Instant::now();
    let mut last_status: Option<StatusSnapshot> = None;
    let mut last_error: Option<String> = None;
    let mut page = TopPage::Nodes;
    let mut selected_node = 0usize;

    loop {
        if Instant::now() >= next_refresh {
            match fetch_status(socket) {
                Ok(snap) => {
                    selected_node =
                        clamp_selection(selected_node, snap.peers.connected_peers.len());
                    render_top(
                        terminal.terminal_mut(),
                        &snap,
                        socket,
                        interval_ms,
                        page,
                        selected_node,
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
            if let Some(next_page) = page.after_event(&event) {
                page = next_page;
                if let Some(snap) = &last_status {
                    render_top(
                        terminal.terminal_mut(),
                        snap,
                        socket,
                        interval_ms,
                        page,
                        selected_node,
                    )?;
                } else if let Some(error) = &last_error {
                    render_top_error(terminal.terminal_mut(), socket, interval_ms, page, error)?;
                }
            } else if let Some(next_selection) =
                selected_node_after_event(&event, selected_node, last_status.as_ref())
            {
                selected_node = next_selection;
                if let Some(snap) = &last_status {
                    render_top(
                        terminal.terminal_mut(),
                        snap,
                        socket,
                        interval_ms,
                        page,
                        selected_node,
                    )?;
                }
            } else if matches!(event, Event::Resize(_, _)) {
                if let Some(snap) = &last_status {
                    render_top(
                        terminal.terminal_mut(),
                        snap,
                        socket,
                        interval_ms,
                        page,
                        selected_node,
                    )?;
                } else if let Some(error) = &last_error {
                    render_top_error(terminal.terminal_mut(), socket, interval_ms, page, error)?;
                }
            }
        }
    }
    Ok(())
}

fn clamp_selection(selected: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        selected.min(len - 1)
    }
}

fn selected_node_after_event(
    event: &Event,
    selected: usize,
    status: Option<&StatusSnapshot>,
) -> Option<usize> {
    let len = status?.peers.connected_peers.len();
    if len == 0 {
        return None;
    }
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
            KeyCode::Up => Some(selected.saturating_sub(1)),
            KeyCode::Down => Some((selected + 1).min(len - 1)),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TopPage {
    Nodes,
    Status,
}

impl TopPage {
    const ALL: [TopPage; 2] = [TopPage::Nodes, TopPage::Status];

    fn index(self) -> usize {
        Self::ALL.iter().position(|p| *p == self).unwrap_or(0)
    }

    fn title(self) -> &'static str {
        match self {
            TopPage::Nodes => "Nodes",
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
        "  network_id:  {}{}",
        s.network_id,
        if s.network_id == 1 { " (mainnet)" } else { "" },
    );
    println!("  eth:         {}", s.identity.eth_address);
    println!("  overlay:     {}", s.identity.overlay);
    println!("  peer_id:     {}", s.identity.peer_id);
    if s.listeners.is_empty() {
        println!("  listeners:   (none yet)");
    } else {
        println!("  listeners:");
        for l in &s.listeners {
            println!("    - {l}");
        }
    }
    println!();
    println!("Peers:");
    println!("  connected:   {}", s.peers.connected);
    if let Some(h) = &s.peers.last_handshake {
        let age = now.saturating_sub(h.at_unix);
        let agent = if h.agent_version.is_empty() {
            "(unknown agent)".to_string()
        } else {
            h.agent_version.clone()
        };
        println!(
            "  last bzz:    {} ({}, {} ago){}",
            h.remote_overlay,
            agent,
            format_duration(age),
            if h.full_node { ", full-node" } else { "" },
        );
    } else {
        println!("  last bzz:    (none yet)");
    }
    println!();
    println!("Control:");
    println!("  socket:      {}", s.control_socket);
}

fn render_top(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    s: &StatusSnapshot,
    socket: &Path,
    interval_ms: u64,
    page: TopPage,
    selected_node: usize,
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
        TopPage::Status => {
            let rows = vec![
                kv("process", ""),
                kv("agent", &ctx.status.agent),
                kv("pid", &ctx.status.pid.to_string()),
                kv("uptime", &format_duration(ctx.uptime)),
                kv("refresh", &format!("{}ms", ctx.interval_ms)),
                kv("updated", &ctx.now.to_string()),
                kv("", ""),
                kv("network", ""),
                kv("network", &network_id),
                kv("eth", &ctx.status.identity.eth_address),
                kv("overlay", &ctx.status.identity.overlay),
                kv("peer_id", &ctx.status.identity.peer_id),
                kv("connected", &ctx.status.peers.connected.to_string()),
                kv("listeners", &ctx.status.listeners.len().to_string()),
                kv("", ""),
                kv("control", ""),
                kv("socket", &ctx.status.control_socket),
                kv("requested", &ctx.socket.display().to_string()),
                kv("protocol", &format!("v{}", ctx.status.protocol_version)),
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
        TopPage::Status => {
            let rows = [
                kv("daemon", "disconnected"),
                kv("socket", &socket.display().to_string()),
                kv("error", error),
                kv("retry", "polling"),
                kv("refresh", &format!("{interval_ms}ms")),
            ];
            draw_panel(frame, body, "Status", &rows);
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
            Constraint::Length(3),
            Constraint::Percentage(58),
            Constraint::Percentage(42),
        ],
    );
    draw_connected_progress(frame, progress_area, s);
    draw_nodes_table(frame, table_area, s, selected);
    draw_node_details(frame, detail_area, s, now, selected);
}

fn draw_connected_progress(frame: &mut Frame, area: TuiRect, s: &StatusSnapshot) {
    let limit = s.peers.node_limit.max(1);
    let connected = (s.peers.connected_peers.len() as u32).min(limit);
    let ratio = connected as f64 / limit as f64;
    draw_progress_gauge(frame, area, &progress_label(connected, limit), ratio);
}

fn draw_progress_gauge(frame: &mut Frame, area: TuiRect, label: &str, ratio: f64) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .style(Style::default().bg(Color::Blue));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let prefix = format!(" Connected: {label} ");
    let bar_width = inner
        .width
        .saturating_sub(prefix.chars().count() as u16 + 3) as usize;
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
        inner,
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
            Constraint::Length(3),
            Constraint::Percentage(58),
            Constraint::Percentage(42),
        ],
    );
    draw_progress_gauge(frame, progress_area, "000/???", 0.0);
    draw_empty_nodes_table(frame, table_area);
    let rows = [
        kv("daemon", "disconnected"),
        kv("nodes", "(waiting for antd)"),
        kv("error", error),
    ];
    draw_panel(frame, detail_area, "Details", &rows);
}

fn draw_empty_nodes_table(frame: &mut Frame, area: TuiRect) {
    let rows = vec![Row::new(vec![
        "(waiting for antd)".to_string(),
        "-".to_string(),
        "-".to_string(),
        "-".to_string(),
    ])];
    let header = Row::new(vec!["peer id", "address", "agent", "type"]).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );
    let block = Block::default()
        .title(" Nodes ")
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue));
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(32),
            Constraint::Percentage(32),
            Constraint::Percentage(24),
            Constraint::Length(8),
        ],
    )
    .header(header)
    .column_spacing(1)
    .style(Style::default().bg(Color::Blue).fg(Color::White))
    .block(block);
    frame.render_widget(table, area);
}

fn draw_nodes_table(frame: &mut Frame, area: TuiRect, s: &StatusSnapshot, selected: usize) {
    let visible_rows = area.height.saturating_sub(3).max(1) as usize;
    let total = s.peers.connected_peers.len();
    let selected = clamp_selection(selected, total);
    let start = if total <= visible_rows {
        0
    } else if selected >= visible_rows {
        selected + 1 - visible_rows
    } else {
        0
    };
    let end = (start + visible_rows).min(total);

    let rows: Vec<Row> = if s.peers.connected_peers.is_empty() {
        vec![Row::new(vec![
            "(none yet)".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ])]
    } else {
        s.peers
            .connected_peers
            .iter()
            .enumerate()
            .skip(start)
            .take(end - start)
            .map(|(idx, peer)| {
                let row = Row::new(vec![
                    peer.peer_id.clone(),
                    peer.address.clone(),
                    unknown_if_empty(&peer.agent_version).to_string(),
                    node_type(peer).to_string(),
                ]);
                if idx == selected {
                    row.style(
                        Style::default()
                            .bg(Color::Cyan)
                            .fg(Color::Black)
                            .add_modifier(Modifier::BOLD),
                    )
                } else {
                    row
                }
            })
            .collect()
    };

    let header = Row::new(vec!["peer id", "address", "agent", "type"]).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );
    let block = Block::default()
        .title(" Nodes ")
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(Color::Cyan))
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Blue));
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(32),
            Constraint::Percentage(32),
            Constraint::Percentage(24),
            Constraint::Length(8),
        ],
    )
    .header(header)
    .column_spacing(1)
    .style(Style::default().bg(Color::Blue).fg(Color::White))
    .block(block);

    frame.render_widget(table, area);
}

fn draw_node_details(
    frame: &mut Frame,
    area: TuiRect,
    s: &StatusSnapshot,
    now: u64,
    selected: usize,
) {
    let selected = clamp_selection(selected, s.peers.connected_peers.len());
    let rows = match s.peers.connected_peers.get(selected) {
        Some(peer) => {
            let mut rows = vec![
                kv("peer_id", &peer.peer_id),
                kv("direction", &peer.direction),
                kv("address", &peer.address),
                kv("agent", unknown_if_empty(&peer.agent_version)),
                kv(
                    "connected",
                    &format_duration(now.saturating_sub(peer.connected_at_unix)),
                ),
            ];
            if let Some(overlay) = &peer.bzz_overlay {
                rows.push(kv("overlay", overlay));
            }
            if let Some(full_node) = peer.full_node {
                rows.push(kv(
                    "kind",
                    if full_node { "full-node" } else { "light-node" },
                ));
            }
            if let Some(at) = peer.last_bzz_at_unix {
                rows.push(kv(
                    "bzz age",
                    &format!("{} ago", format_duration(now.saturating_sub(at))),
                ));
            }
            rows
        }
        None => vec![kv("node", "(none selected)")],
    };
    draw_panel(frame, area, "Details", &rows);
}

fn node_type(peer: &ant_control::PeerConnectionInfo) -> &str {
    match peer.full_node {
        Some(true) => "full",
        Some(false) => "light",
        None => "unknown",
    }
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
