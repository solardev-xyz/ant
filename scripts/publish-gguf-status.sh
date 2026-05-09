#!/usr/bin/env bash
# Publish a public status page for the in-flight GGUF upload to
# /home/ubuntu/www/freedom.baby/ai/index.html. Designed to be invoked
# from cron every 5 minutes.
#
# Robust against:
#   - antd being down (status fields fall back to "—")
#   - watchdog dead (footer reports "watchdog DEAD")
#   - job missing / completed / failed (status field reflects whatever
#     `antctl upload status` returns)
#   - log file truncated or absent (rate sampling silently degrades)
#
# Atomic write: compose to a .tmp sibling, then mv into place, so
# Apache never serves a half-written file.
set -u

JOB="${JOB:-000002119b4e1fd1}"
LOG="${LOG:-/home/ubuntu/antd-uploads/gguf-${JOB}.log}"
ANTCTL="${ANTCTL:-/home/ubuntu/ant/target/release/antctl}"
OUT_DIR="${OUT_DIR:-/home/ubuntu/www/freedom.baby/ai}"
OUT="$OUT_DIR/index.html"
TMP="$OUT.tmp"
ENV_FILE="${ENV_FILE:-/home/ubuntu/ant/.env}"
CB_CACHE="${CB_CACHE:-/home/ubuntu/antd-uploads/.chequebook-cache.json}"
CB_CACHE_TTL="${CB_CACHE_TTL:-300}"  # seconds; chequebook state changes on the order of hours

now_utc="$(date -u +'%Y-%m-%d %H:%M:%S UTC')"
now_epoch="$(date +%s)"

# --- gather ---------------------------------------------------------------

status_block="$("$ANTCTL" upload status "$JOB" 2>/dev/null || true)"
status="$(awk -F': *' '/^status:/  {print $2; exit}' <<<"$status_block")"
source_path="$(awk -F': *' '/^source:/  {print $2; exit}' <<<"$status_block")"
size="$(awk -F': *' '/^size:/    {print $2; exit}' <<<"$status_block")"
progress="$(awk -F': *' '/^progress:/{print $2; exit}' <<<"$status_block")"
chunks_line="$(awk -F': *' '/^chunks:/  {print $2; exit}' <<<"$status_block")"
mode="$(awk -F': *' '/^mode:/    {print $2; exit}' <<<"$status_block")"
reference="$(awk -F': *' '/^reference:/{print $2; exit}' <<<"$status_block")"

cur_chunks=0
total_chunks=0
if [[ "$chunks_line" =~ ^([0-9]+)[[:space:]]*/[[:space:]]*([0-9]+) ]]; then
  cur_chunks="${BASH_REMATCH[1]}"
  total_chunks="${BASH_REMATCH[2]}"
fi
percent="—"
if [[ "$total_chunks" -gt 0 ]]; then
  percent="$(awk -v c="$cur_chunks" -v t="$total_chunks" 'BEGIN{printf "%.2f", c*100/t}')"
fi

peers="$("$ANTCTL" status 2>/dev/null | awk '/Connected:/ {print $2; exit}')"
peers="${peers:-—}"

# Rate: average chunks/s over the last 10 minutes of watchdog samples.
# Watchdog logs once per minute as:
#   2026-05-09T09:29:44Z running 1.0GiB / 6.7GiB (15.5%) chunks=270998 / 1762403 stalls=0
rate_10m="—"
rate_60m="—"
last_log_ts="—"
last_stalls="—"
if [[ -r "$LOG" ]]; then
  # Use awk to walk the file once, keep the first-and-last in two windows.
  read rate_10m rate_60m last_log_ts last_stalls < <(awk '
    BEGIN { now = systime() }
    function ts2epoch(s,    y,mo,d,h,mi,se) {
      # "2026-05-09T09:29:44Z" -> epoch
      y=substr(s,1,4); mo=substr(s,6,2); d=substr(s,9,2)
      h=substr(s,12,2); mi=substr(s,15,2); se=substr(s,18,2)
      return mktime(sprintf("%d %d %d %d %d %d UTC", y, mo, d, h, mi, se))
    }
    /^[0-9]{4}-/ {
      ts=ts2epoch($1)
      # extract chunks=NNN from "chunks=NNN / TTT"
      for (i=1; i<=NF; i++) if ($i ~ /^chunks=/) { c=$i; sub("chunks=","",c); chunks=c+0 }
      for (i=1; i<=NF; i++) if ($i ~ /^stalls=/) { st=$i; sub("stalls=","",st); stalls=st+0 }
      # 10-min window
      if (ts >= now - 600) { if (start10_ts==0) { start10_ts=ts; start10_c=chunks } end10_ts=ts; end10_c=chunks }
      # 60-min window
      if (ts >= now - 3600) { if (start60_ts==0) { start60_ts=ts; start60_c=chunks } end60_ts=ts; end60_c=chunks }
      last_ts_str=$1; last_stalls_v=stalls
    }
    END {
      r10="—"; r60="—"
      if (end10_ts > start10_ts) r10=sprintf("%.1f", (end10_c-start10_c)/(end10_ts-start10_ts))
      if (end60_ts > start60_ts) r60=sprintf("%.1f", (end60_c-start60_c)/(end60_ts-start60_ts))
      if (last_ts_str=="") last_ts_str="—"
      if (last_stalls_v=="") last_stalls_v="—"
      print r10, r60, last_ts_str, last_stalls_v
    }
  ' "$LOG") || true
fi

# Byte throughput — Swarm chunks are 4 KiB of payload each, so
# rate_chunks/s × 4 ≈ KiB/s on the wire (ignores cheque/protocol
# overhead, which is small).
rate_10m_kib="—"
rate_60m_kib="—"
if [[ "$rate_10m" != "—" ]]; then
  rate_10m_kib="$(awk -v r="$rate_10m" 'BEGIN{printf "%.1f", r*4}')"
fi
if [[ "$rate_60m" != "—" ]]; then
  rate_60m_kib="$(awk -v r="$rate_60m" 'BEGIN{printf "%.1f", r*4}')"
fi

# ETA: prefer 10-min rate, fall back to 60-min, fall back to "—".
eta_human="—"
remaining=0
chosen_rate=""
if [[ "$total_chunks" -gt 0 ]]; then
  remaining=$(( total_chunks - cur_chunks ))
  if [[ "$rate_10m" != "—" ]] && awk -v r="$rate_10m" 'BEGIN{exit !(r+0 > 1)}'; then
    chosen_rate="$rate_10m"
  elif [[ "$rate_60m" != "—" ]] && awk -v r="$rate_60m" 'BEGIN{exit !(r+0 > 1)}'; then
    chosen_rate="$rate_60m"
  fi
  if [[ -n "$chosen_rate" ]]; then
    eta_secs="$(awk -v r="$chosen_rate" -v n="$remaining" 'BEGIN{printf "%d", n/r}')"
    eta_h=$(( eta_secs / 3600 ))
    eta_m=$(( (eta_secs % 3600) / 60 ))
    eta_target="$(date -u -d "+${eta_secs} seconds" +'%H:%M UTC')"
    eta_human="~${eta_h}h ${eta_m}m  (≈ ${eta_target})"
  fi
fi

# Once the job is `completed`, ETA is meaningless. Replace it with the
# completion timestamp (if we can recover one from the watchdog log).
if [[ "$status" == "completed" ]]; then
  if [[ -r "$LOG" ]]; then
    completed_ts="$(awk '/COMPLETED:/ {ts=$1} END {print ts}' "$LOG")"
    if [[ -n "$completed_ts" ]]; then
      eta_human="completed ${completed_ts}"
    else
      eta_human="completed"
    fi
  else
    eta_human="completed"
  fi
fi

# Watchdog liveness. The watchdog runs as `bash gguf-watchdog.sh <job> <log>`.
watchdog_state="DEAD"
if pgrep -f "gguf-watchdog.sh ${JOB}" >/dev/null 2>&1; then
  watchdog_state="alive"
fi

# Tail of the watchdog log for the recent-history block.
recent_tail="$(tail -n 12 "$LOG" 2>/dev/null || echo "(log file unreadable)")"

# Status pill class.
pill_class="pill-grey"
case "$status" in
  running)   pill_class="pill-green" ;;
  paused)    pill_class="pill-amber" ;;
  completed) pill_class="pill-blue"  ;;
  failed)    pill_class="pill-red"   ;;
esac

# --- chequebook ----------------------------------------------------------
#
# `antctl chequebook show --json` does ~5 eth_calls against the Gnosis RPC.
# Chequebook state moves on the order of hours (peers cash sporadically;
# we top up rarely), so we cache the JSON for $CB_CACHE_TTL seconds and
# only re-fetch on miss / staleness. If the call fails (RPC down,
# rate-limited), we leave the cache stale and let the page show whatever
# it last got. If we have no cache at all, fields fall through to "—".

cb_json=""
if [[ -r "$CB_CACHE" ]]; then
  cb_age=$(( now_epoch - $(stat -c %Y "$CB_CACHE") ))
else
  cb_age=999999
fi

if [[ "$cb_age" -gt "$CB_CACHE_TTL" ]]; then
  if [[ -r "$ENV_FILE" ]]; then
    fresh="$(set -a; . "$ENV_FILE"; set +a; "$ANTCTL" chequebook show --json 2>/dev/null || true)"
    if [[ -n "$fresh" && "$fresh" == \{* ]]; then
      printf '%s' "$fresh" >"$CB_CACHE"
    fi
  fi
fi

if [[ -r "$CB_CACHE" ]]; then
  cb_json="$(cat "$CB_CACHE")"
fi

# Tiny extractor for `"key":"value"` from a flat JSON object — sufficient
# for `antctl chequebook show --json` which never nests.
cb_get() {
  local key=$1
  [[ -n "$cb_json" ]] || { printf '—'; return; }
  printf '%s' "$cb_json" | grep -oE "\"${key}\":\"[^\"]*\"" | head -1 | sed -E 's/.*":"([^"]*)"/\1/'
}

cb_address="$(cb_get chequebook)"
cb_issuer="$(cb_get issuer)"
cb_balance_plur="$(cb_get balance_plur)"
cb_liquid_plur="$(cb_get liquid_balance_plur)"
cb_total_paid_plur="$(cb_get total_paid_out_plur)"

# 1 BZZ = 10^16 PLUR. Render with 6 fractional digits so changes at the
# milli-BZZ level are visible without overwhelming the eye.
plur_to_bzz() {
  local p=$1
  if [[ -z "$p" || "$p" == "—" ]]; then printf '—'; return; fi
  awk -v p="$p" 'BEGIN{printf "%.6f", p / 1e16}'
}

cb_balance_bzz="$(plur_to_bzz "$cb_balance_plur")"
cb_liquid_bzz="$(plur_to_bzz "$cb_liquid_plur")"
cb_total_paid_bzz="$(plur_to_bzz "$cb_total_paid_plur")"

# Cache freshness label for the footer of the chequebook card.
if [[ -r "$CB_CACHE" ]]; then
  cb_age_now=$(( now_epoch - $(stat -c %Y "$CB_CACHE") ))
  cb_freshness="cached ${cb_age_now}s ago"
else
  cb_freshness="unavailable"
fi

# --- HTML escape helper ---------------------------------------------------
#
# Pure bash, no subprocess. ~10x faster than spawning python3 per call,
# which mattered when this script went from a 5-minute cron to a
# 1-minute cron (8 escapes × python3 startup ≈ 250 ms).

esc() {
  local s=$1
  s=${s//&/&amp;}
  s=${s//</&lt;}
  s=${s//>/&gt;}
  s=${s//\"/&quot;}
  s=${s//\'/&#39;}
  printf '%s' "$s"
}

esc_status="$(esc "${status:-—}")"
esc_source="$(esc "${source_path:-—}")"
esc_size="$(esc "${size:-—}")"
esc_progress="$(esc "${progress:-—}")"
esc_mode="$(esc "${mode:-—}")"
esc_ref="$(esc "${reference:-—}")"
esc_eta="$(esc "${eta_human}")"
esc_recent="$(esc "${recent_tail}")"
esc_cb_address="$(esc "${cb_address:-—}")"
esc_cb_issuer="$(esc "${cb_issuer:-—}")"
esc_cb_freshness="$(esc "${cb_freshness}")"

# --- emit -----------------------------------------------------------------

cat >"$TMP" <<HTML
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>gemma4:e2b → Swarm — upload status</title>
  <style>
    :root {
      --bg: #0e1116;
      --fg: #e6edf3;
      --muted: #8b949e;
      --card: #161b22;
      --border: #30363d;
      --accent: #58a6ff;
      --green: #3fb950;
      --amber: #d29922;
      --red: #f85149;
      --blue: #58a6ff;
    }
    body {
      margin: 0;
      padding: 0;
      background: var(--bg);
      color: var(--fg);
      font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    }
    main {
      max-width: 760px;
      margin: 40px auto;
      padding: 0 16px;
    }
    h1 {
      font-size: 22px;
      margin: 0 0 4px;
    }
    .sub {
      color: var(--muted);
      margin: 0 0 24px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 20px 22px;
      margin-bottom: 16px;
    }
    .row {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 8px 0;
      border-bottom: 1px solid var(--border);
    }
    .row:last-child { border-bottom: none; }
    .row .k { color: var(--muted); flex: 0 0 140px; }
    .row .v { text-align: right; word-break: break-all; font-variant-numeric: tabular-nums; }
    .pill {
      display: inline-block;
      padding: 2px 10px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .pill-green { background: rgba(63,185,80,.18); color: var(--green); }
    .pill-amber { background: rgba(210,153,34,.18); color: var(--amber); }
    .pill-red   { background: rgba(248,81,73,.18);  color: var(--red); }
    .pill-blue  { background: rgba(88,166,255,.18); color: var(--blue); }
    .pill-grey  { background: rgba(139,148,158,.18); color: var(--muted); }
    .progress-wrap {
      width: 100%;
      background: rgba(139,148,158,.2);
      border-radius: 6px;
      height: 12px;
      margin: 12px 0 4px;
      overflow: hidden;
    }
    .progress-bar {
      background: linear-gradient(90deg, #58a6ff, #3fb950);
      height: 100%;
      width: ${percent}%;
      transition: width 1s ease;
    }
    .progress-num {
      display: flex;
      justify-content: space-between;
      color: var(--muted);
      font-size: 13px;
      font-variant-numeric: tabular-nums;
    }
    pre {
      background: #0a0d12;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 12px 14px;
      font-size: 12px;
      line-height: 1.45;
      color: #c9d1d9;
      overflow-x: auto;
      margin: 0;
    }
    a { color: var(--accent); }
    footer {
      color: var(--muted);
      font-size: 12px;
      text-align: center;
      margin-top: 24px;
    }
  </style>
</head>
<body>
<main>
  <h1>gemma4:e2b → Swarm</h1>
  <p class="sub">A 6.7 GiB AI model uploading itself, chunk by chunk, to a decentralised storage network.</p>

  <div class="card">
    <div class="row">
      <span class="k">Status</span>
      <span class="v"><span class="pill ${pill_class}">${esc_status}</span></span>
    </div>
    <div class="row">
      <span class="k">Progress</span>
      <span class="v"><strong>${percent}%</strong></span>
    </div>
    <div class="progress-wrap"><div class="progress-bar"></div></div>
    <div class="progress-num">
      <span>${cur_chunks} / ${total_chunks} chunks</span>
      <span>${esc_progress}</span>
    </div>
  </div>

  <div class="card">
    <div class="row"><span class="k">ETA</span><span class="v">${esc_eta}</span></div>
    <div class="row"><span class="k">Throughput (10 min)</span><span class="v">${rate_10m_kib} KiB/s · ${rate_10m} chunks/s</span></div>
    <div class="row"><span class="k">Throughput (60 min)</span><span class="v">${rate_60m_kib} KiB/s · ${rate_60m} chunks/s</span></div>
    <div class="row"><span class="k">Connected peers</span><span class="v">${peers}</span></div>
    <div class="row"><span class="k">Watchdog</span><span class="v">${watchdog_state}, stalls=${last_stalls}</span></div>
  </div>

  <div class="card">
    <div class="row"><span class="k">File</span><span class="v">${esc_source}</span></div>
    <div class="row"><span class="k">Size</span><span class="v">${esc_size}</span></div>
    <div class="row"><span class="k">Mode</span><span class="v">${esc_mode}</span></div>
    <div class="row"><span class="k">Job ID</span><span class="v">${JOB}</span></div>
    <div class="row"><span class="k">Reference</span><span class="v">${esc_ref}</span></div>
  </div>

  <div class="card">
    <h3 style="margin:0 0 10px;font-size:14px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;">Chequebook</h3>
    <div class="row"><span class="k">Address</span><span class="v"><a href="https://gnosisscan.io/address/${esc_cb_address}" rel="noopener">${esc_cb_address}</a></span></div>
    <div class="row"><span class="k">Issuer</span><span class="v">${esc_cb_issuer}</span></div>
    <div class="row"><span class="k">Balance</span><span class="v">${cb_balance_bzz} xBZZ</span></div>
    <div class="row"><span class="k">Liquid balance</span><span class="v">${cb_liquid_bzz} xBZZ</span></div>
    <div class="row"><span class="k">Total cashed by peers</span><span class="v">${cb_total_paid_bzz} xBZZ</span></div>
    <div class="row"><span class="k" style="flex-basis:auto;font-size:12px;">${esc_cb_freshness}</span><span class="v"></span></div>
  </div>

  <div class="card">
    <h3 style="margin:0 0 10px;font-size:14px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;">Recent activity</h3>
<pre>${esc_recent}</pre>
  </div>

  <footer>Updated ${now_utc} · refreshes every 30 s · regenerated every 1 min</footer>
</main>
</body>
</html>
HTML

mv -f "$TMP" "$OUT"
