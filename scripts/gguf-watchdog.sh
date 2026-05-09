#!/usr/bin/env bash
# gguf-watchdog.sh — keep one antd upload job alive until completion.
#
# Polls `antctl upload status <job>` every $POLL_SECS. On `failed`, runs
# `antctl upload resume <job>`. Tracks chunks-pushed across resumes to
# detect "no progress" stalls; if $MAX_STALLS consecutive checks show no
# progress AND the job is failed, gives up.
#
# Usage:
#   gguf-watchdog.sh <job_id> <log_path>
#
# Designed for unattended overnight runs: the owning shell can exit and
# this keeps going (idempotent — antctl just talks to the daemon socket).
set -u

JOB_ID=${1:?usage: gguf-watchdog.sh <job_id> <log_path>}
LOG=${2:?usage: gguf-watchdog.sh <job_id> <log_path>}
POLL_SECS=${POLL_SECS:-60}
# 30 polls × 60s = 30 min of zero progress before giving up. Empirically
# (2026-05-09 07:11 UTC) bee can stall a running upload for ~15 min and
# recover on its own without `resume`, so a 15-min cap was too tight.
# 30 min covers observed worst-case bee-side debit/credit imbalance
# without leaving the upload abandoned for hours after a real failure.
MAX_STALLS=${MAX_STALLS:-30}
ANTCTL=${ANTCTL:-/usr/local/bin/antctl}

ts() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }
log() { echo "$(ts) $*" >> "$LOG"; }

log "=== gguf-watchdog start: job=$JOB_ID poll=${POLL_SECS}s max_stalls=$MAX_STALLS ==="

last_chunks=-1
stalls=0
resumes=0

while true; do
    # Snapshot the job state. `antctl upload status` returns lines like:
    #   status:    running
    #   progress:  178.5MiB / 256.0MiB (69.7%)
    #   chunks:    45690 / 66054
    out=$("$ANTCTL" upload status "$JOB_ID" 2>&1) || {
        log "antctl status failed: $out"
        sleep "$POLL_SECS"
        continue
    }
    status=$(printf '%s\n' "$out" | awk -F: '/^status:/ {gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit}')
    progress=$(printf '%s\n' "$out" | awk -F: '/^progress:/ {sub(/^progress:[ \t]*/,""); print; exit}')
    chunks_line=$(printf '%s\n' "$out" | awk -F: '/^chunks:/ {sub(/^chunks:[ \t]*/,""); print; exit}')
    chunks=$(printf '%s' "$chunks_line" | awk '{print $1}')

    case "$status" in
        completed)
            ref=$(printf '%s\n' "$out" | awk -F: '/^reference:/ {gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit}')
            url=$(printf '%s\n' "$out" | awk -F: '/^url:/ {gsub(/^[ \t]+|[ \t]+$/,"",$2$3); print $2":"$3; exit}')
            log "COMPLETED: chunks=$chunks_line  reference=$ref  url=$url"
            log "=== gguf-watchdog exit (success) ==="
            exit 0
            ;;
        cancelled)
            log "CANCELLED externally; exiting watchdog"
            exit 2
            ;;
        running|paused)
            # Track progress to spot true stalls. A "running" job that
            # isn't pushing chunks for 15 polls means something's wedged
            # at the libp2p/bee layer; we flag it but keep waiting since
            # `resume` won't help while status is already running.
            if [ "$chunks" = "$last_chunks" ]; then
                stalls=$((stalls + 1))
            else
                stalls=0
                last_chunks=$chunks
            fi
            log "$status  $progress  chunks=$chunks_line  stalls=$stalls"
            ;;
        failed)
            err=$(printf '%s\n' "$out" | awk -F: '/^error:/ {sub(/^error:[ \t]*/,""); print; exit}')
            log "FAILED: chunks=$chunks_line  err=$err  resumes=$resumes"
            # If we made progress since last poll, count this as a fresh
            # failure and reset the stall counter so the next resume gets
            # a full window.
            if [ "$chunks" != "$last_chunks" ]; then
                stalls=0
                last_chunks=$chunks
            else
                stalls=$((stalls + 1))
            fi
            if [ "$stalls" -ge "$MAX_STALLS" ]; then
                log "ABORT: $MAX_STALLS consecutive failed polls with no chunk progress; giving up"
                log "=== gguf-watchdog exit (stalled) ==="
                exit 3
            fi
            # Gentle back-off before each resume — gives bee peers a few
            # seconds to recover their accountingPeer state after the
            # disconnect cascade that produced the failure.
            sleep 10
            resume_out=$("$ANTCTL" upload resume "$JOB_ID" 2>&1) || true
            log "RESUMED (#$((++resumes))): $resume_out"
            ;;
        *)
            log "unknown status='$status' raw=$(printf '%s' "$out" | head -c 200)"
            ;;
    esac
    sleep "$POLL_SECS"
done
