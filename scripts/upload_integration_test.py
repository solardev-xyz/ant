#!/usr/bin/env python3
"""Upload reliability integration test for a running `antd`.

For each requested size it generates a random file, uploads it through the
daemon's pushsync path with `antctl upload start`, downloads the result back
through a public gateway (`vibing.at/ant` by default) and asserts the
SHA-256 round-trips. The download deliberately goes through a *different*
node than the uploader, so a pass proves the chunks are actually retrievable
from the network — not merely cached on the box that pushed them.

This is the harness the `upload-integration` GitHub workflow runs against a
freshly-started node. It is intentionally dependency-free (stdlib only) so it
runs on any CI image with Python 3.9+.

Configuration is via environment variables (all optional except the socket
when the default is wrong):

  ANT_ANTCTL        path to the `antctl` binary        (default: antctl on PATH)
  ANT_SOCKET        daemon control socket path         (default: ~/.antd/antd.sock)
  ANT_GATEWAY       gateway base URL for verification  (default: https://vibing.at/ant)
  ANT_BATCH         postage batch override (0x…)       (default: daemon default)
  ANT_SIZES_MB      comma-separated sizes in MiB       (default: 1,10)
  ANT_RUNS          repeats per size                   (default: 1)
  ANT_UPLOAD_TIMEOUT  per-upload timeout, seconds      (default: 1800)
  ANT_FETCH_TIMEOUT   per-download timeout, seconds    (default: 1800)
  ANT_WORKDIR       scratch dir for generated files    (default: a temp dir)

Exit code is 0 only if every (size × run) uploaded and round-tripped.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error

ANTCTL = os.environ.get("ANT_ANTCTL", "antctl")
SOCKET = os.environ.get("ANT_SOCKET", os.path.expanduser("~/.antd/antd.sock"))
GATEWAY = os.environ.get("ANT_GATEWAY", "https://vibing.at/ant").rstrip("/")
BATCH = os.environ.get("ANT_BATCH", "").strip()
SIZES_MB = [int(s) for s in os.environ.get("ANT_SIZES_MB", "1,10").split(",") if s.strip()]
RUNS = int(os.environ.get("ANT_RUNS", "1"))
UPLOAD_TIMEOUT = int(os.environ.get("ANT_UPLOAD_TIMEOUT", "1800"))
FETCH_TIMEOUT = int(os.environ.get("ANT_FETCH_TIMEOUT", "1800"))
WORKDIR = os.environ.get("ANT_WORKDIR", "")

MIB = 1 << 20
CHUNK = 1 << 20


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_json_objects(s: str) -> list:
    """`antctl … --json` can emit several whitespace-separated JSON objects
    (a progress line, then the final view). Return all of them in order."""
    decoder = json.JSONDecoder()
    out, idx, n = [], 0, len(s)
    while idx < n:
        while idx < n and s[idx] in " \t\r\n":
            idx += 1
        if idx >= n:
            break
        try:
            obj, end = decoder.raw_decode(s, idx)
        except json.JSONDecodeError:
            idx += 1
            continue
        out.append(obj)
        idx = end
    return out


def last_view(s: str) -> dict | None:
    """Pick the last object that looks like an upload job view."""
    for obj in reversed(parse_json_objects(s)):
        if isinstance(obj, dict) and ("reference" in obj or "status" in obj or "job_id" in obj):
            return obj
    return None


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(CHUNK), b""):
            h.update(blk)
    return h.hexdigest()


def gen_random_file(path: str, size: int) -> str:
    h = hashlib.sha256()
    with open(path, "wb") as f:
        remaining = size
        while remaining:
            n = min(remaining, 1 << 22)
            blk = os.urandom(n)
            f.write(blk)
            h.update(blk)
            remaining -= n
    return h.hexdigest()


def upload(path: str) -> dict:
    cmd = [ANTCTL, "--socket", SOCKET, "upload", "start", "--no-preflight", "--json"]
    if BATCH:
        cmd += ["--batch", BATCH]
    cmd.append(path)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=UPLOAD_TIMEOUT)
    view = last_view(proc.stdout)
    if view is None:
        raise RuntimeError(
            f"no upload view in antctl output\n--- stdout ---\n{proc.stdout[-2000:]}\n"
            f"--- stderr ---\n{proc.stderr[-2000:]}"
        )
    return view


def gateway_fetch(reference: str, dest: str) -> tuple[int, str]:
    ref = reference[2:] if reference.startswith("0x") else reference
    last_err: Exception | None = None
    # A non-raw upload returns a single-file mantaray manifest → /bzz.
    # Fall back to /bytes for raw references.
    for route in ("bzz", "bytes"):
        url = f"{GATEWAY}/{route}/{ref}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp, open(dest, "wb") as out:
                total = 0
                while True:
                    blk = resp.read(CHUNK)
                    if not blk:
                        break
                    out.write(blk)
                    total += len(blk)
            return total, sha256_file(dest)
        except urllib.error.HTTPError as e:
            last_err = RuntimeError(f"/{route} -> HTTP {e.code}")
        except Exception as e:  # noqa: BLE001 - report and try next route
            last_err = e
    raise RuntimeError(f"gateway fetch failed for {ref}: {last_err}")


def run_case(workdir: str, size_mb: int, run: int) -> dict:
    size = size_mb * MIB
    src = os.path.join(workdir, f"src_{size_mb}mb_{run}.bin")
    dst = os.path.join(workdir, f"dl_{size_mb}mb_{run}.bin")
    log(f"=== {size_mb} MiB run {run}: generating {size} bytes")
    src_hash = gen_random_file(src, size)

    log(f"{size_mb} MiB run {run}: uploading via {SOCKET}")
    t0 = time.time()
    view = upload(src)
    up_secs = time.time() - t0
    status = view.get("status")
    ref = view.get("reference")
    log(
        f"{size_mb} MiB run {run}: upload status={status} ref={ref} "
        f"chunks={view.get('chunks_pushed')}/{view.get('chunks_total')} "
        f"requeued={view.get('chunks_requeued')} stalled={view.get('stalled')} "
        f"({up_secs:.1f}s)"
    )
    if status != "completed" or not ref:
        os.remove(src)
        return {
            "size_mb": size_mb, "run": run, "ok": False,
            "reason": f"upload not completed (status={status}, ref={ref})",
        }

    log(f"{size_mb} MiB run {run}: downloading from {GATEWAY}")
    t0 = time.time()
    try:
        dl_size, dl_hash = gateway_fetch(ref, dst)
    except Exception as e:  # noqa: BLE001
        os.remove(src)
        return {"size_mb": size_mb, "run": run, "ok": False, "reason": str(e)}
    fetch_secs = time.time() - t0

    ok = (dl_size == size and dl_hash == src_hash)
    log(
        f"{size_mb} MiB run {run}: download size={dl_size} match={ok} ({fetch_secs:.1f}s)"
    )
    for p in (src, dst):
        try:
            os.remove(p)
        except OSError:
            pass
    return {
        "size_mb": size_mb, "run": run, "ok": ok, "reference": ref,
        "upload_secs": round(up_secs, 1), "fetch_secs": round(fetch_secs, 1),
        "reason": None if ok else f"hash/size mismatch (size={dl_size} vs {size})",
    }


def main() -> int:
    log(f"antctl={ANTCTL} socket={SOCKET} gateway={GATEWAY}")
    log(f"sizes={SIZES_MB} MiB runs={RUNS} batch={BATCH or '(daemon default)'}")

    cleanup = False
    workdir = WORKDIR
    if not workdir:
        workdir = tempfile.mkdtemp(prefix="ant-upload-it-")
        cleanup = True
    else:
        os.makedirs(workdir, exist_ok=True)

    results = []
    try:
        for size_mb in SIZES_MB:
            for run in range(1, RUNS + 1):
                results.append(run_case(workdir, size_mb, run))
    finally:
        if cleanup:
            try:
                os.rmdir(workdir)
            except OSError:
                pass

    passed = sum(1 for r in results if r["ok"])
    total = len(results)
    print("\n=== upload integration summary ===", flush=True)
    for r in results:
        mark = "PASS" if r["ok"] else "FAIL"
        extra = "" if r["ok"] else f" — {r['reason']}"
        print(f"  [{mark}] {r['size_mb']} MiB run {r['run']}{extra}", flush=True)
    print(f"{passed}/{total} cases passed", flush=True)
    print(json.dumps({"passed": passed, "total": total, "results": results}), flush=True)
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
