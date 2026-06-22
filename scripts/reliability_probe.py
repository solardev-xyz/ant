#!/usr/bin/env python3
"""Upload-availability probe for AntDrive-style uploads.

Mirrors exactly what the AntDrive app does: upload a file through the daemon's
`antctl upload start` job path (which builds a single-file mantaray manifest
and pushsyncs every chunk), then measure how quickly the result becomes
retrievable from *several independent nodes in different neighbourhoods* plus a
public gateway — i.e. whether the file is available "on all corners" the moment
the upload reports `completed`.

For each (size, run) it:
  1. generates a random file,
  2. uploads it via `antctl upload start --json` against the uploader socket,
  3. captures the manifest reference and the moment the upload call returns,
  4. probes every verifier (and the public gateway) on a schedule of offsets
     measured from that return, recording the first offset at which:
        * the manifest *root chunk* is retrievable  (GET /chunks/<root>)
        * the *whole file* is retrievable + SHA-correct (GET /bzz/<root>/)
  5. records the daemon's final job status.

A perfect result is: every target serves the whole file correctly at the very
first probe (offset 0) for every (size, run). Anything later than offset 0 is
propagation lag — the "I had to push again" symptom.

Stdlib only. Config via env (see DEFAULTS below) or --json-config.
"""

from __future__ import annotations

import concurrent.futures as cf
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
UPLOADER_SOCKET = os.environ.get("ANT_UPLOADER_SOCKET", "")
BATCH = os.environ.get("ANT_BATCH", "").strip()
# Comma-separated verifier base URLs, e.g. http://127.0.0.1:1810,http://127.0.0.1:1811
VERIFIERS = [v.strip().rstrip("/") for v in os.environ.get("ANT_VERIFIERS", "").split(",") if v.strip()]
# Optional labels (overlay short hashes) aligned with VERIFIERS, comma-separated.
VERIFIER_LABELS = [s.strip() for s in os.environ.get("ANT_VERIFIER_LABELS", "").split(",") if s.strip()]
PUBLIC_GATEWAY = os.environ.get("ANT_GATEWAY", "https://vibing.at/ant").rstrip("/")
USE_PUBLIC = os.environ.get("ANT_USE_PUBLIC", "1") not in ("0", "", "no", "false")
# Sizes accept human suffixes: 1K, 100K, 1M, 3M, 4M, 8M (binary).
SIZES = os.environ.get("ANT_SIZES", "1K,100K,1M,3M,4M,8M")
RUNS = int(os.environ.get("ANT_RUNS", "3"))
# Probe offsets (seconds from upload-return). 0 = immediately.
SCHEDULE = [float(x) for x in os.environ.get("ANT_SCHEDULE", "0,3,8,20,45,90").split(",") if x.strip()]
UPLOAD_TIMEOUT = int(os.environ.get("ANT_UPLOAD_TIMEOUT", "600"))
HTTP_TIMEOUT = int(os.environ.get("ANT_HTTP_TIMEOUT", "12"))
WORKDIR = os.environ.get("ANT_WORKDIR", "")
OUT_JSON = os.environ.get("ANT_OUT", "")

MIB = 1 << 20


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_size(s: str) -> int:
    s = s.strip().upper()
    mult = 1
    if s.endswith("K"):
        mult, s = 1 << 10, s[:-1]
    elif s.endswith("M"):
        mult, s = 1 << 20, s[:-1]
    elif s.endswith("G"):
        mult, s = 1 << 30, s[:-1]
    return int(float(s) * mult)


def human(n: int) -> str:
    for unit, sz in (("M", 1 << 20), ("K", 1 << 10)):
        if n >= sz:
            return f"{n / sz:.0f}{unit}" if n % sz == 0 else f"{n / sz:.1f}{unit}"
    return f"{n}B"


def parse_json_objects(s: str) -> list:
    dec = json.JSONDecoder()
    out, i, n = [], 0, len(s)
    while i < n:
        while i < n and s[i] in " \t\r\n":
            i += 1
        if i >= n:
            break
        try:
            obj, end = dec.raw_decode(s, i)
            out.append(obj)
            i = end
        except json.JSONDecodeError:
            i += 1
    return out


def antctl(args: list[str], timeout: int) -> str:
    cmd = [ANTCTL] + args
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} exited {p.returncode}: {p.stderr.strip()}")
    return p.stdout


def gen_file(path: str, size: int) -> str:
    h = hashlib.sha256()
    with open(path, "wb") as f:
        remaining = size
        while remaining > 0:
            block = os.urandom(min(remaining, MIB))
            f.write(block)
            h.update(block)
            remaining -= len(block)
    return h.hexdigest()


AWAIT_SYNC = os.environ.get("ANT_AWAIT_SYNC", "0") not in ("0", "", "no", "false")


def upload(path: str, name: str) -> tuple[str, str, float]:
    """Returns (reference_hex_no0x, final_status, upload_seconds)."""
    args = ["upload", "start", "--json", "--socket", UPLOADER_SOCKET,
            "--name", name, "--content-type", "image/jpeg"]
    if AWAIT_SYNC:
        # Block until the daemon's heal confirms deep reachability, so the
        # upload call returns only when the file is retrievable everywhere.
        args.append("--await-sync")
    if BATCH:
        args += ["--batch", BATCH]
    args.append(path)
    t0 = time.monotonic()
    out = antctl(args, UPLOAD_TIMEOUT)
    dt = time.monotonic() - t0
    objs = parse_json_objects(out)
    ref, status = "", ""
    for o in objs:
        if isinstance(o, dict):
            if o.get("reference"):
                ref = o["reference"]
            if o.get("status"):
                status = o["status"]
    ref = ref[2:] if ref.lower().startswith("0x") else ref
    return ref, status, dt


def http_ok(url: str, expect_sha: str | None) -> bool:
    try:
        req = urllib.request.Request(url, headers={"Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            if r.status not in (200, 206):
                return False
            data = r.read()
    except Exception:
        return False
    if expect_sha is None:
        return len(data) > 0
    return hashlib.sha256(data).hexdigest() == expect_sha


def probe_target(base: str, ref: str, expect_sha: str) -> tuple[bool, bool]:
    """Returns (root_chunk_ok, whole_file_ok) for one target."""
    root_ok = http_ok(f"{base}/chunks/{ref}", None)
    file_ok = http_ok(f"{base}/bzz/{ref}/", expect_sha)
    return root_ok, file_ok


def targets() -> list[tuple[str, str]]:
    ts = []
    for i, v in enumerate(VERIFIERS):
        label = VERIFIER_LABELS[i] if i < len(VERIFIER_LABELS) else f"verifier{i}"
        ts.append((label, v))
    if USE_PUBLIC:
        ts.append(("public-gw", PUBLIC_GATEWAY))
    return ts


def run_one(size: int, run_idx: int, workdir: str) -> dict:
    name = f"photo_{human(size)}_{run_idx}.jpg"
    path = os.path.join(workdir, name)
    sha = gen_file(path, size)
    ref, status, up_s = upload(path, name)
    if not ref:
        log(f"  !! upload returned no reference (status={status})")
        return {"size": size, "run": run_idx, "error": "no-reference", "status": status}
    log(f"  uploaded {human(size)} run{run_idx}: ref={ref[:12]} status={status} in {up_s:.1f}s")

    tl = targets()
    # first_file[t] / first_root[t] = offset at which target t first served it.
    first_file: dict[str, float | None] = {t[0]: None for t in tl}
    first_root: dict[str, float | None] = {t[0]: None for t in tl}
    t_return = time.monotonic()

    for offset in SCHEDULE:
        due = t_return + offset
        now = time.monotonic()
        if due > now:
            time.sleep(due - now)
        pending = [t for t in tl if first_file[t[0]] is None]
        if not pending:
            break
        with cf.ThreadPoolExecutor(max_workers=len(pending)) as ex:
            futs = {ex.submit(probe_target, base, ref, sha): label for (label, base) in pending}
            for fut in cf.as_completed(futs):
                label = futs[fut]
                try:
                    root_ok, file_ok = fut.result()
                except Exception:
                    root_ok, file_ok = False, False
                if root_ok and first_root[label] is None:
                    first_root[label] = offset
                if file_ok and first_file[label] is None:
                    first_file[label] = offset
        got = sum(1 for t in tl if first_file[t[0]] is not None)
        log(f"    offset {offset:>4}s: {got}/{len(tl)} targets fully available")

    return {
        "size": size,
        "run": run_idx,
        "reference": ref,
        "status": status,
        "upload_seconds": round(up_s, 2),
        "first_file": first_file,
        "first_root": first_root,
        "targets": [t[0] for t in tl],
    }


def summarize(results: list[dict]) -> str:
    lines = []
    lines.append("# Upload availability report\n")
    lines.append(f"- schedule (s): {SCHEDULE}")
    lines.append(f"- runs/size: {RUNS}, verifiers: {len(VERIFIERS)}, public_gw: {USE_PUBLIC}")
    lines.append(f"- batch: {BATCH}\n")
    sizes = sorted({r["size"] for r in results if "error" not in r})
    lines.append("## Fraction of (run x target) fully available by offset\n")
    header = "| size | " + " | ".join(f"t={s}s" for s in SCHEDULE) + " | never |"
    lines.append(header)
    lines.append("|" + "---|" * (len(SCHEDULE) + 2))
    for size in sizes:
        rs = [r for r in results if r.get("size") == size and "error" not in r]
        total = 0
        cum = {s: 0 for s in SCHEDULE}
        never = 0
        for r in rs:
            for label, off in r["first_file"].items():
                total += 1
                if off is None:
                    never += 1
                else:
                    for s in SCHEDULE:
                        if off <= s:
                            cum[s] += 1
        if total == 0:
            continue
        cells = [f"{100*cum[s]/total:.0f}%" for s in SCHEDULE]
        lines.append(f"| {human(size)} | " + " | ".join(cells) + f" | {never} |")
    lines.append("\n## At-return (offset 0) availability per target\n")
    # Per-target t=0 success rate.
    by_target: dict[str, list[int]] = {}
    for r in results:
        if "error" in r:
            continue
        for label, off in r["first_file"].items():
            by_target.setdefault(label, []).append(1 if off == 0 else 0)
    for label, vals in by_target.items():
        rate = 100 * sum(vals) / len(vals) if vals else 0
        lines.append(f"- **{label}**: {rate:.0f}% available at offset 0 ({sum(vals)}/{len(vals)})")
    lines.append("\n## Per-(size,run) detail\n")
    for r in results:
        if "error" in r:
            lines.append(f"- {human(r['size'])} run{r['run']}: ERROR {r['error']} status={r.get('status')}")
            continue
        lags = {k: v for k, v in r["first_file"].items()}
        lines.append(f"- {human(r['size'])} run{r['run']} ref={r['reference'][:10]} "
                     f"status={r['status']} up={r['upload_seconds']}s first_file={lags}")
    return "\n".join(lines) + "\n"


def main() -> int:
    if not UPLOADER_SOCKET:
        log("ANT_UPLOADER_SOCKET is required")
        return 2
    if not VERIFIERS and not USE_PUBLIC:
        log("no verifiers and public gateway disabled — nothing to probe")
        return 2
    workdir = WORKDIR or tempfile.mkdtemp(prefix="ant-rel-")
    os.makedirs(workdir, exist_ok=True)
    sizes = [parse_size(s) for s in SIZES.split(",") if s.strip()]
    log(f"sizes={[human(s) for s in sizes]} runs={RUNS} verifiers={VERIFIERS} public={USE_PUBLIC}")
    results = []
    for size in sizes:
        for run_idx in range(RUNS):
            try:
                results.append(run_one(size, run_idx, workdir))
            except Exception as e:
                log(f"  !! {human(size)} run{run_idx} failed: {e}")
                results.append({"size": size, "run": run_idx, "error": str(e)})
    report = summarize(results)
    print("\n" + report)
    if OUT_JSON:
        with open(OUT_JSON, "w") as f:
            json.dump({"schedule": SCHEDULE, "results": results}, f, indent=2)
        with open(OUT_JSON.replace(".json", ".md"), "w") as f:
            f.write(report)
        log(f"wrote {OUT_JSON} and {OUT_JSON.replace('.json', '.md')}")
    # Exit non-zero if anything was ever unavailable at offset 0.
    any_lag = any(
        off != 0 for r in results if "error" not in r for off in r["first_file"].values()
    ) or any("error" in r for r in results)
    return 1 if any_lag else 0


if __name__ == "__main__":
    sys.exit(main())
