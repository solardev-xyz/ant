#!/usr/bin/env python3
"""Summarize perf/results/*.json into markdown tables (medians + spread).

Usage: perf/summarize.py [--label baseline] [--dir perf/results]

Groups: upload by (label, size_mib); download by (label, reference,
cold/warm); feed by (label, head_index, cold/warm); warmup by label.
Spread = min–max. Median over completed runs only (failures listed
separately with their outcome).
"""

import argparse
import json
import pathlib
import statistics
from collections import defaultdict


def fmt(x, nd=1):
    return "—" if x is None else f"{x:.{nd}f}"


def med_spread(values, nd=1):
    if not values:
        return "—"
    m = statistics.median(values)
    return f"{m:.{nd}f} ({min(values):.{nd}f}–{max(values):.{nd}f}, n={len(values)})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default=None)
    ap.add_argument("--dir", default=str(pathlib.Path(__file__).parent / "results"))
    args = ap.parse_args()

    results = []
    for p in sorted(pathlib.Path(args.dir).glob("*.json")):
        try:
            d = json.loads(p.read_text())
        except Exception:
            continue
        if isinstance(d, dict) and "suite" in d:
            if args.label is None or d.get("label") == args.label:
                results.append(d)

    # ---- upload -----------------------------------------------------------
    up = defaultdict(list)
    for d in results:
        if d["suite"] == "upload":
            up[(d["label"], d["size_mib"])].append(d)
    if up:
        print("### Upload\n")
        print("| label | size | outcome | median KiB/s (spread) | median secs | requeues | peers@start | window |")
        print("|---|---|---|---|---|---|---|---|")
        for (label, size), runs in sorted(up.items()):
            ok = [r for r in runs if r["outcome"] == "completed"]
            bad = [r for r in runs if r["outcome"] != "completed"]
            thr = [r["throughput_kib_s"] for r in ok]
            secs = [r["duration_secs"] for r in ok]
            req = [r["chunks_requeued"] for r in ok]
            peers = [r["peers_at_start"] for r in runs]
            window = f'{runs[0]["started_utc"][:16]}–{runs[-1].get("ended_utc", "")[:16]}'
            print(
                f"| {label} | {size} MiB | {len(ok)}/{len(runs)} ok "
                f"| {med_spread(thr)} | {med_spread(secs, 0)} "
                f"| {med_spread([float(x) for x in req], 0)} "
                f"| {med_spread([float(p) for p in peers], 0)} | {window} |"
            )
            for r in bad:
                print(
                    f"| ↳ run {r['run']} | {size} MiB | **{r['outcome']}** "
                    f"| {fmt(r['throughput_kib_s'])} KiB/s partial "
                    f"| {fmt(r['duration_secs'], 0)}s | {r['chunks_requeued']} "
                    f"| {r['peers_at_start']} | bytes {r['bytes_pushed']}/{r['size_mib']*1048576} |"
                )
        print()

    # ---- download ---------------------------------------------------------
    dl = defaultdict(lambda: {"cold_ttfb": [], "cold_thr": [], "warm_ttfb": [], "warm_thr": [], "fail": 0, "n": 0})
    for d in results:
        if d["suite"] == "download":
            for f in d["fetches"]:
                key = (d["label"], f["reference"][:24])
                e = dl[key]
                e["n"] += 1
                for phase in ("cold", "warm"):
                    ph = f[phase]
                    if ph.get("ok"):
                        e[f"{phase}_ttfb"].append(ph["ttfb_secs"])
                        e[f"{phase}_thr"].append(ph["throughput_kib_s"])
                    elif phase == "cold":
                        e["fail"] += 1
    if dl:
        print("### Download (cold = fresh daemon, warm = same daemon 2nd fetch)\n")
        print("| label | ref | ok | cold TTFB s | cold KiB/s | warm TTFB s | warm KiB/s |")
        print("|---|---|---|---|---|---|---|")
        for (label, ref), e in sorted(dl.items()):
            print(
                f"| {label} | {ref}… | {e['n']-e['fail']}/{e['n']} "
                f"| {med_spread(e['cold_ttfb'], 2)} | {med_spread(e['cold_thr'])} "
                f"| {med_spread(e['warm_ttfb'], 2)} | {med_spread(e['warm_thr'])} |"
            )
        print()

    # ---- feed -------------------------------------------------------------
    fd = defaultdict(lambda: defaultdict(list))
    for d in results:
        if d["suite"] == "feed":
            for r in d["resolutions"]:
                for t in r["timings"]:
                    if t["ok"]:
                        phase = "cold" if t["cold"] else "warm"
                        fd[(d["label"], r["head_index"])][phase].append(t["secs"])
    if fd:
        print("### Feed head resolution\n")
        print("| label | head index | cold secs | warm secs |")
        print("|---|---|---|---|")
        for (label, head), phases in sorted(fd.items()):
            print(
                f"| {label} | {head} | {med_spread(phases.get('cold', []), 2)} "
                f"| {med_spread(phases.get('warm', []), 2)} |"
            )
        print()

    # ---- warmup ------------------------------------------------------------
    wu = defaultdict(list)
    for d in results:
        if d["suite"] == "warmup":
            wu[d["label"]].append(d)
    if wu:
        print("### Session warm-up (process spawn → N BZZ-handshaked peers)\n")
        print("| label | api-up s | to 30 peers s | peers @end |")
        print("|---|---|---|---|")
        for label, runs in sorted(wu.items()):
            print(
                f"| {label} | {med_spread([r['api_up_secs'] for r in runs], 2)} "
                f"| {med_spread([r['secs_to_target'] for r in runs if r['target_reached']], 2)} "
                f"| {med_spread([float(r['peers_at_end']) for r in runs], 0)} |"
            )
        print()


if __name__ == "__main__":
    main()
