#!/usr/bin/env python3
"""Summarize soak JSONL event files into the report's metrics.

Usage: perf/soak_summary.py perf/results/soak-<label>-<ts>.jsonl [...]
"""

import json
import statistics
import sys
from collections import Counter, defaultdict


def pct(n, d):
    return f"{100.0 * n / d:.2f}%" if d else "—"


def classify(msg):
    m = (msg or "").lower()
    if "exhausted pushsync peers" in m:
        return "exhausted-peers"
    if "shallow" in m:
        return "shallow"
    if "timed out" in m:
        return "timeout"
    if "no peers available" in m:
        return "no-peers"
    if "not usable" in m:
        return "batch"
    return (msg or "?")[:60] or "?"


def summarize(path):
    evs = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    evs.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    by = defaultdict(list)
    for e in evs:
        by[e.get("kind", "?")].append(e)

    print(f"\n=== {path} ({len(evs)} events) ===")

    # Writes
    w = by.get("write", [])
    if w:
        total = len(w)
        by_status = Counter(e["status"] for e in w)
        first = [e for e in w if e["attempt"] == 1]
        first_fail = [e for e in first if e["status"] != 201]
        print(f"write attempts: {total}; status mix {dict(by_status)}")
        print(
            f"  first-attempt count={len(first)} fail={len(first_fail)} "
            f"→ first-attempt failure rate {pct(len(first_fail), len(first))}"
        )
        msgs = Counter(classify(e.get("message")) for e in w if e["status"] != 201)
        if msgs:
            print(f"  non-201 message classes: {dict(msgs.most_common(6))}")
        gaveup = len(by.get("write_gaveup", []))
        print(f"  gave up after {8} attempts: {gaveup}")
        # correlate failures with peers + po
        fail_peers = [e["peers"] for e in first_fail if "peers" in e]
        ok_peers = [e["peers"] for e in first if e["status"] == 201 and "peers" in e]
        if fail_peers and ok_peers:
            print(
                f"  peers at first-attempt: ok median {statistics.median(ok_peers)}, "
                f"fail median {statistics.median(fail_peers)}"
            )
        fail_po = [e["po_best"] for e in first_fail if e.get("po_best", -1) >= 0]
        ok_po = [e["po_best"] for e in first if e["status"] == 201 and e.get("po_best", -1) >= 0]
        if fail_po and ok_po:
            print(
                f"  best-connected-PO: ok median {statistics.median(ok_po)}, "
                f"fail median {statistics.median(fail_po)}"
            )
        lat_ok = [e["latency_ms"] for e in w if e["status"] == 201]
        if lat_ok:
            print(
                f"  201 latency ms: median {statistics.median(lat_ok):.0f} "
                f"p95 {sorted(lat_ok)[int(0.95 * (len(lat_ok) - 1))]}"
            )

    # Read-after-own-write
    for kind, name in (("readback_soc", "SOC read-back"), ("readback_feed", "feed latest")):
        r = by.get(kind, [])
        if not r:
            continue
        ok = [e for e in r if e["ok"]]
        first_bad = [e for e in r if e["first_status"] != 200]
        ms = sorted(e["ms_to_ok"] for e in ok)
        print(
            f"{name}: n={len(r)} ok={len(ok)} first-attempt-fail rate "
            f"{pct(len(first_bad), len(r))} (statuses {dict(Counter(e['first_status'] for e in first_bad))})"
        )
        if ms:
            print(
                f"  ms-to-readable: median {ms[len(ms)//2]}, "
                f"p99 {ms[int(0.99 * (len(ms) - 1))]}, max {ms[-1]}"
            )

    # Cross-node
    x = by.get("xnode", [])
    if x:
        per_addr = defaultdict(dict)
        for e in x:
            per_addr[e["addr"]][e["after_secs"]] = e["status"]
        n = len(per_addr)
        ok_by_t = Counter()
        never = 0
        for statuses in per_addr.values():
            landed = [t for t, s in sorted(statuses.items()) if s == 200]
            if landed:
                ok_by_t[landed[0]] += 1
            else:
                never += 1
        cum = 0
        parts = []
        for t in (1, 10, 60, 300):
            cum += ok_by_t.get(t, 0)
            parts.append(f"≤{t}s: {pct(cum, n)}")
        print(f"cross-node: n={n} retrievable {' | '.join(parts)}; never {never} ({pct(never, n)})")

    # Peer timeline
    p = by.get("peers", [])
    if p:
        wp = [e["writer_peers"] for e in p]
        print(
            f"writer peers over run: min {min(wp)} median {statistics.median(wp)} max {max(wp)}"
        )


for path in sys.argv[1:]:
    summarize(path)
