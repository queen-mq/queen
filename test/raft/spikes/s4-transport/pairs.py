#!/usr/bin/env python3
"""Within-round pairwise comparison — the contention-robust read of an
interleaved run (`ROUNDS=n bash run.sh`).

A noisy host moves every absolute number, but inside one round both
configurations saw the same neighbours. This prints, per pair and per metric,
how many rounds each side won and the median ratio.

    python3 pairs.py results/laptop-interleaved/results.jsonl
"""
import json
import statistics
import sys

PAIRS = [
    # (label, key A, key B) — key = (transport, tls, mac, conns_cfg)
    ("framed TCP vs HTTP (no auth)", ("tcp", 0, 0, 2), ("http", 0, 0, 64)),
    ("framed TCP vs HTTP (authenticated)", ("tcp", 0, 1, 2), ("http", 0, 1, 64)),
    ("framed TCP+TLS vs HTTPS+mac", ("tcp", 1, 0, 2), ("http", 1, 1, 64)),
    ("per-frame MAC cost (tcp+mac vs tcp)", ("tcp", 0, 1, 2), ("tcp", 0, 0, 2)),
    ("TLS cost (tcp+tls vs tcp)", ("tcp", 1, 0, 2), ("tcp", 0, 0, 2)),
    ("MAC vs TLS (tcp+mac vs tcp+tls)", ("tcp", 0, 1, 2), ("tcp", 1, 0, 2)),
]
METRICS = [("p50", "rt_send_p50_us"), ("leader CPU/msg", "leader_cpu_us_per_msg"),
           ("recv CPU/msg", "recv_cpu_us_per_msg"), ("bytes/msg", "wire_bytes_per_msg")]


def key(d):
    return (d["transport"], int(d["tls"]), int(d["mac"]), d["conns_cfg"])


def main(path):
    rows = [json.loads(l) for l in open(path) if l.strip()]
    rates = sorted({d["rate_msgs"] for d in rows})
    by = {}
    for d in rows:
        by.setdefault((key(d), d["rate_msgs"]), []).append(d)
    for rate in rates:
        print(f"\n### offered {rate:,} msg/s  (rounds compared pairwise)\n")
        print("| pair | metric | A/B median ratio | rounds A lower | n |")
        print("|---|---|---|---|---|")
        for lab, ka, kb in PAIRS:
            ga, gb = by.get((ka, rate), []), by.get((kb, rate), [])
            n = min(len(ga), len(gb))
            if n == 0:
                continue
            for mlab, m in METRICS:
                ratios = [ga[i][m] / gb[i][m] for i in range(n) if gb[i][m]]
                lower = sum(1 for i in range(n) if ga[i][m] < gb[i][m])
                print(f"| {lab} | {mlab} | {statistics.median(ratios):.2f}x | {lower}/{n} | {n} |")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results/laptop-interleaved/results.jsonl")
