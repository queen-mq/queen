#!/usr/bin/env python3
"""24h soak single-run report — 6 stacked panels over the 24h timeline.
Mirrors the 3-test report style. Inputs in raw/."""
import csv
import os
import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queen-soak24-report.png")


def hms_to_s(h):
    a, b, c = h.split(":")
    return int(a) * 3600 + int(b) * 60 + int(c)


def parse_goload(path):
    rx = re.compile(
        r"\[(\d+:\d+:\d+)\]\s+offered=\s*([\d.]+)/s achieved=\s*([\d.]+)/s shed=\s*([\d.]+)/s.*?"
        r"p50=\s*([\d.]+) p99=\s*([\d.]+) p999=\s*([\d.]+) ms \| "
        r"push=(\d+) pop=(\d+) lag=(-?\d+).*?ack=\s*([\d.]+)/s ackErr=(\d+)"
    )
    t, push_s, pop_s, ack_s, lag, p50, p99, acke = [], [], [], [], [], [], [], []
    last_s = None
    last_push = last_pop = None
    day = 0
    prev_hms = None
    t0 = None
    for line in open(path, errors="replace"):
        m = rx.search(line)
        if not m:
            continue
        hms = hms_to_s(m.group(1))
        if prev_hms is not None and hms < prev_hms - 3600:  # midnight wrap
            day += 1
        prev_hms = hms
        abst = day * 86400 + hms
        if t0 is None:
            t0 = abst
        hrs = (abst - t0) / 3600.0
        pu, po = int(m.group(8)), int(m.group(9))
        if last_push is None:
            last_push, last_pop, last_s = pu, po, abst
            continue
        dt = abst - last_s
        if dt <= 0:
            continue
        t.append(hrs)
        push_s.append((pu - last_push) / dt)
        pop_s.append((po - last_pop) / dt)
        last_push, last_pop, last_s = pu, po, abst
        ack_s.append(float(m.group(11)))
        lag.append(int(m.group(10)))
        p50.append(float(m.group(5)))
        p99.append(float(m.group(6)))
        acke.append(int(m.group(12)))
    return dict(t=t, push=push_s, pop=pop_s, ack=ack_s, lag=lag, p50=p50, p99=p99, ackerr=acke, t0=t0)


def parse_csv(path):
    rows = []
    with open(path, errors="replace") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def fnum(r, k):
    v = r.get(k, "")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def series(rows, k, t0_epoch, scale=1.0):
    t, out = [], []
    for r in rows:
        e, v = fnum(r, "epoch_ms"), fnum(r, k)
        if e is None or v is None:
            continue
        t.append((e - t0_epoch) / 3600000.0)
        out.append(v * scale)
    return t, out


def rate(rows, k, t0_epoch):
    t, out, last, laste = [], [], None, None
    for r in rows:
        e, v = fnum(r, "epoch_ms"), fnum(r, k)
        if e is None or v is None:
            continue
        if last is not None:
            dt = (e - laste) / 1000.0
            if 0 < dt < 120:
                t.append((e - t0_epoch) / 3600000.0)
                out.append((v - last) / dt)
        last, laste = v, e
    return t, out


def main():
    g = parse_goload(os.path.join(RAW, "soak24b.out"))
    bench = parse_csv(os.path.join(RAW, "soak-bench.csv"))
    loader = parse_csv(os.path.join(RAW, "soak-loader.csv"))

    # Anchor CSV time to the goload load start. goload t0 is in absolute
    # seconds-since-first-report-day; bench/loader are epoch_ms. Find the bench
    # epoch where PG CPU first ramps (load start) and use it as the shared 0.
    bench_start = None
    for r in bench:
        if fnum(r, "pg_cpu_pct") and fnum(r, "pg_cpu_pct") > 200:
            bench_start = fnum(r, "epoch_ms")
            break
    if bench_start is None:
        bench_start = fnum(bench[0], "epoch_ms")

    fig, ax = plt.subplots(6, 1, figsize=(20, 26), sharex=True)
    total_g = g["push"] and len(g["push"])
    fig.suptitle(
        "QueenMQ — 24h production-semantics soak (600k msg/s, explicit ack + dedup) — "
        "51.82B messages, 0 restart, err ~0.0001% — commit 615efdc — 2026-07-24/25",
        fontsize=15, y=0.995,
    )

    # 1 throughput
    a = ax[0]
    a.plot(g["t"], g["push"], lw=0.5, label="push msg/s", color="tab:blue")
    a.plot(g["t"], g["pop"], lw=0.5, label="pop msg/s", color="tab:gray", alpha=0.7)
    a.plot(g["t"], g["ack"], lw=0.5, label="ack msg/s", color="tab:green", alpha=0.7)
    a.set_ylabel("msg/s"); a.set_title("Throughput (push / pop / ack)", fontsize=10)
    a.set_ylim(0, 800000); a.legend(loc="lower right", fontsize=8); a.grid(alpha=0.3)

    # 2 lag
    a = ax[1]
    a.plot(g["t"], g["lag"], lw=0.5, color="tab:red")
    a.set_ylabel("messages"); a.set_title("Lag (push − pop backlog)", fontsize=10); a.grid(alpha=0.3)

    # 3 latency
    a = ax[2]
    a.plot(g["t"], g["p50"], lw=0.5, label="p50", color="tab:blue")
    a.plot(g["t"], g["p99"], lw=0.5, label="p99", color="tab:orange")
    a.set_yscale("log"); a.set_ylabel("ms (log)"); a.set_title("End-to-end latency", fontsize=10)
    a.legend(loc="upper right", fontsize=8); a.grid(alpha=0.3)

    # 4 broker + PG cpu/mem
    a = ax[3]
    tt, vv = series(bench, "pg_cpu_pct", bench_start, 0.01); a.plot(tt, vv, lw=0.5, label="PG CPU (cores)", color="tab:blue")
    tt, vv = series(bench, "queen_cpu_pct", bench_start, 0.01); a.plot(tt, vv, lw=0.5, label="Queen CPU (cores)", color="tab:orange")
    a2 = a.twinx()
    tt, vv = series(bench, "pg_mem_mb", bench_start, 1/1024); a2.plot(tt, vv, lw=0.6, ls="--", label="PG mem (GB)", color="tab:cyan")
    tt, vv = series(bench, "queen_mem_mb", bench_start, 1/1024); a2.plot(tt, vv, lw=0.6, ls="--", label="Queen mem (GB)", color="tab:red")
    a.set_ylabel("cores"); a2.set_ylabel("GB (dashed)"); a.set_title("Broker + PG CPU / MEM", fontsize=10)
    a.legend(loc="upper left", fontsize=8); a2.legend(loc="upper right", fontsize=8); a.grid(alpha=0.3)

    # 5 commits / fsync / disk
    a = ax[4]
    tt, vv = rate(bench, "xact_commit_cum", bench_start); a.plot(tt, vv, lw=0.5, label="commit/s", color="tab:blue")
    tt, vv = rate(bench, "wal_fsyncs_cum", bench_start); a.plot(tt, vv, lw=0.5, label="wal fsync/s", color="tab:orange")
    a2 = a.twinx()
    tt, vv = series(bench, "db_size_bytes", bench_start, 1/1e9); a2.plot(tt, vv, lw=1.0, ls="--", color="tab:red", label="DB size (GB)")
    a.set_ylabel("per second"); a2.set_ylabel("GB (dashed)"); a.set_title("PG commits / WAL fsyncs / disk", fontsize=10)
    a.legend(loc="upper left", fontsize=8); a2.legend(loc="lower right", fontsize=8); a.grid(alpha=0.3)

    # 6 loader
    a = ax[5]
    tt, vv = series(loader, "goload_cpu_pct", bench_start, 0.01); a.plot(tt, vv, lw=0.5, label="goload CPU (cores)", color="tab:blue")
    a2 = a.twinx()
    tt, vv = series(loader, "goload_mem_mb", bench_start, 1/1024); a2.plot(tt, vv, lw=0.6, ls="--", label="goload mem (GB)", color="tab:green")
    tt, rx = series(loader, "net_rx_mbps", bench_start); a2.plot(tt, [v/1000 for v in rx], lw=0.5, ls=":", label="net rx (Gbps)", color="tab:purple")
    tt, txv = series(loader, "net_tx_mbps", bench_start); a2.plot(tt, [v/1000 for v in txv], lw=0.5, ls=":", label="net tx (Gbps)", color="tab:brown")
    a.set_ylabel("cores"); a2.set_ylabel("GB / Gbps"); a.set_title("Loader CPU / MEM / network", fontsize=10)
    a.legend(loc="upper left", fontsize=8); a2.legend(loc="upper right", fontsize=8); a.grid(alpha=0.3)
    a.set_xlabel("hours since load start"); a.set_xlim(-0.5, 24.5)

    fig.tight_layout(rect=(0, 0, 1, 0.985))
    fig.savefig(OUT, dpi=100)
    print("wrote", OUT)
    # quick stats
    import statistics as st
    steady = [(t, p) for t, p in zip(g["t"], g["p99"]) if t > 0.5]
    print("p99 median over steady:", round(st.median([p for _, p in steady]), 1), "ms")
    print("push/s median:", round(st.median(g["push"])), " samples:", len(g["push"]))


if __name__ == "__main__":
    main()
