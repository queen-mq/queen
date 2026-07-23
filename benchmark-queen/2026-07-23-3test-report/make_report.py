#!/usr/bin/env python3
"""Single-PNG 3x6 report (Alice's mockup): one column per test, six chart rows.

Inputs (raw/):
  t{1,2}.out        goload -mode openloop stdout, -report 1
  t3.out            goload -mode cm stdout, -report 1
  bench-t{1,2,3}.csv   bench-sampler.sh (broker VM, 1 Hz)
  loader-t{1,2,3}.csv  loader-sampler.sh (loader VM, 1 Hz)

Rows: throughput / lag / latency p50+p99 / broker+PG cpu+mem /
      commit+fsync+disk / loader cpu+mem+net.
"""
import csv
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queen-3test-report.png")

# ---------------------------------------------------------------- parsers


def hms_to_s(h):
    a, b, c = h.split(":")
    return int(a) * 3600 + int(b) * 60 + int(c)


def parse_ol(path):
    """goload openloop stdout -> per-second series (cумulative push/pop deltas)."""
    t, ach, push_s, pop_s, ack_s, lag, p50, p99 = [], [], [], [], [], [], [], []
    lpush = lpop = None
    rx_main = re.compile(
        r"\[(\d+:\d+:\d+)\]\s+offered=\s*([\d.]+)/s achieved=\s*([\d.]+)/s.*?"
        r"p50=\s*([\d.]+) p99=\s*([\d.]+).*?push=(\d+) pop=(\d+) lag=(-?\d+)"
    )
    rx_ack = re.compile(r"ack=\s*([\d.]+)/s")
    for line in open(path, errors="replace"):
        m = rx_main.search(line)
        if not m:
            continue
        ts = hms_to_s(m.group(1))
        pu, po = int(m.group(6)), int(m.group(7))
        if lpush is None:
            lpush, lpop = pu, po
            t0 = ts
            continue
        t.append(ts - t0)
        ach.append(float(m.group(3)))
        push_s.append(pu - lpush)
        pop_s.append(po - lpop)
        lpush, lpop = pu, po
        lag.append(int(m.group(8)))
        p50.append(float(m.group(4)))
        p99.append(float(m.group(5)))
        ma = rx_ack.search(line)
        ack_s.append(float(ma.group(1)) if ma else 0.0)
    # Trim the teardown tail (pacer stopped, drain-only rows with achieved=0):
    # those rows only encode the cancel artifact and wreck the lag scale.
    while ach and ach[-1] == 0.0:
        for s in (t, ach, push_s, pop_s, ack_s, lag, p50, p99):
            s.pop()
    return dict(t=t, push=push_s, pop=pop_s, ack=ack_s, lag=lag, p50=p50, p99=p99)


def parse_cm(path):
    """goload cm stdout -> per-second series (fields are already rates)."""
    keys = ("t", "push", "pop", "ack", "e2e", "lag_av", "lag_pr", "lag_os", "lag_op", "p50", "p99")
    d = {k: [] for k in keys}
    rx = re.compile(
        r"\[(\d+:\d+:\d+)\] prodA=\s*(\d+) prodB=\s*(\d+) \| db=\s*(\d+) cal=\s*(\d+) "
        r"ota=\s*(\d+) otap=\s*(\d+) acked=\s*(\d+) e2e=\s*(\d+)/s \| "
        r"p50=([\d.]+) p99=\s*([\d.]+) ms \| lag avail=(-?\d+) prices=(-?\d+) "
        r"otaSync=(-?\d+) otaPrices=(-?\d+)"
    )
    t0 = None
    for line in open(path, errors="replace"):
        m = rx.search(line)
        if not m:
            continue
        ts = hms_to_s(m.group(1))
        if t0 is None:
            t0 = ts
        pa, pb, db, cal, ota, otap = (int(m.group(i)) for i in range(2, 8))
        d["t"].append(ts - t0)
        d["push"].append(pa + pb + db + cal)        # ingress singles + derived batched msgs
        d["pop"].append(db + cal + ota + otap)      # delivered msgs across all stages
        d["ack"].append(int(m.group(8)))
        d["e2e"].append(int(m.group(9)))
        d["p50"].append(float(m.group(10)))
        d["p99"].append(float(m.group(11)))
        d["lag_av"].append(int(m.group(12)))
        d["lag_pr"].append(int(m.group(13)))
        d["lag_os"].append(int(m.group(14)))
        d["lag_op"].append(int(m.group(15)))
    return d


def parse_csv(path):
    rows = []
    with open(path, errors="replace") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def f(row, k):
    v = row.get(k, "")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def series(rows, k, scale=1.0):
    t, out, t0 = [], [], None
    for r in rows:
        e, v = f(r, "epoch_ms"), f(r, k)
        if e is None or v is None:
            continue
        if t0 is None:
            t0 = e
        t.append((e - t0) / 1000.0)
        out.append(v * scale)
    return t, out


def rate(rows, k):
    """1 Hz delta of a cumulative counter."""
    t, out, t0, last = [], [], None, None
    for r in rows:
        e, v = f(r, "epoch_ms"), f(r, k)
        if e is None or v is None:
            continue
        if t0 is None:
            t0 = e
        if last is not None:
            dt = (e - laste) / 1000.0
            if dt > 0:
                t.append((e - t0) / 1000.0)
                out.append((v - last) / dt)
        last, laste = v, e
    return t, out


# ---------------------------------------------------------------- figure

TESTS = [
    ("t1", "Test 1 — autoAck (QOS0) + dedup 300s\n1M msg/s per side, push-batch 100, 100 partitions"),
    ("t2", "Test 2 — explicit batch acks (QOS1) + dedup 300s\n900k msg/s per side, 200 partitions, ack-async"),
    ("t3", "Test 3 — channel manager (real app)\n25k events/s e2e, 1000 properties, single pushes, total order"),
]


def main():
    fig, axes = plt.subplots(6, 3, figsize=(26, 24))
    fig.suptitle(
        "QueenMQ — 3-test certification report (10 min each, 1 s resolution) — "
        "rustserverandstorage / fix7 — 2026-07-23",
        fontsize=16, y=0.995,
    )

    for col, (tag, title) in enumerate(TESTS):
        out = os.path.join(RAW, f"{tag}.out")
        bcsv = os.path.join(RAW, f"bench-{tag}.csv")
        lcsv = os.path.join(RAW, f"loader-{tag}.csv")
        run = (parse_cm if tag == "t3" else parse_ol)(out) if os.path.exists(out) else None
        bench = parse_csv(bcsv) if os.path.exists(bcsv) else []
        loader = parse_csv(lcsv) if os.path.exists(lcsv) else []

        ax = axes[0][col]
        ax.set_title(title, fontsize=11)
        if run:
            ax.plot(run["t"], run["push"], lw=0.7, label="push msg/s")
            ax.plot(run["t"], run["pop"], lw=0.7, label="pop msg/s")
            if any(run["ack"]):
                ax.plot(run["t"], run["ack"], lw=0.7, label="ack msg/s")
            if tag == "t3":
                ax.plot(run["t"], run["e2e"], lw=1.2, label="events e2e/s")
        ax.set_ylabel("msg/s")
        ax.legend(loc="upper right", fontsize=7)
        ax.grid(alpha=0.3)

        ax = axes[1][col]
        if run:
            if tag == "t3":
                ax.plot(run["t"], run["lag_av"], lw=0.7, label="cm-avail")
                ax.plot(run["t"], run["lag_pr"], lw=0.7, label="cm-prices")
                ax.plot(run["t"], run["lag_os"], lw=0.7, label="cm-ota-sync")
                ax.plot(run["t"], run["lag_op"], lw=0.7, label="cm-ota-prices")
            else:
                ax.plot(run["t"], run["lag"], lw=0.8, label="lag (push-pop)")
        ax.set_ylabel("messages")
        ax.set_title("Lag", fontsize=9)
        ax.legend(loc="upper right", fontsize=7)
        ax.grid(alpha=0.3)

        ax = axes[2][col]
        if run:
            ax.plot(run["t"], run["p50"], lw=0.8, label="p50")
            ax.plot(run["t"], run["p99"], lw=0.8, label="p99")
            ax.set_yscale("log")
        ax.set_ylabel("ms (log)")
        ax.set_title("Latency (e2e for T3, push→pop for T1/T2)", fontsize=9)
        ax.legend(loc="upper right", fontsize=7)
        ax.grid(alpha=0.3)

        ax = axes[3][col]
        if bench:
            for key, lbl in (("pg_cpu_pct", "PG CPU"), ("queen_cpu_pct", "Queen CPU")):
                tt, vv = series(bench, key, 0.01)
                ax.plot(tt, vv, lw=0.8, label=f"{lbl} (cores)")
            ax2 = ax.twinx()
            for key, lbl in (("pg_mem_mb", "PG mem"), ("queen_mem_mb", "Queen mem")):
                tt, vv = series(bench, key, 1.0 / 1024)
                ax2.plot(tt, vv, lw=0.8, ls="--", label=f"{lbl} (GB)")
            ax2.set_ylabel("GB (dashed)")
            ax2.legend(loc="lower right", fontsize=7)
        ax.set_ylabel("cores")
        ax.set_title("Broker + PG CPU / MEM", fontsize=9)
        ax.legend(loc="upper right", fontsize=7)
        ax.grid(alpha=0.3)

        ax = axes[4][col]
        if bench:
            tt, vv = rate(bench, "xact_commit_cum")
            ax.plot(tt, vv, lw=0.8, label="commit/s")
            tt, vv = rate(bench, "wal_fsyncs_cum")
            ax.plot(tt, vv, lw=0.8, label="wal fsync/s")
            ax2 = ax.twinx()
            tt, vv = series(bench, "db_size_bytes", 1.0 / (1 << 30))
            ax2.plot(tt, vv, lw=1.0, ls="--", color="tab:red", label="DB size (GB)")
            ax2.set_ylabel("GB (dashed)")
            ax2.legend(loc="lower right", fontsize=7)
        ax.set_ylabel("per second")
        ax.set_title("PG commits / WAL fsyncs / disk", fontsize=9)
        ax.legend(loc="upper left", fontsize=7)
        ax.grid(alpha=0.3)

        ax = axes[5][col]
        if loader:
            tt, vv = series(loader, "goload_cpu_pct", 0.01)
            ax.plot(tt, vv, lw=0.8, label="goload CPU (cores)")
            ax2 = ax.twinx()
            tt, vv = series(loader, "goload_mem_mb", 1.0 / 1024)
            ax2.plot(tt, vv, lw=0.8, ls="--", label="goload mem (GB)")
            tt, rxv = series(loader, "net_rx_mbps")
            tt2, txv = series(loader, "net_tx_mbps")
            ax2.plot(tt, [v / 1000 for v in rxv], lw=0.6, ls=":", label="net rx (Gbps)")
            ax2.plot(tt2, [v / 1000 for v in txv], lw=0.6, ls=":", label="net tx (Gbps)")
            ax2.set_ylabel("GB / Gbps (dashed)")
            ax2.legend(loc="lower right", fontsize=7)
        ax.set_ylabel("cores")
        ax.set_title("Loader CPU / MEM / network", fontsize=9)
        ax.legend(loc="upper left", fontsize=7)
        ax.grid(alpha=0.3)
        ax.set_xlabel("seconds since run start")

    fig.tight_layout(rect=(0, 0, 1, 0.985))
    fig.savefig(OUT, dpi=110)
    print("wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())
