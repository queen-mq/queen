#!/usr/bin/env python3
"""Chart the pgmq single-queue saturation sweep (direct connections, MODE=plain).
Parses sweep.log per-step summaries + per-step docker-stats for PG CPU."""
import os, re, glob
import matplotlib; matplotlib.use("Agg")
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
txt = open(os.path.join(BASE, "sweep.log")).read()

# ---- parse per-step summaries ----
blocks = re.split(r'######## STEP: (\d+) clients/role', txt)
rows = []
for i in range(1, len(blocks), 2):
    n = int(blocks[i]); body = blocks[i + 1]
    def g(pat):
        m = re.search(pat, body); return float(m.group(1)) if m else np.nan
    rows.append(dict(
        n=n,
        push=g(r'PRODUCER:\s*msg/s=(\d+)'),
        push_p99=g(r'PRODUCER:.*?p99=([\d.]+)ms'),
        pop=g(r'CONSUMER:\s*msg/s=(\d+)'),
        pop_p99=g(r'CONSUMER:.*?p99=([\d.]+)ms'),
        ba=g(r'avg_active=([\d.]+)'),
    ))
rows = [r for r in rows if not np.isnan(r["push"])]
rows.sort(key=lambda r: r["n"])

# ---- PG CPU (cores) per step from docker-stats ----
def cpu_cores(n):
    f = os.path.join(BASE, "results", f"plain-direct-c{n}.dockerstats.csv")
    if not os.path.exists(f): return np.nan
    vals = []
    for ln in open(f):
        m = re.search(r',([\d.]+)%,', ln)
        if m:
            v = float(m.group(1))
            if v > 100: vals.append(v / 100.0)   # ignore idle setup samples
    return float(np.median(vals)) if vals else np.nan

clients = np.array([r["n"] for r in rows])
push    = np.array([r["push"] for r in rows]) / 1000.0
pop     = np.array([r["pop"] for r in rows]) / 1000.0
push_p99= np.array([r["push_p99"] for r in rows])
pop_p99 = np.array([r["pop_p99"] for r in rows])
backends= np.array([r["ba"] for r in rows])
cpu     = np.array([cpu_cores(r["n"]) for r in rows])

print("clients", clients)
print("push k/s", push); print("pop k/s", pop)
print("pop p99 ms", pop_p99); print("backends", backends); print("cpu cores", cpu)

# ---- style ----
BG = "#1d1d1f"; ICE = "#22d3ee"; EMBER = "#fb7185"; OK = "#4ade80"; WARN = "#e6b450"
DARK = {
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG,
    "text.color": "#9a9a9a", "axes.labelcolor": "#9a9a9a", "axes.titlecolor": "#e6e6e6",
    "xtick.color": "#6a6a6a", "ytick.color": "#6a6a6a",
    "axes.edgecolor": "#3a3b42", "grid.color": "#3a3b42", "font.size": 12,
}

with mpl.rc_context(DARK):
    fig, axs = plt.subplots(2, 2, figsize=(14, 9.6))
    fig.patch.set_facecolor(BG)
    (a_thr, a_lat), (a_be, a_cpu) = axs
    for ax in axs.flat:
        ax.set_facecolor(BG)
        for s in ("top", "right"): ax.spines[s].set_visible(False)
        for s in ("left", "bottom"): ax.spines[s].set_color("#3a3b42")
        ax.set_xscale("log"); ax.set_xticks(clients); ax.set_xticklabels([str(c) for c in clients])
        ax.set_xlabel("clients per role (direct PG backends = 2x)")
        ax.grid(True, which="major", color="#3a3b42", linewidth=0.8, alpha=0.85)
        ax.set_axisbelow(True); ax.tick_params(length=0)

    def t(ax, s): ax.set_title(s, color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")

    # 1. throughput
    a_thr.plot(clients, push, color=ICE, lw=2.2, marker="o", ms=5, label="push")
    a_thr.plot(clients, pop, color=EMBER, lw=2.6, marker="o", ms=6, label="pop (bottleneck)")
    a_thr.set_ylim(0, None); a_thr.set_ylabel("throughput (k msg/s)")
    a_thr.legend(framealpha=0, labelcolor="#e6e6e6", fontsize=10)
    a_thr.annotate(f"peak pop {pop.max():.0f}k @ {clients[np.argmax(pop)]}",
                   xy=(clients[np.argmax(pop)], pop.max()), xytext=(clients[np.argmax(pop)]*1.1, pop.max()+6),
                   color=EMBER, fontsize=9)
    t(a_thr, "Throughput vs clients — pop collapses as clients pile on")

    # 2. p99 latency (log)
    a_lat.plot(clients, pop_p99, color=EMBER, lw=2.6, marker="o", ms=6, label="pop p99")
    a_lat.plot(clients, push_p99, color=ICE, lw=2.2, marker="o", ms=5, label="push p99")
    a_lat.set_yscale("log"); a_lat.set_ylabel("p99 latency (ms)")
    a_lat.legend(framealpha=0, labelcolor="#e6e6e6", fontsize=10)
    t(a_lat, "Tail latency vs clients — pop p99 0.1s -> 15s")

    # 3. active backends
    a_be.plot(clients, backends, color=WARN, lw=2.4, marker="o", ms=5)
    a_be.set_ylim(0, None); a_be.set_ylabel("avg active PG backends")
    t(a_be, "Postgres backends — every client contends on one table")

    # 4. PG CPU
    a_cpu.plot(clients, cpu, color=ICE, lw=2.4, marker="o", ms=5)
    a_cpu.axhline(32, color="#6a6a6a", ls="--", lw=1, alpha=0.7)
    a_cpu.text(clients[0], 32.5, "32 cores", color="#6a6a6a", fontsize=9)
    a_cpu.set_ylim(0, 36); a_cpu.set_ylabel("PG CPU (cores of 32)")
    t(a_cpu, "Postgres CPU")

    fig.suptitle("pgmq — single unordered queue, direct connections", color="#ffffff",
                 fontsize=22, fontweight="bold", x=0.5, y=0.978)
    fig.text(0.5, 0.918,
             "PG17 · pgmq 1.11.1 · 32 vCPU / 62 GiB · synchronous_commit=on · send_batch=10 / read=100 · 60s/step",
             ha="center", color="#b6bcc8", fontsize=11.5)
    fig.text(0.5, 0.886,
             "throughput peaks at the LOWEST concurrency and degrades — the single queue table is a contention point "
             "(read = FOR UPDATE SKIP LOCKED + UPDATE vt + DELETE)",
             ha="center", color=EMBER, fontsize=11)
    fig.add_artist(Line2D([0.05, 0.95], [0.862, 0.862], color="#33343b", lw=1.0, transform=fig.transFigure))
    fig.text(0.5, 0.012, "benchmark-queen/pgmq/sweep-206 · direct-connection client sweep",
             ha="center", color="#6a6a6a", fontsize=9)
    fig.tight_layout(rect=[0, 0.02, 1, 0.845])
    out = os.path.join(BASE, "pgmq-sweep-206.png")
    fig.savefig(out, dpi=150, facecolor=BG)
    print("WROTE", out)
