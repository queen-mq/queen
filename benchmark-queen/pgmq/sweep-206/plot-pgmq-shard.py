#!/usr/bin/env python3
"""pgmq sharding scaling: split one queue into N (300 clients, direct)."""
import os, re, csv
import matplotlib; matplotlib.use("Agg")
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
txt = open(os.path.join(BASE, "sweep-shard.log")).read()
rows = []
for m in re.finditer(r'RESULT q=(\d+) push=(\d+) pop=(\d+) push_p99=([\d.]+)ms pop_p99=([\d.]+)ms dead\|live\|size=(\d+)\|(\d+)\|', txt):
    rows.append(dict(q=int(m[1]), push=int(m[2]), pop=int(m[3]),
                     push_p99=float(m[4]), pop_p99=float(m[5]), dead=int(m[6]), live=int(m[7])))
rows.sort(key=lambda r: r["q"])

def cpu_cores(q):
    f = os.path.join(BASE, "results", f"shard-direct-q{q}", "sampler.csv")
    if not os.path.exists(f): return np.nan
    vals = []
    for ln in open(f):
        p = ln.split(",")
        if len(p) >= 2:
            mm = re.search(r"([\d.]+)%", p[1])
            if mm:
                v = float(mm.group(1))
                if v > 200: vals.append(v/100.0)
    return float(np.median(vals)) if vals else np.nan

qs   = np.array([r["q"] for r in rows], float)
push = np.array([r["push"] for r in rows])/1e3
pop  = np.array([r["pop"] for r in rows])/1e3
pop99= np.array([r["pop_p99"] for r in rows])
live = np.array([r["live"] for r in rows])/1e6
cpu  = np.array([cpu_cores(r["q"]) for r in rows])
print("queues", qs); print("pop k/s", pop); print("push k/s", push); print("cpu cores", cpu)

BG="#1d1d1f"; ICE="#22d3ee"; OK="#4ade80"; EMBER="#fb7185"; WARN="#e6b450"
DARK={"figure.facecolor":BG,"axes.facecolor":BG,"savefig.facecolor":BG,"text.color":"#9a9a9a",
      "axes.labelcolor":"#9a9a9a","axes.titlecolor":"#e6e6e6","xtick.color":"#6a6a6a","ytick.color":"#6a6a6a",
      "axes.edgecolor":"#3a3b42","grid.color":"#3a3b42","font.size":12}

with mpl.rc_context(DARK):
    fig, axs = plt.subplots(2, 2, figsize=(14, 9.6)); fig.patch.set_facecolor(BG)
    (a_thr, a_lat), (a_cpu, a_bk) = axs
    for ax in axs.flat:
        ax.set_facecolor(BG)
        for s in ("top","right"): ax.spines[s].set_visible(False)
        for s in ("left","bottom"): ax.spines[s].set_color("#3a3b42")
        ax.set_xscale("log", base=2); ax.set_xticks(qs); ax.set_xticklabels([str(int(x)) for x in qs])
        ax.set_xlabel("number of queues (shards)"); ax.grid(True, color="#3a3b42", lw=0.8, alpha=0.85)
        ax.set_axisbelow(True); ax.tick_params(length=0)
    def t(ax,s): ax.set_title(s, color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")

    a_thr.plot(qs, push, color=ICE, lw=2.2, marker="o", ms=5, label="push")
    a_thr.plot(qs, pop, color=OK, lw=2.6, marker="o", ms=6, label="pop")
    a_thr.set_ylim(0,None); a_thr.set_ylabel("throughput (k msg/s)")
    a_thr.legend(framealpha=0, labelcolor="#e6e6e6", fontsize=10, loc="upper left")
    a_thr.annotate(f"pop {pop[0]:.0f}k\u2192{pop[-1]:.0f}k", xy=(qs[-1],pop[-1]), xytext=(qs[-3],pop[-1]-18), color=OK, fontsize=9)
    t(a_thr, "Throughput vs shards — pop scales ~8x, push plateaus")

    a_lat.plot(qs, pop99, color=EMBER, lw=2.6, marker="o", ms=6)
    a_lat.set_yscale("log"); a_lat.set_ylabel("pop p99 latency (ms)")
    t(a_lat, "Pop p99 — falls as backlog clears (3.5s \u2192 0.5s)")

    a_cpu.plot(qs, cpu, color=ICE, lw=2.4, marker="o", ms=5)
    a_cpu.axhline(32, color="#6a6a6a", ls="--", lw=1, alpha=0.7); a_cpu.text(qs[0], 32.6, "32 cores", color="#6a6a6a", fontsize=9)
    a_cpu.set_ylim(0,36); a_cpu.set_ylabel("PG CPU (cores of 32)")
    t(a_cpu, "Postgres CPU — more useful work as contention spreads")

    a_bk.plot(qs, live, color=WARN, lw=2.4, marker="o", ms=5)
    a_bk.set_ylim(0,None); a_bk.set_ylabel("queue backlog (M live rows, 60s)")
    t(a_bk, "Backlog — pop catches push as shards grow")

    fig.suptitle("pgmq — sharding one queue into N (300 clients, direct)", color="#ffffff",
                 fontsize=21, fontweight="bold", x=0.5, y=0.978)
    fig.text(0.5, 0.918, "PG17 · pgmq 1.11.1 · 32 vCPU / 62 GiB · MODE=plain send10/read100 · 300 clients/role split round-robin · 60s/step",
             ha="center", color="#b6bcc8", fontsize=11.5)
    fig.text(0.5, 0.886, "splitting the single-table contention across N tables lifts pop ~8x (15k\u2192125k); push plateaus ~150k (PG insert/WAL ceiling) \u2014 reaches Queen's ballpark at 32 shards",
             ha="center", color=OK, fontsize=10.5)
    fig.add_artist(Line2D([0.05,0.95],[0.862,0.862], color="#33343b", lw=1.0, transform=fig.transFigure))
    fig.text(0.5, 0.012, "benchmark-queen/pgmq/sweep-206 · shard-direct", ha="center", color="#6a6a6a", fontsize=9)
    fig.tight_layout(rect=[0,0.02,1,0.845])
    out=os.path.join(BASE,"pgmq-shard-206.png"); fig.savefig(out, dpi=150, facecolor=BG)
    print("WROTE", out)
