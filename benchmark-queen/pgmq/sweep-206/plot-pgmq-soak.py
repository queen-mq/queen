#!/usr/bin/env python3
"""pgmq 45-min sustained soak (pooled, single queue) time-series chart.
Throughput derived from PG cumulative n_tup_ins (push) / n_tup_del (pop)."""
import os, csv, re
import matplotlib; matplotlib.use("Agg")
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
M = os.path.join(BASE, "results", "plain-pooled-soak45", "metrics.csv")
DS = os.path.join(BASE, "results", "plain-pooled-soak45.dockerstats.csv")

rows = [r for r in csv.reader(open(M)) if len(r) == 10 and r[0].isdigit()]
ts   = np.array([int(r[0]) for r in rows], float)
act  = np.array([int(r[1]) for r in rows], float)
live = np.array([int(r[3]) for r in rows], float)
dead = np.array([int(r[4]) for r in rows], float)
ins  = np.array([int(r[5]) for r in rows], float)
dele = np.array([int(r[7]) for r in rows], float)
t0 = ts[0]; mins = (ts - t0) / 60.0

def rate(cum, tsec, w=10):
    d = np.diff(cum); dt = np.diff(tsec)
    r = np.where(dt > 0, d / dt, np.nan)
    # rolling mean
    if len(r) >= w:
        k = np.ones(w) / w
        r = np.convolve(np.nan_to_num(r), k, mode="same")
    return (tsec[1:] - t0) / 60.0, r
mp, push = rate(ins, ts); _, pop = rate(dele, ts)

# docker stats CPU
cpu_t, cpu_c = [], []
if os.path.exists(DS):
    for ln in open(DS):
        p = ln.strip().split(",")
        if len(p) >= 2 and p[0].isdigit():
            m = re.search(r"([\d.]+)%", p[1])
            if m:
                cpu_t.append((int(p[0]) - t0) / 60.0); cpu_c.append(float(m.group(1)) / 100.0)
cpu_t = np.array(cpu_t); cpu_c = np.array(cpu_c)

push_avg = np.nanmean(push[mp > 2]); pop_avg = np.nanmean(pop[mp > 2])
print("push avg k/s", push_avg/1e3, "pop avg k/s", pop_avg/1e3, "dur min", mins[-1])

BG="#1d1d1f"; ICE="#22d3ee"; OK="#4ade80"; EMBER="#fb7185"; WARN="#e6b450"
DARK={"figure.facecolor":BG,"axes.facecolor":BG,"savefig.facecolor":BG,"text.color":"#9a9a9a",
      "axes.labelcolor":"#9a9a9a","axes.titlecolor":"#e6e6e6","xtick.color":"#6a6a6a","ytick.color":"#6a6a6a",
      "axes.edgecolor":"#3a3b42","grid.color":"#3a3b42","font.size":12}

with mpl.rc_context(DARK):
    fig, axs = plt.subplots(2, 2, figsize=(14, 9.6)); fig.patch.set_facecolor(BG)
    (a_thr, a_tup), (a_be, a_cpu) = axs
    for ax in axs.flat:
        ax.set_facecolor(BG)
        for s in ("top","right"): ax.spines[s].set_visible(False)
        for s in ("left","bottom"): ax.spines[s].set_color("#3a3b42")
        ax.set_xlim(0, mins[-1]); ax.set_xlabel("elapsed (minutes)")
        ax.grid(True, color="#3a3b42", lw=0.8, alpha=0.85); ax.set_axisbelow(True); ax.tick_params(length=0)
    def t(ax,s): ax.set_title(s, color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")

    a_thr.plot(mp, push/1e3, color=ICE, lw=1.6, label="push")
    a_thr.plot(mp, pop/1e3, color=OK, lw=1.6, ls=(0,(6,4)), label="pop")
    a_thr.set_ylim(0, max(70, np.nanmax(push/1e3)*1.2)); a_thr.set_ylabel("throughput (k msg/s)")
    a_thr.legend(framealpha=0, labelcolor="#e6e6e6", fontsize=10, loc="lower right")
    t(a_thr, "Throughput — balanced push/pop, held for 45 min")

    a_tup.plot(mins, dead/1e6, color=WARN, lw=1.5, label="dead tuples")
    a_tup.plot(mins, live/1e6, color=OK, lw=1.5, label="live (queue depth)")
    a_tup.set_ylim(0, None); a_tup.set_ylabel("tuples (millions)")
    a_tup.legend(framealpha=0, labelcolor="#e6e6e6", fontsize=10, loc="upper right")
    t(a_tup, "Queue table — drained (live ~0), autovacuum bounds dead tuples")

    a_be.plot(mins, act, color=EMBER, lw=1.5)
    a_be.axhline(64, color="#6a6a6a", ls="--", lw=1, alpha=0.7); a_be.text(0.5, 66, "pool=64", color="#6a6a6a", fontsize=9)
    a_be.set_ylim(0, 90); a_be.set_ylabel("active PG backends")
    t(a_be, "PG backends — capped by PgBouncer pool")

    if len(cpu_c):
        a_cpu.plot(cpu_t, cpu_c, color=ICE, lw=1.5)
    a_cpu.axhline(32, color="#6a6a6a", ls="--", lw=1, alpha=0.7); a_cpu.text(0.5, 32.6, "32 cores", color="#6a6a6a", fontsize=9)
    a_cpu.set_ylim(0, 36); a_cpu.set_ylabel("PG CPU (cores of 32)")
    t(a_cpu, "Postgres CPU")

    fig.suptitle("pgmq — 45-minute sustained soak (pooled, single queue)", color="#ffffff",
                 fontsize=21, fontweight="bold", x=0.5, y=0.978)
    fig.text(0.5, 0.918, f"PG17 · pgmq 1.11.1 · 32 vCPU / 62 GiB · MODE=plain send10/read100 · PgBouncer pool=64 · 200 clients/role",
             ha="center", color="#b6bcc8", fontsize=11.5)
    fig.text(0.5, 0.886, f"~{push_avg/1e3:.0f}k push & ~{pop_avg/1e3:.0f}k pop held balanced for 45 min · 0 errors · queue drained (~150 MB) · "
             f"autovacuum kept dead tuples bounded",
             ha="center", color=OK, fontsize=10.5)
    fig.add_artist(Line2D([0.05,0.95],[0.862,0.862], color="#33343b", lw=1.0, transform=fig.transFigure))
    fig.text(0.5, 0.012, "benchmark-queen/pgmq/sweep-206 · plain-pooled-soak45", ha="center", color="#6a6a6a", fontsize=9)
    fig.tight_layout(rect=[0,0.02,1,0.845])
    out=os.path.join(BASE,"pgmq-soak45-pooled-206.png"); fig.savefig(out, dpi=150, facecolor=BG)
    print("WROTE", out)
