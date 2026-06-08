#!/usr/bin/env python3
"""pgmq ordering tax: per-group FIFO (read_grouped_head) vs unordered (plain), single queue, direct.
Plus a semantics comparison vs Queen's ordered 119k."""
import os, re
import matplotlib; matplotlib.use("Agg")
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))

def parse(log):
    txt = open(os.path.join(BASE, log)).read()
    blocks = re.split(r'STEP: (\d+) clients/role', txt)
    rows = []
    for i in range(1, len(blocks), 2):
        n = int(blocks[i]); b = blocks[i + 1]
        def g(p):
            m = re.search(p, b); return float(m.group(1)) if m else np.nan
        rows.append(dict(n=n, push=g(r'PRODUCER:\s*msg/s=(\d+)'),
                         pop=g(r'CONSUMER:\s*msg/s=(\d+)'), pop99=g(r'CONSUMER:.*?p99=([\d.]+)ms')))
    rows = [r for r in rows if not np.isnan(r["pop"])]; rows.sort(key=lambda r: r["n"])
    return rows

P = parse("sweep.log")        # plain / unordered, single queue, direct
F = parse("sweep-fifo.log")   # FIFO / ordered (read_grouped_head, 1000 groups), direct
pn = np.array([r["n"] for r in P]); pp = np.array([r["pop"] for r in P]) / 1e3
fn = np.array([r["n"] for r in F]); fp = np.array([r["pop"] for r in F]) / 1e3
print("plain pop k/s", pp); print("fifo pop k/s", fp)

BG="#1d1d1f"; ICE="#22d3ee"; OK="#4ade80"; EMBER="#fb7185"; GREY="#7a7d87"
DARK={"figure.facecolor":BG,"axes.facecolor":BG,"savefig.facecolor":BG,"text.color":"#9a9a9a",
      "axes.labelcolor":"#9a9a9a","axes.titlecolor":"#e6e6e6","xtick.color":"#9a9a9a","ytick.color":"#6a6a6a",
      "axes.edgecolor":"#3a3b42","grid.color":"#3a3b42","font.size":12}

with mpl.rc_context(DARK):
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 6.2)); fig.patch.set_facecolor(BG)
    for ax in (a1, a2):
        ax.set_facecolor(BG)
        for s in ("top","right"): ax.spines[s].set_visible(False)
        for s in ("left","bottom"): ax.spines[s].set_color("#3a3b42")
        ax.grid(True, color="#3a3b42", lw=0.8, alpha=0.85); ax.set_axisbelow(True); ax.tick_params(length=0)

    # panel 1: pop vs clients, plain vs fifo (log y)
    a1.set_xscale("log"); a1.set_xticks(pn); a1.set_xticklabels([str(int(x)) for x in pn])
    a1.plot(pn, pp, color=ICE, lw=2.6, marker="o", ms=6, label="unordered (plain)")
    a1.plot(fn, fp, color=EMBER, lw=2.6, marker="o", ms=6, label="ordered (per-group FIFO)")
    a1.set_yscale("log"); a1.set_xlabel("clients per role"); a1.set_ylabel("pop throughput (k msg/s)")
    a1.legend(framealpha=0, labelcolor="#e6e6e6", fontsize=10)
    a1.set_title("pgmq pop: ordering tax (single queue, direct)", color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")

    # panel 2: peak sustainable pop by semantics & system
    labels = ["Queen\nordered\n(300 part)", "pgmq\nUNordered\n(32 shards)", "pgmq\nUNordered\n(1 queue)", "pgmq\nORDERED\n(FIFO 1k grp)"]
    vals   = [119, 125, 60, 4.4]
    cols   = [OK, GREY, GREY, EMBER]
    bars = a2.bar(range(4), vals, color=cols, width=0.62, edgecolor="#11121a")
    for i,(v,b) in enumerate(zip(vals,bars)):
        a2.text(i, v+2.5, f"{v:g}k", ha="center", color="#e6e6e6", fontsize=11, fontweight="bold")
    a2.set_xticks(range(4)); a2.set_xticklabels(labels, fontsize=9.5)
    a2.set_ylim(0, 140); a2.set_ylabel("peak balanced/pop throughput (k msg/s)")
    a2.set_title("Same hardware — ordered vs unordered throughput", color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")
    a2.legend(handles=[Line2D([0],[0],color=OK,lw=8,label="ordered (per-key FIFO)"),
                       Line2D([0],[0],color=GREY,lw=8,label="unordered (competing consumers)"),
                       Line2D([0],[0],color=EMBER,lw=8,label="ordered (pgmq)")],
              framealpha=0, labelcolor="#e6e6e6", fontsize=9, loc="upper right")

    fig.suptitle("The ordering tax — pgmq per-group FIFO vs unordered, vs Queen", color="#ffffff",
                 fontsize=20, fontweight="bold", x=0.5, y=0.975)
    fig.text(0.5, 0.908, "PG17 · pgmq 1.11.1 · 32 vCPU / 62 GiB · direct · send10/read100 · FIFO via read_grouped_head over 1000 groups",
             ha="center", color="#b6bcc8", fontsize=11)
    fig.text(0.5, 0.872, "enforcing per-group FIFO drops pgmq pop ~14x (60k \u2192 4.4k) and it collapses under load; Queen delivers the SAME ordering at 119k \u2014 ~27x faster",
             ha="center", color=EMBER, fontsize=10.5)
    fig.add_artist(Line2D([0.05,0.95],[0.845,0.845], color="#33343b", lw=1.0, transform=fig.transFigure))
    fig.text(0.5, 0.015, "benchmark-queen/pgmq/sweep-206 · fifo-direct vs plain-direct", ha="center", color="#6a6a6a", fontsize=9)
    fig.tight_layout(rect=[0,0.03,1,0.83])
    out=os.path.join(BASE,"pgmq-fifo-vs-unordered-206.png"); fig.savefig(out, dpi=150, facecolor=BG)
    print("WROTE", out)
