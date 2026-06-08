#!/usr/bin/env python3
"""Compare pgmq single-queue saturation: DIRECT connections vs PgBouncer POOLING."""
import os, re
import matplotlib; matplotlib.use("Agg")
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))

def parse(logfile):
    txt = open(os.path.join(BASE, logfile)).read()
    blocks = re.split(r'######## STEP: (\d+) clients/role', txt)
    rows = []
    for i in range(1, len(blocks), 2):
        n = int(blocks[i]); b = blocks[i + 1]
        def g(p):
            m = re.search(p, b); return float(m.group(1)) if m else np.nan
        rows.append(dict(n=n,
            push=g(r'PRODUCER:\s*msg/s=(\d+)'), push_p99=g(r'PRODUCER:.*?p99=([\d.]+)ms'),
            pop=g(r'CONSUMER:\s*msg/s=(\d+)'), pop_p99=g(r'CONSUMER:.*?p99=([\d.]+)ms'),
            ba=g(r'avg_active=([\d.]+)'), perr=g(r'PRODUCER:.*?errors=(\d+)'), cerr=g(r'CONSUMER:.*?errors=(\d+)')))
    rows = [r for r in rows if not (np.isnan(r["push"]) and np.isnan(r["pop"]))]
    rows.sort(key=lambda r: r["n"])
    arr = lambda k: np.array([r[k] for r in rows], float)
    return dict(n=arr("n"), push=arr("push")/1e3, pop=arr("pop")/1e3,
                push_p99=arr("push_p99"), pop_p99=arr("pop_p99"), ba=arr("ba"),
                perr=arr("perr"), cerr=arr("cerr"))

D = parse("sweep.log")        # direct
P = parse("sweep-pooled.log") # pooled

BG="#1d1d1f"; ICE="#22d3ee"; EMBER="#fb7185"; WARN="#e6b450"
DARK={"figure.facecolor":BG,"axes.facecolor":BG,"savefig.facecolor":BG,"text.color":"#9a9a9a",
      "axes.labelcolor":"#9a9a9a","axes.titlecolor":"#e6e6e6","xtick.color":"#6a6a6a","ytick.color":"#6a6a6a",
      "axes.edgecolor":"#3a3b42","grid.color":"#3a3b42","font.size":12}
DIR=EMBER; POOL=ICE  # direct=red (collapses), pooled=cyan (holds)

with mpl.rc_context(DARK):
    fig, axs = plt.subplots(2, 2, figsize=(14, 9.6)); fig.patch.set_facecolor(BG)
    (a_pop, a_lat), (a_push, a_be) = axs
    allx = sorted(set(D["n"]).union(P["n"]))
    for ax in axs.flat:
        ax.set_facecolor(BG)
        for s in ("top","right"): ax.spines[s].set_visible(False)
        for s in ("left","bottom"): ax.spines[s].set_color("#3a3b42")
        ax.set_xscale("log"); ax.set_xticks(allx); ax.set_xticklabels([str(int(x)) for x in allx])
        ax.set_xlabel("clients per role"); ax.grid(True, color="#3a3b42", lw=0.8, alpha=0.85)
        ax.set_axisbelow(True); ax.tick_params(length=0)
    def t(ax,s): ax.set_title(s, color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")
    def leg(ax): ax.legend(framealpha=0, labelcolor="#e6e6e6", fontsize=10)

    a_pop.plot(D["n"], D["pop"], color=DIR, lw=2.4, marker="o", ms=5, label="direct")
    a_pop.plot(P["n"], P["pop"], color=POOL, lw=2.6, marker="o", ms=6, label="pooled (PgBouncer 64)")
    a_pop.set_ylim(0,None); a_pop.set_ylabel("pop throughput (k msg/s)"); t(a_pop,"Pop throughput — pooling holds, direct collapses"); leg(a_pop)

    a_lat.plot(D["n"], D["pop_p99"], color=DIR, lw=2.4, marker="o", ms=5, label="direct")
    a_lat.plot(P["n"], P["pop_p99"], color=POOL, lw=2.6, marker="o", ms=6, label="pooled")
    a_lat.set_yscale("log"); a_lat.set_ylabel("pop p99 latency (ms)"); t(a_lat,"Pop p99 — direct 0.1s->15s; pooled <150ms"); leg(a_lat)

    a_push.plot(D["n"], D["push"], color=DIR, lw=2.4, marker="o", ms=5, label="direct")
    a_push.plot(P["n"], P["push"], color=POOL, lw=2.6, marker="o", ms=6, label="pooled")
    a_push.set_ylim(0,None); a_push.set_ylabel("push throughput (k msg/s)"); t(a_push,"Push throughput"); leg(a_push)

    a_be.plot(D["n"], D["ba"], color=DIR, lw=2.4, marker="o", ms=5, label="direct (= clients)")
    a_be.plot(P["n"], P["ba"], color=POOL, lw=2.6, marker="o", ms=6, label="pooled (~64 cap)")
    a_be.set_ylim(0,None); a_be.set_ylabel("avg active PG backends"); t(a_be,"Active PG backends — direct explodes, pooled capped"); leg(a_be)

    # mark pooled error/collapse points
    for i,n in enumerate(P["n"]):
        if (P["perr"][i] if not np.isnan(P["perr"][i]) else 0) > 100 or P["pop"][i] == 0:
            a_pop.annotate("errors", xy=(n, max(P["pop"][i],0.5)), color=WARN, fontsize=8, ha="center")

    fig.suptitle("pgmq single queue — direct connections vs PgBouncer pooling", color="#ffffff",
                 fontsize=21, fontweight="bold", x=0.5, y=0.978)
    fig.text(0.5, 0.918, "PG17 · pgmq 1.11.1 · 32 vCPU / 62 GiB · MODE=plain · send 10 / read 100 · 60s/step",
             ha="center", color="#b6bcc8", fontsize=11.5)
    fig.text(0.5, 0.886, "pooling caps PG backends (~64) & self-balances push/pop \u2192 ~45k held to ~400 clients (<150ms p99);  "
             "direct collapses on contention;  pooled errors out past ~800",
             ha="center", color=ICE, fontsize=10.5)
    fig.add_artist(Line2D([0.05,0.95],[0.862,0.862], color="#33343b", lw=1.0, transform=fig.transFigure))
    fig.text(0.5, 0.012, "benchmark-queen/pgmq/sweep-206", ha="center", color="#6a6a6a", fontsize=9)
    fig.tight_layout(rect=[0,0.02,1,0.845])
    out=os.path.join(BASE,"pgmq-direct-vs-pooled-206.png"); fig.savefig(out, dpi=150, facecolor=BG)
    print("WROTE", out)
    print("direct pop:", D["pop"]); print("pooled pop:", P["pop"])
