#!/usr/bin/env python3
"""Full pgmq ordered-FIFO comparison: read_grouped_head vs read_grouped_rr, 300 vs 1000 lanes, vs Queen."""
import os, re
import matplotlib; matplotlib.use("Agg")
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
def parse(log):
    txt = open(os.path.join(BASE, log)).read()
    blk = re.split(r'STEP: (\d+) clients/role', txt); rows = []
    for i in range(1, len(blk), 2):
        n = int(blk[i]); b = blk[i+1]
        m = re.search(r'CONSUMER:\s*msg/s=(\d+)', b)
        if m: rows.append((n, float(m.group(1))/1e3))
    rows.sort(); return np.array([r[0] for r in rows]), np.array([r[1] for r in rows])

h3 = parse("sweep-fifo-g300.log")      # head, 300
h1 = parse("sweep-fifo.log")           # head, 1000
r3 = parse("sweep-fifo-rr-g300.log")   # rr, 300
r1 = parse("sweep-fifo-rr-g1000.log")  # rr, 1000

BG="#1d1d1f"; OK="#4ade80"; ICE="#22d3ee"; WARN="#e6b450"; EMBER="#fb7185"; GREY="#7a7d87"
DARK={"figure.facecolor":BG,"axes.facecolor":BG,"savefig.facecolor":BG,"text.color":"#9a9a9a",
      "axes.labelcolor":"#9a9a9a","axes.titlecolor":"#e6e6e6","xtick.color":"#9a9a9a","ytick.color":"#6a6a6a",
      "axes.edgecolor":"#3a3b42","grid.color":"#3a3b42","font.size":12}

with mpl.rc_context(DARK):
    fig,(a1,a2)=plt.subplots(1,2,figsize=(14,6.3)); fig.patch.set_facecolor(BG)
    for ax in (a1,a2):
        ax.set_facecolor(BG)
        for s in ("top","right"): ax.spines[s].set_visible(False)
        for s in ("left","bottom"): ax.spines[s].set_color("#3a3b42")
        ax.grid(True,color="#3a3b42",lw=0.8,alpha=0.85); ax.set_axisbelow(True); ax.tick_params(length=0)

    a1.set_xscale("log"); a1.set_xticks(r1[0]); a1.set_xticklabels([str(int(x)) for x in r1[0]])
    a1.plot(r1[0],r1[1],color=OK,lw=2.6,marker="o",ms=6,label="read_grouped_rr · 1000 lanes")
    a1.plot(r3[0],r3[1],color=ICE,lw=2.4,marker="o",ms=5,label="read_grouped_rr · 300 lanes")
    a1.plot(h1[0],h1[1],color=WARN,lw=2.0,ls=(0,(6,4)),marker="s",ms=4,label="read_grouped_head · 1000")
    a1.plot(h3[0],h3[1],color=EMBER,lw=2.0,ls=(0,(6,4)),marker="s",ms=4,label="read_grouped_head · 300")
    a1.set_yscale("log"); a1.set_xlabel("clients per role"); a1.set_ylabel("ordered pop (k msg/s)")
    a1.legend(framealpha=0,labelcolor="#e6e6e6",fontsize=9)
    a1.set_title("pgmq ordered pop — fn & lane-count", color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")

    labels=["Queen\nORDERED\n300 part","pgmq rr\n1000 lanes\n(best)","pgmq head\n1000","pgmq rr\n300 (matched)","pgmq head\n300"]
    vals=[119, float(r1[1].max()), float(h1[1].max()), float(r3[1].max()), float(h3[1].max())]
    cols=[OK, ICE, "#4a4d57", ICE, "#4a4d57"]
    bars=a2.bar(range(5),vals,color=cols,width=0.66,edgecolor="#11121a")
    for i,v in enumerate(vals): a2.text(i,v+2,f"{v:.1f}k" if v<10 else f"{v:.0f}k",ha="center",color="#e6e6e6",fontsize=10.5,fontweight="bold")
    a2.set_xticks(range(5)); a2.set_xticklabels(labels,fontsize=8.5)
    a2.set_ylim(0,138); a2.set_ylabel("peak ordered pop (k msg/s)")
    a2.set_title("Ordered throughput — Queen vs pgmq's best", color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")
    a2.annotate(f"~{119/vals[1]:.0f}\u00d7 vs pgmq best", xy=(1,vals[1]+3), xytext=(1.4,72), color=ICE, fontsize=11, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=ICE, lw=1.4))
    a2.annotate(f"~{119/vals[3]:.0f}\u00d7 at matched 300", xy=(3,vals[3]+3), xytext=(2.5,45), color=EMBER, fontsize=10.5, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=EMBER, lw=1.4))

    fig.suptitle("pgmq ordered FIFO — its best path (read_grouped_rr) vs Queen", color="#ffffff", fontsize=20, fontweight="bold", x=0.5, y=0.975)
    fig.text(0.5,0.908,"PG17 · pgmq 1.11.1 · 32 vCPU / 62 GiB · direct · send10/read100 · Queen soak = 300 partitions, 119k ordered",
             ha="center",color="#b6bcc8",fontsize=11)
    fig.text(0.5,0.872,"read_grouped_rr (disjoint groups via advisory lock) ~3\u00d7 over read_grouped_head; pgmq best ordered ~13k @1000 lanes, ~3k @ matched 300 \u2014 Queen 119k is ~9\u201338\u00d7 faster.",
             ha="center",color=ICE,fontsize=10.2)
    fig.add_artist(Line2D([0.05,0.95],[0.845,0.845],color="#33343b",lw=1.0,transform=fig.transFigure))
    fig.text(0.5,0.015,"benchmark-queen/pgmq/sweep-206 · fifo head/rr × 300/1000 lanes",ha="center",color="#6a6a6a",fontsize=9)
    fig.tight_layout(rect=[0,0.03,1,0.83])
    out=os.path.join(BASE,"pgmq-fifo-full-206.png"); fig.savefig(out,dpi=150,facecolor=BG)
    print("peaks: head300=%.1f head1000=%.1f rr300=%.1f rr1000=%.1f"%(h3[1].max(),h1[1].max(),r3[1].max(),r1[1].max()))
    print("WROTE",out)
