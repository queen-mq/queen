#!/usr/bin/env python3
"""pgmq ordered FIFO at matched cardinality (300 lanes = Queen's 300 partitions),
plus 1000-group and unordered, vs Queen's 119k ordered."""
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
        def g(p):
            m = re.search(p, b); return float(m.group(1)) if m else np.nan
        rows.append((n, g(r'CONSUMER:\s*msg/s=(\d+)')))
    rows = [r for r in rows if not np.isnan(r[1])]; rows.sort()
    return np.array([r[0] for r in rows]), np.array([r[1] for r in rows])/1e3

pn, pp = parse("sweep.log")            # unordered, 1 queue
f1n, f1p = parse("sweep-fifo.log")     # ordered, 1000 groups
f3n, f3p = parse("sweep-fifo-g300.log")# ordered, 300 groups
print("plain", pp); print("fifo1000", f1p); print("fifo300", f3p)

BG="#1d1d1f"; ICE="#22d3ee"; OK="#4ade80"; EMBER="#fb7185"; WARN="#e6b450"; GREY="#7a7d87"
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

    a1.set_xscale("log"); a1.set_xticks(pn); a1.set_xticklabels([str(int(x)) for x in pn])
    a1.plot(pn,pp,color=ICE,lw=2.4,marker="o",ms=5,label="unordered (1 queue)")
    a1.plot(f1n,f1p,color=WARN,lw=2.4,marker="o",ms=5,label="ordered FIFO, 1000 lanes")
    a1.plot(f3n,f3p,color=EMBER,lw=2.6,marker="o",ms=6,label="ordered FIFO, 300 lanes")
    a1.set_yscale("log"); a1.set_xlabel("clients per role"); a1.set_ylabel("pop throughput (k msg/s)")
    a1.legend(framealpha=0,labelcolor="#e6e6e6",fontsize=9.5)
    a1.set_title("pgmq pop — ordering tax & lane-count effect", color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")

    labels=["Queen\nORDERED\n300 partitions","pgmq\nunordered\n1 queue","pgmq\nORDERED\n1000 lanes","pgmq\nORDERED\n300 lanes"]
    vals=[119, float(np.nanmax(pp)), float(np.nanmax(f1p)), float(np.nanmax(f3p))]
    cols=[OK, GREY, WARN, EMBER]
    bars=a2.bar(range(4),vals,color=cols,width=0.62,edgecolor="#11121a")
    for i,v in enumerate(vals): a2.text(i,v+2.5,f"{v:.1f}k" if v<10 else f"{v:.0f}k",ha="center",color="#e6e6e6",fontsize=11,fontweight="bold")
    a2.set_xticks(range(4)); a2.set_xticklabels(labels,fontsize=9)
    a2.set_ylim(0,140); a2.set_ylabel("peak pop throughput (k msg/s)")
    a2.set_title("Matched at 300 lanes: Queen vs pgmq ordered", color="#e6e6e6", fontsize=13, fontweight="bold", loc="left")
    a2.annotate(f"~{119/vals[3]:.0f}\u00d7", xy=(3,vals[3]+4), xytext=(2.55,70), color=EMBER, fontsize=15, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=EMBER, lw=1.5))
    a2.annotate("", xy=(0,119), xytext=(0,8), arrowprops=dict(arrowstyle="-", color="#3a3b42", lw=0))

    fig.suptitle("Ordered throughput at matched cardinality — Queen vs pgmq", color="#ffffff", fontsize=20, fontweight="bold", x=0.5, y=0.975)
    fig.text(0.5,0.908,"PG17 · pgmq 1.11.1 · 32 vCPU / 62 GiB · direct · send10/read100 · FIFO via read_grouped_head · Queen soak = 300 partitions",
             ha="center",color="#b6bcc8",fontsize=11)
    fig.text(0.5,0.872,"at the SAME 300 ordered lanes, pgmq peaks ~1.5k pop and collapses; Queen sustains 119k \u2014 ~80\u00d7. pgmq FIFO rises with lanes (300\u21921.5k, 1000\u21924.4k) but stays orders below Queen.",
             ha="center",color=EMBER,fontsize=10.3)
    fig.add_artist(Line2D([0.05,0.95],[0.845,0.845],color="#33343b",lw=1.0,transform=fig.transFigure))
    fig.text(0.5,0.015,"benchmark-queen/pgmq/sweep-206 · fifo-g300 / fifo (1000) / plain",ha="center",color="#6a6a6a",fontsize=9)
    fig.tight_layout(rect=[0,0.03,1,0.83])
    out=os.path.join(BASE,"pgmq-fifo-matched-206.png"); fig.savefig(out,dpi=150,facecolor=BG); print("WROTE",out)
