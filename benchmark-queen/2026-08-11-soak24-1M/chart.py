#!/usr/bin/env python3
"""Four panels over the 24-hour axis: throughput, CPU, memory, latency.

Reuses the site's chart house style (webdoc/scripts/charts.py) so this figure
sits next to the published ones instead of inventing a second look.

    python3 benchmark-queen/2026-08-11-soak24-1M/chart.py

Writes soak-24h-1M-{light,dark}.png next to this file.

Sources, all in raw/:
  throughput  bench/metrics.csv    broker counters every 10 s (cumulative, so
                                   the rate is a difference over the real gap)
  cpu, memory bench/bench.csv      1 Hz host sampler
  latency     loader-0*/g.out      each loader's own percentiles every 30 s,
                                   averaged across the three
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "webdoc" / "scripts"))

import matplotlib.pyplot as plt  # noqa: E402
from charts import DARK, LIGHT, Theme, decimate, finish, read_csv, style, thousands  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

RAW = HERE / "raw"


def throughput(target: int = 700):
    """(hours, push/s, pop/s) from the broker's own cumulative counters."""
    rows = [r for r in read_csv(RAW / "bench" / "metrics.csv") if r.get("b_push_items")]
    rows = decimate(rows, target)
    t0 = int(rows[0]["epoch_s"])
    hours, push, pop = [], [], []
    for a, b in zip(rows, rows[1:]):
        dt = int(b["epoch_s"]) - int(a["epoch_s"])
        if dt <= 0:
            continue
        hours.append((int(b["epoch_s"]) - t0) / 3600)
        push.append((int(b["b_push_items"]) - int(a["b_push_items"])) / dt)
        pop.append((int(b["b_pop_items"]) - int(a["b_pop_items"])) / dt)
    return hours, push, pop


def resources(target: int = 900):
    """(hours, broker cores, pg cores, broker RSS GB, db GB) from the 1 Hz sampler."""
    rows = [r for r in read_csv(RAW / "bench" / "bench.csv") if r.get("queen_mem_mb")]
    # The sampler outlives the load by a few seconds; those samples are the
    # teardown, not the run, and they drag every line to zero at the right edge.
    rows = rows[:-15]
    rows = decimate(rows, target)
    t0 = int(rows[0]["epoch_ms"])
    hours = [(int(r["epoch_ms"]) - t0) / 3_600_000 for r in rows]
    qcpu = [float(r["queen_cpu_pct"]) / 100 for r in rows]
    pcpu = [float(r["pg_cpu_pct"]) / 100 for r in rows]
    qmem = [float(r["queen_mem_mb"]) / 1024 for r in rows]
    db = [float(r["db_size_bytes"]) / 1073741824 for r in rows]
    return hours, qcpu, pcpu, qmem, db


LAT = re.compile(r"p50=\s*([\d.]+) p99=\s*([\d.]+) p999=\s*([\d.]+)")


def latency(target: int = 700):
    """(hours, p50, p99, p999) averaged across the three loaders.

    The report lines carry a wall clock without a date, so elapsed time comes
    from the line index instead: the loader reports on a fixed 30 s cadence.
    """
    series = []
    for d in sorted(RAW.glob("loader-0*")):
        vals = [
            tuple(float(x) for x in m.groups())
            for m in (LAT.search(line) for line in (d / "g.out").read_text().splitlines())
            if m
        ]
        series.append(vals)
    n = min(len(s) for s in series)
    step = max(1, n // target)
    hours, p50, p99, p999 = [], [], [], []
    for i in range(0, n, step):
        hours.append(i * 30 / 3600)
        p50.append(sum(s[i][0] for s in series) / len(series))
        p99.append(sum(s[i][1] for s in series) / len(series))
        p999.append(sum(s[i][2] for s in series) / len(series))
    return hours, p50, p99, p999


def build(theme: Theme) -> Path:
    th, push, pop = throughput()
    rh, qcpu, pcpu, qmem, db = resources()
    lh, p50, p99, p999 = latency()

    style(theme)
    fig, axes = plt.subplots(
        4, 1, figsize=(7.6, 9.4), sharex=True, gridspec_kw={"hspace": 0.34}
    )
    ax1, ax2, ax3, ax4 = axes
    c = theme.series

    # The two series coincide for the whole run, which is the point of the
    # panel; pop goes on top dashed so the reader can see there are two.
    ax1.plot(th, push, color=c[0], linewidth=1.6, label="push")
    ax1.plot(th, pop, color=c[1], linewidth=0.9, linestyle=(0, (4, 3)), label="pop")
    finish(ax1, theme, "Throughput (messages/s)")
    ax1.set_ylim(0, 1_250_000)
    ax1.yaxis.set_major_formatter(FuncFormatter(thousands))
    ax1.legend(loc="lower right", frameon=False, fontsize=8, labelcolor=theme.ink, ncol=2)

    ax2.plot(rh, qcpu, color=c[0], linewidth=0.9, label="broker")
    ax2.plot(rh, pcpu, color=c[1], linewidth=0.9, label="PostgreSQL")
    finish(ax2, theme, "CPU (cores of 32)")
    ax2.set_ylim(0, 32)
    ax2.legend(loc="upper right", frameon=False, fontsize=8, labelcolor=theme.ink, ncol=2)

    ax3.plot(rh, qmem, color=c[0], linewidth=1.0, label="broker resident memory")
    ax3.plot(rh, db, color=c[2 % len(c)], linewidth=1.0, label="database size")
    finish(ax3, theme, "Memory and database (GB)")
    ax3.set_ylim(0, max(max(db), max(qmem)) * 1.3)
    ax3.legend(loc="center right", frameon=False, fontsize=8, labelcolor=theme.ink)

    ax4.plot(lh, p999, color=c[2 % len(c)], linewidth=0.8, label="p99.9")
    ax4.plot(lh, p99, color=c[1], linewidth=0.8, label="p99")
    ax4.plot(lh, p50, color=c[0], linewidth=1.0, label="p50")
    finish(ax4, theme, "End-to-end latency (ms)")
    ax4.set_ylim(0, 1000)
    ax4.legend(loc="upper right", frameon=False, fontsize=8, labelcolor=theme.ink, ncol=3)

    ax4.set_xlabel("Hours into the run", color=theme.ink, fontsize=8.5)
    ax4.set_xlim(0, 24)
    ax4.set_xticks(range(0, 25, 3))

    out = HERE / f"soak-24h-1M-{theme.name}.png"
    fig.savefig(out, dpi=200, facecolor=theme.surface, bbox_inches="tight")
    plt.close(fig)
    return out


if __name__ == "__main__":
    for t in (LIGHT, DARK):
        print(build(t))
