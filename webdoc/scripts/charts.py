#!/usr/bin/env python3
"""Render the benchmark figures from the archived artifacts.

Every figure on this site is generated from a file under `benchmark-queen/`. A
chart is a claim, and claims here are derived, not drawn: if the artifact
changes, `pnpm --dir webdoc gen` regenerates the figure, and CI fails when what
is committed no longer matches.

Two SVGs are written per figure, `<name>-light.svg` and `<name>-dark.svg`. The
page shows one or the other with CSS, so the chart follows the site's theme
toggle — an <img> cannot inherit `currentColor`, and a single figure tuned for
one surface is unreadable on the other.

Design rules come from the data-visualisation guidance, notably: never a second
y-axis (two measures of different scale become two stacked panels sharing an
x-axis), thin marks, recessive chrome, a legend only when there are two or more
series, direct labels on the last point, and per-mode palettes validated against
this site's actual surfaces (#fcfcfc light, #020202 dark) rather than flipped.

Usage:  python3 charts.py --out <dir>
"""

from __future__ import annotations

import argparse
from pathlib import Path


from matplotlib.ticker import FuncFormatter

from chartlib import plt, BENCH, DARK, LIGHT, Theme, decimate, finish, label_last, parse_progress, read_csv, save, style, thousands
from charts_laravel import (
    fig_laravel_control_plane,
    fig_laravel_event_driven,
    fig_laravel_headline,
    fig_laravel_horizon_latency,
    fig_laravel_horizon_memory,
    fig_laravel_horizon_throughput,
    fig_laravel_prefork,
    fig_laravel_queen_optimizations,
    fig_laravel_replicas,
    fig_laravel_scale_up,
    fig_laravel_soak_memory,
    fig_laravel_vm_capacity,
    fig_laravel_vm_latency,
    fig_laravel_vm_memory,
)



# --------------------------------------------------------------------------
# Figures
# --------------------------------------------------------------------------


def fig_soak24(out: Path, theme: Theme) -> str:
    """24 hours: broker memory and PostgreSQL CPU. Two panels, never two y-axes."""
    rows = read_csv(BENCH / "2026-08-11-soak24-1M" / "raw" / "bench" / "bench.csv")
    rows = [r for r in rows if r.get("queen_mem_mb")]
    rows = decimate(rows, 900)
    t0 = int(rows[0]["epoch_ms"])
    hours = [(int(r["epoch_ms"]) - t0) / 3_600_000 for r in rows]
    mem_gb = [float(r["queen_mem_mb"]) / 1024 for r in rows]
    pg_cpu = [float(r["pg_cpu_pct"]) for r in rows]

    style(theme)
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(7.2, 4.2), sharex=True, gridspec_kw={"hspace": 0.28}
    )

    ax1.plot(hours, mem_gb, color=theme.series[0])
    finish(ax1, theme, "Broker resident memory (GB)")
    ax1.set_ylim(0, max(mem_gb) * 1.35)
    label_last(ax1, hours[-1], mem_gb[-1], f"{mem_gb[-1]:.1f} GB", theme.series[0], theme)

    ax2.plot(hours, pg_cpu, color=theme.series[1])
    finish(ax2, theme, "PostgreSQL CPU (% of one core)")
    ax2.set_xlabel("Hours into the run", color=theme.ink, fontsize=8.5)
    ax2.set_xlim(0, 24)
    ax2.set_xticks(range(0, 25, 4))

    save(fig, out, "soak-24h", theme)
    return "soak-24h"


def fig_pipeline(out: Path, theme: Theme) -> str:
    """T3: the ordered four-stage pipeline — sustained rate and end-to-end p99."""
    rows = parse_progress(
        BENCH / "2026-07-23-3test-report" / "raw" / "t3.out",
        {"events": r"e2e=\s*(\d+)/s", "p99": r"p99=\s*([\d.]+) ms"},
    )
    t = [r["t"] / 60 for r in rows]
    events = [r["events"] for r in rows]
    p99 = [r["p99"] for r in rows]

    style(theme)
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(7.2, 4.2), sharex=True, gridspec_kw={"hspace": 0.28}
    )

    ax1.plot(t, events, color=theme.series[0])
    finish(ax1, theme, "Events per second")
    ax1.yaxis.set_major_formatter(FuncFormatter(thousands))

    ax2.plot(t, p99, color=theme.series[1])
    finish(ax2, theme, "End-to-end p99 (ms)")
    ax2.set_ylim(bottom=0)
    ax2.set_xlabel("Minutes into the run", color=theme.ink, fontsize=8.5)
    ax2.set_xlim(0, max(t))

    save(fig, out, "ordered-pipeline", theme)
    return "ordered-pipeline"


def fig_cell(out: Path, theme: Theme) -> str:
    """The 2-core cell: what the loader offered against what the cell took, and
    the shedding that accounts for the difference.

    `err_429` in the artifact is a cumulative counter, so it is differenced into
    a per-interval rate here — plotted raw it is a straight line that says
    nothing. Only the `load` phase is shown; the trailing `drain` phase is the
    harness winding down, not the system under test.
    """
    rows = read_csv(BENCH / "2026-07-30-1h-soak" / "loader-interval.csv")
    rows = [r for r in rows if r.get("t_sec") and r.get("phase") == "load"]
    t = [float(r["t_sec"]) / 60 for r in rows]
    offered = [float(r["offered_msg_s"]) for r in rows]
    pushed = [float(r["pushed_msg_s"]) for r in rows]

    cum = [float(r["err_429"]) for r in rows]
    secs = [float(r["t_sec"]) for r in rows]
    throttled = [0.0]
    for i in range(1, len(cum)):
        dt = max(secs[i] - secs[i - 1], 1e-9)
        throttled.append(max(cum[i] - cum[i - 1], 0.0) / dt)

    style(theme)
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(7.2, 4.2), sharex=True, gridspec_kw={"hspace": 0.28}
    )

    ax1.plot(t, offered, color=theme.series[1], label="Offered by the loader")
    ax1.plot(t, pushed, color=theme.series[0], label="Accepted by the cell")
    finish(ax1, theme, "Messages per second")
    ax1.set_ylim(0, max(offered) * 1.35)
    ax1.legend(loc="upper right", ncol=2)

    ax2.plot(t, throttled, color=theme.series[2])
    finish(ax2, theme, "Requests answered 429, per second")
    ax2.set_ylim(bottom=0)
    ax2.set_xlabel("Minutes into the run", color=theme.ink, fontsize=8.5)
    ax2.set_xlim(0, max(t))

    save(fig, out, "multitenant-cell", theme)
    return "multitenant-cell"


FIGURES = (
    fig_soak24,
    fig_pipeline,
    fig_cell,
    fig_laravel_control_plane,
    fig_laravel_prefork,
    fig_laravel_replicas,
    fig_laravel_scale_up,
    fig_laravel_event_driven,
    fig_laravel_horizon_throughput,
    fig_laravel_horizon_memory,
    fig_laravel_queen_optimizations,
    fig_laravel_horizon_latency,
    fig_laravel_vm_capacity,
    fig_laravel_vm_memory,
    fig_laravel_vm_latency,
    fig_laravel_headline,
    fig_laravel_soak_memory,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    names = []
    for fn in FIGURES:
        for theme in (LIGHT, DARK):
            names.append(fn(out, theme))
    print(f"{len(set(names))} figures, {len(names)} files")


if __name__ == "__main__":
    main()
