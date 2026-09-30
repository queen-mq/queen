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
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
BENCH = REPO / "benchmark-queen"

# What the browser should render. Substituted into the SVG after saving; the
# figures themselves are laid out with DejaVu Sans so the geometry does not
# depend on which fonts the rendering machine happens to have. See style().
WEB_FONT_STACK = "'Inter', 'Helvetica Neue', 'Arial', sans-serif"


# --------------------------------------------------------------------------
# Theme
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Theme:
    name: str
    surface: str
    ink: str          # axis labels, tick labels
    ink_strong: str   # direct labels
    grid: str
    series: tuple[str, ...]


# Categorical slots 1-3 of the reference palette, stepped per mode. Validated
# against this site's surfaces: all checks pass in both modes; the light aqua
# sits below 3:1, which the direct labels and the in-page tables relieve.
LIGHT = Theme(
    name="light",
    surface="#fcfcfc",
    ink="#52514e",
    ink_strong="#181818",
    grid="#e6e6e4",
    series=("#2a78d6", "#eb6834", "#1baf7a"),
)
DARK = Theme(
    name="dark",
    surface="#020202",
    ink="#c3c2b7",
    ink_strong="#f5f5f5",
    grid="#2a2a29",
    series=("#3987e5", "#d95926", "#199e70"),
)


def style(theme: Theme) -> None:
    plt.rcParams.update(
        {
            # Keep text as text: the page's own font renders it, the file stays
            # small, and it can be selected and translated.
            "svg.fonttype": "none",
            "svg.hashsalt": "queenmq",
            "font.family": "sans-serif",
            # DejaVu Sans ships INSIDE matplotlib, so it resolves identically on
            # every machine. That matters because `svg.fonttype: "none"` keeps
            # the text as text but matplotlib still measures each string with
            # whatever font it resolved, and those measurements are written into
            # the file as coordinates. Naming the page's own stack here made the
            # geometry depend on what happened to be installed: macOS resolved
            # 'Helvetica Neue', the Ubuntu runner had none of the three and fell
            # back to DejaVu, and `gen:check` reported permanent drift. The web
            # stack is substituted back into the SVG after saving, so the browser
            # still renders Inter.
            "font.sans-serif": ["DejaVu Sans"],
            "font.size": 9,
            "figure.facecolor": "none",
            "axes.facecolor": "none",
            "savefig.facecolor": "none",
            "savefig.transparent": True,
            "axes.edgecolor": theme.grid,
            "axes.labelcolor": theme.ink,
            "axes.linewidth": 1.0,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "grid.color": theme.grid,
            "grid.linewidth": 1.0,
            "xtick.color": theme.ink,
            "ytick.color": theme.ink,
            "xtick.labelsize": 8.5,
            "ytick.labelsize": 8.5,
            "legend.frameon": False,
            "legend.fontsize": 8.5,
            "legend.labelcolor": theme.ink,
            "lines.linewidth": 1.6,
            "lines.solid_capstyle": "round",
        }
    )


def finish(ax, theme: Theme, ylabel: str) -> None:
    """Recessive chrome: no box, no top/right rules, a y-grid and nothing else."""
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(theme.grid)
    ax.set_ylabel(ylabel, color=theme.ink, fontsize=8.5)
    ax.set_axisbelow(True)
    ax.tick_params(length=0, pad=6)


def label_last(ax, x, y, text: str, color: str, theme: Theme) -> None:
    """A direct label at the series end — identity without relying on colour."""
    ax.annotate(
        text,
        xy=(x, y),
        xytext=(6, 0),
        textcoords="offset points",
        va="center",
        ha="left",
        fontsize=8.5,
        color=theme.ink_strong,
        annotation_clip=False,
    )


def thousands(v, _pos):
    if v >= 1_000_000:
        return f"{v / 1_000_000:g}M"
    if v >= 1_000:
        return f"{v / 1_000:g}k"
    return f"{v:g}"


def save(fig, out: Path, name: str, theme: Theme) -> None:
    # Align the y-labels of stacked panels so the longer one does not push the
    # figure's bounding box past the other and get clipped.
    fig.align_ylabels()
    path = out / f"{name}-{theme.name}.svg"
    fig.savefig(path, format="svg", bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)
    # matplotlib stamps a <metadata> block carrying the render date, which would
    # make every regeneration a diff. Strip it so `gen:check` compares content.
    text = path.read_text()
    text = re.sub(r"<metadata>.*?</metadata>\s*", "", text, flags=re.S)
    text = re.sub(r"<!-- Created with matplotlib.*?-->\s*", "", text, flags=re.S)
    # Clip-path and marker ids hash full-precision coordinates, whose last bits
    # differ between hosts (fonts, CPU) although the drawing does not. Number
    # them in order of appearance so the file depends only on what is drawn.
    ids: dict[str, str] = {}
    text = re.sub(
        r"\b([mp])[0-9a-f]{10}\b",
        lambda m: ids.setdefault(m.group(0), f"{m.group(1)}{len(ids)}"),
        text,
    )
    # Put the page's font stack back. The figure was laid out with DejaVu Sans
    # (see style()) so the geometry is reproducible; the browser should still
    # render Inter. Every string in these figures uses the one family, so
    # rewriting all of them is the whole substitution.
    text = re.sub(r"font-family:[^;\"]*", f"font-family: {WEB_FONT_STACK}", text)
    path.write_text(text)


# --------------------------------------------------------------------------
# Artifact readers
# --------------------------------------------------------------------------


def read_csv(path: Path) -> list[dict]:
    with path.open() as fh:
        return list(csv.DictReader(fh))


def decimate(rows: list, target: int) -> list:
    """Even stride down to ~target points. Keeps the first and last sample."""
    if len(rows) <= target:
        return rows
    step = len(rows) / target
    picked = [rows[int(i * step)] for i in range(target)]
    if picked[-1] is not rows[-1]:
        picked.append(rows[-1])
    return picked


CLOCK = re.compile(r"^\[(\d{2}):(\d{2}):(\d{2})\]")


def parse_progress(path: Path, fields: dict[str, str]) -> list[dict]:
    """Parse goload's per-second stdout lines.

    `fields` maps an output key to the regex capturing its value. Lines without
    a leading clock stamp (banners, the [final] summary) are skipped.
    """
    out: list[dict] = []
    t0 = None
    for line in path.read_text(errors="replace").splitlines():
        m = CLOCK.match(line)
        if not m:
            continue
        h, mi, s = (int(x) for x in m.groups())
        stamp = timedelta(hours=h, minutes=mi, seconds=s)
        if t0 is None:
            t0 = stamp
        elapsed = (stamp - t0).total_seconds()
        if elapsed < 0:  # crossed midnight
            elapsed += 24 * 3600
        row = {"t": elapsed}
        ok = True
        for key, pattern in fields.items():
            f = re.search(pattern, line)
            if not f:
                ok = False
                break
            row[key] = float(f.group(1))
        if ok:
            out.append(row)
    return out


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


LARAVEL = BENCH / "2026-09-30-laravel-supervisor-features" / "raw"


def fig_laravel_control_plane(out: Path, theme: Theme) -> str:
    """Orchestrator memory of Horizon and both Queen engines, read from the
    qualification summary's median table (the row is the claim; the chart only
    draws it)."""
    summary = BENCH / "laravel-supervisors" / "QUALIFICATION_SUMMARY_20260829.md"
    row = next(
        line for line in summary.read_text().splitlines() if line.startswith("| Orchestrator PSS")
    )
    values = [float(cell.split()[0]) for cell in row.strip("|").split("|")[1:]]
    engines = ["Horizon", "Queen PHP", "Queen Rust"]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.1))
    bars = ax.barh(engines[::-1], values[::-1], color=[theme.series[0], theme.series[2], theme.series[1]][::-1], height=0.55)
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Orchestrator PSS (MiB), median of 3 runs", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(values) * 1.18)
    for bar, value in zip(bars, values[::-1]):
        ax.annotate(f"{value:g} MiB", xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
                    xytext=(6, 0), textcoords="offset points", va="center", fontsize=8.5,
                    color=theme.ink_strong)

    save(fig, out, "laravel-control-plane", theme)
    return "laravel-control-plane"


def fig_laravel_prefork(out: Path, theme: Theme) -> str:
    """Worker memory, started one by one against forked from one booted
    Laravel: the median of three runs per case."""
    from statistics import median

    rows = read_csv(LARAVEL / "prefork-memory.csv")
    cases = [("4", "0", "4 idle workers"), ("8", "600", "8 workers, 600 jobs")]
    groups, spawned, forked = [], [], []
    for workers, jobs, label in cases:
        for opcache, flag in (("0", "opcache off"), ("1", "opcache on")):
            pick = lambda mode: median(
                float(r["pss_mib"]) for r in rows
                if r["workers"] == workers and r["jobs"] == jobs and r["opcache"] == opcache and r["mode"] == mode
            )
            groups.append(f"{label}\n{flag}")
            spawned.append(pick("spawned"))
            forked.append(pick("forked"))

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 3.2))
    x = range(len(groups))
    width = 0.36
    left = ax.bar([i - width / 2 for i in x], spawned, width, color=theme.series[1], label="Each worker boots Laravel")
    right = ax.bar([i + width / 2 for i in x], forked, width, color=theme.series[0], label="Forked from one boot (prefork)")
    finish(ax, theme, "Total PSS (MiB)")
    ax.set_xticks(list(x), groups)
    ax.set_ylim(0, max(spawned) * 1.22)
    ax.legend(loc="upper left", ncol=2)
    for bars in (left, right):
        for bar in bars:
            ax.annotate(f"{bar.get_height():.0f}", xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8,
                        color=theme.ink_strong)

    save(fig, out, "laravel-prefork-memory", theme)
    return "laravel-prefork-memory"


def fig_laravel_replicas(out: Path, theme: Theme) -> str:
    """Two supervisor pods on one queue: the workers they run together against
    the target one supervisor would size for the same backlog."""
    rows = read_csv(LARAVEL / "replicas.csv")

    style(theme)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9), sharey=True, gridspec_kw={"wspace": 0.12})
    for ax, (mode, title) in zip(axes, (("uncoordinated", "Without coordination"), ("coordinated", "With coordination"))):
        series = [r for r in rows if r["mode"] == mode and r["target"]]
        t = [int(r["t"]) for r in series]
        total = [int(r["workers_a"]) + int(r["workers_b"]) for r in series]
        target = [int(r["target"]) for r in series]
        ax.step(t, target, where="post", color=theme.ink, linestyle="--", linewidth=1.2, label="Fleet target")
        ax.plot(t, total, color=theme.series[1] if mode == "uncoordinated" else theme.series[0], label="Workers, both pods")
        finish(ax, theme, "Worker processes" if ax is axes[0] else "")
        ax.set_title(title, color=theme.ink_strong, fontsize=9, loc="left")
        ax.set_xlabel("Seconds", color=theme.ink, fontsize=8.5)
        ax.set_ylim(0, 18)
        ax.legend(loc="lower left")

    save(fig, out, "laravel-replicas", theme)
    return "laravel-replicas"


def fig_laravel_scale_up(out: Path, theme: Theme) -> str:
    """Time to reach twenty workers after a burst: one balance_max_shift step
    per cycle against fast_scale_up, which closes half the gap per cycle."""
    rows = read_csv(LARAVEL / "scale-up.csv")

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    for mode, color, label in (("fast", theme.series[0], "fast_scale_up"), ("step", theme.series[1], "One step per cycle")):
        series = [r for r in rows if r["mode"] == mode]
        t = [float(r["t"]) for r in series]
        workers = [int(r["workers"]) for r in series]
        # Both lines end at max_processes: mark when each got there instead.
        full = next(x for x, w in zip(t, workers) if w == max(workers))
        ax.plot(t, workers, color=color, label=f"{label}: {max(workers)} workers after {full:.1f} s")
        ax.plot([full], [max(workers)], marker="o", markersize=4, color=color)
    finish(ax, theme, "Worker processes")
    ax.set_xlabel("Seconds after the burst", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, 25)
    ax.set_ylim(0, 22)
    ax.set_yticks([0, 5, 10, 15, 20])
    ax.legend(loc="lower right")

    save(fig, out, "laravel-fast-scale-up", theme)
    return "laravel-fast-scale-up"


def fig_laravel_event_driven(out: Path, theme: Theme) -> str:
    """Workers after a burst at the production cadence, polled against woken
    by the broker, every run drawn; the legend carries the median time to the
    full twenty."""
    rows = read_csv(LARAVEL / "event-driven.csv")

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    for mode, color, label in (("event", theme.series[0], "event_driven"), ("poll", theme.series[1], "Polling")):
        runs = []
        for run in sorted({r["run"] for r in rows if r["mode"] == mode}):
            series = [r for r in rows if r["mode"] == mode and r["run"] == run]
            runs.append(([float(r["t"]) for r in series], [int(r["workers"]) for r in series]))
        peak = max(max(workers) for _, workers in runs)
        full = sorted(next(x for x, w in zip(t, workers) if w == peak) for t, workers in runs)
        for index, (t, workers) in enumerate(runs):
            ax.step(
                t,
                workers,
                where="post",
                color=color,
                alpha=0.85,
                linewidth=1.2,
                label=f"{label}: {peak} workers after {full[len(full) // 2]:.1f} s, median of {len(runs)}"
                if index == 0
                else None,
            )
    finish(ax, theme, "Worker processes")
    ax.set_xlabel("Seconds after the burst started", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 22)
    ax.set_yticks([0, 5, 10, 15, 20])
    ax.legend(loc="lower right")

    save(fig, out, "laravel-event-driven", theme)
    return "laravel-event-driven"


HORIZON_RAFT = BENCH / "2026-10-01-laravel-horizon-raft" / "raw" / "runs.csv"


def horizon_raft_median(rows, campaign: str, engine: str, field: str) -> float:
    """The median of one field over a lane's correct runs."""
    from statistics import median

    return median(
        float(r[field]) for r in rows
        if r["campaign"] == campaign and r["engine"] == engine and r["correct"] == "True"
    )


def fig_laravel_horizon_throughput(out: Path, theme: Theme) -> str:
    """Completed jobs per second, Horizon against Queen, per scenario: the
    median of five runs each."""
    rows = read_csv(HORIZON_RAFT)
    scenarios = [
        ("strict-all", "10 ms jobs, both fsync every write"),
        ("everysec-all", "10 ms jobs, Redis fsync once a second"),
        ("noop-all", "Empty jobs"),
        ("cpu", "CPU-bound jobs, about 20 ms"),
        ("burst-all", "Burst, autoscaling 1 to 16 workers"),
    ]
    labels = [label for _, label in scenarios][::-1]
    horizon = [horizon_raft_median(rows, c, "horizon", "jobs_per_second") for c, _ in scenarios][::-1]
    queen = [horizon_raft_median(rows, c, "queen-rust", "jobs_per_second") for c, _ in scenarios][::-1]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    y = range(len(labels))
    height = 0.38
    bars_h = ax.barh([i + height / 2 for i in y], horizon, height, color=theme.series[1], label="Horizon, Redis")
    bars_q = ax.barh([i - height / 2 for i in y], queen, height, color=theme.series[0], label="Queen, Raft broker")
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_yticks(list(y), labels)
    ax.set_xlabel("Completed jobs per second, median of 5 runs", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(queen + horizon) * 1.16)
    for bars in (bars_h, bars_q):
        for bar in bars:
            ax.annotate(f"{bar.get_width():.0f}", xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
                        xytext=(4, 0), textcoords="offset points", va="center", fontsize=8,
                        color=theme.ink_strong)
    ax.legend(loc="lower right")

    save(fig, out, "laravel-horizon-throughput", theme)
    return "laravel-horizon-throughput"


def fig_laravel_horizon_memory(out: Path, theme: Theme) -> str:
    """Proportional set size of the orchestrator, the workers and the lease
    helpers with eight workers, stacked: Horizon, Queen with one helper per
    worker, and Queen renewing leases in the supervisor."""
    rows = read_csv(HORIZON_RAFT)
    stacks = [
        ("Horizon", "strict-all", "horizon"),
        ("Queen, a lease helper\nper worker", "ablation-none", "queen-rust"),
        ("Queen, leases renewed\nby the supervisor", "strict-all", "queen-rust"),
    ][::-1]
    parts = [
        ("orchestrator_pss_mib", "Orchestrator", theme.series[2]),
        ("workers_pss_mib", "Workers", theme.series[0]),
        ("renewers_pss_mib", "Lease helpers", theme.series[1]),
    ]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.6))
    left = [0.0] * len(stacks)
    for field, label, color in parts:
        values = [horizon_raft_median(rows, campaign, engine, field) for _, campaign, engine in stacks]
        ax.barh([name for name, _, _ in stacks], values, left=left, height=0.55, color=color, label=label)
        left = [a + b for a, b in zip(left, values)]
    for index, total in enumerate(left):
        ax.annotate(f"{total:.0f} MiB", xy=(total, index), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=8.5, color=theme.ink_strong)
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("PSS with 8 workers (MiB), median of 5 runs", color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(left) * 1.18)
    ax.legend(loc="lower right", ncol=3)

    save(fig, out, "laravel-horizon-memory", theme)
    return "laravel-horizon-memory"


def fig_laravel_queen_optimizations(out: Path, theme: Theme) -> str:
    """What each worker-side change adds, one at a time, against Horizon at
    the same strict durability."""
    rows = read_csv(HORIZON_RAFT)
    steps = [
        ("Queen 0.5: helpers, synchronous ACK", "ablation-none"),
        ("Leases renewed by the supervisor", "ablation-lease"),
        ("Asynchronous ACK only", "ablation-ack"),
        ("Supervisor renewal and asynchronous ACK", "strict"),
        ("Both, and the next batch popped ahead", "strict-all"),
    ][::-1]
    values = [horizon_raft_median(rows, c, "queen-rust", "jobs_per_second") for _, c in steps]
    horizon = horizon_raft_median(rows, "strict-all", "horizon", "jobs_per_second")

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.9))
    bars = ax.barh([label for label, _ in steps], values, height=0.55, color=theme.series[0])
    ax.axvline(horizon, color=theme.series[1], linestyle="--", linewidth=1.3,
               label=f"Horizon at the same durability: {horizon:.0f}")
    ax.legend(loc="upper left", bbox_to_anchor=(0, -0.2))
    for bar in bars:
        ax.annotate(f"{bar.get_width():.0f}", xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
                    xytext=(4, 0), textcoords="offset points", va="center", fontsize=8,
                    color=theme.ink_strong)
    finish(ax, theme, "")
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Completed jobs per second, 10 ms jobs, fsync every write, median of 5 runs",
                  color=theme.ink, fontsize=8.5)
    ax.set_xlim(0, max(values + [horizon]) * 1.12)
    ax.set_xticks([0, 100, 200, 300, 400, 500, 600])

    save(fig, out, "laravel-queen-optimizations", theme)
    return "laravel-queen-optimizations"


def fig_laravel_horizon_latency(out: Path, theme: Theme) -> str:
    """Dispatch-to-completion latency at a steady arrival rate below capacity:
    p50 and p95 per engine and rate."""
    rows = read_csv(HORIZON_RAFT)
    rates = [("latency-100", "100 jobs/s"), ("latency-300", "300 jobs/s")]

    style(theme)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6), sharey=True, gridspec_kw={"wspace": 0.1})
    for ax, (campaign, title) in zip(axes, rates):
        groups = ["p50", "p95"]
        horizon = [horizon_raft_median(rows, campaign, "horizon", f"end_to_end_{p}_ms") for p in groups]
        queen = [horizon_raft_median(rows, campaign, "queen-rust", f"end_to_end_{p}_ms") for p in groups]
        x = range(len(groups))
        width = 0.36
        bars_h = ax.bar([i - width / 2 for i in x], horizon, width, color=theme.series[1], label="Horizon")
        bars_q = ax.bar([i + width / 2 for i in x], queen, width, color=theme.series[0], label="Queen")
        finish(ax, theme, "End-to-end latency (ms)" if ax is axes[0] else "")
        ax.set_title(title, color=theme.ink_strong, fontsize=9, loc="left")
        ax.set_xticks(list(x), groups)
        for bars in (bars_h, bars_q):
            for bar in bars:
                ax.annotate(f"{bar.get_height():.0f}", xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                            xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8,
                            color=theme.ink_strong)
        ax.legend(loc="upper left")
    top = max(
        horizon_raft_median(rows, c, e, "end_to_end_p95_ms")
        for c, _ in rates for e in ("horizon", "queen-rust")
    )
    axes[0].set_ylim(0, top * 1.2)

    save(fig, out, "laravel-horizon-latency", theme)
    return "laravel-horizon-latency"


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
