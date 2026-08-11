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


def fig_peak(out: Path, theme: Theme) -> str:
    """T1: the accepted rate over the run.

    Offered and accepted are plotted as one series, not two: at per-second
    resolution they sit on top of each other, and the run's shortfall (0.086%)
    is a cumulative total, not something visible in a rate. It is one number,
    so it stays a number — the page's result table carries it.
    """
    rows = parse_progress(
        BENCH / "2026-07-23-3test-report" / "raw" / "t1.out",
        {"achieved": r"achieved=\s*(\d+)/s"},
    )
    rows = [r for r in rows if r["t"] <= 660]
    t = [r["t"] / 60 for r in rows]
    achieved = [r["achieved"] for r in rows]

    style(theme)
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    ax.plot(t, achieved, color=theme.series[0])
    finish(ax, theme, "Accepted per second")
    ax.set_xlabel("Minutes into the run", color=theme.ink, fontsize=8.5)
    ax.yaxis.set_major_formatter(FuncFormatter(thousands))
    ax.set_xlim(0, max(t))
    ax.set_ylim(0, max(achieved) * 1.1)

    save(fig, out, "peak-accepted", theme)
    return "peak-accepted"


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


FIGURES = (fig_soak24, fig_peak, fig_pipeline, fig_cell)


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
