#!/usr/bin/env python3
"""Shared pieces of the benchmark figures: the theme, the matplotlib style, the
SVG writer and the artifact readers. See `charts.py` for the design rules."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

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
