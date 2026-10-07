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


from chartlib import DARK, LIGHT
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


FIGURES = (
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
