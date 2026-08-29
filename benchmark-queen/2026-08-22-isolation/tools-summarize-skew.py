#!/usr/bin/env python3
"""Summarise the hot-entity isolation sweep into one table.

Reads each cell's result.json plus both samplers. CPU is averaged over the
ACTIVE window only (broker queen container above 0.3 cores), matching how the
2026-08-04 cost table was built.
"""
import csv
import json
import os
import sys

BASE = sys.argv[1] if len(sys.argv) > 1 else "campaign"
CELLS = ["skew-f1", "skew-f10", "skew-f50", "skew-f200", "skew-f1000"]


def cpu(path, active_col, thresh=0.3):
    try:
        rows = list(csv.DictReader(open(path)))
    except OSError:
        return None
    act = [r for r in rows if float(r.get(active_col, 0) or 0) > thresh]
    if not act:
        return None
    return act


def mean(rows, col):
    return sum(float(r[col]) for r in rows) / len(rows)


print(f"{'cell':>10} {'F':>5} {'share%':>7} {'corr':>6} "
      f"{'cold p50':>9} {'cold p95':>9} {'cold p99':>9} "
      f"{'hot p50':>9} {'hot p95':>9} {'hot p99':>9} "
      f"{'h/c':>6} {'queen':>6} {'pg':>6} {'TOT':>6} {'ldr%':>6}")
print("-" * 130)

rows_out = []
for cell in CELLS:
    rj = os.path.join(BASE, cell, "result.json")
    if not os.path.exists(rj):
        print(f"{cell:>10}  (missing)")
        continue
    d = json.load(open(rj))
    sk, iso, corr = d.get("skew", {}), d.get("isolation", {}), d["correctness"]
    dh, dc = sk.get("delivered_hot", 0), sk.get("delivered_cold", 0)
    share = 100 * dh / (dh + dc) if (dh + dc) else 0.0

    b = cpu(os.path.join(BASE, cell, "sampler-broker.csv"), "cmbench-queen_cores")
    q = mean(b, "cmbench-queen_cores") if b else float("nan")
    pg = mean(b, "cmbench-queen-pg_cores") if b else float("nan")

    lrows = None
    lp = os.path.join(BASE, cell, "sampler-loader.csv")
    if os.path.exists(lp):
        allrows = list(csv.DictReader(open(lp)))
        lrows = [r for r in allrows if float(r.get("host_cpu_pct", 0) or 0) > 5]
    ldr = mean(lrows, "host_cpu_pct") if lrows else float("nan")

    ok = "PASS" if corr["pass"] else f"FAIL g{corr['gaps']}"
    print(f"{cell:>10} {sk.get('hot_factor', 1):>5} {share:>7.1f} {ok:>6} "
          f"{iso['cold_p50_ms']:>9.1f} {iso['cold_p95_ms']:>9.1f} {iso['cold_p99_ms']:>9.1f} "
          f"{iso['hot_p50_ms']:>9.1f} {iso['hot_p95_ms']:>9.1f} {iso['hot_p99_ms']:>9.1f} "
          f"{iso['hot_over_cold_p99']:>6.2f} {q:>6.2f} {pg:>6.2f} {q + pg:>6.2f} {ldr:>6.1f}")
    rows_out.append((cell, sk, iso, corr, q, pg, ldr))

print()
print("skew detail (configured vs delivered, and the lane arithmetic):")
for cell, sk, iso, corr, q, pg, ldr in rows_out:
    if sk.get("hot_factor", 1) <= 1:
        print(f"  {cell:>10}  uniform baseline — every entity at "
              f"{sk.get('cold_lane_offered_per_sec', 0):.2f} ev/s")
        continue
    print(f"  {cell:>10}  configured share {sk['hot_share_pct']:.1f}%  "
          f"hot lane {sk['hot_lane_offered_per_sec']:.1f} ev/s  "
          f"cold lane {sk['cold_lane_offered_per_sec']:.2f} ev/s  "
          f"ceiling {sk['lane_ceiling_per_sec']:.0f}/s @batch {sk.get('per_key_batch_cap')}  "
          f"saturated={sk['hot_saturated']}")

if rows_out:
    base = next((r for r in rows_out if r[1].get("hot_factor", 1) <= 1), None)
    if base:
        b99 = base[2]["cold_p99_ms"]
        print()
        print(f"cold-cohort degradation vs uniform baseline (cold p99 {b99:.1f} ms):")
        for cell, sk, iso, corr, q, pg, ldr in rows_out:
            if sk.get("hot_factor", 1) <= 1:
                continue
            d99 = iso["cold_p99_ms"]
            print(f"  F={sk['hot_factor']:>5}   cold p99 {d99:>8.1f} ms   "
                  f"{d99 / b99 if b99 else float('nan'):>6.2f}x baseline")
