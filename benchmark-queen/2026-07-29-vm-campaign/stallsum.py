#!/usr/bin/env python3
"""stallsum.py <outdir> <run-id> — read the stall out of the per-second
consumer-row samples.

For each (tenant, partition) that ever built a backlog, report how long it was
dark, and what its consumer row said while it was dark: a live lease
(worker_id + lease_ms_left > 0) means the lease filter in the wildcard candidate
set is what hid it; a backlog with NO lease means something else did.
"""
import csv, os, sys
from collections import defaultdict

outdir, runid = sys.argv[1], sys.argv[2]
p = os.path.join(outdir, runid + ".stalldiag.csv")
series = defaultdict(list)
with open(p) as f:
    for r in csv.DictReader(f):
        try:
            t = float(r["t_unix"]); bl = int(r["backlog"])
        except (ValueError, TypeError):
            continue
        lease = r["lease_ms_left"].strip()
        lease = float(lease) if lease not in ("", "None") else None
        series[(r["tenant"], r["partition"])].append(
            (t, bl, lease, r["worker_id"].strip()))

print(f"=== {runid}: partitions that held a backlog >200 for >3 consecutive samples")
print(f"{'tenant':<10}{'part':<6}{'samples':>8}{'secs':>7}{'maxBacklog':>12}"
      f"{'w/ live lease':>15}{'no lease':>10}{'maxLeaseLeft_ms':>17}")
any_row = False
for (tn, part), s in sorted(series.items()):
    if len(s) < 3:
        continue
    any_row = True
    dur = s[-1][0] - s[0][0]
    leased = sum(1 for _, _, l, w in s if l is not None and l > 0 and w != "-")
    noleaseb = sum(1 for _, _, l, w in s if (l is None or l <= 0))
    maxlease = max([l for _, _, l, _ in s if l is not None] or [0])
    print(f"{tn:<10}{part:<6}{len(s):>8}{dur:>7.0f}{max(x[1] for x in s):>12}"
          f"{leased:>15}{noleaseb:>10}{maxlease:>17.0f}")
if not any_row:
    print("  (no partition held a backlog long enough to sample — no stall this run)")

print("\n--- raw samples for the worst partition")
if series:
    worst = max(series, key=lambda k: max(x[1] for x in series[k]))
    for t, bl, l, w in series[worst][:40]:
        print(f"  t={t:.0f} backlog={bl:<7} lease_ms_left={l} worker={w}")
