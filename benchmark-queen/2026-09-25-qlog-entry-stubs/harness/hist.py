#!/usr/bin/env python3
# hist.py RUN... — count and mean of every queen_raft_* histogram/summary in each run's metrics.txt.
import sys, os, re
runs = sys.argv[1:]
data = {}
for r in runs:
    m = {}
    for line in open(os.path.join(r, "metrics.txt")):
        mm = re.match(r'^(queen_raft_[a-z0-9_]+)_(sum|count) ([0-9.e+-]+)$', line.strip())
        if mm:
            m.setdefault(mm.group(1), {})[mm.group(2)] = float(mm.group(3))
    data[os.path.basename(r)] = m
keys = sorted(set().union(*[d.keys() for d in data.values()]))
print(f"{'metric':52s} " + " ".join(f"{os.path.basename(r)[:26]:>30s}" for r in runs))
for k in keys:
    cells = []
    for r in runs:
        v = data[os.path.basename(r)].get(k, {})
        n, s = v.get("count", 0), v.get("sum", 0)
        cells.append(f"{int(n):>9d} x {s / n if n else 0:>12.4f}" if n else f"{'-':>24s}")
    print(f"{k[11:]:52s} " + " ".join(f"{c:>30s}" for c in cells))
