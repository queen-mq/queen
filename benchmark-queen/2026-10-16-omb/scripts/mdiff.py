#!/usr/bin/env python3
"""mdiff.py <metrics0.txt> <metrics1.txt> [filter] — Prometheus text snapshots, before and after: for every
histogram (_sum/_count pairs) the count and mean over the window, sorted by total time; counters' deltas
when a filter is given."""
import re, sys
def load(p):
    d = {}
    for l in open(p):
        if l.startswith("#") or not l.strip():
            continue
        m = re.match(r"^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN)", l)
        if m:
            try: d[m.group(1) + (m.group(2) or "")] = float(m.group(3))
            except ValueError: pass
    return d
a, b = load(sys.argv[1]), load(sys.argv[2]); flt = sys.argv[3] if len(sys.argv) > 3 else ""
rows = []
for k, v in b.items():
    if "_sum" in k.split("{")[0] and k.split("{")[0].endswith("_sum"):
        base = k.replace("_sum", "_count", 1)
        n = b.get(base, 0) - a.get(base, 0); s = v - a.get(k, 0)
        if n > 0 and (not flt or flt in k):
            rows.append((s, n, k))
rows.sort(reverse=True)
for s, n, k in rows[:40]:
    unit = "s" if "seconds" in k else ""
    mean = s / n
    print(f"{k[:110]:110s} n={n:9.0f} mean={mean*1000 if unit else mean:10.3f}{' ms' if unit else ''} total={s:10.3f}")
