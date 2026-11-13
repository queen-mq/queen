#!/usr/bin/env python3
"""threads.py <step dir>... : per broker, cores by thread name over the step's steady part (the sampler's 10 s lines,
first two and last one left out, as step.py does). Only the names the sampler keeps: its top 14 by ticks."""
import os, sys

for d in sys.argv[1:]:
    d = d.rstrip("/")
    print("/".join(d.split("/")[-2:]))
    for n in (1, 2, 3):
        f = os.path.join(d, f"sample-n{n}.log")
        rows = [dict(p.split("=", 1) for p in l.split() if "=" in p) for l in open(f)] if os.path.exists(f) else []
        rows = [r for r in rows if "ticks" in r]
        m = rows[2:-1] if len(rows) >= 5 else rows
        if len(m) < 2:
            print(f"  n{n}: too few samples ({len(rows)})")
            continue
        dt = float(m[-1]["t"]) - float(m[0]["t"])
        tot = (int(m[-1]["ticks"]) - int(m[0]["ticks"])) / 100.0 / dt
        th = {}
        for k in m[-1]:
            if k.startswith("th_") and k in m[0]:
                th[k[3:]] = (int(m[-1][k]) - int(m[0][k])) / 100.0 / dt
        top = sorted(th.items(), key=lambda kv: -kv[1])
        named = sum(v for _, v in top)
        print(f"  n{n}: {tot:5.2f} cores over {dt:.0f}s | " + "  ".join(f"{k} {v:.2f}" for k, v in top if v >= 0.02) + f"  (other {tot - named:.2f})")
