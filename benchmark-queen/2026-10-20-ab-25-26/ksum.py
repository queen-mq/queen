#!/usr/bin/env python3
"""ksum.py <kruns/tag> <offered total>: one line for a facade point of kab.sh (3 kload processes): steady rates,
produce and e2e p50/p99 over the last 30 s (the tail histograms, pooled), broker cores (sample-n*.log, as step.py)."""
import json, glob, os, sys, collections
d, total = sys.argv[1], int(sys.argv[2])
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../2026-10-12-three-node-new-forms/scripts"))
from report import pct
js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "k*.json")))]
if len(js) != 3:
    print(f"{total:>9,}: only {len(js)} of 3 kload processes reported"); sys.exit(0)
he, hp = collections.Counter(), collections.Counter()
for j in js:
    for k, v in j.get("hist_e2e_tail_us", j["hist_e2e_us"]).items(): he[int(k)] += v
    for k, v in j.get("hist_produce_tail_us", j["hist_produce_us"]).items(): hp[int(k)] += v
st = [j["steady"] for j in js]
off = sum(s["offered_per_s"] for s in st); psh = sum(s["pushed_per_s"] for s in st); pop = sum(s["popped_per_s"] for s in st)
err = sum(j["msgs"]["errors"] for j in js) + sum(j["pop_errors"] for j in js)
a = pct(hp, [0.5, 0.99]); b = pct(he, [0.5, 0.99])
cpu = []
for n in (1, 2, 3):
    f = os.path.join(d, f"sample-n{n}.log")
    rows = [dict(p.split("=", 1) for p in l.split() if "=" in p) for l in open(f)] if os.path.exists(f) else []
    rows = [r for r in rows if "ticks" in r]
    if len(rows) >= 4:
        m = rows[2:-1]; dt = float(m[-1]["t"]) - float(m[0]["t"])
        cpu.append(f"{(int(m[-1]['ticks']) - int(m[0]['ticks'])) / 100.0 / dt:.1f}")
ok = err == 0 and off > 0.985 * total and psh > 0.99 * off and pop > 0.98 * psh
print(f"{total:>9,}: steady offered {off:,.0f} pushed {psh:,.0f} consumed {pop:,.0f} errors {err} | produce p50 {a[0]:.1f} p99 {a[1]:.1f} | "
      f"e2e p50 {b[0]:.1f} p99 {b[1]:.1f} ms | broker cores {' / '.join(cpu)} | {'OK' if ok else 'NOT CARRIED'}")
