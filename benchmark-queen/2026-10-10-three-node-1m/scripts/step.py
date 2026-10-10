#!/usr/bin/env python3
"""step.py <step dir> <offered total>: one line for a ramp step, with OK when the rate was carried."""
import json, glob, os, sys, collections
d, total = sys.argv[1], int(sys.argv[2])
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report import pct
js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
if len(js) != 9:
    print(f"{total:>9,}: only {len(js)} of 9 load processes reported  FAIL"); sys.exit(0)
he, hp = collections.Counter(), collections.Counter()
for j in js:
    for k, v in j["hist_e2e_us"].items(): he[int(k)] += v
    for k, v in j["hist_produce_us"].items(): hp[int(k)] += v
st = [j["steady"] for j in js]
off = sum(s["offered_per_s"] for s in st); psh = sum(s["pushed_per_s"] for s in st); pop = sum(s["popped_per_s"] for s in st)
shed = sum(j["msgs"]["shed"] for j in js); err = sum(j["msgs"]["errors"] for j in js) + sum(j["pop_errors"] for j in js) + sum(j["ack_errors"] for j in js)
lag = sum(j["lag"] for j in js); late = max(j.get("late_start_ms", 0) for j in js)
a = pct(hp, [0.5, 0.99]); b = pct(he, [0.5, 0.99])
cpu = []
for n in (1, 2, 3):
    f = os.path.join(d, f"sample-n{n}.log")
    rows = [dict(p.split("=", 1) for p in l.split() if "=" in p) for l in open(f)] if os.path.exists(f) else []
    rows = [r for r in rows if "th_queen-rsm-apply" in r]
    if len(rows) >= 4:
        m = rows[2:-1]; dt = float(m[-1]["t"]) - float(m[0]["t"])
        ap = (int(m[-1]["th_queen-rsm-apply"]) - int(m[0]["th_queen-rsm-apply"])) / dt
        rf = (int(m[-1].get("th_queen-raft", 0)) - int(m[0].get("th_queen-raft", 0))) / dt
        tot = (int(m[-1]["ticks"]) - int(m[0]["ticks"])) / 100.0 / dt
        cpu.append(f"n{n} {tot:.1f}c apply {ap:.0f}% raft {rf:.0f}%")
ok = shed == 0 and err == 0 and off > 0.985 * total and psh > 0.995 * off and pop > 0.99 * psh and a[1] < 1000
lc = sum(s["cpu_pct"] for s in st) / 100
print(f"{total:>9,}: offered {off:,.0f} pushed {psh:,.0f} consumed {pop:,.0f} shed {shed:,} errors {err} lag {lag:,} | push p50 {a[0]:.1f} p99 {a[1]:.1f} | e2e p50 {b[0]:.1f} p99 {b[1]:.1f} | {'; '.join(cpu)} | loaders {lc:.1f}c  {'OK' if ok else 'NOT CARRIED'} ")
