#!/usr/bin/env python3
"""txnstep.py <step dir> <offered total msg/s> <messages per transaction> <duration s>: one line for a step of the
transactional pipeline (txnrun.sh), with OK when the rate was carried: nothing shed, no error, and the workers and
the readers of the output at 99% of the offered rate or more over the steady windows (the 10 s windows after the
first one and before the producers stop)."""
import collections, glob, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report import pct

d, total, size, dur = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "p*.json")))]
want = int(os.environ.get("NPROC", "9"))
if len(js) != want:
    print(f"{total:>9,}: only {len(js)} of {want} load processes reported  FAIL")
    sys.exit(0)
last = max(2, dur // 10)
hc, he, hp = collections.Counter(), collections.Counter(), collections.Counter()
txn_s = msg_s = pop_s = off_s = 0.0
for j in js:
    for k, v in j["txn"]["hist_commit_us"].items(): hc[int(k)] += v
    for k, v in j["hist_e2e_us"].items(): he[int(k)] += v
    for k, v in j["hist_produce_us"].items(): hp[int(k)] += v
    tw = [w for w in j["txn"]["windows"] if 2 <= w["k"] <= last]
    ww = [w for w in j["windows"] if 2 <= w["k"] <= last]
    if tw:
        txn_s += sum(w["txn_per_s"] for w in tw) / len(tw)
        msg_s += sum(w["msgs_per_s"] for w in tw) / len(tw)
    if ww:
        pop_s += sum(w["popped_per_s"] for w in ww) / len(ww)
        off_s += sum(w["offered_per_s"] for w in ww) / len(ww)
shed = sum(j["msgs"]["shed"] for j in js)
errs = sum(j["msgs"]["errors"] for j in js) + sum(j["pop_errors"] + j["ack_errors"] for j in js) + sum(j["txn"]["errors"] for j in js)
aborts = sum(j["txn"]["aborts"] for j in js)
c = pct(hc, [0.5, 0.99]); e = pct(he, [0.5, 0.99]); p = pct(hp, [0.5, 0.99])
cores = []
t0 = max(j["start_ms"] for j in js) / 1000 + 12; t1 = min(j["producers_end_ms"] for j in js) / 1000 - 2
for n in (1, 2, 3):
    f = os.path.join(d, f"sample-n{n}.log")
    rows = [dict(x.split("=", 1) for x in l.split() if "=" in x) for l in open(f)] if os.path.exists(f) else []
    rows = [r for r in rows if "ticks" in r and t0 <= float(r["t"]) <= t1]
    cores.append(f"{(int(rows[-1]['ticks']) - int(rows[0]['ticks'])) / 100.0 / (float(rows[-1]['t']) - float(rows[0]['t'])):.1f}" if len(rows) >= 2 else "?")
lc = sum(j["steady"]["cpu_pct"] for j in js) / 100
ok = shed == 0 and errs == 0 and msg_s >= 0.99 * total and pop_s >= 0.99 * total
print(f"{total:>9,}: offered {off_s:,.0f} | workers {txn_s:,.0f} txn/s = {msg_s:,.0f} msg/s | read {pop_s:,.0f} | shed {shed:,} errors {errs} aborts {aborts:,} | "
      f"commit p50 {c[0]:.1f} p99 {c[1]:.1f} | e2e p50 {e[0]:.0f} p99 {e[1]:.0f} | feed push p50 {p[0]:.1f} p99 {p[1]:.1f} | cores {' / '.join(cores)} | loaders {lc:.1f}c  {'OK' if ok else 'NOT CARRIED'} ")
