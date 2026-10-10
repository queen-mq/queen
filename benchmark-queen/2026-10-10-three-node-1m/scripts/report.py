#!/usr/bin/env python3
"""report.py <run dir>... : one block per run from the nine load JSONs and the three broker samplers."""
import json, os, sys, glob, collections

def bucket_us(i):
    if i < 1024: return i + 1
    o = 10 + (i - 1024) // 64; sub = (i - 1024) % 64
    return (1 << o) * (1 + (sub + 1) / 64.0)

def pct(h, qs):
    n = sum(h.values()); out = []
    if not n: return [0.0] * len(qs)
    items = sorted(h.items()); acc = 0; k = 0
    targets = [q * n for q in qs]
    for i, c in items:
        acc += c
        while k < len(targets) and acc >= targets[k]:
            out.append(bucket_us(i) / 1000.0); k += 1
    while len(out) < len(qs): out.append(bucket_us(items[-1][0]) / 1000.0)
    return out

def run(d):
    js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
    print(f"== {os.path.basename(d.rstrip('/'))}: {len(js)} load processes")
    if not js: return
    he, hp = collections.Counter(), collections.Counter()
    for j in js:
        for k, v in j["hist_e2e_us"].items(): he[int(k)] += v
        for k, v in j["hist_produce_us"].items(): hp[int(k)] += v
    st = [j["steady"] for j in js]
    off = sum(s["offered_per_s"] for s in st); psh = sum(s["pushed_per_s"] for s in st); pop = sum(s["popped_per_s"] for s in st)
    m = [j["msgs"] for j in js]
    print(f"   steady window {st[0]['secs']:.0f}s: offered {off:,.0f}/s  pushed {psh:,.0f}/s  consumed {pop:,.0f}/s  loader cpu {sum(s['cpu_pct'] for s in st)/100:.1f} cores")
    print(f"   whole run: offered {sum(x['offered'] for x in m):,}  achieved {sum(x['achieved'] for x in m):,}  shed {sum(x['shed'] for x in m):,}  push errors {sum(x['errors'] for x in m):,}  pop errors {sum(j['pop_errors'] for j in js):,}  ack errors {sum(j['ack_errors'] for j in js):,}  popped {sum(j['popped'] for j in js):,}  lag at end {sum(j['lag'] for j in js):,}")
    a = pct(hp, [0.5, 0.99, 0.999]); b = pct(he, [0.5, 0.99, 0.999])
    print(f"   push ms p50 {a[0]:.1f} p99 {a[1]:.1f} p999 {a[2]:.1f}   e2e ms p50 {b[0]:.1f} p99 {b[1]:.1f} p999 {b[2]:.1f}  (n={sum(he.values()):,})")
    # windows by index: totals across processes
    W = collections.defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0])
    for j in js:
        for w in j["windows"]:
            x = W[w["k"]]; x[0] += w["achieved_per_s"]; x[1] += w["popped_per_s"]; x[2] += w["shed_per_s"]; x[3] = max(x[3], w["e2e_p99_ms"]); x[4] += 1
    ks = [k for k in sorted(W) if W[k][4] == len(js)]
    full = [k for k in ks if W[k][0] > 0.5 * off] or ks
    line = lambda k: f"{W[k][0]/1000:.0f}/{W[k][1]/1000:.0f}" + (f"(shed {W[k][2]/1000:.0f})" if W[k][2] > 500 else "")
    print("   per 10 s, pushed/consumed in k msg/s:", " ".join(line(k) for k in ks))
    core = full[2:-1] if len(full) > 6 else full
    if core:
        print(f"   windows after the ramp: pushed min {min(W[k][0] for k in core):,.0f} max {max(W[k][0] for k in core):,.0f}  consumed min {min(W[k][1] for k in core):,.0f}  worst window e2e p99 {max(W[k][3] for k in core):.0f} ms")
    for n in (1, 2, 3):
        f = os.path.join(d, f"sample-n{n}.log")
        if not os.path.exists(f): continue
        rows = []
        for l in open(f):
            kv = dict(p.split("=", 1) for p in l.split() if "=" in p)
            if "anon_kb" in kv: rows.append(kv)
        if len(rows) < 4: print(f"   n{n}: {len(rows)} samples"); continue
        mid = rows[3:-2] if len(rows) > 8 else rows
        dt = float(mid[-1]["t"]) - float(mid[0]["t"])
        ths = [k for k in mid[-1] if k.startswith("th_")]
        cpu = {k[3:]: (int(mid[-1][k]) - int(mid[0].get(k, 0))) / 100.0 / dt for k in ths}
        tot = (int(mid[-1]["ticks"]) - int(mid[0]["ticks"])) / 100.0 / dt
        top = sorted(cpu.items(), key=lambda x: -x[1])[:5]
        anon = [int(r["anon_kb"]) / 1e6 for r in rows]; rr = [int(r["rows"]) for r in rows if r["rows"].isdigit()]
        mf = min(int(r["memfree_kb"]) for r in rows) / 1e6
        print(f"   n{n}: cpu {tot:.1f} cores; threads " + ", ".join(f"{k} {v*100:.0f}%" for k, v in top) + f"; anon GB start {anon[0]:.1f} max {max(anon):.1f} end {anon[-1]:.1f}; rows max {max(rr) if rr else 'na':,} end {rr[-1] if rr else 'na':,}; MemFree min {mf:.1f} GB")
    for n in (1, 2, 3):
        f = os.path.join(d, f"end-n{n}.txt")
        if os.path.exists(f):
            t = open(f).read()
            w = [l for l in t.split("\n") if l.startswith("WARN_ERROR=")]
            role = "leader" if '"role":"leader"' in t else "follower" if '"role":"follower"' in t else "?"
            print(f"   n{n} at the end: {role}, {w[0] if w else 'WARN_ERROR=?'}, data dir {t.strip().split(chr(10))[-1][:12]}")
for d in sys.argv[1:]: run(d)
