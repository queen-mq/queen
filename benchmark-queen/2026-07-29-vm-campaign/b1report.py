#!/usr/bin/env python3
"""b1report.py <outdir> — pair the broker and proxy runs and price the proxy hop.

Latency is compared on p50/p95, never on p99 alone: a ~30s partition delivery
stall (see stall-diag) lands entirely in the p99 and is a broker-side event that
happens to whichever run it happens to, so a p99 delta would measure the stall
lottery rather than the proxy.
"""
import json, subprocess, sys, os, statistics as st

OUT = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))


def rows(ids):
    r = subprocess.run([sys.executable, os.path.join(HERE, "ptsum.py"), "--json", OUT] + ids,
                       capture_output=True, text=True)
    return {x["run"]: x for x in json.loads(r.stdout or "[]")}


def mean(v):
    return sum(v) / len(v) if v else float("nan")


# ---------------------------------------------------------------- low rate
ids = [f"lat-{t}-100-r{i}" for t in ("broker", "proxy") for i in (1, 2, 3)]
d = rows([i for i in ids if os.path.exists(os.path.join(OUT, i + ".json"))])
print("=== B1a  per-request added latency at 100 msg/s (no queueing) ===")
print(f"{'run':<22}{'pushRTT p50':>12}{'pushRTT p95':>12}{'pushRTT p99':>12}"
      f"{'e2e p50':>10}{'e2e p95':>10}{'cellCPU':>9}{'proxyCPU':>10}")
agg = {}
for k in sorted(d):
    r = d[k]
    tgt = r["target"]
    agg.setdefault(tgt, {"p50": [], "p95": [], "e2e50": [], "cell": [], "pxy": []})
    agg[tgt]["p50"].append(r["push_p50"])
    agg[tgt]["p95"].append(r["latency_push_p95"])
    agg[tgt]["e2e50"].append(r["e2e_p50"])
    agg[tgt]["cell"].append(r["cpu_cell"])
    agg[tgt]["pxy"].append(r["cpu_proxy"])
    print(f"{k:<22}{r['push_p50']:>12}{r['latency_push_p95']:>12}{r['push_p99']:>12}"
          f"{r['e2e_p50']:>10}{r['e2e_p95']:>10}{r['cpu_cell']:>9}{r['cpu_proxy']:>10}")
if "broker" in agg and "proxy" in agg:
    b, p = agg["broker"], agg["proxy"]
    print(f"\n  mean pushRTT p50: broker {mean(b['p50']):.3f} ms   proxy {mean(p['p50']):.3f} ms"
          f"   DELTA {mean(p['p50'])-mean(b['p50']):+.3f} ms/request")
    print(f"  mean pushRTT p95: broker {mean(b['p95']):.3f} ms   proxy {mean(p['p95']):.3f} ms"
          f"   DELTA {mean(p['p95'])-mean(b['p95']):+.3f} ms/request")
    print(f"  mean cell CPU:    broker {mean(b['cell']):.3f} c    proxy {mean(p['cell']):.3f} c"
          f"   DELTA {mean(p['cell'])-mean(b['cell']):+.3f} core")
    print(f"  proxy process CPU when in path: {mean(p['pxy']):.3f} core")

# ---------------------------------------------------------------- sweep
print("\n=== B1b  same offered load, two targets ===")
rates = [600, 1200, 1800, 2300]
ids = [f"sweep-{t}-{r}" for r in rates for t in ("broker", "proxy")]
d = rows([i for i in ids if os.path.exists(os.path.join(OUT, i + ".json"))])
hdr = (f"{'rate':>6}{'target':>8}{'push':>8}{'pop':>8}{'p50':>8}{'p95':>9}{'p99':>10}"
       f"{'pushRTT50':>11}{'cpuPG':>8}{'cpuBRK':>8}{'cpuPXY':>8}{'cell':>7}"
       f"{'cmt/msg':>9}{'stalls':>7}  correctness")
print(hdr)
for r in rates:
    for t in ("broker", "proxy"):
        k = f"sweep-{t}-{r}"
        if k not in d:
            continue
        x = d[k]
        print(f"{r:>6}{t:>8}{x['push']:>8}{x['pop']:>8}{x['e2e_p50']:>8}{x['e2e_p95']:>9}"
              f"{x['e2e_p99']:>10}{x['push_p50']:>11}{x['cpu_pg']:>8}{x['cpu_broker']:>8}"
              f"{x['cpu_proxy']:>8}{x['cpu_cell']:>7}{str(x['commits_per_msg']):>9}"
              f"{x.get('stall_intervals',0):>7}  {x['verdict']} m={x['missing']} d={x['dup']}"
              f" x={x['extra']} c={x['cross']}")
    b, p = d.get(f"sweep-broker-{r}"), d.get(f"sweep-proxy-{r}")
    if b and p:
        print(f"{'':>6}{'DELTA':>8}{'':>8}{'':>8}"
              f"{p['e2e_p50']-b['e2e_p50']:>+8.2f}{p['e2e_p95']-b['e2e_p95']:>+9.2f}"
              f"{'':>10}{p['push_p50']-b['push_p50']:>+11.3f}"
              f"{p['cpu_pg']-b['cpu_pg']:>+8.3f}{p['cpu_broker']-b['cpu_broker']:>+8.3f}"
              f"{p['cpu_proxy']-b['cpu_proxy']:>+8.3f}{p['cpu_cell']-b['cpu_cell']:>+7.3f}")
        cell_pct = 100 * (p["cpu_cell"] - b["cpu_cell"]) / b["cpu_cell"] if b["cpu_cell"] else 0
        pxy_pct = 100 * p["cpu_proxy"] / p["cpu_cell"] if p["cpu_cell"] else 0
        print(f"{'':>6}{'':>8}  proxy adds {p['cpu_cell']-b['cpu_cell']:+.3f} core "
              f"({cell_pct:+.1f}% of the cell); proxy process = {pxy_pct:.1f}% of cell CPU; "
              f"pushRTT {p['push_p50']-b['push_p50']:+.3f} ms; "
              f"{p['reqs_s']:.0f} req/s -> {p['proxy_us_per_req']} us of proxy CPU per request; "
              f"stalled: broker={b.get('stalled')} proxy={p.get('stalled')}")

# drift control
d2 = rows([i for i in ("sweep-broker-600", "drift-broker-600")
           if os.path.exists(os.path.join(OUT, i + ".json"))])
if len(d2) == 2:
    a, z = d2["sweep-broker-600"], d2["drift-broker-600"]
    print(f"\n=== B1c  drift control (same point, first vs last) ===")
    print(f"  push  {a['push']} -> {z['push']} msg/s | e2e p50 {a['e2e_p50']} -> {z['e2e_p50']} ms"
          f" | p95 {a['e2e_p95']} -> {z['e2e_p95']} ms"
          f" | cell {a['cpu_cell']} -> {z['cpu_cell']} core"
          f" | pushRTT p50 {a['push_p50']} -> {z['push_p50']} ms")
