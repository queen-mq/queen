#!/usr/bin/env python3
"""b23report.py <outdir> [run-id ...] — ladder / curve table with the full wait
event distribution and the commit accounting, for B2 and B3.
"""
import json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = sys.argv[1]
ids = sys.argv[2:] or sorted(f[:-5] for f in os.listdir(OUT) if f.endswith(".json"))
ids = [i for i in ids if os.path.exists(os.path.join(OUT, i + ".json"))]
r = subprocess.run([sys.executable, os.path.join(HERE, "ptsum.py"), "--json", OUT] + ids,
                   capture_output=True, text=True)
rows = json.loads(r.stdout or "[]")

print(f"{'run':<16}{'tgt':>7}{'offer':>7}{'push':>8}{'pop':>8}{'shed':>6}"
      f"{'p50':>7}{'p95':>8}{'p99':>10}{'cpuPG':>7}{'cpuBRK':>7}{'cpuPXY':>7}"
      f"{'cpuPXDB':>8}{'cell':>6}{'thr':>6}{'load':>6}{'cmt/s':>8}{'cmt/msg':>8}"
      f"{'req/s':>7}{'us/req':>7}  correctness")
for x in rows:
    print(f"{x['run']:<16}{x['target']:>7}{x['offered']:>7}{x['push']:>8}{x['pop']:>8}"
          f"{x['shed']:>6}{x['e2e_p50']:>7}{x['e2e_p95']:>8}{x['e2e_p99']:>10}"
          f"{x['cpu_pg']:>7}{x['cpu_broker']:>7}{x['cpu_proxy']:>7}{x['cpu_pxdb']:>8}"
          f"{x['cpu_cell']:>6}{x['throttled_frac']:>6}{x['cpu_loader']:>6}"
          f"{x['commits_s']:>8}{str(x['commits_per_msg']):>8}{x['reqs_s']:>7}"
          f"{str(x['proxy_us_per_req']):>7}  {x['verdict']} m={x['missing']} d={x['dup']}"
          f" x={x['extra']} c={x['cross']}"
          + ("  STALL" if x.get("stalled") else "")
          + ("  ERR " + json.dumps(x["errs"]) if x["errs"] else ""))

print("\n--- wait events, ACTIVE backends only, sampled 1/s over the steady window")
for x in rows:
    w = " ".join(f"{k}={v*100:.0f}%" for k, v in list(x["waits"].items())[:8])
    print(f"  {x['run']:<16} n={x['wait_samples']:<4} {w}")
