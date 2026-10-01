"""Counter deltas between two Prometheus scrapes, per job: promdelta.py RUN_DIR [filter]."""
import json
import re
import sys
run = sys.argv[1]
pat = re.compile(sys.argv[2]) if len(sys.argv) > 2 else None
def load(path):
    out = {}
    for line in open(path):
        if line.startswith('#') or not line.strip():
            continue
        key, _, value = line.rpartition(' ')
        try:
            out[key] = float(value)
        except ValueError:
            pass
    return out
b = load(f'{run}/backend-metrics.before.prom')
a = load(f'{run}/backend-metrics.after.prom')
jobs = json.load(open(f'{run}/summary.json'))['correctness']['expected']
rows = []
for k, v in a.items():
    d = v - b.get(k, 0)
    if d and (pat is None or pat.search(k)):
        rows.append((k, d))
for k, d in sorted(rows):
    print(f'{d:14.1f} {d / jobs:10.4f}/job  {k}')
