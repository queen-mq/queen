#!/usr/bin/env python3
"""toplevelsum.py <outdir> <run-id> — commits per delivered message, attributed.

Only toplevel=true statements open a transaction of their own, so only those are
commits. The sum is reconciled against pg_stat_database's own xact_commit delta
for the same run: if the two disagree by more than a few percent the breakdown
is not trustworthy and the script says so instead of pretending.
"""
import csv, json, os, re, sys

outdir, runid = sys.argv[1], sys.argv[2]
rows = []
with open(os.path.join(outdir, runid + ".pgss-toplevel.csv")) as f:
    for r in csv.reader(f):
        if len(r) < 6:
            continue
        try:
            rows.append((r[0].strip() == "t", int(r[2]), int(r[3]), float(r[4]), r[5].strip()))
        except ValueError:
            continue

meta = json.load(open(os.path.join(outdir, runid + ".json")))
delivered = meta["achieved"]["poppedMsgs"]
pushed = meta["achieved"]["pushedMsgs"]

BUCKET = [
    ("push",      re.compile(r"log_push", re.I)),
    ("pop",       re.compile(r"log_pop", re.I)),
    ("ack",       re.compile(r"log_ack", re.I)),
    ("lease/wm",  re.compile(r"log_consumers|consumer_watermarks|renew_lease", re.I)),
    ("configure", re.compile(r"configure|log_queues", re.I)),
    ("maint",     re.compile(r"stats|retention|vacuum|analyze|metrics|system_state", re.I)),
]


def bucket(q):
    for n, rx in BUCKET:
        if rx.search(q):
            return n
    return "other"


top = [r for r in rows if r[0]]
nested = [r for r in rows if not r[0]]
tl_calls = sum(r[1] for r in top)

print(f"=== {runid}: {pushed} pushed, {delivered} delivered")
print(f"    top-level statements (= transactions = commits): {tl_calls}")
print(f"    nested statements (inside procedures, share the caller's txn): "
      f"{sum(r[1] for r in nested)}")
print(f"    => {tl_calls/delivered:.3f} top-level statements per delivered message\n")

agg = {}
for _, calls, nrows, ms, q in top:
    a = agg.setdefault(bucket(q), [0, 0.0])
    a[0] += calls; a[1] += ms
print(f"{'bucket':<12}{'commits':>10}{'per msg':>10}{'exec_ms':>10}{'ms/call':>9}")
for k, (c, ms) in sorted(agg.items(), key=lambda kv: -kv[1][0]):
    print(f"{k:<12}{c:>10}{c/delivered:>10.3f}{ms:>10.0f}{ms/c:>9.3f}")

print(f"\n--- every top-level statement")
for _, calls, nrows, ms, q in sorted(top, key=lambda r: -r[1])[:18]:
    print(f"{calls:>9} {calls/delivered:>7.3f}/msg {ms:>9.0f}ms  {bucket(q):<10} {q[:88]}")
