#!/usr/bin/env python3
"""Aggregate goload's per-run verify blocks into one cell-wide verdict.

Reads <dir>/*.json rather than grepping stdout: the JSON is the artifact goload
guarantees, and a text pattern that silently stops matching reports a clean bill
for a run that lost messages.
"""
import json, glob, sys

d = sys.argv[1] if len(sys.argv) > 1 else "/root/soak/gate"
keys = ["sentOk", "received", "missing", "duplicate", "extra", "crossTenant", "undecodable"]
tot = dict.fromkeys(keys, 0)
verdicts, n = {}, 0

for f in sorted(glob.glob(d + "/*.json")):
    if f.endswith("-interval.csv"):
        continue
    try:
        doc = json.load(open(f))
    except Exception as e:
        print("  unreadable %s: %s" % (f, e))
        continue
    v = doc.get("verify") or {}
    if not v:
        continue
    n += 1
    vd = v.get("verdict", "?")
    verdicts[vd] = verdicts.get(vd, 0) + 1
    for k in keys:
        tot[k] += v.get(k) or 0

print("runs with a verify block: %d" % n)
print("verdicts: %s" % verdicts)
print()
for k in keys:
    print("  %-12s %14s" % (k, format(tot[k], ",")))
if tot["sentOk"]:
    loss = 100.0 * tot["missing"] / tot["sentOk"]
    dup = 100.0 * tot["duplicate"] / tot["sentOk"]
    print()
    print("  loss rate       %.6f%%" % loss)
    print("  duplicate rate  %.6f%%" % dup)
