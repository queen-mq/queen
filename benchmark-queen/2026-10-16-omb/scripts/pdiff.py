#!/usr/bin/env python3
"""pdiff.py <prof dir> — the facade's produce and fetch stats over the profile window, per node."""
import json, sys
d = sys.argv[1]
def h(a, b):
    n = b["count"] - a["count"]; m = (b["sumUs"] - a["sumUs"]) / n / 1000 if n else 0
    bk = [(i, y - x) for i, (x, y) in enumerate(zip(a["buckets"], b["buckets"])) if y - x]
    return n, m, bk
for i in (1, 2, 3):
    k0 = json.load(open(f"{d}/status0-{i}.json"))["kafka"]["consume"]
    k1 = json.load(open(f"{d}/status1-{i}.json"))["kafka"]["consume"]
    print(f"== node {i}")
    for t in ("produceStage", "producePlans", "produceCheck", "produceDecode", "produceEnvelope", "producePush", "produceRender", "fetch", "fetchUpstream", "commit"):
        if t in k1:
            n, m, bk = h(k0[t], k1[t]); print(f"  {t:14s} n={n:6d} mean {m:6.2f} ms  {bk[:14]}")
    for c in ("producePushes", "produceRequests", "produceRecords", "fetchEmpty", "fetchRecords"):
        if c in k1: print(f"  {c:14s} +{k1[c] - k0[c]}")
