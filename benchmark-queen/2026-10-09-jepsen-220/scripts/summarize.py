#!/usr/bin/env python3
"""summarize.py <archive dir>: the runs of each set by the build they ran on, by verdict and by family."""
import collections, os, re, sys
A = sys.argv[1]
SWAPS = [("2026-10-09T22:20:53Z", "rc3"), ("2026-10-09T23:01:26Z", "rc4"), ("2026-10-09T23:23:35Z", "rc5"), ("2026-10-10T01:57:39Z", "rc6")]
def build(ts):
    b = "rc1"
    for t, name in SWAPS:
        if ts >= t: b = name
    return b
def base(label):
    return re.sub(r"-(r4|r5|r6|again|x\d+)$", "", label)
allruns = []
for s in ("first-pass", "set-a", "set-b"):
    starts, runs = {}, []
    for line in open(os.path.join(A, s, "summary.txt")):
        p = line.split()
        if len(p) < 3: continue
        if p[1] == "start": starts[p[2].rstrip(":")] = p[0]
        if p[1] == "done":
            label = p[2].rstrip(":"); verdict = p[3]
            runs.append((label, starts.get(label, p[0]), p[0], verdict))
    v = collections.Counter(r[3].split("(")[0] for r in runs)
    print(f"== {s}: {len(runs)} runs, from {runs[0][1]} to {runs[-1][2]}; verdicts {dict(v)}")
    if s != "first-pass":
        b = collections.Counter(build(r[1]) for r in runs)
        print("   by build at the start of the test:", dict(sorted(b.items())))
        nv = [(r[0], r[3], build(r[1])) for r in runs if r[3] != "valid"]
        print("   not valid:", nv)
        for r in runs: allruns.append((s, r[0], build(r[1]), r[3]))
    else:
        print("   not valid:", [(r[0], r[3]) for r in runs if r[3] != "valid"])
bases = collections.defaultdict(list)
for s, label, b, v in allruns: bases[base(label)].append((s, label, b, v))
print(f"== both sets: {len(allruns)} runs of {len(bases)} tests")
on6 = [k for k, rs in bases.items() if any(b == "rc6" for _, _, b, _ in rs)]
not6 = [k for k in bases if k not in on6]
print(f"   tests with a run on rc6: {len(on6)}; without: {len(not6)} {not6[:12]}")
rc6 = [r for r in allruns if r[2] == "rc6"]
print(f"   runs on rc6: {len(rc6)}, verdicts {dict(collections.Counter(r[3].split('(')[0] for r in rc6))}")
fam = collections.Counter(re.match(r"[a-z0-9]+", k).group(0) for k in bases)
print("   tests by label prefix:", dict(sorted(fam.items(), key=lambda kv: -kv[1])))
sfx = collections.Counter((re.search(r"-(r4|r5|r6|again|x\d+)$", l) or [None, "first"])[1] if re.search(r"-(r4|r5|r6|again|x\d+)$", l) else "original" for _, l, _, _ in allruns)
print("   runs by label suffix:", dict(sfx))
