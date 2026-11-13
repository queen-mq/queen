#!/usr/bin/env python3
"""abtable.py <abplan.out> : the A/B as markdown, one table per workload. Each row is one offered rate, with each
build's result side by side: carried or not, the rate it reached, latency (commit and e2e for transactions, push and
e2e for pushes; p50 / p99 ms), and the three brokers' cores. Then the highest rate each build carried."""
import collections, re, sys

STEP = re.compile(r"^((?:ab|big)-(\S+)-(281|sched)) \[\d\d:\d\d:\d\d\] +([\d,]+): (.*)$")
rows = collections.defaultdict(dict)  # workload -> {(rate, build): text}
for line in open(sys.argv[1]):
    m = STEP.match(line.strip())
    if not m:
        continue
    _, work, build, rate, rest = m.groups()
    rows[work][(int(rate.replace(",", "")), build)] = rest

def cell(rest):
    if rest is None:
        return "not run"
    ok = rest.rstrip().endswith("OK")
    g = lambda pat: (re.search(pat, rest) or [None, "?"])[1]
    if "txn/s" in rest:
        lat = f"commit {g(r'commit p50 ([\d.]+)')} / {g(r'commit p50 [\d.]+ p99 ([\d.]+)')}, e2e {g(r'e2e p50 ([\d.]+)')} / {g(r'e2e p50 [\d.]+ p99 ([\d.]+)')}"
        reach = f"{g(r'workers ([\d,]+) txn/s')} txn/s"
    else:
        lat = f"push {g(r'push p50 ([\d.]+)')} / {g(r'push p50 [\d.]+ p99 ([\d.]+)')}, e2e {g(r'e2e p50 ([\d.]+)')} / {g(r'e2e p50 [\d.]+ p99 ([\d.]+)')}"
        reach = f"{g(r'consumed ([\d,]+)')} consumed"
    cores = g(r"cores ([\d.?]+ / [\d.?]+ / [\d.?]+)")
    return f"{'yes' if ok else '**no**'}: {reach}; {lat}; cores {cores}"

for work, cells in rows.items():
    rates = sorted({r for r, _ in cells})
    print(f"\n### {work}\n")
    print("| Offered msg/s | 2.8.1 | footprint scheduler |")
    print("|---|---|---|")
    for r in rates:
        print(f"| {r:,} | {cell(cells.get((r, '281')))} | {cell(cells.get((r, 'sched')))} |")
    best = {}
    for (r, b), rest in cells.items():
        if rest.rstrip().endswith("OK"):
            best[b] = max(best.get(b, 0), r)
    print(f"\nHighest rate carried: 2.8.1 {best.get('281', 0):,}, scheduler {best.get('sched', 0):,} msg/s.")
