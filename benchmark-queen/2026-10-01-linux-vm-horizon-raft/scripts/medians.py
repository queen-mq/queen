#!/usr/bin/env python3
"""Median of each column per group, lane and engine: medians.py raw/runs.csv"""
import csv
import statistics
import sys
from collections import defaultdict

COLUMNS = [
    ("jobs_per_second", "Jobs/s", 0), ("dispatch_jobs_per_second", "Dispatch/s", 0),
    ("queue_wait_p50_ms", "Wait p50 ms", 0), ("queue_wait_p95_ms", "Wait p95 ms", 0),
    ("app_memory_mib", "App MiB", 0), ("workers_pss_mib", "Workers PSS MiB", 0),
    ("app_cpu_ms_per_job", "App CPU ms/job", 2), ("backend_cpu_ms_per_job", "Backend CPU ms/job", 2),
    ("backend_operations_per_job", "Backend ops/job", 1),
]
rows = defaultdict(list)
for row in csv.DictReader(open(sys.argv[1])):
    rows[(row["group"], row["lane"], row["engine"])].append(row)
print("| Group | Lane | Engine | Runs | " + " | ".join(label for _, label, _ in COLUMNS) + " |")
print("|" + "---|" * (4 + len(COLUMNS)))
for (group, lane, engine), runs in rows.items():
    cells = []
    for key, _, digits in COLUMNS:
        values = [float(r[key]) for r in runs if r[key] not in ("", "None")]
        cells.append(f"{statistics.median(values):.{digits}f}" if values else "—")
    print(f"| {group} | {lane} | {engine} | {len(runs)} | " + " | ".join(cells) + " |")
