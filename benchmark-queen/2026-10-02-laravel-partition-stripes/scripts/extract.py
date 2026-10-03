#!/usr/bin/env python3
"""One CSV row per run of the stripes campaign, or the medians per lane and stripe count.

Usage: extract.py RESULTS_ROOT > runs.csv
       extract.py --medians runs.csv > medians.md

RESULTS_ROOT is the directory scripts/stripes.sh wrote: one directory per lane,
each with one `s<stripes>-r<run>` directory written by
`laravel-supervisors/scripts/run.sh --output`.
"""

import csv
import glob
import json
import os
import re
import statistics
import sys

LANES = ("low-50", "paced-500", "drain-64", "drain-128")
FIELDS = [
    "lane", "stripes", "run", "correct", "jobs", "duplicates", "jobs_per_second",
    "queue_wait_p50_ms", "queue_wait_p95_ms", "queue_wait_p99_ms",
    "end_to_end_p50_ms", "end_to_end_p95_ms", "end_to_end_p99_ms",
    "backend_cpu_seconds", "backend_cpu_ms_per_job", "app_cpu_ms_per_job",
    "backend_memory_mib", "backend_operations_per_job", "pop_requests_per_job",
]
# The medians table: (column, heading, digits).
MEDIANS = [
    ("jobs_per_second", "jobs/s", 0),
    ("end_to_end_p50_ms", "p50 ms", 1),
    ("end_to_end_p95_ms", "p95 ms", 1),
    ("end_to_end_p99_ms", "p99 ms", 1),
    ("backend_cpu_ms_per_job", "broker CPU ms/job", 3),
    ("backend_memory_mib", "broker MiB", 0),
    ("pop_requests_per_job", "pops/job", 3),
]


def get(value, *path):
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def rounded(value, digits=2):
    return None if value is None else round(value, digits)


def per_job(value, jobs):
    return None if value is None or not jobs else round(value * 1000 / jobs, 3)


def row(lane, stripes, run, summary):
    jobs = get(summary, "correctness", "expected")
    ops = get(summary, "backend_operations") or {}
    total = ops.get("total_requests") if ops.get("available") else None
    pops = get(ops, "requests", "pop") if ops.get("available") else None
    backend_cpu = get(summary, "resources", "backend", "cpu_seconds")
    memory = get(summary, "resources", "backend", "memory_current_bytes", "p50")
    return {
        "lane": lane,
        "stripes": stripes,
        "run": run,
        "correct": get(summary, "correctness", "correct"),
        "jobs": jobs,
        "duplicates": get(summary, "correctness", "duplicates", "count"),
        "jobs_per_second": rounded(get(summary, "throughput", "completion_span_jobs_per_second"), 1),
        "queue_wait_p50_ms": rounded(get(summary, "latency", "queue", "p50_ms"), 1),
        "queue_wait_p95_ms": rounded(get(summary, "latency", "queue", "p95_ms"), 1),
        "queue_wait_p99_ms": rounded(get(summary, "latency", "queue", "p99_ms"), 1),
        "end_to_end_p50_ms": rounded(get(summary, "latency", "end_to_end", "p50_ms"), 1),
        "end_to_end_p95_ms": rounded(get(summary, "latency", "end_to_end", "p95_ms"), 1),
        "end_to_end_p99_ms": rounded(get(summary, "latency", "end_to_end", "p99_ms"), 1),
        "backend_cpu_seconds": rounded(backend_cpu),
        "backend_cpu_ms_per_job": per_job(backend_cpu, jobs),
        "app_cpu_ms_per_job": per_job(get(summary, "resources", "app", "cpu_seconds"), jobs),
        "backend_memory_mib": None if memory is None else round(memory / 2 ** 20, 1),
        "backend_operations_per_job": None if total is None or not jobs else round(total / jobs, 3),
        "pop_requests_per_job": None if pops is None or not jobs else round(pops / jobs, 3),
    }


def runs(root):
    writer = csv.DictWriter(sys.stdout, FIELDS, lineterminator="\n")
    writer.writeheader()
    for lane in LANES:
        for directory in sorted(glob.glob(f"{root}/{lane}/s*-r*")):
            match = re.fullmatch(r"s(\d+)-r(\d+)", os.path.basename(directory))
            paths = glob.glob(f"{directory}/*/queen-rust/fixed/r01/summary.json")
            if match is None or len(paths) != 1:
                continue
            with open(paths[0]) as handle:
                writer.writerow(row(lane, int(match[1]), int(match[2]), json.load(handle)))


def medians(path):
    with open(path) as handle:
        rows = list(csv.DictReader(handle))
    print("| Lane | Stripes | Runs | " + " | ".join(heading for _, heading, _ in MEDIANS) + " |")
    print("| --- | ---: | ---: | " + " | ".join("---:" for _ in MEDIANS) + " |")
    for lane in LANES:
        for stripes in sorted({int(r["stripes"]) for r in rows if r["lane"] == lane}):
            group = [r for r in rows if r["lane"] == lane and int(r["stripes"]) == stripes]
            cells = []
            for column, _, digits in MEDIANS:
                values = [float(r[column]) for r in group if r[column] not in ("", None)]
                cells.append("" if not values else f"{statistics.median(values):.{digits}f}")
            print(f"| {lane} | {stripes} | {len(group)} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    if sys.argv[1:2] == ["--medians"]:
        medians(sys.argv[2])
    else:
        runs(sys.argv[1])
