#!/usr/bin/env python3
"""One CSV row per measured run of a campaign directory.

Usage: extract.py RESULTS_DIR > runs.csv

RESULTS_DIR holds one subdirectory per campaign, each written by
`laravel-supervisors/scripts/run.sh --output`.
"""

import csv
import glob
import json
import os
import sys

MiB = 2 ** 20
FIELDS = [
    "campaign", "engine", "run", "correct", "jobs", "jobs_per_second",
    "queue_wait_p50_ms", "queue_wait_p95_ms", "queue_wait_p99_ms",
    "end_to_end_p50_ms", "end_to_end_p95_ms", "end_to_end_p99_ms",
    "app_memory_mib", "orchestrator_pss_mib", "workers_pss_mib", "renewers_pss_mib",
    "backend_memory_mib", "app_cpu_seconds", "backend_cpu_seconds",
    "worker_peak", "time_to_peak_seconds", "worker_seconds",
]


def get(value, *path):
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def mib(value):
    return None if value is None else round(value / MiB, 2)


def rounded(value, digits=2):
    return None if value is None else round(value, digits)


def main() -> None:
    base = sys.argv[1]
    writer = csv.DictWriter(sys.stdout, FIELDS, lineterminator="\n")
    writer.writeheader()
    for campaign in sorted(os.listdir(base)):
        for path in sorted(glob.glob(f"{base}/{campaign}/*/*/*/r0*/summary.json")):
            summary = json.load(open(path))
            parts = path.split(os.sep)
            engine, run = parts[-4], parts[-2]
            peak_ns = get(summary, "scaling", "time_to_peak_workers_ns")
            writer.writerow({
                "campaign": campaign,
                "engine": engine,
                "run": run,
                "correct": get(summary, "correctness", "correct"),
                "jobs": get(summary, "correctness", "expected"),
                "jobs_per_second": rounded(get(summary, "throughput", "completion_span_jobs_per_second"), 1),
                "queue_wait_p50_ms": rounded(get(summary, "latency", "queue", "p50_ms"), 1),
                "queue_wait_p95_ms": rounded(get(summary, "latency", "queue", "p95_ms"), 1),
                "queue_wait_p99_ms": rounded(get(summary, "latency", "queue", "p99_ms"), 1),
                "end_to_end_p50_ms": rounded(get(summary, "latency", "end_to_end", "p50_ms"), 1),
                "end_to_end_p95_ms": rounded(get(summary, "latency", "end_to_end", "p95_ms"), 1),
                "end_to_end_p99_ms": rounded(get(summary, "latency", "end_to_end", "p99_ms"), 1),
                "app_memory_mib": mib(get(summary, "resources", "app", "memory_current_bytes", "p50")),
                "orchestrator_pss_mib": mib(get(summary, "resources", "orchestrator", "pss_bytes", "p50")),
                "workers_pss_mib": mib(get(summary, "resources", "workers", "pss_bytes", "p50")),
                "renewers_pss_mib": mib(get(summary, "resources", "lease_renewers", "pss_bytes", "p50")) or 0,
                "backend_memory_mib": mib(get(summary, "resources", "backend", "memory_current_bytes", "p50")),
                "app_cpu_seconds": rounded(get(summary, "resources", "app", "cpu_seconds")),
                "backend_cpu_seconds": rounded(get(summary, "resources", "backend", "cpu_seconds")),
                "worker_peak": get(summary, "scaling", "worker_peak"),
                "time_to_peak_seconds": None if peak_ns is None else round(peak_ns / 1e9, 2),
                "worker_seconds": rounded(get(summary, "scaling", "worker_seconds"), 1),
            })


if __name__ == "__main__":
    main()
