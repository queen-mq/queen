#!/usr/bin/env python3
"""One CSV row per measured run of the Linux VM campaigns.

Usage: extract.py RESULTS_ROOT > runs.csv

RESULTS_ROOT holds one directory per campaign group (load, drain, nofsync,
popfix), each with one subdirectory per lane written by
`laravel-supervisors/scripts/run.sh --output`.
"""

import csv
import glob
import json
import os
import sys

MiB = 2 ** 20
FIELDS = [
    "group", "lane", "engine", "run", "correct", "jobs", "jobs_per_second", "dispatch_jobs_per_second",
    "queue_wait_p50_ms", "queue_wait_p95_ms", "queue_wait_p99_ms",
    "end_to_end_p50_ms", "end_to_end_p95_ms", "end_to_end_p99_ms",
    "app_memory_mib", "orchestrator_pss_mib", "workers_pss_mib", "renewers_pss_mib",
    "backend_memory_mib", "app_cpu_seconds", "backend_cpu_seconds",
    "app_cpu_ms_per_job", "backend_cpu_ms_per_job",
    "backend_operations_per_job", "pop_requests_per_job",
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


def per_job(value, jobs, digits=3):
    return None if value is None or not jobs else round(value * 1000 / jobs, digits)


def operations(summary, jobs):
    """Redis commands (Horizon) or Queen API requests per job, and Queen pops per job."""
    ops = get(summary, "backend_operations") or {}
    if not jobs or not ops.get("available"):
        return None, None
    if "command_calls" in ops:
        total = sum(v for k, v in ops["command_calls"].items() if k != "info")
        return round(total / jobs, 2), None
    pops = get(ops, "requests", "pop")
    return round(ops["total_requests"] / jobs, 3), None if pops is None else round(pops / jobs, 3)


def main() -> None:
    root = sys.argv[1]
    writer = csv.DictWriter(sys.stdout, FIELDS, lineterminator="\n")
    writer.writeheader()
    for group in ("load", "drain", "nofsync", "popfix"):
        for lane in sorted(os.listdir(f"{root}/{group}")):
            for path in sorted(glob.glob(f"{root}/{group}/{lane}/*/*/*/r0*/summary.json")):
                summary = json.load(open(path))
                parts = path.split(os.sep)
                engine, run = parts[-4], parts[-2]
                jobs = get(summary, "correctness", "expected")
                ops_per_job, pops_per_job = operations(summary, jobs)
                peak_ns = get(summary, "scaling", "time_to_peak_workers_ns")
                app_cpu = get(summary, "resources", "app", "cpu_seconds")
                backend_cpu = get(summary, "resources", "backend", "cpu_seconds")
                writer.writerow({
                    "group": group,
                    "lane": lane,
                    "engine": engine,
                    "run": run,
                    "correct": get(summary, "correctness", "correct"),
                    "jobs": jobs,
                    "jobs_per_second": rounded(get(summary, "throughput", "completion_span_jobs_per_second"), 1),
                    "dispatch_jobs_per_second": rounded(get(summary, "throughput", "dispatch_jobs_per_second"), 1),
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
                    "app_cpu_seconds": rounded(app_cpu),
                    "backend_cpu_seconds": rounded(backend_cpu),
                    "app_cpu_ms_per_job": per_job(app_cpu, jobs),
                    "backend_cpu_ms_per_job": per_job(backend_cpu, jobs),
                    "backend_operations_per_job": ops_per_job,
                    "pop_requests_per_job": pops_per_job,
                    "worker_peak": get(summary, "scaling", "worker_peak"),
                    "time_to_peak_seconds": None if peak_ns is None else round(peak_ns / 1e9, 2),
                    "worker_seconds": rounded(get(summary, "scaling", "worker_seconds"), 1),
                })


if __name__ == "__main__":
    main()
