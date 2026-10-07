#!/usr/bin/env python3
"""One row per hour of the 24-hour aging lane, from the sampler's stats file.

Usage: aging24.py <stats.jsonl> > raw/aging24.csv

Each row takes the samples of that hour (one every 10 s): the median of the
workers' private memory and its largest value, the PSS of the 64 workers in
all, the application container's cgroup, and the broker's resident set and
cgroup.
"""

import collections
import csv
import json
import statistics
import sys


def mib(value):
    return round(value / 1048576, 2)


def target(sample, label):
    return next(t for t in sample["targets"] if t["label"] == label)


def main(path):
    hours = collections.defaultdict(list)
    with open(path) as stats:
        for line in stats:
            sample = json.loads(line)
            if sample.get("type") != "sample":
                continue
            hours[int(sample["elapsed_ns"] // 3_600_000_000_000)].append(sample)

    out = csv.writer(sys.stdout, lineterminator="\n")
    out.writerow([
        "hour", "samples", "workers", "worker_private_mib_median", "worker_private_mib_max",
        "workers_pss_mib_total", "app_cgroup_anon_mib", "app_cgroup_current_mib",
        "broker_rss_mib", "broker_cgroup_anon_mib", "broker_cgroup_current_mib",
    ])
    for hour, group in sorted(hours.items()):
        workers = [[p for p in (target(s, "app").get("processes") or []) if p.get("role") == "worker"]
                   for s in group]
        flat = [p for ws in workers for p in ws]
        app = [(target(s, "app").get("cgroup") or {}).get("memory") or {} for s in group]
        broker = [target(s, "backend-broker") for s in group]
        broker_mem = [(b.get("cgroup") or {}).get("memory") or {} for b in broker]
        broker_rss = [sum(p.get("rss_bytes") or 0 for p in (b.get("processes") or [])) for b in broker]
        out.writerow([
            hour, len(group), round(statistics.median([len(ws) for ws in workers])),
            mib(statistics.median([p["private_bytes"] for p in flat])),
            mib(max(p["private_bytes"] for p in flat)),
            mib(statistics.median([sum(p.get("pss_bytes") or 0 for p in ws) for ws in workers])),
            mib(statistics.median([(m.get("stat_bytes") or {}).get("anon", 0) for m in app])),
            mib(statistics.median([m.get("current_bytes") or 0 for m in app])),
            mib(statistics.median(broker_rss)),
            mib(statistics.median([(m.get("stat_bytes") or {}).get("anon", 0) for m in broker_mem])),
            mib(statistics.median([m.get("current_bytes") or 0 for m in broker_mem])),
        ])


if __name__ == "__main__":
    main(sys.argv[1])
