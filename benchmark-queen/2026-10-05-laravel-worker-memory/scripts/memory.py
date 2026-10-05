"""CSV tables from the sampler's stats.jsonl files of a memory campaign.

Usage: memory.py procs|cgroup|aging RESULTS_ROOT

procs   per lane, engine, run and process kind: medians per process and the
        summed PSS, over the idle window after the last job
cgroup  per lane, engine and run: memory.current and memory.stat over the
        same window
aging   per engine of the aging lane, each minute: worker PSS and private
        memory, and the cgroup's anonymous memory

The window is the last 15 samples before the sampler stopped: the run waits
20 s after the last job, with every worker alive and idle, then stops it.
"""
import collections
import csv
import glob
import json
import os
import statistics
import sys

MiB = 2 ** 20
WINDOW = 15
PROCESS_FIELDS = (
    "rss_bytes", "rss_anon_bytes", "rss_file_bytes", "rss_shmem_bytes",
    "pss_bytes", "pss_anon_bytes", "pss_file_bytes", "pss_shmem_bytes", "private_bytes",
)
STAT_FIELDS = ("anon", "file", "shmem", "kernel", "pagetables", "inactive_file")


def samples(path):
    with open(path) as stream:
        return [record for record in map(json.loads, stream) if record.get("type") == "sample"]


def app(sample):
    return next(target for target in sample["targets"] if target["label"] == "app")


def kinds(processes):
    """Name each process by its place in the tree: the sampler records the
    15-character process name, not the command line."""

    by_pid = {process["pid"]: process for process in processes}
    named = {}
    for process in processes:
        parent = by_pid.get(process["ppid"], {})
        if process.get("role") == "worker":
            kind = "worker"
        elif process["name"].startswith("queen-sup"):
            kind = "rust-master"
        elif process.get("role") == "orchestrator":
            if parent.get("name", "").startswith("queen-sup"):
                kind = "fork-server"
            elif parent.get("role") == "orchestrator":
                kind = "horizon-supervisor"
            else:
                kind = "php-master"
        else:
            kind = "other"
        named[process["pid"]] = kind
    return named


def runs(root):
    pattern = os.path.join(root, "*", "*", "*", "fixed", "r*", "stats.jsonl")
    for path in sorted(glob.glob(pattern)):
        parts = path.split(os.sep)
        lane, engine, run = parts[-6], parts[-4], parts[-2]
        yield lane, engine, run, path


def mib(value):
    return round(value / MiB, 1)


def procs(root, out):
    out.writerow(["lane", "engine", "run", "kind", "processes"]
                 + [f.replace("_bytes", "_mib_each") for f in PROCESS_FIELDS] + ["pss_mib_total"])
    for lane, engine, run, path in runs(root):
        window = samples(path)[-WINDOW - 1:-1]
        rows = collections.defaultdict(list)
        for sample in window:
            processes = app(sample).get("processes") or []
            named = kinds(processes)
            for process in processes:
                rows[named[process["pid"]]].append(process)
        for kind in ("worker", "fork-server", "rust-master", "php-master", "horizon-supervisor", "other"):
            group = rows.get(kind)
            if not group:
                continue
            each = [mib(statistics.median([p.get(field) or 0 for p in group])) for field in PROCESS_FIELDS]
            total = mib(sum(p.get("pss_bytes") or 0 for p in group) / len(window))
            out.writerow([lane, engine, run, kind, round(len(group) / len(window))] + each + [total])


def cgroup(root, out):
    out.writerow(["lane", "engine", "run", "workers", "current_mib"] + [f"{f}_mib" for f in STAT_FIELDS])
    for lane, engine, run, path in runs(root):
        window = samples(path)[-WINDOW - 1:-1]
        memory = [((app(s).get("cgroup") or {}).get("memory") or {}) for s in window]
        workers = statistics.median([(app(s).get("role_counts") or {}).get("worker", 0) for s in window])
        stat = [m.get("stat_bytes") or {} for m in memory]
        out.writerow([lane, engine, run, round(workers), mib(statistics.median([m.get("current_bytes") or 0 for m in memory]))]
                     + [mib(statistics.median([s.get(f, 0) for s in stat])) for f in STAT_FIELDS])


def aging(root, out):
    out.writerow(["engine", "minute", "workers", "worker_pss_mib_median", "worker_private_mib_median",
                  "worker_pss_mib_total", "cgroup_anon_mib", "cgroup_current_mib"])
    for lane, engine, run, path in runs(root):
        if lane != "aging":
            continue
        data = samples(path)
        start = data[0]["elapsed_ns"]
        minutes = collections.defaultdict(list)
        for sample in data:
            minutes[int((sample["elapsed_ns"] - start) / 60e9)].append(sample)
        for minute, group in sorted(minutes.items()):
            workers = [[p for p in (app(s).get("processes") or []) if p.get("role") == "worker"] for s in group]
            flat = [p for ws in workers for p in ws]
            if not flat:
                continue
            memory = [((app(s).get("cgroup") or {}).get("memory") or {}) for s in group]
            out.writerow([
                engine, minute, round(statistics.median([len(ws) for ws in workers])),
                mib(statistics.median([p.get("pss_bytes") or 0 for p in flat])),
                mib(statistics.median([p.get("private_bytes") or 0 for p in flat])),
                mib(statistics.median([sum(p.get("pss_bytes") or 0 for p in ws) for ws in workers])),
                mib(statistics.median([(m.get("stat_bytes") or {}).get("anon", 0) for m in memory])),
                mib(statistics.median([m.get("current_bytes") or 0 for m in memory])),
            ])


if __name__ == "__main__":
    mode, root = sys.argv[1], sys.argv[2]
    {"procs": procs, "cgroup": cgroup, "aging": aging}[mode](root, csv.writer(sys.stdout, lineterminator="\n"))
