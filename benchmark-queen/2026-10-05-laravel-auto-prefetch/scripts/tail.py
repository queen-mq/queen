"""Where the slow jobs of one run are: clusters of jobs over 100 ms end to end,
the workers that ran them, and how many jobs other workers started meanwhile.

Usage: tail.py RUN_DIRECTORY   (a run.sh run directory, with events/)
"""
import json, glob, sys, collections
d = sys.argv[1]
jobs = []
for f in glob.glob(f"{d}/events/worker-*.jsonl"):
    for line in open(f):
        r = json.loads(line)
        if r.get("run_id","").endswith("warmup"): continue
        jobs.append(r)
jobs.sort(key=lambda r: r["enqueued_at_ns"])
t0 = jobs[0]["enqueued_at_ns"]
slow = [r for r in jobs if r["end_to_end_ns"] > 100e6]
print("jobs", len(jobs), "slow>100ms", len(slow), "max ms", round(max(r["end_to_end_ns"] for r in jobs)/1e6,1))
if not slow: sys.exit()
# clusters of slow jobs by enqueue time
clusters = []
for r in slow:
    t = (r["enqueued_at_ns"] - t0) / 1e9
    if clusters and t - clusters[-1][-1][0] < 1.0: clusters[-1].append((t, r))
    else: clusters.append([(t, r)])
for c in clusters[:5]:
    ts = [t for t, _ in c]; rs = [r for _, r in c]
    workers = collections.Counter(r["worker_pid"] for r in rs)
    print(f"cluster at {ts[0]:.2f}-{ts[-1]:.2f}s: {len(c)} slow jobs, e2e max {max(r['end_to_end_ns'] for r in rs)/1e6:.0f} ms, queue wait max {max(r['queue_latency_ns'] for r in rs)/1e6:.0f} ms, workers {dict(workers)}")
    # how busy were all workers in that window: jobs started per worker
    lo = min(r["enqueued_at_ns"] for r in rs); hi = max(r["completed_at_ns"] for r in rs)
    window = [r for r in jobs if lo <= r["work_started_at_ns"] <= hi]
    per = collections.Counter(r["worker_pid"] for r in window)
    print(f"   jobs started in that window by worker: {dict(sorted(per.items()))}")
    # gap: was any job started at all during the worst wait?
    worst = max(rs, key=lambda r: r["queue_latency_ns"])
    gap_lo, gap_hi = worst["enqueued_at_ns"], worst["work_started_at_ns"]
    started = [r for r in jobs if gap_lo < r["work_started_at_ns"] < gap_hi]
    print(f"   during the worst job's {((gap_hi-gap_lo)/1e6):.0f} ms wait, {len(started)} other jobs started on {len(set(r['worker_pid'] for r in started))} workers")
