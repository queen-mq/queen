#!/usr/bin/env python3
# table.py RUNS_DIR — one line per run: bytes per message split, entry copies, latency.
import json, os, sys
root = sys.argv[1]
rows = []
for name in sorted(os.listdir(root)):
    d = os.path.join(root, name)
    try:
        b = json.load(open(os.path.join(d, "bytes.json")))
        r = json.load(open(os.path.join(d, "result.json")))
    except Exception as e:
        continue
    m = max(b["messages_in_log"], 1)
    fs = {}
    try:
        for line in open(os.path.join(d, "metrics.txt")):
            for k in ("queen_raft_log_fsync_seconds_sum", "queen_raft_log_fsync_seconds_count"):
                if line.startswith(k + " "):
                    fs[k] = float(line.split()[1])
    except Exception:
        pass
    n = fs.get("queen_raft_log_fsync_seconds_count", 0)
    r["fsync_avg_ms"] = 1000 * fs.get("queen_raft_log_fsync_seconds_sum", 0) / n if n else 0
    rows.append((name, b, r, m))
hdr = f"{'run':28s} {'MB total':>9s} {'B/msg':>7s} {'msg':>6s} {'whole':>7s} {'stub':>6s} {'zeros':>7s} {'avg entry':>9s} {'copies':>6s} {'pushed/s':>8s} {'push p50/p99 ms':>16s} {'e2e p99':>8s} {'fsync ms':>8s}"
print(hdr)
for name, b, r, m in rows:
    print(f"{name:28s} {b['total_bytes']/1e6:9.1f} {b['total_bytes']/m:7.0f} {b['message_records']/m:6.0f} "
          f"{b['entry_whole']/m:7.0f} {b['entry_stub']/m:6.0f} {b['prealloc_zeros']/m:7.0f} "
          f"{b.get('avg_whole_entry_bytes', 0):9.0f} {b.get('copies_weighted', 0):6.1f} "
          f"{r.get('pushed_msg_s', 0):8.0f} {r.get('push_p50_ms', 0):7.1f}/{r.get('push_p99_ms', 0):<8.1f} {r.get('e2e_p99_ms', 0):8.1f} {r['fsync_avg_ms']:8.1f}")
