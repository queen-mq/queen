#!/usr/bin/env python3
# summary.py <runs/name>: OMB's result.json in one line, plus the samplers' peaks.
import json, sys, os, re
d = sys.argv[1]
r = json.load(open(os.path.join(d, 'result.json')))
avg = lambda xs: sum(xs) / len(xs) if xs else 0
print("%s: pub %.0f msg/s, cons %.0f msg/s, pub err %.1f/s, backlog end %d; pub p50/p99/p99.9 %.1f/%.1f/%.1f ms; e2e p50/p99/p99.9/max %.1f/%.1f/%.1f/%.1f ms" % (
    os.path.basename(d), avg(r['publishRate']), avg(r['consumeRate']), avg(r['publishErrorRate']), r['backlog'][-1] if r['backlog'] else -1,
    r['aggregatedPublishLatency50pct'], r['aggregatedPublishLatency99pct'], r['aggregatedPublishLatency999pct'],
    r['aggregatedEndToEndLatency50pct'], r['aggregatedEndToEndLatency99pct'], r['aggregatedEndToEndLatency999pct'], r['aggregatedEndToEndLatencyMax']))
def peak(path, key):
    best = None
    for line in open(path):
        m = re.search(key + r' (\d+)', line)
        if m: best = max(best or 0, int(m.group(1)))
    return best
b = os.path.join(d, 'broker.samples'); w = os.path.join(d, 'workers.samples')
if os.path.exists(b):
    print("  broker peak: rss %s MB, threads %s, fds %s, data %s MB" % (peak(b, 'rss_mb'), peak(b, 'threads'), peak(b, 'fds'), peak(b, 'data_mb')))
if os.path.exists(w):
    ports = {}
    for line in open(w):
        m = re.search(r'port (\d+) rss_mb (\d+) threads (\d+)', line)
        if m:
            p = ports.setdefault(m.group(1), [0, 0]); p[0] = max(p[0], int(m.group(2))); p[1] = max(p[1], int(m.group(3)))
    print("  workers peak: " + ", ".join("%s rss %d MB threads %d" % (k, v[0], v[1]) for k, v in sorted(ports.items())) + "; tcp to broker peak %s" % peak(w, 'tcp_to_broker'))
