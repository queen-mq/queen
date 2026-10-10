#!/usr/bin/env python3
"""mqgen.py <base url> <queues> <seconds> <period s> [prefix]
One message into every queue each <period> seconds, for <seconds>: pushes of 100
items, each item its own queue, spread evenly over the period. Prints pushed/errors."""
import http.client, json, sys, time
from urllib.parse import urlparse

base, nq, secs, period = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
prefix = sys.argv[5] if len(sys.argv) > 5 else "mq"
u = urlparse(base)
conn = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
pad = "x" * 200
batches = [list(range(i, min(i + 100, nq))) for i in range(0, nq, 100)]
gap = period / len(batches)
pushed = errors = rounds = 0
t0 = time.time()
nxt = t0
while time.time() - t0 < secs:
    for b in batches:
        items = [{"queue": f"{prefix}-{q:05d}", "partition": "p", "payload": {"q": q, "r": rounds, "pad": pad}} for q in b]
        try:
            conn.request("POST", "/api/v1/push", json.dumps({"items": items}), {"content-type": "application/json"})
            r = conn.getresponse(); body = r.read()
            if r.status in (200, 201):
                pushed += len(items)
            else:
                errors += 1
                if errors <= 3:
                    print("push", r.status, body[:200], flush=True)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print("push failed", e, flush=True)
            conn.close(); conn = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
        nxt += gap
        d = nxt - time.time()
        if d > 0:
            time.sleep(d)
    rounds += 1
print(f"[final] queues={nq} rounds={rounds} pushed={pushed} errors={errors} seconds={time.time()-t0:.0f}", flush=True)
