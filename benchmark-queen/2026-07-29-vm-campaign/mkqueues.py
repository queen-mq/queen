#!/usr/bin/env python3
"""mkqueues.py — create N idle queues through the proxy, exactly as a tenant would.

S4 (idle floor) needs a cell that holds many queues and serves ZERO traffic, in
two shapes at the same total: many tenants x few queues vs few tenants x many
queues. The loader can only drive one queue per tenant, so this creates them
directly over the same public API (`POST /api/v1/configure`, Bearer key + Host
= cluster slug) instead of writing rows behind the broker's back.

  mkqueues.py --tenants-file F --tenants T --per-tenant Q [--prefix idle]
              [--url http://127.0.0.1:6711] [--concurrency 32]
              [--materialise] [--drain]

--materialise pushes ONE message into each queue so the partition really exists
(a queue whose partition was never touched may cost nothing to poll); --drain
then pops it back out with autoAck so the steady state is "queue exists, has a
partition, has no pending work" — the honest idle shape.

Prints a JSON summary; exit 1 if any queue failed.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def call(url, host, key, path, body=None, method=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url + path, data=data,
                                 method=method or ("POST" if data else "GET"))
    req.add_header("Authorization", "Bearer " + key)
    req.add_header("Content-Type", "application/json")
    req.add_header("Host", host)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenants-file", required=True)
    ap.add_argument("--tenants", type=int, default=0, help="0 = all in file")
    ap.add_argument("--per-tenant", type=int, required=True)
    ap.add_argument("--prefix", default="idle")
    ap.add_argument("--url", default="http://127.0.0.1:6711")
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--materialise", action="store_true")
    ap.add_argument("--drain", action="store_true")
    ap.add_argument("--lease-time", type=int, default=30)
    ap.add_argument("--group", default="workers")
    a = ap.parse_args()

    creds = json.load(open(a.tenants_file))["tenants"]
    if a.tenants:
        creds = creds[:a.tenants]
    jobs = [(c, "%s-%04d" % (a.prefix, i))
            for c in creds for i in range(a.per_tenant)]

    errs, ok = [], [0]
    t0 = time.time()

    def one(job):
        c, q = job
        host, key = c["clusterSlug"], c["apiKey"]
        try:
            call(a.url, host, key, "/api/v1/configure", {
                "queue": q,
                "options": {"retentionEnabled": True,
                            "completedRetentionSeconds": 300,
                            "retentionSeconds": 3600,
                            "leaseTime": a.lease_time},
            })
            if a.materialise:
                call(a.url, host, key, "/api/v1/push",
                     {"items": [{"queue": q, "payload": {"idle": 1}}]})
            if a.drain:
                call(a.url, host, key,
                     "/api/v1/pop/queue/%s?batch=10&wait=false&timeout=0"
                     "&autoAck=true&consumerGroup=%s" % (q, a.group),
                     method="GET")
            ok[0] += 1
        except urllib.error.HTTPError as e:
            errs.append("%s/%s HTTP %s %s" % (host, q, e.code,
                                              e.read()[:160].decode("utf8", "replace")))
        except Exception as e:  # noqa: BLE001
            errs.append("%s/%s %s" % (host, q, e))

    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        list(ex.map(one, jobs))

    out = {"tenants": len(creds), "per_tenant": a.per_tenant,
           "queues_total": len(jobs), "created": ok[0],
           "errors": len(errs), "first_errors": errs[:5],
           "seconds": round(time.time() - t0, 1),
           "materialise": a.materialise, "drain": a.drain}
    print(json.dumps(out))
    sys.exit(1 if errs else 0)


if __name__ == "__main__":
    main()
