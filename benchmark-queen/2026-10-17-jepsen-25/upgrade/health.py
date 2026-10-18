#!/usr/bin/env python3
# health.py <host>: one line from /health (build version, role, cluster version, kinds, applied, lag, status).
import json, sys, urllib.request
try:
    with urllib.request.urlopen(f"http://{sys.argv[1]}:6632/health", timeout=2) as r:
        d = json.load(r)
    ra = d.get("raft", {})
    print(d.get("version"), ra.get("role"), "cv", ra.get("clusterVersion"), "kinds", ra.get("kinds"),
          "applied", ra.get("applied"), "lag", ra.get("lag"), d.get("status"))
except Exception as e:
    print("down", type(e).__name__)
