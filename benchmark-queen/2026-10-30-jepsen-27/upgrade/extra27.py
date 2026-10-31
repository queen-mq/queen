#!/usr/bin/env python3
# extra27.py <cmd> <url> <outdir> [arg]: the checks 2.7.0 adds to the 2.6.0 roll.
#   kvput a|b    a: roll/k1 (nested JSON, unicode, floats, keys out of order) and roll/k2; b: roll/k3 (as k1)
#   kvget <tag>  GET roll/k1 and a getMany of k1,k2,k3 in one KV batch; the raw answers saved as kv-<tag>-*.bin
#   query        register the stream query "roll" on queue roll-src; its id saved
#   claim        POST /streams/v1/claim for a partition id that does not exist: 404 from a 2.6.0 leader,
#                200 with status "missing" from 2.7.0
#   bigpush <n>  one push of <n> messages to queue big (2.6.0 takes any number, 2.7.0 refuses > 10,000)
#   bigpop       pop queue big, batch 15000, autoAck, until a pop comes back empty: each pop's count
import json, sys, urllib.request, urllib.error
cmd, url, out = sys.argv[1], sys.argv[2], sys.argv[3]
arg = sys.argv[4] if len(sys.argv) > 4 else None

def call(method, path, body=None, timeout=60):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url + path, data=data, method=method,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()

V1 = {"b": 2, "a": [1, 2.5, -0.0, 1e21, "x é ☃ \"q\""], "z": None, "n": {"y": True, "x": {}}}
if cmd == "kvput":
    for k, v in ((("k1", V1), ("k2", "plain")) if arg == "a" else (("k3", V1),)):
        st, b = call("PUT", f"/api/v1/kv/roll/{k}", {"value": v, "forever": True})
        print(f"PUT roll/{k}: {st} {b[:120].decode(errors='replace')}")
elif cmd == "kvget":
    st, b = call("GET", "/api/v1/kv/roll/k1")
    open(f"{out}/kv-{arg}-get.bin", "wb").write(b)
    st2, b2 = call("POST", "/api/v1/kv", {"operations": [{"op": "getMany", "ns": "roll", "keys": ["k1", "k2", "k3"]}]})
    open(f"{out}/kv-{arg}-batch.bin", "wb").write(b2)
    same = json.loads(b).get("value") == V1 if st == 200 else False
    print(f"GET roll/k1: {st}, {len(b)} bytes, value equal to what was put: {same}; getMany: {st2}, {len(b2)} bytes")
elif cmd == "query":
    st, b = call("POST", "/streams/v1/queries", {"name": "roll", "source_queue": "roll-src", "config_hash": "roll-1"})
    if st == 200:
        open(f"{out}/query-id", "w").write(json.loads(b)["query_id"])
    print(f"register query: {st} {b[:160].decode(errors='replace')}")
elif cmd == "claim":
    qid = open(f"{out}/query-id").read().strip()
    st, b = call("POST", "/streams/v1/claim", {"query_id": qid, "partition_ids": ["999999"], "lease_seconds": 30})
    print(f"claim: HTTP {st} {b[:200].decode(errors='replace')}")
elif cmd == "bigpush":
    n = int(arg)
    tag = url.split("//")[1].split(":")[0]
    items = [{"queue": "big", "partition": "b", "transactionId": f"big-{tag}-{i}", "payload": {"i": i}} for i in range(n)]
    st, b = call("POST", "/api/v1/push", {"items": items}, timeout=120)
    try:
        ans = json.loads(b)
    except Exception:
        ans = None
    if isinstance(ans, list):
        c = {}
        for a in ans:
            c[a.get("status")] = c.get(a.get("status"), 0) + 1
        print(f"push of {n} via {tag}: HTTP {st}, answers {c}")
    else:
        print(f"push of {n} via {tag}: HTTP {st} {b[:200].decode(errors='replace')}")
elif cmd == "bigpop":
    counts = []
    for _ in range(10):
        st, b = call("GET", "/api/v1/pop/queue/big?batch=15000&autoAck=true&wait=false", timeout=120)
        if st == 204 or not b:
            counts.append(0); break
        if st != 200:
            print(f"pop: HTTP {st} {b[:200].decode(errors='replace')}"); break
        m = len(json.loads(b).get("messages", []))
        counts.append(m)
        if m == 0:
            break
    print(f"pops of batch 15000: {counts}, total {sum(counts)}")
