#!/usr/bin/env python3
# dup.py <urls,comma> <set> <new|dup> <outdir>: 20 pushes of 100 messages to queue "dup" (partitions d0..d9),
# transactionIds <set>-<k>-<j>. "new": every item must answer queued (offsets kept in <outdir>/dup-<set>.json);
# "dup": every item must answer duplicate, naming the partition and offset its first push got.
import json, sys, time, urllib.request
urls, S, mode, out = sys.argv[1].split(","), sys.argv[2], sys.argv[3], sys.argv[4]
B, N, P = 20, 100, 10
path = f"{out}/dup-{S}.json"
seen = {} if mode == "new" else json.load(open(path))
stat, mism, retries, lost = {}, 0, 0, 0
for k in range(B):
    items = [{"queue": "dup", "partition": f"d{k % P}", "transactionId": f"{S}-{k}-{j}",
              "payload": {"set": S, "k": k, "j": j}} for j in range(N)]
    body = json.dumps({"items": items}).encode()
    ans = None
    for attempt in range(4):
        u = urls[(k + attempt) % len(urls)]
        try:
            req = urllib.request.Request(u + "/api/v1/push", data=body, method="POST",
                                         headers={"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                ans = json.load(r)
            break
        except Exception:
            retries += 1
            time.sleep(1)
    if ans is None:
        lost += 1
        continue
    for it, a in zip(items, ans):
        st = a.get("status")
        stat[st] = stat.get(st, 0) + 1
        got = [it["partition"], a.get("offset")]
        if mode == "new":
            seen[it["transactionId"]] = got
        elif seen.get(it["transactionId"]) != got:
            mism += 1
if mode == "new":
    json.dump(seen, open(path, "w"))
want = "queued" if mode == "new" else "duplicate"
ok = stat.get(want, 0) == B * N and mism == 0 and lost == 0
print(f"set {S} {mode}: {B * N} items, answers {stat}, offset mismatches {mism}, retries {retries}, pushes lost {lost} -> {'OK' if ok else 'FAIL'}")
