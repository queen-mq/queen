#!/usr/bin/env python3
"""wl.py — write a known data set to a Queen cluster, then prove what survived.

Every write is recorded in a ledger (state dir, JSON lines) with its outcome:
  ok       the broker answered that it stored it (push status queued/duplicate,
           KV put 200, ack success) — it MUST survive any recovery that is safe
  unknown  no clear answer (timeout, connection reset, 5xx): may or may not exist

  wl.py write   --tag A [--queues 3 --partitions 4 --msgs 300 --kv 40]
  wl.py consume --group g1 [--max 100]     pop+ack per queue, ledger the acks
  wl.py load    --seconds 60 [--rate 40]   steady pushes (+KV) while you break things
  wl.py verify  [--allow-loss]             every ok write present exactly once,
                                            KV values, group cursors not behind acks
Nodes: --nodes 0,1,2 (ports 16630+i), default: every node in .qr_n.
State: --state DIR (default ./state).
"""
import argparse, hashlib, json, os, random, sys, time, uuid
import concurrent.futures as cf
import requests

HERE = os.path.dirname(os.path.abspath(__file__))


def nodes_default():
    try:
        n = int(open(os.path.join(HERE, ".qr_n")).read().strip())
    except Exception:
        n = 3
    return list(range(n))


class Cluster:
    def __init__(self, nodes, timeout=10):
        self.urls = [f"http://localhost:{16630 + i}" for i in nodes]
        self.timeout = timeout
        self.s = requests.Session()

    def call(self, method, path, body=None, tries=6, params=None, ok_status=(200, 201, 204)):
        """One request, retried on other nodes while the answer is unclear.
        Returns (status, json|None, clear) — clear False when every try was
        a timeout/connection error/5xx (the outcome is unknown)."""
        last = (None, None, False)
        urls = self.urls[:]
        random.shuffle(urls)
        for t in range(tries):
            u = urls[t % len(urls)]
            try:
                r = self.s.request(method, u + path, json=body, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                last = (None, {"error": str(e)[:200]}, False)
                time.sleep(0.3 * (t + 1))
                continue
            try:
                j = r.json() if r.content else None
            except ValueError:
                j = {"raw": r.text[:200]}
            if r.status_code >= 500 or r.status_code == 429:
                last = (r.status_code, j, False)
                time.sleep(0.3 * (t + 1))
                continue
            return (r.status_code, j, True)
        return last


def sha(s):
    return hashlib.sha256(s.encode()).hexdigest()[:16]


class Ledger:
    def __init__(self, state):
        os.makedirs(state, exist_ok=True)
        self.path = os.path.join(state, "ledger.jsonl")
        self.f = open(self.path, "a")

    def add(self, rec):
        rec["at"] = time.time()
        self.f.write(json.dumps(rec) + "\n")
        self.f.flush()

    @staticmethod
    def read(state):
        p = os.path.join(state, "ledger.jsonl")
        if not os.path.exists(p):
            return []
        return [json.loads(l) for l in open(p) if l.strip()]


PAD = 0


def push_one(c, led, q, p, tx, extra=None):
    data = {"tx": tx, "q": q, "p": p, "n": random.randint(0, 10**9)}
    if PAD:
        import base64
        data["pad"] = base64.b64encode(os.urandom(PAD * 3 // 4)).decode()
    if extra:
        data.update(extra)
    data["sha"] = sha(f"{tx}|{q}|{p}|{data['n']}")
    body = {"items": [{"queue": q, "partition": p, "transactionId": tx, "payload": data}]}
    st, j, clear = c.call("POST", "/api/v1/push", body)
    item = j[0] if isinstance(j, list) and j else {}
    status = item.get("status") if isinstance(item, dict) else None
    if clear and status in ("queued", "duplicate"):
        led.add({"t": "push", "q": q, "p": p, "tx": tx, "sha": data["sha"], "n": data["n"],
                 "ok": "ok", "status": status, "offset": item.get("offset")})
        return "ok"
    led.add({"t": "push", "q": q, "p": p, "tx": tx, "sha": data["sha"], "n": data["n"],
             "ok": "unknown", "http": st, "answer": j if not isinstance(j, list) else item})
    return "unknown"


def kv_put(c, led, ns, key, val):
    st, j, clear = c.call("PUT", f"/api/v1/kv/{ns}/{key}", {"value": val, "forever": True})
    ok = "ok" if clear and st == 200 else ("rejected" if clear else "unknown")
    led.add({"t": "kv", "ns": ns, "key": key, "val": val, "ok": ok, "http": st,
             "answer": j if ok != "ok" else None})
    return ok


def cmd_write(a):
    c = Cluster(a.nodes)
    led = Ledger(a.state)
    jobs = []
    for qi in range(a.queues):
        q = f"rec.{a.tag.lower()}{qi}"
        for m in range(a.msgs):
            p = f"p{m % a.partitions}"
            jobs.append((q, p, f"{a.tag}-{qi}-{m}"))
    t0 = time.time()
    res = {"ok": 0, "unknown": 0, "rejected": 0}
    with cf.ThreadPoolExecutor(8) as ex:
        for r in ex.map(lambda j: push_one(c, led, *j), jobs):
            res[r] += 1
    for k in range(a.kv):
        r = kv_put(c, led, "rec", f"{a.tag}-k{k}", {"tag": a.tag, "k": k, "v": random.randint(0, 10**9)})
        res[r] += 1
    print(json.dumps({"write": a.tag, "pushes": len(jobs), "kv": a.kv, "ok": res["ok"],
                      "unknown": res["unknown"], "secs": round(time.time() - t0, 1)}))


def cmd_load(a):
    c = Cluster(a.nodes, timeout=5)
    led = Ledger(a.state)
    end = time.time() + a.seconds
    i = 0
    res = {"ok": 0, "unknown": 0, "rejected": 0}
    tag = a.tag or f"L{int(time.time())}"
    gap = 1.0 / a.rate
    nxt = time.time()
    worst = 0.0
    while time.time() < end:
        t = time.time()
        if i % 10 == 9:
            r = kv_put(c, led, "rec", f"{tag}-k{i % 50}", {"tag": tag, "i": i})
        else:
            r = push_one(c, led, f"rec.load", f"p{i % 8}", f"{tag}-{i}")
        res[r] += 1
        worst = max(worst, time.time() - t)
        i += 1
        nxt += gap
        d = nxt - time.time()
        if d > 0:
            time.sleep(d)
        if i % (a.rate * 5) == 0:
            print(json.dumps({"load": tag, "sent": i, **res, "worst_s": round(worst, 2)}), flush=True)
    print(json.dumps({"load": tag, "sent": i, **res, "worst_s": round(worst, 2), "done": True}), flush=True)


def queues_of(ledger):
    return sorted({r["q"] for r in ledger if r["t"] == "push"})


def read_all(c, q):
    """Every retained message of q, through a fresh group from the oldest one."""
    g = f"verify-{uuid.uuid4().hex[:8]}"
    out = []
    empties = 0
    while empties < 2:
        st, j, clear = c.call("GET", f"/api/v1/pop/queue/{q}", params={
            "consumerGroup": g, "subscriptionMode": "all", "autoAck": "true",
            "batch": 1000, "partitions": 64, "wait": "false"})
        if not clear:
            raise SystemExit(f"read {q}: no clear answer: {st} {j}")
        msgs = (j or {}).get("messages") or [] if st == 200 else []
        if not msgs:
            empties += 1
            time.sleep(0.2)
            continue
        empties = 0
        out.extend(msgs)
    return out


def cmd_consume(a):
    c = Cluster(a.nodes)
    led = Ledger(a.state)
    ledger = Ledger.read(a.state)
    total = 0
    for q in queues_of(ledger):
        got = 0
        while got < a.max:
            st, j, clear = c.call("GET", f"/api/v1/pop/queue/{q}", params={
                "consumerGroup": a.group, "subscriptionMode": "all", "batch": min(50, a.max - got),
                "partitions": 1, "wait": "false"})
            if not clear or st != 200 or not (j or {}).get("messages"):
                break
            msgs = j["messages"]
            acks = [{"transactionId": m["transactionId"], "partitionId": m["partitionId"],
                     "leaseId": j["leaseId"], "status": "completed"} for m in msgs]
            st2, j2, clear2 = c.call("POST", "/api/v1/ack/batch",
                                    {"consumerGroup": a.group, "acknowledgments": acks})
            for m, r in zip(msgs, j2 if isinstance(j2, list) else [None] * len(msgs)):
                ok = "ok" if clear2 and isinstance(r, dict) and r.get("success") else "unknown"
                d = m.get("data") or {}
                led.add({"t": "ack", "q": q, "p": d.get("p"), "pid": m["partitionId"], "group": a.group,
                         "offset": m.get("offset"), "tx": m["transactionId"], "ok": ok})
            got += len(msgs)
        total += got
    print(json.dumps({"consume": a.group, "acked": total}))


def cmd_verify(a):
    c = Cluster(a.nodes)
    ledger = Ledger.read(a.state)
    report = {"queues": 0, "pushes_ok": 0, "pushes_unknown": 0, "missing": [], "dup": [],
              "bad_payload": [], "unknown_present": 0, "kv_ok": 0, "kv_bad": [],
              "groups_ok": 0, "groups_behind": []}
    want = {}
    unknown = set()
    for r in ledger:
        if r["t"] == "push":
            if r["ok"] == "ok":
                want[r["tx"]] = r
            else:
                unknown.add(r["tx"])
    report["pushes_ok"] = len(want)
    report["pushes_unknown"] = len(unknown - set(want))
    seen = {}
    for q in queues_of(ledger):
        report["queues"] += 1
        for m in read_all(c, q):
            tx = m["transactionId"]
            seen.setdefault(tx, []).append(m)
    for tx, r in want.items():
        got = seen.get(tx)
        if not got:
            report["missing"].append(tx)
            continue
        if len(got) > 1:
            report["dup"].append(tx)
        d = got[0].get("data") or {}
        if d.get("sha") != r["sha"] and r.get("status") != "duplicate":
            report["bad_payload"].append(tx)
    report["unknown_present"] = len([t for t in unknown - set(want) if t in seen])
    for tx, ms in seen.items():
        if len(ms) > 1 and tx not in report["dup"]:
            report["dup"].append(tx)
    # KV: the last ok put of each key, unless an unknown put came after it.
    last = {}
    for r in ledger:
        if r["t"] == "kv":
            last[(r["ns"], r["key"])] = r if r["ok"] == "ok" else dict(r, ok="unknown")
    for (ns, key), r in last.items():
        st, j, clear = c.call("GET", f"/api/v1/kv/{ns}/{key}")
        if r["ok"] != "ok":
            continue
        val = (j or {}).get("value") if isinstance(j, dict) else None
        if st == 200 and val == r["val"]:
            report["kv_ok"] += 1
        else:
            report["kv_bad"].append({"key": f"{ns}/{key}", "http": st, "want": r["val"], "got": j})
    # Group cursors: never behind an ok ack.
    acked = {}
    for r in ledger:
        if r["t"] == "ack" and r["ok"] == "ok" and r.get("offset") is not None:
            k = (r["group"], r["q"], r["p"])
            acked[k] = max(acked.get(k, -1), int(r["offset"]))
    by_group = {}
    for (g, q, p), off in acked.items():
        by_group.setdefault(g, []).append((q, p, off))
    for g, items in by_group.items():
        st, j, clear = c.call("POST", "/api/v1/consumer-groups/positions",
                              {"consumerGroup": g, "entries": [{"queue": q, "partition": p} for q, p, _ in items]})
        ents = (j or {}).get("entries") or []
        for (q, p, off), e in zip(items, ents):
            nxt = e.get("offset")
            if nxt is not None and int(nxt) > off:
                report["groups_ok"] += 1
            else:
                report["groups_behind"].append({"group": g, "q": q, "p": p, "acked": off, "next": nxt})
    bad = report["missing"] or report["dup"] or report["bad_payload"] or report["kv_bad"] or report["groups_behind"]
    summary = {k: (len(v) if isinstance(v, list) else v) for k, v in report.items()}
    summary["verdict"] = "FAIL" if bad else "PASS"
    print(json.dumps(summary))
    if bad:
        detail = {k: v[:10] for k, v in report.items() if isinstance(v, list) and v}
        print(json.dumps(detail, default=str)[:4000])
        if not a.allow_loss:
            sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["write", "consume", "load", "verify"])
    ap.add_argument("--nodes", default=None)
    ap.add_argument("--state", default=os.path.join(HERE, "state"))
    ap.add_argument("--tag", default="A")
    ap.add_argument("--queues", type=int, default=3)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--msgs", type=int, default=300)
    ap.add_argument("--kv", type=int, default=40)
    ap.add_argument("--group", default="g1")
    ap.add_argument("--max", type=int, default=100)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--rate", type=int, default=40)
    ap.add_argument("--allow-loss", action="store_true")
    ap.add_argument("--pad", type=int, default=0, help="payload padding bytes per message")
    a = ap.parse_args()
    global PAD
    PAD = a.pad
    a.nodes = [int(x) for x in a.nodes.split(",")] if a.nodes else nodes_default()
    {"write": cmd_write, "consume": cmd_consume, "load": cmd_load, "verify": cmd_verify}[a.cmd](a)


if __name__ == "__main__":
    main()
