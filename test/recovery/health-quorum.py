#!/usr/bin/env python3
"""What /health answers through a lost majority, its return, a leader failover and a voter that
comes back empty (F8, F2 in FINDINGS.md).

Three nodes of the build under test as plain processes on this machine's loopback, on ports that
are no default of anything (HTTP 46641-46643, raft 47411-47413). No Docker and no image: it takes
the binary, so it runs against a debug build in a minute.

    python3 health-quorum.py                      # ../../server/target/debug/queen
    QUEEN_BIN=/path/to/queen python3 health-quorum.py

It refuses to start if something already answers on its ports, stops every node it started and
removes its data directory. The lines that begin with RESULT are what it measured; SAMPLE lines
are /health bodies as the nodes sent them.

A debug build is not the place to judge F1 (a voter whose log went backwards): the wiped voter of
the last step catches up on a debug build, and F1 was reproduced on release images.
"""
import json, os, shutil, signal, subprocess, sys, tempfile, threading, time, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.environ.get("QUEEN_BIN", os.path.join(HERE, "..", "..", "server", "target", "debug", "queen"))
ROOT = tempfile.mkdtemp(prefix="queen-health-quorum-")
HTTP = {1: 46641, 2: 46642, 3: 46643}
RAFT = {1: 47411, 2: 47412, 3: 47413}
PEERS = ",".join(f"{i}=127.0.0.1:{RAFT[i]}/127.0.0.1:{HTTP[i]}" for i in (1, 2, 3))
procs = {}
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:7.2f}s]", *a, flush=True)


def start(i):
    d = os.path.join(ROOT, "data", f"n{i}")
    os.makedirs(d, exist_ok=True)
    env = dict(os.environ)
    env.update(
        QUEEN_RAFT_DIR=d, PORT=str(HTTP[i]), QUEEN_BIND_ADDR="127.0.0.1", QUEEN_SERVER_ID=f"hq-n{i}",
        LOG_LEVEL="info", QUEEN_RAFT_REPLICATOR="openraft", QUEEN_RAFT_NODE_ID=str(i),
        QUEEN_RAFT_PEERS=PEERS, QUEEN_RAFT_LISTEN=f"127.0.0.1:{RAFT[i]}",
        QUEEN_RAFT_TOKEN="health-quorum-rehearsal", QUEEN_RAFT_MAP_BYTES=str(256 << 20),
    )
    out = open(os.path.join(ROOT, "logs", f"n{i}.log"), "ab")
    procs[i] = subprocess.Popen([BIN], env=env, stdout=out, stderr=out, stdin=subprocess.DEVNULL)


def kill(i, sig=signal.SIGKILL):
    p = procs.pop(i, None)
    if p:
        p.send_signal(sig)
        p.wait(timeout=20)


def health(i, timeout=1.0):
    """(status code or None, body dict or None)"""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{HTTP[i]}/health", timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception:
        return None, None


def raw(i):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{HTTP[i]}/health", timeout=1.0) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, repr(e)


def post(i, path, body, timeout=5.0, method="POST"):
    req = urllib.request.Request(f"http://127.0.0.1:{HTTP[i]}{path}", data=json.dumps(body).encode(),
                                 headers={"content-type": "application/json"}, method=method)
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, time.time() - t
    except urllib.error.HTTPError as e:
        return e.code, time.time() - t
    except Exception:
        return 0, time.time() - t


def wait_all(nodes, want=200, limit=60):
    end = time.time() + limit
    while time.time() < end:
        if all(health(i)[0] == want for i in nodes):
            return True
        time.sleep(0.2)
    return False


def leader():
    for i in list(procs):
        c, b = health(i)
        if b and b["raft"]["role"] == "leader":
            return i
    return None


def push(i, n, tag):
    ok = 0
    for k in range(n):
        c, _ = post(i, "/api/v1/push", {"items": [{"queue": "health-quorum", "partition": f"p{k % 8}",
                                                    "transactionId": f"{tag}-{k}", "payload": {"k": k}}]})
        ok += c in (200, 201)
    return ok


def main():
    for i in (1, 2, 3):
        c, _ = health(i, 0.3)
        if c is not None:
            sys.exit(f"something already answers on port {HTTP[i]}: not mine, stopping")
    if not os.path.exists(BIN):
        sys.exit(f"no broker binary at {BIN}: build one, or set QUEEN_BIN")
    os.makedirs(os.path.join(ROOT, "logs"))
    try:
        for i in (1, 2, 3):
            start(i)
        assert wait_all((1, 2, 3)), "the cluster did not form"
        L = leader()
        F = [i for i in (1, 2, 3) if i != L]
        log(f"formed: leader n{L}, followers n{F[0]} n{F[1]}")
        log("pushed", push(L, 40, "a"), "of 40")
        time.sleep(1.5)
        print("SAMPLE follower healthy:", raw(F[0])[1])
        print("SAMPLE leader healthy:  ", raw(L)[1])

        # 1. the majority is lost: both followers killed at once
        log(f"== kill -9 n{F[0]} and n{F[1]}")
        t = time.time()
        kill(F[0]); kill(F[1])
        first503 = None; at3 = None; at8 = None; probe = None
        while time.time() - t < 12:
            c, b = health(L)
            dt = time.time() - t
            if c == 503 and first503 is None:
                first503 = dt
                log(f"leader n{L} first 503 at +{dt:.2f}s quorumAckMs={b['raft'].get('quorumAckMs') if b else None}")
            if at3 is None and dt >= 3.0:
                at3 = raw(L)
                box = {}
                th = threading.Thread(target=lambda: box.update(r=post(L, f"/api/v1/kv/probe/n{L}", {"value": {"at": "rehearsal"}, "ttlSeconds": 120}, timeout=5.0, method="PUT")))
                th.start()
            if at8 is None and time.time() - t >= 8.4:
                at8 = raw(L)
            time.sleep(0.05)
        print("SAMPLE leader +3s:", at3)
        th.join(); probe = box["r"]
        print(f"WRITE PROBE through the leader at +3s: {probe[0]:03d} in {probe[1]:.6f}s")
        print("SAMPLE leader +8s:", at8)
        log(f"RESULT lost majority: 503 from +{first503:.2f}s" if first503 else "RESULT lost majority: NEVER 503 in 12 s")

        # 2. one node returns
        log(f"== start n{F[0]} again")
        t = time.time(); start(F[0])
        back = None
        while time.time() - t < 30:
            if health(L)[0] == 200:
                back = time.time() - t; break
            time.sleep(0.1)
        log(f"RESULT majority back: leader 200 again after {back:.2f}s" if back else "RESULT: leader not 200 within 30 s")
        start(F[1]); assert wait_all((1, 2, 3)), "not all healthy after the return"
        log("all three healthy again; leader now n%s" % leader())

        # 3. a plain leader failover
        L = leader(); rest = [i for i in (1, 2, 3) if i != L]
        log(f"== kill -9 the leader n{L}")
        t = time.time(); kill(L)
        bad = []; elected = None
        while time.time() - t < 12:
            for i in rest:
                c, b = health(i, 0.5)
                if c != 200:
                    r = b["raft"] if b else {}
                    bad.append((round(time.time() - t, 2), f"n{i}", c, "leader=%s" % r.get("leader"), "quorumAckMs=%s" % r.get("quorumAckMs"), r.get("role")))
                if elected is None and b and b["raft"]["role"] == "leader":
                    elected = time.time() - t
            time.sleep(0.1)
        log(f"RESULT failover: new leader after {elected:.2f}s; answers that were not 200 on the two survivors: {len(bad)} {bad[:6]}")
        start(L); assert wait_all((1, 2, 3)), "not all healthy after the failover"

        # 4. a voter that comes back empty under its id, with the cluster more than 1000 entries ahead
        L = leader(); V = [i for i in (1, 2, 3) if i != L][0]
        n = push(L, 1300, "b")
        cL, bL = health(L)
        log(f"pushed {n} of 1300; leader commit {bL['raft']['commit']}")
        log(f"== stop n{V}, wipe its disk, start it under its id")
        kill(V, signal.SIGTERM)
        shutil.rmtree(os.path.join(ROOT, "data", f"n{V}"))
        t = time.time(); start(V)
        seen = []
        while time.time() - t < 15:
            c, b = health(V, 0.5)
            if c is not None:
                r = b["raft"] if b else {}
                seen.append((round(time.time() - t, 2), c, r.get("role"), r.get("applied"), r.get("lag"), r.get("quorumAckMs")))
            time.sleep(0.25)
        turns = [seen[k] for k in range(len(seen)) if k == 0 or seen[k][1] != seen[k-1][1] or seen[k][2] != seen[k-1][2]]
        log("wiped voter, each change of answer:", turns)
        codes = sorted(set(s[1] for s in seen))
        log(f"RESULT wiped voter: answers {len(seen)}, codes seen {codes}, first {seen[0] if seen else None}, last {seen[-1] if seen else None}")
        print("SAMPLE wiped voter:", raw(V))
    finally:
        for i in list(procs):
            try:
                kill(i, signal.SIGTERM)
            except Exception:
                try:
                    procs[i].kill()
                except Exception:
                    pass
        shutil.rmtree(ROOT, ignore_errors=True)
        log("stopped every node and removed", ROOT)


main()
