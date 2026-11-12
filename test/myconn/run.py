#!/usr/bin/env python3
"""End-to-end suite of the MySQL source running IN-PROCESS in the broker:
real `queen` processes (one node, or a three-node raft cluster), a real
MySQL 8.0+ (in Docker), real kill -9.

  python3 test/myconn/run.py              # every scenario (a, b)
  python3 test/myconn/run.py b            # one of them
  python3 test/myconn/run.py --seed 42    # replay a run's randomness

The MySQL server: `--mysql-container` (default queen-mysql84-test), reached
by the broker at `--mysql-host`:`--mysql-port` (127.0.0.1:53306) as root
(`--mysql-password`). It needs gtid_mode=ON, enforce_gtid_consistency=ON and
binlog_row_metadata=FULL, e.g.

  docker run -d --name queen-mysql84-test -p 127.0.0.1:53306:3306 \\
    -e MYSQL_ROOT_PASSWORD=rootpw mysql:8.4 --gtid-mode=ON \\
    --enforce-gtid-consistency=ON --binlog-row-metadata=FULL --server-id=1

The oracle of every scenario: the queue, replayed per partition in offset
order (`c`/`r`/`u` put `after`, `d` removes the key), is the table; no two
messages share a transaction id; no write's token is in two stream events.

Exit code: 0 passed, 1 a scenario failed, 2 the suite could not run.
Standard library only; the broker side is test/pgconn/queen.py.
"""

import argparse
import json
import os
import random
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "test", "pgconn"))
sys.dont_write_bytecode = True

from queen import Broker, Failed, QueueReader, find_lease, make_cluster, phase_of, status_of, wait_leader  # noqa: E402


class MySQL:
    """`mysql` inside the server's container: statements on stdin."""

    def __init__(self, args):
        self.container = args.mysql_container
        self.password = args.mysql_password
        self.host = args.mysql_host
        self.port = args.mysql_port

    def cmd(self, db=None, force=False):
        c = ["docker", "exec", "-i", self.container, "mysql", "-uroot", f"-p{self.password}", "-N", "-B"]
        if force:
            c.append("--force")
        if db:
            c.append(db)
        return c

    def run(self, sql, db=None):
        p = subprocess.run(self.cmd(db), input=sql.encode(), capture_output=True, timeout=300)
        if p.returncode != 0:
            raise Failed(f"mysql: {p.stderr.decode(errors='replace')[-500:]}")
        return [line.split("\t") for line in p.stdout.decode().splitlines()]


class Writer(threading.Thread):
    """Random transactions of one to four writes on `t`, each write a unique
    token, through one long-lived client."""

    def __init__(self, my, db, rng, ids):
        super().__init__(daemon=True)
        self.p = subprocess.Popen(my.cmd(db, force=True), stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.rng, self.ids = rng, ids
        self.stop_flag = threading.Event()
        self.writes = 0

    def run(self):
        n = 0
        while not self.stop_flag.is_set():
            k = self.rng.randint(1, 4)
            sql = ["START TRANSACTION;"]
            for _ in range(k):
                n += 1
                tok = f"w{n}"
                i = self.rng.randint(1, self.ids)
                r = self.rng.random()
                if r < 0.7:
                    sql.append(f"UPDATE t SET v = v + 1, s = '{tok}' WHERE id = {i};")
                elif r < 0.9:
                    sql.append(f"INSERT INTO t VALUES ({i}, 0, '{tok}') ON DUPLICATE KEY UPDATE v = v + 1, s = '{tok}';")
                else:
                    sql.append(f"DELETE FROM t WHERE id = {i};")
            sql.append("COMMIT;\n")
            self.p.stdin.write(" ".join(sql).encode())
            self.p.stdin.flush()
            self.writes += k
            time.sleep(0.002)

    def finish(self):
        self.stop_flag.set()
        self.join(10)
        self.p.stdin.close()
        self.p.wait(60)


class Harness:
    def __init__(self, args):
        self.args = args
        self.bin = args.bin
        self.work = args.work
        self.rust_log = args.rust_log
        self.live = set()
        self.encryption_key = secrets.token_hex(32)
        self.lease_ttl_ms = 3000
        self.my = MySQL(args)
        os.makedirs(os.path.join(self.work, "logs"), exist_ok=True)
        os.makedirs(os.path.join(self.work, "data"), exist_ok=True)

    def say(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    def shutdown(self):
        for b in list(self.live):
            try:
                if b.alive():
                    b.kill9()
            except Exception:
                pass


def doc(h, db, queue, chunk=200):
    return {
        "kind": "source",
        "engine": "mysql",
        "connection": {
            "host": h.my.host,
            "port": h.my.port,
            "database": db,
            "user": "root",
            "password": h.my.password,
            "sslMode": "require",
        },
        "source": {"tables": [{"table": "t", "queue": queue}], "snapshotChunkRows": chunk, "heartbeatSeconds": 1},
    }


def wait(what, timeout, f, every=0.25):
    deadline = time.time() + timeout
    while True:
        v = f()
        if v:
            return v
        if time.time() > deadline:
            raise Failed(f"timed out ({timeout}s) waiting for {what}")
        time.sleep(every)


def owner_of(nodes, name):
    for b in nodes:
        if not b.alive():
            continue
        e = b.connector(name)
        if e and phase_of(e) in ("streaming", "snapshot"):
            return b
    return None


def table(h, db):
    return {
        json.dumps({"id": int(i)}): {"id": int(i), "v": int(v), "s": s}
        for i, v, s in h.my.run("SELECT id, v, s FROM t ORDER BY id", db)
    }


def check(h, broker, db, queue):
    """The oracle; returns the event count."""
    want = table(h, db)
    deadline = time.time() + 120
    reader = QueueReader(broker, queue, tag="check")
    while True:
        reader.drain()
        state, ids, tokens = {}, set(), {}
        for m in reader.messages:
            e = m.get("data")
            if isinstance(e, str):
                e = json.loads(e)
            tid = m.get("transactionId")
            if tid in ids:
                raise Failed(f"transaction id {tid} twice")
            ids.add(tid)
            key = json.dumps(e["key"], sort_keys=True)
            if e["op"] in ("c", "r", "u"):
                state[key] = e["after"]
            elif e["op"] == "d":
                state.pop(key, None)
            if e["op"] != "r":
                s = (e.get("after") or {}).get("s")
                if s and s.startswith("w"):
                    tokens[s] = tokens.get(s, 0) + 1
        twice = [t for t, n in tokens.items() if n > 1]
        if twice:
            raise Failed(f"tokens in two stream events: {twice[:5]}")
        if state == want:
            return len(reader.messages)
        if time.time() > deadline:
            missing = [k for k in want if k not in state][:5]
            extra = [k for k in state if k not in want][:5]
            differ = [(k, want[k], state[k]) for k in want if k in state and state[k] != want[k]][:3]
            raise Failed(f"the queue is not the table: missing {missing} extra {extra} differ {differ}")
        time.sleep(1)


def seed_table(h, db, rows):
    vals = ",".join(f"({i}, 0, 'init{i}')" for i in range(1, rows + 1))
    h.my.run(f"CREATE TABLE t (id BIGINT PRIMARY KEY, v INT NOT NULL, s VARCHAR(40) NOT NULL); INSERT INTO t VALUES {vals};", db)


def scenario_a(h, rng):
    """One node: a snapshot under writes, three kill -9s (in the snapshot
    and in the stream), restarts; the queue ends as the table."""
    db, name, queue = f"myc_a_{secrets.token_hex(3)}", "shop-a", "my-a"
    h.my.run(f"CREATE DATABASE {db}")
    try:
        seed_table(h, db, 20000)
        b = Broker(h, "a-node").start()
        b.wait_healthy()
        b.put_connector(name, doc(h, db, queue))
        wait("the source to start", 60, lambda: (phase_of(b.connector(name)) or "") in ("snapshot", "streaming"))
        w = Writer(h.my, db, rng, 24000)
        w.start()
        for k in range(3):
            time.sleep(rng.uniform(0.5, 2.5))
            ph = phase_of(b.connector(name))
            b.kill9()
            b.wait_exit(30, expect=None)
            h.say(f"a: kill -9 #{k + 1} while {ph}, {w.writes} writes so far")
            b.start()
            b.wait_healthy()
        wait("the snapshot to end", 300, lambda: phase_of(b.connector(name)) == "streaming")
        time.sleep(1)
        w.finish()
        n = check(h, b, db, queue)
        st = status_of(b.connector(name))
        h.say(f"a: PASS: {w.writes} writes, {n} messages, the queue is the table "
              f"(chunk rows dropped by writes since the last start: {st.get('snapshotRowsDropped')})")
        b.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}")


def scenario_b(h, rng):
    """Three nodes: the owner is killed under writes; another node takes
    over; the queue ends as the table."""
    db, name, queue = f"myc_b_{secrets.token_hex(3)}", "shop-b", "my-b"
    h.my.run(f"CREATE DATABASE {db}")
    try:
        seed_table(h, db, 5000)
        nodes = make_cluster(h, "b", 3)
        for n in nodes:
            n.start()
        for n in nodes:
            n.wait_healthy()
        leader = wait_leader(nodes)
        leader.put_connector(name, doc(h, db, queue))
        owner = wait("an owner", 60, lambda: owner_of(nodes, name))
        h.say(f"b: {owner.name} owns the source")
        w = Writer(h.my, db, rng, 6000)
        w.start()
        wait("the snapshot to end", 300, lambda: phase_of(owner.connector(name)) == "streaming")
        time.sleep(1.5)
        t0 = owner.kill9()
        owner.wait_exit(15, expect=None)
        h.say(f"b: kill -9 of the owner {owner.name} after {w.writes} writes")
        others = [n for n in nodes if n is not owner]
        nxt = wait("another owner", 60, lambda: owner_of(others, name), every=0.1)
        h.say(f"b: {nxt.name} took over {time.time() - t0:.1f} s after the kill")
        time.sleep(2)
        owner.start()
        owner.wait_healthy()
        time.sleep(1)
        w.finish()
        lease = find_lease(nxt.connector(name))
        n = check(h, nxt, db, queue)
        h.say(f"b: PASS: {w.writes} writes, {n} messages, lease on {(lease or {}).get('node')}, the queue is the table")
        for x in nodes:
            x.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}")


SCENARIOS = {"a": scenario_a, "b": scenario_b}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("only", nargs="*")
    ap.add_argument("--bin", default=os.path.join(REPO, "server", "target", "debug", "queen"))
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--rust-log", default="info")
    ap.add_argument("--mysql-container", default="queen-mysql84-test")
    ap.add_argument("--mysql-host", default="127.0.0.1")
    ap.add_argument("--mysql-port", type=int, default=53306)
    ap.add_argument("--mysql-password", default="rootpw")
    args = ap.parse_args()
    if not os.path.exists(args.bin):
        print(f"no broker binary at {args.bin}", file=sys.stderr)
        return 2
    args.work = tempfile.mkdtemp(prefix="myconn-")
    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    h = Harness(args)
    h.say(f"work dir {args.work}, seed {seed}")
    try:
        h.my.run("SELECT 1")
    except Exception as e:
        print(f"MySQL is not reachable: {e}", file=sys.stderr)
        return 2
    picked = [s for a in args.only for s in a.split(",")] or sorted(SCENARIOS)
    failed = []
    for s in picked:
        rng = random.Random(f"{seed}:{s}")
        t0 = time.time()
        try:
            SCENARIOS[s](h, rng)
            h.say(f"{s}: done in {time.time() - t0:.0f} s")
        except Exception as e:
            failed.append(s)
            h.say(f"{s}: FAIL: {e}")
            traceback.print_exc()
        finally:
            h.shutdown()
    if not args.keep and not failed:
        shutil.rmtree(args.work, ignore_errors=True)
    else:
        h.say(f"kept {args.work}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
