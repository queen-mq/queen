#!/usr/bin/env python3
"""End-to-end suite of the MySQL source running IN-PROCESS in the broker:
real `queen` processes (one node, or a three-node raft cluster), a real
MySQL 8.0+ (in Docker, here or on another machine), real kill -9.

  python3 test/myconn/run.py              # scenarios a, b (a local container)
  python3 test/myconn/run.py b            # one of them
  python3 test/myconn/run.py --seed 42    # replay a run's randomness

  a  one node: a snapshot under writes, three kill -9s, restarts
  b  three nodes: the owner killed under writes, another takes over
  c  a MySQL failover: the replica promoted behind a virtual IP (needs
     --replica-port; the VIP is a socat on this machine, --vip-port)
  d  schema changes: ALTER during the snapshot and the stream, a rename
     that stops the source, an atomic swap that does not
  e  settings: sslMode disable, verify-full (--ca-file, --tls-host),
     compressed binlog, generated columns, charsets
  f  load: --load-rows copied under --load-writers for --load-seconds
  g  big rows: BLOBs of 1 to 24 MB inserted and updated (each update
     carries the row twice): what goes through, and how a refusal shows
  h  catch-up: the broker stopped while MySQL writes a backlog (one
     1M-row transaction, then many 5,000-row ones); how fast it drains
  n  a silent network: the broker's packets to MySQL dropped (iptables,
     no reset) for 30 s while it streams; it must notice and come back
  s  soak: three nodes for --soak-minutes under writes, a random node
     killed every one to two minutes; memory and lag along the way

The MySQL server: `--mysql-container` (default queen-mysql84-test), reached
by the broker at `--mysql-host`:`--mysql-port` (127.0.0.1:53306) as root
(`--mysql-password`). With `--mysql-container ''` the suite talks to a remote
server through a client container (`--mysql-client-image`) on the host
network. It needs gtid_mode=ON, enforce_gtid_consistency=ON and
binlog_row_metadata=FULL, e.g.

  docker run -d --name queen-mysql84-test -p 127.0.0.1:53306:3306 \\
    -e MYSQL_ROOT_PASSWORD=rootpw mysql:8.4 --gtid-mode=ON \\
    --enforce-gtid-consistency=ON --binlog-row-metadata=FULL --server-id=1

The connector logs in as `--cdc-user` (default: root) to test the grants the
docs name (REPLICATION SLAVE, REPLICATION CLIENT, SELECT).

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
import re
import secrets
import shutil
import signal
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

from queen import Broker, Failed, QueueReader, error_code_of, find_lease, make_cluster, phase_of, status_of, wait_leader  # noqa: E402


class MySQL:
    """A server the suite drives: `mysql` in its container (local), or in a
    client container on the host network (remote). Statements on stdin."""

    def __init__(self, args, port=None):
        self.container = args.mysql_container
        self.image = args.mysql_client_image
        self.password = args.mysql_password
        self.host = args.mysql_host
        self.port = port or args.mysql_port
        self.args = args

    def at(self, port):
        return MySQL(self.args, port)

    def cmd(self, db=None, force=False):
        if self.container:
            c = ["docker", "exec", "-i", self.container, "mysql", "-uroot", f"-p{self.password}", "-N", "-B",
                 "--default-character-set=utf8mb4"]
        else:
            c = ["docker", "run", "--rm", "-i", "--network", "host", self.image, "mysql",
                 "-h", self.host, "-P", str(self.port), "-uroot", f"-p{self.password}", "-N", "-B",
                 "--default-character-set=utf8mb4", "--max-allowed-packet=1G"]
        if force:
            c.append("--force")
        if db:
            c.append(db)
        return c

    def run(self, sql, db=None, check=True):
        p = subprocess.run(self.cmd(db), input=sql.encode(), capture_output=True, timeout=600)
        if check and p.returncode != 0:
            err = "\n".join(l for l in p.stderr.decode(errors="replace").splitlines() if "Using a password" not in l)
            raise Failed(f"mysql {self.port}: {err[-500:]}")
        return [line.split("\t") for line in p.stdout.decode().splitlines()]

    def one(self, sql, db=None):
        rows = self.run(sql, db)
        return rows[0][0] if rows and rows[0] else ""


class Writer(threading.Thread):
    """Random transactions of one to four writes on `table`, each write a
    unique token, through one long-lived client."""

    def __init__(self, my, db, rng, ids, table="t", tag="w", pause=0.002, per_txn=(1, 4)):
        super().__init__(daemon=True)
        self.p = subprocess.Popen(my.cmd(db, force=True), stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.rng, self.ids, self.table, self.tag, self.pause = rng, ids, table, tag, pause
        self.per_txn = per_txn
        self.stop_flag = threading.Event()
        self.writes = 0

    def run(self):
        n = 0
        t = self.table
        while not self.stop_flag.is_set():
            k = self.rng.randint(*self.per_txn)
            sql = ["START TRANSACTION;"]
            for _ in range(k):
                n += 1
                tok = f"{self.tag}{n}"
                i = self.rng.randint(1, self.ids)
                r = self.rng.random()
                if r < 0.7:
                    sql.append(f"UPDATE {t} SET v = v + 1, s = '{tok}' WHERE id = {i};")
                elif r < 0.9:
                    sql.append(f"INSERT INTO {t} (id, v, s) VALUES ({i}, 0, '{tok}') ON DUPLICATE KEY UPDATE v = v + 1, s = '{tok}';")
                else:
                    sql.append(f"DELETE FROM {t} WHERE id = {i};")
            sql.append("COMMIT;\n")
            try:
                self.p.stdin.write(" ".join(sql).encode())
                self.p.stdin.flush()
            except (BrokenPipeError, OSError):
                return
            self.writes += k
            if self.pause:
                time.sleep(self.pause)

    def finish(self):
        """Stop writing. The client is given a while to run what it has
        queued, then stopped: MySQL rolls back a transaction it was in, and
        the checks read the table as it is, so nothing is lost to them."""
        self.stop_flag.set()
        self.join(10)
        try:
            self.p.stdin.close()
        except OSError:
            pass
        try:
            self.p.wait(300)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait(30)


class Vip:
    """A virtual IP for MySQL: socat on this machine, re-pointed in a
    failover. Killing its process group drops every connection through it,
    as a failover does."""

    def __init__(self, port):
        self.port = port
        self.p = None

    def point(self, host, port):
        self.drop()
        self.p = subprocess.Popen(
            ["socat", f"TCP-LISTEN:{self.port},fork,reuseaddr,bind=127.0.0.1", f"TCP:{host}:{port}"],
            start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)

    def drop(self):
        if self.p is not None:
            try:
                os.killpg(self.p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.p.wait(10)
            self.p = None


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
        self.vip = None
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
        if self.vip:
            self.vip.drop()


def doc(h, db, queue, chunk=200, table="t", host=None, port=None, ssl="require", root_cert=None, **source):
    a = h.args
    conn = {
        "host": host or a.mysql_host,
        "port": port or a.mysql_port,
        "database": db,
        "user": a.cdc_user or "root",
        "password": a.cdc_password if a.cdc_user else a.mysql_password,
        "sslMode": ssl,
    }
    if root_cert:
        conn["sslRootCert"] = root_cert
    src = {"tables": [{"table": table, "queue": queue}], "snapshotChunkRows": chunk, "heartbeatSeconds": 1}
    src.update(source)
    return {"kind": "source", "engine": "mysql", "connection": conn, "source": src}


def wait(what, timeout, f, every=0.25):
    deadline = time.time() + timeout
    while True:
        v = f()
        if v:
            return v
        if time.time() > deadline:
            raise Failed(f"timed out ({timeout}s) waiting for {what}")
        time.sleep(every)


def entry(b, name):
    """The connector as `b` sees it, or None while `b` cannot say (it is
    restarting, or its cluster is electing a leader after a kill)."""
    try:
        return b.connector(name)
    except (Failed, OSError):
        return None


def owner_of(nodes, name):
    for b in nodes:
        if not b.alive():
            continue
        e = entry(b, name)
        if e and phase_of(e) in ("streaming", "snapshot"):
            return b
    return None


def table(my, db, name="t", cols=("id", "v", "s")):
    rows = my.run(f"SELECT {', '.join(cols)} FROM {name} ORDER BY id", db)
    out = {}
    for r in rows:
        row = {}
        for c, v in zip(cols, r):
            row[c] = None if v == "NULL" else (int(v) if c in ("id", "v", "x") else v)
        out[json.dumps({"id": row["id"]})] = row
    return out


# A writer's token: w12, x12 (the second writer of c), w3x12 (writer 3 of f).
TOKEN = re.compile(r"(w\d+x|w|x)\d+")


def replay(messages, cols=None):
    """The queue's end state, ids and stream tokens; `cols` keeps only those
    columns of `after` (a table whose columns changed on the way)."""
    state, ids, tokens = {}, set(), {}
    for m in messages:
        e = m.get("data")
        if isinstance(e, str):
            e = json.loads(e)
        tid = m.get("transactionId")
        if tid in ids:
            raise Failed(f"transaction id {tid} twice")
        ids.add(tid)
        key = json.dumps(e["key"], sort_keys=True)
        if e["op"] in ("c", "r", "u"):
            after = e["after"]
            state[key] = {c: after.get(c) for c in cols} if cols else after
        elif e["op"] == "d":
            state.pop(key, None)
        if e["op"] != "r":
            s = (e.get("after") or {}).get("s")
            if s and TOKEN.fullmatch(s):
                tokens[s] = tokens.get(s, 0) + 1
    twice = [t for t, n in tokens.items() if n > 1]
    if twice:
        raise Failed(f"tokens in two stream events: {twice[:5]}")
    return state


def check(h, broker, db, queue, my=None, name="t", cols=("id", "v", "s"), subset=False, deadline_s=180):
    """The oracle; returns the number of messages read."""
    my = my or h.my
    want = table(my, db, name, cols)
    deadline = time.time() + deadline_s
    reader = QueueReader(broker, queue, tag="check")
    while True:
        reader.drain(max_seconds=deadline_s)
        state = replay(reader.messages, list(cols) if subset else None)
        if state == want:
            return len(reader.messages)
        if time.time() > deadline:
            missing = [k for k in want if k not in state][:5]
            extra = [k for k in state if k not in want][:5]
            differ = [(k, want[k], state[k]) for k in want if k in state and state[k] != want[k]][:3]
            raise Failed(f"the queue is not the table: missing {missing} extra {extra} differ {differ}")
        time.sleep(1)


def seed_table(my, db, rows, name="t", extra="", fill=""):
    """`rows` rows in one statement (a recursive CTE); `extra` columns,
    `fill` their values (`, REPEAT('p', 150)`)."""
    my.run(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, v INT NOT NULL, s VARCHAR(40) NOT NULL{extra});", db)
    cols = "id, v, s" + (", pad" if fill else "")
    my.run(f"SET SESSION cte_max_recursion_depth = {rows + 10}; "
           f"INSERT INTO {name} ({cols}) WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq "
           f"WHERE n < {rows}) SELECT n, 0, CONCAT('init', n){fill} FROM seq;", db)


def fresh_db(h, prefix, my=None):
    db = f"{prefix}_{secrets.token_hex(3)}"
    (my or h.my).run(f"CREATE DATABASE {db}")
    return db


def scenario_a(h, rng):
    """One node: a snapshot under writes, three kill -9s (in the snapshot
    and in the stream), restarts; the queue ends as the table."""
    db, name, queue = fresh_db(h, "myc_a"), "shop-a", "my-a"
    try:
        seed_table(h.my, db, 20000)
        b = Broker(h, "a-node").start()
        b.wait_healthy()
        b.put_connector(name, doc(h, db, queue))
        wait("the source to start", 60, lambda: (phase_of(entry(b, name)) or "") in ("snapshot", "streaming"))
        w = Writer(h.my, db, rng, 24000)
        w.start()
        for k in range(3):
            time.sleep(rng.uniform(0.5, 2.5))
            ph = phase_of(entry(b, name))
            b.kill9()
            b.wait_exit(30, expect=None)
            h.say(f"a: kill -9 #{k + 1} while {ph}, {w.writes} writes so far")
            b.start()
            b.wait_healthy()
        wait("the snapshot to end", 300, lambda: phase_of(entry(b, name)) == "streaming")
        time.sleep(1)
        w.finish()
        n = check(h, b, db, queue)
        h.say(f"a: PASS: {w.writes} writes, {n} messages, the queue is the table")
        b.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def scenario_b(h, rng):
    """Three nodes: the owner is killed under writes; another node takes
    over; the queue ends as the table."""
    db, name, queue = fresh_db(h, "myc_b"), "shop-b", "my-b"
    try:
        seed_table(h.my, db, 5000)
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
        wait("the snapshot to end", 300, lambda: phase_of(entry(owner, name)) == "streaming")
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
        lease = find_lease(entry(nxt, name))
        n = check(h, nxt, db, queue)
        h.say(f"b: PASS: {w.writes} writes, {n} messages, lease on {(lease or {}).get('node')}, the queue is the table")
        for x in nodes:
            x.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def gtid_subset(my_small, my_big):
    """Every transaction of the first server is on the second."""
    small = my_small.one("SELECT @@GLOBAL.gtid_executed").replace("\\n", "")
    return my_big.one(f"SELECT GTID_SUBSET('{small}', @@GLOBAL.gtid_executed)") == "1"


def scenario_c(h, rng):
    """A MySQL failover behind a virtual IP: writes stop, the replica
    catches up, the primary shuts down, the replica is promoted and the VIP
    moves to it; writes go on through the VIP. The source follows by GTID:
    the queue ends as the table on the new primary."""
    a = h.args
    if not a.replica_port:
        raise Failed("c needs --replica-port (a replica of the primary with GTID auto-position)")
    primary, replica = h.my, h.my.at(a.replica_port)
    h.vip = Vip(a.vip_port)
    h.vip.point(a.mysql_host, a.mysql_port)
    db, name, queue = fresh_db(h, "myc_c"), "shop-c", "my-c"
    seed_table(primary, db, 5000)
    wait("the replica to have the table", 60, lambda: gtid_subset(primary, replica))
    b = Broker(h, "c-node").start()
    b.wait_healthy()
    b.put_connector(name, doc(h, db, queue, host="127.0.0.1", port=a.vip_port))
    wait("streaming", 120, lambda: phase_of(entry(b, name)) == "streaming")
    uuid0 = status_of(entry(b, name)).get("serverUuid")
    w = Writer(primary, db, rng, 6000)
    w.start()
    time.sleep(4)
    w.finish()
    wait("the replica to catch up", 120, lambda: gtid_subset(primary, replica))
    h.say(f"c: {w.writes} writes on the primary; the replica has them; shutting the primary down")
    primary.run("SHUTDOWN", check=False)
    replica.run("STOP REPLICA; RESET REPLICA ALL;")
    t0 = time.time()
    h.vip.point(a.mysql_host, a.replica_port)
    w2 = Writer(replica, db, rng, 6000, tag="x")
    w2.start()
    wait("the source on the new primary", 120,
         lambda: (status_of(entry(b, name)).get("serverUuid") not in (None, uuid0))
         and phase_of(entry(b, name)) == "streaming", every=0.5)
    h.say(f"c: the source streams from the new primary {time.time() - t0:.1f} s after the VIP moved")
    time.sleep(4)
    w2.finish()
    n = check(h, b, db, queue, my=replica)
    st = status_of(entry(b, name))
    h.say(f"c: PASS: {w.writes} + {w2.writes} writes across the failover, {n} messages, the queue is the "
          f"table on the new primary (server {uuid0} -> {st.get('serverUuid')}, errors so far: {st.get('error')})")
    b.stop()
    h.vip.drop()


def scenario_d(h, rng):
    """Schema changes: ADD/DROP COLUMN during the snapshot and the stream;
    a RENAME that leaves the configured name empty stops the source with
    table_gone (the document then follows the new name and it goes on); an
    atomic swap (gh-ost, pt-online-schema-change) does not stop it."""
    db, name, queue = fresh_db(h, "myc_d"), "shop-d", "my-d"
    try:
        seed_table(h.my, db, 20000)
        b = Broker(h, "d-node").start()
        b.wait_healthy()
        b.put_connector(name, doc(h, db, queue, chunk=100))
        wait("the snapshot", 60, lambda: phase_of(entry(b, name)) == "snapshot")
        w = Writer(h.my, db, rng, 22000)
        w.start()
        time.sleep(1)
        h.my.run("ALTER TABLE t ADD COLUMN x INT NOT NULL DEFAULT 7", db)
        h.say("d: ADD COLUMN x during the snapshot")
        wait("the snapshot to end", 300, lambda: phase_of(entry(b, name)) == "streaming")
        time.sleep(1)
        h.my.run("ALTER TABLE t DROP COLUMN x", db)
        h.my.run("ALTER TABLE t ADD COLUMN y VARCHAR(10) NULL", db)
        h.say("d: DROP COLUMN x, ADD COLUMN y while streaming")
        time.sleep(2)
        w.finish()
        n = check(h, b, db, queue, subset=True)
        h.say(f"d: the queue is the table on (id, v, s) after the ALTERs: {n} messages, {w.writes} writes")
        # A rename away: the source stops and says why.
        h.my.run("RENAME TABLE t TO t_old", db)
        wait("table_gone", 60, lambda: "table_gone" in error_code_of(entry(b, name)), every=0.5)
        st = status_of(entry(b, name))
        h.say(f"d: RENAME stopped the source: {st.get('phase')} {(st.get('error') or {}).get('message', '')[:140]}")
        # The document follows the new name: it goes on from its pointer.
        b.put_connector(name, doc(h, db, queue, chunk=100, table="t_old"))
        wait("streaming again", 60, lambda: phase_of(entry(b, name)) == "streaming")
        h.my.run("UPDATE t_old SET v = v + 100, s = 'after-rename' WHERE id <= 10", db)
        wait("the rows changed after the rename", 60, lambda: _count(b, queue, "after-rename") >= 10)
        # An atomic swap keeps the name: no stop.
        h.my.run("CREATE TABLE t_new LIKE t_old; INSERT INTO t_new SELECT * FROM t_old; "
                 "RENAME TABLE t_old TO t_del, t_new TO t_old;", db)
        h.my.run("UPDATE t_old SET s = 'after-swap' WHERE id <= 5", db)
        wait("the rows changed after the swap", 60, lambda: _count(b, queue, "after-swap") >= 5)
        if "table_gone" in error_code_of(entry(b, name)) and phase_of(entry(b, name)) != "streaming":
            raise Failed("the swap stopped the source")
        h.say("d: PASS: ALTERs followed, the rename stopped the source with table_gone until the document "
              "followed it, the swap went through")
        b.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def _count(b, queue, token):
    r = QueueReader(b, queue, tag="peek")
    r.drain()
    return sum(1 for m in r.messages if token in json.dumps(m.get("data")))


def scenario_e(h, rng):
    """Settings: sslMode disable (MySQL 8 auth over plain TCP), verify-full
    against the server's CA by name, a compressed binlog, generated and
    invisible columns, charsets; each type renders alike in `r` and `c`."""
    a = h.args
    db, queue = fresh_db(h, "myc_e"), "my-e"
    b = Broker(h, "e-node").start()
    b.wait_healthy()
    try:
        h.my.run("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT NOT NULL, s VARCHAR(40) NOT NULL)", db)
        cases = [("e-plain", {"ssl": "disable"})]
        if a.ca_file and a.tls_host:
            with open(a.ca_file) as f:
                pem = f.read()
            cases.append(("e-verify", {"ssl": "verify-full", "host": a.tls_host, "root_cert": pem}))
        for name, kw in cases:
            b.put_connector(name, doc(h, db, f"{queue}-{name}", **kw))
            wait(f"{name} streaming", 60, lambda: phase_of(entry(b, name)) == "streaming")
            h.my.run(f"INSERT INTO t VALUES ({rng.randint(1, 10**9)}, 1, '{name}')", db)
            wait(f"{name} message", 60, lambda: _count(b, f"{queue}-{name}", name) >= 1)
            h.say(f"e: {name}: streams ({kw.get('ssl')})")
        # A compressed binlog: transactions arrive as one payload event.
        h.my.run("SET GLOBAL binlog_transaction_compression = ON")
        try:
            vals = ",".join(f"({i}, 2, 'zip{i}')" for i in range(2_000_000_001, 2_000_002_001))
            h.my.run(f"INSERT INTO t VALUES {vals}", db)
            h.my.run("UPDATE t SET s = 'zipped' WHERE id BETWEEN 2000000001 AND 2000000100", db)
            wait("the compressed transactions", 120, lambda: _count(b, f"{queue}-e-plain", "zip") >= 1000)
            n = check(h, b, db, f"{queue}-e-plain")
            h.say(f"e: compressed binlog: the queue is the table ({n} messages)")
        finally:
            h.my.run("SET GLOBAL binlog_transaction_compression = OFF")
        # Generated, invisible and charset columns: one row before the source
        # starts (an r), the same after (a c).
        h.my.run(
            "CREATE TABLE g (id INT PRIMARY KEY, a INT, b INT AS (a * 2) VIRTUAL, c INT AS (a + 1) STORED, "
            "inv INT INVISIBLE DEFAULT 5, l1 VARCHAR(20) CHARACTER SET latin1, u3 VARCHAR(20) CHARACTER SET utf8mb3, "
            "gbk VARCHAR(20) CHARACTER SET gbk, u16 VARCHAR(20) CHARACTER SET utf16, bin BINARY(3), "
            "e ENUM('x','y') CHARACTER SET latin1, j JSON)", db)
        row = "(%d, 21, 9, 'café €', 'ünï', '中文', 'ab€', x'01', 'y', '{\"k\": [1, \"€\"]}')"
        h.my.run("INSERT INTO g (id, a, inv, l1, u3, gbk, u16, bin, e, j) VALUES " + (row % 1), db)
        b.put_connector("e-types", doc(h, db, f"{queue}-types", table="g"))
        wait("the r row", 60, lambda: _count(b, f"{queue}-types", '"op": "r"') >= 1)
        h.my.run("INSERT INTO g (id, a, inv, l1, u3, gbk, u16, bin, e, j) VALUES " + (row % 2), db)
        r = QueueReader(b, f"{queue}-types", tag="types")
        wait("the c row", 60, lambda: (r.drain() or True) and len(r.messages) >= 2)
        evs = [m["data"] if isinstance(m["data"], dict) else json.loads(m["data"]) for m in r.messages]
        rr = next(e for e in evs if e["op"] == "r")["after"]
        cc = next(e for e in evs if e["op"] == "c")["after"]
        rr.pop("id"), cc.pop("id")
        if rr != cc:
            raise Failed(f"r and c differ:\nr {rr}\nc {cc}")
        h.say(f"e: generated, invisible, charset columns render alike: {json.dumps(cc, ensure_ascii=False)}")
        h.say("e: PASS")
    finally:
        b.stop()
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def rss_mb(b):
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(b.proc.pid)], capture_output=True, text=True).stdout
        return int(out.strip() or 0) // 1024
    except Exception:
        return 0


def scenario_f(h, rng):
    """Load: a big table copied while writers run, then the stream alone;
    what it costs (time, lag, broker memory) and the oracle at the end."""
    a = h.args
    db, name, queue = fresh_db(h, "myc_f"), "shop-f", "my-f"
    try:
        t0 = time.time()
        seed_table(h.my, db, a.load_rows, extra=", pad VARCHAR(200) NOT NULL DEFAULT ''",
                   fill=", REPEAT('p', 150)")
        h.say(f"f: {a.load_rows} rows of ~200 B loaded in {time.time() - t0:.0f} s")
        b = Broker(h, "f-node").start()
        b.wait_healthy()
        t1 = time.time()
        b.put_connector(name, doc(h, db, queue, chunk=2000))
        ws = [Writer(h.my, db, random.Random(rng.random()), a.load_rows + a.load_rows // 10, tag=f"w{k}x", pause=0,
                     per_txn=(a.load_batch // 2, a.load_batch * 3 // 2))
              for k in range(a.load_writers)]
        for w in ws:
            w.start()
        peak_rss, peak_lag, snap_s = 0, 0, None
        end = time.time() + a.load_seconds
        while time.time() < end or snap_s is None:
            st = status_of(entry(b, name))
            if snap_s is None and st.get("phase") == "streaming":
                snap_s = time.time() - t1
                h.say(f"f: snapshot done in {snap_s:.0f} s ({a.load_rows / snap_s:.0f} rows/s) under "
                      f"{sum(w.writes for w in ws)} writes; dropped by them: {st.get('snapshotRowsDropped')}")
            peak_rss = max(peak_rss, rss_mb(b))
            peak_lag = max(peak_lag, st.get("lagSeconds") or 0)
            time.sleep(2)
            if time.time() > end + 1800:
                raise Failed("the snapshot did not end in 30 min past the load")
        writes = sum(w.writes for w in ws)
        for w in ws:
            w.finish()
        dt = time.time() - t1
        wait("caught up", 600, lambda: (status_of(entry(b, name)).get("lagSeconds") or 0) == 0, every=1)
        st = status_of(entry(b, name))
        h.say(f"f: {writes} writes in {dt:.0f} s ({writes / dt:.0f}/s) by {a.load_writers} writers; "
              f"{st.get('messages')} messages, {st.get('transactions')} transactions; peak lag {peak_lag} s, "
              f"peak broker RSS {peak_rss} MB")
        t2 = time.time()
        n = check(h, b, db, queue, cols=("id", "v", "s"), subset=True, deadline_s=1800)
        h.say(f"f: PASS: the queue is the table ({n} messages read back in {time.time() - t2:.0f} s)")
        b.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def scenario_g(h, rng):
    """Big rows: BLOBs of 1, 8 and 24 MB, inserted, then a small column of
    each updated (the update's message carries the row before and after,
    and bytes travel as hex). What arrives, and how a refusal shows."""
    db, name, queue = fresh_db(h, "myc_g"), "shop-g", "my-g"
    b = Broker(h, "g-node").start()
    b.wait_healthy()
    try:
        h.my.run("CREATE TABLE big (id INT PRIMARY KEY, v INT NOT NULL, blob_ LONGBLOB)", db)
        b.put_connector(name, doc(h, db, queue, table="big", snapshot="never"))
        wait("streaming", 60, lambda: phase_of(entry(b, name)) == "streaming")
        sizes = [1, 8, 24]
        for i, mb in enumerate(sizes, 1):
            h.my.run(f"INSERT INTO big VALUES ({i}, 0, REPEAT('a', {mb} * 1048576))", db)
        for i, _ in enumerate(sizes, 1):
            h.my.run(f"UPDATE big SET v = 1 WHERE id = {i}", db)
        deadline = time.time() + 120
        r = QueueReader(b, queue, tag="big")
        while time.time() < deadline:
            r.drain()
            st = status_of(entry(b, name))
            if len(r.messages) >= 2 * len(sizes) or st.get("error"):
                break
            time.sleep(1)
        got = []
        for m in r.messages:
            e = m["data"] if isinstance(m["data"], dict) else json.loads(m["data"])
            size = len(json.dumps(e))
            got.append((e["op"], e["key"]["id"], round(size / 1048576, 1)))
        st = status_of(entry(b, name))
        h.say(f"g: messages (op, id, MB): {got}")
        h.say(f"g: source {st.get('phase')}; error: {(st.get('error') or {}).get('message', 'none')[:300]}")
        if len(got) < 2 * len(sizes):
            raise Failed(f"only {len(got)} of {2 * len(sizes)} messages arrived")
        h.say("g: PASS: every big row and its update arrived")
    finally:
        b.stop()
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def scenario_s(h, rng):
    """Soak: three nodes under writes for --soak-minutes; every one to two
    minutes a random node is killed with -9 and comes back 5 to 15 s later.
    The oracle at the end; memory, lag and owners along the way."""
    a = h.args
    db, name, queue = fresh_db(h, "myc_s"), "shop-s", "my-s"
    try:
        seed_table(h.my, db, 50000)
        nodes = make_cluster(h, "s", 3)
        for n in nodes:
            n.start()
        for n in nodes:
            n.wait_healthy()
        wait_leader(nodes).put_connector(name, doc(h, db, queue))
        wait("an owner", 60, lambda: owner_of(nodes, name))
        ws = [Writer(h.my, db, random.Random(rng.random()), 60000, tag=f"w{k}x", per_txn=(1, 10), pause=0.02)
              for k in range(2)]
        for w in ws:
            w.start()
        end = time.time() + a.soak_minutes * 60
        next_kill = time.time() + rng.uniform(60, 120)
        kills, owners, peak_rss, peak_lag = 0, [], {}, 0
        while time.time() < end:
            o = owner_of(nodes, name)
            if o and (not owners or owners[-1] != o.name):
                owners.append(o.name)
            if o:
                peak_lag = max(peak_lag, status_of(entry(o, name)).get("lagSeconds") or 0)
            for n in nodes:
                if n.alive():
                    peak_rss[n.name] = max(peak_rss.get(n.name, 0), rss_mb(n))
            if time.time() >= next_kill:
                victim = rng.choice([n for n in nodes if n.alive()])
                victim.kill9()
                victim.wait_exit(15, expect=None)
                kills += 1
                down = rng.uniform(5, 15)
                h.say(f"s: kill -9 #{kills} {victim.name}{' (owner)' if o is victim else ''}, back in {down:.0f} s; "
                      f"{sum(w.writes for w in ws)} writes so far")
                time.sleep(down)
                victim.start()
                victim.wait_healthy()
                next_kill = time.time() + rng.uniform(60, 120)
            time.sleep(5)
        writes = sum(w.writes for w in ws)
        for w in ws:
            w.finish()
        o = wait("an owner at the end", 120, lambda: owner_of(nodes, name))
        n = check(h, o, db, queue, deadline_s=600)
        st = status_of(entry(o, name))
        h.say(f"s: PASS: {a.soak_minutes} min, {writes} writes, {kills} kills, owners {owners}, {n} messages, the queue "
              f"is the table; peak lag {peak_lag} s; peak RSS MB {peak_rss}; source errors on the owner: "
              f"{st.get('restarts')} restarts")
        for x in nodes:
            x.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def scenario_h(h, rng):
    """Catch-up: with the broker stopped, MySQL writes a backlog (one
    transaction updating --load-rows rows, then 5,000-row transactions as
    many again); started, the source drains it. How fast, and the oracle."""
    a = h.args
    db, name, queue = fresh_db(h, "myc_h"), "shop-h", "my-h"
    try:
        seed_table(h.my, db, a.load_rows)
        b = Broker(h, "h-node").start()
        b.wait_healthy()
        extra = {}
        if a.max_bundle_messages:
            extra = {"maxBundleMessages": a.max_bundle_messages, "maxBundleBytes": 64 * 1024 * 1024}
        b.put_connector(name, doc(h, db, queue, snapshot="never", **extra))
        wait("streaming", 120, lambda: phase_of(entry(b, name)) == "streaming")
        b.stop()
        t0 = time.time()
        h.my.run("UPDATE t SET v = v + 1", db)
        step = 5000
        sql = "".join(f"UPDATE t SET v = v + 1 WHERE id BETWEEN {lo} AND {lo + step - 1};"
                      for lo in range(1, a.load_rows + 1, step))
        h.my.run(sql, db)
        events = 2 * a.load_rows
        h.say(f"h: backlog of {events} row changes written in {time.time() - t0:.0f} s "
              f"(one {a.load_rows}-row transaction, {a.load_rows // step} of {step})")
        b.start()
        b.wait_healthy()
        t1 = time.time()
        wait("the backlog drained", 3600,
             lambda: (status_of(entry(b, name)).get("messages") or 0) >= events
             and (status_of(entry(b, name)).get("lagSeconds") or 0) == 0, every=1)
        dt = time.time() - t1
        st = status_of(entry(b, name))
        h.say(f"h: drained {st.get('messages')} messages in {dt:.0f} s ({events / dt:.0f} row changes/s), "
              f"{st.get('bundles')} QueenMQ transactions, broker RSS {rss_mb(b)} MB")
        n = check(h, b, db, queue, deadline_s=1800)
        h.say(f"h: PASS: the queue is the table ({n} messages)")
        b.stop()
    finally:
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


def scenario_n(h, rng):
    """A silent network: while the source streams under writes, every packet
    from this machine to MySQL's port is dropped for 30 s (no reset, the
    connection just goes quiet). The source must see the silence, retry, and
    go on once the path is back: the queue ends as the table."""
    a = h.args
    # Both ways: no packet out to MySQL, none back from it (heartbeats
    # included), as on a dead link.
    rules = [["OUTPUT", "-p", "tcp", "-d", a.mysql_host, "--dport", str(a.mysql_port), "-j", "DROP"],
             ["INPUT", "-p", "tcp", "-s", a.mysql_host, "--sport", str(a.mysql_port), "-j", "DROP"]]
    db, name, queue = fresh_db(h, "myc_n"), "shop-n", "my-n"
    b = Broker(h, "n-node").start()
    b.wait_healthy()
    try:
        seed_table(h.my, db, 5000)
        b.put_connector(name, doc(h, db, queue))
        wait("streaming", 120, lambda: phase_of(entry(b, name)) == "streaming")
        time.sleep(2)
        for r in rules:
            subprocess.run(["iptables", "-A", *r], check=True)
        t0 = time.time()
        h.say("n: packets to MySQL dropped")
        seen = wait("the source to notice", 120,
                    lambda: phase_of(entry(b, name)) in ("error", "connecting") or entry(b, name) and
                    (status_of(entry(b, name)).get("error") or {}).get("code"), every=0.5)
        h.say(f"n: noticed after {time.time() - t0:.0f} s: {phase_of(entry(b, name))} "
              f"{(status_of(entry(b, name)).get('error') or {}).get('message', '')[:120]}")
        time.sleep(max(0, 30 - (time.time() - t0)))
        for r in rules:
            subprocess.run(["iptables", "-D", *r], check=True)
        t1 = time.time()
        h.say("n: path back; writes resume")
        w = Writer(h.my, db, rng, 6000)
        w.start()
        wait("streaming again", 180, lambda: phase_of(entry(b, name)) == "streaming", every=0.5)
        h.say(f"n: streaming again {time.time() - t1:.0f} s after the path came back")
        time.sleep(3)
        w.finish()
        n = check(h, b, db, queue)
        h.say(f"n: PASS: {w.writes} writes after the outage, {n} messages, the queue is the table")
    finally:
        for r in rules:
            subprocess.run(["iptables", "-D", *r], capture_output=True)
        b.stop()
        h.my.run(f"DROP DATABASE IF EXISTS {db}", check=False)


SCENARIOS = {"a": scenario_a, "b": scenario_b, "c": scenario_c, "d": scenario_d, "e": scenario_e, "f": scenario_f,
             "g": scenario_g, "h": scenario_h, "n": scenario_n, "s": scenario_s}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("only", nargs="*")
    ap.add_argument("--bin", default=os.path.join(REPO, "server", "target", "debug", "queen"))
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--rust-log", default="info")
    ap.add_argument("--mysql-container", default="queen-mysql84-test")
    ap.add_argument("--mysql-client-image", default="mysql:8.4")
    ap.add_argument("--mysql-host", default="127.0.0.1")
    ap.add_argument("--mysql-port", type=int, default=53306)
    ap.add_argument("--mysql-password", default="rootpw")
    ap.add_argument("--cdc-user", default=None)
    ap.add_argument("--cdc-password", default=None)
    ap.add_argument("--replica-port", type=int, default=None)
    ap.add_argument("--vip-port", type=int, default=3310)
    ap.add_argument("--ca-file", default=None)
    ap.add_argument("--tls-host", default=None)
    ap.add_argument("--load-rows", type=int, default=1_000_000)
    ap.add_argument("--load-writers", type=int, default=4)
    ap.add_argument("--load-seconds", type=int, default=300)
    ap.add_argument("--load-batch", type=int, default=2, help="writes per writer transaction, on average")
    ap.add_argument("--soak-minutes", type=int, default=30)
    ap.add_argument("--max-bundle-messages", type=int, default=None, help="h: the source's maxBundleMessages")
    args = ap.parse_args()
    if not os.path.exists(args.bin):
        print(f"no broker binary at {args.bin}", file=sys.stderr)
        return 2
    args.work = tempfile.mkdtemp(prefix="myconn-")
    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    h = Harness(args)
    h.say(f"work dir {args.work}, seed {seed}, MySQL {args.mysql_host}:{args.mysql_port}")
    try:
        h.say(f"MySQL {h.my.one('SELECT VERSION()')}")
    except Exception as e:
        print(f"MySQL is not reachable: {e}", file=sys.stderr)
        return 2
    picked = [s for a in args.only for s in a.split(",")] or ["a", "b"]
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
    h.say(f"result: {'FAIL ' + ','.join(failed) if failed else 'PASS'} ({','.join(picked)})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
