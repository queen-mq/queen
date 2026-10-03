"""Broker side of the pgconn e2e suite: real `queen` processes (one node, or a
three-node openraft cluster on the loopback) and the HTTP calls the suite
makes to them. Process management is the S3 sink suite's
(test/s3sink/run.py in the s3-sink worktree): explicit environment, fresh
data dirs, readiness via /health, kill -9, SIGTERM, one log file per node
with every restart appended.
"""

import json
import os
import re
import secrets
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from decimal import Decimal
from http.client import HTTPException

# The broker's default tenant (server/src/config.rs DEFAULT_TENANT): the
# connectors' runtime KV (pointer, lease) lives there when tenancy is off.
DEFAULT_TENANT = "00000000-0000-0000-0000-000000000001"
KV_NS = "queen-pg"

# Environment the broker inherits; everything else is explicit, so a QUEEN_*
# variable in the caller's shell cannot leak in.
KEEP_ENV = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "LOGNAME")


class Failed(Exception):
    """A scenario failed: the message is the evidence."""


class Unsupported(Exception):
    """The suite cannot run here (binary without the pg feature, PG missing)."""


def loads(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    return json.loads(raw, parse_float=Decimal)


def short(b, n=300):
    if isinstance(b, bytes):
        b = b.decode("utf-8", "replace")
    b = str(b)
    return b if len(b) <= n else b[:n] + f"...(+{len(b) - n})"


def free_ports(n):
    socks = []
    try:
        for _ in range(n):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def http(method, url, body=None, headers=None, timeout=30):
    """(status, headers, body bytes). An HTTP error status is an answer; a
    transport failure raises OSError/ConnectionError."""
    if isinstance(body, (dict, list)):
        body = json.dumps(body).encode()
        headers = dict(headers or {})
        headers.setdefault("content-type", "application/json")
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read()
    except HTTPException as e:
        raise ConnectionError(f"{type(e).__name__}: {e}") from None


def _no_core():
    try:
        import resource

        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    except Exception:  # noqa: BLE001
        pass


class Broker:
    """One broker process."""

    def __init__(self, h, name, node_id=1):
        self.h = h
        self.name = name
        self.node_id = node_id
        self.http_port = free_ports(1)[0]
        self.raft_port = None
        self.cluster_env = {}
        self.data_dir = os.path.join(h.work, "data", name)
        self.log_path = os.path.join(h.work, "logs", f"{name}.log")
        self.proc = None
        self.starts = 0
        self.log_offset = 0
        self.started_at = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.http_port}"

    def env(self, extra=None):
        full = {k: os.environ[k] for k in KEEP_ENV if k in os.environ}
        full.update(
            {
                "PORT": str(self.http_port),
                "QUEEN_BIND_ADDR": "127.0.0.1",
                "QUEEN_RAFT_DIR": self.data_dir,
                "QUEEN_RAFT_DISK_HIGH_PCT": "99.9",
                "QUEEN_RAFT_DISK_LOW_PCT": "99.8",
                "QUEEN_SERVER_ID": self.name,
                "QUEEN_ENCRYPTION_KEY": self.h.encryption_key,
                "RUST_LOG": self.h.rust_log,
                # The connectors (PLAN §3.3): every node runs the manager.
                "QUEEN_PG_CONNECTORS": "true",
                "QUEEN_PG_THREADS": "2",
                "QUEEN_PG_RELOAD_MS": "500",
                "QUEEN_PG_LEASE_TTL_MS": str(self.h.lease_ttl_ms),
                "QUEEN_PG_SHUTDOWN_GRACE_MS": "10000",
                "QUEEN_PG_ALLOW_PRIVATE_NETWORKS": "true",
            }
        )
        full.update(self.cluster_env)
        full.update(extra or {})
        return full

    def start(self, extra=None):
        os.makedirs(self.data_dir, exist_ok=True)
        env = self.env(extra)
        self.starts += 1
        self.started_at = time.time()
        shown = " ".join(f"{k}={v}" for k, v in sorted(env.items()) if k.startswith("QUEEN_PG_") or k.startswith("QUEEN_RAFT_NODE"))
        with open(self.log_path, "ab") as log:
            self.log_offset = log.tell()
            log.write(f"\n===== harness: start #{self.starts} of {self.name} at {time.strftime('%H:%M:%S')}: {shown}\n".encode())
            log.flush()
            self.proc = subprocess.Popen(
                [self.h.bin],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                cwd=self.data_dir,
                start_new_session=True,
                preexec_fn=_no_core,
            )
        self.h.live.add(self)
        return self

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def health(self):
        try:
            _, _, body = http("GET", self.url + "/health", timeout=3)
            return json.loads(body)
        except (OSError, ValueError):
            return None

    def role(self):
        hh = self.health()
        return ((hh or {}).get("raft") or {}).get("role")

    def wait_healthy(self, timeout=120):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            if not self.alive():
                raise Failed(f"{self.name} exited ({self.proc.returncode}) before it was healthy\n{self.tail()}")
            last = self.health()
            if last and last.get("status") == "healthy":
                return last
            time.sleep(0.2)
        raise Failed(f"{self.name} not healthy after {timeout}s: {last}\n{self.tail()}")

    def sigterm(self):
        self.proc.send_signal(signal.SIGTERM)
        return time.time()

    def kill9(self):
        if self.alive():
            self.proc.send_signal(signal.SIGKILL)
        return time.time()

    def wait_exit(self, timeout, expect=0):
        try:
            rc = self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(10)
            raise Failed(f"{self.name} still running {timeout}s later; killed\n{self.tail()}") from None
        self.h.live.discard(self)
        if expect is not None and rc != expect:
            raise Failed(f"{self.name} exited {rc}, expected {expect}\n{self.tail()}")
        return rc

    def stop(self, timeout=60):
        if not self.alive():
            return self.proc.returncode if self.proc else None
        self.sigterm()
        return self.wait_exit(timeout)

    def crash_restart(self, timeout=120):
        """kill -9, then start again on the same data dir and wait healthy.
        Returns (seconds down, seconds to healthy)."""
        t0 = self.kill9()
        self.wait_exit(15, expect=None)
        t1 = time.time()
        self.start()
        self.wait_healthy(timeout)
        return t1 - t0, time.time() - t1

    def log(self, whole=False):
        try:
            with open(self.log_path, "rb") as f:
                if not whole:
                    f.seek(self.log_offset)
                return f.read().decode("utf-8", "replace")
        except OSError:
            return ""

    def tail(self, n=25, pattern=None):
        lines = self.log(whole=True).splitlines()
        if pattern:
            lines = [l for l in lines if re.search(pattern, l)]
        return "\n".join(f"    | {l}" for l in lines[-n:]) or "    | (empty)"

    def grep(self, pattern, whole=True, n=12):
        lines = [l for l in self.log(whole=whole).splitlines() if re.search(pattern, l)]
        return lines[-n:]

    # --- HTTP API --------------------------------------------------------------------------

    def call(self, method, path, body=None, timeout=30, headers=None):
        """(status, parsed JSON or None, raw)."""
        st, _, raw = http(method, self.url + path, body, headers, timeout=timeout)
        try:
            doc = loads(raw) if raw and raw.strip() else None
        except ValueError:
            doc = None
        return st, doc, raw

    def configure_queue(self, queue, options=None, timeout=30):
        opts = {"retentionEnabled": False}
        opts.update(options or {})
        deadline = time.time() + timeout
        while True:
            try:
                st, _, raw = self.call("POST", "/api/v1/configure", {"queue": queue, "options": opts})
            except OSError as e:
                st, raw = None, str(e)
            if st == 200:
                return
            if time.time() > deadline:
                raise Failed(f"configure {queue!r} via {self.name} -> {st}: {short(raw)}")
            time.sleep(0.3)

    # connectors (PLAN §3.1)

    def connectors(self):
        st, doc, raw = self.call("GET", "/api/v1/connectors", timeout=10)
        if st == 404 and b"no_such_route" in (raw or b""):
            raise Unsupported(f"{self.name}: GET /api/v1/connectors -> 404 no_such_route: the binary has no pg connectors")
        if st != 200:
            raise Failed(f"GET /api/v1/connectors via {self.name} -> {st}: {short(raw)}")
        return connector_list(doc)

    def connector(self, name):
        """The connector's entry (redacted doc + status) as THIS node sees it,
        or None."""
        try:
            st, doc, raw = self.call("GET", f"/api/v1/connectors/{name}", timeout=10)
        except OSError:
            return None
        if st == 200 and isinstance(doc, dict):
            return doc
        if st == 404:
            return None
        raise Failed(f"GET /api/v1/connectors/{name} via {self.name} -> {st}: {short(raw)}")

    def put_connector(self, name, doc, want=200):
        st, body, raw = self.call("PUT", f"/api/v1/connectors/{name}", doc, timeout=30)
        if want is not None and st != want:
            raise Failed(f"PUT /api/v1/connectors/{name} via {self.name} -> {st}: {short(raw, 800)}")
        return st, body, raw

    def delete_connector(self, name, drop_slot=False):
        q = "?dropSlot=true" if drop_slot else ""
        return self.call("DELETE", f"/api/v1/connectors/{name}{q}", timeout=30)

    def resync(self, name):
        return self.call("POST", f"/api/v1/connectors/{name}/resync", {}, timeout=30)

    # KV (the connector's own tenant = the default tenant here)

    def kv(self, ops, timeout=10):
        st, doc, raw = self.call("POST", "/api/v1/kv", {"operations": ops}, timeout=timeout)
        if st != 200 or not isinstance(doc, dict):
            raise Failed(f"kv via {self.name} -> {st}: {short(raw)}")
        return doc

    def kv_get(self, key, ns=KV_NS):
        doc = self.kv([{"ns": ns, "op": "get", "key": key}])
        res = (doc.get("results") or [{}])[0]
        return res

    def kv_value(self, key, ns=KV_NS):
        r = self.kv_get(key, ns)
        return r.get("value") if r.get("found") else None

    def kv_prefix(self, prefix, ns=KV_NS):
        doc = self.kv([{"ns": ns, "op": "getPrefix", "prefix": prefix, "limit": 1000}])
        res = (doc.get("results") or [{}])[0]
        return res.get("rows") or []

    # DLQ, partitions, metrics

    def dlq(self, queue, group=None, limit=1000):
        q = {"queue": queue, "limit": str(limit)}
        if group:
            q["consumerGroup"] = group
        st, doc, raw = self.call("GET", "/api/v1/dlq?" + urllib.parse.urlencode(q), timeout=15)
        if st != 200:
            raise Failed(f"GET /api/v1/dlq via {self.name} -> {st}: {short(raw)}")
        return doc

    def partitions(self, queue):
        """[{name, lastOffset, logStart}] of every partition of `queue`."""
        out, after = [], None
        while True:
            e = {"queue": queue, "limit": 1000}
            if after:
                e["after"] = after
            st, doc, raw = self.call("POST", "/api/v1/partitions/changed", {"entries": [e]}, timeout=30)
            if st != 200:
                raise Failed(f"partitions/changed {queue} via {self.name} -> {st}: {short(raw)}")
            ent = doc["entries"][0]
            if ent.get("error"):
                if ent["error"] == "UNKNOWN_TOPIC_OR_PARTITION":
                    return []
                raise Failed(f"partitions/changed {queue}: {ent}")
            out.extend(ent.get("partitions") or [])
            after = ent.get("next")
            if not after:
                return out

    def metrics(self):
        st, _, body = http("GET", self.url + "/metrics/prometheus", timeout=10)
        return body.decode("utf-8", "replace") if st == 200 else ""


def connector_list(doc):
    """GET /api/v1/connectors answers a list, or an object holding one."""
    if isinstance(doc, list):
        return doc
    if isinstance(doc, dict):
        for k in ("connectors", "items", "results", "data"):
            if isinstance(doc.get(k), list):
                return doc[k]
    return []


def status_of(entry):
    """The live status block of a connector entry."""
    if not isinstance(entry, dict):
        return {}
    st = entry.get("status")
    return st if isinstance(st, dict) else entry


def phase_of(entry):
    return status_of(entry).get("phase")


def error_code_of(entry):
    """Every error code the entry names (status.error.code, …): searched, so
    the shape may move without the suite going blind."""
    codes = []

    def walk(v):
        if isinstance(v, dict):
            c = v.get("code")
            if isinstance(c, str):
                codes.append(c)
            for x in v.values():
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)

    walk(status_of(entry))
    return codes


def find_lease(entry):
    """`state.lease` (or any `lease` object with a `node`) in the entry."""
    found = []

    def walk(v):
        if isinstance(v, dict):
            l = v.get("lease")
            if isinstance(l, dict):
                found.append(l)
            for x in v.values():
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)

    walk(entry)
    return found[0] if found else None


# --- the cluster -----------------------------------------------------------------------------


def make_cluster(h, prefix, n=3):
    nodes = [Broker(h, f"{prefix}-node{i}", node_id=i) for i in range(1, n + 1)]
    raft_ports = free_ports(n)
    for b, rp in zip(nodes, raft_ports):
        b.raft_port = rp
    peers = ",".join(f"{b.node_id}=127.0.0.1:{b.raft_port}/127.0.0.1:{b.http_port}" for b in nodes)
    token = secrets.token_hex(16)
    for b in nodes:
        b.cluster_env = {
            "QUEEN_RAFT_REPLICATOR": "openraft",
            "QUEEN_RAFT_NODE_ID": str(b.node_id),
            "QUEEN_RAFT_PEERS": peers,
            "QUEEN_RAFT_LISTEN": f"127.0.0.1:{b.raft_port}",
            "QUEEN_RAFT_TOKEN": token,
        }
    return nodes


def leader_of(nodes):
    for b in nodes:
        if b.alive() and b.role() == "leader":
            return b
    return None


def wait_leader(nodes, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        l = leader_of(nodes)
        if l is not None:
            return l
        time.sleep(0.2)
    raise Failed(f"no leader among {[b.name for b in nodes]} after {timeout}s")


def wait_caught_up(nodes, node, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader = leader_of(nodes)
        if leader is not None:
            lh, nh = leader.health(), node.health()
            if lh and nh and nh["raft"].get("applied", 0) >= lh["raft"].get("commit", 0):
                return
        time.sleep(0.3)
    raise Failed(f"{node.name} not caught up with the leader after {timeout}s")


# --- push ----------------------------------------------------------------------------------------


def push_body(items):
    """items: (queue, partition, transactionId, payload JSON TEXT). The payload
    is spliced as text: never through a JSON library on this side."""
    parts = []
    for q, p, txn, payload in items:
        parts.append(
            '{"queue":' + json.dumps(q) + ',"partition":' + json.dumps(p) + ',"transactionId":' + json.dumps(txn) + ',"payload":' + payload + "}"
        )
    return ('{"items":[' + ",".join(parts) + "]}").encode("utf-8")


def push(urls, items, timeout_s=180):
    """Push until every item is acknowledged (queued or duplicate), rotating
    over `urls`. Items carry transactionIds, so a retry after a lost answer is
    deduplicated by the broker. Returns {txn: offset}."""
    rest = list(items)
    acked = {}
    deadline = time.time() + timeout_s
    attempt = 0
    why = ""
    while rest:
        url = urls[attempt % len(urls)]
        attempt += 1
        try:
            st, _, raw = http("POST", url + "/api/v1/push", push_body(rest), {"content-type": "application/json"}, timeout=30)
        except OSError as e:
            st, raw = None, str(e).encode()
        if st == 201:
            answers = json.loads(raw)
            left = []
            for it, ans in zip(rest, answers):
                if ans.get("status") in ("queued", "duplicate"):
                    acked[it[2]] = ans.get("offset")
                else:
                    left.append(it)
                    why = f"item answered {ans}"
            rest = left
        else:
            why = f"HTTP {st}: {short(raw)}"
            if st in (400, 413):
                raise Failed(f"push refused: {why}")
        if rest:
            if time.time() > deadline:
                raise Failed(f"push not acknowledged after {timeout_s}s: {why}")
            time.sleep(0.3)
    return acked


# --- the reader --------------------------------------------------------------------------------


class QueueReader:
    """Reads EVERY message of a queue, without a sink: wildcard pops of a
    consumer group of its own, `autoAck=true`, 64 partitions per call, until
    the queue answers empty. The pop answer splices each payload's stored
    bytes (`data`), so numbers arrive exactly as the source wrote them (the
    /api/v1/fetch route re-renders payloads through f64; never used for
    values here).

    Incremental: call `drain()` again to pick up what arrived since. A
    transport error loses an auto-acked answer, so it restarts from scratch
    with a fresh group (and says so in `restarts`)."""

    def __init__(self, broker, queue, tag="reader", consume=None):
        self.broker = broker
        self.queue = queue
        self.tag = tag
        # consume(message): hand each message over instead of keeping it (a
        # reader of millions); duplicates are then told by per-partition
        # offsets, and a lost answer cannot be recovered.
        self.consume = consume
        self.count = 0
        self.restarts = 0
        self.pops = 0
        self._reset()

    def _reset(self):
        if self.consume is not None and self.count:
            raise Failed(f"reading {self.queue}: a pop answer was lost after {self.count} consumed messages (auto-acked, unrecoverable)")
        self.group = f"pgc-{self.tag}-{secrets.token_hex(4)}"
        self.messages = []  # every message as delivered: dicts with data parsed
        self.seen = set()  # (partition, offset)
        self.last_off = {}  # partition -> highest offset (consume mode)
        self.dup_deliveries = []

    def set_broker(self, broker):
        self.broker = broker

    def drain(self, max_seconds=300):
        deadline = time.time() + max_seconds
        empties = 0
        while True:
            q = urllib.parse.urlencode(
                {
                    "consumerGroup": self.group,
                    "batch": "2000",
                    "partitions": "64",
                    "autoAck": "true",
                    "wait": "false",
                    "subscriptionMode": "all",
                }
            )
            try:
                st, _, raw = http("GET", f"{self.broker.url}/api/v1/pop/queue/{urllib.parse.quote(self.queue, safe='')}?{q}", timeout=60)
            except OSError:
                if not self.broker.alive():
                    raise
                self.restarts += 1
                self._reset()
                time.sleep(0.5)
                continue
            self.pops += 1
            if st == 204 or not raw.strip():
                empties += 1
                if empties >= 2:
                    return
                time.sleep(0.05)
                continue
            if st != 200:
                if time.time() > deadline:
                    raise Failed(f"pop {self.queue} via {self.broker.name} -> {st}: {short(raw)}")
                time.sleep(0.3)
                continue
            doc = loads(raw)
            msgs = doc.get("messages") or []
            if not msgs:
                empties += 1
                if empties >= 2:
                    return
                continue
            empties = 0
            for m in msgs:
                key = (m.get("partition"), m.get("offset"))
                if self.consume is not None:
                    lo = self.last_off.get(key[0])
                    if lo is not None and key[1] <= lo:
                        self.dup_deliveries.append(key)
                        continue
                    self.last_off[key[0]] = key[1]
                    self.count += 1
                    self.consume(m)
                    continue
                if key in self.seen:
                    self.dup_deliveries.append(key)
                    continue
                self.seen.add(key)
                self.count += 1
                self.messages.append(m)
            if time.time() > deadline:
                raise Failed(f"reading {self.queue} took more than {max_seconds}s ({len(self.messages)} messages so far)")

    def complete_against(self, parts):
        """Problems with completeness: every offset logStart..lastOffset of
        every partition delivered exactly once."""
        by_part = defaultdict(set)
        for m in self.messages:
            by_part[m.get("partition")].add(m.get("offset"))
        problems = []
        for p in parts:
            name, last, start = p["name"], p.get("lastOffset", -1), p.get("logStart", 0)
            have = by_part.get(name, set())
            want = set(range(int(start), int(last) + 1))
            if have != want:
                missing = sorted(want - have)
                extra = sorted(have - want)
                problems.append(f"partition {name!r}: missing offsets {missing[:5]} ({len(missing)}), unexpected {extra[:5]}")
        names = {p["name"] for p in parts}
        stray = [n for n in by_part if n not in names]
        if stray:
            problems.append(f"messages from partitions the listing does not know: {stray[:5]}")
        return problems


__all__ = [
    "Broker",
    "DEFAULT_TENANT",
    "Failed",
    "KV_NS",
    "QueueReader",
    "Unsupported",
    "connector_list",
    "error_code_of",
    "find_lease",
    "free_ports",
    "http",
    "leader_of",
    "loads",
    "make_cluster",
    "phase_of",
    "push",
    "short",
    "status_of",
    "wait_caught_up",
    "wait_leader",
]
