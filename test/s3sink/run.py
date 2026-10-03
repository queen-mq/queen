#!/usr/bin/env python3
"""End-to-end suite for the S3 / data-lake sink running IN-PROCESS in the broker.

Nothing is simulated except the bucket: the real broker binary is started and
killed as a process (one node, or a three-node raft cluster), it runs the sink
with QUEEN_S3_EMBEDDED=true, and the sink writes over real HTTP to fake_s3.py,
a small S3-compatible server that stores every object as a file. Everything is
verified by reading those files — the lake, as a lake reader sees it — never by
asking the sink how it thinks it did.

Scenarios (see README.md for what each one proves):

  s3   the fake S3 itself answers like S3 (no broker)
  self the lake checker refuses every kind of damage it exists to catch
  a    one node, scale: queues x partitions x records pushed while the sink runs
  b    one node, the crash matrix: QUEEN_S3_CRASH_AT at each of its five points
  b2   one node, mid_upload on a window big enough for a multipart upload
  c    one node, SIGTERM drain while filling, mid-upload, and cut by the grace
  d    three nodes: ownership, follower commits, kill -9 takeover, SIGTERM
       hand-over, rejoin, exactly once across every queue
  e    one node, edge cases: payload whitespace (newlines), escaped queue and
       partition names, a payload above the fetch byte budget
  g    one node, the bucket down at boot, then throttling and 500s mid-traffic
  f    one node, the lease refresh against frequent windows: no self-fence
  h    one node, a cell (embedded proxy, tenancy): the default tenant's env
       sink and three control-plane tenants on one queue name — isolation,
       redacted secrets, DELETE, suspend, push_blocked, rotation, enabled,
       and a cell without QUEEN_ENCRYPTION_KEY
  i    three nodes, two control-plane tenants: the PUT reaches every node,
       kill -9 takeover, a node with the wrong key, DELETE everywhere
  j    three nodes booted 1.5 s apart: do the queues spread? (reproduction)
  k    SIGTERM the leader and the followers in turn: is every lease released
       before its expiry? (reproduction)

  python3 test/s3sink/run.py                     # every scenario
  python3 test/s3sink/run.py --scenario a,d      # some of them
  python3 test/s3sink/run.py --keep --work DIR   # keep data, logs and bucket

Exit code: 0 every scenario passed, 1 something failed, 2 the suite could not
run (no binary, a port, a bad flag).

Standard library only.
"""

import argparse
import calendar
import concurrent.futures
import gzip
import hashlib
import json
import os
import random
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from http.client import HTTPException

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
FAKE_S3 = os.path.join(HERE, "fake_s3.py")
BUCKET = "queen-lake"
ACCESS_KEY = "s3sink-e2e"
SECRET_KEY = "s3sink-e2e-secret"
KV_NS = "queen-s3"
# The broker's default tenant (server/src/config.rs DEFAULT_TENANT): every key
# of the environment's sink carries it (connectors/queen-s3/src/layout.rs).
DEFAULT_TENANT = "00000000-0000-0000-0000-000000000001"
# The buckets the fake S3 holds: the default one, and one per tenant for the
# multi-tenant scenarios (each tenant mirrors to a bucket of its own).
BUCKETS = [BUCKET, "env-lake", "acme-lake", "globex-lake", "tenant-a-lake", "tenant-b-lake", "nokey-lake"]
CRASH_POINTS = ["after_intent", "mid_upload", "after_upload", "before_commit", "after_commit"]
PART_SIZE = 16 * 1024 * 1024  # connectors/queen-s3/src/s3/client.rs PART_SIZE

# Environment variables the broker inherits from this process. Everything else
# is set explicitly, so a QUEEN_* variable in the caller's shell cannot leak in.
KEEP_ENV = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "LOGNAME")


class Failed(Exception):
    pass


class Harness:
    """What every scenario shares: flags, the work dir, the fake bucket, and
    every process started, so none outlives the run."""

    def __init__(self, args):
        self.args = args
        self.bin = args.bin
        self.work = args.work
        self.rust_log = args.rust_log
        self.live = set()
        self.s3 = None
        self.transcript = open(os.path.join(self.work, "harness.log"), "a", buffering=1)
        os.makedirs(os.path.join(self.work, "logs"), exist_ok=True)
        os.makedirs(os.path.join(self.work, "data"), exist_ok=True)

    @property
    def bucket_dir(self):
        return os.path.join(self.s3.root, BUCKET)

    def bucket(self, name=BUCKET):
        """The directory of bucket `name` in the fake S3."""
        return os.path.join(self.s3.root, name)

    def say(self, msg):
        line = f"[{time.strftime('%H:%M:%S')}] {msg}"
        print(line, flush=True)
        self.transcript.write(line + "\n")

    def kill_all(self):
        for b in list(self.live):
            if b.alive():
                self.say(f"  (killing {b.name}, still running)")
                b.kill9()
                b.wait_exit(10, expect=None)
        self.live.clear()


# --- small things ---------------------------------------------------------------


def free_ports(n):
    """`n` distinct free TCP ports on the loopback, bound at once so that the
    kernel cannot hand the same one out twice."""
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


def wait_port(port, timeout=30, proc=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        if proc is not None and proc.poll() is not None:
            raise Failed(f"the process meant to listen on {port} exited ({proc.returncode})")
        time.sleep(0.1)
    raise Failed(f"nothing listens on 127.0.0.1:{port} after {timeout}s")


def http(method, url, body=None, headers=None, timeout=30):
    """(status, headers, body). An HTTP error status is an answer, not an
    exception; a transport failure raises OSError/URLError."""
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read()
    except HTTPException as e:
        # A peer that died mid-answer (IncompleteRead, BadStatusLine): a
        # transport failure like any other.
        raise ConnectionError(f"{type(e).__name__}: {e}") from None


def esc(name):
    """connectors/queen-s3/src/layout.rs `escape`: everything outside
    [A-Za-z0-9._-] percent-encoded, uppercase hex, byte by byte."""
    out = []
    for b in name.encode("utf-8"):
        if 48 <= b <= 57 or 65 <= b <= 90 or 97 <= b <= 122 or b in b"._-":
            out.append(chr(b))
        else:
            out.append("%%%02X" % b)
    return "".join(out)


ISO_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{6})Z$")


def iso_us(s):
    """The broker's timestamp rendering (six fractional digits, `Z`) to
    microseconds since the epoch."""
    m = ISO_RE.match(s)
    if not m:
        raise ValueError(f"not a broker timestamp: {s!r}")
    return calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")) * 1_000_000 + int(m.group(2))


KV_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(?:Z|\+00:00)$")


def kv_ts_s(s):
    """A KV answer's timestamp (`…[.f]+00:00`, server/src/rsm/planner/kv.rs
    `ts_jsonb`) to epoch seconds."""
    m = KV_TS_RE.match(s or "")
    if not m:
        raise ValueError(f"not a KV timestamp: {s!r}")
    secs = calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
    return secs + int((m.group(2) or "0").ljust(6, "0")) / 1e6


def json_str(s):
    """A JSON string literal escaped exactly as serde_json escapes it: `"`,
    `\\`, the five short forms, other C0 controls as \\u00xx — nothing else."""
    return json.dumps(s, ensure_ascii=False).encode("utf-8")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def short(b, n=160):
    if isinstance(b, bytes):
        b = b.decode("utf-8", "replace")
    return b if len(b) <= n else b[:n] + f"...(+{len(b) - n})"


# --- the fake bucket ---------------------------------------------------------------


class FakeS3:
    def __init__(self, h, name="s3"):
        self.h = h
        self.root = os.path.join(h.work, name)
        self.port = free_ports(1)[0]
        self.log_path = os.path.join(h.work, "logs", f"fake_{name}.log")
        self.proc = None

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        log = open(self.log_path, "ab")
        buckets = []
        for b in BUCKETS:
            buckets += ["--bucket", b]
        self.proc = subprocess.Popen(
            [sys.executable, FAKE_S3, "--root", self.root, "--port", str(self.port)] + buckets,
            stdout=subprocess.DEVNULL,
            stderr=log,
            start_new_session=True,
        )
        log.close()
        wait_port(self.port, 20, self.proc)

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def admin(self, what):
        st, _, body = http("GET", f"{self.endpoint}/_fake/{what}")
        if st != 200:
            raise Failed(f"fake S3 /_fake/{what} -> {st}")
        return json.loads(body)

    def requests(self, key_prefix=""):
        """Every request the fake logged whose key starts with `key_prefix`."""
        out = []
        with open(self.log_path, "rb") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if (r.get("key") or "").startswith(key_prefix):
                    out.append(r)
        return out


# --- the broker ----------------------------------------------------------------------


class Broker:
    """One broker process: started, watched and stopped the way an
    orchestrator would."""

    def __init__(self, h, name, node_id=1):
        self.h = h
        self.name = name
        self.node_id = node_id
        self.http_port = free_ports(1)[0]
        self.raft_port = None
        # The embedded proxy's own port (QUEEN_PROXY_PORT) and control-plane
        # token, for the cells that run it.
        self.proxy_port = None
        self.cp_token = None
        # A cell's environment (embedded proxy, tenancy, encryption key),
        # applied on every start of this node.
        self.cell_env = {}
        self.cluster_env = {}
        self.data_dir = os.path.join(h.work, "data", name)
        self.log_path = os.path.join(h.work, "logs", f"{name}.log")
        self.proc = None
        self.starts = 0
        self.log_offset = 0  # where the current incarnation's log starts

    @property
    def url(self):
        return f"http://127.0.0.1:{self.http_port}"

    def start(self, env):
        os.makedirs(self.data_dir, exist_ok=True)
        full = {k: os.environ[k] for k in KEEP_ENV if k in os.environ}
        full.update(
            {
                "PORT": str(self.http_port),
                "QUEEN_BIND_ADDR": "127.0.0.1",
                "QUEEN_RAFT_DIR": self.data_dir,
                # This disk runs near full; the broker refuses writes above
                # its high mark (default 85%).
                "QUEEN_RAFT_DISK_HIGH_PCT": "99.9",
                "QUEEN_RAFT_DISK_LOW_PCT": "99.8",
                "RUST_LOG": self.h.rust_log,
            }
        )
        full.update(self.cluster_env)
        full.update(self.cell_env)
        full.update(env)
        self.starts += 1
        self.started_at = time.time()
        shown = " ".join(
            f"{k}={v}"
            for k, v in sorted(env.items())
            if k.startswith("QUEEN_S3_") and "KEY" not in k and "ENDPOINT" not in k
        )
        with open(self.log_path, "ab") as log:
            self.log_offset = log.tell()
            log.write(f"\n===== harness: start #{self.starts} of {self.name} at {time.strftime('%H:%M:%S')}: {shown}\n".encode())
            log.flush()
            self.proc = subprocess.Popen(
                [self.h.bin],
                env=full,
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
            st, _, body = http("GET", self.url + "/health", timeout=3)
            return json.loads(body)
        except (OSError, ValueError):
            return None

    def role(self):
        h = self.health()
        return (h or {}).get("raft", {}).get("role") if h else None

    def wait_healthy(self, timeout=90):
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

    def status(self):
        st, _, body = http("GET", self.url + "/status", timeout=5)
        if st != 200:
            raise Failed(f"{self.name} /status -> {st}: {short(body)}")
        return json.loads(body)

    def s3(self):
        """The `s3` block of /status: the manager's phase and one entry per
        tenant sink on this node under `sinks`."""
        return self.status().get("s3") or {}

    def s3_sinks(self):
        """{tenant: its sink's entry} on this node."""
        return {s.get("tenant"): s for s in self.s3().get("sinks", [])}

    def s3_sink(self, tenant=DEFAULT_TENANT):
        return self.s3_sinks().get(tenant) or {}

    def s3_rows(self, tenant=DEFAULT_TENANT):
        """{queue: row} of one tenant's sink on this node."""
        return {r["name"]: r for r in self.s3_sink(tenant).get("queues", [])}

    @property
    def proxy_url(self):
        return f"http://127.0.0.1:{self.proxy_port}"

    def cp(self, method, path, body=None, token=None, timeout=30):
        """A control-plane call on this node's embedded proxy: (status, JSON
        or None, raw body)."""
        headers = {"x-queen-cp-token": token or self.cp_token}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["content-type"] = "application/json"
        st, _, raw = http(method, self.proxy_url + "/api/cp" + path, data, headers, timeout=timeout)
        try:
            doc = json.loads(raw) if raw else None
        except ValueError:
            doc = None
        return st, doc, raw

    def metrics(self):
        st, _, body = http("GET", self.url + "/metrics/prometheus", timeout=10)
        if st != 200:
            raise Failed(f"{self.name} /metrics/prometheus -> {st}")
        return body.decode()

    def sigterm(self):
        self.proc.send_signal(signal.SIGTERM)
        return time.time()

    def kill9(self):
        if self.alive():
            self.proc.send_signal(signal.SIGKILL)
        return time.time()

    def wait_exit(self, timeout, expect=0):
        """Wait for the process to end; `expect` is the return code it must
        end with (None: any)."""
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

    def log(self, whole=False):
        with open(self.log_path, "rb") as f:
            if not whole:
                f.seek(self.log_offset)
            return f.read().decode("utf-8", "replace")

    def tail(self, n=30):
        try:
            return "\n".join(f"    | {l}" for l in self.log(whole=True).splitlines()[-n:])
        except OSError:
            return "    | (no log)"


def _no_core():
    try:
        import resource

        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    except Exception:  # noqa: BLE001 — best effort, in the child
        pass


def sink_env(h, queues, prefix, **overrides):
    """The sink's configuration, identical on every node of a scenario."""
    env = {
        "QUEEN_S3_EMBEDDED": "true",
        "QUEEN_S3_ENDPOINT": h.s3.endpoint,
        "QUEEN_S3_PATH_STYLE": "true",
        "QUEEN_S3_REGION": "us-east-1",
        "QUEEN_S3_BUCKET": BUCKET,
        "QUEEN_S3_ACCESS_KEY": ACCESS_KEY,
        "QUEEN_S3_SECRET_KEY": SECRET_KEY,
        "QUEEN_S3_QUEUES": ",".join(queues),
        "QUEEN_S3_PREFIX": prefix,
        "QUEEN_S3_START": "earliest",
        "QUEEN_S3_FORMAT": "jsonl",
        "QUEEN_S3_COMPRESSION": "gzip",
        "QUEEN_S3_ALIGN": "none",
        "QUEEN_S3_MAX_WINDOW_MS": "1000",
        "QUEEN_S3_DISCOVERY_INTERVAL_MS": "100",
        # 0: the windows close right at safeTime, so the claim that the raft
        # broker's safeTime is exact is what is being tested, not a margin.
        "QUEEN_S3_SAFE_GUARD_MS": "0",
        "QUEEN_S3_LEASE_TTL_MS": "3000",
        "QUEEN_S3_SHUTDOWN_GRACE_MS": "10000",
        "QUEEN_S3_CHECKPOINT_EVERY": "2",
    }
    env.update({k: str(v) for k, v in overrides.items()})
    return env


NO_SINK = {"QUEEN_S3_EMBEDDED": "false"}


def configure_queues(h, broker, queues):
    for q in queues:
        body = json.dumps({"queue": q, "options": {"retentionEnabled": False}}).encode()
        deadline = time.time() + 30
        while True:
            try:
                st, _, raw = http("POST", broker.url + "/api/v1/configure", body, {"content-type": "application/json"})
            except OSError as e:
                st, raw = None, str(e).encode()
            if st == 200:
                break
            if time.time() > deadline:
                raise Failed(f"configure {q!r} -> {st}: {short(raw)}")
            time.sleep(0.3)


def provision(h, broker, queues):
    """Create the queues BEFORE the sink starts.

    A queue that does not exist when the sink first discovers it stops with
    UNKNOWN_TOPIC_OR_PARTITION and is retried only after a minute
    (connectors/queen-s3/src/sink.rs TERMINAL_RETRY), so a fresh broker gets
    its queues in a boot of its own, with the sink off."""
    broker.start(NO_SINK)
    broker.wait_healthy()
    configure_queues(h, broker, queues)
    broker.stop()


# --- what was pushed -----------------------------------------------------------------


class Item:
    __slots__ = ("queue", "partition", "txn", "payload", "tenant")

    def __init__(self, queue, partition, txn, payload, tenant=DEFAULT_TENANT):
        self.queue = queue
        self.partition = partition
        self.txn = txn
        self.payload = payload  # raw JSON TEXT, never parsed on this side
        # The broker tenant it is pushed to (`x-queen-tenant`; the default
        # tenant sends no header).
        self.tenant = tenant


class Ledger:
    """Every push the broker acknowledged: (tenant, queue, partition, offset)
    -> (transactionId, payload text), from the push answers themselves."""

    def __init__(self):
        self.lock = threading.Lock()
        self.records = {}
        self.by_txn = {}
        self.attempted = {}
        self.duplicates = 0

    def attempt(self, items):
        with self.lock:
            for it in items:
                if it.txn is not None:
                    self.attempted.setdefault(it.txn, it)

    def ack(self, item, ans):
        st = ans.get("status")
        txn = ans.get("transaction_id")
        off = ans.get("offset")
        if item.txn is not None and txn != item.txn:
            raise Failed(f"push answered transaction_id {txn!r} for an item sent with {item.txn!r}")
        if not isinstance(off, int):
            raise Failed(f"push answer without an offset: {ans}")
        key = (item.tenant, item.queue, item.partition, off)
        with self.lock:
            if st == "duplicate":
                self.duplicates += 1
            prev = self.by_txn.get(txn)
            if prev is not None:
                if prev != key:
                    raise Failed(f"transaction {txn!r} acknowledged at {prev} and again at {key}")
                return
            other = self.records.get(key)
            if other is not None and other[0] != txn:
                raise Failed(f"offset {key} acknowledged to two transactions: {other[0]!r} and {txn!r}")
            self.records[key] = (txn, item.payload)
            self.by_txn[txn] = key

    def count(self, queue, tenant=DEFAULT_TENANT):
        with self.lock:
            return sum(1 for (t, q, _, _) in self.records if q == queue and t == tenant)

    def expected(self, queue, tenant=DEFAULT_TENANT):
        with self.lock:
            return {(p, o): v for (t, q, p, o), v in self.records.items() if q == queue and t == tenant}

    def unacknowledged(self):
        with self.lock:
            return [t for t in self.attempted if t not in self.by_txn]


def push_body(items):
    parts = []
    for it in items:
        s = '{"queue":' + json.dumps(it.queue) + ',"partition":' + json.dumps(it.partition)
        if it.txn is not None:
            s += ',"transactionId":' + json.dumps(it.txn)
        # Whitespace around the payload on purpose: it is not part of the value.
        s += ',"payload": ' + it.payload + " }"
        parts.append(s)
    return ('{"items":[' + ",".join(parts) + "]}").encode("utf-8")


def by_tenant(items):
    """`items` split by the tenant they are pushed to, in first-seen order: a
    push request names one tenant (its header)."""
    groups = {}
    for it in items:
        groups.setdefault(it.tenant, []).append(it)
    return list(groups.values())


def push_once(url, items, ledger, timeout=30):
    """One push attempt of items of ONE tenant. Returns (items not
    acknowledged, why)."""
    tenant = items[0].tenant
    if any(it.tenant != tenant for it in items):
        raise Failed("one push request carries one tenant: split the items with by_tenant()")
    ledger.attempt(items)
    headers = {"content-type": "application/json"}
    if tenant != DEFAULT_TENANT:
        headers["x-queen-tenant"] = tenant
    try:
        st, _, raw = http("POST", url + "/api/v1/push", push_body(items), headers, timeout=timeout)
    except (OSError, urllib.error.URLError) as e:
        return items, f"{type(e).__name__}: {e}"
    if st == 400 or st == 413:
        raise Failed(f"push refused with {st}: {short(raw, 400)}")
    if st != 201:
        return items, f"HTTP {st}: {short(raw)}"
    answers = json.loads(raw)
    if not isinstance(answers, list) or len(answers) != len(items):
        raise Failed(f"push answered {short(raw)} for {len(items)} items")
    rest = []
    for it, ans in zip(items, answers):
        if ans.get("status") in ("queued", "duplicate"):
            ledger.ack(it, ans)
        else:
            rest.append(it)
    return rest, (f"{len(rest)} items answered {answers[items.index(rest[0])]}" if rest else "")


def push(urls, items, ledger, *, retry=True, timeout_s=180, alive=None):
    """Push until every item is acknowledged, rotating over `urls` and
    retrying (an item with a transactionId is deduplicated by the broker, so a
    retry after an answer that was lost is safe). Returns the items left when
    `alive()` turns false — the broker died under the push."""
    left = []
    groups = by_tenant(items)
    for gi, group in enumerate(groups):
        rest = list(group)
        deadline = time.time() + timeout_s
        attempt = 0
        why = ""
        while rest:
            if alive is not None and not alive():
                return left + rest + [it for g in groups[gi + 1 :] for it in g]
            rest, why = push_once(urls[attempt % len(urls)], rest, ledger)
            attempt += 1
            if not rest:
                break
            if not retry and any(it.txn is None for it in rest):
                raise Failed(f"a push of items without a transactionId failed and cannot be retried safely: {why}")
            if time.time() > deadline:
                raise Failed(f"push not acknowledged after {timeout_s}s: {why}")
            time.sleep(0.2)
    return left


def push_batches(urls, items, ledger, batch=150, threads=3, retry=True):
    batches = [g[i : i + batch] for g in by_tenant(items) for i in range(0, len(g), batch)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        for _ in pool.map(lambda b: push(urls, b, ledger, retry=retry), batches):
            pass


class Pusher:
    """Pushes rounds of records in the background until stopped; what it was
    pushing when stopped is `leftover`, to be pushed again later."""

    def __init__(self, ledger, gen, urls, pause=0.05):
        self.ledger = ledger
        self.gen = gen
        self._urls = list(urls)
        self.lock = threading.Lock()
        self.pause = pause
        self.stop_evt = threading.Event()
        self.leftover = []
        self.rounds = 0
        self.failures = 0
        self.last_failure = ""
        self.fatal = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def set_urls(self, urls):
        with self.lock:
            self._urls = list(urls)

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        try:
            n = 0
            while not self.stop_evt.is_set():
                groups = by_tenant(self.gen(self.rounds))
                self.rounds += 1
                for gi, rest in enumerate(groups):
                    while rest and not self.stop_evt.is_set():
                        with self.lock:
                            urls = list(self._urls)
                        rest, why = push_once(urls[n % len(urls)], rest, self.ledger, timeout=15)
                        n += 1
                        if rest:
                            self.failures += 1
                            self.last_failure = why
                            time.sleep(0.2)
                    if rest:
                        self.leftover.extend(rest)
                        for g in groups[gi + 1 :]:
                            self.leftover.extend(g)
                        return
                time.sleep(self.pause)
        except Exception as e:  # noqa: BLE001 — reported by stop()
            self.fatal = e

    def stop(self):
        self.stop_evt.set()
        self.thread.join(60)
        if self.fatal:
            raise Failed(f"the background pusher failed: {self.fatal}")
        return self.leftover


# Payloads: raw JSON TEXT. Numbers beyond 64 bits, exponents, negative zero,
# escapes, raw non-ASCII (including U+2028, U+0085 and DEL, which JSON allows
# raw inside a string), odd whitespace. Never a raw newline or carriage return
# here: scenario e is about those.
BIGNUMS = [
    "123456789012345678901234567890",
    "-98765432109876543210987654321098765",
    "18446744073709551616",
    "9223372036854775808",
    "-9223372036854775809",
    "9007199254740993",
    "1.0000000000000000000000000000001",
    "-0",
    "0.000000000000000000000000000000000000000000001",
    "1E+400",
    "2.5e-400",
    "123456789012345678901234567890.123456789012345678901234567890e-5",
]
STRINGS = [
    "plain",
    'quote \\" inside',
    "back\\\\slash",
    "escapes \\n \\r \\t \\b \\f \\u0000 \\u001F \\/",
    "unicode é ü ß 日本語 \U0001f986",
    "escaped pair \\ud83e\\udd86 and \\u00e9",
    "raw line sep   para sep   nel \u0085 del \x7f",
    "html <tag> & 'apos'",
    "",
]


def gen_payload(rnd, tag, partition, i):
    kind = rnd.randrange(8)
    s = rnd.choice(STRINGS)
    n = rnd.choice(BIGNUMS)
    if kind == 0:
        text = '{"tag":"%s","i":%d,"big":%s,"s":"%s"}' % (tag, i, n, s)
    elif kind == 1:
        text = '{ "tag" : "%s" ,\t"nested" : { "a" : [ %s , %s , { "deep" : [ [ [ ] ] ] } ] } , "i" : %d }' % (
            tag,
            n,
            rnd.choice(BIGNUMS),
            i,
        )
    elif kind == 2:
        text = n
    elif kind == 3:
        text = '"%s #%d"' % (s, i)
    elif kind == 4:
        text = rnd.choice(["null", "true", "false", "[]", "{}", '""', "0"])
    elif kind == 5:
        text = "[" + ",".join(rnd.choice(BIGNUMS) for _ in range(5)) + "]"
    elif kind == 6:
        text = '{"pad":"%s","i":%d}' % ("x" * rnd.randrange(100, 4000), i)
    else:
        text = '{"tag":"%s","p":%s,"i":%d}' % (tag, json.dumps(partition, ensure_ascii=False), i)
    json.loads(text)  # the generator's own check that it wrote JSON; never compared parsed
    return text


# --- the lake ------------------------------------------------------------------------


PAYLOAD_SEP = b',"payload":'


def queue_root(prefix, queue, tenant=DEFAULT_TENANT):
    """Where one (tenant, queue)'s data objects live (layout.rs `data_key`)."""
    return f"{prefix}/tenant={esc(tenant)}/queue={esc(queue)}"


def sidecar_root(prefix, queue, tenant=DEFAULT_TENANT):
    """Where its manifests and checkpoints live (layout.rs `sidecar_root`)."""
    return f"{prefix}/_queen/tenant={esc(tenant)}/queue={esc(queue)}"


def manifest_key(prefix, queue, k, tenant=DEFAULT_TENANT):
    return f"{sidecar_root(prefix, queue, tenant)}/windows/{k:010d}.json"


def read_manifests(h, prefix, queue, tenant=DEFAULT_TENANT, bucket=BUCKET):
    d = os.path.join(h.bucket(bucket), *sidecar_root(prefix, queue, tenant).split("/"), "windows")
    out = {}
    if not os.path.isdir(d):
        return out
    for name in os.listdir(d):
        m = re.fullmatch(r"(\d{10})\.json", name)
        if not m:
            raise Failed(f"unexpected object in {d}: {name}")
        with open(os.path.join(d, name), "rb") as f:
            out[int(m.group(1))] = json.loads(f.read())
    return out


def data_keys(h, prefix, queue, tenant=DEFAULT_TENANT, bucket=BUCKET):
    root = h.bucket(bucket)
    d = os.path.join(root, *queue_root(prefix, queue, tenant).split("/"))
    keys = []
    for dirpath, _, files in os.walk(d):
        for f in files:
            keys.append(os.path.relpath(os.path.join(dirpath, f), root).replace(os.sep, "/"))
    return sorted(keys)


def all_files(h, prefix, bucket=BUCKET):
    root = h.bucket(bucket)
    out = {}
    for dirpath, _, files in os.walk(os.path.join(root, prefix)):
        for f in files:
            p = os.path.join(dirpath, f)
            out[os.path.relpath(p, root).replace(os.sep, "/")] = p
    return out


def fingerprint(h, prefix, bucket=BUCKET):
    return {k: sha256_file(p) for k, p in all_files(h, prefix, bucket).items()}


def object_path(h, key, bucket=BUCKET):
    return os.path.join(h.bucket(bucket), *key.split("/"))


def data_key(prefix, queue, k, t0, t1, ext, tenant=DEFAULT_TENANT):
    g = time.gmtime(max(t0, 0) // 1_000_000)
    return (
        f"{queue_root(prefix, queue, tenant)}/dt={time.strftime('%Y-%m-%d', g)}/hour={time.strftime('%H', g)}"
        f"/w-{k:010d}-{max(t0, 0):016d}-{t1:016d}.{ext}"
    )


def progress(h, prefix, queue, tenant=DEFAULT_TENANT, bucket=BUCKET):
    ms = read_manifests(h, prefix, queue, tenant, bucket)
    named = {o["key"] for m in ms.values() for o in m["objects"]}
    files = set(data_keys(h, prefix, queue, tenant, bucket))
    return {
        "windows": len(ms),
        "k": max(ms) if ms else 0,
        "records": sum(m["records"] for m in ms.values()),
        "orphans": sorted(files - named),
        "missing": sorted(named - files),
    }


def wait_lake(
    h,
    prefix,
    queues,
    ledger,
    brokers,
    timeout=180,
    nudge=None,
    what="every record pushed",
    tenant=DEFAULT_TENANT,
    bucket=BUCKET,
):
    """Wait until the lake holds exactly what the ledger says was pushed:
    the manifests' record counts match and every data object is named by a
    manifest (a crash between an object and its manifest leaves one that is
    not, until the window is redone)."""
    deadline = time.time() + timeout
    t0 = time.time()
    while True:
        prog = {q: progress(h, prefix, q, tenant, bucket) for q in queues}
        want = {q: ledger.count(q, tenant) for q in queues}
        over = {q: (p["records"], want[q]) for q, p in prog.items() if p["records"] > want[q]}
        if over:
            raise Failed(f"the lake {bucket}/{prefix} of tenant {tenant} holds MORE records than were pushed: {over}")
        if all(p["records"] == want[q] and not p["orphans"] and not p["missing"] for q, p in prog.items()):
            return prog, time.time() - t0
        if time.time() > deadline:
            lines = [f"timed out after {timeout}s waiting for {what} (bucket {bucket}, tenant {tenant}):"]
            for q, p in prog.items():
                lines.append(
                    f"  {q}: lake {p['records']}/{want[q]} records in {p['windows']} windows, "
                    f"orphans {p['orphans'][:2]}, missing {p['missing'][:2]}"
                )
            for b in brokers:
                if b.alive():
                    try:
                        rows = b.s3_rows(tenant)
                        for q in queues:
                            r = rows.get(q, {})
                            lines.append(
                                f"  {b.name} /status {q}: owned={r.get('ownedHere')} heldBy={r.get('heldBy')} "
                                f"state={r.get('state')} k={r.get('k')} records={r.get('records')} "
                                f"lastError={r.get('lastError')}"
                            )
                        sink = b.s3_sink(tenant)
                        lines.append(f"  {b.name} sink of {tenant}: phase={sink.get('phase')} error={sink.get('error')}")
                    except Exception as e:  # noqa: BLE001 — diagnostics only
                        lines.append(f"  {b.name} /status failed: {e}")
                    lines.append(b.tail(15))
                else:
                    lines.append(f"  {b.name} is not running")
            raise Failed("\n".join(lines))
        if nudge:
            nudge()
        time.sleep(0.5)


def parse_line(ln):
    """One JSONL line -> (partition, offset, txn, ts, raw payload bytes), the
    envelope checked byte for byte against the documented spelling."""
    obj = json.loads(ln)
    if list(obj.keys()) != ["partition", "offset", "transactionId", "ts", "payload"]:
        raise ValueError(f"fields {list(obj.keys())}")
    i = ln.find(PAYLOAD_SEP)
    head = (
        b'{"partition":'
        + json_str(obj["partition"])
        + b',"offset":'
        + str(obj["offset"]).encode()
        + b',"transactionId":'
        + json_str(obj["transactionId"])
        + b',"ts":'
        + json_str(obj["ts"])
    )
    if i < 0 or ln[:i] != head:
        raise ValueError(f"envelope is not byte-exact: {short(ln[: max(i, 0) + 10])}")
    if not ln.endswith(b"}"):
        raise ValueError("line does not end with }")
    return obj["partition"], obj["offset"], obj["transactionId"], obj["ts"], ln[i + len(PAYLOAD_SEP) : -1]


def lake_form(payload_text):
    """What the lake holds for a pushed payload: the text itself, with every
    raw newline and carriage return written as a space so the record stays
    one JSONL line (writer/jsonl.rs `splice_payload`: exact for valid JSON,
    where a raw line break can only be whitespace between tokens)."""
    return payload_text.replace("\n", " ").replace("\r", " ").encode("utf-8")


def verify_queue(
    h,
    prefix,
    queue,
    expected,
    *,
    sink="default",
    compression="gzip",
    ext="jsonl.gz",
    tenant=DEFAULT_TENANT,
    bucket=BUCKET,
    complete=True,
):
    """Everything the lake must be for one (tenant, queue), read off the
    bucket directory alone. `expected` is {(partition, offset): (txn, payload
    text)} from the push answers. With `complete=False` the lake may hold only
    a prefix of each partition (a sink stopped mid-stream): every record it
    holds must still be acknowledged, exactly once, byte-exact, and each
    partition's offsets must run from 0 without a gap. Raises Failed listing
    what is wrong."""
    problems = []

    def bad(msg):
        if len(problems) < 25:
            problems.append(msg)

    ms = read_manifests(h, prefix, queue, tenant, bucket)
    ks = sorted(ms)
    if not ks and complete:
        bad("no manifest at all")
    elif ks != list(range(1, len(ks) + 1)):
        bad(f"window numbers are not 1..{len(ks)}: {ks[:8]}")
    seen = {}
    named = set()
    prev_end = None
    total_bytes = 0
    lines_total = 0
    for k in ks:
        m = ms[k]
        for f, want in (
            ("sink", sink),
            ("tenant", tenant),
            ("queue", queue),
            ("k", k),
            ("format", "jsonl"),
            ("compression", compression),
            ("layout", "merged"),
        ):
            if m.get(f) != want:
                bad(f"manifest {k}: {f}={m.get(f)!r}, expected {want!r}")
        if not str(m.get("writer", "")).startswith("queen-s3/"):
            bad(f"manifest {k}: writer {m.get('writer')!r}")
        if m.get("lost"):
            bad(f"manifest {k} names lost ranges {m['lost']}")
        t0, t1 = m["tStart"], m["tEnd"]
        if not t0 < t1:
            bad(f"manifest {k}: tStart {t0} is not before tEnd {t1}")
        if prev_end is not None and t0 != prev_end:
            bad(f"window {k} starts at {t0} but window {k - 1} ended at {prev_end}: the windows do not tile")
        prev_end = t1
        if len(m["objects"]) != 1:
            bad(f"manifest {k} has {len(m['objects'])} objects; a merged window has exactly one")
        parts_in = set()
        min_ts = max_ts = None
        obj_records = obj_bytes = 0
        for o in m["objects"]:
            key = o["key"]
            named.add(key)
            want_key = data_key(prefix, queue, k, t0, t1, ext, tenant)
            if key != want_key:
                bad(f"manifest {k} names {key}, the layout says {want_key}")
            path = object_path(h, key, bucket)
            if not os.path.isfile(path):
                bad(f"manifest {k} names {key}, which is not in the bucket")
                continue
            with open(path, "rb") as f:
                raw = f.read()
            if len(raw) != o["bytes"]:
                bad(f"{key}: {len(raw)} bytes, manifest says {o['bytes']}")
            if hashlib.sha256(raw).hexdigest() != o["sha256"]:
                bad(f"{key}: sha256 differs from the manifest's")
            try:
                text = gzip.decompress(raw) if compression == "gzip" else raw
            except (OSError, EOFError) as e:
                bad(f"{key}: not a gzip stream: {e}")
                continue
            if text and not text.endswith(b"\n"):
                bad(f"{key}: does not end with a newline")
            lines = text.split(b"\n")[:-1]
            lines_total += len(lines)
            if len(lines) != o["records"]:
                bad(f"{key}: {len(lines)} lines, manifest says {o['records']} records")
            last = None
            for n, ln in enumerate(lines):
                try:
                    part, off, txn, ts_s, payload = parse_line(ln)
                    ts = iso_us(ts_s)
                except ValueError as e:
                    bad(f"{key} line {n + 1}: {e}: {short(ln)}")
                    continue
                order = (part.encode("utf-8"), off)
                if last is not None and not order > last:
                    bad(f"{key} line {n + 1}: ({part!r}, {off}) follows {last}: not sorted by (partition, offset)")
                last = order
                if not t0 <= ts < t1:
                    bad(f"{key} line {n + 1}: ts {ts_s} outside the window [{t0}, {t1})")
                min_ts = ts if min_ts is None else min(min_ts, ts)
                max_ts = ts if max_ts is None else max(max_ts, ts)
                parts_in.add(part)
                pk = (part, off)
                if pk in seen:
                    bad(f"({part!r}, {off}) is in the lake twice: window {seen[pk][0]} and window {k}")
                    continue
                seen[pk] = (k, ts)
                exp = expected.get(pk)
                if exp is None:
                    bad(f"({part!r}, {off}) txn {txn!r} in window {k} was never acknowledged by a push to this tenant")
                    continue
                if txn != exp[0]:
                    bad(f"({part!r}, {off}): transactionId {txn!r}, the push answered {exp[0]!r}")
                if payload != lake_form(exp[1]):
                    bad(f"({part!r}, {off}): payload {short(payload)} differs from what was pushed: {short(exp[1])}")
            obj_records += o["records"]
            obj_bytes += o["bytes"]
        total_bytes += obj_bytes
        if m["records"] != obj_records:
            bad(f"manifest {k}: records {m['records']} but its objects hold {obj_records}")
        if m["bytes"] != obj_bytes:
            bad(f"manifest {k}: bytes {m['bytes']} but its objects are {obj_bytes}")
        if m.get("partitions") != len(parts_in):
            bad(f"manifest {k}: partitions {m.get('partitions')} but the object holds {len(parts_in)}")
        if m.get("minTs") != min_ts or m.get("maxTs") != max_ts:
            bad(f"manifest {k}: minTs/maxTs {m.get('minTs')}/{m.get('maxTs')}, the records say {min_ts}/{max_ts}")
    for orphan in sorted(set(data_keys(h, prefix, queue, tenant, bucket)) - named):
        bad(f"{orphan} is in the bucket but no manifest names it")
    missing = [pk for pk in expected if pk not in seen]
    if missing and complete:
        bad(f"{len(missing)} acknowledged records are not in the lake, e.g. {sorted(missing)[:3]}")
    by_part = defaultdict(list)
    for (p, o), (_, ts) in seen.items():
        by_part[p].append((o, ts))
    for p, rows in by_part.items():
        rows.sort()
        offs = [o for o, _ in rows]
        if offs != list(range(len(offs))):
            bad(f"partition {p!r}: offsets in the lake are not 0..{len(offs) - 1} (first {offs[:5]})")
        tss = [t for _, t in rows]
        if any(b < a for a, b in zip(tss, tss[1:])):
            bad(f"partition {p!r}: ts goes backwards with the offset")
    exp_parts = defaultdict(list)
    for (p, o) in expected:
        exp_parts[p].append(o)
    for p, offs in exp_parts.items():
        if sorted(offs) != list(range(len(offs))):
            bad(f"partition {p!r}: the push answers' offsets are not 0..{len(offs) - 1}")
    if problems:
        raise Failed(
            f"lake check of {queue!r} (tenant {tenant}) under {bucket}/{prefix}/ failed:\n  - " + "\n  - ".join(problems)
        )
    return {
        "windows": len(ks),
        "objects": len(named),
        "records": lines_total,
        "partitions": len(by_part),
        "bytes": total_bytes,
        "missing": len(missing),
    }


# --- KV ----------------------------------------------------------------------------------


def kv_key(queue, what, sink="default"):
    return f"s3:{sink}:{esc(queue)}:{what}"


def kv_get(broker, key):
    body = json.dumps({"operations": [{"ns": KV_NS, "op": "get", "key": key}]}).encode()
    st, _, raw = http("POST", broker.url + "/api/v1/kv", body, {"content-type": "application/json"}, timeout=10)
    if st != 200:
        raise Failed(f"kv get {key} via {broker.name} -> {st}: {short(raw)}")
    doc = json.loads(raw)
    return doc["results"][0]


def metric(text, name, labels):
    total = None
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        m = re.match(re.escape(name) + r"(\{[^}]*\})?\s+([0-9.eE+-]+)$", line.strip())
        if not m:
            continue
        if labels and labels not in (m.group(1) or ""):
            continue
        total = (total or 0.0) + float(m.group(2))
    return total


def series(text, name):
    """Every sample of metric `name`: [(labels dict, value)], labels parsed
    exactly (so `{queue="q"}` and `{tenant="t",queue="q"}` are told apart)."""
    out = []
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = re.match(re.escape(name) + r"(?:\{([^}]*)\})?\s+([0-9.eE+-]+)$", line.strip())
        if not m:
            continue
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', m.group(1) or ""))
        out.append((labels, float(m.group(2))))
    return out


def wait_owned(h, nodes, queues, timeout=60, tenant=DEFAULT_TENANT):
    """Until every queue of `tenant` is owned (ownedHere) by exactly one live
    node."""
    deadline = time.time() + timeout
    while True:
        owners = owners_now(nodes, tenant)
        if all(len(owners.get(q, [])) == 1 for q in queues):
            return {q: owners[q][0] for q in queues}
        if time.time() > deadline:
            raise Failed(f"queues not owned by exactly one node after {timeout}s: {owners}")
        time.sleep(0.2)


def owners_now(nodes, tenant=DEFAULT_TENANT):
    owners = defaultdict(list)
    for b in nodes:
        if not b.alive():
            continue
        try:
            for name, r in b.s3_rows(tenant).items():
                if r.get("ownedHere"):
                    owners[name].append(b.name)
        except Exception:  # noqa: BLE001 — a node that does not answer owns nothing it can show
            pass
    return dict(owners)


COMMIT_RE = re.compile(r"window committed queue=(\S+) k=(\d+) records=(\d+)")
# The sink's lines sit inside its spans (`sink{tenant=…}:queue{queue=…}:`)
# between the level and the target, so any span text is allowed there.
FENCE_RE = re.compile(
    r"(\S+Z) +WARN (?:\S+: )?queen-s3: queue fenced queue=(\S+) why=\"((?:[^\"\\]|\\.)*)\""
)


def commits_in_log(text):
    return [(m.group(1), int(m.group(2)), int(m.group(3))) for m in COMMIT_RE.finditer(text)]


def fences_in_log(text):
    """(time, queue, why) of every `queue fenced` line: on a single node
    there is no other instance, so every one of them is the node fencing
    ITSELF (scenario f)."""
    return [(m.group(1), m.group(2), m.group(3)) for m in FENCE_RE.finditer(text)]


def cleanup(h, brokers, prefixes, bucket=BUCKET):
    if h.args.keep:
        return
    for b in brokers:
        shutil.rmtree(b.data_dir, ignore_errors=True)
    for p in prefixes:
        shutil.rmtree(os.path.join(h.bucket(bucket), p), ignore_errors=True)
        shutil.rmtree(os.path.join(h.s3.root, ".fakes3", "meta", bucket, p), ignore_errors=True)


# =============================================================================================
# Scenarios
# =============================================================================================


def scenario_s3(h):
    """The fake answers like S3: integrity checks, HEAD/GET/404 shapes, LIST
    paging both ways, multipart with its ETag rule and its refusals, abort."""
    base = f"{h.s3.endpoint}/{BUCKET}"
    pre = "selftest"

    def put(key, body, headers=None):
        return http("PUT", f"{base}/{pre}/{key}", body, headers or {})

    def code_of(raw):
        m = re.search(rb"<Code>([^<]+)</Code>", raw)
        return m.group(1).decode() if m else None

    import base64

    body = b"hello lake\n"
    md5 = hashlib.md5(body)
    st, hd, _ = put("a.txt", body, {"Content-MD5": base64.b64encode(md5.digest()).decode()})
    assert_eq(st, 200, "PUT with a right Content-MD5")
    assert_eq(hd.get("ETag"), f'"{md5.hexdigest()}"', "PUT ETag is the quoted MD5")
    st, _, raw = put("bad.txt", body, {"Content-MD5": base64.b64encode(b"0" * 16).decode()})
    assert_eq((st, code_of(raw)), (400, "BadDigest"), "PUT with a wrong Content-MD5")
    st, _, raw = put("bad.txt", body, {"x-amz-content-sha256": "0" * 64})
    assert_eq((st, code_of(raw)), (400, "XAmzContentSHA256Mismatch"), "PUT with a wrong payload hash")
    st, hd, raw = http("HEAD", f"{base}/{pre}/a.txt")
    assert_eq((st, hd.get("Content-Length"), hd.get("ETag"), raw), (200, str(len(body)), f'"{md5.hexdigest()}"', b""), "HEAD")
    st, _, raw = http("HEAD", f"{base}/{pre}/nope")
    assert_eq((st, raw), (404, b""), "HEAD of a missing key")
    st, _, raw = http("HEAD", f"{base}/{pre}/")
    assert_eq(st, 404, "HEAD of a prefix (the sink's bucket probe)")
    st, _, raw = http("GET", f"{base}/{pre}/a.txt")
    assert_eq((st, raw), (200, body), "GET")
    st, _, raw = http("GET", f"{base}/{pre}/nope")
    assert_eq((st, code_of(raw), b"<Key>selftest/nope</Key>" in raw), (404, "NoSuchKey", True), "GET of a missing key")
    st, _, raw = http("GET", f"{h.s3.endpoint}/no-such-bucket/x")
    assert_eq((st, code_of(raw)), (404, "NoSuchBucket"), "another bucket")
    # Keys that need escaping on the wire, and the sink's own key alphabet.
    odd = "queue=a%2Fb/dt=2026-10-02/w-1 + é.json"
    st, _, _ = http("PUT", f"{base}/{pre}/" + urllib.request.quote(odd), b"{}")
    assert_eq(st, 200, "PUT of a key with = % + space and UTF-8")
    st, _, raw = http("GET", f"{base}/{pre}/" + urllib.request.quote(odd))
    assert_eq((st, raw), (200, b"{}"), "GET of that key")
    # LIST paging: start-after (what the sink sends) and continuation-token.
    for i in range(5):
        put(f"lst/k{i}", b"x")
    names = []
    after = None
    for _ in range(5):
        q = f"list-type=2&prefix={pre}/lst/&max-keys=2" + (f"&start-after={urllib.request.quote(after)}" if after else "")
        st, _, raw = http("GET", f"{base}?{q}")
        keys = [k.decode() for k in re.findall(rb"<Key>([^<]+)</Key>", raw)]
        names += keys
        if b"<IsTruncated>true</IsTruncated>" not in raw:
            break
        after = keys[-1]
    assert_eq(names, [f"{pre}/lst/k{i}" for i in range(5)], "LIST paged by start-after")
    names, token = [], None
    for _ in range(5):
        q = f"list-type=2&prefix={pre}/lst/&max-keys=2" + (f"&continuation-token={urllib.request.quote(token)}" if token else "")
        st, _, raw = http("GET", f"{base}?{q}")
        names += [k.decode() for k in re.findall(rb"<Key>([^<]+)</Key>", raw)]
        m = re.search(rb"<NextContinuationToken>([^<]+)</NextContinuationToken>", raw)
        if not m:
            break
        token = m.group(1).decode()
    assert_eq(names, [f"{pre}/lst/k{i}" for i in range(5)], "LIST paged by continuation-token")
    st, _, raw = http("GET", f"{base}?list-type=2&prefix={pre}/&delimiter=/")
    assert_eq(re.findall(rb"<Prefix>([^<]+)</Prefix>", raw)[1:], [b"selftest/lst/", b"selftest/queue=a%2Fb/"], "LIST with a delimiter")

    # Multipart: two parts, the first at the 5 MiB floor.
    p1 = os.urandom(5 * 1024 * 1024)
    p2 = b"tail" * 1000
    st, _, raw = http("POST", f"{base}/{pre}/mp.bin?uploads", b"")
    upload = re.search(rb"<UploadId>([^<]+)</UploadId>", raw).group(1).decode()
    st1, h1, _ = http("PUT", f"{base}/{pre}/mp.bin?partNumber=1&uploadId={upload}", p1)
    st2, h2, _ = http("PUT", f"{base}/{pre}/mp.bin?partNumber=2&uploadId={upload}", p2)
    assert_eq((st1, st2), (200, 200), "UploadPart")
    e1, e2 = h1["ETag"].strip('"'), h2["ETag"].strip('"')
    xml = f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{e1}\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"{e2}\"</ETag></Part></CompleteMultipartUpload>"
    st, _, raw = http("POST", f"{base}/{pre}/mp.bin?uploadId={upload}", xml.encode())
    want = hashlib.md5(hashlib.md5(p1).digest() + hashlib.md5(p2).digest()).hexdigest() + "-2"
    assert_eq((st, re.search(rb"<ETag>([^<]+)</ETag>", raw).group(1).decode()), (200, f"&quot;{want}&quot;"), "CompleteMultipartUpload ETag")
    st, hd, raw = http("GET", f"{base}/{pre}/mp.bin")
    assert_eq((st, raw == p1 + p2, hd.get("ETag")), (200, True, f'"{want}"'), "GET of the assembled object")
    # A part below 5 MiB that is not the last is refused, a wrong ETag too.
    st, _, raw = http("POST", f"{base}/{pre}/small.bin?uploads", b"")
    up2 = re.search(rb"<UploadId>([^<]+)</UploadId>", raw).group(1).decode()
    _, ha, _ = http("PUT", f"{base}/{pre}/small.bin?partNumber=1&uploadId={up2}", b"a" * 10)
    _, hb, _ = http("PUT", f"{base}/{pre}/small.bin?partNumber=2&uploadId={up2}", b"b" * 10)
    xml = f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{ha['ETag']}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{hb['ETag']}</ETag></Part></CompleteMultipartUpload>"
    st, _, raw = http("POST", f"{base}/{pre}/small.bin?uploadId={up2}", xml.encode())
    assert_eq((st, code_of(raw)), (400, "EntityTooSmall"), "a non-last part below 5 MiB")
    xml = f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{'0' * 32}\"</ETag></Part></CompleteMultipartUpload>"
    st, _, raw = http("POST", f"{base}/{pre}/small.bin?uploadId={up2}", xml.encode())
    assert_eq((st, code_of(raw)), (400, "InvalidPart"), "a part ETag that does not match")
    listed = [u["uploadId"] for u in h.s3.admin("uploads")]
    assert_eq(up2 in listed, True, "an upload in progress is listed")
    st, _, _ = http("DELETE", f"{base}/{pre}/small.bin?uploadId={up2}")
    assert_eq(st, 204, "AbortMultipartUpload")
    st, _, raw = http("POST", f"{base}/{pre}/small.bin?uploadId={up2}", xml.encode())
    assert_eq((st, code_of(raw)), (404, "NoSuchUpload"), "completing an aborted upload")
    assert_eq(up2 in [u["uploadId"] for u in h.s3.admin("uploads")], False, "an aborted upload is gone")
    # Injected faults: the next GET of a.txt throttled, then served again.
    h.s3.admin("fail?op=get&status=503&code=SlowDown&count=1&retryAfter=2&match=a.txt")
    st, hd, raw = http("GET", f"{base}/{pre}/a.txt")
    assert_eq((st, code_of(raw), hd.get("Retry-After")), (503, "SlowDown", "2"), "an injected 503 SlowDown")
    assert_eq(http("GET", f"{base}/{pre}/a.txt")[:1], (200,), "the request after the injected fault")
    h.s3.admin("fail?clear=1")
    st, _, _ = http("DELETE", f"{base}/{pre}/a.txt")
    st2, _, _ = http("DELETE", f"{base}/{pre}/a.txt")
    assert_eq((st, st2), (204, 204), "DELETE, twice")
    assert_eq(http("GET", f"{base}/{pre}/a.txt")[0], 404, "GET after DELETE")
    shutil.rmtree(os.path.join(h.bucket_dir, pre), ignore_errors=True)
    shutil.rmtree(os.path.join(h.s3.root, ".fakes3", "meta", BUCKET, pre), ignore_errors=True)
    return "fake S3: PUT/HEAD/GET/DELETE, Content-MD5 and payload-hash checks, LIST by start-after, token and delimiter, multipart (ETag rule, EntityTooSmall, InvalidPart, abort) all answer like S3"


def assert_eq(got, want, what):
    if got != want:
        raise Failed(f"{what}: got {got!r}, expected {want!r}")


def iso_from_us(us):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(us // 1_000_000)) + ".%06dZ" % (us % 1_000_000)


def write_lake(h, prefix, queue, windows):
    """Write a lake by hand, the way the sink writes one: `windows` is a list
    of (tStart, tEnd, [(partition, offset, txn, ts_us, payload text)])."""
    for k, (t0, t1, recs) in enumerate(windows, start=1):
        lines = b""
        for p, o, txn, ts, payload in recs:
            lines += (
                b'{"partition":' + json_str(p) + b',"offset":' + str(o).encode() + b',"transactionId":'
                + json_str(txn) + b',"ts":' + json_str(iso_from_us(ts)) + PAYLOAD_SEP + payload.encode() + b"}\n"
            )
        body = gzip.compress(lines, mtime=0)
        key = data_key(prefix, queue, k, t0, t1, "jsonl.gz")
        os.makedirs(os.path.dirname(object_path(h, key)), exist_ok=True)
        with open(object_path(h, key), "wb") as f:
            f.write(body)
        tss = [r[3] for r in recs]
        man = {
            "sink": "default", "tenant": DEFAULT_TENANT, "queue": queue, "k": k, "tStart": t0, "tEnd": t1, "format": "jsonl",
            "compression": "gzip", "layout": "merged",
            "objects": [{"key": key, "bytes": len(body), "records": len(recs), "sha256": hashlib.sha256(body).hexdigest()}],
            "records": len(recs), "bytes": len(body), "partitions": len({r[0] for r in recs}),
            "minTs": min(tss), "maxTs": max(tss), "lost": [], "writer": "queen-s3/1.5.0 jsonl+gzip",
            "committedAt": "2026-10-02T00:00:00.000000Z",
        }
        mk = manifest_key(prefix, queue, k)
        os.makedirs(os.path.dirname(object_path(h, mk)), exist_ok=True)
        with open(object_path(h, mk), "w") as f:
            json.dump(man, f)


def scenario_self(h):
    """The checker catches what it exists to catch: a lake written by hand is
    accepted, and each kind of damage done to it is refused with its reason."""
    q = "self.q"
    base = 1_790_000_000_000_000
    recs = [("a", 0, "t-a0", base + 10, '{"n":1}'), ("a", 1, "t-a1", base + 20, "123456789012345678901234567890"),
            ("b é", 0, "t-b0", base + 10, '"x"'), ("a", 2, "t-a2", base + 1010, "null"), ("b é", 1, "t-b1", base + 1020, "[1, 2]")]
    expected = {(p, o): (t, pl) for p, o, t, _, pl in recs}

    def good():
        w1 = sorted([r for r in recs if r[3] < base + 1000], key=lambda r: (r[0].encode(), r[1]))
        w2 = sorted([r for r in recs if r[3] >= base + 1000], key=lambda r: (r[0].encode(), r[1]))
        return [(base + 10, base + 1000, w1), (base + 1000, base + 2000, w2)]

    mutations = {
        "payload text": (lambda w: w[0][2].__setitem__(0, w[0][2][0][:4] + ('{"n":2}',)), "differs from what was pushed"),
        "transactionId": (lambda w: w[0][2].__setitem__(0, w[0][2][0][:2] + ("t-zz",) + w[0][2][0][3:]), "transactionId"),
        "duplicate": (lambda w: w[1][2].insert(0, (w[0][2][0][0], w[0][2][0][1], w[0][2][0][2], base + 1000, w[0][2][0][4])), "twice"),
        "missing record": (lambda w: w[1][2].pop(), "not in the lake"),
        "order": (lambda w: w[0][2].reverse(), "not sorted"),
        "ts outside": (lambda w: w[0][2].__setitem__(0, w[0][2][0][:3] + (base + 5000,) + w[0][2][0][4:]), "outside the window"),
        "tiling": (lambda w: w.__setitem__(1, (base + 1001,) + w[1][1:]), "do not tile"),
    }
    prefix = "self-check"
    write_lake(h, prefix, q, good())
    verify_queue(h, prefix, q, expected)
    caught = []
    for name, (mutate, needle) in mutations.items():
        shutil.rmtree(os.path.join(h.bucket_dir, prefix), ignore_errors=True)
        w = [(a, b, list(c)) for a, b, c in good()]
        mutate(w)
        write_lake(h, prefix, q, w)
        try:
            verify_queue(h, prefix, q, expected)
        except Failed as e:
            if needle not in str(e):
                raise Failed(f"checker self-test: {name} was refused for another reason: {e}")
            caught.append(name)
            continue
        raise Failed(f"checker self-test: a lake with a broken {name} was ACCEPTED")
    # Damage to the files themselves: an orphan object, and bytes that no longer
    # match the manifest.
    shutil.rmtree(os.path.join(h.bucket_dir, prefix), ignore_errors=True)
    write_lake(h, prefix, q, good())
    orphan = object_path(h, data_key(prefix, q, 9, base, base + 1, "jsonl.gz"))
    with open(orphan, "wb") as f:
        f.write(gzip.compress(b""))
    try:
        verify_queue(h, prefix, q, expected)
        raise Failed("checker self-test: an orphan object was ACCEPTED")
    except Failed as e:
        if "no manifest names it" not in str(e):
            raise
        caught.append("orphan object")
    os.unlink(orphan)
    victim = object_path(h, data_key(prefix, q, 1, base + 10, base + 1000, "jsonl.gz"))
    with open(victim, "ab") as f:
        f.write(b"\0")
    try:
        verify_queue(h, prefix, q, expected)
        raise Failed("checker self-test: an object that does not match its manifest was ACCEPTED")
    except Failed as e:
        if "sha256 differs" not in str(e):
            raise
        caught.append("bytes vs manifest")
    shutil.rmtree(os.path.join(h.bucket_dir, prefix), ignore_errors=True)
    return f"the lake checker accepts a hand-written lake and refuses each of: {', '.join(caught)}"


# --- a: scale --------------------------------------------------------------------------------


def partition_names(n, special=True):
    names = [f"cust-{i:05d}" for i in range(n)]
    if special and n >= 8:
        # Names a lake reader meets as JSON strings: a space, a slash, UTF-8,
        # an emoji, a quote, a backslash, a percent sign and a colon.
        names[1] = "ünï cødé/1"
        names[2] = "p:2 100%"
        names[3] = "\U0001f986-3"
        names[4] = 'q"uote\\4'
        names[5] = "日本-5"
    return names


def scenario_a(h):
    args = h.args
    queues = ["a.orders", "a.events_v2", "a-metrics"]
    widths = [int(x) for x in args.a_partitions.split(",")]
    widths = (widths * 3)[:3]
    # The widest queue goes past one discovery page (1000 partitions) and one
    # fetch call (1024 entries), so the sink pages through both.
    parts_of = {q: partition_names(w) for q, w in zip(queues, widths)}
    prefix = "a-scale"
    node = Broker(h, "a-node1")
    provision(h, node, queues)
    node.start(sink_env(h, queues, prefix))
    node.wait_healthy()
    owners = wait_owned(h, [node], queues)
    h.say(f"  a: sink up, owns {sorted(owners)}")

    ledger = Ledger()
    rnd = random.Random(20261002)
    t_push = time.time()
    for r in range(args.a_records):
        items = []
        for q in queues:
            for i, p in enumerate(parts_of[q]):
                # One item in five carries no transactionId: the broker mints
                # one, and the lake must carry the minted one.
                txn = None if (r + i) % 5 == 0 else f"a-{q}-{i}-{r}"
                items.append(Item(q, p, txn, gen_payload(rnd, "a", p, r)))
        rnd.shuffle(items)
        push_batches([node.url], items, ledger, batch=150, threads=3, retry=False)
        # Rounds spaced out so that windows close between them: the lake is
        # written while the pushing goes on, not after it.
        time.sleep(args.a_pause)
    push_s = time.time() - t_push
    total = sum(ledger.count(q) for q in queues)
    h.say(f"  a: pushed {total} records in {push_s:.1f}s; waiting for the lake")
    _, waited = wait_lake(h, prefix, queues, ledger, [node], timeout=300)

    stats = {q: verify_queue(h, prefix, q, ledger.expected(q)) for q in queues}
    rows = node.s3_rows()
    text = node.metrics()
    for q in queues:
        r = rows[q]
        if not r["ownedHere"] or r["records"] != ledger.count(q) or r["windowsCommitted"] != stats[q]["windows"] or r["recordsLost"]:
            raise Failed(f"/status for {q} disagrees with the lake: {r} vs {stats[q]}")
        written = metric(text, "queen_s3_records_written_total", f'queue="{q}"')
        if written != ledger.count(q):
            raise Failed(f"queen_s3_records_written_total{{queue={q}}} = {written}, pushed {ledger.count(q)}")
        committed = kv_get(node, kv_key(q, "committed"))
        doc = committed.get("value") or {}
        last = read_manifests(h, prefix, q)[stats[q]["windows"]]
        if doc.get("k") != stats[q]["windows"] or iso_us(doc.get("tEnd")) != last["tEnd"]:
            raise Failed(f"commit pointer {doc} does not match the last manifest (k={stats[q]['windows']}, tEnd={last['tEnd']})")
    node.stop(40)
    self_fences = fences_in_log(node.log(whole=True))
    cleanup(h, [node], [prefix])
    w = sum(s["windows"] for s in stats.values())
    o = sum(s["objects"] for s in stats.values())
    return (
        f"{len(queues)} queues of {'/'.join(str(len(parts_of[q])) for q in queues)} partitions x {args.a_records} = {total} records "
        f"({sum(1 for t in ledger.by_txn if not t.startswith('a-'))} with broker-minted transactionIds) pushed in {push_s:.1f}s "
        f"while the sink ran; lake complete {waited:.1f}s after the last push: {w} windows, {o} objects, "
        f"{sum(s['records'] for s in stats.values())} records, each (queue, partition, offset) exactly once, "
        f"payload text and transactionId identical, offsets contiguous, objects sorted; /status, metrics and "
        f"commit pointers agree"
        + (f"; the node fenced ITSELF {len(self_fences)} time(s) on the way (bug f): {[(q, w_) for _, q, w_ in self_fences]}" if self_fences else "")
    )


# --- b: the crash matrix -------------------------------------------------------------------


def crash_items(rnd, queue, parts, tag, n):
    return [
        Item(queue, p, f"{tag}-{i}-{j}", gen_payload(rnd, tag, p, j))
        for j in range(n)
        for i, p in enumerate(parts)
    ]


def scenario_b(h):
    lines = []
    for point in h.args.crash_points:
        lines.append(crash_point(h, point))
    return "; ".join(lines)


def crash_point(h, point):
    q = "b.crash"
    parts = [f"lane-{i}" for i in range(8)]
    prefix = f"b-{point}"
    node = Broker(h, f"b-{point}")
    provision(h, node, [q])
    env = sink_env(h, [q], prefix)
    ledger = Ledger()
    rnd = random.Random(point)

    # 1. A first, honest run, so the crash lands on a window that has
    #    committed windows behind it.
    node.start(env)
    node.wait_healthy()
    for r in range(3):
        push([node.url], crash_items(rnd, q, parts, f"A{r}", 10), ledger)
        wait_lake(h, prefix, [q], ledger, [node], timeout=120, what="the warm-up records")
    node.stop(40)

    # 2. The armed run: push until the sink aborts the broker.
    node.start({**env, "QUEEN_S3_CRASH_AT": point})
    node.wait_healthy()
    batch_b = crash_items(rnd, q, parts, "B", 20)
    leftover = []
    for i in range(0, len(batch_b), 16):
        leftover += push([node.url], batch_b[i : i + 16], ledger, alive=node.alive, timeout_s=60)
        if not node.alive():
            leftover += batch_b[i + 16 :]
            break
        time.sleep(0.3)
    try:
        rc = node.proc.wait(90)
    except subprocess.TimeoutExpired:
        node.kill9()
        node.wait_exit(10, expect=None)
        raise Failed(f"{point}: the broker never reached its crash point\n{node.tail()}") from None
    h.live.discard(node)
    fired = f"QUEEN_S3_CRASH_AT fired: aborting" in node.log() or "aborting between multipart parts" in node.log()
    if rc != -signal.SIGABRT or not fired:
        raise Failed(f"{point}: the broker exited {rc}, expected an abort (SIGABRT) at the crash point\n{node.tail()}")
    snapshot = fingerprint(h, prefix)

    # 3. What the crash left, read with the sink OFF so nothing redoes it first.
    node.start({**env, **NO_SINK})
    node.wait_healthy()
    intent = kv_get(node, kv_key(q, "intent")).get("value")
    committed = kv_get(node, kv_key(q, "committed")).get("value")
    node.stop(40)
    ik, ck = intent["k"], committed["k"]
    inflight_key = data_key(prefix, q, ik, intent["tStart"], intent["tEnd"], "jsonl.gz")
    man_key = manifest_key(prefix, q, ik)
    has_obj, has_man = inflight_key in snapshot, man_key in snapshot
    want = {
        "after_intent": (ck + 1, False, False),
        "mid_upload": (ck + 1, True, False),
        "after_upload": (ck + 1, True, True),
        "before_commit": (ck + 1, True, True),
        "after_commit": (ck, True, True),
    }[point]
    if (ik, has_obj, has_man) != want:
        raise Failed(
            f"{point}: after the crash intent k={ik}, committed k={ck}, window object present={has_obj}, "
            f"manifest present={has_man}; expected intent k={want[0]}, object={want[1]}, manifest={want[2]}"
        )

    # 4. Restart without the crash point, push more, and let it settle.
    node.start(env)
    node.wait_healthy()
    push([node.url], leftover + crash_items(rnd, q, parts, "C", 10), ledger)
    wait_lake(h, prefix, [q], ledger, [node], timeout=120)
    stats = verify_queue(h, prefix, q, ledger.expected(q))
    after = fingerprint(h, prefix)
    changed = [k for k, d in snapshot.items() if after.get(k) != d]
    if changed:
        raise Failed(f"{point}: objects present before the crash changed or vanished across the restart: {changed}")
    # Which of them were PUT again after the restart (with identical bytes, as
    # the fingerprint just showed).
    rewrites = rewritten_keys(h, prefix, snapshot, node)
    node.stop(40)
    log = node.log()
    restored = re.search(r"queue restored queue=\S+ committed_k=(\d+) redo=(\w+)", log)
    want_redo = "false" if point == "after_commit" else "true"
    if not restored or restored.group(2) != want_redo:
        raise Failed(f"{point}: the restart restored {restored and restored.group(0)!r}, expected redo={want_redo}")
    finished = "committing it from its manifest" in log
    if point in ("after_upload", "before_commit") and not finished:
        raise Failed(f"{point}: the redo did not commit the finished upload from its manifest\n{node.tail()}")
    if point in ("after_intent", "mid_upload") and finished:
        raise Failed(f"{point}: a window whose upload never finished was committed from a manifest")
    cleanup(h, [node], [prefix])
    return (
        f"{point}: abort rc={rc} at intent k={ik} (committed k={ck}); {len(snapshot)} objects before the restart, "
        f"all byte-identical after ({len(rewrites)} re-PUT with the same bytes{': ' + ', '.join(sorted(rewrites)) if rewrites else ''}); "
        f"{stats['windows']} windows, {stats['records']} records exactly once"
        + ("; finished upload committed from its manifest" if finished else "")
    )


def rewritten_keys(h, prefix, snapshot, node):
    """Keys of `snapshot` that were PUT again during the last incarnation of
    `node` (the restart after the crash), from the fake S3's request log."""
    out = set()
    for r in h.s3.requests(prefix + "/"):
        if r["method"] == "PUT" and r["status"] == 200 and r["key"] in snapshot and r["t"] >= node.started_at:
            out.add(r["key"].rsplit("/", 1)[-1])
    return out


# --- b2: mid_upload inside a real multipart upload ------------------------------------


def scenario_b2(h):
    q = "b2.big"
    parts = [f"lane-{i}" for i in range(4)]
    prefix = "b2-multipart"
    node = Broker(h, "b2-node1")
    provision(h, node, [q])
    # No compression, so a window of ~20 MB of JSONL is a ~20 MB object: above
    # the 5 MB multipart threshold and above one 16 MiB part, so the upload has
    # a "between two parts" for the crash point to land in.
    big = dict(
        QUEEN_S3_COMPRESSION="none",
        QUEEN_S3_MULTIPART_THRESHOLD_MB=5,
        QUEEN_S3_TARGET_MB=20,
    )
    env = sink_env(h, [q], prefix, **big)
    ledger = Ledger()
    rnd = random.Random(2)

    def items(tag, n):
        out = []
        for j in range(n):
            for i, p in enumerate(parts):
                pad = "".join(rnd.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(64)) * 128
                out.append(Item(q, p, f"{tag}-{i}-{j}", '{"tag":"%s","j":%d,"big":%s,"pad":"%s"}' % (tag, j, rnd.choice(BIGNUMS), pad)))
        return out

    # A first, unarmed run: under start=earliest the FIRST window opens at -inf
    # and is closed at once, whatever its size (window.rs `aged`), so it would
    # take the crash point with a one-PUT object. Commit it here.
    node.start(env)
    node.wait_healthy()
    push([node.url], [Item(q, p, f"W-{i}", '{"warm":%d}' % i) for i, p in enumerate(parts)], ledger)
    wait_lake(h, prefix, [q], ledger, [node], timeout=120, what="the warm-up records")
    node.stop(40)

    # The armed run, with windows that close by size only (ten minutes of age).
    node.start({**env, "QUEEN_S3_CRASH_AT": "mid_upload", "QUEEN_S3_MAX_WINDOW_MS": "600000"})
    node.wait_healthy()
    batch = items("M", 850)  # 3400 records of ~8.3 KB: ~28 MB of payload
    leftover = []
    for i in range(0, len(batch), 40):
        leftover += push([node.url], batch[i : i + 40], ledger, alive=node.alive, timeout_s=120)
        if not node.alive():
            leftover += batch[i + 40 :]
            break
    try:
        rc = node.proc.wait(120)
    except subprocess.TimeoutExpired:
        node.kill9()
        node.wait_exit(10, expect=None)
        raise Failed(f"b2: the broker never aborted between multipart parts\n{node.tail()}") from None
    h.live.discard(node)
    if rc != -signal.SIGABRT or "aborting between multipart parts" not in node.log():
        raise Failed(f"b2: exit {rc}, expected the multipart mid_upload abort\n{node.tail()}")
    stranded = [u for u in h.s3.admin("uploads") if u["key"].startswith(prefix + "/")]
    if len(stranded) != 1 or len(stranded[0]["parts"]) != 1 or stranded[0]["parts"][0]["bytes"] != PART_SIZE:
        raise Failed(f"b2: expected one stranded upload holding one 16 MiB part, found {stranded}")
    stranded_key = stranded[0]["key"]
    part1 = os.path.join(h.s3.root, ".fakes3", "uploads", stranded[0]["uploadId"], "part-00001")
    part1_sha = sha256_file(part1)
    if os.path.exists(object_path(h, stranded_key)):
        raise Failed(f"b2: {stranded_key} exists although its upload never completed")
    snapshot = fingerprint(h, prefix)

    # Restart unarmed, with age-closed windows again so the tail ships too.
    node.start(env)
    node.wait_healthy()
    push([node.url], leftover + items("N", 25), ledger)
    wait_lake(h, prefix, [q], ledger, [node], timeout=300)
    stats = verify_queue(h, prefix, q, ledger.expected(q), compression="none", ext="jsonl")
    after = fingerprint(h, prefix)
    changed = [k for k, d in snapshot.items() if after.get(k) != d]
    if changed:
        raise Failed(f"b2: objects present before the crash changed: {changed}")
    if not os.path.exists(object_path(h, stranded_key)):
        raise Failed(f"b2: the redone window {stranded_key} is not in the bucket")
    with open(object_path(h, stranded_key), "rb") as f:
        head = f.read(PART_SIZE)
        size = PART_SIZE + len(f.read())
    if hashlib.sha256(head).hexdigest() != part1_sha:
        raise Failed("b2: the redone object's first 16 MiB differ from the part uploaded before the crash")
    completes = [r for r in h.s3.requests(prefix + "/") if r["op"] == "multipart_complete" and r["status"] == 200]
    redone = [r for r in completes if r["key"] == stranded_key]
    if not redone or redone[0]["info"]["parts"] < 2:
        raise Failed(f"b2: the redone window was not completed as a multipart upload of 2+ parts: {redone}")
    still = [u for u in h.s3.admin("uploads") if u["key"] == stranded_key]
    node.stop(40)
    cleanup(h, [node], [prefix])
    for u in still:
        shutil.rmtree(os.path.join(h.s3.root, ".fakes3", "uploads", u["uploadId"]), ignore_errors=True)
    return (
        f"abort between parts 1 and 2 of window {stranded_key.rsplit('/', 1)[-1]}; restart redid it as a "
        f"{redone[0]['info']['parts']}-part upload of {size} bytes whose first part is byte-identical to the stranded one; "
        f"{len(completes)} multipart completes in all; {stats['windows']} windows, {stats['records']} records "
        f"exactly once; the stranded upload is {'still listed (left to the bucket lifecycle rule)' if still else 'gone'}"
    )


# --- c: SIGTERM drain ------------------------------------------------------------------------


def scenario_c(h):
    return "; ".join(
        [
            # The window being filled when the signal lands.
            drain_case(h, "filling", put_delay_ms=0, grace_ms=10_000, ttl_ms=60_000),
            # A window whose upload is in flight and finishes inside the grace:
            # the drain must commit it, then give the leases back.
            drain_case(h, "upload", put_delay_ms=2_500, grace_ms=10_000, ttl_ms=60_000),
            # An upload that outlives the grace: the broker exits at the grace,
            # the window stays an intent, and the next start redoes it.
            drain_case(h, "cut", put_delay_ms=12_000, grace_ms=4_000, ttl_ms=3_000),
        ]
    )


def drain_case(h, label, put_delay_ms, grace_ms, ttl_ms):
    tag = {"filling": "cf", "upload": "cu", "cut": "cc"}[label]
    queues = [f"{tag}.q0", f"{tag}.q1"]
    nudge_q = f"{tag}.nudge"
    parts = partition_names(50, special=False)
    prefix = f"c-{label}"
    node = Broker(h, f"c-{label}")
    provision(h, node, queues + [nudge_q])
    # TTL 60 s where the release is checked: a lease the stopping node did NOT
    # give back would still be alive when it is read after the restart.
    env = sink_env(h, queues, prefix, QUEEN_S3_LEASE_TTL_MS=ttl_ms, QUEEN_S3_SHUTDOWN_GRACE_MS=grace_ms)
    ledger = Ledger()
    rnd = random.Random(label)

    def gen(r):
        return [Item(q, p, f"{tag}-{q}-{i}-{r}", gen_payload(rnd, tag, p, r)) for q in queues for i, p in enumerate(parts)]

    def nudge():
        # With a minute of lease TTL the refresh no longer moves the log clock
        # every second: a write to a queue nobody sinks does.
        push_once(node.url, [Item(nudge_q, "n", None, "0")], Ledger())

    node.start(env)
    node.wait_healthy()
    wait_owned(h, [node], queues)
    pusher = Pusher(ledger, gen, [node.url], pause=0.02).start()
    deadline = time.time() + 120
    while any(progress(h, prefix, q)["windows"] < 3 for q in queues):
        if time.time() > deadline:
            raise Failed(f"c/{label}: fewer than 3 windows per queue after 120s of traffic\n{node.tail()}")
        time.sleep(0.2)
    before = {q: progress(h, prefix, q)["windows"] for q in queues}
    if put_delay_ms:
        # Data objects only (`<prefix>/tenant=…/queue=…`), never a sidecar.
        h.s3.admin(f"delay?ms={put_delay_ms}&count=1&match={urllib.request.quote(prefix + '/tenant=', safe='')}")
        deadline = time.time() + 30
        while h.s3.admin("inflight")["held"] < 1:
            if time.time() > deadline:
                raise Failed(f"c/{label}: no data object PUT reached the fake S3 in 30s")
            time.sleep(0.02)
    t_term = node.sigterm()
    try:
        rc = node.proc.wait(grace_ms / 1000 + 25)
    except subprocess.TimeoutExpired:
        node.kill9()
        raise Failed(f"c/{label}: the broker did not exit within the grace ({grace_ms} ms) + 25 s\n{node.tail()}") from None
    exit_s = time.time() - t_term
    h.live.discard(node)
    leftover = pusher.stop()
    log1 = node.log()
    if rc != 0:
        raise Failed(f"c/{label}: SIGTERM exit code {rc}\n{node.tail()}")
    if put_delay_ms:
        # The held PUT lands when its delay is over, broker or no broker.
        deadline = time.time() + put_delay_ms / 1000 + 10
        while h.s3.admin("inflight")["held"] > 0:
            if time.time() > deadline:
                raise Failed(f"c/{label}: the held PUT never finished")
            time.sleep(0.1)
        h.s3.admin("delay?clear=1")
    drained = sorted(re.findall(r"queue drained queue=(\S+)", log1))
    decisions = re.findall(
        r"shutting down: stopping the reads queue=(\S+) buffered=(\d+) aged=(\w+) closing=(\w+) state=(\S+)", log1
    )
    stop_at = log1.find("stopping: no new reads")
    drain_commits = commits_in_log(log1[stop_at:]) if stop_at >= 0 else []
    cut = "did not stop inside its grace window" in log1
    grace_s = grace_ms / 1000
    if label == "cut":
        if not cut:
            raise Failed(f"c/cut: an upload held {put_delay_ms} ms did not hit the {grace_ms} ms grace\n{node.tail()}")
        if not grace_s - 0.5 <= exit_s <= grace_s + 10:
            raise Failed(f"c/cut: exit after {exit_s:.1f}s; the grace is {grace_s}s")
    else:
        if cut or drained != sorted(queues):
            raise Failed(f"c/{label}: drained {drained} (cut short: {cut}), expected every queue\n{node.tail()}")
        if exit_s > grace_s + 10:
            raise Failed(f"c/{label}: exit took {exit_s:.1f}s, more than the grace + 10 s")
    if label == "upload":
        if not drain_commits:
            raise Failed(f"c/upload: the window whose upload was in flight was not committed by the drain\n{node.tail()}")
        if exit_s < put_delay_ms / 1000 - 1.0:
            raise Failed(f"c/upload: exit after {exit_s:.1f}s, before the {put_delay_ms} ms upload could have finished")

    # The KV rows, read with the sink OFF so nothing touches them first.
    node.start({**env, **NO_SINK})
    node.wait_healthy()
    leases = {q: kv_get(node, kv_key(q, "lease")) for q in queues}
    pointers = {
        q: ((kv_get(node, kv_key(q, "intent")).get("value") or {}).get("k"), (kv_get(node, kv_key(q, "committed")).get("value") or {}).get("k"))
        for q in queues
    }
    node.stop(40)
    dangling = {q: ik for q, (ik, ck) in pointers.items() if ik != ck}
    if label != "cut":
        live = {q: r for q, r in leases.items() if r.get("found")}
        if live:
            raise Failed(f"c/{label}: lease rows still present after a clean stop (TTL {ttl_ms} ms, so NOT released): {live}")
        if dangling:
            raise Failed(f"c/{label}: an intent is ahead of its commit after a drain that finished: {pointers}")
    elif len(dangling) != 1 or any(ik != ck + 1 for q, (ik, ck) in pointers.items() if q in dangling):
        raise Failed(f"c/cut: expected exactly one window left as an intent (k = commit + 1), pointers {pointers}")
    snapshot = fingerprint(h, prefix)

    # Restart with the sink, push what was in flight and some more, settle.
    node.start(env)
    node.wait_healthy()
    wait_owned(h, [node], queues, timeout=2 * ttl_ms / 1000 + 20)
    claim_s = time.time() - node.started_at
    push([node.url], leftover, ledger)
    push_batches([node.url], gen(10_000), ledger)
    wait_lake(h, prefix, queues, ledger, [node], timeout=180, nudge=nudge)
    stats = {q: verify_queue(h, prefix, q, ledger.expected(q)) for q in queues}
    after = fingerprint(h, prefix)
    changed = [k for k, d in snapshot.items() if after.get(k) != d]
    if changed:
        raise Failed(f"c/{label}: objects changed across the restart: {changed}")
    rewrites = rewritten_keys(h, prefix, snapshot, node)
    node.stop(40)
    redo = re.findall(r"queue restored queue=(\S+) committed_k=(\d+) redo=true", node.log())
    if label == "cut" and [q for q, _ in redo] != list(dangling):
        raise Failed(f"c/cut: the restart redid {redo}, expected the window left as an intent in {list(dangling)}")
    cleanup(h, [node], [prefix])
    what = {
        "filling": "SIGTERM while windows fill",
        "upload": f"SIGTERM while a data object PUT is held {put_delay_ms} ms by the fake S3",
        "cut": f"SIGTERM while a data object PUT is held {put_delay_ms} ms, longer than the {grace_ms} ms grace",
    }[label]
    tail = {
        "filling": "lease rows gone (TTL 60 s: released, not expired), intent == commit",
        "upload": f"the drain committed {[(q, k) for q, k, _ in drain_commits]} before exiting, lease rows gone (TTL 60 s), intent == commit",
        "cut": f"the grace cut the drain, {list(dangling)} left at intent k = commit + 1, the restart redid it "
        f"({len(rewrites)} object re-PUT with identical bytes: {sorted(rewrites)})",
    }[label]
    return (
        f"{label}: {what}: exit rc=0 after {exit_s:.2f}s (grace {grace_ms} ms); drain decisions "
        f"{[(d[0], 'close' if d[3] == 'true' else 'abandon', d[4]) for d in decisions]}; {tail}; "
        f"restart owned both queues {claim_s:.1f}s after its start; {len(leftover)} in-flight items re-pushed; "
        + ", ".join(f"{q} {s['windows']}w/{s['records']}r" for q, s in stats.items())
        + " exactly once"
    )


# --- d: three nodes ------------------------------------------------------------------------


def scenario_d(h):
    queues = [f"d.q{i}" for i in range(6)]
    parts = partition_names(16, special=False)
    prefix = "d-cluster"
    ttl_ms = 3000
    grace_ms = 10_000
    nodes = [Broker(h, f"d-node{i}", node_id=i) for i in (1, 2, 3)]
    raft_ports = free_ports(3)
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
    by_name = {b.name: b for b in nodes}
    report = []

    # Provision: the cluster forms once with the sink off, the queues are
    # created, every node stops.
    for b in nodes:
        b.start(NO_SINK)
    for b in nodes:
        b.wait_healthy(120)
    configure_queues(h, nodes[0], queues)
    for b in nodes:
        b.sigterm()
    for b in nodes:
        b.wait_exit(60)

    env = sink_env(h, queues, prefix, QUEEN_S3_LEASE_TTL_MS=ttl_ms, QUEEN_S3_SHUTDOWN_GRACE_MS=grace_ms)
    for b in nodes:
        b.start(env)
    for b in nodes:
        b.wait_healthy(120)
    owners = wait_owned(h, nodes, queues, timeout=60)
    roles = {b.name: b.role() for b in nodes}
    instances = {b.name: b.s3_sink().get("instance") for b in nodes}
    # Every node's /status agrees on who owns each queue: the owner says
    # ownedHere, every other node names the owner's lease instance.
    deadline = time.time() + 2 * ttl_ms / 1000 + 10
    while True:
        rows = {b.name: b.s3_rows() for b in nodes}
        disagree = [
            (q, b.name, rows[b.name].get(q, {}).get("heldBy"))
            for q in queues
            for b in nodes
            if b.name != owners[q] and rows[b.name].get(q, {}).get("heldBy") != instances[owners[q]]
        ]
        if not disagree:
            break
        if time.time() > deadline:
            raise Failed(f"d: /status of the non-owners does not name the owner: {disagree[:6]}")
        time.sleep(0.3)
    spread = sorted(set(owners.values()))
    h.say(f"  d: roles {roles}; owners {owners}")
    report.append(
        f"cold start, sink on all 3 nodes: roles {roles}; the {len(queues)} queues are owned by {len(spread)} node(s) "
        f"{spread}, and every other node's /status names that owner's lease instance"
    )

    ledger = Ledger()
    rnd = random.Random(4)

    def gen(r):
        return [Item(q, p, f"d-{q}-{i}-{r}", gen_payload(rnd, "d", p, r)) for q in queues for i, p in enumerate(parts)]

    pusher = Pusher(ledger, gen, [b.url for b in nodes], pause=0.05).start()

    # At least one queue owned AND committed by a FOLLOWER: its reads served
    # from the follower's own applied state, its KV writes (intent, commit,
    # lease) forwarded to the leader. "Committed as a follower" = two samples,
    # both with the node a follower and owning the queue, two windows apart.
    def follower_owned():
        out = {}
        for b in nodes:
            if not b.alive() or b.role() != "follower":
                continue
            for q, r in b.s3_rows().items():
                if r.get("ownedHere"):
                    out[q] = (b.name, r.get("windowsCommitted", 0))
        return out

    def follower_commits(timeout=15):
        first = {}
        deadline = time.time() + timeout
        while time.time() < deadline:
            now = follower_owned()
            for q, (n, w) in now.items():
                if first.get(q, (None,))[0] != n:
                    first[q] = (n, w)
            done = {q: (n, w - first[q][1]) for q, (n, w) in now.items() if first[q][0] == n and w - first[q][1] >= 2}
            if done:
                return done
            time.sleep(0.3)
        return {}

    moves = []
    while True:
        fc = follower_commits()
        if fc:
            break
        if len(moves) >= 5:
            raise Failed(f"d: no follower owned and committed a queue, after {len(moves)} moves {moves}; owners {owners_now(nodes)}")
        # Every queue is on the leader: move them with a rolling restart of
        # the owner, and look again.
        owner = by_name[owners_now(nodes)[queues[0]][0]]
        pusher.set_urls([b.url for b in nodes if b is not owner])
        owner.stop(grace_ms / 1000 + 20)
        moved = wait_owned(h, [b for b in nodes if b is not owner], queues, timeout=2 * ttl_ms / 1000 + 20)
        moves.append((owner.name, sorted(set(moved.values()))))
        owner.start(env)
        owner.wait_healthy(120)
        wait_caught_up(nodes, owner)
        pusher.set_urls([b.url for b in nodes])
    report.append(
        f"queues committed by a follower: {fc}"
        + (f" (after moving them off the leader with {len(moves)} rolling restart(s): {moves})" if moves else "")
    )
    h.say(f"  d: {report[-1]}")
    deadline = time.time() + 60
    while any(progress(h, prefix, q)["windows"] < 2 for q in queues):
        if time.time() > deadline:
            raise Failed(f"d: not every queue has 2 windows: {[progress(h, prefix, q) for q in queues]}")
        time.sleep(0.3)

    # --- kill -9 the node that owns the most queues ---------------------------------------
    owners = wait_owned(h, nodes, queues)
    counts = defaultdict(list)
    for q, n in owners.items():
        counts[n].append(q)
    victim = by_name[max(counts, key=lambda n: (len(counts[n]), n))]
    lost = sorted(counts[victim.name])
    survivors = [b for b in nodes if b is not victim]
    pusher.set_urls([b.url for b in survivors])
    time.sleep(1.0)
    victim_role = victim.role()
    base_rows = {b.name: b.s3_rows() for b in survivors}
    t_kill = victim.kill9()
    victim.wait_exit(10, expect=None)
    claimed, first_commit = {}, {}
    deadline = t_kill + 2 * ttl_ms / 1000 + 25
    while len(first_commit) < len(lost):
        for b in survivors:
            try:
                rows = b.s3_rows()
            except Exception:  # noqa: BLE001 — a node mid-election may not answer
                continue
            for q in lost:
                r = rows.get(q, {})
                if r.get("ownedHere") and q not in claimed:
                    claimed[q] = (b.name, time.time() - t_kill)
                # Credited to whichever survivor owns it when it commits: the
                # first claimer may lose it again (scenario f) before it does.
                if r.get("ownedHere") and q not in first_commit:
                    before = base_rows[b.name].get(q, {}).get("windowsCommitted", 0)
                    if r.get("windowsCommitted", 0) > before:
                        first_commit[q] = time.time() - t_kill
        if time.time() > deadline:
            raise Failed(f"d: after kill -9 of {victim.name}, claimed {claimed}, committed {first_commit} of {lost} within {deadline - t_kill:.0f}s")
        time.sleep(0.1)
    worst_claim = max(s for _, s in claimed.values())
    if worst_claim > 2 * ttl_ms / 1000 + 10:
        raise Failed(f"d: takeover took {worst_claim:.1f}s, more than 2 x TTL + 10 s")
    report.append(
        f"kill -9 {victim.name} ({victim_role}, owned {lost}): taken over "
        + ", ".join(f"{q} by {claimed[q][0]} in {claimed[q][1]:.1f}s (first commit {first_commit[q]:.1f}s)" for q in lost)
    )
    h.say(f"  d: {report[-1]}")

    # The killed node comes back and rejoins.
    victim.start(env)
    victim.wait_healthy(120)
    wait_caught_up(nodes, victim)
    report.append(f"{victim.name} restarted and caught up")

    # --- SIGTERM (rolling restart) of a node that owns queues -----------------------------------
    owners = wait_owned(h, nodes, queues)
    counts = defaultdict(list)
    for q, n in owners.items():
        counts[n].append(q)
    stopping = by_name[max(counts, key=lambda n: (len(counts[n]), n))]
    moving = sorted(counts[stopping.name])
    others = [b for b in nodes if b is not stopping]
    pusher.set_urls([b.url for b in others])
    time.sleep(1.0)
    stopping_role = stopping.role()
    base_rows = {b.name: b.s3_rows() for b in others}
    watch = LeaseWatch(others[0], moving, instances[stopping.name]).start()
    log_before_term = len(stopping.log())
    t_term = stopping.sigterm()
    try:
        rc = stopping.proc.wait(grace_ms / 1000 + 20)
    except subprocess.TimeoutExpired:
        stopping.kill9()
        raise Failed(f"d: {stopping.name} did not exit within grace + 20 s after SIGTERM\n{stopping.tail()}") from None
    exit_s = time.time() - t_term
    h.live.discard(stopping)
    if rc != 0:
        raise Failed(f"d: {stopping.name} exited {rc} on SIGTERM\n{stopping.tail()}")
    claimed2 = {}
    deadline = t_term + 2 * ttl_ms / 1000 + 25
    while len(claimed2) < len(moving):
        for b in others:
            try:
                rows = b.s3_rows()
            except Exception:  # noqa: BLE001
                continue
            for q in moving:
                if rows.get(q, {}).get("ownedHere") and q not in claimed2:
                    claimed2[q] = (b.name, time.time() - t_term)
        if time.time() > deadline:
            raise Failed(f"d: after SIGTERM of {stopping.name}, claimed {claimed2} of {moving}")
        time.sleep(0.1)
    released = watch.stop()
    log_term = stopping.log()
    drained = sorted(re.findall(r"queue drained queue=(\S+)", log_term[log_before_term:]))
    # A queue the node had fenced itself out of before the signal (scenario f)
    # is not one it runs any more, so it has nothing to give back: its row
    # stays until it expires. Only the queues it drained must be released.
    fenced_before = sorted({q for _, q, _ in fences_in_log(log_term[:log_before_term]) if q in moving and q not in drained})
    late = {q: v for q, v in released.items() if q in drained and not v.get("before_expiry")}
    if sorted(set(drained) | set(fenced_before)) != moving:
        raise Failed(f"d: {stopping.name} drained {drained}, had fenced itself out of {fenced_before}, of {moving}")
    # A lease a drained queue did not give back is a failure, but one that
    # costs hand-over time, not data: the scenario carries on to its lake
    # checks and fails at the end, with everything it saw.
    deferred = None
    if late:
        deferred = (
            f"d: SIGTERM of {stopping.name} ({stopping_role}): drained {drained}, but the lease rows of "
            f"{sorted(late)} were only seen gone AFTER their own expiresAt (margins "
            + ", ".join(f"{q} {v['margin_s']:.3f}s" for q, v in sorted(late.items()))
            + "): they expired, they were not released (see scenario k)"
        )
    report.append(
        f"SIGTERM {stopping.name} ({stopping_role}, owned {moving}): exit rc=0 in {exit_s:.2f}s, drained {drained}, "
        f"leases released ahead of expiry by "
        + ", ".join(f"{q} {v['margin_s']:.1f}s" for q, v in sorted(released.items()) if q in drained and q not in late)
        + (f"; NOT released, expired: {sorted(late)}" if late else "")
        + (f"; NOT released (fenced itself out before the signal, bug f; rows expired): {fenced_before}" if fenced_before else "")
        + "; handed over "
        + ", ".join(f"{q} to {claimed2[q][0]} in {claimed2[q][1]:.1f}s" for q in moving)
    )
    h.say(f"  d: {report[-1]}")
    stopping.start(env)
    stopping.wait_healthy(120)
    wait_caught_up(nodes, stopping)
    report.append(f"{stopping.name} restarted and caught up")

    # --- settle and verify ----------------------------------------------------------------------
    time.sleep(2.0)
    leftover = pusher.stop()
    urls = [b.url for b in nodes]
    push(urls, leftover, ledger)
    push_batches(urls, gen(10_000), ledger)
    unacked = ledger.unacknowledged()
    if unacked:
        raise Failed(f"d: {len(unacked)} pushed items never acknowledged, e.g. {unacked[:3]}")
    _, waited = wait_lake(h, prefix, queues, ledger, nodes, timeout=240)
    stats = {q: verify_queue(h, prefix, q, ledger.expected(q)) for q in queues}

    # No window was committed by two nodes (the lease fence), as the logs tell it.
    by_k = defaultdict(set)
    for b in nodes:
        for q, k, _ in commits_in_log(b.log(whole=True)):
            by_k[(q, k)].add(b.name)
    twice = {qk: n for qk, n in by_k.items() if len(n) > 1}
    if twice:
        raise Failed(f"d: windows committed by more than one node: {twice}")
    per_node = defaultdict(int)
    for (q, k), n in by_k.items():
        per_node[next(iter(n))] += 1
    fenced = {b.name: len(fences_in_log(b.log(whole=True))) for b in nodes}
    final_owners = owners_now(nodes)
    for b in nodes:
        b.sigterm()
    for b in nodes:
        b.wait_exit(60)
    cleanup(h, nodes, [prefix])
    report.append(
        f"final owners {final_owners}; windows committed per node {dict(per_node)}, none by two nodes; "
        f"lake complete {waited:.1f}s after the last push: "
        + ", ".join(f"{q} {s['windows']}w/{s['records']}r" for q, s in stats.items())
        + f" = {sum(s['records'] for s in stats.values())} records exactly once ({ledger.duplicates} push retries answered duplicate); "
        f"'queue fenced' lines per node {fenced}"
    )
    if deferred:
        raise Failed(deferred + ". Everything else passed: " + "; ".join(report))
    return "; ".join(report)


def wait_caught_up(nodes, node, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader = next((b for b in nodes if b.alive() and b.role() == "leader"), None)
        if leader is not None:
            lh, nh = leader.health(), node.health()
            if lh and nh and nh["raft"]["applied"] >= lh["raft"]["commit"]:
                return
        time.sleep(0.3)
    raise Failed(f"{node.name} not caught up with the leader after {timeout}s")


class LeaseWatch:
    """Reads the lease rows of `queues` through `via` until stopped, and
    reports, per queue, whether the row of `instance` disappeared (or was
    taken by another instance) BEFORE its own expiresAt — which only a
    release can do."""

    def __init__(self, via, queues, instance):
        self.via = via
        self.queues = queues
        self.instance = instance
        self.stop_evt = threading.Event()
        self.obs = defaultdict(list)
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self, timeout=15):
        """Start watching, and return once every queue's row has been seen
        held by `instance` — only then is a later absence evidence."""
        self.thread.start()
        deadline = time.time() + timeout
        while True:
            seen = {q for q in self.queues if any(self._mine(r) for _, r in list(self.obs[q]))}
            if seen == set(self.queues):
                return self
            if time.time() > deadline:
                self.stop_evt.set()
                raise Failed(f"lease rows of {sorted(set(self.queues) - seen)} never seen held by {self.instance}")
            time.sleep(0.02)

    def _mine(self, r):
        return r.get("found") and (r.get("value") or {}).get("instance") == self.instance

    def _run(self):
        while not self.stop_evt.is_set():
            for q in self.queues:
                try:
                    r = kv_get(self.via, kv_key(q, "lease"))
                except Exception:  # noqa: BLE001 — a read lost to an election is just a gap
                    continue
                self.obs[q].append((time.time(), r))
            time.sleep(0.02)

    def stop(self):
        self.stop_evt.set()
        self.thread.join(10)
        out = {}
        for q in self.queues:
            last_mine = None
            for t, r in self.obs[q]:
                inst = (r.get("value") or {}).get("instance") if r.get("found") else None
                if inst == self.instance:
                    last_mine = r
                    continue
                if last_mine is None:
                    continue
                expires = kv_ts_s(last_mine["expiresAt"]) if last_mine.get("expiresAt") else None
                out[q] = {
                    "seen_at": t,
                    "now": "absent" if not r.get("found") else f"held by {inst}",
                    "expires_at": expires,
                    "before_expiry": expires is not None and t < expires,
                    "margin_s": (expires - t) if expires else 0.0,
                }
                break
            out.setdefault(q, {"before_expiry": False, "observations": len(self.obs[q]), "last_mine": last_mine})
        return out


# --- e: edge cases ---------------------------------------------------------------------------


def scenario_e(h):
    """Names that need escaping in the bucket key and the KV key, a record
    above the fetch byte budget, and payloads whose JSON whitespace contains
    a newline or a carriage return."""
    findings = []
    q_odd = "e.ord/eu été:1%"
    q_ws = "e.whitespace"
    queues = [q_odd, q_ws]
    prefix = "e-edge"
    node = Broker(h, "e-node1")
    provision(h, node, queues)
    node.start(sink_env(h, queues, prefix))
    node.wait_healthy()
    wait_owned(h, [node], queues)
    ledger = Ledger()
    rnd = random.Random(5)
    # 1. The odd queue name, a 1.5 MB payload (above the 1 MiB fetch budget,
    #    which "always delivers an entry's first record") and ordinary ones.
    odd_items = [Item(q_odd, p, f"e-odd-{i}-{j}", gen_payload(rnd, "e", p, j)) for j in range(5) for i, p in enumerate(partition_names(8))]
    odd_items.append(Item(q_odd, "huge", "e-huge", '{"huge":"%s"}' % ("h" * 1_500_000)))
    push([node.url], odd_items, ledger)
    # 2. Payloads that are valid JSON with a raw newline / CR as whitespace
    #    between tokens. JSONL is one object PER LINE.
    ws_payloads = [
        '{"ok":1}',
        '{"a":\n1}',
        '[1,\r\n2]',
        '{\n  "pretty": [\n    1,\n    2\n  ]\n}',
        '{"after":"ws"}',
    ]
    for t in ws_payloads:
        json.loads(t)
    push([node.url], [Item(q_ws, "p", f"e-ws-{i}", t) for i, t in enumerate(ws_payloads)], ledger)
    wait_lake(h, prefix, queues, ledger, [node], timeout=120)

    # The odd queue: its whole lake must check out, keys escaped.
    s_odd = verify_queue(h, prefix, q_odd, ledger.expected(q_odd))
    lease = kv_get(node, kv_key(q_odd, "lease"))
    if not lease.get("found"):
        raise Failed(f"e: the lease row of {q_odd!r} is not under {kv_key(q_odd, 'lease')!r}")
    findings.append(
        f"queue {q_odd!r} lives under {queue_root(prefix, q_odd)}/ and KV {kv_key(q_odd, 'lease')!r}: "
        f"{s_odd['records']} records incl. a 1.5 MB payload, exactly once"
    )

    # The whitespace queue: read the raw object and say what a JSONL reader sees.
    ms = read_manifests(h, prefix, q_ws)
    raw_lines = []
    for k in sorted(ms):
        for o in ms[k]["objects"]:
            with open(object_path(h, o["key"]), "rb") as f:
                raw_lines += gzip.decompress(f.read()).split(b"\n")[:-1]
    records = sum(m["records"] for m in ms.values())
    unparsable = []
    for ln in raw_lines:
        try:
            json.loads(ln)
        except ValueError:
            unparsable.append(ln)
    node.stop(40)
    if len(raw_lines) != records or unparsable:
        raise Failed(
            f"{findings[0]} (PASS); BUT JSONL broken by payload whitespace: {records} records were written as "
            f"{len(raw_lines)} lines, {len(unparsable)} of which are not JSON on their own, e.g. "
            f"{[short(l, 60) for l in unparsable[:3]]}. A payload whose JSON whitespace holds a raw \\n or \\r "
            f"(a pretty-printed document) is spliced verbatim (connectors/queen-s3/src/writer/jsonl.rs), so one "
            f"record spans several lines and a newline-delimited reader fails on it. Lake: "
            f"{os.path.join(h.bucket_dir, *queue_root(prefix, q_ws).split('/'))}"
        )
    # One line per record, and each payload exactly what was pushed with its
    # raw line breaks as spaces (lake_form): nothing else of it moved.
    s_ws = verify_queue(h, prefix, q_ws, ledger.expected(q_ws))
    findings.append(
        f"{s_ws['records']} records whose payload whitespace holds raw \\n / \\r: one JSONL line each, the line "
        f"breaks written as spaces and every other byte as pushed"
    )
    cleanup(h, [node], [prefix])
    return "; ".join(findings)


# --- g: the bucket misbehaves ------------------------------------------------------------------


def scenario_g(h):
    """The sink never drops, it only lags: the bucket is DOWN when the broker
    boots (records are pushed meanwhile), then it throttles data PUTs (503
    SlowDown with Retry-After) and fails sidecar PUTs (500) while records
    flow. The lake still ends exactly once."""
    queues = ["g.q0", "g.q1"]
    parts = partition_names(8, special=False)
    prefix = "g-faults"
    main_s3 = h.s3
    late = FakeS3(h, name="s3-late")
    node = Broker(h, "g-node1")
    provision(h, node, queues)
    env = sink_env(h, queues, prefix, QUEEN_S3_ENDPOINT=late.endpoint)
    ledger = Ledger()
    rnd = random.Random(7)

    def gen(r):
        return [Item(q, p, f"g-{q}-{i}-{r}", gen_payload(rnd, "g", p, r)) for q in queues for i, p in enumerate(parts)]

    try:
        node.start(env)
        node.wait_healthy()
        deadline = time.time() + 30
        while node.s3_sink().get("bucket", {}).get("reachable") is not False:
            if time.time() > deadline:
                raise Failed(f"g: /status never reported the bucket unreachable: {node.s3_sink().get('bucket')}")
            time.sleep(0.2)
        down = node.s3_sink()
        push([node.url], gen(0) + gen(1), ledger)
        time.sleep(3.0)
        t_up = time.time()
        late.start()
        h.s3 = late
        wait_owned(h, [node], queues, timeout=90)
        wait_lake(h, prefix, queues, ledger, [node], timeout=120, what="the records pushed while the bucket was down")
        recovered_s = time.time() - t_up

        # Throttle data objects and fail sidecars while the pusher runs.
        pusher = Pusher(ledger, lambda r: gen(r + 2), [node.url], pause=0.05).start()
        time.sleep(2.0)
        q = urllib.request.quote
        late.admin(f"fail?op=put&status=503&code=SlowDown&count=4&retryAfter=1&match={q(prefix + '/tenant=', safe='')}")
        late.admin(f"fail?op=put&status=500&code=InternalError&count=2&match={q(prefix + '/_queen/', safe='')}")
        deadline = time.time() + 120
        while True:
            st = late.admin("stats")
            if st.get("put 503", 0) >= 4 and st.get("put 500", 0) >= 2:
                break
            if time.time() > deadline:
                raise Failed(f"g: the injected faults were not all hit: {st}")
            time.sleep(0.3)
        time.sleep(2.0)
        leftover = pusher.stop()
        push([node.url], leftover, ledger)
        wait_lake(h, prefix, queues, ledger, [node], timeout=180)
        stats = {qq: verify_queue(h, prefix, qq, ledger.expected(qq)) for qq in queues}
        text = node.metrics()
        seen = {c: metric(text, "queen_s3_s3_requests_total", f'op="put",code="{c}"') for c in ("503", "500")}
        if (seen["503"] or 0) < 4 or (seen["500"] or 0) < 2:
            raise Failed(f"g: queen_s3_s3_requests_total did not count the faults: {seen}")
        node.stop(40)
        cleanup(h, [node], [prefix])
    finally:
        h.s3 = main_s3
        late.stop()
        if not h.args.keep:
            shutil.rmtree(late.root, ignore_errors=True)
    return (
        f"bucket down at boot: /status said reachable=false ({short(str(down['bucket'].get('error')), 70)}), "
        f"{2 * len(parts) * len(queues)} records pushed meanwhile; bucket up -> lake complete {recovered_s:.1f}s later; "
        f"then 4 x 503 SlowDown on data PUTs and 2 x 500 on sidecar PUTs mid-traffic, retried (metrics: 503 x{int(seen['503'])}, "
        f"500 x{int(seen['500'])}); "
        + ", ".join(f"{qq} {s['windows']}w/{s['records']}r" for qq, s in stats.items())
        + " exactly once"
    )


# --- f: a node fences ITSELF out of its own queue (bug reproduction) --------------------------


def scenario_f(h):
    """One node, so there is no other instance anywhere. The lease refresh
    task (lease.rs `spawn_refresh`) and the driver's intent and commit
    batches (driver.rs `do_intent`/`do_commit`, op 0 = `Lease::fence_op`)
    both write the lease row expecting the version this node last wrote,
    concurrently and unsynchronised. When both are in flight, the second to
    apply loses its precondition: the queue is fenced ("another instance
    owns this queue"), its handle is marked lost so it releases nothing, and
    it waits out its OWN lease before it runs again. Made frequent here
    (TTL 1 s: a refresh every 333 ms; a window — two fenced batches — every
    ~100 ms); it is the same race at any setting."""
    queues = ["f.q0", "f.q1"]
    parts = partition_names(8, special=False)
    prefix = "f-selffence"
    ttl_ms = 1000
    node = Broker(h, "f-node1")
    provision(h, node, queues)
    env = sink_env(
        h, queues, prefix, QUEEN_S3_LEASE_TTL_MS=ttl_ms, QUEEN_S3_MAX_WINDOW_MS=100, QUEEN_S3_DISCOVERY_INTERVAL_MS=10
    )
    node.start(env)
    node.wait_healthy()
    wait_owned(h, [node], queues)
    instance = node.s3_sink()["instance"]
    ledger = Ledger()
    rnd = random.Random(6)

    def gen(r):
        return [Item(q, p, f"f-{q}-{i}-{r}", gen_payload(rnd, "f", p, r)) for q in queues for i, p in enumerate(parts)]

    pusher = Pusher(ledger, gen, [node.url], pause=0.01).start()
    t0 = time.time()
    deadline = t0 + h.args.f_seconds
    first = None
    held_by_self = []
    leases = {}
    while time.time() < deadline:
        fences = fences_in_log(node.log())
        if fences and first is None:
            first = (time.time() - t0, fences[0])
            leases = {q: kv_get(node, kv_key(q, "lease")) for q in queues}
        if first is not None:
            for q, r in node.s3_rows().items():
                if r.get("heldBy") == instance:
                    held_by_self.append((round(time.time() - t0, 2), q, r.get("state"), r.get("heldBy")))
            if time.time() - t0 > first[0] + 3 * ttl_ms / 1000 + 2:
                break
        time.sleep(0.05)
    leftover = pusher.stop()
    push([node.url], leftover, ledger)
    wait_lake(h, prefix, queues, ledger, [node], timeout=120)
    stats = {q: verify_queue(h, prefix, q, ledger.expected(q)) for q in queues}
    lost_metric = metric(node.metrics(), "queen_s3_commit_precondition_lost_total", "") or 0
    node.stop(40)
    log = node.log()
    fences = fences_in_log(log)
    lost = re.findall(r"lease lost: another instance owns this queue queue=(\S+) error=([^\n]*)", log)
    cleanup(h, [node], [prefix])
    exactly = ", ".join(f"{q} {s['windows']}w/{s['records']}r" for q, s in stats.items())
    if not fences:
        return f"no self-fence in {h.args.f_seconds}s of 100 ms windows against a 333 ms lease refresh; {exactly} exactly once"
    raise Failed(
        f"BUG: one node, no other instance anywhere, fenced ITSELF {len(fences)} time(s) in {time.time() - t0:.0f}s "
        f"(first after {first[0]:.1f}s): {[(q, why) for _, q, why in fences[:4]]}; "
        f"`lease lost: another instance owns this queue` x{len(lost)}: {lost[:2]}; "
        f"queen_s3_commit_precondition_lost_total={int(lost_metric)}; right after the first fence the lease rows were "
        f"{[(q, (r.get('value') or {}).get('instance'), r.get('version')) for q, r in leases.items()]} (this node is {instance})"
        + (f"; /status then showed the queue held by THIS node itself: {held_by_self[:3]}" if held_by_self else "")
        + f". The lake is still exactly-once ({exactly}): the fence costs liveness, not data. Log: {node.log_path}"
    )


# --- multi-tenant cells: the embedded proxy's control plane drives per-tenant sinks -------------

PROXY_TENANT = "00000000-0000-0000-0000-00000000fffe"  # proxy/src/store/schema.rs
RELOAD_S = 5  # server/src/s3_inproc.rs RELOAD: every node re-reads the sink rows this often


def grace_s(margin):
    """How long a SIGTERMed node may take: the sink's grace plus `margin`."""
    return 10 + margin


def make_cell(nodes, encryption_key=None):
    """Turn `nodes` into a cell: the proxy embedded on a port of its own
    (QUEEN_PROXY_PORT, so PORT keeps serving the broker router itself), one
    control-plane token, tenancy on (a push names its broker tenant with
    `x-queen-tenant`), and the cell's QUEEN_ENCRYPTION_KEY when one is given."""
    token = secrets.token_hex(16)
    for b, port in zip(nodes, free_ports(len(nodes))):
        b.proxy_port = port
        b.cp_token = token
        b.cell_env = {
            "QUEEN_PROXY_EMBEDDED": "true",
            "QUEEN_PROXY_PORT": str(port),
            "QUEEN_PROXY_CP_TOKEN": token,
            "QUEEN_TENANCY_HEADER": "true",
            # The broker refuses the tenant header without this affirmation
            # that a proxy in front sets it (and strips a client's).
            "QUEEN_KV_TRUSTED_PROXY": "1",
            "QUEEN_PROXY_SPOOL_DIR": os.path.join(b.data_dir, "proxy-spool"),
        }
        if encryption_key:
            b.cell_env["QUEEN_ENCRYPTION_KEY"] = encryption_key
    return token


def node_knobs():
    """The node-wide sink knobs (environment only; a tenant document may not
    set them), without any of the environment sink's own variables."""
    return {
        "QUEEN_S3_EMBEDDED": "true",
        "QUEEN_S3_DISCOVERY_INTERVAL_MS": "100",
        "QUEEN_S3_SAFE_GUARD_MS": "0",
        "QUEEN_S3_LEASE_TTL_MS": "3000",
        "QUEEN_S3_SHUTDOWN_GRACE_MS": "10000",
        "QUEEN_S3_CHECKPOINT_EVERY": "2",
    }


def tenant_doc(h, bucket, queues, secret=None, **extra):
    """A tenant's sink document for `PUT /api/cp/clusters/:slug/s3`."""
    doc = {
        "queues": queues,
        "endpoint": h.s3.endpoint,
        "region": "us-east-1",
        "bucket": bucket,
        "prefix": "lake",
        "accessKey": f"AK-{bucket}",
        "pathStyle": True,
        "format": "jsonl",
        "compression": "gzip",
        "align": "none",
        "start": "earliest",
        "maxWindowMs": 1000,
    }
    doc.update(extra)
    if secret is not None:
        doc["secretKey"] = secret
    return doc


def ensure_cluster(node, tenant_slug, cluster_slug, timeout=60):
    """`POST /api/cp/clusters`, retried while the proxy's state is not seeded
    yet (no cell: 400; the KV not ready: 503). Returns the cluster row."""
    deadline = time.time() + timeout
    while True:
        try:
            st, doc, raw = node.cp("POST", "/clusters", {"tenant_slug": tenant_slug, "slug": cluster_slug})
        except OSError as e:
            st, doc, raw = None, None, str(e).encode()
        if st in (200, 201) and doc and doc.get("broker_tenant_uuid"):
            return doc
        if time.time() > deadline:
            raise Failed(f"POST /api/cp/clusters {cluster_slug} -> {st}: {short(raw, 300)}\n{node.tail()}")
        time.sleep(0.5)


def cp_ok(node, method, path, body=None, want=(200,), what=""):
    st, doc, raw = node.cp(method, path, body)
    if st not in want:
        raise Failed(f"{what or method + ' ' + path} -> {st}: {short(raw, 400)}")
    return doc, raw


def wait_sink(nodes, tenant, phase="running", timeout=60, gone=False, since=None):
    """Until every node in `nodes` reports `tenant`'s sink in `phase` (or, with
    `gone`, no sink of it at all — or one stopped). Returns {node: seconds
    since `since`, default the call}."""
    t0 = since or time.time()
    seen = {}
    last = {}
    while len(seen) < len(nodes):
        for b in nodes:
            if b.name in seen:
                continue
            try:
                s = b.s3_sink(tenant)
            except Exception:  # noqa: BLE001 — a node mid-boot answers nothing
                continue
            last[b.name] = (s.get("phase"), s.get("error"))
            if gone and (not s or s.get("phase") in ("stopped",)):
                seen[b.name] = time.time() - t0
            elif not gone and s.get("phase") == phase and s.get("running") is not False:
                seen[b.name] = time.time() - t0
        if time.time() - t0 > timeout:
            raise Failed(f"tenant {tenant}'s sink not {'gone' if gone else phase} on every node after {timeout}s: {last}")
        time.sleep(0.2)
    return seen


def bucket_keys(h, bucket):
    root = h.bucket(bucket)
    out = []
    for dirpath, _, files in os.walk(root):
        for f in files:
            out.append(os.path.relpath(os.path.join(dirpath, f), root).replace(os.sep, "/"))
    return sorted(out)


def tenant_roots(prefix, tenant):
    return (f"{prefix}/tenant={esc(tenant)}/", f"{prefix}/_queen/tenant={esc(tenant)}/")


def assert_bucket_holds_only(h, bucket, prefix, *tenants):
    """Every object of `bucket` is under `<prefix>/tenant=<t>/` or its sidecar
    root `<prefix>/_queen/tenant=<t>/` for one of `tenants`: nothing of any
    other tenant, nothing outside the layout. {tenant: objects}."""
    keys = bucket_keys(h, bucket)
    out = {t: 0 for t in tenants}
    stray = []
    for k in keys:
        owner = [t for t in tenants if k.startswith(tenant_roots(prefix, t))]
        if owner:
            out[owner[0]] += 1
        else:
            stray.append(k)
    if stray:
        raise Failed(f"bucket {bucket} (tenants {tenants}) holds keys of another tenant or outside the layout: {stray[:5]}")
    return out


def tenant_snapshot(h, bucket, prefix, tenant):
    """sha256 of every object of one tenant in a bucket (its data and its
    sidecars), whoever else writes there."""
    return {k: d for k, d in fingerprint(h, prefix, bucket).items() if k.startswith(tenant_roots(prefix, tenant))}


def tenant_manifests(h, bucket, prefix):
    """{tenant field of every manifest in the bucket}."""
    out = set()
    root = os.path.join(h.bucket(bucket), prefix, "_queen")
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.endswith(".json") and os.path.basename(dirpath) == "windows":
                with open(os.path.join(dirpath, f), "rb") as fh:
                    out.add(json.loads(fh.read()).get("tenant"))
    return out


def scenario_h(h):
    """One node, four tenants on one queue NAME: the default tenant with its
    environment sink (bucket env-lake), and three the control plane creates
    and configures — acme -> acme-lake, globex -> globex-lake, and initech,
    which SHARES acme's bucket and prefix. Isolation, the redacted secret,
    DELETE, suspend (cluster and owner tenant), push_blocked, a secret
    rotation, enabled=false, re-enable, and a cell without
    QUEEN_ENCRYPTION_KEY."""
    report = []
    q = "h.orders"
    parts = partition_names(8, special=False)
    prefix = "lake"
    key = secrets.token_hex(32)
    node = Broker(h, "h-node1")
    make_cell([node], key)
    env = {
        **node_knobs(),
        **sink_env(h, [q], prefix, QUEEN_S3_BUCKET="env-lake"),
    }
    # No provisioning boot: the queue is created after the sink starts, and
    # the sink must find it (MISSING_RETRY).
    node.start(env)
    node.wait_healthy()
    wait_port(node.proxy_port, 30, node.proc)
    configure_queues(h, node, [q])

    clusters = {s: ensure_cluster(node, s, f"{s}-main") for s in ("acme", "globex", "initech")}
    tenants = {s: c["broker_tenant_uuid"] for s, c in clusters.items()}
    if len(set(tenants.values()) | {DEFAULT_TENANT}) != 4:
        raise Failed(f"h: the clusters' broker tenants are not distinct: {tenants}")
    for s in clusters:
        cp_ok(node, "POST", f"/clusters/{s}-main/configure", {"queue": q, "options": {"retentionEnabled": False}})
    bucket_of = {"acme": "acme-lake", "globex": "globex-lake", "initech": "acme-lake"}
    buckets = {DEFAULT_TENANT: "env-lake", **{tenants[s]: b for s, b in bucket_of.items()}}
    slug_of = {DEFAULT_TENANT: "default", **{t: s for s, t in tenants.items()}}
    secrets_of = {s: f"{s}-SECRET-" + secrets.token_hex(12) for s in clusters}

    # A document the sink would refuse is refused by the PUT, and stores nothing.
    st, doc, raw = node.cp("PUT", "/clusters/acme-main/s3", tenant_doc(h, "acme-lake", [q], secrets_of["acme"], leaseTtlMs=1000))
    if st != 400 or b"leaseTtlMs" not in raw or secrets_of["acme"].encode() in raw:
        raise Failed(f"h: a node-wide field in a tenant document -> {st}: {short(raw, 300)}")
    st, _, raw = node.cp("GET", "/clusters/acme-main/s3")
    if st != 404:
        raise Failed(f"h: a refused PUT stored something: GET -> {st}: {short(raw)}")

    t_put = {}
    for s in clusters:
        t_put[s] = time.time()
        doc, raw = cp_ok(node, "PUT", f"/clusters/{s}-main/s3", tenant_doc(h, bucket_of[s], [q], secrets_of[s]))
        if any(x.encode() in raw for x in secrets_of.values()):
            raise Failed(f"h: the PUT answer of {s} carries a secret: {short(raw, 300)}")
        if doc.get("secretKeySet") is not True or "secretKey" in doc.get("config", {}):
            raise Failed(f"h: the PUT answer of {s} is not the redacted view: {doc}")
        got, raw = cp_ok(node, "GET", f"/clusters/{s}-main/s3")
        if any(x.encode() in raw for x in secrets_of.values()) or set(got) != {
            "cluster", "tenant", "enabled", "config", "secretKeySet", "updatedAt"
        }:
            raise Failed(f"h: GET /clusters/{s}-main/s3 shows more than the redacted view: {short(raw, 400)}")
    started = {s: wait_sink([node], tenants[s], timeout=30, since=t_put[s])[node.name] for s in clusters}
    wait_sink([node], DEFAULT_TENANT, timeout=30)
    sources = {slug_of.get(t, t): (v.get("source"), v.get("phase")) for t, v in node.s3_sinks().items()}
    if sources.get("default", (None,))[0] != "env" or any(sources.get(s, (None,))[0] != "cp" for s in clusters):
        raise Failed(f"h: /status sinks are not env + 3 cp: {sources}")
    report.append(
        f"4 sinks on one node, /status {sources}; each cp sink running "
        + ", ".join(f"{s} {v:.1f}s" for s, v in started.items())
        + f" after its PUT (reload {RELOAD_S}s); a document with a node-wide field refused 400 and nothing stored; "
        f"PUT and GET answers are the redacted view (secretKeySet only)"
    )

    ledger = Ledger()
    rnd = random.Random(8)
    rounds = iter(range(10_000))

    def gen(only=None):
        r = next(rounds)
        return [
            Item(q, p, f"h-{slug_of[t]}-{i}-{r}", gen_payload(rnd, slug_of[t], p, r), tenant=t)
            for t in (only or list(buckets))
            for i, p in enumerate(parts)
        ]

    def settle(which=None):
        for t in which or list(buckets):
            wait_lake(h, prefix, [q], ledger, [node], timeout=90, tenant=t, bucket=buckets[t], what=f"tenant {slug_of[t]}'s records")

    for _ in range(3):
        push([node.url], gen(), ledger)
        time.sleep(0.4)
    settle()
    stats = {slug_of[t]: verify_queue(h, prefix, q, ledger.expected(q, t), tenant=t, bucket=b) for t, b in buckets.items()}
    holds = {
        "env-lake": assert_bucket_holds_only(h, "env-lake", prefix, DEFAULT_TENANT),
        "acme-lake": assert_bucket_holds_only(h, "acme-lake", prefix, tenants["acme"], tenants["initech"]),
        "globex-lake": assert_bucket_holds_only(h, "globex-lake", prefix, tenants["globex"]),
    }
    for b in set(buckets.values()):
        named = tenant_manifests(h, b, prefix)
        allowed = {t for t, bb in buckets.items() if bb == b}
        if named != allowed:
            raise Failed(f"h: bucket {b} holds manifests of tenants {named}, expected {allowed}")
    report.append(
        f"queue {q!r} in all 4 tenants, same partition names: each tenant's records exactly once in its own bucket "
        + ", ".join(f"{s} {v['windows']}w/{v['records']}r" for s, v in stats.items())
        + f"; objects per tenant root { {b: {slug_of[t]: n for t, n in v.items()} for b, v in holds.items()} }: acme and "
        f"initech share acme-lake and prefix '{prefix}' with no key in common; every manifest names its own tenant; the "
        f"default tenant's queue was created after its sink started and was found (missing-queue retry)"
    )

    # Secrets: never in /status, the broker's log or the fake S3's log; stored
    # sealed in the proxy's table.
    status_raw = json.dumps(node.status())
    logs = node.log(whole=True) + open(h.s3.log_path, encoding="utf-8", errors="replace").read()
    leaked = [s for s in secrets_of.values() if s in status_raw or s in logs]
    if leaked:
        raise Failed(f"h: an S3 secret appears in /status or a log: {leaked}")
    sealed_note = sealed_rows(node, secrets_of)

    # DELETE acme's sink: it stops within a reload plus its drain; the other
    # three keep running — initech into the very same bucket — and acme's
    # objects stop moving.
    doc, _ = cp_ok(node, "DELETE", "/clusters/acme-main/s3")
    if doc.get("removed") is not True:
        raise Failed(f"h: DELETE answered {doc}")
    gone = wait_sink([node], tenants["acme"], gone=True, timeout=RELOAD_S + 20)[node.name]
    again, _ = cp_ok(node, "DELETE", "/clusters/acme-main/s3")
    st, _, _ = node.cp("GET", "/clusters/acme-main/s3")
    if again.get("removed") is not False or st != 404:
        raise Failed(f"h: a second DELETE answered {again}, GET after it {st}")
    before = tenant_snapshot(h, "acme-lake", prefix, tenants["acme"])
    others = [t for t in buckets if t != tenants["acme"]]
    moved_from = {slug_of[t]: progress(h, prefix, q, t, buckets[t])["records"] for t in others}
    for _ in range(6):
        push([node.url], gen(), ledger)
        time.sleep(1.0)
    if tenant_snapshot(h, "acme-lake", prefix, tenants["acme"]) != before:
        raise Failed("h: acme's objects moved after its sink was DELETEd")
    settle(others)
    moved_to = {slug_of[t]: progress(h, prefix, q, t, buckets[t])["records"] for t in others}
    report.append(
        f"DELETE acme's sink: gone from /status {gone:.1f}s later, a second DELETE answers removed=false and GET 404; "
        f"over 6 s of pushes acme's objects did not move while the others kept shipping ({moved_from} -> {moved_to} "
        f"records, initech into the bucket acme shares)"
    )

    # globex: suspend the cluster, then the owner tenant; each stops the sink,
    # back to active resumes it from its commit pointer.
    gid = clusters["globex"]["id"]
    gt = tenants["globex"]
    for what, path in (("cluster", f"/clusters/{gid}/status"), ("owner tenant", "/tenants/globex/status")):
        cp_ok(node, "PUT", path, {"status": "suspended"}, what=f"suspend globex's {what}")
        stopped = wait_sink([node], gt, gone=True, timeout=RELOAD_S + 20)[node.name]
        snap = tenant_snapshot(h, "globex-lake", prefix, gt)
        for _ in range(3):
            push([node.url], gen(only=[gt]), ledger)
            time.sleep(1.0)
        if tenant_snapshot(h, "globex-lake", prefix, gt) != snap:
            raise Failed(f"h: globex's lake moved while its {what} was suspended")
        cp_ok(node, "PUT", path, {"status": "active"}, what=f"reactivate globex's {what}")
        resumed = wait_sink([node], gt, timeout=RELOAD_S + 20)[node.name]
        settle([gt])
        report.append(
            f"suspend globex's {what}: its sink stopped {stopped:.1f}s later and its objects held still under 3 s of "
            f"pushes; active again: running {resumed:.1f}s later, the backlog shipped"
        )
    # push_blocked keeps the sink (pushes refused at the edge, data still to ship).
    cp_ok(node, "PUT", f"/clusters/{gid}/status", {"status": "push_blocked"})
    time.sleep(RELOAD_S + 1)
    if node.s3_sink(gt).get("phase") != "running":
        raise Failed(f"h: push_blocked stopped globex's sink: {node.s3_sink(gt).get('phase')}")
    push([node.url], gen(only=[gt]), ledger)
    settle([gt])
    cp_ok(node, "PUT", f"/clusters/{gid}/status", {"status": "active"})
    report.append("push_blocked: globex's sink kept running and shipped what the broker port took")

    # A secret rotation while records flow: the row moves, the sink is rebuilt
    # (a new unit: its uptime starts again), nothing is lost or doubled.
    up_before = node.s3_sink(gt).get("uptimeMs", 0)
    pusher = Pusher(ledger, lambda r: gen(only=[gt]), [node.url], pause=0.2).start()
    time.sleep(1.5)
    t_rot = time.time()
    retired = secrets_of["globex"]
    secrets_of["globex"] = "globex-SECRET-" + secrets.token_hex(12)
    cp_ok(node, "PUT", "/clusters/globex-main/s3", tenant_doc(h, "globex-lake", [q], secrets_of["globex"]))
    deadline = time.time() + RELOAD_S + 20
    rebuilt = None
    while time.time() < deadline:
        s = node.s3_sink(gt)
        if s.get("phase") == "running" and s.get("uptimeMs", 0) < (time.time() - t_rot) * 1000 + 500:
            rebuilt = time.time() - t_rot
            break
        time.sleep(0.1)
    time.sleep(2.0)
    push([node.url], pusher.stop(), ledger)
    if rebuilt is None:
        raise Failed(f"h: globex's sink was not rebuilt after its secret rotated (uptime {up_before} ms before)")
    settle([gt])
    report.append(f"secret rotated mid-traffic: globex's sink rebuilt {rebuilt:.1f}s later, its records still exactly once")

    # enabled=false keeps the row (and its sealed secret) and stops the sink.
    it = tenants["initech"]
    off_doc = tenant_doc(h, "acme-lake", [q])
    doc, _ = cp_ok(node, "PUT", "/clusters/initech-main/s3", {**off_doc, "enabled": False})
    if doc.get("enabled") is not False or doc.get("secretKeySet") is not True:
        raise Failed(f"h: PUT enabled=false answered {doc}")
    stopped = wait_sink([node], it, gone=True, timeout=RELOAD_S + 20)[node.name]
    snap = tenant_snapshot(h, "acme-lake", prefix, it)
    for _ in range(3):
        push([node.url], gen(only=[it]), ledger)
        time.sleep(1.0)
    if tenant_snapshot(h, "acme-lake", prefix, it) != snap:
        raise Failed("h: initech's objects moved while its sink was disabled")
    cp_ok(node, "PUT", "/clusters/initech-main/s3", {**off_doc, "enabled": True})
    resumed = wait_sink([node], it, timeout=RELOAD_S + 20)[node.name]
    settle([it])
    report.append(
        f"initech enabled=false (secret kept): stopped {stopped:.1f}s later, objects held still under 3 s of pushes; "
        f"enabled=true without resending the secret: running {resumed:.1f}s later, backlog shipped"
    )

    # acme again: a PUT without a stored secret needs one; with it the sink
    # resumes from its commit pointer and ships what was pushed meanwhile —
    # every record once.
    st_nosecret, _, _ = node.cp("PUT", "/clusters/acme-main/s3", tenant_doc(h, "acme-lake", [q]))
    cp_ok(node, "PUT", "/clusters/acme-main/s3", tenant_doc(h, "acme-lake", [q], secrets_of["acme"]))
    wait_sink([node], tenants["acme"], timeout=RELOAD_S + 20)
    push([node.url], gen(), ledger)
    settle()
    final = {slug_of[t]: verify_queue(h, prefix, q, ledger.expected(q, t), tenant=t, bucket=b) for t, b in buckets.items()}
    assert_bucket_holds_only(h, "env-lake", prefix, DEFAULT_TENANT)
    assert_bucket_holds_only(h, "acme-lake", prefix, tenants["acme"], it)
    assert_bucket_holds_only(h, "globex-lake", prefix, gt)
    status_raw = json.dumps(node.status())
    logs = node.log(whole=True)
    leaked = [s for s in list(secrets_of.values()) + [retired] if s in status_raw or s in logs]
    if leaked:
        raise Failed(f"h: an S3 secret appears in /status or the log: {leaked}")
    # One exposition, the tenant a label of every cp sink's series and absent
    # from the default tenant's: the environment sink ran without a restart,
    # so its counter is exactly its lake.
    written = series(node.metrics(), "queen_s3_records_written_total")
    by_tenant = defaultdict(float)
    for labels, v in written:
        if labels.get("queue") == q:
            by_tenant[labels.get("tenant")] += v
    if by_tenant.get(None) != final["default"]["records"] or any(tenants[s] not in by_tenant for s in clusters):
        raise Failed(f"h: queen_s3_records_written_total by tenant label {dict(by_tenant)}, lake {final}")
    metrics_note = (
        f"queen_s3_records_written_total{{queue={q!r}}}: no tenant label = default tenant = {int(by_tenant[None])} "
        f"(its lake), tenant=\"<uuid>\" series for acme/globex/initech (counters restart with a rebuilt sink)"
    )
    report.append(
        f"acme re-PUT (without a secret while it has none: {st_nosecret}; with one: resumed from its commit pointer); "
        f"final "
        + ", ".join(f"{s} {v['windows']}w/{v['records']}r" for s, v in final.items())
        + " exactly once, nothing in another tenant's root, no secret in /status or the log"
    )
    report.append(metrics_note)
    report.append(sealed_note)
    node.stop(40)
    for b in set(buckets.values()):
        cleanup(h, [], [prefix], bucket=b)
    cleanup(h, [node], [])

    # A cell WITHOUT QUEEN_ENCRYPTION_KEY: no tenant secret is ever stored.
    bare = Broker(h, "h-nokey")
    make_cell([bare], None)
    bare.start(node_knobs())
    bare.wait_healthy()
    wait_port(bare.proxy_port, 30, bare.proc)
    c = ensure_cluster(bare, "nokey", "nokey-main")
    secret = "nokey-SECRET-" + secrets.token_hex(8)
    st, doc, raw = bare.cp("PUT", "/clusters/nokey-main/s3", tenant_doc(h, "nokey-lake", ["n.q"], secret))
    code = (doc or {}).get("code")
    st_get, _, _ = bare.cp("GET", "/clusters/nokey-main/s3")
    time.sleep(RELOAD_S + 1)
    has_sink = bool(bare.s3_sink(c["broker_tenant_uuid"]))
    bare.stop(40)
    cleanup(h, [bare], [])
    if st != 409 or code != "encryption_required" or secret.encode() in raw or st_get != 404 or has_sink:
        raise Failed(
            f"h: PUT on a cell without QUEEN_ENCRYPTION_KEY -> {st} {code} (secret echoed: {secret.encode() in raw}); "
            f"GET after it {st_get}; a sink started: {has_sink}"
        )
    report.append("cell without QUEEN_ENCRYPTION_KEY: PUT -> 409 encryption_required (secret not echoed), GET -> 404, no sink")
    return "; ".join(report)


def scenario_i(h):
    """Three nodes, two tenants configured through the control plane only (no
    environment sink). The PUT made on one node reaches every node's manager;
    each tenant's queues are owned by one node each and ship exactly once;
    kill -9 of the node owning the most tenant queues moves them to the
    survivors; the killed node rejoins; a DELETE made on one node stops the
    sink on every node."""
    report = []
    prefix = "lake"
    key = secrets.token_hex(32)
    nodes = [Broker(h, f"i-node{i}", node_id=i) for i in (1, 2, 3)]
    for b, rp in zip(nodes, free_ports(3)):
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
    make_cell(nodes, key)
    by_name = {b.name: b for b in nodes}
    for b in nodes:
        b.start(node_knobs())
    for b in nodes:
        b.wait_healthy(120)
        wait_port(b.proxy_port, 30, b.proc)

    # Two tenants, created on node 1; their queues configured on node 2; their
    # sinks PUT on node 3.
    ca = ensure_cluster(nodes[0], "tenanta", "ta-main")
    cb = ensure_cluster(nodes[0], "tenantb", "tb-main")
    ta, tb = ca["broker_tenant_uuid"], cb["broker_tenant_uuid"]
    queues = {ta: ["ia.q0", "ia.q1", "ia.q2", "i.shared"], tb: ["ib.q0", "ib.q1", "ib.q2", "i.shared"]}
    bucket = {ta: "tenant-a-lake", tb: "tenant-b-lake"}
    slug = {ta: "ta-main", tb: "tb-main"}
    for t, qs in queues.items():
        for q in qs:
            cp_ok(nodes[1], "POST", f"/clusters/{slug[t]}/configure", {"queue": q, "options": {"retentionEnabled": False}})
    t_put = time.time()
    for t in queues:
        cp_ok(nodes[2], "PUT", f"/clusters/{slug[t]}/s3", tenant_doc(h, bucket[t], queues[t], "i-secret-" + secrets.token_hex(8)))
    reach = {t: wait_sink(nodes, t, timeout=RELOAD_S * 3 + 30, since=t_put) for t in queues}
    report.append(
        "sinks PUT on i-node3 running on every node after "
        + "; ".join(f"{slug[t]}: " + ", ".join(f"{n} {s:.1f}s" for n, s in sorted(reach[t].items())) for t in queues)
    )
    owners = {t: wait_owned(h, nodes, queues[t], timeout=60, tenant=t) for t in queues}
    roles = {b.name: b.role() for b in nodes}
    placement = defaultdict(list)
    for t, o in owners.items():
        for q, n in o.items():
            placement[n].append(f"{slug[t]}/{q}")
    report.append(f"roles {roles}; tenant queues owned per node {dict(sorted((n, len(v)) for n, v in placement.items()))}")
    h.say(f"  i: {report[-1]}")

    ledger = Ledger()
    rnd = random.Random(9)
    parts = partition_names(8, special=False)
    tag = {ta: "a", tb: "b"}

    def gen(r):
        return [
            Item(q, p, f"i-{tag[t]}-{q}-{i}-{r}", gen_payload(rnd, tag[t], p, r), tenant=t)
            for t, qs in queues.items()
            for q in qs
            for i, p in enumerate(parts)
        ]

    pusher = Pusher(ledger, gen, [b.url for b in nodes], pause=0.05).start()
    deadline = time.time() + 90
    while any(progress(h, prefix, q, t, bucket[t])["windows"] < 2 for t, qs in queues.items() for q in qs):
        if time.time() > deadline:
            raise Failed(f"i: not every tenant queue committed 2 windows in 90s: { {(slug[t], q): progress(h, prefix, q, t, bucket[t])['windows'] for t, qs in queues.items() for q in qs} }")
        time.sleep(0.3)

    # kill -9 the node owning the most tenant queues.
    owners = {t: wait_owned(h, nodes, queues[t], timeout=60, tenant=t) for t in queues}
    load = defaultdict(list)
    for t, o in owners.items():
        for q, n in o.items():
            load[n].append((t, q))
    victim = by_name[max(load, key=lambda n: (len(load[n]), n))]
    lost = load[victim.name]
    survivors = [b for b in nodes if b is not victim]
    pusher.set_urls([b.url for b in survivors])
    time.sleep(1.0)
    victim_role = victim.role()
    base = {b.name: {t: b.s3_rows(t) for t in queues} for b in survivors}
    t_kill = victim.kill9()
    victim.wait_exit(10, expect=None)
    claimed, committed = {}, {}
    deadline = t_kill + 2 * 3 + 30
    while len(committed) < len(lost):
        for b in survivors:
            for t in queues:
                try:
                    rows = b.s3_rows(t)
                except Exception:  # noqa: BLE001 — mid-election
                    continue
                for (lt, q) in lost:
                    if lt != t:
                        continue
                    r = rows.get(q, {})
                    if r.get("ownedHere"):
                        claimed.setdefault((t, q), (b.name, time.time() - t_kill))
                        before = base[b.name][t].get(q, {}).get("windowsCommitted", 0)
                        if r.get("windowsCommitted", 0) > before:
                            committed.setdefault((t, q), time.time() - t_kill)
        if time.time() > deadline:
            raise Failed(f"i: after kill -9 of {victim.name} ({victim_role}), claimed {claimed}, committed {committed} of {lost}")
        time.sleep(0.1)
    report.append(
        f"kill -9 {victim.name} ({victim_role}, owned {len(lost)} tenant queues): taken over "
        + ", ".join(f"{slug[t]}/{q} by {claimed[(t, q)][0]} in {claimed[(t, q)][1]:.1f}s (commit {committed[(t, q)]:.1f}s)" for (t, q) in lost)
    )
    h.say(f"  i: {report[-1]}")
    victim.start(node_knobs())
    victim.wait_healthy(120)
    wait_port(victim.proxy_port, 30, victim.proc)
    wait_caught_up(nodes, victim)
    rejoin = wait_sink([victim], ta, timeout=RELOAD_S * 3 + 30)[victim.name]
    report.append(f"{victim.name} restarted, caught up, both tenant sinks running on it again ({rejoin:.1f}s)")

    time.sleep(2.0)
    leftover = pusher.stop()
    urls = [b.url for b in nodes]
    push(urls, leftover, ledger)
    push_batches(urls, gen(10_000), ledger)
    if ledger.unacknowledged():
        raise Failed(f"i: pushed items never acknowledged: {ledger.unacknowledged()[:3]}")
    stats = {}
    for t, qs in queues.items():
        wait_lake(h, prefix, qs, ledger, nodes, timeout=240, tenant=t, bucket=bucket[t])
        for q in qs:
            stats[(t, q)] = verify_queue(h, prefix, q, ledger.expected(q, t), tenant=t, bucket=bucket[t])
        assert_bucket_holds_only(h, bucket[t], prefix, t)
    # No window committed by two nodes, from the logs — for the queues whose
    # name only one tenant has: the log line names the queue, not the tenant.
    by_k = defaultdict(set)
    for b in nodes:
        for q, k, _ in commits_in_log(b.log(whole=True)):
            if q != "i.shared":
                by_k[(q, k)].add(b.name)
    twice = {qk: n for qk, n in by_k.items() if len(n) > 1}
    if twice:
        raise Failed(f"i: windows committed by more than one node: {twice}")
    report.append(
        "final, per tenant: "
        + "; ".join(
            f"{slug[t]} " + ", ".join(f"{q} {stats[(t, q)]['windows']}w/{stats[(t, q)]['records']}r" for q in qs)
            for t, qs in queues.items()
        )
        + f" = {sum(s['records'] for s in stats.values())} records exactly once, each bucket only its tenant's keys, no "
        f"window committed by two nodes"
    )

    # One node restarted with a DIFFERENT QUEEN_ENCRYPTION_KEY cannot open the
    # tenants' secrets: its sinks say so in /status and run nothing, the other
    # nodes keep every tenant queue, and the lake stays exactly once.
    odd = nodes[1]
    odd.stop(grace_s(30))
    odd.cell_env["QUEEN_ENCRYPTION_KEY"] = secrets.token_hex(32)
    odd.start(node_knobs())
    odd.wait_healthy(120)
    wait_caught_up(nodes, odd)
    deadline = time.time() + RELOAD_S * 3 + 20
    while True:
        views = {t: odd.s3_sink(t) for t in queues}
        if all(v.get("phase") == "error" for v in views.values()):
            break
        if time.time() > deadline:
            raise Failed(f"i: a node with the wrong QUEEN_ENCRYPTION_KEY reports {[(v.get('phase'), v.get('error')) for v in views.values()]}")
        time.sleep(0.2)
    errors = sorted({str(v.get("error"))[:90] for v in views.values()})
    owned_there = {t: [q for q, r in odd.s3_rows(t).items() if r.get("ownedHere")] for t in queues}
    push_batches([b.url for b in nodes], gen(20_000), ledger)
    for t, qs in queues.items():
        wait_lake(h, prefix, qs, ledger, nodes, timeout=120, tenant=t, bucket=bucket[t])
        for q in qs:
            verify_queue(h, prefix, q, ledger.expected(q, t), tenant=t, bucket=bucket[t])
    if any(owned_there.values()):
        raise Failed(f"i: the node that cannot open the secrets owns tenant queues: {owned_there}")
    odd.stop(grace_s(30))
    odd.cell_env["QUEEN_ENCRYPTION_KEY"] = key
    odd.start(node_knobs())
    odd.wait_healthy(120)
    healed = wait_sink([odd], ta, timeout=RELOAD_S * 3 + 30)[odd.name]
    report.append(
        f"{odd.name} restarted with a different QUEEN_ENCRYPTION_KEY: its tenant sinks in phase=error ({errors}), no "
        f"tenant queue owned there, the others shipped a further round exactly once; back on the cell's key its "
        f"sinks ran again ({healed:.1f}s)"
    )

    # A DELETE made on one node stops the tenant's sink on every node.
    cp_ok(nodes[0], "DELETE", f"/clusters/{slug[tb]}/s3")
    stops = wait_sink(nodes, tb, gone=True, timeout=RELOAD_S * 3 + 30)
    still = {b.name: b.s3_sink(ta).get("phase") for b in nodes}
    if any(p != "running" for p in still.values()):
        raise Failed(f"i: deleting tenant b's sink disturbed tenant a's: {still}")
    report.append(
        f"DELETE of {slug[tb]}'s sink on i-node1: stopped on every node ("
        + ", ".join(f"{n} {s:.1f}s" for n, s in sorted(stops.items()))
        + f"), {slug[ta]}'s sink still running everywhere"
    )
    for b in nodes:
        b.sigterm()
    for b in nodes:
        b.wait_exit(60)
    for t in queues:
        cleanup(h, [], [prefix], bucket=bucket[t])
    cleanup(h, nodes, [])
    return "; ".join(report)


def scenario_j(h):
    """Queues end spread over the nodes after a STAGGERED cold start.

    At a cold start the node that can claim first (its reads go through while
    the others boot into an election, or it simply booted first — a
    StatefulSet starts its pods one after another) takes most of the queues.
    Claiming alone cannot undo that; rebalancing does (connectors/queen-s3
    src/placement.rs): every node counts the live sink nodes through presence
    rows, a node over its fair share ceil(queues / nodes) gives one queue back
    at a time, a node below it takes it. Here: nodes 1 and 2 boot, then node 3
    boots 1.5 s later (a rolling rollout, a slow pod), six queues, TTL 3 s.
    Proves: the placement converges to the fair share (2/2/2) within a
    deadline, and once there nothing moves for three TTLs (no thrash), with
    every record still exactly once is the other scenarios' business."""
    queues = [f"j.q{i}" for i in range(6)]
    prefix = "j-stagger"
    nodes = [Broker(h, f"j-node{i}", node_id=i) for i in (1, 2, 3)]
    for b, rp in zip(nodes, free_ports(3)):
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
    for b in nodes:
        b.start(NO_SINK)
    for b in nodes:
        b.wait_healthy(120)
    configure_queues(h, nodes[0], queues)
    for b in nodes:
        b.sigterm()
    for b in nodes:
        b.wait_exit(60)
    env = sink_env(h, queues, prefix)
    nodes[0].start(env)
    nodes[1].start(env)
    time.sleep(1.5)
    nodes[2].start(env)
    for b in nodes:
        b.wait_healthy(120)

    def counts_of(owners):
        return {b.name: sum(1 for n in owners.values() if n == b.name) for b in nodes}

    share = 2  # ceil(6 queues / 3 nodes)
    first = counts_of(wait_owned(h, nodes, queues, timeout=60))
    t0 = time.time()
    deadline = t0 + 120
    while True:
        owners = wait_owned(h, nodes, queues, timeout=60)
        counts = counts_of(owners)
        if max(counts.values()) <= share:
            break
        if time.time() > deadline:
            for b in nodes:
                b.sigterm()
            for b in nodes:
                b.wait_exit(60)
            raise Failed(
                f"BUG (rebalancing): 6 queues still placed {counts} {time.time() - t0:.0f}s after a staggered "
                f"cold start (first placement {first}); every node should converge to its fair share {share}"
            )
        time.sleep(0.5)
    settled_s = time.time() - t0

    def hold_steady(members, owners, seconds):
        """Every move seen among `members` while `seconds` pass (none expected)."""
        moved = []
        hold_until = time.time() + seconds
        while time.time() < hold_until:
            now = wait_owned(h, members, queues, timeout=60)
            if now != owners:
                moved.append({q: (owners[q], now[q]) for q in queues if now[q] != owners[q]})
                owners = now
            time.sleep(0.5)
        return moved

    # Settled: nothing moves for three TTLs (no thrash).
    hold_s = 3 * 3
    moved = hold_steady(nodes, owners, hold_s)
    if moved:
        raise Failed(f"BUG (thrash): queues moved after the placement settled at {counts}: {moved}")
    report = [
        f"staggered cold start: first {first}, {counts} {settled_s:.1f}s later, no move for {hold_s}s"
    ]

    # --- A late joiner, under traffic: the StatefulSet case --------------------------------
    # Node 3 stops; nodes 1 and 2 take its queues (3/3, the share of two). Node 3
    # comes back: now they hold more than the share of three, and each gives one
    # queue back to it — drained and released while records keep arriving — and
    # every record still lands exactly once.
    ledger = Ledger()
    rnd = random.Random(10)
    parts = [f"p{i}" for i in range(4)]

    def gen(r):
        return [
            Item(q, p, f"j-{q}-{i}-{r}", gen_payload(rnd, "j", p, r))
            for q in queues
            for i, p in enumerate(parts)
        ]

    late = nodes[2]
    pair = nodes[:2]
    pusher = Pusher(ledger, gen, [b.url for b in pair], pause=0.05).start()
    late.stop(30)
    deadline = time.time() + 60
    while True:
        two = counts_of(wait_owned(h, pair, queues, timeout=60))
        if max(two.values()) <= 3:
            break
        if time.time() > deadline:
            raise Failed(f"j: two nodes did not settle at 3/3 after node 3 stopped: {two}")
        time.sleep(0.5)
    t_join = time.time()
    late.start(env)
    late.wait_healthy(120)
    pusher.set_urls([b.url for b in nodes])
    deadline = t_join + 120
    while True:
        owners = wait_owned(h, nodes, queues, timeout=60)
        counts = counts_of(owners)
        if max(counts.values()) <= share:
            break
        if time.time() > deadline:
            raise Failed(
                f"BUG (rebalancing): after {late.name} rejoined, 6 queues still placed {counts} "
                f"{time.time() - t_join:.0f}s later (before it: {two}); the share of three nodes is {share}"
            )
        time.sleep(0.5)
    joined_s = time.time() - t_join
    moved = hold_steady(nodes, owners, hold_s)
    if moved:
        raise Failed(f"BUG (thrash): queues moved after the late joiner settled at {counts}: {moved}")
    time.sleep(2.0)
    leftover = pusher.stop()
    urls = [b.url for b in nodes]
    push(urls, leftover, ledger)
    unacked = ledger.unacknowledged()
    if unacked:
        raise Failed(f"j: {len(unacked)} pushed items never acknowledged, e.g. {unacked[:3]}")
    wait_lake(h, prefix, queues, ledger, nodes, timeout=240)
    stats = {q: verify_queue(h, prefix, q, ledger.expected(q)) for q in queues}
    records = sum(s["records"] for s in stats.values())
    report.append(
        f"late joiner under traffic: {two} with node 3 down, {counts} {joined_s:.1f}s after it rejoined "
        f"(queues given back while records kept arriving), no move for {hold_s}s; "
        f"{records} records exactly once"
    )

    for b in nodes:
        b.sigterm()
    for b in nodes:
        b.wait_exit(60)
    cleanup(h, nodes, [prefix])
    return "; ".join(report)


def scenario_k(h):
    """Does a SIGTERMed node give its leases back — the LEADER as well as a
    follower?

    At the signal the sink starts its drain at once (main.rs), beside the
    leadership hand-off: every queue stops reading and calls
    `Lease::release` (lease.rs: a KV get, then a fenced delete, tried again
    for up to ten seconds when a call fails). On the leader those calls race
    with its own hand-off. Rounds alternate: SIGTERM the leader when it owns queues, else a
    follower that does; a LeaseWatch on a surviving node tells, per lease,
    whether the row went away before its own expiresAt (released) or at it
    (expired). TTL 3 s; the default 30 s makes an expired lease ten times as
    costly."""
    queues = [f"k.q{i}" for i in range(6)]
    prefix = "k-release"
    nodes = [Broker(h, f"k-node{i}", node_id=i) for i in (1, 2, 3)]
    for b, rp in zip(nodes, free_ports(3)):
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
    for b in nodes:
        b.start(NO_SINK)
    for b in nodes:
        b.wait_healthy(120)
    configure_queues(h, nodes[0], queues)
    for b in nodes:
        b.sigterm()
    for b in nodes:
        b.wait_exit(60)
    env = sink_env(h, queues, prefix)
    for b in nodes:
        b.start(env)
    for b in nodes:
        b.wait_healthy(120)
    wait_owned(h, nodes, queues, timeout=60)
    instances = {b.name: b.s3_sink().get("instance") for b in nodes}
    outcome = {"leader": [], "follower": []}
    rounds = []
    for rnd in range(6):
        owners = wait_owned(h, nodes, queues, timeout=60)
        held_by = defaultdict(list)
        for q, n in owners.items():
            held_by[n].append(q)
        roles = {b.name: b.role() for b in nodes}
        leader = next(n for n, r in roles.items() if r == "leader")
        want = "leader" if rnd % 2 == 0 else "follower"
        pick = None
        if want == "leader" and held_by.get(leader):
            pick = leader
        else:
            followers = [n for n in held_by if roles.get(n) == "follower"]
            if followers:
                pick = max(followers, key=lambda n: len(held_by[n]))
            elif held_by.get(leader):
                pick = leader
        if pick is None:
            continue
        kind = roles[pick]
        target = next(b for b in nodes if b.name == pick)
        others = [b for b in nodes if b is not target]
        held = sorted(held_by[pick])
        watch = LeaseWatch(others[0], held, instances[pick]).start()
        target.sigterm()
        target.wait_exit(40)
        deadline = time.time() + 3 + 3 + 15
        while time.time() < deadline:
            o = owners_now(others)
            if all(len(o.get(q, [])) == 1 for q in held):
                break
            time.sleep(0.1)
        time.sleep(0.5)
        released = watch.stop()
        for q in held:
            outcome[kind].append(bool(released[q].get("before_expiry")))
        rounds.append((pick, kind, {q: ("released" if released[q].get("before_expiry") else "EXPIRED") for q in held}))
        target.start(env)
        target.wait_healthy(120)
        wait_caught_up(nodes, target)
    for b in nodes:
        b.sigterm()
    for b in nodes:
        b.wait_exit(60)
    cleanup(h, nodes, [prefix])
    tally = {k: f"{sum(v)}/{len(v)} released" for k, v in outcome.items()}
    if not all(outcome["leader"] + outcome["follower"]):
        raise Failed(
            f"BUG: leases held by a SIGTERMed node expired instead of being released: {tally} "
            f"(rounds: {rounds}). The drain starts at the signal beside the leadership hand-off, and "
            f"Lease::release gives up silently when its KV get or delete fails"
        )
    return f"every lease of a SIGTERMed node released before its expiry: {tally}; rounds {rounds}"


def sealed_rows(node, secrets_of):
    """What the proxy's sink table holds, read straight from the broker port
    as the proxy's own tenant: a sealed secret, never the plaintext. Reported,
    not asserted, when the broker does not answer that tenant's KV."""
    body = json.dumps({"operations": [{"ns": "px.s3sinks", "op": "getPrefix", "prefix": "#", "limit": 100}]}).encode()
    st, _, raw = http(
        "POST",
        node.url + "/api/v1/kv",
        body,
        {"content-type": "application/json", "x-queen-tenant": PROXY_TENANT},
        timeout=10,
    )
    if st != 200:
        return f"proxy table not readable through the broker port as the proxy tenant ({st}: {short(raw, 120)})"
    if any(s.encode() in raw for s in secrets_of.values()):
        raise Failed(f"h: the proxy's sink table holds an S3 secret in clear: {short(raw, 300)}")
    rows = json.loads(raw)["results"][0].get("rows", [])
    sealed = [r["value"].get("secret_key_sealed", "") for r in rows if isinstance(r.get("value"), dict)]
    return (
        f"the proxy's sink rows ({len(rows)}, read through the broker port as tenant {PROXY_TENANT}) hold the secret "
        f"sealed only ({[len(s) for s in sealed]} chars, no plaintext)"
    )


# =============================================================================================


SCENARIOS = [
    ("s3", scenario_s3),
    ("self", scenario_self),
    ("a", scenario_a),
    ("b", scenario_b),
    ("b2", scenario_b2),
    ("c", scenario_c),
    ("d", scenario_d),
    ("e", scenario_e),
    ("g", scenario_g),
    ("f", scenario_f),
    ("h", scenario_h),
    ("i", scenario_i),
    ("j", scenario_j),
    ("k", scenario_k),
]


def main():
    ap = argparse.ArgumentParser(description="End-to-end suite of the in-process S3 sink.")
    ap.add_argument("--bin", default=os.environ.get("QUEEN_BIN", os.path.join(REPO, "target", "debug", "queen")))
    ap.add_argument("--work", default=None, help="work directory (default: a new temp dir)")
    ap.add_argument("--keep", action="store_true", help="keep data dirs, logs and the bucket")
    ap.add_argument("--scenario", default=",".join(n for n, _ in SCENARIOS))
    ap.add_argument("--crash-points", default=",".join(CRASH_POINTS))
    ap.add_argument("--a-partitions", default="1100,200,50", help="partitions of each of scenario a's three queues")
    ap.add_argument("--a-records", type=int, default=20, help="records per partition in scenario a")
    ap.add_argument("--a-pause", type=float, default=0.3, help="seconds between scenario a's push rounds")
    ap.add_argument("--f-seconds", type=int, default=40, help="how long scenario f drives the race")
    ap.add_argument("--rust-log", default="warn,queen-s3=info,boot=info,shutdown=info")
    args = ap.parse_args()
    args.crash_points = [p for p in args.crash_points.split(",") if p]
    for p in args.crash_points:
        if p not in CRASH_POINTS:
            ap.error(f"unknown crash point {p}")
    wanted = [s for s in args.scenario.split(",") if s]
    known = dict(SCENARIOS)
    for s in wanted:
        if s not in known:
            ap.error(f"unknown scenario {s}; known: {', '.join(known)}")
    if not os.access(args.bin, os.X_OK):
        print(f"no broker binary at {args.bin}: build it with cargo build --manifest-path server/Cargo.toml --bin queen", file=sys.stderr)
        return 2
    made_work = args.work is None
    args.work = os.path.abspath(args.work or tempfile.mkdtemp(prefix="queen-s3sink-"))
    os.makedirs(args.work, exist_ok=True)
    h = Harness(args)
    h.s3 = FakeS3(h)
    h.s3.start()
    h.say(f"s3sink e2e: binary {args.bin}; work {args.work}; fake S3 {h.s3.endpoint}")
    results, failures = [], []
    started = time.time()
    try:
        for name in wanted:
            t0 = time.time()
            h.say(f"--- scenario {name}")
            try:
                line = known[name](h)
                results.append((name, line))
                h.say(f"PASS {name} ({time.time() - t0:.0f}s): {line}")
            except Exception as e:  # noqa: BLE001 — the suite reports, it does not stop
                failures.append((name, str(e)))
                h.say(f"FAIL {name} ({time.time() - t0:.0f}s): {e}")
            finally:
                h.kill_all()
    finally:
        h.kill_all()
        h.s3.stop()
    h.say("")
    h.say("=" * 30 + " s3sink e2e " + "=" * 30)
    for name, line in results:
        h.say(f"PASS {name}: {line}")
    for name, why in failures:
        h.say(f"FAIL {name}: {why.splitlines()[0]}")
    h.say(f"{len(results)} passed, {len(failures)} failed in {time.time() - started:.0f}s; logs in {os.path.join(args.work, 'logs')}")
    if not args.keep and not failures:
        if made_work:
            shutil.rmtree(args.work, ignore_errors=True)
        else:
            shutil.rmtree(os.path.join(args.work, "data"), ignore_errors=True)
            shutil.rmtree(h.s3.root, ignore_errors=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
