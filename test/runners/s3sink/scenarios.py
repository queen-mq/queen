#!/usr/bin/env python3
"""The S3 sink end to end (PLAN_S3_SINK.md §9), with nothing simulated.

A real broker (the compose stack), a real S3 gateway (versitygw, posix backend),
the real `queen-s3` binary started and killed as a process, and a real lake
reader (DuckDB) that never speaks to Queen — it globs the bucket the way the
reference page tells a user to.

The scenarios, in the plan's numbering:

  1  scale       N partitions, M records, pushed WHILE the sink runs: the lake
                 holds every record exactly once, with no gap per partition and
                 in offset order inside each object.
  2  crash       the five points of `QUEEN_S3_CRASH_AT`, each one killing a real
                 process mid-protocol. After the restart, every object that
                 existed before the crash is BYTE-IDENTICAL, and (1) still holds.
  3  retention   a queue whose retention eats records the sink had not read:
                 `queen_s3_records_lost_total` counts them and the window's
                 manifest names the gap.
  5  alignment   every record's `ts` falls inside its object's own `dt=`/`hour=`
                 bucket, under `hour` and under `day`.
  6  layout      `per-partition` on a sixteen-lane numbered queue: one object per
                 lane per window, with the offset range in the key.
  7  start       `start=latest` after a backlog: nothing from before T_0.

Two rules this file follows, both learned from the assertions that are worth
having:

  * **assert through the reader, not through the sink.** Counts, duplicates and
    gaps come from DuckDB over the objects; `/metrics` is used for things only
    the sink knows (records lost, windows committed).
  * **never assert on a timing you did not force.** Window boundaries move with
    the broker's clock, so nothing here compares two independent runs; the crash
    matrix compares a run against ITSELF, across the crash.
"""

import concurrent.futures
import hashlib
import json
import os
import pathlib
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import duckdb

# --- the world ---------------------------------------------------------------

QUEEN = os.environ.get("QUEEN_HTTP_URL", "http://broker:6632").rstrip("/")
DATA = os.environ.get("S3SINK_DATA", "/data")
BUCKET = "queen-lake"
GW_PORT = 7070
GW_ENDPOINT = f"http://127.0.0.1:{GW_PORT}"
AK = "queens3test"
SK = "queens3secret"
LISTEN_BASE = 9333
_next_port = [LISTEN_BASE]


def take_port():
    _next_port[0] += 1
    return _next_port[0]


# Every sink this process starts, so a scenario that raises cannot leave one
# running: the next scenario would find its port taken and fail for a reason
# that has nothing to do with what it tests.
LIVE = []
LOGS = pathlib.Path("/tmp/s3sink-logs")

# Sized so the whole suite finishes in a few minutes on a laptop; both are env
# knobs so the same file can be pointed at a real cluster.
SCALE_PARTITIONS = int(os.environ.get("S3SINK_SCALE_PARTITIONS", "2000"))
SCALE_PER_PARTITION = int(os.environ.get("S3SINK_SCALE_PER_PARTITION", "50"))
PUSH_BATCH = int(os.environ.get("S3SINK_PUSH_BATCH", "1000"))
PUSH_THREADS = int(os.environ.get("S3SINK_PUSH_THREADS", "4"))

# The sink's own knobs for a test: a two-second window instead of five minutes,
# and no guard of its own on top of the broker's five seconds.
SINK_TUNING = {
    "QUEEN_S3_MAX_WINDOW_MS": "2000",
    "QUEEN_S3_DISCOVERY_INTERVAL_MS": "250",
    "QUEEN_S3_SAFE_GUARD_MS": "0",
    "QUEEN_S3_CHECKPOINT_EVERY": "2",
    "QUEEN_S3_TARGET_MB": "16",
    "QUEEN_S3_LEASE_TTL_MS": "5000",
    "QUEEN_S3_START": "earliest",
    "QUEEN_S3_PATH_STYLE": "true",
    "RUST_LOG": "warn,queen_s3=info,queen-s3=info",
}


class Failed(Exception):
    pass


def say(msg):
    print(msg, flush=True)


# --- the broker --------------------------------------------------------------


def api(path, body=None, method=None, timeout=120):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        QUEEN + path,
        data=data,
        method=method or ("POST" if data is not None else "GET"),
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        raise Failed(f"{path} -> {e.code}: {e.read()[:400].decode(errors='replace')}") from None
    return json.loads(raw) if raw else {}


def configure(queue, **options):
    api("/api/v1/configure", {"queue": queue, "options": options})


def push(items):
    api("/api/v1/push", {"items": items})


def kv_get(key):
    res = api("/api/v1/kv", {"operations": [{"ns": "queen-s3", "op": "get", "key": key}]})
    row = res.get("results", [{}])[0]
    return row.get("value") if row.get("found") else None


def changed(queue, limit=1000):
    """One page of the discovery endpoint — used here to WATCH the broker
    (a partition's logStart moving under retention), never to drive the sink."""
    res = api("/api/v1/partitions/changed", {"entries": [{"queue": queue, "limit": limit}]})
    entry = res["entries"][0]
    if entry.get("error"):
        raise Failed(f"partitions/changed({queue}) -> {entry['error']}")
    return res["safeTime"], res.get("safeTimeDegraded", False), entry["partitions"]


def push_records(queue, partitions, per_partition, tag):
    """Push `per_partition` records into each partition, batched and in
    parallel. Returns the number of records pushed."""
    items = []
    for p in partitions:
        for i in range(per_partition):
            items.append(
                {
                    "queue": queue,
                    "partition": p,
                    "transactionId": f"{tag}-{p}-{i}",
                    "payload": {"tag": tag, "p": p, "i": i, "pad": "x" * 24},
                }
            )
    batches = [items[i : i + PUSH_BATCH] for i in range(0, len(items), PUSH_BATCH)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=PUSH_THREADS) as pool:
        for _ in pool.map(push, batches):
            pass
    return len(items)


# --- the gateway --------------------------------------------------------------


def wait_port(host, port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex((host, port)) == 0:
                return
        time.sleep(0.2)
    raise Failed(f"nothing listening on {host}:{port} after {timeout}s")


def start_gateway():
    """versitygw with the posix backend: the bucket is a directory under
    /data, so every object the sink writes is a FILE the reader can open."""
    pathlib.Path(f"{DATA}/{BUCKET}").mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    log = open(LOGS / "versitygw.log", "wb")
    env = dict(os.environ, ROOT_ACCESS_KEY=AK, ROOT_SECRET_KEY=SK)
    proc = subprocess.Popen(
        ["versitygw", "posix", DATA], env=env, stdout=log, stderr=subprocess.STDOUT
    )
    wait_port("127.0.0.1", GW_PORT)
    return proc


# --- the sink -----------------------------------------------------------------


class Sink:
    """One `queen-s3` process. Started, watched through /metrics, and stopped
    with the signal an orchestrator would send."""

    def __init__(self, name, prefix, queues, sink="default", **extra):
        self.port = take_port()
        self.listen = f"127.0.0.1:{self.port}"
        self.metrics_url = f"http://127.0.0.1:{self.port}/metrics"
        self.name = name
        self.prefix = prefix
        self.queues = queues
        self.sink = sink
        self.extra = extra
        self.proc = None
        self.log_path = LOGS / f"sink-{name}.log"

    def env(self):
        env = dict(os.environ)
        env.update(SINK_TUNING)
        env.update(
            {
                "QUEEN_URL": QUEEN,
                "QUEEN_S3_QUEUES": self.queues,
                "QUEEN_S3_SINK": self.sink,
                "QUEEN_S3_ENDPOINT": GW_ENDPOINT,
                "QUEEN_S3_REGION": "us-east-1",
                "QUEEN_S3_BUCKET": BUCKET,
                "QUEEN_S3_PREFIX": self.prefix,
                "QUEEN_S3_ACCESS_KEY": AK,
                "QUEEN_S3_SECRET_KEY": SK,
                "QUEEN_S3_LISTEN": self.listen,
            }
        )
        # An inherited crash point would arm every sink in the process; only
        # what this Sink was constructed with counts.
        env.pop("QUEEN_S3_CRASH_AT", None)
        env.update(self.extra)
        return env

    def start(self):
        LOGS.mkdir(parents=True, exist_ok=True)
        log = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            ["queen-s3"], env=self.env(), stdout=log, stderr=subprocess.STDOUT
        )
        LIVE.append(self)
        # /metrics answers as soon as the listener is up, which is before any
        # queue is claimed — enough to know the process booted.
        try:
            wait_port("127.0.0.1", self.port, timeout=30)
        except Failed:
            raise Failed(f"sink {self.name} did not start:\n{self.tail()}") from None
        if not self.alive():
            raise Failed(
                f"sink {self.name} exited {self.proc.returncode} at boot\n{self.tail()}"
            )
        return self

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def tail(self, lines=40):
        try:
            return "\n".join(self.log_path.read_text(errors="replace").splitlines()[-lines:])
        except OSError:
            return "(no log)"

    def metrics(self):
        with urllib.request.urlopen(self.metrics_url, timeout=10) as r:
            return r.read().decode()

    def metric(self, name, label=None, default=0.0):
        want = re.compile(
            "^" + re.escape(name) + r"(\{[^}]*\})?\s+([0-9.eE+-]+)$",
        )
        total = None
        for line in self.metrics().splitlines():
            if line.startswith("#"):
                continue
            m = want.match(line.strip())
            if not m:
                continue
            if label and label not in (m.group(1) or ""):
                continue
            total = (total or 0.0) + float(m.group(2))
        return default if total is None else total

    def wait_metric(self, name, at_least, label=None, timeout=180, what=""):
        deadline = time.time() + timeout
        seen = 0.0
        while time.time() < deadline:
            if not self.alive():
                raise Failed(
                    f"sink {self.name} exited (rc={self.proc.returncode}) while waiting for "
                    f"{name} >= {at_least}\n{self.tail()}"
                )
            try:
                seen = self.metric(name, label)
            except Exception:  # noqa: BLE001 — a scrape that failed is not an answer
                seen = 0.0
            if seen >= at_least:
                return seen
            time.sleep(0.5)
        raise Failed(
            f"timed out after {timeout}s waiting for {name} >= {at_least} "
            f"(last {seen}) {what}\n{self.tail()}"
        )

    def wait_exit(self, timeout=120):
        """For a run with QUEEN_S3_CRASH_AT set: the process is expected to die
        on its own, by abort."""
        try:
            rc = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Failed(
                f"sink {self.name} never reached its crash point\n{self.tail()}"
            ) from None
        if self in LIVE:
            LIVE.remove(self)
        return rc

    def kill(self):
        if self.alive():
            self.proc.kill()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        if self in LIVE:
            LIVE.remove(self)

    def stop(self, timeout=60):
        if not self.alive():
            return self.proc.returncode if self.proc else None
        self.proc.send_signal(signal.SIGTERM)
        try:
            rc = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            raise Failed(f"sink {self.name} ignored SIGTERM for {timeout}s\n{self.tail()}")
        if self in LIVE:
            LIVE.remove(self)
        if rc != 0:
            raise Failed(f"sink {self.name} exited {rc} on SIGTERM\n{self.tail()}")
        return rc


def wait_port_free(host, port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex((host, port)) != 0:
                return
        time.sleep(0.2)
    raise Failed(f"{host}:{port} is still in use after {timeout}s")


# --- the lake ------------------------------------------------------------------


def lake_root(prefix):
    return f"{DATA}/{BUCKET}/{prefix}"


def data_files(prefix):
    root = pathlib.Path(lake_root(prefix))
    if not root.exists():
        return []
    return sorted(
        str(p)
        for p in root.rglob("*")
        if p.is_file() and "_queen" not in p.parts and not p.name.startswith(".")
    )


def manifests(prefix):
    root = pathlib.Path(lake_root(prefix)) / "_queen"
    if not root.exists():
        return []
    out = []
    for p in sorted(root.rglob("windows/*.json")):
        out.append(json.loads(p.read_text()))
    return sorted(out, key=lambda m: m["k"])


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(prefix):
    """Every data object's key and content hash: what a crash must not change."""
    return {p: sha256_of(p) for p in data_files(prefix)}


def glob_for(prefix, ext, hourly=True):
    hour = "/hour=*" if hourly else ""
    return f"{lake_root(prefix)}/queue=*/dt=*{hour}/*.{ext}"


def read_lake(prefix, ext="jsonl.zst", hourly=True, filename=False):
    """The reader of plan §7: DuckDB, straight off the files, no Queen."""
    con = duckdb.connect()
    reader = "read_parquet" if ext == "parquet" else "read_json_auto"
    args = f"'{glob_for(prefix, ext, hourly)}'"
    if filename:
        args += ", filename = true"
    return con, f"{reader}({args})"


def lake_stats(prefix, ext="jsonl.zst", hourly=True):
    con, src = read_lake(prefix, ext, hourly)
    total, distinct, partitions = con.execute(
        f"""SELECT count(*), count(DISTINCT (partition || ':' || CAST("offset" AS VARCHAR))),
                   count(DISTINCT partition)
            FROM {src}"""
    ).fetchone()
    gaps = con.execute(
        f"""WITH r AS (SELECT partition, "offset" AS off FROM {src}),
                 s AS (SELECT partition, off,
                              lag(off) OVER (PARTITION BY partition ORDER BY off) AS prev
                       FROM r)
            SELECT count(*) FROM s WHERE prev IS NOT NULL AND off <> prev + 1"""
    ).fetchone()[0]
    return {
        "records": total,
        "distinct": distinct,
        "partitions": partitions,
        "gaps": gaps,
    }


def assert_lake(prefix, expect_records, expect_partitions, ext="jsonl.zst", hourly=True):
    s = lake_stats(prefix, ext, hourly)
    if s["records"] != expect_records:
        raise Failed(f"{prefix}: {s['records']} records in the lake, expected {expect_records}")
    if s["distinct"] != expect_records:
        raise Failed(
            f"{prefix}: {s['records'] - s['distinct']} duplicate (partition, offset) pairs"
        )
    if s["partitions"] != expect_partitions:
        raise Failed(f"{prefix}: {s['partitions']} partitions, expected {expect_partitions}")
    if s["gaps"]:
        raise Failed(f"{prefix}: {s['gaps']} offset gaps inside a partition")
    return s


def assert_physical_order(prefix):
    """Inside every object, records are in (partition, offset) order — the
    property that makes `ORDER BY partition, offset` a sort of an already
    sorted file, and the one a reader cannot check for itself."""
    checked = 0
    for path in data_files(prefix):
        if not path.endswith(".jsonl.zst"):
            continue
        raw = subprocess.run(
            ["zstd", "-dc", path], check=True, capture_output=True
        ).stdout.decode()
        last = None
        for line in raw.splitlines():
            if not line:
                continue
            row = json.loads(line)
            key = (row["partition"], row["offset"])
            if last is not None and key <= last:
                raise Failed(f"{path}: {key} follows {last}: the object is not sorted")
            last = key
        checked += 1
    return checked


def lake_settled(prefix, total):
    """The post-condition of a restart, used as the thing to WAIT for.

    Counting records is not enough: a crash mid-upload leaves a data object
    that no manifest names yet, and a reader counting rows would see the final
    number while the window it belongs to is still uncommitted. So the wait is
    for the invariant itself — every object named by a manifest, and the count
    right — which is also true immediately when the crash left nothing to redo.
    """
    try:
        named = {
            f"{DATA}/{BUCKET}/{obj['key']}"
            for m in manifests(prefix)
            for obj in m["objects"]
        }
        if set(data_files(prefix)) != named:
            return False
        return lake_stats(prefix)["records"] == total
    except Exception:  # noqa: BLE001 — an empty or half-written glob is not an answer
        return False


def assert_manifests(prefix, expect_lost=False):
    """The manifest describes what is actually in the bucket, and the windows
    tile with no gap."""
    ms = manifests(prefix)
    if not ms:
        raise Failed(f"{prefix}: no manifest was written")
    prev = None
    lost = 0
    named = set()
    for m in ms:
        for obj in m["objects"]:
            path = f"{DATA}/{BUCKET}/{obj['key']}"
            if not os.path.exists(path):
                raise Failed(f"manifest {m['k']} names {obj['key']}, which is not in the bucket")
            if sha256_of(path) != obj["sha256"]:
                raise Failed(f"{obj['key']}: sha256 does not match the manifest")
            if os.path.getsize(path) != obj["bytes"]:
                raise Failed(f"{obj['key']}: size does not match the manifest")
            named.add(path)
        if prev is not None:
            if m["k"] != prev["k"] + 1:
                raise Failed(f"{prefix}: window numbering skipped at {m['k']}")
            if m["tStart"] != prev["tEnd"]:
                raise Failed(
                    f"{prefix}: window {m['k']} starts at {m['tStart']}, "
                    f"window {prev['k']} ended at {prev['tEnd']}"
                )
        prev = m
        lost += sum((r["to"] - r["from"] + 1) for r in m.get("lost", []))
    on_disk = set(data_files(prefix))
    if on_disk != named:
        raise Failed(
            f"{prefix}: objects with no manifest: {sorted(on_disk - named)[:3]}; "
            f"manifests with no object: {sorted(named - on_disk)[:3]}"
        )
    if expect_lost and lost == 0:
        raise Failed(f"{prefix}: no manifest names a lost range")
    return {"windows": len(ms), "lost": lost}


def assert_bucket_exactness(prefix, hourly=True):
    """Every record's `ts` is inside its object's own Hive bucket — the property
    `align` buys, checked through the reader's own hive_partitioning."""
    # The bucket is read out of the KEY with a regex rather than through
    # hive_partitioning: the question here is what the KEY says, and a hive
    # column would answer with what DuckDB decided the key means.
    con, src = read_lake(prefix, "jsonl.zst", hourly, filename=True)
    key_dt = "regexp_extract(filename, 'dt=([0-9-]+)', 1)"
    key_hour = "regexp_extract(filename, 'hour=([0-9]+)', 1)"
    where = f"{key_dt} <> strftime(ts, '%Y-%m-%d')"
    if hourly:
        where += f" OR {key_hour} <> strftime(ts, '%H')"
    bad = con.execute(f"SELECT count(*) FROM {src} WHERE {where}").fetchone()[0]
    if bad:
        raise Failed(f"{prefix}: {bad} records outside the bucket their object is in")
    bucket_expr = (
        "regexp_extract(filename, 'dt=[0-9-]+/hour=[0-9]+', 0)"
        if hourly
        else "regexp_extract(filename, 'dt=[0-9-]+', 0)"
    )
    return con.execute(f"SELECT count(DISTINCT {bucket_expr}) FROM {src}").fetchone()[0]


# --- scenarios ------------------------------------------------------------------


def scenario_scale(results):
    """(1) Partitions and records at scale, pushed while the sink runs."""
    queue = "s3sink-scale"
    prefix = "s1"
    partitions = [f"cust-{i:05d}" for i in range(SCALE_PARTITIONS)]
    total = SCALE_PARTITIONS * SCALE_PER_PARTITION
    configure(queue, retentionEnabled=False)

    sink = Sink("scale", prefix, queue).start()
    t0 = time.time()
    pushed = push_records(queue, partitions, SCALE_PER_PARTITION, "s1")
    push_s = time.time() - t0
    sink.wait_metric(
        "queen_s3_records_written_total",
        pushed,
        label=f'queue="{queue}"',
        timeout=600,
        what="(the whole backlog committed)",
    )
    windows = sink.metric("queen_s3_windows_committed_total", f'queue="{queue}"')
    lag = sink.metric("queen_s3_lag_seconds", f'queue="{queue}"')
    sink.stop()

    stats = assert_lake(prefix, total, SCALE_PARTITIONS)
    files = len(data_files(prefix))
    ordered = assert_physical_order(prefix)
    man = assert_manifests(prefix)
    results.append(
        f"1 scale       {SCALE_PARTITIONS} partitions x {SCALE_PER_PARTITION} = {total} records "
        f"pushed in {push_s:.1f}s -> {int(windows)} windows, {files} objects, "
        f"{stats['records']} rows read back, 0 duplicates, 0 gaps, {ordered} objects sorted, "
        f"{man['windows']} manifests, lag {lag:.1f}s"
    )


CRASH_POINTS = ["after_intent", "mid_upload", "after_upload", "before_commit", "after_commit"]


def scenario_crash(results):
    """(2) The crash matrix: a real process killed at each of the five points."""
    queue = "s3sink-crash"
    configure(queue, retentionEnabled=False)
    partitions = [f"lane-{i}" for i in range(8)]
    # One log, five independent sinks over it: a distinct QUEEN_S3_SINK is a
    # distinct pair of commit-pointer keys and a distinct prefix is a distinct
    # lake, so the five points never see each other's state — and the log is
    # pushed once instead of five times.
    total = push_records(queue, partitions, 60, "c0")

    lines = []
    for point in CRASH_POINTS:
        prefix = f"s2-{point}"
        sink_name = f"c-{point}"
        # A first, honest run: the crash must land on a window with a committed
        # one behind it, or "the objects that already existed" is an empty set.
        warm = Sink(f"{sink_name}-warm", prefix, queue, sink=sink_name).start()
        warm.wait_metric(
            "queen_s3_windows_committed_total", 1, f'queue="{queue}"', timeout=180
        )
        warm.stop()
        before_more = len(data_files(prefix))

        # More arrives while the sink is down, so the crash lands on a window
        # that has a committed one behind it.
        total += push_records(queue, partitions, 40, f"c1-{point}")

        crash = Sink(
            f"{sink_name}-crash",
            prefix,
            queue,
            sink=sink_name,
            QUEEN_S3_CRASH_AT=point,
        ).start()
        rc = crash.wait_exit(timeout=180)
        if rc == 0:
            raise Failed(f"{point}: the sink exited cleanly; the crash point never fired")
        snapshot = fingerprint(prefix)
        intent = kv_get(f"s3:{sink_name}:{queue}:intent")
        committed = kv_get(f"s3:{sink_name}:{queue}:committed")
        if point == "after_intent":
            if not intent or not committed or intent["k"] != committed["k"] + 1:
                raise Failed(
                    f"{point}: expected an intent one window ahead of the commit, "
                    f"got intent={intent} committed={committed}"
                )
        if point in ("mid_upload", "after_upload", "before_commit") and len(snapshot) <= before_more:
            raise Failed(
                f"{point}: expected the in-flight window's object to be on disk before the "
                f"restart ({len(snapshot)} objects, {before_more} before the crash)"
            )

        again = Sink(f"{sink_name}-restart", prefix, queue, sink=sink_name).start()
        # Wait on the LAKE, not on a commit: `after_commit` crashes with the
        # pointer already moved, so its restart has nothing left to ship and a
        # wait for "one more window" would hang on the one point where the
        # protocol worked perfectly.
        deadline = time.time() + 180
        while time.time() < deadline:
            if lake_settled(prefix, total):
                break
            time.sleep(1.0)
        again.stop()

        for path, digest in snapshot.items():
            if not os.path.exists(path):
                raise Failed(f"{point}: {path} disappeared across the restart")
            if sha256_of(path) != digest:
                raise Failed(
                    f"{point}: {path} changed across the restart — a redo must be byte-identical"
                )
        stats = assert_lake(prefix, total, len(partitions))
        assert_physical_order(prefix)
        man = assert_manifests(prefix)
        lines.append(
            f"{point}(rc={rc}, {len(snapshot)} objects identical, "
            f"{man['windows']} windows, {stats['records']} records)"
        )

    results.append("2 crash       " + "; ".join(lines))


def scenario_retention(results):
    """(3) Retention deletes what the sink had not read yet."""
    queue = "s3sink-retention"
    prefix = "s3"
    partitions = [f"lane-{i}" for i in range(4)]
    configure(queue, retentionEnabled=False)
    first = push_records(queue, partitions, 40, "r0")

    sink = Sink("retention", prefix, queue, QUEEN_S3_CHECKPOINT_EVERY="1").start()
    sink.wait_metric("queen_s3_records_written_total", first, f'queue="{queue}"', timeout=180)
    sink.stop()

    # The sink is down. More arrives, and retention eats it before the sink
    # comes back: a gap it can only report, never recover.
    lost_push = push_records(queue, partitions, 40, "r1")
    configure(queue, retentionEnabled=True, retentionSeconds=1)
    # Wait for retention to pass the sink's OWN position, not merely to start:
    # the first cycle deletes the segment the sink already shipped, which is no
    # gap at all. `per_lane` is everything pushed so far, so a logStart at or
    # above it means the r1 records — the ones the sink never read — are gone.
    per_lane = (first + lost_push) // len(partitions)
    deadline = time.time() + 240
    log_start = 0
    while time.time() < deadline:
        _, _, parts = changed(queue)
        log_start = min(p["logStart"] for p in parts) if parts else 0
        if log_start >= per_lane:
            break
        time.sleep(1.0)
    if log_start < per_lane:
        raise Failed(
            f"retention moved logStart only to {log_start}, not past the sink's position "
            f"({per_lane}); there is no gap to report"
        )
    configure(queue, retentionEnabled=False)

    survivors = push_records(queue, partitions, 20, "r2")
    again = Sink("retention-2", prefix, queue, QUEEN_S3_CHECKPOINT_EVERY="1").start()
    again.wait_metric(
        "queen_s3_records_lost_total",
        1,
        f'queue="{queue}"',
        timeout=240,
        what="(the sink must COUNT the gap, not stall on it)",
    )
    lost = again.metric("queen_s3_records_lost_total", f'queue="{queue}"')
    # …and it keeps committing rather than stalling on the gap: everything
    # retention left lands.
    again.wait_metric(
        "queen_s3_records_written_total",
        survivors,
        f'queue="{queue}"',
        timeout=240,
        what="(a sink that stalls on a gap loses more than one that carries on)",
    )
    windows = again.metric("queen_s3_windows_committed_total", f'queue="{queue}"')
    written = again.metric("queen_s3_records_written_total", f'queue="{queue}"')
    again.stop()

    man = assert_manifests(prefix, expect_lost=True)
    if man["lost"] == 0:
        raise Failed("no manifest names the gap retention made")
    stats = lake_stats(prefix)
    if stats["distinct"] != stats["records"]:
        raise Failed("the lake holds a duplicate after a retention overrun")
    results.append(
        f"3 retention   {first} committed, {lost_push} deleted by retention under the sink "
        f"(logStart -> {log_start}), {survivors} pushed after: records_lost_total={int(lost)}, "
        f"manifest names {man['lost']} lost offsets, {int(written)} records written in "
        f"{int(windows)} window(s), "
        f"{stats['records']} rows in the lake, 0 duplicates"
    )


def scenario_alignment(results):
    """(5) Hive buckets are exact, under `hour` and under `day`."""
    queue = "s3sink-align"
    partitions = [f"lane-{i}" for i in range(4)]
    configure(queue, retentionEnabled=False)
    pushed = push_records(queue, partitions, 25, "a0")

    hourly = Sink("align-hour", "s5-hour", queue, sink="align-hour").start()
    hourly.wait_metric("queen_s3_records_written_total", pushed, f'queue="{queue}"', timeout=180)
    hourly.stop()
    buckets = assert_bucket_exactness("s5-hour", hourly=True)
    assert_lake("s5-hour", pushed, len(partitions))
    hour_keys = data_files("s5-hour")
    if not all("/hour=" in k for k in hour_keys):
        raise Failed("align=hour must put an hour= component in every key")

    daily = Sink(
        "align-day", "s5-day", queue, sink="align-day", QUEEN_S3_ALIGN="day"
    ).start()
    daily.wait_metric("queen_s3_records_written_total", pushed, f'queue="{queue}"', timeout=180)
    daily.stop()
    day_keys = data_files("s5-day")
    if any("/hour=" in k for k in day_keys):
        raise Failed("align=day must not write an hour= component")
    assert_bucket_exactness("s5-day", hourly=False)
    assert_lake("s5-day", pushed, len(partitions), hourly=False)
    results.append(
        f"5 alignment   {pushed} records: hour lane {len(hour_keys)} objects across "
        f"{buckets} dt/hour bucket(s), every ts inside its own bucket; day lane "
        f"{len(day_keys)} objects with dt= and no hour="
    )


def scenario_per_partition(results):
    """(6) `per-partition` on a sixteen-lane numbered queue."""
    queue = "s3sink-numbered"
    prefix = "s6"
    partitions = [str(i) for i in range(16)]
    configure(queue, retentionEnabled=False)
    pushed = push_records(queue, partitions, 20, "p0")

    sink = Sink(
        "per-partition", prefix, queue, QUEEN_S3_LAYOUT="per-partition"
    ).start()
    sink.wait_metric("queen_s3_records_written_total", pushed, f'queue="{queue}"', timeout=180)
    sink.stop()

    keys = data_files(prefix)
    if not keys:
        raise Failed("per-partition wrote nothing")
    for key in keys:
        if not re.search(r"-p-\d+-\d{12}-\d{12}\.jsonl\.zst$", key):
            raise Failed(f"{key} is not a Connect-shaped per-partition key")
    per_object = set()
    for key in keys:
        con = duckdb.connect()
        names = con.execute(
            f"SELECT DISTINCT partition FROM read_json_auto('{key}')"
        ).fetchall()
        if len(names) != 1:
            raise Failed(f"{key} holds {len(names)} partitions; per-partition means one")
        per_object.add(names[0][0])
    if per_object != set(partitions):
        raise Failed(f"lanes with no object: {sorted(set(partitions) - per_object)}")
    stats = assert_lake(prefix, pushed, len(partitions))
    assert_manifests(prefix)
    results.append(
        f"6 layout      per-partition on {len(partitions)} numbered lanes: {len(keys)} objects, "
        f"one lane each, offset range in every key, {stats['records']} records, 0 gaps"
    )


def scenario_start_latest(results):
    """(7) `start=latest` after a backlog."""
    queue = "s3sink-latest"
    prefix = "s7"
    partitions = [f"lane-{i}" for i in range(4)]
    configure(queue, retentionEnabled=False)
    backlog = push_records(queue, partitions, 30, "old")

    # T_0 is the first safeTime the sink sees, and safeTime trails now() by the
    # broker's guard: let the backlog fall provably below it before starting.
    time.sleep(12)
    sink = Sink("latest", prefix, queue, QUEEN_S3_START="latest").start()
    sink.wait_metric("queen_s3_discovery_partitions", 1, f'queue="{queue}"', timeout=120)
    time.sleep(2)
    fresh = push_records(queue, partitions, 15, "new")
    sink.wait_metric("queen_s3_records_written_total", fresh, f'queue="{queue}"', timeout=240)
    time.sleep(5)
    written = sink.metric("queen_s3_records_written_total", f'queue="{queue}"')
    sink.stop()

    con, src = read_lake(prefix)
    tags = con.execute(f"SELECT DISTINCT payload->>'tag' FROM {src}").fetchall()
    tags = sorted(t[0] for t in tags)
    if tags != ["new"]:
        raise Failed(f"start=latest shipped {tags}, expected only the records after T_0")
    stats = lake_stats(prefix)
    if stats["records"] != fresh:
        raise Failed(f"start=latest shipped {stats['records']} records, expected {fresh}")
    results.append(
        f"7 start       backlog of {backlog} records ignored, {fresh} records after T_0 "
        f"shipped ({int(written)} written, {stats['records']} in the lake, tags={tags})"
    )


SCENARIOS = [
    ("scale", scenario_scale),
    ("crash", scenario_crash),
    ("retention", scenario_retention),
    ("alignment", scenario_alignment),
    ("layout", scenario_per_partition),
    ("start", scenario_start_latest),
]


def main():
    say(f"s3sink: broker={QUEEN} gateway={GW_ENDPOINT} bucket={BUCKET} data={DATA}")
    gw = start_gateway()
    say("s3sink: versitygw up (posix backend)")
    results = []
    failures = []
    started = time.time()
    for name, fn in SCENARIOS:
        t0 = time.time()
        try:
            fn(results)
            say(f"  PASS {name} ({time.time() - t0:.0f}s)")
        except Exception as e:  # noqa: BLE001 — the suite reports, it does not raise
            failures.append((name, str(e)))
            say(f"  FAIL {name} ({time.time() - t0:.0f}s): {e}")
        finally:
            for leaked in list(LIVE):
                say(f"  (killing {leaked.name}, left running by {name})")
                leaked.kill()
    gw.send_signal(signal.SIGTERM)

    say("")
    say("=========================== s3sink scenarios ===========================")
    for line in results:
        say(line)
    for name, why in failures:
        say(f"! {name}: {why}")
    say("========================================================================")
    say(f"RESULT: {len(results)} passed, {len(failures)} failed in {time.time() - started:.0f}s")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Failed as e:
        say(f"s3sink: fatal: {e}")
        sys.exit(2)
