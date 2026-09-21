"""The real body of the crash scenarios (PLAN_RAFT.md §13.5), phase 1.

`crashdrv.py` owns the catalogue, the plan and the dry run; this module is what
`--run` calls once a scenario is `implemented`. It:

  1. starts a raft1 broker (`QUEEN_STORAGE=raft`, NO Postgres, and — for Phase
     A3a — `QUEEN_RAFT_QLOG=1` so the per-queue qlog is a WAL: fsynced at each
     store commit, read by pop) with one crash point armed through
     `QUEEN_TEST_FAULTS` (rsm/faults.rs);
  2. drives a workload over the HTTP wire that RECORDS what it sent and what it
     was answered into a run log (test/raft/checker's JSONL, so the shared Go
     checker judges this harness too — the §13.6 lesson: console output cannot
     be re-judged). The workload is NOT push-only: from the first round it pops
     and acks a `warm` prefix (a COMPLETION crosses the pipeline) and pops a
     `held` prefix it deliberately never acks (a bare CLAIM/LEASE crosses the
     pipeline), all BEFORE the armed crash, so claim, lease, cursor and
     completion effects — not only `Append`s — flow through the armed pipeline.
     A crash that lands on the later points (the `durable.*` points, and a
     later hit of the apply/commit points) is then a crash with ack/claim
     effects in flight, which the push-only shape never exercised. It keeps
     pushing FRESH, never-popped `orders` messages every round so the post-crash
     drain always has a non-empty judged set;
  3. waits for the armed point to abort the process (a point that never fires
     is a FAILED run, never a pass — the code path was not taken);
  4. restarts the same node with no faults armed, waits for health;
  5. retries every unanswered write with its ORIGINAL transactionId, then drains
     every queue;
  6. checks: every answered write delivered (Go `delivery-at-least-once`), the
     bytes delivered are the bytes pushed (Go `payload-hash`), each transaction
     id maps to exactly ONE offset and is delivered exactly once (Python), the
     node is a single healthy leader with a monotone applied index, and no error
     line except the fault's own. It also judges the CLAIM/ACK recovery path,
     which a push-only or drain-immediately shape cannot (the facade lease is
     fixed at 60 s, far longer than a drain): when a claim or a completion
     crossed the crash the harness WAITS the lease out and re-drains `warm` and
     `held`, then asserts (a) every `held` message claimed-not-acked before the
     crash redelivers exactly once after the lease — the positive control that
     proves the window is past the lease and that a lost claim would be caught —
     and (b) no `warm` message acked-completed before the crash EVER comes back
     (the ack-path exactly-once property, now falsifiable because the control
     shows the lease has expired).

Stdlib only (urllib, subprocess): the VM of §13.6 has no `requests` and the
harness never touches the data path, so its speed does not matter.

Binary: the broker MUST be a raft-aware build (it understands
`QUEEN_STORAGE=raft`); a pre-raft binary boots the Postgres class and dies at
schema apply. `find_broker` cannot tell the two apart by looking at the file,
so the driver makes the resolved binary PROVE it booted raft mode
(`assert_raft_aware`) before any cell runs, and aborts the whole run — echoing
the broker's stderr tail — when it does not. Debug or release is a free choice:
crash injection is a CORRECTNESS test, not a measurement, so a debug binary is
faithful (§0.3 reserves release/VM numbers for measurements). The cell records
which binary it used.

What this does NOT test: durability under dropped/unsynced writes. `faults::hit`
dies by SIGKILL, which leaves the OS page cache intact, so a frame written but
not yet fsynced is fully present at restart and replays. This harness therefore
falsifies recovery BOOKKEEPING (offsets, cursors, completion, dedup across the
kill), not unsynced-byte loss (I11's dropped-unflushed-writes clause). That
needs a fault-injecting block device (dm-flakey) on the Linux VM and is WP-1.11.
"""

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER_DIR = REPO_ROOT / "test" / "raft" / "checker"

# The 11 phase-1 points a push/pop/ack workload actually reaches. The other two
# §13.5 phase-1 points, `gc.before_unlink` / `gc.after_unlink`, need a file to
# become collectable — which in phase 1 means retention or a delete, and neither
# has an HTTP route yet (§10.3, retention is WP-2.7). They are wired in
# rsm/faults.rs and proven to fire by the Rust apply-crash fault test; the
# gc-compaction scenario (WP-2.7) drives them over the wire. See RESULTS.md.
PUSHACK_REACHABLE = {
    "batcher.drained",
    "planner.planned",
    "propose.sent",
    "log.appended",
    "log.flushed",
    "commit.before_apply",
    "apply.mid_entry",
    "apply.segment_written",
    "apply.store_committed",
    "durable.files_synced",
    "durable.store_committed",
    # ALICE_PGLESS_NEWARCH.md §5 (Phase A3a): the qlog WAL durability cells. They
    # fire in the store commit's qlog block (a push crosses it every round with
    # QUEEN_RAFT_QLOG on — see QLOG_KNOB below), so the push/pop/ack workload
    # reaches them exactly as it reaches apply.store_committed.
    "qlog.record_written",
    "qlog.record_fsynced",
}

# ALICE_PGLESS_NEWARCH.md §5 (Phase A3a): the crash matrix runs with the per-queue
# qlog ON, so (a) the two `qlog.*` cells have a qlog to crash, and (b) every
# EXISTING §13.5 cell is proven to still recover with the knob on (the raft log is
# still the WAL in A3a; the qlog is ADDED durability, nothing is removed). With the
# knob on, pop reads the payload FROM the qlog (facade `read_owned`), so the cells
# also exercise "the acked records are readable from the qlog" across the crash.
# Overridable from the harness environment for a knob-off control run.
QLOG_KNOB = os.environ.get("QUEEN_RAFT_QLOG", "1")

# The default consumer group a plain queue pop uses (handlers/data.rs).
QUEUE_MODE_GROUP = "__QUEUE_MODE__"
# Three queues, on purpose (see `_drive_until_crash`):
#   * `orders` is the JUDGED queue — fresh messages are pushed every round and
#     NEVER popped before the crash, so the post-crash drain owns them with no
#     pop-D6.
#   * `warm` is popped AND acked-completed every round, so a completion (the ack
#     half of I4) crosses the armed pipeline before the crash. `acked` are the
#     ones the broker confirmed completed: a completion must NOT come back.
#   * `held` is popped and DELIBERATELY never acked, so a bare claim/lease (no
#     completion) crosses the pipeline. `claimed_held` are the ones a pop
#     returned with a definite answer and no ack was ever attempted: their
#     completion CANNOT have landed, so after the lease expires each one MUST
#     redeliver exactly once. That makes `held` a POSITIVE CONTROL for the
#     `acked-not-redelivered` check (see `_check`): the lease the phase-1 facade
#     grants is fixed at 60 s, longer than a drain, so a completed message a
#     recovery bug wrongly resurrected as leased/claimed would be invisible to
#     an immediate drain and the negative check could never fail. The harness
#     therefore waits the claim's lease out and re-drains; the `held` control
#     reappearing after that wait is what PROVES the observation window is past
#     the lease, so a completed message that did NOT reappear genuinely stayed
#     completed rather than merely staying hidden.
DEFAULT_QUEUE = "orders"
WARM_QUEUE = "warm"
HELD_QUEUE = "held"

# The pop lease the phase-1 facade grants, in SECONDS. It is HARDCODED at
# `server/src/rsm/facade/real.rs` (`lease_seconds: 60` on the pop command, and
# the default queue config's `lease_time: 60`); phase 1 has no per-request or
# per-queue override (the facade's `depth`/`configure`/`has_pending` are
# `Unsupported`, so there is also no state-read endpoint to ask "is this txn
# completed?"). The harness mirrors the value here so it can wait a pre-crash
# claim's lease out and OBSERVE the recovery outcome. KEEP IN SYNC with real.rs:
# if the facade lease changes, change this. The wait is incurred only by the
# cells that actually put a claim or a completion across the crash.
FACADE_LEASE_SECS = 60
# Slack added over the lease before re-draining, so a claim taken at wall-clock
# T is DEMONSTRABLY expired when we look again (client and broker share this
# machine's wall clock; the planner stamps lease expiry from it, D5).
LEASE_EXPIRY_MARGIN_SECS = 5


class NoAnswer(Exception):
    """A request got no usable answer: connection reset/refused, timeout, or a
    5xx. This is the D6 case — the write's outcome is UNKNOWN — and it is what
    a `push_unanswered` records."""


class BrokerNotRaftAware(RuntimeError):
    """The resolved binary did not boot raft mode: `/health` never reported
    `engine=raft` with a ready RSM (a pre-raft binary boots the Postgres class
    and dies at schema apply). This aborts the WHOLE run, not one cell — every
    cell would fail identically, and the misleading per-cell "exited before
    health" is exactly what the WP-1.8 reproducibility refutation was about.
    Carries the broker's stderr tail so the reason is on screen, not in a file
    nobody opened."""


def find_broker() -> Path | None:
    """The broker binary, resolved as: an explicit `QUEEN_BROKER`, else the
    FRESHEST of the release/debug builds by mtime.

    It MUST be a raft-aware build. `find_broker` cannot tell a raft-aware binary
    from a stale pre-raft one by looking at the file, so it does NOT blindly
    prefer release over debug (the old behaviour: a stale release binary left
    over from a measurement made `--run-matrix` fail every cell at boot). It
    prefers the freshest build, and the caller makes the binary prove it booted
    raft mode (`assert_raft_aware`). Pin one explicitly with `QUEEN_BROKER`.

    Debug is correct for crash injection (§0.3 reserves release/VM numbers for
    measurements); the cell records which binary ran."""
    env = os.environ.get("QUEEN_BROKER")
    if env:
        p = Path(env)
        return p if p.exists() else None
    existing = [REPO_ROOT / rel for rel in
                ("server/target/release/queen", "server/target/debug/queen")
                if (REPO_ROOT / rel).exists()]
    if not existing:
        return None
    return max(existing, key=lambda p: p.stat().st_mtime)


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@dataclass
class RunLog:
    """The append-only JSONL the checkers read (test/raft/checker/log.go)."""

    path: Path
    writer: str = "crash"
    _seq: int = 0
    _fh: object = None

    def open(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "w", buffering=1)  # line-buffered

    def close(self):
        if self._fh:
            self._fh.close()
            self._fh = None

    def emit(self, kind: str, **fields):
        self._seq += 1
        ev = {"seq": self._seq, "ts": _rfc3339(), "kind": kind, "writer": self.writer}
        for k, v in fields.items():
            if v is not None:
                ev[k] = v
        self._fh.write(json.dumps(ev) + "\n")


def _rfc3339() -> str:
    t = time.time()
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t))
    return f"{base}.{int((t % 1) * 1e9):09d}Z"


def payload_hash(obj) -> str:
    """A canonical hash of a JSON value, stable across re-serialization (key
    order, whitespace): what the pushed frame and the delivered `data` are
    compared by."""
    canon = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    return sha256(canon).hexdigest()


class Broker:
    """One raft1 broker process the harness owns. Killed only by the PID we
    started (§0.3: never by port)."""

    def __init__(self, binary: Path, data_dir: Path, port: int, log_dir: Path):
        self.binary = binary
        self.data_dir = data_dir
        self.port = port
        self.log_dir = log_dir
        self.proc: subprocess.Popen | None = None
        self.base = f"http://127.0.0.1:{port}"
        self._stderr_paths: list[Path] = []

    def start(self, fault: str | None, phase_tag: str):
        # Never leave a previous process of this Broker running: the pre-crash
        # one has usually aborted itself, but a defensive kill keeps a cell from
        # ever overlapping two brokers on one data dir (which would corrupt the
        # LOCK and slow the whole matrix).
        if self.proc is not None and self.proc.poll() is None:
            self.kill()
        env = dict(os.environ)
        env["QUEEN_STORAGE"] = "raft"
        env["QUEEN_RAFT_DIR"] = str(self.data_dir)
        env["PORT"] = str(self.port)
        env["QUEEN_BIND_ADDR"] = "127.0.0.1"
        env["JWT_ENABLED"] = "false"  # auth off (the default); explicit here
        # A fast durable cadence so both nth=1 and a later hit of the durable
        # points are reached in under a second, and so a kill lands at every
        # periodic boundary (§13.6). Node-local timing only (Appendix H).
        env["QUEEN_RAFT_DURABLE_EVERY_MS"] = "150"
        # ALICE_PGLESS_NEWARCH.md §5 (Phase A3a): run with the per-queue qlog on,
        # so the qlog.* cells have a qlog to crash and every existing cell is
        # proven to still recover with the knob on (default "1"; see QLOG_KNOB).
        env["QUEEN_RAFT_QLOG"] = QLOG_KNOB
        # Keep the log buffers out of /var/lib (a benign WARN otherwise, but it
        # keeps the error scan's input clean).
        env["FILE_BUFFER_DIR"] = str(self.log_dir / "buffers")
        if fault:
            env["QUEEN_TEST_FAULTS"] = fault
        else:
            env.pop("QUEEN_TEST_FAULTS", None)
        stderr_path = self.log_dir / f"{phase_tag}.stderr"
        self._stderr_paths.append(stderr_path)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        fh = open(stderr_path, "w")
        self.proc = subprocess.Popen(
            [str(self.binary)],
            env=env,
            stdout=fh,
            stderr=subprocess.STDOUT,
            cwd=str(REPO_ROOT),
        )

    def wait_healthy(self, timeout=20.0) -> dict:
        """Block until /health answers with a ready raft RSM, or raise. Returns
        the raft block.

        On an early exit or a timeout the message carries the broker's stderr
        tail, so the reason (e.g. a pre-raft binary's `FATAL: schema apply
        failed`) is on screen instead of buried in a per-cell log file — the
        WP-1.8 refutation's "surfaces only 'exited before health', never the
        FATAL line"."""
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"broker exited before health (rc={self.proc.returncode}); "
                    f"stderr tail:\n{self.stderr_tail()}")
            try:
                _, body = self._http("GET", "/health", timeout=1.0)
                raft = body.get("raft", {})
                if raft.get("storageReady"):
                    return raft
                last = body
            except (NoAnswer, Exception):  # noqa: BLE001 - health-poll retries
                pass
            time.sleep(0.1)
        raise RuntimeError(
            f"broker never became healthy in {timeout:.0f}s (last={last}); "
            f"stderr tail:\n{self.stderr_tail()}")

    def stderr_tail(self, n: int = 25) -> str:
        """The last `n` lines of the most recent stderr capture, indented for a
        multi-line error message. Empty/absent files are said so, never a
        traceback of their own."""
        if not self._stderr_paths:
            return "    | (no stderr captured yet)"
        p = self._stderr_paths[-1]
        if not p.exists():
            return f"    | (no stderr file at {p})"
        lines = p.read_text(errors="replace").splitlines()
        tail = lines[-n:] if len(lines) > n else lines
        return "\n".join(f"    | {ln}" for ln in tail) or "    | (stderr empty)"

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def wait_exit(self, timeout=15.0):
        """Wait for the faulted process to die. Returns the exit code (a negative
        value is the signal, e.g. -9 for SIGKILL)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.proc.poll()
            if rc is not None:
                return rc
            time.sleep(0.05)
        return None

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    # -- HTTP ---------------------------------------------------------------

    def _http(self, method: str, path: str, body=None, timeout=5.0):
        url = self.base + path
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                status = resp.status
        except urllib.error.HTTPError as e:
            status = e.code
            raw = e.read()
            if status >= 500:
                raise NoAnswer(f"{method} {path}: {status}")
        except (urllib.error.URLError, http.client.HTTPException, ConnectionError,
                socket.timeout, OSError) as e:
            raise NoAnswer(f"{method} {path}: {e}")
        try:
            return status, json.loads(raw) if raw else {}
        except json.JSONDecodeError as e:
            raise NoAnswer(f"{method} {path}: bad json: {e}")

    def push(self, items: list[dict]):
        return self._http("POST", "/api/v1/push", {"items": items})

    def pop(self, queue: str, batch: int):
        return self._http("GET", f"/api/v1/pop/queue/{queue}?batchSize={batch}")

    def ack_batch(self, acks: list[dict]):
        return self._http("POST", "/api/v1/ack/batch", {"acknowledgments": acks})

    def health_raw(self, timeout=1.0) -> dict:
        _, body = self._http("GET", "/health", timeout=timeout)
        return body


def assert_raft_aware(broker_bin: str, run_dir) -> dict:
    """Boot the resolved binary ONCE, with NO fault, and make it prove it booted
    raft mode before any cell runs: `/health` must report `engine=="raft"` and a
    `raft` block with `storageReady=true`.

    This is the WP-1.8 reproducibility gate. A pre-raft binary ignores
    `QUEEN_STORAGE=raft`, boots the Postgres class and dies at schema apply, so
    without this probe EVERY cell fails identically at boot with a misleading
    "exited before health", hiding the real `FATAL` line. On failure this raises
    `BrokerNotRaftAware` with the broker's stderr tail, which the driver prints
    once and aborts the whole run. Returns the raft block on success.
    """
    probe_dir = Path(run_dir) / "_preflight"
    data_dir = probe_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    port = free_port()
    broker = Broker(Path(broker_bin), data_dir, port, probe_dir)
    try:
        broker.start(fault=None, phase_tag="preflight")
        try:
            raft = broker.wait_healthy(timeout=20.0)
        except RuntimeError as exc:
            raise BrokerNotRaftAware(
                f"{broker_bin} did not boot raft mode.\n{exc}\n"
                "  Build a raft-aware broker (`cargo build --bin queen`) or set "
                "QUEEN_BROKER to one.") from None
        # Defence in depth: a binary that serves /health but is not the raft
        # class (no `engine:raft`) would slip past the storageReady poll only if
        # it also faked a raft block; assert the top-level engine too.
        try:
            engine = broker.health_raw().get("engine")
        except NoAnswer:
            engine = None
        if engine != "raft":
            raise BrokerNotRaftAware(
                f"{broker_bin} answered /health with engine={engine!r}, not "
                f"'raft'.\n{broker.stderr_tail()}\n"
                "  This is not a raft-aware broker; set QUEEN_BROKER to one.")
        return raft
    finally:
        broker.kill()
        import shutil
        shutil.rmtree(probe_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# The push-ack scenario
# ---------------------------------------------------------------------------


@dataclass
class PushRecord:
    txn: str
    payload: dict
    phash: str
    queue: str = DEFAULT_QUEUE
    answered: bool = False
    status: str | None = None   # queued | duplicate | error
    offset: int | None = None


@dataclass
class Result:
    cell: str
    scenario: str
    point: str
    nth: int
    topology: str
    fired: bool = False
    exit_code: int | None = None
    verdict: str = "PASS"
    reasons: list[str] = field(default_factory=list)
    checks: dict = field(default_factory=dict)
    stats: dict = field(default_factory=dict)

    def fail(self, why: str):
        self.verdict = "FAIL"
        self.reasons.append(why)


def run_push_ack(broker_path, point, nth, run_dir, topology, out=sys.stdout) -> Result:
    cell = f"{point.replace('.', '_')}__nth{nth}"
    res = Result(cell=cell, scenario="push-ack", point=point, nth=nth, topology=topology)
    if topology != "raft1":
        res.fail(f"the push-ack runner is phase 1 (raft1 only); got {topology}")
        return res
    if point not in PUSHACK_REACHABLE:
        res.verdict = "N/A"
        res.reasons.append(
            "not reachable by a phase-1 push/pop/ack workload "
            "(no retention or delete route yet; §10.3, WP-2.7)"
        )
        return res

    cell_dir = Path(run_dir) / cell
    data_dir = cell_dir / "data"
    for d in (cell_dir, data_dir):
        d.mkdir(parents=True, exist_ok=True)
    port = free_port()
    broker = Broker(Path(broker_path), data_dir, port, cell_dir)
    log = RunLog(cell_dir / "run.jsonl")
    log.open()

    pushes: dict[str, PushRecord] = {}
    popped: set[str] = set()         # warm txns a pre-crash pop returned
    acked: set[str] = set()          # warm txns the broker confirmed completed pre-crash
    claimed_held: set[str] = set()   # held txns claimed pre-crash and NEVER acked
    fault_spec = f"{point}:{nth}"

    try:
        # 1. start with the fault armed.
        broker.start(fault=fault_spec, phase_tag="pre")
        try:
            broker.wait_healthy()
        except RuntimeError as exc:
            # A per-cell boot failure is a clean FAIL with the stderr tail, not
            # an unhandled traceback (the matrix-wide raft-awareness gate runs
            # in assert_raft_aware; this is the belt for a one-off boot fault).
            res.fail(f"broker did not become healthy with the fault armed: {exc}")
            return res
        log.emit("node_event", event="fault", text=fault_spec)

        # 2. workload: push (orders, judged), pop+ack (warm) and pop-and-hold
        #    (held) serially, recording every send and answer, until the armed
        #    point aborts the process (or a time budget passes, which is a
        #    FAILED run — the point never fired). Returns the wall-clock time of
        #    the LAST pre-crash claim (a warm pop or a held pop), so step 6 can
        #    wait that claim's lease out before it judges the recovery outcome.
        last_claim_wall = _drive_until_crash(broker, log, pushes, popped, acked,
                                             claimed_held, out)

        # 3. the point must have fired. If the process is still alive, the code
        #    path was never taken: FAIL, don't pretend.
        rc = broker.wait_exit(timeout=15.0)
        if rc is None:
            res.fail("the armed point never fired within the time budget "
                     "(the code path was not taken)")
            broker.kill()
            return res
        res.exit_code = rc
        res.fired = True
        # SIGKILL (-9) is how faults::hit dies (kill -9 model); any other death
        # is suspicious (a panic, a clean exit) and worth a note.
        if rc != -signal.SIGKILL:
            res.reasons.append(f"note: exited with {rc} (expected -9/SIGKILL)")
        log.emit("node_event", event="kill9", text=f"killed at {fault_spec} rc={rc}")

        # 4. restart with no fault, wait for health.
        broker.start(fault=None, phase_tag="post")
        raft = broker.wait_healthy()
        log.emit("node_event", event="restart",
                 appliedIndex=int(raft.get("applied", 0)))
        res.stats["applied_after_restart"] = int(raft.get("applied", 0))
        res.stats["role_after_restart"] = raft.get("role")

        # 5. retry every unanswered push with its ORIGINAL transactionId.
        _retry_unanswered(broker, log, pushes)

        # 6. drain the JUDGED queue (orders): pop + ack to empty, one delivery
        #    per message, with the drain-complete note the Go at-least-once
        #    check scopes on. Then drain WARM and HELD too (no drain-complete
        #    note, so Go ignores them). This FIRST drain happens within seconds
        #    of restart, i.e. well inside the 60 s facade lease, so a message
        #    claimed before the crash is still leased and does NOT appear here —
        #    which is exactly why an immediate drain alone can never falsify the
        #    ack-path check (a completed message wrongly resurrected as leased
        #    would also be hidden). It is the baseline; the lease-out re-drain
        #    below is what actually judges recovery.
        delivered = _drain(broker, log, DEFAULT_QUEUE, drain_note=True)
        warm_delivered = _drain(broker, log, WARM_QUEUE, drain_note=False)
        held_delivered = _drain(broker, log, HELD_QUEUE, drain_note=False)
        res.stats["delivered"] = len(delivered)
        res.stats["pushed"] = len(pushes)
        res.stats["answered"] = sum(1 for p in pushes.values() if p.answered)
        res.stats["unanswered"] = sum(1 for p in pushes.values() if not p.answered)
        # Ack/claim coverage evidence: how many warm messages were acked-completed
        # (`popacked`) and how many held messages were claimed-not-acked
        # (`claimed`) BEFORE the crash. Either > 0 means the armed point fired
        # with claim/ack/cursor effects already flowing through the pipeline, and
        # there is a recovery outcome to judge.
        res.stats["popacked_before_crash"] = len(acked)
        res.stats["claimed_held_before_crash"] = len(claimed_held)

        # 6b. Judge the CLAIM/ACK recovery path. Only when a claim or completion
        #     actually crossed the crash: otherwise there is nothing a lease
        #     could hide and the wait would be pointless. Wait the last pre-crash
        #     claim's lease out (§7.4: leases expire on the planner's wall clock,
        #     which is this machine's), then re-drain WARM and HELD. After this
        #     wait a claimed-not-acked `held` message MUST have redeliver-ed (its
        #     completion never landed) and a `warm` completion MUST NOT have.
        warm_delivered_late: list = []
        held_delivered_late: list = []
        need_lease_wait = bool(acked) or bool(claimed_held)
        res.stats["lease_waited"] = need_lease_wait
        if need_lease_wait:
            _wait_lease_out(log, last_claim_wall)
            warm_delivered_late = _drain(broker, log, WARM_QUEUE, drain_note=False)
            held_delivered_late = _drain(broker, log, HELD_QUEUE, drain_note=False)

        # final health for the raft1 liveness check.
        final = broker.wait_healthy()
        res.stats["applied_final"] = int(final.get("applied", 0))
        log.emit("digest", node="node-0", appliedIndex=int(final.get("applied", 0)),
                 digest="raft1-single-node")

    finally:
        log.close()
        broker.kill()

    # 7. the checks.
    _check(res, broker, pushes, delivered,
           warm_delivered + warm_delivered_late,
           held_delivered, held_delivered_late,
           popped, acked, claimed_held, need_lease_wait,
           cell_dir, log.path)
    return res


def _wait_lease_out(log: RunLog, last_claim_wall: float):
    """Sleep until every pre-crash claim's 60 s lease is DEMONSTRABLY expired,
    so a re-drain observes the recovery outcome the lease would otherwise hide.

    The lease was stamped `pop_wall + 60 s` by the planner (D5, §7.4) at claim
    time; `last_claim_wall` is the client wall clock right after the last such
    pop (same machine, same clock). Waiting to `last_claim_wall + 60 s + margin`
    guarantees expiry regardless of how quickly the crash, restart and first
    drain went. The elapsed time since the claim counts against the wait, so
    the sleep is usually well under 60 s."""
    target = (last_claim_wall or time.time()) + FACADE_LEASE_SECS + LEASE_EXPIRY_MARGIN_SECS
    remaining = target - time.time()
    log.emit("node_event", event="lease_wait",
             text=f"waiting {max(0.0, remaining):.1f}s for pre-crash leases to expire")
    while True:
        remaining = target - time.time()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 1.0))


def _drive_until_crash(broker: Broker, log: RunLog, pushes: dict,
                       popped: set, acked: set, claimed_held: set, out) -> float:
    """Drive push + pre-crash pop+ack + pop-and-hold until the broker dies.
    Returns the wall-clock time of the LAST claim (a warm pop+ack or a held
    pop), or 0.0 if none happened — the caller waits that lease out.

    Order matters, and is chosen so the goals never fight:

      * The VERY FIRST operation is a JUDGED (orders) push, so even a `nth=1`
        crash on the very first entry leaves an answered orders message to retry
        and judge — the run never "judges nothing".
      * `warm` is seeded, then popped+acked every round, so a COMPLETION (the
        ack half of I4) crosses the armed pipeline BEFORE the crash.
      * `held` is seeded, then popped-and-NEVER-acked every round, so a bare
        CLAIM/LEASE crosses the pipeline. Because no ack is ever attempted, a
        held message's completion cannot have landed, so it MUST redeliver after
        the lease — a deterministic, falsifiable recovery outcome (see `_check`).
      * `orders` gets fresh, never-popped messages every round, so the
        post-crash drain always owns a non-empty, pop-D6-free judged set.

    A crash on a later point (the `durable.*` points, and a later hit of the
    apply/commit points) then fires with completion AND claim effects in flight.
    The first ENTRIES are unavoidably pushes (a claim needs a prior push), so
    the early points still crash on an `Append` with nothing claimed; their
    ack-path atomicity is proven at the Rust level (RESULTS.md discloses this)."""
    budget = time.time() + 15.0
    last_claim = 0.0
    # First: a judged push, so a crash on entry 1 still leaves something to judge.
    if not _push_batch(broker, log, pushes, DEFAULT_QUEUE, "o", 0, 2):
        return last_claim
    # Seed the warm and held backlogs so round 1 already has something to claim.
    if not _push_batch(broker, log, pushes, WARM_QUEUE, "wseed", 0, 8):
        return last_claim
    if not _push_batch(broker, log, pushes, HELD_QUEUE, "hseed", 0, 8):
        return last_claim
    rnd = 0
    while time.time() < budget:
        if not broker.is_alive():
            return last_claim
        rnd += 1
        # fresh JUDGED pushes (orders); never popped here.
        if not _push_batch(broker, log, pushes, DEFAULT_QUEUE, "o", rnd, 2):
            return last_claim
        # claim + ack a warm prefix so a completion is in flight.
        before = len(acked)
        if not _popack_prefix(broker, log, popped, acked, 2):
            return last_claim
        if len(acked) > before:
            last_claim = time.time()
        # claim-and-HOLD a held prefix so a bare lease is in flight (never acked).
        before = len(claimed_held)
        if not _pop_hold(broker, log, claimed_held, 2):
            return last_claim
        if len(claimed_held) > before:
            last_claim = time.time()
        # replenish warm and held so the next round always has something to claim.
        if not _push_batch(broker, log, pushes, WARM_QUEUE, "w", rnd, 2):
            return last_claim
        if not _push_batch(broker, log, pushes, HELD_QUEUE, "h", rnd, 2):
            return last_claim
        time.sleep(0.01)
    # Fell through the budget without a crash: leave the process alive; the
    # caller's wait_exit reports the point never fired.
    return last_claim


def _push_batch(broker: Broker, log: RunLog, pushes: dict, queue: str,
                tag: str, rnd: int, count: int) -> bool:
    """Push `count` unique messages to `queue`, recording each send and answer.
    Returns False (the crash) when the push gets no answer — the D6 case, logged
    as `push_unanswered`."""
    batch, recs = [], []
    for k in range(count):
        txn = f"{tag}{rnd}-{k}"
        payload = {"tx": txn, "n": rnd * 100 + k}
        rec = PushRecord(txn=txn, payload=payload, phash=payload_hash(payload),
                         queue=queue)
        pushes[txn] = rec
        recs.append(rec)
        batch.append({"queue": queue, "payload": payload, "transactionId": txn})
    try:
        _, items = broker.push(batch)
    except NoAnswer:
        for rec in recs:
            log.emit("push_unanswered", queue=queue, txnId=rec.txn,
                     payloadHash=rec.phash)
        return False
    for rec, item in zip(recs, _by_index(items, recs)):
        rec.answered = True
        rec.status = item.get("status")
        rec.offset = item.get("offset")
        _log_push_answer(log, rec)
    return True


def _log_push_answer(log: RunLog, rec: PushRecord):
    if rec.status == "queued":
        log.emit("push_ok", queue=rec.queue, txnId=rec.txn,
                 payloadHash=rec.phash, offset=_off(rec.offset))
    elif rec.status == "duplicate":
        log.emit("push_duplicate", queue=rec.queue, txnId=rec.txn,
                 payloadHash=rec.phash, offset=_off(rec.offset))
    else:
        log.emit("push_rejected", queue=rec.queue, txnId=rec.txn,
                 payloadHash=rec.phash, status=rec.status)


def _popack_prefix(broker: Broker, log: RunLog, popped: set, acked: set,
                   count: int) -> bool:
    """Pop up to `count` WARM messages and ack them completed, so a claim and a
    completion cross the armed pipeline. Records the ids a pop returned
    (`popped`) and the ids the broker confirmed completed (`acked`) for the
    ack-path checks. Returns False when a pop or ack gets no answer (the crash).

    A pop that returns nothing is not a crash (the warm backlog is momentarily
    empty); it returns True."""
    try:
        _, body = broker.pop(WARM_QUEUE, count)
    except NoAnswer:
        return False
    msgs = body.get("messages") or []
    if not msgs:
        return True
    lease = body.get("leaseId", "")
    pid = body.get("partitionId", "")
    acks = []
    for m in msgs:
        txn = m.get("transactionId")
        dh = payload_hash(m.get("data"))
        popped.add(txn)
        log.emit("delivery", queue=WARM_QUEUE, group=QUEUE_MODE_GROUP, txnId=txn,
                 payloadHash=dh, offset=_off(m.get("offset")), leaseId=lease,
                 partitionId=pid)
        acks.append({"transactionId": txn, "partitionId": pid,
                     "leaseId": lease, "status": "completed"})
    try:
        _, ackres = broker.ack_batch(acks)
    except NoAnswer:
        # The ack was in flight when the process aborted: the messages are
        # popped (claimed) but NOT confirmed completed — the D6 case for an ack.
        return False
    for a in ackres:
        txn = a.get("transactionId")
        if a.get("success"):
            acked.add(txn)
            log.emit("ack_ok", queue=WARM_QUEUE, group=QUEUE_MODE_GROUP,
                     txnId=txn, status="completed")
        else:
            log.emit("ack_fail", queue=WARM_QUEUE, group=QUEUE_MODE_GROUP,
                     txnId=txn)
    return True


def _pop_hold(broker: Broker, log: RunLog, claimed_held: set, count: int) -> bool:
    """Pop up to `count` HELD messages and DELIBERATELY do not ack them, so a
    bare claim/lease (no completion) crosses the armed pipeline. Records the ids
    a pop returned AND ANSWERED into `claimed_held`: their completion cannot have
    landed (no ack was ever sent), so after the lease expires each MUST redeliver
    exactly once — the positive control `_check` uses to prove the observation
    window is past the lease. Returns False when the pop gets no answer (the
    crash); a pop that returns nothing (backlog momentarily empty) returns True.

    Only an ANSWERED pop adds to `claimed_held`: a pop whose answer was lost is
    D6 (the claim may or may not have committed) and cannot be a definite
    control."""
    try:
        _, body = broker.pop(HELD_QUEUE, count)
    except NoAnswer:
        return False
    msgs = body.get("messages") or []
    if not msgs:
        return True
    lease = body.get("leaseId", "")
    pid = body.get("partitionId", "")
    for m in msgs:
        txn = m.get("transactionId")
        dh = payload_hash(m.get("data"))
        claimed_held.add(txn)
        # A delivery event (with the payload hash) so the Go payload-hash check
        # judges the claim's bytes; NO ack event — the lease is left to expire.
        log.emit("delivery", queue=HELD_QUEUE, group=QUEUE_MODE_GROUP, txnId=txn,
                 payloadHash=dh, offset=_off(m.get("offset")), leaseId=lease,
                 partitionId=pid)
    return True


def _retry_unanswered(broker: Broker, log: RunLog, pushes: dict):
    """Retry each unanswered push with its ORIGINAL transactionId (D6), on its
    ORIGINAL queue. The retry must return a definite answer — duplicate if the
    original committed, queued if it did not — and must not create a second
    message."""
    for rec in pushes.values():
        if rec.answered:
            continue
        item = None
        for _ in range(20):
            try:
                _, items = broker.push([{
                    "queue": rec.queue, "payload": rec.payload,
                    "transactionId": rec.txn,
                }])
                item = items[0]
                break
            except NoAnswer:
                time.sleep(0.1)
        if item is None:
            log.emit("push_unanswered", queue=rec.queue, txnId=rec.txn,
                     payloadHash=rec.phash, text="retry got no answer")
            rec.status = "retry_no_answer"
            continue
        rec.answered = True
        rec.status = item.get("status")
        rec.offset = item.get("offset")
        _log_push_answer(log, rec)


def _drain(broker: Broker, log: RunLog, queue: str, drain_note: bool) -> list[dict]:
    """Pop and ack `queue` until it is empty, logging one delivery per message
    and one ack per batch. Returns the delivered rows.

    `drain_note` writes the `drain-complete` note the Go at-least-once check
    scopes on. The JUDGED queue (orders) writes it; the WARM queue does NOT —
    warm messages are not required to be delivered after recovery (a lost claim
    leaves them leased for the 60 s facade lease), so scoping at-least-once over
    them would be wrong. The warm drain is only read for the ack-path
    exactly-once check (a completed message must not come back)."""
    delivered = []
    empty_polls = 0
    for _ in range(200):
        try:
            _, body = broker.pop(queue, 64)
        except NoAnswer:
            time.sleep(0.1)
            continue
        msgs = body.get("messages") or []
        if not msgs:
            empty_polls += 1
            if empty_polls >= 2:
                break
            continue
        empty_polls = 0
        lease = body.get("leaseId", "")
        pid = body.get("partitionId", "")
        acks = []
        for m in msgs:
            txn = m.get("transactionId")
            off = m.get("offset")
            dh = payload_hash(m.get("data"))
            row = {"txn": txn, "offset": off, "hash": dh}
            delivered.append(row)
            log.emit("delivery", queue=queue, group=QUEUE_MODE_GROUP,
                     txnId=txn, payloadHash=dh, offset=_off(off), leaseId=lease,
                     partitionId=pid)
            acks.append({"transactionId": txn, "partitionId": pid,
                         "leaseId": lease, "status": "completed"})
        try:
            _, ackres = broker.ack_batch(acks)
            for a in ackres:
                log.emit("ack_ok", queue=queue, group=QUEUE_MODE_GROUP,
                         txnId=a.get("transactionId"), status="completed")
        except NoAnswer:
            for a in acks:
                log.emit("ack_fail", queue=queue, group=QUEUE_MODE_GROUP,
                         txnId=a.get("transactionId"))
    if drain_note:
        # The drain-complete note the at-least-once check keys on.
        log.emit("note", text="drain-complete", group=QUEUE_MODE_GROUP, queue=queue)
    return delivered


def _check(res: Result, broker: Broker, pushes: dict, delivered: list,
           warm_delivered: list, held_immediate: list, held_late: list,
           popped: set, acked: set, claimed_held: set, lease_waited: bool,
           cell_dir, log_path):
    # `warm_delivered` is the immediate PLUS the post-lease warm drain (the
    # caller concatenates them): a completion wrongly resurrected as leased is
    # invisible to the immediate drain but appears once the lease is out.
    # -- Go checker: at-least-once + payload-hash --------------------------
    # NOT `-strict`: `delivery-at-least-once` SKIPs (correctly) when the crash
    # preceded any acknowledged push — there is nothing acknowledged to judge,
    # and the plan's `-strict` would call that a failure. That case is judged
    # instead by the Python `answered-then-delivered` check below, which judges
    # the retried duplicates (a `duplicate` answer means the message committed).
    # A real Go FAIL (a violation) still fails the cell.
    per_check, go_note = _run_go_checker(log_path, cell_dir)
    for name, state in per_check.items():
        res.checks[f"go:{name}"] = state
        if state == "FAIL":
            res.fail(f"go checker: {name} FAIL — see {log_path}")
    if go_note:
        res.reasons.append(f"note: go checker: {go_note}")

    all_delivered = delivered + warm_delivered + held_immediate + held_late

    # -- exactly-once (Python) --------------------------------------------
    # Each transaction id maps to exactly ONE offset, across every push answer
    # (pre-crash, retry) and every delivery (orders drain AND warm drain). Two
    # offsets for one id = a message created twice (dedup broke across the
    # crash).
    offsets: dict[str, set] = {}
    for rec in pushes.values():
        if rec.offset is not None and rec.status in ("queued", "duplicate"):
            offsets.setdefault(rec.txn, set()).add(rec.offset)
    for d in all_delivered:
        if d["offset"] is not None:
            offsets.setdefault(d["txn"], set()).add(d["offset"])
    multi = {t: sorted(s) for t, s in offsets.items() if len(s) > 1}
    if multi:
        res.fail(f"a transaction id maps to more than one offset (dedup broke): {multi}")
    res.checks["exactly-one-offset-per-txn"] = "PASS" if not multi else f"FAIL {multi}"

    # Each delivered id delivered exactly once in the judged (orders) drain.
    seen = {}
    for d in delivered:
        seen[d["txn"]] = seen.get(d["txn"], 0) + 1
    dup_deliv = {t: c for t, c in seen.items() if c > 1}
    if dup_deliv:
        res.fail(f"a message was delivered more than once in one drain: {dup_deliv}")
    res.checks["delivered-exactly-once"] = "PASS" if not dup_deliv else f"FAIL {dup_deliv}"

    # Every ANSWERED, JUDGED (orders) push that was NOT popped before the crash
    # is delivered after recovery — the exactly-once counterpart of at-least-once,
    # checked here too so a run whose Go check SKIPs still asserts it. Warm
    # pushes are excluded: a warm message popped before the crash is either
    # completed (gone) or left leased for the 60 s facade lease (invisible in the
    # drain window), so requiring its redelivery would be wrong. orders is never
    # popped pre-crash, so `not in popped` is belt-and-suspenders.
    answered_ids = {rec.txn for rec in pushes.values()
                    if rec.queue == DEFAULT_QUEUE
                    and rec.status in ("queued", "duplicate")
                    and rec.txn not in popped}
    delivered_ids = {d["txn"] for d in delivered}
    missing = sorted(answered_ids - delivered_ids)
    if missing:
        res.fail(f"an answered/committed push was never delivered after recovery: {missing}")
    res.checks["answered-then-delivered"] = (
        f"PASS ({len(answered_ids)} judged)" if not missing else f"FAIL {missing}")

    # THE CLAIM/LEASE RECOVERY CONTROL (WP-1.8): a `held` message claimed but
    # DELIBERATELY never acked before the crash has no completion, so after its
    # lease expires it MUST redeliver — exactly once, and never BEFORE the lease
    # is out. This is the positive control that makes the ack-path check below
    # able to fail at all: a resurrected completion is only visible in a re-drain
    # taken AFTER the lease, and the control reappearing is what proves the
    # re-drain WAS taken after the lease (a claim held here appearing in the
    # post-lease drain means claimed messages that stay hidden really are hidden
    # by nothing but a live lease). A lost claim (the message never comes back)
    # fails here; a claim redelivered before its lease expired (recovery dropped
    # the lease) fails here too. N/A when no claim crossed the crash.
    held_immediate_ids = {d["txn"] for d in held_immediate}
    held_counts: dict[str, int] = {}
    for d in held_immediate + held_late:
        held_counts[d["txn"]] = held_counts.get(d["txn"], 0) + 1
    early = sorted(t for t in claimed_held if t in held_immediate_ids)
    never = sorted(t for t in claimed_held if held_counts.get(t, 0) == 0)
    twice = sorted(t for t in claimed_held if held_counts.get(t, 0) > 1)
    if early:
        res.fail(f"a claim held (unacked) across the crash redelivered BEFORE its "
                 f"lease expired — recovery dropped the lease: {early}")
    if never:
        res.fail(f"a claim held (unacked) across the crash never redelivered after "
                 f"the lease expired — lost claim/message: {never}")
    if twice:
        res.fail(f"a held claim redelivered more than once after the lease: {twice}")
    control_ok = not (early or never or twice)
    if not claimed_held:
        res.checks["claim-redelivered-after-lease"] = "N/A (no claim crossed the crash)"
    else:
        res.checks["claim-redelivered-after-lease"] = (
            f"PASS ({len(claimed_held)} claims redelivered exactly once after the lease)"
            if control_ok else "FAIL")

    # THE ACK-PATH EXACTLY-ONCE PROPERTY (the WP-1.8 coverage-gap fix): a warm
    # message the broker confirmed COMPLETED before the crash must NOT come back,
    # EVER — not in the immediate drain, not in the post-lease drain (`warm_
    # delivered` is both). The post-lease drain is the load-bearing one: a
    # completion wrongly resurrected as leased/claimed is invisible until the
    # lease is out, so an immediate-only observation could NEVER falsify this —
    # the WP-1.8 refutation. The verdict states which observation window backed
    # it: a live control (a held claim redelivered in the same post-lease drain),
    # a timed lease-out wait, or none (the early points, N/A — RESULTS.md
    # discloses this).
    warm_delivered_ids = {d["txn"] for d in warm_delivered}
    redelivered = sorted(acked & warm_delivered_ids)
    if redelivered:
        res.fail(f"a message acked-completed before the crash came back after "
                 f"recovery — lost completion, exactly-once broken: {redelivered}")
    if not acked:
        res.checks["acked-not-redelivered"] = "N/A (no completion crossed the crash)"
    elif redelivered:
        res.checks["acked-not-redelivered"] = f"FAIL {redelivered}"
    elif claimed_held and control_ok:
        res.checks["acked-not-redelivered"] = (
            f"PASS ({len(acked)} completions absent after the lease expired; the "
            f"held control confirmed the window is past the lease)")
    elif lease_waited:
        res.checks["acked-not-redelivered"] = (
            f"PASS ({len(acked)} completions absent after a timed lease-out wait; "
            f"no live held control this cell)")
    else:
        res.checks["acked-not-redelivered"] = (
            f"WEAK ({len(acked)} acked; lease not confirmed expired)")
        res.reasons.append(
            "note: acked-not-redelivered had no lease-expiry observation this cell")

    # A run that judged nothing proves nothing (the -strict spirit): every cell
    # must have at least one answered id delivered and a non-empty judged drain.
    if not answered_ids or not delivered:
        res.fail(f"the run judged nothing (answered={len(answered_ids)}, "
                 f"delivered={len(delivered)}) — the point may have fired too early "
                 "to leave any committed message")

    # payload bytes: delivered hash equals the pushed hash (Python belt for the
    # Go check; catches a crossed message even if the Go check SKIPs). Both
    # drains.
    ph = {rec.txn: rec.phash for rec in pushes.values()}
    bad_hash = [d["txn"] for d in all_delivered
                if d["txn"] in ph and d["hash"] != ph[d["txn"]]]
    if bad_hash:
        res.fail(f"delivered payload hash != pushed hash (crossed/corrupt): {bad_hash}")
    res.checks["payload-hash-python"] = "PASS" if not bad_hash else f"FAIL {bad_hash}"

    # -- raft1 liveness ----------------------------------------------------
    live_ok = True
    if res.stats.get("role_after_restart") != "leader":
        res.fail(f"the single voter is not a leader after restart "
                 f"({res.stats.get('role_after_restart')})")
        live_ok = False
    if res.stats.get("applied_final", 0) < res.stats.get("applied_after_restart", 0):
        res.fail("the applied index went backwards after the drain")
        live_ok = False
    res.checks["raft1-leader-and-monotone-applied"] = "PASS" if live_ok else "FAIL"

    # -- no error line except the fault's own ------------------------------
    errs = _scan_errors(broker._stderr_paths)
    if errs:
        res.fail(f"error/panic lines other than the fault's own: {errs[:5]}"
                 + (f" (+{len(errs) - 5} more)" if len(errs) > 5 else ""))
    res.checks["no-unexpected-error-lines"] = "PASS" if not errs else f"FAIL ({len(errs)})"


def _scan_errors(stderr_paths) -> list[str]:
    """Every ERROR-level or panic line that is NOT the fault's own.

    An "error line" is one with the ERROR level token or a Rust panic; a WARN
    line that merely carries an `error=` field (the benign spool fallback) is
    not one. The fault's own line contains "fault: crash point".
    """
    bad = []
    for p in stderr_paths:
        if not p.exists():
            continue
        for line in p.read_text(errors="replace").splitlines():
            if "fault: crash point" in line:
                continue
            is_err = (" ERROR " in line) or ("panicked" in line) or line.startswith("thread '")
            if is_err:
                bad.append(line.strip())
    return bad


def _run_go_checker(log_path, cell_dir):
    """Build (once) and run the shared Go checker (test/raft/checker). Returns
    (per_check, note) where per_check maps each check id to PASS/SKIP/FAIL and
    note is a non-empty string only when the checker could not run at all."""
    import re
    import shutil

    checks = "delivery-at-least-once,payload-hash"
    per_check = {c: "NORUN" for c in checks.split(",")}
    if shutil.which("go") is None:
        return per_check, "go is not on PATH"
    checker_bin = cell_dir.parent / "_checker"
    if not checker_bin.exists():
        env = dict(os.environ, GOWORK="off")
        build = subprocess.run(
            ["go", "build", "-o", str(checker_bin), "."],
            cwd=str(CHECKER_DIR), env=env, capture_output=True, text=True,
        )
        if build.returncode != 0:
            return per_check, f"go build failed: {build.stderr.strip()[:200]}"
    run = subprocess.run(
        [str(checker_bin), "-log", str(log_path), "-check", checks],
        capture_output=True, text=True,
    )
    if run.returncode == 2:
        return per_check, f"checker could not run: {(run.stderr or run.stdout).strip()[:200]}"
    # Lines look like "  PASS delivery-at-least-once   ..." / "  SKIP ..." / "  FAIL ...".
    for line in run.stdout.splitlines():
        m = re.match(r"\s*(PASS|SKIP|FAIL)\s+(\S+)", line)
        if m and m.group(2) in per_check:
            per_check[m.group(2)] = m.group(1)
    return per_check, ""


def _by_index(items, recs):
    """Push answers come back in input order with an `index`; align them to the
    records we sent, tolerating a server that reorders."""
    by_txn = {it.get("transaction_id"): it for it in items}
    out = []
    for i, rec in enumerate(recs):
        out.append(by_txn.get(rec.txn) or (items[i] if i < len(items) else {}))
    return out


def _off(v):
    return int(v) if v is not None else None
