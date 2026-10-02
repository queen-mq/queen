#!/usr/bin/env python3
"""The expected-failure matrix: Horizon and the Queen supervisor under every
failure they are meant to survive, each checked for lost jobs, duplicate
completions and where final failures land.

Usage:
  failure_matrix.py --output DIR [--scenarios a,b] [--profiles horizon,queen-php,queen-rust] [--build]

Each lane is one scenario on one profile, on a fresh Compose project from
compose.raft.yml (Redis for Horizon, one Raft broker node for Queen). Jobs are
`App\\Jobs\\FailureMatrixJob`; `bench:matrix-report` returns every attempt, the
Laravel failed-job rows and the broker's dead-letter entries. A lane writes
`<output>/<scenario>/<profile>.json`; the run writes `<output>/summary.md`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

BENCH = Path(__file__).resolve().parents[1]
COMPOSE_FILE = BENCH / "compose.raft.yml"
# Another tag keeps a run apart from other runs on the same Docker host; the
# compose files read the same variable.
APP_IMAGE = os.environ.get("BENCH_APP_IMAGE", "queen-laravel-supervisor-bench:local")


@dataclass(frozen=True)
class Profile:
    name: str
    engine: str
    connection: str
    env: dict[str, str]
    # How the supervisor master shows in `ps`, to find it among its workers.
    master: str


# Close to a production Laravel deployment: one job per pop, renewal by the supervisor.
PRODUCTION_LIKE = {"QUEEN_PREFETCH": "1", "BENCH_QUEEN_ACK_ASYNC": "false", "BENCH_QUEEN_POP_AHEAD": "false"}

PROFILES = {
    "horizon": Profile("horizon", "horizon", "redis", {}, "artisan horizon"),
    "queen-php": Profile("queen-php", "queen-php", "queen", PRODUCTION_LIKE, "artisan queen:supervise"),
    "queen-rust": Profile("queen-rust", "queen-rust", "queen", PRODUCTION_LIKE, "queen-supervisor"),
    # Every throughput option on; not in the default set.
    "queen-rust-fast": Profile("queen-rust-fast", "queen-rust", "queen", {
        "QUEEN_PREFETCH": "4",
        "BENCH_QUEEN_ACK_ASYNC": "true",
        "BENCH_QUEEN_POP_AHEAD": "true",
    }, "queen-supervisor"),
}
DEFAULT_PROFILES = ("horizon", "queen-php", "queen-rust")


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Jobs:
    """The report of one run, per job: its events in order."""

    raw: dict

    def __post_init__(self) -> None:
        if not isinstance(self.raw.get("jobs"), dict):
            self.raw["jobs"] = {}

    def ids(self) -> list[str]:
        return sorted(self.raw["jobs"])

    def events(self, job: str) -> list[str]:
        return [event[0] for event in self.raw["jobs"].get(job, {}).get("events", [])]

    def count(self, job: str, event: str) -> int:
        return self.events(job).count(event)

    def times(self, job: str, event: str) -> list[float]:
        return [e[2] for e in self.raw["jobs"].get(job, {}).get("events", []) if e[0] == event]

    def attempts(self, job: str) -> list[tuple[float, float | None]]:
        """Each attempt's start and end; the end is None when it was killed."""
        spans: list[tuple[float, float | None]] = []
        open_spans: dict[int | None, int] = {}
        for event, attempt, at, _ in self.raw["jobs"].get(job, {}).get("events", []):
            if event == "started":
                open_spans[attempt] = len(spans)
                spans.append((at, None))
            elif event in ("completed", "threw", "released", "failed_by_job"):
                index = open_spans.pop(attempt, None)
                if index is not None:
                    spans[index] = (spans[index][0], at)
        return spans

    def overlapping(self, job: str) -> bool:
        """Whether an attempt started before the previous one ended."""
        spans = sorted(self.attempts(job))
        return any(end is not None and later[0] < end for (_, end), later in zip(spans, spans[1:]))

    def failed_store(self) -> list[str] | None:
        store = self.raw.get("failed_store", {})
        return store.get("job_ids") if store.get("available") else None

    def dead_letter(self) -> int | None:
        letters = self.raw.get("dead_letter", {})
        return letters.get("entries") if letters.get("available") else None


class Lane:
    """One Compose project: one engine, its backend and the producer."""

    def __init__(self, scenario: str, profile: Profile, env: dict[str, str], output: Path) -> None:
        token = uuid.uuid4().hex[:8]
        self.profile = profile
        self.project = f"qfm-{scenario}-{profile.name}-{token}".lower()
        self.volume = f"{self.project}-results"
        self.run_id = f"{scenario}-{profile.name}-{token}"
        self.output = output
        self.timeline: list[tuple[float, str]] = []
        self.started = time.monotonic()
        # Scenario-specific observations, saved with the lane's result.
        self.extra: dict = {}
        self.env = {
            "BENCH_RESULTS_VOLUME": self.volume,
            "BENCH_PROFILE": "fixed",
            "BENCH_QUEUE": "benchmark",
            "BENCH_GROUP": "benchmark",
            "BENCH_WORKERS": "2",
            "BENCH_MIN_WORKERS": "2",
            "BENCH_MAX_WORKERS": "2",
            "BENCH_TIMEOUT": "20",
            "BENCH_RETRY_AFTER": "30",
            "BENCH_DISPATCH_MODE": "single",
            "BENCH_LEDGER_MODE": "off",
            "BENCH_QUEUES": "",
            "BENCH_FAILED_DRIVER": "file",
            "BENCH_LEASE_RENEWAL": "true",
            "BENCH_WORKER_MEMORY": "128",
            "BENCH_CONNECTION": profile.connection,
            "QUEUE_CONNECTION": profile.connection,
            "BENCH_LANE": profile.name,
            "BENCH_QUEEN_PREFORK": "true",
            "BENCH_QUEEN_LEASE_SERVICE": "true",
            "BENCH_QUEEN_HTTP_TRANSPORT": "curl",
            "BENCH_OPCACHE_CLI": "1" if profile.connection == "queen" else "0",
            "QUEEN_ACK_BATCH": "1",
            "BENCH_REDIS_APPENDONLY": "yes",
            "BENCH_REDIS_APPEND_FSYNC": "always",
            **profile.env,
            **env,
        }

    # ---------------------------------------------------------------- plumbing

    def note(self, event: str) -> None:
        self.timeline.append((round(time.monotonic() - self.started, 2), event))
        print(f"    {self.timeline[-1][0]:7.1f}s  {event}", flush=True)

    def compose(self, *args: str, check: bool = True, timeout: float = 600) -> subprocess.CompletedProcess[str]:
        command = [
            "docker", "compose", "--file", str(COMPOSE_FILE), "--project-name", self.project,
            "--profile", self.profile.engine, "--profile", "tools", *args,
        ]
        return subprocess.run(command, check=check, capture_output=True, text=True, timeout=timeout,
                              env={**os.environ, **self.env})

    @property
    def backend(self) -> str:
        return "redis" if self.profile.engine == "horizon" else "broker"

    def containers(self, service: str) -> list[str]:
        return self.compose("ps", "--all", "--quiet", service).stdout.split()

    def container(self, service: str) -> str:
        found = self.containers(service)
        return found[0] if found else ""

    def docker(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["docker", *args], check=check, capture_output=True, text=True, timeout=600)

    def artisan(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        # The producer runs the matrix's own tools: bench:matrix-report reads a whole attempt log, and
        # a soak of 2.5 hours (about 100,000 events) passes PHP's default 128 MiB.
        return self.compose("exec", "--no-TTY", "producer", "php", "-d", "memory_limit=1G", "artisan", "--no-ansi",
                            *args, check=check)

    def app_artisan(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        """Artisan in the supervisor's own container, beside its workers."""
        return self.docker("exec", self.container(self.profile.engine), "php", "artisan", "--no-ansi", *args, check=check)

    # ---------------------------------------------------------------- lifecycle

    def up(self, replicas: int = 1, prepare: Callable[[Lane], None] | None = None) -> None:
        self.replicas = replicas
        self.docker("volume", "create", self.volume)
        self.docker("run", "--rm", "--user", "0:0", "--mount", f"type=volume,src={self.volume},dst=/results",
                    APP_IMAGE, "sh", "-ceu", "chown 1000:1000 /results; chmod 0770 /results")
        if prepare is not None:
            prepare(self)
        self.compose("up", "--detach", "--no-build", "--scale", f"{self.profile.engine}={replicas}",
                     self.profile.engine, "producer")
        self.wait_healthy()
        self.wait_workers(int(self.env["BENCH_WORKERS"]))
        self.note("ready" if replicas == 1 else f"ready, {replicas} replicas")

    def restart_app(self) -> None:
        self.compose("up", "--detach", "--no-build", "--scale", f"{self.profile.engine}={self.replicas}",
                     self.profile.engine)
        self.wait_healthy()
        self.note("app up again")

    def down(self) -> None:
        self.compose("down", "--volumes", "--remove-orphans", "--timeout", "5", check=False)
        self.docker("volume", "rm", "--force", self.volume, check=False)

    def wait_healthy(self, timeout: float = 180) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            apps = self.containers(self.profile.engine)
            states = [self.docker("inspect", "--format",
                                  "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                                  app, check=False).stdout.strip() for app in apps]
            if len(apps) >= getattr(self, "replicas", 1) and states and all(s == "healthy" for s in states):
                return
            time.sleep(1)
        raise RuntimeError(f"{self.profile.engine} did not become healthy")

    def processes(self) -> list[tuple[int, int, str]]:
        app = self.container(self.profile.engine)
        result = self.docker("exec", app, "ps", "-eo", "pid=,ppid=,args=", check=False)
        rows = []
        for line in result.stdout.splitlines():
            parts = line.split(None, 2)
            if len(parts) == 3:
                rows.append((int(parts[0]), int(parts[1]), parts[2]))
        return rows

    def workers(self) -> list[int]:
        needle = "horizon:work" if self.profile.engine == "horizon" else "artisan queue:work"
        return [pid for pid, _, args in self.processes() if needle in args]

    def master(self) -> int:
        for pid, _, args in self.processes():
            if self.profile.master in args and "horizon:" not in args and "sh -c" not in args and pid != 1:
                return pid
        raise RuntimeError("no supervisor master found")

    def wait_workers(self, count: int, timeout: float = 120) -> list[int]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pids = self.workers()
            if len(pids) >= count:
                return pids
            time.sleep(1)
        raise RuntimeError(f"fewer than {count} workers after {timeout}s")

    # ---------------------------------------------------------------- jobs

    def dispatch(self, mode: str, jobs: int, *, first: int = 0, sleep_ms: int = 0, tries: int = 1,
                 backoff: int = 0, timeout: int = 60, allocate_mib: int = 0, partition: str = "",
                 check: bool = True) -> subprocess.CompletedProcess[str]:
        result = self.artisan(
            "bench:matrix-dispatch", f"--run-id={self.run_id}", f"--mode={mode}", f"--jobs={jobs}",
            f"--first={first}", f"--sleep-ms={sleep_ms}", f"--tries={tries}", f"--backoff={backoff}",
            f"--timeout={timeout}", f"--allocate-mib={allocate_mib}",
            *([f"--partition={partition}"] if partition else []), check=check,
        )
        self.note(f"dispatched {jobs} x {mode} (exit {result.returncode})")
        return result

    def report(self) -> Jobs:
        return Jobs(json.loads(self.artisan("bench:matrix-report", self.run_id).stdout))

    def wait_until(self, done: Callable[[Jobs], bool], timeout: float, label: str) -> Jobs:
        deadline = time.monotonic() + timeout
        report = self.report()
        while not done(report) and time.monotonic() < deadline:
            time.sleep(2)
            report = self.report()
        self.note(f"{label}: {'reached' if done(report) else 'TIMED OUT'}")
        return report

    def settle(self, seconds: float) -> Jobs:
        """Give late duplicates the time to appear before judging."""
        time.sleep(seconds)
        return self.report()


# -------------------------------------------------------------------- checks


def every(jobs: Jobs, ids: list[str], name: str, test: Callable[[str], bool]) -> Check:
    bad = [job for job in ids if not test(job)]
    detail = "" if not bad else f"{len(bad)} of {len(ids)}: " + "; ".join(f"{j} {jobs.events(j)}" for j in bad[:3])
    return Check(name, not bad, detail)


def ids(first: int, count: int) -> list[str]:
    return [f"{index:06d}" for index in range(first, first + count)]


def completed_once(jobs: Jobs, expected: list[str], *, duplicates_allowed: bool = False) -> list[Check]:
    """Every job done, never by two attempts at once; twice only where at-least-once allows it."""
    twice = [j for j in expected if jobs.count(j, "completed") > 1]
    return [
        every(jobs, expected, "every job completed", lambda j: jobs.count(j, "completed") >= 1),
        every(jobs, expected, "no two attempts overlapped", lambda j: not jobs.overlapping(j)),
        Check("completed twice (allowed: the ACK could not reach the backend)" if duplicates_allowed
              else "no job completed twice", duplicates_allowed or not twice,
              f"{len(twice)} of {len(expected)}: {twice[:5]}" if twice else ""),
        Check("no failed-job row", jobs.failed_store() == [], f"{jobs.failed_store()}"),
    ]


def failed_finally(lane: Lane, jobs: Jobs, expected: list[str]) -> list[Check]:
    checks = [
        every(jobs, expected, "failed() ran once", lambda j: jobs.count(j, "failed_hook") == 1),
        every(jobs, expected, "never completed", lambda j: jobs.count(j, "completed") == 0),
        Check("one failed-job row each", jobs.failed_store() == expected, f"{jobs.failed_store()}"),
    ]
    if lane.profile.connection == "queen":
        checks.append(Check("one dead-letter entry each", jobs.dead_letter() == len(expected),
                            f"{jobs.dead_letter()} entries"))
    return checks


# ------------------------------------------------------------------ scenarios


def exception_retry(lane: Lane) -> list[Check]:
    expected = ids(0, 6)
    lane.dispatch("throw", 6, tries=3, backoff=2)
    jobs = lane.wait_until(lambda r: all(r.count(j, "failed_hook") for j in expected), 180, "all failed")
    jobs = lane.settle(5)

    def spaced(job: str) -> bool:
        starts, throws = jobs.times(job, "started"), jobs.times(job, "threw")
        return all(starts[i + 1] - throws[i] >= 1.8 for i in range(min(len(starts) - 1, len(throws))))

    return [
        every(jobs, expected, "three attempts each", lambda j: jobs.count(j, "started") == 3),
        every(jobs, expected, "retries wait the 2 s backoff", spaced),
        *failed_finally(lane, jobs, expected),
    ]


def exception_once(lane: Lane) -> list[Check]:
    expected = ids(0, 6)
    lane.dispatch("throw-once", 6, tries=3, backoff=1)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 120, "all completed")
    jobs = lane.settle(5)
    return [
        every(jobs, expected, "two attempts each", lambda j: jobs.count(j, "started") == 2),
        *completed_once(jobs, expected),
    ]


def release_and_fail(lane: Lane) -> list[Check]:
    released, failing = ids(0, 4), ids(4, 4)
    lane.dispatch("release-once", 4, tries=3)
    lane.dispatch("fail", 4, first=4, tries=3)
    jobs = lane.wait_until(
        lambda r: all(r.count(j, "completed") for j in released) and all(r.count(j, "failed_hook") for j in failing),
        120, "released completed, failing failed")
    jobs = lane.settle(5)
    return [
        every(jobs, released, "released job ran twice", lambda j: jobs.count(j, "started") == 2),
        every(jobs, released, "released job completed once", lambda j: jobs.count(j, "completed") == 1),
        every(jobs, failing, "fail() is final: one attempt", lambda j: jobs.count(j, "started") == 1),
        *failed_finally(lane, jobs, failing),
    ]


def job_timeout(lane: Lane) -> list[Check]:
    expected = ids(0, 4)
    workers_before = set(lane.workers())
    lane.dispatch("ok", 4, sleep_ms=15_000, tries=2, timeout=3)
    jobs = lane.wait_until(lambda r: all(r.count(j, "failed_hook") for j in expected), 240, "all failed")
    jobs = lane.settle(5)
    workers_after = set(lane.wait_workers(int(lane.env["BENCH_WORKERS"])))
    return [
        every(jobs, expected, "two attempts, both cut short", lambda j: jobs.count(j, "started") == 2),
        Check("timed-out workers replaced", not workers_after & workers_before or not workers_before,
              f"before {sorted(workers_before)} after {sorted(workers_after)}"),
        *failed_finally(lane, jobs, expected),
    ]


def memory_limit(lane: Lane) -> list[Check]:
    """Over the worker's --memory (96 MiB), under PHP's memory_limit (128 MiB)."""
    expected = ids(0, 6)
    lane.dispatch("memory", 6, allocate_mib=110, tries=3)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 180, "all completed")
    jobs = lane.settle(5)
    pids = {pid for job in expected for pid in jobs.raw["jobs"].get(job, {}).get("pids", [])}
    return [
        Check("a worker over its memory limit was replaced", len(pids) > int(lane.env["BENCH_WORKERS"]),
              f"{len(pids)} distinct worker PIDs for 6 jobs"),
        *completed_once(jobs, expected),
    ]


def memory_fatal(lane: Lane) -> list[Check]:
    """Over PHP's memory_limit on every attempt: the worker dies mid-job each time."""
    expected = ids(0, 2)
    lane.dispatch("memory", 2, allocate_mib=256, tries=2)
    jobs = lane.wait_until(lambda r: all(r.count(j, "failed_hook") for j in expected), 240, "all failed")
    jobs = lane.settle(5)
    return [
        every(jobs, expected, "two attempts, both died", lambda j: jobs.count(j, "started") == 2),
        *failed_finally(lane, jobs, expected),
    ]


def worker_kill(lane: Lane) -> list[Check]:
    expected = ids(0, 8)
    lane.dispatch("ok", 8, sleep_ms=4_000, tries=3)
    jobs = lane.wait_until(lambda r: any(r.count(j, "started") for j in expected), 60, "a job started")
    victim = next(pid for job in expected for pid in jobs.raw["jobs"].get(job, {}).get("pids", []))
    lane.docker("exec", lane.container(lane.profile.engine), "kill", "-KILL", str(victim))
    lane.note(f"SIGKILL worker {victim}")
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 240, "all completed")
    jobs = lane.settle(10)
    return [
        Check("the killed job ran again", any(jobs.count(j, "started") == 2 for j in expected),
              "; ".join(f"{j} {jobs.events(j)}" for j in expected if jobs.count(j, "started") > 1)),
        *completed_once(jobs, expected),
    ]


def master_kill(lane: Lane) -> list[Check]:
    expected = ids(0, 8)
    lane.dispatch("ok", 8, sleep_ms=4_000, tries=3)
    lane.wait_until(lambda r: any(r.count(j, "started") for j in expected), 60, "a job started")
    master = lane.master()
    lane.docker("exec", lane.container(lane.profile.engine), "kill", "-KILL", str(master))
    lane.note(f"SIGKILL master {master}")
    time.sleep(3)
    lane.restart_app()
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 240, "all completed")
    jobs = lane.settle(10)
    return completed_once(jobs, expected)


def stop_short(lane: Lane) -> list[Check]:
    """A rolling update while jobs shorter than the shutdown grace run."""
    expected = ids(0, 2)
    lane.dispatch("ok", 2, sleep_ms=10_000, tries=3)
    lane.wait_until(lambda r: all(r.count(j, "started") for j in expected), 60, "both started")
    lane.docker("stop", "--time", "90", lane.container(lane.profile.engine))
    lane.note("app stopped (SIGTERM, 90 s grace)")
    lane.restart_app()
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 120, "all completed")
    jobs = lane.settle(10)
    return [
        every(jobs, expected, "finished during the grace, not run again", lambda j: jobs.count(j, "started") == 1),
        *completed_once(jobs, expected),
    ]


def stop_long(lane: Lane) -> list[Check]:
    """A rolling update while jobs longer than the supervisor's shutdown grace run."""
    expected = ids(0, 2)
    lane.dispatch("ok", 2, sleep_ms=60_000, timeout=120, tries=3)
    lane.wait_until(lambda r: all(r.count(j, "started") for j in expected), 60, "both started")
    lane.docker("stop", "--time", "90", lane.container(lane.profile.engine))
    lane.note("app stopped (SIGTERM, 90 s grace)")
    lane.restart_app()
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 300, "all completed")
    jobs = lane.settle(10)
    return [
        Check("attempts per job (information)", True, "; ".join(f"{j}: {jobs.count(j, 'started')}" for j in expected)),
        *completed_once(jobs, expected),
    ]


def stop_long_batch(lane: Lane) -> list[Check]:
    """One worker, two long jobs in one Queen partition: with prefetch, one pop leases both and the
    worker runs the second from its batch. A deploy kills it during the second job. The first job
    finished before the deploy, so it must not run again: its ACK must have reached the backend."""
    expected = ids(0, 2)
    lane.dispatch("ok", 2, sleep_ms=60_000, timeout=120, tries=3, partition="matrix-batch")
    lane.wait_until(lambda r: r.count("000000", "completed") and r.count("000001", "started"), 150,
                    "first done, second started")
    lane.docker("stop", "--time", "90", lane.container(lane.profile.engine))
    lane.note("app stopped (SIGTERM, 90 s grace)")
    lane.restart_app()
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 360, "all completed")
    jobs = lane.settle(10)
    return [
        Check("attempts per job (information)", True, "; ".join(f"{j}: {jobs.count(j, 'started')}" for j in expected)),
        Check("the job done before the deploy ran once", jobs.count("000000", "started") == 1,
              f"{jobs.count('000000', 'started')} starts"),
        *completed_once(jobs, expected),
    ]


def backend_restart(lane: Lane) -> list[Check]:
    expected = ids(0, 300)
    lane.dispatch("ok", 300, sleep_ms=100, tries=3)
    time.sleep(3)
    lane.docker("restart", lane.container(lane.backend))
    lane.note(f"{lane.backend} restarted")
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 300, "all completed")
    jobs = lane.settle(35)
    return completed_once(jobs, expected)


def backend_pause(lane: Lane, pause: float, sleep_ms: int, *, duplicates_allowed: bool = False) -> list[Check]:
    expected = ids(0, 2)
    lane.dispatch("ok", 2, sleep_ms=sleep_ms, tries=3)
    lane.wait_until(lambda r: all(r.count(j, "started") for j in expected), 60, "both started")
    backend = lane.container(lane.backend)
    lane.docker("pause", backend)
    lane.note(f"{lane.backend} paused for {pause:.0f} s")
    time.sleep(pause)
    lane.docker("unpause", backend)
    lane.note(f"{lane.backend} resumed")
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 300, "all completed")
    jobs = lane.settle(40)
    return [
        Check("attempts per job (information)", True, "; ".join(f"{j}: {jobs.count(j, 'started')}" for j in expected)),
        *completed_once(jobs, expected, duplicates_allowed=duplicates_allowed),
    ]


def pause_short(lane: Lane) -> list[Check]:
    """A stall shorter than the lease: nothing may run twice."""
    checks = backend_pause(lane, 5, 10_000)
    jobs = lane.report()
    return [*checks, every(jobs, ids(0, 2), "not run again", lambda j: jobs.count(j, "started") == 1)]


def pause_long(lane: Lane) -> list[Check]:
    """The backend unreachable for longer than the lease (retry_after 30 s). A job
    that finished while it was away could not be acknowledged: it runs again."""
    return backend_pause(lane, 45, 20_000, duplicates_allowed=True)


def dispatch_backend_down(lane: Lane) -> list[Check]:
    backend = lane.container(lane.backend)
    lane.docker("stop", backend)
    lane.note(f"{lane.backend} stopped")
    refused = lane.dispatch("ok", 1, check=False)
    lane.docker("start", backend)
    lane.note(f"{lane.backend} started")
    time.sleep(5)
    expected = ids(1, 2)
    accepted = lane.dispatch("ok", 2, first=1, check=False)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 120, "all completed")
    error_lines = (refused.stderr or refused.stdout).strip().splitlines()
    return [
        Check("dispatch fails loudly while the backend is down", refused.returncode != 0,
              error_lines[-1][:200] if error_lines else ""),
        Check("dispatch works once it is back", accepted.returncode == 0, accepted.stderr.strip()[-200:]),
        *completed_once(jobs, expected),
    ]


COMPAT_SCENARIOS = (
    "delay", "chain", "chain-failure", "batch", "batch-failure", "unique", "without-overlapping",
    "rate-limited", "backoff-array", "retry-until", "max-exceptions", "fail-on-timeout", "encrypted",
    "after-commit", "events", "failed-commands", "queue-size",
)
# `bench:compat-more`: the second command, same lane. prune-failed empties the failed-job store, and
# no scenario after it reads that store. fork-in-job is last: where a worker dies, its retries go on.
COMPAT_MORE_SCENARIOS = (
    "queued-listener", "queued-notification", "queued-closure", "throttles-exceptions", "skip-middleware",
    "unique-until-processing", "missing-models", "batch-allow-failures", "batch-cancel", "retry-batch",
    "delay-datetime", "release-delay", "queue-monitor", "prune-failed", "fork-in-job",
)


def replicas_kill(lane: Lane) -> list[Check]:
    """Two supervisor replicas on one queue; one is killed outright, as a node loss would."""
    expected = ids(0, 40)
    lane.dispatch("ok", 40, sleep_ms=2_000, tries=3)
    lane.wait_until(lambda r: sum(r.count(j, "started") for j in expected) >= 3, 60, "jobs started")
    victim = lane.containers(lane.profile.engine)[0]
    lane.docker("kill", victim)
    lane.note(f"replica {victim[:12]} killed (SIGKILL)")
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 300, "all completed")
    jobs = lane.settle(35)
    return completed_once(jobs, expected)


def replicas_rolling(lane: Lane) -> list[Check]:
    """A rolling update of two replicas while jobs run: each stops gracefully, then starts again."""
    expected = ids(0, 60)
    lane.dispatch("ok", 60, sleep_ms=1_000, tries=3)
    lane.wait_until(lambda r: sum(r.count(j, "started") for j in expected) >= 3, 60, "jobs started")
    for replica in lane.containers(lane.profile.engine):
        lane.docker("stop", "--time", "90", replica)
        lane.note(f"replica {replica[:12]} stopped (SIGTERM)")
        lane.docker("start", replica)
        lane.wait_healthy()
        lane.note(f"replica {replica[:12]} started again")
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 300, "all completed")
    jobs = lane.settle(10)
    return [
        Check("jobs that ran twice (information)", True,
              f"{sum(1 for j in expected if jobs.count(j, 'started') > 1)} of {len(expected)}"),
        *completed_once(jobs, expected),
    ]


SOAK_SECONDS = int(os.environ.get("MATRIX_SOAK_SECONDS", "2700"))
SOAK_RATE = int(os.environ.get("MATRIX_SOAK_RATE", "5"))


def memory_sample(lane: Lane, elapsed: float) -> dict:
    """The master's and the workers' resident memory, in MiB."""
    master_rss, workers = None, []
    for container in lane.containers(lane.profile.engine):
        rows = lane.docker("exec", container, "ps", "-eo", "rss=,args=", check=False).stdout.splitlines()
        for row in rows:
            parts = row.strip().split(None, 1)
            if len(parts) != 2 or not parts[0].isdigit():
                continue
            rss, args = int(parts[0]) / 1024, parts[1]
            if ("horizon:work" if lane.profile.engine == "horizon" else "artisan queue:work") in args:
                workers.append(rss)
            elif lane.profile.master in args and "horizon:" not in args and "sh -c" not in args:
                master_rss = rss
    workers.sort()
    return {"t": round(elapsed), "master_rss_mib": master_rss, "workers": len(workers),
            "worker_rss_median_mib": workers[len(workers) // 2] if workers else None}


def growth(samples: list[dict], key: str) -> tuple[float | None, float | None]:
    """Median of the first third against the last third of a series."""
    values = [s[key] for s in samples if s.get(key) is not None]
    if len(values) < 6:
        return None, None
    third = len(values) // 3
    first, last = sorted(values[:third]), sorted(values[-third:])
    return first[len(first) // 2], last[len(last) // 2]


def soak_report(report: dict) -> dict:
    """`bench:matrix-report --summary` with its maps as dicts: PHP encodes an empty array as a list."""
    kinds = report["summary"] if isinstance(report["summary"], dict) else {}
    return {**report, "summary": {name: {**kind, "anomalies": kind["anomalies"] or {}} for name, kind in kinds.items()}}


def soak_drained(report: dict, dispatched: int | None) -> bool:
    """Every dispatched job has reached a worker and ended. A job that never started is absent
    from the report, not pending in it: a delayed job dispatched in the last minute is due
    only after the dispatch ends."""
    seen = sum(kind["jobs"] for kind in report["summary"].values())
    pending = any(anomaly.startswith("not ") for kind in report["summary"].values()
                  for anomaly in kind["anomalies"].values())
    return seen >= (dispatched or 0) and not pending


def soak(lane: Lane) -> list[Check]:
    """A long mixed workload: a worker killed every 10 minutes, one rolling restart halfway,
    then every job must end as its kind says, and memory must stay flat."""
    command = ["docker", "compose", "--file", str(COMPOSE_FILE), "--project-name", lane.project,
               "--profile", lane.profile.engine, "--profile", "tools", "exec", "--no-TTY", "producer",
               "php", "artisan", "--no-ansi", "bench:matrix-soak", f"--run-id={lane.run_id}",
               f"--seconds={SOAK_SECONDS}", f"--rate={SOAK_RATE}"]
    dispatcher = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                  env={**os.environ, **lane.env})
    lane.note(f"dispatching {SOAK_RATE} jobs/s for {SOAK_SECONDS} s")
    started, next_kill, restarted, samples = time.monotonic(), 600.0, False, []
    while dispatcher.poll() is None:
        elapsed = time.monotonic() - started
        samples.append(memory_sample(lane, elapsed))
        if elapsed >= next_kill:
            victims = lane.workers()
            if victims:
                lane.docker("exec", lane.container(lane.profile.engine), "kill", "-KILL", str(victims[0]), check=False)
                lane.note(f"SIGKILL worker {victims[0]}")
            next_kill += 600
        if not restarted and elapsed >= SOAK_SECONDS / 2:
            app = lane.container(lane.profile.engine)
            lane.docker("stop", "--time", "90", app)
            lane.note("rolling restart: stopped (SIGTERM)")
            lane.docker("start", app)
            lane.wait_healthy()
            lane.note("rolling restart: started again")
            restarted = True
        time.sleep(60)
    out, err = dispatcher.communicate()
    dispatched = json.loads(out.strip().splitlines()[-1]) if out.strip() else {"error": err[-500:]}
    lane.note(f"dispatch done: {dispatched}")

    def summary() -> dict:
        return soak_report(json.loads(lane.artisan("bench:matrix-report", lane.run_id, "--summary").stdout))

    deadline, report = time.monotonic() + 900, summary()
    while time.monotonic() < deadline and not soak_drained(report, dispatched.get("dispatched")):
        time.sleep(15)
        report = summary()
    lane.note("drained" if soak_drained(report, dispatched.get("dispatched")) else "drain: TIMED OUT")
    lane.extra.update({"dispatched": dispatched, "summary": report, "memory": samples})

    checks = []
    total = sum(kind["jobs"] for kind in report["summary"].values())
    checks.append(Check("every dispatched job was seen by a worker", total == dispatched.get("dispatched"),
                        f"{total} of {dispatched.get('dispatched')}"))
    for name, kind in report["summary"].items():
        checks.append(Check(f"{name} jobs ended as expected", kind["as_expected"] == kind["jobs"],
                            f"{kind['as_expected']} of {kind['jobs']}; {dict(list(kind['anomalies'].items())[:5])}"))
    bad = report["summary"].get("bad", {}).get("jobs", 0)
    checks.append(Check("one failed-job row per permanent failure", report["failed_store"] == bad,
                        f"{report['failed_store']} rows, {bad} bad jobs"))
    if lane.profile.connection == "queen":
        checks.append(Check("one dead-letter entry per permanent failure", report["dead_letter"] == bad,
                            f"{report['dead_letter']} entries"))
    for key, limit in (("master_rss_mib", 0.25), ("worker_rss_median_mib", 0.30)):
        first, last = growth(samples, key)
        checks.append(Check(f"{key.replace('_', ' ')} stays flat", first is None or last <= first * (1 + limit) + 8,
                            f"{first} → {last} MiB (first third → last third)"))
    return checks


COMPAT_DATABASE = "/results/compat.sqlite"


def compat_database(lane: Lane) -> None:
    """The SQLite file and its tables, before any worker starts: queue:work reads the
    cache (the restart signal) as it boots, and the compatibility lanes' cache is this
    database. It lives on the results volume, shared by every container of the lane."""
    lane.compose("run", "--rm", "--no-deps", "producer", "sh", "-ceu",
                 f"touch {COMPAT_DATABASE} && php artisan --no-ansi bench:compat setup")


def laravel_compat(lane: Lane) -> list[Check]:
    """Laravel's queue features, each checked by `bench:compat` or `bench:compat-more`
    inside the supervisor's container, beside its workers."""
    checks: list[Check] = []
    runs = [("bench:compat", name) for name in COMPAT_SCENARIOS]
    runs += [("bench:compat-more", name) for name in COMPAT_MORE_SCENARIOS]
    for command, name in runs:
        result = lane.app_artisan(command, name, f"--run-id={lane.run_id}-{name}", check=False)
        lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
        try:
            data = json.loads(lines[-1])
        except (IndexError, json.JSONDecodeError):
            checks.append(Check(f"{name}: ran", False, (result.stderr or result.stdout).strip()[-300:]))
            lane.note(f"{name}: no result")
            continue
        lane.extra[name] = data
        checks.extend(Check(f"{name}: {c['name']}", c["passed"], c["detail"]) for c in data["checks"])
        lane.note(f"{name}: {'pass' if data['passed'] else 'FAIL'}")
    return checks


@dataclass(frozen=True)
class Scenario:
    name: str
    run: Callable[[Lane], list[Check]]
    env: dict[str, str] = field(default_factory=dict)
    replicas: int = 1
    prepare: Callable[[Lane], None] | None = None


SCENARIOS = [
    Scenario("exception-retry", exception_retry),
    Scenario("exception-once", exception_once),
    Scenario("release-and-fail", release_and_fail),
    Scenario("job-timeout", job_timeout),
    Scenario("memory-limit", memory_limit, {"BENCH_WORKER_MEMORY": "96"}),
    Scenario("memory-fatal", memory_fatal),
    Scenario("worker-kill", worker_kill),
    Scenario("master-kill", master_kill),
    Scenario("stop-short", stop_short),
    # Horizon has no lease renewal: its retry_after must outlast the job.
    Scenario("stop-long", stop_long, {"BENCH_RETRY_AFTER": "90"}),
    Scenario("stop-long-batch", stop_long_batch, {
        "BENCH_RETRY_AFTER": "90", "BENCH_WORKERS": "1", "BENCH_MIN_WORKERS": "1", "BENCH_MAX_WORKERS": "1",
    }),
    Scenario("backend-restart", backend_restart),
    Scenario("pause-short", pause_short),
    Scenario("pause-long", pause_long),
    Scenario("dispatch-backend-down", dispatch_backend_down),
    Scenario("replicas-kill", replicas_kill, {"BENCH_QUEEN_COORDINATION": "true"}, replicas=2),
    Scenario("replicas-rolling", replicas_rolling, {"BENCH_QUEEN_COORDINATION": "true"}, replicas=2),
    Scenario("soak", soak, {"BENCH_WORKERS": "8", "BENCH_MIN_WORKERS": "8", "BENCH_MAX_WORKERS": "8"}),
    Scenario("laravel-compat", laravel_compat, {
        "BENCH_CACHE_STORE": "database", "BENCH_DB_DATABASE": COMPAT_DATABASE,
        "BENCH_WORKERS": "3", "BENCH_MIN_WORKERS": "3", "BENCH_MAX_WORKERS": "3",
    }, prepare=compat_database),
]


def run_lane(scenario: Scenario, profile: Profile, output: Path) -> dict:
    lane = Lane(scenario.name, profile, scenario.env, output)
    print(f"\n== {scenario.name} / {profile.name} ({lane.project})", flush=True)
    result: dict = {"scenario": scenario.name, "profile": profile.name, "run_id": lane.run_id}
    try:
        lane.up(scenario.replicas, scenario.prepare)
        checks = scenario.run(lane)
        result["checks"] = [check.__dict__ for check in checks]
        result["passed"] = all(check.passed for check in checks)
        result["report"] = lane.report().raw
        result["extra"] = lane.extra
    except Exception as error:  # a lane that cannot finish is a failed lane, not a crashed matrix
        result["passed"] = False
        result["error"] = f"{type(error).__name__}: {error}"
        lane.note(f"ERROR {result['error']}")
        if isinstance(error, subprocess.CalledProcessError):
            result["stderr"] = (error.stderr or "")[-2000:]
    finally:
        logs = lane.compose("logs", "--no-color", "--timestamps", check=False, timeout=120)
        (output / scenario.name).mkdir(parents=True, exist_ok=True)
        (output / scenario.name / f"{profile.name}.log").write_text(logs.stdout[-2_000_000:], encoding="utf-8")
        result["timeline"] = lane.timeline
        lane.down()
    (output / scenario.name / f"{profile.name}.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
    for check in result.get("checks", []):
        mark = "ok  " if check["passed"] else "FAIL"
        print(f"    {mark} {check['name']}{'  — ' + check['detail'] if check['detail'] and not check['passed'] else ''}")
    return result


def summary(results: list[dict]) -> str:
    lines = ["| Scenario | Profile | Result | Failed checks |", "| --- | --- | --- | --- |"]
    for result in results:
        failed = [c["name"] for c in result.get("checks", []) if not c["passed"]]
        if "error" in result:
            failed.append(result["error"])
        lines.append(f"| {result['scenario']} | {result['profile']} | {'pass' if result['passed'] else 'FAIL'} "
                     f"| {'; '.join(failed)} |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scenarios", default=",".join(s.name for s in SCENARIOS))
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    parser.add_argument("--build", action="store_true", help="rebuild the application image first")
    args = parser.parse_args()

    wanted = args.scenarios.split(",")
    unknown = set(wanted) - {s.name for s in SCENARIOS} | set(args.profiles.split(",")) - set(PROFILES)
    if unknown:
        parser.error(f"unknown scenario or profile: {', '.join(sorted(unknown))}")
    if args.build:
        subprocess.run(["docker", "compose", "--file", str(COMPOSE_FILE), "--profile", "tools", "build", "producer"],
                       check=True)
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    for scenario in (s for s in SCENARIOS if s.name in wanted):
        for name in args.profiles.split(","):
            results.append(run_lane(scenario, PROFILES[name], args.output))
            (args.output / "summary.md").write_text(summary(results), encoding="utf-8")
    print("\n" + summary(results))
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
