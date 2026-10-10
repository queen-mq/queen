#!/usr/bin/env python3
"""The expected-failure matrix: Horizon and the Queen supervisor under every
failure they are meant to survive, each checked for lost jobs, duplicate
completions and where final failures land.

Usage:
  failure_matrix.py --output DIR [--scenarios a,b] [--profiles horizon,queen-php,queen-rust]
                    [--prefork on,off] [--only item,item] [--build]

Each lane is one scenario on one profile, on a fresh Compose project from
compose.raft.yml (Redis for Horizon, one Raft broker node for Queen). Jobs are
`App\\Jobs\\FailureMatrixJob`; `bench:matrix-report` returns every attempt, the
Laravel failed-job rows and the broker's dead-letter entries. A lane writes
`<output>/<scenario>/<profile>.json`; the run writes `<output>/summary.md`.

Scenarios that record an outcome are also held to the Horizon lane of the same
scenario: the same attempts, failed-job rows and callbacks, and times within a
tolerance. The run writes that comparison to `<output>/parity.md` and
`parity.json`, and fails when a Queen lane differs.
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
from dataclasses import dataclass, field, replace
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
    # The same on the PHP engine, whose workers renew through a PHP helper each.
    "queen-php-fast": Profile("queen-php-fast", "queen-php", "queen", {
        "QUEEN_PREFETCH": "4",
        "BENCH_QUEEN_ACK_ASYNC": "true",
        "BENCH_QUEEN_POP_AHEAD": "true",
    }, "artisan queen:supervise"),
    # The Rust supervisor installed as root for the user that runs it, in a container that starts
    # as root (compose.raft.yml queen-installed); only the scenarios that name its engine use it.
    # The launcher runs the binary from its pinned directory, as ./queen-supervisor.
    "queen-installed": Profile("queen-installed", "queen-installed", "queen", PRODUCTION_LIKE,
                               "queen-supervisor --php"),
}
DEFAULT_PROFILES = ("horizon", "queen-php", "queen-rust")
# The Horizon lane every other lane of a scenario is compared with.
REFERENCE = "horizon"
PREFORK_MODES = ("on", "off")


# Settings a stack lays over every lane, after the scenario's: "*" for every engine, then the
# engine's own. `balanced`: pools that balance by backlog, more than one
# queue per pool (routed: BENCH_ROUTED_BALANCE), the command-line opcache on. The image decides
# Laravel and PHP (BENCH_LARAVEL_VERSION, BENCH_PHP_VERSION at build time).
STACKS: dict[str, dict[str, dict[str, str]]] = {
    "default": {},
    "balanced": {
        "*": {
            "BENCH_OPCACHE_CLI": "1", "BENCH_PROFILE": "auto", "BENCH_QUEUES": "benchmark,benchmark-low",
            "BENCH_WORKERS": "2", "BENCH_MIN_WORKERS": "2", "BENCH_MAX_WORKERS": "4",
            "BENCH_ROUTED_BALANCE": "auto", "BENCH_APP_MEMORY": "2048m",
        },
        # Horizon's minProcesses counts per queue: one for each of the two is Queen's two per pool.
        "horizon": {"BENCH_MIN_WORKERS": "1"},
    },
}


def stack_env(stack: str, profile: Profile) -> dict[str, str]:
    layers = STACKS[stack]
    return {**layers.get("*", {}), **layers.get(profile.engine, {})}


def stack_versions(config_json: str, supervisor: str = "") -> dict:
    """What a lane ran on, from `bench:config` and `queen-supervisor --version`."""
    config = json.loads(config_json)
    versions = {key: config.get(key) for key in ("php", "laravel", "horizon", "queen_client", "opcache_cli")}
    versions["supervisor"] = supervisor.strip().removeprefix("queen-supervisor ") or None
    # The layout the application resolved, not the one asked for: a setting that does not
    # reach the container shows here.
    benchmark = config.get("benchmark") or {}
    versions["layout"] = {"profile": benchmark.get("profile"), "queues": benchmark.get("queues"),
                          "routed": benchmark.get("routed"), "routed_balance": benchmark.get("routed_balance")}
    return versions


def lane_profiles(names: list[str], prefork: list[str]) -> list[Profile]:
    """Each Queen profile once per prefork mode; Horizon, which has no prefork, once.
    With prefork on a profile keeps its name, so earlier runs stay comparable."""
    profiles: list[Profile] = []
    for name in names:
        profile = PROFILES[name]
        if profile.connection != "queen":
            profiles.append(profile)
            continue
        for mode in prefork:
            profiles.append(profile if mode == "on" else replace(
                profile, name=f"{profile.name}-prefork-off", env={**profile.env, "BENCH_QUEEN_PREFORK": "false"}))
    return profiles


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
        for event, attempt, at, *_ in self.raw["jobs"].get(job, {}).get("events", []):
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

    def codes(self, job: str) -> list[str | None]:
        """The code each run of the job ran (DeployedCode, as its worker loaded it at boot)."""
        return [(e[5] or {}).get("code") if len(e) > 5 else None
                for e in self.raw["jobs"].get(job, {}).get("events", []) if e[0] == "started"]

    def attempts_of(self, job: str, event: str) -> list[int | None]:
        """The attempt each `event` of the job was logged with, in order."""
        return [e[1] for e in self.raw["jobs"].get(job, {}).get("events", []) if e[0] == event]

    def failed_with(self, job: str) -> str | None:
        """The exception failed() received, by class name."""
        return next((e[3] for e in self.raw["jobs"].get(job, {}).get("events", []) if e[0] == "failed_hook"), None)

    def outcome(self, job: str) -> dict:
        """What the Horizon lane and a Queen lane must share about one job."""
        store = self.failed_store()
        return {"runs": self.count(job, "started"), "completed": self.count(job, "completed"),
                "failed": self.count(job, "failed_hook"), "failed_with": self.failed_with(job),
                "failed_row": None if store is None else job in store}

    def failed_store(self) -> list[str] | None:
        store = self.raw.get("failed_store", {})
        return store.get("job_ids") if store.get("available") else None

    def dead_letter(self) -> int | None:
        letters = self.raw.get("dead_letter", {})
        return letters.get("entries") if letters.get("available") else None


class Lane:
    """One Compose project: one engine, its backend and the producer."""

    def __init__(self, scenario: str, profile: Profile, env: dict[str, str], output: Path,
                 only: frozenset[str] = frozenset()) -> None:
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
        # What parity() compares with the Horizon lane, per item: a compatibility
        # scenario's name, or this scenario's. Each is {"same": {...}, "near": {...}}.
        self.outcome: dict[str, dict] = {}
        # The compatibility scenarios to run, when not all of them (--only).
        self.only = only
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

    def probe(self, *options: str, user: str = "") -> tuple[int, str]:
        """`queen:supervisor status` in the supervisor's container, as a Kubernetes exec probe runs
        it: its exit code and what it printed. `user` runs it as another user, as a probe of a
        container that starts as root does."""
        result = self.docker("exec", *(["--user", user] if user else []), self.container(self.profile.engine),
                             "php", "artisan", "--no-ansi", "queen:supervisor", "status", *options, check=False)
        return result.returncode, result.stdout + result.stderr

    def status(self) -> tuple[bool, dict]:
        """Whether the readiness probe passes, and the status it read (empty when none)."""
        code, output = self.probe("--json", "--check")
        line = next((line for line in output.splitlines() if line.startswith("{")), "")
        try:
            return code == 0, json.loads(line) if line else {}
        except json.JSONDecodeError:
            return code == 0, {}

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
    replaced = not workers_after & workers_before or not workers_before
    # The killed attempt holds its reservation (Redis) or lease (Queen) until
    # retry_after: Laravel retries it no sooner. Later is the engine's pace.
    lease = int(lane.env["BENCH_RETRY_AFTER"])
    retried_after_lease = all(starts[1] - starts[0] >= lease - 2 for j in expected
                              if len(starts := jobs.times(j, "started")) > 1)
    lane.outcome["job-timeout"] = {"same": {
        **{f"job {j}": jobs.outcome(j) for j in expected},
        "timed-out workers replaced": replaced,
        f"retried no sooner than retry_after ({lease} s)": retried_after_lease,
    }, "near": {}}
    return [
        every(jobs, expected, "two attempts, both cut short", lambda j: jobs.count(j, "started") == 2),
        Check("timed-out workers replaced", replaced, f"before {sorted(workers_before)} after {sorted(workers_after)}"),
        *failed_finally(lane, jobs, expected),
    ]


def memory_limit(lane: Lane) -> list[Check]:
    """Over the worker's --memory (96 MiB), under PHP's memory_limit (128 MiB)."""
    expected = ids(0, 6)
    lane.dispatch("memory", 6, allocate_mib=110, tries=3)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 180, "all completed")
    jobs = lane.settle(5)
    pids = {pid for job in expected for pid in jobs.raw["jobs"].get(job, {}).get("pids", [])}
    replaced = len(pids) > int(lane.env["BENCH_WORKERS"])
    lane.outcome["memory-limit"] = {"same": {
        **{f"job {j}": jobs.outcome(j) for j in expected},
        "a worker over its memory limit was replaced": replaced,
    }, "near": {}}
    return [
        Check("a worker over its memory limit was replaced", replaced, f"{len(pids)} distinct worker PIDs for 6 jobs"),
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
    lane.outcome["stop-short"] = {"same": {f"job {j}": jobs.outcome(j) for j in expected}, "near": {}}
    return [
        every(jobs, expected, "finished during the grace, not run again", lambda j: jobs.count(j, "started") == 1),
        *completed_once(jobs, expected),
    ]


def stop_lease(lane: Lane) -> list[Check]:
    """A deploy (SIGTERM) right after a job started that outlives its lease: on Queen, retry_after
    30 s for a job of 36 s, so only renewals during the drain keep the lease, until the job ends
    within shutdown_grace (40 s). A second replica is idle: if the lease lapses, it takes the job,
    which then runs twice. Horizon cannot renew, so its lane runs with a retry_after longer than
    the job; and a stopping Horizon supervisor waits for its workers only `timeout` seconds,
    then exits and the job dies, so its timeout must outlast the drain too. A Horizon
    deployment needs both."""
    expected = ids(0, 1)
    lane.dispatch("ok", 1, sleep_ms=36_000, timeout=60, tries=3)
    jobs = lane.wait_until(lambda r: r.count("000000", "started"), 60, "the job started")
    hosts = jobs.raw["jobs"].get("000000", {}).get("hosts", [])
    replicas = lane.containers(lane.profile.engine)
    # A container's host name is the start of its id.
    victim = next((c for c in replicas if hosts and hosts[0] and c.startswith(hosts[0])), "")
    if not victim:
        raise RuntimeError(f"no replica runs the job: hosts {hosts}, replicas {replicas}")
    lane.docker("stop", "--time", "90", victim)
    lane.note(f"replica {victim[:12]} stopped (SIGTERM, 90 s grace)")
    lane.docker("start", victim)
    lane.wait_healthy()
    lane.note(f"replica {victim[:12]} started again")
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 120, "all completed")
    jobs = lane.settle(10)
    runs = jobs.raw["jobs"].get("000000", {})
    lane.outcome["stop-lease"] = {"same": {f"job {j}": jobs.outcome(j) for j in expected}, "near": {}}
    return [
        every(jobs, expected, "finished during the drain, not run again", lambda j: jobs.count(j, "started") == 1),
        Check("ran in one replica only", len(runs.get("hosts", [])) == 1, f"hosts {runs.get('hosts')}"),
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


def run_in_progress(jobs: Jobs, job: str) -> tuple[int, int] | None:
    """The job's last run, if it has not ended: its number and the pid of its worker. A run
    that released itself, threw or completed has ended."""
    events = jobs.raw["jobs"].get(job, {}).get("events", [])
    starts = [index for index, event in enumerate(events) if event[0] == "started"]
    if not starts or len(events[starts[-1]]) < 5:
        return None
    if any(event[0] in ("released", "threw", "completed", "failed_hook") for event in events[starts[-1] + 1:]):
        return None
    return len(starts), int(events[starts[-1]][4])


def worker_death(lane: Lane, item: str, *, mode: str, tries: int, timeout: int, kill: bool,
                 runs: list[int], deliveries: list[int], failed_with: str) -> list[Check]:
    """A job whose worker dies in the middle of a run, every run: Laravel's timeout handler
    kills it (`kill` False: the job sleeps past its `timeout`), or SIGKILL from outside, as
    the kernel's OOM killer would (`kill` True). A second message waits behind it in the
    same partition, the lane's only one: a job stuck in its lease would hold it forever.
    On Queen the attempt Laravel sees is the payload's runs plus the broker's delivery
    count; a lease that expires adds one delivery, as Redis adds one reservation."""
    killer, follower = "000000", "000001"
    lane.dispatch(mode, 1, sleep_ms=30_000, tries=tries, timeout=timeout)
    lane.dispatch("ok", 1, first=1, tries=1)
    container = lane.container(lane.profile.engine)
    handled: set[int] = set()
    kills: list[dict] = []
    deadline = time.monotonic() + 240
    jobs = lane.report()
    while time.monotonic() < deadline and not (
            (jobs.count(killer, "failed_hook") or jobs.count(killer, "completed")) and jobs.count(follower, "completed")):
        run = run_in_progress(jobs, killer) if kill else None
        if run is not None and run[0] not in handled:
            # A run that releases itself does so at once; one that sleeps is still running.
            time.sleep(1)
            if run_in_progress(lane.report(), killer) == run:
                lane.docker("exec", container, "kill", "-KILL", str(run[1]), check=False)
                kills.append({"run": run[0], "pid": run[1]})
                lane.note(f"SIGKILL worker {run[1]} in run {run[0]}")
            handled.add(run[0])
        time.sleep(1)
        jobs = lane.report()
    lane.note("killer ended, follower completed" if jobs.count(follower, "completed") else "TIMED OUT")
    jobs = lane.settle(5)
    observed = {
        "kills": kills, "deliveries": jobs.attempts_of(killer, "event_before"),
        "runs": jobs.attempts_of(killer, "started"), "failed_with": jobs.failed_with(killer),
        "dead_letter_entries": jobs.dead_letter(), "failed_rows": jobs.failed_store(),
        "follower_started_after_killer_failed": round(
            (jobs.times(follower, "started") or [0])[0] - (jobs.times(killer, "failed_hook") or [0])[0], 2),
    }
    lane.extra["worker_death"] = observed
    lane.outcome[item] = {"same": {
        f"job {killer}": {**jobs.outcome(killer), "deliveries": observed["deliveries"], "attempts of its runs": observed["runs"]},
        "next message on the partition": jobs.outcome(follower),
    }, "near": {}}
    return [
        Check(f"deliveries {deliveries}, runs {runs}", observed["deliveries"] == deliveries and observed["runs"] == runs,
              f"deliveries {observed['deliveries']}, runs {observed['runs']}, kills {kills}"),
        Check(f"failed with {failed_with}", str(observed["failed_with"]).endswith(failed_with), f"{observed['failed_with']}"),
        *failed_finally(lane, jobs, [killer]),
        Check("the next message on the partition ran, once", jobs.count(follower, "completed") == 1,
              f"{jobs.events(follower)}"),
    ]


# One partition, so a message stuck in its lease blocks the next; a lease of 10 s, so a death
# costs seconds. A renewal request may take 1 s, for the renewal budget to fit in the lease.
DEATH_ENV = {"QUEEN_PARTITIONS": "1", "BENCH_RETRY_AFTER": "10", "BENCH_TIMEOUT": "5",
             "BENCH_LEASE_RENEWAL_TIMEOUT": "1"}


def death_timeout(lane: Lane) -> list[Check]:
    """tries 2, a run that outlives its timeout: Laravel fails the job when the second run
    times out, then kills the worker."""
    return worker_death(lane, "death-timeout", mode="ok", tries=2, timeout=3, kill=False,
                        runs=[1, 2], deliveries=[1, 2], failed_with="TimeoutExceededException")


def death_sigkill(lane: Lane) -> list[Check]:
    """tries 2, SIGKILL at each run: no handler runs, so the third delivery exceeds tries and
    Laravel fails it before handle()."""
    return worker_death(lane, "death-sigkill", mode="ok", tries=2, timeout=9, kill=True,
                        runs=[1, 2], deliveries=[1, 2, 3], failed_with="MaxAttemptsExceededException")


def death_release_timeout(lane: Lane) -> list[Check]:
    """tries 3, released once, then runs that outlive their timeout: the release is the first
    attempt, so the second timeout, at attempt 3, fails the job."""
    return worker_death(lane, "death-release-timeout", mode="release-then-ok", tries=3, timeout=3, kill=False,
                        runs=[1, 2, 3], deliveries=[1, 2, 3], failed_with="TimeoutExceededException")


def death_release_sigkill(lane: Lane) -> list[Check]:
    """tries 3, released once, then SIGKILL at each run: the fourth delivery exceeds tries."""
    return worker_death(lane, "death-release-sigkill", mode="release-then-ok", tries=3, timeout=9, kill=True,
                        runs=[1, 2, 3], deliveries=[1, 2, 3, 4], failed_with="MaxAttemptsExceededException")


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
# `bench:compat-parity`: how Laravel counts attempts, compared attempt by attempt with Horizon.
COMPAT_PARITY_SCENARIOS = ("release-attempts", "retry-until-precedence", "overlap-attempts", "rate-limited-attempts")
# `bench:compat-routed`: the routed default connection; only on a routed lane.
COMPAT_ROUTED_SCENARIOS = (
    "route-push", "route-later", "route-bulk", "route-batch", "route-chain", "route-after-commit", "route-pop",
)
# The compatibility scenarios held to the Horizon lane: attempts and release(), retryUntil,
# maxExceptions, timeouts, WithoutOverlapping and RateLimited, unique jobs, batches and their
# callbacks, chains, failed() and failed-job rows, also of a queued closure. None empties the
# failed-job store, and none leaves a worker dying.
PARITY_RUNS = (
    *(("bench:compat", name) for name in (
        "chain", "chain-failure", "batch", "batch-failure", "unique", "without-overlapping", "rate-limited",
        "backoff-array", "retry-until", "max-exceptions", "fail-on-timeout", "after-commit", "events",
        "failed-commands")),
    *(("bench:compat-parity", name) for name in COMPAT_PARITY_SCENARIOS),
    *(("bench:compat-more", name) for name in (
        "queued-closure", "unique-until-processing", "batch-allow-failures", "release-delay")),
)
ROUTED_RUNS = (*(("bench:compat-routed", name) for name in COMPAT_ROUTED_SCENARIOS), *PARITY_RUNS)


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


def run_compat(lane: Lane, runs: tuple[tuple[str, str], ...] | list[tuple[str, str]]) -> list[Check]:
    """Compatibility scenarios, each run by its artisan command inside the supervisor's
    container, beside its workers. Each one's checks join the lane's; its outcome is
    kept for parity()."""
    checks: list[Check] = []
    for command, name in runs:
        if lane.only and name not in lane.only:
            continue
        result = lane.app_artisan(command, name, f"--run-id={lane.run_id}-{name}", check=False)
        lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
        try:
            data = json.loads(lines[-1])
        except (IndexError, json.JSONDecodeError):
            checks.append(Check(f"{name}: ran", False, (result.stderr or result.stdout).strip()[-300:]))
            lane.note(f"{name}: no result")
            continue
        lane.extra[name] = data
        lane.outcome[name] = data.get("outcome") or {}
        checks.extend(Check(f"{name}: {c['name']}", c["passed"], c["detail"]) for c in data["checks"])
        lane.note(f"{name}: {'pass' if data['passed'] else 'FAIL'}")
    return checks


def laravel_compat(lane: Lane) -> list[Check]:
    """Every Laravel queue feature of `bench:compat`, `bench:compat-parity` and `bench:compat-more`."""
    return run_compat(lane, [
        *(("bench:compat", name) for name in COMPAT_SCENARIOS),
        *(("bench:compat-parity", name) for name in COMPAT_PARITY_SCENARIOS),
        *(("bench:compat-more", name) for name in COMPAT_MORE_SCENARIOS),
    ])


def laravel_parity(lane: Lane) -> list[Check]:
    """The compatibility scenarios that parity() holds to Horizon (PARITY_RUNS)."""
    return run_compat(lane, PARITY_RUNS)


ROUTED_POP_ERROR = "The routed queue connection only dispatches"


def routed_parity(lane: Lane) -> list[Check]:
    """queue.default only dispatches, to one connection per pool. The routing
    scenarios, then PARITY_RUNS, all dispatched through it. A worker that popped from the
    router would log its LogicException, and the supervisor would restart it."""
    checks = run_compat(lane, ROUTED_RUNS)
    logs = lane.compose("logs", "--no-color", lane.profile.engine, check=False, timeout=120).stdout
    pops = logs.count(ROUTED_POP_ERROR)
    lane.outcome["routed-logs"] = {"same": {"workers that popped from the routed connection": pops}, "near": {}}
    checks.append(Check("no worker popped from the routed connection", pops == 0, f"{pops} LogicExceptions in the logs"))
    return checks


def queue_restart(lane: Lane) -> list[Check]:
    """A deploy that runs only `php artisan queue:restart`, as Forge and Envoyer do. Every worker
    stops after its job and is replaced. With prefork the replacements must come from a fork server
    booted after the signal; from the one booted before it, they would keep its code. The deploy
    changes DeployedCode in the supervisor's container first: the jobs dispatched after the restart
    must run the new value, and a job that started before the deploy the old one, which shows the
    value is the one loaded at boot."""
    first, later = ids(0, 4), ids(4, 4)
    deployed = f"deployed-{lane.run_id[-8:]}"
    old_workers = set(lane.workers())
    old_servers = {pid for pid, _, args in lane.processes() if "queen:fork-server" in args}
    lane.dispatch("ok", 4, sleep_ms=3_000, tries=1)
    early = lane.wait_until(lambda r: any(r.count(j, "started") for j in first), 60, "a job started")
    started_early = [j for j in first if early.count(j, "started")]
    lane.docker("exec", lane.container(lane.profile.engine), "sed", "-i", f"s/VERSION = 'build'/VERSION = '{deployed}'/",
                "app/Support/DeployedCode.php")
    lane.note(f"deployed {deployed}")
    lane.artisan("queue:restart")
    lane.note("queue:restart")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        current = set(lane.workers())
        if len(current) >= int(lane.env["BENCH_WORKERS"]) and not current & old_workers:
            break
        time.sleep(1)
    lane.note("workers replaced" if not set(lane.workers()) & old_workers else "workers NOT replaced")
    lane.dispatch("ok", 4, first=4, sleep_ms=100, tries=1)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in first + later), 120, "all completed")
    jobs = lane.settle(5)
    after = lane.processes()
    needle = "horizon:work" if lane.profile.engine == "horizon" else "artisan queue:work"
    workers = {pid: ppid for pid, ppid, args in after if needle in args}
    servers = {pid for pid, _, args in after if "queen:fork-server" in args}
    lane.extra["queue_restart"] = {"old_workers": sorted(old_workers), "old_fork_servers": sorted(old_servers),
                                   "workers": workers, "fork_servers": sorted(servers)}
    replaced = bool(workers) and not set(workers) & old_workers
    codes = {j: jobs.codes(j) for j in first + later}
    lane.extra["queue_restart"]["codes"] = codes
    new_code = all(codes[j] == [deployed] for j in later)
    lane.outcome["queue-restart"] = {"same": {
        **{f"job {j}": jobs.outcome(j) for j in first + later},
        "every worker was replaced": replaced,
        "the jobs after the restart ran the deployed code": new_code,
    }, "near": {}}
    checks = [
        Check("every worker was replaced", replaced, f"before {sorted(old_workers)}, after {sorted(workers)}"),
        Check("the jobs after the restart ran the deployed code", new_code, f"{codes}"),
        Check("a job started before the deploy ran the code its worker booted with",
              bool(started_early) and all(codes[j] == ["build"] for j in started_early),
              f"started before the deploy: {started_early}; {codes}"),
        *completed_once(jobs, first + later),
    ]
    if old_servers:
        parents = set(workers.values())
        checks += [
            Check("the new workers come from a fork server booted after the signal",
                  bool(parents) and parents <= servers and not parents & old_servers,
                  f"fork servers before {sorted(old_servers)}, after {sorted(servers)}, worker parents {sorted(parents)}"),
            Check("the old fork server exited with its last worker", not old_servers & servers,
                  f"still running: {sorted(old_servers & servers)}"),
        ]
    return checks


def issue_codes(status: dict, key: str) -> list[str]:
    """The codes of a status's readiness_issues or processing_health_issues."""
    issues = status.get(key)
    return sorted({issue.get("code", "?") for issue in issues if isinstance(issue, dict)}) if isinstance(issues, list) else []


def not_consuming_seconds(status: dict) -> int | None:
    """How long the worker of any pool that has failed its pops the longest has done so."""
    pools = status.get("pool_status")
    seconds = [pool.get("not_consuming_seconds") for pool in pools if isinstance(pool, dict)] if isinstance(pools, list) else []
    return max((s for s in seconds if isinstance(s, int)), default=None)


# Longer than a pop that hangs through its timeout and retries (QUEEN_TIMEOUT_MS 30 s, 3
# attempts) and the 60 s after which every worker that failed its pops is not consuming.
PROBE_OUTAGE_SECONDS = 150


def probe_broker_hung(lane: Lane) -> list[Check]:
    """The broker hangs, as a node whose disk stalls does: `docker pause`, so connections open
    and nothing answers, for longer than every timeout of a pop. Kubernetes runs the probes
    of docs/guides/laravel/kubernetes. The liveness probe must pass throughout, or every pod
    restarts for an outage a restart cannot fix; the readiness probe must fail while no
    worker consumes and pass again once the broker is back; and the same workers must
    consume again, with no restart."""
    ready_before, _ = lane.status()
    master = lane.master()
    workers_before = set(lane.workers())
    broker = lane.container("broker")
    lane.docker("pause", broker)
    lane.note(f"broker paused for {PROBE_OUTAGE_SECONDS} s")
    samples: list[dict] = []
    paused = time.monotonic()
    try:
        while (elapsed := time.monotonic() - paused) < PROBE_OUTAGE_SECONDS:
            ready, status = lane.status()
            live = lane.probe("--check-liveness")[0] == 0
            samples.append({"t": round(elapsed, 1), "ready": ready, "live": live,
                            "readiness": issue_codes(status, "readiness_issues"),
                            "capacity": issue_codes(status, "processing_health_issues"),
                            "not_consuming_seconds": not_consuming_seconds(status)})
            time.sleep(3)
    finally:
        lane.docker("unpause", broker)
    lane.note("broker resumed")
    resumed, recovered = time.monotonic(), None
    while time.monotonic() - resumed < 120:
        if lane.status()[0]:
            recovered = round(time.monotonic() - resumed, 1)
            break
        time.sleep(2)
    lane.note(f"ready again after {recovered} s" if recovered is not None else "NOT ready again")
    expected = ids(0, 4)
    lane.dispatch("ok", 4, sleep_ms=100, tries=1)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 120, "all completed")
    jobs = lane.settle(5)
    workers_after = set(lane.workers())
    lane.extra["probe"] = {"samples": samples, "recovered_after": recovered,
                           "workers_before": sorted(workers_before), "workers_after": sorted(workers_after)}
    not_live = [s["t"] for s in samples if not s["live"]]
    first_not_ready = next((s["t"] for s in samples if not s["ready"]), None)
    reported = next((s["t"] for s in samples if "pool_not_consuming" in s["readiness"]), None)
    pids = [pid for pid, _, args in lane.processes() if pid == master]
    return [
        Check("ready before the outage", ready_before),
        Check("the liveness probe passed throughout the outage", not not_live, f"failed at {not_live[:5]} s"),
        Check("the readiness probe failed during the outage", first_not_ready is not None,
              f"first failure at {first_not_ready} s"),
        Check("the status said no pool consumes", reported is not None,
              f"pool_not_consuming first at {reported} s; last sample {samples[-1] if samples else None}"),
        Check("ready again within 60 s of the broker's return", recovered is not None and recovered <= 60,
              f"{recovered} s"),
        Check("the master survived", bool(pids), f"master {master}"),
        Check("the workers that waited out the outage consume again", workers_after == workers_before,
              f"before {sorted(workers_before)}, after {sorted(workers_after)}"),
        *completed_once(jobs, expected),
    ]


POISON_KINDS = ("not-json", "no-job", "missing-class")


def poison_messages(lane: Lane) -> list[Check]:
    """Three messages no worker can run, ahead of a job in one partition: a string that is not
    JSON and an object that names no job, as another producer of the queue may push, and a
    Laravel job whose class a deploy removed. A lease expiry never charges the broker's
    retry budget, so a message that is neither ACKed nor failed holds its partition forever.
    Each must go to the dead-letter queue at its first delivery, without a worker dying, and
    the job behind them must run without waiting for a lease."""
    partition = "matrix-poison"
    workers_before = set(lane.workers())
    for kind in POISON_KINDS:
        lane.artisan("bench:matrix-raw", f"--run-id={lane.run_id}", f"--kind={kind}", f"--partition={partition}")
    lane.note(f"pushed {', '.join(POISON_KINDS)}")
    dispatched = time.monotonic()
    lane.dispatch("ok", 1, tries=1, partition=partition)
    jobs = lane.wait_until(lambda r: r.count("000000", "completed") >= 1, 150, "the job behind them completed")
    waited = round(time.monotonic() - dispatched, 1)
    jobs = lane.settle(5)
    lease = int(lane.env["BENCH_RETRY_AFTER"])
    ready, status = lane.status()
    lane.extra["poison"] = {"waited": waited, "dead_letter": jobs.dead_letter(), "ready": ready,
                            "readiness": issue_codes(status, "readiness_issues")}
    return [
        Check("the job behind them completed once", jobs.count("000000", "completed") == 1, f"{jobs.events('000000')}"),
        Check(f"without waiting for a lease ({lease} s)", waited < lease, f"{waited} s"),
        Check(f"the {len(POISON_KINDS)} reached the dead-letter queue", jobs.dead_letter() == len(POISON_KINDS),
              f"{jobs.dead_letter()} entries"),
        Check("no worker died", set(lane.workers()) == workers_before,
              f"before {sorted(workers_before)}, after {sorted(lane.workers())}"),
        Check("still ready", ready, f"{issue_codes(status, 'readiness_issues')}"),
    ]


def string_timeout(lane: Lane) -> list[Check]:
    """A job whose $timeout is a numeric string, as `$this->timeout = env('JOB_TIMEOUT')` leaves
    it, ahead of two jobs in one partition. Laravel's worker accepts the string. The job must
    run as any other, the jobs behind it too, and no worker may leave for it: one that did
    would leave the job leased, and the next worker would pop it and leave in turn."""
    partition = "matrix-string-timeout"
    workers_before = set(lane.workers())
    lane.dispatch("string-timeout", 1, timeout=20, partition=partition)
    lane.dispatch("ok", 2, first=1, tries=1, partition=partition)
    expected = ids(0, 3)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in expected), 120, "all completed")
    jobs = lane.settle(5)
    workers_after = set(lane.workers())
    lane.outcome["string-timeout"] = {"same": {f"job {j}": jobs.outcome(j) for j in expected}, "near": {}}
    return [
        *completed_once(jobs, expected),
        every(jobs, expected, "each ran once", lambda j: jobs.count(j, "started") == 1),
        Check("no worker left", workers_after == workers_before,
              f"before {sorted(workers_before)}, after {sorted(workers_after)}"),
    ]


def binary_failure(lane: Lane) -> list[Check]:
    """Two jobs, tries 1, that throw an exception whose message is not UTF-8, as a database
    error quoting a latin-1 value does. Each must fail once, with that exception: failed()
    once, on Queen one dead-letter entry. An ACK that could not carry the message would leave
    the job leased, and its redelivery would fail it a second time, past its tries."""
    expected = ids(0, 2)
    lane.dispatch("throw-binary", 2, tries=1)
    jobs = lane.wait_until(lambda r: all(r.count(j, "failed_hook") for j in expected), 120, "all failed")
    # Long enough for a lease the ACK left behind to expire and the job to come back.
    jobs = lane.settle(int(lane.env["BENCH_RETRY_AFTER"]) + 5)
    lane.extra["binary_failure"] = {"failed_store": jobs.failed_store(), "dead_letter": jobs.dead_letter()}
    lane.outcome["binary-failure"] = {"same": {f"job {j}": {k: v for k, v in jobs.outcome(j).items() if k != "failed_row"}
                                               for j in expected}, "near": {}}
    checks = [
        every(jobs, expected, "ran once", lambda j: jobs.count(j, "started") == 1),
        every(jobs, expected, "failed() ran once", lambda j: jobs.count(j, "failed_hook") == 1),
        every(jobs, expected, "failed with the job's own exception",
              lambda j: str(jobs.failed_with(j)).endswith("RuntimeException")),
        every(jobs, expected, "never completed", lambda j: jobs.count(j, "completed") == 0),
    ]
    if lane.profile.connection == "queen":
        checks.append(Check("one dead-letter entry each", jobs.dead_letter() == len(expected),
                            f"{jobs.dead_letter()} entries"))
    return checks


INSTALL_PATH = "/opt/queen-supervisor-bin"
APP_USER = "benchmark"


def process_users(lane: Lane) -> dict[int, tuple[int, str]]:
    """Every process of the supervisor's container: its uid and arguments, by pid."""
    rows = lane.docker("exec", lane.container(lane.profile.engine), "ps", "-eo", "pid=,uid=,args=", check=False)
    users = {}
    for line in rows.stdout.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
            users[int(parts[0])] = (int(parts[1]), parts[2])
    return users


def install_owner(lane: Lane) -> list[Check]:
    """An image that installs the supervisor as root for the user that runs it (`--owner`), and
    a container that starts as root and runs the Composer launcher as that user, as
    docs/guides/laravel/kubernetes describes. Every file of the installation must be the
    user's; the master and its workers must run as the user; a probe run as root, the
    container's user, must fail with a message that names the owner and the fix, not crash;
    the probe wrapped in su must pass; and a SIGTERM to the container must reach the master
    through the launcher, which replaced itself, and drain the job in flight."""
    app = lane.container(lane.profile.engine)
    owners = lane.docker("exec", app, "find", INSTALL_PATH, "-printf", "%u %p\\n", check=False).stdout.split("\n")
    not_owned = [line for line in owners if line and not line.startswith(f"{APP_USER} ")]
    users = process_users(lane)
    master = lane.master()
    workers = lane.workers()
    master_uid, master_args = users.get(master, (-1, ""))
    worker_uids = sorted({users.get(pid, (-1, ""))[0] for pid in workers})

    as_root = lane.probe("--check")
    as_owner = lane.probe("--check", user="1000:1000")
    wrapped = lane.docker("exec", app, "su", "-s", "/bin/sh", APP_USER, "-c",
                          "php artisan --no-ansi queen:supervisor status --check", check=False)
    lane.extra["install_owner"] = {"not_owned": not_owned[:10], "master": [master, master_uid, master_args],
                                   "worker_uids": worker_uids, "as_root": as_root,
                                   "as_owner": as_owner[0], "wrapped": wrapped.returncode}

    lane.dispatch("ok", 1, sleep_ms=8_000, timeout=30, tries=3)
    lane.wait_until(lambda r: r.count("000000", "started") >= 1, 60, "the job started")
    lane.docker("stop", "--time", "60", app)
    lane.note("container stopped (SIGTERM, 60 s grace)")
    exit_code = lane.docker("inspect", "--format", "{{.State.ExitCode}}", app, check=False).stdout.strip()
    jobs = lane.settle(2)

    def refusal(output: str) -> bool:
        return f"uid 1000 ({APP_USER})" in output and f"su -s /bin/sh {APP_USER}" in output and "runAsUser" in output

    return [
        Check("the installation is the user's, every directory and file", bool(owners) and not not_owned,
              f"{not_owned[:5]}"),
        Check("the master runs the installed binary as the user", master_uid == 1000 and "queen-supervisor" in master_args,
              f"uid {master_uid}: {master_args[:120]}"),
        Check("the workers run as the user", bool(workers) and worker_uids == [1000], f"uids {worker_uids}"),
        Check("the probe as the user passes", as_owner[0] == 0, as_owner[1][-300:]),
        Check("the probe wrapped in su passes, as root", wrapped.returncode == 0, (wrapped.stdout + wrapped.stderr)[-300:]),
        Check("the probe as root fails, naming the owner, su and runAsUser",
              as_root[0] == 1 and refusal(as_root[1]), f"exit {as_root[0]}: {as_root[1][-400:]}"),
        Check("SIGTERM drained the job in flight and the supervisor exited 0",
              exit_code == "0" and jobs.count("000000", "completed") == 1 and jobs.count("000000", "started") == 1,
              f"exit {exit_code}, {jobs.events('000000')}"),
    ]


METRICS_CLASS = "App\\Jobs\\FailureMatrixJob"


def job_metrics_totals(lane: Lane) -> dict:
    """The FailureMatrixJob row of the dashboard's Jobs page, or {} when there is none."""
    result = lane.artisan("bench:job-metrics", check=False)
    line = next((line for line in result.stdout.splitlines() if line.startswith("{")), "")
    try:
        read = json.loads(line) if line else {}
    except json.JSONDecodeError:
        return {}
    return next((row for row in read.get("classes") or [] if row.get("class") == METRICS_CLASS), {})


def wait_for_metrics(lane: Lane, expected: dict[str, int], timeout: float = 60) -> dict:
    """The Jobs page's row once it shows `expected`, or as it stands at the timeout. A worker
    writes its counts at most every ten seconds, while it waits for a job or when it stops."""
    deadline, row = time.monotonic() + timeout, job_metrics_totals(lane)
    while time.monotonic() < deadline and {key: row.get(key) for key in expected} != expected:
        time.sleep(3)
        row = job_metrics_totals(lane)
    lane.note(f"metrics {row}")
    return row


def job_metrics(lane: Lane) -> list[Check]:
    """The Jobs page after every way an attempt ends, as docs/guides/laravel/monitoring states
    (Jobs per class): returned and released attempts are processed; an attempt that throws,
    calls fail() or outlives its timeout is a failed attempt. Then a worker killed outright: its
    attempt records nothing, and its redelivery, past its tries, is one failed attempt more. The
    kill comes once every count before it is written: a killed worker loses the counts it has
    not written yet, up to ten seconds of them, as documented."""
    lane.dispatch("ok", 4, tries=1)                                        # 4 processed
    lane.dispatch("throw", 2, first=4, tries=2)                            # 2 x 2 failed
    lane.dispatch("fail", 2, first=6, tries=3)                             # 2 failed
    lane.dispatch("release-once", 2, first=8, tries=3)                     # 2 x 2 processed
    lane.dispatch("ok", 1, first=10, sleep_ms=15_000, tries=1, timeout=3)  # 1 failed, the worker killed
    succeeded, failed = ids(0, 4) + ids(8, 2), ids(4, 4) + ids(10, 1)
    jobs = lane.wait_until(lambda r: all(r.count(j, "completed") for j in succeeded)
                           and all(r.count(j, "failed_hook") for j in failed), 180, "every job ended")
    before_kill = wait_for_metrics(lane, {"processed": 8, "failed": 7})
    lane.dispatch("ok", 1, first=11, sleep_ms=30_000, tries=1, timeout=40)
    victim = lane.wait_until(lambda r: r.count("000011", "started") >= 1, 60, "the job to kill started")
    run = run_in_progress(victim, "000011")
    if run is not None:
        lane.docker("exec", lane.container(lane.profile.engine), "kill", "-KILL", str(run[1]), check=False)
        lane.note(f"SIGKILL worker {run[1]}")
    jobs = lane.wait_until(lambda r: r.count("000011", "failed_hook") >= 1, 120, "the killed job failed")
    after_kill = wait_for_metrics(lane, {"processed": 8, "failed": 8})
    lane.extra["job_metrics"] = {"before_kill": before_kill, "after_kill": after_kill}
    return [
        Check("every job ended", all(jobs.count(j, "completed") for j in succeeded)
              and all(jobs.count(j, "failed_hook") for j in failed + ["000011"]), ""),
        Check("8 processed attempts", before_kill.get("processed") == 8, f"{before_kill}"),
        Check("7 failed attempts: throws, fail() and the timeout", before_kill.get("failed") == 7, f"{before_kill}"),
        Check("the longest attempt is the timed-out one, about 3 s", 2_500 <= (before_kill.get("max_ms") or 0) < 10_000,
              f"max_ms {before_kill.get('max_ms')}"),
        Check("the worker running the job to kill was killed", run is not None, f"{run}"),
        Check("the killed job's redelivery, past its tries, is one failed attempt more",
              after_kill.get("failed") == 8 and after_kill.get("processed") == 8, f"{after_kill}"),
    ]


@dataclass(frozen=True)
class Scenario:
    name: str
    run: Callable[[Lane], list[Check]]
    env: dict[str, str] = field(default_factory=dict)
    replicas: int = 1
    prepare: Callable[[Lane], None] | None = None
    # Settings for one engine only, over `env`: where Horizon needs another setting to run the
    # same job safely, such as a retry_after longer than a job it cannot renew.
    engine_env: dict[str, dict[str, str]] = field(default_factory=dict)
    # The engines the scenario runs on; empty for every one. A Queen feature that Horizon does not
    # have, such as the readiness probe, has no Horizon lane.
    engines: tuple[str, ...] = ()

    def env_for(self, profile: Profile) -> dict[str, str]:
        return {**self.env, **self.engine_env.get(profile.engine, {})}

    def runs_on(self, profile: Profile) -> bool:
        return not self.engines or profile.engine in self.engines


# The compatibility lanes share a cache and a database across the container's processes.
COMPAT_ENV = {"BENCH_CACHE_STORE": "database", "BENCH_DB_DATABASE": COMPAT_DATABASE}
QUEEN_ENGINES = ("queen-php", "queen-rust")
THREE_WORKERS = {"BENCH_WORKERS": "3", "BENCH_MIN_WORKERS": "3", "BENCH_MAX_WORKERS": "3"}


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
    # queue:work reads the restart signal from the cache, which every process must share.
    Scenario("queue-restart", queue_restart, COMPAT_ENV, prepare=compat_database),
    Scenario("laravel-compat", laravel_compat, {**COMPAT_ENV, **THREE_WORKERS}, prepare=compat_database),
    # One worker per replica, so the job's replica can be stopped while the other waits idle.
    # A pool timeout of 25 s gives the Queen supervisor a shutdown_grace of 40 s.
    Scenario("stop-lease", stop_lease, {
        "BENCH_QUEEN_COORDINATION": "true", "BENCH_TIMEOUT": "25", "BENCH_RETRY_AFTER": "30",
        "BENCH_WORKERS": "1", "BENCH_MIN_WORKERS": "1", "BENCH_MAX_WORKERS": "1",
    }, replicas=2, engine_env={"horizon": {"BENCH_TIMEOUT": "60", "BENCH_RETRY_AFTER": "90"}}),
    Scenario("death-timeout", death_timeout, DEATH_ENV),
    Scenario("death-sigkill", death_sigkill, DEATH_ENV),
    Scenario("death-release-timeout", death_release_timeout, DEATH_ENV),
    Scenario("death-release-sigkill", death_release_sigkill, DEATH_ENV),
    Scenario("laravel-parity", laravel_parity, {**COMPAT_ENV, **THREE_WORKERS}, prepare=compat_database),
    # Every dispatch through the routed default connection to the pools'.
    Scenario("routed-parity", routed_parity, {**COMPAT_ENV, **THREE_WORKERS, "BENCH_ROUTED": "true"},
             prepare=compat_database),
    # Job settings and failures Laravel accepts, which a Queen worker must survive as Horizon's does.
    Scenario("string-timeout", string_timeout),
    Scenario("binary-failure", binary_failure, DEATH_ENV),
    # What Horizon has no counterpart of: the probes, the dead-letter queue, the Jobs page.
    Scenario("probe-broker-hung", probe_broker_hung, engines=QUEEN_ENGINES),
    Scenario("poison-messages", poison_messages, engines=QUEEN_ENGINES),
    Scenario("job-metrics", job_metrics, engines=QUEEN_ENGINES),
    Scenario("install-owner", install_owner, engines=("queen-installed",)),
]
# The scenarios that record an outcome for parity(), besides the compatibility runs.
PARITY_SCENARIOS = ("job-timeout", "memory-limit", "stop-short", "stop-lease", "queue-restart",
                    "death-timeout", "death-sigkill", "death-release-timeout", "death-release-sigkill",
                    "laravel-parity", "routed-parity", "string-timeout", "binary-failure")


def scenario_profiles(scenario: Scenario, names: list[str], prefork: list[str]) -> list[Profile]:
    """The lanes of a scenario: the selected profiles it runs on. A scenario of an engine that no
    selected profile has, such as queen-installed, runs on its own engine's profile."""
    profiles = [profile for profile in lane_profiles(names, prefork) if scenario.runs_on(profile)]
    return profiles or lane_profiles([name for name in scenario.engines if name in PROFILES], prefork)


def run_lane(scenario: Scenario, profile: Profile, output: Path, only: frozenset[str] = frozenset(),
             stack: str = "default") -> dict:
    lane = Lane(scenario.name, profile, {**scenario.env_for(profile), **stack_env(stack, profile)}, output, only)
    print(f"\n== {scenario.name} / {profile.name} ({lane.project})", flush=True)
    result: dict = {"scenario": scenario.name, "profile": profile.name, "run_id": lane.run_id,
                    "settings": {key: lane.env[key] for key in sorted(LANE_SETTINGS) if key in lane.env},
                    "stack": {"name": stack, "image": APP_IMAGE}}
    try:
        lane.up(scenario.replicas, scenario.prepare)
        supervisor = "" if profile.engine not in ("queen-rust", "queen-installed") else lane.docker(
            "exec", lane.container(profile.engine), "queen-supervisor", "--version", check=False).stdout
        result["stack"].update(stack_versions(lane.artisan("bench:config").stdout, supervisor))
        checks = scenario.run(lane)
        result["checks"] = [check.__dict__ for check in checks]
        result["passed"] = all(check.passed for check in checks)
        result["outcome"] = lane.outcome
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


# The settings a lane's result records, to tell its prefork mode, lease and layout apart.
LANE_SETTINGS = ("BENCH_QUEEN_PREFORK", "BENCH_ROUTED", "BENCH_RETRY_AFTER", "BENCH_TIMEOUT", "BENCH_WORKERS",
                 "QUEEN_PREFETCH", "BENCH_QUEEN_LEASE_SERVICE", "BENCH_QUEEN_COORDINATION", "BENCH_PROFILE",
                 "BENCH_QUEUES", "BENCH_MIN_WORKERS", "BENCH_MAX_WORKERS", "BENCH_ROUTED_BALANCE", "BENCH_OPCACHE_CLI")


def as_map(value: object) -> dict:
    """PHP encodes an empty map as a list."""
    return value if isinstance(value, dict) else {}


def compare_outcomes(reference: dict, candidate: dict) -> tuple[list[str], list[str]]:
    """How a lane's outcome differs from the Horizon lane's: values of `same` must be equal,
    times of `near` within the larger tolerance. Checks are compared where both lanes ran
    them; a check that only one backend has (a Queen partition, say) is a note, not a
    divergence. Returns the divergences and the notes."""
    divergences: list[str] = []
    notes: list[str] = []
    ours, theirs = as_map(reference.get("same")), as_map(candidate.get("same"))
    for key in sorted(set(ours) | set(theirs)):
        if key == "checks":
            mine, other = as_map(ours.get(key)), as_map(theirs.get(key))
            for name in sorted(set(mine) & set(other)):
                if mine[name] != other[name]:
                    divergences.append(f"check '{name}': Horizon {'passes' if mine[name] else 'fails'}, "
                                       f"this lane {'passes' if other[name] else 'fails'}")
            if set(mine) ^ set(other):
                notes.append("checked on one side only: " + "; ".join(sorted(set(mine) ^ set(other))))
        elif key not in ours or key not in theirs:
            divergences.append(f"{key}: {'not recorded on Horizon' if key not in ours else 'not recorded here'}")
        elif ours[key] != theirs[key]:
            divergences.append(f"{key}: Horizon {json.dumps(ours[key])}, this lane {json.dumps(theirs[key])}")
    ours, theirs = as_map(reference.get("near")), as_map(candidate.get("near"))
    for key in sorted(set(ours) | set(theirs)):
        if key not in ours or key not in theirs:
            divergences.append(f"{key}: {'not recorded on Horizon' if key not in ours else 'not recorded here'}")
            continue
        tolerance = max(float(ours[key]["tolerance"]), float(theirs[key]["tolerance"]))
        if abs(float(ours[key]["value"]) - float(theirs[key]["value"])) > tolerance:
            divergences.append(f"{key}: Horizon {ours[key]['value']} s, this lane {theirs[key]['value']} s "
                               f"(tolerance {tolerance} s)")
    return divergences, notes


def parity(results: list[dict]) -> list[dict]:
    """Every outcome item of every lane, against the same item of the Horizon lane of its
    scenario. A lane that ended in an error has no outcome: its items read `missing`."""
    rows = []
    for scenario in dict.fromkeys(r["scenario"] for r in results):
        lanes = [r for r in results if r["scenario"] == scenario]
        reference = next((r for r in lanes if r["profile"] == REFERENCE), None)
        for lane in lanes:
            if lane is reference:
                continue
            items = as_map(lane.get("outcome"))
            expected = as_map(reference.get("outcome")) if reference else {}
            for item in sorted(set(items) | set(expected)):
                row = {"scenario": scenario, "item": item, "profile": lane["profile"], "divergences": [], "notes": []}
                if reference is None:
                    row["status"] = "no Horizon lane"
                elif item not in expected or item not in items:
                    row["status"] = "missing"
                    row["divergences"] = [f"not recorded {'on Horizon' if item not in expected else 'here'}"]
                else:
                    row["divergences"], row["notes"] = compare_outcomes(expected[item], items[item])
                    row["status"] = "differs" if row["divergences"] else "same"
                rows.append(row)
    return rows


def parity_markdown(rows: list[dict]) -> str:
    lines = ["| Scenario | Item | Profile | Parity with Horizon | Divergences |", "| --- | --- | --- | --- | --- |"]
    for row in rows:
        detail = "; ".join(row["divergences"]).replace("|", "\\|")
        lines.append(f"| {row['scenario']} | {row['item']} | {row['profile']} | {row['status']} | {detail} |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scenarios", default=",".join(s.name for s in SCENARIOS),
                        help="comma-separated scenario names, or `parity` for the scenarios held to Horizon")
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    parser.add_argument("--prefork", default="on",
                        help="on, off or on,off: the Queen profiles' prefork modes; with off a profile is "
                             "named <profile>-prefork-off (default: on)")
    parser.add_argument("--only", default="",
                        help="run only these compatibility scenarios of a compatibility lane, by name")
    parser.add_argument("--stack", default="default", choices=sorted(STACKS),
                        help="settings laid over every lane: balanced pools balance by backlog over several queues, with the CLI opcache on (default: default)")
    parser.add_argument("--build", action="store_true", help="rebuild the application image first")
    args = parser.parse_args()

    wanted = list(PARITY_SCENARIOS) if args.scenarios == "parity" else args.scenarios.split(",")
    prefork = args.prefork.split(",")
    only = frozenset(filter(None, args.only.split(",")))
    known_items = {*COMPAT_SCENARIOS, *COMPAT_MORE_SCENARIOS, *COMPAT_PARITY_SCENARIOS, *COMPAT_ROUTED_SCENARIOS}
    unknown = (set(wanted) - {s.name for s in SCENARIOS} | set(args.profiles.split(",")) - set(PROFILES)
               | set(prefork) - set(PREFORK_MODES) | only - known_items)
    if unknown:
        parser.error(f"unknown scenario, profile, prefork mode or compatibility scenario: {', '.join(sorted(unknown))}")
    if args.build:
        subprocess.run(["docker", "compose", "--file", str(COMPOSE_FILE), "--profile", "tools", "build", "producer"],
                       check=True)
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    rows: list[dict] = []
    for scenario in (s for s in SCENARIOS if s.name in wanted):
        for profile in scenario_profiles(scenario, args.profiles.split(","), prefork):
            results.append(run_lane(scenario, profile, args.output, only, args.stack))
            (args.output / "summary.md").write_text(summary(results), encoding="utf-8")
            rows = parity(results)
            (args.output / "parity.md").write_text(parity_markdown(rows), encoding="utf-8")
            (args.output / "parity.json").write_text(json.dumps(rows, indent=1), encoding="utf-8")
    print("\n" + summary(results))
    differing = [row for row in rows if row["status"] in ("differs", "missing")]
    if rows:
        statuses = {status: sum(1 for row in rows if row["status"] == status) for status in dict.fromkeys(
            row["status"] for row in rows)}
        print(f"Parity with Horizon, {len(rows)} items: " + ", ".join(f"{n} {s}" for s, n in statuses.items()))
        for row in differing:
            print(f"  {row['scenario']} / {row['item']} / {row['profile']}: {'; '.join(row['divergences'])}")
    return 0 if all(result["passed"] for result in results) and not differing else 1


if __name__ == "__main__":
    sys.exit(main())
