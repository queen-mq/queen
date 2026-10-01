#!/usr/bin/env python3
"""The expected-failure matrix: Horizon and the Queen supervisor under every
failure they are meant to survive, each checked for lost jobs, duplicate
completions and where final failures land.

Usage:
  failure_matrix.py --output DIR [--scenarios a,b] [--profiles horizon,queen,queen-fast] [--build]

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
APP_IMAGE = "queen-laravel-supervisor-bench:local"


@dataclass(frozen=True)
class Profile:
    name: str
    engine: str
    connection: str
    env: dict[str, str]


PROFILES = {
    "horizon": Profile("horizon", "horizon", "redis", {}),
    # Close to a production Laravel deployment: one job per pop, renewal by the master.
    "queen": Profile("queen", "queen-rust", "queen", {
        "QUEEN_PREFETCH": "1",
        "BENCH_QUEEN_ACK_ASYNC": "false",
        "BENCH_QUEEN_POP_AHEAD": "false",
    }),
    # Every throughput option on.
    "queen-fast": Profile("queen-fast", "queen-rust", "queen", {
        "QUEEN_PREFETCH": "4",
        "BENCH_QUEEN_ACK_ASYNC": "true",
        "BENCH_QUEEN_POP_AHEAD": "true",
    }),
}


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

    def container(self, service: str) -> str:
        return self.compose("ps", "--all", "--quiet", service).stdout.strip()

    def docker(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["docker", *args], check=check, capture_output=True, text=True, timeout=600)

    def artisan(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.compose("exec", "--no-TTY", "producer", "php", "artisan", "--no-ansi", *args, check=check)

    # ---------------------------------------------------------------- lifecycle

    def up(self) -> None:
        self.docker("volume", "create", self.volume)
        self.docker("run", "--rm", "--user", "0:0", "--mount", f"type=volume,src={self.volume},dst=/results",
                    APP_IMAGE, "sh", "-ceu", "chown 1000:1000 /results; chmod 0770 /results")
        self.compose("up", "--detach", "--no-build", self.profile.engine, "producer")
        self.wait_healthy()
        self.wait_workers(int(self.env["BENCH_WORKERS"]))
        self.note("ready")

    def restart_app(self) -> None:
        self.compose("up", "--detach", "--no-build", self.profile.engine)
        self.wait_healthy()
        self.note("app up again")

    def down(self) -> None:
        self.compose("down", "--volumes", "--remove-orphans", "--timeout", "5", check=False)
        self.docker("volume", "rm", "--force", self.volume, check=False)

    def wait_healthy(self, timeout: float = 180) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            app = self.container(self.profile.engine)
            if app:
                state = self.docker("inspect", "--format",
                                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                                    app, check=False).stdout.strip()
                if state == "healthy":
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
        needle = "artisan horizon" if self.profile.engine == "horizon" else "queen-supervisor"
        for pid, _, args in self.processes():
            if needle in args and "horizon:" not in args and pid != 1:
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
                 backoff: int = 0, timeout: int = 60, allocate_mib: int = 0,
                 check: bool = True) -> subprocess.CompletedProcess[str]:
        result = self.artisan(
            "bench:matrix-dispatch", f"--run-id={self.run_id}", f"--mode={mode}", f"--jobs={jobs}",
            f"--first={first}", f"--sleep-ms={sleep_ms}", f"--tries={tries}", f"--backoff={backoff}",
            f"--timeout={timeout}", f"--allocate-mib={allocate_mib}", check=check,
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


@dataclass(frozen=True)
class Scenario:
    name: str
    run: Callable[[Lane], list[Check]]
    env: dict[str, str] = field(default_factory=dict)


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
    Scenario("backend-restart", backend_restart),
    Scenario("pause-short", pause_short),
    Scenario("pause-long", pause_long),
    Scenario("dispatch-backend-down", dispatch_backend_down),
]


def run_lane(scenario: Scenario, profile: Profile, output: Path) -> dict:
    lane = Lane(scenario.name, profile, scenario.env, output)
    print(f"\n== {scenario.name} / {profile.name} ({lane.project})", flush=True)
    result: dict = {"scenario": scenario.name, "profile": profile.name, "run_id": lane.run_id}
    try:
        lane.up()
        checks = scenario.run(lane)
        result["checks"] = [check.__dict__ for check in checks]
        result["passed"] = all(check.passed for check in checks)
        result["report"] = lane.report().raw
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
    parser.add_argument("--profiles", default=",".join(PROFILES))
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
