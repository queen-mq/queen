#!/usr/bin/env python3
"""The failure, replica, soak and Laravel compatibility tables of this dataset.

Usage:
  tables.py [RAW_DIR]        (default: ../raw next to this script)

Reads the lane results that `laravel-supervisors/scripts/failure_matrix.py`
wrote under RAW_DIR and prints Markdown tables, one column per engine.

The first VM matrix ran before the profiles were renamed: its `queen` lanes
are the Rust supervisor with prefetch 1 (`queen-rust`), its `queen-fast` lanes
the Rust supervisor with prefetch 4, ack_async and pop_ahead
(`queen-rust-fast`). The tables use the new names.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

RENAMED = {"queen": "queen-rust", "queen-fast": "queen-rust-fast"}
ENGINES = ("horizon", "queen-php", "queen-rust", "queen-rust-fast")
ENGINE_TITLES = {
    "horizon": "Horizon",
    "queen-php": "Queen PHP",
    "queen-rust": "Queen Rust",
    "queen-rust-fast": "Queen Rust, prefetch 4",
}

# What each lane does to the jobs, and what a Laravel developer expects.
FAILURES = {
    "exception-retry": ("A job throws on every attempt (`tries` 3, `backoff` 2)",
                        "3 attempts at least 2 s apart, then one failed-job row"),
    "exception-once": ("A job throws on its first attempt only",
                       "It completes on its second attempt, once"),
    "release-and-fail": ("`release()` once, or `fail()`",
                         "A released job completes on its second attempt; `fail()` is final"),
    "job-timeout": ("A 15 s job with `timeout` 3 (`tries` 2)",
                    "The worker is killed and replaced at each attempt; one failed-job row"),
    "memory-limit": ("A job takes the worker over `--memory` (110 of 96 MiB)",
                     "The job completes once, then the worker is replaced"),
    "memory-fatal": ("A job exceeds PHP's `memory_limit` (256 of 128 MiB, `tries` 2)",
                     "The worker dies at each attempt; one failed-job row"),
    "worker-kill": ("SIGKILL on a worker during a 4 s job",
                    "The killed job runs again; all 8 complete once"),
    "master-kill": ("SIGKILL on the master during 4 s jobs, then a restart",
                    "All 8 jobs complete once"),
    "stop-short": ("A deploy (SIGTERM) during 10 s jobs",
                   "The jobs finish during the grace and do not run again"),
    "stop-long": ("A deploy (SIGTERM) during 60 s jobs, longer than the grace",
                  "The killed jobs run again; each completes once"),
    "stop-long-batch": ("One worker runs two 60 s jobs of one prefetched batch; a deploy kills it in the second",
                        "The first job, done before the deploy, does not run again"),
    "backend-restart": ("The backend restarts while 300 jobs of 100 ms run",
                        "All 300 complete once"),
    "pause-short": ("The backend freezes for 5 s during 10 s jobs",
                    "Nothing runs twice"),
    "pause-long": ("The backend freezes for 45 s, longer than the 30 s lease",
                   "All complete; a job may run twice (at-least-once)"),
    "dispatch-backend-down": ("`dispatch()` while the backend is stopped",
                              "`dispatch()` throws, and works again once the backend is back"),
}
REPLICAS = {
    "replicas-kill": ("Two masters on one queue; SIGKILL on one, as a node loss would",
                      "All 40 jobs complete once"),
    "replicas-rolling": ("A rolling restart of two masters while 60 jobs run",
                         "All 60 complete once"),
}
SOAK = {
    "soak": ("45 min of mixed jobs at 5/s; a worker killed every 10 min; a deploy halfway",
             "Every job ends as its kind says; memory stays flat"),
}
# What each `bench:compat` scenario checks, in Laravel's words.
COMPAT_NAMES = {
    "delay": "`delay()`",
    "chain": "`Bus::chain()`",
    "chain-failure": "a failing chain runs its `catch()`",
    "batch": "`Bus::batch()` with `then()`, `catch()` and `finally()`",
    "batch-failure": "a batch with a failing job",
    "unique": "`ShouldBeUnique`",
    "without-overlapping": "`WithoutOverlapping`",
    "rate-limited": "`RateLimited`",
    "backoff-array": "`backoff` as an array",
    "retry-until": "`retryUntil()`",
    "max-exceptions": "`maxExceptions`",
    "fail-on-timeout": "`failOnTimeout`",
    "encrypted": "`ShouldBeEncrypted`",
    "after-commit": "`afterCommit()`",
    "events": "`Queue::before()`, `after()` and `failing()`",
    "failed-commands": "`queue:retry`, `queue:forget` and `queue:flush`",
    "queue-size": "`Queue::size()`",
    "queued-listener": "a queued event listener",
    "queued-notification": "a queued notification",
    "queued-closure": "a queued closure with `catch()`",
    "throttles-exceptions": "`ThrottlesExceptions`",
    "skip-middleware": "`Skip`",
    "unique-until-processing": "`ShouldBeUniqueUntilProcessing`",
    "missing-models": "`$deleteWhenMissingModels`",
    "batch-allow-failures": "`allowFailures()`",
    "batch-cancel": "`$batch->cancel()`",
    "retry-batch": "`queue:retry-batch`",
    "delay-datetime": "`delay()` with a `DateTimeInterface`",
    "release-delay": "`release()` with a delay",
    "queue-monitor": "`queue:monitor`",
    "prune-failed": "`queue:prune-failed`",
    "fork-in-job": "a job that calls `pcntl_fork()`",
}

# The first runs, with the PHP client of the evening.
FIRST = ("failure-matrix", "failure-matrix-php", "replicas", "batch-prefix")
# The runs with the night's client and supervisor fixes. They replace the first
# results in the tables; `changed` lists what they changed.
FINAL = ("rerun-fixed", "batch-fixed", "rc", "final-a", "final-b", "final-c", "soak")


def load(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def lanes(raw: Path, groups: tuple[str, ...]) -> dict[tuple[str, str], dict]:
    """Every lane result of these groups, keyed by (scenario, engine); a later group overrides."""
    found: dict[tuple[str, str], dict] = {}
    for group in groups:
        for path in sorted((raw / group).glob("*/*.json")):
            result = load(path)
            if result is None or "checks" not in result:
                continue
            engine = RENAMED.get(path.stem, path.stem)
            found[(path.parent.name, engine)] = result
    return found


def cell(result: dict | None) -> str:
    if result is None:
        return "–"
    if result.get("error"):
        return f"error: {str(result['error'])[:80]}"
    failed = [c for c in result["checks"] if not c["passed"]]
    if not failed:
        return "pass"
    return "; ".join(f"{c['name']}: {str(c.get('detail', ''))[:60]}".rstrip(": ") for c in failed)


def table(title: str, scenarios: dict, results: dict, engines: tuple[str, ...]) -> list[str]:
    head = ["What happens", "Expected", *(ENGINE_TITLES[e] for e in engines)]
    rows = []
    for scenario, (what, expected) in scenarios.items():
        cells = [cell(results.get((scenario, engine))) for engine in engines]
        if any(c != "–" for c in cells):
            rows.append(f"| {what} | {expected} | " + " | ".join(cells) + " |")
    if not rows:
        return []
    return [f"## {title}", "", "| " + " | ".join(head) + " |", "|" + " --- |" * len(head), *rows, ""]


def compat(raw: Path) -> list[str]:
    results = {e: load(raw / "final-c" / "laravel-compat" / f"{e}.json")
               or load(raw / "rc" / "laravel-compat" / f"{e}.json")
               or load(raw / "compat" / "laravel-compat" / f"{e}.json") for e in ENGINES}
    engines = tuple(e for e in ENGINES if results[e] is not None)
    if not engines:
        return []
    scenarios: list[str] = []
    for engine in engines:
        for name in (results[engine].get("extra") or {}):
            if name not in scenarios:
                scenarios.append(name)
    head = ["Laravel feature", *(ENGINE_TITLES[e] for e in engines)]
    lines = ["## Laravel compatibility", "", "| " + " | ".join(head) + " |", "|" + " --- |" * len(head)]
    for name in scenarios:
        cells = []
        for engine in engines:
            lane = (results[engine].get("extra") or {}).get(name)
            if lane is None:
                cells.append("–")
                continue
            failed = [c for c in lane.get("checks", []) if not c["passed"]]
            cells.append("pass" if not failed else "; ".join(
                f"{c['name']}: {str(c.get('detail', ''))[:60]}" for c in failed))
        lines.append(f"| {COMPAT_NAMES.get(name, f'`{name}`')} | " + " | ".join(cells) + " |")
    return [*lines, ""]


def soak_details(raw: Path, title: str = "Soak in numbers", per_profile: bool = False) -> list[str]:
    """The soak lanes in numbers: jobs, failures and memory drift per engine. The soak of PR #69's
    head ran each profile in a directory of its own, `soak-pr-<profile>`."""
    candidates = ENGINES if per_profile else ENGINES[:3]
    lanes = {e: load(raw / (f"soak-pr-{e}" if per_profile else "soak") / "soak" / f"{e}.json") for e in candidates}
    engines = [e for e in candidates if lanes[e] and lanes[e].get("extra")]
    if not engines:
        return []

    def check(engine: str, prefix: str) -> str:
        found = next((c for c in lanes[engine]["checks"] if c["name"].startswith(prefix)), None)
        return found["detail"] if found else "–"

    def drift(engine: str, key: str) -> str:
        detail = check(engine, key.replace("_", " "))
        first, _, last = detail.partition(" → ")
        try:
            return f"{float(first):.1f} → {float(last.split()[0]):.1f} MiB"
        except ValueError:
            return detail

    rows = {
        "Jobs dispatched": lambda e: f"{lanes[e]['extra']['dispatched']['dispatched']:,}",
        "Jobs that ended as their kind says": lambda e: "all" if all(
            c["passed"] for c in lanes[e]["checks"] if c["name"].endswith("jobs ended as expected")) else "not all",
        "Rows in `failed_jobs`, one per permanent failure": lambda e: check(e, "one failed-job row").split(" rows")[0],
        "Dead-letter entries": lambda e: check(e, "one dead-letter entry").split(" entries")[0],
        "Master memory, first third → last third": lambda e: drift(e, "master_rss_mib"),
        "Median worker memory, first third → last third": lambda e: drift(e, "worker_rss_median_mib"),
    }
    head = ["", *(ENGINE_TITLES[e] for e in engines)]
    lines = [f"## {title}", "", "| " + " | ".join(head) + " |", "|" + " --- |" * len(head)]
    for title, value in rows.items():
        lines.append(f"| {title} | " + " | ".join(value(e) for e in engines) + " |")
    return [*lines, ""]


def changed(before: dict, after: dict) -> list[str]:
    rows = []
    for key in sorted(after):
        old, new = cell(before.get(key)), cell(after[key])
        if key in before and old != new:
            rows.append(f"| {key[0]} | {ENGINE_TITLES.get(key[1], key[1])} | {old} | {new} |")
    if not rows:
        return []
    return ["## Changed by the client fixes", "", "| Lane | Engine | Before | After |",
            "| --- | --- | --- | --- |", *rows, ""]


def main() -> int:
    raw = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "raw"
    first = lanes(raw, FIRST)
    results = lanes(raw, (*FIRST, *FINAL))
    out = ["# Tables", "", f"Generated by `scripts/tables.py` from `{raw.name}/`.", ""]
    out += table("Failures", FAILURES, results, ENGINES)
    out += table("Replicas", REPLICAS, results, ENGINES)
    out += table("Soak", SOAK, results, ENGINES[:3])
    out += soak_details(raw)
    out += soak_details(raw, "Soak of the release candidate in numbers", per_profile=True)
    out += compat(raw)
    out += changed(first, results)
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
