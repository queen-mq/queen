#!/usr/bin/env python3
"""killrun — the VM kill-test runner of PLAN_RAFT.md §13.6.

Table-driven: the scenarios are rows (killscenarios.py), the kill times come
from a stratified scheduler (killsched.py), and the results go through one
reporter (killreport.py) that refuses to average over runs that did not happen.

    killrun.py --list                          # the scenario table
    killrun.py --schedule --runs 6             # the kill times, with coverage
    killrun.py --dry-run --scenario kill9-leader
    killrun.py --selftest
    killrun.py --run --scenario kill9-leader   # refused: bodies are stubs (WP-0.7)

WHERE THIS RUNS: the Linux VM of §13.6 (root@164.90.215.224), never a laptop —
macOS has no dm-delay, no iptables and a different fsync. Several scenarios need
root and change machine-wide state (iptables rules, the clock, loop devices), so
the runner refuses to start when another phase-0 job is on the box, and every
scenario declares its teardown in the table.

Exit codes: 0 ok · 1 a scenario failed · 2 could not run.
"""

import argparse
import contextlib
import io
import json
import platform
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import killreport  # noqa: E402
import killsched  # noqa: E402
import killscenarios  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
VM_HOST = "164.90.215.224"  # §13.6


def git_commit() -> str:
    try:
        out = subprocess.run(["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


def host_label() -> str:
    return f"{socket.gethostname()} ({platform.system().lower()})"


def timing_from_args(args) -> killsched.Timing:
    return killsched.Timing(
        warmup_s=args.warmup,
        duration_s=args.duration,
        durable_every_s=args.durable_every,
        snapshot_every_s=args.snapshot_every,
        snapshot_build_s=args.snapshot_build,
    )


# ------------------------------------------------------------------- printing

def cmd_list(out=None):
    out = out or sys.stdout
    print("kill scenarios (PLAN_RAFT.md §13.6):", file=out)
    for s in killscenarios.SCENARIOS:
        mark = "x" if s.implemented else " "
        root = " [root]" if s.root_needed else ""
        print(f"  [{mark}] {s.name:22s} {s.title}{root}", file=out)
        print(f"      target:   {s.target} on {s.topology}", file=out)
        print(f"      proves:   {s.proves}", file=out)
        print(f"      teardown: {s.teardown}", file=out)
        if not s.implemented:
            print(f"      owes:     {s.owes}", file=out)


def cmd_schedule(scenarios, runs, seed, timing, out=None):
    out = out or sys.stdout
    ok = True
    for s in scenarios:
        kills = killsched.schedule(s.name, runs, seed, timing)
        cov = killsched.coverage(kills)
        print(f"{s.name}: {runs} run(s), seed {seed}", file=out)
        for k in kills:
            print(f"  run {k.run:2d}  t+{k.offset_s:8.3f}s  {k.phase:16s} "
                  f"durable#{k.durable_index:<4d} snapshot#{k.snapshot_epoch}  — {k.why}", file=out)
        print(f"  coverage: {cov['durable_intervals']} durable interval(s), "
              f"{cov['snapshot_builds']} snapshot build(s), phases: {', '.join(cov['phases'])}", file=out)
        try:
            killsched.validate(kills, timing)
            print("  schedule: OK for §13.6", file=out)
        except killsched.ScheduleTooNarrow as exc:
            ok = False
            print(f"  schedule: REFUSED — {exc}", file=out)
    return ok


def cmd_dry_run(scenarios, runs, seed, timing, topology, out=None):
    out = out or sys.stdout
    ok = True
    for s in scenarios:
        kills = killsched.schedule(s.name, runs, seed, timing)
        print(f"scenario {s.name}: {s.title}", file=out)
        print(f"  topology {topology}   target {s.target}   root needed: {s.root_needed}", file=out)
        print(f"  proves   {s.proves}", file=out)
        if not s.implemented:
            print(f"  STATUS   STUB (WP-0.7) — owes: {s.owes}", file=out)
        for k in kills:
            print(f"  run {k.run}:", file=out)
            for i, step in enumerate(plan_for(s, k, timing, topology), 1):
                print(f"    {i}. {step}", file=out)
        print(f"  teardown  {s.teardown}", file=out)
        try:
            killsched.validate(kills, timing)
        except killsched.ScheduleTooNarrow as exc:
            ok = False
            print(f"  schedule REFUSED: {exc}", file=out)
    return ok


def plan_for(scenario, kill, timing, topology):
    """The steps one run would take. Data, so --dry-run shows exactly the run."""
    return [
        f"start {topology} with QUEEN_RAFT_DURABLE_EVERY_MS={int(timing.durable_every_s * 1000)}, "
        f"QUEEN_RAFT_SNAPSHOT_MAX_INTERVAL_S={int(timing.snapshot_every_s)} (overridden so a "
        f"{timing.duration_s:g}s run covers snapshot builds; recorded in the report)",
        f"start goload A20k over 64 partitions, writing a run log (test/raft/checker format)",
        f"wait {timing.warmup_s:g}s of warm-up",
        f"at t+{kill.offset_s:.3f}s ({kill.phase}: {kill.why}) apply the fault to the {scenario.target}",
        "measure: time until a new leader answers a write; client-visible unavailability from the load's own errors",
        "restore the node / rule / device, wait for the cluster to be whole",
        "drain every queue, write the drain-complete notes, stop the load",
        "run test/raft/checker -strict over the run log",
        f"teardown: {scenario.teardown}",
    ]


def preflight(topology, scenarios, out=None):
    """What a real campaign needs before it touches the VM.

    §0.3 and the task rules: another phase-0 job must not be running, and a
    scenario that needs root must say so before it starts, not halfway through.
    """
    out = out or sys.stdout
    problems = []
    if platform.system() != "Linux":
        problems.append(f"this is {platform.system()}: §13.6 numbers come from the Linux VM ({VM_HOST}); "
                        f"dm-delay, iptables and loop devices do not exist here")
    if any(s.root_needed for s in scenarios) and hasattr(__import__("os"), "geteuid"):
        import os
        if os.geteuid() != 0:
            problems.append("scenarios needing root are selected and this process is not root "
                            "(iptables, dm-setup, losetup, clock)")
    if topology != "raft3" and any(s.topology == "raft3" for s in scenarios):
        problems.append(f"topology is {topology} but the selected scenarios need raft3")
    return problems


# ------------------------------------------------------------------- selftest

def selftest(out=None) -> int:
    out = out or sys.stdout
    failures = []

    def check(name, fn):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{name}: {exc}")

    def table_matches_the_plan():
        expected = {
            "kill9-leader", "kill9-follower", "sigstop-leader", "partition-leader", "wipe-follower",
            "clock-jump-back", "clock-jump-forward", "slow-disk-follower", "enospc-follower",
            "rolling-restart",
        }
        assert set(killscenarios.BY_NAME) == expected, "the table does not match the §13.6 list"
        for s in killscenarios.SCENARIOS:
            assert s.proves and s.teardown, f"{s.name} does not say what it proves or what it cleans up"

    def schedule_is_deterministic():
        a = killsched.schedule("kill9-leader", 6, 1234)
        b = killsched.schedule("kill9-leader", 6, 1234)
        c = killsched.schedule("kill9-leader", 6, 1235)
        assert a == b, "the same seed produced two different schedules"
        assert a != c, "two seeds produced the same schedule"

    def schedule_covers_the_boundaries():
        kills = killsched.schedule("kill9-leader", 6, 99)
        killsched.validate(kills)
        cov = killsched.coverage(kills)
        assert cov["durable_intervals"] >= killsched.MIN_DURABLE_INTERVALS
        assert cov["snapshot_builds"] >= killsched.MIN_SNAPSHOT_BUILDS
        assert set(cov["phases"]) == set(killsched.PHASE_NAMES), "not every phase is exercised"

    def a_pgless_shaped_schedule_is_refused():
        # The pgless mistake: a run too short to reach the first periodic boundary.
        narrow = killsched.Timing(warmup_s=5, duration_s=25, snapshot_every_s=30, snapshot_build_s=5)
        kills = killsched.schedule("kill9-leader", 6, 7, narrow)
        try:
            killsched.validate(kills, narrow)
        except killsched.ScheduleTooNarrow as exc:
            assert "snapshot build" in str(exc)
        else:
            raise AssertionError("a schedule that never reaches a snapshot build was accepted")

    def report_excludes_runs_that_did_not_happen():
        c = killreport.Campaign(host="h", commit="c", started="t", topology="raft3", seed=1,
                                timing={}, command="killrun.py --selftest")
        c.add(killreport.RunResult("kill9-leader", 1, killreport.OK, leader_ms=900, unavailable_ms=1200))
        c.add(killreport.RunResult("kill9-leader", 2, killreport.NOT_RUN, note="stub"))
        row = c.summary_rows()[0]
        assert row["executed"] == 1 and row["not_run"] == 1
        assert row["leader_ms_p50"] == 900
        assert c.verdict() == killreport.NOT_RUN, "a campaign with unexecuted runs must not report ok"
        assert "DID NOT EXECUTE" in c.text()

    def one_failure_fails_the_scenario():
        c = killreport.Campaign(host="h", commit="c", started="t", topology="raft3", seed=1,
                                timing={}, command="x")
        for i in range(4):
            c.add(killreport.RunResult("kill9-leader", i + 1, killreport.OK, leader_ms=800))
        c.add(killreport.RunResult("kill9-leader", 5, killreport.FAILED, note="a push was lost"))
        assert c.summary_rows()[0]["verdict"] == killreport.FAILED
        assert c.verdict() == killreport.FAILED

    def running_a_stub_is_refused():
        sink = io.StringIO()
        with contextlib.redirect_stderr(sink):
            rc = run_campaign(killscenarios.select(["kill9-leader"]), runs=5, seed=1,
                              timing=killsched.Timing(), topology="raft3", out=sink, force=True)
        assert rc == 2, f"running a stub scenario returned {rc}, want 2"
        assert "stubs in WP-0.7" in sink.getvalue(), "the refusal does not say why"

    def a_campaign_refuses_a_laptop():
        sink = io.StringIO()
        with contextlib.redirect_stderr(sink):
            rc = run_campaign(killscenarios.select(["kill9-leader"]), runs=5, seed=1,
                              timing=killsched.Timing(), topology="raft3", out=sink, force=False)
        assert rc == 2, "a campaign started without a preflight pass"

    check("the table matches §13.6", table_matches_the_plan)
    check("the schedule is deterministic", schedule_is_deterministic)
    check("the schedule covers the boundaries", schedule_covers_the_boundaries)
    check("a pgless-shaped schedule is refused", a_pgless_shaped_schedule_is_refused)
    check("the report excludes runs that did not happen", report_excludes_runs_that_did_not_happen)
    check("one failed run fails the scenario", one_failure_fails_the_scenario)
    check("running a stub scenario is refused", running_a_stub_is_refused)
    check("a campaign refuses to start off the VM", a_campaign_refuses_a_laptop)

    for f in failures:
        print(f"selftest FAILED: {f}", file=sys.stderr)
    if failures:
        return 2
    print("killrun selftest ok (10 scenarios, stratified schedule, refusals, reporting)", file=out)
    return 0


# ------------------------------------------------------------------ campaigns

def run_campaign(scenarios, runs, seed, timing, topology, out=None, force=False):
    out = out or sys.stdout
    problems = preflight(topology, scenarios)
    for p in problems:
        print(f"killrun: preflight: {p}", file=sys.stderr)
    if problems and not force:
        print("killrun: refusing to start (use --force only when you know why each line above is fine)",
              file=sys.stderr)
        return 2
    stubs = [s.name for s in scenarios if not s.implemented]
    if stubs:
        print(f"killrun: scenario bodies are stubs in WP-0.7: {', '.join(stubs)}", file=sys.stderr)
        print("killrun: refusing to report a campaign for scenarios that do not run.", file=sys.stderr)
        return 2
    raise AssertionError("unreachable while every scenario body is a stub")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="killrun.py",
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="PLAN_RAFT.md §13.6. Scenario bodies are stubs in WP-0.7; the table, the scheduler "
               "and the reporting are real. Runs on the Linux VM, never on a laptop.",
    )
    ap.add_argument("--scenario", action="append", default=None,
                    help="scenario name (repeatable); default: every scenario")
    ap.add_argument("--topology", default="raft3", choices=("raft1", "raft3"))
    ap.add_argument("--runs", type=int, default=killsched.MIN_RUNS, help="runs per scenario (§13.6: at least 5)")
    ap.add_argument("--seed", type=int, default=1, help="seed for the kill times; a campaign is replayable from it")
    ap.add_argument("--warmup", type=float, default=killsched.Timing.warmup_s)
    ap.add_argument("--duration", type=float, default=killsched.Timing.duration_s, help="workload seconds per run")
    ap.add_argument("--durable-every", type=float, default=killsched.Timing.durable_every_s,
                    help="QUEEN_RAFT_DURABLE_EVERY_MS as seconds")
    ap.add_argument("--snapshot-every", type=float, default=killsched.Timing.snapshot_every_s,
                    help="QUEEN_RAFT_SNAPSHOT_MAX_INTERVAL_S for the run")
    ap.add_argument("--snapshot-build", type=float, default=killsched.Timing.snapshot_build_s,
                    help="measured snapshot build seconds; update from a real run")
    ap.add_argument("--list", action="store_true", help="print the scenario table and exit")
    ap.add_argument("--schedule", action="store_true", help="print the kill times and their coverage, and exit")
    ap.add_argument("--dry-run", action="store_true", help="print what each run would do, run nothing")
    ap.add_argument("--preflight", action="store_true", help="report what a real campaign is missing")
    ap.add_argument("--selftest", action="store_true", help="offline smoke test")
    ap.add_argument("--json", action="store_true", help="machine-readable output where it applies")
    ap.add_argument("--force", action="store_true", help="start despite preflight problems (say why in the report)")
    args = ap.parse_args(argv)

    try:
        scenarios = killscenarios.select(args.scenario)
    except KeyError as exc:
        print(f"killrun: {exc}", file=sys.stderr)
        return 2
    timing = timing_from_args(args)

    if args.list:
        cmd_list()
        return 0
    if args.selftest:
        return selftest()
    if args.preflight:
        problems = preflight(args.topology, scenarios)
        for p in problems:
            print(f"missing: {p}")
        print("preflight: ok" if not problems else f"preflight: {len(problems)} problem(s)")
        return 0 if not problems else 1
    if args.schedule:
        if args.json:
            payload = {s.name: [k.as_dict() for k in killsched.schedule(s.name, args.runs, args.seed, timing)]
                       for s in scenarios}
            print(json.dumps(payload, indent=2))
            return 0
        return 0 if cmd_schedule(scenarios, args.runs, args.seed, timing) else 1
    if args.dry_run:
        return 0 if cmd_dry_run(scenarios, args.runs, args.seed, timing, args.topology) else 1

    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _ = killreport.Campaign(host=host_label(), commit=git_commit(), started=started,
                            topology=args.topology, seed=args.seed, timing=vars(args),
                            command="killrun.py " + " ".join(argv or sys.argv[1:]))
    return run_campaign(scenarios, args.runs, args.seed, timing, args.topology, force=args.force)


if __name__ == "__main__":
    sys.exit(main())
