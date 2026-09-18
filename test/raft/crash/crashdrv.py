#!/usr/bin/env python3
"""crashdrv — the crash-point driver of PLAN_RAFT.md §13.5.

Python, on purpose: this harness is process supervision (start, arm a fault,
wait for a death, restart, read stderr) plus a call to the Go checker. Python's
subprocess and signal handling make that readable, and the harness never touches
the data path, so its speed does not matter. The two tools that DO touch the
data path — difffuzz and the flatness preloader — are Go.

    crashdrv.py --list-points                    # the fault catalogue (§13.5)
    crashdrv.py --list-scenarios
    crashdrv.py --dry-run --scenario push-ack --point apply.segment_written
    crashdrv.py --dry-run --matrix --topology raft3
    crashdrv.py --selftest

State (WP-0.7): the catalogue, the plan, the matrix and the reporting are real;
every scenario BODY is a stub, so --run refuses. What each body owes is printed
by --list-scenarios and lives in scenarios.py.

Exit codes: 0 ok · 1 a scenario failed · 2 could not run (flags, missing tool).
"""

import argparse
import contextlib
import io
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import points  # noqa: E402
import scenarios  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER_DIR = REPO_ROOT / "test" / "raft" / "checker"


def fault_env(point: str, nth: int | None) -> dict:
    """The environment a broker is started with to arm one point.

    Returned as a dict rather than exported, so a dry run can PRINT it: a
    harness that arms faults through a mutation you cannot see is a harness
    nobody trusts after the first surprising result.
    """
    return {"QUEEN_TEST_FAULTS": points.spec(point, nth)}


def build_plan(scenario_name: str, point: str, nth: int, topology: str):
    if topology not in ("raft1", "raft3"):
        raise ValueError(f"unknown topology {topology!r} (raft1, raft3)")
    scen = scenarios.BY_NAME.get(scenario_name)
    if scen is None:
        raise KeyError(f"unknown scenario {scenario_name!r} (--list-scenarios)")
    if topology not in scen.topologies:
        raise ValueError(f"scenario {scenario_name!r} does not run on {topology} "
                         f"(it needs {', '.join(scen.topologies)})")
    p = points.BY_NAME.get(point)
    if p is None:
        raise KeyError(f"unknown crash point {point!r} (--list-points)")
    if topology not in p.topology:
        raise ValueError(f"crash point {point!r} cannot be armed on {topology} "
                         f"(it needs {', '.join(p.topology)})")
    return scen, p, scen.actions(point, nth, topology)


def print_plan(scen, point, actions, topology, nth, out=None):
    # `out=None` and not `out=sys.stdout`: a default argument binds the stream
    # ONCE, at import, so a test that captures stdout (pytest's capsys) would
    # read an empty buffer while the plan went to the real terminal. Resolve it
    # at call time.
    out = out or sys.stdout
    spec = points.spec(point.name, nth)
    print(f"scenario  {scen.name}: {scen.title}", file=out)
    print(f"topology  {topology}", file=out)
    print(f"point     {spec}", file=out)
    print(f"          stage: {point.stage}", file=out)
    print(f"          invariants: {point.invariant}", file=out)
    print(f"env       {json.dumps(fault_env(point.name, nth))}", file=out)
    if not scen.implemented:
        print(f"status    STUB — this scenario's body is not written yet.", file=out)
        print(f"          owes: {scen.owes}", file=out)
    print("plan", file=out)
    for i, a in enumerate(actions, 1):
        print(f"  {i:2d}. {a.kind:8s} {a.what}", file=out)
        if a.detail:
            print(f"      ↳ {a.detail}", file=out)


def cmd_list_points(topology, out=None):
    out = out or sys.stdout
    print(f"crash points (PLAN_RAFT.md §13.5), armable on {topology}:", file=out)
    shown = {p.name for p in points.points_for(topology)}
    for group in points.GROUPS:
        print(f"  {group}:", file=out)
        for p in points.POINTS:
            if p.group != group:
                continue
            mark = "x" if p.name in shown else " "
            print(f"    [{mark}] {p.name:28s} {p.stage}", file=out)
            print(f"        invariants: {p.invariant}", file=out)
    print(f"\n  {len(shown)} of {len(points.POINTS)} points are armable on {topology}.", file=out)
    print("  spec format: QUEEN_TEST_FAULTS=\"point[:nth],…\" (off unless set)", file=out)


def cmd_list_scenarios(out=None):
    out = out or sys.stdout
    print("crash scenarios (PLAN_RAFT.md §13.5):", file=out)
    for s in scenarios.SCENARIOS:
        mark = "x" if s.implemented else " "
        print(f"  [{mark}] {s.name:20s} {s.title}   [{', '.join(s.topologies)}]", file=out)
        if not s.implemented:
            print(f"      owes: {s.owes}", file=out)


def cmd_matrix(topology, scenario_names, out=None):
    out = out or sys.stdout
    cells = scenarios.matrix(topology, scenario_names)
    print(f"crash matrix for {topology}: {len(cells)} cell(s)", file=out)
    current = None
    for scen, point in cells:
        if scen != current:
            print(f"  {scen}:", file=out)
            current = scen
        print(f"    {point}", file=out)
    return cells


def preflight(out=None) -> list:
    """What a real run needs. Reported, never assumed."""
    problems = []
    if not (CHECKER_DIR / "main.go").exists():
        problems.append(f"the checker is missing at {CHECKER_DIR}")
    if shutil.which("go") is None:
        problems.append("go is not on PATH (the checker is a Go tool)")
    broker = REPO_ROOT / "server" / "target" / "release" / "queen"
    if not broker.exists():
        problems.append(f"no broker binary at {broker} "
                        f"(cargo build --release --bin queen; raft mode arrives in phase 1)")
    return problems


def selftest(out=None) -> int:
    """Offline smoke test: the catalogue, the plan and the refusals.

    Mirrors test_crashdrv.py so the harness can be checked on a box with no
    pytest (the VM of §13.6 is one).
    """
    out = out or sys.stdout
    failures = []

    def check(name, fn):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - a selftest reports, it does not raise
            failures.append(f"{name}: {exc}")

    def every_point_has_a_stage():
        assert len(points.POINTS) == 24, f"§13.5 lists 24 points, the catalogue has {len(points.POINTS)}"
        for p in points.POINTS:
            assert p.stage and p.invariant, f"{p.name} has no stage or no invariant"
            assert p.topology, f"{p.name} names no topology"

    def spec_format():
        assert points.spec("gc.before_unlink") == "gc.before_unlink"
        assert points.spec("gc.before_unlink", 3) == "gc.before_unlink:3"
        try:
            points.spec("no.such.point")
        except KeyError:
            pass
        else:
            raise AssertionError("an unknown point was accepted")

    def plan_substitutes_the_point():
        _, _, actions = build_plan("push-ack", "apply.segment_written", 2, "raft1")
        assert any("apply.segment_written:2" in a.what for a in actions), "the plan does not carry the fault spec"
        assert actions[0].kind == "start" and actions[-1].kind == "scan", "the plan does not start and end where §13.5 says"
        assert any(a.kind == "check" for a in actions), "a plan with no check proves nothing"

    def raft3_only_points_are_refused_on_raft1():
        try:
            build_plan("push-ack", "snapshot.install_staged", 1, "raft1")
        except ValueError:
            pass
        else:
            raise AssertionError("a raft3-only point was armed on raft1")

    def matrix_is_the_product():
        cells = scenarios.matrix("raft1")
        n_points = len(points.points_for("raft1"))
        n_scen = len(scenarios.scenarios_for("raft1"))
        assert len(cells) == n_points * n_scen, f"{len(cells)} cells, want {n_points * n_scen}"

    def running_a_stub_is_refused():
        sink = io.StringIO()
        with contextlib.redirect_stderr(sink):
            rc = run_scenario("push-ack", "apply.segment_written", 1, "raft1", dry_run=False, out=sink)
        assert rc == 2, f"running a stub scenario returned {rc}, want 2"
        assert "documented stub" in sink.getvalue(), "the refusal does not say why"

    check("every point has a stage", every_point_has_a_stage)
    check("fault spec format", spec_format)
    check("the plan substitutes the point", plan_substitutes_the_point)
    check("a raft3-only point is refused on raft1", raft3_only_points_are_refused_on_raft1)
    check("the matrix is scenarios x points", matrix_is_the_product)
    check("running a stub scenario is refused", running_a_stub_is_refused)

    for f in failures:
        print(f"selftest FAILED: {f}", file=sys.stderr)
    if failures:
        return 2
    print("crashdrv selftest ok (24 points, plan, matrix, refusals)", file=out)
    return 0


def run_scenario(scenario_name, point, nth, topology, dry_run, out=None, run_dir=None):
    out = out or sys.stdout
    try:
        scen, p, actions = build_plan(scenario_name, point, nth, topology)
    except (KeyError, ValueError) as exc:
        print(f"crashdrv: {exc}", file=sys.stderr)
        return 2
    print_plan(scen, p, actions, topology, nth, out=out)
    if dry_run:
        print("\ndry run: nothing was started, nothing was killed.", file=out)
        return 0
    problems = preflight()
    for prob in problems:
        print(f"crashdrv: preflight: {prob}", file=sys.stderr)
    if not scen.implemented:
        print(f"crashdrv: scenario {scen.name!r} is a documented stub (WP-0.7): {scen.owes}", file=sys.stderr)
        print("crashdrv: refusing to report a result for a scenario that does not run.", file=sys.stderr)
        return 2
    raise AssertionError("unreachable while every scenario is a stub")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="crashdrv.py",
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="PLAN_RAFT.md §13.5. Scenario bodies are stubs in WP-0.7; --dry-run and the "
               "catalogues are real.",
    )
    ap.add_argument("--topology", default="raft1", choices=("raft1", "raft3"))
    ap.add_argument("--scenario", default=None,
                    help="scenario name (--list-scenarios); default: push-ack for a run, ALL for --matrix")
    ap.add_argument("--point", default="apply.segment_written", help="crash point (--list-points)")
    ap.add_argument("--nth", type=int, default=1, help="fire the nth time control reaches the point (1-based)")
    ap.add_argument("--run-dir", default=None, help="directory for data dirs, logs and the run log")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and the armed environment, run nothing")
    ap.add_argument("--matrix", action="store_true", help="expand the full (scenario x point) matrix for the topology")
    ap.add_argument("--list-points", action="store_true", help="print the crash-point catalogue and exit")
    ap.add_argument("--list-scenarios", action="store_true", help="print the scenario catalogue and exit")
    ap.add_argument("--preflight", action="store_true", help="report what a real run is missing and exit")
    ap.add_argument("--selftest", action="store_true", help="offline smoke test (no broker, no network)")
    args = ap.parse_args(argv)

    if args.list_points:
        cmd_list_points(args.topology)
        return 0
    if args.list_scenarios:
        cmd_list_scenarios()
        return 0
    if args.selftest:
        return selftest()
    if args.preflight:
        problems = preflight()
        for p in problems:
            print(f"missing: {p}")
        print("preflight: ok" if not problems else f"preflight: {len(problems)} thing(s) missing")
        return 0 if not problems else 1
    if args.matrix:
        try:
            cells = cmd_matrix(args.topology, [args.scenario] if args.scenario else None)  # no --scenario: every scenario
        except KeyError as exc:
            print(f"crashdrv: {exc}", file=sys.stderr)
            return 2
        if not args.dry_run:
            print("crashdrv: --matrix without --dry-run would run every cell; "
                  "every scenario body is a stub in WP-0.7.", file=sys.stderr)
            return 2
        return 0
    return run_scenario(args.scenario or "push-ack", args.point, args.nth, args.topology,
                        dry_run=args.dry_run, run_dir=args.run_dir)


if __name__ == "__main__":
    sys.exit(main())
