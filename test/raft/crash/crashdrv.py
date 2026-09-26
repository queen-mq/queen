#!/usr/bin/env python3
"""crashdrv — the crash-point driver of PLAN_RAFT.md §13.5.

Python, on purpose: this harness is process supervision (start, arm a fault,
wait for a death, restart, read stderr) plus a call to the Go checker. Python's
subprocess and signal handling make that readable, and the harness never touches
the data path, so its speed does not matter. The two tools that DO touch the
data path — the flatness preloader — is Go.

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
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import points  # noqa: E402
import runner  # noqa: E402
import scenarios  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER_DIR = REPO_ROOT / "test" / "raft" / "checker"

# The push-ack scenario is the phase-1 body (runner.py). Everything else is a
# documented stub until its phase.
IMPLEMENTED_RUNNERS = {"push-ack": runner.run_push_ack}


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
    if runner.find_broker() is None:
        problems.append(
            "no broker binary (set QUEEN_BROKER, or "
            "`cargo build --bin queen` for debug / `--release` for release; "
            "crash injection is a correctness test, so debug is fine)"
        )
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
        # 24 §13.5 points + 2 ALICE_PGLESS_NEWARCH §5 (Phase A3a) qlog points = 26.
        assert len(points.POINTS) == 26, f"24 §13.5 + 2 qlog points = 26, the catalogue has {len(points.POINTS)}"
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
        # txn-riders is still a documented stub (its body lands in phase 2).
        sink = io.StringIO()
        with contextlib.redirect_stderr(sink):
            rc = run_scenario("txn-riders", "log.flushed", 1, "raft1", dry_run=False, out=sink)
        assert rc == 2, f"running a stub scenario returned {rc}, want 2"
        assert "documented stub" in sink.getvalue(), "the refusal does not say why"

    def push_ack_is_implemented():
        assert scenarios.BY_NAME["push-ack"].implemented
        assert "push-ack" in IMPLEMENTED_RUNNERS

    def the_run_matrix_covers_the_wired_phase_one_points():
        wired = [p for p in points.POINTS if p.phase == 1 and "raft1" in p.topology]
        # 13 §13.5 phase-1 points + 2 A3a qlog points = 15 wired; 13 are
        # push-ack-reachable (11 §13.5 + the 2 qlog cells), 2 (gc.*) need
        # retention. The matrix arms all 15 and reports the 2 gc.* as N/A.
        assert len(wired) == 15, f"{len(wired)} wired phase-1 raft1 points, want 15"
        reachable = [p for p in wired if p.name in runner.PUSHACK_REACHABLE]
        assert len(reachable) == 13, f"{len(reachable)} push-ack-reachable, want 13"

    check("every point has a stage", every_point_has_a_stage)
    check("fault spec format", spec_format)
    check("the plan substitutes the point", plan_substitutes_the_point)
    check("a raft3-only point is refused on raft1", raft3_only_points_are_refused_on_raft1)
    check("the matrix is scenarios x points", matrix_is_the_product)
    check("running a stub scenario is refused", running_a_stub_is_refused)
    check("push-ack is implemented", push_ack_is_implemented)
    check("the run matrix covers the wired phase-1 points", the_run_matrix_covers_the_wired_phase_one_points)

    for f in failures:
        print(f"selftest FAILED: {f}", file=sys.stderr)
    if failures:
        return 2
    print("crashdrv selftest ok (26 points, plan, matrix, refusals, phase-1 run matrix)", file=out)
    return 0


def run_scenario(scenario_name, point, nth, topology, dry_run, out=None, run_dir=None,
                 broker=None):
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

    body = IMPLEMENTED_RUNNERS.get(scen.name) if scen.implemented else None
    if body is None:
        print(f"crashdrv: scenario {scen.name!r} is a documented stub: {scen.owes}", file=sys.stderr)
        print("crashdrv: refusing to report a result for a scenario that does not run.", file=sys.stderr)
        return 2

    problems = preflight()
    # The Go checker is one check of several; a missing `go` degrades that check
    # to a note, it does not stop the run. A missing broker does.
    if runner.find_broker() is None and broker is None:
        for prob in problems:
            print(f"crashdrv: preflight: {prob}", file=sys.stderr)
        return 2
    for prob in problems:
        print(f"crashdrv: preflight: {prob} (continuing)", file=sys.stderr)

    broker_bin = broker or str(runner.find_broker())
    run_dir = run_dir or _default_run_dir()
    # Make the resolved binary PROVE it booted raft mode before we run: a
    # pre-raft binary boots the Postgres class and dies at schema apply, which
    # would otherwise fail as a misleading per-cell "exited before health".
    try:
        runner.assert_raft_aware(broker_bin, run_dir)
    except runner.BrokerNotRaftAware as exc:
        print(f"crashdrv: {exc}", file=sys.stderr)
        return 2
    res = body(broker_bin, point, nth, run_dir, topology, out=out)
    _print_result(res, out=out)
    return 0 if res.verdict in ("PASS", "N/A") else 1


def _default_run_dir():
    base = os.environ.get("TMPDIR", "/tmp")
    return str(Path(base) / f"crashdrv-{int(time.time())}")


def _print_result(res, out=None):
    out = out or sys.stdout
    print(f"\nresult    {res.cell}: {res.verdict}", file=out)
    print(f"          fired={res.fired} exit={res.exit_code} stats={res.stats}", file=out)
    for name, verdict in res.checks.items():
        print(f"  check   {name}: {verdict}", file=out)
    for r in res.reasons:
        print(f"  note    {r}", file=out)


def cmd_run_matrix(topology, run_dir, broker, late_nth, out=None):
    """Run the phase-1 crash matrix — the §13.5 points a phase-1 push/pop/ack
    workload can drive, each at nth=1 and a later hit — and write RESULTS.md.

    "Every point" is every §13.5 point wired in this phase; the raft3-only and
    phase-2+ points are named in the report as out of scope, not run.
    """
    out = out or sys.stdout
    if topology != "raft1":
        print(f"crashdrv: the run matrix is phase 1 (raft1); got {topology}", file=sys.stderr)
        return 2
    if runner.find_broker() is None and broker is None:
        print("crashdrv: no broker binary; build one or set QUEEN_BROKER", file=sys.stderr)
        return 2
    broker_bin = broker or str(runner.find_broker())
    run_dir = run_dir or _default_run_dir()
    Path(run_dir).mkdir(parents=True, exist_ok=True)

    # Record the resolved binary BEFORE the run, and make it prove it booted
    # raft mode before any cell runs. A stale/pre-raft binary otherwise fails
    # EVERY cell identically at boot (the WP-1.8 reproducibility refutation): one
    # clear abort with the broker's stderr tail beats 26 misleading cell logs.
    print(f"crashdrv: resolved broker binary: {broker_bin}", file=out, flush=True)
    _write_results_header(Path(__file__).resolve().parent / "RESULTS.md",
                          broker_bin, run_dir, late_nth)
    try:
        runner.assert_raft_aware(broker_bin, run_dir)
    except runner.BrokerNotRaftAware as exc:
        print(f"crashdrv: {exc}", file=sys.stderr)
        print("crashdrv: aborting the whole matrix (no cell can run with a "
              "non-raft binary).", file=sys.stderr)
        return 2

    phase1 = [p for p in points.POINTS if p.phase == 1 and "raft1" in p.topology]
    nths = [1, late_nth]
    results = []
    total = len(phase1) * len(nths)
    i = 0
    for p in phase1:
        for nth in nths:
            i += 1
            print(f"[{i}/{total}] {p.name}:{nth} …", file=out, flush=True)
            res = runner.run_push_ack(broker_bin, p.name, nth, run_dir, topology, out=out)
            _print_result(res, out=out)
            results.append(res)

    results_md = Path(__file__).resolve().parent / "RESULTS.md"
    _write_results_md(results_md, results, broker_bin, run_dir, late_nth)
    print(f"\nwrote {results_md}", file=out)
    failed = [r for r in results if r.verdict == "FAIL"]
    print(f"matrix: {len(results)} cells, "
          f"{sum(1 for r in results if r.verdict == 'PASS')} PASS, "
          f"{len(failed)} FAIL, "
          f"{sum(1 for r in results if r.verdict == 'N/A')} N/A", file=out)
    return 1 if failed else 0


def _results_header_lines(broker_bin, run_dir, late_nth):
    """The RESULTS.md metadata block, shared by the pre-run header and the final
    write so the resolved binary is recorded even if the run aborts."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    is_release = "release" in str(broker_bin)
    lines = []
    lines.append("# Crash matrix — WP-1.8 (PLAN_RAFT.md §13.5)")
    lines.append("")
    lines.append(f"- **date** {now}")
    lines.append(f"- **topology** raft1 (single voter, `QUEEN_STORAGE=raft`, no Postgres)")
    lines.append(f"- **scenario** push-ack: pushes with recorded ids + payload hashes to a "
                 "JUDGED queue (`orders`, never popped before the crash); pops+acks a WARM queue "
                 "every round so a COMPLETION crosses the armed pipeline BEFORE the crash; and "
                 "pops-and-never-acks a HELD queue every round so a bare CLAIM/LEASE crosses it. "
                 "One retry of every unanswered push with its original transactionId. After "
                 "recovery, when a claim or completion crossed the crash, the harness waits the "
                 "60 s facade lease out and re-drains, so the ack/claim recovery outcome is "
                 "observable instead of hidden behind a live lease (the WP-1.8 refutation fix).")
    lines.append(f"- **broker** `{broker_bin}` ({'release' if is_release else 'debug'} — "
                 "crash injection is a correctness test, so a debug binary is faithful; "
                 "§0.3 reserves release/VM for measurements). MUST be a raft-aware build: the "
                 "driver makes it prove it booted raft mode (`/health` `engine:raft`, "
                 "`storageReady:true`) before any cell runs, and aborts the whole run — echoing "
                 "the broker's stderr tail — when it does not.")
    lines.append(f"- **cadence** `QUEEN_RAFT_DURABLE_EVERY_MS=150` so a later hit crosses a "
                 "durable point quickly (§13.6: cover every periodic boundary)")
    lines.append(f"- **qlog** `QUEEN_RAFT_QLOG=1` (ALICE_PGLESS_NEWARCH.md §5, Phase A3b — the "
                 "double-write kill): the payload is written ONCE. The LOG WRITER writes each "
                 "`Append`'s payload to its queue's qlog and fsyncs it BEFORE the referencing "
                 "raft-log entry, which is now PAYLOAD-FREE; apply does the metadata only and "
                 "reads the payload from the qlog. This run proves (a) the two writer cells "
                 "`qlog.record_written` (payload on the page cache, entry not written) and "
                 "`qlog.record_fsynced` (payload DURABLE, entry not written) both recover — a kill "
                 "before the group fsync loses the unacked op cleanly, a kill after it recovers the "
                 "op with its payload; (b) every EXISTING §13.5 cell still recovers with the knob "
                 "on; and (c) `apply.segment_written` is N/A (apply writes no segment on this "
                 "path). With the knob on, pop reads the payload FROM the qlog, so the cells also "
                 "cross \"acked records are readable from the qlog\". A knob-OFF control run "
                 "(`QUEEN_RAFT_QLOG=0`) arms `apply.segment_written` and marks the two qlog cells "
                 "N/A, proving today's path is unchanged.")
    lines.append(f"- **nth** each point armed at `nth=1` and `nth={late_nth}` (a later hit)")
    lines.append(f"- **run dir** `{run_dir}` (data dirs, per-cell `run.jsonl`, `pre.stderr`, "
                 "`post.stderr`)")
    lines.append("")
    lines.append("> **What this does NOT test.** `faults::hit` dies by SIGKILL, which leaves the "
                 "OS page cache intact, so a frame written but not yet fsynced is fully present at "
                 "restart and replays. This matrix therefore proves recovery BOOKKEEPING (offsets, "
                 "cursors, completion, dedup across the kill), NOT unsynced-byte loss (I11's "
                 "dropped-unflushed-writes clause). That needs a fault-injecting block device "
                 "(dm-flakey) on the Linux VM and is WP-1.11.")
    lines.append("")
    return lines


def _write_results_header(path, broker_bin, run_dir, late_nth):
    """Write the metadata block before the run, so the resolved binary is on
    record even if the run aborts (e.g. the raft-awareness gate rejects it)."""
    lines = _results_header_lines(broker_bin, run_dir, late_nth)
    lines.append("_(run in progress — this file is rewritten with the full matrix when the run "
                 "completes)_")
    lines.append("")
    path.write_text("\n".join(lines) + "\n")


def _write_results_md(path, results, broker_bin, run_dir, late_nth):
    lines = _results_header_lines(broker_bin, run_dir, late_nth)
    lines.append("## Checks (each cell, after restart)")
    lines.append("")
    lines.append("1. **go: delivery-at-least-once** + **payload-hash** (the shared Go checker, "
                 "`test/raft/checker`): every answered push delivered; delivered bytes == pushed "
                 "bytes; no phantom (a rejected id delivered). Scoped by the `drain-complete` note "
                 "the harness writes for the JUDGED (`orders`) queue only. Run WITHOUT `-strict`: "
                 "`delivery-at-least-once` SKIPs (not fails) when the crash preceded any "
                 "acknowledged orders push — there is nothing acknowledged to judge, and that case "
                 "is judged instead by check 4 (which judges the retried duplicates). A real Go "
                 "violation still fails the cell.")
    lines.append("2. **exactly-one-offset-per-txn**: every transactionId maps to a single "
                 "offset across every push answer and every delivery — a second offset would be "
                 "a message dedup created twice across the crash.")
    lines.append("3. **delivered-exactly-once**: no id delivered twice in the single post-crash "
                 "drain of the judged queue.")
    lines.append("4. **answered-then-delivered**: every JUDGED (`orders`) push the broker "
                 "ANSWERED (queued, or a retry's duplicate) — and that was not popped before the "
                 "crash — is present after recovery (exactly-once counterpart of at-least-once).")
    lines.append("5. **claim-redelivered-after-lease** (the WP-1.8 claim/lease recovery control): "
                 "a HELD message claimed but DELIBERATELY never acked before the crash has no "
                 "completion, so after its 60 s lease expires it MUST redeliver — exactly once, and "
                 "never BEFORE the lease is out. A lost claim (never comes back) or a claim "
                 "redelivered too early (recovery dropped the lease) fails the cell. The `claimed` "
                 "column is how many such claims each cell put across the crash; it is 0 for the "
                 "points that fire before any claim (N/A). This control is what lets check 6 fail: "
                 "the held claim reappearing in the post-lease re-drain PROVES the observation "
                 "window is genuinely past the lease.")
    lines.append("6. **acked-not-redelivered** (the ack-path exactly-once property): a WARM message "
                 "the broker confirmed COMPLETED before the crash must NOT come back — NOT in the "
                 "immediate drain, NOT in the post-lease re-drain. An immediate-only observation "
                 "could never falsify this (a completion wrongly resurrected as leased is hidden "
                 "for 60 s); the re-drain after the lease, backed by the check-5 control, is what "
                 "makes it able to fail. The `popacked` column is how many completions each cell "
                 "put across the crash; 0 (and thus N/A) for the early points (see the disclosure "
                 "below).")
    lines.append("7. **raft1-leader-and-monotone-applied**: the single voter is a healthy leader "
                 "after restart and its applied index does not go backwards.")
    lines.append("8. **no-unexpected-error-lines**: broker stderr (pre- and post-crash) has no "
                 "`ERROR`/panic line except the fault's own `fault: crash point … fired`.")
    lines.append("")
    lines.append("## Matrix")
    lines.append("")
    lines.append("| point | nth | fired | exit | verdict | applied@restart | delivered | popacked | claimed | lease-wait | notes |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        note = "; ".join(r.reasons) if r.reasons else ""
        note = note.replace("|", "\\|")
        exit_str = str(r.exit_code) if r.exit_code is not None else "—"
        applied = r.stats.get("applied_after_restart", "—")
        deliv = r.stats.get("delivered", "—")
        popacked = r.stats.get("popacked_before_crash", "—")
        claimed = r.stats.get("claimed_held_before_crash", "—")
        waited = "yes" if r.stats.get("lease_waited") else "—"
        lines.append(f"| `{r.point}` | {r.nth} | {r.fired} | {exit_str} | "
                     f"**{r.verdict}** | {applied} | {deliv} | {popacked} | {claimed} | "
                     f"{waited} | {note} |")
    lines.append("")
    lines.append("## Ack-path crash coverage (the WP-1.8 coverage-gap fix)")
    lines.append("")
    lines.append("The workload puts BOTH halves of the claim/ack path across the armed pipeline "
                 "before the crash: a COMPLETION (the warm queue, popped and acked every round — "
                 "the `popacked` column) and a bare CLAIM/LEASE (the held queue, popped and never "
                 "acked — the `claimed` column). After recovery the harness does not judge from an "
                 "immediate drain, which cannot distinguish a completed message that stayed "
                 "completed from one wrongly resurrected as leased (both are hidden by the 60 s "
                 "facade lease — the WP-1.8 refutation). Instead, when a claim or completion "
                 "crossed the crash (`lease-wait = yes`), it waits the lease out and re-drains. The "
                 "held claim MUST then redeliver exactly once (`claim-redelivered-after-lease`): "
                 "that reappearance is the positive control PROVING the observation window is past "
                 "the lease, which is precisely what lets `acked-not-redelivered` fail — a "
                 "completion that does not reappear in the same post-lease drain genuinely stayed "
                 "completed. The `durable.*` points, which fire on the periodic durable boundary "
                 "after many mixed entries, are the cells that carry both halves in the HTTP "
                 "matrix.")
    lines.append("")
    lines.append("RESIDUAL, disclosed: the first ENTRIES of any run are unavoidably pushes (a "
                 "claim needs a prior push), so `apply.mid_entry`, `commit.before_apply` and the "
                 "other apply/log points at the matrix's `nth∈{1,2}` still crash on an `Append`, "
                 "with `popacked=claimed=0`. A CLAIM/COMPLETION crashed MID-APPLY (I1, I11) is "
                 "therefore NOT exercised over HTTP by those cells; it is proven at the Rust level "
                 "by `rsm::tests::apply_crash`. `a_completion_entry_crashed_mid_apply_"
                 "repairs` arms `apply.mid_entry` on an entry whose first effect is a `CursorSet` "
                 "(a completion) and aborts AFTER that completion is in the open store txn but "
                 "before the entry commits, then reopens and replays to a byte-equal digest — the "
                 "specific 'a claim/ack entry crashed mid-apply' case. "
                 "`each_fault_point_fires_and_the_node_repairs` covers the GENERAL mechanism (an "
                 "uncommitted store txn is discarded and the whole entry is replayed, identical "
                 "across effect kinds) for every apply-side point. The raft3 crash matrix under "
                 "load (WP-4.10) exercises mid-apply completions over the wire. Phase-1 HTTP "
                 "ack-path crash coverage for those early points is otherwise only at the Rust "
                 "level.")
    lines.append("")
    lines.append("## Per-cell checks")
    lines.append("")
    for r in results:
        if r.verdict == "N/A":
            continue
        lines.append(f"### `{r.point}` nth={r.nth} — {r.verdict}")
        for name, verdict in r.checks.items():
            lines.append(f"- {name}: {verdict}")
        lines.append("")
    lines.append("## Points not run here, and why")
    lines.append("")
    lines.append("- **`gc.before_unlink`, `gc.after_unlink`** — phase-1 §13.5 points, WIRED in "
                 "`rsm/apply.rs::unlink_staged` and proven to fire by the Rust apply-crash fault "
                 "test (`rsm::tests::apply_crash`), but NOT reachable from a phase-1 push/pop/ack "
                 "workload: a file becomes collectable only through retention or a delete, and "
                 "neither has an HTTP route in phase 1 (§10.3). The gc-compaction scenario drives "
                 "them once retention lands (WP-2.7).")
    lines.append("- **`seg.rolled`, `seg.qidx_written`** — extra points (R-107) for the segment "
                 "roll tests, not in the §13.5 HTTP matrix; the default 64 MiB segment does not "
                 "roll under a phase-1 workload.")
    lines.append("- **snapshots, compaction, identity, membership, transfer** — phase 2+ / "
                 "raft3-only (`points.py` `phase`/`topology`); their code paths do not exist in a "
                 "phase-1 raft1 broker, which refuses to arm them (`rsm/faults.rs` exits 2).")
    lines.append("")
    path.write_text("\n".join(lines) + "\n")


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
    ap.add_argument("--run-matrix", action="store_true",
                    help="RUN the phase-1 crash matrix (push-ack, every wired point at nth=1 and a "
                         "later hit) and write RESULTS.md")
    ap.add_argument("--broker", default=None, help="broker binary (default: QUEEN_BROKER, then release, then debug)")
    ap.add_argument("--late-nth", type=int, default=2, help="the 'later hit' nth for --run-matrix (default 2)")
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
    if args.run_matrix:
        return cmd_run_matrix(args.topology, args.run_dir, args.broker, args.late_nth)
    if args.matrix:
        try:
            cells = cmd_matrix(args.topology, [args.scenario] if args.scenario else None)  # no --scenario: every scenario
        except KeyError as exc:
            print(f"crashdrv: {exc}", file=sys.stderr)
            return 2
        if not args.dry_run:
            print("crashdrv: --matrix without --dry-run expands the catalogue only; "
                  "use --run-matrix to RUN the phase-1 matrix.", file=sys.stderr)
            return 2
        return 0
    return run_scenario(args.scenario or "push-ack", args.point, args.nth, args.topology,
                        dry_run=args.dry_run, run_dir=args.run_dir, broker=args.broker)


if __name__ == "__main__":
    sys.exit(main())
