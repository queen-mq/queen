"""pytest for the crash driver.

    python3 -m pytest test/raft/crash -q

The same assertions as `crashdrv.py --selftest`, one per test, plus the ones
that need pytest's machinery (capsys, raises).
"""

import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402

import crashdrv  # noqa: E402
import points  # noqa: E402
import scenarios  # noqa: E402


# --- the catalogue ------------------------------------------------------------

def test_every_point_of_section_135_is_declared():
    # The 24 points §13.5 lists, by name. This test is the contract with
    # rsm/faults.rs: when a point is renamed there, it is renamed here, and the
    # diff shows up in review instead of a silently untested code path.
    expected = {
        "batcher.drained", "planner.planned", "propose.sent",
        "log.appended", "log.flushed", "commit.before_apply", "apply.mid_entry",
        "apply.segment_written", "apply.store_committed",
        "durable.files_synced", "durable.store_committed",
        "snapshot.sealed", "snapshot.built", "snapshot.file_sent",
        "snapshot.install_staged", "snapshot.install_activated",
        "gc.before_unlink", "gc.after_unlink", "compaction.copied", "compaction.loc_committed",
        "transfer.started", "identity.written", "membership.learner_added", "membership.changed",
    }
    assert set(points.BY_NAME) == expected


def test_every_point_names_a_stage_and_an_invariant():
    for p in points.POINTS:
        assert p.stage, f"{p.name}: no stage"
        assert p.invariant, f"{p.name}: no invariant"
        assert p.group in points.GROUPS


def test_fault_spec_format():
    assert points.spec("log.flushed") == "log.flushed"
    assert points.spec("log.flushed", 4) == "log.flushed:4"
    with pytest.raises(KeyError):
        points.spec("nope")
    with pytest.raises(ValueError):
        points.spec("log.flushed", 0)


def test_fault_env_is_the_only_knob():
    env = crashdrv.fault_env("gc.after_unlink", 2)
    assert env == {"QUEEN_TEST_FAULTS": "gc.after_unlink:2"}


# --- the plan -----------------------------------------------------------------

def test_plan_carries_the_point_and_ends_with_the_checks():
    scen, point, actions = crashdrv.build_plan("push-ack", "apply.segment_written", 2, "raft1")
    assert scen.name == "push-ack" and point.name == "apply.segment_written"
    assert any("apply.segment_written:2" in a.what for a in actions)
    kinds = [a.kind for a in actions]
    assert kinds[0] == "start"
    assert "restart" in kinds, "a crash scenario that never restarts checks nothing"
    assert "check" in kinds, "a crash scenario with no check proves nothing"
    assert kinds[-1] == "scan", "§13.5 ends with: no error lines except the fault's own"


def test_plan_is_pure():
    a = crashdrv.build_plan("push-ack", "log.flushed", 1, "raft1")[2]
    b = crashdrv.build_plan("push-ack", "log.flushed", 1, "raft1")[2]
    assert a == b


@pytest.mark.parametrize("point", ["snapshot.install_staged", "membership.changed", "transfer.started"])
def test_raft3_only_points_are_refused_on_raft1(point):
    with pytest.raises(ValueError):
        crashdrv.build_plan("push-ack", point, 1, "raft1")


def test_raft3_only_scenarios_are_refused_on_raft1():
    with pytest.raises(ValueError):
        crashdrv.build_plan("snapshot-install", "log.flushed", 1, "raft1")


def test_unknown_names_are_refused():
    with pytest.raises(KeyError):
        crashdrv.build_plan("no-such-scenario", "log.flushed", 1, "raft1")
    with pytest.raises(KeyError):
        crashdrv.build_plan("push-ack", "no.such.point", 1, "raft1")


# --- the matrix ---------------------------------------------------------------

def test_matrix_is_scenarios_times_points():
    for topo in ("raft1", "raft3"):
        cells = scenarios.matrix(topo)
        assert len(cells) == len(scenarios.scenarios_for(topo)) * len(points.points_for(topo))
        assert len(set(cells)) == len(cells), "the matrix repeats a cell"


def test_raft3_matrix_is_larger_than_raft1():
    assert len(scenarios.matrix("raft3")) > len(scenarios.matrix("raft1"))


# --- the CLI ------------------------------------------------------------------

def test_dry_run_prints_the_plan_and_runs_nothing(capsys):
    rc = crashdrv.main(["--dry-run", "--scenario", "push-ack", "--point", "apply.mid_entry"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "QUEEN_TEST_FAULTS" in out and "apply.mid_entry" in out
    assert "dry run: nothing was started" in out
    assert "STUB" in out, "a stub scenario must say so in its dry run"


def test_list_points_marks_what_the_topology_supports(capsys):
    crashdrv.main(["--list-points", "--topology", "raft1"])
    out = capsys.readouterr().out
    assert "[x] apply.segment_written" in out.replace("    ", " ").replace("  ", " ")
    assert "[ ] snapshot.install_staged" in out.replace("    ", " ").replace("  ", " ")


def test_list_scenarios_says_what_each_stub_owes(capsys):
    crashdrv.main(["--list-scenarios"])
    out = capsys.readouterr().out
    for s in scenarios.SCENARIOS:
        assert s.name in out
        if not s.implemented:
            assert "owes:" in out


def test_running_a_stub_scenario_is_refused(capsys):
    rc = crashdrv.main(["--scenario", "push-ack", "--point", "log.flushed"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "documented stub" in err


def test_selftest_passes():
    assert crashdrv.selftest(out=io.StringIO()) == 0
