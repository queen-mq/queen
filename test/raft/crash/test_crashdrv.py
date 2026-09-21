"""pytest for the crash driver.

    python3 -m pytest test/raft/crash -q

The same assertions as `crashdrv.py --selftest`, one per test, plus the ones
that need pytest's machinery (capsys, raises).
"""

import io
import os
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402

import crashdrv  # noqa: E402
import points  # noqa: E402
import runner  # noqa: E402
import scenarios  # noqa: E402


# --- the catalogue ------------------------------------------------------------

def test_every_point_of_section_135_is_declared():
    # The 24 points §13.5 lists, by name, PLUS the 2 ALICE_PGLESS_NEWARCH §5
    # (Phase A3a) qlog WAL points. This test is the contract with rsm/faults.rs:
    # when a point is renamed there, it is renamed here, and the diff shows up in
    # review instead of a silently untested code path.
    section_135 = {
        "batcher.drained", "planner.planned", "propose.sent",
        "log.appended", "log.flushed", "commit.before_apply", "apply.mid_entry",
        "apply.segment_written", "apply.store_committed",
        "durable.files_synced", "durable.store_committed",
        "snapshot.sealed", "snapshot.built", "snapshot.file_sent",
        "snapshot.install_staged", "snapshot.install_activated",
        "gc.before_unlink", "gc.after_unlink", "compaction.copied", "compaction.loc_committed",
        "transfer.started", "identity.written", "membership.learner_added", "membership.changed",
    }
    qlog_a3a = {"qlog.record_written", "qlog.record_fsynced"}
    assert set(points.BY_NAME) == section_135 | qlog_a3a


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
    # push-ack is implemented now (WP-1.8), so its dry run does NOT carry the
    # scenario-stub status line (an unrelated action detail may mention STUB).
    assert "status    STUB" not in out


def test_dry_run_of_a_stub_scenario_says_so(capsys):
    rc = crashdrv.main(["--dry-run", "--scenario", "txn-riders", "--point", "log.flushed"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "status    STUB" in out, "a stub scenario must say so in its dry run"


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
    # txn-riders is still a stub (phase 2); running it must refuse, not pretend.
    rc = crashdrv.main(["--scenario", "txn-riders", "--point", "log.flushed"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "documented stub" in err


def test_push_ack_is_implemented_and_has_a_runner():
    import runner  # noqa: E402
    assert scenarios.BY_NAME["push-ack"].implemented
    assert crashdrv.IMPLEMENTED_RUNNERS.get("push-ack") is runner.run_push_ack


def test_phase_marks_the_wired_phase_one_points():
    # 13 §13.5 phase-1 points + 2 ALICE_PGLESS_NEWARCH §5 (Phase A3a) qlog points.
    wired = [p for p in points.POINTS if p.phase == 1]
    names = {p.name for p in wired}
    assert names == {
        "batcher.drained", "planner.planned", "propose.sent",
        "log.appended", "log.flushed", "commit.before_apply", "apply.mid_entry",
        "apply.segment_written", "apply.store_committed",
        "durable.files_synced", "durable.store_committed",
        "gc.before_unlink", "gc.after_unlink",
        "qlog.record_written", "qlog.record_fsynced",
    }


def test_selftest_passes():
    assert crashdrv.selftest(out=io.StringIO()) == 0


# --- the reproducibility gate (WP-1.8 refutation) -----------------------------

def _fake_binaries(root: Path, release_mtime: float, debug_mtime: float):
    """Two zero-byte stand-ins for the broker builds, with chosen mtimes."""
    for rel, mt in (("server/target/release/queen", release_mtime),
                    ("server/target/debug/queen", debug_mtime)):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"")
        os.utime(p, (mt, mt))


def test_find_broker_prefers_the_freshest_not_release(tmp_path, monkeypatch):
    # The refutation: find_broker preferred release unconditionally, so a stale
    # pre-raft release binary (left over from a measurement) was chosen over a
    # fresh raft-aware debug build and every cell failed at boot. It must prefer
    # the FRESHEST build.
    monkeypatch.delenv("QUEEN_BROKER", raising=False)
    monkeypatch.setattr(runner, "REPO_ROOT", tmp_path)
    # Debug is newer than release: debug must win.
    _fake_binaries(tmp_path, release_mtime=1000, debug_mtime=2000)
    assert runner.find_broker() == tmp_path / "server/target/debug/queen"
    # Release is newer than debug: release wins (a fresh release is fine).
    _fake_binaries(tmp_path, release_mtime=3000, debug_mtime=2000)
    assert runner.find_broker() == tmp_path / "server/target/release/queen"


def test_find_broker_honours_an_explicit_pin(tmp_path, monkeypatch):
    pinned = tmp_path / "mybroker"
    pinned.write_bytes(b"")
    monkeypatch.setenv("QUEEN_BROKER", str(pinned))
    assert runner.find_broker() == pinned
    # A pin that does not exist resolves to None (not a stale fallback).
    monkeypatch.setenv("QUEEN_BROKER", str(tmp_path / "nope"))
    assert runner.find_broker() is None


def test_assert_raft_aware_rejects_a_non_raft_binary(tmp_path):
    # A binary that does not boot raft mode (here: exits 1 with a FATAL, as the
    # stale pre-raft binary does at schema apply) must abort the WHOLE run with
    # BrokerNotRaftAware, and the broker's stderr tail must be IN the message —
    # never the old misleading "exited before health" with the FATAL line lost.
    fake = tmp_path / "fake-broker"
    fake.write_text("#!/bin/sh\necho 'FATAL: schema apply failed: not raft' >&2\nexit 1\n")
    fake.chmod(0o755)
    with pytest.raises(runner.BrokerNotRaftAware) as ei:
        runner.assert_raft_aware(str(fake), tmp_path / "run")
    msg = str(ei.value)
    assert "did not boot raft mode" in msg
    assert "FATAL: schema apply failed: not raft" in msg, "the stderr tail is not echoed"


# --- the ack-path exactly-once check (WP-1.8 coverage-gap fix) -----------------

def _min_go_clean_log(path: Path):
    """A minimal, Go-checker-clean run log: one orders push, its delivery, and
    the drain-complete note. The ack-path logic under test is driven by the
    Python args to _check, not by this log."""
    import json
    lines = [
        {"seq": 1, "kind": "push_ok", "writer": "crash", "queue": "orders",
         "txnId": "o0-0", "payloadHash": "aa", "offset": 0},
        {"seq": 2, "kind": "delivery", "writer": "crash", "queue": "orders",
         "group": "__QUEUE_MODE__", "txnId": "o0-0", "payloadHash": "aa", "offset": 0},
        {"seq": 3, "kind": "note", "writer": "crash", "text": "drain-complete",
         "group": "__QUEUE_MODE__", "queue": "orders"},
    ]
    path.write_text("".join(json.dumps(x) + "\n" for x in lines))


def _result_for_check():
    res = runner.Result(cell="c", scenario="push-ack", point="durable.files_synced",
                        nth=1, topology="raft1")
    res.stats["role_after_restart"] = "leader"
    res.stats["applied_after_restart"] = 1
    res.stats["applied_final"] = 1
    return res


def _orders_pushes():
    return {
        "o0-0": runner.PushRecord(txn="o0-0", payload={}, phash="aa",
                                  queue="orders", answered=True, status="queued", offset=0),
    }


_ORDERS_DELIVERED = [{"txn": "o0-0", "offset": 0, "hash": "aa"}]
# A held claim that redelivered once in the POST-LEASE drain: the positive
# control that makes the ack-path check trustworthy.
_GOOD_CONTROL = dict(held_immediate=[], held_late=[{"txn": "h1-0", "offset": 0, "hash": "cc"}],
                     claimed_held={"h1-0"})


def test_acked_not_redelivered_catches_a_resurrection_only_the_post_lease_drain_sees(tmp_path):
    # THE WP-1.8 REFUTATION, as a test. A completion wrongly resurrected as
    # leased is invisible to the immediate drain and appears ONLY once the lease
    # is out. The check must see the POST-LEASE warm drain (the caller passes the
    # immediate + late drains concatenated as `warm_delivered`) and FAIL. The old
    # immediate-only check could never see `w1-0` here, so it could never fail.
    log_path = tmp_path / "run.jsonl"
    _min_go_clean_log(log_path)
    broker = types.SimpleNamespace(_stderr_paths=[])
    res = _result_for_check()
    runner._check(res, broker, _orders_pushes(), _ORDERS_DELIVERED,
                  warm_delivered=[{"txn": "w1-0", "offset": 0, "hash": "bb"}],  # only in the LATE drain
                  held_immediate=_GOOD_CONTROL["held_immediate"],
                  held_late=_GOOD_CONTROL["held_late"],
                  popped={"w1-0"}, acked={"w1-0"},
                  claimed_held=_GOOD_CONTROL["claimed_held"], lease_waited=True,
                  cell_dir=tmp_path, log_path=log_path)
    assert res.verdict == "FAIL"
    assert "FAIL" in res.checks["acked-not-redelivered"]


def test_acked_not_redelivered_passes_only_when_a_control_confirms_the_window(tmp_path):
    # No resurrection AND the held control redelivered after the lease: a real,
    # window-confirmed PASS — not the vacuous "PASS (n acked)" the refutation hit.
    log_path = tmp_path / "run.jsonl"
    _min_go_clean_log(log_path)
    broker = types.SimpleNamespace(_stderr_paths=[])
    res = _result_for_check()
    runner._check(res, broker, _orders_pushes(), _ORDERS_DELIVERED,
                  warm_delivered=[],  # the completion did NOT come back, even post-lease
                  held_immediate=_GOOD_CONTROL["held_immediate"],
                  held_late=_GOOD_CONTROL["held_late"],
                  popped={"w1-0"}, acked={"w1-0"},
                  claimed_held=_GOOD_CONTROL["claimed_held"], lease_waited=True,
                  cell_dir=tmp_path, log_path=log_path)
    assert res.verdict == "PASS", res.reasons
    assert res.checks["acked-not-redelivered"].startswith("PASS")
    assert "control confirmed" in res.checks["acked-not-redelivered"]
    assert res.checks["claim-redelivered-after-lease"].startswith("PASS")


def test_claim_redelivered_after_lease_catches_a_lost_claim(tmp_path):
    # A claim held (never acked) across the crash MUST redeliver after the lease.
    # If it never comes back (neither drain), the recovery lost the claim: FAIL.
    log_path = tmp_path / "run.jsonl"
    _min_go_clean_log(log_path)
    broker = types.SimpleNamespace(_stderr_paths=[])
    res = _result_for_check()
    runner._check(res, broker, _orders_pushes(), _ORDERS_DELIVERED,
                  warm_delivered=[], held_immediate=[], held_late=[],  # never redelivered
                  popped=set(), acked=set(),
                  claimed_held={"h1-0"}, lease_waited=True,
                  cell_dir=tmp_path, log_path=log_path)
    assert res.verdict == "FAIL"
    assert res.checks["claim-redelivered-after-lease"] == "FAIL"


def test_claim_redelivered_catches_a_claim_that_came_back_before_the_lease(tmp_path):
    # A held claim that reappears in the IMMEDIATE drain (before its lease
    # expired) means recovery dropped the lease: FAIL. This is the falsifiable
    # half the immediate-only warm drain could not express.
    log_path = tmp_path / "run.jsonl"
    _min_go_clean_log(log_path)
    broker = types.SimpleNamespace(_stderr_paths=[])
    res = _result_for_check()
    runner._check(res, broker, _orders_pushes(), _ORDERS_DELIVERED,
                  warm_delivered=[],
                  held_immediate=[{"txn": "h1-0", "offset": 0, "hash": "cc"}],  # too early
                  held_late=[], popped=set(), acked=set(),
                  claimed_held={"h1-0"}, lease_waited=True,
                  cell_dir=tmp_path, log_path=log_path)
    assert res.verdict == "FAIL"
    assert res.checks["claim-redelivered-after-lease"] == "FAIL"


def test_ack_path_checks_are_na_when_no_claim_crossed_the_crash(tmp_path):
    # An early point that crashed before any pop: nothing claimed, nothing
    # completed. Both ack-path checks are honestly N/A — never a hollow PASS —
    # and no lease-out wait was needed.
    log_path = tmp_path / "run.jsonl"
    _min_go_clean_log(log_path)
    broker = types.SimpleNamespace(_stderr_paths=[])
    res = _result_for_check()
    runner._check(res, broker, _orders_pushes(), _ORDERS_DELIVERED,
                  warm_delivered=[], held_immediate=[], held_late=[],
                  popped=set(), acked=set(), claimed_held=set(), lease_waited=False,
                  cell_dir=tmp_path, log_path=log_path)
    assert res.verdict == "PASS", res.reasons
    assert res.checks["acked-not-redelivered"].startswith("N/A")
    assert res.checks["claim-redelivered-after-lease"].startswith("N/A")


def test_answered_then_delivered_excludes_warm_and_popped(tmp_path):
    # A warm push that was popped before the crash is NOT required to be
    # delivered by the post-crash drain (it may be completed, or leased for the
    # 60 s facade lease). Only judged, never-popped orders pushes are required.
    log_path = tmp_path / "run.jsonl"
    _min_go_clean_log(log_path)
    broker = types.SimpleNamespace(_stderr_paths=[])
    pushes = {
        "o0-0": runner.PushRecord(txn="o0-0", payload={}, phash="aa",
                                  queue="orders", answered=True, status="queued", offset=0),
        "w1-0": runner.PushRecord(txn="w1-0", payload={}, phash="bb",
                                  queue="warm", answered=True, status="queued", offset=0),
    }
    delivered = [{"txn": "o0-0", "offset": 0, "hash": "aa"}]  # warm NOT redelivered
    res = _result_for_check()
    runner._check(res, broker, pushes, delivered, warm_delivered=[],
                  held_immediate=[], held_late=[],
                  popped={"w1-0"}, acked={"w1-0"}, claimed_held=set(), lease_waited=True,
                  cell_dir=tmp_path, log_path=log_path)
    assert res.verdict == "PASS", res.reasons
    assert res.checks["answered-then-delivered"] == "PASS (1 judged)"
