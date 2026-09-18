"""pytest for the kill runner.

    python3 -m pytest test/raft/kill -q

Most of these tests are about the SCHEDULE, because the schedule is the part
that decides whether a campaign proves anything: §13.6's whole lesson is that
pgless's kills never reached a periodic boundary.
"""

import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402

import killreport  # noqa: E402
import killrun  # noqa: E402
import killsched  # noqa: E402
import killscenarios  # noqa: E402


# --- the table ----------------------------------------------------------------

def test_table_is_the_section_136_list():
    assert set(killscenarios.BY_NAME) == {
        "kill9-leader", "kill9-follower", "sigstop-leader", "partition-leader", "wipe-follower",
        "clock-jump-back", "clock-jump-forward", "slow-disk-follower", "enospc-follower",
        "rolling-restart",
    }


def test_every_scenario_declares_proof_and_teardown():
    for s in killscenarios.SCENARIOS:
        assert s.proves, f"{s.name}: says nothing about what it proves"
        assert s.teardown, f"{s.name}: declares no teardown (§0.3: leave no loop or dm device behind)"
        if not s.implemented:
            assert s.owes, f"{s.name}: a stub with no owes line"


def test_root_scenarios_are_the_ones_that_touch_the_machine():
    root = {s.name for s in killscenarios.SCENARIOS if s.root_needed}
    assert root == {"partition-leader", "clock-jump-back", "clock-jump-forward",
                    "slow-disk-follower", "enospc-follower"}


def test_unknown_scenario_is_refused():
    with pytest.raises(KeyError):
        killscenarios.select(["no-such-scenario"])


# --- the scheduler ------------------------------------------------------------

def test_schedule_is_deterministic_in_seed_and_scenario():
    a = killsched.schedule("kill9-leader", 6, 42)
    assert a == killsched.schedule("kill9-leader", 6, 42)
    assert a != killsched.schedule("kill9-leader", 6, 43)
    assert a != killsched.schedule("kill9-follower", 6, 42), "two scenarios kill at identical instants"


def test_default_schedule_meets_section_136():
    kills = killsched.schedule("kill9-leader", killsched.MIN_RUNS, 7)
    killsched.validate(kills)  # raises if it does not
    cov = killsched.coverage(kills)
    assert cov["durable_intervals"] >= killsched.MIN_DURABLE_INTERVALS
    assert cov["snapshot_builds"] >= killsched.MIN_SNAPSHOT_BUILDS


def test_six_runs_cover_every_phase():
    kills = killsched.schedule("kill9-leader", 6, 7)
    assert {k.phase for k in kills} == set(killsched.PHASE_NAMES)


def test_no_kill_lands_in_the_warmup_or_past_the_end():
    t = killsched.Timing()
    for seed in range(20):
        for k in killsched.schedule("kill9-leader", 6, seed, t):
            assert t.warmup_s <= k.offset_s < t.duration_s, f"seed {seed}: kill at {k.offset_s}"


def test_snapshot_phases_land_around_a_build():
    t = killsched.Timing()
    for k in killsched.schedule("kill9-leader", 12, 3, t):
        start = t.snapshot_start(k.snapshot_epoch)
        if k.phase == "pre-snapshot":
            assert start - 2 * t.edge_s <= k.offset_s <= start
        elif k.phase == "during-snapshot":
            assert start <= k.offset_s <= start + t.snapshot_build_s
        elif k.phase == "post-snapshot":
            assert start + t.snapshot_build_s <= k.offset_s <= start + t.snapshot_build_s + t.edge_s


def test_the_pgless_schedule_is_refused():
    # A 25 s run whose first checkpoint is at 30 s: the mistake §13.6 names.
    narrow = killsched.Timing(warmup_s=5, duration_s=25, snapshot_every_s=30, snapshot_build_s=5)
    kills = killsched.schedule("kill9-leader", 6, 1, narrow)
    with pytest.raises(killsched.ScheduleTooNarrow) as exc:
        killsched.validate(kills, narrow)
    assert "snapshot build" in str(exc.value)


def test_too_few_runs_is_refused():
    kills = killsched.schedule("kill9-leader", 2, 1)
    with pytest.raises(killsched.ScheduleTooNarrow):
        killsched.validate(kills)


# --- the report ---------------------------------------------------------------

def _campaign():
    return killreport.Campaign(host="vm", commit="abc1234", started="2026-09-17T00:00:00+00:00",
                               topology="raft3", seed=1, timing={"duration_s": 300},
                               command="killrun.py --scenario kill9-leader --runs 5")


def test_unexecuted_runs_contribute_no_numbers():
    c = _campaign()
    c.add(killreport.RunResult("kill9-leader", 1, killreport.OK, leader_ms=1200, unavailable_ms=1500))
    c.add(killreport.RunResult("kill9-leader", 2, killreport.NOT_RUN))
    row = c.summary_rows()[0]
    assert row["executed"] == 1 and row["not_run"] == 1
    assert row["leader_ms_p50"] == 1200 and row["leader_ms_max"] == 1200
    assert c.verdict() == killreport.NOT_RUN
    assert "DID NOT EXECUTE" in c.text()
    assert "did not execute" in c.markdown()


def test_one_failure_fails_the_scenario_and_the_campaign():
    c = _campaign()
    for i in range(4):
        c.add(killreport.RunResult("kill9-leader", i + 1, killreport.OK, leader_ms=800, unavailable_ms=900))
    c.add(killreport.RunResult("kill9-leader", 5, killreport.FAILED, note="an acknowledged push was lost"))
    assert c.summary_rows()[0]["verdict"] == killreport.FAILED
    assert c.verdict() == killreport.FAILED


def test_markdown_carries_host_commit_and_command():
    c = _campaign()
    c.add(killreport.RunResult("kill9-leader", 1, killreport.OK, leader_ms=1000, unavailable_ms=1100))
    md = c.markdown()
    for want in ("vm", "abc1234", "killrun.py --scenario kill9-leader", "time to new leader"):
        assert want in md
    assert "|" in md and md.count("\n") > 3


def test_json_is_machine_readable():
    import json
    c = _campaign()
    c.add(killreport.RunResult("kill9-leader", 1, killreport.OK, leader_ms=1000))
    payload = json.loads(c.json())
    assert payload["host"] == "vm" and payload["summary"][0]["scenario"] == "kill9-leader"


# --- the CLI ------------------------------------------------------------------

def test_list_prints_every_row(capsys):
    assert killrun.main(["--list"]) == 0
    out = capsys.readouterr().out
    for s in killscenarios.SCENARIOS:
        assert s.name in out
    assert "teardown:" in out


def test_schedule_command_reports_coverage(capsys):
    rc = killrun.main(["--schedule", "--scenario", "kill9-leader", "--runs", "6"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "coverage:" in out and "OK for §13.6" in out


def test_schedule_command_fails_on_a_narrow_window(capsys):
    rc = killrun.main(["--schedule", "--scenario", "kill9-leader", "--runs", "6",
                       "--duration", "25", "--warmup", "5", "--snapshot-every", "30"])
    out = capsys.readouterr().out
    assert rc == 1, "a schedule that proves nothing exited 0"
    assert "REFUSED" in out


def test_dry_run_shows_the_plan(capsys):
    rc = killrun.main(["--dry-run", "--scenario", "kill9-leader", "--runs", "5"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "QUEEN_RAFT_DURABLE_EVERY_MS" in out
    assert "checker -strict" in out
    assert "STUB" in out


def test_a_real_campaign_is_refused_here(capsys):
    rc = killrun.main(["--scenario", "kill9-leader"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "preflight" in err or "stub" in err


def test_selftest_passes():
    assert killrun.selftest(out=io.StringIO()) == 0
