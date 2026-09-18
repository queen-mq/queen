"""Crash scenarios: what the driver does around a fault (PLAN_RAFT.md §13.5).

A scenario is DATA, not code, until its body is written: a list of actions the
driver would take, in order, which `--dry-run` prints and the tests assert over.
That is deliberate. The five checks §13.5 requires after a restart

    every answered write delivered;
    unanswered writes at most once, and their retries exactly once;
    transaction riders all or nothing;
    one leader, equal applied indexes, equal state digests;
    no error lines except the fault's own

are only meaningful if the scenario RECORDED what it sent and what it was
answered, which is what the run log of test/raft/checker is for. Writing the
plan first makes the missing piece obvious in review: an action list with no
`log` action cannot support the check that follows it.

State of this file (WP-0.7): every scenario body is a stub. The driver, the
point catalogue, the plan and the reporting are real.
"""

from dataclasses import dataclass, field

import points


@dataclass(frozen=True)
class Action:
    """One step of a plan.

    kind is one of:
      start     start a topology, optionally with faults armed
      client    run a client workload that writes to the run log
      await     wait for something (the fault to fire, a leader, a durable point)
      kill      kill a process the driver started (never by port, §0.3)
      restart   start the same node again with no faults armed
      check     run test/raft/checker over the run log
      inspect   read something from the broker(s) and write it to the run log
      scan      scan broker stderr for error lines
    """
    kind: str
    what: str
    detail: str = ""


@dataclass(frozen=True)
class Scenario:
    name: str
    title: str
    topologies: tuple
    implemented: bool
    owes: str  # what the body still needs, printed by --list-scenarios
    plan: tuple

    def actions(self, point: str, nth: int, topology: str):
        """The plan with the point substituted in. Pure: the dry run prints
        exactly what the real run would do."""
        spec = points.spec(point, nth)
        out = []
        for a in self.plan:
            out.append(Action(a.kind,
                              a.what.replace("{point}", spec).replace("{topology}", topology),
                              a.detail.replace("{point}", spec).replace("{topology}", topology)))
        return out


_CHECKS_AFTER_RESTART = (
    Action("check", "checker -strict -check delivery-at-least-once,payload-hash",
           "§13.5: every answered write delivered; the run log's drain-complete notes scope it"),
    Action("check", "checker -strict -check txn-atomic  [STUB: §13.7]",
           "transaction riders all or nothing"),
    Action("inspect", "read applied index, leader and state digest from every node",
           "§13.5: one leader, equal applied indexes, equal state digests; written to the run log as digest events"),
    Action("scan", "scan every broker's stderr for error lines",
           "§13.5: no error lines except the fault's own"),
)


SCENARIOS = [
    Scenario(
        name="push-ack",
        title="pushes and acks in flight when the fault fires",
        topologies=("raft1", "raft3"),
        # WP-1.8: the raft1 body is written (runner.py). raft3 stays owed to
        # phase 4 (WP-4.10), so a raft3 run still refuses.
        implemented=True,
        owes="raft3 body (WP-4.10); the gc.* points need retention (WP-2.7)",
        plan=(
            Action("start", "start {topology} with QUEEN_TEST_FAULTS={point}",
                   "one data dir per node under the run directory; never a shared one"),
            Action("client", "push, pop and ack a bounded workload, logging every send and every answer",
                   "an unanswered write is logged as push_unanswered: it is the D6 case the checks judge"),
            Action("await", "wait for the faulted process to exit (bounded; a point that never fires is a FAILED run, not a pass)",
                   "a point that does not fire means the code path was not taken: report it, never skip it"),
            Action("restart", "restart the faulted node with no faults armed, wait for health",
                   "raft1: it must recover alone; raft3: it must catch up from the leader"),
            Action("client", "retry every unanswered write with its ORIGINAL request id, then drain every queue",
                   "D6: the retry must return the recorded outcome, not a second message; the drain writes the "
                   "drain-complete notes the at-least-once check needs"),
        ) + _CHECKS_AFTER_RESTART,
    ),
    Scenario(
        name="txn-riders",
        title="a transaction bundle with KV and timer riders in flight",
        topologies=("raft1", "raft3"),
        implemented=False,
        owes="the bundle workload and the all-or-nothing read-back (every rider read after the restart)",
        plan=(
            Action("start", "start {topology} with QUEEN_TEST_FAULTS={point}", ""),
            Action("client", "send transaction bundles (pushes + acks + kv riders + timer riders), logging txn_committed / txn_failed with txnRefs", ""),
            Action("await", "wait for the faulted process to exit", ""),
            Action("restart", "restart with no faults armed, wait for health", ""),
            Action("client", "read back every member of every bundle and log it", ""),
        ) + _CHECKS_AFTER_RESTART,
    ),
    Scenario(
        name="gc-compaction",
        title="file GC and compaction while entries keep applying",
        topologies=("raft1", "raft3"),
        implemented=False,
        owes="a workload that produces deletable files (retention on, short window) and a post-restart "
             "read of every message still referenced (I10)",
        plan=(
            Action("start", "start {topology} with QUEEN_TEST_FAULTS={point} and a short retention window", ""),
            Action("client", "push, consume and let retention delete, logging every answer", ""),
            Action("await", "wait for the faulted process to exit", ""),
            Action("restart", "restart with no faults armed, wait for health", ""),
            Action("client", "read every message the state still references", "I10: a file unlinked too early shows up here"),
        ) + _CHECKS_AFTER_RESTART,
    ),
    Scenario(
        name="snapshot-install",
        title="a follower installing a snapshot when the fault fires",
        topologies=("raft3",),
        implemented=False,
        owes="the follower-behind setup (stop a follower, write past the log retention, restart it) and the "
             "CURRENT-file assertion of I17",
        plan=(
            Action("start", "start raft3 with QUEEN_TEST_FAULTS={point} on the follower under test", ""),
            Action("client", "write enough to force a snapshot transfer to that follower", ""),
            Action("await", "wait for the faulted follower to exit", ""),
            Action("inspect", "assert the live state directory is the one CURRENT names, and that it is intact",
                   "I17: a crash mid-install leaves the OLD state live"),
            Action("restart", "restart the follower with no faults armed", ""),
            Action("await", "wait for it to catch up to the leader's applied index", ""),
        ) + _CHECKS_AFTER_RESTART,
    ),
    Scenario(
        name="identity-membership",
        title="identity and membership changes interrupted by the fault",
        topologies=("raft3",),
        implemented=False,
        owes="the join/replace flow (add learner, promote, remove) and the I9 assertion that a wiped dir "
             "never votes",
        plan=(
            Action("start", "start raft3 with QUEEN_TEST_FAULTS={point}", ""),
            Action("client", "run the node replacement flow (remove, wipe, add as learner, promote)", ""),
            Action("await", "wait for the faulted process to exit", ""),
            Action("restart", "restart it with no faults armed", ""),
            Action("inspect", "assert IDENTITY matches the membership record, and that a wiped dir refused to vote", "I9"),
        ) + _CHECKS_AFTER_RESTART,
    ),
]

BY_NAME = {s.name: s for s in SCENARIOS}


def scenarios_for(topology: str):
    return [s for s in SCENARIOS if topology in s.topologies]


def matrix(topology: str, scenario_names=None):
    """The (scenario, point) pairs a full crash-matrix run would cover.

    §13.5 says "arm each point": the matrix is every point a topology supports,
    crossed with every scenario that can exercise it. It is printed, and its
    size reported, before anything runs — a 100-cell matrix at 60 s a cell is a
    100-minute run, and that is a number to know in advance, not to discover.
    """
    scens = scenarios_for(topology)
    if scenario_names:
        scens = [s for s in scens if s.name in scenario_names]
        missing = set(scenario_names) - {s.name for s in scens}
        if missing:
            raise KeyError(f"unknown or unsupported scenario(s) for {topology}: {', '.join(sorted(missing))}")
    out = []
    for s in scens:
        for p in points.points_for(topology):
            out.append((s.name, p.name))
    return out
