"""The kill scenarios of PLAN_RAFT.md §13.6, as a table.

    scenarios: kill -9 the leader; kill -9 a follower; SIGSTOP the leader for
    5 s; partition the leader (iptables drop on 6634); wipe a follower's disk
    and run the replacement flow; clock jump -10 s and +10 s on the leader
    (leases and TTLs must follow §7.4, the skew alarm must fire); slow disk on
    a follower (dm-delay 50 ms); ENOSPC on a follower (small loop device);
    rolling restart with leadership transfer

Each row carries what it does, what it must prove, what it cleans up, and what
the body still owes (WP-0.7 ships the table, the scheduler and the reporting;
the bodies are stubs).

CLEANUP IS PART OF THE TABLE, not an afterthought: §0.3 says leave no process,
loop device or dm device behind on the VM. A scenario that creates one declares
it here, and the runner refuses to start a campaign whose previous run left
cleanup pending.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class KillScenario:
    name: str
    title: str          # the §13.6 phrase
    target: str         # leader | follower | cluster
    topology: str       # raft3 for all but the single-node smoke
    implemented: bool
    proves: str         # what a passing run establishes
    teardown: str       # what it must remove afterwards ("nothing" is an answer)
    owes: str           # what the body still needs
    root_needed: bool = False  # iptables, dm-setup, losetup need root on the VM


SCENARIOS = [
    KillScenario(
        name="kill9-leader", title="kill -9 the leader", target="leader", topology="raft3",
        implemented=False,
        proves="a new leader inside the election timeout, no acknowledged write lost, "
               "unanswered writes at most once and their retries exactly once (D6)",
        teardown="the killed process is gone; its data dir is kept for the post-mortem",
        owes="process discovery (which pod is leader), the kill, and the wait for a new leader",
    ),
    KillScenario(
        name="kill9-follower", title="kill -9 a follower", target="follower", topology="raft3",
        implemented=False,
        proves="no client-visible effect at all, and the follower catches up from its own log "
               "or a snapshot (I11)",
        teardown="none",
        owes="the same, minus the leader wait; plus the catch-up assertion on applied index",
    ),
    KillScenario(
        name="sigstop-leader", title="SIGSTOP the leader for 5 s", target="leader", topology="raft3",
        implemented=False,
        proves="the quorum-check lease (D14) makes the frozen leader step down; the cluster elects "
               "another; when SIGCONT arrives the old leader does not answer as leader (I13)",
        teardown="SIGCONT is sent even when the run fails (a stopped broker holds its data dir lock)",
        owes="the stop/cont pair with a guaranteed SIGCONT, and the split-brain assertion",
    ),
    KillScenario(
        name="partition-leader", title="partition the leader (iptables drop on 6634)", target="leader",
        topology="raft3", implemented=False, root_needed=True,
        proves="the minority leader stops answering writes within the hold window (D13) and the "
               "majority elects a new one; no split brain",
        teardown="every iptables rule this scenario added is removed, including on failure",
        owes="the rule add/remove (by comment tag so cleanup is exact) and the two-sided client check",
    ),
    KillScenario(
        name="wipe-follower", title="wipe a follower's disk and run the replacement flow",
        target="follower", topology="raft3", implemented=False,
        proves="I9: an empty data dir never votes; the node rejoins only as a new member "
               "(remove, add as learner, promote)",
        teardown="the wiped data dir is recreated empty; the membership is back to 3 voters",
        owes="the admin calls of the replacement flow and the assertion that the wiped dir refused to vote",
    ),
    KillScenario(
        name="clock-jump-back", title="clock jump -10 s on the leader", target="leader", topology="raft3",
        implemented=False, root_needed=True,
        proves="I5: planner time is monotone across the jump; leases and TTLs follow §7.4; "
               "the skew alarm fires (QUEEN_RAFT_CLOCK_SKEW_ALARM_MS)",
        teardown="the clock is restored and NTP is re-enabled",
        owes="the clock change (it moves the whole VM's clock: it must be the only job on the box) "
             "and the lease/TTL assertions",
    ),
    KillScenario(
        name="clock-jump-forward", title="clock jump +10 s on the leader", target="leader", topology="raft3",
        implemented=False, root_needed=True,
        proves="TTLs and leases expire at the leader-stamped now, not at a follower's clock; "
               "the skew alarm fires",
        teardown="the clock is restored and NTP is re-enabled",
        owes="as above",
    ),
    KillScenario(
        name="slow-disk-follower", title="slow disk on a follower (dm-delay 50 ms)", target="follower",
        topology="raft3", implemented=False, root_needed=True,
        proves="I15: /health, /metrics and ephemeral endpoints stay responsive; the leader does not "
               "stall on the slow follower beyond the quorum it needs",
        teardown="the dm device is removed and the loop device detached, in that order",
        owes="the dm-delay setup on a loop-backed data dir and the responsiveness probes",
    ),
    KillScenario(
        name="enospc-follower", title="ENOSPC on a follower (small loop device)", target="follower",
        topology="raft3", implemented=False, root_needed=True,
        proves="§11.8: the planner refuses pushes with 507 above the high watermark using the "
               "HIGHEST reported usage, and service resumes below the low one; ENOSPC on the "
               "consensus log stops that node rather than corrupting it",
        teardown="the loop device is detached and its backing file removed",
        owes="the loop device, the fill, and the 507 / recovery assertions",
    ),
    KillScenario(
        name="rolling-restart", title="rolling restart with leadership transfer", target="cluster",
        topology="raft3", implemented=False,
        proves="G-4: zero-downtime deploys; every restart transfers leadership first (§12.7) and "
               "client-visible unavailability stays under the hold window",
        teardown="every node is back up and the membership is unchanged",
        owes="the ordered restart with a transfer before each, and the unavailability measurement",
    ),
]

BY_NAME = {s.name: s for s in SCENARIOS}


def names():
    return [s.name for s in SCENARIOS]


def select(names_wanted=None):
    if not names_wanted:
        return list(SCENARIOS)
    unknown = [n for n in names_wanted if n not in BY_NAME]
    if unknown:
        raise KeyError(f"unknown scenario(s): {', '.join(unknown)} (known: {', '.join(names())})")
    return [BY_NAME[n] for n in names_wanted]
