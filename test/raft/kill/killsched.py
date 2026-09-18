"""Kill-time scheduling for the VM kill tests (PLAN_RAFT.md §13.6).

THE LESSON THIS FILE EXISTS FOR, quoted from §13.6:

    pgless's kill tests killed at 25 s, before the first 30 s checkpoint, and
    never exercised checkpoints. Kill schedules must cover every periodic
    boundary.

A kill at a uniformly random time is NOT good enough: the interesting moments
are a few tens of milliseconds wide (just before a durable point, inside a
snapshot build, just after a file is unlinked) and uniform sampling lands in
them by accident, if ever. So the schedule is STRATIFIED: each run is assigned a
phase relative to the periodic work, and the jitter happens inside that phase.

The periodic work, with the knobs of Appendix H:

    durable point   every QUEEN_RAFT_DURABLE_EVERY_MS (1000) or 256 MiB
    snapshot build  every QUEEN_RAFT_SNAPSHOT_MAX_INTERVAL_S (1800) or 4 GiB of log
                    — a kill run overrides it (60 s) so a 10-minute run covers
                    several builds; the override is recorded in the report,
                    because a run that covered boundaries only by shrinking them
                    has to say so.

The §13.6 requirement is a MINIMUM, and `validate` enforces it: kill times
randomized across at least 3 durable-point intervals and 2 snapshot builds.
"""

import random
from dataclasses import dataclass, asdict

# Phases, in the order they are assigned. Each says where in the periodic cycle
# the kill lands and what it is there to break.
PHASES = (
    ("mid-durable", "halfway between two durable points: the ordinary case"),
    ("pre-durable", "just before a durable point: applied entries that are not durable yet (I11)"),
    ("post-durable", "just after a durable point: the store commit landed, the next entries did not"),
    ("pre-snapshot", "just before a snapshot build starts: sealing is about to roll every active file"),
    ("during-snapshot", "inside a snapshot build: sealed files, manifest half written (I10, I17)"),
    ("post-snapshot", "just after a build completes: a fresh manifest pins files GC wants"),
)
PHASE_NAMES = tuple(name for name, _ in PHASES)


@dataclass(frozen=True)
class KillTime:
    """When one run kills, and why that moment."""
    run: int
    offset_s: float        # seconds after the workload starts
    phase: str
    durable_index: int     # which durable-point interval the kill falls in
    snapshot_epoch: int    # which snapshot build it is relative to
    why: str

    def as_dict(self):
        return asdict(self)


class ScheduleTooNarrow(Exception):
    """Raised when a schedule does not cover what §13.6 requires."""


@dataclass(frozen=True)
class Timing:
    """The periodic model a schedule is built against. Every field is a number
    the run must actually configure; the report prints them next to the results,
    so a schedule can never be read without the boundaries it assumed."""
    warmup_s: float = 20.0            # workload ramp before any kill is allowed
    duration_s: float = 300.0         # one run's workload length
    durable_every_s: float = 1.0      # QUEEN_RAFT_DURABLE_EVERY_MS = 1000
    snapshot_every_s: float = 60.0    # QUEEN_RAFT_SNAPSHOT_MAX_INTERVAL_S override for tests
    snapshot_build_s: float = 5.0     # measured build time; update it from a real run
    edge_s: float = 0.05              # how close "just before/after" is

    def snapshot_start(self, epoch: int) -> float:
        """When snapshot build `epoch` starts (epoch 1 is the first one)."""
        return self.warmup_s + epoch * self.snapshot_every_s

    def max_snapshot_epoch(self) -> int:
        n = 0
        while self.snapshot_start(n + 1) + self.snapshot_build_s < self.duration_s:
            n += 1
        return n


def schedule(scenario: str, runs: int, seed: int, timing: Timing = Timing()) -> list:
    """The kill times for one scenario: deterministic in (scenario, seed).

    Deterministic on purpose: a kill run that found a bug has to be repeatable,
    and "we killed at a random time" is not a bug report. The scenario name is
    mixed into the seed so that two scenarios in one campaign do not kill at
    exactly the same instants (which would correlate their results).
    """
    if runs < 1:
        raise ValueError("runs must be >= 1")
    rnd = random.Random(f"{seed}:{scenario}")
    max_epoch = timing.max_snapshot_epoch()
    out = []
    for i in range(runs):
        phase, why = PHASES[i % len(PHASES)]
        # Phase and snapshot epoch advance on DIFFERENT cycles (6 phases, N
        # builds), so a campaign of 5-6 runs covers several builds instead of
        # hammering the first one — which is how "2 snapshot builds" (§13.6)
        # is actually met at the minimum run count.
        epoch = 1 + (i % max(max_epoch, 1))
        offset = _offset_for(phase, epoch, i, rnd, timing)
        offset = min(max(offset, timing.warmup_s), timing.duration_s - 1.0)
        durable_index = int(offset / timing.durable_every_s)
        out.append(KillTime(run=i + 1, offset_s=round(offset, 3), phase=phase,
                            durable_index=durable_index, snapshot_epoch=epoch, why=why))
    return out


def _offset_for(phase, epoch, i, rnd, t: Timing) -> float:
    snap = t.snapshot_start(epoch)
    if phase == "pre-snapshot":
        return snap - t.edge_s - rnd.uniform(0, t.edge_s)
    if phase == "during-snapshot":
        return snap + rnd.uniform(0.1, max(t.snapshot_build_s - 0.1, 0.2))
    if phase == "post-snapshot":
        return snap + t.snapshot_build_s + rnd.uniform(0, t.edge_s)
    # Durable-point phases: pick a durable interval that is NOT adjacent to a
    # snapshot build, so the phase means what it says.
    span_lo = t.warmup_s + 1.0
    span_hi = max(snap - 2.0, span_lo + t.durable_every_s * 3)
    k = rnd.randrange(int(span_lo / t.durable_every_s), max(int(span_hi / t.durable_every_s), int(span_lo / t.durable_every_s) + 1))
    boundary = (k + 1) * t.durable_every_s
    if phase == "pre-durable":
        return boundary - rnd.uniform(0.001, t.edge_s)
    if phase == "post-durable":
        return boundary + rnd.uniform(0.001, t.edge_s)
    return boundary - t.durable_every_s / 2 + rnd.uniform(-0.1, 0.1)  # mid-durable


def coverage(kills: list) -> dict:
    """What a schedule actually covers. Printed with every campaign."""
    return {
        "runs": len(kills),
        "durable_intervals": len({k.durable_index for k in kills}),
        "snapshot_builds": len({k.snapshot_epoch for k in kills}),
        "phases": sorted({k.phase for k in kills}),
        "first_offset_s": min((k.offset_s for k in kills), default=0.0),
        "last_offset_s": max((k.offset_s for k in kills), default=0.0),
    }


MIN_DURABLE_INTERVALS = 3   # §13.6
MIN_SNAPSHOT_BUILDS = 2     # §13.6
MIN_RUNS = 5                # §13.6: "each scenario >= 5 runs"


def validate(kills: list, timing: Timing = Timing(), min_runs: int = MIN_RUNS) -> None:
    """Refuse a schedule that would repeat the pgless mistake.

    Raises ScheduleTooNarrow naming exactly what is missing. This is called
    BEFORE a campaign starts: discovering after two hours of killing that every
    kill landed in the same interval is the failure mode this guards.
    """
    cov = coverage(kills)
    problems = []
    if cov["runs"] < min_runs:
        problems.append(f"{cov['runs']} run(s), §13.6 requires at least {min_runs} per scenario")
    if cov["durable_intervals"] < MIN_DURABLE_INTERVALS:
        problems.append(f"kills land in {cov['durable_intervals']} durable-point interval(s), "
                        f"§13.6 requires at least {MIN_DURABLE_INTERVALS}")
    if cov["snapshot_builds"] < MIN_SNAPSHOT_BUILDS:
        problems.append(f"kills cover {cov['snapshot_builds']} snapshot build(s), "
                        f"§13.6 requires at least {MIN_SNAPSHOT_BUILDS} "
                        f"(the run is {timing.duration_s:g}s and a build starts every "
                        f"{timing.snapshot_every_s:g}s: lengthen the run or shorten the interval)")
    if any(k.offset_s < timing.warmup_s for k in kills):
        problems.append("a kill lands inside the warm-up, where the workload has not reached steady state")
    if any(k.offset_s >= timing.duration_s for k in kills):
        problems.append("a kill lands after the workload ends")
    if problems:
        raise ScheduleTooNarrow(
            "; ".join(problems) +
            " — pgless killed at 25 s, before its first 30 s checkpoint, and never exercised checkpoints (§13.6)")
