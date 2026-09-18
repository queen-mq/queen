"""Reporting for the kill campaigns (PLAN_RAFT.md §13.6, §15.0).

§13.6 asks for two numbers per run — "time to a new leader and client-visible
unavailability" — and for the checkers to pass. §0.3 asks that numbers be
reported with the command line, the duration and the host. This module is what
turns run results into something that can be pasted into RAFT_STATUS.md under
Measurements without a human re-typing anything.

Two rules are enforced here rather than trusted:

  1. A run that did not execute (a stub body, a crashed harness, a scheduling
     refusal) contributes NO numbers, and the summary says how many such runs
     there were. A median over "the runs that happened to work" is how a
     campaign reports 8 ms while the bad case is 4 s.
  2. A scenario with any failed run is FAILED in the summary, whatever the
     others did. Kill tests are looking for the rare case; averaging it away
     defeats the exercise.
"""

import json
import statistics
from dataclasses import dataclass, field, asdict

OK = "ok"
FAILED = "failed"
NOT_RUN = "not-run"
ERROR = "error"


@dataclass
class RunResult:
    scenario: str
    run: int
    status: str                      # ok | failed | not-run | error
    kill_offset_s: float = 0.0
    phase: str = ""
    durable_index: int = 0
    snapshot_epoch: int = 0
    leader_ms: float | None = None       # time to a new leader (None when not measured)
    unavailable_ms: float | None = None  # client-visible unavailability
    checks: dict = field(default_factory=dict)  # check id -> PASS | FAIL | SKIP
    note: str = ""

    def as_dict(self):
        return asdict(self)


@dataclass
class Campaign:
    """One campaign: several scenarios, several runs each, one host, one commit."""
    host: str
    commit: str
    started: str
    topology: str
    seed: int
    timing: dict
    command: str                      # the exact command line that produced it
    results: list = field(default_factory=list)

    def add(self, r: RunResult):
        self.results.append(r)

    # ----------------------------------------------------------------- summary

    def by_scenario(self):
        out = {}
        for r in self.results:
            out.setdefault(r.scenario, []).append(r)
        return out

    def summary_rows(self):
        rows = []
        for name, runs in self.by_scenario().items():
            executed = [r for r in runs if r.status in (OK, FAILED)]
            failed = [r for r in runs if r.status in (FAILED, ERROR)]
            not_run = [r for r in runs if r.status == NOT_RUN]
            leader = [r.leader_ms for r in executed if r.leader_ms is not None]
            unavail = [r.unavailable_ms for r in executed if r.unavailable_ms is not None]
            rows.append({
                "scenario": name,
                "runs": len(runs),
                "executed": len(executed),
                "not_run": len(not_run),
                "verdict": FAILED if failed else (OK if executed and not not_run else NOT_RUN),
                "leader_ms_p50": _p50(leader),
                "leader_ms_max": max(leader) if leader else None,
                "unavail_ms_p50": _p50(unavail),
                "unavail_ms_max": max(unavail) if unavail else None,
                "phases": sorted({r.phase for r in executed if r.phase}),
            })
        rows.sort(key=lambda r: r["scenario"])
        return rows

    def verdict(self):
        rows = self.summary_rows()
        if not rows:
            return NOT_RUN
        if any(r["verdict"] == FAILED for r in rows):
            return FAILED
        if any(r["verdict"] == NOT_RUN for r in rows):
            return NOT_RUN
        return OK

    # ------------------------------------------------------------------ output

    def text(self):
        lines = [
            f"kill campaign  host={self.host} commit={self.commit} topology={self.topology} "
            f"seed={self.seed} started={self.started}",
            f"  command: {self.command}",
            f"  timing:  {json.dumps(self.timing)}",
            "",
            f"  {'scenario':22s} {'runs':>4s} {'exec':>4s} {'n/run':>5s} {'verdict':>8s} "
            f"{'leader p50':>10s} {'leader max':>10s} {'unavail p50':>11s} {'unavail max':>11s}",
        ]
        for r in self.summary_rows():
            lines.append(
                f"  {r['scenario']:22s} {r['runs']:4d} {r['executed']:4d} {r['not_run']:5d} "
                f"{r['verdict']:>8s} {_ms(r['leader_ms_p50']):>10s} {_ms(r['leader_ms_max']):>10s} "
                f"{_ms(r['unavail_ms_p50']):>11s} {_ms(r['unavail_ms_max']):>11s}")
        not_run = sum(1 for x in self.results if x.status == NOT_RUN)
        if not_run:
            lines.append(f"\n  {not_run} of {len(self.results)} runs DID NOT EXECUTE; "
                         f"no number above includes them.")
        lines.append(f"\n  campaign verdict: {self.verdict().upper()}")
        return "\n".join(lines)

    def markdown(self):
        """The RAFT_STATUS.md Measurements row shape (§15.0): date, commit, host."""
        out = [
            f"### Kill campaign {self.started} — {self.topology} on {self.host} ({self.commit})",
            "",
            f"`{self.command}`",
            "",
            "| scenario | runs | executed | verdict | time to new leader p50 / max | client-visible unavailability p50 / max |",
            "|---|---:|---:|---|---|---|",
        ]
        for r in self.summary_rows():
            out.append(
                f"| {r['scenario']} | {r['runs']} | {r['executed']} | {r['verdict']} | "
                f"{_ms(r['leader_ms_p50'])} / {_ms(r['leader_ms_max'])} | "
                f"{_ms(r['unavail_ms_p50'])} / {_ms(r['unavail_ms_max'])} |")
        not_run = sum(1 for x in self.results if x.status == NOT_RUN)
        if not_run:
            out += ["", f"{not_run} of {len(self.results)} runs did not execute; the numbers above exclude them."]
        return "\n".join(out)

    def json(self):
        return json.dumps({
            "host": self.host, "commit": self.commit, "started": self.started,
            "topology": self.topology, "seed": self.seed, "timing": self.timing,
            "command": self.command, "verdict": self.verdict(),
            "summary": self.summary_rows(),
            "runs": [r.as_dict() for r in self.results],
        }, indent=2)


def _p50(xs):
    return round(statistics.median(xs), 1) if xs else None


def _ms(v):
    return "—" if v is None else f"{v:.0f} ms"
