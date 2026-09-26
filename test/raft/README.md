# test/raft — the verification harnesses for the raft storage class

Everything PLAN_RAFT.md §13 asks for that is not a Rust test lives here. The
plan's rule (§0.3) is the reason this directory exists at all:

> Harnesses, scripts, fixtures and checkers live in the repo under
> `test/raft/`, never only in `/private/tmp` (pgless lost its harness that way).

Five harnesses, created by WP-0.7 (phase 0, §15). Each one is a skeleton: the
part that decides whether a run PROVES anything — the operation catalogue, the
run-log format, the crash-point list, the kill schedule, the acceptance
thresholds — is written and tested; the parts that need a raft broker that does
not exist yet are documented stubs that refuse to run rather than report
nothing. Every tool prints what it still owes (`--list-ops`, `--list-checks`,
`--list-points`, `--list`, the `STUB` lines in a dry run).

| directory | what it is | plan section | state |
|---|---|---|---|
| `checker/` | run-log checkers: the invariants that judge any harness's log | §13.7 | at-least-once and payload-hash implemented; 7 checks are documented stubs |
| `crash/` | crash-point driver: arm `QUEEN_TEST_FAULTS`, drive a scenario, restart, check | §13.5 | 24 points, 5 scenarios, plan/matrix/dry run real; scenario bodies stubs |
| `kill/` | VM kill campaigns: table of scenarios, stratified kill times, reporting | §13.6 | 10 scenarios, scheduler and reporting real; scenario bodies stubs |
| `flatness/` | the G-3 / I8 test: bulk preload, regime runs, comparator | §13.6 | RESULTS format, comparator and preload CLI real; send loop and regime runner stubs |

Three neighbours in this directory belong to other work packages and are not
described here: `spikes/` (the phase-0 spikes S1–S4), `vm/` (the WP-0.2
postgres baseline on the Linux VM) and `lint/` (WP-1.4:
`lint/deny-bites.sh` proves that the I2 pin in `server/clippy.toml` and the
`#![deny(clippy::disallowed_methods)]` in `rsm/apply.rs`, `rsm/state/` and
`rsm/store/` actually refuse a clock, an environment read and a random number —
it appends one call of each, runs clippy, and restores the file).

## Running them

Go tools: one module each, no vendoring, standard library only, and **always
`GOWORK=off`** — the repo's `go.work` lists only `clients/client-cli` and
`clients/client-go`, so a `go` command without it fails here.

```sh
cd test/raft/checker  && GOWORK=off go test ./... && GOWORK=off go run . -selftest
cd test/raft/flatness && GOWORK=off go test ./... && GOWORK=off go run . selftest
```

Python tools: standard library and `pytest`, no virtualenv, run from the repo
root:

```sh
python3 -m pytest test/raft/crash test/raft/kill -q
python3 test/raft/crash/crashdrv.py --selftest
python3 test/raft/kill/killrun.py --selftest
```

Every tool also answers `-h` / `--help`, and each has a `--selftest` that runs
the same assertions as its test suite without a test runner — the Linux VM of
§13.6 has no pytest.

Exit codes are the same everywhere, because CI and the other harnesses read
them: **0** clean, **1** the thing being judged failed (a divergence, a
violated invariant, a comparison outside the thresholds), **2** the tool could
not run (bad flags, no broker, a stub).

## checker — §13.7

Judges a **run log**: newline-delimited JSON, one event per line, written by
whichever harness produced the run (`log.go` is the format; `Writer` is the
helper harnesses use). One set of checkers for every harness, and a log that
can be re-judged later by a newer checker.

```sh
cd test/raft/checker
GOWORK=off go run . -list-checks
GOWORK=off go run . -log /path/to/run.jsonl -strict
```

Three verdicts, never two: PASS, FAIL and **SKIP with a reason**. A check whose
input the log does not contain says so instead of passing — `-strict` turns
SKIP into a failure, which is what the crash and kill runners use, because a
run that cannot be judged proved nothing. An unknown event kind fails the load
rather than being skipped.

Implemented: every acknowledged push delivered at least once (scoped by the
`drain-complete` notes the harness writes), and no payload mismatch (including
"delivered something that was never pushed" and "delivered a push the broker
refused").
Owed: offsets monotone per lease, transaction atomicity, KV linearizability
(porcupine), timer generations, streams counts, delivery after a partition
delete, DLQ payloads.

## crash — §13.5

Python (process supervision and a call to the Go checker; the tools that touch
the data path are Go).

```sh
python3 test/raft/crash/crashdrv.py --list-points --topology raft3
python3 test/raft/crash/crashdrv.py --list-scenarios
python3 test/raft/crash/crashdrv.py --dry-run --scenario push-ack --point apply.segment_written --nth 2
python3 test/raft/crash/crashdrv.py --dry-run --matrix --topology raft3
```

`points.py` is the spec side of the `QUEEN_TEST_FAULTS="point[:nth],…"`
contract: all 24 points of §13.5, each with the state of the world when it
fires, the invariants it tests, and the topologies that can arm it. It exists
before `rsm/faults.rs` does, and `test_crashdrv.py` pins the names, so a
renamed point shows up in review instead of quietly ceasing to be tested.

A dry run prints the environment it would set and every step it would take, and
the plan always ends the way §13.5 does: checks, then a scan of the broker's
stderr for error lines other than the fault's own. Owed: the scenario bodies
(the workloads, the restarts, the digest comparison).

## kill — §13.6

Python, table-driven, **for the Linux VM only** (`root@164.90.215.224`):
several scenarios need root and change machine-wide state — iptables rules, the
clock, loop and dm devices — and macOS has none of them. Each row declares what
it proves and what it must tear down.

```sh
python3 test/raft/kill/killrun.py --list
python3 test/raft/kill/killrun.py --schedule --scenario kill9-leader --runs 6
python3 test/raft/kill/killrun.py --dry-run --scenario rolling-restart
```

The scheduler is the part that matters. §13.6:

> pgless's kill tests killed at 25 s, before the first 30 s checkpoint, and
> never exercised checkpoints. Kill schedules must cover every periodic
> boundary.

So kill times are **stratified**, not uniform: each run is assigned a phase —
mid-interval, just before and just after a durable point, just before, inside
and just after a snapshot build — and jitters inside it, deterministically from
the seed. `validate()` refuses a schedule that does not reach at least 3
durable-point intervals and 2 snapshot builds over at least 5 runs, and says
what is missing; a 25-second run whose first build is at 30 s is refused with
that sentence quoted.

The reporter (`killreport.py`) records the two numbers §13.6 asks for — time to
a new leader, client-visible unavailability — and enforces two rules: a run
that did not execute contributes no numbers (and the report says how many there
were), and one failed run fails the scenario. It emits text, JSON, and the
markdown table for RAFT_STATUS.md.

## flatness — §13.6 (G-3, I8)

```sh
cd test/raft/flatness
GOWORK=off go run . preload -dry-run -url http://vm:6632 -state /root/raft/preload.state
GOWORK=off go run . compare -baseline examples/EXAMPLE-empty-A20k.results \
                            -candidate examples/EXAMPLE-preloaded-A20k.results
```

One run of one regime against one store state writes one **RESULTS** file
(format in `results.go`: a `# key=value` header carrying regime, state, host,
commit, command and duration, then `metric value` lines). The comparator reads
two of them — empty store versus preloaded — so a flatness result can be
re-judged later without re-running the hours of preload.

Thresholds, from §13.6 and I8:

- every gated metric within **±15%** of the empty-store run: `p50_ms`,
  `p99_ms`, `p999_ms`, `ack_rtt_ms`, `cpu_cores`, `disk_mbps`,
  `durable_point_ms`;
- RSS flat within **±5%** across the run (`rss_start_mb` vs `rss_end_mb`),
  checked inside EACH run, which is the half a metric-by-metric comparison
  misses;
- `snapshot_build_s` is **reported and not gated**: I8 exempts a snapshot build
  from the flatness rule (it may cost O(state));
- throughput-shaped metrics are two-sided — a run that moved far fewer bytes
  did less work and is not a pass;
- a metric present on only one side, or two files with different regime,
  topology or commit, or two files with the same `state`, is an **error**, not
  a skip.

The `examples/` files are invented numbers that demonstrate the format and a
failing verdict; they are labelled as such in their own header and are not a
measurement. Real numbers go in RAFT_STATUS.md with their host and commit.

Note: `vm/baseline/RESULTS.md` (WP-0.2) is prose for humans; the `.results`
format here is the machine-readable one this comparator reads. A future regime
runner should write both.

Owed: the preloader's send loop (partition walk with the state file, pacing,
per-item status checking — a 201 alone proves nothing) and the regime runner
that drives goload and writes the RESULTS file.

## Conventions

- **No dependencies.** Standard library only, in both languages: these tools
  run on a VM that is rsynced without a module cache. The one planned exception
  is `porcupine` for the KV linearizability check, and it is not added until
  that check is written.
- **Nothing is reported that was not measured.** Every tool distinguishes "did
  not run" from "passed", refuses to average over runs that did not happen, and
  prints what it skipped and why.
- **Determinism is the contract.** A seed plus a config reproduces a run; the
  generators and schedulers are pure functions of them, and the tests pin that.
- **Numbers carry their host, commit and command line** (§0.3), which is why
  both the RESULTS header and the kill report require them.
