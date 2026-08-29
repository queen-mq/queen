# Horizon vs Queen PHP vs Queen Rust

## Final diagnostic report

- Campaign date: August 28, 2026
- Branch: `feat/laravel-horizon-replacement`
- Commit: `473d8bfa7f0fe4be5b250e259a22719309bd4e01`
- Classification: **diagnostic**, not yet publishable

This document interprets the complete campaign. The [generated report](report.md),
the [JSON report](report.json), and each lane's summaries and events remain the
source of per-run values.

## Executive summary

1. The campaign is internally valid: 18/18 correct lanes, 36,000/36,000 jobs
   completed, zero missing, duplicate, failed, or retried jobs, zero OOMs, and
   zero restarts. PSS sampling of orchestrators and workers has 100% coverage.
2. The defect found in the first campaign was corrected before using the
   results: Horizon, Queen PHP, and Queen Rust now all start with four workers
   in the fixed profile. The pre-fix campaign is excluded from the verdict.
3. The strongest and most stable result is control-plane RAM. In the fixed
   profile, median PSS is 61.7 MiB for Horizon, 33.5 MiB for Queen PHP, and
   2.87 MiB for Queen Rust. In the auto profile it is 68.1, 38.8, and 3.05 MiB.
4. Queen Rust therefore reduces orchestrator PSS by about 95.3–95.5% versus
   Horizon and by 91.4–92.1% versus the PHP supervisor.
5. Queen does not yet match Horizon's end-to-end performance in this workload.
   In the fixed profile, paired comparisons put Queen PHP at about 49.3% and
   Queen Rust at 50.6% of Horizon throughput. In the auto profile, the values
   are 55.5% and 56.5%.
6. PHP and Rust share the worker/client/broker/PostgreSQL path and have similar
   fixed-profile performance. Together with equivalent application work, this
   indicates that the main gap is in the queue fetch/ACK/backend path, not the
   orchestrator.
7. The orchestrator RAM advantage is not yet enough to reduce full-stack RAM:
   Redis uses much less than the Queen broker plus PostgreSQL. The median fixed
   stack is 187.1 MiB for Horizon, 303.0 MiB for Queen PHP, and 274.1 MiB for
   Queen Rust.
8. Nominal reliability is good, but this campaign contains no faults. It does
   not yet demonstrate crash recovery, redelivery, retry correctness, DLQ
   behavior, or crash durability. Those require a separate test contract,
   described at the end of this report.

## Changes under benchmark

The branch contains these commits, all predating the final campaign:

```text
d4d498bf feat: add Laravel queue driver and worker supervisors
f42ded00 test: add Laravel supervisor benchmark harness
ef7623e5 fix: persist sampler data outside worker results
4937606c fix: normalize supervisor header maps
ef679e80 fix: exclude container init from control-plane metrics
473d8bfa fix: establish supervisor baseline capacity immediately
```

Commit `473d8bfa` corrects baseline-capacity startup and restoration:
`balance_max_shift` continues to govern elastic changes, but no longer slows
the startup of minimum/fixed processes or recovery after a crash. Both PHP and
Rust tests cover the correction.

Before the final commit, these checks passed:

- complete PHP suite: 326 tests, 1,294 assertions;
- Rust suite: 32 tests;
- PHP lint and strict Composer checks;
- `cargo fmt` and `cargo clippy -- -D warnings`.

The worktree was clean at the start of the campaign and remained clean.

## Protocol

### Environment

- macOS 26.5 ARM64 host;
- Docker Desktop 29.7.2, LinuxKit 7.0.12, cgroup v2;
- 10 vCPUs and 8,214,851,584 bytes of RAM available to the Docker VM;
- PHP 8.3.33, Laravel 12.68.0, Horizon 5.48.3;
- Redis 7.4.2, PostgreSQL 16.10;
- Queen broker 1.1.0, Queen Rust supervisor 0.1.0;
- Queen Laravel client identified by Composer as `dev-main`, with source pinned
  by the Git commit and image digest.

### Workload

- 2,000 jobs per lane, 18 lanes, 36,000 jobs total;
- three repetitions for each cell;
- a single `benchmark` queue;
- each job sleeps for 10 ms with no synthetic CPU load;
- fixed profile: four workers;
- auto profile: minimum 1, maximum 4, `size` strategy, target 10 jobs/process,
  3-second cooldown, and maximum shift 1;
- PSS/cgroup sampling every second;
- isolated lanes, fresh backends and volumes for every execution, and order
  rotated across repetitions.

The application has a 4-CPU and 1-GiB limit. The Horizon backend has Redis
with 2 CPUs and 2 GiB. The Queen backend has the broker and PostgreSQL with 1
CPU and 1 GiB each: the same nominal aggregate budget, but divided across two
cgroups.

### Window and metrics

Headline throughput is measured from dispatch through completion of the final
job. Resources exclude warm-up but include drain. Control-plane and worker
memory use PSS; backend and stack memory use cgroup `memory.current`, because
the sampler cannot reliably read PSS for all backend UIDs.

## Correctness gates

| Check | Result |
| --- | ---: |
| Correct lanes | 18/18 |
| Completed jobs | 36,000/36,000 |
| Missing / duplicate / failed | 0 / 0 / 0 |
| Retries or attempt > 1 | 0 |
| Invalid or foreign records | 0 |
| Healthy containers | 48/48 |
| Restarts / OOMKilled | 0 / 0 |
| Sampler errors, cadence gaps, overruns | 0 |
| Orchestrator/worker PSS coverage | 100% / 100% |

An independent check of the 36,000 raw records also verified, for every lane,
the exact set of IDs `000000000..000001999`, SHA-256 checksum,
connection/queue/workload, attempt, timestamp order, and equality of derived
latencies. It found no semantic errors.

In the fixed profile, all nine runs have `initial_workers=4`, `peak=4`, and
`final_workers=4`. The four workers are already stable for at least three
consecutive samples before dispatch: 2.2–2.7 seconds for Horizon, 2.7–4.3
seconds for Queen PHP, and 3.1–4.6 seconds for Queen Rust. All four then begin
consuming within 275 ms of dispatch. The focused pre-campaign check had already
confirmed the same behavior for PHP and Rust with 200 jobs per lane.

## Performance

Values are medians of three runs; the minimum–maximum range is in parentheses.

### Fixed, with four effective workers from the start

| Engine | jobs/s | E2E p50 s | E2E p95 s | E2E p99 s |
| --- | ---: | ---: | ---: | ---: |
| Horizon | 285.2 (270.2–287.2) | 3.05 | 6.01 (5.46–6.41) | 6.28 |
| Queen PHP | 140.5 (124.1–144.8) | 4.11 | 12.10 (11.32–13.93) | 12.62 |
| Queen Rust | 138.1 (120.8–145.2) | 4.73 | 12.44 (10.27–14.81) | 13.02 |

Throughput computed only over the completion span yields the same conclusion:
286.6 jobs/s for Horizon, 141.3 for Queen PHP, and 138.5 for Queen Rust.

### Autoscaling 1–4

| Engine | jobs/s | E2E p50 s | E2E p95 s | E2E p99 s |
| --- | ---: | ---: | ---: | ---: |
| Horizon | 169.6 (165.6–211.3) | 7.01 | 10.15 (8.48–10.58) | 10.60 |
| Queen PHP | 105.6 (91.8–116.6) | 10.10 | 17.15 (14.75–19.87) | 17.86 |
| Queen Rust | 95.9 (89.8–106.9) | 11.16 | 19.32 (16.72–20.05) | 19.92 |

The auto profile has material variability, especially for Horizon: one
repetition reached four workers in 4.15 seconds, while the other two took
about 9.3–9.5 seconds. With three repetitions on Docker Desktop, small
differences are inconclusive. `horizon-auto-r03` shows a sampled transition of
`0 → 3 → 4` and 211 jobs/s, compared with `1 → 2 → 3 → 4` and 166–170 jobs/s
in the other two runs. It should be treated as a procedural outlier/startup
race and repeated in the next campaign.

### Paired ratios against Horizon

Each ratio is computed between engines in the same repetition and then
aggregated by the median. This avoids dividing medians from different temporal
conditions.

| Profile | Engine | Throughput | E2E p95 | Orchestrator PSS | Stack RAM |
| --- | --- | ---: | ---: | ---: | ---: |
| fixed | Queen PHP | 49.3% | 2.07× | 54.3% | 1.63× |
| fixed | Queen Rust | 50.6% | 1.94× | 4.66% | 1.46× |
| auto | Queen PHP | 55.5% | 1.88× | 57.2% | 1.59× |
| auto | Queen Rust | 56.5% | 1.90× | 4.47% | 1.44× |

The E2E latencies of a 2,000-job burst naturally include time spent in the
backlog; they are not individual job execution times. Application-work p95 is
about 12.5 ms in every cell. In the fixed profile, the p50 gap between one job
ending and the next starting on the same worker is about 2.17 ms for Horizon,
5.76 ms for Queen PHP, and 6.60 ms for Queen Rust; p95 is 4.98, 28.80, and
31.24 ms. This locates the gap in the consumption/ACK/fetch cycle and backend,
not the simulated work.

### Queen Rust versus Queen PHP

In the fixed profile, paired Rust/PHP ratios are:

- throughput 95.4% (range 85.9–117.0%);
- p95 1.03× (range 0.74–1.31×);
- orchestrator PSS 8.57%;
- application RAM 81.0%;
- stack RAM 90.5%.

In the auto profile they are:

- throughput 85.0% (range 82.2–116.5%);
- p95 1.17× (range 0.84–1.31×);
- orchestrator PSS 7.86%;
- application RAM 81.0%;
- stack RAM 90.7%.

The Rust RAM advantage is clear. The PHP/Rust performance difference is not
yet attributable to the supervisor: the three repetitions are noisy, and
PostgreSQL was throttled in every fixed Rust run and two auto Rust runs.

## Memory

Median peaks in MiB:

| Profile | Engine | Orch PSS | Worker PSS | App cgroup | Backend cgroup | Stack cgroup |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| fixed | Horizon | 61.7 | 122.7 | 171.6 | 15.7 | 187.1 |
| fixed | Queen PHP | 33.5 | 130.8 | 151.6 | 151.7 | 303.0 |
| fixed | Queen Rust | 2.87 | 134.4 | 122.6 | 151.7 | 274.1 |
| auto | Horizon | 68.1 | 123.4 | 172.9 | 13.2 | 185.9 |
| auto | Queen PHP | 38.8 | 131.0 | 151.7 | 144.5 | 296.0 |
| auto | Queen Rust | 3.05 | 135.0 | 123.2 | 146.8 | 268.6 |

Interpretation:

- Queen PHP saves about 42.8–45.7% of control-plane PSS versus Horizon;
- Queen Rust saves about 95.3–95.5% versus Horizon and more than 91% versus
  Queen PHP;
- the Rust application cgroup uses about 19% less than Queen PHP and 28–29%
  less than Horizon;
- PostgreSQL dominates a material part of the Queen backend. As a result, the
  Rust stack is about 9–10% lighter than the PHP stack but remains 44–46%
  above Horizon in this topology.

These figures answer two different questions: the Rust supervisor solves the
control-plane RAM problem; the complete Queen stack still requires
optimization and a comparison at equivalent durability.

## CPU

Median CPU seconds across the complete measured window:

| Profile | Engine | Orch | Worker | App | Backend | Stack |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| fixed | Horizon | 0.040 | 3.418 | 3.442 | 1.052 | 4.415 |
| fixed | Queen PHP | 0.035 | 2.825 | 2.860 | 5.041 | 7.901 |
| fixed | Queen Rust | 0.087 | 3.529 | 3.657 | 5.846 | 9.341 |
| auto | Horizon | 0.319 | 4.681 | 5.255 | 1.526 | 6.860 |
| auto | Queen PHP | 0.132 | 4.101 | 4.377 | 7.067 | 11.444 |
| auto | Queen Rust | 0.053 | 3.721 | 3.904 | 6.053 | 9.919 |

In the auto profile, paired ratios indicate that Queen PHP uses about 62% less
control-plane CPU than Horizon; Queen Rust uses about 83.5% less than Horizon
and 60% less than Queen PHP. In the fixed profile, master CPU times are too
small and variable for a conclusion: the Rust median is higher, but the ratio
range crosses parity widely.

Isolating only the active auto window, control-plane medians are 0.261 CPU-s
for Horizon, 0.098 for PHP, and 0.028 for Rust; the paired Rust/PHP ratio is
0.319. This reinforces the Rust CPU advantage under autoscaling without making
fixed-profile CPU conclusive.

The Queen stack uses more CPU, especially in the backend. Against Horizon, the
median paired stack ratio is about 1.79× for PHP and 2.12× for Rust in the
fixed profile, and 1.61× for PHP and 1.42× for Rust in the auto profile.

PostgreSQL accumulated throttling in the three fixed Rust runs (0.193–0.988
seconds), in two auto Rust runs (up to 0.354 seconds), and only in two PHP runs
at very small values. Queen's aggregate budget equals Redis's, but dividing it
into two one-CPU limits can introduce a bottleneck. Small PHP/Rust deltas must
therefore be replicated without throttling.

## Autoscaling

| Engine | Observed initial workers | Peak | Median time to peak | Final | Actual return to 1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Horizon | 1 (one run sampled at 0) | 4 | 9.32 s (4.15–9.52) | 1 | 8.68 s (8.46–10.53) |
| Queen PHP | 1 (one run sampled at 2) | 4 | 8.32 s (7.94–8.43) | 1 | 8.54 s (8.50–10.80) |
| Queen Rust | 1 | 4 | 8.43 s (8.08–8.51) | 1 | 7.82 s (7.73–10.66) |

Every auto run reaches four workers and ends at one. Initial values of 0/2 are
artifacts of one-second resolution during the first moments; they do not
indicate different configuration limits. The return times above were
recalculated against the configured minimum. The automatic report's `Return`
field instead measures return to the first sampled value: it therefore reports
5.79 seconds for `queen-php-auto-r01` (return to 2, not 1) and `n/a` for
`horizon-auto-r03` (first sample at 0). Horizon and Queen dynamics are not
semantically identical even with the same numbers, so the auto profile
compares product behavior, not the same algorithm.

## Reliability: what is and is not demonstrated

Demonstrated under nominal conditions:

- 36,000 jobs handled without observed loss or duplication;
- no unexpected retry/failure;
- no OOM, restart, unhealthy container, or worker outside its limits;
- complete autoscaling and return to minimum;
- intact artifacts and sampler data.

Not yet demonstrated:

- recovery after killing a worker, master, or backend;
- correct redelivery before and after a side effect;
- timeouts, backoff, retry exhaustion, failed store, and DLQ;
- absence of stuck reserved/delayed jobs after a fault;
- crash durability and RPO;
- absence of memory/FD/PID leaks during a soak;
- correctness across multiple queues and under overload.

The current event is written at the end of the handler but before the
framework ACK. The benchmark does not yet record `job_processed` or perform an
explicit final reconciliation of pending/reserved/delayed work. The current
gate is therefore appropriate for the nominal benchmark, not an exactly-once
or crash-recovery claim.

## Limitations and threats to validity

1. Three repetitions on Docker Desktop ARM64/LinuxKit: the result is
   diagnostic, not statistically publishable.
2. Durability is not equivalent: Redis has RDB/AOF disabled; PostgreSQL uses
   `synchronous_commit=on`, but its data are on tmpfs. Throughput and stack are
   therefore comparisons of the declared topologies, not equal crash
   durability.
3. One queue and 10 ms sleep-bound jobs: empty and CPU-bound jobs, variable
   payloads, and multiple queues are missing.
4. The producer is not included in the measured stack.
5. Horizon produces about 493–505 KiB of logs per run; Queen produces about 6
   KiB. This asymmetry can affect CPU in particular.
6. One-second sampling limits scaling-time precision to about one second.
7. Backend PSS is unavailable: backend/stack memory is correctly reported as
   `memory.current`, not PSS.
8. PostgreSQL CPU throttling and variability across three repetitions prevent
   attributing small differences solely to the PHP/Rust master.
9. During bootstrap, `queen-rust-auto-r01` recorded a transient
   `database "queen" does not exist`; health/readiness recovered and the
   measured window remains correct, but bootstrap should be quieter.

## Product verdict

The orchestrator thesis is confirmed: a Rust variant can provide Horizon-like
functions with a control plane of about 3 MiB PSS, compared with 34–39 MiB for
the PHP supervisor and 62–69 MiB for Horizon. The PHP variant remains useful
as a pure-Laravel fallback and nearly halves master memory versus Horizon.

The end-to-end replacement thesis is not yet confirmed. In the current
workload, Queen delivers about half of Horizon's paired throughput, roughly
doubles burst p95, and uses more whole-stack RAM/CPU. The technical priority is
therefore to optimize and profile the path shared by PHP and Rust:

1. fetch/long-poll and the number of round trips per job;
2. reserve/ACK batching where semantically possible;
3. HTTP/database connection reuse and serialization cost;
4. PostgreSQL queries/indexes/transactions;
5. broker/store CPU limits and durability policy;
6. the interval between an ACK and the next fetch in the Laravel client.

## Stress and reliability plan

The stress test must use a separate `reliability/v2` contract. The nominal
benchmark gate must not be relaxed: retries and duplicate executions can be
expected outcomes in an at-least-once system, but only the scenario policy may
allow them, and they must be reconciled with application effects.

### P0 scenarios

| Scenario | Minimum configuration | Repetitions | What to observe |
| --- | --- | ---: | --- |
| Idle control plane | workers 1/4/16, empty queue, 15 min | 5 | baseline CPU/PSS, PID/FD/RAM leaks |
| Fixed I/O-bound | workers 1/4/8/16, ≥120 seconds active | 5 | throughput, p95/p99, scaling |
| Fixed CPU-bound | work calibrated to 2/20 ms | 5 | master cost under saturation |
| Sustained | 50/75/90/110% capacity, 30 min | 3 | backpressure and stability |
| Auto burst | min 1, max 16 | 10 bursts | peak/downscale time, worker-s |
| Retry/DLQ | success, exception, timeout, release | 10,000 jobs | attempt and terminal state |
| Worker kill | before/after side effect, long job | 20 faults | redelivery and replacement |
| Master kill | full backlog and empty queue | 10+10 | continuity, orphans, RTO |
| Graceful deploy | SIGTERM with in-flight/backlog | 10 | drain within grace period |
| Endpoint/backend outage | 10/30/120 seconds | 10/duration | backoff, RTO/RPO, drain |
| Soak | 75% capacity, seeded faults | 6 h × 3 | leaks and recovery p95 |

P1 adds an 80/15/5 multi-queue mix, one hot queue, 200% overload, payloads of
1 KiB/64 KiB/1 MiB, network latency/loss, connection exhaustion, disk-full,
and a 24-hour release-candidate soak.

### Invariants

- Every published job must reach exactly one declared terminal outcome:
  success or failed store/DLQ. No silent loss.
- Attempts must be monotonic and not exceed `tries`.
- A duplicate delivery may be allowed only for a job plausibly in flight
  during the fault. The raw ledger may show it, while an idempotent table must
  contain exactly one logical effect per job.
- After recovery: pending, reserved, and delayed at zero; no stuck lease; no
  zombie/orphan; workers stable at the target.
- Every fault must be armed, triggered, and verified; sending a `kill` is not
  sufficient without evidence that the correct PID disappeared.
- Ratios between engines are permitted only when scenario hash, seed, job
  plan, timeout/retry, actual fault, limits, instrumentation, and durability
  match.

### Reliability metrics

- success rate, silent loss, duplicate delivery/effect, and DLQ;
- detection time, worker replacement time, and first post-fault success;
- RTO, RPO, and backlog-drain time;
- queue depth/reserved/delayed over time;
- throughput and p50/p95/p99 before, during, and after the fault;
- PSS/RSS/cgroup memory, CPU/throttling, PID/FD, and restarts;
- robust RAM, PSS, FD, and PID slopes during soaks.

### Required harness changes

1. Typed lifecycle events: planned/published, `job_started`,
   `effect_committed`, handler completed, exception/release/failure, and
   `job_processed` after ACK.
2. Canonical scenario JSON with a seed and hash, scenario-specific correctness
   policy, and identical job/fault plan for every engine.
3. An allowlisted fault injector driven by observable events rather than
   arbitrary timed sleeps.
4. A restart-aware sampler with PID/cgroup generations and a one-second
   health check.
5. Artifact salvage before teardown even when a lane fails.
6. Normalized queue state and final reconciliation.
7. Two separate topologies: volatile and durable. In the durable topology,
   Redis AOF and PostgreSQL/broker must use comparable storage and fsync.

### Proposed performance gates before running the stress test

These thresholds are an initial proposal and must be frozen before inspecting
new results:

- Queen Rust versus Queen PHP: throughput ≥95%, p95 ≤1.10×, master PSS ≤15%,
  and master CPU ≤PHP;
- Queen versus Horizon, if marketed as an equivalent replacement: throughput
  ≥90%, p95 ≤1.25×, and stack RAM ≤1.25× at equivalent durability;
- recovery: p99 within 20% of baseline within two minutes;
- worker replacement within `poll + restart_backoff + 5 s`;
- soak: no monotonic PID/FD growth, RAM <1%/hour and <10% total over 24 hours.

The current campaign passes nominal correctness gates and the Rust master RAM
gate. It does not yet pass the proposed Queen/Horizon equivalence gates for
throughput, p95, and stack RAM.
