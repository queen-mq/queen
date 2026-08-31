# Frozen protocol — GA qualification of Queen Laravel supervisors

Frozen on August 29, 2026 at 05:40:35 UTC (07:40:35 Europe/Rome), before
running and inspecting the results of the new GA campaign.

- Protocol freeze commit: `ec85800f7088c430ec29545305395e48ea9896f8`.
- Branch: `feat/laravel-horizon-replacement`.
- The previous campaign is exploratory evidence that identified the risks to
  verify; it will not be counted again in the new confirmatory corpus.
- The product commit, image IDs, and protocol hash will be pinned in metadata
  before the first lane.

## 1. Objective and classification

Verify whether the Queen Laravel client and the PHP/Rust supervisors satisfy
the gates required for a release candidate that can replace Horizon's control
plane within the declared scope. The questions are kept separate:

1. lease correctness and preservation of workload effects;
2. failed-job lifecycle and multi-queue behavior;
3. throughput, latency, CPU, memory, and autoscaling at matched concurrency;
4. stability over time and recovery from worker, backend, network, and storage
   faults;
5. repeatability on Linux `amd64` and `arm64`.

Runs on Docker Desktop are `diagnostic`. Runs on hosted Linux VMs are
`diagnostic_native`: they use native Linux kernels but not dedicated hardware.
Only a qualified, dedicated Linux host with no foreign workload can produce a
`publishable_candidate` campaign.

This protocol was designed by the Queen team and is not an independent
certification. All results, including failures and pilots, will be retained.

## 2. Frozen configurations

### 2.1 Lanes

- Horizon: Laravel Horizon with Redis.
- Queen PHP: Laravel `queue:work queen`, PHP orchestrator.
- Queen Rust: Laravel `queue:work queen`, Rust orchestrator.

Each paired comparison uses the same checkout, workload, worker count, cgroup
budget, and dispatch order. One lane runs at a time; every sample gets fresh
backends and volumes. The producer remains outside the measured cgroups, but
its host contention is disclosed.

### 2.2 Queen profiles

- conservative: prefetch 1, ACK batch 1;
- performance: prefetch 4, ACK batch 1;
- renewal: prefetch 4, ACK batch 1, renewal helper enabled;
- ACK batch greater than 1: discovery/idempotent-fault testing only, never a
  general GA candidate.

The renewal profile must fail before consuming more jobs when it can no longer
renew safely. A failure after an external effect preserves at-least-once
semantics and may require idempotency; it is not called exactly-once.

## 3. Environment and provenance gates

Before every lane, the harness must save and verify:

- commit/dirty state, branch, image IDs, and resolved manifests;
- kernel, architecture, CPU model/count, RAM, Docker/Compose, and cgroup
  version;
- thermal state when exposed;
- complete container inventory and the absence of foreign containers;
- resolved CPU/RAM/PID limits and the expected backend;
- clock and initial/final free space.

A lane is excluded automatically if the checkout, image, or configuration
changes; if foreign containers appear; if samples are missing; if uninjected
OOMs or restarts occur; if the backend does not become healthy again; or if the
queue does not become quiescent. Overriding a gate produces only explicitly
marked diagnostic evidence.

## 4. Correctness and ledger

The reliability workload uses a ledger outside the queue with the idempotency
key `run_id + job_id`. It must distinguish:

- expected and accepted dispatches;
- execution attempts;
- application effects attempted, created, and already present;
- observed completions;
- ACK/queue quiescence and failed jobs.

Strict steady-state gates:

- complete expected job set;
- one created effect for every job and no foreign effect;
- zero missing jobs, payload/checksum mismatches, and failures;
- queue ready/reserved/delayed/processing at zero for the settle window;
- zero OOMs, restarts, and sampler errors.

Fault tests publish these results separately:

- `at_least_once`: all expected effects are present and the queue is
  quiescent;
- `strict_execution`: no duplicate attempt or completion;
- `idempotent_effect`: exactly one effect created per key despite any retry.

The ledger proves only the fixture's side effect; it does not generalize to an
application's databases or APIs.

## 5. Operational feature parity

### 5.1 Failed jobs

For each engine, verify with a fresh backend:

1. a terminally failed job is visible in the Laravel repository;
2. the Queen DLQ snapshot is consistent where applicable;
3. `queue:failed`, `queue:retry`, `queue:forget`, flush, and prune;
4. retry republishes exactly one unit of work and clears indexes only after
   success;
5. DLQ cleanup failure does not prematurely remove the Laravel record.

### 5.2 Multiple queues

Use three queues (`high`, `default`, `low`) with a frozen 60%/30%/10%
distribution and at least one reachable worker for each queue. Verify the
global set, each per-queue set, absence of starvation, a process allocated to
each active queue, drain, and return to minimums. Ordering and priority are
reported as distinct properties: Horizon and Queen algorithms are not assumed
to be semantically identical.

## 6. Performance campaigns

### 6.1 Matched scaling

Fixed profile, 10 ms jobs, single dispatch, p4/a1, concurrency `1, 2, 4, 8`;
10,000 jobs and five repetitions per cell on a host with at least 10 CPUs. On a
four-CPU host, the 8-worker point is diagnostic and excluded from the primary
curve. Engine order rotates for each repetition.

Auto profile `1..4`, same workload, 6,000 jobs, and five repetitions. Metrics:
completion jobs/s, E2E p50/p95/p99, control-plane CPU/PSS, stack CPU/RAM,
worker-seconds, scale-up time, drain, and return to minimum.

The renewal profile is compared in paired runs with the conservative p4/a1
profile to quantify helper cost; causality is not attributed from unpaired
campaigns.

### 6.2 Partitions and pop fusion

One-shot Queen Rust discovery with partitions `1, 4, 16, 64` and fusion
`off/on`, 4 workers, p4/a1, 20,000 zero-work jobs, and a repeated anchor at the
end. Select the smallest configuration within 5% of the maximum valid result.
Confirm the candidate and baseline with five repetitions; no outlier is
removed.

## 7. Soak

Run a local diagnostic soak of at least one hour first, then a dedicated
24-hour Linux soak for each engine, one lane at a time. Use a mixed,
deterministic workload: short sleep-bound jobs, CPU-bound jobs, long jobs within
timeout, bursts, and idle periods; record the seed and mix in metadata.

Sample at least every five seconds: queue depth, throughput, latencies, worker
count, CPU, RSS/PSS/cgroup memory, throttling, connection errors, OOMs, and
restarts. Gates: preserved ledger, drained queue, no unexpected restart/OOM, no
unexpected terminal failure, and no unbounded memory growth. Report the RAM
slope with an interval and chart, not only the peak.

## 8. Fault injection

Each scenario uses at least five fresh backends per engine and retains the
timeline, logs, container state, ledger, and queue state:

- SIGKILL a worker during user code;
- SIGKILL the supervisor/master;
- stop/restart the broker or Redis with declared persistence;
- stop/restart PostgreSQL;
- isolate the app↔backend and broker↔PostgreSQL networks, then restore them;
- controlled latency and packet loss when `tc/netem` is available;
- fill storage on a disposable volume, followed by recovery or fail-closed
  behavior.

The storage test never reuses the damaged backend for a later lane. Volatile
Redis and durable PostgreSQL are not called equivalent: recovery tests either
use declared persistent configurations or report expected loss as a topology
difference.

## 9. Statistics and reporting

- Report every repetition; no post-hoc removal.
- Median, quartiles, range, and per-run values.
- Paired candidate/Horizon ratios only for identical workloads and hosts.
- Deterministic paired 95% descriptive bootstrap and exact sign test; neither
  is presented as population confidence with `n=5`.
- No numeric ratios across architectures or different hosts.
- Screening, smoke, failure harnesses, and confirmations remain separate
  corpora.
- Raw artifacts, logs, and reports receive SHA-256 manifests; corpus counts
  declare inclusions and exclusions.

## 10. GA decision

The release candidate does not pass the general gate if any of these are
missing:

- safe lease renewal, or a p1 default for workloads without a bounded runtime;
- end-to-end failed-job lifecycle and multi-queue coverage;
- a 24-hour Linux soak with a ledger;
- worker/backend/network/storage faults with documented recovery;
- at least one dedicated-host Linux amd64 campaign;
- no regression in PHP/Rust suites and artifact gates.

Linux arm64 is a support gate for publishing arm64 binaries and claims; it does
not block a GA explicitly limited to amd64. Dashboard and operational metrics
remain a separate feature-parity workstream and must be disclosed before Queen
is presented as a complete replacement for the Horizon experience.
