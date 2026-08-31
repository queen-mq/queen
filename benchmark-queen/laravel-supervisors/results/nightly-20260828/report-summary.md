# Horizon vs Queen PHP vs Queen Rust — decision summary

**Status:** auditable report, August 29, 2026. Laravel results are classified as
`diagnostic`; fault tests as `diagnostic_smoke`.

Sources: [frozen local protocol](protocol.md) and the
[complete technical report](technical-report.md).

## Decision

Queen can replace Horizon's control plane in the tested path, but configuration
matters as much as implementation:

- **Queen Rust is the recommended orchestrator**; Queen PHP remains the
  portability fallback.
- The **shipping default remains p1/a1** (prefetch 1, ACK batch 1). It is
  conservative but not performance-equivalent: in the legacy profile Queen
  PHP/Rust measured 118.15/121.39 jobs/s versus 261.85 for Horizon, about -55%.
- **p4/a1 is the short-job performance profile**: on the measured post-guard
  commit 3655cd2a it exceeds Horizon by about 14%, but it requires a bounded
  runtime and `retry_after > prefetch × worst-case runtime + margin`.
- Any ACK batch greater than 1 remains opt-in for jobs and external effects
  that are idempotent: under SIGKILL it substantially increases the observed
  duplication.

This is not yet a general claim of full production-ready replaceability: the
tests cover performance and recovery, not feature parity with Horizon for the
dashboard, metrics, failed-job store, pause/resume, ordering, or priorities.

## Primary post-guard result

The [primary fixed campaign](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.md)
uses four workers, 10,000 jobs of 10 ms each, p4/a1, a 120 s timeout, and a
481 s retry_after. Clean commit 3655cd2a, 15/15 valid and quiescent runs,
150,000/150,000 unique jobs, zero missing/duplicates/failures, OOM events,
restarts, throttling, or sampler errors.

Medians; the "measured stack" is the supervisor+worker consumer application
plus the complete backend. Producer, observer, and sampler are excluded.

| Engine | Completion jobs/s | E2E p95 | Orch. CPU | Orch. PSS | Stack CPU | Stack RAM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Horizon | 253.38 | 37,547 ms | 0.1445 s | 71.05 MiB | 27.19 s | 217.66 MiB |
| Queen PHP | 289.17 | 30,403 ms | 0.0320 s | 35.15 MiB | 16.56 s | 338.23 MiB |
| Queen Rust | 289.13 | 30,067 ms | 0.0141 s | 2.87 MiB | 16.42 s | 303.63 MiB |

Paired candidate/Horizon ratios, median with descriptive 95% bootstrap:

| Metric | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion | 1.1428 [1.1345–1.1515] | 1.1405 [1.1205–1.1632] |
| E2E p95 | 0.8096 [0.8036–0.8141] | 0.8037 [0.7974–0.8221] |
| Orchestrator CPU | 0.2184 [0.2080–0.2269] | 0.0987 [0.0902–0.1011] |
| Orchestrator PSS | 0.4948 [0.4948–0.4967] | 0.0405 [0.0404–0.0405] |
| Stack CPU | 0.6057 [0.5933–0.6131] | 0.6006 [0.5916–0.6040] |
| Stack RAM | 1.5540 [1.5216–1.5620] | 1.3930 [1.3893–1.4201] |

Queen Rust is therefore +14.05% in completion, -19.63% in p95, -90.13% in
orchestrator CPU, -95.95% in orchestrator PSS, -39.94% in stack CPU, and
+39.30% in stack RAM. However, the orchestrator-only CPU saving is just
0.1445→0.0141 s, about 130 ms over a 39.5 s drain: PSS and consumer+backend CPU
are more material.

Both Queen lanes outperform Horizon in all 5/5 paired runs. With five
concordant signs, the exact two-sided sign test is p=0.0625: the bootstrap
describes the ratios observed on this host, not statistical significance or a
population confidence interval.

The p95 is enqueue-to-completion and includes arrival shape and dispatch; it
is not isolated broker latency. The headline favors Queen (~1.186×), while
Horizon dispatch is ~1.8× faster; the producer is outside the accounted
cgroups but shares the VM and may introduce contention.

## Other scenarios

| Scenario | Horizon | Queen PHP | Queen Rust | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Auto p4/a1 post-guard, jobs/s | 243.75 | 234.13 | 235.69 | PHP -5.0%; Rust -3.3%; 9/9 valid |
| Auto p16/a16, jobs/s | 245.22 | 240.47 | 244.96 | Rust at parity; ACK16 opt-in |
| CPU-bound, jobs/s | 359.49 | 365.02 | 364.05 | substantially equal; stack CPU ~+2% |
| Stress 50k zero-work, jobs/s | 841.97 | 2,042.17 | 2,007.13 | Queen ~2.4×; Queen backend throttled |
| Fixed p16/a16, jobs/s | 255.96 | 295.91 | 293.85 | +15.6/+14.8%; not a causal A/B vs p4 |

In the post-guard auto p4/a1 profile, the four-worker peak is detected at
4.653 s for Horizon, 8.644 s for PHP, and 8.631 s for Rust; worker-seconds are
127.27/111.53/111.53 (-12.4% for Queen). Rust uses -29.9% stack CPU and -96.0%
orchestrator PSS, with +42.1% stack RAM. The algorithms are not semantically
identical. At one worker Queen is about +2%; at eight workers the medians are
6–8% below Horizon, but the n=3 CIs cross 1. W1/w8 do not form a matched curve
with the primary w4 profile.

The zero-work ACK4 discovery reaches 7,306.88 jobs/s for PHP and 6,989.41 for
Rust, ~3.58×/~3.48× compared with the a1 stress test, but this is an unpaired
cross-campaign comparison and is not transferable to real application jobs.

## Recovery and reliability

The [post-guard p4/a1 fault campaign](fault-p4-a1-guarded-r01/report.md) uses
two workers, 24 jobs of 2 s each, a 10 s timeout, and a 41 s retry_after: the
guard requires >4×10=40 s. Across five rounds per engine:

| Profile | ALO lanes | Strict lanes | Unique | Duplicates | Median respawn H/PHP/Rust |
| --- | ---: | ---: | ---: | ---: | ---: |
| p4/a1 post-guard | 15/15 | 15/15 | 360/360 | 0 | 86.7 / 1,614 / 2,932 ms |
| p4/a4 | 10/10 | 3/10 | 240/240 | 7 | Queen only |
| p16/a16 pre-guard | 10/10 | 1/10 | 240/240 | 17 | Queen only |

Selected fault total: **35/35 at-least-once, 19/35 strict, 840 unique jobs,
24 duplicates**; the final-queue and OOM/restart gates pass in 35/35 lanes.
The older 15 pre-guard p4/a1 rounds remain as corroboration and are replaced
one-for-one. The p16/a16 pilot with an insufficient lease is preserved outside
the total: both lanes remained non-quiescent, making the need for the guard
visible.

The post-guard p4/a1 in-flight retry appears at about 42.3/40.6/40.5 s for
Horizon/PHP/Rust. The primary fixed campaign instead uses timeout 120/retry
481 s: it may wait almost eight minutes. The fault test validates the same
knobs and lease rule, not the exact recovery time of the performance preset.

The tests demonstrate at-least-once behavior within the observed scope, not
exactly-once: completion is written before the ACK, the failed-job store is
disabled, and there is no ledger of business effects.

## Volume and the "1 million/s" claim

The first 50k stress test failed the observation/completion gate: 28,617
completions were collected and 21,921 items remained in the queue, with no OOM
recorded. Commit 2a4b107a made JSONL collection incremental; the
[n=3 rerun](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.md)
passed 9/9. The initial artifact does not prove queue data loss.

The [historical broker-native soak](../../../2026-08-11-soak24-1M/results.md)
measures **999,652 accepted pushes/s** on average for 24 h and zero restarts,
but it uses Go HTTP loaders, 200 partitions, 600 consumers, and dedicated
hosts: it does not pass through Laravel. It does not prove 1M jobs/s,
exactly-once, or end-to-end conservation. At cut-off:

- offered−accepted = 41,800; only 400 are explained by four failed push
  requests;
- pushed−popped = 442,600 and popped−acked = 15,000, compatible with in-flight
  work but without a drain/ledger;
- 600 ACK messages failed; shed=0, watchdog restarts=0, declared incidents=0.

## Method, corpus, and provenance

The Queen team designed the test. A timestamped local protocol before
screening, pairing, run retention, and fail-closed gates are anti-bias
measures, not independent certification. The initial grid was one-shot and
dirty; `screen-clean/` is also recorded as dirty. It is excluded, as is the
133.28→112.22 jobs/s anchor drift. On the clean commit, p4/a1 and p16/a1 were
replicated: p4 is the smallest performance baseline among replicated
candidates, not a global optimum. Partitions/fusion were not compared.

The selected corpus contains **99 runs/1,248,000 jobs**, all valid and
quiescent, with n=3 or n=5 per cell. Superseded historical campaigns, smoke
tests, and reruns are preserved but excluded; the gross inventory is listed
only in the technical report. Faults, the negative pilot, and the gate failure
are separate.

Primary performance and the p4/a1 fault test use clean commit 3655cd2a; the
post-guard auto campaign has clean metadata at 774c2413. All use app
`sha256:0e82c6cc…8958`, broker `sha256:522bdc09…801fd`, macOS 26.5 arm64,
Docker Desktop 29.7.2, and 10 CPUs/8.21 GB. Commit 774c2413 clarifies the
negative override but is not included in the application image. Complete
provenance, Redis/PostgreSQL digests, and historical campaigns are in the
technical report; `campaign-stats` does not automatically gate backend
digests.

## Limitations and next gates

- Docker Desktop/macOS, a small n, and non-equivalent topologies: volatile
  Redis versus Queen+PostgreSQL with `synchronous_commit on`, but the data
  directory is on local tmpfs.
- PSS covers only the control plane; stack RAM includes consumer+backend but
  not producer/observer. The nominal producer+app+backend quotas saturate 10
  CPUs.
- The performance runner does not inventory/fail-fast foreign containers for
  every lane. Warm-up does not fully adhere to the freeze.
- No backend crash, network fault, failover, disk-full, native Linux, or
  prolonged Laravel soak; no external-effects ledger.
- A single queue only: multi-queue behavior, ordering, priority, and starvation
  were not verified.

Before GA: repeat on Linux arm64/amd64; run a Laravel soak and backend/network/
storage faults; add a ledger and failed store; complete matched w4 scaling,
multi-queue, and partitions/fusion; introduce lease renewal or retain p1 when
runtime is not bounded; add snapshots/fail-fast checks for foreign containers
to every performance lane.
