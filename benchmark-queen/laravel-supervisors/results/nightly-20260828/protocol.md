# Frozen protocol — Laravel Queen optimization campaign

Frozen at 2026-08-29T00:44:55Z (02:44:55 Europe/Rome), before screening
results were inspected.

- Source: `f2c39fd989` on `feat/laravel-horizon-replacement`; clean worktree.
- App image: `sha256:adaac94a4f2ce503db1c27ca26697a37872f6c8fa57831352afa1025d1d1be3e`.
- Broker image: `sha256:472ac63907a8168702b037053258fd0f01e0c04180d214b429a4f3d79b3dad7a`.
- Host: Docker Desktop, Linux containers, ARM64, 10 vCPU, 8.21 GB, cgroup v2.
- No unrelated containers were running at freeze time; macOS sleep inhibition
  was enabled. Exactly one measured lane runs at a time.

## Status and scope

This is a diagnostic local campaign, not a publishable cross-system capacity
claim. Redis is volatile while Queen uses PostgreSQL with
`synchronous_commit=on`; absolute Horizon/Queen ratios therefore describe this
topology and are not durability-equivalent. The 1M msg/s Queen soak is a
separate broker test and is not compared numerically with Laravel job/s.

## Discovery stage

Use Queen Rust fixed supervisor, four workers, 4,000 jobs, 10 ms sleep, 512
warm-up jobs, fresh backend/volumes, one run per cell. Hold partitions=64,
pop-fusion=off and single dispatch fixed while screening:

- strict ACK: prefetch 1, 4, 8, 16, 32, 64 with `ack_batch=1`;
- full-batch ACK: matching prefetch/ACK values 4, 8, 16, 32, 64;
- repeat the legacy anchor at the end to expose drift.

Discovery results select candidates only; they are not confirmatory evidence.
Every cell must have zero missing, duplicates and failures, healthy containers,
no OOM/restarts, and complete sampler data. No post-hoc outlier deletion.

Selection rule: among valid cells, select the smallest setting within 5% of
the maximum median/headline throughput. `ack_batch>1` is classified
"aggressive" because delete confirmation is deferred and the crash duplicate
window is wider. A strict candidate (`ack_batch=1`) is selected separately.

## Follow-up discovery

For the selected strict/aggressive prefetch, screen partitions 1/4/16/64 and
pop fusion off/on. Test producer single versus Laravel bulk separately; use
the same bulk chunk size for Horizon and Queen.

## Confirmation stage

Rebuild/rerun the legacy anchor on this SHA. Compare Horizon, Queen PHP and
Queen Rust under fixed four workers and auto 1..4 for:

1. legacy (`prefetch=1`, `ack=1`, single dispatch);
2. selected strict profile;
3. selected aggressive profile, clearly separated.

Use at least three fresh repetitions per headline cell (five where remaining
time permits), rotate engine order through the existing runner, and retain all
raw runs. Primary metrics: jobs/s and E2E p95. Guardrails: p99, CPU seconds,
PSS/RSS, cgroup peak, backend CPU, throttling, OOM/restarts and correctness.
Report medians and per-run values; compute paired ratios only within identical
profiles/workloads.

## Robustness and fault scope

Run at least one high-volume sleep=0 burst and one auto-scaling campaign on the
selected profile. A worker-SIGKILL test is diagnostic unless lifecycle events,
queue reconciliation and fault timing are fully captured; do not call a clean
completion file proof of exactly-once processing because it is written before
the broker ACK.
