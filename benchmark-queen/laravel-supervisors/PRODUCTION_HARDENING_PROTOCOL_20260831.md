# Queen Laravel production-hardening confirmation protocol

Frozen on August 31, 2026 at 04:37:49 UTC, before any live campaign on the
production-hardening branch was started.

- Implementation commit: `242a0ed88cfc3315084648a0a299263df119fa3a`.
- Branch: `feat/laravel-production-hardening`.
- Parent qualification protocol: [`GA_PROTOCOL.md`](GA_PROTOCOL.md).
- Host class: macOS arm64 with Docker Desktop and Linux arm64 containers.
- Classification: diagnostic. These results cannot satisfy the dedicated
  native-Linux amd64/arm64 or 24-hour soak gates.

## Objective

Confirm that the production-hardening changes preserve Laravel behavior,
improve failure containment, and retain the previously observed performance
advantage without weakening delivery correctness. The campaign keeps four
questions separate: functional parity, fault recovery, steady-state resource
cost, and high-throughput stress behavior.

## Frozen candidate

The candidate configuration is Queen prefetch 4, ACK batch 1, and lease
renewal enabled. ACK batching above one is excluded because it widens the
unconfirmed-effect window and is not required for this release candidate.
Horizon and both Queen engines use four matched Laravel workers for the primary
fixed-profile comparison. The renewal helper is counted as a distinct process
role and inside the Queen supervisor process budget.

## Execution order

1. Run feature parity for Horizon, Queen PHP, and Queen Rust with the frozen
   `high/default/low` distribution of 60/30/10 jobs.
2. Run worker-SIGKILL recovery for all engines and renewal-helper SIGKILL for
   Queen PHP and Queen Rust. Preserve the attempt/effect ledger and require an
   empty final queue.
3. Run backend, network, and master faults in every applicable engine/scenario
   pair using fresh durable volumes.
4. Run the primary fixed-profile performance campaign: 4,000 jobs, 10 ms job
   runtime, four workers, p4/a1 with renewal, five repetitions per engine,
   Redis AOF `appendfsync=always`.
5. Run a conservative p1/a1 fixed-profile control and a 50,000-job zero-work
   stress screen if the preceding correctness gates pass.

One lane runs at a time. Engine order is rotated by the runner. No run is
removed after inspection. A failed or incomplete run remains in the corpus and
suppresses paired claims.

## Validity gates

- The checkout must be clean and remain on the frozen commit.
- Image IDs are captured and revalidated before every lane.
- Campaign/project identifiers include a random 96-bit nonce.
- No unrelated running container is allowed during a performance lane.
- Every run must preserve the exact expected job/effect set and reach final
  queue quiescence.
- Sampler integrity, cgroup accounting, OOM/restart state, backend counters,
  process roles, and cleanup residue are fail-closed gates.
- Redis and Queen operation counts are reported in their native units and are
  never divided into a cross-backend ratio.
- CPU, PSS/RSS, cgroup memory, throughput, and latency are reported per run and
  as medians/quartiles; no post-hoc outlier removal is allowed.

## Reliability interpretation

Delivery is at least once. Strict single execution is reported separately and
is expected to fail when a worker is killed after user code begins. The fixture
ledger proves one idempotent effect per test key; it does not make arbitrary
application side effects atomic with the Queen ACK. Production jobs must still
be idempotent or use an application-level transaction/outbox boundary.

## Decision rule

A local canary recommendation requires all executed correctness and fault gates
to pass and no material regression against the previous diagnostic baseline.
A universal GA recommendation additionally requires the outstanding native
Linux, 24-hour soak, controlled latency/loss, and disposable-volume disk-full
gates. Missing gates are reported as missing; they are never inferred from a
shorter diagnostic run.
