# Laravel production-hardening technical report

**Date:** August 31, 2026

**Branch:** `feat/laravel-production-hardening`

**Implementation commit:** `242a0ed88cfc3315084648a0a299263df119fa3a`

**Frozen protocol commit:** `bed455b443560825289a8cbe5648e0fb78439c4d`

**Test-driven corrections:** `1673522c` (`fix(queue): preserve delivery attempts after partial ack`)

**Environment:** macOS arm64, Docker Desktop, Linux arm64 containers

**Classification:** diagnostic qualification; not GA evidence

## 1. Executive conclusion

The hardening work materially improves Queen's suitability as a Laravel queue
control plane without adding product surface unrelated to performance,
stability, or operational maturity. Feature parity passed for Horizon, Queen
PHP, and Queen Rust. All three engines recovered every expected idempotent
effect after a worker was killed. Eleven completed infrastructure-fault lanes
also passed their required at-least-once and idempotent-effect gates, and a
separate post-fix Queen PHP master-SIGKILL audit passed.

The campaign also found three real defects in backend attempt accounting and
the fault harness. They were corrected in `1673522c` and covered by regression
tests. The original Queen PHP renewal-helper lane remains retained as a failed
observation: although it completed all 24 jobs with 24 conserved effects and an
empty queue, the stale delivery-attempt marker prevented the harness from
recognizing the tail as a retry. The post-fix live rerun passed for both Queen
PHP and Queen Rust.

The decision is therefore:

- **No-go for a universal GA or an unguarded Horizon replacement.**
- **Go for completing the remaining release-candidate qualification and then
  a bounded Queen Rust canary**, with Queen PHP retained as the operational
  fallback.
- Do not publish a current-commit performance claim yet. The frozen five-run
  p4/a1-with-renewal comparison was not completed before the reporting cutoff.

Native dedicated Linux amd64 and arm64 results, a 24-hour Laravel soak,
controlled network latency/loss, and disposable-volume disk-full testing remain
release gates. Docker Desktop evidence cannot substitute for them.

## 2. Scope and necessity review

The implementation was deliberately restricted to behavior with a direct
production purpose:

- require lease renewal whenever prefetch is greater than one;
- fence a worker when its renewal helper dies while a lease is active;
- perform a bounded, single-attempt best-effort tail release during Laravel
  shutdown;
- validate `timeout`, `retry_after`, renewal timing, response bodies, and
  bounded `Retry-After` values fail closed;
- count renewal helpers in the process budget and clean complete worker process
  groups in both supervisors;
- align PHP and Rust crash-circuit behavior, including an exact half-open probe;
- separate liveness, readiness, and processing-capacity health;
- expose only actionable supervisor health and process-budget state in the
  dashboard;
- strengthen the benchmark with durable ledgers, backend-operation evidence,
  image/source provenance, collision-resistant campaign IDs, ownership-checked
  cleanup, and fault injection.

Static metadata without an operational consumer and proposed micro-caches
without measured value were removed or rejected. This keeps the maintenance
burden proportional to demonstrated risk.

## 3. Method

The confirmation procedure was frozen before live execution in the
[production-hardening protocol](PRODUCTION_HARDENING_PROTOCOL_20260831.md). It
inherits the repository's [GA protocol](GA_PROTOCOL.md). The principal
anti-bias and validity controls were:

- matched Laravel fixtures and worker counts;
- fresh durable backend volumes and one fault lane at a time;
- exact expected job/effect sets and final queue quiescence;
- durable SQLite attempt/effect ledger, explicitly non-atomic with queue ACKs;
- container identity, restart, OOM, process-tree, and functional-readiness
  checks;
- captured image IDs, source provenance, resolved topology, logs, and artifact
  manifests;
- fail-closed handling of incomplete evidence;
- no removal of failed observations and no post-hoc outlier exclusion.

The protocol was designed and executed by the Queen team. It is professional
engineering evidence, not an independent certification. The host was not a
dedicated native Linux machine, so every result in this report remains
diagnostic.

## 4. Software verification

Verification of the hardening candidate completed with these results:

| Area | Result |
| --- | --- |
| PHP 8.3 | 546 tests, 2,366 assertions passed |
| PHP 8.4 | 544 tests, 2,360 assertions passed before the final focused circuit-test additions |
| PHP 8.5 | 544 tests, 2,360 assertions passed before the final focused circuit-test additions |
| Rust supervisor/client | 56 tests passed; Rustfmt and Clippy with `-D warnings` passed |
| Benchmark harness | 78 tests passed; Ruff, `bash -n`, and ShellCheck passed |
| Package | strict Composer validation, locked dependency audit, and PHP lint passed |
| CI/workflows | Actionlint passed |
| Documentation | 150-page build, generated-content check, Markdown lint (252 files), and prose checks passed |
| Language policy | no `*.it.md` files or Italian test/document artifacts found |

The optional full documentation source-of-truth inventory reported 82 stale
declarations in unrelated pre-existing documentation. The CI-equivalent docs
checks above passed; the stale inventory is not attributed to this change.

Commit `1673522c` additionally adds regression coverage for partial-ACK
delivery-attempt semantics and for monotonic, functional master recovery. It
also fixes the infrastructure result reader's missing `sqlite3` import.

## 5. Live functional parity

The weighted multi-queue campaign used three workers and 100 jobs per engine,
distributed as 60 `high`, 30 `default`, and 10 `low`.

| Engine | Exact weighted queues | Failed-job lifecycle | Worker identity | Container gate | Overall |
| --- | --- | --- | --- | --- | --- |
| Horizon | pass | n/a | pass | pass | pass |
| Queen PHP | pass | pass | pass | pass | pass |
| Queen Rust | pass | pass | pass | pass | pass |

All queues reached a settled empty state. Queen's failed lifecycle produced the
matching Laravel failed row and broker DLQ record, then successfully retried
the job and cleared both stores. The retained report is
[results/hardening-20260831-feature-parity/report.md](results/hardening-20260831-feature-parity/report.md).

## 6. Worker-SIGKILL recovery

Each engine ran 24 jobs on two workers. A worker proven to be executing user
code was killed with SIGKILL. The required gate is at-least-once recovery with
one fixture-local idempotent effect per key; strict single execution is a
separate observation.

| Engine | Full-pool respawn | Jobs | Effects | Queue empty | At-least-once | Strict execution |
| --- | ---: | ---: | ---: | --- | --- | --- |
| Horizon | 882.5 ms | 24/24 | 24 | yes | pass | fail |
| Queen PHP | 1,329.0 ms | 24/24 | 24 | yes | pass | fail |
| Queen Rust | 3,889.2 ms | 24/24 | 24 | yes | pass | fail |

Every engine observed a retry, stayed within the attempt bound, and had no
container restart or OOM. The strict gate fails as expected because killing a
worker after user code begins creates an execution window that the queue cannot
atomically close with arbitrary application side effects. The retained report
is [results/hardening-20260831-worker-fault/report.md](results/hardening-20260831-worker-fault/report.md).

The respawn measurements are recovery observations, not performance rankings:
there is one injected event per engine, and the lane includes scheduler and
poll timing.

## 7. Renewal-helper SIGKILL recovery

Queen used prefetch 4, ACK batch 1, renewal enabled, two workers, and 24 jobs.
The selected helper was proven to belong to a worker with active work before it
was killed.

| Engine | Helper fenced worker | Jobs/effects | Queue empty | Retry classified | At-least-once gate | Strict execution |
| --- | --- | --- | --- | --- | --- | --- |
| Queen PHP, original run | yes | 24/24 | yes | no | fail | fail |
| Queen Rust | yes | 24/24 | yes | yes | pass | fail |
| Queen PHP, post-fix rerun | yes | 24/24 | yes | yes | pass | fail |
| Queen Rust, post-fix rerun | yes | 24/24 | yes | yes | pass | fail |

The original PHP run did not lose work: it recorded exactly 24 unique jobs, 24
idempotent effects, no completion duplicates, no missing jobs, and a quiescent
queue. Its gate failed because the retried prefetched tail was still marked as
delivery attempt 1 after a completed prefix advanced the committed cursor.

The backend correction in `1673522c` advances `attempt_offset` to the first
uncommitted frame while a partial lease remains live. After lease expiry, the
tail is consequently classified as attempt 2. The new integration regression
test performs the exact sequence: pop two, ACK the prefix, expire the group,
and require delivery attempt 2 for the tail. This is a correctness fix, not a
relaxation of the fault gate.

The post-fix live rerun exited successfully. Queen PHP recovered full capacity
in 2,051.2 ms and Queen Rust in 3,230.0 ms. Each engine completed 24/24 unique
jobs with 24 conserved effects, zero completion duplicates, an observed retry,
an empty queue, and no container restart or OOM. Strict execution failed as
expected because the injected fault caused a retry. The original evidence is
retained in
[results/hardening-20260831-helper-fault/report.md](results/hardening-20260831-helper-fault/report.md).
The passing post-fix evidence is retained in
[results/hardening-20260831-helper-fault-fixed/report.md](results/hardening-20260831-helper-fault-fixed/report.md).

## 8. Infrastructure-fault campaign

The retry campaign completed 11 of the 13 planned engine/scenario lanes before
the cutoff. Every completed lane observed exactly 32/32 job IDs, an empty final
queue, a passing at-least-once gate, and a passing fixture-local idempotent
effect gate.

| Scenario | Engine | Jobs | Completion duplicates | At-least-once | Idempotent effect | Strict execution |
| --- | --- | ---: | ---: | --- | --- | --- |
| Redis restart | Horizon | 32/32 | 2 | pass | pass | fail |
| Broker restart | Queen PHP | 32/32 | 2 | pass | pass | fail |
| Broker restart | Queen Rust | 32/32 | 0 | pass | pass | pass |
| PostgreSQL restart | Queen PHP | 32/32 | 2 | pass | pass | fail |
| PostgreSQL restart | Queen Rust | 32/32 | 4 | pass | pass | fail |
| Application/backend partition | Horizon | 32/32 | 0 | pass | pass | pass |
| Application/backend partition | Queen PHP | 32/32 | 2 | pass | pass | fail |
| Application/backend partition | Queen Rust | 32/32 | 0 | pass | pass | pass |
| Broker/PostgreSQL partition | Queen PHP | 32/32 | 0 | pass | pass | pass |
| Broker/PostgreSQL partition | Queen Rust | 32/32 | 0 | pass | pass | pass |
| Master SIGKILL | Horizon | 32/32 | 1 | pass | pass | fail |

The official retry corpus did not complete the Queen PHP and Queen Rust master
lanes and therefore has no final aggregate report. A separate post-fix Queen
PHP master-SIGKILL audit completed 32/32 jobs, zero completion duplicates,
queue quiescence, and passing at-least-once and idempotent-effect gates. The
corresponding Queen Rust master lane is still required.

These runs exposed two harness defects, both fixed in `1673522c`:

1. the result writer used SQLite without importing `sqlite3`;
2. recovery deadlines used wall time, and master restart waited for container
   health instead of the configured worker pool and supervisor command to be
   functionally ready.

Deadlines now use a monotonic clock, and master recovery requires full worker
capacity plus functional supervisor readiness. The separate Queen PHP audit
exercised this corrected path. Lane artifacts are retained under
`results/hardening-20260831-infrastructure-faults-retry1`; the supplemental
audit report was generated under `/tmp/queen-master-health-audit.Bc21D8` and
must not be mistaken for a durable release artifact.

## 9. Performance evidence and limitation

The current hardening protocol specified a new matched comparison of Horizon,
Queen PHP, and Queen Rust: 4,000 jobs, 10 ms work, four workers, five repetitions,
Queen p4/a1 with renewal, and Redis AOF `appendfsync=always`. It was **not
completed before the cutoff**. No throughput, latency, CPU, or memory claim for
commit `242a0ed`/`1673522c` is made here.

The latest completed performance evidence remains the August 29 diagnostic
campaign on earlier commit `a79db4de`, with p4/a1 but renewal disabled:

| Median, 3 runs | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 294.30 | 300.08 | 307.67 |
| End-to-end p95 | 6,019.54 ms | 5,478.43 ms | 5,266.49 ms |
| Orchestrator PSS | 65.0 MiB | 35.1 MiB | 2.9 MiB |
| Measured stack CPU | 3.750 s | 4.148 s | 3.938 s |
| Measured stack RAM | 198.1 MiB | 295.1 MiB | 265.3 MiB |

In that earlier sample Queen Rust measured +4.6% completion throughput, -11.2%
p95, and -95.6% orchestrator PSS relative to Horizon. Complete-stack RAM was
34.1% higher because the compared Queen topology includes its broker and
PostgreSQL while Horizon uses Redis. Those numbers are historical optimization
evidence only: they use different code and omit the now-mandatory renewal
helper, so they cannot certify the current release candidate.

The earlier [qualification report](QUALIFICATION_REPORT_20260829.md) also
retains a longer 10,000-job campaign and stress screens. They must remain
separate from the present corpus. In particular, the approximately one million
messages/s Queen result refers to a native broker benchmark with Go loaders,
many partitions, and hundreds of consumers; it is not a Laravel jobs/s claim.

## 10. Reliability interpretation

Horizon and Queen both provide at-least-once delivery. Taking one job at a time
does not make Horizon exactly-once: if a worker dies after an external side
effect and before ACK, Laravel may execute the job again. Prefetch changes how
many leases a worker owns, not this fundamental boundary.

Queen's p4/a1 release profile limits ACK exposure to one completed job while
amortizing pop operations. Renewal protects the prefetched tail during pauses
or long-running work; helper fencing prevents a worker from continuing after
renewal safety is lost. ACK batch values above one remain excluded from the
general profile because they enlarge the unconfirmed-effect window.

The benchmark ledger enforces one fixture effect per `(run_id, job_id)` and
detects repeated attempts. It is not atomic with Queen ACKs, Redis ACKs, or an
arbitrary external system. Production jobs still need idempotency keys,
transactional state transitions, an outbox/inbox boundary, or an equivalent
application guarantee. Laravel's `WithoutOverlapping` middleware can reduce
concurrent execution through a shared cache lock, but it does not replace
idempotency or make effects exactly-once.

## 11. Release gates and next execution

The minimum remaining sequence is:

1. complete the Queen Rust master-SIGKILL lane and repeat the official Queen
   PHP master lane in the durable corpus;
2. execute the frozen five-repetition p4/a1-with-renewal comparison without
   changing the protocol after results are visible;
3. run the conservative p1/a1 control and 50,000-job zero-work stress screen;
4. repeat publishable performance and fault campaigns on dedicated native
   Linux amd64 and Linux arm64 hosts;
5. complete a 24-hour Laravel soak with attempt/effect conservation and resource
   trend analysis;
6. inject controlled latency, packet loss, disconnection, and recovery using
   `tc`/netem or an equivalent dedicated network harness;
7. test backend and ledger behavior under a disposable-volume disk-full fault.

Release approval must also require immutable retained artifacts, a clean pinned
checkout, real release-artifact installer smoke tests, and green CI. None of
these outstanding gates should be inferred from a shorter or different run.

## 12. Final recommendation

The engineering direction is sound: standard Laravel workers are preserved,
Queen Rust provides the low-memory control plane, and Queen PHP provides a
co-versioned fallback. The hardening features address measured failure modes
and have clear operational consumers.

However, the correct decision at this report freeze is **not production GA**.
Complete the missing master-fault corpus and current-commit performance
campaign. If those pass, deploy Queen Rust to a bounded canary with rollback to
Horizon or Queen PHP, alert on readiness/capacity degradation, track retries
and failed jobs, and require idempotent job behavior. Universal replacement
should wait for the native multi-architecture, 24-hour soak, network, and
disk-full gates.
