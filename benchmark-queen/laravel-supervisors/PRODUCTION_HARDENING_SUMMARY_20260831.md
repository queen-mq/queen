# Laravel production-hardening qualification summary

**Date:** August 31, 2026  
**Branch:** `feat/laravel-production-hardening`  
**Candidate:** `242a0ed88cfc3315084648a0a299263df119fa3a`  
**Frozen protocol:** `bed455b443560825289a8cbe5648e0fb78439c4d`  
**Test-driven corrections:** `1673522c`  
**Classification:** diagnostic, Docker Desktop on macOS arm64

## Decision

Queen is materially closer to production readiness, but this corpus does not
authorize universal GA or an unguarded Horizon replacement.

- Feature parity passed for Horizon, Queen PHP, and Queen Rust.
- Worker-SIGKILL at-least-once recovery passed for all three engines.
- Eleven completed infrastructure-fault lanes passed at-least-once delivery and
  fixture-local idempotent effects; a separate Queen PHP master audit also
  passed.
- The original Queen PHP helper-SIGKILL lane exposed stale attempt accounting
  after a partial ACK. The backend and regression test were fixed in
  `1673522c`; the post-fix live rerun passed for both Queen engines.
- The current-commit five-run performance comparison was not completed, so no
  new throughput, CPU, latency, or memory claim is made.

**Recommendation:** finish the missing master-fault corpus and current-commit
performance campaign, then use Queen Rust for a bounded canary with Queen PHP
as fallback. Keep a rapid rollback path. Do not declare GA until the native
Linux and long-duration gates pass.

## Results at a glance

| Gate | Result | Evidence |
| --- | --- | --- |
| Weighted multi-queue | pass, 3/3 engines | 100 jobs/engine, exact 60/30/10 set, empty queues |
| Queen failed lifecycle | pass, 2/2 engines | Laravel failed row + broker DLQ, retry, both stores empty |
| Worker SIGKILL | pass, 3/3 at-least-once | 24/24 jobs and effects per engine, empty queues |
| Renewal-helper SIGKILL | post-fix pass, 2/2 engines | 24/24 jobs and effects per engine, retry observed, empty queues; original failed observation retained |
| Infrastructure faults | pass, 11/11 completed lanes | 32/32 jobs, quiescent queue, at-least-once and idempotent-effect gates |
| Supplemental Queen PHP master SIGKILL | pass | 32/32 jobs, zero completion duplicates, functional capacity recovered |
| Current-candidate performance | incomplete | frozen p4/a1-with-renewal five-run comparison not completed |
| Universal GA | fail/incomplete | native Linux, 24-hour soak, netem, and disk-full outstanding |

Strict single execution is not the required recovery gate. It failed when a
worker was deliberately killed after user code began, as expected for
at-least-once systems. Neither Horizon nor Queen can make arbitrary external
effects atomic with queue acknowledgement; production jobs must be idempotent
or use a transaction/outbox-style boundary.

## Defects found and fixed

Commit `1673522c` contains only production-relevant corrections:

1. partial ACK now advances the live lease's `attempt_offset`, so an expired
   prefetched tail is reported as a redelivery;
2. the infrastructure result reader imports SQLite correctly;
3. fault deadlines use a monotonic clock;
4. master restart waits for actual supervisor readiness and configured worker
   capacity, not only container health.

No speculative caching or unconsumed metadata was retained.

## Verification

- PHP 8.3: 546 tests, 2,366 assertions.
- Rust: 56 tests; Rustfmt and Clippy clean.
- Benchmark harness: 78 tests; Ruff, Bash syntax, and ShellCheck clean.
- Composer validation/audit, PHP lint, Actionlint, documentation build/lint,
  and the English-only artifact scan passed.

## Performance context, not a current claim

The latest completed diagnostic comparison is from earlier commit `a79db4de`
and omitted the renewal helper now required for prefetch greater than one:

| Median, 3 runs | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 294.30 | 300.08 | 307.67 |
| End-to-end p95 | 6,019.54 ms | 5,478.43 ms | 5,266.49 ms |
| Orchestrator PSS | 65.0 MiB | 35.1 MiB | 2.9 MiB |
| Stack CPU | 3.750 s | 4.148 s | 3.938 s |
| Stack RAM | 198.1 MiB | 295.1 MiB | 265.3 MiB |

This supports the Rust control-plane design, especially its 95.6% lower
orchestrator PSS in that sample. It does not qualify commit `242a0ed` or
`1673522c`. Complete-stack RAM was higher for Queen because that topology
includes the broker and PostgreSQL rather than Redis.

## Remaining release gates

1. complete official Queen PHP/Rust master-SIGKILL corpus;
2. five repetitions of the frozen p4/a1-with-renewal comparison;
3. p1/a1 control and 50,000-job stress screen;
4. dedicated native Linux amd64 and arm64 campaigns;
5. 24-hour Laravel soak with resource-trend and ledger checks;
6. controlled latency/loss/disconnection fault injection;
7. disposable-volume disk-full recovery testing.

See the [full technical report](PRODUCTION_HARDENING_REPORT_20260831.md) and
[frozen protocol](PRODUCTION_HARDENING_PROTOCOL_20260831.md).
