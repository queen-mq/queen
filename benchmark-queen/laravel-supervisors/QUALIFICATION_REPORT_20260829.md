# Technical qualification: Horizon vs Queen PHP vs Queen Rust

**Date:** August 29, 2026

**Product and benchmark branch:** `feat/laravel-horizon-replacement`

**Measured commit:** `a79db4de9bc9d59cdc6facf42a475bbb7a3dd7a1`

**Dashboard and report branch:** `feat/laravel-supervisor-dashboard`

**Classification:** `diagnostic` performance; `diagnostic_smoke` feature and
fault tests.

## 1. Technical conclusion

The correct implementation does not use Queen as a replacement for
`queue:work`: workers remain standard Laravel processes. Queen PHP or Queen
Rust is the control plane that creates, sizes, drains, restarts, and distributes
those workers among queues, using shared configuration and limits equivalent
to a Horizon supervisor.

The results support these conclusions within the tested scope:

1. Queen Rust removes almost all resident memory from the PHP master: median
   PSS of 2.9 MiB versus 65.0 MiB for Horizon on the final commit.
2. P4/a1 removes the bottleneck observed with the old prefetch 1 profile. In
   the final confirmation, Queen Rust is +4.6% in completion and -11.2% in p95;
   in the longer historical confirmation it was +14.05% and -19.63%. These
   campaigns did not include the renewal helper that is now mandatory for
   p>1, so they remain diagnostic rather than a production performance claim.
3. Control-plane savings do not equal stack savings: the Queen broker plus
   PostgreSQL use more RAM than the Redis topology used by the Horizon fixture.
4. Recovery from SIGKILL preserves idempotent effects and demonstrates
   at-least-once semantics, not exactly-once.
5. Failed jobs, multiple queues, renewal, control, and precompiled distribution
   are now covered by the Laravel client. The dashboard completes the first
   application control surface, limited to the local master.

Queen Rust is therefore the recommended choice; Queen PHP remains the fallback
and reference implementation. General GA qualification remains conditional on
the native Linux and extended fault/soak gates listed in section 12.

## 2. Compared scope

| Lane | Backend | Orchestrator | Worker |
| --- | --- | --- | --- |
| Horizon | Redis | Horizon PHP master/supervisor | `artisan horizon:work` |
| Queen PHP | Queen + PostgreSQL | Laravel PHP master | `artisan queue:work queen` |
| Queen Rust | Queen + PostgreSQL | Rust `queen-supervisor` | `artisan queue:work queen` |

The benchmark uses the same Laravel application, serialized job, JSONL sink,
and worker limits. Metrics retain two distinct scopes:

- **control plane:** master/orchestrator processes, excluding workers and
  backends;
- **measured stack:** consumer application container plus the complete backend,
  Redis for Horizon and broker+PostgreSQL for Queen.

The producer, observer, and sampler are not attributed to the stack, although
they share the host. Redis and Queen+PostgreSQL do not have equivalent topology
or durability; the comparison represents the cost of the declared
configurations, not a broker-to-broker microbenchmark.

## 3. Method and anti-bias protections

The [GA protocol](GA_PROTOCOL.md) was frozen before the new campaign; its
[dashboard amendment](GA_PROTOCOL_AMENDMENT_PANEL.md) recorded the panel gates
before implementation. The protocol was designed by the Queen team and is not
an independent certification.

The applied procedure includes:

- one lane at a time, with fresh backends and volumes for every sample;
- matched workload, workers, cgroups, and configuration;
- rotated Horizon/Queen PHP/Queen Rust order;
- exact job set and queue quiescence as preliminary gates;
- no post-hoc outlier removal;
- medians, quartiles, per-run values, and deterministic paired bootstrap;
- container snapshots and a continuous Docker event watcher;
- fail-closed behavior for a dirty checkout, image/config mismatch, foreign or
  replaced containers, restart/OOM, incomplete sampling, and non-empty queue;
- retained raw artifacts even when a gate fails.

Fail-closed behavior was exercised in practice. The first final campaign had
9/9 functionally valid runs, but an unrelated local modification
(`benchmark-queen/.DS_Store`) made `git.dirty=true`, so all paired statistics
were suppressed. After preserving that change outside the worktree, the
campaign was repeated on the same clean commit and became comparable. No
override was used.

Bootstrap intervals describe only run variation on this host. With n=3, they
are not population guarantees and do not remove host, topology, or workload
bias.

## 4. Final-confirmation environment

| Field | Value |
| --- | --- |
| Host | macOS 26.5, arm64 |
| Runtime | Docker Desktop / Engine 29.7.2 |
| Container kernel | LinuxKit 7.0.12, aarch64 |
| Exposed resources | 10 CPUs, 8,214,851,584 bytes RAM |
| cgroup | v2 |
| Profile | fixed, 4 workers, p4/a1 |
| Lease renewal | disabled; profile now accepted only as diagnostic evidence |
| Workload | 2,000 jobs per lane, 10 ms sleep |
| Repetitions | 3 per engine, 9 total |
| Validity | 9/9 correct, quiescent, isolated |
| Qualification | `diagnostic` |

Docker Desktop is not a dedicated native Linux host. The figures are useful
for diagnosis, regression, and design choices, but they are not yet a
publishable multi-platform claim.

## 5. Final results on commit a79db4de

Median values; complete aggregates are in the [clean campaign
artifact](results/final-a79db4de-fixed-safe-clean/20260829T075448Z-a79db4de9b/campaign-stats.md).

| Metric | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 294.30 | 300.08 | 307.67 |
| E2E p95 ms | 6,019.54 | 5,478.43 | 5,266.49 |
| Orchestrator PSS MiB | 65.0 | 35.1 | 2.9 |
| Stack CPU s | 3.750 | 4.148 | 3.938 |
| Stack RAM MiB | 198.1 | 295.1 | 265.3 |

Paired candidate/Horizon ratios by repetition:

| Candidate | Throughput | E2E p95 | Orchestrator PSS | Stack CPU | Stack RAM |
| --- | --- | --- | --- | --- | --- |
| Queen PHP | 1.018×, CI 0.937–1.041 | 0.924×, CI 0.874–0.933 | 0.541× | 1.061×, CI 1.018–1.110 | 1.488× |
| Queen Rust | 1.046×, CI 1.044–1.064 | 0.888×, CI 0.850–0.898 | 0.044× | 1.031×, CI 0.950–1.060 | 1.341× |

Correct interpretation:

- Queen Rust completes 4.6% more jobs/s and reduces p95 by 11.2% in the final
  sample;
- Rust control-plane PSS falls by 95.6%;
- stack CPU is compatible with parity because the interval crosses 1;
- stack RAM increases by 34.1%, primarily because of the different backend;
- Queen PHP reduces master PSS by 45.9%, but its stack CPU is higher in the
  final sample and throughput is inconclusive.

The 60-job [short smoke
test](results/final-a79db4de-smoke/20260829T074910Z-a79db4de9b/report.md) is
retained but not used for the decision: startup and drain dominate such a short
window.

## 6. Historical optimization evidence

The overnight campaign isolated the main configuration error: p1/a1 limited
Queen to about 118–121 jobs/s versus 262 for Horizon. The p4/a1 confirmation on
commit `3655cd2a`, with 4 workers, 10,000 jobs with 10 ms of work, and 5 runs
per engine, recorded 15/15 valid runs and 150,000/150,000 unique jobs:

| Median metric | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 253.38 | 289.17 | 289.13 |
| E2E p95 ms | 37,547 | 30,403 | 30,067 |
| Orchestrator PSS MiB | 71.05 | 35.15 | 2.87 |
| Stack CPU s | 27.19 | 16.56 | 16.42 |
| Stack RAM MiB | 217.66 | 338.23 | 303.63 |

For Queen Rust, the ratios were +14.05% completion, -19.63% p95, -95.95%
orchestrator PSS, -39.94% stack CPU, and +39.30% stack RAM. This corpus uses an
earlier commit and is reported separately; it is not combined with the
a79db4de confirmation.

The selected historical corpus contains 99 runs and 1,248,000 valid,
quiescent jobs. It also observed:

- auto p4/a1: Queen Rust -3.3% jobs/s, -29.9% stack CPU, and -96.0% master PSS;
- CPU-bound: throughput substantially at parity;
- 50k zero-work stress: Queen Rust about 2.4× Horizon;
- ACK batch 4/16: higher throughput but more duplicates during faults;
- at 8 workers the advantage is not confirmed: paired CIs cross 1.

These scenarios guide configuration but do not authorize a single universal
ratio.

## 7. Correctness, failed jobs, and multiple queues

The [final feature-parity
test](results/final-a79db4de-feature-parity/report.md) used fresh backends and
verified:

| Engine | Multiple queues | Failed lifecycle | Worker identity | Container gate |
| --- | --- | --- | --- | --- |
| Horizon | pass | n/a | pass | pass |
| Queen PHP | pass | pass | pass | pass |
| Queen Rust | pass | pass | pass | pass |

Each engine completed exactly 24/24 jobs across two queues, and the final state
was empty. PIDs and Linux start ticks rule out an unobserved worker replacement.
For Queen, the test creates a terminally failed job, verifies both the Laravel
repository and broker DLQ, runs `queue:retry`, observes success, and requires
both stores to be empty.

The synchronized failed-job provider uses a random one-shot retry fence and a
distributed lock covering validation, DLQ cleanup, transactional publish, and
old-record removal. In a multi-host topology, that lock must actually be
shared; a local file lock is insufficient.

## 8. Faults and delivery semantics

In the [final fault
test](results/final-a79db4de-fault-recovery/report.md), a worker is killed
during user code:

| Engine | Respawn | Unique | Missing | Effects | Retry | Queue empty | At-least-once | Strict |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| Horizon | 997.5 ms | 24/24 | 0 | 24 | yes | yes | pass | fail |
| Queen PHP | 1,509.5 ms | 24/24 | 0 | 24 | yes | yes | pass | fail |
| Queen Rust | 1,442.8 ms | 24/24 | 0 | 24 | yes | yes | pass | fail |

The strict failure is expected: an interrupted attempt is retried. The
fixture's SQLite ledger preserves one effect per `(run_id, job_id)`, but it
does not make the ACK atomic with a side effect in an external database or
service. The application must be idempotent to tolerate at-least-once delivery.

Historical faults show why ACK batch remains 1 for the general profile: p4/a1
post-guard observed strict 15/15 under that specific timing, versus 3/10 for
p4/a4 and 1/10 for p16/a16. This is not an exactly-once guarantee: the final
fault during user code fails strict for every engine even with a1. Every lane
does preserve at-least-once delivery.

## 9. Leases and supported profiles

The profile without a helper is safe only with prefetch 1 and:

```text
retry_after > worst_case_runtime_of_one_job + margin
```

P1/a1 is the conservative choice without a helper when one job's runtime stays
bounded below `retry_after`. Every `prefetch > 1` instead requires renewal:
Laravel can retain an already leased tail during maintenance mode,
`queue:pause`, or a `Looping` listener for a duration that cannot be bounded by
the timeout/rest formula alone. The production performance profile to qualify
is therefore p4/a1 with the helper; the p4/a1 figures in section 5 measure
batching potential without that cost and must be repeated with renewal.

Renewal counts only a response with `renewed > 0` as successful, validates any
RFC 3339 expiration, and stops and fences the worker before it begins more work
when it can no longer guarantee the lease. This still does not turn delivery
into exactly-once.

Retained smoke tests include two 20-second PHP jobs completed with renewal, and
a Rust drain during the second 10.5-second job from a prefetched lease: 2/2
completions, zero duplicates, empty queue, and a passing ledger. In the renewal
fault with an unavailable broker, the helper fences the worker; after recovery,
the job runs on its second attempt, with one idempotent effect and an empty
queue. This is fail-safe/at-least-once evidence, not exactly-once evidence.

## 10. Distribution with the Laravel client

The Rust crate remains at the repository root because it has its own toolchain,
lockfile, and build lifecycle; **distribution** is coordinated by the Laravel
package:

```console
php artisan queen:supervisor-install
vendor/bin/queen-supervisor --php php --artisan artisan
```

Composer installs the launcher and performs no automatic download. The
explicit command:

1. detects OS and architecture without cross-platform fallback;
2. downloads a versioned manifest and archive over HTTPS, or uses a mirror or
   offline source;
3. requires exact version, target, filename, and SHA-256;
4. rejects symlinks, non-regular files, oversized input, and unexpected archive
   entries;
5. requires the install base's immediate parent to already exist as a trusted
   real directory, pins it by device/inode, and creates only the final base
   leaf;
6. publishes atomically under
   `storage/queen-supervisor-bin/<version>/<os>-<arch>`, retaining the pinned
   working directory and relative paths for locks, temporary files, and
   publication;
7. pins and rechecks the base, version directory, and target before running
   `./<temp-binary> --version`, then writes a local receipt;
8. has the launcher recheck the receipt and hash at every start, pin base,
   version, and target again, and call `pcntl_exec('./queen-supervisor')`
   without leaving a resident PHP process. Relative CLI paths are first
   resolved against the original application directory.

The manifest and receipt include the source commit. A deployment can provide
`--manifest-sha256` (or the equivalent environment variable) with the digest
obtained after Sigstore verification; comparison occurs before archive
selection, extraction, or execution.

The pipeline uses Rust 1.88 and locked dependencies, builds the binary and
archive twice, and requires byte-for-byte identity. It publishes checksums, a
canonical manifest, a Sigstore bundle, and provenance attestations. The GitHub
Release begins as a draft; the remote names and digests of every asset are
compared with `dist/` before publication. The preparation job resolves the tag
to an immutable commit SHA, all later checkouts use that SHA, and the
publication job reads the remote tag again and fails if it moved. Rustfmt,
tests, Clippy, the Laravel suite, Composer validate/audit, and the real Linux
amd64 installer are release gates. By default, the installer does not verify
an independent digest or manifest signature: without an explicit pin, it
trusts HTTPS and the release controls, or an already trusted file/mirror. It
then validates schema, version, commit, and target and uses the SHA-256 in the
manifest to verify the archive. Sigstore remains an explicit step for a
high-assurance mirror/deploy; the resulting digest can be required by the
installer. Filesystem-root bases, traversal, and symlinks in dynamic
directories are rejected. The immediate parent must already exist and be real;
if `storage/` is a symlink, the installation path must name its real target. A
different principal able to rename a writable ancestor can cause a failure or
DoS, but the pinned working directory prevents exec redirection; a process with
the supervisor's effective UID is inside the trust boundary.

| OS | amd64/x64 | arm64 | Status |
| --- | --- | --- | --- |
| Linux | native static musl | native static musl | preview pipeline |
| macOS 12+ | prepared native binary | prepared native binary | preview pipeline |
| Windows | — | — | explicitly rejected |

At report time, no public `supervisor/v*` tag/release exists, so none of the
four assets is distributed yet. The workflow is prepared for double
build/archive, binary smoke testing, and testing of the real Laravel installer
on Linux amd64. Linux arm64 and both macOS targets use native runners but do not
yet have Laravel/process-tree end-to-end tests or Developer ID
signing/notarization. Native qualification and provenance verification for
each asset remain release gates.

The repository must also enable GitHub immutable releases and a ruleset that
prevents changes or deletion of `supervisor/v*` tags. The workflow narrows the
window by creating a draft, comparing digests, and rechecking the tag
immediately before publication, but server-side policies prevent later
mutation. Composer publication must occur only after smoke tests against the
just-published real URLs; that coordination is currently procedural.

Windows requires a dedicated backend:

- Job Objects to own and terminate the complete worker tree;
- console control events or an equivalent pause/drain protocol;
- locking and atomicity with Windows semantics;
- instance fencing and crash tests without Unix signals/process groups;
- native x64/arm64 CI runners and end-to-end suites.

The recommended order is Windows x64 before ARM64. The first increment must
abstract the Rust process backend, assign every worker to a kill-on-close Job
Object, replace signals and process groups with cooperative control or console
events, use Windows locking and private DACLs, reject reparse points, and add
`.exe` naming/launcher support without `pcntl_exec`. The target can enter the
manifest only after crash, drain, takeover, and installation end-to-end tests
pass on native Windows runners. Windows ARM64 follows once the toolchain and
PHP/Laravel runtime are qualified natively.

Publishing a cross-compiled `.exe` today would make a false operational
promise. WSL can use the Linux artifact, but that is not native Windows
support.

## 11. Supervisor dashboard

The Laravel panel covers the local control plane, while the Queen broker
dashboard continues to cover queues, analytics, and the DLQ. The first version
exposes:

- live/stale state, engine, instance ID, PID, and heartbeat;
- global paused/running state and pools with active or draining workers;
- non-sensitive resolved configuration and queue depths when available;
- a failed-job summary without payloads, exception bodies, tokens, or headers;
- `pause`, `continue`, and `terminate` POSTs protected by authorization, CSRF,
  and expected `instance_id`; acceptance uses POST/Redirect/GET with a 303
  response and indicates only that the fenced command is pending.

It is disabled by default and denied by default in production until the
application defines the `viewQueenDashboard` Gate. It uses local assets, Blade
escaping, a Content Security Policy, and `no-store`; unavailable backends or
status become explicit states rather than errors that reveal details.

The panel reads the state directory of one master. It does not offer a
multi-host view or make two supervisors active-active; that would require
centralized heartbeats and fenced distributed leadership.

Status `v1` includes an allowlist of the configuration actually loaded by the
running generation, without URLs, tokens, or headers. From that same snapshot,
the dashboard and CLI use pools, depths, `control_ttl`, and
`heartbeat_timeout`: a change or rebuilt config cache before restart cannot
retroactively change the validity of the current master. Both engines limit
publishable pools to 256 and configuration/status to 1 MiB; telemetry, PIDs,
and queues have bounded cardinalities and sizes. Draining workers remain in
`process_limit` until they have actually exited. A status publication error is
fail-fast: the master drains workers and exits non-zero. After a forced kill,
it also retains the generation lock until every child is observed terminated,
preventing takeover and duplicate capacity during a failed or delayed kill.

The failed-job summary is intentionally read-only and bounded: it neither
exposes nor returns payloads or exceptions, and it does not run `COUNT(*)` on
every refresh. The file driver must still decode the bounded Laravel document
before discarding those fields. Retry, forget, flush, and prune remain in
Laravel commands, while global DLQ inspection and operations remain in the
broker dashboard. The index-policy label is inferred from the configured
connection and does not assert the live existence of the corresponding DLQ
row. Consequently, the frozen “access to the lifecycle” gate is only partially
satisfied by the first version: index state is visible and the operating path
is documented, but there are no direct web lifecycle actions yet.

## 12. Readiness and remaining gates

| Area | Status | Note |
| --- | --- | --- |
| PHP/Rust supervisor | RC-ready within Unix scope | single master |
| Autoscaling/multiple queues | functional | extended scaling still needs Linux replication |
| Failed lifecycle | functional | shared lock mandatory for multi-host |
| Lease renewal | implemented, unit/E2E smoke | mandatory with p>1; p1 without helper only for bounded jobs |
| Laravel dashboard | implemented on dedicated branch | local, not multi-host |
| Control/status protocol | generation-fenced and bounded | rolling incompatibility fails closed |
| Failed-job lifecycle in panel | partial | bounded metadata; mutations through Artisan/broker |
| Linux/macOS x64/arm64 release | preview targets prepared | no public asset; native and macOS-signing gates |
| Native Windows | unsupported | process backend to implement |
| Publishable performance | not yet | current host is Docker Desktop |
| At-least-once worker fault | verified | not exactly-once |
| Backend/network/storage faults | incomplete | GA gate |
| 24-hour Laravel soak | incomplete | GA gate |

Before a general GA claim:

1. repeat fixed/auto/scaling on dedicated Linux amd64 and arm64 hosts;
2. run a 24-hour Laravel soak per engine with a ledger and RAM-slope analysis;
3. inject broker, PostgreSQL, network, latency/loss, and full-storage faults;
4. complete the 1/2/4/8-worker matrix, three queues, partitions 1/4/16/64, and
   pop fusion off/on;
5. run native smoke tests and provenance verification for every release
   artifact;
6. document required idempotency and retain ACK batch 1 as the default;
7. for Windows, first complete the native backend and end-to-end suite;
8. decide whether to complete failed-job web actions or record an explicit
   revision of the panel gate before GA;
9. add macOS signing/notarization, enable immutable releases/tag rulesets, and
   automate GitHub-release-then-smoke-then-Composer publication.

## 13. Final software verification

Verification on the dashboard branch, in addition to the scenario tests above,
produced:

| Verification | Result |
| --- | --- |
| Complete PHPUnit, PHP 8.5.9 | 473 tests, 1,992 assertions, pass |
| Focused dashboard PHPUnit | 21 tests, 117 assertions, pass |
| Focused supervisor PHPUnit | 108 tests, 474 assertions, pass |
| Focused installer PHPUnit | 21 tests, 146 assertions, pass |
| Rust `cargo test --locked` | 53 tests, pass |
| Python benchmark harness | 48 tests, pass |
| Python release tooling | 5 tests, pass |
| Composer validate and audit | valid, no advisory |
| PHP lint (103 files), Rustfmt, Clippy `-D warnings` | pass |
| Ruff, ShellCheck, Bash syntax, Actionlint | pass |
| Language of modified tests/fixtures | English, pass |

This matrix verifies code, contracts, parsers, and static workflows; it does
not replace pipeline execution on all four target runners or the native Linux
and Windows gates above. The policy added to `CONTRIBUTING.md` requires English
names, comments, fixtures, output, and assertion messages in tests. The
qualification reports are now English as well.

## 14. The “one million messages/s” claim

The [historical broker-native
soak](../2026-08-11-soak24-1M/results.md) measured about 999,652 accepted
pushes/s on average for 24 hours with Go loaders, 200 partitions, 600
consumers, dedicated hosts, and 10 Gbit/s networking. It covers declared broker
capacity but does not traverse Laravel serialization, PHP workers,
orchestration, or application side effects.

It therefore does not conflict with 250–310 Laravel jobs/s for these workloads,
and it must not be presented as one million Laravel jobs/s or as exactly-once
evidence. The two benchmarks answer different questions.

## 15. Primary artifacts

- [GA protocol](GA_PROTOCOL.md)
- [Dashboard amendment](GA_PROTOCOL_AMENDMENT_PANEL.md)
- [Final clean confirmation](results/final-a79db4de-fixed-safe-clean/20260829T075448Z-a79db4de9b/campaign-stats.md)
- [Dirty fail-closed example](results/final-a79db4de-fixed-safe/20260829T075042Z-a79db4de9b/campaign-stats.md)
- [Final feature parity](results/final-a79db4de-feature-parity/report.md)
- [Final fault](results/final-a79db4de-fault-recovery/report.md)
- [Rust graceful drain with renewal](results/final-rust-graceful-drain/gate.json)
- [PHP renewal, runtime beyond lease](results/lease-renewal-success-php/result.json)
- [Rust renewal fault and retry](results/lease-renewal-fault-retry-rust-v2/ledger-analysis.json)
- [Historical overnight summary](results/nightly-20260828/report-summary.md)
- [Historical overnight technical report](results/nightly-20260828/technical-report.md)
- [README and reproduction](README.md)

Artifacts under `results/` are intentionally excluded from the Git repository
when they are too large, but they are preserved in the workspace. The two
qualification reports and protocols are versioned.
