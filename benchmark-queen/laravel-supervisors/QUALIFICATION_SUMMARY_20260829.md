# Horizon vs Queen PHP vs Queen Rust — qualification summary

**Date:** August 29, 2026

**Classification:** `diagnostic` performance results, run on Docker Desktop
arm64; `diagnostic_smoke` functional and fault tests.

**Measured code:** `a79db4de9bc9d59cdc6facf42a475bbb7a3dd7a1`.

## Outcome

Queen Rust is the recommended candidate to replace Horizon's control plane.
Queen PHP remains the reference engine and operational fallback. On the final
measured commit, with four workers, 2,000 jobs with 10 ms of work, prefetch 4,
ACK batch 1, and renewal disabled, all 9 lanes were correct, quiescent, and
isolated. This profile is now classified as diagnostic only:

| Median, 3 runs | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 294.30 | 300.08 | 307.67 |
| E2E p95 | 6,019.54 ms | 5,478.43 ms | 5,266.49 ms |
| Orchestrator PSS | 65.0 MiB | 35.1 MiB | 2.9 MiB |
| Measured stack CPU | 3.750 s | 4.148 s | 3.938 s |
| Measured stack RAM | 198.1 MiB | 295.1 MiB | 265.3 MiB |

In paired ratios, Queen Rust measured **+4.6% throughput**, **-11.2% p95**, and
**-95.6% orchestrator-only PSS** against Horizon. Stack CPU is inconclusive
with three pairs (ratio 1.031; 95% descriptive bootstrap 0.950–1.060). RAM for
the complete measured topology is **+34.1%**: Horizon uses Redis, while Queen
includes the broker and PostgreSQL. The memory advantage therefore applies to
the control plane, not automatically to the complete stack.

The longer historical campaign on commit `3655cd2a`, with five runs and 10,000
jobs per lane, measured Queen Rust at +14.05% throughput, -19.63% p95, -95.95%
orchestrator PSS, and -39.94% stack CPU, but +39.30% stack RAM. It is useful
optimization evidence and is not combined with the final-commit confirmation.

## Reliability and operations

- Multiple queues: 24/24 jobs per engine, exact sets, empty queues, and stable
  worker identity; all three lanes pass the gate.
- Queen failed jobs: Laravel record and DLQ snapshot present, successful manual
  retry, and both indexes cleared; Queen PHP and Rust pass the gate.
- Worker SIGKILL: Horizon, Queen PHP, and Queen Rust preserve 24/24 effects,
  observe the retry, and drain the queue. The at-least-once gate passes 3/3; the
  strict gate fails as expected because the interrupted attempt runs again.
- Leases: a Laravel queue can remain paused for an unbounded time while holding
  the prefetched tail. Therefore p4/a1 always requires renewal; p1/a1 can run
  without the helper only when one job finishes within the lease.
- The Laravel dashboard adds live/stale state, pools and workers, a bounded
  failed-job summary, and fenced `pause`, `continue`, and `terminate` commands.
  Failed-job mutations remain in Artisan commands, while global DLQ operations
  remain in the broker dashboard, so the related panel gate is only partially
  satisfied. The view is local to one master, not active-active multi-host.
- The state protocol publishes the active generation's effective
  configuration: the dashboard and CLI do not reinterpret Laravel
  configuration changed after startup. Heartbeats and command TTLs are
  generation-specific; pools, processes, documents, and telemetry share
  fail-closed limits across both engines.

## Distribution

The Laravel package distributes the launcher and explicit installer; Composer
does not download the Rust binary. The release and client are versioned
together. The installer selects the exact artifact, accepts a manifest from
HTTPS or a trusted local source, validates its schema/version/target, and
verifies the archive SHA-256, executable version, and local receipt. The
release pipeline builds and packages twice with byte-for-byte comparison and
publishes a Sigstore bundle and provenance. Checkout and build are pinned to
the prepared immutable commit, and the tag is checked again before publication.
The deploy or high-assurance mirror verifies the manifest signature; the
installer does not do so automatically. They can, however, require the
SHA-256 of the already verified manifest before extracting or executing the
binary. The release is prepared as a draft and published only after checking
the names and digests of all assets. The installer requires the install base's
immediate parent to exist as a trusted real directory, pins it before creating
only the final leaf, then pins base, version, and target by device/inode. The
launcher repeats the base/version/target pin and executes through a relative
path, so renaming an ancestor between check and exec cannot redirect the
binary. Filesystem root, traversal, and symlinks in dynamic directories are
rejected; when `storage/` is a symlink, its real target must be configured.
The supervisor's effective UID remains an explicit part of the trust boundary.

| Platform | x64 / amd64 | ARM64 |
| --- | --- | --- |
| Linux | prepared target, native static musl | prepared target, native static musl |
| macOS | native preview, Apple signing pending | native preview, Apple signing pending |
| Windows | not yet supported | not yet supported |

At report time, no public `supervisor/v*` tag/release exists: the matrix
describes what the pipeline is ready to generate, not assets that are already
distributed. GA requires native smoke/installer coverage for every target;
macOS also requires Developer ID signing and notarization. Before the first
release, immutable releases and server-side protection for `supervisor/v*`
tags must also be enabled; the later Composer publication must be conditional
on smoke tests against the real URLs.

Windows requires native backends for Job Objects, console control events,
locking, and process fencing. Cross-compilation alone would not make pause,
drain, and shutdown safe; the release explicitly rejects Windows. The planned
path starts with Windows x64 and native end-to-end tests, followed by ARM64
after PHP/Laravel availability on that target is qualified.

## Decision

Use Queen Rust as the default orchestrator and Queen PHP as fallback. P1/a1 is
the conservative profile without a helper; the production performance profile
is p4/a1 with renewal. The p4/a1 campaign reported here did not include the
helper and therefore remains diagnostic: it must be repeated with renewal
before a GA performance claim. Do not publish a universal GA claim yet: native
Linux amd64/arm64 publishable campaigns, a 24-hour Laravel soak, and
broker/network/PostgreSQL/storage faults remain outstanding. The historical
claim of about one million messages/s concerns the native broker with Go
loaders, 200 partitions, and 600 consumers, not Laravel jobs/s.

Final software verification: the complete PHP suite passed 473 tests/1,992
assertions, the Rust suite 53 tests, the benchmark harness 48 tests, and release
tooling 5 tests; Composer validate/audit, lint for 103 PHP files, Rustfmt,
Clippy, Ruff, ShellCheck, and Actionlint were green. Names, comments, fixtures,
output, and assertion messages in modified tests are in English; the rule is
now explicit in `CONTRIBUTING.md`.

Methodology, limitations, and artifacts are detailed in the
[technical report](QUALIFICATION_REPORT_20260829.md).
