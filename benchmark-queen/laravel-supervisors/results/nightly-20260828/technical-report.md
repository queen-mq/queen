# Horizon vs Queen PHP vs Queen Rust — technical report

**Document status:** complete, auditable report, updated August 29, 2026.

**Laravel result classification:** diagnostic.

**Fault-test classification:** diagnostic_smoke.

**Frozen protocol:** [protocol.md](protocol.md).

The Queen team designed and ran the benchmark. A predefined protocol, run
retention, pairing, and fail-closed gates are anti-bias measures; they make the
analysis reproducible and auditable, not independently certified.

## 1. Objective and conclusion

The campaign addresses four separate questions:

1. Is the Queen Laravel client, when configured correctly, competitive with
   Horizon?
2. What is the cost of the PHP or Rust orchestrator alone?
3. What is the cost of the consumer+backend scope actually measured?
4. Do worker crashes, high volume, and prolonged operation preserve
   correctness and operability?

The artifacts support the following answer:

- the old prefetch 1 configuration severely constrained Queen; with
  **prefetch 4 and ACK batch 1**, Queen PHP and Queen Rust outperform Horizon
  by about 14% in the primary fixed profile on measured post-guard commit
  3655cd2a;
- Queen Rust retains throughput similar to Queen PHP while reducing control
  plane PSS to about 4.0% of Horizon;
- in the primary profile Queen Rust uses about 40% less CPU in the measured
  stack, but about 39% more memory than the Horizon+Redis topology;
- in the CPU-bound workload, throughput and measured-stack CPU converge while
  the Rust orchestrator advantage remains;
- with one worker Queen is about 2% above Horizon; with eight workers its
  medians are lower, but the paired CIs cross 1;
- all 15 post-guard p4/a1 fault lanes complete strict recovery; p4/a4
  preserves at-least-once behavior in 10/10 lanes but strict passes only 3/10
  because of seven duplicates;
- pre-guard p16/a16, with a nominal runtime floor below the lease, preserves
  at-least-once behavior in 10/10 lanes but strict passes only 1/10, with 17
  duplicates;
- the approximately 1M msg/s soak demonstrates Queen's broker-native
  capability, but is not a Laravel benchmark and cannot be numerically
  compared with jobs/s.

The proposal is a **Rust orchestrator**, but with the conservative **p1/a1**
shipping default. P4/a1 remains a short-job performance profile only when
`retry_after > prefetch × worst-case runtime + margin`, or when lease renewal
exists. The 64 partitions and disabled pop fusion are the verified baseline,
not parameters selected through a dedicated comparison. Queen PHP remains a
fallback; every ACK batch >1 remains an idempotent opt-in. The conclusion is a
conditional readiness recommendation: it does not yet authorize a general
claim that Queen is a production-ready replacement for Horizon. The scope
verifies performance and recovery, not feature parity for Horizon's dashboard,
metrics, failed-job store, pause/resume, or other operational controls.

The performance claim does not carry over to p1/a1: in the legacy profile
Queen PHP/Rust measured 118.15/121.39 jobs/s versus Horizon's 261.85, about
-55%. Rust is recommended as the control plane; equivalent or better
performance requires p4 with a suitable lease or future lease renewal.

## 2. Systems compared

| Lane | Queue backend | Orchestrator | Worker process |
| --- | --- | --- | --- |
| Horizon | Redis | Horizon master and supervisors in PHP | artisan horizon:work |
| Queen PHP | Queen broker + PostgreSQL | Queen Laravel command in PHP | artisan queue:work queen |
| Queen Rust | Queen broker + PostgreSQL | queen-supervisor in Rust | artisan queue:work queen |

The fixture uses PHP 8.3.33, Laravel 12.68.0, and Horizon 5.48.3. The Queen
client is built from the checkout and reported as dev-main; the Rust
supervisor is built from the same checkout. Every lane runs the same serialized
BenchmarkJob and writes to the same type of JSONL sink outside the queue
backend. The complete fixture description is in the
[benchmark README](../../README.md).

The comparison intentionally exposes two views:

- **control plane:** CPU and PSS of orchestration processes, excluding workers
  and backends;
- **measured stack:** consumer application cgroup (supervisor and workers) plus
  the complete backend, meaning Redis for Horizon and broker+PostgreSQL for
  Queen; the harness producer, observer, and sampler are excluded from the
  aggregate.

Comparing only the Queen broker with Redis while omitting PostgreSQL would be
misleading. Likewise, orchestrator PSS cannot be used as the memory of the
measured scope, which in turn does not include the producer and observer.

## 3. Provenance

### 3.1 Freeze and code history

The protocol was frozen before the results were inspected:

| Step | Commit | Meaning |
| --- | --- | --- |
| Freeze | f2c39fd98987c2628c7d9a8bb6cd57c4ecbb16cc | protocol and selection rules |
| Hot path | 92c2cbe33c6c80c6133957573a69182ca02bb9dc | trimmed Laravel worker paths |
| Harness | 5b47a7d579abd9258cb073492b2b229a7f95f377 | made the supervisor methodology fail-closed |
| Observer | 2a4b107a0690bdfd68b7cfeb840b3987d773b5e4 | scalable, streaming result collection |
| Aggressive confirm/fault | 327849a1edbae2b1c5f17c402062dbf5dc5c4237 | p4/a4+p16/a16 confirmation and recovery |
| Post-benchmark lease guard | c405902736d527a1a03fceda380e68f89c7df814 | fail-fast for unsafe prefetch/lease windows |
| Harness lease budget | 3655cd2a6302e296722bab207a704e38726805f7 | guard application, smoke, fixed, and measured post-guard fault tests |
| Tooling hardening | 774c24136a55cd5bf4ec9f518ef41af3b20e975f | clarification of the negative override; after the measured images |

This is a timestamped local freeze: the mtime of `protocol.md` (02:45:19 local)
precedes the start of screening (02:45:33), but the document is not a signed
preregistration or an immutable third-party artifact.

The worktrees recorded in metadata for the selected and confirmatory campaigns
are clean; one-shot dirty screenings are excluded. The freeze recorded
preliminary app image
sha256:adaac94a4f2ce503db1c27ca26697a37872f6c8fa57831352afa1025d1d1be3e and
broker image
sha256:472ac63907a8168702b037053258fd0f01e0c04180d214b429a4f3d79b3dad7a.
These must not be confused with the images actually measured, which were
captured in each campaign's metadata:

| Measured campaigns | Clean commit | App image | Broker image |
| --- | --- | --- | --- |
| historical pre-guard fixed/auto, legacy, CPU, select p4/p16 | 5b47a7d579abd9258cb073492b2b229a7f95f377 | sha256:39f291705a9acf7408daa4d06818e766c865f77dbebcdd451883f1c60394b918 | sha256:c239df18950ee0598d1517a9f9989a29dd8aa2ed664b33ead1f2f3feebd4187a |
| a1 stress, w1/w8 scaling, a4 discovery, a1 fault | 2a4b107a0690bdfd68b7cfeb840b3987d773b5e4 | sha256:9386d3716853a6f9d09c9e455ce9c95326a01516655747fa785566543b7ccf6c | sha256:c239df18950ee0598d1517a9f9989a29dd8aa2ed664b33ead1f2f3feebd4187a |
| fixed/auto p16/a16 and p4/a4+p16/a16 fault | 327849a1edbae2b1c5f17c402062dbf5dc5c4237 | sha256:b631f6f6ff90202611ac337fecf10d88e557509ea545f9965df023aa0c0cc21d | sha256:c239df18950ee0598d1517a9f9989a29dd8aa2ed664b33ead1f2f3feebd4187a |
| primary fixed, p4/a1 fault, and measured post-guard smoke | 3655cd2a6302e296722bab207a704e38726805f7 | sha256:0e82c6cc8930410001d2a65de5c73abe1f39811f0dd3e78476f8e09071aa8958 | sha256:522bdc090da5c0845990444194d25c916abe3ce8a34bc653093e1aba87e801fd |
| post-guard auto p4/a1 | 774c24136a55cd5bf4ec9f518ef41af3b20e975f | sha256:0e82c6cc8930410001d2a65de5c73abe1f39811f0dd3e78476f8e09071aa8958 | sha256:522bdc090da5c0845990444194d25c916abe3ce8a34bc653093e1aba87e801fd |

The table uses the complete image IDs recorded in the
[stress metadata](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/metadata.json),
the [post-guard a1 fault images](fault-p4-a1-guarded-r01/images.json), the
[a4 fault images](fault-p4-a4-r01/images.json), and the
[p16/a16 fault images](fault-p16-a16-valid-r01/images.json). The resolved
manifests of individual base images are in
[containers.json](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/queen-rust/fixed/r01/containers.json).
There is no SBOM attestation that cryptographically binds commits to images;
the relationship is the one recorded by the harness.

Commits c4059027/3655cd2a are advances made after the historical campaigns:
the primary fixed and p4/a1 fault tests were repeated on the measured
post-guard image, while auto, stress, scaling, and aggressive profiles remain
on the images listed in their respective rows. This separates experimental
evidence, mitigation derived from the pilot, and post-fix verification.

The tracked branch now ends at 774c2413 with a clean worktree; that commit
clarifies override semantics in the tooling and is not incorporated into app
image sha256:0e82c6cc…8958. Fixed and fault metadata point to 3655cd2a; the
post-guard auto campaign was orchestrated later with metadata at 774c2413,
reusing the same measured application image.

The "Broker image" column reports the Queen digest recorded in campaign
metadata, not every backend in the lane. A separate audit of the
`containers.json` files from post-guard campaign 3655cd2a and the pre-guard
fixed campaign resolves Redis to
`sha256:02419de7eddf55aa5bcf49efb74e88fa8d931b4d77c07eff8a6b2144472b6952`
and PostgreSQL to
`sha256:38471f330eb885e04de130b768d6db4e10469e2311879c7e5c699f6d2d8a1c74`.
`campaign-stats` does not use these digests as an automatic gate: provenance
must be read campaign by campaign, not as one implicit digest for all 99 runs.

### 3.2 Local host

All considered Laravel campaigns ran on the same host:

| Field | Value |
| --- | --- |
| Host | macOS 26.5, arm64 |
| Runtime | Docker Desktop / Engine 29.7.2 |
| Container VM | LinuxKit 7.0.12, aarch64 |
| Exposed CPUs | 10 |
| Exposed memory | 8,214,851,584 bytes |
| cgroup | v2 |
| Mode | one measured lane at a time |

The protocol records the absence of foreign containers at freeze time and the
inhibition of macOS sleep. Fault metadata confirms that no pre-existing
containers were present in the five p4/a1 rounds.

Docker Desktop shares the kernel among containers, so monotonic timestamps are
comparable within a run. It is not equivalent to native Linux for scheduling,
I/O, or cgroup counter quality; the qualification therefore remains
diagnostic.

### 3.3 Soak provenance

The [24 h soak](../../../2026-08-11-soak24-1M/results.md) belongs to another
campaign:

- Queen broker 1.0.0, commit 074de2e; the image identifies itself as beta.4
  because the version bump did not modify server/src;
- broker and PostgreSQL on queen-01: 32 vCPU Xeon Gold 6548N, 62 GiB, 387 GB
  ext4 disk, fdatasync about 70 µs;
- three separate loaders, each with 16 vCPU and 31 GiB;
- VPC measured at 10.1 Gbit/s in each direction.

The soak report does not record an OCI digest. It must therefore not be
attributed to the Laravel campaign images.

## 4. Controlled methodology and anti-bias measures

### 4.1 Predefined rules

The [protocol](protocol.md) was frozen before screening was inspected. It
specified:

- fixed as the primary comparison;
- the same worker bounds and job for every lane;
- selection of the smallest setting within 5% of the valid maximum;
- ACK batches greater than 1 classified separately as aggressive;
- correctness, queue, sampler, OOM, and restart as gates rather than notes;
- no post-hoc outlier removal;
- at least three replicates per primary cell, five where possible;
- high-volume stress and auto testing on the selected profile;
- fault testing qualified as diagnostic, with no exactly-once claim.

The harness rotates lane order, recreates backends and volumes for every
sample, and retains every run. Builds and dependency installation do not
overlap measurement.

The initial discovery was a one-shot Queen Rust grid: one run per cell, strict
prefetch 1/4/8/16/32/64 and full-batch 4/8/16/32/64, with initial and final
anchors. Every cell in [screen](screen/01-anchor-start/20260829T004533Z-f2c39fd989/metadata.json)
and the [screen-clean](screen-clean/02-p4-a1/20260829T005411Z-f2c39fd989/metadata.json)
subset records `metadata.git.dirty=true` and is excluded from confirmations
and the 99 selected runs. Anchor completion-span decreases from
[133.28](screen/01-anchor-start/20260829T004533Z-f2c39fd989/report.json) to
[112.22 jobs/s](screen/12-anchor-end/20260829T005149Z-f2c39fd989/report.json),
a -15.8% change, so the grid is not confirmatory quantitative evidence. For
initial selection, only p4/a1 and p16/a1 were replicated after the clean
commit; p16/a16 was confirmed across engines but was not selected from a
replicated clean grid. The planned follow-up for partitions 1/4/16/64 and pop
fusion off/on was not run. P4/a1 is therefore the smallest performance
baseline among replicated candidates, not the global optimum or a shipping
safety default demonstrated on the final SHA.

The protocol prescribed 512 warm-up jobs for discovery. `select-p4`,
`select-p16`, and the historical safe fixed campaign on commit 5b47a7d5 do not
record `warmup_jobs` because that runner version did not expose it; the primary
post-guard fixed campaign at 3655cd2a explicitly records warm-up 0, while the
aggressive fixed confirmation records 512. This does not break pairing within
individual campaigns, but it is a protocol deviation and rules out a causal
safe↔aggressive comparison.

### 4.2 Budget and scope

| Component | Budget per lane |
| --- | --- |
| Application/supervisor/workers | 4 CPU, 1 GiB, pids 512 |
| Horizon backend | Redis: 2 CPU, 2 GiB |
| Queen backend | broker: 1 CPU, 1 GiB; PostgreSQL: 1 CPU, 1 GiB |
| Sampling | 0.5 s in the reported campaigns |

The aggregate backend budget is therefore 2 CPU/2 GiB for both topologies.
The producer runs outside the measured cgroups. The result sink is a shared
volume, separate from Redis and PostgreSQL, so observation cost is not credited
to either backend.

"Outside the measured cgroups" does not mean free from perturbation: the
producer has a 4 CPU quota and shares the same VM. Application 4 CPU + backend
2 + producer 4 nominally saturate the 10 exposed CPUs before the sampler; in
post-guard fixed campaign 3655cd2a, Queen dispatch lasts about 1.8× as long as
Horizon dispatch. The accounting correctly excludes the producer from
resource ratios, but cannot exclude contention during the dispatch window.

Budget parity does not make the semantics equivalent: Redis is volatile in
the Horizon fixture, while Queen uses PostgreSQL with synchronous_commit on.
In addition, the local Compose configuration mounts /var/lib/postgresql/data
on a 768 MiB tmpfs: synchronous_commit governs database commits but does not
measure persistence on physical storage here. The ratios describe CPU and
memory for the selected topology, not production-equivalent durability or I/O.

### 4.3 Validity gates

A run is eligible only when:

- the completed job set exactly matches the expected set;
- missing, duplicate, failed, and unexpected jobs are zero;
- the queue is stably at zero, including ready, reserved, and delayed when
  exposed;
- the sampler and PSS coverage are valid;
- the sampler reports no OOM events;
- configuration, workload, resources, and sampling match within the baseline/
  candidate pair.

The campaign aggregator matches profile+rXX and compares a canonical key that
includes workload, worker bounds, dispatch mode/batch, Queen knobs, transport,
sampling, and resolved limits. Expected differences in engine, connection,
and backend topology are allowed; every other difference suppresses the ratio.

Across the six selected post-guard fixed/auto, legacy, CPU, and select p4/p16
groups, 48/48 runs are valid, 336,000/336,000 jobs completed, 48/48 queues are
quiescent, and there are no missing jobs, duplicates, failures, sampler errors,
or OOM events. The a1 stress test adds 9/9 runs and 450,000 jobs; w1/w8 scaling
adds 18/18 runs and 54,000 jobs; a4 discovery adds 6/6 runs and 300,000 jobs;
the p16/a16 fixed/auto confirmations add 18/18 runs and 108,000 jobs. The
selected total is therefore **99 runs and 1,248,000 measured jobs**, all valid
and quiescent. Post-guard fixed and auto replace the old pre-guard campaigns
one-for-one (24 runs/204,000 jobs), which are retained as redundant
corroboration and are not added to the matrix. The gross inventory of all
successful retained performance artifacts, including redundant confirmations
and smoke tests, is **132 lanes and 1,602,360 jobs**: corpus 99 + historical
fixed 15 + historical auto 9 + stress-v2 3 + post-streaming smoke 3 +
post-lease smoke 3. It excludes the initial smoke and post-freeze smoke (6
lanes/360 jobs), dirty screenings, the observer failure, and faults. This is an
explicitly enumerated selection of successful artifacts for provenance
transparency; it does not increase n.

Job count measures volume and correctness, not artificially increased
statistical sample size: the independent unit remains the run, n=3 or n=5 per
confirmatory cell.

The selected 99-run corpus excludes warm-ups, the first failed stress run,
dirty `screen/` and `screen-clean/`, post-freeze smoke, the one-repetition-per-
engine stress-v2 confirmation, and other redundant screenings/reruns not in
the matrix. Fault tests are counted separately: 35 lanes and 840 unique jobs,
35/35 at-least-once and 19/35 strict, with 24 duplicates observed under p4/a4
or p16/a16. The two lanes from the negative p16/a16 test with an insufficient
lease are preserved but excluded from these totals. The 15 post-guard p4/a1
fault lanes replace the 15 pre-guard rounds one-for-one, which are retained as
redundant corroboration. The retained gross fault inventory is therefore 50
lanes/1,200 unique jobs, 50/50 at-least-once, 34/50 strict, and 24 duplicates;
the two-lane/48-job negative pilot remains separate.

The campaign aggregator does not use RestartCount as an automatic gate. A
separate audit of retained `containers.json` artifacts nevertheless finds
RestartCount=0 and OOMKilled=false in all 99 runs. "Zero restarts" is therefore
a verified result from container artifacts, not a fail-closed property of the
aggregator.

The primary fixed campaign was repeated on measured post-guard commit
3655cd2a with a 120 s timeout, prefetch 4, and retry_after 481 s, so it passes
the `>480 s` guard: 15/15 runs are valid and quiescent. The previous fixed
campaign with retry_after 180 s remains pre-guard corroboration; p16 campaigns
would now require >1,920 s and remain historical evidence on the declared
images. The n=1 smoke test per engine is not used for inference; the n=5
post-guard campaign is the primary performance result. The p4/a1 fault test
was also repeated post-guard with retry_after 41 s, but its timeout/retry and
worker count differ from the 481 s performance profile: it validates knob
safety, not the profile's exact recovery time.

### 4.4 Statistics

Absolute aggregates report n, median, R-7 quartiles, and IQR. Comparisons are
candidate/Horizon ratios for the same repetition. The 95% CI is a
deterministic percentile bootstrap of the median paired ratios:

| Parameter | Value |
| --- | --- |
| Global seed | 20260829 |
| Resamples | 10,000 |
| Per-cell seed | first 64 bits of SHA-256(seed, profile, engine, metric) |
| Ratio | candidate / Horizon |

For throughput, a ratio greater than 1 is favorable; for latency, CPU, and
memory, a ratio less than 1 is favorable. With n=3, the bootstrap may mostly
reproduce observed values and does not justify broad inference. The intervals
quantify variability in the paired ratios observed on this host; they are not
a guarantee, a population inference, or a significance test. No correction
for multiple comparisons is applied.

### 4.5 Operational definitions

- **Completion-span throughput:** completed jobs divided by the interval
  between first and last completion; this is the primary drain metric.
- **Headline throughput:** dispatch-to-last-completion; includes the producer
  window.
- **E2E p95/p99:** monotonic enqueue and completion timestamps in the shared
  sink; includes arrival shape and scenario dispatch duration and does not
  isolate broker/service latency. Paired comparisons keep dispatch mode and
  batch equal; dispatch throughput and headline throughput are mandatory
  guardrails for interpreting the metric.
- **Orchestrator CPU:** CPU of identified supervisor/master processes.
- **Orchestrator PSS:** peak PSS of those processes; workers are excluded.
- **Stack CPU/RAM:** consumer application cgroup plus complete backend, using
  cgroup CPU delta and peak memory.current; producer, observer, and sampler are
  excluded.
- **Worker-seconds:** integral of the observed worker count in the auto profile.

## 5. Campaign matrix

| Campaign | Engine | Profile | Runs/engine | Jobs/run | Work shape | Dispatch | Queen |
| --- | --- | --- | ---: | ---: | --- | --- | --- |
| [fixed post-guard 3655](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json) | 3 | fixed 4 | 5 | 10,000 | sleep 10 ms, warm-up 0 | single | p4/a1, retry 481 s, 64, fusion off |
| [legacy](confirm-fixed-legacy/20260829T014350Z-5b47a7d579/campaign-stats.json) | 3 | fixed 4 | 3 | 6,000 | sleep 10 ms | single | p1/a1, 64, fusion off |
| [auto p4/a1 post-guard](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.json) | 3 | auto 1…4 | 3 | 6,000 | sleep 10 ms | single | p4/a1, retry 481 s, 64, fusion off |
| [aggressive fixed](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.json) | 3 | fixed 4 | 3 | 6,000 | sleep 10 ms, warm-up 512 | single | p16/a16, 64, fusion off |
| [aggressive auto](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.json) | 3 | auto 1…4 | 3 | 6,000 | sleep 10 ms | single | p16/a16, 64, fusion off |
| [safe CPU](confirm-cpu-safe/20260829T020132Z-5b47a7d579/campaign-stats.json) | 3 | fixed 4 | 3 | 6,000 | 25,000 iterations | single | p4/a1, 64, fusion off |
| [select p4](select-p4/20260829T012603Z-5b47a7d579/campaign-stats.json) | Rust | fixed 4 | 3 | 4,000 | sleep 10 ms | single | p4/a1 |
| [select p16](select-p16/20260829T012742Z-5b47a7d579/campaign-stats.json) | Rust | fixed 4 | 3 | 4,000 | sleep 10 ms | single | p16/a1 |
| [scale w1](scale-w1-safe/20260829T024935Z-2a4b107a06/campaign-stats.json) | 3 | fixed 1 | 3 | 3,000 | sleep 10 ms, warm-up 256 | single | p4/a1 |
| [scale w8](scale-w8-safe/20260829T030004Z-2a4b107a06/campaign-stats.json) | 3 | fixed 8 | 3 | 3,000 | sleep 10 ms, warm-up 256 | single | p4/a1 |
| [stress 50k](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.json) | 3 | fixed 4 | 3 | 50,000 | zero-work | bulk 100 | p4/a1, 64, fusion off |
| [discovery ACK4](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.json) | PHP/Rust | fixed 4 | 3 | 50,000 | zero-work | bulk 100 | p4/a4, 64, fusion off |

## 6. Primary fixed results

The primary comparison is the
[confirm-fixed-safe-head](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json):
campaign: clean commit 3655cd2a, app image sha256:0e82c6cc…8958, four workers,
10,000 jobs per run, 10 ms sleep, warm-up 0, single dispatch, prefetch 4, ACK
batch 1, 120 s timeout, and retry_after 481 s. All 15/15 runs are valid and
quiescent: five repetitions per engine and 150,000/150,000 unique jobs. The
audit of 40 container snapshots finds zero restarts/OOM events; app, backend,
and stack report zero throttling in every run.

Median [Q1–Q3]:

| Metric | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 253.38 [252.37–253.51] | 289.17 [288.40–290.01] | 289.13 [285.03–292.61] |
| Headline jobs/s | 243.66 [242.84–243.67] | 288.98 [288.19–289.81] | 288.94 [284.84–292.39] |
| Dispatch jobs/s | 6,322.41 [6,306.81–6,332.39] | 3,568.46 [3,548.47–3,573.32] | 3,530.46 [3,525.64–3,545.22] |
| E2E p95 | 37,547 ms [37,530–37,706] | 30,403 ms [30,300–30,438] | 30,067 ms [30,015–30,355] |
| E2E p99 | 39,078 ms [39,010–39,216] | 31,499 ms [31,398–31,541] | 31,208 ms [31,179–31,605] |
| Orchestrator CPU | 0.14447 s [0.14172–0.14547] | 0.03201 s [0.03095–0.03247] | 0.01409 s [0.01379–0.01426] |
| Orchestrator PSS | 71.05 MiB | 35.15 MiB | 2.87 MiB |
| Worker CPU | 21.461 s | 5.376 s | 5.195 s |
| Backend CPU | 5.607 s | 11.061 s | 11.132 s |
| Stack CPU | 27.191 s [27.135–27.370] | 16.563 s [16.471–16.637] | 16.423 s [16.339–16.439] |
| Stack RAM | 217.66 MiB | 338.23 MiB | 303.63 MiB |

Queen's lower worker CPU must not be read in isolation: Queen moves more work
into broker+PostgreSQL, as backend CPU shows. Stack CPU is the correct view of
the overall operating cost.

Paired candidate/Horizon ratios, median [95% bootstrap CI]:

| Metric | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion-span | 1.14276 [1.13453–1.15147] | 1.14049 [1.12052–1.16322] |
| Headline | 1.18675 [1.18023–1.19656] | 1.18577 [1.16440–1.20805] |
| Dispatch | 0.56429 [0.55205–0.58451] | 0.56074 [0.55610–0.57749] |
| E2E p95 | 0.80964 [0.80357–0.81408] | 0.80370 [0.79740–0.82212] |
| E2E p99 | 0.80349 [0.80131–0.81448] | 0.80254 [0.79371–0.81016] |
| Orchestrator CPU | 0.21838 [0.20800–0.22691] | 0.09874 [0.09022–0.10109] |
| Orchestrator PSS | 0.49480 [0.49477–0.49671] | 0.04045 [0.04043–0.04051] |
| Stack CPU | 0.60573 [0.59332–0.61312] | 0.60062 [0.59159–0.60399] |
| Stack RAM | 1.55398 [1.52163–1.56203] | 1.39299 [1.38927–1.42009] |

Interpretation for Queen Rust:

- throughput +14.05%, with the paired interval entirely above 1;
- p95 -19.63%;
- orchestrator CPU -90.13% and PSS -95.95%;
- stack CPU -39.94%;
- stack RAM +39.30%.

The Rust advantage is therefore very strong in the control plane, but
PostgreSQL memory remains a real deployment cost.

The 90.13% orchestrator CPU reduction must also be read in absolute terms:
0.14447→0.01409 s over a drain of about 39.5 s, or about 130 ms saved. The
percentage is large on a small base; control-plane PSS and consumer+backend
stack CPU are more material for sizing. Horizon dispatch is about 1.8× faster,
while headline and completion favor Queen: because the producer is outside the
cgroups, dispatch and headline remain guardrails, not a causal explanation for
the measured saving.

Queen completion ratios are above 1 in all 5/5 paired runs on this host. With
only five concordant signs, the exact two-sided sign test is p=0.0625: the
bootstrap interval remains descriptive of observed ratios and is not presented
as population statistical significance.

## 7. Configuration search

### 7.1 Legacy p1/a1 versus p4/a1

| Engine | Legacy p1/a1, jobs/s | Pre-guard p4/a1, jobs/s | p4/p1 |
| --- | ---: | ---: | ---: |
| Horizon | 261.85 | 261.46 | 0.9985 |
| Queen PHP | 118.15 | 290.87 | 2.4619 |
| Queen Rust | 121.39 | 290.64 | 2.3942 |

In the legacy profile paired against Horizon:

| Candidate | Completion ratio, 95% CI | p95 ratio, 95% CI |
| --- | ---: | ---: |
| Queen PHP | 0.4512 [0.4265–0.4648] | 2.1774 [2.0908–2.2811] |
| Queen Rust | 0.4628 [0.4429–0.4643] | 2.1110 [2.0903–2.2120] |

These data explain the initial impression that Queen was slower: that
conclusion was correct for p1/a1, but not for the optimized client. In legacy
p1/a1, stack cost also worsens: Rust/Horizon is 1.5759 for CPU and 1.5117 for
RAM (PHP 1.6097 and 1.6303). P1 is therefore the safety default, not a profile
that is already competitive for throughput or consumer+backend cost.

The p1→p4 comparison uses separate campaigns with 6,000/10,000 jobs and
n=3/n=5, so it has no paired CI and does not establish causality by itself. The
Horizon control changes by only -0.15%, while both Queen implementations grow
by 139–146%: this is strong directional evidence consistent with prefetch as
the bottleneck. Both table columns use commit 5b47a7d5 and predate the guard;
they locate the historical bottleneck but are not the primary result. Campaign
3655cd2a separately confirms post-guard p4/a1 with an accepted lease at
289.17/289.13 jobs/s for PHP/Rust versus 253.38 for Horizon.

### 7.2 Prefetch 4 versus 16

The following two screenings include only Queen Rust and are separate
campaigns, each with n=3 and 4,000 jobs:

| Metric | p4/a1 | p16/a1 | p16/p4 |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 292.026 | 290.901 | -0.39% |
| Headline jobs/s | 285.008 | 283.235 | -0.62% |
| E2E p95 | 12,343 ms | 12,314 ms | -0.23% |
| E2E p99 | 12,764 ms | 12,816 ms | +0.41% |
| Orchestrator CPU | 0.00817 s | 0.00806 s | -1.32% |
| Orchestrator PSS | 2.87 MiB | 2.86 MiB | -0.10% |
| Worker CPU | 2.251 s | 2.659 s | **+18.13%** |
| Stack CPU | 6.399 s | 6.503 s | +1.63% |
| Stack RAM | 287.83 MiB | 289.82 MiB | +0.69% |

P16 does not increase throughput and costs more worker CPU. Applying the
predefined rule of selecting the smallest setting within 5% of the maximum,
p4 is the smallest performance profile among replicated candidates. It is not
the universal safety default; that remains p1. ACK batch remains 1, so an
apparent benefit was not exchanged for a wider deferred-ACK window.

### 7.3 Fixed scaling at 1 and 8 workers

The w1 and w8 campaigns share commit 2a4b107a, app image
sha256:9386d3716853a6f9d09c9e455ce9c95326a01516655747fa785566543b7ccf6c,
3,000 10 ms jobs, warm-up 256, p4/a1, and n=3. All 18 runs are valid,
quiescent, and configuration-consistent.

Absolute medians:

| Workers | Engine | Completion jobs/s | Headline jobs/s | E2E p95 | E2E p99 | Stack CPU | Stack RAM |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Horizon | 61.25 | 60.49 | 46,757 ms | 48,631 ms | 11.040 s | 111.41 MiB |
| 1 | Queen PHP | 62.58 | 62.25 | 45,309 ms | 46,960 ms | 12.887 s | 222.18 MiB |
| 1 | Queen Rust | 62.43 | 62.07 | 45,389 ms | 47,124 ms | 13.018 s | 193.27 MiB |
| 8 | Horizon | 546.08 | 508.32 | 5,153 ms | 5,357 ms | 5.125 s | 319.45 MiB |
| 8 | Queen PHP | 503.02 | 494.91 | 4,769 ms | 4,921 ms | 3.928 s | 447.76 MiB |
| 8 | Queen Rust | 513.37 | 463.85 | 4,610 ms | 4,864 ms | 4.242 s | 440.03 MiB |

Paired candidate/Horizon ratios:

| Workers | Metric | Queen PHP, median [95% CI] | Queen Rust, median [95% CI] |
| ---: | --- | ---: | ---: |
| 1 | Completion | 1.0216 [1.0167–1.0299] | 1.0192 [1.0112–1.0213] |
| 1 | E2E p95 | 0.9690 [0.9689–0.9706] | 0.9701 [0.9693–0.9830] |
| 1 | Stack CPU | 1.1636 [1.1489–1.1856] | 1.1737 [1.1692–1.2111] |
| 1 | Stack RAM | 1.9968 [1.9944–1.9968] | 1.7312 [1.7259–1.7389] |
| 8 | Completion | 0.9210 [0.9063–1.0662] | 0.9400 [0.8565–1.0648] |
| 8 | E2E p95 | 0.9055 [0.8012–0.9515] | 0.8946 [0.7985–0.9993] |
| 8 | Stack CPU | 0.7690 [0.7429–0.8218] | 0.8304 [0.7615–0.9887] |
| 8 | Stack RAM | 1.4017 [1.3858–1.4132] | 1.3775 [1.3109–1.3823] |

At one worker Queen is about 2% faster, with CIs entirely above 1, but uses
16–17% more stack CPU and 73–100% more stack RAM. At eight workers, completion
medians favor Horizon by 6–8%, but the CIs cross 1: n=3 and Queen variability
preclude a throughput conclusion. P95 and stack CPU instead favor Queen in all
three paired runs, while RAM remains 38–40% higher.

The descriptive w8/w1 ratio is 8.92× for Horizon, 8.04× for Queen PHP, and
8.22× for Queen Rust. It is a ratio between separate campaigns, without
pairing or bootstrap, and must not be interpreted as guaranteed linear
efficiency.

The primary w4 campaign is not a matched cell: it uses 10,000 jobs, warm-up 0,
n=5, commit 3655cd2a, and app image sha256:0e82c6cc…8958. It is not
artificially inserted between w1 and w8 to construct a 1→4→8 curve. A new w4
campaign with 3,000 jobs on the commit and image used by the two scaling
campaigns is needed.

### 7.4 Aggressive fixed p16/a16 confirmation

The
[confirm-fixed-aggressive](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.json)
campaign uses commit 327849a1, app image sha256:b631f6f6…cc21d, four workers,
6,000 10 ms jobs, warm-up 512, single dispatch, prefetch 16, and ACK batch 16.
All 9/9 runs are valid, quiescent, and configuration-consistent; no container
has restarted or encountered an OOM event, and median app/backend CPU
throttling is zero.

Absolute medians:

| Metric | Horizon | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 255.96 | 295.91 | 293.85 |
| Q1–Q3 completion | 255.94–259.43 | 289.64–297.79 | 285.49–298.87 |
| Headline jobs/s | 252.12 | 290.82 | 291.69 |
| E2E p95 | 21,669 ms | 18,437 ms | 18,179 ms |
| E2E p99 | 22,565 ms | 18,878 ms | 18,711 ms |
| Orchestrator CPU | 0.05843 s | 0.02704 s | 0.01093 s |
| Orchestrator PSS | 65.23 MiB | 35.15 MiB | 2.87 MiB |
| Worker CPU | 12.278 s | 2.945 s | 2.828 s |
| Backend CPU | 3.262 s | 3.795 s | 3.843 s |
| Stack CPU | 15.600 s | 6.813 s | 6.685 s |
| Stack RAM | 209.34 MiB | 319.98 MiB | 289.79 MiB |

Paired candidate/Horizon ratios, median [95% bootstrap CI]:

| Metric | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: |
| Completion | 1.1563 [1.0778–1.1708] | 1.1482 [1.0827–1.1559] |
| Headline | 1.1536 [1.0870–1.1764] | 1.1571 [1.0870–1.1640] |
| E2E p95 | 0.8508 [0.8318–0.9016] | 0.8509 [0.8389–0.9002] |
| E2E p99 | 0.8364 [0.8198–0.8921] | 0.8301 [0.8290–0.8806] |
| Orchestrator CPU | 0.4632 [0.4357–0.4693] | 0.1831 [0.1778–0.2210] |
| Orchestrator PSS | 0.5388 [0.5388–0.5403] | 0.0440 [0.0438–0.0441] |
| Stack CPU | 0.4370 [0.4227–0.4670] | 0.4416 [0.4249–0.4434] |
| Stack RAM | 1.5303 [1.5212–1.5381] | 1.3838 [1.3686–1.3843] |

In aggressive fixed, Queen is 14.8–15.6% above Horizon, reduces p95 by about
15% and stack CPU by about 56%; stack RAM remains 38–53% higher. Rust and PHP
have similar throughput, while Rust retains the control-plane advantage.

This is not a causal A/B test against safe fixed p4/a1: commit, app image,
warm-up, job count, and n all differ. P16/a16 changes prefetch and ACK batch at
the same time; the previous p16/a1 screening showed no benefit over p4/a1, but
that does not quantitatively isolate the ACK16 effect across these campaigns.

The subsequent direct p16/a16 fault test uses the same prefetch/ACK knobs but
not the full steady-state preset: two workers, 500 ms jobs, and retry_after 12
s versus four workers, 10 ms jobs, and retry_after 180 s. It passes
at-least-once 10/10 but strict only 1/10. Together with seven strict failures
under p4/a4, this makes it prudent to classify any ACK batching as opt-in;
different workloads prevent a causal comparison of duplication rates.

## 8. Autoscaling

The
[auto p4/a1 post-guard](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.json)
campaign uses clean metadata at 774c2413, app image sha256:0e82c6cc…8958,
size strategy, min 1, max 4, cooldown 3 s, max shift 1, retry_after 481 s, and
6,000 10 ms jobs. All 9/9 runs are valid, quiescent, and configuration-
consistent; 24 container snapshots have zero restarts/OOM events and zero
app/backend/stack throttling in every run. The algorithms respect the same
limits but are not semantically identical: Horizon tends to allocate the
maximum to an active queue; Queen calculates demand from backlog and target.

### 8.1 Performance

| Median metric | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 243.75 | 234.13 | 235.69 |
| Headline jobs/s | 228.61 | 233.93 | 235.46 |
| E2E p95 | 24,173 ms | 23,362 ms | 22,932 ms |
| E2E p99 | 25,082 ms | 23,795 ms | 23,703 ms |
| Orchestrator CPU | 0.13549 s | 0.04332 s | 0.02010 s |
| Orchestrator PSS | 76.93 MiB | 40.35 MiB | 3.04 MiB |
| Worker CPU | 13.231 s | 4.300 s | 4.183 s |
| Backend CPU | 3.520 s | 7.639 s | 7.738 s |
| Stack CPU | 16.915 s | 12.004 s | 12.007 s |
| Stack RAM | 208.00 MiB | 323.81 MiB | 295.33 MiB |

Paired candidate/Horizon ratios, median [95% bootstrap CI]:

| Metric | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion | 0.9500 [0.9430–0.9605] | 0.9669 [0.9360–0.9714] |
| Headline | 1.0106 [1.0035–1.0233] | 1.0300 [0.9961–1.0334] |
| E2E p95 | 0.9720 [0.9665–0.9768] | 0.9487 [0.9460–0.9849] |
| E2E p99 | 0.9571 [0.9486–0.9733] | 0.9450 [0.9413–0.9758] |
| Orchestrator CPU | 0.3197 [0.3196–0.3374] | 0.1486 [0.1419–0.1518] |
| Orchestrator PSS | 0.5245 [0.5245–0.5254] | 0.0396 [0.0396–0.0396] |
| Stack CPU | 0.7124 [0.7071–0.7375] | 0.7009 [0.6813–0.7575] |
| Stack RAM | 1.5579 [1.5419–1.5857] | 1.4209 [1.3946–1.4620] |

Queen PHP/Rust have completion-span lower by 5.0%/3.3%, but median headline
higher by 1.1%/3.0% and p95 lower by 2.8%/5.1%. Rust reduces stack CPU by
29.9% and orchestrator PSS by 96.0%, with stack RAM +42.1%. Completion is below
Horizon in all 3/3 paired runs for both lanes; the two-sided sign test with n=3
is p=0.25. Completion span starts at the first completion and responds
differently to ramp-up: it must be published together with headline and
dispatch guardrails, without significance claims.

### 8.2 Worker dynamics

Medians over three runs, sampled every 0.5 s:

| Engine | First worker detected | Time to peak | First downscale | Worker-seconds | Initial/peak/final | Return to minimum after completion |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Horizon | 1.654 s | 4.653 s | 30.635 s | 127.27 | 0/4/1 | unavailable |
| Queen PHP | 0.141 s | 8.644 s | 27.135 s | 111.53 | 1/4/1 | 7.497 s |
| Queen Rust | 0.130 s | 8.631 s | 27.152 s | 111.53 | 1/4/1 | 7.663 s |

Queen reaches the maximum about 4 s after Horizon but uses about 12.4% fewer
worker-seconds. For Horizon, the sampler sees initial_workers=0 and
final_workers=1, so it does not produce a return to the same initial minimum;
this must not be artificially converted to zero seconds. These are sampling
times, not controller-emitted events; uncertainty is at least on the order of
the 0.5 s interval.

### 8.3 Aggressive auto p16/a16 confirmation

The
[confirm-auto-aggressive](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.json)
campaign uses the same auto limits of 1…4, 3 s cooldown, and max shift 1, but
sets Queen to prefetch 16 and ACK batch 16. All 9/9 runs are valid, quiescent,
and consistent: 54,000/54,000 unique jobs completed, with zero missing,
duplicate, or failed jobs. A separate check of 24 containers records
RestartCount=0 and OOMKilled=false; median application/backend CPU throttling
is zero.

| Median metric | Horizon | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 245.22 | 240.47 | 244.96 |
| Headline jobs/s | 230.06 | 240.26 | 244.72 |
| E2E p95 | 24,031 ms | 22,961 ms | 22,352 ms |
| E2E p99 | 24,923 ms | 23,285 ms | 22,768 ms |
| Orchestrator CPU | 0.13558 s | 0.04885 s | 0.02173 s |
| Orchestrator PSS | 76.93 MiB | 40.35 MiB | 3.03 MiB |
| Worker CPU | 13.158 s | 3.349 s | 3.261 s |
| Stack CPU | 16.787 s | 7.712 s | 7.556 s |
| Stack RAM | 207.82 MiB | 315.91 MiB | 285.26 MiB |

| Paired candidate/Horizon ratio | Queen PHP p16/a16 | Queen Rust p16/a16 |
| --- | ---: | ---: |
| Completion | 0.9836 [0.9574–0.9941] | 0.9990 [0.9963–1.0067] |
| Headline | 1.0473 [1.0201–1.0587] | 1.0637 [1.0615–1.0719] |
| E2E p95 | 0.9532 [0.9322–0.9781] | 0.9301 [0.9270–0.9335] |
| E2E p99 | 0.9320 [0.9112–0.9554] | 0.9113 [0.9096–0.9201] |
| Orchestrator CPU | 0.3593 [0.3561–0.3685] | 0.1587 [0.1551–0.1603] |
| Orchestrator PSS | 0.5245 [0.5244–0.5245] | 0.0394 [0.0394–0.0395] |
| Stack CPU | 0.4578 [0.4568–0.4759] | 0.4516 [0.4501–0.4570] |
| Stack RAM | 1.5206 [1.5197–1.5206] | 1.3726 [1.3695–1.3727] |

For completion span, PHP is 1.6% below Horizon with the CI entirely below 1;
Rust is substantially at parity and its CI crosses 1. Headline and latency
instead favor both Queen lanes: +4.7%/-4.7% p95 for PHP and +6.4%/-7.0% p95
for Rust. Rust reduces orchestrator CPU by 84.1%, PSS by 96.1%, and stack CPU
by 54.8%, but stack RAM grows by 37.3%; for PHP, stack CPU falls by 54.2% and
RAM grows by 52.1%.

| Engine | First worker detected | Time to peak | First downscale | Worker-seconds | Initial/peak/final | Return to minimum after completion |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Horizon | 1.644 s | 4.645 s | 30.653 s | 127.28 | 0/4/1 | unavailable |
| Queen PHP | 0.127 s | 8.628 s | 27.135 s | 111.53 | 1/4/1 | 8.151 s |
| Queen Rust | 0.138 s | 8.641 s | 28.652 s | 112.03 | 1/4/1 | 10.125 s |

Queen uses 12.4% (PHP) and 12.0% (Rust) fewer worker-seconds. Here too, the
data come from polling every 0.5 s: Horizon's initial zero is an artifact of
the first observation, not the absence of a worker, and the null return does
not indicate a failure to scale down.

The comparison with safe auto p4/a1 is directional only: campaign, commit, and
app image differ and the runs are not interleaved. We therefore do not
attribute the difference from safe to prefetch/ACK16. Clean steady-state runs
do not by themselves establish p16/a16 fault safety; crash evaluation with the
same knobs and a different workload is covered in section 12.

## 9. CPU-bound workload

The campaign uses four workers, 6,000 jobs, sleep 0, and 25,000 CPU iterations.

| Median metric | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 359.49 | 365.02 | 364.05 |
| E2E p95 | 14,681 ms | 13,778 ms | 14,047 ms |
| Orchestrator CPU | 0.02998 s | 0.01808 s | 0.00755 s |
| Orchestrator PSS | 65.04 MiB | 35.15 MiB | 2.86 MiB |
| Stack CPU | 65.902 s | 67.237 s | 67.160 s |
| Stack RAM | 209.48 MiB | 325.03 MiB | 292.64 MiB |

| Candidate/Horizon ratio | Queen PHP | Queen Rust |
| --- | ---: | ---: |
| Completion, 95% CI | 1.01249 [0.92743–1.01539] | 1.01173 [1.00948–1.01634] |
| E2E p95, 95% CI | 0.94140 [0.92086–0.94362] | 0.96204 [0.95041–0.96648] |
| Orchestrator CPU, 95% CI | 0.59828 [0.56988–0.60304] | 0.25737 [0.24906–0.26705] |
| Stack CPU, 95% CI | 1.02027 [1.01985–1.11210] | 1.01909 [1.01313–1.02093] |
| Stack RAM, 95% CI | 1.55282 [1.53852–1.65555] | 1.39700 [1.38888–1.40284] |

The PHP throughput CI is wide and crosses 1 because one of the three paired
runs is variable. For Rust, the effect is small but consistent across samples.
In both cases stack CPU is about 2% higher: when application job work
dominates, Queen's advantage must not be described as end-to-end CPU savings.
The operational reason for Rust remains the much lighter control plane.

## 10. Confirmed 50k stress test

### 10.1 Configuration and gates

The
[stress-50k-safe-confirm](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.json)
campaign uses:

- three engines, with three repetitions each;
- fixed 4 workers, 50,000 jobs per run;
- sleep 0, CPU iterations 0, no warm-up;
- Laravel bulk dispatch, chunk 100;
- Queen p4/a1, 64 partitions, pop fusion off;
- 0.5 s sampling, 3 s post-drain, 1,200 s timeout.

All 9/9 runs have 50,000/50,000 unique jobs, max attempt 1, and zero missing,
duplicate, failed, or unexpected jobs. Every final queue has
size=ready=reserved=delayed=0; sampler integrity_errors=0, OOM events=0,
orchestrator/worker PSS coverage=1.0; container restarts=0 and OOMKilled=false.

### 10.2 Absolute medians

| Metric | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Completion jobs/s | 841.97 | 2,042.17 | 2,007.13 |
| Headline jobs/s | 820.33 | 2,039.59 | 2,004.57 |
| Dispatch jobs/s | 6,998.07 | 4,508.05 | 4,602.23 |
| E2E p95 | 49,799 ms | 11,571 ms | 11,990 ms |
| E2E p99 | 51,632 ms | 12,690 ms | 13,085 ms |
| Orchestrator CPU | 0.27398 s | 0.01704 s | 0.00813 s |
| Orchestrator PSS | 71.05 MiB | 35.15 MiB | 2.86 MiB |
| Orchestrator RSS | 96.01 MiB | 49.98 MiB | 4.06 MiB |
| Worker CPU | 26.174 s | 11.286 s | 11.266 s |
| Application CPU | 26.486 s | 11.303 s | 11.278 s |
| Application RAM, max sampled | 205.41 MiB | 186.58 MiB | 155.59 MiB |
| Backend CPU | 8.491 s | 26.619 s | 27.077 s |
| Backend RAM, max sampled | 130.03 MiB | 281.52 MiB | 276.77 MiB |
| Stack CPU | 34.977 s | 37.923 s | 38.355 s |
| Stack RAM | 320.55 MiB | 467.98 MiB | 432.24 MiB |

The Horizon producer publishes faster, but the drain is slower. Because the
producer is outside the measured cgroups, dispatch throughput must be read as
a guardrail and not as a lane resource saving.

### 10.3 Paired ratios

Median [95% bootstrap CI], three eligible pairs out of three:

| Metric | Queen PHP / Horizon | Queen Rust / Horizon |
| --- | ---: | ---: |
| Completion-span | 2.42604 [2.17922–2.50006] | 2.38075 [2.18877–2.49219] |
| Headline | 2.48836 [2.23958–2.56267] | 2.44057 [2.24956–2.55609] |
| Dispatch | 0.64464 [0.59967–0.69889] | 0.64479 [0.61564–0.71625] |
| E2E p95 | 0.23236 [0.21667–0.25704] | 0.23837 [0.22473–0.26468] |
| E2E p99 | 0.24578 [0.23365–0.27522] | 0.25082 [0.23749–0.28160] |
| Orchestrator CPU | 0.05993 [0.05732–0.06431] | 0.02770 [0.02762–0.02969] |
| Orchestrator PSS | 0.49475 [0.49475–0.49480] | 0.04030 [0.04029–0.04034] |
| Orchestrator RSS | 0.52051 [0.51936–0.52057] | 0.04231 [0.04221–0.04235] |
| Worker CPU | 0.43118 [0.40898–0.43209] | 0.43110 [0.40342–0.43163] |
| Application CPU | 0.42677 [0.40552–0.42813] | 0.42646 [0.39979–0.42742] |
| Application RAM | 0.90834 [0.90646–0.90963] | 0.75738 [0.75694–0.75845] |
| Backend CPU | 3.13514 [3.13326–3.16522] | 3.18054 [3.05576–3.26476] |
| Backend RAM | 2.16795 [2.10209–2.18914] | 2.12846 [2.11569–2.15929] |
| Stack CPU | 1.07508 [1.05415–1.08424] | 1.06473 [1.05337–1.10599] |
| Stack RAM | 1.45921 [1.43830–1.46917] | 1.34860 [1.33866–1.36406] |

### 10.4 Throttling and cgroup peaks

Cgroup guardrails show that the stress test saturates Queen's backend budget:

| Guardrail, median n=3 | Horizon | Queen PHP | Queen Rust |
| --- | ---: | ---: | ---: |
| Backend cpu_nr_throttled | 0 | 224 | 231 |
| Backend cpu_throttled_seconds, aggregate | 0.000 s | 26.999 s | 25.860 s |
| App cpu_nr_throttled | 0 | 0 | 0 |
| App cpu_throttled_seconds | 0.000 s | 0.000 s | 0.000 s |
| App memory_peak_reported | 205.55 MiB | 186.60 MiB | 155.64 MiB |
| Backend memory_peak_reported | 130.48 MiB | 308.17 MiB | 303.40 MiB |

The Queen backend comprises two cgroups, broker and PostgreSQL, each limited to
1 CPU; reported throttled time is aggregated. This makes the comparison
consistent with the shared 2 CPU budget but does not represent unconstrained
capacity. Median throttling is zero in fixed sleep-bound, legacy, and auto
profiles. In the CPU-bound profile, only minor application throttling is
observed: median 5 events/0.031 s for Horizon and 1 event/0.006 s or 0.004 s
for Queen PHP/Rust.

The stress test shows two facts at once:

- Queen completes the same 50,000 jobs with about 2.4× the throughput and
  reduces p95 by about 76%;
- the Queen backend uses about 3.1–3.2 times the Redis CPU, taking stack CPU
  6–8% above Horizon and RAM 35–46% above.

The workload contains no sleep or CPU work. It therefore stresses the queue/
client/result-sink path and does not represent the productivity of a business
job. For that reason, it highlights both Queen's lower worker overhead and the
cost of the PostgreSQL backend.

### 10.5 P4/a4 discovery: throughput and semantics

The
[discovery-ack4-stress](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.json)
retains the same 50,000-job Queen zero-work workload, p4, bulk 100, and commit
2a4b107a, but increases ACK batch from 1 to 4. It does not include Horizon.
All 6/6 runs are correct, quiescent, and configuration-consistent.

| Median metric n=3 | Queen PHP p4/a4 | Queen Rust p4/a4 |
| --- | ---: | ---: |
| Completion jobs/s | 7,306.88 | 6,989.41 |
| Q1–Q3 completion | 6,257.14–7,341.95 | 6,975.94–7,162.24 |
| Headline jobs/s | 7,272.61 | 6,958.31 |
| E2E p95 | 34.64 ms | 30.98 ms |
| E2E p99 | 94.72 ms | 98.81 ms |
| Orchestrator CPU | 0.00813 s | 0.00382 s |
| Orchestrator PSS | 35.15 MiB | 2.86 MiB |
| Stack CPU | 15.510 s | 15.492 s |
| Stack RAM | 424.72 MiB | 393.13 MiB |

PHP p95 is highly dispersed: 32.28 ms, 34.64 ms, and 3,277.43 ms across the
three runs. With n=3, the median alone does not summarize this instability.

Against the p4/a1 medians from the separate stress campaign, completion is
3.58× for PHP and 3.48× for Rust. This is a discovery/unpaired comparison: it
has no bootstrap CI and does not mix the paired ratios produced within each
campaign. Completion is also recorded before the ACK; deferred batching makes
verification of the final queue and crash behavior especially important.

The subsequent p4/a4 fault tests show the tradeoff: at-least-once passes in
10/10 lanes, but seven lanes observe a duplicate and strict passes only 3/10.
For this reason, a4 does not replace the conservative a1 ACK baseline. The
subsequent fixed and auto p16/a16 confirmations pass the steady-state gate on
10 ms jobs; the p16/a16 fault test matched on knobs passes at-least-once 10/10
but strict only 1/10 with 500 ms jobs. Zero-work discovery is therefore not a
new default.

## 11. Initial observation/completion gate failure

### 11.1 Preserved evidence

The first stress run,
[stress-50k-safe](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/summary.json),
used commit 5b47a7d5 and app image sha256:39f291…b918. It stopped during the
first Horizon lane:

| Signal | Value |
| --- | ---: |
| Expected jobs | 50,000 |
| Observed unique completions | 28,617 |
| Missing in partial summary | 21,383 |
| Duplicates / failures | 0 / 0 |
| Queue size | 21,921 |
| Ready / reserved / delayed | 21,918 / 3 / 0 |
| Result-check | 0-byte file |

Sources:
[partial summary](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/summary.json),
[queue state](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/queue-state.final.json),
[result-check](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/result-check.json).

The queue was not empty: the 21,383 jobs not yet observed cannot be classified
as lost. There are no recorded container OOM events or sampler errors. The old
observer's stderr and exit reason were not retained, so the artifact proves an
observation/completion gate failure, not its precise cause.

### 11.2 Mitigation and confirmation

Commit 2a4b107a, “test: scale benchmark result collection”, replaces full
result rereading/materialization with incremental JSONL snapshots and compact
aggregates. The [streaming test](../../app/tests/streaming_results.php)
verifies 50,000 records with a 64 MiB memory_limit, as well as incremental
snapshots, percentiles, counts, and rejection of malformed JSON.

The verification sequence is:

1. preserved failure of the old observation/completion gate;
2. streaming change and regression test;
3. [single stress 50k v2 run](stress-50k-safe-v2/20260829T022644Z-2a4b107a06/campaign-stats.json)
   completed;
4. 50k stress test with n=3, 9/9 valid and quiescent runs.

The relationship is chronological and the mitigation eliminates a known mode
of memory growth. Without the original stderr, it is not declared a certain
forensic root cause and, most importantly, is not attributed to the queue.

## 12. Fault injection and recovery

### 12.1 Post-guard p4/a1 design

Every round uses clean commit 3655cd2a, app image sha256:0e82c6cc…8958, a
fresh backend, and for each engine:

- fixed 2 workers, 24 jobs of 2,000 ms each;
- single dispatch, tries 2, 10 s worker timeout, retry_after 41 s;
- Queen prefetch 4 and ACK batch 1;
- selection of a single previously qualified non-master child worker;
- SIGKILL after 100 ms while backlog is still present;
- wait for respawn, retry, the exact completion set, and a queue stably at zero;
- container inspection for restarts and OOM events.

The nominal workload floor is 4×2=8 s and the product guard requires
retry_after >4×timeout10=40 s: 41 s is therefore accepted by the measured
guard. It is not the full primary fixed preset, which uses four workers, 10 ms
jobs, a 120 s timeout, and retry_after 481 s. The fault test observes
redelivery at about 40–45 s; the primary configuration may instead wait almost
eight minutes. P4/a1 and the lease rule are matched, not the exact recovery
time of the performance profile.

All targets have target_proved_work_before_kill=true and
target_is_container_init=false. The kill timestamp and completions use the
Docker VM's monotonic clock; time to first retry completion is correctly
expressed as a lower/upper interval, not as a point.

### 12.2 Post-guard p4/a1: all rounds

| Rep | Engine | Full pool after kill | First retry-completion bound | Completions at attempt >1 |
| --- | --- | ---: | ---: | ---: |
| r01 | Horizon | 85.4 ms | 42,704.9–42,770.5 ms | 1 |
| r01 | Queen PHP | 3,261.6 ms | 40,465.7–40,524.5 ms | 3 |
| r01 | Queen Rust | 2,887.2 ms | 43,125.7–43,192.6 ms | 4 |
| r02 | Horizon | 84.8 ms | 41,958.5–42,021.0 ms | 1 |
| r02 | Queen PHP | 1,543.8 ms | 44,571.2–44,638.1 ms | 2 |
| r02 | Queen Rust | 1,540.3 ms | 40,494.6–40,553.9 ms | 3 |
| r03 | Horizon | 88.6 ms | 41,930.7–41,992.6 ms | 1 |
| r03 | Queen PHP | 3,108.4 ms | 43,278.8–43,345.0 ms | 4 |
| r03 | Queen Rust | 3,003.7 ms | 40,430.4–40,492.2 ms | 3 |
| r04 | Horizon | 923.3 ms | 43,210.9–43,273.6 ms | 1 |
| r04 | Queen PHP | 1,614.3 ms | 40,609.2–40,673.7 ms | 3 |
| r04 | Queen Rust | 2,964.8 ms | 41,121.6–41,195.9 ms | 2 |
| r05 | Horizon | 86.7 ms | 42,275.6–42,339.2 ms | 1 |
| r05 | Queen PHP | 1,495.5 ms | 40,506.0–40,569.8 ms | 3 |
| r05 | Queen Rust | 2,932.1 ms | 40,464.0–40,525.3 ms | 4 |

Descriptive summary across five rounds:

| Engine | Median full pool (range) | Median retry bound | Total retry completions |
| --- | ---: | ---: | ---: |
| Horizon | 86.7 ms (84.8–923.3) | 42,275.6–42,339.2 ms | 5 |
| Queen PHP | 1,614.3 ms (1,495.5–3,261.6) | 40,609.2–40,673.7 ms | 15 |
| Queen Rust | 2,932.1 ms (1,540.3–3,003.7) | 40,494.6–40,553.9 ms | 16 |

Queen respawn times remain within 3.3 s and the expected timeout was 30 s;
Horizon restores the pool faster in these runs. The number of completions at
an attempt greater than one is not a final duplicate: the unique set remains
exact. It is compatible with a wider in-flight window at p4, but the test does
not causally isolate that factor.

### 12.3 Post-guard p4/a1 reliability gates

Across all 15 lanes:

| Gate | Result |
| --- | ---: |
| at_least_once_pass | 15/15 |
| strict_observation_pass | 15/15 |
| Worker respawned | 15/15 |
| In-flight retry observed | 15/15 |
| Unique jobs | 24/24 per lane; 360/360 total |
| Record / max attempt | 360 / 2 |
| Missing / duplicates / unexpected | 0 / 0 / 0 |
| Failure signals in logs | 0 |
| Final queue | size/ready/reserved/delayed = 0 |
| Probe errors / queue timeouts | 0 / 0 |
| Container OOM events / restarts | 0 / 0 |

Primary artifacts:
[r01](fault-p4-a1-guarded-r01/report.json),
[r02](fault-p4-a1-guarded-r02/report.json),
[r03](fault-p4-a1-guarded-r03/report.json),
[r04](fault-p4-a1-guarded-r04/report.json),
[r05](fault-p4-a1-guarded-r05/report.json).
Each directory also retains the timeline, target identity, process tree,
events, queue state, container state, and logs. The five historical
`fault-p4-r01…r05` rounds remain preserved as pre-guard corroboration, but are
excluded one-for-one from the 35 selected fault lanes.

### 12.4 P4/a4: at-least-once passes, strict does not

Five subsequent rounds run the same fault for Queen PHP and Queen Rust with
prefetch 4 and ACK batch 4. They use commit
327849a1edbae2b1c5f17c402062dbf5dc5c4237, a clean worktree, and app image
sha256:b631f6f6ff90202611ac337fecf10d88e557509ea545f9965df023aa0c0cc21d.

| Rep | Engine | At-least-once | Strict | Duplicates | Unique jobs | Missing/unexpected | OOM/restarts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| r01 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r01 | Queen Rust | pass | pass | 0 | 24/24 | 0/0 | 0/0 |
| r02 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r02 | Queen Rust | pass | pass | 0 | 24/24 | 0/0 | 0/0 |
| r03 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r03 | Queen Rust | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r04 | Queen PHP | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r04 | Queen Rust | pass | fail | 1 | 24/24 | 0/0 | 0/0 |
| r05 | Queen PHP | pass | pass | 0 | 24/24 | 0/0 | 0/0 |
| r05 | Queen Rust | pass | fail | 1 | 24/24 | 0/0 | 0/0 |

All 10/10 lanes observe respawn, in-flight retry, max attempt 2, the exact set
of 24 jobs, failure signal 0, and a final queue quiescent at zero.
At-least-once passes 10/10; strict passes 1/5 for PHP and 2/5 for Rust, or
3/10 overall. The seven duplicates are distributed across seven distinct runs.

Artifacts:
[a4 r01](fault-p4-a4-r01/report.json),
[a4 r02](fault-p4-a4-r02/report.json),
[a4 r03](fault-p4-a4-r03/report.json),
[a4 r04](fault-p4-a4-r04/report.json),
[a4 r05](fault-p4-a4-r05/report.json).

The result makes the tradeoff measurable: ACK4 greatly increases zero-work
throughput, but widens the window in which a crash redelivers work already
completed in the application but not yet acknowledged to the broker. It is
usable only as an opt-in for idempotent jobs and external effects, with
duplicates treated as expected at-least-once behavior.

### 12.5 P16/a16 matched on knobs: at-least-once 10/10, strict 1/10

The campaign matched on knobs,
[p16/a16 r01](fault-p16-a16-valid-r01/report.json)…[r05](fault-p16-a16-valid-r05/report.json)
uses prefetch 16/ACK batch 16, two workers, and 24 500 ms jobs; steady-state
confirmations instead use four workers, 10 ms jobs, and retry_after 180 s. The
nominal batch floor is about 16×0.5=8 s, below retry_after 12 s. The current
guard instead uses timeout 10 s and would require retry_after >160 s: these
pre-guard artifacts, despite the `valid` suffix, would be rejected today.

| Engine | At-least-once lanes | Strict lanes | Runs with duplicates | Records / unique | Duplicates | Median respawn (range) | Median lower retry bound |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Queen PHP | 5/5 | 0/5 | 5/5 | 129 / 120 | 9 | 2,910 ms (1,588–2,948) | 11,496 ms |
| Queen Rust | 5/5 | 1/5 | 4/5 | 128 / 120 | 8 | 3,035 ms (1,626–3,113) | 11,565 ms |
| Total | 10/10 | 1/10 | 9/10 | 257 / 240 | 17 | — | — |

All lanes observe respawn, in-flight retry, max attempt 2, 24/24 unique jobs,
zero missing/unexpected/failure signals, a quiescent final queue, and zero OOM
events/restarts. The knobs therefore preserve at-least-once behavior in the
observed nominal workload, but strict observation fails in 9/10 lanes because
of duplicates. This is a direct p16/a16 fault test; however, it does not allow
a quantitative comparison of the duplication rate with p4/a4, which uses 2 s
jobs rather than 500 ms jobs.

### 12.6 Negative pilot: insufficient p16/a16 lease budget

The initial
[fault-p16-a16-r01](fault-p16-a16-r01/report.json) pilot reused 2 s jobs and
retry_after 12 s: 16×2=32 s exceeds the lease. It was stopped after both lanes
of the first round and is excluded from the primary fault totals.

| Engine | Unique / records | Duplicates | Failure signals | Final queue size/reserved | At-least-once / strict |
| --- | ---: | ---: | ---: | ---: | ---: |
| Queen PHP | 24 / 39 | 15 | 763 | 16 / 16 | fail / fail |
| Queen Rust | 24 / 29 | 5 | 993 | 24 / 24 | fail / fail |

Both lanes complete all 24 unique IDs and have no OOM events/restarts, but
after 120 s the queue is not quiescent; the fail-closed at-least-once gate
therefore remains false. The pilot is reported in full as a negative
configuration test and demonstrates the need to validate `retry_after`
against the entire prefetched batch; it does not measure the reliability of
the separately reported, also pre-guard `valid-r` campaign.

### 12.7 Post-benchmark mitigation c4059027

Post-benchmark commit `c4059027` introduces two fail-fast guards:

- the [shared supervisor configuration](../../../../clients/client-laravel/src/Laravel/Supervisor/SupervisorConfiguration.php)
  rejects `retry_after <= prefetch × timeout`;
- the [fault harness](../../scripts/fault-recovery.sh) rejects at least
  `prefetch × sleep_ms >= retry_after`, unless the explicit
  `--allow-lease-risk` override is used for negative protocols, and records the
  override in metadata. The override does not bypass the product guard in the
  post-guard image: an end-to-end negative test requires a pre-guard image;
  otherwise, it remains a dry-run. Documentation commit 774c2413 clarifies this
  limitation but is not included in the measured image.

The second check is only a lower bound: CPU work and framework overhead require
additional margin. Two regression tests cover the guard; the post-fix suite
passes **342 tests / 1,349 assertions**. The suite was run locally with PHP
8.5.9, while the benchmark fixture uses PHP 8.3.33: this is a guard regression,
not a cross-version runtime test. This mitigation postdates the pre-guard
artifacts and does not improve their results retroactively; the new primary
fixed campaign instead uses post-guard image 3655cd2a, which includes the
guard, as does the new p4/a1 fault test.

The [post-guard smoke test](post-lease-guard-smoke/20260829T034840Z-3655cd2a63/campaign-stats.json)
uses p4/a1, a 120 s timeout, and retry_after 481 s: all 3/3 lanes (Horizon,
PHP, Rust) are valid and quiescent. With 60 jobs, warm-up 10, and one
repetition per engine, it is a compatibility check rather than performance
evidence; completions of 251.73/246.02/248.11 jobs/s are not compared
inferentially.

The subsequent
[fixed post-guard 3655](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json)
uses the same accepted lease with 10,000 jobs and n=5: 15/15 runs are valid and
quiescent. It is the primary performance confirmation of guard+profile
compatibility. The five [post-guard p4/a1 fault rounds](fault-p4-a1-guarded-r01/report.json)
also achieve strict recovery in 15/15 lanes with retry_after 41 s;
timeout/retry and worker count differ from the primary fixed campaign, so they
do not measure its almost eight-minute recovery under the 481 s lease.

### 12.8 Fault-test limitations

- The durable failed-job store is disabled: durable_failure_count is null and
  failure signals are derived from logs. The report must not say "zero durable
  failed jobs."
- The target must have a previous completion, backlog, and long jobs; there is
  no start event for the current job. This is strong qualification, not
  nanosecond-level proof that SIGKILL occurs inside user code.
- Completion is written at the end of handle, before the ACK.
- The fixture has no external ledger for verifying the idempotency of business
  effects.
- One crash per engine/profile on a fresh backend, repeated five times, does
  not estimate a failure probability.
- No broker, PostgreSQL, or Redis crashes were injected, nor were network
  faults, disk-full conditions, or failover.

The correct result to communicate is: post-guard p4/a1 passes at-least-once and
strict recovery in 15/15 smoke lanes; p4/a4 passes at-least-once in 10/10 but
strict only in 3/10; p16/a16 with a nominal runtime floor below the lease
passes at-least-once in 10/10 but strict in 1/10. Primary total: 35/35
at-least-once lanes, 19/35 strict, 840 unique jobs, and 24 observed duplicates.
The two pilot lanes with an insufficient lease are separate. No profile proves
exactly-once.

## 13. Queen broker-native 1M msg/s soak

### 13.1 Setup

The separate test runs for 86,400 seconds with:

- an open-loop target of 1,000,000 msg/s and a 60-second ramp;
- push batches of 100 and 256-byte payloads;
- leases, asynchronous explicit ACKs with 256 in flight, 60-second
  deduplication, and retention;
- one queue, 200 partitions, one consumer group, and 600 consumers;
- pop batches of 1,000;
- PostgreSQL 18.4 with `synchronous_commit` on;
- three Go loaders over HTTP, not Laravel.

### 13.2 Result

| Metric | Value |
| --- | ---: |
| Offered messages | 86,370,017,100 |
| Accepted pushes | 86,369,975,300 |
| Offered−accepted gap | 41,800 |
| Mean accepted throughput, including ramp | 999,652 msg/s |
| Popped messages | 86,369,532,700 |
| ACKed messages | 86,369,517,700 |
| Shed | 0 |
| Failed push requests | 4 |
| Failed pop requests | 0 |
| Failed ACKs, messages | 600 |
| Watchdog restarts / declared incident counter | 0 / 0 |

The pushed−popped gap is 442,600 and the popped−ACKed gap is 15,000 at the
cutoff. They are compatible with in-flight state, but without a final drain
and an ID ledger they do not prove end-to-end conservation and do not support
a zero-loss claim. Of the 41,800 messages offered but not accepted, only 400
are explicitly explained by the four failed push-batch requests; the artifact
does not reconcile the remainder. The 600 failed ACKs are another operational
counter. These signals must not be hidden behind “zero incidents” or added
together without distinguishing their respective semantics.

Stability and latency:

| Metric | Value |
| --- | ---: |
| Consecutive hours within ±0.02% | 23 |
| One-minute windows outside ±5% | 1/1,401, the ramp minute |
| Median p50 across 30-second reports | 92.7 ms |
| Median p99 across 30-second reports | 288.8 ms |
| Maximum p99 | 593.9 ms |
| 30-second intervals over 1 second | 0 |

Broker+PostgreSQL node resources:

| Resource | Value |
| --- | ---: |
| Broker RSS | 4.10 GB mean, 4.37 GB peak |
| Broker CPU | 10.92 cores mean, 12.90 peak |
| PostgreSQL CPU | 11.54 cores mean, 13.88 peak |
| Active backends | 43 |
| PostgreSQL commits | 8,056/s |
| WAL | 83 MB/s, 6.8 TB total |
| Final database | 31.2 GB |

The database reaches a plateau of about 29.6–31.4 GB due to retention. The
6.8 TB of WAL is an important operational cost to size even though the
resident database remains flat.

Sources:
[results](../../../2026-08-11-soak24-1M/results.md),
[broker metrics](../../../2026-08-11-soak24-1M/raw/bench/metrics.csv),
[host sampler](../../../2026-08-11-soak24-1M/raw/bench/bench.csv),
[loader 01](../../../2026-08-11-soak24-1M/raw/loader-01/g.out),
[loader 02](../../../2026-08-11-soak24-1M/raw/loader-02/g.out),
[loader 03](../../../2026-08-11-soak24-1M/raw/loader-03/g.out).

### 13.3 Mandatory separation from the Laravel benchmark

The soak measures batched native broker calls on dedicated hardware with 600
consumers. The Laravel benchmark measures serialized jobs, `queue:work`, the
PHP client, orchestration, and a completion sink on Docker Desktop. The
following differ:

- software path and protocol;
- batching and concurrency;
- host and budget;
- commit/version;
- unit of work: broker message versus Laravel job.

The soak shows that, in that specific setup, the broker has headroom well
beyond the hundreds of jobs/s observed in Laravel. It does not prove a general
absence of limits or that the Laravel integration can process 1M jobs/s, and
it does not support a numeric ratio of 1,000,000/290. It is also historical,
on a different commit and infrastructure: 999,652 is broker-native accepted
throughput, not a capacity claim for the Laravel client or the current SHA.

## 14. Overall assessment

| Criterion | Horizon | Queen PHP | Queen Rust |
| --- | --- | --- | --- |
| Fixed post-guard 3655 throughput | baseline | +14.28% | +14.05% |
| Fixed post-guard 3655 p95 | baseline | -19.04% | -19.63% |
| Orchestrator cost | highest | about half the PSS | substantially lower |
| Measured fixed post-guard stack CPU | baseline | -39.43% | -39.94% |
| Measured fixed post-guard stack RAM | baseline | +55.40% | +39.30% |
| Fixed w1 completion | baseline | +2.2% | +1.9% |
| Median fixed w8 completion | baseline | -7.9%, CI crosses 1 | -6.0%, CI crosses 1 |
| Auto post-guard completion span | best | -5.0% | -3.3% |
| Auto post-guard worker-seconds | baseline | -12.4% | -12.4% |
| Legacy p1/a1 completion | baseline | -54.9% | -53.7% |
| Fixed p16/a16 completion | baseline | +15.6% | +14.8% |
| Auto p16/a16 completion | baseline | -1.6% | -0.1%, CI crosses 1 |
| Strict post-guard p4/a1 fault gate | 5/5 | 5/5 | 5/5 |
| Strict p4/a4 fault gate | not applicable | 1/5 | 2/5 |
| Strict p16/a16 fault gate | not applicable | 0/5 | 1/5 |
| ACK4 | not applicable | idempotent opt-in | idempotent opt-in |
| ACK16 | not applicable | idempotent opt-in | idempotent opt-in |
| Portability | Horizon ecosystem | PHP only | requires a Rust binary |
| Orchestrator role | control baseline | fallback | **proposed default** |
| Shipping queue knobs | not applicable | **p1/a1** | **p1/a1** |
| Performance profile | not applicable | p4/a1 conditional on safe leases | p4/a1 conditional on safe leases |

Rust is the natural choice when the original objective is to eliminate the
cost of the Horizon orchestrator: its PSS advantage over Queen PHP is an order
of magnitude, without sacrificing throughput in the measured p4 profile. The
default p1 profile, however, remains about 54% behind Horizon. The Queen
topology also requires more RAM within the measured scope than Horizon+Redis;
production sizing must use this consumer+backend figure, not only the Rust
process, and then add the producer/observer and the other excluded costs.

## 15. Threats to validity and limitations

1. **Diagnostic host.** Docker Desktop on macOS does not replace native Linux,
   especially for cgroups, I/O, and scheduling.
2. **Small sample.** n=3 or n=5; bootstrap CIs are not population CIs and are
   not adjusted for multiple comparisons.
3. **Different topologies.** Volatile Redis versus broker+PostgreSQL with
   `synchronous_commit` on, while local PostgreSQL uses tmpfs. Equal budgets do
   not imply equal durability or storage I/O.
4. **Temporal order.** Engine order is rotated and every backend is fresh, but
   execution remains sequential on the same host and can experience drift.
5. **Producer excluded.** Dispatch is not included in the lane cgroups. The
   producer, application, and backend still share the VM: their nominal quotas
   total 10 CPUs in addition to the sampler, and Queen dispatch takes about
   1.8× as long as Horizon in fixed post-guard campaign 3655cd2a. Contention
   and measurement disturbance remain possible.
6. **Shared sink.** It is necessary for comparability, but can become a
   material share of cost in the zero-work workload.
7. **Partial PSS.** PSS describes the control plane; `memory.current` describes
   the stack. Neither can substitute for the other.
8. **Single queue.** The autoscaling test does not cover multi-queue
   distribution, priority, starvation, or time-warmed strategies.
9. **Limited faults.** No backend crash, failover, network fault, corruption,
   disk-full condition, or side-effect ledger.
10. **Failed store disabled.** Failures in the fault tests are derived from
    logs.
11. **Separate soak.** Version, protocol, host, and unit of work differ; no
    numeric comparison with Laravel and no extended Laravel soak has been
    completed.
12. **Scaling not fully matched.** W1 and w8 are included and consistent with
    each other; the primary w4 uses a different commit, image, job count, and n.
13. **Aggressive profiles not isolated.** The p4/a4 discovery is zero-work;
    the p16/a16 confirmations vary prefetch and ACK together. The matched
    p16/a16 knob fault test uses 500 ms, whereas p4/a4 uses 2 seconds: both
    observe duplicates but do not support a causal comparison of their rates.
14. **Partitions/fusion not selected.** The baseline has always been 64
    partitions with fusion off, not the winner of a comparative follow-up.
15. **Feature parity out of scope.** Tests cover performance and recovery of
    the measured path, not the dashboard, metrics, failed-job persistence,
    pause/resume, or other Horizon operational controls. The sink validates
    the set of IDs, not ordering or priority; single-queue Redis and Queen with
    64 partitions can diverge with more workers.
16. **Freeze and warm-up.** The protocol is a local freeze, not a signed
    preregistration; p4/p16 selection and historical fixed-safe campaigns do
    not record the prescribed 512-job warm-up, while fixed post-guard records
    a warm-up of 0. Internal pairing remains valid, but adherence is not
    complete.
17. **Prefetch lease.** Queen reserves the batch before local execution; ACK1
    does not prevent a buffered job from expiring before it starts. Every
    prefetch greater than 1 requires a bounded runtime, margin, and an explicit
    guard or renewal.
18. **Foreign containers.** The performance runner retains final snapshots but
    not an inventory or fail-fast check of pre-existing containers for every
    lane; the fault harness does. The absence recorded at the freeze does not
    prove absence during every performance run.

## 16. Production-readiness recommendation

### 16.1 Technical default

- Queen prefetch **1** as the conservative shipping default;
- ACK batch **1**;
- 64 partitions with pop fusion off as the tested, unoptimized baseline;
- **Rust** supervisor;
- PHP supervisor available as a fallback and bootstrap tool.

P1/a1 gives up the throughput observed with p4, but by default avoids a local
queue of already-leased jobs. P4/a1 is the verified performance profile only
for short, bounded jobs when
`retry_after > prefetch × worst-case runtime + margin`, or with lease renewal.
ACK1 avoids the additional deferred-ACK window, not the risk that prefetched
jobs expire before starting. Any `ack_batch` greater than 1, including p4/a4
and p16/a16, remains an opt-in for idempotent jobs with duplicate observability.

The Rust recommendation therefore concerns the control plane; it does not
mean that Queen p1/a1 is performance-equivalent to Horizon: in the legacy
campaign it is about 54–55% slower. Fixed post-guard campaign 3655cd2a shows
that p4 with a lease accepted by the guard can outperform Horizon for short
10 ms jobs. Generalizing performance and safety together requires a bounded
runtime with a verified margin, or lease renewal.

### 16.2 Rollout

1. Begin with fixed workers or conservative autoscaling limits in a canary.
2. Observe queue age, ready/reserved/delayed counts, worker count, retries,
   lease expiry, backend CPU, WAL, and consumer+backend memory.
3. Apply and monitor
   `retry_after > prefetch × worst-case runtime + margin`, including overhead,
   queuing, and jitter; use p1 or lease renewal if runtime is not bounded.
4. Make jobs idempotent and record external effects because semantics remain
   at-least-once.
5. If enabling `ack_batch > 1`, label it an aggressive profile and include
   duplicate counts and matched fault regressions in the gates.
6. Retain alerts for respawns, OOMs, restarts, non-quiescent queues, and growth
   in delayed/reserved counts.
7. Compare full infrastructure costs: Rust control-plane savings do not remove
   PostgreSQL RAM and WAL costs.

### 16.3 Gates before GA

- a fixed w4 cell matched with w1/w8 to complete the curve;
- repetition on native Linux arm64 and amd64;
- a multi-hour Laravel soak with sleep-bound, CPU-bound, and mixed jobs;
- repeated and concurrent worker faults;
- broker, PostgreSQL, and Redis restarts/crashes;
- network latency, partitions, failover, and disk pressure;
- a durable failed-job store and idempotent side-effect ledger;
- a multi-queue workload with ordering, priority, and autoscaling;
- if p16/a16 remains available, repeated matched fault tests and an external
  effect ledger in addition to the completed fixed/auto confirmations;
- declared screening of partitions and pop fusion;
- client, broker, and supervisor upgrade/rollback;
- formal throughput, p95/p99, recovery, and backlog budgets and SLOs;
- snapshots and fail-fast checks for foreign containers in every performance
  lane.

## 17. Scaling status

W1 and w8 are complete: 18/18 runs are valid, quiescent, and comparable within
each worker count. Section 7.3 reports Queen/Horizon ratios and paired CIs.

The end-to-end curve is not yet homogeneous because w4 is missing on the same
commit `2a4b107a`, application image `sha256:9386…ccf6c`, 3,000 jobs, warm-up
256, and n=3. Primary post-guard w4 campaign `3655cd2a` remains the strongest
cross-engine evidence, but it uses a different commit, image, 10,000 jobs,
warm-up 0, and n=5; it is not used with w1/w8 to calculate a causal speedup.

## 18. Artifact index

### Aggregate campaigns

- [Fixed post-guard 3655 JSON](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.json)
  and [Markdown](confirm-fixed-safe-head/20260829T035034Z-3655cd2a63/campaign-stats.md)
- [Historical pre-guard fixed-safe JSON](confirm-fixed-safe/20260829T012952Z-5b47a7d579/campaign-stats.json)
  and [Markdown](confirm-fixed-safe/20260829T012952Z-5b47a7d579/campaign-stats.md),
  redundant corroboration excluded from the 99
- [Legacy JSON](confirm-fixed-legacy/20260829T014350Z-5b47a7d579/campaign-stats.json)
  and [Markdown](confirm-fixed-legacy/20260829T014350Z-5b47a7d579/campaign-stats.md)
- [Post-guard auto p4/a1 JSON](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.json)
  and [Markdown](confirm-auto-safe-head/20260829T042032Z-774c24136a/campaign-stats.md)
- [Historical pre-guard auto JSON](confirm-auto-safe/20260829T015317Z-5b47a7d579/campaign-stats.json),
  redundant corroboration excluded from the 99
- [CPU JSON](confirm-cpu-safe/20260829T020132Z-5b47a7d579/campaign-stats.json)
  and [Markdown](confirm-cpu-safe/20260829T020132Z-5b47a7d579/campaign-stats.md)
- [Aggressive fixed JSON](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.json)
  and [Markdown](confirm-fixed-aggressive/20260829T031618Z-327849a1ed/campaign-stats.md)
- [Aggressive auto JSON](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.json)
  and [Markdown](confirm-auto-aggressive/20260829T032231Z-327849a1ed/campaign-stats.md)
- [Select p4 JSON](select-p4/20260829T012603Z-5b47a7d579/campaign-stats.json)
  and [p16 JSON](select-p16/20260829T012742Z-5b47a7d579/campaign-stats.json)
- [Scaling w1 JSON](scale-w1-safe/20260829T024935Z-2a4b107a06/campaign-stats.json)
  and [Markdown](scale-w1-safe/20260829T024935Z-2a4b107a06/campaign-stats.md)
- [Scaling w8 JSON](scale-w8-safe/20260829T030004Z-2a4b107a06/campaign-stats.json)
  and [Markdown](scale-w8-safe/20260829T030004Z-2a4b107a06/campaign-stats.md)
- [Stress 50k JSON](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.json)
  and [Markdown](stress-50k-safe-confirm/20260829T024030Z-2a4b107a06/campaign-stats.md)
- [Single-repetition stress-v2 JSON](stress-50k-safe-v2/20260829T022644Z-2a4b107a06/campaign-stats.json),
  retained in the gross corpus but excluded from the 99
- [Post-streaming smoke](post-streaming-smoke/20260829T022544Z-2a4b107a06/report.md),
  retained in the gross corpus but excluded from the 99
- [ACK4 discovery JSON](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.json)
  and [Markdown](discovery-ack4-stress/20260829T030358Z-2a4b107a06/campaign-stats.md)
- [Post-guard smoke JSON](post-lease-guard-smoke/20260829T034840Z-3655cd2a63/campaign-stats.json)
  and [Markdown](post-lease-guard-smoke/20260829T034840Z-3655cd2a63/campaign-stats.md)
- [Dirty grid: initial anchor](screen/01-anchor-start/20260829T004533Z-f2c39fd989/report.json)
  and [final anchor](screen/12-anchor-end/20260829T005149Z-f2c39fd989/report.json)
- [Dirty screen-clean subset](screen-clean/02-p4-a1/20260829T005411Z-f2c39fd989/report.json)

### Correctness and recovery

- [Post-guard p4/a1 fault r01](fault-p4-a1-guarded-r01/report.json)
- [Post-guard p4/a1 fault r02](fault-p4-a1-guarded-r02/report.json)
- [Post-guard p4/a1 fault r03](fault-p4-a1-guarded-r03/report.json)
- [Post-guard p4/a1 fault r04](fault-p4-a1-guarded-r04/report.json)
- [Post-guard p4/a1 fault r05](fault-p4-a1-guarded-r05/report.json)
- [Historical pre-guard p4/a1 fault r01](fault-p4-r01/report.json), with
  r02…r05 retained as redundant corroboration
- [ACK4 fault r01](fault-p4-a4-r01/report.json)
- [ACK4 fault r02](fault-p4-a4-r02/report.json)
- [ACK4 fault r03](fault-p4-a4-r03/report.json)
- [ACK4 fault r04](fault-p4-a4-r04/report.json)
- [ACK4 fault r05](fault-p4-a4-r05/report.json)
- [p16/a16 fault r01](fault-p16-a16-valid-r01/report.json)
- [p16/a16 fault r02](fault-p16-a16-valid-r02/report.json)
- [p16/a16 fault r03](fault-p16-a16-valid-r03/report.json)
- [p16/a16 fault r04](fault-p16-a16-valid-r04/report.json)
- [p16/a16 fault r05](fault-p16-a16-valid-r05/report.json)
- [Insufficient-lease p16/a16 pilot](fault-p16-a16-r01/report.json)
- [First 50k failure](stress-50k-safe/20260829T020655Z-5b47a7d579/horizon/fixed/r01/summary.json)
- [Observer regression test](../../app/tests/streaming_results.php)
- [Lease-guard regression test](../../../../clients/client-laravel/tests/LaravelSupervisorProductionTest.php)

### Broker-native

- [24-hour / 1M soak report](../../../2026-08-11-soak24-1M/results.md)
- [Raw soak sampler](../../../2026-08-11-soak24-1M/raw/bench/bench.csv)
- [Soak broker counters](../../../2026-08-11-soak24-1M/raw/bench/metrics.csv)
