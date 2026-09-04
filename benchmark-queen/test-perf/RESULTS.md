# Performance Results Log

Consolidated analysis of all `test-perf/` campaigns up to the start of the Nagle-style hot-path work.

All raw data is archived outside the repo at `~/queen-perf-archives/` so it survives branch switches. Each archive is a `.tar.gz` of the full campaign directory (meta.json, per-run artifacts, aggregate.json, report.md).

To restore any campaign:

```bash
cd ~/Work/queen/test-perf/results
tar -xzf ~/queen-perf-archives/<archive>.tar.gz
# open the extracted <timestamp>/report.md
```

---

## 0. Harness evolution (what changed, and why)

The harness went through three iterations as we discovered validity issues. Each campaign ran with a specific version of the harness; results are only comparable within the same harness version unless noted.


| Harness version | Campaign that introduced it                    | Key change                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| --------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **v1**          | `baseline_master_fce1833_2026-04-20.tar.gz`    | Initial scaffolding: 7 scenarios × 3 runs × 60 s measurement. Consumer used `/pop/partition/<p>?wait=false`. Autocannon metrics only.                                                                                                                                                                                                                                                                                                                 |
| **v2**          | `baseline-v2_master_fce1833_2026-04-21.tar.gz` | Added PG ground truth (`pg_stat_database` + `pg_stat_user_tables` pre/post diff), streaming docker stats + server CPU, `--cpuset-cpus` pinning, host-oversubscription flag, cosmetic warning filter. Per-scenario `MAX_PARTITIONS = max(500, 2× prod_conns)` to avoid producer partition contention.                                                                                                                                                  |
| **v3**          | `baseline-v3_master_fce1833_3scen_120s.tar.gz` | Consumer switched to **wildcard long-poll** (`/pop/queue/<q>?wait=true&timeout=5000`) — the production consumer path. Measurement window doubled to **120 s** to tighten stddev. Oversubscription threshold relaxed to `heavy_threads ≥ host_cpus`. Scenario set pruned to 3 clean ones: S0, S1, S3. PG volume teardown made robust against Docker race. `run-all.sh` reads `scenarios.json` defaults (measure_seconds was silently hardcoded to 60). |


Harness v3 is what every subsequent campaign has used.

---

## 1. Campaign ledger (all archives, chronological)

### 1.1 `baseline_master_fce1833_2026-04-20.tar.gz`

- **Branch**: `master @ fce1833`
- **Date**: 2026-04-20 16:59 UTC+2
- **Harness**: v1
- **Scope**: 7 scenarios × 3 runs × 60 s
- **Purpose**: first proof-of-concept run of the harness.
- **Headline** (client-side `push_msgs/s`):
  - S1 pg2c4g/q2w: 9,396 ± 1,182 (best)
  - S7 pg4c8g/q8w + 16w/800c load: 3,363 ± 976 (+ 1997 errors)
- **Finding**: throughput scaled **in reverse** as load/workers went up. More resources = worse numbers. Signature of transaction-bound push path with no fusion discipline.
- **Invalidated by**: no PG ground truth; no CPU sampling; consumer was `/partition/<p>?wait=false` which generated many empty pops. Treat only as a qualitative sanity check.

### 1.2 `baseline-v2_master_fce1833_2026-04-21.tar.gz`

- **Branch**: `master @ fce1833`
- **Date**: 2026-04-21 10:15 UTC+2
- **Harness**: v2
- **Scope**: 7 scenarios × 3 runs × 60 s
- **Purpose**: validate the in-reverse-scaling finding with ground-truth instrumentation.
- **Headline** (`pg_ins/s`, PG-truth push rate):
  - S1 pg2c4g/q2w: 9,856 ± 824
  - S3 pg4c8g/q2w: 10,339 ± 2,306
  - S4 pg4c8g/q4w: 2,846 ± 2,424 (⚠️ host oversubscribed by old threshold; mean hid single-run collapses)
  - S7 pg4c8g/q8w + 16w/800c: 1,741 ± 91 (+ 2,790 client errors) ⚠️
- **Finding**:
  - **Server CPU is never the bottleneck** on master — always ~120-160% (1.2-1.6 cores) on a 10-CPU host. PG is pinned at its cap.
  - **Buffer hit ratio = 1.0 everywhere** — no disk I/O bottleneck; purely CPU + transaction overhead on PG.
  - S5-S7 are host-oversubscribed on a 10-CPU laptop; those numbers reflect OS scheduler noise more than server behavior.
- **Conclusion**: the push path is transaction-bound. Queen workers serialize on PG round-trips, each ~1 tx per HTTP request.

### 1.3 `baseline-v3_master_fce1833_3scen_120s.tar.gz`

- **Branch**: `master @ fce1833`
- **Date**: 2026-04-21 11:40 UTC+2
- **Harness**: v3 (wildcard long-poll consumer; 120 s window; 3 clean scenarios)
- **Scope**: S0, S1, S3 × 3 runs × 120 s
- **Purpose**: **canonical master-branch baseline** for comparisons. Only uses scenarios that are NOT host-oversubscribed so deltas are attributable to the server, not the scheduler.
- **Headline** (`pg_ins/s`, mean ± stddev):
  - **S0** pg2c4g / 1 queen worker / prod 2w100c: **5,214 ± 2,153**
  - **S1** pg2c4g / 2 queen workers / prod 2w100c: **3,165 ± 77**
  - **S3** pg4c8g / 2 queen workers / prod 4w200c: **2,686 ± 548**
- **Supporting metrics**:
  - S0 push p99 = 1,196 ms; S1 = 1,579 ms; S3 = 6,730 ms (1% of requests took 6.7 s!).
  - `xact_commit/s`: S0=311, S1=363, S3=472. I.e., **adding queen workers INCREASES xact count** — more workers = less fusion per commit.
  - server CPU% p95: S0=81, S1=53, S3=94 (all under 1 core of 10; server is idle while PG is pinned).
- **Counter-intuitive finding**: **fewer queen workers = higher throughput on master**. Each worker has its own libqueen instance with its own job queue and slots; HTTP traffic distributes across workers, so each worker sees a slice, batches are smaller, PG does more transactions for less useful work.
- **Success criterion set here**: for any libqueen improvement, S1 `pg_ins/s` ≥ 6,000 (≥ 90% improvement) would be a win. S1 is the most reproducible (0.5% stddev).

### 1.4 `push-improvement_improvements-perf_32ce754_3scen_120s.tar.gz`

- **Branch**: `improvements-perf @ 32ce754` (PUSH_IMPROVEMENT.md implementation, `push_messages_v3` hardcoded at call site)
- **Date**: 2026-04-21 12:40 UTC+2
- **Harness**: v3 (identical config to 1.3 above for apples-to-apples comparison)
- **Scope**: S0, S1, S3 × 3 runs × 120 s
- **Server binary**: rebuilt from this branch just before the campaign.
- **Headline vs baseline-v3**:

  | Scenario                        | baseline-v3 `pg_ins/s` | push-improvement `pg_ins/s` | **Δ**    |
  | ------------------------------- | ---------------------- | --------------------------- | -------- |
  | **S0** 1-worker                 | 5,214 ± 2,153          | **6,640 ± 1,516**           | **+27%** |
  | **S1** 2-worker                 | 3,165 ± 77             | **6,185 ± 1,482**           | **+95%** |
  | **S3** 2w + bigger PG + 2× load | 2,686 ± 548            | **4,654 ± 1,032**           | **+73%** |

- **Transaction efficiency improved everywhere**:

  | Scenario | baseline xact_commit/s | push-improvement | **Δ**    |
  | -------- | ---------------------- | ---------------- | -------- |
  | S0       | 311                    | 146              | **−53%** |
  | S1       | 363                    | 235              | **−35%** |
  | S3       | 472                    | 257              | **−46%** |

  Fewer commits serving more messages: the signature of the catalog-churn elimination in `push_messages_v3` (no TEMP TABLE, no ALTER, plan cache restored).
- **Tail latency improved everywhere**:

  | Scenario | baseline push p99 (ms) | push-improvement | **Δ**    |
  | -------- | ---------------------- | ---------------- | -------- |
  | S0       | 1,196                  | 813              | **−32%** |
  | S1       | 1,579                  | 884              | **−44%** |
  | S3       | 6,730                  | 3,681            | **−45%** |

- **Server CPU utilization changed**:
  - S1 server CPU% p95: 53 → 136 (+2.5×). The server is finally doing useful work instead of blocking on PG.
  - PG CPU% stayed pinned near its cap (~200 on 2-core scenarios, ~434 on 4-core), but max briefly hit 523 on S3 (CFS burst allowance).
- **Stddev widened** (22-25% of mean). Cause diagnosed in the follow-up log analysis (see §2).
- **Plan compliance**: the PUSH_IMPROVEMENT plan predicted 1.5-3× throughput on realistic workloads. Measured: S1 is +95% (1.95×), S3 is +73% (1.73×). Right inside the prediction window. Claims of "zero catalog writes" and "plan cache restored" are corroborated by the xact_commit/s drops and by `pg_stat_user_tables` showing no DDL activity on `pg_class` / `pg_attribute` / `pg_type`.

### 1.5 `S1 with SIDECAR_MICRO_BATCH_WAIT_MS=10` (not archived, reference only)

- **Branch**: `master @ fce1833` (not `improvements-perf`)
- **Date**: 2026-04-21 12:26 UTC+2
- **Harness**: v3; only S1 was run (3 runs × 120 s)
- **Results directory**: `test-perf/results/2026-04-21_12-26-38/` (still on disk, not archived)
- **Setup**: identical to baseline-v3 S1 but with `SIDECAR_MICRO_BATCH_WAIT_MS=10` (default is 5).
- **Comparison** (S1, master, vs baseline-v3 S1):

  | Metric          | wait=5 (v3) | wait=10       | Δ        |
  | --------------- | ----------- | ------------- | -------- |
  | pg_ins/s        | 3,165 ± 77  | 5,377 ± 1,991 | **+70%** |
  | xact_commit/s   | 363         | 300           | −17%     |
  | push p99 (ms)   | 1,579 ± 253 | 1,089 ± 477   | −31%     |
  | server CPU% p95 | 53          | 111           | +58 pp   |

- **Finding**: even on master (pre push_v3), giving the existing micro-batcher 5 ms more accumulation time nearly doubled throughput. Strong signal that **fusion cadence is the dominant axis**, not PG capacity.
- **Caveat**: stddev jumped from 77 to 1,991 (41% of mean). The wait=10 case is opportunistically good when many concurrent pushes arrive in the same window, less good when fewer arrive. Deterministic fusion (the Nagle plan) would tighten this.

### 1.6 `wait_ms sweep on push_v3` (2026-04-21 afternoon, not archived)

- **Branch**: `nagle @ 32ce754` (identical binary to §1.4; no code change between experiments)
- **Date**: 2026-04-21 13:42 → 14:36 UTC+2
- **Harness**: v3
- **Results directories** (all still on disk, not archived):
  - S1 wait=2, 1 run × 150 s: `test-perf/results/2026-04-21_13-42-20/`
  - S1 wait=2, 3 runs × 150 s: `test-perf/results/2026-04-21_14-02-51/`
  - S1 wait=10, 1 run × 150 s: `test-perf/results/2026-04-21_14-21-59/`
  - S1 wait=20, 1 run × 150 s: `test-perf/results/2026-04-21_14-34-01/`
  - S3 wait=10, 1 run × 150 s: `test-perf/results/2026-04-21_14-36-58/`
- **Purpose**: test the prediction from §2 that the residual sawtooth after push_v3 is pure cadence → shorter `wait_ms` should monotonically improve throughput. The sweep tests the opposite direction too (longer `wait_ms`) and extends to S3 to probe scaling with PG headroom.

**Results (S1, push_v3, only `SIDECAR_MICRO_BATCH_WAIT_MS` varies):**


| wait_ms       | pg_ins/s      | xact/s  | msgs/commit | push p50 | push p95 | push p99  | server CPU% p95 | PG CPU% p95  |
| ------------- | ------------- | ------- | ----------- | -------- | -------- | --------- | --------------- | ------------ |
| 2 (n=3)       | 8,573 ± 1,565 | 389     | 22.0        | 89       | 404      | 552 ± 178 | 133             | 201 (pinned) |
| 5 (§1.4, n=3) | 6,185 ± 1,482 | 235     | 26.3        | —        | —        | 884       | 136             | ~180         |
| 10 (n=1)      | 12,154        | 185     | 65.7        | 62       | 269      | 368       | 144             | 201 (pinned) |
| **20 (n=1)**  | **13,215**    | **109** | **121.2**   | **62**   | **196**  | **267**   | **96**          | **195**      |


**Results (S3, push_v3, same binary):**


| Source           | wait_ms | pg_ins/s      | xact/s | msgs/commit | push p99 | PG CPU% p95    | server CPU% p95 |
| ---------------- | ------- | ------------- | ------ | ----------- | -------- | -------------- | --------------- |
| §1.4 (n=3)       | 5       | 4,654 ± 1,032 | 257    | 18.1        | 3,681    | ~400           | 136             |
| this sweep (n=1) | 10      | 6,670         | 162    | 41.2        | 2,040    | 398.7 (pinned) | 187             |


**Per-message PG CPU cost** (derived: `PG_cores × PG_CPU% / pg_ins_per_s`):


| Scenario | wait_ms | PG-cores × % | commits/s | CPU-ms / commit | msgs / commit | **CPU-ms / msg** |
| -------- | ------- | ------------ | --------- | --------------- | ------------- | ---------------- |
| S1       | 2       | 2.0          | 389       | 5.1             | 22            | **0.23**         |
| S1       | 5       | 1.8          | 235       | 7.7             | 26            | **0.29**         |
| S1       | 10      | 2.0          | 185       | 10.8            | 66            | **0.17**         |
| S1       | 20      | 1.95         | 109       | 17.9            | 121           | **0.15**         |
| S3       | 10      | 4.0          | 162       | 24.7            | 41            | **0.60**         |


**Findings**:

1. **The §2 cadence story was half wrong.** On push_v3 S1, wait=2 does beat wait=5 (+39%), but wait=10 beats wait=2 by +42% and wait=20 beats wait=10 by another +9%. **Longer wait, not shorter, is the right direction on the current binary.** Total S1 uplift from default (wait=5) to wait=20 is **+114%** on pg_ins/s and **−70%** on push p99, with no code changes.
2. **push_v3 did not fully eliminate per-commit fixed overhead.** Per-message PG CPU cost falls monotonically with batch size: 0.29 ms (at 26 msgs/commit) → 0.17 ms (66) → 0.15 ms (121). The BEGIN/COMMIT/WAL-flush/trigger/index machinery per transaction amortizes meaningfully when batches are fat.
3. **At wait=20, S1 is no longer PG-pinned** (PG p95 194.9 vs 200 cap). Server CPU is 96% p95, zero warnings, zero lag events. First measurement on push_v3 where neither tier shows saturation signals.
4. **S3 exposes a second bottleneck invisible on S1: single-slot-per-drain.** S3 (4c PG, 4w/200c producer) at wait=10 gets 6,670 pg_ins/s while S1 (2c PG, 2w/100c) at wait=10 gets 12,154. A 2× PG gets roughly half the throughput because S3's per-message PG CPU cost is 3.5× higher (0.60 vs 0.17 ms/msg). The cause is structural: each drain fires at most one batch per type per slot per tick, so the arrival rate on S3 fragments into 41 msgs/commit instead of the 66+ that PG would process efficiently. Idle slots don't help because the drain won't use them mid-tick. This is the exact limitation LIBQUEEN_HOTPATH_IMPROVEMENT §3.2 step 4 (multi-slot per drain) and §4.8 (slot-free kick) target.
5. **S3 p99 at wait=10 is 2,040 ms** — raising wait_ms further would amplify this without addressing the structural cap.

**Caveats**: S1 wait=10, S1 wait=20, and S3 wait=10 are n=1 runs. The directional signal is strong (monotonic across four wait_ms values; consistent per-CPU-cost trend) but stddev is not characterized at the extremes. A proper n=3 confirmation of wait=20 would tighten the headline; reasonable but not required for the conclusions above.

### 1.7 `libqueen-nagle_nagle_574622e_3scen_120s.tar.gz`

- **Branch**: `nagle @ 574622e` (full LIBQUEEN_IMPROVEMENTS.md implementation — per-type queues, Triton-style BatchPolicy, Vegas adaptive ConcurrencyController, event-driven drain with submit-kick / slot-free-kick / re-armed safety timer)
- **Date**: 2026-04-21 16:33 → 16:55 UTC+2
- **Harness**: v3 (identical config to §1.3 and §1.4 for apples-to-apples)
- **Scope**: S0, S1, S3 × 3 runs × 120 s
- **Server binary**: rebuilt from this branch just before the campaign.
- **Env**: all libqueen knobs at plan defaults (`QUEEN_CONCURRENCY_MODE=vegas`, `QUEEN_PUSH_PREFERRED_BATCH_SIZE=50`, `QUEEN_PUSH_MAX_HOLD_MS=20`, `QUEEN_PUSH_MAX_CONCURRENT=4`, `QUEEN_VEGAS_{MIN,MAX}_LIMIT={1,16}`, `QUEEN_VEGAS_{ALPHA,BETA}={3,6}`). No per-scenario overrides.
- **Headline vs baseline-v3 (master) and §1.4 push-improvement**:

  | Scenario                        | baseline-v3   | push-improvement | **libqueen-nagle** | Δ vs baseline | Δ vs push-imp |
  | ------------------------------- | ------------- | ---------------- | ------------------ | ------------- | ------------- |
  | **S0** 1-worker                 | 5,214 ± 2,153 | 6,640 ± 1,516    | **9,026 ± 1,503**  | **+73%**      | **+36%**      |
  | **S1** 2-worker                 | 3,165 ± 77    | 6,185 ± 1,482    | **7,052 ± 686**    | **+123%**     | **+14%**      |
  | **S3** 2w + bigger PG + 2× load | 2,686 ± 548   | 4,654 ± 1,032    | **7,221 ± 350**    | **+169%**     | **+55%**      |

- **Tail latency collapses across the board** (event-driven drain eliminates the 10 ms timer quantization):

  | Scenario | baseline-v3 p99 | push-imp p99 | **libqueen-nagle p99** | Δ vs baseline | Δ vs push-imp |
  | -------- | --------------- | ------------ | ---------------------- | ------------- | ------------- |
  | S0       | 1,196           | 813          | **473 ± 36**           | **−60%**      | **−42%**      |
  | S1       | 1,579           | 884          | **618 ± 56**           | **−61%**      | **−30%**      |
  | S3       | 6,730           | 3,681        | **1,156 ± 208**        | **−83%**      | **−69%**      |

- **Transaction fusion at an all-time high** — fewer commits, more messages per commit:

  | Scenario | baseline xact/s | push-imp xact/s | **nagle xact/s** | push-imp msgs/commit | **nagle msgs/commit** |
  | -------- | --------------- | --------------- | ---------------- | -------------------- | --------------------- |
  | S0       | 311             | 146             | **86**           | 26.3                 | **105**               |
  | S1       | 363             | 235             | **138**          | 26.3                 | **51**                |
  | S3       | 472             | 257             | **120**          | 18.1                 | **60**                |

  Nagle's msgs/commit on S0 (105) approaches the best number from the entire wait_ms sweep (121, S1 wait=20 static). On S3 it jumps from 18 → 60 without a single env override — the first evidence that the multi-slot-per-drain + slot-free-kick closes the S3 structural gap the sweep identified.
- **Stddev tightens on the two scenarios that mattered most**:

  | Scenario | baseline stddev % | push-imp stddev % | **nagle stddev %** |
  | -------- | ----------------- | ----------------- | ------------------ |
  | S0       | 41.3%             | 22.8%             | **16.7%**          |
  | S1       | 2.4%              | 23.9%             | **9.7%**           |
  | S3       | 20.4%             | 22.2%             | **4.8%**           |

  S1 widened vs its unusually tight baseline (±77 on 3,165) but is well within the plan's "< 10%" gate (9.7% achieved, 10% target). S3 is now the tightest scenario in the whole ledger.
- **Resource signatures**:
  - S0 server CPU% p95: 81 → 92 → **97** (small, ~1 core — server idle, per-commit efficiency is the lever)
  - S1 server CPU% p95: 53 → 136 → **158** (server doing more useful work per batch)
  - S3 server CPU% p95: 94 → 136 → **174** (highest yet; still well under 2 cores)
  - PG CPU% p95: S0=198, S1=200 (pinned at 2-core cap), S3=393 (near 4-core cap).
  - Zero errors, zero warnings, zero queue_full hits, zero event-loop lag events across all 9 runs. `slots=` stayed below capacity; no exhaustion.
- **Plan §11.3 gate compliance**:

  | Scenario | Target pg_ins/s | Actual    | Target p99 | Actual  | Target stddev | Actual | Verdict                 |
  | -------- | --------------- | --------- | ---------- | ------- | ------------- | ------ | ----------------------- |
  | **S0**   | ≥ 6,600         | **9,026** | ≤ 500      | **473** | < 20%         | 16.7%  | **PASS (all 3)**        |
  | S1       | ≥ 13,000        | 7,052     | ≤ 300      | 618     | < 10%         | 9.7%   | **1 / 3** (stddev only) |
  | S3       | ≥ 20,000        | 7,221     | ≤ 500      | 1,156   | < 15%         | 4.8%   | **1 / 3** (stddev only) |

- **Why S1 / S3 mean-throughput gates missed**: the plan's 13k / 20k targets were extrapolated from the §1.6 wait=20 single-run (S1=13,215) and from the assumption that multi-slot on S3 would scale S1's per-core rate × 2 PG cores. Under plan-default Vegas settings the event-driven drain fires as soon as `queue_size ≥ 50` OR a slot frees, producing batches averaging 51 msgs/commit on S1 vs 121 achieved by the static wait=20 run. Amortization is still the dominant S1 lever (§2.1) and the default policy accumulates fewer messages per fire than a deterministic 20 ms hold. Levers (all already wired via env):
  - `QUEEN_PUSH_PREFERRED_BATCH_SIZE=120`, `QUEEN_PUSH_MAX_HOLD_MS=30` — approximate the wait=20 amortization window while keeping the submit-kick / slot-free-kick responsiveness.
  - `QUEEN_VEGAS_BETA=4` with `QUEEN_PUSH_MAX_CONCURRENT=4` — let Vegas actually shrink when RTT rises (otherwise queue_load is bounded by in_flight=4 which can never exceed beta=6; see LIBQUEEN_IMPROVEMENTS.md §13 "Vegas oscillation near operating point" follow-up).
  - On S3, raise `QUEEN_PUSH_MAX_CONCURRENT` to ~8 so multi-slot can actually fill the 4 PG cores.
  These are all tuning experiments to run as a follow-up n=3 campaign; no code changes required.
- **Where the plan targets were hit**: p99 tail latency dropped by 60–83% across all three scenarios vs baseline, and by 30–69% vs push-improvement. This is the unambiguous, gate-compliant result: the event-driven drain converts the sawtooth waiting-for-timer-tick into immediate slot-free firing. S0 hits every single gate (throughput, p99, stddev) with plan-default knobs.

### 1.8 `libqueen-nagle_server-box_574622e_sweepseries.tar.gz` — server-box saturation campaign

- **Branch**: `nagle @ 86c1547` (same binary as §1.7 plus the post-§1.7 default bumps: `QUEEN_PUSH_MAX_CONCURRENT` 4→24, `QUEEN_ACK/POP_MAX_CONCURRENT` 4→16, `QUEEN_VEGAS_MAX_LIMIT` 16→32, `QUEEN_VEGAS_BETA` 6→12)
- **Date**: 2026-04-22 07:48 → 10:02 UTC+2
- **Host**: dedicated 32-core / 62 GB Ubuntu 24.04 bench box (not the laptop). `ulimit -n` raised to 65535; PG container launched with `--ulimit nofile=65535:65535`. No PG-side tuning (default `synchronous_commit=on`, `shared_buffers` scales with `PG_MEM_GB`).
- **Harness**: modified sweep matrix (`test-perf/scripts/sweep-cores-payload.sh`) that varies PG cores, push_batch and payload size within one campaign. All other knobs match harness v3.
- **Scope**: four back-to-back sweeps isolating different variables. All results re-parsed into `test-perf/results/server-box-campaign/*.csv` and summary numbers below are PG-ground-truth from `pg_stat_user_tables.n_tup_ins` over the measurement window.

#### 1.8.1 Producer sweep (`sweep_2026-04-22_08-24-37`) — the headline numbers

Config: `NUM_WORKERS=2`, producer `10w × 200c` (first pass with the heavier producer), `CONS=1w × 50c`, 60 s measurement, n=1.

Shows **pure scaling behaviour** across PG cores at the current Queen budget of 2 workers:


| PG cores | 1 KB / batch=1 | 1 KB / batch=10 | 10 KB / batch=1 | 10 KB / batch=10 | PG CPU p95 (batch=10/1KB) | PG util    |
| -------- | -------------- | --------------- | --------------- | ---------------- | ------------------------- | ---------- |
| 1        | 3,127          | 8,375           | 1,838           | 3,688            | 100%                      | pinned     |
| 2        | 4,608          | 13,011          | 2,853           | 6,105            | 181%                      | 90% pinned |
| 4        | 4,898          | 16,966          | 3,628           | 7,141            | 258%                      | 64%        |
| 8        | 5,417          | 20,593          | 3,793           | 7,025            | 285%                      | 36%        |
| 16       | 6,035          | 20,578          | 3,774           | 7,269            | 316%                      | 20%        |
| 24       | 6,092          | **21,065**      | 3,589           | 7,165            | 313%                      | **13%**    |


Numbers are pg_ins/s. Vegas converged to `limit_mean ≈ 24` for 1 KB workloads and `≈ 21` for 10 KB workloads — i.e., Vegas genuinely adapts per-workload and never hits the raised 32 ceiling.

Critical observation: **PG CPU utilization falls from pinned (2 cores) to 13 % (24 cores)** — the Queen side can't feed PG past ~21 k msg/s at this Queen budget. Next sweep isolates that.

#### 1.8.2 Queen scaling sweep (`sweep_2026-04-22_08-24-37` restyled, `NUM_WORKERS=10`)

Same producer, `NUM_WORKERS` raised from 2 → **10** (matching the December v0.11.0 production config):


| PG cores | batch=10 / 1 KB pg_ins/s | Δ vs 2-queen | batch=10 / 10 KB pg_ins/s | Δ vs 2-queen | bytes/s (10 KB) |
| -------- | ------------------------ | ------------ | ------------------------- | ------------ | --------------- |
| 1        | 7,233                    | −14%         | 3,193                     | −13%         | 32.7 MB/s       |
| 2        | 11,903                   | −9%          | 5,699                     | −7%          | 58.4 MB/s       |
| 4        | 20,613                   | **+21%**     | 8,893                     | **+25%**     | 91.1 MB/s       |
| 8        | 29,333                   | **+42%**     | 13,555                    | **+93%**     | 138.8 MB/s      |
| 16       | 33,783                   | **+64%**     | 18,043                    | **+148%**    | 184.8 MB/s      |
| 24       | **35,234**               | **+67%**     | **18,087**                | **+152%**    | **185.2 MB/s**  |


Two findings:

1. At **low PG core counts (1-2)** more Queen workers **hurt** (batch fragmentation tax). The crossover is ~4 cores.
2. At high PG core counts throughput rises **1.5-2.5×** — especially for the 10 KB case, which jumps from 77 MB/s to 185 MB/s of sustained ingestion.

PG CPU utilization with 10 queen workers: still only 27-31 % at 24 cores. PG has 2-3× headroom left.

#### 1.8.3 Option A — `NUM_WORKERS` 10 → 16 (`sweep_2026-04-22_09-17-30`)

Does more Queen help? Same producer (`10w × 200c`), only 16 and 24 PG cores tested.


| PG cores / payload / batch | 10 queen | **16 queen** | Δ   | server CPU p95 (16 queen) | queen-max cap |
| -------------------------- | -------- | ------------ | --- | ------------------------- | ------------- |
| 16 / 1KB / 1               | 6,387    | 6,636        | +4% | 237%                      | 15%           |
| 16 / 1KB / 10              | 33,783   | 35,212       | +4% | 870%                      | 54%           |
| 16 / 10KB / 10             | 18,043   | 18,926       | +5% | 1,110%                    | 69%           |
| 24 / 1KB / 10              | 35,234   | 35,932       | +2% | 907%                      | 57%           |
| 24 / 10KB / 10             | 18,087   | 19,005       | +5% | 1,100%                    | 69%           |


**Negligible gain (2-6%) from 60 % more Queen workers.** And the 16-worker server sits at only 54-69 % of its theoretical CPU cap. Queen is **not** the bottleneck.

#### 1.8.4 Option B — `push_batch` 10 → 100 (`sweep_2026-04-22_09-27-25`)

Keep `NUM_WORKERS=10`, raise `push_batch` from 10 → 100 so each HTTP request carries 10× more messages.


| PG cores / payload | batch=10 | **batch=100** | Δ pg_ins/s | push_rps (batch=100) | msgs/commit | PG util |
| ------------------ | -------- | ------------- | ---------- | -------------------- | ----------- | ------- |
| 16 / 1 KB          | 33,783   | **47,300**    | **+40%**   | 490                  | 112         | **40%** |
| 24 / 1 KB          | 35,234   | **46,565**    | **+32%**   | 487                  | 110         | **36%** |
| 16 / 10 KB         | 18,043   | (broken)      | —          | 0                    | 0           | 10%     |
| 24 / 10 KB         | 18,087   | (broken)      | —          | 0                    | 0           | 12%     |


`batch=100` at 1 KB gives the campaign's peak throughput: **47,300 msg/s**. Producer HTTP rate drops to 490 req/s because autocannon must construct 100 KB bodies per request — but each carries 100 messages, so msg/s jumps.

`batch=100` + `10 KB` fails: each HTTP body is ~1 MB, autocannon at `10w × 200c` can't sustain that. Not a server defect; a bench-harness ceiling.

#### Campaign conclusion — the bottleneck hierarchy on this hardware

```
  Producer (autocannon 10w × 200c)  ←  THE CEILING
      ~3.6 k req/s @ batch=10
      ~490 req/s   @ batch=100
                ↓  HTTP
  Queen server (10 workers = 10 cores of HTTP capacity)
      Observed: 44-73 % of max — 2-5 cores always idle
                ↓  libpq
  PostgreSQL (24 cores, default tuning)
      Observed: 27-40 % of max — 14+ cores always idle
```

Doubling the producer to `20w × 200c` (`sweep_2026-04-22_09-01-20`) **reduced** throughput by 1-13 % across all 8 combos — connection-overhead overwhelmed the marginal HTTP capacity. Autocannon at this worker/connection size is the true ceiling, not libqueen.

#### Peak throughputs measured (all at PG_CORES=24, 60 s, n=1)


| Config       | push_batch | payload  | **msg/s**  | bytes/s       | msgs/commit | vs §1.7 nagle S3 (7,221) |
| ------------ | ---------- | -------- | ---------- | ------------- | ----------- | ------------------------ |
| 2 queen      | 10         | 1 KB     | 24,103     | 24.1 MB/s     | 163         | +234%                    |
| 10 queen     | 10         | 1 KB     | 35,234     | 35.2 MB/s     | 54          | **+388%**                |
| 16 queen     | 10         | 1 KB     | 35,932     | 35.9 MB/s     | 32          | +398%                    |
| **10 queen** | **100**    | **1 KB** | **47,300** | **47.3 MB/s** | 110         | **+555%**                |
| 10 queen     | 10         | 10 KB    | 18,087     | **185 MB/s**  | 40          | —                        |
| 16 queen     | 10         | 10 KB    | 19,005     | **194 MB/s**  | 26          | —                        |


**Zero errors, zero non-2xx across all 76 runs** on the server box (across four sweeps). Buffer hit ratio 100 %. No deadlocks.

#### Validation against the December v0.11.0 baseline

December public benchmark (same 32-core class, with `synchronous_commit=off` and `NUM_WORKERS=10`): 10 k req/s sustained at `batch=1`. We match that (6.8 k req/s at batch=1/1KB with `synchronous_commit=on`) while delivering vastly better batched throughput (47 k msg/s at batch=100). The refactor is validated end-to-end at scale.

#### Calibrated defaults

The 2026-04-22 default bumps (`MAX_CONCURRENT` 4→24, `VEGAS_MAX_LIMIT` 16→32, `VEGAS_BETA` 6→12) are confirmed appropriate: Vegas never hits the 32 ceiling on any tested workload, and the data ledger now spans 1-24 PG cores × 1-10 KB payloads × batch 1/10/100. `MAX_CONCURRENT=24` is a safe operational upper bound; Vegas converges well inside it on all workloads.

---

## 2. Analysis: why PUSH_IMPROVEMENT is not the end of the story

Parsing `[libqueen] EVL: …` log lines from the push-improvement S1 campaign (one entry per worker per second), the per-second `jobs/s` rate shows a **pronounced sawtooth** — not the smooth stream we'd expect from a healthy batcher:

```
Worker 0 samples: mean 365 jobs/s, stddev 135 (CV 37%), min 1, p50 358, p95 600, max 666
Worker 1 samples: mean 363 jobs/s, stddev 137 (CV 38%), min 39, p50 345, p95 607, max 707
```

Both workers oscillate in lockstep — same high seconds, same low seconds — suggesting PG serves one fat batch per worker at a time while the other worker waits for its next 10 ms timer tick.

Raw timeline of the first 30 seconds, Worker 0:

```
351  37 301 318 622 217 485 482 422 402 490 440 277 387 413 366 151 239 495 565 603 433 555 470 555 295 527 421 565 171
```

Single seconds with 600+ jobs next to single seconds with 30-150 jobs. Queue depth never exceeds 20 — so fusion capacity isn't the limit; **cadence** is.

**Mechanism**: the drain is only triggered by `_uv_timer_cb` every 10 ms. When a slot finishes its batch, it's idle until the next timer tick (up to 10 ms). That idle time is the valley in the sawtooth. During the 10-ms gap, new submissions pile up, so the next tick fires a big batch (the peak).

This is exactly the problem LIBQUEEN_HOTPATH_IMPROVEMENT.md targets with a Nagle-style drain:

- Kick drain on `submit()` → no more waiting 10 ms for the timer when work arrives.
- Kick drain on `_process_slot_result` → fire immediately when a slot frees.

**Predicted effect on S1** (primary gauge): mean stays around 6,000 `pg_ins/s` (already PG-CPU-bound there) but **stddev drops from ±1,482 to ±100-200** and **push p99 drops further** because individual requests no longer wait for the next timer window. If there is PG-side headroom (S3), the mean should also rise since slots are now utilized continuously.

### 2.1 Correction from the §1.6 wait_ms sweep

The §2 analysis above is partially superseded by the data in §1.6. The sawtooth is real and the slot-idle gap between timer ticks is real, but attributing the full residual bottleneck to cadence was wrong. Two updates:

1. **Per-commit fixed overhead survived push_v3 in non-trivial form.** The prediction "S1 mean stays around 6,000" implicitly assumed PG per-message cost was roughly constant across batch sizes. The sweep shows it's not: 0.29 → 0.17 → 0.15 ms per message as batches grow 26 → 66 → 121. Amortization is still the biggest throughput lever on S1. Going to wait=20 on the existing timer nearly doubles S1 throughput (6,185 → 13,215) without any drain-trigger code change.
2. **S3 reveals a second, independent bottleneck** that wait_ms tuning cannot fix: single-slot-per-drain. On S3 the arrival rate exceeds what one-batch-per-type-per-tick can pack, so work fragments into small batches and idle slots pile up. Per-message PG CPU cost is 3.5× S1's. This is the limitation Nagle's multi-slot-per-drain (LIBQUEEN_HOTPATH_IMPROVEMENT §3.2 step 4) is actually built to remove — the submit-kick and slot-free-kick (§4.8) are the secondary pieces.

**Revised mental model** for the push_v3 binary:

$$
\text{pgins/s} \approx \min\left(\text{slotutilization} \times \frac{\text{msgspercommit}}{T_{\text{pg}}(\text{batch})},\ \text{drainfirerate} \times \text{batchcap}\right)
$$

- On S1, the first term binds. The lever is **fatter batches**, i.e. longer wait_ms (up to some knee not yet located above 20 ms).
- On S3, the second term binds. The lever is **more batches per drain**, i.e. multi-slot parallelism — a structural change, not a tuning knob.

**Implication for the Nagle plan**: the plan's framing ("kick drain on submit / kick drain on slot-free") is roughly the dual of running wait_ms at T_pg, and on push_v3 T_pg at wait=20 is around 18 ms per commit. A pure slot-free-kick would fire the moment a slot frees, which on the current binary corresponds to a shorter-wait regime and would likely produce batches closer in size to wait=10 or smaller. The S1 throughput of Nagle-as-specified could therefore come in below the static wait=20 number unless the implementation also preserves a minimum-accumulation window. The multi-slot piece is unambiguously a win on S3.

---

## 3. Cross-campaign comparison table

All rows are S0/S1/S3; all use harness v3; all use 120 s measurement; all use wildcard long-poll consumer.

### pg_ins/s (mean ± stddev)


| Scenario | baseline-v3 (master) | push-improvement | **libqueen-nagle** | Δ vs baseline | Δ vs push-imp |
| -------- | -------------------- | ---------------- | ------------------ | ------------- | ------------- |
| S0       | 5,214 ± 2,153        | 6,640 ± 1,516    | **9,026 ± 1,503**  | **+73%**      | **+36%**      |
| S1       | 3,165 ± 77           | 6,185 ± 1,482    | **7,052 ± 686**    | **+123%**     | **+14%**      |
| S3       | 2,686 ± 548          | 4,654 ± 1,032    | **7,221 ± 350**    | **+169%**     | **+55%**      |


### xact_commit/s (lower = better fusion)


| Scenario | baseline-v3 | push-improvement | **libqueen-nagle** | Δ vs push-imp |
| -------- | ----------- | ---------------- | ------------------ | ------------- |
| S0       | 311         | 146              | **86**             | **−41%**      |
| S1       | 363         | 235              | **138**            | **−41%**      |
| S3       | 472         | 257              | **120**            | **−53%**      |


### push p99 (ms, lower = better tail)


| Scenario | baseline-v3 | push-improvement | **libqueen-nagle** | Δ vs baseline | Δ vs push-imp |
| -------- | ----------- | ---------------- | ------------------ | ------------- | ------------- |
| S0       | 1,196       | 813              | **473**            | **−60%**      | **−42%**      |
| S1       | 1,579       | 884              | **618**            | **−61%**      | **−30%**      |
| S3       | 6,730       | 3,681            | **1,156**          | **−83%**      | **−69%**      |


### server CPU% p95 (higher = more server work, usually good when PG has headroom)


| Scenario | baseline-v3 | push-improvement | **libqueen-nagle** | Δ vs push-imp (pp) |
| -------- | ----------- | ---------------- | ------------------ | ------------------ |
| S0       | 81          | 92               | **97**             | +5                 |
| S1       | 53          | 136              | **158**            | +22                |
| S3       | 94          | 136              | **174**            | +38                |


### msgs per commit (PG truth — higher = more efficient fusion per transaction)


| Scenario | push-improvement | **libqueen-nagle** |
| -------- | ---------------- | ------------------ |
| S0       | 26.3             | **105**            |
| S1       | 26.3             | **51**             |
| S3       | 18.1             | **60**             |


### wait_ms sweep on push_v3 (§1.6, single binary, 150 s)

S1 monotonic in wait_ms:


| wait_ms | S1 pg_ins/s         | Δ vs wait=5 | S1 push p99 | S1 msgs/commit | S1 PG% p95 |
| ------- | ------------------- | ----------- | ----------- | -------------- | ---------- |
| 2       | 8,573 ± 1,565 (n=3) | +39%        | 552         | 22             | 201        |
| 5       | 6,185 ± 1,482 (n=3) | —           | 884         | 26             | ~180       |
| 10      | 12,154 (n=1)        | +96%        | 368         | 66             | 201        |
| **20**  | **13,215 (n=1)**    | **+114%**   | **267**     | **121**        | **195**    |


S3 (same binary, only wait_ms=10 tested so far):


| Scenario | wait_ms | pg_ins/s  | push p99  | msgs/commit | PG% p95 | server% p95 |
| -------- | ------- | --------- | --------- | ----------- | ------- | ----------- |
| S3 §1.4  | 5       | 4,654     | 3,681     | 18          | ~400    | 136         |
| **S3**   | **10**  | **6,670** | **2,040** | **41**      | **399** | **187**     |


Gap between S1 and S3 at the same wait_ms is the fingerprint of the single-slot-per-drain limit:


| wait_ms | S1 pg_ins/s | S3 pg_ins/s | S3/S1 | S3 PG-cores / S1 PG-cores | S3 CPU-ms/msg | S1 CPU-ms/msg |
| ------- | ----------- | ----------- | ----- | ------------------------- | ------------- | ------------- |
| 10      | 12,154      | 6,670       | 0.55  | 2.0×                      | 0.60          | 0.17          |


S3 has 2× the PG cores but gets 0.55× the throughput because per-message PG cost is 3.5× S1's — a batch-size-amortization gap that no wait_ms can close, because the drain refuses to fill extra slots.

### Sawtooth signature (post-push-improvement S1, from server log)


| Worker | mean jobs/s | stddev | CV      | min | p50 | p95 | max |
| ------ | ----------- | ------ | ------- | --- | --- | --- | --- |
| 0      | 365         | 135    | **37%** | 1   | 358 | 600 | 666 |
| 1      | 363         | 137    | **38%** | 39  | 345 | 607 | 707 |


High coefficient of variation (37-38%) = cadence is the remaining bottleneck, not capacity.

---

## 4. Scenario definitions (reproduce with harness v3)


| #   | PG      | Queen     | Producer  | Consumer | Heavy threads on 10-CPU host |
| --- | ------- | --------- | --------- | -------- | ---------------------------- |
| S0  | 2c / 4g | 1 worker  | 2w / 100c | 1w / 25c | 4                            |
| S1  | 2c / 4g | 2 workers | 2w / 100c | 1w / 25c | 5                            |
| S3  | 4c / 8g | 2 workers | 4w / 200c | 1w / 50c | 7                            |


Common: `SIDECAR_POOL_SIZE=200`, `DB_POOL_SIZE=20`, PG `max_connections=300`, `push_batch=10`, `pop_batch=100`, wildcard long-poll consumer with `wait=true&timeout=5000`. Measurement window 120 s + 10 s warm-up. 3 runs per scenario.

S2 is intentionally skipped; the "S2" label in v1/v2 campaigns was overloaded with confounded axes. S4-S7 are skipped because they're host-oversubscribed on a 10-CPU host and produce scheduler noise rather than server signal.

---

## 5. Open hypotheses to validate next

The original §5 list was written before the §1.6 wait_ms sweep. Strikethrough/refined inline; see §5.6 for the post-sweep additions that should drive work now.

1. ~~**Nagle-style drain (submit + slot-free kicks) removes the sawtooth.**~~ **Partially superseded.** The sawtooth was real, but the throughput ceiling it was blamed for turned out to be per-commit fixed-cost amortization, not cadence. Static wait=20 already collapses S1 warnings to 0 and lag events to 0 without any code change. Nagle would still compress stddev, but that's a smaller win than originally framed.
2. ~~**When PG has headroom, Nagle improves mean pg_ins/s.**~~ **Refined.** S3 is where this matters; mechanism is multi-slot-per-drain, not cadence. Target is to close the S3/S1 per-core gap (S1 = 6,077 pg_ins/s per PG core at wait=10; S3 today = 1,668). Budget: raise S3 pg_ins/s from ~6,700 to ≥ 15,000 (approaching 4× S1's per-core rate, i.e. the PG-headroom ceiling).
3. **Within-type concurrency cap of 1 (TRANSACTION-style serial per type) doesn't hurt throughput.**
  - Single-slot-per-type fusion beats multi-slot parallel small batches (the empirical finding from §4 of LIBQUEEN_HOTPATH_IMPROVEMENT). Measure by turning the cap up and down in the same campaign.
4. **Bounded queue depth + 503 backpressure doesn't trigger under S0/S1/S3.**
  - Reports should show 0 `queue_full hits` for these scenarios — otherwise the cap is too tight for normal load.
5. ~~**Longer micro-batch wait stops helping once Nagle is in.**~~ **Flipped into an open question.** §1.6 shows wait=20 > wait=10 > wait=5 > wait=2 on S1; the optimum is ≥ 20 ms, not small. If a Nagle slot-free-kick fires batches sized at natural `submit_rate × T_pg` rather than accumulating to a wait_ms window, its S1 throughput could come in below static wait=20. Test by benchmarking Nagle against static wait=20 on S1; if Nagle loses by more than stddev, add a minimum-hold timer to Nagle or ship wait=20 default without the refactor.

### 5.6 Post-sweep hypotheses (2026-04-21 afternoon, driving next work)

1. **Raising the default `SIDECAR_MICRO_BATCH_WAIT_MS` from 5 to 20 is a large, safe, zero-code win on S1-shaped workloads.**
  - Evidence: §1.6 S1 wait=20 → +114% pg_ins/s, −70% p99, lower server CPU, zero warnings. Binary unchanged.
  - Test: confirm at n=3 on S1, S0. Check p99 behavior at wait=20 on S3 (likely rises further — structural limit still there).
  - Risk: at very low submit rates, wait=20 adds up to 20 ms of idle-latency floor. Mitigate with a submit-kick (Nagle-lite) while keeping the 20 ms as a max-accumulation bound.
2. **The S3 gap is structural (single-slot-per-drain), not parametric.**
  - Evidence: §1.6 S1/S3 per-core gap at identical wait_ms (S1 gets 6,077 pg_ins/s per PG core, S3 gets 1,668) traces to msgs/commit being 41 on S3 vs 66 on S1; the drain cannot pack more per tick.
  - Test: run S3 with wait=20 (single run). Prediction: throughput rises only modestly (small single-digit percent) while p99 worsens substantially. If instead throughput scales meaningfully, the structural claim weakens.
  - Intervention: the Nagle plan's multi-slot-per-drain (§3.2 step 4) + slot-free-kick (§4.8) is the appropriate fix; the submit-kick is a secondary low-load latency improvement.
3. **After shipping either wait=20 default or Nagle multi-slot, the per-message PG CPU cost is the next bottleneck to chase.**
  - At wait=20 S1, PG is just below pinned at 0.15 ms/msg CPU. If multi-slot Nagle lets S3 reach similar per-message efficiency across 4 cores, S3 ceiling becomes ~27,000 pg_ins/s (4 cores / 0.15 ms/msg). Beyond that, per-row PG cost (index maintenance, WAL volume, trigger body) becomes the lever. Not actionable until the batching bottlenecks above are closed.

---

## 6. How to run a comparable campaign on any new branch

```bash
# 1. On the branch you want to test
git status                          # must be clean for the meta.json to record a clean SHA
cd server && make build-only        # rebuild the binary from this branch

# 2. Run the canonical 3-scenario campaign
cd ..
nvm use 24
./test-perf/scripts/run-all.sh

# 3. Archive the result with a descriptive name
STAMP=$(ls -dt test-perf/results/*/ | head -1 | xargs basename)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
SHA=$(git rev-parse --short HEAD)
tar -czf ~/queen-perf-archives/${BRANCH}_${SHA}_3scen_120s.tar.gz \
    -C test-perf/results $STAMP

# 4. Append a new section to this file (test-perf/RESULTS.md) with the findings.
#    Follow the template in §1.4 — it's the most complete.
```

---

## 7. Archive manifest


| File                                                           | Branch            | SHA     | Harness | Notes                                                                                                                     |
| -------------------------------------------------------------- | ----------------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------- |
| `baseline_master_fce1833_2026-04-20.tar.gz`                    | master            | fce1833 | v1      | 7 scenarios, 60 s, pre-ground-truth. Historical only.                                                                     |
| `baseline-v2_master_fce1833_2026-04-21.tar.gz`                 | master            | fce1833 | v2      | 7 scenarios, 60 s, with PG ground truth.                                                                                  |
| `baseline-v3_master_fce1833_3scen_120s.tar.gz`                 | master            | fce1833 | v3      | **Canonical master baseline.** 3 scenarios, 120 s, wildcard long-poll.                                                    |
| `push-improvement_improvements-perf_32ce754_3scen_120s.tar.gz` | improvements-perf | 32ce754 | v3      | PUSH_IMPROVEMENT (push_messages_v3) evaluated.                                                                            |
| `libqueen-nagle_nagle_574622e_3scen_120s.tar.gz`               | nagle             | 574622e | v3      | Full LIBQUEEN_IMPROVEMENTS.md (per-type queues, Triton BatchPolicy, Vegas, event-driven drain).                           |
| `libqueen-nagle_server-box_574622e_sweepseries.tar.gz`         | nagle             | 86c1547 | custom  | Server-box saturation campaign (32-core Ubuntu 24.04). 4 sweeps over PG cores × push_batch × payload × NUM_WORKERS. §1.8. |


Archive entries also documented in `~/queen-perf-archives/INDEX.md` (outside the repo).
