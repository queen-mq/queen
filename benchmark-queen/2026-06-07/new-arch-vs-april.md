# Queen new-arch ("pushser", 0.16.0.beta.2) — benchmark results vs April 2026 baseline

**Date:** 2026-06-07 · **Box:** DigitalOcean 32 vCPU / 64 GB (62.79 GiB usable) / 387 GB disk (`206.189.51.203`)

> Source-of-truth results for updating the public benchmark page. All numbers are measured, not estimated.

---

## TL;DR

- With the **workload-appropriate config** (`QUEEN_PUSH_MAX_HOLD_MS=0` for this low-concurrency harness), the **new arch beats the April baseline on 10 of 11 comparable cells, by 1.3×–3.0×**.
- **Only regression:** `hi-part-10` (10 partitions, 500 conns, 1 msg/push) at **−11%**.
- **Partition scaling is the standout:** April *degrades* 27.9k → 21.0k msg/s as partitions grow 10 → 10000; the new arch stays **flat ~32–34k**, so the advantage *widens* to **1.55× at 10k partitions**.
- **Consumer-group fan-out:** **2.5×–2.9×** (5 and 10 groups).
- **Key config lesson:** the push fusion-hold must match workload concurrency. Low-concurrency/batched → `hold=0` (pipelines, broker push `f=21/24`). High-concurrency throughput (the 24h soak) → a larger hold for fusion (sustained ~118k, zero-loss).

---

## Setup / methodology (for reproducibility)

- **Hardware:** 32 vCPU, 62.79 GiB RAM, 387 GB disk. Confirmed the **same droplet class** as the April runs (62.79 GiB host) and the soak box → absolute numbers are directly comparable.
- **New-arch image:** `smartnessai/queen-mq:pushser`, built from branch `jsonperf` @ `d7b3d14` (arch commits `81773a7` "testing the new arch", `1806feb` "0.16.0.beta.2"). Verified clean working tree (no leftover horizon code).
- **April baseline image:** `smartnessai/queen-mq:0.14.0.alpha.3` (the version April's harness pinned); baseline numbers are from the stored **2026-04-25/26** run.
- **Broker env (new arch):** `NUM_WORKERS=10`, `DB_POOL_SIZE=50`, `SIDECAR_POOL_SIZE=250`, `QUEEN_CONCURRENCY_MODE=static`, `QUEEN_POP_MAX_CONCURRENT=16`. Two push-hold runs: **default** (pushser default) and **hold0** (`QUEEN_PUSH_MAX_HOLD_MS=0`).
- **Postgres:** identical to April — `max_connections=300`, `shared_buffers=24GB`, `effective_cache_size=48GB`, `max_wal_size=16GB`, `synchronous_commit=on`, `wal_compression=on`, autovacuum tuned. Held constant so the delta isolates the broker arch.
- **Harness:** April's autocannon-based load (`bench-producer.js` / `bench-consumer.js`), reused unchanged. Per cell: wipe containers → start PG → start queen → autocannon producer + consumer → collect `/api/v1/status`, analytics, docker stats, queen log.
- **Duration:** 300s/cell (April used 900s). Throughput is a rate (autocannon `reqPerSec`), so duration-independent and comparable.
- **Metric:** **push throughput** = producer autocannon *aggregate* `reqPerSec` × messages-per-push. (Pop/consumer throughput is recorded per cell in the consumer logs; this table reports push throughput, matching April's `producer.log` metric.)
- **Errors:** `non2xx ≈ 0` across all cells (≤2 per run, ~3×10⁻⁵ %).

---

## Push fusion-hold A/B — the key config finding

The new arch's push path fuses requests, holding briefly to batch. At **low concurrency** (this harness's 50 conns) a non-zero hold **serializes** the push lane (broker push concurrency `f=1/24`), adding latency; `**hold=0` pipelines** it (`f=21/24`).

**bp-10** (50 conns, 10 msg/push), standalone 120s:


| config            | push msg/s | p50     | p99  | broker push (f / p99rtt) |
| ----------------- | ---------- | ------- | ---- | ------------------------ |
| April (0.14)      | 39.1k      | 11ms    | 38ms | —                        |
| new, default hold | 26.0k      | 18ms    | 48ms | f=1/24, 45ms             |
| new, hold=2ms     | 34.2k      | 13ms    | 39ms | f≈1–2/24, 28ms           |
| **new, hold=0**   | **92.5k**  | **4ms** | 31ms | **f=21/24, 6ms**         |


→ `hold=0` = **2.4× April** here. Takeaway: tune the hold to the workload's concurrency.

---

## Full comparison — push throughput (new arch @ hold=0 vs April)

`req/s` = aggregate producer requests/sec; `msg/s` = `req/s` × msgs-per-push. **New = hold=0** (correct config for this low-conc harness). **Factor = new ÷ April**.


| cell               | conns | msg/push | April req/s | April msg/s | new req/s | new msg/s | new p50 | **factor** |
| ------------------ | ----- | -------- | ----------- | ----------- | --------- | --------- | ------- | ---------- |
| bp-1               | 50    | 1        | 5,796       | 5.8k        | 17,513    | 17.5k     | 2ms     | **3.02×**  |
| bp-10              | 50    | 10       | 3,906       | 39.1k       | 8,505     | 85.1k     | 4ms     | **2.18×**  |
| bp-100             | 50    | 100      | 1,044       | 104.4k      | 1,716     | 171.6k    | 18ms    | **1.64×**  |
| q-1                | 50    | 10       | 3,913       | 39.1k       | 8,473     | 84.7k     | 4ms     | **2.17×**  |
| q-10               | 50    | 10       | 4,050       | 40.5k       | 8,646     | 86.5k     | 4ms     | **2.13×**  |
| bp-10-cg5 (5 CG)   | 50    | 10       | 2,689       | 26.9k       | 6,801     | 68.0k     | 5ms     | **2.53×**  |
| bp-10-cg10 (10 CG) | 50    | 10       | 1,789       | 17.9k       | 5,268     | 52.7k     | 7ms     | **2.94×**  |
| hi-part-10         | 500   | 1        | 27,902      | 27.9k       | 24,745    | 24.7k     | 23ms    | **0.89×**  |
| hi-part-100        | 500   | 1        | 26,041      | 26.0k       | 33,846    | 33.8k     | 13ms    | **1.30×**  |
| hi-part-1000       | 500   | 1        | 25,016      | 25.0k       | 33,373    | 33.4k     | 13ms    | **1.33×**  |
| hi-part-10000      | 500   | 1        | 21,044      | 21.0k       | 32,464    | 32.5k     | 13ms    | **1.55×**  |


**New-arch default-hold run** (the *wrong* config for these low-conc cells — for reference), msg/s:
bp-1 11.5k · bp-10 26.0k · bp-100 34.0k · q-1 26.0k · q-10 25.3k · cg5 24.2k · cg10 22.5k · hi-part-10/100/1000/10000 = 25.6k / 33.7k / 33.0k / 32.1k.

(Note: for the **hi-part** cells, default-hold ≈ hold0 — the hold is neutral at 500 conns; the bp/q/cg gains come entirely from `hold=0`.)

---

## Analysis

1. **Near clean sweep.** New arch wins 10/11 cells (1.3–3.0×). Sole loss: `hi-part-10` (−11%) — few partitions (10) under high concurrency is the new arch's one weak spot.
2. **Batched / low-concurrency = biggest wins** (bp/q/cg: 2–3×), where `hold=0` unlocks the push pipeline.
3. **Partition scaling is an architectural win.** April falls 27.9k → 21.0k msg/s from 10 → 10000 partitions; the new arch holds ~32–34k flat → gap grows to **1.55× at 10k partitions**.
4. **Consumer-group fan-out** improves with group count: 2.53× (5 CG) → 2.94× (10 CG).

---

## Resource usage (new arch @ hold=0, averaged over the run)

CPU shown as **cores** (docker CPU% ÷ 100; host = 32 cores). Queen mem = container RSS; PG mem = RSS (shared_buffers demand-paged in).


| cell          | new msg/s | queen CPU (cores) | queen mem | PG CPU (cores) | PG mem   |
| ------------- | --------- | ----------------- | --------- | -------------- | -------- |
| bp-1          | 17.5k     | 3.4               | 32 MiB    | 7.5            | 3.9 GiB  |
| bp-10         | 85.1k     | 2.0               | 46 MiB    | 8.7            | 13.5 GiB |
| bp-100        | 171.6k    | 0.9               | 73 MiB    | 11.2           | 25.0 GiB |
| q-1           | 84.7k     | 1.8               | 43 MiB    | 7.9            | 13.4 GiB |
| q-10          | 86.5k     | 1.8               | 45 MiB    | 8.1            | 13.7 GiB |
| bp-10-cg5     | 68.0k     | 2.1               | 106 MiB   | 11.5           | 11.7 GiB |
| bp-10-cg10    | 52.7k     | 2.3               | 101 MiB   | 15.0           | 9.7 GiB  |
| hi-part-10    | 24.7k     | 4.1               | 34 MiB    | 2.4            | 4.6 GiB  |
| hi-part-100   | 33.8k     | 5.5               | 41 MiB    | 7.2            | 6.3 GiB  |
| hi-part-1000  | 33.4k     | 5.8               | 48 MiB    | 7.3            | 6.2 GiB  |
| hi-part-10000 | 32.5k     | 5.4               | 53 MiB    | 8.5            | 6.1 GiB  |


- **Broker is lean:** queen RSS **32–106 MiB** and **≤6 cores** on every cell — never the bottleneck here.
- **PG does the work:** **2.4–15 cores**, **3.9–25 GiB** (shared_buffers warming). PG CPU peaks on consumer-group fan-out (cg10 = 15 cores for 10 groups) and big batches (bp-100 = 11 cores / 25 GiB).
- **Efficiency highlight:** bp-100 pushes **171.6k msg/s on ~0.9 broker cores + ~11 PG cores** (big batches → few HTTP requests → minimal broker CPU).
- hi-part cells use more broker CPU (~~5.5 cores, partition discovery) with PG comfortable (~~7–8 cores).
- (Raw per-minute samples in each cell's `docker-stats.log`; `system-metrics.json` / `postgres-stats.json` have finer detail.)

---

## Caveats

- This is the **low-concurrency** autocannon harness (50–500 conns). It does **not** reflect the new arch's high-concurrency ceiling — see the 24h soak below (~118k sustained).
- `q-100`: no April baseline (April ran only q-1/q-10) and the new q-100 producer log didn't record cleanly → omitted.
- April-only cells not re-run: `part-1/10/100`, `hi-part-1` (the master used hi-part-10…10000).
- Metric is **push** throughput (producer). Pop side is in the consumer logs (not summarized here).
- April's `v12` (0.12) cells (version-comparison) are not folded into this table.

---

## Raw data locations

- **New arch (206.189.51.203):** `/root/bench-runs/results/` (hold=0), `/root/bench-runs/results-defaulthold/` (default). Per cell: `producer.log`, `consumer*.log`, `status.json`, `queue-ops.json`, `system-metrics.json`, `postgres-stats.json`, `queen.log`, `docker-stats-final.txt`, `metadata.json`.
- **April baseline:** repo `benchmark-queen/2026-04-26/<cell>/raw/`.
- **Runner (patched):** `benchmark-queen/2026-04-26/_runner/run_test_v3.sh` (image→pushser, +`QUEEN_CONCURRENCY_MODE=static` +`QUEEN_POP_MAX_CONCURRENT=16`, +optional `PUSH_HOLD_MS`); masters `run_master.sh` / `run_cg_master.sh` (300s).

---

## 24h soak (separate run — HIGH-concurrency regime) — *final numbers at ~15:00 CEST*

- **Box:** soak broker `165.232.78.92`, loader `167.99.246.68` (kept fully separate from the matrix box).
- **Config:** `pushser`, `NUM_WORKERS=10`, static pop `POP_MAX_CONCURRENT=16`, `QUEEN_PUSH_MAX_HOLD_MS=20` (fusion); PG `max_wal_size=96GB`, `max_connections=800`. Loader: goload, **650 producers / 200 consumers**, push-batch 10 / pop-batch 300, 300 partitions, queue `benchq`.
- **Through ~17h (snapshot):** sustained **~118k push ≈ ~118k pop** (balanced); cumulative **7.18B messages**, pop = **≥99.99% of push** (the ~~80k gap is the steady in-flight pipeline, not loss); oldest message flat ~125s (no stranding). **Fusion ≈ 220 messages / ≈ 12 HTTP calls per PG commit** (~~1,058 commits/s for ~235k ops/s). Broker ~5 vCPU, PG ~20/32 vCPU; memory plateaued ~33 GB; **0 restarts**.
- **Chart (interim, 6.4h):** `benchmark-queen/2026-06-04/soak-2026-06-06-pushser.png`.
- **TODO:** add final 24h numbers + full-run chart after ~15:00 CEST.