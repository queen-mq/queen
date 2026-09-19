# NIGHT-B — dedup at the product default (D-06, D10 option (a) lean, R-105)

PLAN_RAFT.md §7.1–7.3, I3/I5/D4, §11.4/§11.8, §13.6, Appendix G, O14, O19,
D10, and the WP-0.4 gap RAFT_STATUS.md names: *"probe p99 at 50k msg/s with a
1 h window — never run."* Night of 2026-09-18/19, VM `root@164.90.215.224`
(`queenpgless-01`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4 `/dev/vda1`). All work
under `/root/raft/night/b` (the A50k run) and `/root/raft/night/b2` (the
supplementary run). Ran after NIGHT-A; VM left clean.

- **Binary** the F-1-fixed night binary, `queen` release from branch `raft` @
  HEAD (`29bc7cdd`; fix committed `f8bfa672`), md5-12 **`cdc5bb59af27`** — the
  exact NIGHT-A binary (differs from WP-1.11's `be80d860b4a1`). `QUEEN_STORAGE=raft`,
  `QUEEN_RAFT_PIPELINE=4`, LocalReplicator, no Postgres. `ulimit -n 262144` (F-2).
- **Loader** `/root/goload` md5 `2c97cccb0d1e` (the WP-0.2/1.11 loader).
- **Dedup window = the facade default 3600 s, NOT shortened.** The raft facade
  serves no `/api/v1/configure` (goload's `configure` gets `503
  raft_phase1_unsupported`), so every implicitly-created queue takes
  `default_queue_config` — `dedup_window_seconds: 3600`, retention off. `dedup_cache`
  default (on, 512 MB); store map default (64 GiB, §11.8). Nothing tuned.
- **Store engine confirmed heed/LMDB** (D9 ratified "heed everywhere"): the data
  dir is `log/` (replicated log, rotates+prunes), `seg/` (256 bucket dirs,
  payloads), `store/data.mdb` (the LMDB dedup index + counters + cursors). "LMDB
  data file size" below = `store/data.mdb`; "segment bytes" = `du seg/`.

## TL;DR

1. **At the product default (50 000 offered, 3600 s window) the broker does NOT
   survive to fill the window — it congestion-collapses at ~minute 55, ~93.5 M
   rows, well before the 180 M / minute-60 expiry the task targets.** It holds
   50k for ~60 s (ack 17k/s), then decays over ~35 min to ~25–35k achieved
   (shedding the rest) with push p50 climbing to ~7 s; once goload's inflight
   pins at its 20 000 cap (~min 48) push latency runs away 7 s → 12 → 20 → 27 s
   and **achieved throughput hits 0 at ~min 55** (metastable latency collapse).
   Consume is essentially dead the whole run (popped **3.8 %** of pushed). The
   broker never errors: **0 poison, 0 `TimeWentBackwards`, 0 broker-error lines,
   0 `storage_full`/`MAP_FULL`, no OOM**, `applied=commit`, `lag=0`, leader
   throughout — the raft/apply state machine is healthy; the failure is *unbounded
   push-admission queueing*, not a crash. **F-1 holds.**
2. **RSS is bounded and reclaimable, as R-105 hoped.** Across the 50k run RSS
   plateaued at **~10.0–10.9 GB** while the LMDB index grew to **15.5 GB** and the
   segments to **35.8 GB** (total 51 GB on disk); `MemAvailable` never fell below
   **8.8 GB**. The store's growth past resident memory is file-backed mmap the
   kernel reclaims — it does not turn into anonymous RSS and does not OOM.
3. **Because the 50k run collapsed before the window could fill or expire, the
   "expiry after minute 60" and clean "probe-cost trend" deliverables were run on
   a supplementary sustainable fill** (§B below): push-only **15 000 msg/s** for
   64 min, which the broker sustained flawlessly (offered=achieved **57.56 M**,
   shed 0, pushErr 0, push p50 **5.4 ms**). On it:
   - **Expiry at minute 60 is LOGICAL ONLY.** A re-push of an id older than
     3600 s returns `queued` **with a fresh offset** (re-enqueued) — 26/26 early
     originals expired correctly; a fresh id returns `duplicate`. But the **store
     does not shrink**: `data.mdb` grows straight through minute 60 (7.78 → 7.91
     → 8.04 → 8.17 GB across min 59–62), because **phase 1 has no txns-purge**
     (the `Watermark` effect is never produced live; `planner/mod.rs:105
     pub mod retention {}` is a stub). The window bounds *correctness*, not memory.
   - **Dedup-probe cost is flat.** Over 6 300 in-window probes: **p50 1.16 ms,
     p99 8.97 ms**; by 15-min band the p99 creeps only **5.6 → 13.9 ms** as the
     index grows 0 → 8 GB / 57 M rows, because the index stays in page cache
     (`MemAvailable` steady ~13.7 GB). The RAM-wall probe degradation R-105 warns
     of needs an index bigger than page cache, which 15k/64 min does not reach and
     which 50k could not reach (it collapsed first). **0 wrong verdicts, 0 errors.**

The one-line verdict: **option (a) lean dedup is exact and RAM-bounded at the
product default, but the product default is not a survivable *offered* rate at
50k on one node — the node congestion-collapses long before the 3600 s window
matters, and even when it is filled the window frees nothing because phase-1
retention/txns-purge is a stub.**

---

## (A) The task run — A50k, 3600 s window (`/root/raft/night/b`)

`run.sh`: fresh broker, `QUEEN_RAFT_PIPELINE=4`, ulimit raised, data dir
`/root/raft/night/b/data`. goload `-mode openloop -queue bq-A50k -rate 50000
-push-batch 10 -partitions 100 -consumers 48 -pop-batch 200 -manual-ack -payload
256 -duration 4500 -ramp-sec 5 -report 10` (the Appendix-G A50k shape). Sampler
every 10 s; `probe.py` duplicate side-loop on `nbprobe`. Started 23:42:13Z.

**goload `[final]`:** offered **213.78 M** (achieved **93.55 M = 43.8 %**, shed
113.39 M, pushErr 663 305); pushed 93.55 M, **popped 3.58 M (3.8 %)**, lag
89.97 M; overall push **p50 6651 ms / p99 23 200 ms / p999 28 443 ms**; acked
3.57 M, ackErr 5000, ackAvg 3606 ms.

### The decay, then the collapse (goload per-interval)

| wall | min | achieved/s | shed/s | inflight | push p50 | ack/s | ackAvg |
|---|---|---|---|---|---|---|---|
| 23:43:08 | ~1 | **49 963** | 0 | 2 284 | 228 ms | 16 939 | 51 ms |
| 23:46:08 | ~4 | 38 544 | 11 461 | 20 000 | 4 948 ms | 960 | 425 ms |
| 00:06:08 | ~24 | 28 949 | 21 444 | 19 610 | 6 980 ms | 640 | 2 075 ms |
| 00:28:08 | ~46 | 24 972 | 24 796 | 20 000 | 7 242 ms | 700 | 3 156 ms |
| 00:31:38 | ~49 | 20 427 | 29 590 | 19 980 | 8 716 ms | 480 | 3 292 ms |
| 00:33:08 | ~51 | 9 922 | 40 083 | 20 000 | 19 792 ms | 340 | 3 368 ms |
| 00:35:08 | ~53 | 8 770 | 41 228 | 20 000 | 25 297 ms | 320 | 3 479 ms |
| 00:36:38 | ~54 | 4 725 | 45 271 | 20 000 | 25 821 ms | 160 | 3 559 ms |
| **00:37:08** | **~55** | **0** | 45 273 | 20 000 | timeouts | 0 | — |
| 00:37 → 00:53 | 55–71 | **0 (sustained)** | ~45 k | 20 000 | 10 s+ hangs | 0 | — |

Read: push p50 stays competitive for ~1 min (228 ms, ack 17k/s), but consume
never keeps up (ack collapses to <1k/s within 4 min, lag climbs linearly to
90 M), goload's inflight fills to its 20 000 cap by ~min 48, and from there push
p50 runs away (7 → 12 → 20 → 27 s) until responses stop arriving inside goload's
timeout at ~min 55. A direct single `curl` push at min 57 also timed out
(`http=000`, 10 s): the broker is not rejecting, it is **queueing without bound**
(no push-side shed / 503; `planner_queue_depth=1024`). It stayed collapsed for
the 14 min I watched; I then ended goload (min 71 of the planned 75) once the
collapse was confirmed sustained, to reclaim VM budget for run B — the last
18 min were identical zeros. `applied=commit` kept advancing the whole time
(150 k → 200 k entries) — the accepted backlog drains, new pushes cannot get a
turn.

### Store / RSS (sampler, per §13.6 columns) — the 30/60-min marks

| mark | RSS | LMDB `data.mdb` | seg | total data | disk free | MemAvail | applied |
|---|---|---|---|---|---|---|---|
| min 30 (1800 s) | 9 991 MB | 9.62 GB | 21.86 GB | 31.5 GB | 152 GB | 10 237 MB | 74 009 |
| min 60 (3602 s)¹ | 10 025 MB | 15.22 GB | 34.99 GB | 50.2 GB | 134.8 GB | 160 379 |
| min 71 (end)² | 10 210 MB | 15.48 GB | 35.82 GB | 51.3 GB | 133.8 GB | 199 736 |

¹ by min 60 the broker had collapsed (min 55); the store kept growing only as
the ~93.5 M accepted backlog applied — **not** a steady 50k fill. The index
reached ~**93.5 M rows / 15.5 GB**, short of the task's 180 M target: the node
collapsed before the window filled. ² there is no min-75 mark — the run ended at
min 71 (see above). **RSS peak 10 872 MB; MemAvailable min 8 837 MB — never OOM.**

**§11.8 gate not hit:** `data.mdb` reached 15.5 GB of the default 64 GiB map
(24 %), far under the 85 % `over_high_water` planner refusal, so 0
`storage_full`; the collapse is unrelated to the map.

### Probe cost under 50k — not isolable from the load

The dedup lookup itself is cheap (a duplicate push is `Plan::Empty`, no log
append), but there is **no privileged probe path**: a probe re-push queues behind
the same saturated admission as every push. The first ~10 probes, before
saturation, were a clean **40–145 ms**; thereafter the per-round mean tracked
push p50 — **round 1 mean 1.8 s, round 2 5.4 s, round 3 6.3 s, round 4 6.9 s,
round 5 9.3 s**, then round 6 was 29/30 `the request deadline elapsed` + 1
timeout. **In-window correctness held while answers came back: 499/500 `duplicate`
(1 deadline error), ages ≤ 27 min, 0 wrong.** WP-0.4's "probe p99 at 50k" is
therefore **admission-queueing-bound, not lookup-bound**, on this node under a
sustained 50k offered load.

---

## (B) Supplementary run — sustainable push-only 15k, 3600 s window (`/root/raft/night/b2`)

**Why:** run A collapsed before the window could fill or expire, so the two
deliverables that need a responsive broker across the full 3600 s window —
*expiry after minute 60* and a *clean probe-cost trend* — could not be taken on
it. `run2.sh`: fresh broker (same binary/pipeline/ulimit/default window),
**push-only** (`-consumers 0`, removes the consume-collapse confound of run A)
at **`-rate 15000`** (a rate the node sustains — cf. NIGHT-A's sustained A20k),
`-max-inflight 2048`, `-duration 3840` (64 min), + the same `probe.py`. Started
00:55:36Z. This is a deliberate deviation from the A50k spec, stated plainly:
run A *is* the A50k datum; run B exists to observe the dedup-window mechanism the
collapse hid.

**goload `[final]`:** offered **57.56 M = achieved 57.56 M** (shed 0, pushErr 0);
push **p50 5.41 ms / p99 481 ms / p999 594 ms**. Held 15k for the full 64 min,
inflight ~0–680, 0 shed. **0 poison / 0 error / 0 storage_full**, leader,
`applied=commit=2.73 M`, `lag=0`.

### Store / RSS — the 30/60-min marks (fill is linear, no flatten at 60)

| mark | RSS | LMDB `data.mdb` | seg | total data | disk free | MemAvail |
|---|---|---|---|---|---|---|
| min 30 (1808 s) | 4 162 MB | 4.02 GB | 9.95 GB | 14.0 GB | 168.6 GB | 13 777 MB |
| min 60 (3605 s) | 7 912 MB | 7.89 GB | 19.87 GB | 27.8 GB | 155.7 GB | 13 702 MB |
| min 64 (end) | 8 392 MB | 8.39 GB | 21.20 GB | 29.6 GB | 154.0 GB | 13 683 MB |

`data.mdb` per-minute across the window boundary: min 59 **7.78** → min 60 **7.91**
→ min 61 **8.04** → min 62 **8.17 GB** — **strictly linear, no shrink**. RSS peak
8 392 MB; MemAvailable never below **13 664 MB**. The ~57.6 M-row index (8.4 GB)
sits inside page cache the whole run.

### Expiry at minute 60 — LOGICAL only (end-of-run probe vs an idle broker)

After goload, the broker was left up and idle (responsive); `expiry_check.py`
re-pushed early originals (pushed in the first ~4 min, now >60 min old) and a
fresh control:

```
EARLY nborig-0..750 (>60 min old)  -> queued  (fresh offsets 8065..8090)   26/26
FRESH nbfresh-END   (age 0)        -> duplicate (offset 8091)               1/1
```

So the push-path floor (`dedup_probe_one`, `floor = now − dedup_window_seconds`,
`planner/push.rs`) correctly **expires** an occurrence older than 3600 s — the
re-push is treated as a *new* enqueue (`queued`, new offset), while an in-window
id is `duplicate`. Exactly the D10 option-(a) semantics. **But nothing frees the
row**: the `data.mdb` growth above never flattens, and code confirms why — the
`Watermark`/txns-purge effect is produced only in tests; the leader retention
loop (`planner/mod.rs:105 pub mod retention {}`) is a **phase-1 stub**, and
`retention_enabled` is false by default. **The 3600 s window bounds duplicate
detection, not the store's memory/disk footprint, in phase 1.**

### Probe-cost trend — the WP-0.4 number, cleanly

6 300 in-window probes (once per minute, 100 re-pushes of recorded originals at
their natural ages), broker responsive throughout:

| index age | probes | p50 | p99 | max |
|---|---|---|---|---|
| min 1–15 (index → 2.2 GB) | 1 500 | 1.05 ms | 5.59 ms | 383 ms |
| min 16–30 (→ 4.0 GB) | 1 500 | 0.97 ms | 9.78 ms | 394 ms |
| min 31–45 (→ 6.3 GB) | 1 500 | 1.33 ms | 11.77 ms | 562 ms |
| min 46–60 (→ 7.9 GB) | 1 500 | 1.24 ms | 13.85 ms | 605 ms |
| **overall** | **6 300** | **1.16 ms** | **8.97 ms** | 605 ms |

**Probe p50 is flat at ~1 ms; p99 creeps 5.6 → 13.9 ms** as the index grows to
57 M rows / 8 GB; the p999 tail (~515 ms) is the batched store-commit cadence
(§11.3), not the lookup. **0 wrong verdicts, 0 errors across all 6 300.** This is
the D-06/WP-0.4 statistic that did not exist: at a *sustained* fill, the lean
option-(a) dedup probe is ~1 ms p50 / ~9 ms p99 over a filling 3600 s window,
*as long as the index fits page cache*. It stayed in cache here (15 GB RAM,
8 GB index); the >-RAM regime R-105 warns of would need a larger index than
either run reached (50k collapsed; 15k/64 min = 57 M rows).

---

## Findings

- **N-B1 (severity: high, capacity/liveness — the headline).** At the product
  default (50k offered, 3600 s window) one raft1 node **congestion-collapses to
  0 achieved throughput at ~minute 55 / ~93.5 M rows**, before the window fills
  (180 M) or expiry begins (minute 60). Mechanism: consume never keeps up (3.8 %
  popped), lag → 90 M, goload inflight pins at its 20 000 cap, push p50 runs away
  7 → 27 s, responses stop. The broker does **not** shed or 503 on the push path
  — it queues without bound (`planner_queue_depth=1024`, no push admission-reject)
  — so a saturated node hangs pushes rather than rejecting them. Relevant to D13
  (receivers-hold → 503) and the §11.4/admission design: **there is no push-side
  backpressure that turns overload into a fast 503 instead of a 10 s+ hang.**
- **N-B2 (severity: medium, memory model — confirms R-105 favourably).** RSS is
  bounded and reclaimable: 50k RSS plateaued ~10–10.9 GB while the store grew to
  51 GB on disk; MemAvailable never below 8.8 GB; no OOM (no swap on this VM).
  The dedup index + segments are file-backed mmap the kernel reclaims. R-105's
  "does resident memory stay bounded" → **yes**; the cost is on-disk growth, not
  RSS.
- **N-B3 (severity: medium, correctness-vs-footprint — phase-1 gap).** The
  3600 s dedup window is enforced *logically* at push time (floor); a >window id
  re-enqueues correctly. But **phase 1 never reclaims the expired rows** — the
  txns-purge/retention leader loop is a stub (`planner/mod.rs:105`), so
  `data.mdb` grows straight through minute 60. At 50k that is +10.6 GB/hour of
  dedup index that is never freed (D10's own estimate); a long-lived cell needs
  the WP-2.x retention loop before the window's memory is bounded. Not a
  regression — a not-yet-built piece — but it means **D-06's "then expiring" does
  not free anything in phase 1**.
- **N-B4 (severity: low, observability — matches NIGHT-A O18).** The dedup-probe
  cost cannot be isolated from load in phase 1 (no privileged read/probe path;
  under 50k it queues behind pushes). And `store/StoreMetrics` (commits,
  durable_commits = the durable point, map_full, …) are **not exposed on any
  endpoint** (WP-2.6 maps them to `queen_raft_store_*`); the raft `/health` block
  gives only `applied`/`commit`/`lag`, so "durable-point time" was sampled as
  `applied`/`commit` progress (both tracked `commit`, `lag=0`, all run). The
  broker log emitted **no** windowed rate lines and **no** slow-command lines at
  `LOG_LEVEL=info` — consistent with NIGHT-A O18 (slow-command plumbed, not wired).
- **Positive: F-1 holds under 75 min of the heaviest load yet.** Across both runs
  (a full congestion collapse and a 64-min sustained fill, ~151 M pushes total),
  **0 poison / 0 `TimeWentBackwards` / 0 broker-error / 0 `MAP_FULL`**; leader,
  `applied=commit`, `lag=0` throughout. The pipeline=4 batcher fix is not stressed
  into I5 even here.

## Numbers at a glance

| | A50k (task) | 15k push-only (supplementary) |
|---|---|---|
| offered / achieved | 213.8 M / 93.5 M (43.8 %) | 57.56 M / 57.56 M (100 %) |
| shed / pushErr | 113.4 M / 663 305 | 0 / 0 |
| popped | 3.58 M (3.8 %) | 0 (push-only) |
| push p50 / p99 (overall) | 6 651 ms / 23 200 ms | 5.41 ms / 481 ms |
| rate over time | 50k → 0 (collapse ~min 55) | held 15k, 64 min |
| index at min 60 | 15.22 GB / ~93.5 M rows¹ | 7.89 GB / ~54 M rows |
| RSS min30/min60/peak | 9 991 / 10 025 / 10 872 MB | 4 162 / 7 912 / 8 392 MB |
| MemAvailable min | 8 837 MB | 13 664 MB |
| dedup-probe p50/p99 | admission-bound (1.8–9.3 s/round) | 1.16 ms / 8.97 ms |
| expiry @ min 60 | not reached (collapsed) | logical yes / physical no |
| poison / error / MAP_FULL | 0 / 0 / 0 | 0 / 0 / 0 |

¹ frozen at collapse; not a steady 50k fill.

## Commands

```
# A) task run
bash /root/raft/night/b/run.sh          # DUR=4500, A50k, probe.py side-loop
# B) supplementary sustainable fill
DUR=3840 RATE=15000 bash /root/raft/night/b2/run2.sh   # push-only 15k, probe.py
python3 /root/raft/night/b2/expiry_check.py            # end-of-run, idle broker
```

## Harness notes / deviations

- **goload has no deliberate-duplicate flag** (`goload -h`: only `-dedup-window`,
  a no-op on the raft facade, and `-retries`). Per the task the fallback was used:
  `probe.py` pushes tagged originals (`nborig-<seq>`) to `nbprobe/p0` at ~3/s,
  recording each, and once a minute re-pushes 100 recorded ids at their natural
  (random) ages, checking the duplicate/expiry verdict and timing the re-push
  (the dedup probe). Each id is probed at most once, so every row is a clean
  single-occurrence floor test. The single global LMDB `dedup` database (one
  `(pid,hash)` B-tree for all queues) means an `nbprobe` probe reflects the whole
  index the main load fills.
- **Deviation 1:** run A ended at min 71 of the planned 75 — after 14 min of
  confirmed sustained collapse (achieved = 0, identical), to reclaim budget.
- **Deviation 2 (added):** the supplementary push-only 15k run (B). The A50k run
  cannot answer "expiry after minute 60" or give a clean probe-cost trend because
  it collapses at min 55; run B is a responsive fill that does. Its rate (15k)
  and push-only shape are chosen for survivability across the full window, not for
  comparability with A50k — run A is the A50k datum.
- **"durable-point time"** is not a phase-1 metric (store metrics unmapped, WP-2.6);
  sampled as `/health` `applied`/`commit`/`lag` (all: `applied=commit`, `lag=0`).
- Single-VM, co-resident loader (O13: final numbers need three VMs).

## Cleanup

Both brokers killed, both data dirs deleted (peak 51 GB + 30 GB), no loop / dm /
mount, nothing running; disk back to baseline (182 GB free). Evidence (CSVs, goload
logs, broker logs, run scripts, both `probes.csv`, `expiry_check` output) tarred to
`/root/raft/night/evidence/nightb-artifacts.tgz` (156 KB). Finished 2026-09-19T02:02Z,
well inside 03:30Z.
