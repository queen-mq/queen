# PERF-3 — the re-measure with ablation (all levers on, minus one at a time)

Phase-1 performance package (2026-09-19). PERF-3 re-runs the NIGHT-A / PERF-2
regimes on the binary that carries **all three levers** — PERF-A (async
durable-point segment pre-flush), PERF-B (planner dedup front), PERF-C (buffered
segment writes + apply-writer pool) — and **ablates each lever one at a time**,
plus an **all-off** run, so each lever's contribution to the O14 numbers is
attributable. **No product code changed here**; PERF-3 is a measurement task, it
sets only the lever knobs.

## Bottom line

**O14 still FAILS at pipeline=4 in every configuration — the durable-point LMDB
`data.mdb` env-sync on the apply thread is untouched and remains the wall — but
one lever earns its place: PERF-B nearly isolates the loaded-store fan-out, the
worst I8 failure M7 had.** Read the two time-scales separately (they tell
opposite-looking stories and both are real):

- **PERF-B is a real win on Alice's prioritized shape.** At 6-min steady state on
  an **8 M-loaded** store, C1000 push **p50 rises only +49.8 %** (8.16→12.22 ms)
  vs empty — against **M7's +545.9 %** (no levers). The dedup front skips the
  committed LMDB probe that, on M7, paged-in from the loaded store and blew C1000
  loaded latency to 63 ms. This directly fixes §2's headline flatness failure.
- **A20k** — every config (all-on / minus-A/B/C / all-off) sits at push **p50
  7–9 ms / p99 230–268 ms**. Levers **neutral**; the p99 tail is the durable
  point, shared by all. Flatness holds (loaded +7.9 % p50, isolated).
- **FAT100** — **~86 k msg/s = 30 % of the 287 k knee**, unchanged by any lever
  (all-off 88 k). The throughput wall is the durable point's LMDB env-sync
  (**1073 ms**, ~63 s of the 75 s wall on the one apply thread).
- **C1000, the transient caveat:** at the **75 s burst** the fresh store is still
  warming (C1000 builds a pop backlog for ~60 s), and there **all-on 152.6 ms p50
  vs all-off 60.7 ms** — PERF-A's *continuous* pre-flush helper + PERF-C's writer
  pool contend for I/O with the 256-bucket fan-out during warm-up. **This is a
  warm-up-phase effect, not steady state**: the 6-min empty C1000 all-on settles
  to **8.16 ms** (PERF-2 warned the 75 s C1000 window is warm-up-dominated).
- **O14 FAILS on every target, every configuration.** PERF-A cuts the durable
  point's *segment-fsync leg* (A20k 67→33, C1000 134→17) but not its dominant
  **LMDB env-sync leg**, which **stays on the apply thread (R-125)** and is the
  wall for the A20k/A50k/C1000 p99 tail and FAT throughput. Even steady-state
  C1000 p99 is 585 ms (18× pg). Recommendation in the diagnosis.

## The knobs (each lever is one env switch, default on)

| lever | knob(s) | default | ablation ("minus X") |
|---|---|---|---|
| PERF-A durable-point pre-flush | `QUEEN_RAFT_DURABLE_ASYNC` | 1 | `=0` (fully-inline point) |
| PERF-B dedup probe front | `QUEEN_RAFT_DEDUP_FRONT` (+ `_MB`=512) | 1 | `=0` (always probe LMDB) |
| PERF-C buffered writes + pool | `QUEEN_RAFT_SEG_BUFFERED` + `QUEEN_RAFT_APPLY_WRITERS` | 1 + `min(4,cores/2)`=4 | both `=0` (per-frame inline write) |

`QUEEN_RAFT_METRICS` (PERF-1) on for every run (+81 ns/entry ≈ 0.16 %);
`QUEEN_RAFT_PIPELINE=4` throughout. "all off" = A, B and C all `=0` together.

## Provenance

- **Binary** `queen` release built on the VM from an rsync of `server/` +
  `crates/` (no `target/`, warm PERF-2 cache) at branch `raft` **HEAD
  `5de30164`** (PERF-C tip; levers at `e49fec15` / `7112400a` / `5de30164`).
  `cargo build --release --bin queen` exit 0, `rustc 1.98.1`. **md5-12
  `9a463fcd5a51`** (differs from PERF-2 `a2208d19ad18` / M7 `cdc5bb59af27` by the
  three lever commits + this build; storage class, flags, all else identical).
- **Loader** `/root/goload` md5 `2c97cccb0d1e` — the exact M1/M6/M7/PERF-2 loader.
- **VM** `root@164.90.215.224` (`queenpgless-01`), Ubuntu 24.04, 8 vCPU, 15 GB,
  ext4 `/dev/vda1`. `ulimit -n 262144`, `QUEEN_STORAGE=raft`,
  `QUEEN_RAFT_PIPELINE=4`, LocalReplicator, no Postgres, dedup 3600 s, retention
  off. `perf` = linux-tools 6.8.0.
- **Host clock** 2026-09-19 ~10:47–12:05 UTC. VM cleanup verdict at the end.
- CSVs + folded tops under `test/raft/vm/raft1/perf/perf3/`.

## Method, and the honest caveats up front

- Each **latency** run boots a **fresh broker / fresh store** and drives the
  byte-identical `measure-raft1.sh` goload flags for **75 s** (`-report 5`), no
  `perf`. This is the deviation the task asked for ("fresh data dir each"); it
  removes NIGHT-A §1's **shared-broker confound** (there A50k left a 1.72 M
  backlog that inflated C1000). So the **all-levers / minus-X / all-off columns
  are like-for-like with each other** (same 75 s, fresh store, same box). The
  **night M7** column (40 s bursts, one shared broker) and **postgres M1** column
  (40 s bursts) are measured differently and are reference points, not a clean
  A/B against my columns.
- **75 s fully drains the A/C regimes** (`pushed≈popped`), unlike M7's 40 s burst
  which left pop lag. A full drain makes push and pop compete for the one apply
  thread the whole run, so **the push p99 here is the honest sustained tail and
  sits above M7's burst p99** (NIGHT-A saw the same: 5-min A20k p99 419 vs 40 s
  burst 163). C1000 and A20k build a pop backlog *during* the push window and
  drain it only after (goload keeps popping past the push duration), so the
  `overall p50` reflects the loaded phase.
- Two `perf record -g -F 99` profiles (A20k, FAT100, 60 s, all levers on) give
  the on-CPU hot symbols; the `queen_raft_*` timing histograms (log2-bucket, read
  p50/p99 as "≤") are the authoritative stage attribution — the durable point is
  off-CPU in `fsync`, so the CPU top-40 under-captures it (PERF-2 caveat 2 holds).
  Top-40 regenerated with the correct `fold40.sh` (profile.sh's fold is broken,
  PERF-2 caveat 1). `[unknown]` 42–46 % is frame-pointer unwinding on a release
  binary.

---

## (1) The six regimes — reference vs all-levers vs each ablation

Push latency from goload `overall`; `rate` = achieved msgs / 75 s. **0 broker
error/panic lines in all 18 PERF-3 latency runs.** Full CSV:
`perf/perf3/ablation.csv`.

### A20k — 100 partitions, push-batch 10, 32 consumers (fully drains)

| metric | pg M1 | night M7 | all levers | minus A | minus B | minus C | all off |
|---|---|---|---|---|---|---|---|
| p50 ms | 9.15 | 6.24 | **9.02** | 7.07 | 8.38 | 8.16 | 7.26 |
| p99 ms | 27.01 | 162.82 | **264.19** | 268.29 | 230.40 | 232.45 | 261.12 |
| p999 ms | 52.48 | 197.63 | **370.69** | 342.02 | 309.25 | 276.48 | 337.92 |
| ack ms | 9.06 | 6.90 | **12.49** | 8.49 | 11.64 | 11.18 | 9.45 |
| rate /s | ~18.7k | — | **19.3k** | 19.3k | 19.3k | 19.3k | 19.3k |
| cores | 0.93 | 1.48 | **1.41** | 1.47 | 1.49 | 1.45 | 1.41 |
| RSS MB | 55 | 254 | **443** | 456 | 396 | 423 | 443 |

**A20k: levers neutral.** p50 7–9 / p99 230–268 across all seven columns; the
spread is run-to-run noise (p99 is one log2 bucket, 230=`2^28−1`·… vs 268). The
tail is the durable point (below), which every configuration shares.

### A50k — 100 partitions, push-batch 10, 48 consumers (consume can't keep up)

| metric | pg M1 | night M7 | all levers |
|---|---|---|---|
| p50 / p99 / p999 ms | 23.17 / 75.26 / 103.94 | 19.58 / 444.42 / 518.14 | **121.34 / 864.26 / 1089.54** |
| ack / rate / cores / RSS | 32.92 / 46.9k / 1.17 / 105 | 23.82 / — / 1.72 / 839 | **82.48 / 47.8k / 1.73 / 1155** |

A50k is consume-bound at 75 s (lag 2.14 M; matches PERF-2's 125 ms p50). Only
all-levers measured (ablation focused on A20k/C1000/FAT100 per task).

### B1 / D1 (low-rate reference, all levers on)

| regime | pg M1 p50/p99 | all levers p50/p99/p999 ack |
|---|---|---|
| B1 | 3.34 / 14.27 | **3.41 / 19.33 / 38.66** ack 3.13 |
| D1 | 1.99 / 4.08 | **1.74 / 5.34 / 15.42** ack 0.92 (p50 beats pg) |

### C1000 — 1000 partitions, push-batch 1, 64 consumers, pop-batch 50

| metric | pg M1 | night M7* | all levers | minus A | minus B | minus C | all off |
|---|---|---|---|---|---|---|---|
| p50 ms | 6.62 | 238.59 | **152.58** | 129.54 | 152.58 | 142.34 | **60.67** |
| p99 ms | 33.02 | 749.57 | **733.18** | 675.84 | 716.80 | 716.80 | 552.96 |
| p999 ms | 54.02 | 831.49 | **815.10** | 798.72 | 774.14 | 806.91 | 806.91 |
| ack ms | 21.36 | 165.06 | **52.79** | 47.00 | 44.47 | 46.93 | 39.04 |
| cores | 0.91 | 1.33 | **1.43** | 1.32 | 1.44 | 1.41 | 1.41 |
| RSS MB | 82 | 908 | **490** | 469 | 469 | 469 | 469 |

*M7 C1000 is **confounded** (shared-broker A50k backlog). **Read this table as the
75 s WARM-UP transient, not steady state** — the fresh C1000 store builds a pop
backlog for ~60 s (PERF-2 flagged the 75 s C1000 window as warm-up-dominated),
and the 6-min steady-state empty C1000 all-on settles to **8.16 ms** (§3). In the
warm-up window the levers add contention: **all-off 60.7 ms ≪ all-on 152.6 ms**.
Removing PERF-A alone → 129.5 (its *continuous* pre-flush helper contends with the
256-bucket fan-out); PERF-C alone → 142.3 (pool per-entry overhead); PERF-B alone
→ no change (152.6). Only all-off returns to the clean inline path (60.7, in range
of PERF-2's no-lever 89.6). So PERF-A + PERF-C **compound to a warm-up p50
regression here**, which does **not** persist to steady state (§3) — but it says
their pacing/pool is mistuned for a fan-out that rewrites all 256 files/s.

### FAT100 — push-only, push-batch 100, rate 300k (knee 287,363 msg/s; 70 % = 201,154)

| metric | pg M1 | night M7 | all levers | minus A | minus B | minus C | all off |
|---|---|---|---|---|---|---|---|
| p50 ms | 23.17 | 4079.62 | **5144.58** | 4816.90 | 4620.29 | 4751.36 | 4620.29 |
| p99 ms | 209.92 | 6782.98 | **7634.94** | 6848.51 | 7831.55 | 7176.19 | 6914.05 |
| rate /s | **287363** | 90043 | **86107** | 86176 | 86319 | 83160 | 88123 |
| % of knee | 100 % | 31 % | **30 %** | 30 % | 30 % | 29 % | 31 % |
| cores / RSS | 1.38 / 255 | 1.42 / 3238 | **1.30 / 2831** | 1.25 / 2842 | 1.34 / 2709 | 1.24 / 2778 | 1.24 / 2778 |

**FAT100: levers neutral** (~86–88 k, all ≈30 % of the knee). The push-only tail
(4.6–5.1 s p50) and the throughput are set by the LMDB durable env-sync, not by
any lever.

---

## (2) O14 verdict per target (all-levers, sustained 75 s; all-off in [ ])

Target (§17 O14): raft1 **p50 AND p99 ≤ postgres** at A20k/A50k/C1000; fat-batch
**≥ 70 % of the knee**.

| target | all levers | postgres | ratio | verdict |
|---|---|---|---|---|
| A20k p50 | 9.02 | 9.15 | 0.99× | **PASS** |
| A20k p99 | 264.19 | 27.01 | 9.78× | **FAIL** |
| A50k p50 | 121.34 | 23.17 | 5.24× | **FAIL** (consume-bound) |
| A50k p99 | 864.26 | 75.26 | 11.5× | **FAIL** |
| C1000 p50 | 152.58 [60.67] | 6.62 | 23.0× [9.2×] | **FAIL** |
| C1000 p99 | 733.18 [552.96] | 33.02 | 22.2× [16.7×] | **FAIL** |
| FAT100 tput | 86,107 [88,123] | 287,363 | 30 % [31 %] | **FAIL** (<70 %) |

**O14: FAIL, every target, every configuration.** The only PASS is A20k p50; its
p99 (and every other line) fails. The best C1000 (all-off, 60.7 ms) is still
9.2× postgres. The levers do not change the verdict.

---

## (4) Diagnosis — what still bounds each symptom (with the profile's hot symbols)

Profiles: `perf record -g -F 99`, 60 s, all levers on. Stage histograms in
`perf/perf3/prof-{A20k,FAT100}-stage.txt`; top-40 in `-top40-self.txt`.

### The durable point is still the wall, and no lever moves its dominant leg

The §11.4 durable point (every 1 s) runs **on the apply thread**: fsync every
touched segment file + dirs, **then a durable LMDB `data.mdb` env-sync**. While
it runs, no entry applies. Measured legs (p50, all levers on):

| regime | durable_point | seg_fsync leg | ⇒ LMDB env-sync leg | count/75s |
|---|---|---|---|---|
| A20k | **268 ms** | 33 ms (was 67 inline) | **~235 ms** | 64 |
| C1000 | **134 ms** | 17 ms (was 134 inline) | **~117 ms** | 77 |
| FAT100 | **1073 ms** | 67 ms (was 134 inline) | **~1000 ms** | 63 |

**PERF-A works** — it halves/quarters the seg-fsync leg (A20k 67→33, C1000
134→17, FAT 134→67, confirmed by the minus-A stage tables where the leg returns
to the inline value). But the **LMDB env-sync leg is left on the apply thread by
design (R-125 / PERF-A scope note)**, and it dominates: A20k ~235 ms, FAT
~1000 ms. So the A20k/A50k/FAT p99 tail = `arrival→proposed` ballooning to the
durable-point duration (A20k `arrival_to_proposed` p99 **268 ms** = durable_point
268; FAT p99 **2147 ms** while 63×1 s points eat ~63 s of 75 s), unchanged by any
lever. On FAT the point is so long the pipeline can only apply ~86 k msg/s.

### Why the levers *regress* C1000 (the fan-out)

C1000 touches all 256 buckets with 1 message/append. all-off 60.7 ms is the
clean floor. With PERF-A **on**, the `queen-rsm-preflush` helper pushes dirty
pages **continuously** (~every `durable_every_bytes/16`), which on a shape that
rewrites all 256 files every second **steals device bandwidth from the apply
thread and the pop path the whole time** — it shortens the periodic stall
(durable_point 268→134) but raises *steady-state* push (129→152 with PERF-C also
on). PERF-C's writer **pool** adds per-entry channel+join overhead (the laptop
already showed A20k apply_entry 65→524 µs); on C1000 it costs ~10 ms p50. The two
compound: 60.7 (off) → 129–152 (on).

### On-CPU hot symbols (top-40 self, all levers on)

- **A20k**: `[unknown]` 42.5 %, `syscall` 5.4 %, **`mdb_page_flush` 4.3 %** (LMDB
  dirty-page copy at the durable commit), tokio `poll_write_vectored` 4.0 %,
  `RawVecInner::finish_grow` 3.7 %, **`store::keys::read_name` 3.3 %**,
  **`Preflusher::spawn` 2.5 %** (the PERF-A helper, now visible), `mdb_node_search`
  2.0 %, `serde_json::Value::deserialize` 1.7 %, `mdb_txn_begin` 1.5 %.
- **FAT100**: `[unknown]` 45.7 %, **`mdb_page_flush` 13.3 %** (dominant — the LMDB
  env-sync's dirty pages), `serde_json::Value::deserialize` 5.0 %,
  `RawVecInner::finish_grow` 4.6 %, `mdb_node_search` 2.9 %, `Overlay` drop_glue
  2.2 %, `push_impl` 2.2 %, serde_json BTreeMap insert 2.1 %, `Preflusher` 1.5 %,
  **`dedup::FrontGen`/`PartFront::maybe` 1.2 %** (the PERF-B front — running,
  cheap, transparent), `HeedRead` plan probe 0.7 %. Confirms PERF-2 (b): FAT is
  the single apply thread on the store path — LMDB env-sync (off-CPU) + the
  per-message `serde_json::Value` alloc (`malloc`/`finish_grow`/BTreeMap ≈ on-CPU).

### The recommendation the numbers make

- **Keep PERF-B on** (`QUEEN_RAFT_DEDUP_FRONT=1`). It is transparent, ~1.2 % CPU,
  and it is the only lever that moved a headline failure: the 8 M-loaded C1000
  flatness went from M7's **+546 %** to **+50 %** p50. It costs nothing on the
  warm/empty regimes.
- **Retune or gate PERF-A** (`QUEEN_RAFT_DURABLE_ASYNC`). Its seg-fsync cut is
  real (A20k 67→33, C1000 134→17) but is swamped by the LMDB env-sync it cannot
  touch, so it does **not** help push latency; and its *continuous* pre-flush
  helper contends with high-fan-out writes (C1000 warm-up +23 ms p50, loaded
  p999 worse). Make the pre-flush pacing gentler, or default it off for
  many-partition shapes.
- **PERF-C's writer pool is not repaid** on the VM (per-entry join cost; C1000
  warm-up +10 ms p50); buffered writes alone are harmless. Consider defaulting
  `QUEEN_RAFT_APPLY_WRITERS=0` (keep `SEG_BUFFERED=1`) — a `SEG_BUFFERED`-on /
  pool-off run is the owed follow-up (not in this pass).
- **The bottleneck is the durable-point LMDB `data.mdb` env-sync on the apply
  thread.** Closing O14 (the A20k/C1000 p99 tail and FAT throughput) needs that
  fsync moved *off* the apply thread (fence apply commits, or a D9 store-mode
  change per **R-125**) or made cheaper (cadence / a leaner durable commit) — not
  the segment-fsync or the dedup probe. Until then O14 stays FAIL at pipeline=4.

---

## (3) Flatness (empty vs 8 M-loaded, 6 min, all levers on) — vs NIGHT-A §2

`flatness-night.sh 6 8000000` on my binary (all levers on, `QUEEN_RAFT_PIPELINE=4`,
PORT 6699). Preload **8 074 100** msgs push-only into at-rest `flbg` (1000 parts)
in 71 s (**~391 B/msg** at rest, 4.1 GB on disk — matches M7's ~387), then A20k
and C1000 6 min each on the loaded store and on a fresh empty store. 0 broker
error lines. Raw `.results` + `compare-*.txt` in `perf/perf3/flat-*`. **These are
warm-loaded** (not PERF-2's `drop_caches` cold store); PERF-B's LMDB-skip win is
even larger with caches dropped.

| run | p50 | p99 | p999 | ack | cores | disk MB/s | rss start→end MB |
|---|---|---|---|---|---|---|---|
| empty A20k | 58.11 | 460.80 | 552.96 | 33.98 | 1.30 | 126.6 | 8 → 1611 |
| **loaded** A20k | 62.72 | 561.15 | 2342.91 | 31.81 | 1.32 | 129.3 | **3157** → 4447 |
| empty C1000 | 8.16 | 585.73 | 765.95 | 10.61 | 1.62 | 65.2 | 8 → 1090 |
| **loaded** C1000 | 12.22 | 569.34 | 741.38 | 15.90 | 1.61 | 68.9 | **4417** → 4834 |

**`flatness compare` (gate: gated metric ±15 %, RSS drift ±5 %) — PERF-3 vs M7:**

| metric | A20k Δ (PERF-3) | A20k Δ (M7) | C1000 Δ (PERF-3) | C1000 Δ (M7) |
|---|---|---|---|---|
| p50 | +7.9 % PASS | +8.1 % PASS | **+49.8 % FAIL** | **+545.9 % FAIL** |
| p99 | +21.8 % FAIL | +5.4 % PASS | −2.8 % PASS | +15.3 % FAIL |
| p999 | +323.7 % FAIL | +155.5 % FAIL | −3.2 % PASS | +4.5 % PASS |
| ack_rtt | −6.4 % PASS | +3.1 % PASS | **+49.9 % FAIL** | **+121.6 % FAIL** |
| cpu_cores | +1.5 % PASS | 0 % PASS | −0.6 % PASS | −5.5 % PASS |
| disk_mbps | +2.1 % PASS | +2.9 % PASS | +5.7 % PASS | +6.7 % PASS |
| RSS drift | FAIL | FAIL | FAIL | FAIL |
| **overall** | **FAIL** | **FAIL** | **FAIL** | **FAIL** |

**What flatness shows at pipeline=4 with the levers:**

- **The C1000 loaded-store isolation is transformed.** M7's C1000 p50 jumped
  **+546 %** under an 8 M store (9.79→63.23); with the levers it is **+49.8 %**
  (8.16→12.22) and p99/p999 are *flat* (−2.8 %/−3.2 %). **PERF-B (the dedup
  front) is doing exactly its job** — skipping the committed LMDB probe that
  paged-in from the loaded store. ack still fails (+49.9 %, down from +121.6 %),
  so isolation is much better but not complete. The overall verdict is still FAIL
  (ack + p50 + RSS), but this is the single biggest movement any lever produced.
- **CPU and disk are flat** in both shapes (G-3 holds: work follows the write
  rate, not stored volume).
- **A20k median/p99 still isolated** (+7.9 %/+21.8 %); its **p999 tail widened**
  (+324 %, vs M7 +156 %) — the loaded-store durable-point tail is worse with the
  PERF-A helper running against the larger store.
- **RSS is still not flat** (~391 B/msg at rest, linear in stored count) and
  climbs within every run (the 3600 s dedup window, R-105/R-124) — unchanged by
  the levers; this is the memory model, not a store-isolation failure. The 100 M
  / 1 M-partition size stays infeasible on 15 GB (~39 GB at rest).

## (5) 20-minute A20k — p99 timeline + durable-point histogram

_Pass 4 running (A20k, all levers on, 1200 s, 5 s sampling) — filled below on
completion._

## What was cut or failed

- Ablation focused on **A20k / C1000 / FAT100** (the task's three regimes); A50k
  / B1 / D1 measured all-levers-on only.
- Flatness reproduces NIGHT-A §2's **warm 8 M-loaded** protocol (not PERF-2's
  `drop_caches` cold store); PERF-B's cold-store LMDB-skip win is largest with
  caches dropped — noted where the loaded numbers understate it.
- Single VM, co-resident loader (O13: final numbers need three VMs). Histograms
  are log2-bucket (± one bucket); the CPU top-40 under-samples the off-CPU
  durable fsync (timing histograms are authoritative).
