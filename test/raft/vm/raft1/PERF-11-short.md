# PERF-11 — the round-4 SHORT measure (PERF-J pop-empty fastpath + push-priority, PERF-K cycle-shape excision)

Phase-1 performance package, round 4 (2026-09-19/20). PERF-11 measures the binary that
carries **PERF-J** (`QUEEN_RAFT_POP_FASTPATH_EMPTY` + `QUEEN_RAFT_PUSH_PRIORITY`, both
default **on**; plus the unconditional diagnostics-honesty fix — `received_at` no longer
restamped on defer) and **PERF-K** (the `CycleShape::Continuous` batcher shape removed,
`Drain` kept as the only shape, `QUEEN_RAFT_CYCLE_TRACE` a default-**off** diagnostic) on
top of the round-3 default config. This is the **60/75/60 s** consume-matched, drained
short measure — three regimes, plus a one-knob-off ablation on A20k and C1000 for the two
**new round-4 knobs** (`POP_FASTPATH_EMPTY`, `PUSH_PRIORITY`). **No product code changed
here**; PERF-11 sets only knobs and measures.

## Bottom line — `proceed=false`

**Round 4 is a real, uniform, regression-free improvement over round 3 on all three
regimes — A20k push p50 6.05→5.54, C1000 push p50 212→157, FAT100 90.7 %→94.3 % of the
knee — and PERF-J's diagnostics fix finally makes `arrival→proposed` honest (C1000 now
reports its true 67 ms / 1073 ms wait, not round-3's 8.39 ms restamp artifact). But it
does not move either push-latency bound to the aggressive gate.** The two bounds are the
same two round 3 named, now measured truthfully:

- **A20k: parity-to-slightly-better, p50 floor intact — `proceed` misses on p50.**
  [**Corrected — see Round-4 review correction §1/§2: A20k is under-consumed (pop ~10.5 k/s
  vs push ~20 k/s), "drains" is a phase-2 catch-up, and the strict-O14 "PASS" is NOT
  like-for-like with postgres M1.**] Round-4 default push **p50 5.54 / p99 15.04 / p999
  22.91 ms**, phase-2 catch-up lag → −517 k, pop **10.5 k/s**. That is a clean step under
  round 3 (6.05 / 17.28 / 25.22) and **0.61× / 0.56× of postgres M1** (9.15 / 27.01) — but M1
  ran with consume KEEPING UP, so this comparison is under a lighter effective load. **But p50 5.54 > 4.0 ms: the gate misses on the p50 floor.**
  The floor is the batcher group-commit cycle: `arrival→proposed` **p50 ~2.1 ms** (a log2
  bucket better than round-3's 4.2, but **identical in all three A20k columns** — see the
  ablation) plus the commit round-trip; `push_h_await` p50 4.19 ms, `durable_point` 16.8 ms,
  `log_fsync` ~1 ms. **Neither round-4 knob touches A20k** (min-FP and min-PP are both within
  run-to-run noise of the default) — as PERF-J predicted, the fastpath and push-priority
  target C1000's empty-pop load, not A20k's push path.
- **C1000: p50 lower (directional-within-spread), honest histograms — bound is the propose/fsync ingress.**
  [**Corrected — see §4: 212→157 is inside C1000's 142–230 ms spread, single-run; read as
  directional (fastpath mechanism), not a firm ~26 %. See §5 for the O14 denominator: 23.7×
  vs the contractual M1 6.62, but ≈10.4× vs the same-load E6 15.04.**]
  Round-4 default push **p50 156.67 / p99 733.18 / p999 831.49 ms** (round 3 211.97 / 790.53),
  fully drains at 75 s (pushed = popped = 220 490, lag 1; phase-2 lag 15), pops/acks ≈ 2940/s
  consume-matched. **p50 156.67 > 13, p99 733.18 > 66: the gate misses by ~12× / ~11×.** The
  bound is the **command-queue fill feeding a single serialized log fsync**: `push_h_submit`
  **p99 537 ms** (the HTTP handler blocking on the bounded command channel), `arrival→proposed`
  **p50 67 ms / p99 1073 ms**, while every downstream stage is cheap (`proposed→committed`
  1 ms, `log_fsync` 1 ms, `apply_entry` 66 µs, `pop_wildcard` plan 33 µs / 1 ms p99). The
  broker commits **~55 k entries / 75 s ≈ 735 entries/s** and appends ~202 k msgs (~2700/s)
  — the fsync-serialized entry rate under combined push+pop at 1000 partitions is the ceiling,
  exactly PERF-J's finding. **The ~1 s stall persists** (`queue_wait` / `arrival→proposed`
  p99 = 1073 ms, the ~1 s log2 bucket) in every C1000 run — the deep-queue tail, not the pop
  plan.
- **FAT100: 271 130 msg/s achieved-under-3.3%-shed vs 287 363 postgres-sustained, PASS.**
  [**Corrected — see §6: not "% of the knee." Raft's 271 130 is achieved while shedding 573 k
  (3.3 %), i.e. over its own knee; the 287 363 postgres figure is a SUSTAINED shed-0 rate.**]
  Round-4 push-only **271 130 msg/s** (16.27 M / 60 s), ratio **0.94** to the 287 363 postgres
  sustained rate — a step up from round 3's 260 498 (0.91). Clears ≥ 250 k and the ≥ 70 % O14
  bar. (Overload push latency p50 537 / p99 1597 ms is the expected shed-regime shape; the
  loader sheds 573 k of the offered 17.25 M.)
- **The ablation attributes the C1000 win to the fastpath, and nothing to A20k.** On A20k,
  min-FP (5.73 / 15.17) and min-PP (5.41 / 14.02) are both inside run-to-run noise of the
  default (5.54 / 15.04); `arrival→proposed` p50 is **2.1 ms in all three** — the round-4
  knobs are A20k no-ops. On C1000, **min-FP regresses to p50 230.40** (vs default 156.67):
  with the fastpath off, provably-empty wildcard pops re-enter the serial batcher
  (`pop_wildcard` planned **105 878 vs 97 186**, `arrival→proposed` p50 134 vs 67 ms),
  lengthening the queue pushes wait behind. **min-PP is p50 156.67** (identical to the
  default) with p99 765.95 — push-priority does not measurably move C1000 p50 here. So the
  round-4 C1000 gain is the **fastpath**, single-run and inside C1000's known 142–230 ms
  spread, but mechanism-supported (fewer pops into the batcher). [**Round-4 review correction
  §3: because PUSH_PRIORITY moves no number in any column here, its default-on ship is NOT
  justified by this experiment — hold it as provisional pending an ablation that moves a
  number; only POP_FASTPATH_EMPTY is supported.**]

## Round-4 review correction (2026-09-20)

A coordinator review of this round-4 package found the **measurements sound and the code
claims verified** — `received_at` is no longer restamped on defer (the `batcher.rs` defer
sites preserve the original `arrival`, restamping only `enqueued_at`); there is **no**
per-partition propose lock and **no** `std::sync::Mutex` held across `.await` on the propose
path (grep clean); loader shapes are byte-identical to the baseline (A20k `-consumers 32`,
C1000 `-consumers 64`); the M1 column matches `test/raft/vm/baseline/RESULTS.md` exactly and
every O14 ratio is arithmetically correct. The **gate verdict is unchanged** (`proceed=false`:
A20k p50 5.54 > 4.0; C1000 fails). But several **conclusions** in the bottom line and the O14
table overreach and are corrected here:

1. **A20k is NOT steady-state consume-matched; its O14 "PASS" is not like-for-like.** In
   phase 1 (while pushing) A20k consumes **~10.5 k pop/s against the ~20 k msg/s push offer**
   — roughly HALF the offered rate — so steady-state lag grows to ~570 k–940 k (PERF-10
   measured 750 k–940 k). The "drains (phase-2 lag −517 k)" claim above is a *post-load
   catch-up* window property (push cut to 100/s for 80 s), not steady-state consume-matching.
   Postgres M1 (9.15/27.01) was measured with the **same 32 consumers keeping up**
   (baseline achieved≈offered, shed 0) because each postgres request is its own pooled txn
   (push and pop run in parallel), whereas raft's single serial pipeline starves the consume
   side. So raft's push p50 5.54 was measured under a **lighter effective load** than its
   postgres baseline, and "0.61× / 0.56× of postgres, strict O14 PASS" is drawn from an
   under-consumed run. **Re-label A20k as under-consumed (pop ~10.5 k/s vs push ~20 k/s).**
   The operative verdict — FAIL on the 4 ms p50 gate — is unaffected.
2. **Reconcile the drain definition with PERF-10.** One package definition:
   *consume-matched / drained* = **phase-1 steady-state lag ≈ 0 while pushing**. By that
   definition A20k does **not** drain (PERF-10 is correct); PERF-11's "drains" is only the
   phase-2 catch-up after push stops. The two reports' numbers are consistent — only the
   labels differed. PERF-10's call to re-check round-3 (PERF-7) "A20k consume-matched and
   drained (lag 0)" is **validated**: that was the phase-2 property, not steady-state matching.
3. **PUSH_PRIORITY is default-on but this experiment supports no benefit.** The ablation:
   A20k min-PP 5.41/14.02 is within noise of the default 5.54/15.04; **C1000 min-PP p50
   156.67 is identical to the default** (p99 765.95 vs 733.18, inside the spread); FAT100 has
   no PP column. Only `POP_FASTPATH_EMPTY` is ablation-supported (C1000 min-FP 230 vs 157). So
   the round-4 gain is the **fastpath alone**; PUSH_PRIORITY moved no measured number.
   **Hold PUSH_PRIORITY as default-on only provisionally** — its mechanism (push-first drain
   under a pop burst) is sound and targets C1000's shape, but it must move a number in an
   ablation before it is claimed as a win or kept as a justified default. Flagged for the
   commit agent. (Code is correct and I2-clean; this is an attribution/default question, not a
   defect.)
4. **The C1000 157-vs-212 win is directional-within-spread, not a firm ~26 %.** Round-3 212,
   round-4 157 and min-FP 230 all sit inside C1000's own **142–230 ms** run-to-run spread,
   single-run per cell. Read the ~26 % as **directional**, mechanism-supported (pop_wildcard
   planned 97 k vs 106 k with the fastpath), not established. Establishing it needs ≥3
   replicates that move the number beyond its noise band (a VM re-measure — see below).
5. **State one canonical C1000 O14 denominator.** Two postgres C1000 numbers exist: WP-0.2
   **M1 6.62 ms** (the *contractual* gate baseline, but measured in the sequential
   single-broker regimes **without** 64 concurrent busy consumers) and PERF-9 **E6 15.04 ms**
   (postgres on the **same box under the same 64-consumer C1000 load**). The like-for-like
   same-load control is E6: raft 156.67 / E6 15.04 ≈ **10.4×**. Against the contractual M1
   6.62 the ratio is 23.7×. Both fail the gate; **report both and name E6 15.04 as the
   like-for-like denominator**, so the raft-vs-postgres *architecture* gap (≈10×: serial
   pipeline vs pooled txns) is not conflated with the ≈24× that also carries the baseline's
   lighter load.
6. **FAT100: achieved-under-shed vs postgres-sustained, not "% of knee."** Raft's
   271,130 msg/s is achieved while **shedding 573,700 (3.3 % of the offer)** — raft is already
   over its own knee. Postgres's 287,363 msg/s (and the calibration's 300 k) is a **sustained**
   rate at shed 0. The throughput ratio 0.94 still clears the ≥70 % O14 bar and ≥250 k (PASS
   stands), but "94.3 % of the knee" compares a shed-regime achieved rate to a sustained one;
   frame it as **achieved-under-3.3%-shed vs postgres-sustained**.

**Recommended VM re-measures** (need the perf8 VM lock; **not run here** — these are report
errors, corrected above from in-hand evidence): (a) C1000 round-4 default ×3 replicates to test
whether 157 survives the 142–230 spread; (b) A20k with enough consumers for phase-1 lag ≈ 0,
then re-read push p50 under equal effective load. Until then A20k's O14 is under-consumed and
the C1000 win is directional.

## Provenance

- **Binary** `queen` release, VM-built from a fresh rsync of `server/` + `crates/` (no
  `target/`) at branch `raft` **HEAD `1a3de800`** (PERF-J committed) **plus the uncommitted
  working tree** carrying PERF-K (`server/src/handlers/raft.rs`, `rsm/replicator/local.rs`,
  `rsm/tests/batcher.rs` — laptop↔VM md5 verified identical before build).
  `cargo build --release --bin queen` **EXIT=0** in 2 m 53 s, `rustc` via `~/.cargo/env`.
  **md5-12 `e2467156dc41`**.
- **Loader** `/root/goload` md5 `2c97cccb0d1e` — the exact M1 / PERF-2..7 loader.
- **VM** `root@164.90.215.224`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4 `/dev/vda1`, `uptime`
  load ~0 at start, `pgrep queen|goload` = none (nothing of ours or another agent's
  running), `losetup`/`dmsetup` = none. `ulimit -n 262144`, `QUEEN_STORAGE=raft`, METRICS
  on, LocalReplicator, no Postgres, dedup 3600 s, retention off. **Round-4 default knobs**
  (explicit): round-3 set `QUEEN_RAFT_PIPELINE=4 BATCH_COUNTERS=1 DEDUP_INDEX=txns
  DEDUP_FRONT=1 DURABLE_ASYNC=1 SEG_BUFFERED=1 BUCKETS=16 DRIVER_NOTIFY=1 WRITER_PIPELINE=0
  PENDING_TRANSITIONS=0 CLAIM_FROM_RING=1` **+ `POP_FASTPATH_EMPTY=1 PUSH_PRIORITY=1`**.
  Port 6714; each run booted a FRESH broker + fresh data dir with the settle-guard
  (`sync`; wait `Dirty < 20 MB`, all runs `waited=1s dirty<170 kB`), killed **only** its own
  PID, and I deleted every `run-*/data` and `run-*/buffers` after. **`ERRLINES=0` in all 7
  runs.** Host clock **2026-09-19 23:33–23:48 UTC**. Work under `/root/raft/perf8/short/`
  (runner `perf8run.sh`, driver `campaign.sh`, raw `campaign.out`).
- **The gate** (task PERF-11): consume-matched **A20k p50 ≤ 4.0 ∧ p99 ≤ 27 ∧ drained;
  C1000 p50 ≤ 13 ∧ p99 ≤ 66 ∧ drained; FAT100 ≥ 250 k msg/s.**

## The configs

| column | knobs (on the round-3 default set + `PIPELINE=4 METRICS=1`) |
|---|---|
| **round 4** | `POP_FASTPATH_EMPTY=1 PUSH_PRIORITY=1` (the round-4 defaults) |
| **minus FP** | `POP_FASTPATH_EMPTY=0` (PERF-J empty-pop fastpath off) |
| **minus PP** | `PUSH_PRIORITY=0` (PERF-J push-first drain off) |

`CYCLE_TRACE` stays off (a diagnostic, not a perf knob); PERF-K's `Continuous`-shape removal
is not behind a knob (`Drain` is now the only shape) so it has no ablation column.

---

## (1) A20k — 100 partitions, push-batch 10, 32 consumers, pop-batch 200 (60 s + 80 s drain, consume-matched)

Push latency = goload `overall` (phase 1, under consume). Every kept run fully drains in
phase 2 (lag → negative). `pop /s` = phase-1 popped / 60 s.

| metric | pg M1 | round 3 (PERF-7) | **round 4** | minus FP | minus PP |
|---|---|---|---|---|---|
| push p50 ms | 9.15 | 6.05 | **5.54** | 5.73 | 5.41 |
| push p99 ms | 27.01 | 17.28 | **15.04** | 15.17 | 14.02 |
| push p999 ms | 52.48 | 25.22 | **22.91** | 22.91 | 21.38 |
| pop /s (ph1) | — | 6.3k | **10.5k** | 10.2k | 11.5k |
| drained? | — | yes | **yes (−517k)** | yes (−541k) | yes (−460k) |
| push_h_await p50/p99 | — | — | **4.19/16.8** | 4.19/16.8 | 4.19/16.8 |
| push_h_submit p50/p99 | — | — | **0.001/0.002** | 0.001/0.002 | 0.001/0.002 |
| arrival→proposed p50/p99 | — | 4.19/16.8 | **2.10/8.39** | 2.10/8.39 | 2.10/8.39 |
| proposed→committed p50/p99 | — | — | **2.10/4.19** | 2.10/4.19 | 2.10/4.19 |
| log_fsync p50/p99 | — | — | **1.05/2.10** | 1.05/2.10 | 1.05/2.10 |
| apply_entry p50/p99 | — | — | **0.066/0.524** | 0.066/0.524 | 0.066/0.524 |
| durable_point p50/p99 | — | 16.8/33.6 | **16.8/33.6** | 16.8/33.6 | 16.8/33.6 |
| pop_wildcard plan p50/p99 | — | 0.066/0.131 | **0.066/0.262** | 0.066/0.131 | 0.066/0.262 |
| CPU cores / RSS MB | 0.93/55 | 1.30/166 | **1.19/180** | 1.32/177 | 1.42/176 |

The three A20k columns are indistinguishable on every stage — the round-4 knobs do nothing
on A20k. The pop-rate step (6.3k→10.5k) is inside PERF-6's known 3.4–11.4 k single-run A20k
spread, so the robust fact is **drained=yes**, not a pop win.

## (2) C1000 — 1000 partitions, push-batch 1, 64 consumers, pop-batch 50 (75 s + 40 s drain, consume-matched)

| metric | pg M1 | round 3 (PERF-7) | **round 4** | minus FP | minus PP |
|---|---|---|---|---|---|
| push p50 ms | 6.62 | 211.97 | **156.67** | 230.40 | 156.67 |
| push p99 ms | 33.02 | 790.53 | **733.18** | 757.76 | 765.95 |
| push p999 ms | 54.02 | 831.49 | **831.49** | 1138.69 | 831.49 |
| ph1 pushed/popped/lag | — | 220490/220490/0 | **220491/220490/1** | 220476/220474/2 | 220487/220487/0 |
| drained? | — | yes | **yes (ph2 lag 15)** | yes (135) | yes (26) |
| pops/s ≈ acks/s | — | ~2940 | **~2940** | ~2940 | ~2940 |
| push_h_await p50/p99 | — | — | **134/1073** | 134/1073 | 134/1073 |
| push_h_submit p50/p99 | — | — | **0.001/537** | 0.001/537 | 0.001/537 |
| arrival→proposed p50/p99 | — | 8.39*/1073 | **67.1/1073** | 134/1073 | 134/1073 |
| proposed→committed p50/p99 | — | ~1/8 | **1.05/4.19** | 1.05/4.19 | 1.05/4.19 |
| log_fsync p50/p99 | — | ~1 | **1.05/4.19** | 1.05/4.19 | 1.05/4.19 |
| apply_entry p50/p99 | — | 0.065 | **0.066/1.05** | 0.066/1.05 | 0.066/2.10 |
| durable_point p50/p99 | — | 67.1/134 | **67.1/1073** | 67.1/67.1 | 33.6/67.1 |
| pop_wildcard plan p50/p99 | — | 0.033/1.05 | **0.033/1.05** | 0.033/1.05 | 0.033/1.05 |
| pop_wildcard planned / goload empty | — | — | **97186 / 158786** | 105878 / 78466 | 97430 / 149044 |
| entries (75 s) / appends | — | — | **55208 / 202402** | 54430 / 202399 | 55593 / 202402 |
| CPU cores / RSS MB | 0.91/82 | 1.50/474 | **1.68/504** | 1.45/517 | 1.70/510 |

\* **Round-3 `arrival→proposed` p50 8.39 ms was a measurement artifact**: round-3 restamped
`received_at` on each defer, so the histogram saw only the wait after the last defer. PERF-J's
unconditional fix stops restamping, so round-4's **67 ms** is the true whole-command wait —
**not a regression**; the underlying push p50 fell 212→157. The `durable_point` p99 1073 ms in
the default column is one of 75 async samples catching the 1 s tail (min-FP/min-PP read
67 ms); not load-bearing. C1000 "fully drains" at 75 s is a **window property** (the PERF-2
baseline and every ablation also drain at 75 s), not attributed to round-4 code.

## (3) FAT100 — push-only, push-batch 100, rate 300 k (knee 287 363)

| metric | pg M1 | round 3 (PERF-7) | **round 4** |
|---|---|---|---|
| rate msg/s | 287363 | 260498 | **271130** |
| % of knee | 100 % | 90.7 % | **94.3 %** |
| pushed / shed (60 s) | — | — | **16 267 800 / 573 700** |
| durable_point p50/p99 | — | 67.1/111 | **67.1/134** |
| durable_seg_fsync p50/p99 | — | 33.6/67.1 | **33.6/33.6** |
| apply_entry p50/p99 | — | — | **2.10/8.39** |
| CPU cores / RSS MB | 1.38/255 | 2.64/2977 | **2.63/2982** |

**"% of knee" is a loose framing (Round-4 review correction §6):** raft's 271 130 msg/s is
achieved while **shedding 573 700 (3.3 %)** — over its own knee — whereas the 287 363 postgres
figure is a **sustained shed-0 rate** (calibration held 300 k/s at shed 0 for the full 60 s;
`test/raft/vm/baseline/RESULTS.md`). Report it as raft-achieved-under-shed vs
postgres-sustained; the 0.94 ratio still clears ≥ 250 k and ≥ 70 %.

## (4) O14 ratios and the proceed gate

| target | round 4 | pg M1 | O14 ratio | proceed gate | verdict |
|---|---|---|---|---|---|
| A20k p50 | 5.54 | 9.15 | **0.61×** (≤ pg) | ≤ 4.0 ms | **FAIL** (5.54) |
| A20k p99 | 15.04 | 27.01 | **0.56×** (≤ pg) | ≤ 27 ms | **PASS** |
| A20k drained | −517k | — | — | drained | **PASS** |
| C1000 p50 | 156.67 | 6.62 | **23.7×** | ≤ 13 ms | **FAIL** |
| C1000 p99 | 733.18 | 33.02 | **22.2×** | ≤ 66 ms | **FAIL** |
| C1000 drained | lag 1 | — | — | drained | **PASS** |
| FAT100 tput | 271,130 | 287,363 | **94.3 %** | ≥ 250k | **PASS** |

`proceed=false`. **O14-table caveats (Round-4 review correction):** (a) the A20k "drained
PASS" is a phase-2 catch-up, not steady-state — A20k under-consumes at ~10.5 k pop/s vs 20 k
push, so its strict-O14 0.61×/0.56× is under a lighter effective load than the postgres M1
baseline and is **not like-for-like** (§1/§2); it misses the aggressive 4 ms p50 gate
regardless. (b) The C1000 23.7×/22.2× is against the **contractual** M1 6.62/33.02; against the
**same-load** control (PERF-9 E6, 64-consumer postgres on this box, 15.04 ms) the p50 ratio is
≈**10.4×** (§5) — both fail. (c) FAT's 0.94 is raft-achieved-under-3.3%-shed over
postgres-sustained, not "% of knee" (§6); it still clears ≥250 k and ≥70 %. C1000 fails the
gate and O14 on the same upstream bound as round 3, now measured honestly. FAT clears both.

## (5) What still bounds each failing target (from the stage histograms)

- **A20k p50 (fails 4.0 ms) — the batcher group-commit cycle.** Every store-side stage is
  cheap and flat across all three columns: `proposed→committed` 2.1 ms, `log_fsync` 1.05 ms,
  `apply_entry` 66 µs, `durable_point` 16.8 ms, `push_h_submit` ~1 µs. The floor is
  `arrival→proposed` **p50 ~2.1 ms** (the plan-cycle / freed-pipeline-slot wait) folded into
  the commit round-trip → `push_h_await` p50 4.19 ms → goload 5.54 ms. This is structural to
  the single-plan-cycle Drain batcher on a ~1 ms fsync, and **neither round-4 knob nor round-3
  G/H/I touches it** (min-FP/min-PP identical; PERF-7 showed PIPELINE=8 makes it worse). The
  next lever is per-partition propose parallelism / a shorter plan cycle, a code work item.
- **C1000 p50/p99 (fails 13/66 ms) — the command-queue → single serialized fsync ingress.**
  The HTTP push blocks in `push_h_submit` (**p99 537 ms**) waiting for room in the bounded
  command channel, then `arrival→proposed` **p50 67 / p99 1073 ms** to be proposed; once
  proposed the commit is 1 ms and the pop plan is 33 µs. The broker sustains only **~735
  committed entries/s** (55 k / 75 s) under 1000-partition combined push+pop, so the offered
  ~2940 push/s + pop load backs up in the queue — a **fsync-serialized ingress ceiling**, not
  a compute or claim-plan cost. The fastpath (`POP_FASTPATH_EMPTY`) buys ~26 % p50 by keeping
  empty pops off the batcher (min-FP 230 vs 157), but cannot lift the entry-rate ceiling.
  Closing this needs batched/parallel propose+fsync across partitions, a code work item
  outside a knob.

## (6) What was cut / not done

- Short runs only, one replicate per cell (A20k pop rate 3.4–11.4 k run-to-run per PERF-6;
  C1000 p50 142–230 ms run-to-run) — the min-FP 230 vs default 157 is mechanism-supported
  (pop_wildcard planned 106k vs 97k) but single-run, so read it as directional.
- The pg M1 column is WP-0.2's postgres baseline (`test/raft/vm/baseline/RESULTS.md`), carried
  forward exactly as PERF-7 used it; postgres emits no raft-stage histograms, hence the `—`.
- Single VM, co-resident loader (O13: final numbers need three VMs); histograms are
  log2-bucket (p50/p99 are bucket **upper** bounds, ± one bucket) — the A20k `arrival→proposed`
  4.2→2.1 "improvement" is one bucket and inside that granularity.
- `run-*/data` + `run-*/buffers` deleted; the small logs / `metrics-*.txt` / `campaign.out`
  and the release build kept under `/root/raft/perf8/short/`. Nothing left running; 173 G free.
