# PERF-6 — the consume A/B: what makes A20k's pop rate low (TASK AB-0)

Phase-1 performance package, round 3 (2026-09-19). AB-0 **settles the consume
regression before anyone fixes it.** PERF-4 left it open: "new-b1 popped 218 480
(~3.6 k/s) vs old 551 990 (~9.2 k/s) in the same push window … whether that is
warm-up or a `DEDUP_INDEX=txns` regression is NOT settled." This is the owed
steady-state A/B — **PENDING_TRANSITIONS × DEDUP_INDEX, a 2×2, everything else the
round-2 best config (BUCKETS=16), 60 s A20k push+consume, then a drain — run
twice** to price the run-to-run noise. No product code changed; AB-0 is a
measurement/diagnosis task and sets only the two knobs.

## Bottom line

**The low A20k pop rate is not caused by either knob. It is the ordinary
under-consume shape of A20k — a run-to-run-variable split of the single apply
thread between push and pop — and there is no ring/lease pathology.** Four facts
settle it, two replicates each:

1. **Run-to-run variance swamps any knob effect.** The *identical* config
   `PT=0,rows` popped **195 500 (3.4 k/s)** in replicate 1 and **655 450
   (11.4 k/s)** in replicate 2 — a **3.35×** swing with nothing changed. That is
   larger than any gap between configs.
2. **The 2×2 ordering is incoherent and reshuffles between replicates.** R1:
   pt1-rows 7.8 k > pt0-txns 7.0 k > pt1-txns 5.6 k > pt0-rows 3.4 k. R2: pt0-rows
   11.4 k > pt1-rows 7.2 k > pt1-txns 6.8 k > pt0-txns 6.2 k. PENDING_TRANSITIONS
   *hurts* txns in R1 (7.0→5.6) but *helps* it in R2 (6.2→6.8); it *helps* rows in
   R1 (3.4→7.8) but *hurts* it in R2 (11.4→7.2). A real mechanism cannot flip sign
   run to run.
3. **The stage histograms are byte-identical where the pop rates differ most.**
   Across all four cells and both replicates `pop_read` p99 is **262 µs**, `plan`
   p99 **2.10 ms**, and the two rows cells share **durable_point 134 / 268 ms**
   while popping 195 k and 448 k (R1) / 655 k and 414 k (R2). A 2–3× pop gap with
   **zero** measured per-op cost difference is scheduling, not knob cost.
   PENDING_TRANSITIONS moves **nothing** measurable (pt0-txns ≡ pt1-txns
   histograms).
4. **Every one of the 8 runs fully drains once push load is lifted.** Phase 2
   (push cut to 100/s) pops at **~11–12 k/s** — 1.5–3.5× the phase-1 rate — and
   clears the *entire* backlog: phase-2 popped = phase-1 lag + ~6 000 (the new
   pushes) in **all eight** runs, plateauing in ~5–10 s. Empty polls are **flat**
   through phase 1 (~2.6–3.5 k/s, no growth) and lag grows **perfectly linearly**.
   Nothing is stuck; the ring re-arms correctly.

**PERF-4's attribution is refuted.** DEDUP_INDEX=txns does not hurt consume: the
pop path (`segs_from`/`hashes_in_range`) reads the **Txns** keyspace regardless of
the knob (`pop_read` p99 262 µs, identical everywhere), no txns-window dedup scan
runs on the pop path, and there is no stuck ring. PERF-4's new-b1 3.6 k/s sits
inside this noise band (the eight runs here span 3.4→11.4 k/s; one 60 s run's pop
ceiling is not reproducible to better than ~3×). The one **consistent** knob
effect is on **push, not consume**: DEDUP_INDEX=rows carries push p99 **167–183 ms**
and durable_point **134 / 268 ms** (the per-message dedup RMW that loads the LMDB
env-sync — the exact wall PERF-E removed) with RSS ~**350–382 MB**, versus txns
**18–20 ms / 16.8 / 33.6 ms** and RSS ~**165–171 MB**. That is the PERF-E win,
orthogonal to the consume question.

## Provenance

- **Binary** `queen` release, VM-built from an rsync of `server/` + `crates/`
  (no `target/`) at branch `raft` **HEAD `7e5f0d61`** (committed round-2 tip:
  report `7e5f0d61`, PERF-E `431171e4`, PERF-F `e669dcc2`, PERF-D `1d017f7d`).
  `cargo build --release --bin queen` exit 0 in 2m52s, `rustc 1.98.1`.
  **md5-12 `af5fa8dc3608`**.
- **Loader** `/root/goload` md5 `2c97cccb0d1e` — the exact M1/PERF-2/3/4 loader.
- **VM** `root@164.90.215.224`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4 `/dev/vda1`.
  `ulimit -n 262144`, `QUEEN_STORAGE=raft`, `QUEEN_RAFT_PIPELINE=4`,
  `QUEEN_RAFT_METRICS=1`, LocalReplicator, no Postgres. Round-2 best config held
  fixed on every run: `BATCH_COUNTERS=1 DEDUP_FRONT=1 DURABLE_ASYNC=1
  SEG_BUFFERED=1 BUCKETS=16` (writers adaptive). A/B knobs:
  `QUEEN_RAFT_PENDING_TRANSITIONS ∈ {0,1}` × `QUEEN_RAFT_DEDUP_INDEX ∈ {txns,rows}`.
- **Host clock** 2026-09-19 ~16:06–16:12 UTC (replicate 1), ~16:2x–16:3x
  (replicate 2). Work under `/root/raft/perf5/`. Each run booted a FRESH broker +
  fresh data dir and deleted the data dir after; **0 broker error/panic lines in
  every run**. Driver `perf5run.sh` (fresh broker, one config): phase 1 = 60 s
  A20k push+consume (`measure-raft1.sh` A20k goload flags, 32 consumers,
  `-duration 60 -ramp-sec 5 -report 5`); phase 2 = a drain — openloop refuses
  rate 0, so push is cut to **100/s** while the 32 closed-loop consumers clear the
  phase-1 backlog for up to 60 s. `timing.sh` scraped `/metrics/prometheus` across
  phase 1; the end-of-push snapshot is the histogram source. VM left clean: no
  queen/goload procs, no loop/dm, every store data dir deleted (small run logs
  kept under `run-<cfg>/`, `run-r2-<cfg>/`).

## The 2×2 (A20k, 60 s push+consume, buckets=16), two replicates

goload `overall` push latency; pop/s = the flat per-5 s-window rate; empty/ack from
the `[final]` line; stage histograms from the end-of-push `/metrics` snapshot
(log2-bucket, read "≤"). Drain = phase-2 popped vs phase-1 lag.

| metric | pt0-txns | pt1-txns | pt0-rows | pt1-rows |
|---|---|---|---|---|
| PENDING_TRANSITIONS / DEDUP_INDEX | 0 / txns | 1 / txns | 0 / rows | 1 / rows |
| **R1 popped / pop-s (flat)** | 402,470 / **7,000** | 321,950 / **5,600** | 195,500 / **3,400** | 448,500 / **7,800** |
| **R2 popped / pop-s (flat)** | 356,490 / **6,200** | 391,000 / **6,800** | 655,450 / **11,400** | 413,990 / **7,200** |
| R1 / R2 empty polls | 207,634 / 203,925 | 210,331 / 206,927 | 193,609 / 182,949 | 180,494 / 179,322 |
| R1 push p50 / p99 ms | 6.62 / 18.30 | 6.69 / 18.82 | 7.33 / **183.30** | 7.39 / **166.91** |
| R2 push p50 / p99 ms | 6.75 / 20.10 | 6.75 / 20.10 | 6.50 / **179.20** | 7.58 / **183.30** |
| ackAvg ms (R1) | 6.24 | 6.40 | 7.42 | 7.43 |
| plan p99 ms (both) | 2.10 | 2.10 | 2.10 | 2.10 |
| pop_read p99 ms (both) | 0.262 | 0.262 | 0.262 | 0.262 |
| arrival→proposed p99 ms | 16.8 | 16.8 | **134.2** | **134.2** |
| durable_point p50 / p99 ms | 16.8 / 33.6 | 16.8 / 33.6 | **134.2 / 268.4** | **134.2 / 268.4** |
| CPU cores (R1 / R2) | 1.19 / 1.15 | 1.18 / 1.23 | 1.16 / 1.19 | 1.16 / 1.15 |
| RSS max MB (R1 / R2) | 166 / 168 | 165 / 171 | **349 / 364** | **341 / 382** |
| R1 drain: phase-2 popped (lag) | 753,500 (747,320) | 834,080 (827,950) | 960,510 (954,360) | 707,530 (701,440) |
| R2 drain: phase-2 popped (lag) | 799,510 (793,470) | 765,030 (758,970) | 500,570 (494,510) | 742,020 (735,890) |
| drained fully? | **yes** (both) | **yes** (both) | **yes** (both) | **yes** (both) |

Per-config pop/s across the two replicates: pt0-txns 7.0 k / 6.2 k; pt1-txns
5.6 k / 6.8 k; pt0-rows **3.4 k / 11.4 k**; pt1-rows 7.8 k / 7.2 k. The
within-config spread (pt0-rows 3.35×) is bigger than any between-config gap.

**One consistent knob signature is on the pop side — the empty polls.** Unlike
the noisy pop rate, empty polls separate cleanly by **DEDUP_INDEX with no sign
flip**: every txns cell (R1/R2 207 634 / 203 925 / 210 331 / 206 927, mean
**207 k**) exceeds every rows cell (193 609 / 182 949 / 180 494 / 179 322, mean
**184 k**) — a ~12 % gap that holds across both replicates. So `DEDUP_INDEX=txns`
does leave a small, consistent pop-path footprint (its partitions poll empty
slightly more — consistent with the bloom-"maybe" txns-window scan being on the
claim path), which qualifies the "does not touch the pop path" wording below. It
does **not** change the verdict: pop throughput is noise-dominated, `pop_read`
p99 is byte-identical (262 µs), and every cell drains fully either way.

## The per-window shape (why "flat" matters)

Every run's pop rate is **dead flat** across all eleven 5 s windows — e.g. R2
pt0-rows was 11 386…11 414/s, R1 pt0-txns exactly 7 000/s, R1 pt0-rows 3 400/s —
with empty polls flat and lag climbing a straight line at (20 k − pop/s). There is
**no warm-up ramp and no decay**: the contention ceiling is fixed within the 5 s
ramp and holds for the run. That is what makes PERF-4's "flat-low pop with
elevated empty polls and growing lag" the **normal** A20k under-consume signature
(offered pop capacity < push 20 k/s), not a stuck ring. The drain confirms it: cut
push to 100/s and the same broker pops the whole backlog at ~11–12 k/s.

## Diagnosis (one paragraph)

**Neither knob causes the low pop rate; it is a run-to-run-variable split of the
single apply thread between push-apply and pop-apply, and the ring is not the
problem.** From the code: the apply thread (`rsm/apply.rs`) is the sole writer and
serializes every `Append` and every pop/ack `CursorSet` (I2/D7); a pop's ready
ring (`rsm/state` `Derived`/`ReadyIndex`) is a RAM mirror of the `pending`
keyspace that the planner only *reads* (I1, `candidates`→`walk`, over the `ready`
deque only), and a partition is re-armed to `ready` by an append
(`append_pending`→`set_pending`) or by an ack/lease-release (`cursor_set`, which
puts `ready_at=now` whenever backlog remains) — **not** by a timer:
`Derived::promote_due`/`tick` (PLAN_RAFT §9.5/§10.2 "deferred promotions for
pending gates") have **no production caller** in `server/src` (only tests). That
absence does not bite A20k, because manual-ack acks the whole leased batch within
~6 ms ≪ the 60 s `lease_time`, so the ack re-arms every leased partition long
before its lease could expire — which is exactly why every cell **fully drains**
and empty polls stay flat. PENDING_TRANSITIONS only gates the *append-path* re-arm
(write/push only when `new ready_at < stored pending`); it is a no-op for A20k
(identical histograms, sign-flipping pop deltas across replicates), and its one
stuck case needs a lease to *expire* while a partition is deferred — which A20k
never does. DEDUP_INDEX only changes the *push* dedup authority (rows = the
per-message `(pid,hash)` RMW → durable_point 134/268 ms, RSS 2×; txns = PERF-E's
segment-native authority → 16.8/33.6 ms) and does not measurably change the pop
path's per-op cost, which reads the Txns keyspace either way (`pop_read` p99
262 µs, `segs_from`/`hashes_in_range`) — though the txns cells poll empty ~12 %
more (mean 207 k vs 184 k, no sign flip), a small consistent signature that
leaves the drain/throughput verdict intact. The clincher that the pop-rate spread is scheduling, not cost:
pt0-rows and pt1-rows have byte-identical stage histograms yet popped 195 k vs
448 k (R1). So the lever for A20k consume throughput is the **batcher/apply
push-vs-pop interleaving** (Alice's item 1 — `arrival→proposed` is a ~4 ms hold
both paths pay), not the pending-row knob or the dedup index; the correct
correction to PERF-4 is that its 3.6 k/s was inside the noise, `DEDUP_INDEX=txns`
does **not** regress consume (it *helps* push), and there is **no consume
regression in the ring to fix**.

## What was cut / not done

- Short 60 s runs only, ×2 replicates (Alice: iterate; reproduce to be sure). No
  6-min flatness, no cold-cache pass, no perf profile (AB-0 is the consume A/B).
- The drain uses a 100/s trickle (openloop refuses rate 0); the 32 consumers are
  closed-loop drainers, so the trickle only keeps the loop alive. The last ~32
  partitions leased by phase-1's dying consumers drain after their 60 s leases
  expire (small late pop bursts) — reclaimed lazily at claim time (a `lease_live`
  cursor becomes claimable), not by the unwired promote sweep — but every backlog
  cleared inside the 60 s window.
- Single VM, co-resident loader; histograms log2-bucket (± one bucket); CPU top
  not sampled here.
