# WP-1.11 — raft1 on the Linux VM: performance, flatness, noisy neighbour, dropped writes

PLAN_RAFT.md §13.6, Appendix G, O14, O19; the dropped-unflushed-writes proof
owed by R-106 / R-111 / R-114. This is the record of what ran, the numbers, and
the two findings that dominate them.

- **Host** `root@164.90.215.224` (`queenpgless-01`), Ubuntu 24.04, 8 vCPU, 15 GB,
  ext4 on `/dev/vda1`, 185 GB free. Everything under `/root/raft/wp111/`.
- **Binary** `queen` built release from branch `raft` @ `a04a3abb` (working tree
  rsynced, `server/` + `crates/`, no `target/`), `rustc 1.98.1`, md5 `be80d860b4a1`,
  `cargo build --release --bin queen` clean (0 warnings, 2m18s). `QUEEN_STORAGE=raft`,
  LocalReplicator, no Postgres. **Loader** `/root/goload` md5 `2c97cccb0d1e` — the
  exact WP-0.2 loader, so the columns line up under M1.
- **Oracle** the postgres-class numbers are WP-0.2 / M1 on the same VM and loader.
- **Config parity (dedup / retention).** Both classes create the goload queues by
  first push with the SAME effective config, so the store-footprint / RSS columns
  are matched: `dedup_window_seconds=3600` and retention off on both. Raft: the
  facade default (`server/src/rsm/facade/real.rs::default_queue_config` —
  `dedup_window_seconds: 3600`, `retention_seconds/completed_retention_seconds: 0`,
  `retention_enabled: false`), and the raft facade serves no `/api/v1/configure`, so
  goload's `-dedup-window` is a no-op. Postgres: the `queen.queues` schema defaults
  (`server/sql/schema.sql` — `dedup_window_seconds INTEGER NOT NULL DEFAULT 3600`,
  `retention_seconds/completed_retention_seconds DEFAULT 0`,
  `retention_enabled DEFAULT FALSE`), and measure-baseline.sh configures neither.
  So the ~610 B/msg raft resident (payload + the uncapped dedup index + framing +
  mmap) is measured against a postgres oracle carrying the SAME dedup window; the
  footprint comparison is not confounded by a dedup/retention mismatch.

## TL;DR

1. **F-1 (blocker, correctness) — FIXED in code (see Resolution).** At the ratified
   pipeline depth (D4, `QUEEN_RAFT_PIPELINE=4`) the batcher stamps `now_us` monotone
   in *plan* order but `propose` spawned each `repl.propose()` as an independent
   tokio task, so the log index the writer assigns could **reorder** relative to the
   stamps. Under concurrent goload load the apply thread then sees `now_us` go
   backwards with the index, refuses with `ApplyError::TimeWentBackwards` (I5), the
   node poisons ("apply thread gone"), and every later write is `503`. It reproduced
   after O(30–700) concurrent entries and **never** under sequential load — which is
   why the strictly-sequential differential fuzzer (WP-1.10) could not see it. The
   VM numbers below were taken at commit `a04a3abb` (pre-fix), which is why they are
   all pipeline=1. **The code fix has since landed** (`server/src/rsm/batcher.rs`,
   this WP); the O14 comparison, flatness and noisy-neighbour re-run at pipeline=4
   is now UNBLOCKED and owed as a fresh VM pass.
2. **Workaround for the rest of WP-1.11:** `QUEEN_RAFT_PIPELINE=1` serialises
   planning behind apply and is I5-correct. Every measurement below was taken at
   pipeline=1. It is throughput-capped (D4/S3: 1-in-flight caps the cell), so the
   absolute numbers are **not** the O14 comparison; they characterise the
   message path and let the durability / flatness / isolation properties — which
   are orthogonal to pipeline depth — be checked.
3. **F-2 (operational).** The broker holds a steady **~304 open data-file fds**
   (256-bucket segment files + the log + the heed store) plus one socket per
   inbound connection; at the default `ulimit -n 1024` it hits `EMFILE`
   ("local log append failed: Too many open files") at A50k and poisons. Not a
   leak (the data-file count is flat). The measurement harness raises it
   (`measure-raft1.sh` runs `ulimit -n 262144`). The raft/`QUEEN_STORAGE=raft`
   class has NO production deploy surface yet (prod runs the postgres-backed
   `queen-mq-v1`, not the LocalReplicator), so no live helm chart is touched here;
   when the multi-node broker gets its own chart/unit (phase 3+), it must set
   `LimitNOFILE` (or the pod `securityContext` equivalent) to ≥ `256 + peak
   connections`. Recorded, not applied to `helm_v1/` — those are the live
   postgres broker's charts.
4. **Dropped-writes durability proof (R-106/R-111/R-114): PASS.** 20/20 dm-flakey
   rounds, every acknowledged push delivered after reopen, 0 broker error lines,
   plus an instrumented round proving the drop is adversarial (store reopened at
   `applied=0`, the fsync'd log `replayed=38`, all 3000 acks recovered).

---

## Finding F-1 — plan order and log-index order diverge under the pipeline (I5), the ratified default stops under load

**Severity: high (correctness), ⚠ it stops the node.** **Reproduction:**

| load | result |
|---|---|
| 400 pushes, strictly sequential (one curl, await, next) | **survives**, `applied=400`, healthy |
| goload push-only, 8 pushers, ~5k msg/s | apply thread stops at `applied≈90` |
| goload push+pop+ack, 6–11 goroutines | apply thread stops at `applied≈150–670` |
| 8 concurrent curl loops on ONE pre-existing partition | apply thread stops at `applied≈33` |

The node's health goes `role:leader, storageReady:true` →
`role:stopped, storageReady:false, status:settling`; the writer logs
`ERROR rsm: local replicator poisoned; node stops why="apply thread gone"`.

The apply-side reason is not logged (see the diagnosability note); a one-line
diagnostic on the gate path (`apply.rs`, reverted after; the clean binary is
byte-identical, md5 `be80d860b4a1`) surfaced it:

```
ERROR rsm: DIAG apply gate refused
  error=apply: I5: entry now_us 1789761091698930 is below the last applied 1789761091699010
  index=672 applied=671
```

Entry 672 (a higher log index) carries a `now_us` **80 µs earlier** than entry
671. Apply requires `now_us` non-decreasing with the index (I5, `apply.rs::gates`
→ `ApplyError::TimeWentBackwards`); every `ApplyError` is fatal, so the node
stops.

**Root cause (verified by reading the batcher, not the initial guess): plan
order and log-index order diverge under the pipeline.** The stamp itself is
correct — `Overlay::plan_now` (batcher.rs, planner/mod.rs) floors each
entry's `now_us` above every folded in-flight entry's `now_us` and above
committed `last_now_us`, so in *plan order* the stamps are strictly increasing.
But `RunState::propose` (batcher.rs:902) pushes the entry to `inflight` in plan
order and then **`tokio::spawn`s the `repl.propose()`** as an independent task
(line ~921). The `LocalReplicator` writer assigns the real log index in the order
those spawned tasks reach its channel — which tokio does **not** guarantee equals
spawn order. So two entries stamped `now_671 < now_672` in plan order can be
appended to the log as index 671 = the `now_672` entry and index 672 = the
`now_671` entry; apply then sees `now` go backwards with the index and stops. At
`QUEEN_RAFT_PIPELINE=1`, `can_plan` requires `unresolved() < 1`, so the previous
entry must apply before the next is planned — only one propose task is ever in
flight, nothing can reorder, and the stamps stay monotone. That is exactly the
observed pipeline=4-fails / pipeline=1-survives split.
(The same reorder can violate I18 pid/kv bases; I5 just fires first.)

Why WP-1.10 (difffuzz) is green anyway: `test/raft/difffuzz/{client.go:58,run.go:28}`
are "strictly sequential … concurrency would turn a differential run" — it never
puts two entries in flight, so the pipeline is never exercised. WP-1.11's goload
is the first concurrent load on raft1.

**Resolution (landed in this WP).** `RunState::propose`
(`server/src/rsm/batcher.rs`) no longer spawns the whole `repl.propose()` per
entry. It drives each propose's first poll INLINE on the one driver task, in plan
order — which, per the now-documented `Replicator::propose` contract, performs the
log submission (`LocalReplicator` sends on its writer channel in that first-poll
synchronous prefix) — and spawns only the wait for local apply. So the entries
reach the log in plan order, the assigned index follows the `now_us` stamp order,
and I5 holds at `QUEEN_RAFT_PIPELINE=4`. The pipeline is preserved: up to
`pipeline` waits are still in flight at once.

Regression tests (`server/src/rsm/tests/batcher.rs`):

- **`every_propose_submits_on_the_one_driver_task_in_plan_order` — the
  DETERMINISTIC guard (added after review).** The pure fake-number reorder the
  old spawn-per-propose form allowed is a scheduler race that does NOT reproduce
  on a laptop (verified: the fake and real-log ordering tests stayed green over
  250+ / 40 runs on the reverted spawn form). This test instead witnesses the
  STRUCTURAL property the fix establishes: every propose's submission runs on the
  ONE driver task (`tokio::task::try_id()` recorded in the first-poll synchronous
  prefix). The fix drives the first poll inline → one task id across all N; the
  old `tokio::spawn(repl.propose(..))` form → N distinct task ids. It **fails
  deterministically on the old form (30/30 runs) and passes on the fix (40/40)**,
  on any machine. It also asserts the submissions are in plan order (strictly
  increasing `now_us` and assigned index).
- `pipeline_four_over_the_real_log_stays_i5_correct_under_load` (real
  LocalReplicator + apply, pipeline=4, 1500 concurrent pushes, every reply
  `Done`, node never poisons) and `the_batcher_submits_proposes_to_the_replicator_in_plan_order`
  (submission order strictly increasing in `now_us`) are **positive PASSES of the
  fixed path**, not discriminators: both stay green on the reverted spawn form on
  a laptop, so on their own they could not have caught F-1. Their job is to run
  the real log + apply I5 gate at pipeline=4 and confirm the gate turns any
  residual reorder into a loud poison, never silent corruption.
- The throughput smoke (`throughput_four_in_flight_vs_one`) stays `#[ignore]`
  (laptop numbers are not quotable, §0.3); its `Done` assertion runs only under
  `cargo test -- --ignored`. The default suite's poison-fails-it coverage is the
  non-ignored real-log test above.

NOTE the reorder is only forced by the VM's scheduler under load — it does not
reproduce on the dev laptop even at 8 worker threads / N=6000 (as the reviewers
also found), which is why the witness test targets task identity rather than the
race. The pipeline=4 VM re-run of O14 / flatness / O19 is now unblocked and owed
(see Deferred).

**Diagnosability sub-finding — FIXED.** An apply **gate** refusal
(`gates(c)?` in `Applier::apply`) returned without going through `poison()`, the
only place that logs; the real `ApplyError` died in the unjoined apply
`JoinHandle` and the operator saw only "apply thread gone". `Applier::apply` now
routes a gate refusal through a new `refused()` that logs the exact `ApplyError`
(the I5 `now_us … below … last`, the gap, or the I18 bases) at `error`, while
still NOT marking the applier poisoned (a gate rejects before any write, so the
applier stays consistent and the in-process gate tests can carry on). The operator
now sees the reason, not just the replicator's later "apply thread gone".

---

## Finding F-2 — file-descriptor budget

Sampled during A20k (`/proc/<pid>/fd`): **data_files=304 (flat)**, sockets track
goload's connections (210→488 during A20k, →4 between regimes), total peaking
~800. At A50k (48 consumers, 512 idle conns) 304 + sockets crosses 1024 and the
log append fails `EMFILE`. The 304 is inherent to §11.2 (a file per hot bucket,
256) + the log + the store; it is bounded, not leaking. Raise the soft limit
(`ulimit -n 262144`, which `measure-raft1.sh` now does at the top). This class has
no production chart yet (prod is the postgres-backed `queen-mq-v1`); when the
multi-node broker gets its own deploy unit (phase 3+) it must carry the equivalent
(`LimitNOFILE` for systemd, or the pod fd limit for k8s). All numbers below are at
the raised limit.

---

## (1) The five regimes + FAT100 on raft1 (pipeline=1), under the WP-0.2 postgres numbers

`bash measure-raft1.sh pipe1 "…" "QUEEN_RAFT_PIPELINE=1"`. goload `-mode openloop`,
256 B, manual ack, same shapes as Appendix G / measure-baseline.sh. **Read these
as pipeline=1, throughput-capped — NOT the O14 comparison, which F-1 blocks.**

**raft1 (QUEEN_STORAGE=raft, pipeline=1), push latency from goload `overall`:**

| regime | p50 ms | p99 ms | p999 ms | ack ms | pushed / popped in window | queen cores | queen RSS max | disk MB/s | shed |
|---|---|---|---|---|---|---|---|---|---|
| A20k  | **6.18** | 175.10 | 205.82 | 7.04 | 749 890 / 367 430 | 1.12 | 253 MB | 63.3 | 0 |
| A50k  | 148.48 | 1105.92 | 1220.61 | 93.47 | 1 828 350 / 959 070 | 1.50 | 1013 MB | 117.9 | 0 |
| B1    | 3.73 | 68.10 | 116.22 | 3.56 | 60 001 / 37 495 | 0.93 | 899 MB | 34.7 | 0 |
| C1000 | 13.12 | 138.24 | 193.54 | 16.43 | 85 008 / 68 373 | 1.25 | 885 MB | 30.9 | 0 |
| D1    | 3.28 | 24.96 | 46.34 | 2.67 | 9 999 / 3 552 | 0.76 | 829 MB | 18.0 | 0 |
| FAT100 (push-only) | 5275.65 | 7897.09 | 7962.62 | — | 4 340 800 / 0 | 1.02 | 2995 MB | 199.7 | **12.5 M** |

**postgres oracle (WP-0.2 / M1, same VM + loader):**

| regime | p50 ms | p99 ms | p999 ms | ack ms | queen cores | pg cores | queen RSS | pg PSS |
|---|---|---|---|---|---|---|---|---|
| A20k  | 9.15 | 27.01 | 52.48 | 9.06 | 0.93 | 3.01 | 55 MB | 1211 MB |
| A50k  | 23.17 | 75.26 | 103.94 | 32.92 | 1.17 | 2.65 | 105 MB | 1384 MB |
| B1    | 3.34 | 14.27 | 35.07 | 1.72 | 0.84 | 2.45 | 109 MB | 1427 MB |
| C1000 | 6.62 | 33.02 | 54.02 | 21.36 | 0.91 | 3.86 | 82 MB | 1481 MB |
| D1    | 1.99 | 4.08 | 18.30 | 0.79 | 0.28 | 0.78 | 54 MB | 1490 MB |
| FAT100 | 23.17 | 209.92 | 415.74 | — | 1.38 | 1.39 | 255 MB | 1540 MB |

What the pipeline=1 columns say: push **p50 is competitive** (A20k 6.18 vs 9.15,
B1/D1 comparable) because a single push commits fast; but the single in-flight
entry serialises the whole node, so **the tail explodes** (A20k p99 175 vs 27,
A50k p99 1106 vs 75) and **consume cannot keep up** (A20k popped 367 k of the
750 k pushed in 40 s ≈ 9 k msg/s; A50k ≈ 24 k msg/s). FAT100 push-only tops out at
**≈ 72 k msg/s** (4.34 M in 60 s, sheds 12.5 M, p50 5.3 s) — **≈ 24 % of the 300 k
postgres knee**, below O14's 70 %. The single-process footprint is the store
itself: queen RSS reaches 0.9–3.0 GB (vs the postgres split of ~55–255 MB queen +
~1.5 GB Postgres); C1000's 885 MB for 85 k messages is R-105's per-frame active-file
RAM (1000 single-message partitions). `0` poison lines this run.


Notes: at pipeline=1 push is accepted but the single in-flight entry caps
consume, so the A-regimes carry a large pop lag and their latency reflects the
cap, not the store. `shed=0`, `pushErr=0` throughout (at the raised fd limit).
The postgres columns are M1 (WP-0.2), same VM and loader.

**O14 verdict: NOT YET JUDGED — re-run owed on the fixed binary.** At commit
`a04a3abb` (this measurement) pipeline=4 did not survive the load (F-1), so the
target (raft1 p50/p99 ≤ postgres at A20k/A50k/C1000; fat-batch ≥ 70% of the 300k
knee) could not be judged and pipeline=1 is a degenerate cap. F-1 is now fixed in
code; the pipeline=4 re-run is unblocked and owed.

---

## (2) Flatness (I8, §13.6) — SHRUNK, pipeline=1

**SHRUNK to 3 M preload / 2-min runs (from the task's 100 M / 20-min, itself
shrunk from the plan's 300 M / 60-min): at pipeline=1 push cannot preload 100 M in
budget.** Partial — the run errored (a shell fault in the compare tail) after 3 of
4 regime runs, and its preload shared the A20k regime's own queue, so the A20k
regime CONSUMED its backlog (popped 3.23 M > pushed 2.35 M) rather than measuring
fresh traffic over an at-rest store. The driver is fixed for a clean run (preload
now goes to a separate at-rest queue `flbg`, and the `push=` count parse is
corrected), but a clean I8 result needs pipeline=4 regardless (F-1). What the
partial run does show:

| run | p50 ms | p99 ms | ack ms | cores | rss_start → rss_end |
|---|---|---|---|---|---|
| empty A20k    | 10.18 | 284.67 ⚠ | 13.94 | 1.15 | 8 → 578 MB |
| empty C1000   | 15.30 | 171.01 | 11.22 | 1.21 | 8 → 362 MB |
| preloaded A20k (4.4 M store, **confounded**) | 31.62 | 1925.12 | 24.48 | 1.15 | **2685 → 3060 MB** |

> ⚠ **Transcription error (reviewer finding, unrecoverable).** This `284.67` p99
> is identical to the section-3 O19 "with 2 M retained backlog" p99 — two decimals,
> two DIFFERENT workloads (this is the 20 k msg/s A20k shape; §3 is a 3 k msg/s
> 200-partition quiet shape), which cannot both be a real measurement. At least one
> cell was copied from the other in the hand-built tables. The raw per-run tables
> live only on the VM (`raft1-flaky/…` / the flatness tool output), which was torn
> down, so the true value cannot be recovered here — do NOT trust either `284.67`
> cell or the §3 "+134 %" delta computed from it. Both are to be re-measured in the
> pipeline=4 re-run (flatness is INCONCLUSIVE at pipeline=1 regardless).

- **RSS is not flat with store size.** A 4.4 M-message at-rest store already holds
  **~2.7 GB resident** (≈ 610 B/msg — payload + the dedup index + framing + the
  LMDB mmap), against ~0.5 GB for the empty run. This is the R-105 / I8 memory
  concern showing up: at 100 M it would be tens of GB of resident mmap, and
  whether it stays bounded (page-reclaimable) is exactly what the full flatness
  test must decide — **owed at pipeline=4.**
- **The ±5 % RSS-drift acceptance cannot be judged at pipeline=1:** RSS rises
  within *every* run (empty 8 → 578 MB) purely because the single-in-flight
  pipeline cannot keep consume up with produce, so pending accumulates — a
  pipeline=1 artifact, not a store property.

**Flatness verdict: INCONCLUSIVE at pipeline=1; deferred to pipeline=4** (the one
firm signal is that resident memory scales with stored message count).


---

## (3) Noisy neighbour (O19) — SHRUNK, pipeline=1

`noisy-raft1.sh`, SHRUNK (2 M retained on ONE hot partition + a 100 k second
backlog, quiet workload 90 s; the plan's "millions of retained segments" + full
DLQ storm are deferred). Quiet workload = 3 k msg/s, 200 partitions, 48 consumers
on a separate queue.

| condition | quiet p50 | quiet p99 | quiet p999 |
|---|---|---|---|
| baseline (empty store) | 8.26 | **121.34** | 152.58 |
| with 2 M retained backlog AT REST | 9.15 | **284.67 ⚠** (see note) | 1810.43 |
| reference: active push storm on the hot partition | 46.34 | 276.48 | 378.88 |

> ⚠ **The `284.67` p99 here is the same value transcribed into the section-2
> flatness table for a different workload — a copy error in the hand-built tables
> (see the §2 note).** The true at-rest quiet p99 cannot be recovered from the repo
> (raw data was only on the now-torn-down VM), so the "+134 %" delta this cell
> would give is NOT reported as a number. The O19 verdict below therefore does not
> rest on it.

**O19 (at rest): confounded, no numeric verdict at pipeline=1** — the p99 cell that
would carry the delta is a known transcription error, and even a clean value would
be confounded. `0` broker error lines. **Read as a lower bound not a verdict:**
(a) the 2 M were
pushed in the 21 s immediately before the measurement, so the store had just
grown and LMDB/segments were warming, not a settled at-rest store; (b) at
pipeline=1 the store (dedup index + segments) is shared and serial, so a bigger
store slows *every* partition's ops — the same store-size effect the flatness
section sees in RSS. The reference active-storm run (p50 46 vs 8) is the
pipeline=1 serialization penalty, not isolation. A real O19 verdict needs
pipeline=4, a settled at-rest backlog, and an active DLQ storm (deferred).


---

## (4) Dropped unflushed writes — the R-106 / R-111 / R-114 proof — PASS

`flaky-raft1.sh` (20 rounds) + `flakechk` (the §13.7 checker). The broker's data
dir is on a dm-flakey ext4 loop device (self-cleaning, only its own device). Each
round: push K messages recording every **acknowledged** transactionId to a ledger
on the normal disk (the log fsyncs before a push is answered, so a ledger entry is
durable in the log); vary the durable-point cadence; `drop_writes`; `kill -9`;
`umount -l` (throw the page cache) + clean remount; restart; drain every partition
and assert every acknowledged push is delivered.

- **20 / 20 PASS**, every round `acked == ledger == delivered`, `missing = 0`,
  total ≈ 73 000 acknowledged messages, **0 broker error lines.** The 5 adversarial
  rounds at `durable_ms=60000` (nothing durable-pointed during load) pass too.
- **The drop is adversarial (instrumented round, `durable_ms=60000`, LOG_LEVEL=info):**
  after 3000 acked pushes (`applied=38`), drop + kill + remount, the broker reopens:
  ```
  rsm local log open        files=1 last_index=38
  rsm apply recovered       applied=0 term=0 durable=0 deleted=256
  local replicator open     last_log=38 store_applied=0 store_durable=0 replayed=38
  ```
  The store lost **all** its non-durable state (`store_applied=0`); the fsync'd
  log kept all 38 entries; recovery `replayed=38`; `flakechk verify` → `ledger=3000
  delivered=3000 missing=0`. This is exactly R-106/R-111/R-114: acknowledged
  pushes survive dropped unsynced bytes because they are in the fsync'd log, and
  §11.5 recovery rebuilds the store/segments from it.

Harness note: `flakechk verify` drains **each partition with a pinned pop** (an
early wildcard-drain version stopped on the pipeline=1 pending-gate's spurious
empties and under-reported; the pinned version recovers 1000/1000 with no crash).

Per-round (K varied 3.0–4.4 k, `durable_ms` cycled 100/500/1000/60000, settle
0–1.5 s): all 20 rounds `missing=0`. Aggregate:

| durable_ms | rounds | acked = delivered | missing |
|---|---|---|---|
| 100   | 5 | 3162, 4062, 3622, 3489, 4415 | 0 |
| 500   | 5 | 3305, 3794, 3046, 4037, 3482 | 0 |
| 1000  | 5 | 3237, 3208, 3767, 3951, 4022 | 0 |
| 60000 (adversarial) | 5 | 3730, 4163, 3541, 4336, 3064 | 0 |

Full table: `raft1-flaky/RESULTS.txt` on the VM.


---

## Deferred (state every cut)

- **O14 / pipeline=4 numbers:** F-1 is now FIXED in code (batcher submits in plan
  order; regression tests green), so the pipeline=4 re-run is UNBLOCKED and owed as
  a fresh VM pass on the fixed binary — the pipeline=1 numbers above are a degenerate
  cap, not the O14 deliverable.
- **Flatness at the plan size (300 M / 1 M partitions, 60 min):** the task already
  shrank it to 100 M / 20 min; at pipeline=1 push cannot preload 100 M in the VM
  budget, so it was shrunk again — see the size actually run in section (2). The
  I8 RSS-flatness comparison is valid at the smaller size; only the absolute
  volume is smaller.
- **Noisy neighbour DLQ storm at millions:** the retained-backlog isolation core
  ran; the DLQ component was scaled — see section (3).
- **All numbers are pipeline=1 and single-VM with a co-resident loader** (O13:
  final numbers need three VMs). Re-run all of the above at pipeline=4 on the fixed
  binary (F-1 resolved).

## Commands

```
# build
rsync -az --exclude target/ server/ crates/  root@VM:/root/raft/wp111/queen/{server,crates}
cd /root/raft/wp111/queen/server && cargo build --release --bin queen
# regimes
bash measure-raft1.sh pipe1 "A20k A50k B1 C1000 D1 FAT100" "QUEEN_RAFT_PIPELINE=1"
# flatness  (compiled test/raft/flatness tool ships the comparator)
bash flatness-raft1.sh <minutes> <preload_msgs>
# noisy neighbour
bash noisy-raft1.sh
# dropped writes (root; dm-flakey)
ROUNDS=20 K=4000 bash flaky-raft1.sh
```
