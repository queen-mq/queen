# PERF-10 — the A20k median, the cycle shape, and why "continuous" is a phase-3 lever

Phase-1 performance package, **round 4 diagnosis** (2026-09-19/20). TASK PERF-K.
Instrumentation + a knobbed prototype, no default behaviour change. The question
from the round-3 finding: A20k push p50 ≈ 6 ms with every RSM stage ~1 ms; the
hypothesis was the **batcher cycle shape** — "plan until 4 entries are in
flight, the writer fsyncs them as one group, all resolve together, so a command
arriving mid-cycle waits a whole round trip".

## Bottom line — `ours=true`

The 6 ms A20k median is **real, server-side, and it is the single-log fsync
serialized under a pipeline of 4** — but the round-3 mental model is **wrong on
one load-bearing point and the proposed fix does not move the median**:

1. **The writer does NOT fsync the four as one group.** Measured
   `group_entries` p50 = **1** (trace mean 1.50/group): the batcher feeds
   entries to the log writer *arrival-paced*, one at a time, so each entry gets
   its own ~1 ms fsync. The "lockstep burst of 4 resolving together" is really
   **four separate, serialized ~1 ms fsyncs**. The median command waits
   `queue_wait` p50 **2.10 ms** for a pipeline slot (gated by the fsync of the
   entries ahead) **+** its own `proposed_to_committed` **2.10 ms** ≈ the 6 ms.
2. **`continuous` cannot help the A20k median on a single node.** On the
   phase-1 `LocalReplicator` an entry APPLIES ~65 µs after it COMMITS
   (`apply_entry` p50 65 µs; the writer fsyncs and hands straight to the local
   apply thread). "Free the propose slot on commit instead of apply" is
   therefore only ~65 µs earlier — it cannot move a 6 ms median. The whole
   premise (decouple propose-ahead from apply) needs commit to precede local
   apply by a network RTT, which only happens on a **3-voter cluster (phase 3)**.
3. Measured A/B confirms it: `continuous`+`GROUP_MAX_US=300` moves A20k push
   p50 **6.11 → 6.43 ms** (worse), p99 15.04 → 13.76 (better), and drain
   +9 % (popped 368 k → 402 k). It **regresses C1000** badly (push p50
   **374.78 → 733.18 ms**, p99 1106 → 3686) and is a **wash for FAT100**. The
   two levers only combine positively; alone each hurts (see the table).

Recommendation: **do not default `continuous` in phase 1.** Keep it (and
`GROUP_MAX_US`, `MAX_INFLIGHT`) as knobs, I2-clean, for a phase-3 A/B. The A20k
median lever is not the shape — it is getting the empty pops off the serial
propose path and a push/pop lane split (PERF-9 fixes 1 & 2), plus fsync
amortization, which `GROUP_MAX_US` only pays off once there are enough real
entries to batch.

## Round-4 review correction (2026-09-20)

The coordinator review **confirms this report's central finding** and asks only that it be
reconciled with PERF-11 on one drain definition. Package-wide definition:
*consume-matched / drained* = **phase-1 steady-state lag ≈ 0 while pushing**. By that
definition PERF-10 is correct — **A20k does NOT drain in any variant** (phase-1 lag
750 k–940 k), and the round-3 (PERF-7) "A20k consume-matched and drained (lag 0)" claim this
report flags is the **phase-2 catch-up** window property (push cut to 100/s), not steady-state
matching. PERF-11's "A20k drains (phase-2 lag −517 k)" measures that same phase-2 catch-up, so
the two reports **do not conflict once the window is named** — the labels differed, the numbers
agree. Consequence for the package: A20k's O14 push-latency numbers (this report's 6.11 ms,
PERF-11's 5.54 ms) are measured while the consume side runs at **~half the offered rate**
(pop ~10.5 k/s vs push 20 k/s), i.e. under a **lighter effective load** than the postgres M1
baseline (same 32 consumers, shed 0, consume keeping up because each request is its own pooled
txn). **A20k O14 must be read as under-consumed, not a like-for-like PASS.** The
`continuous`-shape verdict (do not default in phase 1; keep the knobs for a phase-3 A/B) is
unaffected.

## Provenance

- **Binary** `queen` release, VM-built from an rsync of `server/` + `crates/`
  at branch `raft` HEAD `d095d5d4` **plus PERF-J's uncommitted push/pop
  HTTP-boundary instrumentation and THIS task's uncommitted code**
  (`server/src/rsm/timing.rs`, `batcher.rs`, `replicator/{mod,local,fake}.rs`,
  and its tests). `cargo build --release --bin queen` exit 0, **md5-12
  `5ce3c2552539`** (an earlier build, before the trace-sink refactor below, was
  `78ecbdfdcb17`). `cargo test --lib rsm::` green (**379 passed**, incl. 3 new
  continuous-shape tests). `clippy` clean for the touched files; the I2
  `disallowed_methods` list stays green (0 errors).
- **Loader** `/root/goload` md5 `2c97cccb0d1e`, `-mode openloop` (the exact
  PERF-2..9 loader). Push latency = goload `overall` p50/p99/p999
  (coordinated-omission from each request's scheduled instant).
- **VM** `root@164.90.215.224`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4. `ulimit -n
  262144`. Round-3 default knobs (`QUEEN_RAFT_PIPELINE=4 BATCH_COUNTERS=1
  DEDUP_INDEX=txns DEDUP_FRONT=1 DURABLE_ASYNC=1 SEG_BUFFERED=1 BUCKETS=16
  DRIVER_NOTIFY=1 CLAIM_FROM_RING=1`). Host clock 2026-09-19 21:34–22:03 UTC.
  Work under `/root/raft/perf8/perf-k/`; each run booted a FRESH broker + fresh
  data dir, killed only its own PID, deleted its data dir. `errlines=0` in every
  run. Nothing of ours left running.
- **Shapes** (measure-raft1.sh): A20k `-rate 20000 -push-batch 10 -partitions
  100 -consumers 32 -pop-batch 200 -manual-ack -payload 256 -ramp-sec 5` 60 s;
  C1000 `-rate 3000 -push-batch 1 -partitions 1000 -consumers 64 -pop-batch 50
  -manual-ack -payload 256 -ramp-sec 3` 40 s; FAT100 `-rate 300000 -push-batch
  100 -partitions 100 -consumers 0 -payload 256 -max-inflight 4096 -idle-conns
  4096` 60 s. Histograms log2-bucket — a p50 prints as the bucket CEILING
  (`0.004194303 s` = bucket [2.1, 4.19) ms → read as ~4 ms, ± one bucket).

## The trace (A20k, drain, 60 s, `QUEEN_RAFT_CYCLE_TRACE=1`)

The gated trace emits one `CYCLETRACE` line per batcher cycle (drained cmds,
uncommitted/unresolved/inflight before→after, gap since previous cycle, the wake
reason, whether an entry was proposed and its cmd count) and one `GROUPTRACE`
line per log-writer fsync group (entries, bytes, write+fsync µs, gap). It writes
through a dedicated writer thread over a bounded channel (the first cut used
`eprintln!` inline on the batcher task and throttled the very cycle it measured;
the sink fixed that). **Caveat:** even off the hot path the trace thread eats a
core, so the trace run does not drain and its absolute rates/gaps are inflated —
the **structure** is the finding; the untraced A/B histograms below are the true
rates. Analysis over the 1.0 s window t=30.0–31.0 s (2527 cycles, 733 groups):

- **wake reason: 91 % `arrival`**, 7.5 % `result`, 1.3 % `applied_notify`. The
  batcher is arrival-driven, not backlog-driven.
- **`qbefore` (queue left after each drain) = 0 for every cycle** → the batcher
  keeps up with arrivals; there is no standing queue.
- **`uncommitted_before` is 0/1/2/3, ~evenly, and NEVER 4** → the pipeline fills
  to the bound (4) and *gates* (`can_plan` blocks at `unresolved == pipeline`);
  incoming commands then wait for the next slot-free (`applied_notify`/`result`)
  wake, which drains the accumulated queue (4–32 cmds) into ONE entry.
- **56 % of cycles produce NO entry** — empty wildcard pops that return
  `Plan::Empty` yet still pay a full serial cycle (the PERF-9 signature).
- **entries per fsync group: mean 1.50** (470×1, 161×2, 101×3, 1×4). The writer
  barely coalesces. `write_fsync` per group p50 **864 µs** / p99 1956 µs;
  inter-group gap p50 1041 µs.

So: entries reach the writer one at a time (arrival-paced when the pipeline has
room), each costs ~1 fsync, and the writer serializes them on its one thread →
~1000 entries/s ceiling. A command that arrives while the pipeline is gated
waits ~one fsync-round-trip for a slot.

## The A/B (untraced — the ground truth)

All A20k stages were bucket-identical across variants: `arrival_to_proposed` p50
**4.19 ms**, `queue_wait` p50 **2.10 ms**, `proposed_to_committed` p50 **2.10
ms**, `log_fsync` p50 **1.05 ms**, `apply_entry` p50 **65 µs**, `push_h_await`
(facade propose→commit→apply→answer) p50 **8.39 ms**.

### A20k (consume-matched, 32 consumers, 60 s)

| variant | push p50 / p99 / p999 ms | popped (drain) | lag | entries/s | group_entries p50 | CPU cores |
|---|---|---|---|---|---|---|
| **drain (today)** | **6.11** / 15.04 / 22.14 | 367,980 | 781,970 | 1,075 | 1 | 1.51 |
| drain + `GROUP_MAX_US=300` | 6.30 / 14.78 / 22.14 | 333,470 | 816,490 | 998 | 3 | 1.46 |
| **continuous** (`GROUP_MAX_US=300`) | 6.43 / **13.76** / 20.86 | **402,460** | 747,440 | 1,061 | 3 | 1.46 |
| continuous + `GROUP_MAX_US=0` | 6.62 / 14.66 / 22.66 | 206,980 | 942,940 | 1,053 | 1 | 1.52 |

- The median **does not move** (6.11 → 6.43). It is set by `queue_wait` 2.10 +
  own commit 2.10, both fsync-bound; freeing on commit (≈65 µs earlier than
  apply here) cannot touch it.
- **The two levers interact.** `GROUP_MAX_US=300` alone (drain) raises
  `group_entries` 1 → 3 (fsync amortized) but drops entries/s and *hurts* drain
  — the hold adds latency with no propose-ahead to fill the bigger groups.
  `continuous` alone (`g0`) is the **worst** (drain 207 k) — freeing on commit
  spreads proposes further, so the writer forms even smaller groups. Only
  **combined** does A20k drain improve (+9 % popped, p99 15.04 → 13.76) at a
  ~0.3 ms p50 cost.
- **A20k did NOT drain in ANY variant** (lag 750 k–940 k of 1.15 M pushed).
  This contradicts the round-3 premise that A20k was "consume-matched and
  drained (lag 0)". The consume side is starved by the same shared serial
  pipeline PERF-9 found for C1000, milder — the coordinator should re-check the
  round-3 A20k drain claim.

### C1000 (pop-contention, 64 consumers, 40 s)

| variant | push p50 / p99 ms | popped | lag |
|---|---|---|---|
| drain | **374.78** / 1105.92 | 49,533 | 63,990 |
| continuous (`g300`) | 733.18 / 3686.40 | 56,032 | 57,171 |

`continuous` nearly **doubles** C1000 push p50 and triples p99: proposing ahead
deepens the effective queue for pushes stuck behind the pop flood — exactly the
PERF-9 "deeper `PIPELINE` is worse" (E8/E9) regression, for a small drain gain.

### FAT100 (push-only, 300 k msg/s, 60 s)

| variant | push p50 / p99 ms | achieved msg/s | shed | CPU |
|---|---|---|---|---|
| drain | 700.42 / 1613.82 | 269 k | 671 k | 2.63 |
| continuous (`g300`) | 782.34 / 1597.44 | 271 k | 596 k | 2.70 |

A wash. FAT100 already batches at the command level (`drain_commands` p50 = 127,
i.e. a full pipeline of push-batch-100 requests per entry), so the fsync is
amortized without `GROUP_MAX_US`; `continuous` adds nothing.

## What the code adds (all behind knobs, default = today)

- `QUEEN_RAFT_CYCLE_TRACE` (default off): the per-cycle / per-group trace, via a
  dedicated writer thread over a bounded (drop-on-full) channel, reported drop
  count at shutdown. `timing.rs`.
- `QUEEN_RAFT_CYCLE_SHAPE=drain|continuous` (default `drain`): `continuous`
  gates proposing on **uncommitted < pipeline** (a slot frees at COMMIT via a
  new `Replicator::committed_notify`/`committed_index`) instead of **unapplied <
  pipeline**. `batcher.rs`, `replicator/{mod,local,fake}.rs`.
- `QUEEN_RAFT_MAX_INFLIGHT` (default `pipeline*8`): the hard cap on total
  UNAPPLIED entries in `continuous`, so the overlay (which folds every unapplied
  entry, §7.2) and the in-flight deque stay bounded if apply stalls (I8/I15).
- `QUEEN_RAFT_GROUP_MAX_US` (default 0): the log writer may hold a group open up
  to this many µs to batch consecutive proposes into one fsync. `replicator/local.rs`.

### Invariant reasoning (why `continuous` is safe)

- **D7 / I4 untouched.** The commit signal only frees the PROPOSE slot; a
  waiter is still answered only after LOCAL apply (`resolve_ok`/`resolve_applied`
  on the applied index). Test `continuous_answers_only_after_apply_and_with_the_right_index`.
- **I3, reworded.** Kept bound: at most `pipeline` **uncommitted** entries
  proposed ahead, **and** at most `MAX_INFLIGHT` total unapplied. Safe because a
  committed-but-unapplied entry (resolved=None) is still folded into the overlay
  by `plan_cycle_blocking` (it drops from `inflight` only once
  `store_applied` passes it AND it is resolved), so no effect is lost from the
  planner's view; and on any propose error / step-down every in-flight entry
  (committed or not) drops together and its waiters get `Retry`
  (`lose_leadership`). Test `continuous_proposes_ahead_on_commit_while_drain_waits_for_apply`.
- **I5 held.** Proposes still submit inline on the one driver task in plan
  order; freeing slots on commit does not reorder them. Test
  `continuous_keeps_plan_order_under_the_pipeline`.

## Fix plan

1. **Do not default `continuous` in phase 1.** It cannot move the A20k median
   on a single node (commit ≈ apply, 65 µs apart), it regresses C1000, and it is
   a wash for FAT100. Keep the three knobs (default drain/0), I2-clean and
   tested, and re-run this A/B in **phase 3** where commit precedes local apply
   by a network RTT and the propose-ahead has room. *No product default change.*
2. **The A20k median lever is not the shape.** It is (a) the PERF-9 fixes —
   empty/read-only pops off the serial propose path (56 % of A20k cycles are
   empty pops) and a push/pop lane split — which free pipeline slots and fsync
   budget for real work and should let the consume side drain (A20k lag 750 k is
   the same starvation as C1000); and (b) fsync amortization, where
   `GROUP_MAX_US` finally pays off once (a) has created enough real entries to
   batch. Order: do (a) first.
3. **If a lower A20k median is itself the goal**, the direct floor is the
   single-log fsync (~1 ms) under the durability knobs — out of scope here — or
   fewer entries competing for it (the lane split). The cycle shape does not
   touch it.

## What was cut / not done

- Single VM, co-resident loader (O13: three VMs for final numbers). One 60/40 s
  replicate per cell; A20k/C1000 push p50 has run-to-run spread (PERF-9 noted
  C1000 142–222 ms) — treat the *direction* of each A/B, not the third digit.
  Histograms log2-bucket (± one bucket).
- The trace run's absolute rates/drain are inflated by the trace thread; only
  its structure is used. The A/B runs had the trace OFF (`TRACE_SINK` spawns no
  thread when the knob is off), so they carry no trace overhead.
- `committed_notify` is wired only for `LocalReplicator` / `FakeReplicator`; the
  openraft adapter (phase 3) must supply it (a commit that genuinely precedes
  local apply) for `continuous` to be evaluated on its intended substrate.
- No product default was changed; the code is uncommitted on `raft`. The commit
  agent decides what to keep (the trace and the three knobs are self-contained
  and default-off/drain).
