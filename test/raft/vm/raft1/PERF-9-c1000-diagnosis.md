# PERF-9 — where C1000's 200 ms go, and what the 1 s "stall" is

Phase-1 performance package, **round 4 diagnosis** (2026-09-19). TASK PERF-J.
No product change — this is instrumentation + decisive experiments. The
question from the round-3 finding: a C1000 push spends ~10 ms in the RSM stages
at the median while goload reports 212 ms; the missing ~200 ms is "outside the
RSM pipeline" (HTTP, admission, spawn_blocking hops, tokio scheduling with the
64 consumers, or the loader's own queueing). **None of those. The ~200 ms is
real, server-side push reply-wait inside the single serial raft pipeline, and
the per-stage histograms were lying because they reset their clock on every
defer.**

## Bottom line — `ours=true`

The bound is the **single serial command pipeline** every raft message command
shares: one bounded `cmd_tx` channel → one `Batcher` → one ordered log → one
apply thread (I1/I3). Push, pop **and** ack all funnel through it. C1000's 64
wildcard-pop consumers inject a high rate of pop commands whose **plan is
~0.5 ms each — 100× a push's 4 µs** — and every pop in a drained batch is planned
on the **same serial `spawn_blocking(plan_cycle_blocking)` the batcher awaits
before it can propose the next push entry**. At ~3000 push/s + pops the shared
FIFO command queue sits ~760 deep, so a push waits **~270 ms** behind it
(`push_h_await` p50 268 ms; Little's law: goload in-flight ~800 ÷ ~2 890 achieved/s
≈ 277 ms). Remove the consumers and push p50 is **1.9 ms**; run the SAME loader
against the SAME binary in **postgres mode** (no single serial pipeline — each
request is its own pooled DB txn) and C1000 push p50 is **15 ms and it fully
drains**. The loader, the HTTP layer, admission and the request-id expiry are
all exonerated by experiment.

Why every earlier round said "the stages are fast": `Submission.received_at`
(`batcher.rs:372`) is **restamped on every defer/re-queue** (`batcher.rs:1083`
and `1162`), so `arrival_to_proposed` / `queue_wait` measure only the FINAL
re-plan leg (16.8 ms) and hide the real wait. The new **push-only** histogram
`push_h_await`, anchored at the facade and immune to re-queue, shows the true
268 ms. This is also why the round-3 report invented a "per-partition propose
lock" to explain the gap — there is no such lock in the code, and there is no
missing 200 ms outside the RSM: it is inside it, mis-measured.

## Round-4 review correction (2026-09-20)

The coordinator review **upholds this report's diagnosis** (single serial pipeline shared by
push/pop/ack; the `received_at`-restamp artifact; admission is not in the raft path; the ~1 s
tail is the deep-queue log2 bucket, not request-id expiry — E5) and corrects two framings:

1. **E6 is not "a touch above" M1 — it is the like-for-like denominator.** E6 (postgres on
   this box under the SAME 64-consumer C1000 load) is **15.04 ms — >2× WP-0.2's M1 6.62 ms**,
   not "a touch above": 64 concurrent busy consumers cost postgres ~2.3× on the shared box.
   So **E6 15.04, not M1 6.62, is the like-for-like O14 denominator** for a 64-consumer C1000
   comparison. This report's "~20× gap" was raft's then-current ~296 ms / 15.04; at round-4's
   156.67 ms the like-for-like ratio to E6 is ≈**10.4×** (and 23.7× to the contractual M1
   6.62). Both are gate fails; the package should **state both** and name E6 as the same-load
   control, so the architecture gap (serial pipeline vs pooled txns) is not conflated with the
   baseline's lighter load.
2. **E3a is push-only by construction (already labelled).** Removing all 64 consumers
   (`-consumers 0`) isolates "any consume load exists," not specifically "pop plan cost";
   E3a's 1.90 ms is correctly a push-only number. The specific pop-plan mechanism was
   **directly confirmed later** by PERF-11's `POP_FASTPATH_EMPTY` ablation (C1000 min-FP 230
   vs default 157; pop_wildcard planned 106 k vs 97 k) — so the cause named here is inference
   confirmed downstream, not an in-report moved number. Fine, but worth naming.

## Provenance

- **Binary** `queen` release, VM-built from an rsync of `server/` + `crates/`
  at branch `raft` HEAD `d095d5d4` **plus this task's uncommitted push/pop
  HTTP-boundary instrumentation** (`server/src/rsm/timing.rs`,
  `server/src/rsm/facade/real.rs`, `server/src/handlers/raft.rs`). `cargo build
  --release --bin queen` exit 0, **md5-12 `cb827a77cef5`**. `cargo test --lib
  rsm::` green (376 passed). Instrumentation gated by `QUEEN_RAFT_METRICS`
  (default on) through `timing::stamp()`, so the ablation still prices it.
- **Loader** `/root/goload` md5 `2c97cccb0d1e` — the exact PERF-2..7 loader,
  `-mode openloop`. Push latency = goload `overall` p50 (coordinated-omission:
  latency measured from each request's SCHEDULED instant, so loader-side stalls
  show as latency, not vanish — this is what would have caught a loader problem).
- **VM** `root@164.90.215.224`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4. `ulimit -n
  262144`. Round-3 default knobs (`QUEEN_RAFT_PIPELINE=4 BATCH_COUNTERS=1
  DEDUP_INDEX=txns DEDUP_FRONT=1 DURABLE_ASYNC=1 SEG_BUFFERED=1 BUCKETS=16
  DRIVER_NOTIFY=1 CLAIM_FROM_RING=1`). Host clock 2026-09-19 20:42–21:02 UTC.
  Work under `/root/raft/perf8/perf-j/`, port 6714/6716; each run booted a FRESH
  broker + fresh data dir with the SETTLE guard (`Dirty < 20 MB`), killed only
  its own PID, deleted its data dir. `ERRLINES=0` in every kept raft run.
- **C1000 shape** (measure-raft1.sh): `-rate 3000 -push-batch 1 -partitions 1000
  -consumers 64 -pop-batch 50 -manual-ack -payload 256 -ramp-sec 3`, 60 s.
  Histograms log2-bucket, so a p50 prints as the bucket ceiling (e.g. 268 ms =
  bucket [134, 268) ms) — read ± one bucket.

## The decomposition (E2 — C1000 as-is, round-3 defaults)

goload push **p50 296.96 / p99 790.53 / p999 823.30 ms**. Server-side, push-only:

| stage (push-only) | p50 ms | p99 ms | note |
|---|---|---|---|
| `push_h_prep` (parse+dedup+frame pack) | **0.004** | 0.016 | negligible; the double body-parse is not it |
| `push_h_submit` (`cmd_tx.send` await) | **0.001** | 536 | median instant; the tail = the command channel FILLING (back-pressure) |
| `push_h_await` (propose→commit→apply→answer) | **268** | 1073 | **the ~270 ms is here — inside the RSM** |
| `push_h_total` (whole handler) | 536(bkt) | 1073 | ≈ goload's number, within the log2 bucket |
| `pop_h_total` (pop handler, pop-only) | 67 | 1073 | pops are queued behind the same backlog |
| — mixed `arrival_to_proposed` | **16.8** | 1073 | **MISLEADING** — reset on defer (see below) |
| `proposed_to_committed` | 1.05 | 4.19 | commit is fast |
| `apply_entry` | 0.066 | 2.10 | apply is fast at the median |
| `durable_point` (once/s) | 33.5 | 67.1 | async (DURABLE_ASYNC), not on the push path |

Per-kind plan cost: **push 0.004 ms p50, pop_wildcard 0.524 ms p50 / 1.05 p99 /
6.0 max** — the pop plan is 100× the push plan and runs on the serial plan cycle.
Command mix: push 173 349, pop_wildcard 18 330 (13 784 of them empty), ack 4 526.
Throughput: **appends ≈ 2 850/s, entries 41 427 (690/s)**. CPU 1.38 cores.

## Which experiment moved the number, and by how much

| # | change | goload push p50 | `push_h_await` p50 | verdict |
|---|---|---|---|---|
| **E2** | C1000 as-is (baseline) | 296.96 ms | 268 ms | the bug |
| **E3a** | push-only (`-consumers 0`) | **1.90 ms** | 2.1 ms | **the 64 pop consumers cause it** (appends 2 873/s, entries 2 380/s, CPU 0.53) |
| **E3b** | 64 consumers, long-poll (`-pop-wait`) | 411 ms | 268 ms | no help — fewer pop cmds (13 559) but same wait; not the empty-poll *rate* |
| **E7a** | `BATCH_MAX_CMDS=64` | 411 ms | 268 ms | no help — **not** the re-plan-the-backlog cost; note `arrival_to_proposed` ROSE to 536 ms (less re-queue ⇒ less clock-reset ⇒ the truth surfaces) |
| **E7b** | `BATCH_MAX_CMDS=256` | 374 ms | 268 ms | no help |
| **E5** | `REQUEST_ID_WINDOW_S=20` (expiry actively fires from ~t20) | 346 ms | 268 ms | **request-id expiry exonerated** as the p99 stall |
| **E8** | `PIPELINE=8` | 477 ms (p99 2089) | 268 ms | **worse** — more queue in front of the serial apply |
| **E9** | `PIPELINE=16` | 651 ms (p99 16 056, shed 28 993) | 536 ms | **much worse** — confirms apply/serial-bound, not depth-bound |
| **E6** | **postgres broker, same loader/box/binary** | **15.04 ms** (p99 75, **fully drains**, lag 46) | n/a | **loader + HTTP layer exonerated; the raft serial pipeline is the cause** |

Admission (WP-1.7) is **not in the raft message path at all**: `handle_push`
and `handle_pop*` branch to `handlers::raft::dispatch_*` BEFORE the postgres
path's `st.admission.acquire(...)`, and the facade/batcher/planner take no
admission slot (`grep` is clean). So the "admission bypass" experiment is moot —
there is nothing to bypass. Recorded here so it is not re-tried.

## The cause, named, with evidence

1. **Single serial pipeline shared by push and pop.** Every mutating command —
   push, pop (even an empty claim reads through it), ack — is one `Submission`
   on one bounded `cmd_tx`, drained by one `Batcher`, planned in one
   `spawn_blocking(plan_cycle_blocking)` the batcher **awaits**, proposed to one
   ordered log, applied by one apply thread (I1/I3). There is no push lane and
   no pop lane.
2. **Pops are the expensive, high-variance tenant of that pipeline.** A wildcard
   pop plans in ~0.5 ms (ring scan over up to the 64-wide checkout; 100× a
   push's 4 µs) and a claiming pop's apply leases 50 messages. Empty wildcard
   pops return `Plan::Empty` (no log entry — `planner/pop.rs:178`) but STILL pay
   the ~0.5 ms plan inside the serial cycle. Every pop in a drained batch delays
   the push entries planned with it.
3. **At C1000's rate the shared FIFO backs up.** Message throughput is the SAME
   with and without pops (~2 850 appends/s ≈ offered), so this is not a
   throughput ceiling — it is **queueing latency from service-time variance**:
   the pops make each cycle's service bursty, the queue equilibrates ~760 deep,
   and pushes wait ~270 ms behind it. Little's law closes: goload in-flight ~800
   ÷ ~2 890 achieved/s ≈ 0.277 s. `push_h_submit` p99 536 ms shows the command
   channel itself filling and blocking the facade send at the worst moments.
4. **Postgres has no such pipeline** — each request is its own pooled txn, so
   push and pop run in parallel across 64 connections: 15 ms and it drains
   (E6). Removing the consumers removes the contention: 1.9 ms (E3a). Deeper
   pipeline only lengthens the queue in front of the serial apply: worse
   (E8/E9). All consistent with one serial pipeline being the bound.

**The 1 s "stall" (queue_wait / arrival_to_proposed p99 = 1.07 s) is not a
discrete periodic event.** Request-id expiry is exonerated (E5: shortening the
window so expiry actively retires from t≈20 s changed nothing; and it is bounded
to 4096 rows/cycle over a time-ordered `RequestExpiry` index that early-stops at
the cutoff — in a 60–75 s run with a 600 s window it finds ~0 victims anyway).
The durable point is 33–67 ms and async. The 1.07 s is simply the log2 bucket
the deep-queue tail lands in — the worst moments of the same shared-pipeline
back-up (the channel full, the facade `cmd_tx.send` blocked up to ~0.5 s).

**The measurement artifact that misled rounds 1–3.** `Submission.received_at`
is `timing::stamp()`-ed at facade ingress (`batcher.rs:372`) but **re-stamped**
every time a budget-cut command is re-queued (`batcher.rs:1083`, `1162`). So
`arrival_to_proposed` and `queue_wait` measure from the LAST re-queue, not from
arrival, and a command that waited 270 ms across several defer cycles reports
16.8 ms. Proof: shrinking `BATCH_MAX_CMDS` (E7a) reduces re-queuing and
`arrival_to_proposed` p50 jumps 16.8 → 536 ms toward the true value, while
`push_h_await` (facade-anchored, never reset) is a steady 268 ms in EVERY pop
config (E2/E3b/E5/E7). The "every RSM stage is fast, so the 200 ms must be
outside the pipeline" conclusion — and the nonexistent "per-partition propose
lock" — both come from this reset.

## Fix plan (for a later WP — no product change here)

Target: keep push off the pops' critical serial path. In leverage order.

1. **Empty/read-only pops off the serial propose path.** *Mechanism:* an empty
   wildcard pop changes no replicated state (it returns `Plan::Empty`); most
   pops here are empty (13 784/18 330). Give the pop a cheap read-only
   pre-check (the ready-ring / notifier gate) BEFORE it submits a
   `PopWildcard` command, so a claim-nothing poll never enters the batcher's
   serial plan cycle and never competes with pushes; only a poll that WILL claim
   submits a command. *Files:* `server/src/rsm/facade/real.rs` (`pop_run`
   pre-check), `server/src/rsm/planner/pop.rs` (expose the ring read),
   `server/src/rsm/batcher.rs`. *Knob:* `QUEEN_RAFT_POP_FASTPATH_EMPTY` (default
   on). *Expected:* large — removes ~¾ of pop commands (and all their serial
   plan cost) from the pipeline; it should also fix consume drain (raft popped
   only 92 k of 173 k pushed vs postgres draining fully — the consumers are
   throttled by the same pipeline).
2. **Two-lane / push-priority drain.** *Mechanism:* `drain_batch` currently
   pulls FIFO, so pushes wait behind pops; drain pushes preferentially (or
   round-robin push vs pop) so a burst of pops cannot head-of-line-block pushes.
   *Files:* `server/src/rsm/batcher.rs` (`drain_batch`). *Knob:*
   `QUEEN_RAFT_PUSH_PRIORITY` (default on). *Expected:* moderate; complementary
   to (1). Watch per-partition push ordering is preserved (it is — pushes to one
   partition stay in arrival order within the lane).
3. **Diagnostics correctness (do this first, it is cheap and unblocks future
   rounds):** stop re-stamping `received_at` on defer — keep the original facade
   arrival and add a separate per-cycle "wait since last cycle" stamp if that
   leg is still wanted — so `arrival_to_proposed` measures the true
   facade→proposed wait. Ship the `push_h_*` / `pop_h_total` histograms added
   here. *Files:* `server/src/rsm/batcher.rs`, `server/src/rsm/timing.rs`.
   *Expected:* no perf change; it stops the pipeline from being mis-measured.

Not the fix: deeper `PIPELINE` (E8/E9 regress — apply-serial-bound); smaller/larger
`BATCH_MAX_CMDS` (E7 — no move); long-poll (E3b — no move); admission (not in path).

## What was cut / not done

- Single VM, co-resident loader (O13: three VMs for final numbers). One 60 s
  replicate per cell; C1000 push p50 has run-to-run spread (round-3 142–222 ms),
  so treat the *direction* of each experiment as the result, not the third
  digit. Histograms log2-bucket (± one bucket).
- The postgres control (E6) ran my instrumented binary in postgres mode (the
  raft instrumentation is a no-op there) and dropped/re-applied the `queen`
  schema on the VM's system postgres before and after; its 15 ms is **>2× (not "a touch
  above")** WP-0.2's M1 6.62 ms because it ran concurrently with the 64 busy
  consumers on the shared box — the ~20× gap to raft (at this report's ~296 ms; ≈10.4× at
  round-4's 156.67 ms) is the point, not the digit. [**Round-4 review correction: E6 15.04 is
  the like-for-like 64-consumer O14 denominator; M1 6.62 is the lighter-load contractual
  baseline — report both.**]
- I did not implement any fix (task is diagnosis-only). Instrumentation is
  uncommitted on `raft`; the commit agent decides whether to keep it (fix
  direction 3 recommends keeping the `push_h_*`/`pop_h_total` families).
