# Raft class, soak-24h shape, two hosts — 2026-09-23

**Max balanced throughput: ~200,000 msg/s each way for the first ~4 minutes**
(push 199,940/s, ack 199,760/s, backlog flat) with long-poll consumers, on one
8-vCPU broker, no Postgres. **Correction (5-minute runs, §9):** at 200k the
backlog stays flat for about 4 minutes and then tips into the overload spiral,
so 200k is not a sustained rate on this box; the sustained ceiling is below it.
Push latency p50 45 ms / p99 130 ms; broker 4.26 cores, 920 MB RSS, 56 MB/s disk.
Above that the broker is overloaded and consumption decays (see §4).

## 1. Rig

| Role | Host | Size | Address used |
|---|---|---|---|
| Broker | 164.90.215.224 (DigitalOcean) | 8 vCPU, 15 GB RAM, local ext4 disk | binds VPC 10.114.0.2 |
| Loader | 164.90.187.246 (DigitalOcean) | 16 vCPU, 31 GB RAM | VPC 10.114.0.3 |

VPC link between the two hosts: 857 MB/s measured (2026-09-22, same VPC).

## 2. Broker configuration

- **Binary:** `queen-ep`, built from branch `empty-pop` (`2b4a5724`) = `raft` at
  `09673afa` plus one change: the wildcard "provably empty" check runs inline
  instead of on the blocking pool. **Every run below sets
  `QUEEN_RAFT_POP_FASTPATH_EMPTY=1`**, i.e. the fast path is ON as on `raft`.
- **Environment:**
  ```
  ulimit -n 1048576
  QUEEN_STORAGE=raft
  QUEEN_RAFT_DEDUP_INDEX=segment
  QUEEN_RAFT_POP_FASTPATH_EMPTY=1
  QUEEN_BIND_ADDR=10.114.0.2
  JWT_ENABLED=false
  QUEEN_TENANCY_HEADER=false
  LOG_LEVEL=warn
  ```
  Everything else default: queue-log preallocation + fdatasync, zstd level 1
  codec (compression started at propose), parallel fsync, push-first batch
  ordering (`QUEEN_RAFT_PUSH_PRIORITY=1`), single node (local replicator, no
  replication, no failover).
- Fresh data directory per run; the queue is created and configured by goload at
  t0 through `/api/v1/configure` (dedup window 60 s, completed retention 300 s).

## 3. Loader configuration

`goload` from `benchmark-queen/2026-07-29-vm-campaign/goload` (with the
`-payload-json` mode added 2026-09-22). One process:

```
goload -mode openloop -url http://10.114.0.2:<port> -queue <fresh> \
  -rate <R> -push-batch 100 -partitions 200 -consumers 200 \
  -pop-batch 1000 -payload 256 -payload-json \
  -manual-ack -ack-async -ack-inflight 256 \
  -dedup-window 60 -completed-retention 300 \
  -pop-wait -pop-timeout 2000 -timeout 60000 \
  -ramp-sec 5 -report 5
```

- Payload: realistic JSON events (ids, timestamps, phone, price, random-word
  text), distinct per message, **average 289 B**. Compressible, like the soak-24h
  payload.
- Same shape as `queenmq.com/benchmarks/soak-24h` (push batch 100, 200
  partitions, 200 consumers, pop batch 1000, async acks 256 in flight, dedup
  60 s) **plus long-poll** (`-pop-wait`).

## 4. Results (long-poll)

Each run lasts 50 s; everything is measured over the **25–50 s window**. Push and
ack are the median of goload's 5-second interval lines; backlog slope is
(lag at window end − lag at window start) / window.

| Offered | Push/s | Ack/s | Backlog | Push p50 / p99 | Ack round trip | Broker CPU | RSS | Disk | Batcher busy |
|---|---|---|---|---|---|---|---|---|---|
| 100k | 99,960 | 99,990 | flat (end 300) | 5.2 / 23 ms | 3.7 ms | 2.20 cores | 423 MB | 47 MB/s | 38% |
| **200k** | **199,940** | **199,760** | **flat (end 5,800)** | **45 / 130 ms** | 37–46 ms | **4.26 cores** | **920 MB** | **56 MB/s** | **87%** |
| 200k, 30 s poll timeout | ~200k | ~197–213k per interval | flat (end ~10k) | ~48 / 122–142 ms | — | — | — | — | — |
| 300k (overload) | 293,330 | 104,600 | **+201,600/s** | 1,835 / 1,966 ms | 367 → 524 ms, rising | 3.94 cores | 3.4 GB, rising | 64 MB/s | 93% |

- **Latency field:** goload's open-loop histogram measures each **push request
  from its scheduled send time** (coordinated-omission corrected). The soak-24h
  page reports the same field as "end-to-end".
- **Empty pops with long-poll:** 0/s at 100k and 200k (vs 17–19k/s at 100k
  without long-poll).
- **Ceiling:** at 200k the serial batcher is 87% busy (planning 69%). It is the
  limit, not CPU (4.3 of 8 cores used) or disk.

### Overload mechanism (300k)

Commands queue in front of the single serial batcher (queue wait ~330 ms). Acks
wait behind pushes, so ack latency climbs (4 ms at 100k → 40 ms at 200k →
370–600 ms and rising at 300k). A partition can be claimed again only after its
ack commits, so consumption caps near 200 partitions × 1000 messages ÷
turnaround (~100k/s) while pushes keep arriving at ~290k/s. The backlog grows,
the queue slows further — positive feedback. Checked and ruled out: the pop
deadline (2 s and 30 s timeouts behave the same) and push-first ordering
(`QUEEN_RAFT_PUSH_PRIORITY=0`: ack 86k/s vs 105k, no help). Candidate fixes: a
priority lane for acks/pops, and push backpressure (429).

## 5. Without long-poll (for reference)

Same shape without `-pop-wait`, fast path ON vs OFF (same binary, interleaved,
2 repeats each):

| Offered | Fast path | Push = Ack | Empty pops/s | Push p50 / p99 | Broker CPU | Planner busy |
|---|---|---|---|---|---|---|
| 20k | ON | 20k | 46–47k | 6.1–6.5 / 15–19 ms | 4.4–4.5 cores | 8% |
| 20k | OFF | 20k | 41–42k | 6.2–6.3 / 19–29 ms | 4.3 cores | 19% |
| 100k | ON | 100k | 17.6–18.4k | 14.0–14.2 / 46–48 ms | 3.9–4.1 cores | 30% |
| 100k | OFF | 100k | 16.7–16.9k | 12.8 / 38–45 ms | 3.8–3.9 cores | 32–33% |

Polling consumers spend most of the broker's CPU on empty pops; long-poll removes
them (100k: 2.2 cores with long-poll vs ~4 without).

## 6. What this does not establish

- Not a soak: 50-second runs. Memory and backlog trends over hours are unknown.
- One broker, no replication, no failover.
- One shape (batch 100, 289 B JSON, 200 partitions). Batch 1 or large payloads
  are different measurements.
- The binary is `raft` `09673afa` plus the inline fast-path read, not the exact
  `raft` tip.

## 7. Sources

Driver scripts (on the author's machine, session scratchpad): `lp_test.sh`,
`pp_test.sh`, `ab_fastpath.sh`; broker helpers `/root/raft/ab_start.sh`,
`ab_snap.sh`, `ab_stop.sh`; loader helpers `/root/ab_load.sh`,
`/root/ab_load_wait.sh`. Raw goload logs: loader `/root/ab-*/g.log`. Broker
logs: `/root/raft/m12/ab-*/b.log`.

## 8. Planner measurement at 200k (5-minute run, 2026-09-23)

Same rig and shape as §3 (long-poll, 200k offered). Binary `queen-pm` = the
§2 binary plus measurement-only counters (branch `planner-measure`, `ed9a9e03`,
not for merge): thread CPU time of the planning call next to its wall time, and
a count of commands drained but deferred unplanned by the 5 ms plan budget
(`QUEEN_RAFT_PLAN_MAX_MS`). Raw output: `planner-windows.txt`,
`planner-profile.txt`.

**Clean window (30–120 s):** push 199,950/s, ack 199,930/s, backlog flat,
push p50 47 ms / p99 129 ms, broker 4.30 cores.

| Serial stage, per cycle (271 cycles/s) | Wall | Thread CPU | CPU / wall |
|---|---|---|---|
| Whole batcher cycle (drain → propose) | 3.27 ms (89% of wall) | — | — |
| Planning call on the blocking pool | 2.86 ms (82%) | 2.00 ms (58%) | **0.70** |
| of which the command loop | 2.46 ms (71%) | 1.70 ms (49%) | 0.69 |
| Outside the planning call (encode, routing, propose, pool hop) | 0.41 ms | — | — |

- **~30% of the planner's wall time is off-CPU** (0.86 ms per cycle). A CPU
  profile cannot attribute it; futex waits show up at 2.6% inclusive. An
  off-CPU profile would be needed to name it.
- **Deferred unplanned:** 12,847 of 19,632 commands drained per second (65%;
  ~45 per cycle) are pushed back by the 5 ms budget cut and re-drained later.
  Both numbers count re-drains, so this is churn, not lost work.

**Planner call graph** (perf, `--call-graph dwarf`, 99 Hz, 20 s at t=120–140 s,
1,373 planner samples = ~0.7 core; the planner ran on 53 different pool
threads). Inclusive share of planner samples:

| Share | Where |
|---|---|
| 35.8% | `plan_push` (dedup probe `probe_plan` 9.9%, dedup front `maybe` 7.6%) |
| 31.8% | `plan_pop_over_queue` (`claim_one` 19.3%, `delivered_from_gathered` 6.3%) |
| ~30–40% | allocation churn across both: `realloc` 18.1%, `finish_grow` 19.1%, `do_reserve_and_handle` 11.6%, `cfree` 11.9%, `malloc` 7.9%, `drop_glue` 7.6% |
| 30.8% (self) | unresolved leaf frames in libc — most likely `memcpy`/`memmove` behind the grows above |
| 9.9% | `Overlay::fold_effect` — rebuilding the in-flight overlay every cycle |
| 6.0% | `rebuild_rings` → `scan_pending` |
| 11.3% | partition lookups (`get_raw` 7.4%) |
| 6.0% | `ack_target` |

Caveats: 1,373 samples (±~3 points on the top rows); the perf capture itself
perturbed the run — after it the backlog grew and never recovered (window B in
`planner-windows.txt`), which is also a live demonstration of how little headroom
89% batcher load leaves without backpressure.

**Reading for the lanes question:** lanes would parallelize the 2.86 ms planning
call but not the 0.41 ms outside it; the writer's fsync is the next wall. Before
lanes, the single-thread waste is large and cheap to attack: allocation churn
(~30–40% of planner CPU), the per-cycle overlay and ring rebuild (~16%), the 65%
deferral churn, and the 30% off-CPU time. Lanes would multiply all of it.

## 9. After: dedicated planner thread + pre-sized vectors (5-minute run)

Binary `queen-ab` = §8 binary + `f75c5914` (planning on one dedicated
`queen-planner` thread instead of the shared blocking pool, with its run-queue
wait recorded per call) + `7affdc27` (push blob, pop claim set and the cycle's
entry pre-sized). Branch `planner-thread`. Same rig, shape and rate; **no perf
capture** this time. Raw: `planner-windows-after.txt`.

| Clean window (30–120 s), per cycle | Before (§8) | After |
|---|---|---|
| Push / ack | 199,950 / 199,930 | 199,980 / 200,270 |
| Push p50 / p99 | 47 / 129 ms | 43.5 / 137 ms |
| Ack round trip | 58 ms | 41.5 ms |
| Batcher cycles/s × ms | 271 × 3.27 ms (89% busy) | 541 × 1.43 ms (**77%**) |
| Planning call wall / CPU | 2.86 / 2.00 ms (82% busy) | 1.16 / 0.93 ms (**69%**) |
| CPU / wall | 0.70 | **0.80** |
| Planner wall per planned command | ~114 µs | **~86 µs** |
| Off-CPU per call | 0.86 ms | 0.23 ms = run-queue 0.10 + blocked 0.14 |
| Deferred unplanned by the 5 ms cut | 65% of drained | **28%** |
| Outside the planning call | 0.41 ms | 0.27 ms |
| Broker CPU | 4.30 cores | 4.50 cores |

Planner thread (from `/proc/…/task/<tid>/schedstat`, whole window): on-CPU 57%
of wall, run-queue 8%; 4,050 voluntary and 320 involuntary switches/s. Inside
planning calls the remaining off-CPU time is **~43% waiting for a core, ~57%
blocked** — both now small (0.23 ms per call).

Other serial stages in the same window: log writer fsync 49% busy (1.16 ms per
group), writer pickup 34%, apply 12%. **The planner is still the first limit**,
with more headroom than before.

**The ~4-minute tip:** backlog ≤ 8k until t≈225 s, then 19k → 375k → 3M within
45 s (queue wait 1.6 → 72 ms, RSS 1.4 → 6.2 GB). Writer and apply stage times
barely moved between the windows (fsync 1.16 → 1.37 ms), so the trigger is not
yet identified — candidates are a periodic step inside the serial planner
(maintenance, retention) or page-cache pressure once ~14 GB of queue log has
been written into 15 GB of RAM. Without push backpressure any such stall turns
into the spiral.

## 10. All three changes: + kept overlay and ring (5-minute run)

Binary `queen-ko` = §9 + `98a7bc09` (cherry-pick of the agent's `766719d2`:
the planner keeps its overlay and wildcard rings between cycles and updates them
per landed entry; `QUEEN_RAFT_KEEP_OVERLAY=0` restores the rebuild; a gate test
checks kept == rebuilt after every cycle, and on vs off proposes byte-identical
entries). `rsm::` 537 green. Raw: `planner-windows-keptoverlay.txt`.

| Clean window (30–120 s), 200k | Original (§8) | + thread + pre-size (§9) | **+ kept overlay** |
|---|---|---|---|
| Planning call wall / CPU per cycle | 2.86 / 2.00 ms | 1.16 / 0.93 ms | **0.95 / 0.75 ms** |
| Planner busy (share of wall) | 82% | 69% | **64%** |
| Batcher cycle busy | 89% | 77% | **73%** |
| Planner wall per planned command | ~114 µs | ~86 µs | **~79 µs** |
| Deferred unplanned (share of drained) | 65% | 28% | 26% |
| Push p50 / p99 | 47 / 129 ms | 43.5 / 137 ms | 42 / 131 ms |
| Ack round trip | 58 ms | 41.5 ms | 40.5 ms |
| Broker CPU | 4.30 cores | 4.50 cores | 4.42 cores |
| Backlog tips at | ~120 s (perf capture) | ~225 s | ~255 s |

- Kept overlay in production conditions: advanced on every cycle (673/s), 0
  rebuilds, 0 fallbacks, 0 mismatches, 0 ring reloads.
- Serial stages in the clean window: planner 64%, log writer fsync 50%, writer
  pickup 40%, apply 12%. **The planner is still the first limit, but only just**
  — the writer is next.
- **The ~4-minute tip is unchanged by planner speed** (225 s → 255 s): the
  backlog stays flat, then goes 23k → 200k → 2.3M within 30 s. The sustained
  ceiling on this box is set by that trigger plus the missing backpressure, not by
  planner cost. Hunt the trigger and add push backpressure before lanes; lanes
  would buy at most ~1.3–1.5× before the writer binds.
