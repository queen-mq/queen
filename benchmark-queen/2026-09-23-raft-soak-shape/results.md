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

## 11. The ~4-minute collapse: root cause (2026-09-23)

Five-minute runs at 200k from a worktree off `raft` `278d523c`, sampling every
second (broker stage timings, debug counters, kernel memory/reclaim/PSI, disk).

**Ruled out:** page-cache pressure — with a 12.5 GB file pre-loaded into the page
cache (0.3 GB free from the start) the broker ran 165 s balanced; during the
normal collapse there were no disk reads, no direct reclaim and ~0% memory
pressure. Gradual planner slowdown — a planner-thread-only profile at t=200–210 s
looked like the one at t=55–65 s (57% vs 63% busy).

**Cause: the push dedup probe.** On a filter "maybe", the planner builds the
partition's committed dedup view by walking **every retained record from
offset 0** with its hashes (`build_committed_txns_rows` →
`committed_frames(.., from_base 0, ..)`), not just the 60 s dedup window.

| t (s) | dedup builds/s | records read per build | records read/s | planner busy | backlog |
|---|---|---|---|---|---|
| 50–100 | 200–400 | 190–330 | 64–74k | 70% | small (earlier episode, recovered) |
| 110–150 | 0–2 | 220–1,040 | < 3k | 55–58% | small |
| 200 | 13 | 1,548 | 20k | 63% | 1k |
| 230 | 31 | 1,845 | 57k | 73% | 7k |
| 250 | 55 | 2,048 | 113k | 84% | 20k |
| 270 | 81 | 2,240 | 180k | 92% | 148k |
| 300 | 113 | 2,489 | 282k | 95% | 4.2M, acks 0 |

- **Records per build grow linearly with run time** (~10 per second = the
  partition's retained record count at 1,000 msg/s and 100 per push), until
  retention (300 s) would cap them.
- **Builds per second rise as the current filter generation fills**: a
  generation's false-positive rate climbs toward full. The earlier episode
  (40–100 s) was the full 32k-hash generation; it ended when that generation
  aged out of the 60 s window (~97 s). The next generation holds 262k hashes and
  takes ~262 s to fill at 1,000 msg/s per partition, so "maybe"s climb again from
  ~150 s. All 200 partitions fill in step (uniform load), so the timing repeats
  run after run (225–270 s).
- Both factors grow at once, so dedup work grows super-linearly until it exceeds
  the serial planner's headroom (~220–250 s). Then the amplifier from §10 takes
  over: the 5 ms budget cut plus push-first ordering defers every ack and pop,
  consumption falls to 0, and without backpressure the backlog grows unbounded.

**Fix directions:** (1) bound the build to the dedup window instead of offset 0;
(2) keep the filter's false-positive rate flat — seal generations by time, not
only by count; (3) extend the per-partition build across cycles instead of
rebuilding it every cycle. Independently: backpressure, and ack/pop not
starving behind pushes under the budget cut.

## 12. The fix (2026-09-23): the collapse is gone

Branch `bench/collapse` (worktree `/Users/alice/Work/queen-collapse`), on `raft`
`278d523c`: `75bd62ea` + test `6c7c1bd5`. `rsm::` 537 green plus a new test at
the collapse load (1,000 msg/s into one partition for 300 s, 60 s window).

1. **Dedup filter generations close after a slice of the window** (window/4,
   ≥ 250 ms), not only when full; a time-closed generation sizes its successor
   to the rate it saw. A "maybe" band now covers at most one slice.
2. **Priority lane (`QUEEN_RAFT_DRAIN_LANE`, default on):** acks, pops,
   renewals, nacks, transactions, timers and KV are drained before any push; a
   budget-cut command returns to the front of its own lane.

Same config, same 5-minute run at 200k:

| t (s) | backlog before → after | ack/s before → after | planner busy before → after | dedup records read/s before → after | queue wait before → after |
|---|---|---|---|---|---|
| 70 | 3.7k → 1.7k | 200k → 199k | 70% → 58% | 69.7k → 8.1k | 1.7 → 1.7 ms |
| 230 | 7.0k → 4.9k | 229k → 192k | 73% → 61% | 56.8k → 2.0k | 2.1 → 1.7 ms |
| 270 | 148k → 3.4k | 143k → 199k | 92% → 62% | 180k → 2.1k | 122 → 1.9 ms |
| 310 | **5.4M → 4.1k** | **0 → 195k** | 94% → 62% | 291k → 2.1k | **974 → 1.7 ms** |

Deferred unplanned: 7–89% before, 1–3% after. Planner load is now flat for the
whole run instead of climbing.

## 13. Backpressure: a push admission budget (2026-09-23)

Branch `bench/collapse`: `3d43ac23`, `5ba5834d`, `d1fd6f8b` (not merged into
`raft`). `rsm::` + `handlers::` 655 green.

**How it works (`d1fd6f8b`):**

1. A byte budget, 64 MB by default (`QUEEN_RAFT_ADMIT_MAX_MB`, 0 = off), covers
   push and transaction bodies from admission until their reply.
2. The push and transaction routes take their share at the HTTP edge, sized by
   `Content-Length`, BEFORE the body is read. A waiting push holds only its
   connection: its bytes stay in the kernel socket, and TCP slows the sender.
3. When the budget is full, a push waits up to 15 s ±25%
   (`QUEEN_RAFT_ADMIT_HOLD_MS`), then gets `429` with a `Retry-After` of
   1–5 s. Acks and pops never wait.
4. Metrics: `queen_raft_admit_bytes{kind=cap|held}`,
   `queen_raft_admit_waiting`, `queen_raft_admit_total{outcome=waited|refused}`.

**Test:** 300k msg/s offered (above the ~200k ceiling), the §2 shape, 180 s;
the loader keeps at most 20,000 requests in flight and sheds the rest.

| | budget off | v1: gate after the body read, 5 s hold, `Retry-After: 5` | v2 (`d1fd6f8b`): edge gate, jittered |
|---|---|---|---|
| RSS at 180 s | **10.0 GB, still rising** | 6.0 GB | **5.3 GB** (peak 5.5) |
| seconds with pops < 50k msg/s | 0 | **14** | 0 |
| backlog at the end | 0 | **6.4M** | 0 |
| push / ack, last 60 s | 188k / 190k | 264k / 193k | 178k / 180k |
| 429s | 0 | 47,465 | 1,246 |
| loader errors, push / ack | 0 / 0 | 207 / 12,200 | 27 / 1,600 |
| accept-queue overflows | not measured | not measured | 49 |

**Why v1 failed:** requests that queued together were refused together. The
SDKs retry a 429 by themselves (Go: up to 10 attempts, honoring
`Retry-After`), so they came back together about 5 s later. Every wave
reconnected thousands of sockets at once into a 128-slot accept queue, and
pops and acks stalled for 2–4 s.

**What is still open:**

- **A waiting push costs ~70 KB of broker RAM.** Measured with 5,000 waiting
  connections: 66 KB with headers only, 74 KB with the body sent. That is
  hyper, axum and tokio state, not the body. At 20k waiting pushes it adds up to
  ~1.4 GB. The limit is the clients' concurrency, not the budget.
- **The listen backlog is 128** (`ss -ltn`: `LISTEN 0 128`), the Rust
  default; `somaxconn` is 4096.
- **A waiting push whose client disconnects stays** until its hold expires.
- **RSS grows ~8.5 MB/s at ~200k msg/s even at balanced load.** In the §12
  run it went 0.5 → 3.1 GB and kept rising after the 300 s retention started
  deleting files. That is ≈ 43 B per message, and an OOM on this 15 GB VM after
  ~30 minutes. The budget neither causes nor fixes it; the structure that grows
  is not identified yet.

## 14. Why RSS grows ~8.5 MB/s at 200k: the request-outcome log (2026-09-23)

Branch `bench/collapse` `045c7853`, which adds the gauges used here:
`queen_raft_store_ram_rows{ks,kind}`, `queen_malloc_bytes` (glibc
`mallinfo2`, `QUEEN_DEBUG_MALLINFO=1`), and feature `jemalloc-prof` (jemalloc
with its heap profiler). Two 6-minute runs at 200k msg/s, balanced (push = ack
= 200k/s, max lag 44k), the §2 shape:

| | system allocator (glibc) | jemalloc + heap profiler |
|---|---|---|
| RSS growth, 30 s → end | +8.7 MB/s (0.78 → 3.61 GB) | +8.8 MB/s (→ 3.34 GB) |
| heap in use / free in the allocator | +6.7 MB/s / flat at ~0.4 GB | — |
| `request_ids` rows | +5,884/s, 2.1M at 6 min | +5,927/s, 2.1M at 6 min |
| `seg_loc` rows | +1,980/s, 716k at 6 min | +1,980/s, 717k at 6 min |
| new heap, 41 s → 356 s, by call site | — | **97.9%** apply thread writing RAM-store rows (`apply → put_raw`); planner 1.5%, queue-log hash reads 0.4% |

The allocator's free space is flat, so this is live data, not fragmentation.
The heap is ~1.2 KB per request id.

**The mechanism:**

1. Every command gets a request id the broker mints itself (`ReqCtx::new`).
   Clients never see it. Its outcome (per-item push/ack/pop results) is written
   to the RAM keyspaces `request_ids` + `request_expiry`, so that a retried
   command can be answered from the log. The retries in question are the
   broker's own forwarding and leader-change retries.
2. The outcomes live `QUEEN_RAFT_REQUEST_ID_WINDOW_S` = **600 s**.
3. Expiry runs every **10 s** (`request_expire_every_ms`, not a knob) and
   retires at most **4,096** rows per step (`REQUEST_EXPIRE_LIMIT`, a
   replicated constant by design). That is ~410 rows/s against ~5,900
   commands/s at 200k msg/s.

So the table grows at full speed for the first 10 minutes (to ~4 GB), and after
that it keeps growing at ~5,500 rows/s (~6.5 MB/s), for ever. On this 15 GB VM
that is an OOM kill after roughly 35 minutes. `raft` has the same settings
(`batcher.rs` 600 s / 10 s, `apply.rs` 4096).

**Fix options:**

1. **Make the expiry keep up (needed in every case).** When a step retires a
   full 4,096, send the next one in the next cycle instead of 10 s later. This
   only changes timing on the leader; the replicated 4,096-per-step rule stays.
2. **Shorten the window (decision D6).** Nothing waits 600 s for these ids:
   the retries they serve end within seconds (5 s propose deadline). At 200k
   msg/s, 600 s is ~4 GB of RAM at a steady state; 60 s is ~0.4 GB.
3. Later: store less per outcome.

`seg_loc` (one row per queue-log record, removed when retention moves the
partition watermark) was not trimmed within 6 minutes, even though retention is
300 s. It is small (~0.2 MB/s), but a longer run has to show that it levels off.

## 15. The fix: expiry keeps up, window 60 s (2026-09-23)

Branch `bench/collapse` `4fb435b3` (not in `raft` yet). `rsm::` + `handlers::`
656 green. The new test `a_request_id_expiry_backlog_clears_after_one_tick`
fails on the old cadence (6,145 outcomes left) and passes with the fix.

1. A `RequestIdsExpire` step that finds a full `REQUEST_EXPIRE_LIMIT` past the
   cutoff in the committed store runs again in the next cycle, not at the next
   10 s tick. Only the leader's cadence changes; what a step retires does not
   (I2).
2. `QUEEN_RAFT_REQUEST_ID_WINDOW_S` defaults to 60 s (was 600).

Same shape, 200k msg/s balanced (push = ack = 200k/s in both), glibc gauges.
The run was stopped at 279 s once the curve was flat:

| t | RSS before → after | heap in use before → after | request-id rows before → after |
|---|---|---|---|
| 60 s | 1.04 → 0.76 GB | 0.65 → 0.54 GB | 314k → 340k |
| 120 s | 1.56 → 1.46 GB | 1.02 → 0.71 GB | 671k → 370k |
| 240 s | 2.58 → **1.65 GB** | 1.82 → **0.66 GB** | 1,387k → **367k** |
| slope from 150 s | **+8.6 → +0.6 MB/s** | **+6.8 → −0.7 MB/s** | +5,879/s → flat |

After the fix, RSS is ~1.0 GB of anonymous memory plus ~0.65 GB of file-backed
pages (the mapped store and index files, which the kernel can reclaim). What
is left: `seg_loc` still grows ~2k rows/s (550k at 279 s), about +0.3 MB/s
of anonymous memory. It should stop when retention trims it; that needs a run
longer than 5 minutes.
