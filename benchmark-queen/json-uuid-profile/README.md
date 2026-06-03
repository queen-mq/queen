# JSON + UUID CPU profiling harness

Measures **how much broker CPU is actually spent on JSON (nlohmann parse/dump)
and UUIDv7 generation (stringstream + global mutex)** under hard push load.

This answers the question: "PG is the bottleneck, but if Queen routed messages
more efficiently could we claw back ~5% overall?" — by *measuring* the
recoverable broker overhead instead of guessing.

## What it does

```
loadgen (autocannon) ──HTTP──▶ broker (queen-server, profiling build) ──▶ postgres
```

- The broker is built `-O3` (production perf) **plus** `-g -fno-omit-frame-pointer`
so the **gperftools CPU profiler** can attribute samples to functions/lines.
- libprofiler is `LD_PRELOAD`ed in **signal-toggle mode** — no source changes.
`run.sh` brackets only the steady-state window with `SIGUSR2`.
- The broker is **CPU-capped** (default 3 CPUs) so it saturates before PG,
which is the regime where per-request overhead is visible. On a box where PG
is the hard limit and the broker idles, this overhead is real but won't move
throughput — see the parent discussion.

## Requirements

- Docker + Docker Compose v2 (tested on Docker Desktop, arm64).
- No `perf`/privileged mode needed (that's why we use gperftools, which works
inside Docker Desktop's VM where `perf` usually doesn't).

## Usage

```bash
cd benchmark-queen/json-uuid-profile

# Build the profiling broker image + loadgen (first build is slow: full C++ -O3 build)
docker compose build broker
docker compose build loadgen

# Run the full measurement (warmup -> measured load -> profile -> analyze)
./run.sh

# Re-analyze a previous profile without re-running load:
BROKER_CONTAINER=qjup-broker PROFILE=/profiles/queen.prof ./analyze.sh

# Tear down
docker compose down -v
```

## Knobs (env vars)


| Var                  | Default | Meaning                                               |
| -------------------- | ------- | ----------------------------------------------------- |
| `BROKER_CPUS`        | `3.0`   | CPU cap on the broker (lower → saturates sooner)      |
| `NUM_WORKERS`        | `3`     | broker worker threads                                 |
| `CONNECTIONS`        | `200`   | autocannon connections                                |
| `LOADGEN_WORKERS`    | `4`     | autocannon worker threads                             |
| `PUSH_BATCH`         | `10`    | items per push request                                |
| `MAX_PARTITIONS`     | `200`   | distinct partitions                                   |
| `PAYLOAD_SIZE_BYTES` | `256`   | approx per-message payload size (bump to stress JSON) |
| `DURATION`           | `60`    | measured load seconds                                 |
| `PROFILE_WINDOW`     | `40`    | seconds of profiling inside the load                  |


To make the **JSON** cost dominate, raise `PAYLOAD_SIZE_BYTES` (e.g. `4096`).
To make the **UUID/lock** cost dominate, use small payloads + large `PUSH_BATCH`
(more messages = more `generate_uuid()` calls per request) and more `NUM_WORKERS`
(more threads contending the single `uuid_mutex`).

## Reading the output

`analyze.sh` prints, from the captured profile:

1. **Top 40 functions by self CPU** — eyeball the real hotspots.
2. **Bucketed attribution** — flat% summed into JSON / UUID / LOCK / ALLOC.
  - `ALLOC` (new/delete/malloc) is largely JSON-DOM-driven, so
   `JSON + ALLOC` is the realistic upper bound on JSON-attributable CPU.
  - `LOCK` (futex/mutex) captures contention on the global `uuid_mutex`.
3. **Line-level cost inside `generate_uuid()`** — shows the `std::stringstream`
  hotspot directly.
4. **Collapsed stacks** — feed to a flamegraph tool if you want a picture.

`run-meta.txt` records the **PG-ground-truth push msg/s** (diff of
`pg_stat_user_tables.n_tup_ins` on `queen.messages`) for the profiled window, so
you can correlate the CPU buckets with real throughput.

## Measured result (first run, 2026-06-03)

Setup: arm64, broker capped at **3 CPU / 3 workers**, PG at 4 CPU, co-located;
load = 200 conns × 4 autocannon workers, `PUSH_BATCH=10`, 200 partitions,
~256 B payloads; 40 s profiling window.

- **Throughput (PG ground truth):** ~23.6k msg/s push (2.14M rows in 40 s).
- **Saturation:** broker ~2.0–2.5 of 3 cores, PG ~2–4 of 4 cores, broker
RSS ~40–60 MB. Co-located, broker-contributing regime.

Broker self-CPU attribution (`google-pprof --text`, 7,656 samples):


| Bucket                                         | Flat %                    | Notes                  |
| ---------------------------------------------- | ------------------------- | ---------------------- |
| **JSON** (nlohmann parse/dump/lexer/serialize) | **38.3%**                 | dominant               |
| ALLOC (new/delete/malloc/morecore)             | 13.5%                     | mostly JSON-DOM-driven |
| IO/sys (send/recv/epoll/timer/libpq)           | 22.3%                     |                        |
| **UUID** (`generate_uuid` + stringstream)      | **0.9%** flat (~1.7% cum) | small                  |
| LOCK (mutex/futex incl. global `uuid_mutex`)   | 0.6%                      | small at 3 workers     |


Hottest individual frames: `serializer::dump_escaped` 10.1% self;
`parser::sax_parse_internal` **21.7% cum**; `serializer::dump` **14.8% cum**
(note: push.cpp calls `.dump()` **twice** on the same array);
`~basic_json` + `json_value::destroy` ~13–15% cum (DOM teardown);
`assert_invariant` 3.6% (nlohmann internal checks — pure overhead).

**Takeaways**

- **JSON ≈ 38% flat, ~50% incl. its allocator** — the real recoverable broker
CPU. The duplicate `.dump()` and the full DOM round-trip of pass-through
payloads are the fattest, easiest targets. In this co-located regime, cutting
JSON CPU directly frees cores for PG → the "~5% overall" is realistic (and
likely conservative) for JSON work.
- **UUID ≈ 1.7%, lock < 0.7%** at 3 workers — the `stringstream` + global mutex
we flagged are *real but minor* here. They're a tail-latency / worker-scaling
fix, **not** a throughput lever on their own. (Raise `NUM_WORKERS` to see the
lock share grow.)

Raw artifacts: `results/20260603-163006/` (`analysis.txt`, `queen.prof.0`,
`run-meta.txt`, `docker-stats.txt`).

## Stage 1 result — simdjson + raw payload pass-through (2026-06-03)

`POST /api/v1/push` behind `QUEEN_PUSH_SIMD` (0 = nlohmann baseline, 1 = simd).
Same harness, fresh DB per run, 3-core broker. `./ab.sh` reproduces.


| flag     | payload | msg/s      | broker CPU% | CPU/msg vs base |
| -------- | ------- | ---------- | ----------- | --------------- |
| nlohmann | 1 KB    | 26,040     | 149         | —               |
| **simd** | 1 KB    | 22,691     | **114**     | **−12%**        |
| nlohmann | 4 KB    | 13,071     | 248         | —               |
| **simd** | 4 KB    | **16,848** | **159**     | **−50%**        |


- **Correctness:** verified — nested objects, escaped quotes, unicode, arrays,
empty `{}`, and quote-containing partition names all round-trip intact;
`non2xx=0` under load at both sizes.
- **Efficiency:** broker CPU/message drops **−12% at 1 KB, −50% at 4 KB** — the
win scales with payload size exactly as predicted (raw pass-through avoids
parsing/copying/reserializing the payload).
- **Throughput:** **+29% at 4 KB** (broker contending for cores → freed CPU
becomes throughput). At 1 KB the broker is *not* saturated (~1.1–1.5 of 3
cores), so throughput is PG/client-gated and the absolute number is within
run-to-run noise — the robust signal there is the CPU/msg drop.
- The `JSON%` bucket only partly drops because (a) simdjson's own parse CPU
isn't matched by the nlohmann-oriented bucket regex, and (b) the **PG-result
parse is still nlohmann** — that's the Stage 2 lever (`json::parse(result)`).

## Stage 2 result — raw result pass-through (2026-06-03)

`QUEEN_PUSH_RAW_RESULT`: on the success path, stream the stored-procedure result
string straight to the client (skip the route handler's `json::parse(result)` +
`send_response` re-`dump`). Independent of Stage 1. `./ab2.sh` reproduces.

A/B matrix (`cpu_per_1k_msg`, lower = better; fresh DB per run):


| simd | raw | payload | msg/s  | broker CPU% | cpu/1k-msg |
| ---- | --- | ------- | ------ | ----------- | ---------- |
| 0    | 0   | 1 KB    | 25,271 | 170         | 6.73       |
| 1    | 0   | 1 KB    | 23,764 | 119         | **5.01**   |
| 0    | 1   | 1 KB    | 22,122 | 177         | 8.00       |
| 1    | 1   | 1 KB    | 24,981 | 113         | **4.52**   |
| 0    | 0   | 4 KB    | 13,906 | 229         | 16.47      |
| 1    | 0   | 4 KB    | 18,062 | 167         | **9.25**   |
| 1    | 1   | 4 KB    | 15,330 | 170         | 11.09      |


- **Correctness:** verified — raw path returns the normal `status:"queued"`
array; `non2xx=0` everywhere.
- **Stage 2 effect is tiny and within the harness's CPU-measurement noise**
(baseline cpu/1k-msg itself drifts ~±18% between matrices). Stage 1 (ingest)
remains the dominant lever; `1:1 @ 1 KB = 4.52` is the best config overall.
- **Why Stage 2 barely moves it — the key finding.** A profile diff of `1:0`
vs `1:1` (both simd ingest, so the *only* nlohmann left is result handling)
shows nlohmann at **39.6% → 38.9%** of broker CPU — Stage 2 removed just
~0.7 pp. The remaining ~39% is **not in the route handler**: it's in
**libqueen `_process_slot_result` (`lib/queen.hpp:1233`)**, which must
`nlohmann::parse` the full PG result (to demux one batched PG call back to N
HTTP jobs by `idx` and extract `partition_updates`) and then `dump()` a
per-job result subset for each callback. Stage 1/2 never touch it.

### Implication for the next step

The real result-side win is in **libqueen**, not the routes: parse the PG result
with simdjson and slice each job's result subset *raw* instead of
nlohmann-parse + per-job `dump()`. That's the shared hot loop for
push/pop/ack/transaction, so it's higher blast-radius / higher risk — but it's
where the remaining ~39% result-side JSON CPU actually lives. The route-level
`QUEEN_PUSH_RAW_RESULT` is correct and zero-downside, so keep it, but it pairs
with that libqueen change to matter.

## libqueen conversion — Step 1: fire-side merge (2026-06-03)

`_fire_batched` (`lib/queen.hpp`) merged each batched job's items array via
`nlohmann::parse` + idx renumber + one big `dump()` per DB call — and it
re-parsed the very string Stage 1 had just built. Converted to simdjson
On-Demand raw-slicing with `idx`/`index` injected at each item's front; the 7
routes that emitted `idx`/`index` (pop ×2, ack ×2, cycle, register_query,
leases, state_get) no longer do (libqueen injects, behavior-preserving — it
always renumbered anyway). No flag (test branch).

Correctness: batched concurrent load (61k req) + push→pop round-trip preserved
nested objects, escaped quotes, empty `{}`, and demux routing exactly;
`non2xx=0`.

Impact vs. the prior nlohmann fire-side, same harness:


| config                | payload | cpu/1k-msg               | msg/s             |
| --------------------- | ------- | ------------------------ | ----------------- |
| 0:0 (nlohmann routes) | 1 KB    | 6.73 → **4.68** (−30%)   | 25.3k → 28.6k     |
| 0:0                   | 4 KB    | 16.47 → **12.02** (−27%) | 13.9k → 19.0k     |
| 1:1 (simd routes)     | 1 KB    | 4.52 → **2.59** (−43%)   | 25.0k → 30.5k     |
| 1:1                   | 4 KB    | 11.09 → **3.87** (−65%)  | 15.3k → **23.5k** |


The fire-side merge was a major hidden cost (it ran on *every* DB call, all
types). With it gone, the residual nlohmann in the 1:1 profile dropped from
~39% to ~3% per top symbol, and the broker at 1:1 now runs < 1 core (PG-bound).

### Still on nlohmann (next step)

`_process_slot_result` (result demux: parse PG result, split by idx, per-job
`dump`, **POP message mutation**, metrics) and `_process_custom_result`. This is
the larger, more delicate rewrite (7 job types incl. POP's `partitionId`/
`leaseId` stamping) and will be done with a differential test. With the broker
already < 1 core at 1:1, its remaining upside is broker CPU/cost + tail latency
rather than throughput (which is now PG-bound).

## libqueen conversion — Step 2: result demux (2026-06-03)

`_process_slot_result` parsed the PG result with nlohmann, demuxed by idx, and
`dump()`-ed per job. Added `_process_results_simd` (simdjson On-Demand parse +
raw-slice demux + in-pass metrics + PUSH partition_updates follow-up) for the
homogeneous pass-through types: **PUSH, ACK, RENEW_LEASE, STREAMS_STATE_GET,
STREAMS_REGISTER_QUERY**. **POP** (per-message `partitionId`/`leaseId` mutation

- lag), **TRANSACTION** (object wrapper), **STREAMS_CYCLE** (nested metrics)
stay on the untouched nlohmann path — converting POP's mutation is the remaining
careful step (differential test).

Correctness: 16 concurrent single-item pushes (forces batched merge + result
demux) each returned exactly their own 1 result; pop payload round-trip exact;
explicit ACK returns correct `success:true` shape; `non2xx=0`.

### Cumulative cpu/1k-msg across the libqueen stages


| config   | original | +fire | +result   | total |
| -------- | -------- | ----- | --------- | ----- |
| 0:0 @1KB | 6.73     | 4.68  | **3.52**  | −48%  |
| 0:0 @4KB | 16.47    | 12.02 | **10.19** | −38%  |
| 1:1 @1KB | 4.52     | 2.59  | **1.54**  | −66%  |
| 1:1 @4KB | 11.09    | 3.87  | **2.39**  | −78%  |


Throughput (msg/s): 1:1 @1KB 24,981 → **36,447** (+46%); 1:1 @4KB 15,330 →
**27,230** (+78%).

On the PUSH (1:1) path the profile now shows **nlohmann = 0.2%** (the simd
structural indexer at ~8.6% replaced the old ~32% nlohmann parse); the broker
runs ~0.6 cores at 1:1 (56–65% of 3) — IO/sys is now 58%, i.e. truly
network/PG-bound.

### Still on nlohmann

Result side: **POP / TRANSACTION / STREAMS_CYCLE**; plus `_process_custom_result`
(low-frequency admin/metrics). POP is the meaningful remaining one (frequent,
payload-heavy result) and needs its message mutation ported with a differential
test before flipping it to simd.

## libqueen conversion — Step 3: POP + TRANSACTION (2026-06-03)

- **TRANSACTION** (`_process_transaction_simd`): wrapper object delivered to the
job verbatim; metrics credited from `results[]` via simdjson. (TXN is batch=1.)
- **POP** (`_process_pop_simd`): the hard one. Demuxes result_items by idx;
per item it preserves the **long-poll parking** control flow, stamps each
message's `partitionId`/`leaseId` from the parent **only if absent** (so v4
self-describing multi-partition batches keep their own ids), records
pop/lag/auto-ack metrics, and emits the result. The result_item is passed
through RAW except the `messages` array, whose byte span is **spliced** with
the stamped version — so every other field is preserved byte-for-byte and
message payloads are never value-parsed (only top-level fields are probed).

Validated end-to-end:

- single-partition pop: valid JSON after splice, `partitionId`+`leaseId`
stamped, payload intact (escaped quotes + nested arrays);
- multi-partition wildcard pop: each message carries its partitionId;
- **16 concurrent pops** each returned exactly their own partition's message
(batched POP demux correct);
- empty pop → `messages:[]`; **long-poll** (`wait=true`) parks and returns at
~timeout (not immediately, not hung);
- autoAck pop; TRANSACTION push op → verbatim `{success,results,transactionId}`.

### libqueen hot path is now simdjson end-to-end

PUSH, ACK, POP, TRANSACTION, RENEW_LEASE, STREAMS_STATE_GET/REGISTER — both the
fire-side merge and the result demux — are simdjson. Remaining nlohmann in
libqueen: `**_process_custom_result`** (CUSTOM / `update_partition_lookup_v1`
follow-ups) and **STREAMS_CYCLE** result metrics — lower-frequency paths. (The
dashboard/services layer in `server/src/services/`* still uses nlohmann; that's
outside libqueen.)

## Caveats

- Bucket sums are **regex heuristics** over symbol names; treat them as
±a couple of points, and confirm against the Top-40 list.
- nlohmann is header-only, so its code inlines into the route TUs; `-g` keeps
the `json.hpp` source lines visible, but some JSON cost shows up as ALLOC and
libstdc++ string ops rather than `nlohmann::` symbols.
- This measures **attribution** (what fraction of CPU). To measure
**recoverable** CPU, pair it with an A/B run after applying the fixes
(single `dump()`, allocation-free `thread_local` UUID) and compare msg/s and
broker CPU at the same load.

