# CM-BENCH — fair matrix at matched lane counts, 2026-08-02

Supersedes `../2026-08-02-validation/RESULTS.md`, whose Queen row was **not
comparable**: the Queen adapter silently ignored `-lanes`, so Queen ran 1000 real
partitions against Kafka's 200 and RabbitMQ's 100. Fixed; every system below was
run at the same two lane counts.

| | |
|---|---|
| broker VM | 104.248.245.59 — 8 vCPU, 16 GB, no per-container CPU caps |
| loader VM | 159.89.104.168 — same spec, stayed under 8% CPU throughout |
| workload | R = 2000 ev/s, P = 1000 properties, 180 s + 20 s ramp + 90 s drain |
| demanded | 12 000 deliveries/s, 325 ordered lanes of concurrency |
| lane counts | 200 and 1000 ordered lanes per topic, for every system |

CPU is the mean over the **active window** (samples above a 0.3-core floor), so all
eight rows are computed identically.

## The matrix

| system | lanes | broker cores | e2e p50 | p95 | p99 | disk write | lanes provisioned | consumers | served the rate |
|---|---|---|---|---|---|---|---|---|---|
| **Kafka** | 200 | **2.05** | 143 ms | 170 | 185 | 5.1 MB/s | 800 | 48 | yes |
| **Kafka** | 1000 | **2.41** | 143 ms | 170 | 202 | 13.5 MB/s | 4000 | 48 | yes |
| **Queen** | 200 | 6.10 (1.70 broker + 4.39 PG) | 262 ms | 572 | 809 | 12.3 MB/s | 800 | 96 | yes |
| **Queen** | 1000 | 5.97 (1.67 + 4.29) | **1049 ms** | 3846 | 6469 | 13.8 MB/s | 4000 | 96 | yes |
| **pgmq** | 200 | 7.13 | 340 ms | 680 | 882 | 28.9 MB/s | 2400 | 96 | yes |
| **pgmq** | 1000 | 7.10 | 262 ms | 441 | 524 | 29.3 MB/s | 12000 | 96 | yes |
| **RabbitMQ** | 200 | 6.90 | **93 ms** | 110 | 131 | 2.0 MB/s | 2400 | 2400 | yes |
| **RabbitMQ** | 1000 | 5.18 | 262 ms | 19952 | 43515 | 75.5 MB/s | 12000 | 12000 | **no** — 1799/1900 published, 107 544 undrained |

All eight PASS the correctness contract: 0 gaps, 0 order violations, 0 duplicates.

## The finding: only two of the four pay for lane count

Same rate, same work, only the number of ordered lanes changes:

| system | 200 → 1000 lanes | CPU change |
|---|---|---|
| Kafka | 143 → 143 ms — **flat** | 2.05 → 2.41 |
| pgmq | 340 → 262 ms — **improves** | 7.13 → 7.10 (flat) |
| **Queen** | 262 → **1049 ms** — **4× worse** | 6.10 → 5.97 (flat) |
| **RabbitMQ** | 93 ms → cannot keep up | 6.90 → 5.18 |

**Queen's lane cost is pure latency, not CPU.** The CPU is identical at 200 and 1000
lanes; what changes is the queue. At 1 msg/s per partition the broker must perform
roughly one partition-visit per message delivered, the ready set stands at ~730
partitions (from the broker's own `hotlist` telemetry), and at a service rate of
~1000 visits/s per stream that is ~730 ms of waiting. Halve the lanes and the ready
set halves with it.

The mechanism is in the SQL: the claim loop in `server/sql/procedures/004_log_pop.sql`
is **serial per partition** (`EXIT WHEN v_remaining <= 0 OR v_claimed >= v_max_parts`,
one `log_pop_v1` call per partition), so claiming N partitions costs N segment reads.
Queen's unit of work is the partition-visit, and a visit yields `rate/partitions`
messages. July 2026 ran the same shape at 25k ev/s over the same 1000 partitions —
12.5 messages per visit, 12.5× more efficient on identical code.

pgmq is the control that proves this is architectural rather than substrate: same
Postgres, same durability, same `postgres.conf`, and it is **flat** across lane
counts, because a pgmq group is a header value and an index, not an object.

## What each system is really trading

**Kafka** is 3× cheaper than everything else on CPU and completely insensitive to
partition count — but at a weaker durability tier. Measured separately on this rig:
forced to `flush.messages=1` (fsync per record, the tier that matches Postgres
`synchronous_commit=on`) it served only ~1250 ev/s of the 2000 offered, with e2e p50
at 31 s. Kafka's real durability answer is replication across a cluster, which a
single node cannot express, so the honest reading is "matched to PG's durability on
one node, Kafka costs 40% of its throughput", not "Kafka is slow".

**RabbitMQ** is the fastest at 200 lanes, at 6.90 cores and 2400 consumers. At 1000
lanes it needs 12 000 queues and 12 000 consumers, and it stops keeping up: 75.5 MB/s
of disk (37× its 200-lane figure), a 107 544-message backlog it never drained, and a
p99 of 43 s behind a still-healthy-looking p50. This is the clearest "cannot express
the workload at this cardinality" result in the campaign.

**pgmq** costs the most CPU (7.1 cores, near the 8-core ceiling) and the most disk
(29 MB/s) because of the fan-out it has to materialise: 6 physical inserts per ingress
event against Queen's and Kafka's 2, plus an UPDATE and a DELETE per delivery. It buys
lane-count insensitivity with that.

**Queen** sits between them and its position depends entirely on lane density: at 200
lanes it beats pgmq on the same substrate (262 vs 340 ms) at less CPU (6.10 vs 7.13);
at 1000 lanes pgmq beats it 4×.

## The cardinality curve — Queen, fixed rate, lanes swept

R = 2000 ev/s, P = 1000 properties, only the lane count varies (90 s runs):

| lanes | e2e p50 | p95 | p99 | pops | msgs/pop |
|---|---|---|---|---|---|
| 50 | 220 ms | 371 | 1360 | 153 691 | 6.5 |
| 100 | 286 ms | 481 | 1049 | 134 200 | 7.5 |
| 200 | 371 ms | 742 | 1144 | 119 268 | 8.4 |
| 500 | 441 ms | 1049 | 1617 | 113 831 | 8.8 |
| 1000 | 1617 ms | 5932 | 11863 | 103 173 | 9.7 |

Latency rises monotonically with lane count while the message total is constant
(~1.00 M every run) — Queen is paying for lanes, not for messages.

### Why no client knob could ever fix it

The claim loop in `server/sql/procedures/004_log_pop.sql` is a serial `FOREACH` over
candidates: one `log_pop_v1` call per partition. So a pop's round-trip time is
**linear in the number of candidates** it claims. Then:

```
pops/s        = concurrency / (k · per_entry_cost)
entries/pop   ∝ k
entries/s     = concurrency / per_entry_cost      <- k cancels
```

**Entry-service rate is invariant to every client-side knob.** That is the algebraic
reason `pop-partitions` 10 → 512, batch 100 → 512 and workers 96 → 24 all left p50
pinned: they change `k`, and `k` cancels. Latency is then half a lap of the
entry ring, `entries / entry_service_rate`, and `entries` = stages × lanes.

Measured entry-service rate on this box: ~13 000 entries/s. At 12 stages × 1000 lanes
= 12 000 entries that is a ~0.9 s lap, ~450 ms of waiting per consumer hop, two
consumer hops per flow, plus 45 ms of simulated work — **~945 ms predicted against
961 ms measured**. At 50 lanes the ring is 20× smaller and the floor collapses to
220 ms.

### The product-level fix

Raise the entry-service rate, which means making the claim path **set-based instead
of serial**: claim N partitions in one query rather than N sequential `log_pop_v1`
calls. Nothing on the client can substitute for it. Everything else — dense
partitions, fewer lanes, higher rate — is working around the same constant, which is
exactly why the July run at 25k ev/s over the same 1000 partitions looked healthy: at
12.5 messages per visit the ring lap is amortised 12.5×.

## Ruled out along the way

Two hypotheses were tested and **refuted by measurement**, which is worth recording so
nobody re-litigates them:

- **CPU caps.** The broker container had been capped at 2 cores. Measured cgroup
  throttling: 56 periods out of 17 965 (0.3%). Removing the caps bought ~16% latency
  and half a core. Real, but not the cause. The caps are gone anyway — a rigid 2+6
  split is not the same offer as the single 8-core limit the other systems get.
- **Vegas congestion window.** The broker's adaptive DB-lane limiter reported
  `vegas_pop=5` against defaults of init 16 / min 4 / max 64, and July's tuning used
  512/2048, so this looked like the answer — a 42-agent code investigation also
  ranked it the #1 root cause. Raising it to 64/512/2048 (verified in the boot log)
  made latency **worse**: p50 809 → 1360 ms with the pop count unchanged. Reverted.
  The limiter is real and it is a queue in front of the measured span, but it is not
  the floor.
- **Hot-list wheel park.** `hotlist.rs:1119` parks a manual-ack entry for up to
  exactly 1000 ms, numerically degenerate with the 0.9 s ring lap. The cardinality
  sweep separates them: a wall-clock park would hold p50 near 900 ms at any lane
  count, and instead p50 fell to 220 ms at 50 lanes. Refuted.
- **Long-poll backoff, spool drain, `last_write_at` quantization.** All excluded by
  the broker's own telemetry (`parked=0`, `pop_empty_pct=0`, `buffered=false`).

## Harness defects found and fixed today

1. **Queen adapter ignored `SetupOpts.PhysicalLanes`** — the whole reason the previous
   table was incomparable. Fixed: properties now hash onto lanes exactly as they do
   for Kafka and RabbitMQ.
2. **pgmq adapter had the same defect** — it pinned one group per property. Found by
   auditing all four adapters after fixing Queen. Fixed the same way.
3. **Queen adapter ignored `Reset`** — a second Queen run inherited the first run's
   messages and cursors, producing 32 640 phantom order violations. Fixed.
4. **The readiness probe was killing RabbitMQ.** `docker exec rabbitmq-diagnostics`
   runs as root; if it lands before the server has created `/var/lib/rabbitmq/.erlang.cookie`,
   root creates it 0400 and the server (uid 999) dies with `eacces`. Hours of "RabbitMQ
   won't boot" were self-inflicted. Readiness is now a TCP probe from outside the
   container, and the compose carries no named volume under `/var/lib/rabbitmq`.
5. **The verifier passed empty runs.** An empty log has no gaps and no violations.
   Now any stream that recorded nothing fails the run.
6. **Postgres prunes unreferenced CTEs**, so the pgmq publish statement returned
   success while inserting nothing. Every CTE is now referenced by the final SELECT.
7. **The Kafka adapter silently DROPPED refused batches.** On a handler error it
   skipped the offset commit and continued — but franz-go tracks its consumed
   position in memory, so the next poll returns the NEXT records and the refused
   chunk is never replayed. Queen, pgmq and RabbitMQ all genuinely redeliver, so
   Kafka was the only system that could lose a batch and score better for it. Fixed
   with an explicit `SetOffsets` back to the chunk's start offsets.
8. **pgmq's connection pool was smaller than Queen's** (64 against Queen's
   `DB_POOL_SIZE=160`) on a `postgres.conf` allowing 400 — and pgmq's ceiling set the
   campaign's design point. Raised to 160 and re-measured: pgmq still saturates at
   ~2400 ev/s and the pool only reached 90 connections, so the pool was **not** the
   constraint and R = 2000 stands. Real defect, no impact on the result.
9. **The sampler emits negative cores when a container disappears** (missing cgroup
   file reads as 0, so the delta goes negative). Not yet fixed in the script; the
   numbers above filter those samples out. Fix before the next campaign.

## What this does NOT yet establish

- One rate only (2000 ev/s). No ceiling for any system at matched lanes.
- Two cardinality points. Enough to show the trend, not enough for the two-term cost
  model (`mCPU ≈ a·msgs + b·lanes + c`) that SPEC.md §6.3 asks for.
- Queen's targeted pop mode was not re-run after the lanes fix; the earlier targeted
  numbers came from a naive sweeper and should not be quoted.
- RabbitMQ ran classic queues only. The quorum-queue tier is untested.
