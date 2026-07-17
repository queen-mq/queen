# go-hotpath-spike

A **Go reimplementation of Queen MQ's hot path** (`push` / `pop` / `ack`) that
keeps the PostgreSQL **stored procedures AS-IS** and faithfully replicates
libqueen's **batch-fusion** engine and the **exact benchmark tuning**. Its only
purpose is to answer one question with data:

> If we rewrite the broker in Go (well), keeping the SQL untouched, how close to
> the C++ broker's throughput do we get?

It is **not** production-complete (see [Scope](#scope)). It is a measurement tool.

---

## Why this is a fair test

- **Same SQL.** It calls the identical procedures the C++ broker calls:
  `queen.push_messages_v3($1)`, `queen.pop_unified_batch_v4($1)`,
  `queen.ack_messages_v2($1)`, plus the `partition_lookup` follow-up
  (`update_partition_lookup_v1`) and reconcile safety net that wildcard pop
  depends on. The correctness-critical logic is untouched.
- **Same HTTP contract.** The existing clients and the `goload` loader hit it
  **unmodified** (`/api/v1/push`, `/api/v1/pop/queue/{q}[/partition/{p}]`,
  `/api/v1/ack`, `/api/v1/ack/batch`, `/api/v1/configure`, `/api/v1/status`).
- **Same fusion.** `batcher.go` reproduces libqueen's `_fire_batched`: it merges
  N same-type requests into one SP call, front-injecting a renumbered
  `idx`/`index` per item, records per-job idx ranges, and demuxes the fused
  result back to each caller. Payloads pass through **raw** (no
  re-serialization), exactly like the C++ simdjson path.
- **Same tuning.** Defaults are the values captured in the 24h soak
  (`benchmark-queen/.../start-broker.sh`, `queen-inspect.json`):

  | Type | preferred (jobs) | max_hold | max_batch (jobs) | max_concurrent |
  |------|------------------|----------|------------------|----------------|
  | PUSH | 50               | 20 ms    | 500              | 24             |
  | POP  | 20               | 5 ms     | 500              | 16             |
  | ACK  | 50               | 20 ms    | 500              | 16             |

  Shared pool `DB_POOL_SIZE=50`. Every knob is overridable via the **same env
  var names** the C++ broker uses (`QUEEN_PUSH_MAX_CONCURRENT`, …).

Sizes are counted in **jobs** (HTTP requests), matching libqueen's
`BatchPolicy::should_fire()` `queue_size` — not in messages.

## Scope (what is intentionally NOT here)

Auth/JWT, encryption, DLQ/consumer-group dashboards, streams, traces, metrics
endpoints, file-buffer failover, maintenance mode, long-poll parking, and
retention *enforcement* (retention is left to Postgres/ops for the spike;
`goload -mode max` uses server-side `autoAck`, so the ACK endpoint is only lightly
exercised). None of these affect the push/pop throughput being measured.

## Metrics & fusion

`GET /metrics/prometheus` exposes counters whose names line up **1:1 with the
C++ broker** (`lib/queen/metrics.hpp` + `server/src/routes/prometheus.cpp`), so
the same scrape/diff computes req/s and msg/s for both:

- `queen_cluster_{push,pop,ack}_requests_total`, `..._messages_total`, `queen_pop_empty_total`
- `queen_batches_fired_total{op=}`, `queen_batch_items_fired_total{op=}` — the
  **fusion ratio** is `items/batches` = items per stored-procedure call
- `queen_fusion_items_per_batch{op=}` (lifetime gauge), `queen_batch_rtt_milliseconds{op=,quantile=}`
- `queen_process_resident_memory_bytes` (RSS via `/proc` on Linux)

The **PG-level fusion** (messages per commit) is measured identically for both
brokers via Postgres itself — `pg_stat_database.xact_commit` — matching the soak
methodology (*"~220 messages / ~12 HTTP calls per PG commit"*). `compare.sh`
captures all of this automatically and prints, per run:

```
  req/s:  push=…  pop=…
  msg/s:  push=…  pop=…
  PG commits/s=…  fusion(push msgs/commit)=…
```

Local sanity check (laptop, pool 20): push fully fuses to the 500-item cap, pop
coalesces **~12 requests per SP call**, and Postgres saw **~183 messages per
commit** — squarely in the soak's ballpark, confirming the fusion is faithful.

## Build & run

```bash
cd server-go
go build -o bin/go-hotpath ./...     # isolated go.work; does not touch repo-root workspace

PG_HOST=localhost PG_PORT=5432 PG_PASSWORD=postgres DB_POOL_SIZE=50 PORT=6632 \
  ./bin/go-hotpath
```

Postgres must have the Queen schema + procedures loaded
(`lib/schema/schema.sql` then `lib/schema/procedures/*.sql` in sorted order).

Docker (for a docker-stats-parity comparison against the C++ image):

```bash
docker build -t go-hotpath-spike .
```

## Reproduce the comparison

```bash
./compare.sh                 # fresh PG + schema, runs C++ then Go, prints stats
DURATION=60 PARTITIONS=300 ./compare.sh
./compare.sh teardown        # remove containers
```

Both brokers run **sequentially** against the **same** Postgres so PG is never
contended between them.

## Preliminary result (local smoke — NOT the real benchmark)

macOS laptop, Postgres-in-Docker, `DB_POOL_SIZE=20`, 100 partitions / 100
producers / 60 consumers / pushBatch 10 / popBatch 200, 20 s balanced:

| Broker        | msg/s (balanced) | broker RSS | broker CPU | PG CPU  |
|---------------|------------------|------------|------------|---------|
| C++ 0.16.0    | ~15k (13–20k)    | 46 MiB     | ~1.0 core  | ~3.7 core |
| Go hot-path   | ~14k (12.7–16.2k)| 43 MiB     | ~0.6 core  | ~4.5 core |

Zero loss on both (`pop ≈ push`, `errs=0`). **Postgres is the wall** (≈4 cores)
while both brokers idle at ~1 core — so the two land at **parity**, which is the
whole point: with the SQL unchanged, the broker language does not move the
ceiling. Absolute numbers are laptop/Docker-Postgres-limited and understate both
brokers equally; they are **not** comparable to the 32-core, ~119k-msg/s soak.

### To make it "defensible"

Run `compare.sh` on the actual 32-core benchmark droplet, with `DB_POOL_SIZE=50`
and the real PG tuning, multiple runs + warmup, and capture the same
`docker stats` CPU/mem and the fusion ratio (messages per PG commit). That is
the number that decides the C++-vs-Go question; this spike shows the plumbing is
correct and the ratio holds.
