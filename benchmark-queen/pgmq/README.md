# Queen vs pgmq benchmark (Docker: PgBouncer + Postgres + pgmq)

A self-contained Docker stack and load harness to benchmark **pgmq** under the
same realistic queue workloads we test Queen with, so the two are directly
comparable.

```
harness ──(N client conns)──▶ PgBouncer (transaction pool) ──▶ Postgres + pgmq
 pgmq-bench.js                      :6432                            :5432
```

## Why PgBouncer is in the picture (read this first)

pgmq is a **library/extension**, not a broker. To drive it at high client
concurrency you either open one Postgres connection per client (a wall — each
connection is a PG backend process) or you put a pooler in front. The honest,
production-shaped setup is **PgBouncer in transaction mode**.

The whole point of the comparison:

> **Pooling ≠ coalescing.** PgBouncer *reuses* a small set of backends, but it
> does **not** merge 50 independent `send`s into one `INSERT`. Each logical
> operation stays its own statement + transaction. Queen's broker coalesces N
> concurrent requests into a single stored-procedure call. That difference is
> exactly what these runs measure.

## What's fair here

- **Identical durability**: both run on Postgres with `synchronous_commit=on`
  (WAL fsync on commit). This is the cleanest durability parity in the whole
  benchmark suite — unlike Kafka's `acks=1`.
- **Same PG tuning** as the Queen runs (autovacuum aggressive, etc.).
- **pgmq at its best**: batched `send_batch`, `read(qty)` batch reads, read+delete
  in a single round-trip, a GIN FIFO index for grouped reads.
- **Honest handicap**: Queen carries an HTTP/JSON hop that pgmq (SQL/libpq) does
  not. So pgmq will likely win on CPU-per-message and single-op latency. The
  thesis is **throughput-per-connection under concurrency** + **high-cardinality
  ordering**, not CPU efficiency.

## Two modes

| Mode | Producer | Consumer | Models |
|------|----------|----------|--------|
| `fifo` (default) | `pgmq.send(... headers x-pgmq-group=<gid>)` to one of `NUM_PARTITIONS` groups | `pgmq.read_grouped_head(q, vt, qty)` + `delete` | **Ordered, high-cardinality** — the apples-to-apples vs Queen partitions. `read_grouped_head` = one msg per group, up to `qty` groups, `SKIP LOCKED` (the analog of Queen's multi-partition pop). |
| `plain` | `pgmq.send_batch` | `pgmq.read(q, vt, qty)` + `delete` | **SQS-style competing consumers** — pgmq's best case, no ordering. |

## How it maps to the Queen harness

| Queen (`run_test_v3.sh`) | pgmq here |
|---|---|
| autocannon connections | `CONNECTIONS` (per role) closed-loop virtual clients |
| `MSGS_PER_PUSH` (1–10) | `MSGS_PER_PUSH` |
| pop `?batch=100` | `READ_QTY` (plain: msgs/read; fifo: groups/read) |
| `MAX_PARTITION` (1000) | `NUM_PARTITIONS` (fifo groups) |
| `POST /push` → `push_messages_v3` | `pgmq.send` / `send_batch` |
| `GET /pop` → `pop_unified_batch_v4` | `pgmq.read` / `read_grouped_head` + `delete` |

## Requirements

- Docker + Docker Compose v2
- Node 24 (the runner sources `nvm use 24`)

## Quick start

```bash
cd benchmark-queen/pgmq

# Smoke test (short, laptop): ordered, 1000 groups, 50 conns, 30s
MODE=fifo CONNECTIONS=50 DURATION=30 ./run.sh smoke

# The real comparison axis — the "connection wall" sweep (run on the 32 vCPU host):
for C in 100 250 500 1000; do
  MODE=fifo CONNECTIONS=$C MSGS_PER_PUSH=10 READ_QTY=100 \
  NUM_PARTITIONS=1000 DURATION=900 ./run.sh "wall-c${C}"
done

# High-cardinality ordering axis (fixed concurrency, sweep partitions):
for P in 1 100 1000 10000; do
  MODE=fifo CONNECTIONS=500 NUM_PARTITIONS=$P DURATION=900 ./run.sh "card-p${P}"
done

docker compose down -v   # tear down
```

## For the real run (match the Queen host)

The Queen Kafka/RabbitMQ runs used a 32 vCPU / 62 GiB host. Match it:

```bash
PG_SHARED_BUFFERS=24GB PG_EFFECTIVE_CACHE=48GB docker compose up -d
# and bump default_pool_size in pgbouncer/pgbouncer.ini if you want pgmq to have
# more backend headroom (then compare msg/s *per active backend*).
```

## Output

Each run writes `results/<run_name>/`:

- `producer.json` / `consumer.json` — result (msg/s, p50/p90/p99/avg/max latency, errors)
- `metrics.csv` — 1 Hz samples: **active/total PG backends**, **dead tuples**,
  ins/upd/del, autovacuum runs, queue depth
- `pgmq-version.txt`, `final-sizes.txt`

The runner prints a summary with the three metrics that matter together:
**msg/s + active PG backends + p99**, plus peak dead tuples / autovacuum runs
(the bloat story).

## The charts to build from this

1. **Connection wall**: x=client concurrency (100→1000), y1=msg/s, y2=active PG
   backends. Queen stays flat ~2.5 backends; pgmq is pinned at `default_pool_size`
   with p99 climbing as concurrency rises.
2. **High-cardinality ordering**: x=partitions/groups (1→10000), y=msg/s. Queen
   degrades sublinearly; watch where `read_grouped_head` bends.
3. **Bloat**: x=time, y=dead tuples + autovacuum runs. pgmq UPDATEs vt + DELETEs
   per message; Queen never mutates the messages table.

## Notes / caveats

- **FIFO support**: `read_grouped_head` / `create_fifo_index` are recent pgmq
  features. If `create_fifo_index` fails at setup, the image is too old — pin a
  newer `PGMQ_IMAGE` (e.g. `ghcr.io/pgmq/pg17-pgmq:latest`). The runner records
  the detected version + grouped-read functions in `pgmq-version.txt`.
- `auth_type = trust` in PgBouncer is **only** because this is a throwaway local
  network. Never reuse this config anywhere real.
- This measures broker capacity (read+delete with no artificial processing time).
  Set `PROCESS`-like delays at the app layer if you want to model worker-bound
  regimes — but per Queen's own "throughput numbers can mislead" point, that
  shifts the bottleneck off both systems equally.
