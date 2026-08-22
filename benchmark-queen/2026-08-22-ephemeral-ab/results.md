# Ephemeral vs durable, same broker, same machine — 2026-08-22

Apple M4, 10 cores, 24 GB, macOS 15.5. Broker `1.1.0` built from `8b613b6d`, run as a local
release binary on port 6642. PostgreSQL 16 in Docker on port 5456, **data directory on tmpfs**.

One broker process, one Postgres, both lanes back to back. Same 256-byte payload, same batch
sizes, same partition count, same worker count. The only difference is the route family, and
therefore whether PostgreSQL is in the path.

Both lanes seed their partitions and prime the consumer group *before* the measured phases.
Without that the durable lane measures zero: `subscriptionMode` defaults to `new`, so a group
first created at drain time starts from now and never sees the backlog.

## Throughput, 200k messages, 8 partitions, 8 workers, batches of 100

| | durable | ephemeral | ratio |
| --- | --- | --- | --- |
| push, msg/s | 242k – 326k | 600k – 604k | **1.9x – 2.5x** |
| push call p99 | 24.4 – 28.7 ms | 4.3 – 4.5 ms | **~6x tighter** |
| drain (pop+ack), msg/s | 28.2k – 30.1k | 317k – 355k | **~11x** |
| drain call p99 | 8.2 – 9.1 ms | 4.9 – 5.2 ms | ~1.7x |

Two runs per lane; the spread above is the two runs.

## Delivery latency, one message in flight, long-polled pop

Park a `wait=true` pop, push one message, measure until the pop answers. `autoAck` on both.
300 iterations, 4 partitions.

| | durable | ephemeral | ratio |
| --- | --- | --- | --- |
| p50 | 4.42 ms | 1.06 ms | **4.2x** |
| p95 | 9.07 ms | 2.21 ms | 4.1x |
| p99 | 9.92 ms | 3.14 ms | 3.2x |
| min | 1.94 ms | 0.31 ms | 6.3x |

## High partition cardinality, 50k messages, 500 partitions

The shape the class exists for: many short-lived per-client inboxes.

| | durable | ephemeral | ratio |
| --- | --- | --- | --- |
| push, msg/s | 110k | 340k | 3.1x |
| drain, msg/s | 51.2k | 369k | **7.2x** |

The durable drain is *faster* at 500 partitions (51k/s) than at 8 (30k/s): the claim loop is
serial per partition, so partition count is what buys it parallelism. The ephemeral drain is
flat at ~320–370k/s across both, because there is no claim transaction to serialise.

## Two caveats that cut in opposite directions

**The durable lane is flattered.** Postgres runs on tmpfs here, so a durable push pays no real
fsync. On a real disk the durable numbers fall and every ratio above widens. The durable push
figure in particular (242–326k msg/s) is not a number to quote anywhere.

**The ephemeral lane is throttled by default, and it is not the ring that throttles it.**
`QUEEN_EPHEMERAL_RATE` defaults to 5,000 messages/second per tenant with
`QUEEN_EPHEMERAL_BURST` 10,000, charged per message on a direct push. A 20k-message run at
stock settings dies with `429 rate_limited` after the burst drains — reproduced before these
runs. The durable family has no equivalent default cap. Every number above was taken with the
bucket lifted (`RATE`/`BURST` at 1e8, `QUEUE_MAX_LENGTH` 500k) so the measurement is the engine
and not the admission gate. Out of the box the ephemeral class admits 5k msg/s and the durable
one does not, which inverts the entire comparison for anyone who does not touch the knob.

## Reproduce

```
docker run -d --name queen-eph-bench-pg -p 5456:5432 -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres --tmpfs /var/lib/postgresql/data postgres:16 \
  -c max_connections=200 -c shared_buffers=512MB

PG_HOST=127.0.0.1 PG_PORT=5456 PG_USER=postgres PG_PASSWORD=postgres PG_DATABASE=postgres \
QUEEN_APPLY_SCHEMA=1 PORT=6642 DB_POOL_SIZE=32 \
QUEEN_EPHEMERAL_RATE=100000000 QUEEN_EPHEMERAL_BURST=100000000 \
QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH=500000 QUEEN_EPHEMERAL_QUEUE_MAX_BYTES=536870912 \
QUEEN_EPHEMERAL_MAX_BYTES=4294967296 ./server/target/release/queen

node loadgen.mjs --url http://localhost:6642 --class ephemeral --messages 200000
node loadgen.mjs --url http://localhost:6642 --class durable   --messages 200000
node loadgen.mjs --url http://localhost:6642 --class ephemeral --mode latency --iters 300
```

Raw JSON per run in `raw/`.
