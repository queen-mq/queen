# queen-seg-rust — Rust broker for the Storage-v2 (segments) engine

A full re-implementation of the Queen hot path (push / pop) in async Rust
(`tokio` + `axum` + `deadpool-postgres`) on top of the **storage-v2 "segments"**
SQL architecture, with **cross-request fusion** ported from the C++ broker.

Business logic stays in PostgreSQL stored procedures (`queen.seg_*`); this crate
is the network/broker layer: HTTP, message framing, zstd, fusion, adaptive
concurrency, and the SP calls.

## Why segments + fusion

The segments engine stores messages as **zstd-compressed segments** — one blob
per *K* messages — in `queen.seg_segments`, instead of one row per message. A
single `INSERT` (one commit, one WAL fsync) therefore carries hundreds of
messages. **Cross-request fusion** is what fills those segments: it coalesces
frames from *many concurrent push requests to the same `(queue, partition)`*
into one segment before flushing. The number of **commits/s** is the real
throughput limiter, and fusion is what keeps it low.

## Architecture

```
HTTP (axum)
  ├── POST /api/v1/push
  │     parse items (RawValue payload, zero-copy) → group by (queue,partition)
  │     → submit frames to a fusion shard → park on oneshot until committed
  │
  └── GET  /api/v1/pop/queue/:queue   (wildcard, multi-partition, autoAck)
        → seg_pop_wildcard_wire_v1 → base64→zstd→unpack frames
        → response built by hand, payload spliced raw (no re-serialize)

Fusion (fusion.rs)  — N sharded actor tasks (default 16), keyed by
  hash(queue,partition). Each shard owns HashMap<partkey, FusionGroup> and:
    • flushes a group when it reaches QUEEN_V2_FUSION_FRAMES, OR
    • flushes on the QUEEN_V2_FUSION_HOLD_MS timer.
  Each flush is a spawned task gated by the push Vegas limiter: pack frames →
  zstd → seg_push_segment_wire_v1 (blob as BINARY bytea). Contributing push
  requests are woken (oneshot) when their segment commits.

Vegas (vegas.rs) — TCP-Vegas-style adaptive concurrency limiter (a dynamically
  resized tokio Semaphore) per lane (push / pop): grows the in-flight limit
  while not queueing, shrinks when PG RTT rises.

Frames (frames.rs) — length-prefixed frame codec, wire-compatible with the C++
  broker (server/include/queen/storage_v2.hpp), so push/pop interoperate.
```

## Wire format (frame)

Little-endian, one segment = K frames zstd-compressed together:

```
u32 body_len
body: u8 flags | u8[16] message_id | [u8[16] trace_id]
      | u16 txn_len | txn | [u16 psub_len | psub] | payload(JSON bytes)
flags: 1=trace 2=producer_sub 4=encrypted
```

## Endpoints

- `POST /api/v1/push` — body `{"items":[{queue,partition,payload,transactionId}]}`
- `GET  /api/v1/pop/queue/:queue?batch=&partitions=&autoAck=&wait=&timeout=&consumerGroup=`
- `GET  /status`
- `GET  /metrics` — Prometheus (push/pop req + msg totals, fusion items/batch,
  batch RTT p50/p99, `queen_seg_{push,pop}_vegas_limit`)

The queue must already exist with `storage=segments` (configure it once via the
C++ broker's `/api/v1/configure`; the push SP auto-creates partitions).

## Configuration (env)

| var | default | meaning |
|-----|---------|---------|
| `PORT` | 6632 | listen port |
| `PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DATABASE` | localhost/5432/postgres | PG connection |
| `DB_POOL_SIZE` | 160 | deadpool size (shared push+pop) |
| `QUEEN_V2_ZSTD_LEVEL` | 3 | zstd compression level |
| `QUEEN_V2_FUSION_SHARDS` | 8 | fusion accumulator shards |
| `QUEEN_V2_FUSION_FRAMES` | 500 | flush a group at this many frames |
| `QUEEN_V2_FUSION_HOLD_MS` | 15 | flush a group after this hold |
| `QUEEN_SEG_PUSH_INIT/MIN/MAX` | 16/4/64 | push Vegas bounds |
| `QUEEN_SEG_POP_INIT/MIN/MAX` | 16/4/64 | pop Vegas bounds |
| `QUEEN_VEGAS_ALPHA/BETA` | 3/6 | Vegas grow/shrink thresholds |
| `POP_DEFAULT_TIMEOUT_MS` | 2000 | pop long-poll timeout |
| `POP_WAIT_POLL_MS` | 25 | pop long-poll re-poll interval |

## Build & run

```bash
cargo build --release          # local
docker build -t queen-seg-rust .   # container (bin: /queen-seg)

docker run --rm -p 6682:6632 \
  -e PG_HOST=... -e PG_PASSWORD=... \
  -e QUEEN_V2_FUSION_FRAMES=500 -e QUEEN_V2_FUSION_HOLD_MS=30 \
  queen-seg-rust
```

## Benchmark results (DigitalOcean, 32-core broker VM + 32-core loader VM)

Fresh PostgreSQL per run, `dedup=0`, retention off, payload 256B, push-batch
100, pop-batch 500, wildcard pop `partitions=10`, `synchronous_commit=on`.
Loader (`goload`) drives the broker over the private network.

**Reference config "A"** — 100 partitions, fusion hold 30ms, `commit_delay=200µs`,
1000 producers / 600 consumers (see `runAcd.png`):

| metric | push | pop |
|--------|------|-----|
| messages/s | **~892k** | **~896k** |
| requests/s | ~9,000 | ~1,800 |

- **~1.6M msg/s combined**, sustained, `pushErr=0 popErr=0`
- fusion **~396 messages/segment**, **~8,100 commits/s**
- CPU: **broker ~8 cores, Postgres ~5.6 cores** (of 32 each) — large headroom
- push RTT p50 6.0ms / p99 61ms · pop RTT p50 7.0ms / p99 19ms

### Tuning study (what we learned)

| experiment | msg/seg | commits/s | throughput | note |
|------------|---------|-----------|-----------|------|
| 100 part, hold 15ms | 276 | 10,200 | 1.6M | baseline |
| **100 part, hold 30ms** | **396** | **8,100** | **1.6M** | best — bigger segments, PG relieved |
| 200 part, hold 30ms | 271 | 10,000 | 1.55M | more partitions weaken fusion |
| 1000 part, hold 15ms | 100 | 16,000 | 1.2M | fusion effectively off → PG WAL-bound |

**Bottleneck**: commit throughput = WAL fsync + per-partition serialization
(`Lock:transactionid`, `LWLock:WALWrite`), **not** broker/loader CPU. More
partitions *hurt* because they dilute fusion (fewer concurrent requests per
partition → smaller segments → more commits). The winning direction is *fewer
partitions + longer fusion hold* → bigger segments → fewer commits.
`commit_delay` was marginal (group commit already happens naturally under this
concurrency). The remaining lever to raise the ceiling is relaxing durability
(`synchronous_commit=off`), left as a deliberate trade-off.

## Scripts

- `setup-broker.sh` — (broker VM) fresh PG + tuned broker + background monitor → `/tmp/mon.csv`
- `run-load.sh` — (loader VM) drive `goload` over the private network + mpstat
- `smoke-rust.sh` — end-to-end correctness check (push known payloads → pop → compare)
- `debug-pop.sh` — direct SP vs broker pop comparison
- `plot_run.py` — render `/tmp/mon.csv` into the 4-panel throughput/commit/CPU chart
