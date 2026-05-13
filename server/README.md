# Queen C++ Server — Build & Tuning Guide

Complete guide for building, configuring, and tuning the Queen C++ message queue server.

## Overview

Queen is a high-performance, **fully non-blocking** message broker built on:
- **uWebSockets** for event-driven HTTP/WebSocket handling
- **libuv** event loops (one per HTTP worker, plus per-`Queen` instance)
- **libpq async API** for non-blocking PostgreSQL I/O
- **Two cooperating connection pools** (see [Architecture](#architecture))
- **TCP-Vegas-style adaptive concurrency control** in libqueen

**Key characteristics:**
- ✅ No blocking on database I/O on any HTTP request path
- ✅ Adaptive batching + concurrency per `JobType` (PUSH / POP / ACK / TRANSACTION / RENEW_LEASE / CUSTOM)
- ✅ Sustains ~104k msg/s push and ~165k msg/s fan-out on a 32-core host (see [benchmarks](../benchmark-queen/2026-04-26/README.md))
- ✅ Broker RSS under 100 MB at peak load
- ✅ File-buffer failover keeps PUSHes durable when PostgreSQL is unreachable

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Building the Server](#building-the-server)
- [Performance Tuning](#performance-tuning)
- [Database Configuration](#database-configuration)
- [PostgreSQL Failover](#postgresql-failover)
- [Queue Optimization](#queue-optimization)
- [Monitoring & Debugging](#monitoring--debugging)
- [Production Deployment](#production-deployment)
- [Benchmarking](#benchmarking)
- [Environment Variables Reference](#environment-variables-reference)

---

## Architecture

### Process layout

```
Client → uWS acceptor (port 6632, single thread)
            │  round-robin LB across workers
            ▼
         uWS worker thread × NUM_WORKERS
            │
            │  HOT PATH (PUSH / POP / ACK / TRANSACTION / RENEW_LEASE)
            ├──▶  per-worker libqueen instance (queen::Queen)
            │       │  - own uv_loop
            │       │  - per-worker slice of the sidecar pool
            │       │  - adaptive Vegas controller per JobType
            │       └──▶  PostgreSQL (async libpq, batched stored procs)
            │
            │  EVERYTHING ELSE
            └──▶  global AsyncDbPool (shared across workers)
                    │  - schema init, retention, eviction
                    │  - stats / analytics, /api/v1/resources/*
                    │  - SharedStateManager queries, /metrics
                    │  - file-buffer flush, streams routes
                    └──▶  PostgreSQL (async libpq)
```

### Two pools, not one

The server keeps **two independent connection pools** with very different roles. This is the single most important thing to understand before tuning.

#### 1. Sidecar pool (`SIDECAR_POOL_SIZE`, default **50**)

The **hot path** — PUSH, POP, ACK, TRANSACTION, RENEW_LEASE — runs through a per-worker [`queen::Queen`](../lib/queen.hpp) instance ("libqueen", a.k.a. "sidecar"). At startup the total `SIDECAR_POOL_SIZE` budget is split evenly across `NUM_WORKERS`, with any remainder given to the first few workers (see `server/src/acceptor_server.cpp:597`).

Each `Queen` instance:
- owns its own libuv event loop and slot pool
- runs an adaptive concurrency controller per `JobType` (Vegas by default, or static)
- amortises round-trips by **fusing in-flight jobs into batches** (configured via `QUEEN_<TYPE>_*` env vars — see [ENV_VARIABLES.md](ENV_VARIABLES.md))
- calls the v3/v4 stored procedures directly: `queen.push_messages_v3`, `queen.pop_unified_batch_v4`, `queen.ack_messages_v2`, `queen.renew_lease_v2`, `queen.execute_transaction_v2`

Even at 100 k msg/s peak load, the Vegas controller typically converges to **~2.5 in-flight queries per worker** (measured in the 2026-04-26 benchmark suite). The default of 50 total connections is therefore more than enough headroom for a single-node deployment; raise it only if Vegas is saturating `QUEEN_<TYPE>_MAX_CONCURRENT` (visible in `/metrics/prometheus`).

#### 2. AsyncDbPool (`DB_POOL_SIZE`, default **150**)

Everything that is **not** PUSH/POP/ACK runs through the global `AsyncDbPool`:

- schema initialization (worker 0 only, at startup)
- `MetricsCollector`, `RetentionService`, `EvictionService`, `StatsService`, `PartitionLookupReconcileService`
- `SharedStateManager` queries (queue config refresh, watermarks, cache warm-up)
- the file-buffer flush job
- `/api/v1/resources/*`, `/api/v1/status/*`, `/api/v1/messages`, DLQ routes, traces, migration
- `/streams/v1/cycle`, `/streams/v1/state`, `/streams/v1/register_query`
- `/metrics` and `/metrics/prometheus`

The pool is created in `server/src/acceptor_server.cpp:278-318` with `floor(DB_POOL_SIZE × 0.95)` connections (the 5 % is a safety margin against PG `max_connections`; nothing else uses those slots). With the default 150 that means **142 async connections** available to background services.

**Sizing rule of thumb:**

```
PG max_connections  ≥  DB_POOL_SIZE  +  SIDECAR_POOL_SIZE  +  (any peers / pgBouncer overhead)
                       └─ background ┘  └─ hot path ──────┘
```

For the default `DB_POOL_SIZE=150 + SIDECAR_POOL_SIZE=50` you want `max_connections ≥ 220` (200 + headroom). If you're behind PgBouncer in transaction mode you only need PG-side capacity for active queries; pool sizes are then bounded by PgBouncer's `default_pool_size`.

### Long-polling and inter-instance notification

`GET /api/v1/pop/...?wait=true` doesn't spawn a dedicated poll-worker thread; it parks the request on the worker's libqueen via the `POP_WAIT_*` backoff loop and is woken either by a local pop arriving on the same worker or by a UDP `MESSAGE_AVAILABLE` packet from a peer instance. The relevant code lives in `lib/queen.hpp` (`pop_backoff_tracker`) and `server/src/services/shared_state_manager.cpp` (UDP sync). Tuning knobs:

- `POP_WAIT_INITIAL_INTERVAL_MS` (default 100)
- `POP_WAIT_BACKOFF_THRESHOLD` (default 3 consecutive empties before backoff)
- `POP_WAIT_BACKOFF_MULTIPLIER` (default 2.0)
- `POP_WAIT_MAX_INTERVAL_MS` (default 1000)

A request that hits `wait_timeout_ms` (client-supplied, capped server-side) returns an empty batch.

### Request flow (concrete example: PUSH)

1. Client posts JSON to `/api/v1/push`.
2. The worker validates input, generates UUIDv7 message IDs, optionally encrypts payloads, and registers a deferred response in the per-worker [`ResponseRegistry`](include/queen/response_queue.hpp) (lock-free across workers).
3. `ctx.queen->submit(job_req, callback)` enqueues the job in the worker's libqueen — returns immediately, no I/O.
4. libqueen's event loop fuses the job with any other pending PUSHes for this worker (subject to `QUEEN_PUSH_PREFERRED_BATCH_SIZE` / `QUEEN_PUSH_MAX_HOLD_MS`) and fires one `SELECT queen.push_messages_v3(...)` against an available slot.
5. The slot's libpq socket FD is in the libuv loop; completion fires a callback that delivers the JSON result back to the worker's uWS loop via `uWS::Loop::defer`.
6. The HTTP response is written.

If step 4 fails (DB down, deadlock, etc.) the stored `items_json` is replayed into the file buffer (`push_failover_storage`) and the client gets `{success: true, failover: true}` instead of an error.

---

## Building the Server

### Prerequisites

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install build-essential libpq-dev libssl-dev zlib1g-dev curl unzip
```

**macOS:**

```bash
brew install postgresql openssl curl unzip
```

Or use the Makefile helpers:

```bash
make install-deps-ubuntu      # Ubuntu
make install-deps-macos       # macOS
```

### Quick Build

```bash
cd server
make all              # download deps + build (recommended for first build)
make build-only       # re-build without re-downloading deps
```

The compiled binary lands at `bin/queen-server`.

### Build Targets

```bash
make all           # Download deps and build
make build-only    # Build without downloading deps
make deps          # Download dependencies only
make clean         # Remove build artifacts
make distclean     # Remove build artifacts and downloaded deps
make test          # Run test suite
make dev           # Build and run in development mode
make help          # Show all available targets
```

### Build Configuration

The Makefile detects:
- OS (macOS / Linux)
- PostgreSQL installation path
- OpenSSL installation path
- Homebrew prefix (on macOS)

Inspect with:

```bash
make debug-paths      # detected library paths
make debug-objects    # build statistics
```

Default flags:

```makefile
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -pthread -DWITH_OPENSSL=1
```

Override per-build:

```bash
# Maximum optimization (native arch)
CXXFLAGS="-std=c++17 -O3 -march=native" make

# Debug build with symbols
CXXFLAGS="-std=c++17 -g -O0" make
```

---

## Performance Tuning

> **Single source of truth for env vars: [ENV_VARIABLES.md](ENV_VARIABLES.md).** This section explains the **why** — the table in `ENV_VARIABLES.md` enumerates every variable read by the server, with code-verified defaults.

### Capacity planning at a glance

| Workload | `NUM_WORKERS` | `DB_POOL_SIZE` | `SIDECAR_POOL_SIZE` | PG `max_connections` |
|---|---|---|---|---|
| Dev / smoke test | 2 | 10 | 10 | ≥ 30 |
| Single-node default | 10 (default) | 150 (default) | 50 (default) | ≥ 220 |
| Heavy fan-out / many consumer groups | 10–20 | 200 | 100 | ≥ 320 |
| Cluster member (3-node) | 10 | 100 (per node) | 50 (per node) | ≥ 320 (DB serves 3 nodes) |

`NUM_WORKERS` is automatically capped at the host's CPU core count.

### Tuning the hot path (libqueen / sidecar pool)

The most impactful knobs are **per-`JobType`**, not the pool size. Each type exposes four variables: `QUEEN_<TYPE>_{PREFERRED_BATCH_SIZE,MAX_HOLD_MS,MAX_BATCH_SIZE,MAX_CONCURRENT}`. The defaults are calibrated from the 2026-04-22 Vegas-uncapped sweep (`benchmark-queen/test-perf/results/sweep_2026-04-22_07-41-19`):

| Type | preferred | max_hold_ms | max_batch | max_concurrent |
|---|---:|---:|---:|---:|
| `PUSH` | 50 | 20 | 500 | 24 |
| `POP` | 20 | 5 | 500 | 16 |
| `ACK` | 50 | 20 | 500 | 16 |
| `TRANSACTION` | 1 | 0 | 1 | 1 |
| `RENEW_LEASE` | 10 | 100 | 100 | 2 |
| `CUSTOM` | 1 | 0 | 1 | 1 |

Defaults rationale (`server/include/queen/config.hpp:209-220`):

- `PUSH` / `ACK` `preferred=50` sits above the S1 break-even (~33 jobs/batch); `max_hold_ms=20` is the sweet spot found by the perf campaign.
- `POP` is latency-sensitive — tighter hold, smaller preferred batch.
- `TRANSACTION` and `CUSTOM` are atomic, so they never fuse: batch size 1, concurrency 1.
- `RENEW_LEASE` is background work: modest batch, longer hold.

If you've raised `QUEEN_<TYPE>_MAX_CONCURRENT` you must keep `QUEEN_VEGAS_BETA < MAX_CONCURRENT` — otherwise the Vegas controller never grows. Default `BETA=12` works up to `MAX_CONCURRENT=24` (`config.hpp:197` + 2026-04-22 sweep notes).

### Tuning the background pool (AsyncDbPool)

`DB_POOL_SIZE` mostly affects:

- recovery throughput from the file buffer (the flush job opens up to `FILE_BUFFER_MAX_BATCH × N` concurrent transactions)
- analytics endpoints under heavy dashboard refresh
- stats reconciliation on queues with millions of messages

Raise it if the dashboard times out, if `StatsService` log lines show `statement_timeout` errors during reconciliation, or if file-buffer flush throughput stalls below ~10k events/sec on a healthy PG. **Raising it will not help raw push/pop throughput**, which is bounded by `SIDECAR_POOL_SIZE` and Vegas.

### Worker thread configuration

```bash
export NUM_WORKERS=10          # default
```

- **Light load**: `NUM_WORKERS=2`, `DB_POOL_SIZE=10`, `SIDECAR_POOL_SIZE=10`
- **Medium load** (default): `NUM_WORKERS=10`, `DB_POOL_SIZE=150`, `SIDECAR_POOL_SIZE=50`
- **Heavy fan-out**: `NUM_WORKERS=20`, `DB_POOL_SIZE=200`, `SIDECAR_POOL_SIZE=100`

`NUM_WORKERS` is silently capped at `std::thread::hardware_concurrency()`.

### High-throughput configuration (single node)

```bash
export NUM_WORKERS=10
export DB_POOL_SIZE=200                # background services + analytics headroom
export SIDECAR_POOL_SIZE=100           # ~10 sidecar conns / worker
export RESPONSE_BATCH_SIZE=200         # response queue tick batch
export RESPONSE_BATCH_MAX=1000

# Per-type knobs (the defaults are usually fine; example shows where to push)
export QUEEN_PUSH_MAX_CONCURRENT=32
export QUEEN_ACK_MAX_CONCURRENT=24
export QUEEN_VEGAS_MAX_LIMIT=48        # must be ≥ any MAX_CONCURRENT you raised
export QUEEN_VEGAS_BETA=16             # must be < smallest MAX_CONCURRENT

./bin/queen-server
```

Expected ballpark on a 32 vCPU host (2026-04-26 benchmarks):

- **104 k msg/s** push, `batch=100`, 1 consumer group
- **165 k msg/s** fan-out, 10 consumer groups
- **39 k msg/s** push, production-realistic `batch=10`
- **broker RSS** ≤ 100 MB at peak

PG memory grows past `shared_buffers` (up to ~1.3× at peak) — plan for at least 1.5 × `shared_buffers` of available host RAM.

### Long-polling tuning

```bash
# Tighter latency (higher CPU)
export POP_WAIT_INITIAL_INTERVAL_MS=50
export POP_WAIT_MAX_INTERVAL_MS=500

# Slower (lower CPU)
export POP_WAIT_INITIAL_INTERVAL_MS=200
export POP_WAIT_MAX_INTERVAL_MS=2000

# Backoff curve
export POP_WAIT_BACKOFF_THRESHOLD=5    # consecutive empty pops before backoff starts
export POP_WAIT_BACKOFF_MULTIPLIER=2.0
```

How it works (`lib/queen.hpp::pop_backoff_tracker`):

1. Initial poll at `POP_WAIT_INITIAL_INTERVAL_MS` (default 100 ms).
2. After `POP_WAIT_BACKOFF_THRESHOLD` consecutive empty results the interval doubles each retry.
3. Capped at `POP_WAIT_MAX_INTERVAL_MS` (default 1000 ms).
4. Reset to the initial interval immediately when a message arrives (locally) or a UDP notification fires.
5. The client-side `wait_timeout_ms` parameter bounds the total time before returning an empty batch.

---

## Database Configuration

### Connection settings

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=postgres
export PG_USER=postgres
export PG_PASSWORD=your_password
```

### SSL configuration

For managed PostgreSQL (Cloud SQL, RDS, Azure Database):

```bash
export PG_USE_SSL=true                       # enable SSL
export PG_SSL_REJECT_UNAUTHORIZED=true       # require valid certs (production)
```

Development with self-signed certs:

```bash
export PG_USE_SSL=true
export PG_SSL_REJECT_UNAUTHORIZED=false      # falls back to sslmode=prefer
```

Local dev (no SSL):

```bash
export PG_USE_SSL=false                      # default → sslmode=disable
```

SSL mode mapping (`config.hpp::DatabaseConfig::connection_string`):

- `PG_USE_SSL=true`  + `PG_SSL_REJECT_UNAUTHORIZED=true`  → `sslmode=require`
- `PG_USE_SSL=true`  + `PG_SSL_REJECT_UNAUTHORIZED=false` → `sslmode=prefer`
- `PG_USE_SSL=false`                                      → `sslmode=disable`

### Pool tuning

```bash
export DB_POOL_SIZE=150                # background-services pool (default 150)
export DB_IDLE_TIMEOUT=30000           # ms before idle conn is recycled
export DB_CONNECTION_TIMEOUT=2000      # ms for initial PG connect (libpq connect_timeout)
export DB_STATEMENT_TIMEOUT=30000      # ms (SET statement_timeout on every conn)
export DB_LOCK_TIMEOUT=10000           # ms (SET lock_timeout on every conn)

export SIDECAR_POOL_SIZE=50            # hot-path pool (split across workers)
```

There is no `DB_POOL_ACQUISITION_TIMEOUT`, `DB_QUERY_TIMEOUT`, or `DB_MAX_RETRIES` — those were retired with the synchronous `DatabasePool`. See [ENV_VARIABLES.md](ENV_VARIABLES.md) for the complete current list.

### Silent-drop detection (TCP keepalive + `TCP_USER_TIMEOUT`)

For cloud-managed PG where maintenance windows can black-hole an existing connection:

```bash
export DB_TCP_USER_TIMEOUT_MS=30000    # Linux only; ignored on macOS/Windows
export DB_KEEPALIVES_IDLE=60           # seconds idle before first probe
export DB_KEEPALIVES_INTERVAL=10       # seconds between probes
export DB_KEEPALIVES_COUNT=3           # probes before declaring dead
```

With these defaults the kernel raises an error on a black-holed socket within ~30 s, surfacing as a poll error in libqueen and an `AsyncDbPool` health-check failure. Without them you inherit Linux's `tcp_retries2` default (~14 min) — longer than any realistic maintenance window. See `config.hpp::DatabaseConfig::connection_string()` for the full rationale.

### PostgreSQL server tuning

Recommended `postgresql.conf` for Queen:

```ini
# Connections
max_connections = 220                      # ≥ DB_POOL_SIZE (150) + SIDECAR_POOL_SIZE (50) + headroom

# Memory (these are baseline; tune to host RAM)
shared_buffers = 4GB                       # 25% of RAM
effective_cache_size = 12GB                # 75% of RAM
work_mem = 64MB
maintenance_work_mem = 512MB

# Write performance
wal_buffers = 16MB
checkpoint_completion_target = 0.9
max_wal_size = 4GB
min_wal_size = 1GB

# Query performance
random_page_cost = 1.1                     # for SSD
effective_io_concurrency = 200             # for SSD

# Parallelism
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8

# Autovacuum (Queen is append-heavy; tune more aggressively)
autovacuum_naptime = 10s
autovacuum_vacuum_scale_factor = 0.05

# Logging (production)
log_line_prefix = '%m [%p] %u@%d '
log_min_duration_statement = 1000          # log slow queries > 1s
```

After changes:

```bash
sudo systemctl restart postgresql
```

The 2026-04-26 benchmark host used `shared_buffers=24GB` with `max_connections=300` on 32 vCPU / 62 GiB.

---

## PostgreSQL Failover

Queen provides **zero-message-loss failover** via a file-based buffer system. When PostgreSQL becomes unavailable, PUSHes are buffered to disk and replayed on recovery.

### How failover works

#### 1. Normal operation

```
Client → Worker → libqueen → PostgreSQL → 201 Created
```

#### 2. PostgreSQL goes down

First request (detection):

```
Client → Worker → libqueen → timeout (DB_STATEMENT_TIMEOUT) → File Buffer
                                ↓
                          AsyncDbPool marks DB unhealthy
                          SharedStateManager broadcasts state to peers (if any)
```

Subsequent requests (fast path):

```
Client → Worker → SharedStateManager says "unhealthy" → File Buffer (instant)
```

#### 3. PostgreSQL recovers

```
Background flusher (every FILE_BUFFER_FLUSH_MS):
  ↓
Try queen.push_messages_v3() on AsyncDbPool → Success
  ↓
Mark DB healthy, broadcast → resume normal operation
```

### File buffer architecture

Buffer files are UUIDv7-named for guaranteed ordering:

```
$FILE_BUFFER_DIR/
├── failover_019a0c11-7fe8.buf.tmp     ← currently being written
├── failover_019a0c11-8021.buf         ← complete, ready to flush
├── failover_019a0c11-8054.buf         ← queued
└── failed/
    └── failover_019a0c11-7abc.buf     ← failed, retried every 5s
```

File lifecycle:

1. **Write** — events appended to `.buf.tmp` (one buffer file shared across all workers via `FileBufferManager` owned by worker 0).
2. **Finalize** — at `FILE_BUFFER_EVENTS_PER_FILE` events **or** 200 ms idle, atomic rename to `.buf`.
3. **Flush** — background processor batches `.buf` events into `FILE_BUFFER_MAX_BATCH`-sized transactions against `AsyncDbPool`.
4. **Delete** — file removed after a successful flush.
5. **Retry** — failed files move to `failed/`, retried every 5 seconds.

Properties:

- ✅ Zero message loss — even server crash mid-write leaves events on disk
- ✅ FIFO ordering preserved within partitions (UUIDv7 sort order)
- ✅ Transaction-ID-based dedup catches duplicates from client retries
- ✅ Startup recovery replays any leftover `.buf` files before serving traffic

### Configuration

```bash
# Buffer directory
FILE_BUFFER_DIR=/var/lib/queen/buffers     # Linux default
FILE_BUFFER_DIR=/tmp/queen                  # macOS default

# Processing
FILE_BUFFER_FLUSH_MS=100                    # scan interval for .buf files
FILE_BUFFER_MAX_BATCH=100                   # events per DB transaction
FILE_BUFFER_EVENTS_PER_FILE=10000           # finalize after N events

# Fast failover detection
DB_STATEMENT_TIMEOUT=2000                   # detect DB down in 2 s
DB_CONNECTION_TIMEOUT=1000                  # 1 s connect attempt
```

### Failover scenarios

**Scenario 1: DB down during push**

```
[Worker 0] PUSH: 1000 items to [orders/Default] | Pool: 5/5 conn (0 in use)
... 2 seconds timeout ...
[Worker 0] DB connection failed, using file buffer for failover
[Worker 0] DB known to be down, using file buffer immediately
```

Result: messages buffered; client gets `{pushed: true, dbHealthy: false, failover: true}`.

**Scenario 2: DB recovers**

```
[Background] Failover: Processing 10000 events from failover_019a0c11.buf
[Background] PostgreSQL recovered! Database is healthy again
[Background] Failover: Completed 10000 events in 850ms (11765 events/sec) - file removed
```

**Scenario 3: Duplicate detection**

```
[Background] Failover: Processing 10000 events...
[ERROR] Batch push failed: duplicate key constraint
[INFO] Recovery: Duplicate keys detected, retrying individually...
[INFO] Recovery complete: 1000 new, 9000 duplicates, 0 deleted queues
```

Only the truly new messages are inserted; duplicates from client retries are safely skipped.

### Monitoring failover

```bash
# Buffer + DB health status
curl http://localhost:6632/api/v1/status/buffers
```

```json
{
  "failover": {
    "pending": 50000,
    "failed": 0
  },
  "dbHealthy": false
}
```

```bash
# File-system view
ls -lh /var/lib/queen/buffers/
du -sh /var/lib/queen/buffers
```

### Best practices

1. **Fast failover detection** — production should run with `DB_STATEMENT_TIMEOUT=2000` and `DB_CONNECTION_TIMEOUT=1000` so the broker fails over within 2 s instead of 30 s.
2. **Sufficient disk space** — at ~170 B/message, 1 M msg/hr ≈ 170 MB/hr of buffer. Monitor `du -sh $FILE_BUFFER_DIR`.
3. **Alert on growth** — `du -sh $FILE_BUFFER_DIR > 100 MB` is a strong signal the DB has been unreachable for an extended period.
4. **Client-side transaction IDs** — all official clients generate UUIDv7 `transactionId`s for every PUSH so retries during failover dedupe correctly.

### Tuning for high recovery throughput

```bash
FILE_BUFFER_FLUSH_MS=50                  # scan more often
FILE_BUFFER_MAX_BATCH=1000               # larger DB transactions
FILE_BUFFER_EVENTS_PER_FILE=50000        # bigger files = fewer renames
DB_POOL_SIZE=300                         # more conns for parallel recovery
```

### Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| "Buffered event missing transactionId" | Old client or corrupted file | Upgrade clients to ≥ 0.7.4 |
| Duplicate messages after failover | Client regenerated IDs on retry | Upgrade clients to ≥ 0.2.9 |
| `.tmp` files not finalizing | Workload paused mid-write | Files auto-finalize after 200 ms idle |
| Recovery is slow | Files too large | Raise `FILE_BUFFER_MAX_BATCH` and/or `DB_POOL_SIZE` |

---

## Queue Optimization

### Per-queue settings

Queue-level settings (lease time, retry limit, retention, encryption, max size, priority, DLQ on/off, …) are **stored in the `queen.queues` row**, not configured by environment variable. Set them via any client:

```javascript
await queen.queue('orders').config({
  leaseTime: 300,
  retryLimit: 3,
  retentionSeconds: 86400,
  dlqAfterMaxRetries: true,
  encryptionEnabled: false,
  priority: 5,
  maxSize: 0
}).create()
```

See [API.md](API.md) and any client README (e.g. [`clients/client-js/client-v2/README.md`](../clients/client-js/client-v2/README.md#part-11-queue-configuration---fine-tuning)) for the full list of per-queue fields. The server-side defaults applied at queue creation are in `lib/schema/procedures/012_configure.sql`.

### Server-wide defaults

```bash
export DEFAULT_TIMEOUT=30000             # max ms a long-poll can wait
export DEFAULT_BATCH_SIZE=1              # batch size when client doesn't pass one
export DEFAULT_SUBSCRIPTION_MODE=""      # "" = all messages, "new" = skip history
```

### Retention & eviction services

These services run on `AsyncDbPool` and respect per-queue retention settings:

```bash
# Retention (deletes old messages + cleans partition_consumers / messages_consumed tables)
export RETENTION_INTERVAL=300000              # 5 minutes
export RETENTION_BATCH_SIZE=1000
export PARTITION_CLEANUP_DAYS=30              # delete empty partitions older than N days
export METRICS_RETENTION_DAYS=90              # delete metrics older than N days

# Eviction (moves stuck messages that exceeded their max_wait_time)
export EVICTION_INTERVAL=60000                # 1 minute
export EVICTION_BATCH_SIZE=1000

# Stats reconciliation
export STATS_INTERVAL_MS=10000                # fast aggregation
export STATS_RECONCILE_INTERVAL_MS=120000     # full scan of queen.messages
export STATS_HISTORY_RETENTION_DAYS=7
```

### Partition_lookup safety net (PUSHPOPLOOKUPSOL)

A reconcile service catches `queen.update_partition_lookup_v1` calls missed during crashes or transient failures of the per-push follow-up call. Only the elected stats leader runs the actual reconcile query (gated on `queen.is_stats_leader`):

```bash
export PARTITION_LOOKUP_RECONCILE_INTERVAL_MS=5000        # 5 seconds
export PARTITION_LOOKUP_RECONCILE_LOOKBACK_SECONDS=60
```

---

## Monitoring & Debugging

### Logging

```bash
# Development
export LOG_LEVEL=debug

# Production
export LOG_LEVEL=info
```

Levels: `trace`, `debug`, `info` (default), `warn`, `error`, `critical`, `off`. The server only respects `LOG_LEVEL` — there is no `LOG_FORMAT` or `LOG_TIMESTAMP` env var (spdlog is configured with a fixed format).

### Health & metrics endpoints

```bash
# Liveness
curl http://localhost:6632/health

# Prometheus scrape
curl http://localhost:6632/metrics/prometheus

# Human-readable summary (JSON)
curl http://localhost:6632/metrics

# Resources
curl http://localhost:6632/api/v1/resources/overview
curl http://localhost:6632/api/v1/resources/queues
curl http://localhost:6632/api/v1/status/buffers
curl http://localhost:6632/api/v1/status/queues
```

`/metrics/prometheus` exposes per-`JobType` counters (push/pop/ack request rates, latency histograms, Vegas limit, in-flight slots), shared-state cache hit ratios, AsyncDbPool stats, and worker metrics.

### Development mode

```bash
LOG_LEVEL=debug ./bin/queen-server --dev
# or
make dev
```

### Common issues & solutions

#### 1. High CPU usage when idle

**Cause:** poll interval too aggressive.

```bash
export POP_WAIT_INITIAL_INTERVAL_MS=200
export POP_WAIT_MAX_INTERVAL_MS=2000
```

#### 2. Slow message delivery

**Cause:** poll interval too conservative.

```bash
export POP_WAIT_INITIAL_INTERVAL_MS=50
export POP_WAIT_MAX_INTERVAL_MS=500
```

#### 3. PG `too many clients already` at startup

**Cause:** `max_connections` < `DB_POOL_SIZE + SIDECAR_POOL_SIZE + headroom`.

```bash
# Either raise PG:
ALTER SYSTEM SET max_connections = 300;

# Or shrink Queen:
export DB_POOL_SIZE=50
export SIDECAR_POOL_SIZE=20
```

#### 4. `StatsService` `statement_timeout` in logs

**Cause:** reconciliation query exceeded `DB_STATEMENT_TIMEOUT` on a very large `queen.messages` table.

This is **cosmetic** — an advisory lock prevents pile-up and the fast-path stats keep working. Long-term fix: shorten retention, partition by time, or raise `DB_STATEMENT_TIMEOUT`. Tracked in the 2026-04-26 benchmark notes.

#### 5. PG `deadlock detected` during high-concurrency push

**Cause:** observed on `0.12` at 10 000+ partitions; rare on `0.14+` under multi-cg fan-out.

**Mitigation:** Queen's file-buffer failover catches the rolled-back transaction, retries individually, and the messages land safely. Zero data loss in any benchmarked scenario. The `[error]` log lines will alert in production but are not correctness issues.

#### 6. Build errors on macOS

```bash
brew install postgresql openssl
make debug-paths    # verify detected paths
```

---

## Production Deployment

### Systemd service

Create `/etc/systemd/system/queen-server.service`:

```ini
[Unit]
Description=Queen C++ Message Queue Server
After=postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=queen
Group=queen
WorkingDirectory=/opt/queen
ExecStart=/opt/queen/bin/queen-server
Restart=always
RestartSec=10

# Core
Environment=PORT=6632
Environment=HOST=0.0.0.0
Environment=NUM_WORKERS=10

# Database
Environment=PG_HOST=localhost
Environment=PG_PORT=5432
Environment=PG_DB=queen_production
Environment=PG_USER=queen
Environment=PG_PASSWORD=secure_password
Environment=DB_POOL_SIZE=150
Environment=SIDECAR_POOL_SIZE=50

# Logging
Environment=LOG_LEVEL=info

# Encryption at rest (64 hex chars / 32 bytes)
Environment=QUEEN_ENCRYPTION_KEY=your_64_char_hex_key_here

# Hardening
NoNewPrivileges=true
PrivateTmp=true

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable queen-server
sudo systemctl start queen-server
sudo journalctl -u queen-server -f
```

### Docker

See the repo-root `Dockerfile` and `build.sh`. Single-image deployment example (also in the main README):

```bash
docker network create queen
docker run --name qpg --network queen \
  -e POSTGRES_PASSWORD=postgres -p 5433:5432 -d postgres
docker run -p 6632:6632 --network queen \
  -e PG_HOST=qpg -e PG_PORT=5432 -e PG_PASSWORD=postgres \
  -e NUM_WORKERS=2 -e DB_POOL_SIZE=5 -e SIDECAR_POOL_SIZE=30 \
  smartnessai/queen-mq:0.15.0
```

### Kubernetes (with optional UDP sync for multi-instance)

For a 3-pod cluster that shares cache state via UDP:

```yaml
env:
  - name: NUM_WORKERS
    value: "10"
  - name: DB_POOL_SIZE
    value: "100"
  - name: SIDECAR_POOL_SIZE
    value: "50"
  # Inter-instance notification + shared cache
  - name: QUEEN_UDP_PEERS
    value: "queen-0.queen-hl.ns.svc.cluster.local:6633,queen-1.queen-hl.ns.svc.cluster.local:6633"
  - name: QUEEN_UDP_NOTIFY_PORT
    value: "6633"
  - name: QUEEN_SYNC_SECRET
    valueFrom:
      secretKeyRef:
        name: queen-secrets
        key: sync-secret
```

A more complete StatefulSet example (with Helm templating) lives at [`k8s-example.yaml`](k8s-example.yaml).

### Load balancing

Queen is **stateless** — any HTTP request can hit any instance. Behind a load balancer:

```nginx
upstream queen_cluster {
    server queen1.local:6632;
    server queen2.local:6632;
    server queen3.local:6632;
}

server {
    listen 80;
    location / {
        proxy_pass http://queen_cluster;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

For best consumer-group dispatch consolidation, prefer an **affinity** load-balancing strategy on the client (`loadBalancingStrategy: 'affinity'`) so the same `queue:partition:consumerGroup` key always hits the same backend.

---

## Benchmarking

### Reproducible perf harness

The current performance harness lives at [`benchmark-queen/test-perf/`](../benchmark-queen/test-perf/README.md). It spins up a fresh PG container, the locally-built `queen-server`, runs producer + consumer in parallel for a configurable window, and emits PG-stats-truth metrics (not just autocannon).

```bash
nvm use 22
cd ..    # repo root
./benchmark-queen/test-perf/scripts/run-all.sh                # all scenarios × 3 runs
RUNS_PER_SCENARIO=1 ./benchmark-queen/test-perf/scripts/run-all.sh    # quick smoke
```

### Quick client-side benchmarks

Lightweight JS benchmarks for sanity checks live at [`clients/client-js/benchmark/`](../clients/client-js/benchmark/):

```bash
cd clients/client-js
node benchmark/producer.js          # single-producer
node benchmark/consumer.js          # single-consumer
node benchmark/producer_multi.js    # multi-producer
node benchmark/consumer_multi.js    # multi-consumer
```

### Published results

The 2026-04-26 campaign on a 32 vCPU / 62 GiB host with `NUM_WORKERS=10`, `DB_POOL_SIZE=50`, `SIDECAR_POOL_SIZE=250`, `PG max_connections=300`:

- **104 k msg/s** push (`batch=100`)
- **165 k msg/s** fan-out (10 consumer groups)
- **39 k msg/s** push (production-realistic `batch=10`)
- broker RSS 30–170 MB across the entire suite
- zero message loss across 1.6 B events

Full report: [`benchmark-queen/2026-04-26/README.md`](../benchmark-queen/2026-04-26/README.md).

---

## Environment Variables Reference

**The full, code-verified table is in [ENV_VARIABLES.md](ENV_VARIABLES.md).** This README intentionally does not duplicate it.

Quick reference for the variables most operators touch:

| Variable | Default | Description |
|---|---|---|
| `PORT` | `6632` | HTTP listen port |
| `HOST` | `0.0.0.0` | HTTP bind host |
| `NUM_WORKERS` | `10` | uWS worker threads (capped at CPU cores) |
| `DB_POOL_SIZE` | `150` | Background-services pool; 95 % used (5 % safety margin) |
| `SIDECAR_POOL_SIZE` | `50` | Hot-path pool (PUSH/POP/ACK), split across workers |
| `PG_HOST` / `PG_PORT` / `PG_DB` / `PG_USER` / `PG_PASSWORD` | local defaults | PostgreSQL connection |
| `PG_USE_SSL` / `PG_SSL_REJECT_UNAUTHORIZED` | `false` / `true` | SSL configuration |
| `LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` / `critical` / `off` |
| `POP_WAIT_INITIAL_INTERVAL_MS` | `100` | Long-poll initial interval |
| `POP_WAIT_MAX_INTERVAL_MS` | `1000` | Long-poll max interval (after backoff) |
| `QUEEN_ENCRYPTION_KEY` | (unset) | 64 hex chars; enables per-queue payload encryption |
| `QUEEN_UDP_PEERS` | (unset) | Comma-separated `host:port` list for multi-instance sync |
| `JWT_ENABLED` | `false` | Enable JWT auth (HS256 / RS256 / EdDSA via JWKS) |

---

## Further Reading

- [ENV_VARIABLES.md](ENV_VARIABLES.md) — complete env var reference (single source of truth)
- [API.md](API.md) — HTTP API documentation
- [`../README.md`](../README.md) — project README & release history
- [`../benchmark-queen/`](../benchmark-queen/) — published benchmark sessions
- [`../cdocs/LIBQUEEN_IMPROVEMENTS.md`](../cdocs/LIBQUEEN_IMPROVEMENTS.md) — design notes for the adaptive engine (§9 covers per-type batching + Vegas)

---

## Support

- Logs: `journalctl -u queen-server -f`
- Debug: `LOG_LEVEL=debug`
- Metrics: `curl http://localhost:6632/metrics/prometheus`
- Issues: [github.com/queen-mq/queen/issues](https://github.com/queen-mq/queen/issues)
