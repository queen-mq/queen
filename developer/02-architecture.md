# 02 — Architecture

This page is a developer's mental model of how Queen is laid out, what runs where, and how a request travels through the system. It complements the user-facing `[docs/architecture.html](../docs/architecture.html)`, but here we focus on **code structure** and where to look when you want to change something.

---

## Two-binary architecture

In production Queen runs as **one process** alongside one Postgres:

```
            ┌─────────────────────┐
            │   queen-server      │   ← C++ broker (server/)
            │  ┌──────────────┐   │
            │  │   libqueen   │   │   ← header-only core (lib/)
            │  └──────┬───────┘   │
            │         │           │
            │   uWebSockets       │
            │   libuv + libpq     │
            └────────┬────────────┘
                     │
                     ▼
              ┌────────────┐
              │ PostgreSQL │       ← stores everything
              │  (queen    │
              │   schema)  │
              └────────────┘
```

There are no other services: no Redis, no ZooKeeper, no internal cluster gossip. Horizontal scaling is "run more `queen-server` processes pointed at the same Postgres."

Optional companion processes:

- `**proxy/**` — Node.js auth proxy in front of the broker (JWT, Google OAuth, RBAC). See [11 — Auth proxy](11-proxy.md).
- `**app/**` — Vue 3 dashboard. The broker embeds the prebuilt `dist/` and serves it at `/`. See [10 — Frontend dashboard](10-frontend-dashboard.md).

---

## Inside the broker (`server/`)

### Threading model: acceptor + HTTP workers + function-split engine cluster + services

```
                 ┌────────────┐
   incoming ────▶│  Acceptor  │  one thread, listens on PORT (default 6632)
   TCP/HTTP      └─────┬──────┘
                       │ round-robin
              ┌────────┼───────────────┐
              ▼        ▼               ▼
         ┌────────┐ ┌────────┐    ┌────────┐
         │Worker 1│ │Worker 2│ …  │Worker N│   N = NUM_WORKERS — HTTP I/O +
         └───┬────┘ └───┬────┘    └───┬────┘   JSON parse ONLY (no DB engine)
             │   submit(JobRequest), routed by JobType
             └───────────────┬────────────────────────┘
                             ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  libqueen engine cluster  (PROCESS-GLOBAL, shared by all        │
   │  workers, sized for the DB — NOT one-per-worker, decoupled       │
   │  from NUM_WORKERS). Routing: PUSH/ACK/TXN→push, POP→pop, else→rest│
   │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
   │   │ push/ack eng │    │  pop engine  │    │  rest engine │      │
   │   │ libuv loop + │    │ libuv loop + │    │ libuv loop + │      │
   │   │ conn slots + │    │ conn slots   │    │ conn slots   │      │
   │   │ per-partition│    │ (pop path    │    │ (custom /    │      │
   │   │ in-flight    │    │  unchanged)  │    │ partition_   │      │
   │   │ gate + pl    │    │              │    │ lookup /     │      │
   │   │ coalescer    │    │              │    │ streams …)   │      │
   │   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
   └──────────┼───────────────────┼───────────────────┼─────────────┘
              └───────────────────┴───────────────────┘
                                  │  per-function connection slots
                                  ▼ (QUEEN_PUSH/POP/REST_SLOTS)
                           ┌─────────────┐
                           │ PostgreSQL  │
                           └─────────────┘

  ┌────────────────────────────────────────┐
  │           Background services           │  separate threadpool
  ├────────────────────────────────────────┤
  │ MetricsCollector       ── stats roll-up │
  │ RetentionService       ── cleanup       │
  │ EvictionService        ── max_wait_time │
  │ PartitionLookupReconcileService         │
  │ FileBuffer              ── PG failover  │
  │ SharedStateManager      ── POP_WAIT     │
  └────────────────────────────────────────┘
```

**Function-split engines (push-serialization architecture).** There are exactly
**three** libqueen engines per broker process — push/ack, pop, and rest — created
once and shared across all HTTP workers (`QueenCluster` in `lib/queen_cluster.hpp`).
This replaced the old "one libqueen engine per HTTP worker" model. Two reasons:

1. **Correctness needs a single owner per partition.** Push serialization (below)
   keeps `messages.created_at` commit-ordered per partition via an in-memory
   per-partition in-flight gate; that gate is only simple and lock-free when all
   of a partition's pushes funnel through one engine. Routing PUSH/ACK to a single
   push engine gives that for free.
2. **Decoupling tuning.** `NUM_WORKERS` now sizes only HTTP I/O (TLS, parse,
   dispatch). DB concurrency is sized independently by **per-function connection
   slots** (`QUEEN_PUSH_SLOTS` / `QUEEN_POP_SLOTS` / `QUEEN_REST_SLOTS`) — engine
   count is fixed at 3, not a knob. `Queen::submit` is thread-safe, so all workers
   submit into the shared engines concurrently.

POP runs on its own engine so its load never interferes with push, and the pop
SQL/cursor path is unchanged.

Where this lives in code:


| Concern                                        | File(s)                                                          |
| ---------------------------------------------- | ---------------------------------------------------------------- |
| Acceptor (`uv_listen`, round-robin to workers) | `server/src/acceptor_server.cpp`, `server/src/main_acceptor.cpp` |
| HTTP worker event loop + routes                | `server/src/routes/*.cpp` (push, pop, ack, …)                    |
| Engine cluster (3 function-split engines)      | `lib/queen_cluster.hpp`, wired in `server/src/acceptor_server.cpp` |
| Async DB pool (non-blocking libpq)             | `server/src/database/async_database.cpp`                         |
| Background services                            | `server/src/services/*.cpp`                                      |
| JWT auth (when enabled)                        | `server/src/auth/auth_middleware.cpp`, `server/src/auth/jwt_validator.cpp` |


### HTTP routes

Each HTTP route is one `*.cpp` file in `server/src/routes/`. Notable ones:


| Route file                                    | What it does                                                |
| --------------------------------------------- | ----------------------------------------------------------- |
| `push.cpp`                                    | `POST /api/v1/push` — calls `queen.push_messages_v3`        |
| `pop.cpp`                                     | `POST/GET /api/v1/pop` — calls `queen.pop_unified_batch_v4` |
| `ack.cpp`                                     | `POST /api/v1/ack` — calls `queen.ack_messages_v2`          |
| `transactions.cpp`                            | atomic ack + push — calls `queen.execute_transaction_v2`    |
| `leases.cpp`                                  | `POST /api/v1/renew` — calls `queen.renew_lease_v2`         |
| `dlq.cpp`                                     | dead-letter inspection + replay                             |
| `consumer_groups.cpp`                         | subscription mode, replay-from-timestamp                    |
| `health.cpp`, `metrics.cpp`, `prometheus.cpp` | observability                                               |
| `static_files.cpp`                            | serves the embedded Vue dashboard                           |


Each route validates input, then submits a `JobRequest` to the shared engine cluster (`QueenCluster::submit`, routed by `JobType`), which batches it into a stored-procedure call. **The actual logic of push/pop/ack lives in SQL**, not in C++. See [05 — Database schema](05-database-schema.md).

---

## libqueen (`lib/`)

libqueen is a **header-only C++ library** that lives alongside the broker. The broker `#include`s it (`server/Makefile` adds `-I../lib`) and uses it for:

- **UUIDv7 generation** (time-ordered IDs for ordering guarantees)
- **Function-split drain orchestrators** — three shared engines (push/ack, pop, rest) that batch requests across many concurrent HTTP connections, call the stored procedures in one round-trip, and route responses back to the original waiter
- **Per-partition push serialization** — the push engine keeps an in-memory "≤1 in-flight push transaction per partition" gate and forms disjoint-partition concurrent batches, so `created_at` is assigned in commit order per partition (the property the pop cursor relies on)
- **Concurrency control** — the **data-path lanes (push/pop/ack) default to STATIC** concurrency at a measured optimum (push 24, pop/ack 16); the RTT-adaptive **Vegas** controller is kept only for the auxiliary lanes (custom/renew/streams/partition_lookup). Vegas was found to *under-shoot* push (high commit RTT) and *collapse* pop (long-poll RTT misread as PG queuing), so the hot path uses static limits instead. Override per lane with `QUEEN_<PUSH|POP|ACK>_CONCURRENCY_MODE`.

### The drain orchestrator

This is the trick that lets a small pool of DB connection slots (the per-function split of `SIDECAR_POOL_SIZE`) serve hundreds of thousands of concurrent in-flight HTTP requests:

```
many HTTP handlers ──▶ PerTypeQueue (per JobType)
                            │
                            │  BatchPolicy says FIRE when:
                            │   - queue.size >= preferred_batch_size
                            │   - or oldest_age >= max_hold_ms
                            ▼
                    ConcurrencyController.try_acquire()
                            │  (Vegas-adaptive cap)
                            ▼
                    grab idle DBConnection slot
                            │
                            ▼
                    one stored procedure call,
                    JSONB array of N requests
                            │
                            ▼
                    libuv-poll fires on socket-readable
                    fan-out responses to PendingJob.callback
```

The relevant files (small, readable, all in `lib/queen/`):

| File                           | Layer | Purpose                                               |
| ------------------------------ | ----- | ----------------------------------------------------- |
| `pending_job.hpp`              | —     | `JobType` enum, `JobRequest`, `PendingJob` (callback) |
| `per_type_queue.hpp`           | 1     | thread-safe FIFO of pending jobs, per op type         |
| `batch_policy.hpp`             | 2     | pure FIRE/HOLD decision on (size, oldest_age)         |
| `concurrency/static_limit.hpp` | 3     | fixed concurrency cap                                 |
| `concurrency/vegas_limit.hpp`  | 3     | adaptive cap based on RTT (TCP Vegas-style)           |
| `slot_pool.hpp`                | —     | `DBConnection` struct (a "slot" *is* a libpq conn); holds the push batch's in-flight partition keys |
| `drain_orchestrator.hpp`       | —     | `PerTypeState` aggregate + concurrency-ctrl factory + `concurrency_mode_for` (data-path lanes default static) |
| `../queen_cluster.hpp`         | —     | `QueenCluster`: owns the 3 function-split engines, routes `submit()` by JobType |
| `metrics.hpp`                  | —     | atomics for drain-loop stats                          |
| (drain loop + per-partition gate + pl coalescer) | 4 | inside the `Queen` class in `queen.hpp`        |


See [04 — libqueen](04-libqueen.md) for a code-walkthrough.

---

## Database layer (`lib/schema/`)

Two parts:

1. `**schema.sql**` — base tables, indexes, constraints. Idempotent (`IF NOT EXISTS`). ~488 lines.
2. `**procedures/*.sql**` — stored procedures, all `CREATE OR REPLACE FUNCTION`. ~18 files, ~10k lines.

The broker discovers these on startup (`AsyncQueueManager::initialize_schema`) by walking up the directory tree looking for `schema/schema.sql`. Then it loads every `procedures/*.sql` in lexical order.

The most important procedures:


| File                      | Procedure                  | What it does                              |
| ------------------------- | -------------------------- | ----------------------------------------- |
| `001_push.sql`            | `push_messages_v3` (+ `lock_push_partition` helper) | per-partition advisory lock + batch insert with `created_at=clock_timestamp()` (commit-ordered), dedup, partition upsert |
| `002d_pop_unified_v4.sql` | `pop_unified_batch_v4`     | wildcard multi-partition pop with leases  |
| `003_ack.sql`             | `ack_messages_v2`          | mark consumed, advance `last_consumed_id` |
| `004_transaction.sql`     | `execute_transaction_v2`   | atomic ack + push (transactional outbox)  |
| `005_renew_lease.sql`     | `renew_lease_v2`           | extends a lease for long-running consumers |
| `008_consumer_groups.sql` | replay, subscription modes | per-group state                           |
| `013_stats.sql`           | rolls up `queen.stats`     | dashboard data                            |
| `016_partition_lookup.sql` | `update_partition_lookup_v1`, `reconcile_partition_lookup_v1` | post-push lookup maintenance |


> ⚠ **The procedures are load-bearing and tightly coupled to each other.** See the warning in [05 — Database schema](05-database-schema.md#warning-stored-procedures-are-load-bearing) before changing anything.

---

## Request flow — what happens on a `push`

1. **Client** sends `POST /api/v1/push` to the broker.
2. **Acceptor** picks a worker (round-robin) and hands off the connection.
3. **HTTP worker** parses JSON, validates, computes the distinct `(queue,partition)` keys of the batch, and `submit()`s the request — routed to the shared **push engine**.
4. **Push engine drain loop** queues the request in the push `PerTypeQueue`. The `BatchPolicy` (Layer 2) decides when to fire — `queue.size >= preferred_batch_size` (default 50) or `oldest_age >= max_hold_ms` (default 20 ms). The `ConcurrencyController` (Layer 3, **static** for push, limit 24) decides whether there's headroom. The drain then forms a **disjoint-partition batch**: it skips jobs whose partition already has an in-flight push transaction and caps distinct partitions per batch (`QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH`, default 8), so up to N push transactions run concurrently on disjoint partition sets (≤1 in-flight per partition).
5. The drain grabs an idle `DBConnection` slot, builds one JSONB array of the batch, and calls `queen.push_messages_v3($1::jsonb)` with a single async query.
6. **Postgres** runs the procedure: it upserts queues + partitions, **takes the push partition advisory lock for each partition in sorted order** (two-int keyspace, disjoint from pop/ack), deduplicates by `(partition_id, transaction_id)`, batch-inserts into `queen.messages` stamping **`created_at = clock_timestamp()` under the lock** (so per-partition `created_at` is commit-ordered — see Push serialization below), and returns a JSONB summary.
7. The push engine fans results back to each waiter; each HTTP worker writes its response on its own loop (per-worker response registry).
8. The batch's `partition_updates` are merged into the push engine's **coalescing buffer** and flushed to `queen.update_partition_lookup_v1()` Nagle-style (at most one flush in flight; immediate when idle, batched under load). This replaced the old one-`update_partition_lookup`-call-per-push-batch, which backlogged under high push. Design lineage: `[cdocs/PUSHPOPLOOKUPSOL.md](../cdocs/PUSHPOPLOOKUPSOL.md)`.

### Push serialization (why `created_at` is safe)

`messages.created_at` is the pop cursor's primary sort key, but `NOW()` is the
*transaction-start* time, decoupled from commit order — so under concurrent push
a message could commit "behind" the cursor and be skipped forever. The fix:
serialize pushes **per partition** (the in-memory gate above, plus a Postgres
`pg_advisory_xact_lock` in a dedicated two-int keyspace, held until commit) and
stamp `created_at = clock_timestamp()` *under* that lock. With one push
transaction per partition at a time, `created_at` is monotonic in commit order
per partition, so the `(created_at, id)` cursor can never skip. POP and ACK are
unchanged. All message-insert paths honor the lock: `push_messages_v3`,
`execute_transaction_v2` (push branch, sorted multi-lock), and the streams sink
(via `push_messages_v3`).

---

## Request flow — what happens on a `pop`

1. Same as 1–3 above for `pop`.
2. `pop_unified_batch_v4` is called with up to N partition candidates. Each candidate is probed with `pg_try_advisory_xact_lock(partition_id)` so concurrent consumers don't fight over the same partition.
3. Won partitions are leased (consumer's `worker_id` written into `queen.partition_consumers`, `lease_expires_at = NOW() + lease_time`).
4. Messages above `last_consumed_id` are returned to the consumer.
5. On `ack`, `last_consumed_id` advances and the lease is released (or auto-released when `acked_count >= batch_size`).

If the consumer used `wait=true` and the queue is empty, the long-poll path kicks in: `SharedStateManager` re-checks every 100 ms (with exponential backoff up to 1 s) until either data arrives or the timeout elapses.

---

## Failover path (when Postgres goes down)

A separate code path keeps PUSH durable even when Postgres is unreachable:

```
push request ──▶ try DB ──▶ timeout ──▶ FileBuffer.write()
                                              │
                                              ▼
                                  /var/lib/queen/buffers/*.buf
                                              │
   background processor (every 100 ms) ──▶ try to flush ──▶ DB
```

See [12 — Failover](12-failover.md) for details. Code: `server/src/services/file_buffer.cpp`.

---

## Where to look when…


| Symptom / task                   | Look here                                                                                                     |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Broker won't compile             | `server/Makefile`, [03 — Building the server](03-build-server.md)                                             |
| Adding/changing an HTTP endpoint | `server/src/routes/<topic>.cpp`                                                                               |
| Adding/changing a SQL procedure  | `lib/schema/procedures/`, then [05 — Database schema](05-database-schema.md)                                  |
| Pop/push contention              | `lib/schema/procedures/002d_pop_unified_v4.sql` + `[cdocs/PUSHPOPLOOKUPSOL.md](../cdocs/PUSHPOPLOOKUPSOL.md)` |
| Broker latency / throughput      | `lib/queen/drain_orchestrator.hpp` (per-lane concurrency mode + limits), `lib/queen_cluster.hpp` (engine split), `concurrency/{static,vegas}_limit.hpp` |
| Push correctness / `created_at`  | `lib/schema/procedures/001_push.sql` (`lock_push_partition` + `clock_timestamp`), per-partition gate in `lib/queen.hpp` |
| Cleanup / retention              | `server/src/services/retention_service.cpp` + [09 — Retention](09-retention.md)                               |
| Dashboard                        | `app/src/` + [10 — Frontend dashboard](10-frontend-dashboard.md)                                              |
| Auth                             | `proxy/` + [11 — Auth proxy](11-proxy.md)                                                                     |


