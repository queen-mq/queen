# P6 — Strategic Roadmap

## 1. Summary

The previous priorities (P0–P5) are mechanical or local: hygiene, SQL
cleanup, header decomposition, CI, code refactors, streaming
hardening. None of them changes Queen's deployment shape. This memo
covers the four strategic items that *do* change the shape, and
together gate the move from "very good single-region broker on top of
one Postgres primary" to "broker class users will trust for primary
infrastructure":

1. **Multi-Postgres horizontal write scaling.** Today every PUSH/POP/ACK
   funnels into one PG primary. `NUM_WORKERS` and Vegas absorb broker-
   side concurrency well, but the write ceiling is whatever a single
   `postgresql.conf` can deliver. Documenting and prototyping a sharded
   path removes the ceiling.
2. **PG replication and disaster recovery story.** The file-buffer
   protects PUSHes during PG outages; nothing today addresses
   catastrophic primary loss, point-in-time recovery, or geo
   replication. The story exists implicitly (use streaming
   replication, archive WAL) but is not written down anywhere a
   buyer/operator can find it.
3. **Liveness vs readiness probes.** `GET /health` conflates both
   today (one endpoint, returns 200 or 503 from `health_check()`).
   Kubernetes operators need them distinct so the broker is not
   killed during legitimate startup work (file-buffer recovery,
   schema apply on a fresh DB).
4. **OpenTelemetry / OTLP traces.** Prometheus is mature on the
   counter side; the next step for serious operators is per-message
   trace propagation through PUSH → libqueen batch → SP → POP → ACK.
   The hooks exist in `MetricsCollector` and `worker_metrics.hpp`; a
   thin OTLP exporter binding completes the observability triangle.

This memo is the **design + scoping document** for those four items.
It does not commit to landing any of them in a specific release; each
becomes its own implementation memo when prioritised.

## 2. Motivation

### 2.1 The single-PG ceiling

`benchmark-queen/2026-04-26/` shows the broker peaking at ~104 k msg/s
push and ~165 k msg/s fan-out on a 32-vCPU host with PG tuned to 24 GB
`shared_buffers` / `max_connections=300`. Beyond that, the bottleneck
moves into the Postgres process — WAL fsync, lock manager,
autovacuum overhead on `messages` and `partition_lookup`. No amount
of broker tuning crosses that line.

The schema was designed with sharding in mind without naming it
explicitly:

- `queen.messages` is keyed `(partition_id, created_at, id)`. It is
  trivially shardable on `partition_id`.
- `queen.partition_consumers` is `UNIQUE(partition_id, consumer_group)`
  — co-located with the partition.
- `queen.partition_lookup` is `UNIQUE(queue_name, partition_id)` —
  also co-located.
- `queen.queues` is the only table that needs to be a singleton (the
  catalogue).

A multi-PG path therefore boils down to "one shared catalogue PG, N
data PGs each owning a subset of partitions, libqueen routes each
job to the right shard". This is a *much* smaller change than it
first looks — the SQL stays the same, libqueen grows a routing
layer, the catalogue PG handles `queues` + `consumer_groups_metadata`
+ `system_state`.

### 2.2 The DR gap

`docs/server.html` and `developer/12-failover.md` describe the
file-buffer for PG-down PUSHes. They do not describe:

- What backup tooling to use (`pg_basebackup` + WAL archive vs
  `barman` vs `pgBackRest`).
- How to fail over to a synchronous standby without losing leases.
- The expected RPO/RTO at default settings.
- Whether `transaction_id` dedup correctly absorbs duplicate replays
  after a failover (it does, but it's not stated).
- Any guidance on geo-replication for read scale-out (none exists
  today; pop is single-primary).

A user-facing one-pager is the minimum bar before recommending Queen
for tier-1 workloads.

### 2.3 Probe conflation

`server/src/routes/health.cpp` (54 LOC) returns 503 if
`async_queue_manager->health_check()` is false. That is the right
behaviour for **readiness** (don't accept traffic) but the wrong
behaviour for **liveness** (Kubernetes will kill the pod, lose the
file-buffer recovery progress, and restart the cycle). The
distinction is:

- **Liveness**: process is responsive and not deadlocked. Should
  *almost never* return failure once the binary is up.
- **Readiness**: process is willing to serve real traffic. May
  return failure for valid reasons (PG unreachable, file-buffer
  recovery still running, schema apply in progress).

`FileBufferManager::is_ready()` already exists (`file_buffer.hpp:39`),
but is not surfaced to any HTTP route.

### 2.4 Observability ceiling

Today's metrics surface (`/metrics/prometheus`, ~730 LOC in
`server/src/routes/prometheus.cpp`) is excellent at *aggregate*
behaviour: per-queue throughput, per-worker push/pop/ack rates, DLQ
depth, lease churn. It is **silent on per-message latency
distribution**, end-to-end trace context, and which producer/consumer
contributed to a tail-latency event. OTLP traces close that gap with
a small, bounded code addition.

## 3. Out of scope

- Active-active multi-region writes. The plan in §5.1 is single-
  region multi-PG. Cross-region writes need conflict resolution and
  belong to a separate, much larger memo.
- Replacing Postgres. Queen's defining property is "ACID semantics
  via PG"; nothing in this memo trades that away.
- Per-message billing or quota enforcement. Useful, but layered on
  top of OTLP traces, not a prerequisite.
- Custom client telemetry SDKs. The OTLP exporter ships *server-side*
  spans; client SDKs can adopt the standard OTel libraries
  independently.
- `pg_qpubsub` extension evolution. Tracked in `pg_qpubsub/README.md`.

## 4. Current state

### 4.1 What is in place today

| Concern             | Today                                                           | Gap                                              |
| ------------------- | --------------------------------------------------------------- | ------------------------------------------------ |
| Catalogue           | `queen.queues`, `consumer_groups_metadata`, `system_state` (PG) | No separation between catalogue and data tables |
| Data tables         | `messages`, `partition_consumers`, `partition_lookup`, `dlq`    | Single PG; sharding-friendly schema, no router  |
| Failover            | File-buffer for PUSH (`server/src/services/file_buffer.cpp`)   | No POP/ACK during outage; no DR runbook         |
| Health probe        | `GET /health` (200/503 from PG ping)                            | No distinction between liveness and readiness   |
| Cluster awareness   | `SharedStateManager` over UDP                                   | Brokers know each other; PGs do not             |
| Observability (push) | Prometheus counters, dashboard analytics                       | No traces, no per-message latency histogram     |

### 4.2 Files that change in §5.1 (multi-PG)

| File                                              | Today's responsibility                              | Touch in §5.1                                     |
| ------------------------------------------------- | --------------------------------------------------- | ------------------------------------------------- |
| `server/include/queen/config.hpp`                 | One `DatabaseConfig`                                | Add `CatalogueConfig`, list of `ShardConfig`     |
| `server/src/database/async_database.cpp`          | One `AsyncDbPool`                                   | One pool per shard                                |
| `lib/queen.hpp` (`Queen` class)                   | One libpq connection set                            | One slot pool per shard                           |
| `lib/queen/pending_job.hpp`                       | Hard-codes SP names                                 | Job carries `shard_id` resolved by the router    |
| `server/src/services/shared_state_manager.cpp`    | Per-broker cache of queue config                    | Now also caches partition→shard map               |
| `lib/schema/procedures/016_partition_lookup.sql`  | Reconciles inside one DB                            | Reconciler runs per-shard                         |

### 4.3 Files that change in §5.3 (probes)

| File                                       | Change                                                                            |
| ------------------------------------------ | --------------------------------------------------------------------------------- |
| `server/src/routes/health.cpp`             | Split into `setup_health_routes` (liveness) + `setup_ready_routes` (readiness)    |
| `server/include/queen/file_buffer.hpp`     | Already has `is_ready()`; no change                                               |
| `server/src/acceptor_server.cpp`           | Pass `FileBufferManager*` into route context (it's already there)                 |
| `helm/templates/statefulset.yaml`          | Update `livenessProbe` + add `readinessProbe`                                     |
| `Dockerfile`                               | No change                                                                         |

### 4.4 Files that change in §5.4 (OTLP)

| File                                              | Change                                                  |
| ------------------------------------------------- | ------------------------------------------------------- |
| `server/Makefile`                                 | Optional vendor of `opentelemetry-cpp` (header-only mode) |
| `server/src/routes/push.cpp`, `pop.cpp`, `ack.cpp` | Start span on entry, end on response                    |
| `lib/queen.hpp` (drain orchestrator)              | Inject span context into stored-proc parameters         |
| `server/include/queen/config.hpp`                 | `OTLP_ENDPOINT`, `OTLP_HEADERS`, `OTLP_SAMPLE_RATIO`    |

## 5. Plan

### 5.1 Horizontal write scaling — single-region multi-PG

#### 5.1.1 Topology

```
                    ┌─────────────────────┐
                    │   Catalogue PG      │   queues, consumer_groups_metadata,
                    │   (singleton, HA)   │   system_state, partition→shard map
                    └─────────┬───────────┘
                              │  reads on every queue/cg config refresh,
                              │  writes only on queue create / cg create
                              │
        ┌─────────────────────┼──────────────────────┐
        │                     │                      │
        ▼                     ▼                      ▼
┌──────────────┐      ┌──────────────┐       ┌──────────────┐
│  Shard 0 PG  │      │  Shard 1 PG  │  ...  │  Shard N PG  │
│              │      │              │       │              │
│  messages    │      │  messages    │       │  messages    │
│  partition_  │      │  partition_  │       │  partition_  │
│   consumers  │      │   consumers  │       │   consumers  │
│  partition_  │      │  partition_  │       │  partition_  │
│   lookup     │      │   lookup     │       │   lookup     │
│  dlq         │      │  dlq         │       │  dlq         │
└──────┬───────┘      └──────┬───────┘       └──────┬───────┘
       │                     │                      │
       └─────────────────────┼──────────────────────┘
                             │
                       ┌─────┴───────┐
                       │ queen-server │   one libqueen instance per worker;
                       │   (broker)   │   each has N slot pools, one per shard
                       └──────────────┘
```

**Shard key**: `partition_id` (UUID). Distribution: stable hash
mod-N at partition creation time (recorded in catalogue
`partition_shard_map` table). Once a partition exists, its shard
never changes.

#### 5.1.2 Routing

In `lib/queen.hpp::submit()`, every job already carries a queue name
and (for POP/ACK) a partition id. The router resolves:

- PUSH: queue name → on-demand creation of a new `partition_id` →
  hash → shard. The catalogue allocates the partition row; the
  message rows go to the chosen shard.
- POP: queue name + partition filter → catalogue lookup of
  partition→shard → wildcard pop dispatched in parallel to all
  shards that own at least one partition matching the filter, results
  merged in libqueen.
- ACK / TRANSACTION / RENEW_LEASE: the lease object already carries
  `partition_id` → direct shard dispatch.

The catalogue's `partition_shard_map` is an obvious cache target
(read-only after partition creation). `SharedStateManager` already
caches `queue_config`; extend it to cache the shard map with TTL +
UDP invalidation on new partition creation.

#### 5.1.3 Schema split

- **Catalogue PG** runs a *subset* of `lib/schema/schema.sql`:
  `queues`, `consumer_groups_metadata`, `system_state`, plus a new
  `partition_shard_map (partition_id UUID PRIMARY KEY, shard_id
  INT NOT NULL)`.
- **Shard PGs** run the rest, plus the data-side stored procedures
  in `lib/schema/procedures/`.

Rather than two parallel schema directories, introduce a tag in each
`.sql` file (`-- @location: catalogue` / `-- @location: shard`) and
have `lib/queen.hpp::initialize_schema` filter by tag. This keeps
one source of truth, easy to grep.

#### 5.1.4 Migration path for existing single-PG users

- Default `SHARDS=1` → behaviour unchanged, schema applied to one PG
  that acts as both catalogue and shard 0 (no separation, no router
  cost).
- Adding shards: a new admin endpoint `/api/v1/admin/add_shard`
  registers a new shard PG, creates the data schema there, and
  re-balances by **drain-and-cutover**: new partitions assigned to
  the new shard, existing partitions stay where they are. No live
  data migration in v1; that earns its own memo.

#### 5.1.5 Effort

- Catalogue/shard schema split: 2 days.
- Router in libqueen + per-shard slot pool: 1 week.
- Wildcard POP fan-out: 3 days.
- Shared-state cache extension + UDP invalidation: 2 days.
- Tests (P3 streaming-CI machinery applies cleanly): 3 days.

Total: **~3 weeks** of focused work for an end-to-end prototype with
`SHARDS={1, 2}` proven on the existing benchmark suite.

### 5.2 PG HA / disaster-recovery runbook

This is documentation, not code. Output: a new
`developer/16-disaster-recovery.md` chapter and a one-pager at
`docs/disaster-recovery.html` cross-linking to it.

Topics:

1. **Backup tooling recommendation**: `pgBackRest` (rationale: WAL
   archiving, incremental backups, retention policy, compression).
   Alternative: `barman`. Concrete `pgbackrest.conf` snippet for a
   typical Queen deployment.
2. **Streaming replication**: synchronous standby for zero-RPO,
   asynchronous for lower latency. How to verify the standby is
   in sync (`pg_stat_replication`).
3. **Failover procedure**: promote standby, point Queen at new
   primary by updating `PG_HOST`, restart broker. The file-buffer
   absorbs the cut-over window automatically.
4. **Replay safety**: `(partition_id, transaction_id)` UNIQUE
   constraint guarantees `pg_dump | pg_restore` replays are
   idempotent. Document this explicitly so users running their own
   restore drills know what to expect.
5. **Geo-replication**: today, read-only standby in another region is
   safe; writes must still go to the primary. List this as a known
   limitation, link to §5.1 for the multi-PG path that lifts it.
6. **RPO/RTO baselines**: with synchronous replica + `pgBackRest`
   archive, expected RPO ≤ 10 s, RTO ≤ 2 min on healthy hardware.
   Numbers are illustrative, must be re-measured on user hardware.

#### Effort

**4 days** of writing + one rehearsed failover drill on a staging
cluster with screenshots and timings.

### 5.3 Liveness vs readiness probes

#### 5.3.1 New endpoints

```
GET /health     (liveness)   → 200 unless the process is wedged
GET /ready      (readiness)  → 200 only when the broker can serve
```

Liveness logic (`/health`):

- Worker thread responds (the request itself reaching this handler
  is proof).
- The libqueen drain orchestrator has not been stalled for longer
  than `LIVENESS_STALL_THRESHOLD_MS` (default 30 s; checked via
  `Queen::last_drain_pass_time()` on each worker).
- Returns 200 in all other cases. Never 503 due to PG outage.

Readiness logic (`/ready`):

- AsyncDbPool reports at least one healthy connection **OR**
  file-buffer is healthy (so PUSHes can still be served via failover).
- `FileBufferManager::is_ready()` is true (startup recovery complete).
- Schema apply finished (signalled by an atomic flag set in
  `acceptor_server.cpp` after `initialize_schema()` returns).
- Returns 503 with a JSON body listing which check failed.

#### 5.3.2 Helm chart update

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 6632
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 3      # 30 s grace before kill

readinessProbe:
  httpGet:
    path: /ready
    port: 6632
  initialDelaySeconds: 1
  periodSeconds: 5
  timeoutSeconds: 2
  failureThreshold: 2      # take pod out of service quickly
```

Apply the same to `helm_queen/templates/statefulset.yaml` if both
chart trees survive P0.

#### 5.3.3 Effort

- Code: 1 day (split health.cpp, expose `last_drain_pass_time`).
- Helm + docs: half a day.
- Test: simulate PG outage, confirm pod stays alive but is removed
  from service. Half a day.

Total: **2 days**.

### 5.4 OpenTelemetry / OTLP traces

#### 5.4.1 Approach

Vendor `opentelemetry-cpp` in header-only "API" mode plus the OTLP
HTTP exporter (single ~250 KB header set, no compiled deps beyond
what `server/Makefile` already links).

Span hierarchy:

```
http.push                                  (root, started in push.cpp)
└── libqueen.batch.push                    (started by drain orchestrator)
    └── pg.proc.push_messages_v3           (DB call, ended on slot completion)
http.pop                                   (root)
└── libqueen.batch.pop
    └── pg.proc.pop_unified_batch_v4
http.ack                                   (root)
└── libqueen.batch.ack
    └── pg.proc.ack_messages_v2
```

Cross-process trace propagation:

- Inbound: read `traceparent` header (W3C) on PUSH; propagate to
  `messages.metadata.traceparent` (already a free-form JSONB column
  via the existing `traceId` field — extend it).
- Outbound: emit `traceparent` on POP responses so consumers see
  the original producer's trace context.

Configuration knobs (`config.hpp`):

```
OTLP_ENABLED          (default: false)
OTLP_ENDPOINT         (e.g. http://otel-collector:4318/v1/traces)
OTLP_HEADERS          (comma-separated key=value, for auth tokens)
OTLP_SAMPLE_RATIO     (default 0.01; head sampler)
OTLP_SERVICE_NAME     (default queen-mq)
```

#### 5.4.2 What stays out

- Metric export over OTLP. Prometheus pull is the established
  surface; duplicating it as OTLP push is needless complexity.
  Users who want an OTLP-only world can run the OTel collector's
  Prometheus receiver against `/metrics/prometheus`.
- Log export over OTLP. spdlog is fine; structured logs are a
  separate concern.
- Client SDK instrumentation. Each client team adopts OTel on its
  own runtime; the server emits spans, that's enough to correlate.

#### 5.4.3 Sampling strategy

Default head sampler at 1 % is a starting point. For tail-latency
investigation, prefer running an OTel collector with a tail sampler
that keeps all spans whose root `http.push` exceeds, say, 200 ms.
Document this pattern in `developer/16-tracing.md` (new chapter).

#### 5.4.4 Effort

- Vendor + Makefile + linker flags: 1 day.
- Span instrumentation in 3 hot routes + drain orchestrator: 3 days.
- W3C propagation (header parse + JSONB stamp): 1 day.
- Tests against an OTel collector running in CI: 2 days.
- Docs: 1 day.

Total: **~1.5 weeks**.

## 6. Validation

| Item       | Done when                                                                                                   |
| ---------- | ----------------------------------------------------------------------------------------------------------- |
| §5.1       | Benchmark suite passes with `SHARDS=2`; per-shard throughput sums to ≥ 1.8× of `SHARDS=1` on the same host. |
| §5.2       | A staff engineer can recover from a "primary disk lost" simulation in < 10 min following the runbook only. |
| §5.3       | A pod survives a 60 s PG outage without restart; readiness flips to 503; flips back to 200 within 10 s.    |
| §5.4       | A PUSH→POP→ACK trace is visible end-to-end in Tempo/Jaeger with correct parent/child relations.            |

## 7. Risks & rollback

| Item       | Risk                                                              | Mitigation                                                  |
| ---------- | ----------------------------------------------------------------- | ----------------------------------------------------------- |
| §5.1       | Catalogue PG becomes new SPOF.                                    | Run catalogue as a 3-node Patroni cluster from day one.     |
| §5.1       | Cross-shard wildcard POP is slower than single-PG.                | Benchmark before declaring win; keep `SHARDS=1` default.    |
| §5.1       | Schema split desynchronises across releases.                      | One source-of-truth tagged file (§5.1.3); CI lint enforces. |
| §5.2       | Documentation drifts from reality.                                | Re-run drill once per release; embed timings in chapter.    |
| §5.3       | Misconfigured Helm probe causes spurious restarts.                | Default `failureThreshold` deliberately generous (3×10 s).  |
| §5.4       | OTLP exporter blocks on a slow collector and stalls the broker.   | Use the async batch exporter; bound queue at 2048 spans.    |
| §5.4       | Span cardinality explodes (`partition_id` as label).              | `partition_id` is a span *attribute*, never a metric label. |

All four items are independently revertible:

- §5.1: `SHARDS=1` is byte-equivalent to today's deployment.
- §5.2: documentation only.
- §5.3: keep `/health` returning the legacy combined check when a
  feature flag is off.
- §5.4: `OTLP_ENABLED=false` (default) is a no-op.

## 8. Effort estimate (totals)

| Item | Calendar |
| ---- | -------: |
| §5.1 Multi-PG sharding (prototype, `SHARDS=2`) | 3 weeks |
| §5.2 DR runbook + drill                        | 4 days  |
| §5.3 Liveness/readiness split                  | 2 days  |
| §5.4 OTLP traces                               | 1.5 weeks |

§5.2, §5.3, §5.4 are independent and can land in parallel. §5.1 is
the heavy lift and should not start before P3 (CI) is green so the
multi-shard test matrix has somewhere to run.

## 9. Sequencing recommendation

1. **§5.3 first** — smallest, unblocks safe Kubernetes deployment,
   no broker-internal changes required.
2. **§5.2 next** — pure docs + drill, builds operational confidence,
   informs §5.1's HA story.
3. **§5.4 in parallel with §5.1** — independent code paths, the
   tracing data is immediately useful for validating §5.1's
   correctness.
4. **§5.1 last** — biggest change, and the one that benefits most
   from the previous three landing first (probes, runbook, traces
   all help a multi-PG deployment go to production).

## 10. Follow-ups (out of this memo, but on the radar)

- **Active-active multi-region writes.** Requires a CRDT-style
  conflict resolution on `partition_consumers`. Earns its own memo;
  not before §5.1 is in production.
- **Tiered storage for cold messages.** `messages` rows older than
  `retention_seconds` could be moved to S3-backed cold storage and
  recalled on demand. Useful for replay-from-timestamp on long
  histories.
- **Per-tenant quotas / rate limits at the broker layer.** The 0.15
  streaming `.gate()` operator solves this in user code; a
  broker-level quota would be cheaper and centrally enforceable.
- **Native gRPC alongside HTTP.** uWebSockets does not speak gRPC;
  a small `grpc++` server in front of the same `AsyncQueueManager`
  would give Go/Python/Java clients lower per-call overhead.
