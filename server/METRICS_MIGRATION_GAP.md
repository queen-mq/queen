# Metrics migration gap — old C++ (0.16.0) vs new Rust server

## 1. Executive summary

The Rust port drops two whole layers of the observability stack. **First, ~15 HTTP endpoints that the (byte-identical) dashboard calls are never registered** in `server/src/main.rs` — the entire `/api/v1/analytics/*` family, the deeper `/api/v1/status/*` routes, and all of `/api/v1/migration/*` — so those calls 404. **Second, none of the five C++ background collectors were ported** (`main.rs` spawns only `retention`, `udp`, `fusion` — lines 82, 132–168, 84), so the tables that fed metrics — `queen.stats`, `queen.stats_history`, `queen.system_metrics`, `queen.worker_metrics(_summary)`, `queen.retention_history`, `queen.partition_lookup` — are never written. The blast radius: **System, QueueOperations and Migration pages are fully broken (404); Analytics and Dashboard render but show all-zero throughput/latency/lag**; only Queues/Messages/Consumers/DLQ/Traces still work.

## 2. Missing / broken HTTP endpoints

All "old C++ handler" entries are registered in `queen-old-master/server/src/routes/status.cpp` / `migration.cpp` at the cited lines; none appear in the Rust route table (`main.rs:173–316`, full literal list verified).

| Endpoint | Frontend caller (view) | Old C++ handler | Backing SQL fn / table | Status in Rust |
|---|---|---|---|---|
| `GET /api/v1/status/queues/:queue` | `analytics.getQueueDetail` — QueueDetail.vue | status.cpp:137 | `get_queue_status_*` | **404 — not registered** |
| `GET /api/v1/status/queues/:queue/messages` | QueueDetail (message drill-down) | status.cpp:152 | messages readers | **404 — not registered** |
| `GET /api/v1/status/analytics` | `analytics.getAnalytics` — Analytics.vue | status.cpp:170 | `get_status_v3`/analytics | **404 — not registered** |
| `GET /api/v1/status/buffers` | (buffer/backpressure panel) | status.cpp:330 | in-proc buffer state | **404 — not registered** |
| `GET /api/v1/analytics/system-metrics` | `system.getSystemMetrics` — System.vue | status.cpp:192 | `get_system_metrics_*` ← `queen.system_metrics` | **404 — not registered** |
| `GET /api/v1/analytics/worker-metrics` | `system.getWorkerMetrics` — QueueOperations.vue | status.cpp:213 | `get_worker_metrics_timeseries_v1` ← `queen.worker_metrics` | **404 — not registered** |
| `GET /api/v1/analytics/queue-lag` | `system.getQueueLag` — QueueOperations.vue | status.cpp:234 | `get_queue_lag_v1` ← `queen.worker_metrics` | **404 — not registered** |
| `GET /api/v1/analytics/queue-ops` | `system.getQueueOps` — QueueOperations.vue | status.cpp:258 | `get_queue_ops_v1` ← `queen.worker_metrics` | **404 — not registered** |
| `GET /api/v1/analytics/queue-parked-replicas` | `system.getQueueParkedReplicas` — QueueOperations.vue | status.cpp:279 | `get_queue_parked_per_replica_v1` | **404 — not registered** |
| `GET /api/v1/analytics/retention` | `system.getRetention` — QueueOperations.vue | status.cpp:298 | `get_retention_timeseries_v1` ← `queen.retention_history` | **404 — not registered** |
| `GET /api/v1/analytics/postgres-stats` | `system.getPostgresStats` — System.vue | status.cpp:317 | `get_postgres_stats_v1` (`pg_stat_*`) | **404 — not registered** |
| `POST /api/v1/migration/test-connection` | `migration.testConnection` — Migration.vue | migration.cpp:171 | (pg_dump/restore infra) | **404 — not registered** |
| `POST /api/v1/migration/start` | `migration.start` — Migration.vue | migration.cpp:244 | — | **404 — not registered** |
| `GET /api/v1/migration/status` | `migration.getStatus` — Migration.vue | migration.cpp:433 | — | **404 — not registered** |
| `POST /api/v1/migration/validate` | `migration.validate` — Migration.vue | migration.cpp:447 | — | **404 — not registered** |
| `POST /api/v1/migration/reset` | `migration.reset` — Migration.vue | migration.cpp:590 | — | **404 — not registered** |

**Registered but returns zeros** (route present, SP reads an empty table):
| `GET /api/v1/status` | Dashboard, Analytics | status.cpp:90 | `get_status_v3` ← `queen.worker_metrics`+`queen.system_metrics` (014_worker_metrics.sql:588,602,661,677) | **200 but all-zero throughput/lag** |
| `GET /api/v1/status/queues` | Analytics | status.cpp:112 | `get_status_queues_v2` — raw, **no seg enrichment** (`status.rs:73–92`) | **200 but zero counts** |

**Param routes not present in EITHER server (pre-existing frontend gaps, NOT migration regressions):** `messages.retry` → `POST /messages/:pid/:tid/retry`, `messages.moveToDLQ` → `.../dlq`, `queues.clear` → `DELETE /api/v1/queues/:name/clear`, `queues.getPartitions` → `/api/v1/resources/partitions`. A full literal-route sweep of `queen-old-master/server/src/routes/*.cpp` shows the old C++ server never registered these either (old messages.cpp registers only `GET`/`GET`/`DELETE` on `/messages` and `/messages/:pid/:tid`). Flag for product, but they are out of scope for the port regression.

## 3. Missing background collectors (why the data is zero even where routes exist)

Instantiated in C++ at `queen-old-master/server/src/acceptor_server.cpp:486–553`. Rust `main.rs` spawns none of them — only `retention::spawn` (line 82), UDP (132), and `fusion` (84). Verified: a repo-wide grep for writers of these tables in `/Users/alice/Work/queen/server/src` returns only comments admitting they read 0 (`status.rs:140–143`, `db.rs` ~247/282).

### MetricsCollector — **MISSING**
Sampled host/process metrics every `metrics_sample_interval_ms=1000` (1 s) and `INSERT`ed a 60-s aggregate into `queen.system_metrics` (`metrics_collector.cpp:317 aggregate_and_save`, `metrics_aggregate_interval_s=60`; config.hpp:297–298). No Rust equivalent — `metrics.rs` is Prometheus counters only, no DB flush. → **`queen.system_metrics` never written.** Also, worker-level counters (`worker_metrics.hpp`) that flushed into `queen.worker_metrics` and `update_worker_metrics_summary()` → `queen.worker_metrics_summary` are not flushed to PG in Rust. → **`queen.worker_metrics` + `queen.worker_metrics_summary` never written.**

### StatsService — **MISSING (the P0)**
Fast roll-up every `stats_interval_ms=10000` (10 s) and full reconcile-from-messages every `stats_reconcile_interval_ms=120000` (2 min) (config.hpp:305–306). Called `aggregate_queue_stats_v2`, `aggregate_namespace_stats_v1`, `aggregate_task_stats_v1`, `aggregate_system_stats_v2`, `compute_partition_stats_chunk_v1`, `write_stats_history_v1`, `cleanup_orphaned_stats_v1/2` (grep of `stats_service.cpp`). → **`queen.stats` and `queen.stats_history` never populated.** `db.rs:245` comment confirms: "the segments engine populates queen.queues but NOT queen.stats." The `POST /api/v1/stats/refresh` route IS wired (`main.rs:259` → `refresh_all_stats_v1(true)`), but its SP updates `queen.stats` from `queen.messages` (013_stats.sql:102,285,293) — empty under the segments engine — so an on-demand refresh still yields zeros.

### EvictionService — **effectively PARTIAL**
`max_queue_size` enforcement on a timer; deleted from `queen.messages` and logged to `queen.retention_history`. Rust folds eviction into the retention loop via `queen.seg_evict_v1()` (`retention.rs:119`) against `seg_*`. Functionally eviction happens, but it does **not** write `queen.retention_history`.

### RetentionService — **PARTIAL**
Age/size purge every `retention_interval=300000` (5 min); deleted `queen.messages` and appended `queen.retention_history` (retention_service.cpp DELETE + retention_history). Rust ports the deletion (`seg_retention_sweep_v1`, `retention.rs:115`) but targets `seg_*` and does not write `queen.retention_history`. → **`queen.retention_history` never populated** (retention analytics time series empty even if the route existed).

### PartitionLookupReconcileService — **MISSING**
Stats-leader-gated reconcile every `partition_lookup_reconcile_interval_ms=5000` (5 s) calling `reconcile_partition_lookup_v1` → `queen.partition_lookup` (config.hpp:312). No Rust task. → **`queen.partition_lookup` never reconciled.**

## 4. Unpopulated metrics tables

| Table | Written by (C++) | Written by (Rust)? | Readers that now return zero |
|---|---|---|---|
| `queen.stats` | StatsService (`aggregate_*`, 10 s / 2 min) | **No** (`db.rs:245` comment; refresh SP reads empty `queen.messages`) | `get_system_overview_v3` (Dashboard), `get_status_queues_v2` (Analytics), namespace/task counts |
| `queen.stats_history` | StatsService (`write_stats_history_v1`) | **No** | historical stats trends |
| `queen.system_metrics` | MetricsCollector (`INSERT`, 60 s) | **No** | `get_system_metrics_*` (System.vue), `get_status_v3` health block |
| `queen.worker_metrics` | per-worker flush (`worker_metrics.hpp`) | **No** (metrics.rs = Prometheus only) | `get_worker_metrics_timeseries_v1`, `get_queue_lag_v1`, `get_queue_ops_v1`, `get_status_v3` throughput |
| `queen.worker_metrics_summary` | `update_worker_metrics_summary()` | **No** (`status.rs:142`: "rust broker does not write worker_metrics_summary … read 0") | `get_system_totals_v1`, Prometheus `queen_db_*` lifetime totals |
| `queen.retention_history` | Retention + Eviction services | **No** (Rust sweeps `seg_*`, doesn't log history) | `get_retention_timeseries_v1` |
| `queen.partition_lookup` | PartitionLookupReconcileService (5 s) | **No** | partition-lookup consumers |

## 5. Per-page impact

| App page | API calls | Verdict | What the user sees |
|---|---|---|---|
| **Dashboard.vue** | `analytics.getStatus` (200/zeros), `resources.getOverview` (raw `get_system_overview_v3`, no seg enrichment — `queues.rs:279–291`) | **EMPTY-zeros** | Queue *count* correct; partitions/messages/pending, throughput and lag all 0; flat charts |
| **Analytics.vue** | `analytics.getStatus`, `analytics.getQueues`, `resources.getNamespaces/getTasks` | **EMPTY-zeros** | Page loads; throughput/latency time series flat; per-queue table shows 0 msgs/partitions |
| **QueueDetail.vue** | `analytics.getQueueDetail` → `/status/queues/:queue` | **BROKEN-404** | Detail view fails to load / error state |
| **QueueOperations.vue** | `getQueueLag`, `getQueueOps`, `getQueueParkedReplicas`, `getRetention`, `getWorkerMetrics` (all `/analytics/*`) | **BROKEN-404** | Every panel errors; no ops data at all |
| **System.vue** | `getSystemMetrics`, `getPostgresStats` (`/analytics/*`) | **BROKEN-404** | CPU/mem/PG-stats panels error out |
| **Migration.vue** | `migration.*` (5 routes) + `system.getMaintenance/setMaintenance` (these 2 work) | **BROKEN-404** | Maintenance toggles work; test/start/status/validate/reset all 404 |
| **Queues.vue** | `queues.list` → `/resources/queues` (seg-enriched, `queues.rs:242`) | **WORKING** | Real partition/message counts |
| **Messages.vue** | `messages.list/get/delete` → `/messages*` | **WORKING** (retry/DLQ buttons dead — see §2 note) | Message list/detail fine |
| **Consumers.vue** | `consumers.*` → `/consumer-groups*` | **WORKING** | — |
| **DeadLetter.vue** | `dlq.list/getStatus` (200) | **WORKING** (list) / retry+delete: retry 404s (see §2) | DLQ list renders; retry button 404s |
| **Traces.vue** | `traces.*` | **WORKING** | — |

## 6. Prioritized remediation plan

**P0 — restore stats population + the `/status/*` reads (unblocks Dashboard, Analytics, QueueDetail).**
1. Add a **StatsService equivalent** as a spawned task (new `server/src/stats.rs`, spawned in `main.rs` beside `retention::spawn`, line 82). It must recompute queue/partition/namespace/task counts **from the `seg_*` tables** (segments engine), not from the empty `queen.messages` — i.e. a new `seg_refresh_all_stats_v1()` SP (add to `server/sql/procedures/013_stats.sql`) that `UPDATE`s/`INSERT`s `queen.stats` and appends `queen.stats_history` from `seg_queues/seg_partitions/seg_segments`. Cadence: mirror `stats_interval_ms=10000` / reconcile `120000`. Then rewire `db.rs:849 refresh_all_stats` and `handle_stats_refresh` to call it so the manual button also works.
2. Register the missing status routes in `main.rs`: `GET /api/v1/status/queues/:queue`, `.../messages`, and `GET /api/v1/status/analytics` (new handlers in `handlers/status.rs`, backed by the seg-aware SPs). Alternatively short-term, seg-enrich `handle_status_queues` the same way `handle_list_queues` does (`queues.rs:242`) so Analytics stops showing zeros even before the collector lands.

**P1 — MetricsCollector → `system_metrics`/`worker_metrics` + the `/analytics/*` routes (unblocks System, QueueOperations).**
3. Add a **metrics-collector task** (new `server/src/syscollect.rs`, spawned in `main.rs`): sample host/proc + roll the in-process `metrics.rs` counters and `INSERT` a 60-s aggregate into `queen.system_metrics`, and flush per-worker throughput into `queen.worker_metrics` (+ `update_worker_metrics_summary()`), cadences `metrics_sample_interval_ms=1000` / `metrics_aggregate_interval_s=60`.
4. Register and implement the seven `/api/v1/analytics/*` routes (new `handlers/analytics.rs`): `system-metrics`, `worker-metrics`, `queue-lag`, `queue-ops`, `queue-parked-replicas`, `postgres-stats`, `retention`. The SPs already exist (`sql/procedures/007,014,015,017`); they just need `db.rs` wrappers + route wiring in `main.rs`. `postgres-stats` (`get_postgres_stats_v1`, reads `pg_stat_*`) works immediately once routed.

**P2 — migration, retention-history, partition-lookup.**
5. Port the migration surface: 5 routes (`handlers/migration.rs`) + the pg_dump/restore driver (from `migration.cpp`), or explicitly retire the Migration page if segments migration is handled by the `queen migrate` subcommand (`main.rs:37`).
6. Make the retention loop write `queen.retention_history` (extend `seg_retention_sweep_v1`/`seg_evict_v1` in `retention.rs`/`sql/procedures`) so `get_retention_timeseries_v1` has data once its route is added in step 4.
7. Add a **partition-lookup reconcile task** (calls `reconcile_partition_lookup_v1`, cadence 5 s, leader-gated) if any consumer still depends on `queen.partition_lookup`; otherwise drop the table and its reader.

Also worth an explicit decision: the frontend `messages.retry`/`moveToDLQ`/`queues.clear`/`queues.getPartitions` methods target routes **neither** server implements — either wire them server-side or remove the dead buttons.