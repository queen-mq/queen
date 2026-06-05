# Queen Observability / Metrics Fix Plan

**Status:** open · **Owner:** TBD (handed to a follow-up agent) · **Severity:** high (caused a production-style OOM crash under sustained load on 2026-06-05)

This document is a self-contained brief for an engineer/agent who did **not** witness the
investigation. It explains the bug, the evidence, the root cause (with file references),
and a concrete, prioritized implementation plan.

---

## 1. TL;DR

Under sustained high data-path load (~100k+ msg/s push+pop), Queen's **observability
pipeline degrades and leaks memory**:

- The dashboard and `/api/v1/status` go blank / time out (HTTP 000, >25 s).
- `worker_metrics` (the source for throughput charts) lags **13–28 minutes** behind real time.
- The broker's `**CUSTOM` job queue grows without bound** (observed **38k → 128k jobs *per worker* ≈ 1.5 M total**), inflating broker RSS from ~4 GB → 13.6 GB. Combined with `shared_buffers` and many PG backends, this contributed to a host **OOM kill → Postgres crash**.

Root cause: **every observability/admin HTTP read runs a *live* aggregation query through a
single-concurrency (`f=1/1`), unbounded job lane.** When pollers (dashboard auto-refresh,
Prometheus scrape) arrive faster than that lane can drain — which always happens once the
data path slows Postgres — the lane backs up forever and holds the request memory.

The fix is to **decouple observability reads from the DB hot path** (serve from a cached
snapshot refreshed by one background job on a dedicated connection) and to **bound the
`CUSTOM` lane** as a safety net.

---

## 2. Symptoms (observed evidence)

From the 2026-06-04/05 soak on a 32-core VM (broker + Postgres co-located, clients on a
separate VM):


| Symptom                             | Evidence                                                                                                                                                        |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dashboard blank under load          | `/api/v1/status` returned HTTP 000 after >25 s while data path ran ~100k/s                                                                                      |
| Throughput chart "end dip" artifact | `worker_metrics` latest bucket lagged ~~13 min behind `now()`; rows for bucket `19:40` were written at `20:08` (~~28 min late)                                  |
| Broker memory leak                  | broker RSS 4 GB (20:20) → 13.6 GB (03:05); did **not** shrink when client load stopped                                                                          |
| Unbounded job lane                  | libqueen worker log: `custom(q=38062 f=1/1 …)` (20:25) → `custom(q=128432 f=1/1 …)` (03:14), i.e. ~1.5 M jobs queued across 12 workers, draining at ~1/s/worker |
| OOM crash                           | host memory hit 99% ~02:03 CEST → OOM killer → Postgres crash-recovery loop                                                                                     |


Key point: the **data path itself stayed healthy** (~100k push+pop) the whole time. Only the
**observability path** failed and then took the box down via memory.

---

## 3. Root cause analysis (with code references)

### 3.1 Every observability read = one live `CUSTOM` job

All status/analytics/admin **read** endpoints submit a live stored-procedure call as a
`JobType::CUSTOM` job. There is **no caching** at the HTTP layer.

- `server/src/routes/status.cpp:18-62` — `submit_sp_call()` sets `job_req.op_type = queen::JobType::CUSTOM` and enqueues via `ctx.queen->submit(...)`.
- `server/src/routes/status.cpp:90-108` — `GET /api/v1/status` → live `SELECT queen.get_status_v3($1::jsonb)`.
- Same pattern (live SP call per request) for: `/status/queues`, `/status/analytics`, `/analytics/system-metrics`, `/analytics/worker-metrics`, `/analytics/queue-lag`, `/analytics/queue-ops`, `/analytics/retention`, `/analytics/postgres-stats` (all in `status.cpp`).
- Same `submit_sp_call` → `CUSTOM` pattern in `server/src/routes/resources.cpp`, `consumer_groups.cpp`, `prometheus.cpp` (the `/metrics` scrape!), `traces.cpp`, `messages.cpp`.

### 3.2 The `CUSTOM` lane is single-concurrency and unbounded

- `lib/queen/batch_policy.hpp:123` — `JobType::CUSTOM` default policy is `{preferred=1, hold=0, max_batch=1, max_concurrent=1}`. So custom jobs run **one at a time per worker** (`f=1/1` in the logs).
- The per-type queue (`lib/queen/per_type_queue.hpp`, used from `lib/queen.hpp` `submit()`) is an **unbounded** `deque` — there is no max depth and no load-shedding.
- Net effect: if custom jobs arrive faster than ~1/RTT per worker, the queue grows forever, and each queued `JobRequest` holds its `sql`/`params`/`request_id` in memory → unbounded RSS.

Why arrival outruns draining under load: the aggregation SPs (`get_status_v3`,
`get_system_overview_v3`, `get_analytics_v1`, `refresh_all_stats_v1`) are **expensive** and
get **slower** as the data path saturates Postgres (LWLock/BufferContent contention), while
external pollers (browser dashboard auto-refresh, Prometheus scrape) keep arriving at a fixed
rate. Classic unbounded-queue feedback loop.

### 3.3 StatsService shares the main pool and runs synchronous heavy aggregations

- `server/src/services/stats_service.cpp:146,172,etc.` — `run_full_reconciliation()` / `run_fast_aggregation()` call `db_pool_->acquire()` (the **shared** AsyncDbPool) and run **blocking** `refresh_all_stats_v1`, `aggregate_*_stats`, etc.
- Under load this contends with the data path for connections and PG CPU/locks, so stats
cycles slow down and `queen.stats` goes stale (the dashboard then shows stale/zero values).

### 3.4 `worker_metrics` flush runs on the busy worker event loops

- libqueen workers own the throughput/lag counters and flush them to `queen.worker_metrics`
(see `WorkerMetrics` in `lib/queen/metrics.hpp`, member `_metrics` in `lib/queen.hpp`; the
comment in `stats_service.cpp:37` confirms "System throughput now handled by worker_metrics
in libqueen").
- Because the flush shares the worker's event loop and DB slots with push/pop drains, under
load the flush is starved → buckets are written minutes late → charts/dashboard under-count
the most-recent window (the "end dip" artifact).

---

## 4. Proposed solution (prioritized)

> North star: **observability must be O(1) and isolated** — a read endpoint must never run an
> unbounded amount of DB work, and the observability path must never share fate with the data
> path (neither starve it nor be starved by it).

### Fix 1 — Serve hot read endpoints from an in-memory cached snapshot *(highest impact)*

Stop doing a live DB query per status/metrics request.

- Add a background **ObservabilityService** (or extend `StatsService`) that, every
`N` seconds (configurable, e.g. 2–5 s), computes the payloads for the hot endpoints once:
  - `/api/v1/status` (`get_status_v3` with empty/default filters),
  - the Prometheus `/metrics` text,
  - system overview / system-metrics.
- Store each rendered JSON/text in a process-shared, lock-free-read structure (e.g.
`std::shared_ptr<const std::string>` swapped atomically; or reuse `SharedStateManager` in
`server/src/services/shared_state_manager.cpp`).
- HTTP handlers for those routes **return the cached snapshot directly** — no `submit_sp_call`,
no `CUSTOM` job, no per-request DB hit. Add an `age`/`generatedAt` field so clients know
freshness.
- Result: dashboard + Prometheus polling cost ~0 on the broker/DB regardless of poll rate →
eliminates the `CUSTOM` queue growth for the common case and the timeouts.

Endpoints that take **arbitrary filters / time ranges** (specific queue detail, analytics
time-series with custom `from/to`) can stay "live" but must be covered by Fix 2 + Fix 3.

### Fix 2 — Bound the `CUSTOM` lane and load-shed *(safety net, do alongside Fix 1)*

Make it impossible for the lane to ever leak memory again.

- Add a max queue depth for `JobType::CUSTOM` (env, e.g. `QUEEN_CUSTOM_MAX_QUEUE`, default a
few hundred). In `Queen::submit()` (`lib/queen.hpp`), if the `CUSTOM` queue is at capacity,
**reject** instead of enqueue (return a flag / invoke the callback with an error result so
the route can send **HTTP 503 + Retry-After**).
- Consider raising `CUSTOM` `max_concurrent` from 1 → small N (2–4) via
`QUEEN_CUSTOM_MAX_CONCURRENT` so the lane drains faster, but only **with** the bound in
place. (Note the read-after-write caveat in `batch_policy.hpp:36-39` does not apply to
read-only custom queries.)
- Add a metric/log for current `CUSTOM` queue depth + drops so this is observable.

### Fix 3 — Give observability its own connection lane *(isolation)*

- Route StatsService and the ObservabilityService through a **dedicated** small connection
pool / the existing "service DB threads" (the startup log shows `Service DB threads: 5`),
**not** `db_pool_->acquire()` on the shared pool (`stats_service.cpp`).
- This guarantees stats/observability make progress even when the data-path sidecar pool is
saturated, and that they cannot consume data-path connections.

### Fix 4 — Make the `worker_metrics` flush load-independent *(fixes chart/dashboard truth)*

- Flush `queen.worker_metrics` from a **dedicated service thread + connection**, on a fixed
cadence, independent of the worker drain loops.
- Make it **bounded**: if the flush falls behind, coalesce/aggregate or drop the oldest
in-memory buckets rather than letting them queue — recent buckets must reflect reality.
- Acceptance: under 100k/s load, `max(bucket_time)` stays within ~1 flush interval of `now()`.

### Fix 5 — Confirm the read SPs use pre-aggregated tables, not live scans *(verify/cheapen)*

- Audit `get_status_v3`, `get_analytics_v1`, `get_system_overview_v3`,
`get_status_queues_v2`, `get_queue_lag_v1`, `get_queue_ops_v1` (in
`lib/schema/procedures/*.sql` and `lib/schema/schema.sql`) to ensure they read from
pre-aggregated tables (`queen.stats`, `queen.worker_metrics`, `queen.queue_lag_metrics`) and
**do not scan `queen.messages`** (which is huge under load). Add indexes / pre-aggregation
where any live scan remains. This makes both the cached refresh (Fix 1) and any remaining
live endpoints cheap.

---

## 5. Recommended implementation order

1. **Fix 2** (bound `CUSTOM` lane) — small, isolated, immediately removes the OOM risk. Ship first.
2. **Fix 1** (snapshot cache for `/status`, `/metrics`, overview) — biggest UX + load win.
3. **Fix 3** (dedicated observability connection lane).
4. **Fix 4** (worker_metrics flush resilience).
5. **Fix 5** (SP audit / pre-aggregation) — ongoing hardening.

Fixes 1–3 together fully resolve the incident; 4–5 restore chart/dashboard accuracy and
prevent regressions.

---

## 6. Validation / acceptance tests

Reproduce the load (tooling already built, see §7) and verify:

1. **No unbounded queue:** under 100k+/s push+pop with a dashboard tab open **and** a
  Prometheus scraper hitting `/metrics` every 5 s for ≥30 min, the libqueen `custom(q=…)`
   value stays bounded (< a few hundred) and broker RSS stays flat (no multi-GB growth).
2. **Status stays fast:** `/api/v1/status` returns 200 in < 200 ms throughout (cached), or a
  clean 503 if the live lane is intentionally shed — never a >25 s hang.
3. **Metrics are fresh:** `max(bucket_time)` in `queen.worker_metrics` stays within ~1
  interval of `now()` under load; the dashboard throughput matches the direct
   `n_tup_ins`/`n_tup_del` deltas (cross-check method in §7).
4. **No fate-sharing:** killing/slowing the observability path does not change push/pop
  throughput, and saturating the data path does not blank the dashboard.

---

## 7. Reproduction & cross-check tooling (already in repo)

- **Load generator:** `benchmark-queen/2026-06-04/goload/` (Go, uses the official
`clients/client-go` with a tuned HTTP transport). Build:
`cd benchmark-queen/2026-06-04/goload && GOWORK=off GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o goload-linux-amd64 .`
Run (balanced, retention on): `./goload-linux-amd64 -url http://BROKER:6632 -queue benchq -partitions 300 -producers 1000 -consumers 300 -push-batch 10 -pop-batch 200 -completed-retention 300 -pending-retention 600`
- **Broker bring-up:** `benchmark-queen/2026-06-04/start-broker.sh` (PG + queen) and
`restart-queen.sh` (queen-only; supports `QUEEN_CONCURRENCY_MODE`, pool sizes, hold).
- **Direct ground-truth throughput** (bypasses the laggy `worker_metrics`): sample
`xact_commit` from `pg_stat_database` and `n_tup_ins`/`n_tup_del` from
`pg_stat_user_tables WHERE relname='messages'` over a window. `long-mon.sh` logs these
every 30 s. (This is how the "end dip" was proven to be a metrics artifact, not a real
throughput drop.)

---

## 8. Key knobs & locations cheat-sheet


| Thing                                                | Where                                                                                                                                                                                       |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Custom-job submit (read endpoints)                   | `server/src/routes/status.cpp:18-62`; also `resources.cpp`, `consumer_groups.cpp`, `prometheus.cpp`, `traces.cpp`, `messages.cpp`                                                           |
| `CUSTOM` batch policy (`max_concurrent=1`)           | `lib/queen/batch_policy.hpp:123`                                                                                                                                                            |
| Job submit / per-type queue (unbounded)              | `lib/queen.hpp` (`submit()`), `lib/queen/per_type_queue.hpp`                                                                                                                                |
| Drain loop (fires while policy=FIRE & `try_acquire`) | `lib/queen.hpp` `_drain_orchestrator()` (~line 883)                                                                                                                                         |
| Stats pre-aggregation (shared pool, sync)            | `server/src/services/stats_service.cpp`                                                                                                                                                     |
| worker_metrics counters/flush                        | `lib/queen/metrics.hpp` (`WorkerMetrics`), `_metrics` in `lib/queen.hpp`                                                                                                                    |
| Read SPs to audit                                    | `lib/schema/schema.sql` (`get_status_v3` ~L509, `get_system_overview_v3` ~L413, `get_worker_metrics_timeseries_v1` ~L842), `lib/schema/procedures/009_status.sql`, `014_worker_metrics.sql` |
| Service thread pool ("5 service DB threads")         | `server/src/acceptor_server.cpp` (pool construction)                                                                                                                                        |


---

## 9. Notes / gotchas

- `POST /api/v1/configure` is a **full upsert** — partial option updates reset unspecified
fields to defaults. (Unrelated bug we hit: a client that set `completedRetentionSeconds`
but not `retentionEnabled` silently left retention **off**.) If the fix adds any
configure-style write path for observability, preserve-merge semantics or document the
upsert behavior.
- Do **not** "fix" this by making Vegas/concurrency more aggressive — the data path is fine;
this is purely an observability-isolation + bounding problem.
- The simplest version of Fix 1 (cache just `/status` and `/metrics`) already removes the vast
majority of real-world poll load. Don't over-engineer the first cut.