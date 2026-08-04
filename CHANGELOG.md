# Changelog

Release history for the Queen MQ server and client SDKs. Full release notes live on
[GitHub Releases](https://github.com/queen-mq/queen/releases).

## 1.0.0-beta.2

**One admission arbiter replaces the two Vegas limiters.** Concurrency against PostgreSQL
was governed by a pair of TCP-Vegas-style controllers, one per lane, each inferring
queueing from per-operation round-trip time. On a WAL-bound commit path most of the excess
over the observed minimum is the group-commit flush wait: intrinsic cost that admission
cannot remove. The estimator counted that floor as congestion and backed off from it, and
because its grow and shrink thresholds were absolute the dead band widened as the limit
fell, making low limits an attractor. The broker left cores idle while the connection pool
reported no waiters at all, and nothing exported the controller's inputs, so the
disagreement was invisible.

`server/src/admission.rs` replaces it with a single arbiter for every write transaction:

- **The sensor is passive.** Group commit clusters write completions in time, so grouping
  the broker's own commit completions measures the flush pipeline with no PostgreSQL-side
  telemetry. Train size is amortisation measured rather than assumed; train cadence is the
  flush rate; the gap between train starts is the flush cycle.
- **One budget, four lanes.** Push, Pop, Ack and Maint share a work-conserving budget.
  Guarantees act on the wake order when the budget is exhausted, acks first because they
  unblock lanes. The budget never rises above `DB_POOL_SIZE` minus a reserve, so admitted
  work cannot starve on the pool.
- **Slots are RAII.** Dropping a slot releases it. The previous limiter leaked its
  in-flight counter at eight call sites that took a permit and returned early, until its
  own anti-ramp guard was permanently disabled.
- **A degraded mode is detected, not guessed.** With `synchronous_commit = off` commit
  waits collapse and the trains carry no signal; the arbiter pins a static budget and says
  so in telemetry (`adm_mode`).

**The pop claim path is set-based.** `queen.log_pop_list_v1` used to call `log_pop_v1` once
per candidate partition, about six statement executions each, inside the admission permit
and the committing transaction. It now does the same work in roughly six statements total,
independent of the candidate count. Measured on the sparse-partition shape, the per-pop
cost was 3.42 ms of which 0.17 ms was data work.

**New defaults.** `QUEEN_V2_FUSION_HOLD_MS` is 3, down from 15: the hold is paid twice per
flow, on ingress and on the derived republish. `QUEEN_ADMISSION_MIN` and
`QUEEN_ADMISSION_INIT` now derive from the pool, two thirds of
`DB_POOL_SIZE - QUEEN_ADMISSION_POOL_RESERVE`, which is 96 on the defaults. They are
derived rather than fixed so a small deployment cannot admit more concurrent transactions
than it has connections for. On the 2000 ev/s / 1000-lane shape, raising the floor alone
took p50 from 1143 ms to 240 ms.

**Validated by a soak.** 600,000 msg/s for 3 h 10 m with full production semantics (leases,
explicit async acks, 60 s dedup window, retention active): **6.82 billion messages, zero
push, pop and ack errors, flat lag**, p50 120 ms and p99 297 ms, with the median steady
inside ±8% across the run.

**Known weakness, stated because this is a beta.** The shrink signal is the age of the
oldest admitted slot, which conflates statement execution with commit wait: a transaction
doing genuine heavy work looks like one waiting behind a queue. On workloads whose write
transactions routinely run long the budget hunts rather than settles. This is documented in
`admission.rs` and on [flow control](https://queenmq.com/internals/flow-control), and the
replacement signal is named there.

### Breaking

- **Prometheus metric names changed.** `queen_seg_push_vegas_limit` and
  `queen_seg_pop_vegas_limit` no longer exist. The replacements are
  `queen_admission_budget`, `queen_admission_inflight{lane}`,
  `queen_admission_waiting{lane}`, `queen_admission_trains_per_s`,
  `queen_admission_txn_per_train` and `queen_admission_cycle_ms`. **Dashboards and alerts
  referencing the old names need updating.**
- **The `rates` log line changed shape.** `vegas_push` and `vegas_pop` are replaced by
  `adm_budget`, `adm_mode` and `adm_lanes`.
- **Vegas-era variables are no longer read.** `QUEEN_SEG_{PUSH,POP}_{INIT,MIN,MAX}`,
  `QUEEN_VEGAS_ALPHA` and `QUEEN_VEGAS_BETA` are accepted by the environment and ignored;
  the broker logs a warning at boot for each one it finds set, rather than letting a
  deployment tune a control loop that is not there. The Helm chart no longer sets the eight
  libqueen-era `QUEEN_{PUSH,ACK,POP}_MAX_CONCURRENT` / `QUEEN_VEGAS_*` /
  `QUEEN_PUSH_*_BATCH` variables, none of which the Rust broker ever read.

## 1.0.0-beta.1

First beta of the 1.0 line: the Rust broker on the segment storage engine described under
[1.0.0](#100) below, plus the tenancy surface built on top of it.

- **Multi-tenancy and the gateway.** Native tenant scoping in the broker behind
  `QUEEN_TENANCY_HEADER` (off by default), and `queen-proxy`, a separate Apache-2.0 gateway
  carrying API keys, plans, quotas, rate limiting, metering and a console.
- **One webapp behind the proxy.** Auth, roles, tenant selector and the operator surface in
  a single application, re-keyed onto the documentation site's palette.
- **The migration tool is gone.** It shelled out its PostgreSQL connection parameters,
  which was a remote-code-execution path; the fix landed first and the tool was then
  removed rather than kept.
- **Tenancy correctness and cost.** Each tenant gets its own discovery wake gate, so one
  tenant's pushes stop waking every other tenant's parked consumers. Confirmed
  partition-to-tenant ownership is cached, dropping a round trip per ack.
- **Hot-list lease revisit is bounded**, which removes a stall that could hold a single
  partition indefinitely.

## 1.0.0

**A Rust broker on a new storage engine.** The 0.x line was a C++ implementation
(libqueen, uWebSockets, libpq) storing one row per message in `queen.messages`. 1.0.0
replaces both halves: the broker is a single stateless Rust binary (`queen-seg`, axum and
tokio over a pooled `libpq`), and the storage engine is a log of compressed segments.

### The storage engine

A message's position is now a single monotone per-partition offset. A segment is one row
holding many length-prefixed frames, packed and zstd-compressed, so a push writes segments
rather than rows. Consumption state is one cursor per (partition, consumer group) in
`queen.log_consumers`: there is no per-message delivery state anywhere in the schema, which
is what makes tens of thousands of partitions cheap.

- **Acknowledgement is an offset commit.** Acking a message commits the cursor past it and
  implicitly completes everything before it in that partition for that group. Acks still
  arrive addressed by `transactionId`; the broker resolves them through a 16-byte-per-frame
  hash sidecar (`queen.log_txns`).
- **Two honesty guarantees, both contract-tested.** An explicit `failed`, `dlq` or `retry`
  is never skipped by a later `completed` ack in the same call: the cursor clamps at the
  lowest signal. An ack that lands below the cursor or outside the hash window is reported
  as a no-op rather than silently succeeding.
- **Deduplication is exact and enforced in SQL.** The probe of the transaction-id sidecar
  happens under the partition row lock, before an offset is allocated, so a duplicate writes
  nothing. The broker-side cache can only narrow that probe, never change its verdict.
- **Commits are amortised.** A fusion layer groups pending writes by partition and bundles
  disjoint partitions into one transaction, so N segments cost one commit and one fsync.
- **Pop candidates come from memory.** An in-process hot-list ring replaces the SQL
  candidate scan on the wildcard pop path, with a deferred-visibility wheel for leases and
  timed retries.

### Elsewhere

- **Multi-tenancy.** Native tenant scoping in the broker behind `QUEEN_TENANCY_HEADER`
  (off by default), plus `queen-proxy`, a separate Apache-2.0 gateway with API keys, plans,
  quotas, rate limiting, metering and a console. Documented under
  [Self-hosting](https://queenmq.com/selfhost).
- **Multi-broker coordination is framed TCP**, not UDP. The old `QUEEN_UDP_*` variables are
  accepted as aliases for `QUEEN_MESH_*`. Everything the mesh carries is a best-effort hint;
  PostgreSQL remains the only source of truth. **The mesh port must be firewalled.**
- **The dashboard is compiled into the binary** (`rust_embed`). There is no
  `QUEEN_STATIC_DIR` and no on-disk assets at runtime.
- **The dashboard works broker-direct.** With auth off (the default) the broker answers
  `GET /auth/me` itself with a standalone operator identity, so the full dashboard runs
  with no proxy: every view live, session controls hidden. With `JWT_ENABLED=true` the
  broker serves an explanation page at `/auth/login` instead; a dashboard with logins and
  roles remains `queen-proxy`'s. Documented at
  [queenmq.com/selfhost/dashboard](https://queenmq.com/selfhost/dashboard).
- **New documentation.** [queenmq.com](https://queenmq.com) is rewritten from the current
  source. Its route table, environment-variable reference, metric list, proxy route classes,
  OpenAPI documents and benchmark figures are generated from the code and the archived
  benchmark artifacts, and CI fails when any of them falls behind.

### Breaking

- **The migration tool is gone.** `POST /api/v1/migration/*` and its handler were removed.
  Back up with `pg_dump` over the `queen` and `queen_streams` schemas.
- **The retired engine's objects are dropped at boot.** Applying the 1.0.0 schema removes
  the previous engine's tables and procedures. This is not a data migration: messages stored
  by a 0.x broker do not carry over. Drain a queue before upgrading, or start fresh.
- **Prometheus names moved.** The in-process counters are `queen_process_*`; the
  `queen_cluster_*` namespace now means database-backed lifetime totals, identical on every
  instance. Per-queue series sum across tenants, so the endpoint is not a per-tenant surface.
- **`QUEEN_STATIC_DIR` no longer exists**, and several 0.x tuning variables
  (`NUM_WORKERS`, `QUEEN_*_SLOTS`, `SIDECAR_*`, `RESPONSE_BATCH_*`) have no equivalent: the
  Rust broker's concurrency is adaptive and sized from the connection pool.

### Compatibility

The HTTP message plane keeps the 0.16.0 contract: push, pop, ack, transaction and lease
extension are unchanged on the wire, and an existing SDK keeps working against a 1.0.0
broker for those calls. The SDKs are version-aligned at 1.0.0. Details, including which
client methods target routes that no longer exist, are in
[the compatibility reference](https://queenmq.com/reference/compatibility).

Measured behaviour, with the configuration of every run attached, is at
[queenmq.com/benchmarks](https://queenmq.com/benchmarks).

## Release History

> Every row below **0.16.0 and including it** describes the retired C++ implementation and
> its row-based storage engine. Those measurements and architecture notes do not describe
> 1.0.0.

**JS clients from version 0.12.0 can be run inside a browser**

| Server Version | Description                                                                                                                     | Compatible Clients                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| **1.0.0**      | **Rust broker on a segment-based log engine.** Offsets instead of row cursors, acknowledgement as an offset commit, exact windowed deduplication enforced before offset allocation, amortised commits via request fusion, an in-memory pop candidate ring, framed-TCP mesh, native multi-tenancy and a separate multi-tenant gateway. See the section above. | SDKs are version-aligned at 1.0.0; the message plane keeps the 0.16.0 wire contract |
| **0.16.0**     | **Push-serialization architecture + SIMD JSON.** New function-split libqueen engine cluster (3 shared engines — push/ack, pop, rest — decoupled from `NUM_WORKERS`, sized by per-function connection slots). Per-partition push serialization (in-memory in-flight gate + `pg_advisory_xact_lock` + `clock_timestamp()`) makes `messages.created_at` **commit-ordered**, eliminating the cursor-skip where a message could commit behind an already-advanced pop cursor under high concurrent push. Data-path concurrency (push/pop/ack) is now **static** (Vegas retained for auxiliary lanes), and `partition_lookup` maintenance is coalesced Nagle-style. Hot-path JSON is parsed and assembled with **simdjson** (nlohmann/json fallback), cutting broker CPU on push result fan-out. Balanced ~110–120k msg/s push & pop concurrently, ~190k push-only, on a 32-core host. Validated by a 24-hour soak: **10.4 billion messages, ~119k msg/s balanced, zero loss, flat ~400 MB broker**. [See soak →](https://queenmq.com/benchmarks-0.16-soak.html) | All ≥0.14.0 clients work unchanged — HTTP contract is identical; 0.16.0 SDKs are a version-aligned release |
| **0.15.5**     | Resolves **#30** (proxy compatible with Traefik forward-auth middleware), **#31** (write-only access role), **#32** (hardened Node base image). Robustness: malformed payloads can no longer crash a worker. Invalid UTF‑8 bytes and unpaired UTF‑16 surrogates — in a request body or in DB‑returned data — are serialized leniently and rejected with a clean **400** instead of throwing out of the event loop. Previously‑unguarded admin/metrics routes wrapped in error handling. | All ≥0.14.0 clients work unchanged |
| **0.15.0**     | Cross-language streaming SDK: fluent `Stream` builder + `.gate()` rate limiter + tumbling/sliding/session/cron windows + event-time + watermarks shipping in `queen-mq` (JS), `queen-mq` (Python), and `client-go` — all backed by the same `/streams/v1/*` endpoints and three new stored procedures (`streams_register_query_v1`, `streams_cycle_v1`, `streams_state_get_v1`). Identical SHA-256 `config_hash` across runtimes so a query registered by one client can be resumed by a worker written in another. UUIDv7 stamping in `streams_cycle_v1` push items to preserve FIFO order in batched sink emits. | All ≥0.14.0 clients work unchanged — upgrade clients to 0.15.0 to use the streaming SDK |
| **0.14.3**     | Improved frontend. | All ≥0.14.0 clients work unchanged |
| **0.14.1**     | Updated frontend: new metrics views and embedded developer guide; Google OAuth on the proxy; Prometheus metrics route (`/metrics`); significantly optimized lease renewal (reduced lock contention and DB round-trips); delete partition and delete messages API. | All ≥0.14.0 clients work unchanged |
| **0.14.0**     | Major release: new dynamic libqueen loop; rewritten `push_messages_v3`, `pop_unified_batch_v4`, `ack_messages_v2`, and `stats` stored procedures; `maxPartitions` on all clients (JS, Python, Go, Laravel, C++); new frontend. Benchmarked on real hardware: **104k msg/s** push (batch=100), **165k msg/s** fan-out across 10 consumer groups, pop throughput **+80–90%** vs 0.12 under partition contention, 52 MB server RSS at peak, zero message loss across 1.6B events. [See benchmarks →](https://queenmq.com/benchmarks.html#version) | All ≥0.13.x clients work unchanged — upgrade clients to gain `maxPartitions` support |
| **0.13.0**     | Major release: new libqueen with adaptive batch/concurrency/scheduling engine (S1 ~2x, S3 ~3x push throughput), new `push_messages_v2` stored procedure (temp-table + batched-insert pipeline), new Vue 3 dashboard, and server-stamped `producerSub` from the JWT on every message (closes #23) | All ≥0.12.x work unchanged — 0.13.0 pop responses add a new `producerSub` field that older clients silently ignore. Upgrade to 0.13.0 clients only if you want typed access to `producerSub` (Go struct field, Python TypedDict hint) |
| **0.12.19**    | Fix bug that on seek or cg delete do not deleted the watermark                                                                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.18**    | Improved charts and filters                                                                                                     | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.17**    | Improved stats                                                                                                                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.13**    | Added watermark tracking for efficient wildcard POP discovery. x20 faster pop on high partition count queues                    | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.12**    | Built-in database migration (pg_dump \| pg_restore, no temp file, selective table groups, row count validation)                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.10**    | Fixed JWKS fetch over HTTPS (cpp-httplib TLS support)                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.9**     | Fixed server crash (SIGSEGV) on lease renewal, added EdDSA/JWKS auth, fixed examples                                            | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.8**     | Added single partition move to now to frontend                                                                                  | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.7**     | Optimized cg metadata creation for new consumer groups                                                                          | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.6**     | Improved slow cg discovery when there are tons of partitions                                                                    | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.5**     | Fixed cg lag calculation for "new" cg at first message                                                                          | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.4**     | Fixed window buffer debounce behavior                                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.12.3**     | Added JWT authentication                                                                                                        | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.12.x**     | New frontend and docs                                                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.11.x**     | Libqueen 0.11.0; added stats tables and optimized analytics procedures, added DB statement timeout and stats reconcile interval | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.10.x**     | Total rewrite of the engine with libuv and stored procedures, removed streaming engine                                          | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.8.0**      | Added Shared Cache with UDP sync for clustered deployment                                                                       | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.7.5**      | First stable release                                                                                                            | JS ≥0.7.4, Python ≥0.7.4                                    |

**[Full Release Notes →](https://github.com/queen-mq/queen/releases)**

---

## Latest bug fixing and improvements

- Server 0.16.0: **Push-serialization architecture (commit-ordered `created_at`).** Under high concurrent push to the same partitions, `created_at` (transaction-*start* time) could be assigned out of commit order, letting the wildcard pop cursor `(created_at, id)` advance past a not-yet-committed message — silent loss. The push path now serializes **per partition** (an in-memory "≤1 in-flight push transaction per partition" gate plus a Postgres `pg_advisory_xact_lock` in a dedicated two-int keyspace) and stamps `created_at = clock_timestamp()` under the lock, so per-partition `created_at` is monotonic in commit order. POP and ACK are unchanged. Deterministic repro + proof in `benchmark-queen/2026-06-06-engine-scaling/cursor-repro.sh`.
- Server 0.16.0: **Function-split engine cluster.** libqueen now runs as 3 process-global engines (push/ack, pop, rest) shared by all HTTP workers via `QueenCluster`, replacing the previous one-engine-per-worker model. `NUM_WORKERS` sizes only HTTP I/O; DB concurrency is sized by per-function slots (`QUEEN_PUSH_SLOTS` / `QUEEN_POP_SLOTS` / `QUEEN_REST_SLOTS`). The push engine forms disjoint-partition batches that run concurrently (`QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH`).
- Server 0.16.0: **SIMD JSON on the hot path.** The engine parses Postgres stored-procedure results and assembles batch payloads with **simdjson** (on-demand + DOM parsers), keeping the previous nlohmann/json path as a fallback. Lower CPU and tail latency on push/ack/pop result demultiplexing at 100k+ msg/s.
- Server 0.16.0: **Static data-path concurrency.** push/pop/ack default to static limits (24/16/16). Vegas (RTT-adaptive) mis-reads the hot path — it under-shoots push (high per-commit RTT) and collapses pop (long-poll parking read as PG queuing) — so it is retained only for the auxiliary lanes. Override per lane with `QUEEN_<TYPE>_CONCURRENCY_MODE`.
- Server 0.16.0: **`partition_lookup` coalescing.** Post-push lookup maintenance is batched Nagle-style (at most one flush in flight) instead of one call per push batch, clearing a backlog that appeared under sustained high push.
- Docs 0.16.0: **Architecture docs refreshed.** The developer guide and website now describe the new engine topology, push serialization, and concurrency model (`developer/02-architecture.md`, `04-libqueen.md`, `05-database-schema.md`, `docs/architecture.html`, `server/README.md`, `server/ENV_VARIABLES.md`).
- Proxy 0.15.5: **Traefik forward-auth compatibility (#30).** The Queen proxy can now be used as a Traefik external/forward-auth middleware, not only behind its own login flow.
- Server 0.15.5: **Write-only role (#31).** A new write-only access level lets producers push without being able to read or consume.
- Build 0.15.5: **Hardened Node base image (#32).** The proxy and dashboard images now build on a hardened, minimal Node base.
- Server 0.15.5: **Malformed‑payload hardening.** A request body — or DB‑returned content — containing invalid UTF‑8 or an unpaired UTF‑16 surrogate no longer throws out of the worker event loop. JSON is now serialized with a lenient error handler on every HTTP response and on the libqueen result/callback path, so such input is rejected with a clean **400** instead of crashing the worker (previously a `json.exception.type_error.316` could take a worker down and loop on retry).
- Server 0.15.5: **Defensive error handling on admin/metrics routes.** The shared‑state, partition‑seek, migration‑reset, and `/metrics/prometheus` handlers (including the deferred callback that runs on the event loop) are wrapped in try/catch, so an unexpected exception returns a 500 instead of killing a worker.
- Clients 0.15.0: **Streaming SDK on every runtime.** Ships a fluent `Stream` builder + composable operators (`.map`, `.filter`, `.flat_map`, `.key_by`, `.window_tumbling`, `.window_sliding`, `.window_session`, `.window_cron`, `.reduce`, `.aggregate`, `.gate`, `.to`, `.foreach`) and helper factories (`token_bucket_gate`, `sliding_window_gate`) in JS, Python, and Go. All three packages export the SDK from the same package as the broker client (`queen-mq` on npm/PyPI; `client-go/streams` subpackage in Go) — one install, one import.
- Clients 0.15.0: **Exactly-once cycles via `/streams/v1/cycle`.** State mutations + sink emissions + source acks commit in a single PostgreSQL transaction. On commit failure the entire cycle rolls back; Queen redelivers via the existing lease/retry path.
- Clients 0.15.0: **`.gate()` rate limiter with FIFO preservation.** New per-message ALLOW/DENY operator with persistent per-key state, a partial-ack on deny, and `release_lease=false` so the un-acked tail of the batch is redelivered in original order when the lease expires — no deferred queue, no reordering. The `tokenBucketGate` and `slidingWindowGate` helpers cover all four canonical rate-limit shapes (req/s, msg/s, cost-weighted, sliding-window quota) on every language.
- Clients 0.15.0: **Tumbling, sliding, session, and cron windows.** Per-window `gracePeriod`, `idleFlushMs`, optional `eventTime` extractor with per-partition watermarks (stored under the reserved `__wm__` state key), `allowedLateness`, and `onLate: 'drop' \| 'include'`. The runner emits closed windows on idle partitions via a per-window flush timer.
- Server 0.15.0: **Three new streaming stored procedures.** `streams_register_query_v1`, `streams_cycle_v1`, and `streams_state_get_v1` route through libqueen's existing async pipeline — same uvloop, same connection pool, same metrics attribution. Streaming cycles increment `record_ack_request` / `record_ack_messages` / `record_push_messages_with_queue` so the dashboard's per-queue Ack/s and Push/s charts include streaming throughput.
- Server 0.15.0: **UUIDv7 message IDs in streaming push.** `/streams/v1/cycle` stamps every sink push item with a UUIDv7 server-side, matching the `/api/v1/push` route. Time-ordered IDs preserve partition FIFO order even when batched inserts share a `created_at` timestamp.
- Tests 0.15.0: **75 Python streams tests, 33 Go subtests, 45 JS unit tests pass live.** Plus 11 examples per language ported 1:1 from the JS reference, including a "rate-limiter all canonical models" stress test (100 tenants × 10k messages, 4 runners, ~360 msg/sec aggregate sustained).
- Docs 0.15.0: Added [`use-cases.html`](https://queenmq.com/use-cases.html) landing page and [`use-case-rate-limiter.html`](https://queenmq.com/use-case-rate-limiter.html) with verified end-to-end snippets in JS, Python, and Go.
- Server/App 0.14.3: **Improved frontend.** Further refinements to the dashboard UI and user experience.
- Server/App 0.14.1: **Updated frontend.** New metrics views and an embedded developer guide surfaced directly in the dashboard.
- Proxy 0.14.1: **Google OAuth support.** The proxy now supports Google as an OAuth provider for end-to-end authentication without a custom identity server.
- Server 0.14.1: **Prometheus metrics route.** A `/metrics` endpoint exposes standard Prometheus-compatible metrics for scraping.
- Server 0.14.1: **Significantly optimized lease renewal.** Reduced lock contention and database round-trips on the hot lease-renewal path, lowering tail latency under high consumer concurrency.
- Server/App 0.14.1: **Delete partition and delete messages.** New API and dashboard actions to delete individual partitions or bulk-delete messages from a queue.
- Server and clients 0.14.0: **New dynamic libqueen loop.** Full rewrite of the core scheduling engine — adaptive concurrency controller (TCP-Vegas-style) now drives push, pop, ack, and stats independently. Active DB connections stay at ~2.5 even with a pool of 50 under 104k msg/s peak load. Largely eliminates the PG deadlock mode that appeared under heavy fan-out at high partition counts on 0.12 (occasional deadlocks still observed at 10 001 partitions, all absorbed by file-buffer failover — see [benchmarks](https://queenmq.com/benchmarks.html)).
- Server 0.14.0: **Rewritten stored procedures.** `push_messages_v3`, `pop_unified_batch_v4`, `ack_messages_v2`, and stats procedures redesigned around the new loop. PG memory usage 30–70% lower for equivalent workloads vs 0.12. Pop throughput +80–90% under partition contention.
- Clients 0.14.0: **`maxPartitions` on all clients.** JS, Python, Go, Laravel, and C++ clients expose `maxPartitions` on queue creation and configuration.
- Server 0.14.0: **New frontend.** Redesigned dashboard for the new stats model.
- Server 0.13.0: **New libqueen with adaptive engine.** Per-worker push/ack drain factored into three independently-tuned concerns — batching, concurrency, scheduling — glued by an event-driven orchestrator. Fixes two long-standing bottlenecks: per-commit overhead amortization on small-batch workloads, and the single-slot-per-drain cap on high-fanout workloads. Perf harness numbers: S1 ~6.2k → ~13k pg_ins/s, S3 ~4.7k → ~20k pg_ins/s, PG pinned instead of idle. Design notes in `cdocs/LIBQUEEN_IMPROVEMENTS.md`.
- Server 0.13.0: **New push stored procedure.** `queen.push_messages_v2` rewritten around a temp-table + batched-insert pipeline that feeds cleanly into the adaptive engine. HTTP contract (queued/duplicate/failed) unchanged.
- Server 0.13.0: **New Vue 3 dashboard.** Reworked queues, analytics, DLQ management, and maintenance-mode views. Served by the same C++ acceptor at `/`.
- Server 0.13.0: Added server-stamped `producerSub` to close the impersonation vector from GitHub issue #23. When JWT auth is enabled the server stamps the validated `sub` claim on every pushed message; clients cannot set this field and it is exposed on pop responses and admin message APIs. Schema migration is additive and metadata-only (no table rewrite), safe on tables with millions of rows.
- Clients 0.13.0: All clients (JS, Python, Go, Laravel, C++) expose `producerSub` on popped messages; Go adds a typed `Message.ProducerSub` field.
- Server 0.12.19: Fix bug where seek or cg delete did not delete the watermark.
- Server 0.12.13: Added watermark tracking for efficient wildcard POP discovery — x20 faster pop on high partition count queues.
- Server 0.12.12: Added built-in database migration — stream pg_dump | pg_restore directly from the dashboard, no temp file, selective table groups, row count validation.
- Clients 0.12.2: Added custom `headers` option to JS, Python, and Go clients for API gateway authentication.
- Server 0.12.9: Fixed server crash (SIGSEGV) on lease renewal; added native EdDSA and JWKS JWT authentication (auto-discovery via `JWT_JWKS_URL`).
- Server 0.12.3: Added JWT authentication.
