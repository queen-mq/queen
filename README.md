# Queen MQ - Partitioned Message Queue backed by PostgreSQL

<div align="center">

<img src="assets/queen-partitions.svg" alt="Queen MQ pipeline: producers push to one queue split into ordered partitions per agent session; two consumer groups (agent runner, tracer) each see every message; one slow tool call stalls only its own partition." width="780" />

**Unlimited ordered partitions that never block each other. Consumer groups, replay, transactional delivery — ACID-guaranteed.**


[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
[![Node](https://img.shields.io/badge/node-%3E%3D22.0.0-brightgreen.svg)](https://nodejs.org/)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Go](https://img.shields.io/badge/go-1.24%2B-00ADD8.svg)](https://go.dev/)
[![PHP](https://img.shields.io/badge/php-8.3%2B-777BB4.svg)](https://www.php.net/)
[![C++](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://en.cppreference.com/w/cpp/17)
[![libuv](https://img.shields.io/badge/libuv-1.48.0-blue.svg)](https://libuv.org/)
[![libpq](https://img.shields.io/badge/libpq-15.5-blue.svg)](https://www.postgresql.org/)
[![uWebSockets](https://img.shields.io/badge/uWebSockets-22.0.0-blue.svg)](https://github.com/uNetworking/uWebSockets)

📚 **[Complete Documentation](https://queenmq.com/)** • 🚀 **[Quick Start](https://queenmq.com/quickstart.html)** • ⚖️ **[Comparison & Benchmarks](https://queenmq.com/benchmarks.html)** • 🛠️ **[Contributing — Developer Guide](DEVELOPING.md)**

<p align="center">
  <img src="assets/queen_logo.png" alt="Queen Logo" width="120" />
</p>

</div>

> **Version 0.15.0** — Cross-language streaming SDK on JS, Python, and Go. Built on the same 0.14.x engine — see table below for full history.

---

Queen MQ is a partitioned message queue backed by PostgreSQL, built with uWebSockets, libuv, and libpq async API. It features unlimited FIFO partitions that process independently, consumer groups with replay, transactional delivery, tracing, and ACID-guaranteed durability all in a single stateless binary alongside the Postgres you already run. Version 0.14.0 sustains **104k msg/s** push and **165k msg/s** fan-out on a 32-core host, with a broker RSS under 52 MB.

See [examples/base.js](examples/base.js) for a complete (push, consume, transactionally ack and push to another queue) example. An experimental PostgreSQL extension version is also available at [pg_qpubsub](pg_qpubsub/README.md).

## Why Queen?

Born at [Smartness](https://www.linkedin.com/company/smartness-com/) to power **Smartchat**, Queen solves a unique problem: **unlimited FIFO partitions** where slow processing in one partition doesn't block others.

Perfect for:

- **One ordered lane per entity, no preallocation** — 10,000 partitions cost index rows, not 10,000 commit-log files. A partition is created on first push; a slow consumer on one partition never stalls another.
- **Transactional integration with PostgreSQL** — ACK and push in a single PG transaction.
- **Fan-out with fairness** — consumer groups each get a full copy of every message; the adaptive engine keeps delivery fair across groups at sub-linear CPU cost.
- **52 MB broker at 104k msg/s** — no JVM, no Erlang, no cluster to operate. One Docker container plus your existing Postgres.
- **Replay and DLQ** — rewind any consumer group to any timestamp; failed messages surface in a per-queue dead-letter queue automatically.
- **Zero message loss, verified** — 1.6 billion events across the benchmark suite, zero lost, zero duplicates.

## Quick Start

Create a Docker network and start PostgreSQL and Queen Server:

```bash
# Create a Docker network for Queen components
docker network create queen

# Start PostgreSQL
docker run --name qpg --network queen -e POSTGRES_PASSWORD=postgres -p 5433:5432 -d postgres

# Wait for PostgreSQL to start
sleep 2

# Start Queen Server
docker run -p 6632:6632 --network queen -e PG_HOST=qpg -e PG_PORT=5432 -e PG_PASSWORD=postgres -e NUM_WORKERS=2 -e DB_POOL_SIZE=5 -e SIDECAR_POOL_SIZE=30 smartnessai/queen-mq:0.15.0
```

Then in another terminal, use cURL (or the client libraries) to push and consume messages

**Push message:**

```bash
curl -X POST http://localhost:6632/api/v1/push \
  -H "Content-Type: application/json" \
  -d '{"items": [{"queue": "demo", "payload": {"hello": "world"}}]}'
```

that returns something like:

```json
[{"message_id": "...", "status": "queued", ...}]
```

**Consume message:**

```bash
curl "http://localhost:6632/api/v1/pop/queue/demo?autoAck=true"
```

that returns something like:

```json
{"messages": [{"data": {"hello": "world"}, ...}], "success": true}
```

Then go to the dashboard ([http://localhost:6632](http://localhost:6632)) to see the messages and the status of the queue.

## Documentation

📚 **[Complete Documentation](https://queenmq.com/)**

### Getting Started

- [Quick Start Guide](https://queenmq.com/quickstart.html)
- [Basic Concepts](https://queenmq.com/concepts.html)
- [Architecture](https://queenmq.com/architecture.html)
- [Benchmarks](https://queenmq.com/benchmarks.html) · [Sizing calculator](https://queenmq.com/sizing.html)

### Client Libraries & API

- [Client libraries overview](https://queenmq.com/clients.html) — JavaScript, Python, Go, PHP / Laravel, C++ (same fluent grammar across all five)
- [HTTP API Reference](https://queenmq.com/http-api.html)
- [`queenctl` CLI](https://queenmq.com/cli.html) — single-binary operator CLI built on `client-go`

### Operate

- [Server setup](https://queenmq.com/server.html) — env vars, Docker, Kubernetes, multi-instance UDP sync, JWT auth, proxy
- [Dashboard tour](https://queenmq.com/dashboard.html)

---

## Structure of the repository

The repository is structured as follows:

- `lib`: C++ core queen library (libqueen), implementing libuv loops, sql schema and procedures
- `server`: Queen MQ server, implementing the HTTP API that talks to the libqueen library
- `pg_qpubsub`: PostgreSQL extension for using queen-mq semantics as a PostgreSQL extension
- `clients/client-js`: JavaScript client library (browser and node.js)
- `clients/client-py`: Python client library (python 3.8+)
- `clients/client-go`: Go client library (go 1.24+)
- `clients/client-laravel`: PHP / Laravel client library (php 8.3+)
- `clients/client-cpp`: C++ client library (cpp 17)
- `clients/client-cli`: `queenctl` operator CLI (Go binary built on `client-go`)
- `proxy`: Proxy server (authentication)
- `app`: Vue.js dashboard (vue 3)
- `docs`: Documentation website (vitepress)
- `examples`: JS client examples
- `streams`: JS client streaming examples

---

## Release History

**JS clients from version 0.12.0 can be run inside a browser**


| Server Version | Description                                                                                                                     | Compatible Clients                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| **0.15.0**     | Cross-language streaming SDK: fluent `Stream` builder + `.gate()` rate limiter + tumbling/sliding/session/cron windows + event-time + watermarks shipping in `queen-mq` (JS), `queen-mq` (Python), and `client-go` — all backed by the same `/streams/v1/*` endpoints and three new stored procedures (`streams_register_query_v1`, `streams_cycle_v1`, `streams_state_get_v1`). Identical SHA-256 `config_hash` across runtimes so a query registered by one client can be resumed by a worker written in another. UUIDv7 stamping in `streams_cycle_v1` push items to preserve FIFO order in batched sink emits. | All ≥0.14.0 clients work unchanged — upgrade clients to 0.15.0 to use the streaming SDK |
| **0.14.3**     | Improved frontend. | All ≥0.14.0 clients work unchanged |
| **0.14.1**     | Updated frontend: new metrics views and embedded developer guide; Google OAuth on the proxy; Prometheus metrics route (`/metrics`); significantly optimized lease renewal (reduced lock contention and DB round-trips); delete partition and delete messages API. | All ≥0.14.0 clients work unchanged |
| **0.14.0**     | Major release: new dynamic libqueen loop; rewritten `push_messages_v3`, `pop_unified_batch_v4`, `ack_messages_v2`, and `stats` stored procedures; `maxPartitions` on all clients (JS, Python, Go, Laravel, C++); new frontend. Benchmarked on real hardware: **104k msg/s** push (batch=100), **165k msg/s** fan-out across 10 consumer groups, pop throughput **+80–90%** vs 0.12 under partition contention, 52 MB server RSS at peak, zero message loss across 1.6B events. [See benchmarks →](docs/benchmarks.html#version) | All ≥0.13.x clients work unchanged — upgrade clients to gain `maxPartitions` support |
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

- Clients 0.15.0: **Streaming SDK on every runtime.** Ships a fluent `Stream` builder + composable operators (`.map`, `.filter`, `.flat_map`, `.key_by`, `.window_tumbling`, `.window_sliding`, `.window_session`, `.window_cron`, `.reduce`, `.aggregate`, `.gate`, `.to`, `.foreach`) and helper factories (`token_bucket_gate`, `sliding_window_gate`) in JS, Python, and Go. All three packages export the SDK from the same package as the broker client (`queen-mq` on npm/PyPI; `client-go/streams` subpackage in Go) — one install, one import.
- Clients 0.15.0: **Exactly-once cycles via `/streams/v1/cycle`.** State mutations + sink emissions + source acks commit in a single PostgreSQL transaction. On commit failure the entire cycle rolls back; Queen redelivers via the existing lease/retry path.
- Clients 0.15.0: **`.gate()` rate limiter with FIFO preservation.** New per-message ALLOW/DENY operator with persistent per-key state, a partial-ack on deny, and `release_lease=false` so the un-acked tail of the batch is redelivered in original order when the lease expires — no deferred queue, no reordering. The `tokenBucketGate` and `slidingWindowGate` helpers cover all four canonical rate-limit shapes (req/s, msg/s, cost-weighted, sliding-window quota) on every language.
- Clients 0.15.0: **Tumbling, sliding, session, and cron windows.** Per-window `gracePeriod`, `idleFlushMs`, optional `eventTime` extractor with per-partition watermarks (stored under the reserved `__wm__` state key), `allowedLateness`, and `onLate: 'drop' \| 'include'`. The runner emits closed windows on idle partitions via a per-window flush timer.
- Server 0.15.0: **Three new streaming stored procedures.** `streams_register_query_v1`, `streams_cycle_v1`, and `streams_state_get_v1` route through libqueen's existing async pipeline — same uvloop, same connection pool, same metrics attribution. Streaming cycles increment `record_ack_request` / `record_ack_messages` / `record_push_messages_with_queue` so the dashboard's per-queue Ack/s and Push/s charts include streaming throughput.
- Server 0.15.0: **UUIDv7 message IDs in streaming push.** `/streams/v1/cycle` stamps every sink push item with a UUIDv7 server-side, matching the `/api/v1/push` route. Time-ordered IDs preserve partition FIFO order even when batched inserts share a `created_at` timestamp.
- Tests 0.15.0: **75 Python streams tests, 33 Go subtests, 45 JS unit tests pass live.** Plus 11 examples per language ported 1:1 from the JS reference, including a "rate-limiter all canonical models" stress test (100 tenants × 10k messages, 4 runners, ~360 msg/sec aggregate sustained).
- Docs 0.15.0: Added [`use-cases.html`](docs/use-cases.html) landing page and [`use-case-rate-limiter.html`](docs/use-case-rate-limiter.html) with verified end-to-end snippets in JS, Python, and Go.
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

---

## License

Queen MQ is released under the [Apache 2.0 License](LICENSE.md).

---

**Built with ❤️ by [Smartness](https://www.linkedin.com/company/smartness-com/)**

*Why "Queen"? Because years ago, when I first read "queue", I read it as "queen" in my mind.*

