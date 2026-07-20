# Developing Queen

[![libuv](https://img.shields.io/badge/libuv-1.48.0-blue.svg)](https://libuv.org/)
[![libpq](https://img.shields.io/badge/libpq-15.5-blue.svg)](https://www.postgresql.org/)
[![uWebSockets](https://img.shields.io/badge/uWebSockets-22.0.0-blue.svg)](https://github.com/uNetworking/uWebSockets)

This is the developer guide for the **Queen MQ** repository. It tells you how to build, run, test, and reason about every piece of the system: the C++broker, the libqueen core library, the SQL schema and stored procedures, the Vue dashboard, the auth proxy, and all five client SDKs (JavaScript, Python, Go, PHP/Laravel, C++).

If you only want to **use** Queen, see [README.md](README.md) and [docs/](docs/index.html). This guide is for people who want to **change** Queen — internally at Smartness, or as an external contributor.

---

## Repository map

```
queen/
├── server/                C++ broker (uWebSockets + libuv + libpq async)
│   ├── src/               acceptor + workers, routes, services, managers
│   ├── include/           server-private headers + PCH
│   ├── vendor/            downloaded deps (uWebSockets, uSockets, libuv, …)
│   ├── Makefile           build system
│   └── README.md          operator-focused build & tuning guide
├── lib/                   libqueen — header-only C++ core used by the broker
│   ├── queen.hpp          the entry header (per-engine drain loop)
│   ├── queen_cluster.hpp  QueenCluster — 3 function-split engines (push/pop/rest)
│   ├── queen/             drain orchestrator, slot pool, per-type queue, …
│   ├── schema/            ⚠ database schema + stored procedures
│   │   ├── schema.sql     base schema (tables + indexes)
│   │   └── procedures/    ~18 SQL files loaded on startup (push, pop, ack, …)
│   ├── test_suite.cpp     end-to-end PG-backed tests
│   ├── test_contention.cpp PUSH/POP contention benchmark
│   ├── queen_test.cpp     unit tests (no PG needed)
│   └── Makefile           libqueen build & test targets
├── app/                   Vue 3 + Vite dashboard (served by the broker)
├── proxy/                 Node.js auth proxy (JWT, Google OAuth, RBAC)
├── clients/
│   ├── client-js/         queen-mq on npm (Node 22+)
│   ├── client-py/         queen-mq on PyPI (Python 3.8+)
│   ├── client-go/         github.com/smartpricing/queen/clients/client-go
│   ├── client-laravel/    smartpricing/queen-mq on Packagist (PHP 8.3+)
│   └── client-cpp/        header-only, ships with the repo
├── benchmark-queen/       end-to-end benchmark harness
├── examples/              ~30 runnable Node examples
├── helm/, helm_queen/     Helm charts for k8s deployment
├── docs/                  user-facing static documentation site
├── developer/             you are here — deep-dive developer docs
└── Dockerfile             multi-stage build (frontend + broker)
```

---

## 5-minute developer loop

Quickest possible "I changed something — does it still work?" loop:

```bash
# 1. Postgres in Docker (one time)
docker network create queen 2>/dev/null || true
docker run --name qpg --network queen \
  -e POSTGRES_PASSWORD=postgres -p 5433:5432 -d postgres

# 2. Build the broker
cd server
make all                # downloads deps + compiles, ~3 min cold, ~10 s warm

# 3. Run the broker (it bootstraps the schema on first start)
PG_HOST=localhost PG_PORT=5433 PG_PASSWORD=postgres \
  ./bin/queen-server

# 4. In another terminal: smoke-test
curl http://localhost:6632/health
curl -X POST http://localhost:6632/api/v1/push \
  -H 'Content-Type: application/json' \
  -d '{"items":[{"queue":"demo","payload":{"hi":"there"}}]}'
curl 'http://localhost:6632/api/v1/pop?queue=demo&batch=1&wait=false'
```

That's the whole loop. Everything else in this guide expands on one of these steps.

---

## Table of contents


| #   | Page                                                     | What it covers                                                         |
| --- | -------------------------------------------------------- | ---------------------------------------------------------------------- |
| 01  | [Getting started](developer/01-getting-started.md)       | Prerequisites, Postgres setup, run the broker locally                  |
| 02  | [Architecture](developer/02-architecture.md)             | Acceptor/workers, libqueen, services, request flow                     |
| 03  | [Building the server](developer/03-build-server.md)      | Detailed build, vendor deps, debug builds, troubleshooting             |
| 04  | [libqueen](developer/04-libqueen.md)                     | Drain orchestrator, slot pool, per-type queue, concurrency controllers |
| 05  | [Database schema](developer/05-database-schema.md)       | Tables, indexes, **⚠ stored procedures: do not touch unless…**         |
| 06  | [Client SDKs](developer/06-clients.md)                   | Per-language setup, including Python `venv`                            |
| 07  | [Testing](developer/07-testing.md)                       | libqueen unit + integration tests, client tests, full stack            |
| 08  | [Partitions](developer/08-partitions.md)                 | What a partition is, how many to use, cardinality guidance             |
| 09  | [Retention](developer/09-retention.md)                   | The 4 cleanup jobs, advisory lock, configuration                       |
| 10  | [Frontend dashboard](developer/10-frontend-dashboard.md) | Vue 3 app — dev server, build, API proxy                               |
| 11  | [Auth proxy](developer/11-proxy.md)                      | JWT, Google OAuth, role-based access control                           |
| 12  | [Failover](developer/12-failover.md)                     | File-buffer, zero-message-loss when Postgres is down                   |
| 13  | [Consumer groups](developer/13-consumer-groups.md)       | Subscription modes, fan-out semantics                                  |
| 14  | [Release process](developer/14-release.md)               | Versioning, Docker image, PyPI, npm, Composer, Go module               |


---

## Conventions

- **Language:** all source-of-truth docs are in English. Internal Italian summaries are fine in PR descriptions but not in committed docs.
- **C++:** `-std=c++17`, no Boost. Header-only deps live in `server/vendor/`.
- **Node.js:** `>=22`. Always use `nvm use 22 &&` before `npm` commands so CI and dev match.
- **Python:** `>=3.8`. Prefer a per-client `venv` over a global install.
- **Commits & PRs:** one logical change per PR; mention the affected component in the title (`server:`, `client-py:`, `schema:`, …).
- **Stored procedures:** see [⚠ the warning in chapter 05](developer/05-database-schema.md#warning-stored-procedures-are-load-bearing). Don't change them unless you've read the reasoning.

---

## Segments engine: operational notes

### Segment dedup — hash-collision risk (accepted, documented)

The segments engine dedups pushes on a **64-bit hash** of the transactionId, not the
raw string: `queen.seg_dedup` is keyed `PRIMARY KEY (partition_id, txn_hash)` with
`txn_hash BIGINT = hashtextextended(transactionId, 0)` (`023_storage_v2.sql`). This
is a deliberate space/CPU optimization over the rows engine, which used an
exact-string unique constraint on `(partition_id, transaction_id)` (zero collision
risk by construction).

**Consequence of a collision.** Two *distinct* transactionIds that hash to the same
64-bit value **in the same partition, within the live dedup window** collide: on
push the second is counted as a duplicate and its frame is silently dropped; more
rarely, an ack-by-transactionId (`seg_ack_by_txn_v1`, which resolves positions
through the same `txn_hash`) could resolve to the colliding position. Dedup is a
bounded **window** (`dedup_window_seconds`, default 3600), not permanent.

**Birthday-bound math.** With `N` live dedup entries in one partition-window, the
collision probability is ≈ `N² / 2⁶⁵`, where `N = (push rate to that partition) ×
dedup_window_seconds`:

| N (live entries / partition-window) | P(collision) per partition-window |
| ----------------------------------- | --------------------------------- |
| 10 000                              | ~5 × 10⁻¹²                        |
| 1 000 000                           | ~5 × 10⁻⁸                         |

Aggregated across many partitions and windows the total scales roughly linearly,
and stays negligible at realistic rates (e.g. 1 × 10⁶ partition-windows/day at
N = 10 000 → ~5 × 10⁻⁶/day).

**Decision: accept and document.** The probability is astronomically small at
realistic N, whereas the alternative (revert to an exact-string unique constraint —
add a raw `txn TEXT` column, re-key uniqueness on `(partition_id, txn)`, and update
every resolver that joins on `txn_hash`: push probe, `seg_find_dups_v1`, the
ack-by-txn durable + legacy walks, the multi-push dup path) is a high-blast-radius
uniqueness-model change that forfeits the fixed-width-key advantage. The real tuning
lever is the **dedup window**: a shorter `dedup_window_seconds` lowers `N` (and thus
collision odds) but also shortens how long an ack-by-transactionId remains
resolvable — balance the two.

### Mixed C++ / Rust clusters are out of scope for a rolling upgrade

The C++ (v0.16.0) and Rust brokers use **different UDP payload codecs** for
inter-replica sync (C++ msgpack vs Rust JSON; the framing and HMAC are identical,
but the bodies are not interchangeable — see `server/src/udp.rs`). Therefore a
**mixed C++/Rust cluster does not exchange cross-replica sync**: MESSAGE_AVAILABLE
wakeups, maintenance-mode flips, and queue-config cache invalidations do **not**
cross the codec boundary, and C++ nodes will report Rust peers (and vice versa) as
**dead** in their peer tables.

This does **not** block a rolling `C++ → Rust` migration, because every replica
still converges through the shared Postgres source of truth:

- Maintenance flags heal within `QUEEN_CACHE_REFRESH_INTERVAL_MS` (default 60 s) via
  the DB reconcile loop (`server/src/reconcile.rs`), independent of UDP.
- Queue-config / lease-cache staleness heals on the same reconcile cadence.
- Cross-replica consume still works — a consumer on any replica reads committed
  segments from the DB; it just isn't *woken* early by a push landing on a
  different-codec peer (it falls back to its long-poll re-query interval).

Recommended upgrade path: drain/upgrade replicas one at a time; expect transient
"peer dead" log noise and slightly higher long-poll latency (no sub-millisecond
cross-replica wakeups) during the window when both codecs are present. Do **not**
rely on cross-replica UDP sync while the cluster is mixed.

---

## Where to ask

- Issues or external contributions: [github.com/queen-mq/queen](https://github.com/queen-mq/queen)
- Internal engineering memos (deep-dive analyses of design choices): [cdocs/](cdocs/)
- User-facing reference: [docs/](docs/index.html)

