# Developing Queen

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
│   ├── queen.hpp          the entry header
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
├── pg_qpubsub/            experimental Postgres extension (separate project)
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

## Where to ask

- Issues or external contributions: [github.com/smartpricing/queen](https://github.com/smartpricing/queen)
- Internal engineering memos (deep-dive analyses of design choices): [cdocs/](cdocs/)
- User-facing reference: [docs/](docs/index.html)

