<div align="center">
  <img src="assets/queen-logo-black.png" alt="Queen MQ" width="120">
</div>

# Queen MQ

**The queue that doesn't fall apart at the other end of your workload.**

Every entity gets its own FIFO lane, created on first push, so a slow consumer on one never
stalls another. That design has two ends, and one broker holds both.

**A million messages a second for 24 hours**: 86,369,975,300 messages with leases, explicit acks,
deduplication and retention all on, zero restarts, broker memory flat at 4.1 GB. 

And **a million ordered partitions** in one PostgreSQL, none preallocated, created during the run at a thousand a
second while serving 200,000 messages a second with zero push, pop or ack errors.

Consumer groups, replay and a dead-letter queue at both ends. Windowed aggregation that commits
its state, its output and its acks in one transaction. One stateless binary on the
PostgreSQL you already run. No cluster, no JVM.

[Every number above, with the conditions that make it true →](https://queenmq.com/benchmarks/comparison)

<div align="center">

<img src="assets/queen-features.svg" alt="A queue split into one ordered lane per session, one of them stalled behind a slow consumer and holding up nobody, and that same lane magnified to single messages: four complete behind the committed cursor, three leased, three waiting, one arriving from a push. Three panels show consumer groups replaying from a segment edge, a lease resolving to a retry or to the dead-letter table, and window state, output and the acknowledgement of the source committing together. Underneath, three identical broker processes with nothing between them, over one PostgreSQL boundary holding the partition, segment, consumer and dead-letter tables." width="880" />

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
[![Rust](https://img.shields.io/badge/rust-2021-000000.svg)](https://www.rust-lang.org/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-15%2B-336791.svg)](https://www.postgresql.org/)
[![Node](https://img.shields.io/badge/node-%3E%3D22.0.0-brightgreen.svg)](https://nodejs.org/)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![Go](https://img.shields.io/badge/go-1.24%2B-00ADD8.svg)](https://go.dev/)
[![Rust client](https://img.shields.io/badge/rust%20client-1.86%2B-000000.svg)](clients/client-rust)
[![PHP](https://img.shields.io/badge/php-8.3%2B-777BB4.svg)](https://www.php.net/)

📚 **[Documentation](https://queenmq.com/)** · 🚀 **[Quickstart](https://queenmq.com/start/quickstart)** · 📊 **[Benchmarks](https://queenmq.com/benchmarks)** · 🛠 **[Develop](#developing-on-queen)**

</div>

Queen MQ is a message queue that keeps its data in PostgreSQL. A queue is split into
**partitions**, one per entity, created the first time you push to one. Each partition is a
strictly ordered lane that a consumer group drains independently, so ten thousand partitions
cost ten thousand index rows rather than ten thousand commit-log files or ten thousand
processes, and a consumer stuck on one lane never blocks another.

Everything else follows from that. The broker is one stateless Rust binary holding no cluster
membership and no partition assignments, so you scale it by starting another copy against the
same database. Clients speak plain HTTP and hold no coordination state either, so there is no
rebalancing protocol to wait out when a worker restarts. Durability, backup and replication are
whatever your PostgreSQL already does.

## What you get

- **One ordered lane per entity, no preallocation.** Partitions are logical and created on first push.
- **Consumer groups with replay.** Each group keeps its own cursor per partition, and can be moved back to a timestamp or forward to the end.
- **Acknowledgement as an offset commit.** No per-message delivery state to store, scan or clean up.
- **Transactional handoff.** Acking one message and pushing the next stage's happens in a single PostgreSQL transaction.
- **Windowed aggregation in the same transaction.** Tumbling, sliding, session and cron windows over a queue, with the window state, the emitted messages and the source acknowledgement committing together or not at all. No changelog topic, no state store, no second system.
- **Exact, windowed deduplication.** A `transactionId` you supply makes a push idempotent inside a configurable window, enforced in the database rather than a cache.
- **A dead-letter queue, tracing, and a dashboard**, all served by the same binary.
- **Six client SDKs, an operator CLI, and a plain HTTP API**, so anything that can make an HTTP request is a first-class client.
- **An embeddable engine.** The broker is a Rust library before it is a binary: a product can run the same engine inside its own process instead of shipping a second container. See [Embedding the engine](#embedding-the-engine).

> *Why "Queen"? Because years ago, when I first read "queue", I read it as "queen" in my mind. The name stuck.*

Born at [Smartness](https://www.linkedin.com/company/smartness-com/) to power **Smartchat**, where
one ordered lane per chat session was the requirement that nothing else satisfied cheaply.

## Quickstart

```bash
docker network create queen
```

```bash
docker run --name qpg --network queen -e POSTGRES_PASSWORD=postgres -p 5433:5432 -d postgres:16
```

```bash
docker run -p 6632:6632 --network queen -e PG_HOST=qpg -e PG_PASSWORD=postgres ghcr.io/queen-mq/queen:latest
```

The broker creates its own schema on boot. Then push and consume with the JavaScript SDK
(`npm install queen-mq`):

```js
import { Queen } from 'queen-mq'

const queen = new Queen('http://localhost:6632')

// Queue and partition are created on first use.
await queen
  .queue('orders')
  .partition('customer-42')
  .push([{ data: { hello: 'world' } }])

await queen
  .queue('orders')
  .group('billing')
  .each()
  .consume(async (message) => {
    console.log(message.data)
  })
```

or with curl, from anything:

```bash
curl -X POST http://localhost:6632/api/v1/push -H "Content-Type: application/json" -d '{"items":[{"queue":"demo","payload":{"hello":"world"}}]}'
```

```bash
curl "http://localhost:6632/api/v1/pop/queue/demo?autoAck=true"
```

An empty pop answers `204` with no body at all. The dashboard is at
[http://localhost:6632](http://localhost:6632).

Full walkthrough: **[queenmq.com/start/quickstart](https://queenmq.com/start/quickstart)**.

## Developing on Queen

The only thing you need in a container is PostgreSQL. The broker builds and runs natively,
which keeps the edit-compile-run loop fast and lets you attach a debugger.

**1. PostgreSQL in a container.** Nothing else goes in Docker.

```bash
docker run --name queen-dev-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:16
```

**2. Run the broker from source.** Every connection default already matches that container
(`PG_HOST=localhost`, `PG_PORT=5432`, `PG_USER=postgres`, `PG_PASSWORD=postgres`,
`PG_DATABASE=postgres`), so there is nothing to configure:

```bash
cd server && cargo run
```

The broker applies its schema at every boot under an advisory lock, so there is no migration
step and no ordering to get right. It listens on `:6632`.

**3. Point something at it.** Any SDK, `curl`, or the CLI:

```bash
cd clients/client-cli && go run . --server http://localhost:6632 status
```

The repository's `go.work` is what makes that build against the `client-go` in this tree
rather than the last published one, so run it from inside the repository.

**4. Run the tests.** Unit tests need nothing:

```bash
cd server && cargo test
```

The full client matrix builds throwaway stacks in Docker and runs every language suite against
a freshly built broker, on a single-node stack and on a two-broker mesh:

```bash
test/run.sh
```

### Three things to know before you build

- **The SQL lives inside the binary.** `server/sql/schema.sql` and everything under
  `server/sql/procedures/` is embedded with `include_str!` at compile time. Editing a `.sql`
  file and restarting is not enough: you have to rebuild, or you will be running the previous
  version of your own stored procedure.
- **So does the dashboard.** `server/src/handlers/static_files.rs` embeds `server/webapp/dist`
  at compile time. To work on the UI, build it into place and rebuild the broker:
  `cd app && npm install && npm run build` writes straight into `server/webapp/dist`.
- **`/health` talks to the database.** It answers `503` when PostgreSQL is unreachable, which is
  correct for readiness and wrong for liveness. Do not wire it to a restart policy.

More: **[Contributing](CONTRIBUTING.md)** ·
**[queenmq.com/internals/contributing](https://queenmq.com/internals/contributing)**

## Clients

| Language | Package | Source |
| --- | --- | --- |
| JavaScript / TypeScript | `queen-mq` (npm) | [clients/client-js](clients/client-js) |
| Python | `queen-mq` (PyPI) | [clients/client-py](clients/client-py) |
| Go | `github.com/smartpricing/queen/clients/client-go` | [clients/client-go](clients/client-go) |
| Rust | `queen-mq` (crates.io) | [clients/client-rust](clients/client-rust) |
| PHP / Laravel | in this tree, not yet on Packagist | [clients/client-laravel](clients/client-laravel) |
| C++ | single header | [clients/client-cpp](clients/client-cpp) |
| CLI | `queenctl` | [clients/client-cli](clients/client-cli) |

All of them speak the same HTTP API, which is documented in full and published as
[OpenAPI 3.1](https://queenmq.com/reference/openapi) generated from the router itself.

The Rust client is the one exception to "an SDK re-describes the wire by hand": it and the
broker both depend on [`crates/queen-protocol`](crates/queen-protocol), and the broker's own
tests round-trip its request parsers and rendered responses through those types. A renamed
field fails a test instead of reaching a client.

## Embedding the engine

The clients above talk to a broker. There is a second way in, for Rust products that want to
*be* one: the broker crate has a library target, and the same engine that serves HTTP in the
container runs inside your process. `Broker::start` connects to PostgreSQL, applies the
schema, starts the background machinery and hands back typed operations — push, pop with
long-poll, ack, leases, transactions, configure, delete, the DLQ, metrics. Each one invokes
the same handler functions the HTTP router dispatches to, so behaviour and defaults are the
broker's by construction, not a reimplementation's. What it buys you is one process to build,
version and supervise instead of two.

```toml
[dependencies]
queen-engine = { version = "1.0.0", default-features = false }
```

The package is named `queen-engine` — the bare crates.io name `queen` belongs to an unrelated
crate — but the library still imports as `queen`. `default-features = false` skips the HTTP
server, the dashboard and the tracing subscriber: an embedding application owns its own
surface and its own logging. (`queen-mq` remains the HTTP *client*; this crate is the broker.)

```rust
use queen::{Broker, BrokerConfig};
use queen::protocol as qp;

let broker = Broker::start(
    BrokerConfig::new().pg("localhost", 5432, "postgres", "postgres", "postgres"),
)
.await?;

broker.configure(&qp::ConfigureRequest::new("jobs")).await?;
broker.push(vec![qp::PushItem::new("jobs", serde_json::json!({"n": 1}))]).await?;
let popped = broker.pop("jobs", &qp::PopParams::default()).await?;
```

Facts to know before you build on it. One `Broker` per process lifetime is the supported
shape; the admission arbiter is process-global, so a second concurrent instance or a
start-shutdown-start cycle degrades maintenance metering. A push answered `status:"buffered"`
sits in a per-instance temp spool by default and does not survive a restart — configure
`spool_dir` if you want the outage spool to be durable. N embedded instances over one
PostgreSQL coordinate through the database exactly like N binaries do, minus the mesh:
cross-instance wake-ups ride the periodic floors instead of peer frames. And the v1 surface
is the data plane plus the DLQ; consumer-group administration, listings, traces and streams
still need the HTTP surface. The engine is not a lighter Queen — it is the same engine, minus
the HTTP layer, at the same PostgreSQL cost.

Guide: **[queenmq.com/use/embed](https://queenmq.com/use/embed)** · API reference:
**[queenmq.com/reference/engine](https://queenmq.com/reference/engine)**.

## Repository layout

| Path | What it is |
| --- | --- |
| `server/` | The broker. Rust, package `queen-engine`, library `queen`, binary `queen`. Schema and stored procedures in `server/sql/`, embedded at compile time. |
| `proxy/` | Multi-tenant gateway: API keys, quotas, rate limits, metering, console. Its own PostgreSQL. |
| `app/` | The Vue dashboard, compiled into the broker binary. |
| `clients/` | The six SDKs and the `queenctl` CLI. |
| `crates/` | Crates shared between the broker and a client. Today: `queen-protocol`, the wire types — a regular dependency of both, and the request/response types of the embedded engine. |
| `webdoc/` | This project's documentation site (Astro). Large parts of it are generated from the source in `server/` and `proxy/`. |
| `test/` | The Docker test harness: every client suite against a freshly built broker. |
| `benchmark-queen/` | Benchmark sessions with their raw artifacts. Every number on the website comes from here. |
| `examples/`, `streams/` | Complete runnable examples: `examples/full/` in JavaScript, Python, Go and Rust, with a runner that asserts each one's outcome. |

## Documentation

The site is written from the current source, and its reference material is generated from it:
the route table, the environment-variable reference, the Prometheus family list, the proxy's
route classes, the OpenAPI documents and the benchmark figures are all derived at build time,
and CI fails when any of them falls behind the code.

- [Start here](https://queenmq.com/start): what Queen is, why it exists, and where its limits are
- [Use Queen](https://queenmq.com/use): the model, the SDKs, the embedded engine, worked examples
- [Self-hosting](https://queenmq.com/selfhost): deployment, PostgreSQL, high availability, security, operations, multi-tenancy
- [Internals](https://queenmq.com/internals): segments, offsets, the push and pop paths, the schema
- [Reference](https://queenmq.com/reference): routes, configuration, metrics, client APIs
- [Benchmarks](https://queenmq.com/benchmarks): the runs, their configuration, and their raw output

## Versions

Version **1.0.0** is a Rust broker on a new storage engine. The 0.16.x line was a C++
implementation on a row-based engine and is retired; its measurements and its architecture
documentation do not describe this release. See [CHANGELOG.md](CHANGELOG.md) and
[queenmq.com/reference/compatibility](https://queenmq.com/reference/compatibility).

## Contributing

Bug reports and feature requests are welcome through the
[issue templates](https://github.com/queen-mq/queen/issues/new/choose). Start from
[CONTRIBUTING.md](CONTRIBUTING.md). Security issues: [SECURITY.md](SECURITY.md).

## License

[Apache 2.0](LICENSE.md).

---

**Built with ❤️ by [Smartness](https://www.linkedin.com/company/smartness-com/)**
