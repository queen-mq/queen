<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/queen-tile-light.svg">
  <img src="assets/queen-tile.svg" alt="" width="76" height="76">
</picture>

# Queen MQ

**High-performance transactional messaging for applications that need an ordered stream per entity.**

Queen is a message broker written in **Rust** that keeps every byte of its state in **PostgreSQL**. Its
defining abstraction is one logical ordered partition per application entity (a customer, an
account, a conversation, a device, a workflow, a session, a job), created by the first push that
names it, never provisioned in advance.

[Documentation](https://queenmq.com) · [Benchmarks](https://queenmq.com/benchmarks) · [Quickstart](https://queenmq.com/start/quickstart) · [Try it](https://queenmq.cloud) · Apache-2.0 · v1.4.1


Queen speaks HTTP, but is also protocol-compatible with Kafka and SQS clients.

[Try Queen on a hosted test instance](https://queenmq.cloud)
</div>

---

## The problem Queen solves

Most brokers order per *shard*; you need ordering per *entity*: this customer's events in order,
this conversation's messages not overtaking each other. Hash entities onto a fixed partition count
and the ones that collide block each other; give each its own queue and broker-side objects grow
with your customer list. Queen removes the bridge: **the entity *is* the partition.**

The second problem is quieter and costs more. A broker that only moves bytes leaves you writing the
outbox and its relay, the idempotency table, the scheduler, the retry bookkeeping and replay
scripts, the Redis for state, the state store a stream processor needs. All of that is coordination
state, and Queen commits ack, push, kv and timers together in one transaction, so it stops being
yours to write: not a broker that scales partitions, but one that holds the coordination logic.

**You can offload most of your complex application logic to Queen.**

---

## Features

**Ordered partitions, created on demand.** A partition exists as soon as a push names it, and
ordering holds inside it. Consumer groups with one cursor per partition · subscription modes ·
leases with explicit ack, nack, retry and dlq · replay and seek by offset or timestamp · retry
budgets and a real dead-letter queue, replayable · long-poll consumption · retention by age and by
completion · durable by default.

**Exactly once, in one Postgres commit.** Deduplication at push, keyed on your transaction id
([dedup](https://queenmq.com/internals/dedup)) · `ack + kv + push + timers` commit together or not
at all.

**State and time in the broker.** [`queen.kv`](https://queenmq.com/use/kv), a transactional
key/value store with optimistic locking and an expiry on every write ·
[timers](https://queenmq.com/use/timers) that schedule a real message into a real queue and stay
cancellable until they fire · delayed delivery · window-buffer debounce · conflation, last-value
delivery per partition.

**Stream processing in your process.** An [operator chain](https://queenmq.com/use/streams) with
state in the same PostgreSQL: no job manager, no changelog topic, no state store to deploy. Four
window types (tumbling, sliding, session, cron), map/filter/aggregate, event time with watermarks,
per-message gating. One cycle commits state, sink pushes and the ack together.

**Ephemeral queues, no database in the path.** An
[in-memory class](https://queenmq.com/use/ephemeral) for request/reply, signalling, presence
fan-out and cache invalidation: the shapes that should not pay for replay and retention.

**Kafka and SQS clients connect directly.** Since 1.4.0 Queen speaks both wire protocols, so an
existing client moves over by changing its connection URL. The supported
surface, and every place behaviour differs from the real thing, are in
[reference/kafka](https://queenmq.com/reference/kafka) and
[reference/sqs](https://queenmq.com/reference/sqs). The two protocols share the rows rather than
copying them, so a Kafka producer and a Queen consumer can read the same messages at the same
time with no connector in between:
[Kafka in, Queen out](https://queenmq.com/use/full-examples/cross-protocol)
([code](examples/cross-protocol)).

**One binary, no sidecars.** Stateless, and curl is a first-class client · six SDKs (JavaScript,
Python, Go, Rust, C++, PHP/Laravel) plus `queenctl` · a dashboard served by the same binary on the
same port · Prometheus metrics · JWT/JWKS auth · payload encryption · disk spool for database
outages · multi-tenant · HA replicas.

**~1M msg/s each way, with `synchronous_commit` left on.** 86.4B messages pushed, popped and
acknowledged in a 24-hour soak, 0 restarts, broker memory flat at ~4.1 GB · 1M ordered
partitions in one queue, none preallocated, created at 1,000/s while the run served 200,000
msg/s · 88.5M messages across four stages with 0 duplicates, 0 gaps, 0 order violations. Every
figure's conditions, and what they do not establish, are in
[Published benchmarks](#published-benchmarks).

---

## One entity, one ordered partition

You choose the partition key. It is your ordering boundary, not an infrastructure sizing decision.

```text
customer A  ──►  A1 ──► A2 ──► A3            strict FIFO within a lane
customer B  ──►  B1 ──► B2                   B is not held up by A
customer C  ──►  C1 ──► C2 ──► C3            C is not held up by A or B
```

Each lane is created by the push that first names it. Nothing is preallocated, nothing rebalances
when a consumer restarts. A partition is a row: a million of them measured 315 MB, and the serve
path does not care how many exist.

**Two limits.** A hot partition stays sequential by design, so twenty workers on a one-partition
queue is one worker's throughput and nineteen idle pollers: add lanes, not workers. And do not pick
a key for its cardinality. A partition per message is not a supported shape.
[The model, in one page](https://queenmq.com/use/model).

## Transactional processing

One [`POST /api/v1/transaction`](https://queenmq.com/reference/http/transaction) bundles
acknowledgements, pushes, key/value writes and timer operations into **one PostgreSQL transaction**:

```text
consume input
     │
     ├── update application state   (kv rider)
     ├── produce output             (push, any queue, any partition)
     ├── schedule / cancel a timer  (timers rider)
     └── acknowledge input          (cursor advance)
                │
             COMMIT          all of it, or none of it
```

It is N-to-M across any number of partitions, queues and consumer groups, which is what makes a
fan-in stage possible.

**Where the guarantee stops.** Atomicity covers broker state, not the network: if the response is
lost, a blind retry duplicates the pushes unless your transaction ids are deterministic and the
retry lands inside the deduplication window. No broker makes an external HTTP call exactly-once. The
case that *is* exactly-once end to end is an effect written as a row in this PostgreSQL through the
`kv` rider, where marker, effect, output and cursor advance become a single `COMMIT`.
[Every rollback cause](https://queenmq.com/reference/http/transaction).

## The mental model

```text
Application entity        the thing whose order matters
    ▼  Partition key      your ordering boundary, chosen by you
    ▼  Ordered stream     one row, created on demand, strict FIFO
    ▼  Transaction        ack + state + output in one commit
    ▼  PostgreSQL         the single source of truth
    ▼  Stateless brokers  hold nothing durable; restart freely
    ▼  Cell               one deployment, one failure domain
    ▼  Region             where a cell physically lives
```

The top half is your application's shape, the bottom half is infrastructure, and Queen's bet is that
the two scale independently: **millions of logical entity streams do not require millions of
infrastructure objects.**

## Published benchmarks

| Run | Result | The conditions that make it true |
| --- | --- | --- |
| [Throughput, 24h](https://queenmq.com/benchmarks/soak-24h) | **86,369,975,300 messages in 24 hours**, about 1,000,000 a second *in each direction*: pushed, popped and acknowledged. 0 restarts, memory flat at ~4.1 GB | Explicit acks, **dedup on** (60 s), retention on, 200 partitions, 600 consumers, batch 100, 256 B payloads |
| [Cardinality](https://queenmq.com/benchmarks/cardinality-1m) | **1,000,000 ordered partitions in one queue**, none preallocated, created at 1,000/s during the run while serving **200,000 msg/s**, **0 push / pop / ack errors** | Explicit acks, retention on, **dedup off**, 300 consumers, 1 hour. p50 27.5 ms, p99 115 ms at full space |
| [Ordering correctness](https://queenmq.com/benchmarks/ordered-pipeline) | **88,503,408 messages, 4 stages, 1,000 partitions, 0 duplicates, 0 gaps, 0 order violations** | 25,000 events/s for 600 s, dedup window 300 s, per-stage verifier |
| [Multi-tenant cell](https://queenmq.com/benchmarks/multitenant-cell) | **0 cross-tenant deliveries** over an hour, 12 tenants sharing one queue and group name; a 2-core cell held >2400 msg/s through the proxy | Enforcement on, 429 retry disabled. The run's *aggregate* verdict is FAIL: two rate-limited tenants' unretried refusals count as misses. Isolation is the clean result, not throughput |

The headline runs are Queen 1.0.0 with `synchronous_commit` left `on`, one broker against one
PostgreSQL 18; the pipeline and multi-tenant runs predate the 1.0.0 tag.

**What these do not establish.** Single-shape runs say nothing about *your* throughput, latency,
sizing, disk or partition distribution: those follow from your workload, payloads and hardware. Read
[method and rig](https://queenmq.com/benchmarks/method) before quoting a number.

## Inside a cell

A cell is PostgreSQL plus one or more stateless brokers, optionally fronted by Queen Proxy: at once
the scaling boundary, the failure boundary and the unit of upgrade.

**PostgreSQL is the source of truth, and brokers hold nothing authoritative.** Messages, offsets,
leases, deduplication state, configuration and dead letters are all rows. That is why a broker can
be restarted or rolled without a rebalance, and why deduplication stays exact across replicas with
no coordination protocol at all. Brokers trade latency hints over a mesh port, but nothing on that
wire is authoritative ([the mesh](https://queenmq.com/internals/mesh)).

**Three axes, not interchangeable.** Partitions scale application cardinality, with nothing
provisioned and no rebalance. Brokers scale availability inside a cell, and **three replicas is the
designed ceiling**, because past that the bottleneck is PostgreSQL rather than the broker count.
Cells scale the deployment, with no global cluster to join and no cross-cell coordination in the
message path, which buys bounded failure domains and geographic placement but not cross-region
replication or global ordering.

**The failure domain is PostgreSQL.** Queen does not replicate itself. While the database is
unreachable, pushes spool to a node-local disk buffer and replay later, and reads fail safely
because an unacknowledged lease redelivers ([high availability](https://queenmq.com/deploy/ha)).

## Why PostgreSQL

Messaging state and application state share a transaction: the whole reason for the design, and
something no other storage choice offers. Beyond that, durability, ACID, replication, PITR and
backup you already know how to operate · SQL introspection, because your messages are rows · no
extensions and no migration step, since the broker applies its own schema at boot. PostgreSQL 15+.

The trade, plainly: the database is the throughput ceiling and the single failure domain.

## Queen Proxy and multi-tenancy

`queen_proxy` is a second Rust binary and the tenant-facing boundary, holding what a shared broker
has no business holding: per-cluster API keys and human logins, plan limits on rate, size and count,
and usage metering ([multi-tenant](https://queenmq.com/deploy/multi-tenant)).

**Isolation is split across both processes on purpose.** The broker scopes queue identity natively
as `(tenant, name)` in SQL on every read and write, so two tenants owning a queue called `orders`
own different queues; the proxy is what makes that tenant identity trustworthy. Neither half is
sufficient alone ([isolation](https://queenmq.com/reference/multi-tenant/isolation)).

A **cluster** is the tenant-visible Queen, one hostname and one namespace; a **cell** is the
physical stack it runs on. A cluster never spans two cells, which is why quota accounting is exact
in-process state with nothing to coordinate. Operators place clusters onto cells, so customers
address a region and never a cell, and the control plane stays out of the message data path: its
outage does not stop a cell that is already running.

## Laravel queues and lightweight supervision

The PHP package is a native Laravel queue driver with a portable PHP supervisor, and the separate
[`queen-supervisor`](supervisor/README.md) binary provides the same orchestration with a low-memory
Rust control plane. Both start ordinary `php artisan queue:work` children and scale pools from queue
depth or from backlog multiplied by observed job duration. The topology is deliberately
single-active, one supervisor replica per consumer group: a filesystem lock excludes a second local
process, but there is no distributed fenced leader lease yet. Configuration, `pcntl` requirements
and secret handling are in the [PHP/Laravel client
guide](clients/client-php/README.md#supervisor).

## Quick start

Docker and about five minutes. Nothing is installed into PostgreSQL; there is no migration to run.

```bash
docker network create queen
docker run -d --name queen-pg --network queen -e POSTGRES_PASSWORD=postgres postgres:16
docker run -d --name queen --restart on-failure:10 --network queen -p 6632:6632 \
  -e PG_HOST=queen-pg -e PG_PASSWORD=postgres \
  -v queen-spool:/var/lib/queen/buffers ghcr.io/queen-mq/queen:latest
curl -s http://localhost:6632/health
```

The restart policy covers the seconds PostgreSQL spends initialising: the broker refuses to start
against a database it cannot reach. Open `http://localhost:6632` for the bundled dashboard. The
queue *and* the partition are created by this call:

```bash
curl -X POST http://localhost:6632/api/v1/push -H 'content-type: application/json' -d '{
  "items": [{ "queue": "orders", "partition": "customer-123",
              "transactionId": "order-8891-created",
              "payload": { "orderId": 8891 } }]
}'
```

`transactionId` is your idempotency key: a retry of the same push writes nothing the second time.
Full walkthrough in the [Quickstart](https://queenmq.com/start/quickstart); the whole stack with
proxy and two brokers in [Compose](https://queenmq.com/deploy/compose).

## Documentation

- **[The model](https://queenmq.com/use/model)**: queues, partitions, groups, offsets, leases, retention.
- **[Transactions](https://queenmq.com/reference/http/transaction)**: bundle shape, rollback causes, the exactly-once boundary.
- **[KV](https://queenmq.com/use/kv)** · **[Timers](https://queenmq.com/use/timers)** · **[Streams](https://queenmq.com/use/streams)** · **[Ephemeral](https://queenmq.com/use/ephemeral)**: beyond push and pop.
- **[Deploy](https://queenmq.com/deploy)** · [PostgreSQL](https://queenmq.com/deploy/postgres) · [HA](https://queenmq.com/deploy/ha) · [Kubernetes](https://queenmq.com/deploy/kubernetes) · [Kafka](https://queenmq.com/deploy/kafka) · [SQS](https://queenmq.com/deploy/sqs).
- **[Multi-tenant](https://queenmq.com/deploy/multi-tenant)** · [Proxy](https://queenmq.com/deploy/proxy) · [Isolation](https://queenmq.com/reference/multi-tenant/isolation).
- **[Internals](https://queenmq.com/internals)**: storage model, life of a push and a pop, dedup, retention, mesh.
- **[Benchmarks](https://queenmq.com/benchmarks)** · [method and rig](https://queenmq.com/benchmarks/method) · [comparison](https://queenmq.com/start/compare).
- **[HTTP reference](https://queenmq.com/reference/http)** · **[SDKs](https://queenmq.com/reference/sdk/javascript)**: routes and clients.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [contributing
guide](https://queenmq.com/internals/contributing). Benchmark claims need an archived artifact under
`benchmark-queen/`; doc pages declare the source files they are true of.

## License

Apache-2.0, see [LICENSE.md](LICENSE.md). Broker and proxy both, so the multi-tenant service is
yours to run.
