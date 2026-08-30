<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/queen-tile-light.svg">
  <img src="assets/queen-tile.svg" alt="" width="76" height="76">
</picture>

# Queen MQ

**High-performance transactional messaging for applications that need an ordered stream per entity.**

Queen is a message broker written in Rust that keeps every byte of its state in PostgreSQL. Its
defining abstraction is one logical ordered partition per application entity (a customer, an
account, a conversation, a device, a workflow, a session, a job), created by the first push that
names it, never provisioned in advance.

[Documentation](https://queenmq.com) · [Benchmarks](https://queenmq.com/benchmarks) · [Quickstart](https://queenmq.com/start/quickstart) · Apache-2.0 · v1.1.0

</div>

```text
  ~1M msg/s              1M partitions          24h                0
  pushed AND popped      in one PostgreSQL,     no restarts,       order violations
  86.4B in a 24h soak    created at 1,000/s     RSS flat 4.1 GB    over 88.5M messages
```

Published benchmark results under defined workloads, one broker against one PostgreSQL on a
32 vCPU / 62 GiB machine. They are evidence of what the design reaches, not a capacity guarantee
for your workload. Every figure links to its archived artifacts: [Benchmarks](https://queenmq.com/benchmarks).

---

## The problem Queen solves

Most brokers give you ordering per *shard*. Your requirement is ordering per *entity*: this
customer's events processed in order, this conversation's messages not overtaking each other, this
account's transactions settling in sequence.

Bridging the two is where the pain lives. Hash your entities onto a fixed partition count and the
ones that collide block each other: a slow customer stalls every customer sharing its shard. Give
each entity its own queue instead and broker-side objects grow with your customer list.

Queen removes the bridge: **the entity *is* the partition.**

## One entity, one ordered partition

You choose the partition key. It is your ordering boundary, not an infrastructure sizing decision.

```text
customer_id -> ordered per customer     device_id   -> ordered per device
account_id  -> ordered per account      workflow_id -> ordered per workflow
conversation_id -> ordered per conversation
```

Each partition is an independent ordered lane, created by the push that first names it:

```text
customer A  ──►  A1 ──► A2 ──► A3            strict FIFO within a lane
customer B  ──►  B1 ──► B2                   B is not held up by A
customer C  ──►  C1 ──► C2 ──► C3            C is not held up by A or B
```

Nothing is preallocated, nothing is assigned, nothing rebalances when a consumer restarts.

**Two things this does not mean.**

*A single hot partition stays sequential, by design.* Parallelism comes from many active
partitions, not from splitting one. Twenty workers on a one-partition queue is one worker's
throughput and nineteen idle pollers: add lanes, not workers.

*Do not pick a key because it has high cardinality.* Pick the boundary your application genuinely
requires. A partition per message is not a supported shape.

How partitions, consumer groups, cursors and leases fit together is
[the model, in one page](https://queenmq.com/use/model).

## Transactional processing

The second reason Queen exists, and the reason PostgreSQL is not an implementation detail. A single
[`POST /api/v1/transaction`](https://queenmq.com/reference/http/transaction) bundles
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

The bundle is N-to-M: one call may acknowledge batches leased from any number of partitions, queues
and consumer groups, and push to any number of queues and partitions, which is what makes a fan-in
stage possible.

This is what replaces transactional outbox tables, a separate store for idempotency markers,
offset/state coordination glue, and the reconciliation code that exists only because the broker's
commit and the database's commit were two different commits.

**Where the guarantee stops, precisely.** Atomicity covers broker state, not the network: if the
response is lost you do not know whether it committed, and a blind retry duplicates the pushes
unless your transaction ids are deterministic and the retry lands inside the deduplication window.
Queen does not make an external HTTP call exactly-once, and no broker can.

The one case that *is* exactly-once end to end is when the effect is itself a row in this
PostgreSQL, written through the `kv` rider: marker, effect, output and cursor advance become a
single `COMMIT`. [The exactly-once
boundary](https://queenmq.com/reference/http/transaction) states every rollback cause.

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

The top half is your application's shape. The bottom half is infrastructure. Queen's central bet is
that these two halves should scale independently of each other.

## Published benchmarks

| Run | Result | The conditions that make it true |
| --- | --- | --- |
| [Throughput, 24h](https://queenmq.com/benchmarks/soak-24h) | **86,369,975,300 messages in 24 hours**, about 1,000,000 a second *in each direction*: pushed, popped and acknowledged. 0 restarts, broker memory flat at ~4.1 GB | Leased pops with **explicit acks**, **dedup on** (60 s window), retention on, 200 partitions, 600 consumers, push batch 100, 256 B payloads |
| [Cardinality](https://queenmq.com/benchmarks/cardinality-1m) | **1,000,000 ordered partitions in one queue**, none preallocated, created during the run at 1,000/s while serving **200,000 msg/s**; 722,265,600 messages, **0 push / pop / ack errors** | Leased pops with explicit acks, retention on, **dedup off**, 300 consumers, 1 hour of which 43 min on the completed space. p50 27.5 ms, p99 115 ms at full space |
| [Ordering correctness](https://queenmq.com/benchmarks/ordered-pipeline) | **88,503,408 messages across 4 stages and 1,000 partitions with 0 duplicates, 0 gaps, 0 order violations** | 25,000 events/s for 600 s, dedup window 300 s, explicit bulk acks, per-stage verifier |
| [Multi-tenant cell](https://queenmq.com/benchmarks/multitenant-cell) | **0 cross-tenant deliveries** over an hour with 12 tenants sharing one queue name and one group name; a 2-core cell held >2400 msg/s through the proxy | Enforcement on, dedup off, 429 retry disabled. The run's *aggregate* verdict is FAIL: two rate-limited tenants' unretried refusals count as misses. Isolation is the clean result, not throughput |

The two headline runs are Queen 1.0.0 with PostgreSQL's `synchronous_commit` left `on`, one broker
against one PostgreSQL 18; the pipeline and multi-tenant runs predate the 1.0.0 tag.

**What these do not establish.** Single-shape runs say nothing about *your* throughput, latency,
PostgreSQL sizing, disk, retention capacity or partition distribution: those follow from your
workload, payloads and hardware. Read [method and rig](https://queenmq.com/benchmarks/method)
before quoting a number.

## Application scale is not infrastructure scale

This is the principle the rest of the architecture follows from.

In most brokers the two are welded together: a per-entity ordering guarantee means a per-entity
infrastructure object, either a topic partition with its own files and replicas or a live
server-side queue. Doubling your customers means doubling something an operator has to think about.

In Queen a partition is a row. A million of them measured 315 MB in total, and the serve path does
not care how many exist. **Millions of logical entity streams do not require millions of
infrastructure objects.**

So the two halves scale on different axes. Confusing them is the most common way to mis-size a
deployment.

## Three kinds of scale

**A. Partitions scale application cardinality.** Add entities freely. Nothing is provisioned, no
process is created, no rebalance runs.

**B. Brokers scale serving capacity and availability inside a cell.** Stateless replicas of one
binary against one PostgreSQL, covering a process dying, a rolling restart, one node's network.
**Three replicas is the designed ceiling**: past that the bottleneck is the database, not the
broker count. Do not confuse this with partition scaling: throughput comes from partitions and from
PostgreSQL, and more brokers buy availability first.

```text
  Broker A ─┐
  Broker B ─┼──►  PostgreSQL       every replica serves every route
  Broker C ─┘                      pop from A, ack to B, no affinity
```

**C. Cells scale the deployment.** Capacity grows by adding cells, not by growing one system: there
is no global cluster to join and no cross-cell coordination in the message path.

```text
   one cell  ──►  another cell  ──►  another cell  ──►  another region
```

That buys bounded failure domains, independent upgrades, geographic placement and simpler recovery.
It does not, on its own, solve cross-region replication or global ordering.

## Inside a cell

```text
                    Queen Cell
     ┌────────────────────────────────────┐
     │  Queen Broker ──┐                  │
     │  Queen Broker ──┼──► PostgreSQL    │  the only durable state
     │  Queen Broker ──┘                  │
     │  Queen Proxy  (optional)           │  tenant-facing boundary
     └────────────────────────────────────┘
```

A cell is PostgreSQL plus one or more stateless brokers, optionally fronted by Queen Proxy. It is
at once the scaling boundary, the failure boundary and the unit of upgrade and operational
ownership.

**PostgreSQL is the durable source of truth, and the brokers hold nothing authoritative.** Messages,
offsets, leases, deduplication state, queue configuration and dead letters are all rows. That is why
a broker can be added, removed, restarted or rolled without a rebalance, and why deduplication stays
exact across replicas with no coordination protocol at all. Brokers do exchange hints over a mesh
port to shorten latency, but nothing on that wire is authoritative and a dropped hint costs nothing
but time ([the mesh](https://queenmq.com/internals/mesh)).

**The failure domain is PostgreSQL.** Queen does not replicate itself; keeping the database alive is
PostgreSQL's own tooling. While it is unreachable, pushes spool to a node-local disk buffer and are
replayed later, and reads fail safely because an unacknowledged lease redelivers
([high availability](https://queenmq.com/deploy/ha)).

## Why PostgreSQL

Not "we needed somewhere to put the bytes". PostgreSQL is chosen for what it lets the
*application* do.

- **Messaging state and application state share a transaction.** This is the whole reason for the
  design; no other storage choice offers it.
- Durability, ACID, replication, PITR, backup and recovery you already know how to operate.
- SQL introspection: your messages are rows, queryable with the tools you already have.
- No extensions and no migration step: the broker carries its own schema and applies it at boot.
  PostgreSQL 15+.

The trade, plainly: the database is the throughput ceiling and the single failure domain.

## Queen Proxy and multi-tenancy

```text
   Internet  ─►  Queen Proxy  ─►  Queen brokers  ─►  PostgreSQL
                 the tenant       messaging and
                 boundary         processing only
```

`queen_proxy` is a second Rust binary and the tenant-facing boundary. Queen core stays focused on
messaging; everything a shared broker has no business holding lives in the proxy: per-cluster API
keys and human logins, plan limits on rate, size and count, and usage metering
([multi-tenant](https://queenmq.com/deploy/multi-tenant)).

**Isolation is split across both processes on purpose.** The broker scopes queue identity natively
as `(tenant, name)` in SQL on every read and write, so two tenants both owning a queue called
`orders` own different queues. The proxy is what makes the tenant identity driving that scoping
trustworthy. Neither half is sufficient alone
([isolation](https://queenmq.com/reference/multi-tenant/isolation)).

**Cluster and cell are different things, and the distinction matters.** A **cluster** is the
tenant-visible Queen: one hostname, one plan, one namespace. A **cell** is the physical stack it
runs on. A cluster lives on exactly one cell and never spans two, which is why the proxy's quota
accounting is exact in-process state with nothing to coordinate. One cell hosts many tenants,
bounded by its own measured capacity.

## Fleet and regions

**What exists today.** The proxy's control-plane database models a fleet: cells carry a region, a
class (shared or dedicated) and capacity, and clusters are placed onto them by operators.

```text
        Control plane  (cells, tenants, clusters, plans, usage)
                │  placement and lifecycle only, never the data path
     ┌──────────┼──────────┐
   EU cell   EU cell    US cell        each: Proxy + Brokers + PostgreSQL
```

Two product properties follow, and both hold now:

- **Customers address a cluster in a region, never a cell.** Cells are an internal deployment unit.
  Which one hosts a cluster is an operator concern, invisible to the tenant.
- **The control plane is not in the message data path.** The proxy reads it through a cache, so a
  control-plane outage does not stop a cell that is already running.

**What is planned, and not shipped.** A hosted Queen Cloud with automated cell provisioning and
automatic tenant placement does not exist in this repository. Today cells are created and clusters
placed by operators, not by an API. What *is* shipped is the self-hostable multi-tenant stack above
(proxy, quotas, metering, isolation), Apache-2.0 and yours to run.

## Features

**Messaging.** Ordered FIFO partitions created on demand · consumer groups with one cursor per
partition · subscription modes · leases with explicit ack, nack, retry and dlq · replay and seek by
offset or timestamp · retry budgets and a real dead-letter queue, replayable · long-poll consumption
· retention by age and by completion · durable by default, with no trade of `synchronous_commit`
for speed.

**Exactly-once building blocks.** Deduplication at push, keyed on your transaction id: exact, not
probabilistic, evaluated inside PostgreSQL so a duplicate writes nothing
([dedup](https://queenmq.com/internals/dedup)) · transactional `ack + kv + push + timers` in one
commit.

**State and time.** [`queen.kv`](https://queenmq.com/use/kv), a transactional key/value store with
optimistic locking and an expiry on every write ·
[timers](https://queenmq.com/use/timers) that schedule a real message into a real queue and stay
cancellable until they fire · delayed delivery · window-buffer debounce · conflation, last-value
delivery per partition.

**Stream processing.** An [operator chain](https://queenmq.com/use/streams) that runs in *your*
process with state in the same PostgreSQL: no job manager, no changelog topic, no state store to
deploy. Four window types (tumbling, sliding, session, cron), map/filter/aggregate, event time with
watermarks, and per-message gating. One cycle commits state, sink pushes and the ack together.

**Ephemeral queues.** An [in-memory class](https://queenmq.com/use/ephemeral) with no database in
the path, for request/reply, signalling, presence fan-out and cache invalidation: the shapes that
should not pay for replay and retention. Contents survive nothing; that is the whole trade, and it
is explicit.

**Kafka wire protocol (preview).** [`queen-kafka`](queen-kafka/README.md) is a separate binary that
speaks the Kafka protocol to clients and plain HTTP to a broker or the proxy, so an unmodified
producer or consumer reaches Queen by changing `bootstrap.servers` and nothing else. Thirty-two API
keys are advertised: produce, fetch, consumer groups, the topics and groups admin surface, the
idempotent producer, and transactions that commit records and consumer offsets in one PostgreSQL
transaction. The [client matrix](queen-kafka/compat/CLIENT_MATRIX.md) is nineteen measured rows over
ten languages, fourteen PASS and five PARTIAL with none failing, and every answer is diffed against
`apache/kafka:3.9.1` with nothing left to classify by hand. There is no log compaction, so Kafka
Streams and Connect's exactly-once source stay out, and a transaction is held by one facade process,
so Flink and Spark's two-phase writers do too. The protocol contract is
[reference/kafka](https://queenmq.com/reference/kafka); running it is
[deploy/kafka](https://queenmq.com/deploy/kafka).

**Operations.** One stateless binary, HTTP on port 6632, curl is a first-class client · six SDKs
(JavaScript, Python, Go, Rust, C++, PHP/Laravel) plus `queenctl` · a dashboard served by the same
binary on the same port · Prometheus metrics · JWT/JWKS auth · payload encryption · disk spool for
database outages · HA replicas.

## Laravel queues and lightweight supervision

The PHP package is a native Laravel queue driver and includes a portable PHP
supervisor. The separate [`queen-supervisor`](supervisor/README.md) binary
provides the same process orchestration with a low-memory Rust control plane.
Both start ordinary `php artisan queue:work` children; the broker remains
responsible only for messages, leases, timers, the DLQ and queue metrics.

Pools can scale from group-specific `effectivePending` depth (`size`) or from
backlog multiplied by observed Laravel job duration (`time`). The two engines
share resolved configuration plus local status, pause, continue and terminate
commands. Multiple `QUEEN_URLS` provide broker-endpoint failover.

The current supervisor topology is deliberately single-active: run exactly one
supervisor replica per application/consumer group. Its filesystem lock excludes
a second local process, but Queen does not yet provide a distributed fenced
leader lease. Many worker processes are supported; two autonomous supervisor
masters on different hosts are not. See the [PHP/Laravel client
guide](clients/client-laravel/README.md#worker-supervisor) for configuration,
Unix/`pcntl` requirements, deployment and secret handling.

## Where Queen fits

- **Per-customer or per-account workflows**: `customer-123` is a lane; a slow customer delays only itself.
- **Conversations, chat, agent and tool-call sessions**: `conversation-42` keeps order without a queue per conversation.
- **Financial and ledger processing**: ordering per account, plus ack and state in one commit.
- **IoT and device telemetry**: `device-123` ordering at high device counts.
- **Multi-step pipelines and sagas**: ack-input-and-push-output atomically, no outbox table.
- **Multi-tenant SaaS**: many tenants on one cell behind the proxy, isolation enforced in SQL.

## When something else is the better tool

These systems made different tradeoffs, and some of those tradeoffs are better than Queen's for
workloads that need them.

- **The Kafka ecosystem, past the wire protocol.** The protocol itself is spoken by
  [`queen-kafka`](queen-kafka/README.md) and unmodified clients work through it, but there is no log
  compaction and a transaction lives in one facade process: Kafka Streams, Connect's exactly-once
  source, the Schema Registry's compacted `_schemas` topic, and Flink and Spark's two-phase Kafka
  writers all stay out, and the operational literature about brokers, replicas and log directories
  does not apply.
- **A single ordered stream that must itself scale horizontally.** One lane is sequential. If your
  ordering boundary is "everything", Queen's core idea does nothing for you.
- **Storage that scales independently of the database**, or a system that replicates itself across
  regions. Retention lives in PostgreSQL, which is also the one failure domain; there is no tiered
  object storage and no cross-region replication in this repository.
- **Rich content-based routing**: no exchanges, no bindings, no header matching. That is
  RabbitMQ's strength.
- **No server to operate at all.** That is SQS.

In one sentence: distributed logs make *infrastructure partitions* the fundamental scaling
abstraction; Queen makes *application entities* the fundamental ordering abstraction and *cells* the
infrastructure scaling boundary. A fuller side-by-side is in
[Comparison](https://queenmq.com/start/compare).

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
against a database it cannot reach. Open `http://localhost:6632` for the bundled dashboard, then
push your first message. The queue *and* the partition are created by this call:

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

- **[The model](https://queenmq.com/use/model)**: queues, partitions, groups, offsets, leases, retention, in one page.
- **[Transactions](https://queenmq.com/reference/http/transaction)**: bundle shape, rollback causes, the exactly-once boundary.
- **[KV](https://queenmq.com/use/kv)** · **[Timers](https://queenmq.com/use/timers)** · **[Streams](https://queenmq.com/use/streams)** · **[Ephemeral](https://queenmq.com/use/ephemeral)**: the surfaces beyond push and pop.
- **[Deploy](https://queenmq.com/deploy)** · [PostgreSQL](https://queenmq.com/deploy/postgres) · [HA](https://queenmq.com/deploy/ha) · [Kubernetes](https://queenmq.com/deploy/kubernetes): running it.
- **[Multi-tenant](https://queenmq.com/deploy/multi-tenant)** · [Proxy](https://queenmq.com/deploy/proxy) · [Isolation](https://queenmq.com/reference/multi-tenant/isolation): running it for other people.
- **[Internals](https://queenmq.com/internals)**: storage model, life of a push and a pop, dedup, retention, mesh.
- **[Benchmarks](https://queenmq.com/benchmarks)** · [method and rig](https://queenmq.com/benchmarks/method) · [comparison](https://queenmq.com/start/compare).
- **[HTTP reference](https://queenmq.com/reference/http)** · **[SDKs](https://queenmq.com/reference/sdk/javascript)**: routes and clients.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [contributing
guide](https://queenmq.com/internals/contributing). Benchmark claims need an archived artifact
under `benchmark-queen/`; doc pages declare the source files they are true of.

## License

Apache-2.0, see [LICENSE.md](LICENSE.md). Broker and proxy both, so the multi-tenant service is
yours to run.
