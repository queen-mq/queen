<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/queen-tile-light.svg">
  <img src="assets/queen-tile.svg" alt="" width="76" height="76">
</picture>

# Queen MQ

**High-performance transactional messaging, with an ordered stream per entity.**

**You can offload most of your complex application logic to Queen.**

Queen is a message broker written in **Rust**: one binary per node, keeping its state in a
**replicated log** on the node's own disk, with no external database. Its defining abstraction is
one logical ordered partition per application entity (a customer, an account, a conversation, a
device, a workflow, a session, a job), created by the first push that names it, never provisioned
in advance.

[Documentation](https://queenmq.com) · [Benchmarks](https://queenmq.com/benchmarks) · [Quickstart](https://queenmq.com/start/quickstart) · [Try it](https://queenmq.cloud) · Apache-2.0 · v2.0.0-alpha


Queen speaks HTTP, but is also protocol-compatible with Kafka clients.

[Free on Queen Cloud](https://queenmq.cloud). The same broker, hosted.
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

---

## Features

**Ordered partitions, created on demand.** A partition exists as soon as a push names it, and
ordering holds inside it. Consumer groups with one cursor per partition · subscription modes ·
leases with explicit ack, nack, retry and dlq · replay and seek by offset or timestamp · retry
budgets and a real dead-letter queue, replayable · long-poll consumption · retention by age and by
completion · durable by default.

**Exactly once, in one log entry.** Deduplication at push, keyed on your transaction id
([dedup](https://queenmq.com/internals/dedup)) · `ack + kv + push + timers` commit together or not
at all.

**State and time in the broker.** [`queen.kv`](https://queenmq.com/use/kv), a transactional
key/value store with optimistic locking and an expiry on every write ·
[timers](https://queenmq.com/use/timers) that schedule a real message into a real queue and stay
cancellable until they fire · delayed delivery · window-buffer debounce · conflation, last-value
delivery per partition.

**Stream processing in your process.** An [operator chain](https://queenmq.com/use/streams) with
state in the same broker: no job manager, no changelog topic, no state store to deploy. Four
window types (tumbling, sliding, session, cron), map/filter/aggregate, event time with watermarks,
per-message gating. One cycle commits state, sink pushes and the ack together.

**Ephemeral queues, no disk in the path.** An
[in-memory class](https://queenmq.com/use/ephemeral) for request/reply, signalling, presence
fan-out and cache invalidation: the shapes that should not pay for replay and retention.

**Kafka clients connect directly.** The Kafka facade runs inside the broker process when switched
on, so an existing client moves over by changing its connection URL. The supported surface, and
every place behaviour differs from the real thing, are in
[reference/kafka](https://queenmq.com/reference/kafka).

**One binary, no sidecars, no database.** curl is a first-class client · six SDKs (JavaScript,
Python, Go, Rust, C++, PHP/Laravel) plus `queenctl` · a dashboard served by the same binary on the
same port · Prometheus metrics · JWT/JWKS auth · payload encryption · the multi-tenant proxy in the
same process · Raft replication over three or five nodes.

---

## One entity, one ordered partition

You choose the partition key. It is your ordering boundary, not an infrastructure sizing decision.

```text
customer A  ──►  A1 ──► A2 ──► A3            strict FIFO within a lane
customer B  ──►  B1 ──► B2                   B is not held up by A
customer C  ──►  C1 ──► C2 ──► C3            C is not held up by A or B
```

Each lane is created by the push that first names it. Nothing is preallocated, nothing rebalances
when a consumer restarts. A partition is an entry in the node's ordered store and a range of
offsets in its queue's log: not a file, a process or a replica set of its own.

**Two limits.** A hot partition stays sequential by design, so twenty workers on a one-partition
queue is one worker's throughput and nineteen idle pollers: add lanes, not workers. And do not pick
a key for its cardinality. A partition per message is not a supported shape.
[The model, in one page](https://queenmq.com/use/model).

## Transactional processing

One [`POST /api/v1/transaction`](https://queenmq.com/reference/http/transaction) bundles
acknowledgements, pushes, key/value writes and timer operations into **one entry of the replicated
log**, applied whole or not at all:

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
case that *is* exactly-once end to end is an effect written as state in this broker through the
`kv` rider, where marker, effect, output and cursor advance become a single commit.
[Every rollback cause](https://queenmq.com/reference/http/transaction).

## Published benchmarks

These runs measured **Queen 1.x**, whose storage was PostgreSQL. 2.0 replaced that storage with the
replicated log, so the figures describe the 1.x engine, not 2.0; they stay published as records.

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

## One replicated log

Messaging state and application state share one log: queues, offsets, leases, deduplication, the
key/value store and timers are all state that the same entries produce, which is why a transaction
can span them. One leader orders every write, a write is answered once a majority of the voters
have it on disk, and every node applies the log to its own full copy. Three voters keep serving
through one failure, five through two ([high availability](https://queenmq.com/deploy/ha)).

The trade, plainly: every write goes through one leader and every voter holds a full copy, so one
node's disk bounds retention and more nodes buy availability, not write throughput.

## The proxy and multi-tenancy

The proxy is the tenant-facing boundary and runs inside the broker process
(`QUEEN_PROXY_EMBEDDED=true`), holding what a shared broker has no business holding: per-cluster
API keys and human logins, plan limits on rate, size and count, and usage metering
([multi-tenant](https://queenmq.com/deploy/multi-tenant)). Its own state lives in the same
replicated log, under a system tenant.

**Isolation is split across both halves on purpose.** The broker scopes queue identity natively
as `(tenant, name)` on every read and write, so two tenants owning a queue called `orders` own
different queues; the proxy is what makes that tenant identity trustworthy. Neither half is
sufficient alone ([isolation](https://queenmq.com/reference/multi-tenant/isolation)).

A **cluster** is the tenant-visible Queen, one hostname and one namespace; a **cell** is the
physical deployment it runs on. A cluster never spans two cells.

## Quick start

Docker and about two minutes. There is no database to run and no migration to apply.

```bash
docker run -d --name queen -p 6632:6632 -v queen-data:/var/lib/queen/raft ghcr.io/queen-mq/queen:latest
curl -s http://localhost:6632/health
```

The volume holds the data directory: without it the messages go with the container. Open
`http://localhost:6632` for the bundled dashboard. The queue *and* the partition are created by
this call:

```bash
curl -X POST http://localhost:6632/api/v1/push -H 'content-type: application/json' -d '{
  "items": [{ "queue": "orders", "partition": "customer-123",
              "transactionId": "order-8891-created",
              "payload": { "orderId": 8891 } }]
}'
```

`transactionId` is your idempotency key: a retry of the same push writes nothing the second time.
Full walkthrough in the [Quickstart](https://queenmq.com/start/quickstart); a three-node cluster
with the proxy in [Compose](https://queenmq.com/deploy/compose).

## Documentation

- **[The model](https://queenmq.com/use/model)**: queues, partitions, groups, offsets, leases, retention.
- **[Transactions](https://queenmq.com/reference/http/transaction)**: bundle shape, rollback causes, the exactly-once boundary.
- **[KV](https://queenmq.com/use/kv)** · **[Timers](https://queenmq.com/use/timers)** · **[Streams](https://queenmq.com/use/streams)** · **[Ephemeral](https://queenmq.com/use/ephemeral)**: beyond push and pop.
- **[Deploy](https://queenmq.com/deploy)** · [HA](https://queenmq.com/deploy/ha) · [Kubernetes](https://queenmq.com/deploy/kubernetes) · [Operations](https://queenmq.com/deploy/operations) · [Kafka](https://queenmq.com/deploy/kafka).
- **[Multi-tenant](https://queenmq.com/deploy/multi-tenant)** · [Proxy](https://queenmq.com/deploy/proxy) · [Isolation](https://queenmq.com/reference/multi-tenant/isolation).
- **[Internals](https://queenmq.com/internals)**: the replicated log, storage model, life of a push and a pop, dedup, retention.
- **[Benchmarks](https://queenmq.com/benchmarks)** · [method and rig](https://queenmq.com/benchmarks/method) · [comparison](https://queenmq.com/start/compare).
- **[HTTP reference](https://queenmq.com/reference/http)** · **[SDKs](https://queenmq.com/reference/sdk/javascript)**: routes and clients.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [contributing
guide](https://queenmq.com/internals/contributing). Benchmark claims need an archived artifact under
`benchmark-queen/`; doc pages declare the source files they are true of.

## License

Apache-2.0, see [LICENSE.md](LICENSE.md). Broker and proxy both, so the multi-tenant service is
yours to run.

---

QueenMQ is built at [Smartness](https://www.smartness.com/en).
