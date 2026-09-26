<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/queen-tile-light.svg">
  <img src="assets/queen-tile.svg" alt="" width="76" height="76">
</picture>

# Queen MQ

**High-performance transactional message broker**

Queen is a distributed message broker written in **Rust**: its
defining abstraction is one logical ordered partition per application entity (a customer, an
account, a conversation, a device, a workflow, a session, a job), created by the first push that
names it, never provisioned in advance, with a set of transactional features to make your job easier.

[Documentation](https://queenmq.com) · [Benchmarks](https://queenmq.com/benchmarks) · [Quickstart](https://queenmq.com/start/quickstart) · [Try it](https://queenmq.cloud) · Apache-2.0 · v2.0.0


Queen speaks HTTP, but is also protocol-compatible with **Kafka**.

[Free on Queen Cloud](https://queenmq.cloud). The same broker, hosted.
</div>

---

## The problem Queen solves

Queen gives you the capability to have tons of ordered FIFO partitions, per entity, in order to solve HOL.
It can manage 1M partitions easily, and those partitions are dynamic, created at first push with your key/entity.

Also, Queen is engineered in order to provide transactional features that allow you to offload to it a lot of complex application logic: with one unique Queen transaction is possible to: ACK (or NACK) a message, push to other queues, update counters in its own KV storage, set timers.

Queen is multi tenant, and can spread its tenants over several Raft groups, each with its own leader. It's architeture is engineered to be almost insensible to partition count and make transactions easy and fast.

Queen is running in production at [Smartness](https://smartness.com), and it is tested with Jepsen.

---

## Features

Main features:

- **Ordered partitions, created on demand.** A partition exists as soon as a push names it, and
ordering holds inside it.

- **Dedup at push.** 

- **Consumer group.** Fan out jobs to multiple consumer group, seek by timestamp.

- **Exactly once inside it's transactional features** Deduplication at push plus `ack + kv + push + timers` commit together or not at all.

- **State and time in the broker.** [`queen.kv`](https://queenmq.com/use/kv), a transactional
key/value store with optimistic locking and an expiry on every write ·
[timers](https://queenmq.com/use/timers) that schedule a real message into a real queue and stay
cancellable until they fire · delayed delivery · window-buffer debounce · conflation, last-value
delivery per partition.

- **Stream processing in your process.** An [operator chain](https://queenmq.com/use/streams) with
state in the same brokers: no job manager, no changelog topic, no state store to deploy. Four
window types (tumbling, sliding, session, cron), map/filter/aggregate, event time with watermarks,
per-message gating.

- **Ephemeral queues, no disk in the path.** An
[in-memory class](https://queenmq.com/use/ephemeral) for request/reply, signalling, presence
fan-out and cache invalidation: the shapes that should not pay for replay and retention.

- **Kafka clients connect directly.** Since 1.4.0 Queen speaks Kafka wire protocols, so an
existing client moves over by changing its connection URL.

- **One binary, no sidecars, no database.** curl is a first-class client · six SDKs (JavaScript,
Python, Go, Rust, C++, PHP/Laravel) plus `queenctl` · a dashboard served by the same binary on the
same port · Prometheus metrics · JWT/JWKS auth · payload encryption · multi-tenant · Raft
replication over three or five nodes.


## Published benchmarks



**What these do not establish.** Single-shape runs say nothing about *your* throughput, latency,
sizing, disk or partition distribution: those follow from your workload, payloads and hardware. Read
[method and rig](https://queenmq.com/benchmarks/method) before quoting a number.

## Quick start

Docker and about two minutes:

```bash
docker run -d -p 6632:6632 ghcr.io/queen-mq/queen:latest
```

```bash
curl -X POST http://localhost:6632/api/v1/push -H 'content-type: application/json' -d '{
  "items": [{ "queue": "orders", "partition": "customer-123",
              "transactionId": "order-8891-created",
              "payload": { "orderId": 8891 } }]
}'
```

`transactionId` is your idempotency key: a retry of the same push writes nothing the second time.
Full walkthrough in the [Quickstart](https://queenmq.com/start/quickstart).

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
