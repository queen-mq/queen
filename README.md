<div align="center">

<img src="assets/queen-badge.svg" alt="" width="120" height="120">

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
It can manage 10M partitions easily, and those partitions are dynamic, created at first push with your key/entity.

Also, Queen is engineered in order to provide transactional features that allow you to offload to it a lot of complex application logic: with one unique Queen transaction is possible to: ACK (or NACK) a message, push to other queues, update counters in its own KV storage, set timers. It also have Postgres source and sink connetors to provide exaclty once operations between Queen and Postgres.

Queen is multi tenant, and can spread its tenants over several Raft groups, each with its own leader. It's architeture is engineered to be almost insensible to partition count and make transactions easy and fast [Benchmark](https://queenmq.com/benchmarks/partitions/).

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

- **Postgres source and sink** Exaclty once processing between Queen and Postgres database.

- **One binary, no sidecars, no database.** curl is a first-class client · six SDKs (JavaScript,
Python, Go, Rust, C++, PHP/Laravel) plus `queenctl` · a dashboard served by the same binary on the
same port · Prometheus metrics · JWT/JWKS auth · payload encryption · multi-tenant · Raft
replication over three or five nodes.


## Published benchmarks

Three 16-vCPU nodes, every write fsynced on two of them before the answer, with Kafka 4.3.1,
Redpanda 26.2.3 and Pulsar 4.2.4 on the same machines. Each row links to the run and its conditions.

| | Result |
|---|---|
| [Partitions](https://queenmq.com/benchmarks/partitions/) | 1,000,000 msg/s in and out of one queue at every count from 200 to **10,000,000 partitions**, e2e p99 103 to 163 ms. Kafka fell behind at 100,000 partitions, Redpanda and Pulsar at 50,000. |
| [Transactions](https://queenmq.com/benchmarks/transactions/) | 10 messages per transaction at 9,000 msg/s: commit p99 **4 ms**, e2e p99 19 ms (Kafka 34 and 51 ms, Pulsar 32 and 50 ms). |
| [Kafka clients](https://queenmq.com/benchmarks/kafka-clients/) | franz-go at 1,000,000 msg/s through Queen's Kafka port, e2e p99 91 ms. |
| [Soak](https://queenmq.com/benchmarks/soak/) | 500,000 msg/s for 3 h 23 min over 1,000,000 partitions, through a kill -9 of the leader and of a follower: e2e p99 86 ms in the median window. |
| [Jepsen](https://queenmq.com/benchmarks/jepsen/) | 65 of 65 tests valid: no acknowledged message lost, no lease held twice, transactions atomic. |
| [Laravel](https://queenmq.com/benchmarks/laravel/) | 2,753 jobs/s with 32 workers on one node, against 1,124 for Horizon on Redis. |

Where Queen is behind: with few partitions and very high rates, Kafka carries more (4M msg/s at
p99 75 ms, while one Queen queue tops out near 1.5M, set by its leader) and spends less CPU per
message. [Method and rig](https://queenmq.com/benchmarks/methodology/).

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
