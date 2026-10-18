<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo/queen-tagline-on-dark.svg">
  <img src="assets/logo/queen-tagline.svg" alt="Queen, message queue" width="340">
</picture>

## Partitions are for ordering. Not for scaling.

**A new paradigm for message brokers.**

Queen is an open-source distributed message broker, able to sustain millions of messages per
second. Every customer, order or conversation gets its own ordered partition. Every step a worker
takes is saved as one commit, or not at all. One binary, no database, and Kafka clients work with
it out of the box.

[Documentation](https://queenmq.com) · [Quickstart](https://queenmq.com/start/quickstart/) · [Benchmarks](https://queenmq.com/benchmarks/) · [MCP for agents](https://queenmq.com/start/ai-agents/) · [Try it free on Queen Cloud](https://queenmq.cloud) · Apache-2.0 · v2.5

</div>

---

## The old way

In Kafka, Pulsar or Redpanda, partitions are how you scale. You pick a number, say 64, and hash
your keys onto it. Order is a side effect.

So one slow customer blocks every other customer on the same partition. When you need more
partitions, keys move, and ordering is lost during the change.

The work is spread out too. To handle one event you ack it on the broker, update the database,
write to an outbox, schedule a job and save an idempotency key. Four systems, four commits. A crash
in between leaves the step half done.

## The Queen way

**One partition per entity.** Push to `customer-123` and the partition exists. No count to choose.
A partition uses about 1 KB of memory, so one queue can hold ten million. A slow customer only
slows itself down.

**One commit per step.** In one call, a worker acks the event, saves state, pushes the next events
and sets a timer. Queen writes it all as one log entry. If any part fails, nothing is written. A
retry with the same id is a no-op.

**The queue is a log.** Messages stay after they are processed. You get leases, retries and a
dead-letter queue, plus replay, fan-out to many groups and seek by time.

## In code

```js
// Push: the first message for a customer creates its partition
await queen.queue('orders').partition(customerId)
  .push([{ transactionId: `order-${orderId}`, data: { orderId, customerId } }])

// Consume: each step is one commit
await queen.queue('orders').group('billing').autoAck(false).each()
  .consume(async (message) => {
    const { orderId, customerId } = message.data
    await queen.transaction()
      .ack(message, 'completed', { consumerGroup: 'billing' })
      .kv.put('orders', `order-${orderId}`, { status: 'paid' })
      .queue('receipts').partition(customerId)
        .push([{ transactionId: `receipt-${orderId}`, data: { orderId } }])
      .timer('reminders').key(`order-${orderId}`).delay('24h')
        .payload({ orderId }).schedule()
      .commit()
  })
```

The ack, the new state, the receipt and the reminder are one log entry. Over HTTP:
`POST /api/v1/transaction`. [Transactions](https://queenmq.com/concepts/transactions/)

## What you can remove

- **The outbox table.** The next event is pushed in the same commit.
- **The idempotency table.** A `transactionId` makes a retry a no-op.
- **The cron job.** Timers fire as messages in the entity's partition.
- **Partition planning.** Partitions create themselves.
- **Consumer rebalances.** Workers just take the next entity that has work.

## Also included

- [KV store](https://queenmq.com/concepts/kv/) and [timers](https://queenmq.com/concepts/timers/), in the same commit
- [Stream windows](https://queenmq.com/guides/streams/) and [in-memory queues](https://queenmq.com/guides/ephemeral/) for request/reply
- [Kafka protocol](https://queenmq.com/guides/kafka/), [PostgreSQL](https://queenmq.com/guides/postgres/) in and out, [S3](https://queenmq.com/guides/s3/) export, all inside the broker
- HTTP API, [SDKs](https://queenmq.com/start/clients/) for JS, Python, Go, Rust, C++ and PHP (with Laravel), an [MCP server](https://queenmq.com/start/ai-agents/) for coding agents
- Dashboard, Prometheus metrics, auth, encryption, [multi-tenant proxy](https://queenmq.com/operate/tenants/), [Raft on 3 or 5 nodes](https://queenmq.com/operate/cluster/)

## Numbers

Three GCP nodes, 24 vCPU each, local NVMe. A write is confirmed only after two nodes have written
it to disk (fsync). Kafka, Pulsar and Redpanda ran on the same machines.

- [**3M msg/s**](https://queenmq.com/benchmarks/queen-vs-kafka/#3000000-msgs-of-256-byte-messages) in and out of one queue, from 10 to 10M partitions, p99 15-25 ms. Kafka and Redpanda could not keep up at 50,000 partitions, Pulsar at 100,000.
- [**1M msg/s**](https://queenmq.com/benchmarks/queen-vs-kafka/#1000000-msgs-of-1-kb-messages) of 1 KB messages (about 1 GB/s), p99 16-22 ms.
- [**5M msg/s**](https://queenmq.com/benchmarks/queen-vs-kafka/#the-ceiling-at-1000-partitions) on one queue at most.
- **100 GB of lag drained in 32 seconds** (3.4 GB/s).
- [**4 ms**](https://queenmq.com/benchmarks/transactions/) p99 for a 10-message transaction, at 6,000 transactions per second.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/bench-queen-e2e-dark.svg">
  <img src="assets/bench-queen-e2e-light.svg" alt="Queen 2.4 end to end p99 as one queue grows from 10 to 10 million partitions on three GCP nodes with local NVMe: 17 to 29 ms at 3M msg/s of 256-byte messages, 17 to 25 ms at 1M msg/s of 1 KB messages" width="820">
</picture>

<sub>Queen 2.4 on three GCP c4-standard-24-lssd nodes with local NVMe, 2026-10-15: end to end p99 at 3M msg/s of
256-byte messages and at 1M msg/s of 1 KB messages, the worst load process in seconds 30-60 of 70 (at 3M and
1,000 partitions, the median of five runs). 2.3.0's 3M line read 47 to 78 ms on the same axes.</sub>

Full details: [queenmq.com/benchmarks](https://queenmq.com/benchmarks/)

## Proven

- In production at [Smartness](https://www.smartness.com/en).
- [257 Jepsen tests](https://queenmq.com/benchmarks/jepsen/) on 2.4, all passed. No acknowledged message lost, no lease held twice.
- Written in Rust. Apache-2.0.

## Try it

```bash
docker run -p 6632:6632 ghcr.io/queen-mq/queen:latest
```

Push your first message:

```bash
curl -X POST http://localhost:6632/api/v1/push -H 'content-type: application/json' -d '{
  "items": [{ "queue": "orders", "partition": "customer-123",
              "transactionId": "order-8891-created",
              "payload": { "orderId": 8891 } }]
}'
```

Open http://localhost:6632 to see it in the dashboard. The
[Quickstart](https://queenmq.com/start/quickstart/) takes it from there.

Using a coding agent? Add the MCP server:

```bash
claude mcp add --transport http queen https://queenmq.com/mcp
```

## Documentation

- [The model](https://queenmq.com/concepts/): partitions, consumer groups, transactions, KV, timers, dedup, guarantees.
- [Guides](https://queenmq.com/guides/) · [Examples](https://queenmq.com/examples/): exactly-once, state machines, streams, webhooks, Kafka, PostgreSQL, S3, Laravel.
- [Operate](https://queenmq.com/operate/): a [cluster](https://queenmq.com/operate/cluster/), [Kubernetes](https://queenmq.com/operate/kubernetes/), [monitoring](https://queenmq.com/operate/monitoring/), [recovery](https://queenmq.com/operate/recovery/), [tenants](https://queenmq.com/operate/tenants/).
- [Reference](https://queenmq.com/reference/): the HTTP API, configuration, errors, limits.
- [Internals](https://queenmq.com/internals/): the replicated log, storage, the life of a step.
- [Compared to Kafka, SQS and Temporal](https://queenmq.com/start/compare/).

## License

Apache-2.0, see [LICENSE.md](LICENSE.md).

---

**Nothing happens halfway.**

Queen MQ is built at [Smartness](https://www.smartness.com/en).
