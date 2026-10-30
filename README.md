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

**Live demo, Codemotion week:** [milan.queenmq.com](https://milan.queenmq.com): 50,000 cars in Milan, each car its own partition, 500,000 messages a second on one Queen node.

[Documentation](https://queenmq.com) · [Five-minute guide](https://queenmq.com/#guide) · [The little book (PDF)](https://queenmq.com/book/) · [Benchmarks](https://queenmq.com/benchmarks/) · [MCP for agents](https://queenmq.com/writing-applications/ai-agents/) · [Try it free on Queen Cloud](https://queenmq.cloud) · Apache-2.0 · v2.6

</div>

---

## Queen in five minutes

The short version, in JavaScript. The [full guide](https://queenmq.com/#guide) has nine steps in
JavaScript, Python, Go, Rust and PHP. Every snippet, here and on the site, is cut from
[a program that runs them all](examples/tutorials/js/00-five-minutes.mjs) and checks each one.

### 1. Run a node

```bash
docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.6.0
npm install queen-mq
```

The dashboard is at http://localhost:6632, served by the same process.

### 2. Connect, and configure a queue

Optional: the first push creates a queue with the defaults.

<!-- guide: configure -->
```js
import { Queen } from 'queen-mq'

const queen = new Queen({ url: 'http://localhost:6632' })

await queen.queue('orders')
  .namespace('shop')              // groups queues in the dashboard
  .config({
    leaseTime: 30,                // seconds a worker holds a message before it is redelivered
    retryLimit: 3,                // failures before the message goes to the dead-letter queue
    retentionEnabled: true,       // retention runs only when this is on
    retentionSeconds: 86400,      // then messages older than a day are removed
  })
  .create()
```

### 3. Push to a partition

In Kafka you choose a number of partitions and hash keys onto them, so a slow customer holds up
every customer that shares its partition. Here a partition is one entity: a customer, an order, a
conversation. The push that names it creates it, order holds inside it, and one queue has held ten
million.

<!-- guide: push -->
```js
await queen.queue('orders')
  .partition('customer-42')       // one ordered partition per customer, created by this push
  .push([{
    transactionId: 'order-1001',  // the dedup key: pushing the same id again is a no-op
    data: { orderId: 1001, customer: 42, total: 59.9 },
  }])
```

### 4. Consume

Without a consumer group, each message goes to one worker. Consumer groups, leases and every pop
option are in the [full guide](https://queenmq.com/#guide-consume).

<!-- guide: consume -->
```js
await queen.queue('orders')
  .each()                         // the handler gets one message at a time
  .limit(1)                       // stop after one message; a worker leaves this out
  .consume(async (msg) => {       // acked when the handler returns, retried if it throws
    console.log(msg.data)
  })
```

### 5. One step, one commit

The ack, the order's new state, the next event and tomorrow's reminder commit as one log entry, or
none of them does. That one call replaces the outbox table, the idempotency table and the cron job.

<!-- guide: transaction -->
```js
await queen.queue('orders')
  .group('fulfillment')
  .subscriptionMode('all')
  .autoAck(false)
  .each()
  .limit(1)
  .consume(async (msg) => {
    const { orderId, customer } = msg.data
    await queen.transaction()
      .ack(msg, 'completed', { consumerGroup: 'fulfillment' })  // the input is done
      .kv.put('orders', `order-${orderId}`, { status: 'paid' }, { ttl: '7d' })
      .queue('shipments').partition(msg.partition)              // the next event
      .push([{ transactionId: `ship-${orderId}`, data: { orderId } }])
      .timer('reminders').key(`review-${orderId}`).delay('24h') // a message tomorrow
      .payload({ orderId, customer }).schedule()
      .commit()
  })
```

### 6. Read it again

Consuming does not delete: the queue is a log, kept for its retention. A new group can start from
a point in time and read the same orders again.

<!-- guide: replay -->
```js
await queen.queue('orders')
  .group('audit')                           // a new group, with its own cursor
  .subscriptionFrom('2026-10-22T00:00:00Z') // everything pushed from this time on
  .each()
  .limit(1)
  .consume(async (msg) => {
    console.log('again:', msg.data)
  })
```

Or read a partition by offset, with no group, lease or ack:

<!-- guide: fetch -->
```js
const read = await queen.queue('orders').fetch(['customer-42']) // no group, lease or ack
for (const record of read.get('customer-42').records) {
  console.log(record.offset, record.payload)
}
```

The [full guide](https://queenmq.com/#guide) goes on with locks, stream windows and request/reply
over in-memory queues, in every language.

## Numbers

Three GCP nodes, 24 vCPU each, local NVMe. A write is confirmed only after two nodes have written
it to disk (fsync). Kafka, Pulsar and Redpanda ran on the same machines.

- [**3M msg/s**](https://queenmq.com/benchmarks/queen-vs-kafka/#3000000-msgs-of-256-byte-messages) in and out of one queue, from 10 to 10M partitions, p99 15-25 ms. Kafka and Redpanda could not keep up at 50,000 partitions, Pulsar at 100,000.
- [**1M msg/s**](https://queenmq.com/benchmarks/queen-vs-kafka/#1000000-msgs-of-1-kb-messages) of 1 KB messages (about 1 GB/s), p99 16-22 ms.
- [**5M msg/s**](https://queenmq.com/benchmarks/queen-vs-kafka/#the-ceiling-at-1000-partitions) on one queue at most.
- **100 GB of lag drained in 32 seconds** (3.4 GB/s).
- [**28 s**](https://queenmq.com/manage/recovery/) for a node away five minutes to rejoin a cluster holding 521 GB a node: it gets only what it missed, a 1.1 GB delta, and a restart reads hash sidecars instead of its logs (20 s where it took 171 to 201 s).
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
- [257 Jepsen tests](https://queenmq.com/benchmarks/jepsen/) on 2.6, all passed. No acknowledged message lost, no lease held twice.
- Written in Rust. Apache-2.0.

## Documentation

- [The little book of QueenMQ](https://queenmq.com/book/): the whole story in one free PDF, from what Queen is to the bytes on disk.
- [The model](https://queenmq.com/introduction/model/): partitions, leases, transactions and a plain HTTP interface; [guarantees](https://queenmq.com/introduction/guarantees/).
- [Writing applications](https://queenmq.com/writing-applications/): queues, push, pop, acks, retries, transactions, KV, locks, timers, ephemeral queues and streams, every example in six languages.
- [Patterns](https://queenmq.com/patterns/): work queue, pub/sub, request/reply, workflows, windowed aggregation, rate limiting, delayed messages and more.
- [Integrations](https://queenmq.com/integrations/): Kafka clients, S3, PostgreSQL, Laravel.
- [Manage clusters](https://queenmq.com/manage/): [Docker](https://queenmq.com/manage/docker/), [Kubernetes](https://queenmq.com/manage/kubernetes/), [upgrades and monitoring](https://queenmq.com/manage/upgrades-monitoring/), [recovery](https://queenmq.com/manage/recovery/), [tenants](https://queenmq.com/manage/tenants/).
- [Architecture](https://queenmq.com/architecture/): one leader per tenant, the write path, Raft, storage down to the bytes.
- [Compared to Kafka, SQS and Temporal](https://queenmq.com/introduction/compare/).

Using a coding agent? Add the MCP server:

```bash
claude mcp add --transport http queen https://queenmq.com/mcp
```

## License

Apache-2.0, see [LICENSE.md](LICENSE.md).

---

**Nothing happens halfway.**

Queen MQ is built at [Smartness](https://www.smartness.com/en).
