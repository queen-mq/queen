# Queen MQ 2.0

Queen MQ 2.0 is a transactional event broker: one binary per node,
state in a raft-replicated log on local disk, no external database.
Apps speak JSON over HTTP on port 6632 (SDKs: JS, Python, Go, Rust, PHP, C++);
Kafka clients can use the built-in facade.
From 2.0.0-beta.7, in-broker connectors stream PostgreSQL 17+ tables into queues and queues into tables,
configured on the broker (`PUT /api/v1/connectors/:name`), not in app code.

These rules describe Queen 2.0 (broker {{broker}}).
Code from older posts, 1.x docs or Kafka habits is likely wrong here.
Run `ghcr.io/queen-mq/queen:{{broker}}`, published for linux/amd64 and linux/arm64; `latest` is the current release.

## Model the app

- Model each entity (customer, order) as one partition, named by your app on push
  (`partition: "order-9137"`). The first push creates queue and partition. No partition count to choose.
- Order holds only inside a partition, and a group works a partition with one worker at a time:
  parallelism comes from many entities with work.
- Model each step as one transaction (`POST /api/v1/transaction`, `queen.transaction()`):
  ack the input, push the next events, write KV state, schedule or cancel timers. One log entry, all or nothing.
  It cannot read a value and branch on it: put conditions on KV writes (`expect` with `required: true`).
- Keep workers stateless and entity state in KV, written in the step's transaction
  (KV has no queries; searchable records stay in your database).
- Use timers, not cron, for waiting: keyed by queue and `timerKey`,
  replaced by scheduling again, fired as a real message. Give a timer the entity's partition to keep order.

## Consuming

- No consumer group means queue mode (`__QUEUE_MODE__`): each message goes to one worker.
- Workers sharing a group compete; each group gets every message (fan-out).
- A group's start is fixed at its first pop: `new` (the default), `all` or `subscriptionFrom` a time.
  Move an existing group with a seek.
- A pop leases a partition's batch to one worker for `leaseTime` (60 s on a queue created by push).
  Renew long work, or it is redelivered.
- Delivery is at-least-once. Ack `completed` moves the cursor;
  `failed` spends a retry (past `retryLimit`, 3, it is dead-lettered);
  `retry` redelivers without spending; `dlq` dead-letters now.

## Idempotency

- Give every pushed message a deterministic `transactionId` derived from the work (`receipt-${orderId}`), never a fresh UUID.
  A repeat in the same partition within `dedupWindowSeconds` (3600 s) writes nothing and answers `duplicate`.
- Gate every step with a deterministic push id or `once(ns, key, { ttl })`
  (JS, Python; elsewhere KV `putIfAbsent` with `required: true`): a retry of a committed step rolls back whole.
- A 503 or timeout leaves the outcome unknown, and SDKs retry on their own: make every write safe to repeat.
- External calls are outside any commit: give them an idempotency key from the message.

## Request/reply

Use ephemeral queues (`/api/v1/ephemeral/*`): in memory, outside the raft log, created on first use.
Every node serves every queue: each partition has one owner node and the others forward to it.
The requester mints a unique inbox, sends it as `replyTo` and long-polls it with a timeout.
SIGTERM hands partitions over; a crash loses the dead node's share. No transactions, DLQ or replay.

## Traps that do the most damage

1. A new group skips everything pushed before its first pop unless it asks for `subscriptionMode('all')`.
2. Send the consumer group and the pop's `leaseId` on every ack:
   without it, the ack targets queue mode and the message comes back.
3. Push answers HTTP 201 even when items are refused: check each item's `status` (`queued`, `duplicate`, `error`).
4. A rollback answers HTTP 200 with `success: false`, and SDKs return (not throw) a lost `once` gate.
   The 2.x reason is `rejected_ack`, never `ack_rejected`.
5. Ack one pop's messages per transaction, or put `leaseId` on every ack:
   an ack without its own lease is fenced only if the call names one lease.
6. Only a `failed` ack spends retries: a message that crashes its worker or outlives its lease
   returns forever, never reaching the DLQ.
7. Every KV write needs `ttlSeconds` or `forever: true`; read `applied` on the result.
8. `retryDelay`, `priority`, `ttl`, `maxSize` and `minPopWaitTime` are stored and ignored.
   For a retry backoff, ack and schedule a timer in one transaction.
