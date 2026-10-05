# Queen PHP client reference

This page is the reference for the plain PHP client in `queen-mq/php-client` 2.1.0: namespace
`Queen\`, no framework. It requires PHP 8.3 or later, `guzzlehttp/guzzle` 7 and `ramsey/uuid` 4.
Calls are synchronous. A few calls also have a detached or promise-returning form, listed where
they apply.

## Contents

- [Laravel](#laravel)
- [Connect](#connect)
- [Queues](#queues)
- [Push](#push)
- [Buffered push](#buffered-push)
- [Pop](#pop)
- [Consume](#consume)
- [The KafkaConsumer-style consumer](#the-kafkaconsumer-style-consumer)
- [Multi-partition pop and autopilot](#multi-partition-pop-and-autopilot)
- [Conflation](#conflation)
- [Wildcard consumption](#wildcard-consumption)
- [Ack, nack and lease renewal](#ack-nack-and-lease-renewal)
- [Detached requests](#detached-requests)
- [Transactions](#transactions)
- [Key/value state](#keyvalue-state)
- [Timers](#timers)
- [Dead-letter queue](#dead-letter-queue)
- [Ephemeral queues](#ephemeral-queues)
- [Admin API](#admin-api)
- [Tracing](#tracing)
- [HTTP transports](#http-transports)
- [Retries, failover and 429](#retries-failover-and-429)
- [Errors and exceptions](#errors-and-exceptions)
- [Not in this client](#not-in-this-client)

## Laravel

The same package is a Laravel queue driver, a worker supervisor and a dashboard. None of that is on
this page.

- Guide: <https://queenmq.com/guides/laravel/>
- `config/queen.php` and its environment variables: <https://queenmq.com/guides/laravel/configuration/>

The `queen:consume` Artisan command is part of the Laravel integration
(`src/Laravel/Commands/ConsumeCommand.php`); see the [Laravel guide](https://queenmq.com/guides/laravel/).

## Connect

`new Queen(...)` builds a client. One client holds one HTTP client, one set of push buffers and the
lazily created `admin()`, `kv()`, `timers()` and `ephemeral()` facades.

```php
use Queen\Queen;

// One broker.
$queen = new Queen('http://localhost:6632');

// Several brokers: a list turns on load balancing and failover.
$queen = new Queen(['http://queen-a:6632', 'http://queen-b:6632']);

// A config array.
$queen = new Queen([
    'urls' => ['http://queen-a:6632', 'http://queen-b:6632'],
    'bearerToken' => getenv('QUEEN_TOKEN') ?: null,
    'timeoutMillis' => 10_000,
    'retry429' => ['maxAttempts' => 20, 'capMs' => 10_000],
]);
```

The constructor takes `string|array $config`:

- A string is one URL.
- An array with a key `0` is a list of URLs.
- Any other array is a config array, merged over `Queen\Support\Defaults::CLIENT_DEFAULTS`. It
  needs `url` or `urls`, else it throws `InvalidArgumentException('Must provide urls or url in configuration')`.

| Key | Default | Effect |
| --- | --- | --- |
| `url` | none | One broker URL. |
| `urls` | none | Broker URLs. More than one entry builds the load balancer. |
| `timeoutMillis` | `30000` | Total deadline of one request. |
| `retryAttempts` | `3` | Attempts for a network failure or a 5xx, without failover. |
| `retryDelayMillis` | `1000` | Delay before the second attempt; it doubles per attempt. |
| `loadBalancingStrategy` | `'affinity'` | `'affinity'`, `'round-robin'` or `'session'`. |
| `affinityHashRing` | `128` | Virtual nodes per backend on the affinity hash ring. |
| `enableFailover` | `true` | Try the next backend after a network failure or a 5xx. |
| `healthRetryAfterMillis` | `5000` | How long a failed backend stays out of rotation. |
| `bearerToken` | `null` | Sent as `Authorization: Bearer <token>`. |
| `headers` | `[]` | Extra headers on every request. |
| `retry429` | `[]` | HTTP 429 backoff: `maxAttempts`, `baseMs`, `capMs`. See [Retries](#retries-failover-and-429). |

The load-balancing keys have no effect with one URL. The constructor validates the config and
throws `InvalidArgumentException` on the first problem:

- Every URL is `http://` or `https://` with a host, and has no user, password, query or fragment.
  A trailing `/` is removed.
- `timeoutMillis`, `retryAttempts` and `affinityHashRing` are integers of at least 1;
  `retryDelayMillis` and `healthRetryAfterMillis` are integers of at least 0. A numeric string is
  accepted.
- `enableFailover` is a boolean.
- `bearerToken` is `null` or a non-empty string without spaces or control characters. It replaces
  an `Authorization` entry in `headers`.
- Header names are HTTP tokens. A value is a scalar or a list of scalars, without CR or LF.
- `retry429` accepts only `maxAttempts`, `baseMs` and `capMs`, integers of at least 0, and
  `capMs` is at most `300000`.

### `Queen` methods

| Method | Returns |
| --- | --- |
| `queue(?string $name = null)` | `Builders\QueueBuilder` |
| `transaction()` | `Builders\TransactionBuilder` |
| `admin()` | `Admin`, one per client |
| `kv()` | `Kv`, one per client |
| `timers()` | `Timers`, one per client |
| `ephemeral()` | `Ephemeral`, one per client |
| `ack(array\|string $message, bool\|string $status = true, array $context = [])` | `array` |
| `ackDetached(array $message, bool\|string $status = true, array $context = [])` | `PromiseInterface` |
| `settleAck(PromiseInterface $ack, ?int $timeoutMillis = null)` | `array` |
| `renew(string\|array $messageOrLeaseId, ?int $seconds = null)` | `array` |
| `flushAllBuffers()` | `void` |
| `getBufferStats()` | `array` |
| `deleteConsumerGroup(string $consumerGroup, bool $deleteMetadata = true)` | `mixed` |
| `updateConsumerGroupTimestamp(string $consumerGroup, string $timestamp)` | `mixed` |
| `autopilotOff()` | `bool`, see [autopilot](#multi-partition-pop-and-autopilot) |
| `close()` | `void` |

`deleteConsumerGroup()` calls `DELETE /api/v1/consumer-groups/:group?deleteMetadata=`.
`updateConsumerGroupTimestamp()` posts `{"subscriptionTimestamp": ...}` to
`/api/v1/consumer-groups/:group/subscription`.

`close()` flushes every push buffer and ignores a flush failure, then empties the buffers. Messages
that the flush could not send are discarded. The client installs no shutdown handler, so a
buffered message that is not flushed before the process ends is never sent. When that matters,
call `flushAllBuffers()` and let its exception propagate.

## Queues

`$queen->queue($name)` returns a `QueueBuilder`. Its setters return the builder, so one chain
addresses a queue, a partition and a consumer group, and then ends in an operation.

```php
$queen->queue('orders')
    ->namespace('shop')
    ->task('checkout')
    ->config(['leaseTime' => 60, 'retryLimit' => 5, 'dedupWindowSeconds' => 86400])
    ->create()
    ->execute();

$queen->queue('orders')->delete()->execute();
```

| Method | Default | Effect |
| --- | --- | --- |
| `partition(string $name)` | `'Default'` | The ordered lane for push and pop. |
| `namespace(string $name)` | none | A label on `create()`, and a pop filter. |
| `task(string $name)` | none | A second label on `create()`, and a pop filter. |
| `group(string $name)` | none | Consumer group. None means queue mode (`__QUEUE_MODE__`). |
| `config(array $options)` | none | Queue options for `create()`. |
| `create()` | | `POST /api/v1/configure`, returns an `OperationBuilder`. |
| `delete()` | | `DELETE /api/v1/resources/queues/:queue`, returns an `OperationBuilder`. |

`OperationBuilder` has `onSuccess(Closure $cb)`, `onError(Closure $cb)` and `execute(): mixed`.
`execute()` returns the decoded answer. A failure, or an answer with an `error` key, throws; with
`onError` set, it calls `onError($error)` and returns `['success' => false, 'error' => ...]`.

`delete()` throws `RuntimeException` when the builder has no queue name.

### What `create()` sends

`config()` merges your options over `Defaults::QUEUE_DEFAULTS`, and `create()` without `config()`
sends that array alone. These nine keys therefore always travel:

| Key | Client default |
| --- | --- |
| `leaseTime` | `300` |
| `retryLimit` | `3` |
| `priority` | `0` |
| `delayedProcessing` | `0` |
| `windowBuffer` | `0` |
| `maxSize` | `0` |
| `retentionSeconds` | `0` |
| `completedRetentionSeconds` | `0` |
| `encryptionEnabled` | `false` |

`/configure` merges: an option that the body leaves out keeps its stored value. This client sends
no `mode`, so it always merges. Because the nine keys above always travel, `create()` on an
existing queue resets each of them that you do not name to the client default. For example, it
turns encryption off on an encrypted queue. Other keys that you pass to `config()` are sent as
they are. The options and their effects are on
[queue options](https://queenmq.com/concepts/partitions/#queue-options).

## Push

`push()` writes messages to the builder's queue and partition, in order. With `buffer()` set it
adds them to a client-side buffer instead; see [Buffered push](#buffered-push).

```php
$queen->queue('orders')
    ->partition('customer-42')
    ->push([['data' => ['orderId' => 8891]]])
    ->execute();

$results = $queen->queue('orders')
    ->partition('customer-42')
    ->push([
        ['data' => ['orderId' => 8892], 'transactionId' => 'order-8892'],
        ['data' => ['orderId' => 8893], 'partition' => 'customer-7'],
    ])
    ->onDuplicate(function (array $items, \Throwable $e): void {
        // These transactionIds are already in the partition's dedup window.
    })
    ->execute();

foreach ($results as $item) {
    if ($item['status'] === 'error') {
        // Not stored, and no callback ran for it.
    }
}
```

`push(array $payload): PushBuilder` takes one item array or a list of them. It throws
`RuntimeException` when the builder has no queue name.

| Item key | Effect |
| --- | --- |
| `data` | The payload. |
| `payload` | The payload, when `data` is absent. |
| `transactionId` | The dedup key. Absent, the client mints a UUIDv7 (`Queen\Support\Uuid::v7()`). |
| `partition` | Overrides the builder's partition for this item. |
| `traceId` | Sent as given; see [Tracing](#tracing). |

An item with neither `data` nor `payload` is the payload itself, other keys included. Use `data`
when an item also sets `transactionId`, `partition` or `traceId`.

`PushBuilder` has `onSuccess(Closure)`, `onError(Closure)`, `onDuplicate(Closure)` and
`execute(): mixed`. A direct push posts to `/api/v1/push` and returns the broker's per-item list.
The broker answers each item `queued`, `duplicate` or `error`:

| Item `status` | What the builder does |
| --- | --- |
| `queued` | Passes it to `onSuccess($items)`. |
| `duplicate` | Passes it to `onDuplicate($items, $error)`. |
| `failed` | Passes it to `onError($items, $error)`, or throws `RuntimeException` without `onError`. |
| `error` | Nothing: no callback, no exception. Read `status` in the returned list. |

A request that fails as a whole calls `onError($items, $error)` and returns `null`, or throws
without `onError`.

## Buffered push

`buffer()` turns `push()->execute()` into an enqueue into a bounded client-side buffer, which posts
the messages in batches.

```php
$orders = $queen->queue('orders')
    ->partition('customer-42')
    ->buffer(['messageCount' => 500, 'timeMillis' => 200]);

foreach ($events as $event) {
    $orders->push([['data' => $event]])->execute(); // ['buffered' => true, 'count' => 1]
}

$orders->flushBuffer();    // this queue/partition
$queen->flushAllBuffers(); // every buffer of the client
```

| Option | Default | Effect |
| --- | --- | --- |
| `messageCount` | `100` | Flush when the buffer holds this many messages; also the batch size. |
| `timeMillis` | `1000` | Flush when the oldest message is this old, checked on the next push. |
| `maxSize` | `4 * messageCount` | The bound. Never less than `messageCount`. |
| `retryDelayMillis` | `250` | Pause before a failed batch is sent again. |
| `maxWaitMillis` | `5000` | Deadline of the retry loop of one flush. |

A value of `0` or less selects the default. There is no unbounded setting.

How the buffer behaves:

- One buffer exists per `"<queue>/<partition>"`. The options of the first push to that address
  apply for the life of the buffer.
- PHP has no background timer. The `timeMillis` trigger is checked only when the next message is
  added to the same buffer. Call `flushBuffer()`, `flushAllBuffers()` or `close()` before the
  process ends.
- A flush runs inline, in the call to `execute()`. A batch whose post fails goes back to the front
  of the buffer, in order, and is sent again after `retryDelayMillis`. When the next attempt
  cannot start before `maxWaitMillis`, the flush throws the transport error with the messages
  still buffered.
- At `maxSize` messages, an add first flushes inline. If the buffer is still full after that, the
  add throws `RuntimeException` and the message is not accepted.
- `execute()` returns `['buffered' => true, 'count' => n]` and calls `onSuccess($accepted)`. On a
  failure it calls `onError($unconfirmed, $error)` and returns `null`, or throws without `onError`.
  The unconfirmed items can still be in the buffer; a second push of the same `transactionId` is
  deduplicated by the broker.
- `flushAllBuffers()` sends every batch concurrently, then retries the failed ones one buffer at a
  time, and throws the first error that remains.
- `QueueBuilder::flushBuffer()` throws `RuntimeException` when the builder has no queue name.

`getBufferStats()` returns `activeBuffers`, `totalBufferedMessages`, `oldestBufferAge` (ms) and
`flushesPerformed`.

## Pop

`pop()` makes one claim and returns the messages. It is the low-level read; `consume()` and
`getConsumer()` are loops around the same request.

```php
$messages = $queen->queue('orders')
    ->group('billing')
    ->batch(10)
    ->wait(false)
    ->pop();

foreach ($messages as $message) {
    process($message['data']);
    $queen->ack($message, true, ['group' => 'billing']);
}

$result = $queen->queue('orders')->group('billing')->popResult();
$result['messages'];  // the same list pop() returns
$result['autopilot']; // ['partitions' => int, 'batch' => int, 'waitMillis' => int] or null
```

`pop(): array` returns a list of message arrays, `[]` when nothing was available. A message carries
`id`, `transactionId`, `traceId`, `data`, `producerSub`, `createdAt`, `partitionId`, `partition`,
`leaseId`, `consumerGroup`, `deliveryAttempt` and `offset`.

`pop()` catches nothing. A 4xx, an exhausted 429 budget, a 403 or a network failure throws, so `[]`
means that no message was available.

`popResult(): array` is the same request. It returns `['messages' => array, 'autopilot' => ?array]`,
where `autopilot` is what the broker chose for this pop. It is `null` when the pop did not engage
autopilot, when the broker sent no echo, or when the answer was a bodiless `204`.

The pop route follows the builder:

| Builder | Route |
| --- | --- |
| queue and a partition other than `Default` | `GET /api/v1/pop/queue/:queue/partition/:partition` |
| queue | `GET /api/v1/pop/queue/:queue` |
| `namespace()` or `task()`, no queue | `GET /api/v1/pop` |
| none of these | throws `RuntimeException` |

### Read options

These setters apply to `pop()`, `popResult()`, `consume()` and, where noted, `getConsumer()`.

| Method | Default | Effect |
| --- | --- | --- |
| `batch(int $size)` | broker-sized | Message budget of one pop, shared by every claimed partition. `0` means unset. |
| `partitions(int $n)` | broker-sized | Claim up to N partitions per pop under one `leaseId`. |
| `autopilot(bool $enabled = true)` | on | Let the broker size the unset knobs. |
| `wait(bool $enabled)` | `true` | Long-poll until a message arrives or the timeout passes. |
| `timeoutMillis(int $millis)` | `30000` | Long-poll timeout. The HTTP deadline adds 5 s. |
| `leaseSeconds(int $seconds)` | queue's `leaseTime` | Lease of this pop, at least 1. |
| `subscriptionMode(string $mode)` | broker default | `'new'` or `'all'`, read on the group's first pop. |
| `subscriptionFrom(string $from)` | none | `'now'` or an ISO timestamp, read on the group's first pop. |
| `conflation(bool $enabled = true)` | off | Last-value delivery; see [Conflation](#conflation). |
| `autoAck(bool $enabled)` | `true` | `consume()` only: ack after the handler. See below. |

`getConsumer()` reads `leaseSeconds`, `subscriptionMode`, `subscriptionFrom`, `partitions`,
`conflation` and `autopilot`. It ignores the other setters in this table.

Keep a worker's own job timeout shorter than the lease. A job that outlives its lease is delivered
again while it still runs.

`autoAck` never reaches the broker from this client. On `consume()` it means that the client acks
after the handler returns and nacks when the handler throws. On `pop()` it has no effect:
`pop()` sends `autoAck=true` only when the value differs from the builder default, which is
`true`, so it never sends it (`QueueBuilder::popRequestPath()`). Ack popped messages yourself.

The broker semantics of these parameters are on
[pop options](https://queenmq.com/concepts/consuming/#pop-options).

## Consume

`consume()` runs a pop-handle-ack loop until a stop condition, a signal or an exception ends it.

```php
$queen->queue('orders')
    ->group('billing')
    ->concurrency(4)
    ->limit(1000)
    ->idleMillis(60_000)
    ->each()
    ->consume(function (array $message): void {
        process($message['data']); // throw to nack
    })
    ->onError(function (array $message, \Throwable $error): void {
        error_log($error->getMessage());
    })
    ->execute();
```

Without `each()` the handler receives the whole claimed batch, a list of messages, and the ack
covers the batch.

| Method | Default | Effect |
| --- | --- | --- |
| `concurrency(int $count)` | `1` | Concurrent long-polls. At least 1. |
| `limit(int $count)` | none | Stop after this many messages. |
| `idleMillis(int $millis)` | none | Stop after this long without a message. |
| `renewLease(bool $enabled, ?int $intervalMillis = null)` | off | Extend the lease while messages are handled. |
| `each()` | batch mode | Call the handler once per message. |

`consume(Closure $handler)` returns a `ConsumeBuilder` with `onSuccess(Closure)`, `onError(Closure)`
and `execute(): void`. The callbacks observe and do not change the ack: `onSuccess($msgOrMsgs)`
runs after the handler, and `onError($msgOrMsgs, $error)` runs before the exception goes back to
the loop. Do not ack in `onError` while `autoAck` is on, because the loop acks too.

How the loop behaves:

- With `autoAck` (the default) the loop calls `$queen->ack(..., true)` after the handler and
  `$queen->ack(..., false)` when it throws, with the group as context, and continues. It does not
  read the ack result. With `autoAck(false)` a handler exception ends `execute()`.
- Every message passed to the handler has a `trace` closure; see [Tracing](#tracing).
- With `wait(false)`, an empty pop sleeps for the broker's advised `waitMillis` from the autopilot
  echo, or 100 ms.
- A long-poll timeout continues the loop. A network failure (a message with `Connection refused`
  or `cURL error`) sleeps 1 s and continues. An exhausted 429 backs off and continues. Any other
  error, a 403 included, ends `execute()` with the exception.
- With `pcntl`, the loop handles `SIGINT` and `SIGTERM`: it stops after the current message or
  batch and restores the previous handlers when `execute()` returns.
- `concurrency(N)` with N above 1 sends N long-polls at once through Guzzle and handles their
  results one after the other. `limit()` then applies per poller as `ceil(limit / N)`.
- `renewLease(true, $intervalMillis)` renews only when `$intervalMillis` is set. The check runs
  before each message in `each()` mode, so it can renew between messages of a batch. In batch mode
  it runs once before the handler, when the interval has not yet passed, so it never renews. A
  renewal extends by the broker default of 60 s, and a renewal failure is ignored.

## The KafkaConsumer-style consumer

`getConsumer()` returns a `Consumer\HighLevelConsumer`, a pull loop modelled on php-rdkafka's
`KafkaConsumer`, for code that owns its loop.

```php
$consumer = $queen->queue('orders')->group('processors')->getConsumer();
$consumer->subscribe();

while (!$consumer->isClosed()) {
    $message = $consumer->consume(1000);
    if ($message === null) {
        continue;
    }

    process($message['data']);
    $consumer->ack($message);
}
```

| Method | Effect |
| --- | --- |
| `subscribe(): void` | Resolves the pop route and options. Call it first. |
| `consume(int $timeoutMs = 1000): ?array` | One message, or `null`. |
| `consumeBatch(int $timeoutMs = 1000, int $maxMessages = 10): array` | Up to `$maxMessages`, or `[]`. |
| `ack(array $message, bool $success = true): array` | `Queen::ack()` with the consumer's group. |
| `nack(array $message): array` | `ack($message, false)`. |
| `renewLease(array\|string $messageOrLeaseId, ?int $seconds = null): array` | `Queen::renew()`. |
| `isClosed(): bool` | Dispatches pending signals, then reports the state. |
| `close(): void` | Marks the consumer closed. |

How it behaves:

- `consume()` and `consumeBatch()` before `subscribe()` throw `RuntimeException`.
- Every pop long-polls for `$timeoutMs`. `consume()` sends `batch=1` and `consumeBatch()` sends
  `batch=$maxMessages`; autopilot can still choose the partition count.
- `subscribe()` installs `SIGINT` and `SIGTERM` handlers when `pcntl` is present. They mark the
  consumer closed, and they replace any handler you installed for those signals.
- A timeout or a network failure returns `null` or `[]`. `ConflationUnsupportedException` and
  every other error propagate.
- `ack()` and `nack()` accept one message or a list of messages. The consumer never acks by
  itself.
- Returned messages carry a `trace` closure.

## Multi-partition pop and autopilot

`partitions(N)` lets one pop claim up to N partitions; pop autopilot lets the broker choose
`batch` and `partitions` when you do not set them.

```php
// The broker sizes both knobs.
$queen->queue('events')->group('indexer')->consume($handler)->execute();

// Pin the sweep width; the broker sizes the batch.
$queen->queue('events')->group('indexer')->partitions(8)->consume($handler)->execute();

// The pre-autopilot request: batch 1, one partition, no autopilot parameter.
$queen->queue('events')->group('indexer')->autopilot(false)->consume($handler)->execute();
```

With `partitions(N)`, the `batch` budget is shared by every claimed partition, and every claimed
partition shares one `leaseId`, so one `renew()` extends them all.

An explicit value is never changed. What the builder sends:

| Builder | Sizing parameters sent |
| --- | --- |
| nothing set | `autopilot=true` |
| `partitions(4)` | `autopilot=true`, `partitions=4` |
| `partitions(1)` | `autopilot=true`, `partitions=1` |
| `batch(50)` | `autopilot=true`, `batch=50` |
| `batch(50)->partitions(4)` | `batch=50`, `partitions=4` |
| `autopilot(false)` | `batch=1` |
| `autopilot(false)->partitions(4)` | `batch=1`, `partitions=4` |

With autopilot off, `partitions` travels only above 1.

`QUEEN_SDK_POP_AUTOPILOT` turns autopilot off for a whole process. The values `off`, `false`, `0`,
`no` and `disabled` turn it off, case-insensitive and trimmed; any other value leaves it on. The
client reads it once, in the constructor, from `getenv()` and then from `$_ENV` and `$_SERVER`.
`Queen::autopilotOff()` reports the result. A builder's `autopilot(true)` or `autopilot(false)`
takes precedence over the variable.

Autopilot applies to queue pops. A [wildcard pop](#wildcard-consumption) does not use it, and the
broker then applies `batch` 200 and `partitions` 1 to the knobs the client leaves out
(`server/src/handlers/data.rs`, `handle_pop_discover`).

## Conflation

`conflation()` gives a consumer group last-value delivery: a pop returns only the newest visible
message of each partition, and its ack retires the messages behind it.

```php
$queen->queue('recompute')
    ->group('workers')
    ->conflation()
    ->each()
    ->consume(function (array $message): void {
        recompute($message['data']['entityId']);
    })
    ->execute();
```

- It needs a `group()`; the broker answers 400 without one.
- It is a property of the group, stored at the group's first registration. Later consumers of the
  group get the stored setting.
- The client sends `conflation=true` only when it is on, never `false`.
- The broker refuses conflation with a broker-side `autoAck`. This client never sends `autoAck`
  on a pop, and the client-side ack of `consume()` is compatible.

Every pop answer goes through `Support\ConflationGuard`:

| This consumer | The broker answer | Result |
| --- | --- | --- |
| no `conflation()` | `conflation: true` | Throws `ConflationPolicyMismatchException` before returning messages. |
| `conflation()` | `conflation: true` | Works as declared. |
| `conflation()` | `conflationConflict: true` | The stored setting wins; one `E_USER_WARNING` per queue and group per process. |
| `conflation()` | neither key | Throws `ConflationUnsupportedException`: the broker did not apply it. |

The consume loops and `HighLevelConsumer` rethrow `ConflationUnsupportedException` and stop.

## Wildcard consumption

A builder without a queue name and with `namespace()`, `task()` or both pops across every queue
with those labels, through `GET /api/v1/pop`.

```php
$queen->queue()
    ->namespace('billing')
    ->task('invoices')
    ->group('archiver')
    ->consume(function (array $messages): void {
        foreach ($messages as $message) {
            archive($message['data']);
        }
    })
    ->execute();
```

`pop()`, `popResult()`, `consume()` and `getConsumer()` all accept this form. A queue gets its
labels from `create()` with `namespace()` and `task()`, or, when a push created it, from the first
two dotted parts of its name. Ack the messages with the same group, as for a queue pop. The broker
behaviour of a discovery pop is on [pop options](https://queenmq.com/concepts/consuming/#pop-options).

## Ack, nack and lease renewal

`ack()` settles popped messages for a consumer group; a `false` status is a nack.

```php
$queen->ack($message, true, ['group' => 'billing']);                         // completed
$queen->ack($message, false, ['group' => 'billing', 'error' => 'timeout']);  // failed
$queen->ack($message, 'retry', ['group' => 'billing']);                      // sent as given
$queen->ack($messages, true, ['group' => 'billing']);                        // one batch request

// Per-message statuses in one request.
$queen->ack([
    $first + ['_status' => true],
    $second + ['_status' => 'dlq', '_error' => 'bad payload'],
], true, ['group' => 'billing']);
```

`ack(array|string $message, bool|string $status = true, array $context = []): array`

- `$message` is a message array, a transaction-id string, or a list of message arrays. A list posts
  to `/api/v1/ack/batch`, even with one element. A message array or a string posts to
  `/api/v1/ack`.
- `$status` is `true` (`completed`), `false` (`failed`), or a string sent as given: `completed`,
  `failed`, `retry` or `dlq`.
- `$context` takes `group`, `error` and `affinityKey`. Pass the `group` of the pop: without it the
  ack targets queue mode's cursor, not the group's.
- Every message needs `transactionId` (or `id`) and `partitionId`. A `leaseId` is sent when present.
- In a list, `_status` and `_error` on a message override `$status` and `$context['error']` for
  that message.

The statuses are described in
[Ack, nack and the retry budget](https://queenmq.com/concepts/consuming/#ack-nack-and-the-retry-budget).

`ack()` never throws. A missing `partitionId`, a network failure or an HTTP error returns
`['success' => false, 'error' => '...']`. An empty list returns `['processed' => 0, 'results' => []]`
without a request. Otherwise it returns `['success' => true]` merged with the broker's per-item
list, so the items are at `$result[0]`, `$result[1]` and so on. The broker answers 200 even when it
refuses an item, so the top-level `success` only says that the call completed:

```php
$result = $queen->ack($message, true, ['group' => 'billing']);

if (!$result['success'] || !($result[0]['success'] ?? false)) {
    // Not acked: for example "invalid or expired lease".
}
```

### Lease renewal

`renew(string|array $messageOrLeaseId, ?int $seconds = null): array` extends leases.

```php
$queen->renew($message);              // the broker default, 60 s
$queen->renew($message['leaseId'], 300);
$queen->renew($messages);             // one call per distinct leaseId
```

- It accepts a lease-id string, a message array, or a list of either.
- It posts to `/api/v1/lease/:leaseId/extend` once per distinct lease id. A multi-partition pop
  has one lease id for the whole batch.
- `$seconds` must be in `1..2147483647`, else it throws `InvalidArgumentException`. `null` sends no
  body, and the broker applies 60 s.
- One input returns one result, a list returns a list. A result is
  `['leaseId', 'success' => true, 'newExpiresAt']` or `['leaseId', 'success' => false, 'error']`.
- A renewal counts as successful only when the broker answers `success: true` with `renewed`
  above 0. A lease that has expired cannot be renewed.
- Without any lease id it returns `['success' => false, 'error' => 'No valid lease IDs found for renewal']`.

## Detached requests

A detached request is written to the network when the call returns, and its answer is read later.
A worker uses it to send an ack or the next pop while it runs a job.

```php
$pendingAck = $queen->ackDetached($message, true, ['group' => 'billing']);
$builder = $queen->queue('orders')->group('billing');
$nextPop = $builder->popDetached();

runJob($current);

$ack = $queen->settleAck($pendingAck);      // the ack() result shape
$messages = $builder->settlePop($nextPop);  // the pop() result shape
```

| Method | Effect |
| --- | --- |
| `Queen::ackDetached(array $message, bool\|string $status = true, array $context = [])` | Sends one ack to `/api/v1/ack`. |
| `Queen::settleAck(PromiseInterface $ack, ?int $timeoutMillis = null)` | Waits for it, by default for `timeoutMillis` of the client. |
| `QueueBuilder::popDetached()` | Sends the builder's pop. |
| `QueueBuilder::settlePop(PromiseInterface $pop, ?int $timeoutMillis = null)` | Waits for it, by default for the pop timeout plus 5 s. |

- A detached request makes one attempt against one backend, with no 429 retry and no failover.
  Retry a failed `settleAck()` with `ack()`.
- `ackDetached()` throws `InvalidArgumentException` for a message without `transactionId` or
  `partitionId`.
- A request with a body that cannot be written within 5 s throws at once; nothing reached the
  broker. A pop is waited for 250 ms at most and then left to `settlePop()`.
- `settleAck()` and `settlePop()` throw on a transport or HTTP failure, and when no answer
  arrives before the deadline. They cancel an unanswered request.

## Transactions

`transaction()` bundles acks, pushes, key/value operations and timers into one atomic commit: all
of them apply, or none.

```php
$queen->transaction()
    ->ack($message, 'completed', ['consumerGroup' => 'billing'])
    ->queue('orders.enriched')->partition('customer-42')->push([['data' => ['id' => 1]]])
    ->commit();
```

The idempotency idiom puts a `required` marker in the same bundle as the ack:

```php
$orderId = 'order-8891';

$result = $queen->transaction()
    ->ack($message, 'completed', ['consumerGroup' => 'billing'])
    ->kv('saga')->putIfAbsent($orderId, ['step' => 'reserved'], [
        'ttlSeconds' => 86400,
        'required' => true,
    ])
    ->queue('payments')->push([['data' => $charge, 'transactionId' => "charge-{$orderId}"]])
    ->timers('payments.timeout')->schedule($orderId, 900_000, ['orderId' => $orderId])
    ->commit();

if (($result['reason'] ?? null) === 'kv_precondition') {
    // The step already happened. Nothing was pushed, acked or scheduled: ack the input alone.
    $queen->ack($message, true, ['group' => 'billing']);
}
```

### `TransactionBuilder`

| Method | Returns |
| --- | --- |
| `ack(array\|object $messages, string $status = 'completed', array $context = [])` | `static` |
| `queue(string $queueName)` | `TransactionQueueBuilder` |
| `kv(string $namespace)` | `TransactionKvBuilder` |
| `timers(string $queueName)` | `TransactionTimerBuilder` |
| `commit()` | `array` |

`ack()` takes one message or a list:

- Every message needs `transactionId` (or `id`) and `partitionId`, else `ack()` throws
  `InvalidArgumentException` at once.
- `$status` is sent as given: `completed`, `failed`, `retry` or `dlq`.
- The context key is `consumerGroup`, not `group` as on `Queen::ack()`. Set it for every message
  popped for a group; without it the ack targets queue mode's cursor.
- Each ack carries its message's `leaseId`, which also goes into `requiredLeases`. A fenced ack
  whose lease has expired rolls the whole transaction back.

The sub-builders return the parent `TransactionBuilder` from their operation methods, so one chain
reads as one bundle:

| Sub-builder | Methods |
| --- | --- |
| `TransactionQueueBuilder` | `partition(string)` (returns itself), `push(array $items)` |
| `TransactionKvBuilder` | `get`, `getMany`, `put`, `putIfAbsent`, `delete`, `incr`; signatures as on [`Kv`](#keyvalue-state) without `$ns` |
| `TransactionTimerBuilder` | `schedule`, `reschedule`, `cancel`; signatures as on [`Timers`](#timers) without `$queue` |

Push items use the same keys as `QueueBuilder::push()`; a missing `transactionId` gets a UUIDv7.
Without `partition()` the item has no partition and the broker uses `Default`. The transaction KV
builder has no `getPrefix`. A cancel inside a transaction can be refused with the rest of the
bundle; `Timers::cancel()` uses a route that is never blocked.

### `commit()`

`commit()` posts to `/api/v1/transaction`. KV and timer operations travel as the top-level `kv`
and `timers` arrays, which are left out when empty.

- An empty builder throws `RuntimeException('Transaction has no operations to commit')`.
- A commit returns the broker body: `transactionId`, `success: true` and `results`. The results
  share one index space: the acks and pushes in order, then `kv`, then `timers`.
- A `required` KV operation that lost its precondition does not throw. `commit()` returns the
  rollback body with `success: false`, `reason: 'kv_precondition'`, `failedIndex`, `kvReason`,
  `version` and `value`. `failedIndex` uses the same index space.
- Every other `success: false` throws `RuntimeException("Transaction <id> failed: <error>")`. The
  `reason` is not on the exception.
- HTTP errors throw `HttpException`.

The rollback reasons are listed on
[transaction](https://queenmq.com/reference/transaction/#rollback-reasons).

## Key/value state

`$queen->kv()` reads and writes transactional key/value state through `POST /api/v1/kv`. Writes
that must share the fate of an ack belong in a [transaction](#transactions).

```php
$kv = $queen->kv();

$lock = $kv->putIfAbsent('locks', 'invoice-77', ['owner' => 'worker-3'], ['ttlSeconds' => 30]);
if ($lock['applied']) {
    // This worker won.
}

$row = $kv->get('carts', 'cart-9');
if ($row['found']) {
    $cart = $row['value'];
    $kv->put('carts', 'cart-9', $cart, ['ttlSeconds' => 86400, 'expect' => $row['version']]);
}

$hits = $kv->incr('ratelimit', 'client-12', 1, ['ttlSeconds' => 60, 'max' => 100]);
$page = $kv->getPrefix('carts', 'cart-', ['limit' => 100]);
```

| Method | Options | Returns |
| --- | --- | --- |
| `get(string $ns, string $key)` | none | `found`, `key`, `value`, `version`, `expiresAt`, `updatedAt` |
| `getMany(string $ns, array $keys)` | none | `rows`, `missing`, `truncated` |
| `getPrefix(string $ns, string $prefix, array $opts = [])` | `after`, `limit`, `keysOnly` | `rows`, `truncated`, `nextAfter` |
| `put(string $ns, string $key, mixed $value, array $opts = [])` | `ttlSeconds` or `forever`, `expect`, `required` | `applied`, `key`, `value`, `version` |
| `putIfAbsent(string $ns, string $key, mixed $value, array $opts = [])` | `ttlSeconds` or `forever`, `required` | as `put` |
| `delete(string $ns, string $key, array $opts = [])` | `expect`, `required` | as `put` |
| `incr(string $ns, string $key, int\|float $delta, array $opts = [])` | `ttlSeconds` or `forever`, `min`, `max`, `required` | as `put` |
| `batch(array $ops)` | operations built with `Support\KvOp` | `results`, index-aligned |

The verdict is in the body, not in the HTTP status:

- A miss is `found: false`. A lost write is `applied: false`, with the current `value` and
  `version` and a `reason`: `exists`, `absent`, `version`, `limit` or `type`.
- `put`, `putIfAbsent` and `incr` need exactly one of `ttlSeconds` (an integer above 0) and
  `forever: true`. The broker enforces it; the client never adds a default expiry.
- `expect: 0` means the key must not exist; `expect: N` updates version N only.
- With `max` or `min`, `applied` is the admission decision: a call that would cross the bound
  does not apply.
- `required: true` on a single call returns the precondition verdict `ok: false`,
  `reason: 'kv_precondition'` instead of an element.
- An `HttpException` means that the call failed, never that the predicate lost.

`Support\KvOp` builds the operations for `batch()`. It throws `InvalidArgumentException` for an
option that the operation does not accept, for a `null` option, and for `tenant`, `tenantId`,
`_tenant`, `ttl`, `ttlMillis` and `expiresAt`.

```php
use Queen\Support\KvOp;

$kv->batch([
    KvOp::put('carts', 'cart-9', $cart, ['forever' => true]),
    KvOp::delete('carts', 'cart-8'),
]);
```

The model, the limits and the error codes are on [KV](https://queenmq.com/concepts/kv/).

## Timers

`$queen->timers()` schedules messages: the broker holds a timer and pushes its payload into a
queue when the delay has passed.

```php
$timers = $queen->timers();

$t = $timers->schedule('payments.timeout', 'order-8891', 900_000, ['orderId' => 8891]);
// $t['status'] === 'scheduled'; $t['txn'] is the transactionId of the message it will push.

$c = $timers->cancel('payments.timeout', 'order-8891', $t['txn']);
if ($c['status'] === 'absent') {
    // No longer pending. It may have fired: look for $t['txn'] in the queue.
}

$pending = $timers->count('payments.timeout', 'order-');
```

| Method | Returns |
| --- | --- |
| `schedule(string $queue, string $timerKey, int $delayMs, mixed $payload, array $opts = [])` | `ok`, `status`, `queue`, `timerKey`, `txn`, `messageId`, `deliverAt` |
| `reschedule(...)` | as `schedule` |
| `cancel(string $queue, string $timerKey, ?string $txn = null)` | `ok`, `status`, `queue`, `timerKey`, `txn` |
| `peek(string $queue, string $timerKey)` | `found` and the stored timer |
| `list(string $queue, array $opts = [])` | `rows`, `truncated`, `nextAfter` |
| `count(string $queue, string $prefix)` | `int` |
| `batch(array $ops)` | `results`, index-aligned |

- `schedule` and `reschedule` are the same upsert on `(queue, timerKey)`. A reschedule resets the
  retry budget. A timer already claimed for delivery answers `ok: false`, `status: 'too_late'`.
- `$delayMs` is relative, in milliseconds; a negative delay fires on the first cycle. `deliverAt`
  means "not before".
- `$opts` accepts `txn` and `partition`. A missing `txn` gets a UUIDv7. Any other option, a `null`
  option, and the broker-owned fields (`producerSub`, `messageId`, `deliverAt`, `delaySeconds`,
  `attempts` and others) throw `InvalidArgumentException`.
- The payload is JSON-encoded and base64-encoded, so the consumer reads the same shape as a push.
- `cancel()` uses `DELETE /api/v1/timers/:queue/:timerKey`, which quota and pauses never block.
  `absent` means "no longer pending" and may mean "already delivered". A cancel through `batch()`
  can be refused with the batch.
- `list()` accepts `after` and `limit` and returns no payloads; use `peek()`.
- `count()` counts the pending timers whose key starts with `$prefix`. It throws
  `InvalidArgumentException` for an empty queue or prefix, a NUL, invalid UTF-8 or more than 128
  bytes. On a broker without the count mode it counts list pages, and it throws
  `UnexpectedValueException` for a malformed answer.
- `Support\TimerOp::schedule()`, `reschedule()` and `cancel()` build the operations for `batch()`.

The statuses are `scheduled`, `rescheduled`, `cancelled`, `absent` and `too_late`. The model is on
[timers](https://queenmq.com/concepts/timers/).

## Dead-letter queue

`dlq()` reads the dead letters of a queue; the admin API replays and deletes them.

```php
$page = $queen->queue('orders')->dlq('billing')->limit(50)->offset(0)->get();

foreach ($page['messages'] as $dead) {
    // $dead: id, transactionId, partitionId, partition, queue, consumerGroup,
    //        data, errorMessage, retryCount, failedAt, ...
    $queen->admin()->retryMessage($dead['partitionId'], $dead['transactionId']);
}
```

`dlq(?string $consumerGroup = null): DLQBuilder` throws `RuntimeException` when the builder has no
queue name.

| Method | Default | Effect |
| --- | --- | --- |
| `limit(int $count)` | `100` | Page size, at least 1. The broker caps it at 1000. |
| `offset(int $count)` | `0` | Rows to skip. |
| `from(string $timestamp)` | none | Sent as `from`. |
| `to(string $timestamp)` | none | Sent as `to`. |
| `get(): array` | | `GET /api/v1/dlq`: `messages`, `total`, `pagination`. |

The 2.x broker filters on `queue`, `consumerGroup`, `limit` and `offset` only. It ignores `from`,
`to` and the `partition` that the builder sends for a partition other than `Default`
(`server/src/rsm/facade/real/phase2/reads.rs`, `api_dlq`).

`Admin::retryMessage($partitionId, $transactionId)` moves a dead letter back into its partition,
in one transaction, under the transaction id `dlq:<dead-letter id>`. The answer carries
`result: 'moved'`, or `'duplicate'` when nothing was written and the row stays. A second call for
the same address throws `HttpException` 404. `Admin::deleteMessage()` deletes dead-letter rows
only.

## Ephemeral queues

`$queen->ephemeral()` reaches RAM-class queues: the declared configuration is durable, and the
contents survive no restart, crash or ownership move.

```php
$eph = $queen->ephemeral();

$eph->configure('presence', ['maxLength' => 10_000, 'ttlSeconds' => 60]);
$eph->push('presence', [['user' => 'a', 'room' => 7]], ['partition' => 'room-7']);

$batch = $eph->pop('presence', ['group' => 'dashboard', 'wait' => true, 'batch' => 50]);
foreach ($batch['messages'] as $message) {
    show($message['payload']);
}
$eph->ack('presence', $batch['messages'], ['group' => 'dashboard']);
```

| Method | Options |
| --- | --- |
| `configure(string $queue, array $options = [])` | `maxBytes`, `maxLength`, `policy`, `ttlSeconds`, `leaseSeconds`, `retryLimit`, `windowBuffer` |
| `push(string $queue, mixed $messages, array $opts = [])` | `partition`, `buffered` (`true` or buffer options) |
| `flush(string $queue, ?string $partition = null)` | none |
| `pop(string $queue, array $opts = [])` | `partition`, `batch`, `wait`, `timeout` or `timeoutMillis`, `group`, `autoAck` |
| `ack(string $queue, mixed $acks, array $opts = [])` | `group`, `status`, `error` |
| `reset(string $queue)` | none |
| `delete(string $queue)` | none |
| `queues()` | none |
| `depth(string $queue)` | none |

- `configure()` throws `InvalidArgumentException` for an unknown option.
- A push message is a bare value, `['payload' => ...]` or `['data' => ...]`; `null` throws. Push
  and pop create a queue that does not exist.
- A buffered push uses the [push buffer](#buffered-push) under the address
  `eph:<queue>/<partition>` and returns `['buffered' => true, 'count' => n]`. `intervalMillis` is
  accepted for `timeMillis`.
- `pop()` returns `['queue' => ..., 'messages' => [...]]`, never `null`. A message is `id`,
  `partition`, `payload`, `attempts`. The `wait` timeout defaults to 30000 ms; passing both
  `timeout` and `timeoutMillis` throws.
- The `group` decides consumption: one group is competing consumers, separate groups fan out, no
  group is queue mode. `autoAck` is at-most-once; the default explicit ack redelivers an unacked
  message until `retryLimit`.
- `ack()` takes a popped message, an id string, or a list. A status is `completed`, `failed`,
  `retry`, or `true`/`false`. Outcomes are `acked`, `redelivered`, `stale` or `unknown`.
- A 404 from a broker without the ephemeral routes throws `EphemeralUnsupportedException`. A 404
  `ephemeral_queue_not_found` from `depth()` throws `EphemeralQueueNotFoundException`.

The model is on [ephemeral queues](https://queenmq.com/guides/ephemeral/).

## Admin API

`$queen->admin()` wraps the broker's read and management routes. Each method returns the decoded
body. A method that takes `array $params` sends it as the query string, without `null` values.

### Resources

| Method | Route |
| --- | --- |
| `getOverview()` | `GET /api/v1/resources/overview` |
| `getNamespaces()` | `GET /api/v1/resources/namespaces` |
| `getTasks()` | `GET /api/v1/resources/tasks` |
| `listQueues(array $params = [])` | `GET /api/v1/resources/queues` |
| `getQueue(string $name)` | `GET /api/v1/resources/queues/:name` |
| `getQueueDepth(string $name, ?string $group = null, ?int $timeoutMillis = null)` | `GET /api/v1/resources/queues/:name/depth` |
| `getQueueDepthAsync(...)` | The same, as a promise |
| `getPartitions(array $params = [])` | `GET /api/v1/resources/partitions` |
| `listKv(string $namespace, array $options = [])` | `POST /api/v1/resources/kv/list` |

`getQueueDepth()` is the cheap backlog read: `pending`, `processing`, `ready`, `partitionsPending`,
`partitionsReady`, `conflation`, `effectivePending`, `effectiveReady`, and per partition
`pending`, `processing`, `ready`. For a conflating group the effective values count partitions.

`listKv()` options: `prefix`, `after`, `limit` (1 to 1000, default 100), `keysOnly`,
`includeExpired`. It needs read access only.

`clearQueue()` exists and always throws `BadMethodCallException`: the broker has no atomic
queue clear.

### Messages and traces

| Method | Route |
| --- | --- |
| `listMessages(array $params = [])` | `GET /api/v1/messages` |
| `getMessage(string $partitionId, string $transactionId)` | `GET /api/v1/messages/:partitionId/:transactionId` |
| `deleteMessage(string $partitionId, string $transactionId)` | `DELETE /api/v1/messages/:partitionId/:transactionId` |
| `retryMessage(string $partitionId, string $transactionId)` | `POST /api/v1/messages/:partitionId/:transactionId/retry` |
| `getTraceNames(array $params = [])` | `GET /api/v1/traces/names` |
| `getTracesByName(string $traceName, array $params = [])` | `GET /api/v1/traces/by-name/:traceName` |
| `getTracesForMessage(string $partitionId, string $transactionId)` | `GET /api/v1/traces/:partitionId/:transactionId` |

`getTracesForMessage()` does not URL-encode its arguments. `moveMessageToDLQ()` posts to
`/api/v1/messages/:partitionId/:transactionId/dlq`, a route that the 2.x broker does not have; it
throws `HttpException` 404 `no_such_route`.

### Status and analytics

| Method | Route |
| --- | --- |
| `getStatus(array $params = [])` | `GET /api/v1/status` |
| `getQueueStats(array $params = [])` | `GET /api/v1/status/queues` |
| `getQueueDetail(string $name, array $params = [])` | `GET /api/v1/status/queues/:name` |
| `getAnalytics(array $params = [])` | `GET /api/v1/status/analytics` |
| `getQueueOps(array $params = [])` | `GET /api/v1/analytics/queue-ops` |
| `getQueueOpsAsync(array $params = [], ?int $timeoutMillis = null)` | The same, as a promise |
| `getSystemMetrics(array $params = [])` | `GET /api/v1/analytics/system-metrics` |
| `getWorkerMetrics(array $params = [])` | `GET /api/v1/analytics/worker-metrics` |

`getQueueOps()` returns push, pop and ack counters per queue in time buckets. Its filters are
`from`, `to` and `queue`.

### Consumer groups

| Method | Route |
| --- | --- |
| `listConsumerGroups()` | `GET /api/v1/consumer-groups` |
| `getConsumerGroup(string $name)` | `GET /api/v1/consumer-groups/:name` |
| `getLaggingConsumers(int $minLagSeconds = 60)` | `GET /api/v1/consumer-groups/lagging` |
| `getLaggingConsumersAsync(int $minLagSeconds = 60, ?int $timeoutMillis = null)` | The same, as a promise |
| `deleteConsumerGroupForQueue(string $consumerGroup, string $queueName, bool $deleteMetadata = true)` | `DELETE /api/v1/consumer-groups/:group/queues/:queue` |
| `seekConsumerGroup(string $consumerGroup, string $queueName, array $options = [])` | `POST /api/v1/consumer-groups/:group/queues/:queue/seek` |
| `refreshConsumerStats()` | `POST /api/v1/stats/refresh` |

`seekConsumerGroup()` posts `$options` as given: `['toEnd' => true]` or
`['timestamp' => '<ISO 8601>']`. A 2.x broker answers `refreshConsumerStats()` with
`refreshed: false`, because its counters are always current.

### System

| Method | Route |
| --- | --- |
| `health()` | `GET /health` |
| `metrics()` | `GET /metrics` |

The `...Async()` methods return a Guzzle promise. They retry network failures and 5xx and fail
over like the synchronous calls, and they do not retry a 429.

## Tracing

A consumer records trace events against the message it handles, and the admin API reads them back
by message or by name.

```php
$queen->queue('orders')->group('billing')->each()
    ->consume(function (array $message): void {
        $message['trace']([
            'traceName' => ['tenant-acme', 'order-8891'],
            'eventType' => 'info',
            'data' => ['step' => 'charged'],
        ]);
    })
    ->execute();

$events = $queen->admin()->getTracesByName('order-8891');
```

- Messages given to a `consume()` handler and returned by `HighLevelConsumer` carry a `trace`
  closure. Messages from `pop()` do not.
- The closure posts to `/api/v1/traces` with the message's `transactionId`, `partitionId` and the
  consumer group (`__QUEUE_MODE__` without one).
- `data` is required. `traceName` is a string or a list of strings. `eventType` defaults to
  `'info'`.
- It never throws. A failure returns `['success' => false, 'error' => ...]`.

A push item can carry a `traceId`. The client sends it without validation, and the broker ignores
a value that is not a UUID.

## HTTP transports

The client sends synchronous and detached requests through its own cURL transport when it can, and
through Guzzle otherwise.

The cURL transport (`Http\CurlTransport`) is used when all of these are true at construction:

- `ext-curl` is loaded.
- `QUEEN_SDK_HTTP_TRANSPORT` is not `guzzle` (case-insensitive).
- None of `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and their lowercase forms is set.

```bash
QUEEN_SDK_HTTP_TRANSPORT=guzzle php worker.php
```

Guzzle always carries the promise-based requests: `concurrency()` above 1, `flushAllBuffers()`,
lease renewal inside `consume()`, and the `...Async()` admin methods.

Both paths behave the same way:

- HTTP/1.1 with keep-alive. TLS peer and host are verified. Redirects are not followed, so
  credentials never go to another host.
- Headers: `Content-Type: application/json`, the configured `headers`, and
  `Authorization: Bearer <bearerToken>` when set.
- Connect timeout 5 s; total timeout `timeoutMillis`, or the pop timeout plus 5 s for a pop.
- A `204` answer returns `null`.

The cURL transport adds:

- One reused cURL handle per client, so requests share kept-alive connections. TCP keep-alive
  probes start after 30 s idle and repeat every 15 s.
- No signals (`CURLOPT_NOSIGNAL`), so a `SIGALRM` job timeout is not disturbed.
- Fork safety: a forked child opens its own connection and leaves the parent's open.
- `User-Agent: queen-php-client`.
- A failure without an HTTP answer throws `Http\TransportException`, a
  `GuzzleHttp\Exception\TransferException`, with the message `cURL error <code>: <text> (<host>)`.

## Retries, failover and 429

The HTTP client retries failures that can pass, fails over between backends, and backs off on
rate limiting; it never retries another 4xx.

### Network failures and 5xx

| Setup | Behaviour |
| --- | --- |
| One URL, or `enableFailover: false` | Up to `retryAttempts` attempts, `retryDelayMillis` doubling between them. |
| More than one URL and `enableFailover: true` | One attempt per backend, without delay. |

- A failure that counts: no HTTP answer, a 5xx, or a 2xx whose body is not valid JSON.
- With failover, a failed backend is marked unhealthy and stays out of rotation for
  `healthRetryAfterMillis`. When every backend is unhealthy, the client uses one of them anyway.
- A 4xx is thrown at once and never fails over.

The load balancer picks a backend per request:

- `affinity` hashes a key onto a ring of `affinityHashRing` virtual nodes per backend. Pops, the
  consume loops and their acks use `queue:partition:group` as the key, so one partition's traffic
  stays on one backend.
- `round-robin` rotates over the healthy backends.
- `session` assigns each key a backend on first use and keeps it.
- A request without a key uses one key per client.

### HTTP 429

A 429 is retried in place, against the same backend, under `retry429`. It never marks a backend
unhealthy and never fails over.

| Key | Default | Effect |
| --- | --- | --- |
| `maxAttempts` | `10`; unbounded for a pop with `wait(true)` | Attempts including the first. `0` selects the default. |
| `baseMs` | `500` | First backoff; doubles per attempt. |
| `capMs` | `30000` | Ceiling of one backoff; at most `300000`. |

- A `Retry-After` header (seconds) replaces the computed backoff, capped at `capMs`.
- Every delay gets plus or minus 20 % jitter.
- An explicit `maxAttempts` applies to long-poll pops too.
- The constants are on `Http\Retry429Policy`: `DEFAULT_MAX_ATTEMPTS`, `DEFAULT_BASE_MILLIS`,
  `DEFAULT_CAP_MILLIS`, `MAX_CAP_MILLIS`, `UNBOUNDED` (0) and `KIND_POP`.
- A 429 that exhausts the budget throws `HttpException` with `isRateLimited()` true.
- Promise-based and detached requests do not retry a 429. The concurrent consume loop waits one
  backoff per round instead.

The proxy and broker codes are on [errors](https://queenmq.com/reference/errors/).

## Errors and exceptions

An HTTP status of 400 or above throws `Queen\Exceptions\HttpException`, a `RuntimeException`.

| Property or method | Meaning |
| --- | --- |
| `statusCode` | The HTTP status. |
| `errorCode` | The body's `code`, or `null`. Compare with `ErrorCode` constants. |
| `serverError` | The body's `error`, or `null`. |
| `reason` | The body's `reason` (KV and timers), or `null`. |
| `detail` | The body's `detail`, naming the failed operation, or `null`. |
| `retryAfterSeconds` | `Retry-After` of a 429, or `null`. |
| `isRateLimited()` | `statusCode === 429`. |
| `isClusterSuspended()` | A 403 with `cluster_suspended`. |

The message is the body's `error`, then `: <reason>` and ` (<detail>)` when present, or
`HTTP <status>` without a body. Branch on the properties, not on the message.

`Queen\Exceptions\ErrorCode` constants:

| Constant | Value |
| --- | --- |
| `RATE_LIMITED`, `QUOTA_EXCEEDED` | 429 codes, retried |
| `CLUSTER_SUSPENDED`, `STORAGE_QUOTA_EXCEEDED`, `FEATURE_GATED`, `FORBIDDEN` | 403 codes, terminal |
| `NO_SUCH_ROUTE` | `no_such_route` |
| `UNSUPPORTED` | `unsupported` |
| `EPHEMERAL_UNSUPPORTED` | Set by the client on `EphemeralUnsupportedException` |
| `EPHEMERAL_QUEUE_NOT_FOUND` | `ephemeral_queue_not_found` |

Other exceptions:

| Exception | Thrown when |
| --- | --- |
| `Queen\Http\TransportException` | The cURL transport got no HTTP answer. |
| Guzzle `TransferException` subclasses | The Guzzle path got no HTTP answer. |
| `UnexpectedValueException` | A success body is not valid JSON, after retries; `Timers::count()` got a malformed answer. |
| `Exceptions\ConflationUnsupportedException` | Conflation was asked for and not applied. |
| `Exceptions\ConflationPolicyMismatchException` | The group conflates and this consumer did not ask for it. |
| `Exceptions\EphemeralUnsupportedException` | The broker has no ephemeral routes (extends `HttpException`). |
| `Exceptions\EphemeralQueueNotFoundException` | `depth()` named a missing queue (extends `HttpException`). |
| `InvalidArgumentException` | Invalid config, KV or timer option, renewal seconds, or a transaction ack without ids. |
| `RuntimeException` | A missing queue name, an empty or failed transaction, a full push buffer, a `failed` push item. |
| `BadMethodCallException` | `Admin::clearQueue()`. |

Calls that report instead of throwing:

- `Queen::ack()` returns `success: false`.
- `Queen::renew()` returns `success: false` per lease.
- `TransactionBuilder::commit()` returns the `kv_precondition` verdict.
- The `trace` closure returns `success: false`.
- `PushBuilder`, `OperationBuilder` and `ConsumeBuilder` hand errors to `onError` when it is set.
- KV and timer verdicts (`applied`, `found`, `ok`) are in the body with HTTP 200.

## Not in this client

There is no streaming SDK for PHP: the `Stream` builder and the `/streams/v1/*` runtime exist in
the JavaScript, Python, Go and Rust clients only.
