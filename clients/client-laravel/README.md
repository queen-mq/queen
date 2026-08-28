# Queen MQ - PHP/Laravel Client

PHP client library for [Queen MQ](https://github.com/queen-mq/queen) with optional Laravel integration.

Works standalone with any PHP 8.3+ project. Laravel extras (service provider, facade, artisan command) are auto-discovered.

## Installation

```bash
composer require smartpricing/queen-mq
```

### Laravel Setup

Publish the config file:

```bash
php artisan vendor:publish --tag=queen-config
```

Set your environment variables:

```env
QUEEN_URL=http://localhost:6632
QUEEN_BEARER_TOKEN=your-token
```

### Laravel Queue Driver

Queen can be used as Laravel's queue backend, so existing jobs keep using
`dispatch()`, middleware, retries, backoff and failed-job events:

```env
QUEUE_CONNECTION=queen
QUEEN_QUEUE=default
QUEEN_CONSUMER_GROUP=laravel
QUEEN_RETRY_AFTER=90
QUEEN_BLOCK_FOR=5
```

Run the ordinary Laravel worker:

```bash
php artisan queue:work queen --queue=default --timeout=60 --tries=3
```

`QUEEN_RETRY_AFTER` controls the Queen lease and must be longer than the
worker/job timeout. You can run `queue:work` directly under an existing process
manager, or use Queen's PHP or Rust worker supervisor described below. Process
orchestration remains outside the Queen broker.

`QUEEN_BLOCK_FOR` is the broker long-poll duration. At `0` polling is
non-blocking but keeps the normal 30-second request budget; positive values set
the broker deadline in seconds. The HTTP client adds five seconds of transport
slack, so an idle broker response does not race the network timeout.

By default, jobs are spread deterministically across up to 64 partitions. This
allows concurrent processing without creating a partition for every job. To
preserve ordering for a business entity, implement `QueenPartitionable`:

```php
use Queen\Laravel\Contracts\QueenPartitionable;

final class RebuildCustomer implements QueenPartitionable
{
    public function __construct(public string $customerId) {}

    public function queenPartition(): string
    {
        return 'customer:' . $this->customerId;
    }
}
```

Delayed dispatch and Laravel backoff use Queen timers. Release is atomic: the
current lease is acknowledged in the same Queen transaction that publishes or
schedules the next attempt. Final failures are acknowledged to Queen's DLQ and
still flow through Laravel's normal failed-job lifecycle. The default
failed-job provider decorator keeps the two indexes synchronized:
`queue:retry`, `queue:forget`, `queue:flush` and `queue:prune-failed` remove the
corresponding Queen snapshot only after a safe handoff. A retry uses a stable
new Queen transaction ID and resets attempts, so an ambiguous retry can run
again without duplicating the job.

The broker's group-scoped `deliveryAttempt` increments after a nack, worker
crash or lease expiry. `QueenJob::attempts()` combines it with explicit Laravel
releases, so `--tries` applies to both failure modes. Upgrade the broker before
relying on crash-attempt enforcement; an older broker has no field to report
and the compatibility fallback is `1`.

Laravel's queue-monitoring methods use the same consumer-group depth as its
workers. `size()` is total `pending` (ready plus live leases) plus Laravel-owned
timers, `pendingSize()` is `ready`, and `reservedSize()` is `processing`.
During a rolling broker upgrade, missing lease-aware fields fall back to the
previous pending/effective-pending and queue-detail counters.

`creationTimeOfOldestPendingJob()` first reads depth and returns `null` without
the expensive queue-detail request when `ready` is zero. Depth deliberately has
no timestamps, so when work is ready the method filters queue detail to ready
partitions and returns a conservative approximation: `oldestMessage` is based
on the queue-wide worst cursor and may be older than this consumer group's
first unleased job. Older brokers use that same queue-detail approximation.

Do not use the generic Queen Admin `retryMessage()` for a Laravel failed job:
it cannot update Laravel's `failed_jobs` repository. Use `queue:retry` so the
payload attempts and both failure indexes move together.

### Worker Supervisor

Queen provides two local process-orchestration engines over the same Laravel
configuration. The PHP engine is the reference implementation:

```bash
composer require symfony/process
php -m | grep -q '^pcntl$' # required by the PHP engine and queue workers
```

```bash
php artisan queen:supervise
```

The optimized Rust engine avoids keeping a Laravel application loaded in the
master. It invokes Artisan once to resolve the same configuration, then leaves
only Rust and the ordinary PHP workers resident:

```bash
queen-supervisor --php php --artisan artisan
```

Both read `queen.supervisor.supervisors`, start standard `queue:work` children
and support these balancing modes:

- `auto` dynamically sizes the pool and distributes it by queue pressure;
- `simple` keeps `processes` workers spread evenly across the queues;
- `off` gives every worker the declared comma-separated queue list, preserving
  Laravel queue priority.

`strategy=size` computes pressure from Queen's group-specific
`effectivePending` depth and `target_jobs_per_process`. `strategy=time` uses
`effectivePending × observed job runtime` to target
`target_clear_seconds`. Runtime telemetry is written best-effort by the Laravel
workers; `default_runtime_seconds` is used until samples are available. Both
strategies are bounded by `min_processes`, `max_processes`,
`balance_cooldown`, `balance_max_shift` and the aggregate `process_limit`.
The supervisor establishes `processes` in `simple` mode, or at least
`min_processes` otherwise, immediately; `balance_max_shift` applies only to
elastic changes above that baseline.
Both engines cap exponential worker-restart backoff. Only the Rust engine opens
a circuit after five consecutive crashes and allows a single probe after the
cooldown; the PHP engine has no open/one-probe state and keeps retrying at
`restart_backoff_max`.

A minimal supervisor configuration is:

```php
'supervisor' => [
    'poll_interval' => 3,
    'shutdown_grace' => 75, // must exceed every worker timeout
    'state_directory' => storage_path('queen-supervisor'),
    'supervisors' => [
        'jobs' => [
            'connection' => 'queen',
            'consumer_group' => 'laravel',
            'queues' => ['high', 'default'],
            'balance' => 'auto',
            'strategy' => 'time', // or size
            'min_processes' => 1,
            'max_processes' => 20,
            'target_clear_seconds' => 60,
            'default_runtime_seconds' => 1,
            'balance_cooldown' => 3,
            'balance_max_shift' => 2,
            'scale_down_delay' => 10,
            'restart_backoff' => 1,
            'restart_backoff_max' => 30,
            'stable_after' => 60,
            'timeout' => 60,
        ],
    ],
],
```

Both engines use the same resolved v2 configuration. Inspect it with:

```bash
php artisan queen:supervisor-config --pretty
```

That form redacts credentials. `--for-engine` includes the tokens and header
values required by a supervisor engine.

Control and status are engine-independent because they use the private state
directory:

```bash
php artisan queen:supervisor status
php artisan queen:supervisor status --json
php artisan queen:supervisor status --check # non-zero unless live
php artisan queen:supervisor pause
php artisan queen:supervisor continue
php artisan queen:supervisor terminate
```

Pause and continue use Laravel's `SIGUSR2` and `SIGCONT` worker controls;
terminate drains workers with `SIGTERM` and applies `shutdown_grace` before a
forced kill. Under systemd or another process manager configured to always
restart, `terminate` also reloads Laravel configuration under a fresh master.
Production supervision therefore requires Unix and PHP `pcntl`.

For broker endpoint failover, configure `QUEEN_URLS` as a comma-separated list.
Both v2 engines read each pool's resolved Queen `connections` entry directly
and create an isolated depth client from its endpoints, bearer token and custom
headers; neither reuses the worker's live Laravel client.
`QUEEN_SUPERVISOR_READ_BEARER_TOKEN` can replace the worker token for these
reads, while the remaining validated headers travel with the depth client.
Workers continue to use the application's normal queue credentials. Both
engines poll queue depths in waves of at most 16 concurrent requests and can
move network/5xx failures to another configured endpoint. The resolved JSON
contains the supervisor token and custom headers: the regular config command
redacts them, while `--for-engine` includes them.
Prefer the Rust engine's one-shot Artisan pipe, and protect any credential-bearing
`--config` file with mode `0600`. Keep `state_directory` outside the web root and
accessible only to the application user. Multiple broker endpoints provide
broker failover, not active-active supervisor leadership.

> **Production limit:** run exactly one supervisor process for an application
> and consumer group. The state lock prevents a second local owner, but there
> is no distributed fenced leader lease yet. Two replicas on different hosts
> see the same global backlog and can both scale to the maximum. Workers may be
> numerous; the singleton restriction applies to the supervising master. In
> Kubernetes use one replica without rolling-surge overlap, or a `Recreate`
> strategy.

See [`supervisor/README.md`](../../supervisor/README.md) for deployment and
security details.

## Usage

### Standalone (without Laravel)

```php
use Queen\Queen;

// Single server
$queen = new Queen('http://localhost:6632');

// Multiple servers with config
$queen = new Queen([
    'urls' => ['http://server1:6632', 'http://server2:6632'],
    'loadBalancingStrategy' => 'affinity',
    'bearerToken' => 'my-token',
]);
```

### Laravel Facade

```php
use Queen\Laravel\QueenFacade as Queen;

Queen::queue('orders')->push([['data' => ['test' => true]]])->execute();
```

## Queue Operations

### Create a Queue

```php
$queen->queue('orders')
    ->config(['leaseTime' => 300, 'retryLimit' => 3])
    ->create()
    ->execute();
```

### Delete a Queue

```php
$queen->queue('orders')->delete()->execute();
```

### Push Messages

```php
// Simple push
$queen->queue('orders')
    ->partition('user-123')
    ->push([
        ['data' => ['orderId' => 1, 'amount' => 100]],
        ['data' => ['orderId' => 2, 'amount' => 200]],
    ])
    ->execute();

// Push with callbacks
$queen->queue('orders')
    ->push([['data' => ['orderId' => 1]]])
    ->onSuccess(function ($items) {
        echo "Pushed " . count($items) . " messages\n";
    })
    ->onError(function ($items, $error) {
        echo "Push failed: " . $error->getMessage() . "\n";
    })
    ->onDuplicate(function ($items, $error) {
        echo "Duplicates detected\n";
    })
    ->execute();
```

### Buffered Push

```php
// Messages are accumulated and flushed when count or time threshold is reached
$queen->queue('orders')
    ->buffer(['messageCount' => 100, 'timeMillis' => 1000])
    ->push([['data' => ['orderId' => 1]]])
    ->execute();

// Manually flush
$queen->queue('orders')->flushBuffer();
$queen->flushAllBuffers();
```

| Option | Default | Meaning |
| --- | --- | --- |
| `messageCount` | `100` | Flush once this many messages are waiting. Also the batch size of a flush. |
| `timeMillis` | `1000` | Flush once the oldest waiting message is this old. Checked on the add path, since PHP has no background timer. |
| `maxSize` | `4 * messageCount` | Backpressure bound. At the bound the add flushes inline instead of appending, and raises if it cannot. `0` means the default, not unbounded. |
| `retryDelayMillis` | `250` | Wait before retrying a batch that failed to send. The batch goes back at the front of the buffer, in order — never dropped. |
| `maxWaitMillis` | `5000` | Total deadline for the inline retry loop of one flush. On expiry the push raises with the messages still queued. |

The buffer is bounded and pushes back: a producer that outruns the broker gets
a slower push, then an exception, never a silently growing heap. Buffered
pushes can therefore throw — `->onError(...)` receives the items that were not
confirmed, and `['buffered' => true, 'count' => N]` reports only the messages
the buffer actually took.

`maxWaitMillis` exists in this SDK and not in the Go/JS/Rust ones because PHP
has no background flusher to wait on: the flush runs inline in the caller's
request, so the wait needs a deadline that stays inside `max_execution_time`.
Lower it for web requests, raise it for CLI workers.

### Pop Messages

```php
// Pop a single message
$messages = $queen->queue('orders')->pop();

// Pop a batch with long polling
$messages = $queen->queue('orders')
    ->batch(10)
    ->wait(true)
    ->pop();

// Pop from a specific partition
$messages = $queen->queue('orders')
    ->partition('user-123')
    ->pop();
```

### Multi-Partition Pop (Drain Many Partitions Per Call)

```php
// One round-trip drains up to 200 messages spread across up to 50 partitions.
// batch(200) is the GLOBAL cap on total messages; partitions(50) is the
// hard cap on partitions claimed. All claimed partitions share one leaseId
// — a single renew() call extends every partition's lease atomically.
$messages = $queen->queue('events')
    ->batch(200)
    ->partitions(50)
    ->wait(true)
    ->pop();

// Each message carries its own partition info (per-message partitionId,
// partition name, leaseId, consumerGroup) — ACK and renew always work
// message-by-message regardless of how many partitions the batch spans.
foreach ($messages as $m) {
    echo "from {$m['partition']}: " . json_encode($m['data']) . "\n";
}

// Same builder works on the high-level consumer:
$consumer = $queen->queue('events')
    ->batch(100)
    ->partitions(8)
    ->getConsumer();
$consumer->subscribe();
```

**When to use:** queues with many partitions where each partition only has
a handful of new messages per polling interval (per-customer event streams,
per-tenant work queues, per-device telemetry). Reduces network round-trips
from O(P) to O(P / N) while preserving per-partition FIFO ordering.

**When not to use:** few partitions, or each one busy enough to fill
`batch(B)` on its own. Default is `partitions(1)` which preserves the
legacy single-partition behaviour.

`->partitions(N)` only applies to **wildcard** pops; specifying
`->partition('name')` ignores the cap.

### Conflation (Last-Value Delivery)

Requires broker >= 1.1.0.

For command-style queues where one partition is one logical task key and only
the newest pending message matters — "recompute entity X", dirty-flag
workloads. Under backlog the consumer processes **one** message per partition,
the newest, and the ack retires everything behind it:

```php
$queen->queue('recompute')
    ->group('workers')
    ->conflation(true)
    ->partitions(64)
    ->consume(function (array $messages) {
        foreach ($messages as $m) {
            recompute($m['partition']);   // the freshest state for this key
        }
    })
    ->execute();
```

A 4,000,000-message backlog spread over 12 dirty keys becomes 12 handler
invocations, not 4,000,000. The guarantee that makes it safe: **after the last
push to a partition, at least one run of that partition's handler starts after
that push commits.** Nothing is deleted — retention still governs storage, and
a second, non-conflating group on the same queue still sees every message.

Three things to know:

- **It belongs to the consumer group, not to the call.** The first consumer to
  register the group fixes the policy; every later consumer of that group gets
  the stored setting whatever it asks for, and is warned once if it disagreed.
  That is what lets `workers` conflate while `audit` reads everything.
- **It needs `->group()`.** Queue mode is a shared cursor with no group identity
  to hang a policy on, and the broker answers 400.
- **A conflating pop returns at most 64 messages per round trip** — one per
  claimed partition, capped by the checkout width. `batch()` above that is
  inert; `partitions(N)` is the knob that sizes the call.

Watch it with the depth endpoint, which separates the two numbers:

```php
$depth = $queen->admin()->getQueueDepth('recompute', 'workers');
// pending          => 4000000   total positions still to retire
// processing       => 1000000   positions under live leases
// ready            => 3000000   positions claimable now
// effectivePending => 12        total handler invocations still to retire
// effectiveReady   => 10        partitions claimable now
```

Use `effectiveReady` for immediate scheduling and `effectivePending` for total
remaining work. On a conflating group effective depths are partition counts;
on an ordinary group they equal the corresponding positional depths.

**Against a broker older than 1.1.0** the flag would be silently ignored and the
consumer would quietly drain the whole backlog message by message. It does not:
the consume loop raises `Queen\Exceptions\ConflationUnsupportedException` on the
first response, before a single message is processed.

## Consuming Messages

### High-Level Consumer (recommended)

Inspired by [php-rdkafka](https://github.com/arnaud-lb/php-rdkafka)'s `KafkaConsumer`:

```php
$consumer = $queen->queue('orders')
    ->group('processors')
    ->batch(1)
    ->autoAck(false)
    ->getConsumer();

$consumer->subscribe();

while (!$consumer->isClosed()) {
    $message = $consumer->consume(1000); // timeout in ms

    if ($message === null) {
        continue; // no message within timeout
    }

    // Process the message
    processOrder($message['payload']);

    // Acknowledge
    $consumer->ack($message);
}

$consumer->close();
```

#### Batch consuming

```php
$consumer = $queen->queue('orders')
    ->group('processors')
    ->getConsumer();

$consumer->subscribe();

while (!$consumer->isClosed()) {
    $messages = $consumer->consumeBatch(1000, 10); // timeout, max messages

    if (empty($messages)) {
        continue;
    }

    foreach ($messages as $msg) {
        processOrder($msg['payload']);
    }

    $consumer->ack($messages);
}

$consumer->close();
```

#### Consumer methods

| Method | Description |
|--------|-------------|
| `subscribe()` | Start the consumer (must call before consume) |
| `consume(int $timeoutMs)` | Get one message or `null` |
| `consumeBatch(int $timeoutMs, int $max)` | Get up to `$max` messages |
| `ack(array $message)` | Acknowledge message(s) |
| `nack(array $message)` | Negative-acknowledge (mark failed) |
| `renewLease(array\|string $msg)` | Extend message lease |
| `isClosed()` | Check if consumer was stopped |
| `close()` | Stop the consumer |

### Callback-Based Consumer

```php
$queen->queue('orders')
    ->group('processors')
    ->batch(10)
    ->autoAck(true)
    ->consume(function (array $messages) {
        foreach ($messages as $msg) {
            processOrder($msg['payload']);
        }
    })
    ->execute();
```

### Consumer with Callbacks

```php
$queen->queue('orders')
    ->group('processors')
    ->each()
    ->consume(function (array $message) {
        processOrder($message['payload']);
    })
    ->onSuccess(function ($message) {
        echo "Processed: {$message['transactionId']}\n";
    })
    ->onError(function ($message, $error) {
        echo "Failed: {$error->getMessage()}\n";
    })
    ->execute();
```

## Transactions

Atomic ack + push operations:

```php
$queen->transaction()
    ->ack($messages)
    ->queue('notifications')->partition('user-123')->push([
        ['data' => ['notify' => true]],
    ])
    ->queue('audit-log')->push([
        ['data' => ['action' => 'order-processed']],
    ])
    ->commit();
```

### With key/value state and timers

`kv()` and `timers()` commit **with** the acks and pushes, in one transaction.
This is the idempotency idiom: `required: true` escalates a lost precondition
into a refusal of the whole bundle, so a redelivery neither re-pushes nor
re-acks.

```php
$result = $queen->transaction()
    ->ack($message)
    ->kv('saga')->putIfAbsent($orderId, ['step' => 'reserved'], [
        'ttlSeconds' => 86400,
        'required'   => true,
    ])
    ->queue('payments')->push([['data' => $charge]])
    ->timers('payments.timeout')->schedule($orderId, 900_000, ['orderId' => $orderId])
    ->commit();

if (($result['reason'] ?? null) === 'kv_precondition') {
    // Somebody already did this one. Nothing was pushed, nothing was acked.
    // $result carries failedIndex, kvReason, version and value.
}
```

`commit()` **returns** that verdict instead of throwing. A lost precondition is
the expected outcome of every legitimate redelivery, so it belongs in an `if`
and not in a `catch`, and it must not land in your error metrics. Every other
failure still throws.

The transaction is the **primary fence**: a write here shares its fate with the
ack, which compare-and-swap cannot do: an `expect` on a still-matching version
succeeds even from a consumer whose lease has already expired.

## Key/Value State

Part of every broker, like push and pop. There is no flag to turn on and nothing
to probe before the first call.

```php
$kv = $queen->kv();

// Every write declares exactly one of ttlSeconds and forever:true. There is no
// default, deliberately: a put that inherited a TTL is how a marker becomes
// immortal.
$kv->put('saga', 'order-1', ['step' => 'reserved'], ['ttlSeconds' => 3600]);

$got = $kv->get('saga', 'order-1');
if ($got['found']) { /* $got['value'], $got['version'], $got['expiresAt'] */ }

// "Did I win?". Exactly one caller does, and the loser is handed the winner's
// value and version without a second round trip.
$claim = $kv->putIfAbsent('lock', 'job-7', ['owner' => $me], ['ttlSeconds' => 30]);
if (!$claim['applied']) { /* $claim['reason'] === 'exists', $claim['value'] */ }

// Compare-and-swap. expect:0 means "must not exist"; expect:N is a pure update
// that never creates.
$kv->put('saga', 'order-1', ['step' => 'charged'], [
    'ttlSeconds' => 3600,
    'expect'     => $got['version'],
]);

// The way out of CAS. With max, `applied` IS the admission decision: the
// ceiling does not saturate, so a call that would overshoot does not apply and
// hands back the current value.
$rate = $kv->incr('quota', "acme:" . date('Y-m-d-H'), 1, ['ttlSeconds' => 3600, 'max' => 1000]);
if (!$rate['applied']) { /* refuse the request; nothing was spent */ }

$kv->delete('saga', 'order-1');
$kv->getMany('saga', ['a', 'b']);          // {rows, missing}: absence is a datum
$kv->getPrefix('saga', 'order-', ['limit' => 100]);  // keyset page
```

Things worth knowing before they surprise you:

- **Check `applied` and `found`, not the return value.** Every array is truthy
  in PHP, so `if ($kv->delete(...))` is always true. An exception from this API
  means the CALL failed; a lost race is a 200 with `applied:false`.
- **An expired key is never returned and never counts as existing**, even before
  the sweeper prunes its row.
- **`version` is an opaque monotonic token, not a count of writes on that key.**
  It comes from one sequence shared by every key, so consecutive writes jump by
  whatever else drew from it. `expect => $version + 1` is never right.
- **`putIfAbsent` plus a TTL is not a distributed lock.** A lock that expires is
  not revoked: the old holder keeps working, it just no longer has the row. The
  defence is fencing: carry your `version` as `expect` on every later write.
- **The version handed to a loser is advisory**, never a fencing token to reuse
  blindly.
- `getPrefix` lives only in the request body, never in a URL: a prefix in a query
  string is recorded by every access log between you and the database.

## Timers

Part of every broker, like the KV surface.

A timer is a message the broker holds and pushes into a real queue when its
delay elapses, delivered to whatever already consumes that queue, with the
message id the schedule call promised.

```php
$timers = $queen->timers();

// Milliseconds from now. A delay in the past is legal and fires immediately.
$t = $timers->schedule('payments.retry', "order-{$id}", 250, ['orderId' => $id]);
// $t['status'] === 'scheduled', plus messageId, txn and deliverAt

$timers->reschedule('payments.retry', "order-{$id}", 60_000, $payload);
$timers->cancel('payments.retry', "order-{$id}", $t['txn']);
$timers->peek('payments.retry', "order-{$id}");
$timers->list('payments.retry', ['limit' => 100]);
$timers->count('payments.retry', 'laravel:'); // exact pending count for this literal prefix
```

- **`deliverAt` is "not before", never "exactly at".** Two timers on the same
  queue and partition maturing together enter the log in EXPIRY order, not in
  the order they were scheduled.
- **Durations that can be sub-second are in milliseconds** (`delayMs`); the ones
  that cannot are in seconds (`ttlSeconds`). An absolute instant is not
  expressible: there is one clock, the database's.
- **`cancel()` has its own route, and it is the one a full or throttled cluster
  is required never to refuse.** A cancel placed inside a transaction shares the
  bundle's fate instead.
- **`absent` means "no longer pending" and may mean ALREADY DELIVERED.** There
  is no tombstone. The authority is the log: look for the `txn` the cancel hands
  back in the destination queue. Any saga that cancels a compensation timer must
  have the compensation consumer check the saga's KV state before compensating,
  or "the timer fired 5 ms before the cancel" unwinds a reservation that already
  shipped, while the cancel answered `absent` and looked like a success.
- `too_late` means the timer is already claimed and about to be delivered. The
  remedy is a new key, or waiting for the delivery and acting on the message.
- **`count(queue, prefix)` performs one indexed aggregate**, without fetching
  list pages or payloads. The prefix is literal, non-empty and at most 128
  UTF-8 bytes; `%` and `_` are not wildcards. During a rolling upgrade, the
  immediately preceding broker may ignore `mode=count`: the client recognises
  only an exact, well-formed list page, reuses it as page one and continues by
  keyset. Explicit `no_such_route`/`unsupported` verdicts take the same legacy
  path. Malformed pages, broken cursors and transient failures remain errors.

## Direct ACK

```php
// Single ack
$queen->ack($message);

// Batch ack
$queen->ack($messages);

// Nack (mark as failed)
$queen->ack($message, false);

// With consumer group context
$queen->ack($message, true, ['group' => 'processors']);
```

## Lease Renewal

```php
// Renew a single message
$queen->renew($message);

// Renew by lease ID
$queen->renew('lease-id-string');

// Renew multiple
$queen->renew($messages);
```

## Dead Letter Queue

```php
$result = $queen->queue('orders')
    ->dlq('processors')
    ->limit(50)
    ->offset(0)
    ->from('2024-01-01T00:00:00Z')
    ->to('2024-12-31T23:59:59Z')
    ->get();

// $result = ['messages' => [...], 'total' => 123]
```

## Admin API

```php
$admin = $queen->admin();

// Health
$admin->health();

// Resources
$admin->getOverview();
$admin->listQueues();
$admin->getQueue('orders');
$admin->getPartitions(['queue' => 'orders']);

// Per-partition backlog, cheaper than getQueue (watermarks + live leases only).
// Lease-aware brokers add processing / ready / partitionsReady / effectiveReady;
// see "Conflation" above for the effective-depth distinction.
$admin->getQueueDepth('orders');
$admin->getQueueDepth('orders', 'processors');

// Messages
$admin->listMessages(['queue' => 'orders', 'status' => 'pending']);
$admin->getMessage($partitionId, $transactionId);
$admin->deleteMessage($partitionId, $transactionId);
$admin->retryMessage($partitionId, $transactionId);
$admin->moveMessageToDLQ($partitionId, $transactionId);

// Consumer groups
$admin->listConsumerGroups();
$admin->getConsumerGroup('processors');
$admin->getLaggingConsumers(60);
$admin->seekConsumerGroup('processors', 'orders', ['timestamp' => '2024-01-01T00:00:00Z']);

// System
$admin->getMaintenanceMode();
$admin->setMaintenanceMode(true);
$admin->getSystemMetrics();
$admin->getPostgresStats();
```

## Namespace/Task Wildcards

```php
// Pop from all queues in a namespace
$messages = $queen->queue()
    ->namespace('billing')
    ->task('invoices')
    ->batch(10)
    ->pop();

// Consume from namespace/task
$consumer = $queen->queue()
    ->namespace('billing')
    ->group('invoice-processors')
    ->getConsumer();
```

## Tracing

Messages received from consumers include a `trace` callable:

```php
$consumer = $queen->queue('orders')->group('processors')->getConsumer();
$consumer->subscribe();

while (!$consumer->isClosed()) {
    $message = $consumer->consume(1000);
    if ($message === null) continue;

    // Record a trace event
    $trace = $message['trace'];
    $trace([
        'traceName' => ['tenant-acme', 'order-processing'],
        'eventType' => 'info',
        'data' => ['step' => 'started', 'orderId' => $message['payload']['orderId']],
    ]);

    processOrder($message['payload']);

    $trace([
        'data' => ['step' => 'completed'],
    ]);

    $consumer->ack($message);
}
```

## Laravel Artisan Command

```bash
# Basic consumption
php artisan queen:consume orders App\\Handlers\\OrderHandler --group=processors --auto-ack

# With options
php artisan queen:consume orders App\\Handlers\\OrderHandler \
    --group=processors \
    --batch=10 \
    --auto-ack \
    --timeout=30000 \
    --limit=1000

# Subscription mode
php artisan queen:consume events App\\Handlers\\EventHandler \
    --group=watchers \
    --subscription-mode=all \
    --subscription-from=2024-01-01T00:00:00Z

# Conflation: only the newest message per partition (broker >= 1.1.0)
php artisan queen:consume recompute App\\Handlers\\RecomputeHandler \
    --group=workers \
    --conflation
```

Handler class:

```php
namespace App\Handlers;

class OrderHandler
{
    public function handle(array $messageOrMessages): void
    {
        // Process single message or batch
    }
}
```

## Configuration

### Standalone

```php
$queen = new Queen([
    'urls' => ['http://server1:6632', 'http://server2:6632'],
    'loadBalancingStrategy' => 'affinity',  // 'round-robin', 'session', or 'affinity'
    'bearerToken' => 'my-token',
    'timeoutMillis' => 30000,
    'retryAttempts' => 3,
    'retryDelayMillis' => 1000,
    'affinityHashRing' => 128,
    'enableFailover' => true,
    'healthRetryAfterMillis' => 5000,
    'headers' => ['X-Custom' => 'value'],
    'retry429' => [                         // backoff for rate-limited requests
        'maxAttempts' => 10,                // omit: 10 ordinary, unbounded for long-poll pop
        'baseMs' => 500,
        'capMs' => 30000,
    ],
]);
```

### Laravel (`config/queen.php`)

```php
return [
    'url' => env('QUEEN_URL', 'http://localhost:6632'),
    'urls' => env('QUEEN_URLS') ? explode(',', env('QUEEN_URLS')) : null,
    'bearer_token' => env('QUEEN_BEARER_TOKEN'),
    'timeout' => env('QUEEN_TIMEOUT', 30000),
    'retry_attempts' => env('QUEEN_RETRY_ATTEMPTS', 3),
    'load_balancing_strategy' => env('QUEEN_LB_STRATEGY', 'affinity'),
    'headers' => [],
    'queue' => env('QUEEN_QUEUE', 'default'),
    'consumer_group' => env('QUEEN_CONSUMER_GROUP', 'laravel'),
    'partitions' => env('QUEEN_PARTITIONS', 64),
    'partition_prefix' => env('QUEEN_PARTITION_PREFIX', 'laravel'),
    'retry_after' => env('QUEEN_RETRY_AFTER', 90),
    'block_for' => env('QUEEN_BLOCK_FOR', 0),
    'after_commit' => env('QUEEN_AFTER_COMMIT', false),
    'sync_failed_jobs' => env('QUEEN_SYNC_FAILED_JOBS', true),
    'retry_429' => [
        'maxAttempts' => env('QUEEN_RETRY_429_MAX_ATTEMPTS'),
        'baseMs' => env('QUEEN_RETRY_429_BASE_MS'),
        'capMs' => env('QUEEN_RETRY_429_CAP_MS'),
    ],
];
```

Keep `sync_failed_jobs=true` whenever final failures are retained in both
Laravel and Queen. Disabling it leaves `queue:forget`, flush and prune unable to
remove broker DLQ snapshots.

`queue:clear` is intentionally not advertised: Queen does not yet have an
atomic primitive that clears ready jobs, active leases and Laravel-owned timers
together. `Admin::clearQueue()` fails locally instead of calling a nonexistent
route, and deleting/recreating the queue is not a safe substitute.

Delayed jobs are Queen timers until they fire. If timer delivery itself reaches
the timer engine's permanent-failure budget, the entry is operationally visible
under the `__timer__` DLQ group but cannot emit Laravel `JobFailed`, because no
Laravel worker ever received it. Alert on that DLQ separately.

## Rate Limiting and Quotas

Behind the Queen proxy a request can be rate limited (HTTP 429) or refused by
a plan/tenant rule (HTTP 403). 429s are retried transparently: the client waits
`Retry-After` seconds when the response carries one, otherwise an exponential
backoff (500ms doubling up to 30s), always with ±20% jitter. Ordinary requests
give up after 10 attempts; a long-poll pop (`wait(true)`, the consumers, the
artisan command) retries for as long as it polls. A 429 is a tenant signal, not
a backend-health one, so it never marks a server unhealthy nor fails over to
another one.

403s are terminal and surface immediately. Both carry a machine-readable code:

```php
use Queen\Exceptions\ErrorCode;
use Queen\Exceptions\HttpException;

try {
    $queen->queue('orders')->push([['data' => ['orderId' => 1]]])->execute();
} catch (HttpException $e) {
    switch ($e->errorCode) {
        case ErrorCode::RATE_LIMITED:            // 429, retries exhausted
        case ErrorCode::QUOTA_EXCEEDED:          // 429, message/rate quota
            break;
        case ErrorCode::CLUSTER_SUSPENDED:       // 403, needs operator action
        case ErrorCode::STORAGE_QUOTA_EXCEEDED:  // 403, needs a bigger plan
        case ErrorCode::FEATURE_GATED:           // 403, not on this plan
        case ErrorCode::FORBIDDEN:               // 403, generic
            break;
        default:                                 // null: pre-proxy broker error
            throw $e;
    }
}
```

`$e->statusCode`, `$e->retryAfterSeconds` (429 only) and the helpers
`$e->isRateLimited()` / `$e->isClusterSuspended()` are available too.

### When an operator pauses KV or timers

KV and timers exist on every broker, so there is no "is it on?" to ask. What an
operator still has is a **runtime kill switch**, same class as maintenance mode:
pulled during an incident on a surface that is already running, and expected to
be pulled back. A call made while it is down answers

- **503** `kv_disabled` / `timers_disabled` with `Retry-After` on the KV and
  timer routes: temporary, come back;
- **403** with the same code for a `kv`/`timers` rider inside a transaction,
  deliberately permanent: a bundle carries messages, and retrying it in a loop
  against a cell somebody has just paused is a storm on the hot path.

Read it off `$e->reason` (`$e->errorCode` stays null: these come from the broker
envelope, not the proxy contract, and `$e->retryAfterSeconds` is parsed for 429s
only). Timer **cancel** and the timer reads are never on the switch, so a timer
you have already promised can always be stopped.

```php
try {
    $kv->put('saga', 'order-1', ['step' => 'reserved'], ['ttlSeconds' => 3600]);
} catch (HttpException $e) {
    if ($e->statusCode === 503 && $e->reason === 'kv_disabled') {
        // Somebody pulled the lever. Back off and come back, or shed this work.
        // Not a capability check: the surface has not gone away.
    }
    throw $e;
}
```

## Graceful Shutdown

The high-level consumer handles SIGINT/SIGTERM automatically:

```php
$consumer = $queen->queue('orders')->group('processors')->getConsumer();
$consumer->subscribe();

while (!$consumer->isClosed()) {
    $message = $consumer->consume(1000);
    if ($message === null) continue;
    processOrder($message['payload']);
    $consumer->ack($message);
}
// Ctrl+C sets isClosed() = true, loop exits cleanly

$consumer->close();
$queen->close(); // Flushes any remaining buffers
```
