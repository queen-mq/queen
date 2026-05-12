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
];
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
