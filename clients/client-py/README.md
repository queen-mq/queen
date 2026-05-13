# Queen MQ - Python Client

<div align="center">

**Modern, high-performance message queue client for Python**

[![PyPI](https://img.shields.io/pypi/v/queen-mq.svg)](https://pypi.org/project/queen-mq/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-brightgreen.svg)](https://www.python.org/)

[Quick Start](#quick-start) • [Features](#features) • [Documentation](#documentation) • [Examples](#examples)

</div>

---

## What is Queen MQ?

Queen MQ is a PostgreSQL-backed message queue system with a powerful feature set:

- **FIFO Partitions** - Unlimited ordered partitions within queues
- **Consumer Groups** - Kafka-style consumer groups for scalability
- **Flexible Semantics** - Exactly-once, at-least-once, and at-most-once delivery
- **Transactions** - Atomic operations across push and ack
- **High Performance** — 104K msg/s push, 165K msg/s fan-out with consumer groups on a single 32-core node ([benchmarks](https://github.com/queen-mq/queen/tree/master/benchmark-queen/2026-04-26))
- **Subscription Modes** - Process from beginning, new messages only, or from timestamp
- **Dead Letter Queue** - Automatic failure handling and monitoring
- **Message Tracing** - Debug distributed workflows with trace timelines
- **Client-Side Buffering** - 10x-100x throughput boost for high-volume pushes
- **Real-time Streaming** - Windowed aggregation and processing

This client provides a fluent, async/await API for Python applications.

---

## Installation

```bash
pip install queen-mq
```

**Requirements:** Python 3.8+

---

## Quick Start

```python
import asyncio
from queen import Queen

async def process(message):
    print('Processing:', message['data'])
    # Auto-ack on success, auto-retry on error

async def main():
    # Connect to Queen server
    async with Queen('http://localhost:6632') as queen:
        # Create a queue
        await queen.queue('tasks').create()

        # Push messages
        await queen.queue('tasks').push([
            {'data': {'task': 'send-email', 'to': 'alice@example.com'}}
        ])

        # Consume messages (handler is a regular async function)
        await queen.queue('tasks').consume(process)

asyncio.run(main())
```

---

## Core Concepts

### Queues

Logical containers for messages with configurable settings:

```python
await queen.queue('orders').config({
    'leaseTime': 300,          # 5 minutes
    'retryLimit': 3,
    'priority': 5,
    'encryptionEnabled': False
}).create()
```

### Partitions

Ordered lanes within a queue:

```python
# All messages for user-123 are processed in order
await queen.queue('user-events').partition('user-123').push([
    {'data': {'event': 'login'}},
    {'data': {'event': 'view-page'}},
    {'data': {'event': 'logout'}}
])
```

### Consumer Groups

Multiple consumers sharing work:

```python
async def send_handler(msg):
    await send_email(msg['data'])

async def analytics_handler(msg):
    await log_metrics(msg['data'])

# Worker 1 & 2 share the load
await queen.queue('emails').group('processors').consume(send_handler)

# Separate group processes same messages independently
await queen.queue('emails').group('analytics').consume(analytics_handler)
```

### Subscription Modes

Control whether consumer groups process historical messages:

```python
async def handler(msg):
    ...

# Default: Process ALL messages (including backlog)
await queen.queue('events').group('batch-analytics').consume(handler)

# Skip history, only new messages
await queen.queue('events').group('realtime-monitor').subscription_mode('new').consume(handler)

# Start from specific timestamp
await queen.queue('events').group('replay').subscription_from('2025-10-28T10:00:00.000Z').consume(handler)
```

---

## Connection Options

### Single Server

```python
queen = Queen('http://localhost:6632')
```

### Multiple Servers (High Availability)

```python
queen = Queen(['http://server1:6632', 'http://server2:6632'])
```

### Full Configuration

```python
queen = Queen({
    'urls': ['http://server1:6632', 'http://server2:6632'],
    'timeout_millis': 30000,
    'retry_attempts': 3,
    'load_balancing_strategy': 'affinity',  # or 'round-robin', 'session'
    'enable_failover': True
})
```

---

## Basic Usage Patterns

### Push Messages

```python
# Simple push
await queen.queue('tasks').push([
    {'data': {'job': 'resize-image', 'imageId': 123}}
])

# With partition
await queen.queue('tasks').partition('tenant-456').push([
    {'data': {'action': 'process'}}
])

# With custom transaction ID (for exactly-once)
await queen.queue('tasks').push([
    {'transactionId': 'unique-id-123', 'data': {'value': 42}}
])
```

### Consume Messages (Long-Running Workers)

```python
# Single message processing (batch=1, default)
# Handler receives a single message
async def single_handler(message):
    await process_task(message['data'])
    # Auto-ack on success, auto-retry on error

await queen.queue('tasks').concurrency(10).consume(single_handler)

# Batch processing (batch>1)
# Handler receives an array of messages
async def batch_handler(messages):
    for message in messages:
        await process_task(message['data'])

await queen.queue('tasks').batch(20).concurrency(5).consume(batch_handler)

# Process with limit and stop
await queen.queue('tasks').limit(100).consume(single_handler)
```

### Pop Messages (On-Demand Processing)

```python
# Grab messages manually
messages = await queen.queue('tasks').batch(10).wait(True).pop()

# Manual acknowledgment
for message in messages:
    try:
        await process_message(message['data'])
        await queen.ack(message, True)  # Success
    except Exception as error:
        await queen.ack(message, False)  # Retry
```

### Multi-Partition Pop (Drain Many Partitions Per Call)

```python
# One round-trip drains up to 200 messages spread across up to 50 partitions.
# batch(200) is the GLOBAL cap on total messages; partitions(50) is the
# hard cap on partitions claimed. All claimed partitions share one leaseId
# — a single renew() call extends every partition's lease atomically.
messages = await (queen.queue('events')
                  .batch(200)
                  .partitions(50)
                  .wait(True)
                  .pop())

# Each message carries its own partition info (per-message partitionId,
# partition name, leaseId, consumerGroup) — ACK and renew always work
# message-by-message regardless of how many partitions the batch spans.
for m in messages:
    print(f"from {m['partition']}:", m['data'])

# Same builder works on .consume() for long-running workers
async def handler(msgs):
    for m in msgs:
        await process(m['data'])

await (queen.queue('events')
       .batch(100)
       .partitions(8)
       .consume(handler))
```

**When to use:** queues with many partitions where each partition only has
a handful of new messages per polling interval (per-customer event streams,
per-tenant work queues, per-device telemetry). Reduces network round-trips
from O(P) to O(P / N) while preserving per-partition FIFO ordering.

**When not to use:** few partitions, or each one busy enough to fill
`batch(B)` on its own. Default is `partitions(1)` which preserves the
legacy single-partition behaviour.

`.partitions(N)` only applies to **wildcard** pops; specifying
`.partition('name')` ignores the cap.

### Transactions (Atomic Operations)

```python
# Pop from queue A
messages = await queen.queue('input').pop()

# Atomically: ack input AND push output
await (queen.transaction()
    .ack(messages[0])
    .queue('output')
    .push([{'data': processed_result}])
    .commit())
```

### Client-Side Buffering (High Throughput)

```python
# Buffer messages locally, batch to server
for i in range(10000):
    await queen.queue('events').buffer({'message_count': 500, 'time_millis': 1000}).push([
        {'data': {'id': i}}
    ])

# Flush remaining buffered messages
await queen.flush_all_buffers()

# Result: 10x-100x faster than individual pushes
```

### Dead Letter Queue

```python
# Enable DLQ on queue
await queen.queue('risky').config({'retryLimit': 3, 'dlqAfterMaxRetries': True}).create()

# Query failed messages
dlq = await queen.queue('risky').dlq().limit(10).get()

print(f"Found {dlq['total']} failed messages")
for msg in dlq['messages']:
    print('Error:', msg.get('errorMessage'))
```

### Message Tracing

```python
async def order_handler(msg):
    order_id = msg['data']['orderId']

    # Record trace with name for cross-service correlation
    await msg['trace']({
        'traceName': f"order-{order_id}",
        'eventType': 'info',
        'data': {'text': 'Order processing started'}
    })

    await process_order(msg['data'])

    await msg['trace']({
        'traceName': f"order-{order_id}",
        'eventType': 'processing',
        'data': {'text': 'Order completed', 'total': msg['data']['total']}
    })

await queen.queue('orders').consume(order_handler)

# View traces in webapp: Traces → Search "order-12345"
```

---

## API Reference

### Queue Operations

```python
# Create
await queen.queue('my-queue').create()
await queen.queue('my-queue').config({'priority': 5}).create()

# Delete
await queen.queue('my-queue').delete()
```

### Push

```python
await queen.queue('q').push([{'data': {'value': 1}}])
await queen.queue('q').partition('p1').push([{'data': {'value': 1}}])
await queen.queue('q').buffer({'message_count': 100, 'time_millis': 1000}).push([...])
```

### Pop

```python
msgs = await queen.queue('q').pop()
msgs = await queen.queue('q').batch(10).pop()
msgs = await queen.queue('q').batch(10).wait(True).pop()
msgs = await queen.queue('q').batch(200).partitions(50).pop()  # multi-partition pop
```

### Consume

```python
# batch=1 (default): handler receives single message
async def single_handler(msg):
    ...

await queen.queue('q').consume(single_handler)

# batch>1: handler receives array of messages
async def batch_handler(msgs):
    ...

await queen.queue('q').batch(10).consume(batch_handler)

# Other options
await queen.queue('q').limit(10).consume(single_handler)
await queen.queue('q').concurrency(5).consume(single_handler)
await queen.queue('q').group('my-group').consume(single_handler)
```

### Acknowledgment

```python
await queen.ack(message, True)   # Success
await queen.ack(message, False)  # Retry
await queen.ack(message, False, {'error': 'reason'})
await queen.ack([msg1, msg2], True)  # Batch ack
```

### Transactions

```python
await (queen.transaction()
    .ack(message)
    .queue('output')
    .push([{'data': {'result': 'processed'}}])
    .commit())
```

### Lease Renewal

```python
await queen.renew(message)
await queen.renew([msg1, msg2, msg3])

async def handler(msg):
    ...
await queen.queue('q').renew_lease(True, 60000).consume(handler)
```

### Buffering

```python
await queen.flush_all_buffers()
await queen.queue('q').flush_buffer()
stats = queen.get_buffer_stats()
```

### Dead Letter Queue

```python
dlq = await queen.queue('q').dlq().limit(10).get()
dlq = await queen.queue('q').dlq('consumer-group').limit(10).get()
```

### Shutdown

```python
await queen.close()  # Flush buffers and close connections
```

---

## Configuration Defaults

### Client Defaults

```python
{
    'timeout_millis': 30000,
    'retry_attempts': 3,
    'retry_delay_millis': 1000,
    'load_balancing_strategy': 'affinity',
    'enable_failover': True
}
```

### Queue Defaults

```python
{
    'leaseTime': 300,           # 5 minutes
    'retryLimit': 3,
    'priority': 0,
    'delayedProcessing': 0,
    'windowBuffer': 0,
    'maxSize': 0,              # Unlimited
    'retentionSeconds': 0,     # Keep forever
    'encryptionEnabled': False
}
```

### Consume Defaults

```python
{
    'concurrency': 1,
    'batch': 1,
    'auto_ack': True,
    'wait': True,              # Long polling
    'timeout_millis': 30000,
    'limit': None,             # Run forever
    'renew_lease': False
}
```

---

## Logging

Enable detailed logging for debugging:

```bash
export QUEEN_CLIENT_LOG=true
python your_app.py
```

Example output:
```
[2025-10-28T10:30:45.123Z] [INFO] [Queen.constructor] {"status":"initialized","urls":1}
[2025-10-28T10:30:45.234Z] [INFO] [QueueBuilder.push] {"queue":"tasks","partition":"Default","count":5}
```

---

## Type Hints

Full type hints included for IDE support:

```python
from queen import Queen, Message
from typing import Dict, Any

queen: Queen = Queen('http://localhost:6632')

async def handler(message: Message) -> None:
    data: Dict[str, Any] = message['data']
    print(data)

await queen.queue('orders').consume(handler)
```

---

## Best Practices

1. ✅ **Use `consume()` for workers** - Simpler API, handles retries automatically
2. ✅ **Use `pop()` for control** - When you need precise control over acking
3. ✅ **Buffer for speed** - Always use buffering when pushing many messages
4. ✅ **Partitions for order** - Use partitions when message order matters
5. ✅ **Consumer groups for scale** - Run multiple workers in the same group
6. ✅ **Transactions for consistency** - Use transactions for atomic operations
7. ✅ **Enable DLQ** - Always enable DLQ in production
8. ✅ **Renew long leases** - Use auto-renewal for long-running tasks
9. ✅ **Graceful shutdown** - Use async context manager or call `queen.close()`
10. ✅ **Monitor DLQ** - Regularly check for failed messages

### 📝 Important Notes

**Handler Signatures:**
- When `batch=1` (default), handler receives a **single message**: `async def handler(message): ...`
- When `batch>1`, handler receives an **array of messages**: `async def handler(messages): ...`
- When `each=True`, always receives single messages regardless of batch size

---

## Documentation

- **[Node.js Client](../client-js/README.md)** — Node.js client documentation
- **[HTTP API Reference](https://github.com/queen-mq/queen/blob/master/server/API.md)** — raw HTTP endpoints
- **[Server Guide](https://github.com/queen-mq/queen/blob/master/server/README.md)** — server setup and configuration
- **[Architecture & internals](https://queenmq.com/architecture.html)** — published architecture overview
- **[libqueen design notes](https://github.com/queen-mq/queen/blob/master/cdocs/LIBQUEEN_IMPROVEMENTS.md)** — adaptive engine deep-dive

---

## License

Apache 2.0 - See [LICENSE](../LICENSE.md)

---

## Support

- **GitHub:** [queen-mq/queen](https://github.com/queen-mq/queen)
- **Issues:** [GitHub Issues](https://github.com/queen-mq/queen/issues)
- **LinkedIn:** [Smartness](https://www.linkedin.com/company/smartness-com/)

