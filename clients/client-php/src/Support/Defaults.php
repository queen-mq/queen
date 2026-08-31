<?php

namespace Queen\Support;

class Defaults
{
    public const CLIENT_DEFAULTS = [
        'timeoutMillis' => 30000,
        'retryAttempts' => 3,
        'retryDelayMillis' => 1000,
        'loadBalancingStrategy' => 'affinity',
        'affinityHashRing' => 128,
        'enableFailover' => true,
        'healthRetryAfterMillis' => 5000,
        'bearerToken' => null,
        'headers' => [],
        // HTTP 429 backoff: ['maxAttempts' => int, 'baseMs' => int,
        // 'capMs' => int]. Empty = per-request-kind defaults (Retry429Policy).
        'retry429' => [],
    ];

    public const QUEUE_DEFAULTS = [
        'leaseTime' => 300,
        'retryLimit' => 3,
        'priority' => 0,
        'delayedProcessing' => 0,
        'windowBuffer' => 0,
        'maxSize' => 0,
        'retentionSeconds' => 0,
        'completedRetentionSeconds' => 0,
        'encryptionEnabled' => false,
    ];

    // `batch` and `maxPartitions` here are the AUTOPILOT-OFF defaults. With
    // autopilot on (the default) a knob the caller never set is not defaulted at
    // all -- it is omitted from the pop so the broker sizes it (PopAutopilot).
    // These values are what comes back with QueueBuilder::autopilot(false), or
    // with QUEEN_SDK_POP_AUTOPILOT=off for a whole process.
    public const CONSUME_DEFAULTS = [
        'concurrency' => 1,
        'batch' => 1, // autopilot off only
        'autoAck' => true,
        'wait' => true,
        'timeoutMillis' => 30000,
        'limit' => null,
        'idleMillis' => null,
        'renewLease' => false,
        'renewLeaseIntervalMillis' => null,
        'subscriptionMode' => null,
        'subscriptionFrom' => null,
        'maxPartitions' => 1, // v4 multi-partition pop cap (autopilot off only)
        // Last-value delivery for this consumer GROUP on this queue: a pop of a
        // partition delivers exactly the newest visible message and the ack
        // retires the whole backlog behind it. Declared here, persisted on the
        // group's FIRST registration, and from then on the STORED value wins for
        // every consumer of that group — it is not a per-call flag. Requires a
        // consumer group, and the broker refuses it together with autoAck (a
        // lease-less commit at delivery would turn the guarantee it exists to
        // provide into at-most-once). Broker >= 1.1.0; an older one ignores it,
        // which the response echo makes loud instead of silent (ConflationGuard).
        'conflation' => false,
    ];

    // As in CONSUME_DEFAULTS, `batch` is the autopilot-OFF default.
    public const POP_DEFAULTS = [
        'batch' => 1, // autopilot off only
        'wait' => false,
        'timeoutMillis' => 30000,
        'autoAck' => false,
    ];

    /**
     * Client-side push buffering. `messageCount`/`timeMillis` are the flush
     * triggers; the other three are the backpressure contract, resolved by
     * MessageBuffer::normalizeOptions() where the full reasoning lives.
     */
    public const BUFFER_DEFAULTS = [
        'messageCount' => 100,
        'timeMillis' => 1000,
        // Backpressure bound, in messages. 0 here is the "not set" sentinel,
        // NOT "unbounded": it resolves to 4 * messageCount. Unbounded is not
        // expressible on purpose, because unbounded was the defect. Measured
        // 2026-08-20 on the Go SDK, whose buffer had the same shape as this
        // one: a producer filling at 1.46M msg/s against a 1.0M msg/s flush
        // pipeline accumulated 20.9M messages (11.7 GB RSS) in 45 seconds and
        // lost every one of them at process exit, with zero client-side errors
        // reported anywhere. The bounded version sustains 881,148 msg/s with
        // exact send/receive parity at 71 MB RSS.
        'maxSize' => 0,
        // How long to wait before retrying a batch that failed to send. The
        // batch goes back at the FRONT of the buffer, order preserved, and is
        // retried — never dropped, never silently swallowed. 0 = 250ms.
        'retryDelayMillis' => 250,
        // PHP-ONLY KNOB, and the one deliberate divergence from the Go/JS/Rust
        // SDKs. There, "the buffer is full" parks the producer on a condition
        // variable that a background flusher signals. PHP has no background
        // flusher and no event loop: the flush happens inline, on the add path,
        // in the caller's request. So "block until capacity frees" here means
        // "retry the flush inline until it succeeds", and that loop needs a
        // total deadline, because a web request cannot park forever — PHP's
        // own max_execution_time (30s by default) would kill it, trading lost
        // messages for a hung request. When the deadline expires the add
        // RAISES with the messages still queued, in order: no drop, and no
        // false success. 0 = 5000ms, comfortably inside the default
        // max_execution_time. Note it bounds the RETRY LOOP, checked between
        // attempts — a single in-flight POST is bounded by the HTTP client's
        // own timeoutMillis.
        'maxWaitMillis' => 5000,
    ];
}
