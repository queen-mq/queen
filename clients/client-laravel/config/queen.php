<?php

return [
    'url' => env('QUEEN_URL', 'http://localhost:6632'),
    'urls' => env('QUEEN_URLS') ? explode(',', env('QUEEN_URLS')) : null,
    'bearer_token' => env('QUEEN_BEARER_TOKEN'),
    'timeout' => env('QUEEN_TIMEOUT', 30000),
    'retry_attempts' => env('QUEEN_RETRY_ATTEMPTS', 3),
    'retry_delay' => env('QUEEN_RETRY_DELAY', 1000),
    'load_balancing_strategy' => env('QUEEN_LB_STRATEGY', 'affinity'),
    'enable_failover' => env('QUEEN_ENABLE_FAILOVER', true),
    'affinity_hash_ring' => env('QUEEN_AFFINITY_HASH_RING', 150),
    'health_retry_after' => env('QUEEN_HEALTH_RETRY_AFTER', 30000),
    'headers' => [],

    // Laravel queue driver. Use with QUEUE_CONNECTION=queen and Laravel's
    // standard `php artisan queue:work queen` worker.
    'queue' => env('QUEEN_QUEUE', 'default'),
    'consumer_group' => env('QUEEN_CONSUMER_GROUP', 'laravel'),
    // Fixed stripes preserve concurrency without creating one partition per
    // job. Jobs implementing QueenPartitionable override the stripe with an
    // explicit per-entity ordering key.
    'partitions' => env('QUEEN_PARTITIONS', 64),
    'partition_prefix' => env('QUEEN_PARTITION_PREFIX', 'laravel'),
    // Must be longer than the Laravel worker/job timeout.
    'retry_after' => env('QUEEN_RETRY_AFTER', 90),
    // Seconds to long-poll. Keep 0 when workers consume priority queues in a
    // comma-separated list, otherwise the first empty queue delays the rest.
    'block_for' => env('QUEEN_BLOCK_FOR', 0),
    // Opt-in throughput controls. Prefetch leases multiple jobs in one broker
    // request; ack_batch defers successful ACK confirmation until the threshold
    // or the fetched lease is drained. Values greater than one preserve
    // at-least-once delivery but widen the duplicate/recovery window, so size
    // retry_after for the maximum time needed to process a prefetched batch.
    'prefetch' => env('QUEEN_PREFETCH', 1),
    'ack_batch' => env('QUEEN_ACK_BATCH', 1),
    // Laravel Queue::bulk() is emitted as bounded multi-partition HTTP pushes.
    'bulk_batch' => env('QUEEN_BULK_BATCH', 100),
    'after_commit' => env('QUEEN_AFTER_COMMIT', false),
    // Keep Laravel failed_jobs as the command index while retaining Queen DLQ
    // snapshots. Retry/forget/flush/prune remove the matching DLQ row.
    'sync_failed_jobs' => env('QUEEN_SYNC_FAILED_JOBS', true),

    // Local process orchestration. The PHP and Rust engines consume the same
    // resolved JSON contract exposed by `queen:supervisor-config`.
    'supervisor' => [
        'poll_interval' => env('QUEEN_SUPERVISOR_POLL_INTERVAL', 3),
        'http_timeout' => env('QUEEN_SUPERVISOR_HTTP_TIMEOUT', 5),
        'read_bearer_token' => env('QUEEN_SUPERVISOR_READ_BEARER_TOKEN'),
        'shutdown_grace' => env('QUEEN_SUPERVISOR_SHUTDOWN_GRACE', 75),
        'process_limit' => env('QUEEN_SUPERVISOR_PROCESS_LIMIT', 256),
        'state_directory' => env('QUEEN_SUPERVISOR_STATE_DIRECTORY', storage_path('queen-supervisor')),
        'telemetry_ttl' => env('QUEEN_SUPERVISOR_TELEMETRY_TTL', 300),
        'supervisors' => [
            'default' => [
                'connection' => 'queen',
                'consumer_group' => env('QUEEN_CONSUMER_GROUP', 'laravel'),
                'queues' => [env('QUEEN_QUEUE', 'default')],
                'balance' => env('QUEEN_SUPERVISOR_BALANCE', 'auto'),
                'strategy' => env('QUEEN_SUPERVISOR_STRATEGY', 'size'),
                'min_processes' => env('QUEEN_SUPERVISOR_MIN_PROCESSES', 1),
                'max_processes' => env('QUEEN_SUPERVISOR_MAX_PROCESSES', 10),
                'target_jobs_per_process' => env('QUEEN_SUPERVISOR_TARGET_JOBS', 10),
                'target_clear_seconds' => env('QUEEN_SUPERVISOR_TARGET_CLEAR_SECONDS', 60),
                'default_runtime_seconds' => env('QUEEN_SUPERVISOR_DEFAULT_RUNTIME_SECONDS', 1),
                'balance_cooldown' => env('QUEEN_SUPERVISOR_BALANCE_COOLDOWN', 3),
                'balance_max_shift' => env('QUEEN_SUPERVISOR_BALANCE_MAX_SHIFT', 1),
                // Require a lower target to remain stable before removing
                // capacity; worker crashes use a separate capped backoff.
                'scale_down_delay' => env('QUEEN_SUPERVISOR_SCALE_DOWN_DELAY', 10),
                'restart_backoff' => env('QUEEN_SUPERVISOR_RESTART_BACKOFF', 1),
                'restart_backoff_max' => env('QUEEN_SUPERVISOR_RESTART_BACKOFF_MAX', 30),
                'stable_after' => env('QUEEN_SUPERVISOR_STABLE_AFTER', 60),
                'sleep' => 1,
                'timeout' => 60,
                'retry_after' => env('QUEEN_RETRY_AFTER', 90),
                'tries' => 3,
                'memory' => 128,
                'backoff' => 0,
                'max_jobs' => 0,
                'max_time' => 0,
                'rest' => 0,
                'force' => false,
                // Avoid per-job console I/O in daemon mode.
                'quiet' => true,
            ],
        ],
    ],

    // Backoff for HTTP 429 (rate limited by the proxy), independent of the
    // retry_attempts above. Nulls keep the per-request-kind defaults: 10
    // attempts for ordinary requests, unbounded for long-poll pops, 500ms
    // base doubling up to a 30s cap. A Retry-After header always wins.
    'retry_429' => [
        'maxAttempts' => env('QUEEN_RETRY_429_MAX_ATTEMPTS'),
        'baseMs' => env('QUEEN_RETRY_429_BASE_MS'),
        'capMs' => env('QUEEN_RETRY_429_CAP_MS'),
    ],
];
