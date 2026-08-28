<?php

$benchmark = config('benchmark');

return [
    'default' => $benchmark['connection'],
    'connections' => [
        'sync' => [
            'driver' => 'sync',
        ],
        'redis' => [
            'driver' => 'redis',
            'connection' => 'queue',
            'queue' => $benchmark['queue'],
            'retry_after' => $benchmark['retry_after'],
            'block_for' => $benchmark['block_for'],
            'after_commit' => false,
        ],
        'queen' => [
            'driver' => 'queen',
            'url' => env('QUEEN_URL', 'http://queen:6632'),
            'urls' => env('QUEEN_URLS'),
            'bearer_token' => env('QUEEN_BEARER_TOKEN'),
            'timeout' => (int) env('QUEEN_TIMEOUT_MS', 30_000),
            'retry_attempts' => (int) env('QUEEN_RETRY_ATTEMPTS', 3),
            'retry_delay' => (int) env('QUEEN_RETRY_DELAY_MS', 100),
            'load_balancing_strategy' => 'affinity',
            'enable_failover' => true,
            'headers' => [],
            'queue' => $benchmark['queue'],
            'consumer_group' => $benchmark['consumer_group'],
            'partitions' => (int) env('QUEEN_PARTITIONS', 64),
            'partition_prefix' => 'benchmark',
            'retry_after' => $benchmark['retry_after'],
            'block_for' => $benchmark['block_for'],
            'after_commit' => false,
        ],
    ],
    'batching' => [
        'database' => 'sqlite',
        'table' => 'job_batches',
    ],
    // Failed jobs are counted by the harness; no fourth persistence backend is
    // introduced into the hot path of any lane.
    'failed' => [
        'driver' => 'null',
    ],
];
