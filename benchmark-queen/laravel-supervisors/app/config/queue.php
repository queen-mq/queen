<?php

$benchmark = config('benchmark');

// The routed lanes' pool connections, as cb3 declares them: no lease setting
// of their own (the lease is retry_after), after_commit on, one partition
// for an ordered pool. Every Queen pool connection names the same default
// queue; the router passes the real one with each dispatch.
$pools = [];
if ($benchmark['routed']) {
    $pools['routed'] = ['driver' => 'routed', 'fallback' => $benchmark['connection']];
    foreach ($benchmark['routed_pools'] as $pool => $settings) {
        $pools["redis-{$pool}"] = [
            'driver' => 'redis',
            'connection' => 'queue',
            'queue' => $settings['queues'][0],
            'retry_after' => $settings['retry_after'],
            'block_for' => null,
            'after_commit' => true,
        ];
        $pools["queen-{$pool}"] = [
            'driver' => 'queen',
            'queue' => 'cb-backend.' . config('app.env') . '.default',
            'retry_after' => $settings['retry_after'],
            'partitions' => $settings['partitions'],
            'after_commit' => true,
        ];
    }
}

return [
    'default' => $benchmark['routed'] ? 'routed' : $benchmark['connection'],
    'connections' => $pools + [
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
            'partitions' => $benchmark['queen_partitions'],
            'partition_prefix' => 'benchmark',
            'retry_after' => $benchmark['retry_after'],
            'block_for' => $benchmark['block_for'],
            'prefetch' => $benchmark['queen_prefetch'],
            'ack_batch' => $benchmark['queen_ack_batch'],
            'ack_async' => $benchmark['queen_ack_async'],
            'pop_ahead' => $benchmark['queen_pop_ahead'],
            'bulk_batch' => $benchmark['queen_bulk_batch'],
            'after_commit' => false,
        ],
    ],
    'batching' => [
        'database' => 'sqlite',
        'table' => 'job_batches',
    ],
    // Timed lanes default to null so they do not gain a fourth persistence
    // backend. GA failure probes explicitly select the shared file driver.
    'failed' => [
        'driver' => $benchmark['failed_driver'],
        'path' => $benchmark['failed_path'],
        'limit' => $benchmark['failed_limit'],
    ],
];
