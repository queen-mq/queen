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
