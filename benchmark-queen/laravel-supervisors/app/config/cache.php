<?php

return [
    // The benchmark lanes keep the per-process array store. The Laravel
    // compatibility lanes set CACHE_STORE=database: unique jobs,
    // WithoutOverlapping, the rate limiter and maxExceptions need a cache
    // shared by every process, with atomic locks.
    'default' => env('CACHE_STORE', 'array'),
    'stores' => [
        'array' => [
            'driver' => 'array',
            'serialize' => false,
        ],
        'database' => [
            'driver' => 'database',
            'connection' => null,
            'table' => 'cache',
            'lock_connection' => null,
            'lock_table' => 'cache_locks',
        ],
    ],
    'prefix' => 'queen-supervisor-benchmark-cache-',
];
