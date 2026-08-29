<?php

return [
    'default' => 'sqlite',
    'connections' => [
        // The application never queries this database. Keeping a valid default
        // lets Laravel services resolve without adding a result-store backend.
        'sqlite' => [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
            'foreign_key_constraints' => false,
        ],
    ],
    'migrations' => [
        'table' => 'migrations',
        'update_date_on_publish' => false,
    ],
    'redis' => [
        'client' => 'phpredis',
        'options' => [
            'cluster' => 'redis',
            'prefix' => (string) env('REDIS_PREFIX', 'queen-benchmark:'),
            'persistent' => false,
        ],
        // Horizon's control-plane state is separate from the queue keys while
        // both remain in the same measured Redis server.
        'default' => [
            'host' => env('REDIS_HOST', 'redis'),
            'username' => env('REDIS_USERNAME'),
            'password' => env('REDIS_PASSWORD'),
            'port' => (int) env('REDIS_PORT', 6379),
            'database' => (int) env('REDIS_HORIZON_DB', 1),
        ],
        'queue' => [
            'host' => env('REDIS_HOST', 'redis'),
            'username' => env('REDIS_USERNAME'),
            'password' => env('REDIS_PASSWORD'),
            'port' => (int) env('REDIS_PORT', 6379),
            'database' => (int) env('REDIS_QUEUE_DB', 0),
        ],
    ],
];
