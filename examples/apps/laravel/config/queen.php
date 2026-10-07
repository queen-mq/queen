<?php

// The package's config/queen.php, cut down to what the examples set. The
// package merges its own file under this one, key by key at the top level, so
// every key not named here keeps the package default (the broker URL from
// QUEEN_URL, the consumer group `laravel`, 64 partition stripes).
return [
    // docs:start(app-laravel-supervisor-config)
    'supervisor' => [
        // Read the backlog, start missing workers and reap exited
        // ones every second instead of every three.
        'poll_interval' => 1,
        'shutdown_grace' => 30,
        // The switch for every pool: example:prefork sets it for its run.
        'prefork' => filter_var(
            env('QUEEN_SUPERVISOR_PREFORK', false),
            FILTER_VALIDATE_BOOL,
        ),
        'supervisors' => [
            // Follows the switch: with prefork on, its workers are forked
            // from the fork server.
            'default' => [
                'connection' => 'queen',
                'queues' => [env('EXAMPLE_FORKED_QUEUE', 'default')],
                'balance' => 'simple',
                'processes' => 2,
                'timeout' => 20,
                // The connection long-polls: an idle worker waits on the
                // broker instead of sleeping.
                'sleep' => 0,
            ],
            // Its own prefork wins over the switch (PHP client 2.3.0): its
            // workers are spawned, and each boots Laravel on its own.
            'isolated' => [
                'connection' => 'queen',
                'queues' => [env('EXAMPLE_SPAWNED_QUEUE', 'isolated')],
                'balance' => 'simple',
                'processes' => 2,
                'timeout' => 20,
                'sleep' => 0,
                'prefork' => false,
            ],
        ],
    ],
    // docs:end
];
