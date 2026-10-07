<?php

// Laravel merges these connections with the framework's own (sync, database,
// redis). Every Queen connection takes the defaults of config/queen.php, the
// broker URL included (QUEEN_URL), and overrides only the keys it names.
return [
    'default' => 'queen',

    'connections' => [
        // docs:start(app-laravel-connections)
        // The connection most examples use: one job per pop. An idle worker
        // long-polls for a second (block_for) instead of sleeping, so it takes
        // the next job as soon as the broker has one.
        'queen' => [
            'driver' => 'queen',
            'retry_after' => 30,
            'block_for' => 1,
        ],

        // prefetch 'auto': each pop asks for about 250 ms of work, 1 to 16
        // jobs. The jobs of a batch wait, leased, while the ones before them
        // run, so a prefetch above 1 needs lease renewal.
        'queen-auto' => [
            'driver' => 'queen',
            'retry_after' => 30,
            'block_for' => 1,
            'prefetch' => 'auto',
            'lease_renewal' => true,
        ],

        // A lease of 6 seconds that nothing renews.
        'queen-short-lease' => [
            'driver' => 'queen',
            'retry_after' => 6,
            'block_for' => 1,
        ],

        // The same lease, renewed every second while the job runs. The
        // connection refuses a timing that may not fit in the lease: the
        // interval, two request budgets, one second, the kill grace and the
        // safety margin must add up to less than retry_after (1+1+1+1+0+1 = 5).
        'queen-renewed-lease' => [
            'driver' => 'queen',
            'retry_after' => 6,
            'block_for' => 1,
            'lease_renewal' => true,
            'lease_renewal_interval' => 1,
            'lease_renewal_timeout' => 1,
            'lease_renewal_kill_grace' => 0,
            'lease_renewal_safety_margin' => 1,
        ],
        // docs:end
    ],

    // No job here fails for good, and there is no database to keep a
    // failed_jobs table in.
    'failed' => [
        'driver' => 'null',
    ],
];
