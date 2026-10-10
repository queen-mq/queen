<?php

use Illuminate\Support\Str;

$benchmark = config('benchmark');
$fixed = $benchmark['profile'] === 'fixed';

$supervisor = [
    'connection' => $benchmark['connection'],
    'queue' => $benchmark['queues'],
    'balance' => $fixed ? 'simple' : 'auto',
    'autoScalingStrategy' => $benchmark['strategy'],
    'processes' => $fixed ? $benchmark['workers'] : $benchmark['max_workers'],
    'minProcesses' => $benchmark['min_workers'],
    'maxProcesses' => $benchmark['max_workers'],
    'balanceMaxShift' => $benchmark['balance_max_shift'],
    'balanceCooldown' => $benchmark['balance_cooldown'],
    'maxTime' => 0,
    'maxJobs' => 0,
    'memory' => $benchmark['worker_memory'],
    'tries' => 1,
    'timeout' => $benchmark['timeout'],
    'sleep' => $benchmark['worker_sleep'],
    'rest' => 0,
    'nice' => 0,
    'force' => false,
];
$supervisors = ['bench' => $supervisor];
if ($benchmark['routed']) {
    // The routed lanes: one Horizon supervisor per pool, on its `redis-<pool>`
    // connection, with the pool's timeout and tries. No balancing: every
    // worker works the pool's queues in order, as the Queen pools do.
    $supervisors = [];
    foreach ($benchmark['routed_pools'] as $name => $settings) {
        $supervisors[$name] = array_replace($supervisor, [
            'connection' => "redis-{$name}",
            'queue' => $settings['queues'],
            'balance' => false,
            'processes' => $settings['processes'],
            'minProcesses' => $settings['processes'],
            'maxProcesses' => $settings['processes'],
            'timeout' => $settings['timeout'],
            'tries' => $settings['tries'],
        ]);
    }
}

return [
    'name' => env('HORIZON_NAME', 'benchmark-horizon'),
    'domain' => null,
    'path' => 'horizon',
    'use' => 'default',
    'prefix' => env(
        'HORIZON_PREFIX',
        Str::slug((string) env('APP_NAME', 'queen-supervisor-benchmark'), '_').'_horizon:',
    ),
    'middleware' => [],
    'waits' => array_fill_keys(
        array_map(fn (string $queue): string => 'redis:'.$queue, $benchmark['queues']),
        60,
    ),
    'trim' => [
        'recent' => 60,
        'pending' => 60,
        'completed' => 60,
        'recent_failed' => 60,
        'failed' => 60,
        'monitored' => 60,
    ],
    'silenced' => [],
    'silenced_tags' => [],
    'metrics' => [
        'trim_snapshots' => [
            'job' => 24,
            'queue' => 24,
        ],
    ],
    'fast_termination' => false,
    'memory_limit' => (int) env('HORIZON_MEMORY_LIMIT', 128),
    'defaults' => $supervisors,
    'environments' => [
        'benchmark' => array_map(static fn (): array => [], $supervisors),
    ],
];
