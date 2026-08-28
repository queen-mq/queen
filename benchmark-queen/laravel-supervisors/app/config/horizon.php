<?php

use Illuminate\Support\Str;

$benchmark = config('benchmark');
$fixed = $benchmark['profile'] === 'fixed';

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
    'waits' => [
        'redis:'.$benchmark['queue'] => 60,
    ],
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
    'defaults' => [
        'bench' => [
            'connection' => $benchmark['connection'],
            'queue' => [$benchmark['queue']],
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
        ],
    ],
    'environments' => [
        'benchmark' => [
            'bench' => [],
        ],
    ],
];
