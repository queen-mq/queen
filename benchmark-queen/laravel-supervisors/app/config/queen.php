<?php

$benchmark = config('benchmark');
$fixed = $benchmark['profile'] === 'fixed';
$leaseRenewal = (bool) env('QUEEN_LEASE_RENEWAL', false);

$pool = [
    'connection' => $benchmark['connection'],
    'consumer_group' => $benchmark['consumer_group'],
    'queues' => $benchmark['queues'],
    'balance' => $fixed ? 'simple' : 'auto',
    'strategy' => $benchmark['strategy'],
    'processes' => $benchmark['workers'],
    'min_processes' => $benchmark['min_workers'],
    'max_processes' => $benchmark['max_workers'],
    'target_jobs_per_process' => $benchmark['target_jobs_per_process'],
    'target_clear_seconds' => $benchmark['target_clear_seconds'],
    'default_runtime_seconds' => $benchmark['default_runtime_seconds'],
    'balance_cooldown' => $benchmark['balance_cooldown'],
    'balance_max_shift' => $benchmark['balance_max_shift'],
    'fast_scale_up' => (bool) env('QUEEN_SUPERVISOR_FAST_SCALE_UP', false),
    'scale_down_delay' => $benchmark['scale_down_delay'],
    'restart_backoff' => 1,
    'restart_backoff_max' => 30,
    'stable_after' => 60,
    'sleep' => $benchmark['worker_sleep'],
    'timeout' => $benchmark['timeout'],
    'retry_after' => $benchmark['retry_after'],
    'tries' => 1,
    'memory' => $benchmark['worker_memory'],
    'backoff' => 0,
    'max_jobs' => 0,
    'max_time' => 0,
    'rest' => 0,
    'force' => false,
    'quiet' => true,
];
$supervisors = ['bench' => $pool];
$workers = $benchmark['max_workers'];
$shutdownGrace = $benchmark['timeout'] + 15;
if ($benchmark['routed']) {
    // One pool per routed pool, on its `queen-<pool>` connection and the Queen
    // names of its queues. Like cb3's, they name no consumer group: they work
    // their connection's. Balance off: every worker of a pool works its
    // queues in order, as Horizon's `balance => false` does, so all of them
    // serve the first queue, where the compatibility scenarios dispatch.
    // Balance auto: cb3's, workers follow each queue's backlog.
    $supervisors = [];
    foreach ($benchmark['routed_pools'] as $name => $settings) {
        $supervisors[$name] = array_diff_key(array_replace($pool, [
            'connection' => "queen-{$name}",
            'queues' => array_map(static fn (string $queue): string => 'cb-backend.' . config('app.env') . ".{$queue}", $settings['queues']),
            'balance' => $benchmark['routed_balance'],
            'processes' => $settings['min'],
            'min_processes' => $settings['min'],
            'max_processes' => $settings['max'],
            'timeout' => $settings['timeout'],
            'retry_after' => $settings['retry_after'],
            'tries' => $settings['tries'],
        ]), ['consumer_group' => true]);
    }
    $workers = array_sum(array_column($benchmark['routed_pools'], 'max'));
    // cb3's 330 s: the longest pool timeout, 300 s, and 30 s more.
    $shutdownGrace = max(array_column($benchmark['routed_pools'], 'timeout')) + 30;
}

return [
    'url' => env('QUEEN_URL', 'http://queen:6632'),
    'urls' => env('QUEEN_URLS'),
    'bearer_token' => env('QUEEN_BEARER_TOKEN'),
    'timeout' => (int) env('QUEEN_TIMEOUT_MS', 30_000),
    'retry_attempts' => (int) env('QUEEN_RETRY_ATTEMPTS', 3),
    'retry_delay' => (int) env('QUEEN_RETRY_DELAY_MS', 100),
    'load_balancing_strategy' => 'affinity',
    'enable_failover' => true,
    'affinity_hash_ring' => 150,
    'health_retry_after' => 1_000,
    'headers' => [],
    'queue' => $benchmark['queue'],
    'consumer_group' => $benchmark['consumer_group'],
    'partitions' => $benchmark['queen_partitions'],
    'partition_prefix' => 'benchmark',
    'retry_after' => $benchmark['retry_after'],
    'block_for' => $benchmark['block_for'],
    'prefetch' => $benchmark['queen_prefetch'],
    'ack_batch' => $benchmark['queen_ack_batch'],
    'lease_renewal' => $leaseRenewal,
    'lease_renewal_interval' => env('QUEEN_LEASE_RENEWAL_INTERVAL'),
    'lease_renewal_timeout' => env('QUEEN_LEASE_RENEWAL_TIMEOUT', 5),
    'lease_renewal_kill_grace' => env('QUEEN_LEASE_RENEWAL_KILL_GRACE', 2),
    'lease_renewal_safety_margin' => env('QUEEN_LEASE_RENEWAL_SAFETY_MARGIN', 1),
    'bulk_batch' => $benchmark['queen_bulk_batch'],
    'after_commit' => false,
    'sync_failed_jobs' => $benchmark['failed_driver'] !== 'null',
    'supervisor' => [
        'poll_interval' => $benchmark['poll_interval'],
        'http_timeout' => (int) env('QUEEN_SUPERVISOR_HTTP_TIMEOUT', 5),
        'shutdown_grace' => $shutdownGrace,
        // Renewal uses one lazy helper per worker. Match the supervisor's hard
        // child-process accounting so performance profiles do not fail config
        // validation or silently compare different worker concurrency.
        'process_limit' => $workers * ($leaseRenewal ? 2 : 1),
        'state_directory' => (string) env(
            'QUEEN_SUPERVISOR_STATE_DIRECTORY',
            storage_path('queen-supervisor'),
        ),
        'telemetry_ttl' => 300,
        // Off unless a campaign sets them: replicas sharing one target, and
        // workers forked from one booted Laravel.
        'coordination' => ['enabled' => (bool) env('QUEEN_SUPERVISOR_COORDINATION', false)],
        'prefork' => (bool) env('QUEEN_SUPERVISOR_PREFORK', false),
        'event_driven' => (bool) env('QUEEN_SUPERVISOR_EVENT_DRIVEN', false),
        // Rust lanes on Linux: lease renewal in the master, or one PHP helper
        // per worker when a campaign turns it off. The package reads no
        // environment variable for it, so the harness keeps this one.
        'lease_service' => filter_var(env('QUEEN_SUPERVISOR_LEASE_SERVICE', true), FILTER_VALIDATE_BOOL),
        'supervisors' => $supervisors,
    ],
    'retry_429' => [],
];
