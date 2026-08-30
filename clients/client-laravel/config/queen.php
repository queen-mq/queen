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

    // Laravel queue driver. Use with QUEUE_CONNECTION=queen and Laravel's
    // standard `php artisan queue:work queen` worker.
    'queue' => env('QUEEN_QUEUE', 'default'),
    'consumer_group' => env('QUEEN_CONSUMER_GROUP', 'laravel'),
    // Fixed stripes preserve concurrency without creating one partition per
    // job. Jobs implementing QueenPartitionable override the stripe with an
    // explicit per-entity ordering key.
    'partitions' => env('QUEEN_PARTITIONS', 64),
    'partition_prefix' => env('QUEEN_PARTITION_PREFIX', 'laravel'),
    // Must be longer than the Laravel worker/job timeout.
    'retry_after' => env('QUEEN_RETRY_AFTER', 90),
    // Seconds to long-poll. Keep 0 when workers consume priority queues in a
    // comma-separated list, otherwise the first empty queue delays the rest.
    'block_for' => env('QUEEN_BLOCK_FOR', 0),
    // Opt-in throughput controls. Prefetch leases multiple jobs in one broker
    // request; ack_batch defers successful ACK confirmation until the threshold
    // or the fetched lease is drained. Values greater than one preserve
    // at-least-once delivery but widen the duplicate/recovery window. A
    // Laravel worker may pause indefinitely while retaining its prefetched
    // tail, so Queen supervisors require lease_renewal whenever prefetch > 1.
    'prefetch' => env('QUEEN_PREFETCH', 1),
    'ack_batch' => env('QUEEN_ACK_BATCH', 1),
    // Let the broker choose the pop sweep width instead of the fixed
    // `partitions` stripe count above. Only that one dimension is delegated:
    // `batch` stays pinned to prefetch because the local prefetch buffer, the
    // ack_batch <= prefetch bound and the lease budget all key off it, and
    // `partitions` still stripes pushes. Off keeps the wire bytes unchanged.
    'autopilot' => env('QUEEN_AUTOPILOT', false),
    // Opt-in data-plane helper for jobs whose runtime cannot be bounded by the
    // original pop lease. One small PHP subprocess per Laravel worker renews
    // the single lease shared by its active and prefetched jobs. If renewal can
    // no longer finish safely it TERM/KILL-fences the worker before expiry;
    // effects already emitted by a job can still be duplicated (at-least-once).
    'lease_renewal' => env('QUEEN_LEASE_RENEWAL', false),
    'lease_renewal_interval' => env('QUEEN_LEASE_RENEWAL_INTERVAL'),
    'lease_renewal_timeout' => env('QUEEN_LEASE_RENEWAL_TIMEOUT', 5),
    'lease_renewal_kill_grace' => env('QUEEN_LEASE_RENEWAL_KILL_GRACE', 2),
    'lease_renewal_safety_margin' => env('QUEEN_LEASE_RENEWAL_SAFETY_MARGIN', 1),
    // Laravel Queue::bulk() is emitted as bounded multi-partition HTTP pushes.
    'bulk_batch' => env('QUEEN_BULK_BATCH', 100),
    'after_commit' => env('QUEEN_AFTER_COMMIT', false),
    // Keep Laravel failed_jobs as the command index while retaining Queen DLQ
    // snapshots. Retry/forget/flush/prune remove the matching DLQ row. Use a
    // cache store with distributed-lock support on multi-process/multi-host
    // deployments; array is process-local and file is host-local.
    'sync_failed_jobs' => env('QUEEN_SYNC_FAILED_JOBS', true),
    // Null uses Laravel's default cache store. Production deployments should
    // name a shared store whose locks are visible to every queue worker.
    'failed_jobs_lock_store' => env('QUEEN_FAILED_JOBS_LOCK_STORE'),
    'failed_jobs_lock_name' => env('QUEEN_FAILED_JOBS_LOCK_NAME', 'queen:failed-jobs'),
    // Every flush/prune row gets its own critical section, containing at most
    // one Queen cleanup. Keep the TTL above the complete admin HTTP retry and
    // rate-limit budget; ownership is checked before the Laravel row mutates.
    'failed_jobs_lock_ttl' => env('QUEEN_FAILED_JOBS_LOCK_TTL', 600),
    // Match the TTL by default. An immediately re-failing manual retry must be
    // able to wait for the fenced cleanup/publish hand-off to release its lock
    // instead of losing the new Laravel failed-job index row on contention.
    'failed_jobs_lock_wait' => env('QUEEN_FAILED_JOBS_LOCK_WAIT', 600),

    // Local process orchestration. The PHP and Rust engines consume the same
    // resolved JSON contract exposed by `queen:supervisor-config`.
    'supervisor' => [
        'poll_interval' => env('QUEEN_SUPERVISOR_POLL_INTERVAL', 3),
        'http_timeout' => env('QUEEN_SUPERVISOR_HTTP_TIMEOUT', 5),
        // A control request is queued in the private state directory. Keep
        // this above the longest possible supervisor reconcile iteration so
        // a healthy but busy control loop can still consume it.
        'control_ttl' => env('QUEEN_SUPERVISOR_CONTROL_TTL', 3600),
        // A dashboard/CLI heartbeat is evaluated against the value published
        // by the running generation, not against a newly cached application
        // config. Null lets the resolver choose loop_budget + 1 second.
        'heartbeat_timeout' => env('QUEEN_SUPERVISOR_HEARTBEAT_TIMEOUT'),
        'read_bearer_token' => env('QUEEN_SUPERVISOR_READ_BEARER_TOKEN'),
        'shutdown_grace' => env('QUEEN_SUPERVISOR_SHUTDOWN_GRACE', 75),
        'process_limit' => env('QUEEN_SUPERVISOR_PROCESS_LIMIT', 256),
        // The final directory is 0700. Every existing parent must be owned by
        // root/the supervisor UID and not group/world-writable, except for a
        // trusted sticky directory such as /tmp. See the README if storage/
        // is deployed as 0775.
        'state_directory' => env('QUEEN_SUPERVISOR_STATE_DIRECTORY', storage_path('queen-supervisor')),
        'telemetry_ttl' => env('QUEEN_SUPERVISOR_TELEMETRY_TTL', 300),
        'supervisors' => [
            'default' => [
                'connection' => 'queen',
                'consumer_group' => env('QUEEN_CONSUMER_GROUP', 'laravel'),
                'queues' => [env('QUEEN_QUEUE', 'default')],
                'balance' => env('QUEEN_SUPERVISOR_BALANCE', 'auto'),
                'strategy' => env('QUEEN_SUPERVISOR_STRATEGY', 'size'),
                'min_processes' => env('QUEEN_SUPERVISOR_MIN_PROCESSES', 1),
                'max_processes' => env('QUEEN_SUPERVISOR_MAX_PROCESSES', 10),
                'target_jobs_per_process' => env('QUEEN_SUPERVISOR_TARGET_JOBS', 10),
                'target_clear_seconds' => env('QUEEN_SUPERVISOR_TARGET_CLEAR_SECONDS', 60),
                'default_runtime_seconds' => env('QUEEN_SUPERVISOR_DEFAULT_RUNTIME_SECONDS', 1),
                'balance_cooldown' => env('QUEEN_SUPERVISOR_BALANCE_COOLDOWN', 3),
                'balance_max_shift' => env('QUEEN_SUPERVISOR_BALANCE_MAX_SHIFT', 1),
                // Require a lower target to remain stable before removing
                // capacity; worker crashes use a separate capped backoff.
                'scale_down_delay' => env('QUEEN_SUPERVISOR_SCALE_DOWN_DELAY', 10),
                'restart_backoff' => env('QUEEN_SUPERVISOR_RESTART_BACKOFF', 1),
                'restart_backoff_max' => env('QUEEN_SUPERVISOR_RESTART_BACKOFF_MAX', 30),
                'stable_after' => env('QUEEN_SUPERVISOR_STABLE_AFTER', 60),
                'sleep' => 1,
                'timeout' => 60,
                'retry_after' => env('QUEEN_RETRY_AFTER', 90),
                'tries' => 3,
                'memory' => 128,
                'backoff' => 0,
                'max_jobs' => 0,
                'max_time' => 0,
                'rest' => 0,
                'force' => false,
                // Avoid per-job console I/O in daemon mode.
                'quiet' => true,
            ],
        ],
    ],

    // Package-native local supervisor dashboard. It is intentionally disabled
    // until an application opts in. Production access remains denied unless
    // the application defines the `viewQueenDashboard` Gate ability.
    'dashboard' => [
        'enabled' => env('QUEEN_DASHBOARD_ENABLED', false),
        'path' => env('QUEEN_DASHBOARD_PATH', 'queen'),
        'domain' => env('QUEEN_DASHBOARD_DOMAIN'),
        // The web group is always retained by the package so state-changing
        // controls cannot accidentally lose CSRF protection. Add application
        // authentication/rate-limit middleware here as needed.
        'middleware' => ['web'],
        'refresh_seconds' => env('QUEEN_DASHBOARD_REFRESH_SECONDS', 5),
        'allow_local' => env('QUEEN_DASHBOARD_ALLOW_LOCAL', true),
        'failed_jobs_limit' => env('QUEEN_DASHBOARD_FAILED_JOBS_LIMIT', 50),
    ],

    // The Rust supervisor is version-pinned by this Composer package, but is
    // installed explicitly so dependency scripts are never trusted to execute
    // downloaded code. The Composer launcher resolves this application-local
    // path from the Laravel root. A mirror may replace the release base URL;
    // both its manifest and artifacts must still use HTTPS.
    'supervisor_binary' => [
        'install_path' => env(
            'QUEEN_SUPERVISOR_INSTALL_PATH',
            // Keep binaries outside the private 0700 runtime-state directory.
            // The installer intentionally permits 0755 directories, while
            // both supervisor engines reject shared runtime state.
            storage_path('queen-supervisor-bin'),
        ),
        'release_base_url' => env('QUEEN_SUPERVISOR_RELEASE_BASE_URL'),
        'manifest' => env('QUEEN_SUPERVISOR_MANIFEST'),
        // Optional trust pin promoted from a Sigstore-verified manifest.
        'manifest_sha256' => env('QUEEN_SUPERVISOR_MANIFEST_SHA256'),
    ],

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
