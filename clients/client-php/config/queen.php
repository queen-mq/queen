<?php

use Illuminate\Support\Str;

// Only the values that differ from one environment or deployment to the next
// read the environment: broker URLs and credentials, the queue and consumer
// group, feature switches, paths and notification targets. Every other value
// is a plain setting. Once published, this file belongs to the application:
// add env() wherever a value must differ per environment, such as the pool
// sizes of a supervisor.
$queue = env('QUEEN_QUEUE', 'default');
$consumerGroup = env('QUEEN_CONSUMER_GROUP', 'laravel');

return [
    'url' => env('QUEEN_URL', 'http://localhost:6632'),
    'urls' => env('QUEEN_URLS') ? explode(',', env('QUEEN_URLS')) : null,
    'bearer_token' => env('QUEEN_BEARER_TOKEN'),
    // Broker client. timeout, retry_delay and health_retry_after are in
    // milliseconds. With several urls, a broker that failed is skipped until
    // health_retry_after has passed.
    'timeout' => 30000,
    'retry_attempts' => 3,
    'retry_delay' => 1000,
    'load_balancing_strategy' => 'affinity',
    'enable_failover' => true,
    'affinity_hash_ring' => 150,
    'health_retry_after' => 30000,
    'headers' => [],

    // Laravel queue driver. Use with QUEUE_CONNECTION=queen and Laravel's
    // standard `php artisan queue:work queen` worker.
    'queue' => $queue,
    'consumer_group' => $consumerGroup,
    // Fixed stripes preserve concurrency without creating one partition per
    // job: 1 to 1024, each run by one worker at a time. A pop checks out at
    // most 64 of them. Jobs implementing QueenPartitionable override the
    // stripe with an explicit per-entity ordering key.
    'partitions' => env('QUEEN_PARTITIONS', 64),
    'partition_prefix' => 'laravel',
    // Must be longer than the Laravel worker/job timeout.
    'retry_after' => 90,
    // Seconds to long-poll. Keep 0 when workers consume priority queues in a
    // comma-separated list, otherwise the first empty queue delays the rest.
    'block_for' => 0,
    // Opt-in throughput controls. Prefetch leases multiple jobs in one broker
    // request; ack_batch defers successful ACK confirmation until the threshold
    // or the fetched lease is drained. Values greater than one preserve
    // at-least-once delivery but widen the duplicate/recovery window. A
    // Laravel worker may pause indefinitely while retaining its prefetched
    // tail, so Queen supervisors require lease_renewal whenever prefetch > 1.
    'prefetch' => 1,
    'ack_batch' => 1,
    // Send each successful ACK without waiting for the answer, which the
    // worker reads when it finishes the next job or before its next pop. The
    // broker writes the ACK while the next job runs. A failed ACK is then
    // reported one job later: that job may already have run on the same
    // lease, and the failed one is delivered again (at-least-once). Requires
    // ack_batch 1, and gains little with prefetch 1 without pop_ahead, where
    // the answer is read before the next pop.
    'ack_async' => false,
    // Pop the next batch while the last job of the current one runs, and take
    // it in when that job ends. The batch is leased one job earlier. Only
    // after a full batch: a short one means the queue was nearly empty, and a
    // pop sent ahead would come back empty. Requires lease_renewal.
    'pop_ahead' => false,
    // Let the broker choose the pop sweep width instead of the fixed
    // `partitions` stripe count above. Only that one dimension is delegated:
    // `batch` stays pinned to prefetch because the local prefetch buffer, the
    // ack_batch <= prefetch bound and the lease budget all key off it, and
    // `partitions` still stripes pushes. Off keeps the wire bytes unchanged.
    'autopilot' => false,
    // Opt-in lease renewal for jobs whose runtime cannot be bounded by the
    // original pop lease. The Rust supervisor on Linux renews the single lease
    // shared by a worker's active and prefetched jobs; elsewhere one small PHP
    // subprocess per Laravel worker does. If renewal can no longer finish
    // safely it TERM/KILL-fences the worker before expiry; effects already
    // emitted by a job can still be duplicated (at-least-once). Times are in
    // seconds; a null interval renews every retry_after / 3.
    'lease_renewal' => false,
    'lease_renewal_interval' => null,
    'lease_renewal_timeout' => 5,
    'lease_renewal_kill_grace' => 2,
    'lease_renewal_safety_margin' => 1,
    // Laravel Queue::bulk() is emitted as bounded multi-partition HTTP pushes.
    'bulk_batch' => 100,
    'after_commit' => false,
    // Keep Laravel failed_jobs as the command index while retaining Queen DLQ
    // snapshots. Retry/forget/flush/prune remove the matching DLQ row. Use a
    // cache store with distributed-lock support on multi-process/multi-host
    // deployments; array is process-local and file is host-local.
    'sync_failed_jobs' => true,
    // Null uses Laravel's default cache store. Production deployments should
    // name a shared store whose locks are visible to every queue worker.
    'failed_jobs_lock_store' => null,
    'failed_jobs_lock_name' => 'queen:failed-jobs',
    // Every flush/prune row gets its own critical section, containing at most
    // one Queen cleanup. Keep the TTL above the complete admin HTTP retry and
    // rate-limit budget; ownership is checked before the Laravel row mutates.
    'failed_jobs_lock_ttl' => 600,
    // Match the TTL by default. An immediately re-failing manual retry must be
    // able to wait for the fenced cleanup/publish hand-off to release its lock
    // instead of losing the new Laravel failed-job index row on contention.
    'failed_jobs_lock_wait' => 600,

    // Local process orchestration. The PHP and Rust engines consume the same
    // resolved JSON contract exposed by `queen:supervisor-config`.
    'supervisor' => [
        'poll_interval' => 3,
        'http_timeout' => 5,
        // A control request is queued in the private state directory. Keep
        // this above the longest possible supervisor reconcile iteration so
        // a healthy but busy control loop can still consume it.
        'control_ttl' => 3600,
        // A dashboard/CLI heartbeat is evaluated against the value published
        // by the running generation, not against a newly cached application
        // config. Null lets the resolver choose loop_budget + 1 second.
        'heartbeat_timeout' => null,
        'read_bearer_token' => env('QUEEN_SUPERVISOR_READ_BEARER_TOKEN'),
        // Seconds a stopping worker may finish its job before the supervisor
        // kills it. Must be longer than every pool's timeout.
        'shutdown_grace' => 75,
        // Hard budget for supervised child processes. A normal worker costs
        // one slot; a lease-renewal worker reserves a second slot for its
        // lazy helper, including while the worker is gracefully draining.
        'process_limit' => 256,
        // The final directory is 0700. Every existing parent must be owned by
        // root/the supervisor UID and not group/world-writable, except for a
        // trusted sticky directory such as /tmp. See the README if storage/
        // is deployed as 0775.
        'state_directory' => env('QUEEN_SUPERVISOR_STATE_DIRECTORY', storage_path('queen-supervisor')),
        'telemetry_ttl' => 300,
        // Also publish the status document to the broker's key/value store,
        // so a dashboard served by another host or pod (Kubernetes web pods,
        // a separate web server) can show this supervisor. PHP engine only
        // (php artisan queen:supervise). Read-only: pause, continue and
        // terminate stay with the supervisor's own host. The key must be
        // unique per application and environment on the broker; the default
        // is the application name and environment, such as
        // laravel-production. A null interval publishes at most once per
        // poll_interval; a null TTL keeps the document for twice the
        // heartbeat timeout (minimum 300s).
        'remote_status' => [
            'enabled' => env('QUEEN_SUPERVISOR_REMOTE_STATUS', false),
            'connection' => 'queen',
            'namespace' => 'queen-supervisor',
            'key' => Str::slug(env('APP_NAME', 'laravel') . ' ' . env('APP_ENV', 'production')),
            'interval' => null,
            'ttl' => null,
        ],
        // Boot Laravel once in a fork server and fork every worker from it,
        // instead of booting each worker: workers share the opcache's
        // compiled code outright and the booted heap copy-on-write. Needs
        // ext-pcntl and ext-posix; turn opcache.enable_cli on with it.
        'prefork' => filter_var(env('QUEEN_SUPERVISOR_PREFORK', false), FILTER_VALIDATE_BOOL),
        // Wake on new jobs instead of waiting for the next poll: a read-only
        // long poll on the broker (no lease, no cursor) watches the partition
        // stripes of every autoscaling pool, and a pool with new backlog is
        // resized at once, then every second while it climbs. Needs a broker
        // with POST /api/v1/fetch and a token that may consume.
        'event_driven' => false,
        // Rust engine on Linux: the master renews the leases of every
        // lease_renewal worker itself. False starts one PHP helper process
        // beside each such worker instead, as the PHP engine always does.
        'lease_service' => true,
        // Lets several replicas of this supervisor (Kubernetes pods, hosts)
        // share one backlog. Each replica registers in the broker's key/value
        // store and runs an even share of the worker target, so together they
        // scale like one supervisor; min_processes and max_processes apply to
        // each replica. Replicas coordinate when their consumer group and
        // queue set are the same. Fixed pools (balance=simple) are not split.
        'coordination' => [
            'enabled' => filter_var(env('QUEEN_SUPERVISOR_COORDINATION', false), FILTER_VALIDATE_BOOL),
            'connection' => 'queen',
            'namespace' => 'queen-supervisor',
        ],
        'supervisors' => [
            'default' => [
                'connection' => 'queen',
                'consumer_group' => $consumerGroup,
                'queues' => [$queue],
                'balance' => 'auto',
                'strategy' => 'size',
                'min_processes' => 1,
                // balance=auto only: workers every queue keeps even without
                // backlog, like Horizon's per-queue minProcesses.
                'min_processes_per_queue' => 0,
                // Close half of the gap to the target every cycle when the
                // backlog grows, instead of one balance_max_shift step.
                'fast_scale_up' => false,
                'max_processes' => 10,
                'target_jobs_per_process' => 10,
                'target_clear_seconds' => 60,
                'default_runtime_seconds' => 1,
                'balance_cooldown' => 3,
                'balance_max_shift' => 1,
                // Require a lower target to remain stable before removing
                // capacity; worker crashes use a separate capped backoff.
                'scale_down_delay' => 10,
                'restart_backoff' => 1,
                'restart_backoff_max' => 30,
                'stable_after' => 60,
                'sleep' => 1,
                'timeout' => 60,
                'retry_after' => 90,
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
        'refresh_seconds' => 5,
        'allow_local' => true,
        // Page size of the failed-jobs page (keyset pagination, newest first).
        'failed_jobs_limit' => 50,
        // Base URL of the Queen web console, such as https://queen.example.com.
        // When set, each queue of the Workload page links to its console page
        // and its waiting and running jobs. An http or https URL without user
        // info, query or fragment; any other value fails at boot.
        'console_url' => env('QUEEN_DASHBOARD_CONSOLE_URL'),
    ],

    // Like Horizon's `waits`: seconds the oldest job of a queue may wait
    // before `queen:check-waits` reports it (schedule it every minute). Keys
    // are connection:queue; the wait is measured by the broker per consumer
    // group, not estimated.
    'waits' => [
        'queen:' . $queue => 60,
    ],

    // Where queen:check-waits sends LongWaitDetected besides the event:
    // comma-separated mail addresses. One notification per queue and group
    // per throttle window.
    'notifications' => [
        'mail' => env('QUEEN_NOTIFY_MAIL'),
        'throttle_minutes' => 5,
    ],

    // Per-job-class metrics for the dashboard's Jobs page: every worker of a
    // Queen connection counts jobs, failures and runtime per class and writes
    // them to the broker's key/value store once every ten seconds.
    'job_metrics' => [
        'enabled' => true,
        'connection' => 'queen',
        'namespace' => 'queen-metrics',
    ],

    // Monitored tags, like Horizon's: jobs are tagged when pushed (their
    // tags() method, or one Model:key tag per Eloquent model they carry),
    // and workers record jobs carrying a tag chosen on the dashboard's Tags
    // page, for retention_minutes.
    'tags' => [
        'enabled' => true,
        'connection' => 'queen',
        'namespace' => 'queen-metrics',
        'retention_minutes' => 1440,
    ],

    // Prometheus text for scrapers and autoscalers (Kubernetes HPA through
    // prometheus-adapter, KEDA): queue depth, supervisor instances, workers
    // per pool, coordinated replicas. Off by default; the scraper sends
    // `Authorization: Bearer <token>`, and the token needs 32+ characters.
    'metrics' => [
        'enabled' => filter_var(env('QUEEN_METRICS_ENABLED', false), FILTER_VALIDATE_BOOL),
        'path' => 'queen/metrics',
        'token' => env('QUEEN_METRICS_TOKEN'),
    ],

    // The Rust supervisor is version-pinned by this Composer package, but is
    // installed explicitly so dependency scripts are never trusted to execute
    // downloaded code. The Composer launcher resolves this application-local
    // path from the Laravel root. A mirror may replace the release base URL;
    // both its manifest and artifacts must still use HTTPS. A manifest path
    // or URL and its SHA-256 pin are optional: null installs the release of
    // this package version.
    'supervisor_binary' => [
        'install_path' => env(
            'QUEEN_SUPERVISOR_INSTALL_PATH',
            // Keep binaries outside the private 0700 runtime-state directory.
            // The installer intentionally permits 0755 directories, while
            // both supervisor engines reject shared runtime state.
            storage_path('queen-supervisor-bin'),
        ),
        'release_base_url' => env('QUEEN_SUPERVISOR_RELEASE_BASE_URL'),
        'manifest' => null,
        // Optional trust pin promoted from a Sigstore-verified manifest.
        'manifest_sha256' => null,
    ],

    // Backoff for HTTP 429 (rate limited by the proxy), independent of the
    // retry_attempts above. Nulls keep the per-request-kind defaults: 10
    // attempts for ordinary requests, unbounded for long-poll pops, 500ms
    // base doubling up to a 30s cap. Retry-After wins as the delay source, then
    // the same cap is applied to protect long-running workers from bad headers.
    'retry_429' => [
        'maxAttempts' => null,
        'baseMs' => null,
        'capMs' => null,
    ],
];
