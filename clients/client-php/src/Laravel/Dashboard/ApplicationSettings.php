<?php

namespace Queen\Laravel\Dashboard;

/**
 * This application's Queen settings as this host resolves them, each with its
 * default and what it does: the Settings of the Configuration page, and the
 * values its advice reads. A supervisor started with other environment
 * variables runs with its own values; the pools it published are shown as
 * published.
 *
 * Configuration is untrusted input here. A value the connector or the
 * supervisor would refuse shows as "invalid", never as itself. Nothing that
 * can hold a credential is shown: a setting whose name contains token,
 * secret, password or key shows only whether it is set, a broker URL only its
 * scheme, host and port, and the headers only how many there are.
 */
final class ApplicationSettings
{
    /** A setting with one of these in its name holds a credential. */
    private const SECRET_WORDS = ['token', 'secret', 'password', 'key'];

    private const DEFAULT_URL = 'http://localhost:6632';

    private const MAX_POOLS = 256;

    private const MAX_URLS = 16;

    /**
     * Settings of the `queen` connection: environment variable, kind, default
     * (null when computed), minimum, maximum and meaning. A connection's own
     * value wins over config('queen'), as QueenConnector merges them.
     *
     * @var array<string, array{0: ?string, 1: string, 2: int|bool|null, 3: int, 4: ?int, 5: string}>
     */
    private const CONNECTION = [
        'timeout' => ['QUEEN_TIMEOUT', 'milliseconds', 30000, 1, null, 'How long one broker request may take.'],
        'retry_attempts' => ['QUEEN_RETRY_ATTEMPTS', 'count', 3, 0, null, 'How many times a failed broker request is tried.'],
        'prefetch' => ['QUEEN_PREFETCH', 'count', 1, 1, 1000, 'Jobs a worker leases in one pop. Above 1 needs lease_renewal.'],
        'ack_batch' => ['QUEEN_ACK_BATCH', 'count', 1, 1, 1000, 'Completed jobs a worker acknowledges in one request.'],
        'ack_async' => ['QUEEN_ACK_ASYNC', 'switch', false, 0, null, 'Start the next job without waiting for the answer to the acknowledgement.'],
        'pop_ahead' => ['QUEEN_POP_AHEAD', 'switch', false, 0, null, 'Pop the next batch while the last job of the current one runs. Needs lease_renewal.'],
        'lease_renewal' => ['QUEEN_LEASE_RENEWAL', 'switch', false, 0, null, 'Renew the lease of a running job, so a long job is not delivered twice.'],
        'lease_renewal_interval' => ['QUEEN_LEASE_RENEWAL_INTERVAL', 'seconds', null, 1, null, 'Time between two renewals of a lease.'],
        'lease_renewal_timeout' => ['QUEEN_LEASE_RENEWAL_TIMEOUT', 'seconds', 5, 1, null, 'How long one renewal request may take.'],
        'lease_renewal_kill_grace' => ['QUEEN_LEASE_RENEWAL_KILL_GRACE', 'seconds', 2, 0, null, 'Time between SIGTERM and SIGKILL for a worker whose lease cannot be renewed in time.'],
        'lease_renewal_safety_margin' => ['QUEEN_LEASE_RENEWAL_SAFETY_MARGIN', 'seconds', 1, 1, null, 'Time kept free before the lease deadline.'],
        'retry_after' => ['QUEEN_RETRY_AFTER', 'seconds', 90, 1, null, 'How long a lease lasts: a job not acknowledged by then is delivered again.'],
        'block_for' => ['QUEEN_BLOCK_FOR', 'seconds', 0, 0, null, 'How long a pop on an empty queue waits for a job.'],
        'partitions' => ['QUEEN_PARTITIONS', 'count', 64, 1, 64, 'Partition stripes that ordinary jobs are spread over.'],
        'bulk_batch' => ['QUEEN_BULK_BATCH', 'count', 100, 1, 1000, 'Jobs in one push request of Queue::bulk().'],
        'after_commit' => ['QUEEN_AFTER_COMMIT', 'switch', false, 0, null, 'Dispatch a job only after the open database transaction commits.'],
    ];

    /** @var array<string, array{0: ?string, 1: string, 2: int|bool|null, 3: int, 4: ?int, 5: string}> keys of queen.supervisor */
    private const SUPERVISOR = [
        'poll_interval' => ['QUEEN_SUPERVISOR_POLL_INTERVAL', 'seconds', 3, 1, null, 'How often the supervisor reads the backlog and resizes its pools.'],
        'http_timeout' => ['QUEEN_SUPERVISOR_HTTP_TIMEOUT', 'seconds', 5, 1, null, 'How long one supervisor request to the broker may take.'],
        'shutdown_grace' => ['QUEEN_SUPERVISOR_SHUTDOWN_GRACE', 'seconds', 75, 1, null, 'How long a stopping worker may finish its job before the supervisor kills it.'],
        'process_limit' => ['QUEEN_SUPERVISOR_PROCESS_LIMIT', 'count', 256, 1, 4096, 'Most child processes the master runs, lease helpers included.'],
        'heartbeat_timeout' => ['QUEEN_SUPERVISOR_HEARTBEAT_TIMEOUT', 'seconds', null, 1, 86400, 'Heartbeat age at which the supervisor shows as stale.'],
        'telemetry_ttl' => ['QUEEN_SUPERVISOR_TELEMETRY_TTL', 'seconds', 300, 1, null, 'How long worker runtime samples are kept for the time strategy.'],
        'prefork' => ['QUEEN_SUPERVISOR_PREFORK', 'switch', false, 0, null, 'Boot Laravel once and fork every worker from it.'],
        'event_driven' => ['QUEEN_SUPERVISOR_EVENT_DRIVEN', 'switch', false, 0, null, 'Resize a pool when jobs arrive instead of at the next poll.'],
        'coordination.enabled' => ['QUEEN_SUPERVISOR_COORDINATION', 'switch', false, 0, null, "Share each autoscaling pool's worker target with the other replicas."],
        'remote_status.enabled' => ['QUEEN_SUPERVISOR_REMOTE_STATUS', 'switch', false, 0, null, 'Publish the status to the broker for dashboards on other hosts.'],
        'remote_status.interval' => ['QUEEN_SUPERVISOR_REMOTE_STATUS_INTERVAL', 'seconds', null, 1, null, 'Time between two publishes of the status.'],
        'remote_status.ttl' => ['QUEEN_SUPERVISOR_REMOTE_STATUS_TTL', 'seconds', null, 1, 86400, 'How long the published status is kept.'],
    ];

    /**
     * @param array<string, mixed> $config `queen` and `queue` as in Laravel's
     *   configuration, and `env`: the environment variables the Rust master
     *   reads itself, as this host sees them
     */
    public function __construct(private array $config)
    {
    }

    /**
     * The options a Laravel queue connection runs with: its own over
     * config('queen'). Raw, so untrusted.
     *
     * @return array<string, mixed>
     */
    public function connection(string $name = 'queen'): array
    {
        return array_replace($this->queen(), $this->ownConnection($name));
    }

    /** A connection's integer: the default when unset, null when invalid. */
    public function connectionInteger(string $key, int $default, string $connection = 'queen'): ?int
    {
        return self::integerOr($this->connection($connection)[$key] ?? null, $default);
    }

    /** A connection's switch: the default when unset, null when not a boolean. */
    public function connectionSwitch(string $key, bool $default, string $connection = 'queen'): ?bool
    {
        return self::switchOr($this->connection($connection)[$key] ?? null, $default);
    }

    /** A queen.supervisor integer such as `poll_interval`; null when invalid. */
    public function supervisorInteger(string $path, int $default): ?int
    {
        return self::integerOr($this->supervisorValue($path), $default);
    }

    /** A queen.supervisor switch such as `coordination.enabled`; null when invalid. */
    public function supervisorSwitch(string $path, bool $default): ?bool
    {
        return self::switchOr($this->supervisorValue($path), $default);
    }

    /**
     * Whether QUEEN_SUPERVISOR_LEASE_SERVICE turns the Rust master's lease
     * service off, read as the master reads it: 0, false, no or off.
     */
    public function leaseServiceDisabled(): bool
    {
        $value = $this->environment('QUEEN_SUPERVISOR_LEASE_SERVICE');

        return $value !== null && in_array(strtolower(trim($value)), ['0', 'false', 'no', 'off'], true);
    }

    /**
     * This application's worker pools, with the defaults SupervisorConfiguration
     * applies. A value it would refuse is null; no configured pool is the
     * default pool, as for the supervisor.
     *
     * @return list<array<string, mixed>>
     */
    public function pools(): array
    {
        $queen = $this->queen();
        $configured = $this->supervisorValue('supervisors');
        if (!is_array($configured) || $configured === []) {
            $configured = ['default' => []];
        }

        $pools = [];
        foreach ($configured as $name => $options) {
            $name = self::text((string) $name, 128);
            if ($name === null || !is_array($options) || count($pools) >= self::MAX_POOLS) {
                continue;
            }
            $connectionName = self::text($options['connection'] ?? 'queen', 128);
            // As SupervisorConfiguration::connectionConfig reads it:
            // config('queen') stands under the queen connection only.
            $connection = $connectionName !== null
                ? array_replace($connectionName === 'queen' ? $queen : [], $this->ownConnection($connectionName))
                : [];
            $queues = $options['queues'] ?? $options['queue'] ?? [$queen['queue'] ?? 'default'];
            $queues = is_string($queues) ? explode(',', $queues) : (is_array($queues) ? $queues : []);
            $queues = array_values(array_unique(array_filter(array_map(
                static fn (mixed $queue): ?string => is_string($queue) ? self::text(trim($queue), 256) : null,
                $queues,
            ))));
            $balance = ($options['balance'] ?? 'auto') === false ? 'off' : ($options['balance'] ?? 'auto');
            $strategy = $options['strategy'] ?? 'size';
            $min = self::integerOr($options['min_processes'] ?? null, 1);
            $max = self::positiveOr($options['max_processes'] ?? null, 10);
            $processes = $max !== null ? self::integerOr($options['processes'] ?? null, $max) : null;

            $pools[] = [
                'name' => $name,
                'connection' => $connectionName,
                'consumer_group' => self::text($options['consumer_group'] ?? $queen['consumer_group'] ?? 'laravel', 128),
                'queues' => $queues,
                'balance' => in_array($balance, ['auto', 'simple', 'off'], true) ? $balance : null,
                'strategy' => in_array($strategy, ['size', 'time'], true) ? $strategy : null,
                'processes' => $min !== null && $max !== null && $processes !== null ? min($max, max($min, $processes)) : null,
                'min_processes' => $min,
                'max_processes' => $max,
                'timeout' => self::positiveOr($options['timeout'] ?? null, 60),
                'retry_after' => self::positiveOr($options['retry_after'] ?? $connection['retry_after'] ?? $queen['retry_after'] ?? null, 90),
                'tries' => self::integerOr($options['tries'] ?? null, 3),
                'memory' => self::positiveOr($options['memory'] ?? null, 128),
                'backoff' => self::integerOr($options['backoff'] ?? null, 0),
                'max_jobs' => self::integerOr($options['max_jobs'] ?? null, 0),
                'max_time' => self::integerOr($options['max_time'] ?? null, 0),
                'sleep' => self::integerOr($options['sleep'] ?? null, 1),
                'fast_scale_up' => self::switchOr($options['fast_scale_up'] ?? null, false),
                'lease_renewal' => self::switchOr($connection['lease_renewal'] ?? null, false),
                // Why the supervisor would refuse the pool's sizes, if it would.
                'refused' => $min !== null && $max !== null && $min > $max ? 'min_processes above max_processes' : null,
            ];
        }

        return $pools;
    }

    /**
     * The Pools table: the pools the running supervisor published, which is
     * what runs, or this application's when none did. Supervisors do not
     * publish backoff, max_jobs, max_time and sleep, so those come from this
     * application's pool of the same name, or are null.
     *
     * @param list<array<string, mixed>> $published DashboardRepository's configuration.supervisors
     * @return array{source: 'published'|'application', pools: list<array<string, mixed>>}
     */
    public function poolTable(array $published): array
    {
        $configured = array_column($this->pools(), null, 'name');
        if ($published === []) {
            return ['source' => 'application', 'pools' => array_values($configured)];
        }

        $pools = [];
        foreach ($published as $pool) {
            $local = $configured[$pool['name'] ?? null] ?? [];
            $pools[] = [
                ...$pool,
                'backoff' => $local['backoff'] ?? null,
                'max_jobs' => $local['max_jobs'] ?? null,
                'max_time' => $local['max_time'] ?? null,
                'sleep' => $local['sleep'] ?? null,
            ];
        }

        return ['source' => 'published', 'pools' => $pools];
    }

    /**
     * The Connection and Supervisor settings, ready to show: each row has its
     * name, environment variable, value, default, meaning, whether the value
     * differs from the default and whether it is invalid.
     *
     * @return array{connection: list<array<string, mixed>>, supervisor: list<array<string, mixed>>}
     */
    public function rows(): array
    {
        $connection = $this->connection();
        $retryAfter = $this->connectionInteger('retry_after', 90);
        $connectionRows = [
            $this->endpoints($connection),
            // QueenConnector also accepts the client's own spelling.
            $this->secret('bearer_token', 'QUEEN_BEARER_TOKEN', $connection['bearer_token'] ?? $connection['bearerToken'] ?? null, 'Credential sent to the broker.'),
            $this->headers($connection['headers'] ?? []),
        ];
        foreach (self::CONNECTION as $name => $definition) {
            $connectionRows[] = $this->setting($name, $definition, $connection[$name] ?? null, match ($name) {
                // The connector's own rule: a third of retry_after, at least 1.
                'lease_renewal_interval' => [$retryAfter !== null ? max(1, intdiv($retryAfter, 3)) . ' s' : null, 'retry_after / 3'],
                default => null,
            });
        }
        $connectionRows[] = $this->setting('sync_failed_jobs', ['QUEEN_SYNC_FAILED_JOBS', 'switch', true, 0, null, "Keep Laravel's failed jobs and Queen's dead-letter queue in step."], $this->queen()['sync_failed_jobs'] ?? null);
        if (array_key_exists('autopilot', $connection)) {
            $connectionRows[] = $this->setting('autopilot', ['QUEEN_AUTOPILOT', 'switch', false, 0, null, 'Let the broker choose how many partitions one pop sweeps.'], $connection['autopilot']);
        }
        $connectionRows = $this->refusedTogether($connectionRows);

        $supervisorRows = [
            ...array_map(
                fn (string $name): array => $this->supervisorSetting($name),
                ['poll_interval', 'http_timeout', 'shutdown_grace', 'process_limit', 'heartbeat_timeout', 'telemetry_ttl'],
            ),
            $this->secret('read_bearer_token', 'QUEEN_SUPERVISOR_READ_BEARER_TOKEN', $this->supervisorValue('read_bearer_token'), 'Read-only credential for depth reads and this dashboard.'),
            $this->supervisorSetting('prefork'),
            $this->supervisorSetting('event_driven'),
            $this->fastScaleUp(),
            $this->supervisorSetting('coordination.enabled'),
            $this->leaseService(),
            $this->supervisorSetting('remote_status.enabled'),
            $this->secret('remote_status.key', 'QUEEN_SUPERVISOR_REMOTE_STATUS_KEY', $this->supervisorValue('remote_status.key'), 'Key the status is published under.'),
            $this->supervisorSetting('remote_status.interval'),
            $this->supervisorSetting('remote_status.ttl'),
        ];

        return ['connection' => $connectionRows, 'supervisor' => $supervisorRows];
    }

    /**
     * What QueenConnector refuses together, marked on the setting that depends
     * on the other, with the reason: a worker with any of these cannot connect.
     *
     * @param list<array<string, mixed>> $rows
     * @return list<array<string, mixed>>
     */
    private function refusedTogether(array $rows): array
    {
        $prefetch = $this->connectionInteger('prefetch', 1);
        $ackBatch = $this->connectionInteger('ack_batch', 1);
        $renewal = $this->connectionSwitch('lease_renewal', false);
        $reasons = [
            'prefetch' => $prefetch !== null && $prefetch > 1 && $renewal === false ? 'needs lease_renewal' : null,
            'pop_ahead' => $this->connectionSwitch('pop_ahead', false) === true && $renewal === false ? 'needs lease_renewal' : null,
            'ack_batch' => $ackBatch !== null && $prefetch !== null && $ackBatch > $prefetch ? 'above prefetch' : null,
            'ack_async' => $this->connectionSwitch('ack_async', false) === true && $ackBatch !== null && $ackBatch > 1 ? 'needs ack_batch 1' : null,
            'lease_renewal' => $renewal === true && $this->renewalTimingFits() === false ? 'renewal timing does not fit in retry_after' : null,
        ];
        foreach ($rows as $index => $row) {
            $reason = $reasons[$row['name']] ?? null;
            // A value already invalid on its own keeps that mark.
            if ($reason !== null && !$row['invalid']) {
                $rows[$index] = [...$row, 'value' => $row['value'] . " ({$reason})", 'invalid' => true];
            }
        }

        return $rows;
    }

    /**
     * QueenConnector's renewal budget: the interval, two request budgets (one
     * per broker endpoint each), one second, the kill grace and the safety
     * margin must end before retry_after. Null when a value is invalid.
     */
    private function renewalTimingFits(): ?bool
    {
        $connection = $this->connection();
        $retryAfter = $this->connectionInteger('retry_after', 90);
        $interval = $connection['lease_renewal_interval'] ?? null;
        $interval = $interval === null || $interval === ''
            ? ($retryAfter !== null ? max(1, intdiv($retryAfter, 3)) : null)
            : self::integerOr($interval, 0);
        $timeout = $this->connectionInteger('lease_renewal_timeout', 5);
        $killGrace = $this->connectionInteger('lease_renewal_kill_grace', 2);
        $margin = $this->connectionInteger('lease_renewal_safety_margin', 1);
        if (in_array(null, [$retryAfter, $interval, $timeout, $killGrace, $margin], true)) {
            return null;
        }
        $urls = $connection['urls'] ?? null;
        $urls = is_string($urls) ? array_filter(array_map('trim', explode(',', $urls))) : $urls;
        $budget = $timeout * (is_array($urls) && $urls !== [] ? count($urls) : 1);

        return $interval + 2 * $budget + 1 + $killGrace + $margin < $retryAfter;
    }

    /** @return array<string, mixed> a queen.supervisor setting, with the defaults the supervisor computes */
    private function supervisorSetting(string $name): array
    {
        $pollInterval = $this->supervisorInteger('poll_interval', 3);

        return $this->setting($name, self::SUPERVISOR[$name], $this->supervisorValue($name), match ($name) {
            // An empty heartbeat_timeout is refused, not computed.
            'heartbeat_timeout' => ['computed at start', 'control-loop budget + 1 s', false],
            'remote_status.interval' => [$pollInterval !== null ? $pollInterval . ' s' : null, 'poll_interval'],
            'remote_status.ttl' => ['computed at start', 'twice heartbeat_timeout, at least 300 s'],
            default => null,
        });
    }

    /** @return array<string, mixed> an environment variable of the Rust master, as this host sees it */
    private function leaseService(): array
    {
        $disabled = $this->leaseServiceDisabled();

        return [
            'name' => 'QUEEN_SUPERVISOR_LEASE_SERVICE',
            'env' => null,
            'value' => $disabled ? 'off' : 'on',
            'default' => 'on',
            'changed' => $disabled,
            'invalid' => false,
            'meaning' => "Rust engine on Linux: the master renews the workers' leases; off starts a PHP helper per worker. Read from this host's environment.",
        ];
    }

    /**
     * One setting, as the connector or the supervisor reads it.
     *
     * @param array{0: ?string, 1: string, 2: int|bool|null, 3: int, 4: ?int, 5: string} $definition
     * @param array{0: ?string, 1: string, 2?: bool}|null $computed for a default the
     *   supervisor computes: the value it computes here (null when unknown), how
     *   it computes it, and whether an empty string also means unset (it does
     *   unless false)
     * @return array<string, mixed>
     */
    private function setting(string $name, array $definition, mixed $value, ?array $computed = null): array
    {
        [$env, $kind, $default, $minimum, $maximum, $meaning] = $definition;
        if (self::isSecret($name)) {
            return $this->secret($name, $env, $value, $meaning);
        }
        $row = [
            'name' => $name,
            'env' => $env,
            'value' => '',
            'default' => $computed[1] ?? self::display($kind, $default),
            'changed' => false,
            'invalid' => false,
            'meaning' => $meaning,
        ];
        if ($value === null || ($value === '' && $computed !== null && ($computed[2] ?? true))) {
            return [...$row, 'value' => $computed !== null ? ($computed[0] ?? $computed[1]) : $row['default']];
        }
        $parsed = $kind === 'switch' ? self::switchOr($value, false) : self::integerOr($value, 0);
        if ($parsed === null
            || (is_int($parsed) && ($parsed < $minimum || ($maximum !== null && $parsed > $maximum)))) {
            return [...$row, 'value' => 'invalid', 'invalid' => true];
        }

        return [...$row, 'value' => self::display($kind, $parsed), 'changed' => $parsed !== $default];
    }

    /** @return array<string, mixed> whether a credential is set, never its value */
    private function secret(string $name, ?string $env, mixed $value, string $meaning): array
    {
        $set = $value !== null && $value !== '' && $value !== [];

        return [
            'name' => $name,
            'env' => $env,
            'value' => $set ? 'set' : 'not set',
            'default' => 'not set',
            'changed' => $set,
            'invalid' => false,
            'meaning' => $meaning,
        ];
    }

    /**
     * The broker endpoints: scheme, host and port of each URL, never its user
     * info, path or query.
     *
     * @param array<string, mixed> $connection
     * @return array<string, mixed>
     */
    private function endpoints(array $connection): array
    {
        $urls = $connection['urls'] ?? null;
        $urls = is_string($urls) ? explode(',', $urls) : $urls;
        $urls = is_array($urls) ? array_values(array_filter($urls, static fn (mixed $url): bool => is_string($url) && trim($url) !== '')) : [];
        [$name, $env] = ['urls', 'QUEEN_URLS'];
        if ($urls === []) {
            [$name, $env, $urls] = ['url', 'QUEEN_URL', [$connection['url'] ?? self::DEFAULT_URL]];
        }
        $shown = [];
        $invalid = 0;
        foreach (array_slice($urls, 0, self::MAX_URLS) as $url) {
            $endpoint = self::endpoint($url);
            if ($endpoint === null) {
                ++$invalid;
            } else {
                $shown[] = $endpoint;
            }
        }
        if ($invalid > 0) {
            $shown[] = $invalid . ' invalid';
        }
        $value = implode(', ', $shown);

        return [
            'name' => $name,
            'env' => $env,
            'value' => $value,
            'default' => self::DEFAULT_URL,
            'changed' => $value !== self::DEFAULT_URL,
            'invalid' => $invalid > 0,
            'meaning' => 'Broker endpoints the workers and this dashboard call, shown without credentials.',
        ];
    }

    /** @return array<string, mixed> how many headers are set, never their values */
    private function headers(mixed $headers): array
    {
        return [
            'name' => 'headers',
            'env' => null,
            'value' => match (true) {
                !is_array($headers) => 'invalid',
                $headers === [] => 'none',
                default => count($headers) . ' set, values hidden',
            },
            'default' => 'none',
            'changed' => $headers !== [],
            'invalid' => !is_array($headers),
            'meaning' => 'Extra HTTP headers sent with every broker request.',
        ];
    }

    /** @return array<string, mixed> fast_scale_up is set per pool: the pools that use it */
    private function fastScaleUp(): array
    {
        $pools = $this->pools();
        $on = array_column(array_filter($pools, static fn (array $pool): bool => $pool['fast_scale_up'] === true), 'name');
        $invalid = in_array(null, array_column($pools, 'fast_scale_up'), true);

        return [
            'name' => 'fast_scale_up',
            'env' => 'QUEEN_SUPERVISOR_FAST_SCALE_UP',
            'value' => $invalid ? 'invalid' : ($on === [] ? 'off' : 'on for ' . implode(', ', $on)),
            'default' => 'off',
            'changed' => $on !== [],
            'invalid' => $invalid,
            'meaning' => 'Close half of the gap to the worker target each cycle instead of one step. Set per pool.',
        ];
    }

    /** @return array<string, mixed> */
    private function queen(): array
    {
        return is_array($this->config['queen'] ?? null) ? $this->config['queen'] : [];
    }

    /** @return array<string, mixed> a connection's own options in queue.connections */
    private function ownConnection(string $name): array
    {
        $queue = is_array($this->config['queue'] ?? null) ? $this->config['queue'] : [];
        $connections = is_array($queue['connections'] ?? null) ? $queue['connections'] : [];

        return is_array($connections[$name] ?? null) ? $connections[$name] : [];
    }

    /** A queen.supervisor value by dotted path, such as `remote_status.ttl`. */
    private function supervisorValue(string $path): mixed
    {
        $value = $this->queen()['supervisor'] ?? null;
        foreach (explode('.', $path) as $segment) {
            if (!is_array($value)) {
                return null;
            }
            $value = $value[$segment] ?? null;
        }

        return $value;
    }

    private function environment(string $name): ?string
    {
        $environment = is_array($this->config['env'] ?? null) ? $this->config['env'] : [];
        $value = $environment[$name] ?? null;

        return is_string($value) ? $value : null;
    }

    private static function isSecret(string $name): bool
    {
        $name = strtolower($name);
        foreach (self::SECRET_WORDS as $word) {
            if (str_contains($name, $word)) {
                return true;
            }
        }

        return false;
    }

    /** Scheme, host and port of an http or https URL; null for anything else. */
    private static function endpoint(mixed $url): ?string
    {
        $parts = is_string($url) && preg_match('/[\x00-\x1F\x7F]/', $url) !== 1 ? parse_url(trim($url)) : false;
        if (!is_array($parts)) {
            return null;
        }
        $scheme = strtolower((string) ($parts['scheme'] ?? ''));
        $host = $parts['host'] ?? null;
        if (!in_array($scheme, ['http', 'https'], true)
            || !is_string($host)
            || preg_match('/^(?:[A-Za-z0-9.-]{1,253}|\[[0-9A-Fa-f:.]{2,45}\])$/D', $host) !== 1) {
            return null;
        }

        return $scheme . '://' . $host . (isset($parts['port']) ? ':' . (int) $parts['port'] : '');
    }

    /** As the connector and the supervisor accept one: an int, or a string of digits with an optional plus. */
    private static function integerOr(mixed $value, int $default): ?int
    {
        if ($value === null) {
            return $default;
        }
        if (is_string($value) && preg_match('/^\s*\+?[0-9]{1,18}\s*$/D', $value) === 1) {
            return (int) trim($value);
        }

        return is_int($value) && $value >= 0 ? $value : null;
    }

    private static function positiveOr(mixed $value, int $default): ?int
    {
        $integer = self::integerOr($value, $default);

        return $integer !== null && $integer >= 1 ? $integer : null;
    }

    /** Both read a switch as a real boolean only. */
    private static function switchOr(mixed $value, bool $default): ?bool
    {
        return $value === null ? $default : (is_bool($value) ? $value : null);
    }

    private static function text(mixed $value, int $maximumBytes): ?string
    {
        if (!is_string($value) || trim($value) === '' || strlen($value) > $maximumBytes) {
            return null;
        }

        return preg_match('/[\x00-\x1F\x7F]/', $value) !== 1 && preg_match('//u', $value) === 1 ? $value : null;
    }

    private static function display(string $kind, int|bool|null $value): string
    {
        return match (true) {
            $value === null => '',
            $kind === 'switch' => $value ? 'on' : 'off',
            $kind === 'seconds' => number_format((int) $value) . ' s',
            $kind === 'milliseconds' => number_format((int) $value) . ' ms',
            default => number_format((int) $value),
        };
    }
}
