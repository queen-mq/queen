<?php

namespace Queen\Consumer;

use Queen\Http\HttpClient;

/** Cooperative observations for one fluent consume invocation, off by default. */
final class Supervision
{
    private string $id;
    private int $started;
    private float $monotonic;
    private ?float $lastPublish = null;
    private int $running = 0;
    private int $completed = 0;
    private int $failed = 0;
    private ?int $lastCompleted = null;
    private ?float $busySince = null;
    private bool $warned = false;
    private bool $firstHandler = true;
    private int $heartbeatTimeout;

    public function __construct(private HttpClient $http, private array $config, private array $options)
    {
        $group = $config['group'] ?? null;
        if (!is_string($group) || !preg_match('/\A[A-Za-z0-9][A-Za-z0-9._-]{0,254}\z/', $group) || $group === 'coordination') {
            throw new \InvalidArgumentException('supervision group must be a valid application/deployment name');
        }
        $concurrency = $options['concurrency'] ?? 1;
        if (!is_int($concurrency) || $concurrency < 1 || $concurrency > 4096) {
            throw new \InvalidArgumentException('supervision requires concurrency between 1 and 4096');
        }
        $this->id = bin2hex(random_bytes(16));
        $this->started = time();
        $this->monotonic = $this->now();
        // A normal long poll must fit inside a cooperative heartbeat window.
        $this->heartbeatTimeout = min(86400, max(30, (int) ceil(($options['timeoutMillis'] ?? 30000) / 1000) + 15));
    }

    private function now(): float { return hrtime(true) / 1_000_000_000; }

    public function setRunning(int $running): void { $this->running = $running; }

    public function invoke(\Closure $handler): mixed
    {
        $this->busySince = $this->now();
        $this->publish(force: $this->firstHandler);
        $this->firstHandler = false;
        try {
            $result = $handler();
            $this->completed++;
            return $result;
        } catch (\Throwable $error) {
            $this->failed++;
            throw $error;
        } finally {
            $this->busySince = null;
            $this->lastCompleted = time();
            $this->publish();
        }
    }

    private function scope(string $key): ?string
    {
        $value = $this->options[$key] ?? null;
        return $value === '' ? null : $value;
    }

    public function document(string $state = 'running'): array
    {
        return [
            'schema' => 'queen.consumer.status/v1', 'instance_id' => $this->id,
            'engine' => 'php', 'execution_model' => 'cooperative', 'hostname' => gethostname() ?: null,
            'pid' => getmypid(), 'state' => $state, 'updated_at_epoch' => time(),
            'started_at_epoch' => $this->started, 'uptime_seconds' => (int) ($this->now() - $this->monotonic),
            'configuration' => ['heartbeat_timeout' => $this->heartbeatTimeout],
            'pool_status' => [[
                'name' => 'consumer', 'queue' => $this->scope('queue'),
                'namespace' => $this->scope('namespace'),
                'task' => $this->scope('task'),
                'consumer_group' => $this->scope('group') ?? '__QUEUE_MODE__',
                'desired' => $this->options['concurrency'] ?? 1, 'running' => $this->running,
                'busy' => $this->busySince === null ? 0 : 1, 'completed' => $this->completed, 'failed' => $this->failed,
                'last_completed_at_epoch' => $this->lastCompleted,
                'oldest_inflight_seconds' => $this->busySince === null ? null : (int) ($this->now() - $this->busySince),
            ]],
        ];
    }

    public function publish(string $state = 'running', bool $force = false): void
    {
        $now = $this->now();
        if (!$force && $this->lastPublish !== null && $now - $this->lastPublish < 10) {
            return;
        }
        $this->lastPublish = $now;
        try {
            $bytes = json_encode($this->document($state), JSON_THROW_ON_ERROR);
            if (strlen($bytes) > 45000) {
                throw new \RuntimeException('consumer status exceeds one chunk');
            }
            $write = bin2hex(random_bytes(16));
            $slot = $this->config['group'] . '/' . $this->id;
            $ttl = max(60, 2 * $this->heartbeatTimeout);
            $operations = [
                ['op' => 'put', 'ns' => 'queen-supervisor', 'key' => $slot . '/head', 'ttlSeconds' => $ttl,
                    'value' => ['format' => 'queen.supervisor.remote-status/v1', 'write' => $write, 'chunks' => 1, 'bytes' => strlen($bytes)]],
                ['op' => 'put', 'ns' => 'queen-supervisor', 'key' => $slot . '/chunk/0000', 'ttlSeconds' => $ttl,
                    'value' => ['write' => $write, 'index' => 0, 'data' => base64_encode($bytes)]],
            ];
            // postAsync is one attempt, without the synchronous API's 429 retry.
            $response = $this->http->postAsync('/api/v1/kv', ['operations' => $operations], 2000)->wait();
            $results = $response['results'] ?? null;
            if (!is_array($results) || count($results) !== 2 || array_filter($results, fn($row) => ($row['applied'] ?? null) !== true)) {
                throw new \RuntimeException('consumer status publication was not applied');
            }
            $this->warned = false;
        } catch (\Throwable) {
            if (!$this->warned) {
                error_log('Queen consumer status publication failed; consumption continues');
            }
            $this->warned = true;
        }
    }

    public function stop(): void
    {
        $this->running = 0;
        $this->publish('stopped', true);
    }
}
