<?php

namespace Queen\Laravel\Supervisor;

use Illuminate\Contracts\Queue\Job;

final class WorkerTelemetry
{
    private const MAX_FILE_BYTES = 65536;

    private const MAX_QUEUES = 256;

    /** @var array<int, int> */
    private array $started = [];
    /** @var array<string, array{samples:int,runtime_ewma_seconds:float,failures:int}> */
    private array $queues = [];
    private float $lastPublish = 0.0;

    public function __construct(
        private string $directory,
        private string $connection = 'queen',
        private string $supervisor = 'default',
        private string $consumerGroup = 'laravel',
    ) {
        // PID reuse must not make a new worker inherit a previous process's
        // runtime estimate before it has handled its first job.
        @unlink($this->path());
    }

    public function start(string $connection, Job $job): void
    {
        if ($connection === $this->connection) {
            $this->started[spl_object_id($job)] = hrtime(true);
        }
    }

    public function finish(string $connection, Job $job, bool $failed = false): void
    {
        $key = spl_object_id($job);
        $started = $this->started[$key] ?? null;
        unset($this->started[$key]);
        if ($connection !== $this->connection || $started === null) {
            return;
        }

        $queue = $job->getQueue();
        if (!is_string($queue)
            || $queue === ''
            || strlen($queue) > 256
            || preg_match('/[\x00-\x1F\x7F]/', $queue) === 1
            || (!isset($this->queues[$queue]) && count($this->queues) >= self::MAX_QUEUES)) {
            return;
        }
        $stats =& $this->queues[$queue];
        $stats ??= ['samples' => 0, 'runtime_ewma_seconds' => 0.0, 'failures' => 0];
        $duration = max(0.000001, (hrtime(true) - $started) / 1_000_000_000);
        $stats['samples']++;
        $stats['runtime_ewma_seconds'] = $stats['samples'] === 1
            ? $duration
            : ($stats['runtime_ewma_seconds'] * 0.8) + ($duration * 0.2);
        $stats['failures'] += $failed ? 1 : 0;
        if ($stats['samples'] === 1 || $stats['samples'] % 10 === 0 || microtime(true) - $this->lastPublish >= 1.0) {
            try {
                $this->publish();
            } catch (\Throwable) {
                // Telemetry is best effort and must never change job outcome.
            }
        }
    }

    private function publish(): void
    {
        if (!is_dir($this->directory) && !mkdir($this->directory, 0700, true) && !is_dir($this->directory)) {
            return;
        }
        $directoryMetadata = @lstat($this->directory);
        if ($directoryMetadata === false
            || ($directoryMetadata['mode'] & 0170000) !== 0040000
            || ($directoryMetadata['mode'] & 07777) !== 0700
            || (function_exists('posix_geteuid') && ($directoryMetadata['uid'] ?? null) !== posix_geteuid())) {
            return;
        }
        $path = $this->path();
        $temporary = $path . '.' . bin2hex(random_bytes(8)) . '.tmp';
        $payload = json_encode([
            'pid' => getmypid(),
            'updated_at_epoch' => time(),
            'supervisor' => $this->supervisor,
            'connection' => $this->connection,
            'consumer_group' => $this->consumerGroup,
            'queues' => $this->queues,
        ], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
        if (strlen($payload) > self::MAX_FILE_BYTES) {
            return;
        }
        $written = @file_put_contents($temporary, $payload, LOCK_EX);
        if ($written !== strlen($payload)) {
            @unlink($temporary);
            return;
        }
        if (@chmod($temporary, 0600) && @rename($temporary, $path)) {
            $this->lastPublish = microtime(true);
        } else {
            @unlink($temporary);
        }
    }

    private function path(): string
    {
        return rtrim($this->directory, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . getmypid() . '.json';
    }
}
