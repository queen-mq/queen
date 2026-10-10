<?php

namespace App\Support;

use InvalidArgumentException;
use RuntimeException;

/**
 * Append-only log of failure-matrix attempts, one JSON line per event, in
 * `<results>/<run>/matrix/attempts.jsonl`. Appends are locked, so workers in
 * several processes can share the file.
 */
final class FailureMatrixLog
{
    /**
     * @param array<string, mixed> $detail what the event saw, such as the
     *        connection a worker ran the job on; empty for most events
     */
    public function record(
        string $runId,
        string $jobId,
        ?int $attempt,
        string $event,
        string $mode,
        ?string $exception = null,
        array $detail = [],
    ): void {
        $line = json_encode([
            'run_id' => $runId,
            'job_id' => $jobId,
            'attempt' => $attempt,
            'event' => $event,
            'mode' => $mode,
            'exception' => $exception,
            'detail' => $detail === [] ? null : $detail,
            // Replicas share the results volume and may reuse a pid: the
            // container's host name tells their workers apart.
            'host' => gethostname(),
            'pid' => getmypid(),
            'memory_mib' => intdiv(memory_get_usage(true), 1024 * 1024),
            'at' => microtime(true),
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES) . "\n";
        $path = $this->path($runId);
        if (file_put_contents($path, $line, FILE_APPEND | LOCK_EX) !== strlen($line)) {
            throw new RuntimeException("Unable to append to [{$path}].");
        }
    }

    /** @return list<array<string, mixed>> */
    public function read(string $runId): array
    {
        $path = $this->path($runId);
        if (!is_file($path)) {
            return [];
        }
        $events = [];
        foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $events[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
        }

        return $events;
    }

    private function path(string $runId): string
    {
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new InvalidArgumentException('run-id has an invalid format.');
        }
        $directory = config('benchmark.results_directory') . "/{$runId}/matrix";
        // Workers race to create it: losing that race is not an error.
        if (!is_dir($directory) && !@mkdir($directory, 0770, true) && !is_dir($directory)) {
            throw new RuntimeException("Unable to create [{$directory}].");
        }

        return "{$directory}/attempts.jsonl";
    }
}
