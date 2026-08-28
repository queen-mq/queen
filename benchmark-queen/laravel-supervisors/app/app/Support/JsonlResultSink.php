<?php

namespace App\Support;

use JsonException;
use RuntimeException;

final class JsonlResultSink
{
    public function __construct(private readonly string $baseDirectory)
    {
        if ($baseDirectory === ''
            || !str_starts_with($baseDirectory, DIRECTORY_SEPARATOR)
            || rtrim($baseDirectory, DIRECTORY_SEPARATOR) === ''
            || preg_match('#(?:^|/)(?:\.|\.\.)(?:/|$)#', $baseDirectory) === 1
            || str_contains($baseDirectory, "\0")) {
            throw new RuntimeException(
                'BENCH_RESULTS_DIRECTORY must be an absolute, non-root path without dot segments.',
            );
        }
    }

    /**
     * Append exactly one result while holding an exclusive advisory lock.
     *
     * The completion timestamp is supplied by the job before it waits for the
     * sink. `sink_lock_wait_ns` makes contention visible in the result set.
     *
     * @param array<string, bool|float|int|string|null> $record
     */
    public function append(array $record): void
    {
        $runId = $record['run_id'] ?? null;
        if (!is_string($runId)) {
            throw new RuntimeException('Benchmark results must contain a valid run_id.');
        }
        $this->assertRunId($runId);

        $eventsDirectory = $this->runDirectory($runId).DIRECTORY_SEPARATOR.'events';
        $this->ensureDirectory($eventsDirectory);
        $path = $eventsDirectory.DIRECTORY_SEPARATOR.'worker-'.getmypid().'.jsonl';

        $stream = @fopen($path, 'c+b');
        if ($stream === false) {
            throw new RuntimeException("Unable to open benchmark result sink [{$path}].");
        }

        try {
            $lockStartedAt = hrtime(true);
            if (!flock($stream, LOCK_EX)) {
                throw new RuntimeException("Unable to lock benchmark result sink [{$path}].");
            }

            try {
                $record['sink_lock_wait_ns'] = hrtime(true) - $lockStartedAt;
                $record['recorded_at_ns'] = hrtime(true);
                $line = json_encode(
                    $record,
                    JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION,
                )."\n";

                if (fseek($stream, 0, SEEK_END) !== 0 || fwrite($stream, $line) !== strlen($line)) {
                    throw new RuntimeException("Unable to append to benchmark result sink [{$path}].");
                }
                if (!fflush($stream)) {
                    throw new RuntimeException("Unable to flush benchmark result sink [{$path}].");
                }
            } finally {
                flock($stream, LOCK_UN);
            }
        } catch (JsonException $exception) {
            throw new RuntimeException('Unable to encode benchmark result.', previous: $exception);
        } finally {
            fclose($stream);
        }
    }

    /**
     * Atomically claim a run identifier before any jobs are published. A
     * leftover directory is an incomplete run to inspect, never a run to mix.
     */
    public function reserveRun(string $runId): string
    {
        $this->assertRunId($runId);
        $this->ensureDirectory($this->baseDirectory);
        $directory = $this->runDirectory($runId);
        if (!@mkdir($directory, 0770)) {
            throw new RuntimeException(
                "Benchmark run directory already exists or cannot be created [{$directory}].",
            );
        }

        return $directory;
    }

    /**
     * Return a stable snapshot. Writers cannot leave a partial JSON line while
     * this shared lock is held.
     *
     * @return list<array<string, mixed>>
     */
    public function read(string $runId): array
    {
        $this->assertRunId($runId);
        $paths = glob(
            $this->runDirectory($runId).DIRECTORY_SEPARATOR.'events'.DIRECTORY_SEPARATOR.'worker-*.jsonl',
        );
        if ($paths === false || $paths === []) {
            return [];
        }
        sort($paths, SORT_STRING);

        $records = [];
        foreach ($paths as $path) {
            array_push($records, ...$this->readFile($path));
        }

        return $records;
    }

    /**
     * Persist a completed dispatch manifest by renaming a fully flushed file
     * on the same filesystem. The default path is <run>/dispatch.json.
     *
     * @param array<string, int|string> $metadata
     */
    public function writeDispatchMetadata(string $runId, array $metadata, ?string $path = null): string
    {
        $this->assertRunId($runId);
        $path ??= $this->runDirectory($runId).DIRECTORY_SEPARATOR.'dispatch.json';
        if ($path === '') {
            throw new RuntimeException('Dispatch metadata path must not be empty.');
        }
        if (!str_starts_with($path, DIRECTORY_SEPARATOR)) {
            $path = getcwd().DIRECTORY_SEPARATOR.$path;
        }

        $directory = dirname($path);
        $this->ensureDirectory($directory);
        if (file_exists($path)) {
            throw new RuntimeException("Dispatch metadata already exists [{$path}]. Use a unique run_id.");
        }

        try {
            $json = json_encode(
                $metadata,
                JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES,
            )."\n";
        } catch (JsonException $exception) {
            throw new RuntimeException('Unable to encode dispatch metadata.', previous: $exception);
        }

        $temporary = $path.'.tmp.'.getmypid().'.'.bin2hex(random_bytes(8));
        $stream = @fopen($temporary, 'xb');
        if ($stream === false) {
            throw new RuntimeException("Unable to create dispatch metadata [{$temporary}].");
        }

        try {
            try {
                $written = 0;
                $length = strlen($json);
                while ($written < $length) {
                    $bytes = fwrite($stream, substr($json, $written));
                    if ($bytes === false || $bytes === 0) {
                        throw new RuntimeException("Unable to write dispatch metadata [{$temporary}].");
                    }
                    $written += $bytes;
                }
                if (!fflush($stream)) {
                    throw new RuntimeException("Unable to flush dispatch metadata [{$temporary}].");
                }
                if (function_exists('fsync') && !fsync($stream)) {
                    throw new RuntimeException("Unable to sync dispatch metadata [{$temporary}].");
                }
            } finally {
                fclose($stream);
            }
        } catch (\Throwable $exception) {
            @unlink($temporary);
            throw $exception;
        }

        if (!@rename($temporary, $path)) {
            @unlink($temporary);
            throw new RuntimeException("Unable to publish dispatch metadata [{$path}].");
        }

        return $path;
    }

    /** @return list<array<string, mixed>> */
    private function readFile(string $path): array
    {
        $stream = @fopen($path, 'rb');
        if ($stream === false) {
            throw new RuntimeException("Unable to read benchmark result sink [{$path}].");
        }

        try {
            if (!flock($stream, LOCK_SH)) {
                throw new RuntimeException("Unable to lock benchmark result sink [{$path}].");
            }
            try {
                $records = [];
                $lineNumber = 0;
                while (($line = fgets($stream)) !== false) {
                    ++$lineNumber;
                    if (trim($line) === '') {
                        continue;
                    }

                    try {
                        $record = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
                    } catch (JsonException $exception) {
                        throw new RuntimeException(
                            "Invalid JSON in benchmark result sink at line {$lineNumber}.",
                            previous: $exception,
                        );
                    }
                    if (!is_array($record)) {
                        throw new RuntimeException(
                            "Benchmark result sink line {$lineNumber} is not a JSON object.",
                        );
                    }
                    $records[] = $record;
                }

                if (!feof($stream)) {
                    throw new RuntimeException("Unable to finish reading benchmark result sink [{$path}].");
                }

                return $records;
            } finally {
                flock($stream, LOCK_UN);
            }
        } finally {
            fclose($stream);
        }
    }

    private function runDirectory(string $runId): string
    {
        return rtrim($this->baseDirectory, DIRECTORY_SEPARATOR).DIRECTORY_SEPARATOR.$runId;
    }

    private function assertRunId(string $runId): void
    {
        if (in_array($runId, ['.', '..'], true)
            || preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new RuntimeException('Benchmark run_id has an invalid format.');
        }
    }

    private function ensureDirectory(string $directory): void
    {
        if (is_dir($directory)) {
            return;
        }

        if (!@mkdir($directory, 0770, true) && !is_dir($directory)) {
            throw new RuntimeException("Unable to create benchmark result directory [{$directory}].");
        }
    }
}
