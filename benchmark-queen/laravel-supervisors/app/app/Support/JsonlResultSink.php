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
        return iterator_to_array($this->stream($this->snapshot($runId)), false);
    }

    /**
     * Capture stable, append-only byte limits for every worker result file.
     * Writers hold an exclusive lock across write and flush, so every captured
     * end points immediately after a complete JSONL record.
     */
    public function snapshot(string $runId): JsonlResultSnapshot
    {
        $this->assertRunId($runId);
        $paths = $this->eventPaths($runId);
        $fileEnds = [];

        foreach ($paths as $path) {
            $stream = @fopen($path, 'rb');
            if ($stream === false) {
                throw new RuntimeException("Unable to read benchmark result sink [{$path}].");
            }

            try {
                if (!flock($stream, LOCK_SH)) {
                    throw new RuntimeException("Unable to lock benchmark result sink [{$path}].");
                }
                try {
                    $metadata = fstat($stream);
                    $size = is_array($metadata) ? ($metadata['size'] ?? null) : null;
                    if (!is_int($size) || $size < 0) {
                        throw new RuntimeException("Unable to inspect benchmark result sink [{$path}].");
                    }
                    $fileEnds[$path] = $size;
                } finally {
                    flock($stream, LOCK_UN);
                }
            } finally {
                fclose($stream);
            }
        }

        return new JsonlResultSnapshot($runId, $fileEnds);
    }

    /**
     * Stream records in a stable snapshot, optionally starting immediately
     * after an earlier snapshot. Only the captured byte ranges are read, even
     * if workers append more results while the generator is being consumed.
     *
     * @return iterable<array<string, mixed>>
     */
    public function stream(
        JsonlResultSnapshot $snapshot,
        ?JsonlResultSnapshot $after = null,
    ): iterable {
        $runId = $snapshot->runId();
        $this->assertRunId($runId);
        if ($after !== null && $after->runId() !== $runId) {
            throw new RuntimeException('Benchmark result snapshots belong to different runs.');
        }

        $fileEnds = $snapshot->fileEnds();
        $afterEnds = $after?->fileEnds() ?? [];
        $this->assertSnapshotPaths($runId, $fileEnds);
        $this->assertSnapshotPaths($runId, $afterEnds);

        foreach ($afterEnds as $path => $_end) {
            if (!array_key_exists($path, $fileEnds)) {
                throw new RuntimeException(
                    "Benchmark result sink file disappeared between snapshots [{$path}].",
                );
            }
        }

        foreach ($fileEnds as $path => $end) {
            $start = $afterEnds[$path] ?? 0;
            if ($start > $end) {
                throw new RuntimeException(
                    "Benchmark result sink was truncated between snapshots [{$path}].",
                );
            }
            if ($start === $end) {
                continue;
            }

            yield from $this->streamFileRange($path, $start, $end);
        }
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

    /** @return list<string> */
    private function eventPaths(string $runId): array
    {
        $paths = glob(
            $this->runDirectory($runId).DIRECTORY_SEPARATOR.'events'.DIRECTORY_SEPARATOR.'worker-*.jsonl',
        );
        if ($paths === false || $paths === []) {
            return [];
        }
        sort($paths, SORT_STRING);

        return $paths;
    }

    /**
     * @param array<string, int> $fileEnds
     */
    private function assertSnapshotPaths(string $runId, array $fileEnds): void
    {
        $eventsDirectory = $this->runDirectory($runId).DIRECTORY_SEPARATOR.'events';
        foreach ($fileEnds as $path => $end) {
            if (dirname($path) !== $eventsDirectory
                || preg_match('/^worker-[0-9]+\.jsonl$/D', basename($path)) !== 1
                || !is_int($end)
                || $end < 0) {
                throw new RuntimeException('Benchmark result snapshot contains an invalid file boundary.');
            }
        }
    }

    /** @return iterable<array<string, mixed>> */
    private function streamFileRange(string $path, int $start, int $end): iterable
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
                $metadata = fstat($stream);
                $size = is_array($metadata) ? ($metadata['size'] ?? null) : null;
                if (!is_int($size) || $size < $end) {
                    throw new RuntimeException(
                        "Benchmark result sink was truncated before its snapshot [{$path}].",
                    );
                }

                if ($start > 0) {
                    if (fseek($stream, $start - 1, SEEK_SET) !== 0 || fread($stream, 1) !== "\n") {
                        throw new RuntimeException(
                            "Benchmark result snapshot starts inside a JSON line [{$path}:{$start}].",
                        );
                    }
                }
                if (fseek($stream, $start, SEEK_SET) !== 0) {
                    throw new RuntimeException(
                        "Unable to seek benchmark result sink [{$path}:{$start}].",
                    );
                }

                $position = $start;
                while ($position < $end) {
                    $lineStart = $position;
                    // The snapshot end is a flushed JSONL boundary, so an
                    // unbounded fgets still stops exactly at or before it. Do
                    // not pass the entire remaining file size as a buffer
                    // length: that would recreate a large peak allocation.
                    $line = fgets($stream);
                    if ($line === false) {
                        throw new RuntimeException(
                            "Unable to finish reading benchmark result sink [{$path}].",
                        );
                    }
                    $position = ftell($stream);
                    if (!is_int($position) || $position <= $lineStart || $position > $end) {
                        throw new RuntimeException(
                            "Benchmark result snapshot ends inside a JSON line [{$path}:{$end}].",
                        );
                    }
                    if (trim($line) === '') {
                        continue;
                    }

                    try {
                        $record = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
                    } catch (JsonException $exception) {
                        throw new RuntimeException(
                            "Invalid JSON in benchmark result sink [{$path}:{$lineStart}].",
                            previous: $exception,
                        );
                    }
                    if (!is_array($record)) {
                        throw new RuntimeException(
                            "Benchmark result sink entry is not a JSON object [{$path}:{$lineStart}].",
                        );
                    }
                    yield $record;
                }

                if ($position !== $end) {
                    throw new RuntimeException(
                        "Unable to finish reading benchmark result snapshot [{$path}:{$end}].",
                    );
                }
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
