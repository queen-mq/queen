<?php

namespace App\Support;

use RuntimeException;

/**
 * Immutable byte limits for an append-only benchmark result set.
 *
 * A snapshot is intentionally opaque to callers. JsonlResultSink validates it
 * again before reading so a forged snapshot cannot escape the configured run
 * directory.
 */
final class JsonlResultSnapshot
{
    /**
     * @param array<string, int> $fileEnds Absolute path => exclusive byte end.
     */
    public function __construct(
        private readonly string $runId,
        private readonly array $fileEnds,
    ) {
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new RuntimeException('Benchmark snapshot run_id has an invalid format.');
        }

        foreach ($fileEnds as $path => $end) {
            if (!is_string($path)
                || $path === ''
                || !str_starts_with($path, DIRECTORY_SEPARATOR)
                || !is_int($end)
                || $end < 0) {
                throw new RuntimeException('Benchmark snapshot contains an invalid file boundary.');
            }
        }
    }

    public function runId(): string
    {
        return $this->runId;
    }

    /** @return array<string, int> */
    public function fileEnds(): array
    {
        return $this->fileEnds;
    }
}
