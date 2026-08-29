<?php

$integer = static function (string $name, int $default, int $minimum, int $maximum): int {
    $raw = env($name, $default);
    if (is_int($raw)) {
        $value = $raw;
    } elseif (is_string($raw) && preg_match('/^(0|[1-9][0-9]*)$/D', $raw) === 1) {
        $value = filter_var($raw, FILTER_VALIDATE_INT);
    } else {
        $value = false;
    }

    if ($value === false || $value < $minimum || $value > $maximum) {
        throw new \InvalidArgumentException("{$name} must be an integer in {$minimum}..{$maximum}.");
    }

    return $value;
};

$number = static function (string $name, float $default, float $minimum): float {
    $raw = env($name, $default);
    if (!is_int($raw) && !is_float($raw) && !is_string($raw)) {
        throw new \InvalidArgumentException("{$name} must be a number >= {$minimum}.");
    }
    $value = filter_var($raw, FILTER_VALIDATE_FLOAT);
    if ($value === false || !is_finite((float) $value) || $value < $minimum) {
        throw new \InvalidArgumentException("{$name} must be a number >= {$minimum}.");
    }

    return (float) $value;
};

$oneOf = static function (string $name, string $default, array $allowed): string {
    $value = env($name, $default);
    if (!is_string($value) || !in_array($value, $allowed, true)) {
        throw new \InvalidArgumentException("{$name} must be one of: ".implode(', ', $allowed).'.');
    }

    return $value;
};

$validateIdentifier = static function (mixed $value, string $name, bool $forbidComma = false): string {
    if (!is_string($value)
        || preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $value) !== 1
        || ($forbidComma && str_contains($value, ','))) {
        throw new \InvalidArgumentException(
            "{$name} must be 1..128 ASCII letters, digits, dot, underscore, colon or dash.",
        );
    }

    return $value;
};

$identifier = static function (string $name, string $default, bool $forbidComma = false) use ($validateIdentifier): string {
    return $validateIdentifier(env($name, $default), $name, $forbidComma);
};

$identifierList = static function (string $name, array $default) use ($validateIdentifier): array {
    $raw = env($name);
    if ($raw === null || $raw === '') {
        return $default;
    }
    if (!is_string($raw)) {
        throw new \InvalidArgumentException("{$name} must be a comma-separated queue list.");
    }

    $values = explode(',', $raw);
    if (count($values) > 256) {
        throw new \InvalidArgumentException("{$name} may not contain more than 256 queues.");
    }

    $resolved = [];
    $seen = [];
    foreach ($values as $value) {
        if ($value === '' || $value !== trim($value)) {
            throw new \InvalidArgumentException(
                "{$name} must not contain empty queue names or surrounding whitespace.",
            );
        }
        // Reuse the same strict queue-name grammar as BENCH_QUEUE without
        // letting a CSV entry smuggle in another separator.
        $queue = $validateIdentifier($value, $name, true);
        if (isset($seen['queue:'.$queue])) {
            throw new \InvalidArgumentException("{$name} contains duplicate queue [{$queue}].");
        }
        $seen['queue:'.$queue] = true;
        $resolved[] = $queue;
    }

    return $resolved;
};

$profile = $oneOf('BENCH_PROFILE', 'fixed', ['fixed', 'auto']);
$workers = $integer('BENCH_WORKERS', 4, 1, 256);
$minWorkers = $profile === 'fixed' ? $workers : $integer('BENCH_MIN_WORKERS', 1, 1, 256);
$maxWorkers = $profile === 'fixed' ? $workers : $integer('BENCH_MAX_WORKERS', $workers, 1, 256);
if ($minWorkers > $maxWorkers) {
    throw new \InvalidArgumentException('BENCH_MIN_WORKERS must not exceed BENCH_MAX_WORKERS.');
}

$legacyQueue = $identifier('BENCH_QUEUE', 'benchmark', true);
$queues = $identifierList('BENCH_QUEUES', [$legacyQueue]);
$queue = $queues[0];
if (($profile === 'fixed' ? $workers : $maxWorkers) < count($queues)) {
    throw new \InvalidArgumentException(
        'The fixed worker count or auto max worker count must cover every BENCH_QUEUES entry.',
    );
}

$timeout = $integer('BENCH_TIMEOUT', 120, 1, 86_400);
$retryAfter = $integer('BENCH_RETRY_AFTER', 180, 2, 86_401);
if ($retryAfter <= $timeout) {
    throw new \InvalidArgumentException('BENCH_RETRY_AFTER must be longer than BENCH_TIMEOUT.');
}

$queenPrefetch = $integer('QUEEN_PREFETCH', 1, 1, 1_000);
$queenAckBatch = $integer('QUEEN_ACK_BATCH', 1, 1, 1_000);
if ($queenAckBatch > $queenPrefetch) {
    throw new \InvalidArgumentException('QUEEN_ACK_BATCH must not exceed QUEEN_PREFETCH.');
}

$resultsDirectory = env('BENCH_RESULTS_DIRECTORY', '/results');
if (!is_string($resultsDirectory)
    || !str_starts_with($resultsDirectory, DIRECTORY_SEPARATOR)
    || rtrim($resultsDirectory, DIRECTORY_SEPARATOR) === ''
    || preg_match('#(?:^|/)(?:\.|\.\.)(?:/|$)#', $resultsDirectory) === 1
    || preg_match('/[\x00-\x1F\x7F]/', $resultsDirectory) === 1) {
    throw new \InvalidArgumentException(
        'BENCH_RESULTS_DIRECTORY must be an absolute, non-root path without dot segments.',
    );
}
$resultsDirectory = rtrim($resultsDirectory, DIRECTORY_SEPARATOR);

// Laravel's env() helper normalizes the literal string "null" to null. Read
// this enum from the process environment so Compose can explicitly select the
// framework's null failed-job driver without turning it into an invalid value.
$failedDriver = getenv('BENCH_FAILED_DRIVER');
$failedDriver = $failedDriver === false ? env('BENCH_FAILED_DRIVER', 'null') : $failedDriver;
// Laravel normalizes the literal `.env` value `null` to PHP null. It still
// names the framework's null failed-job driver for this strict enum.
$failedDriver = $failedDriver === null ? 'null' : $failedDriver;
if (!is_string($failedDriver) || !in_array($failedDriver, ['null', 'file'], true)) {
    throw new \InvalidArgumentException('BENCH_FAILED_DRIVER must be one of: null, file.');
}
$failedPath = env('BENCH_FAILED_PATH', $resultsDirectory.'/failed-jobs.json');
if (!is_string($failedPath)
    || !str_starts_with($failedPath, $resultsDirectory.'/')
    || str_ends_with($failedPath, DIRECTORY_SEPARATOR)
    || preg_match('#(?:^|/)(?:\.|\.\.)(?:/|$)#', $failedPath) === 1
    || preg_match('/[\x00-\x1F\x7F]/', $failedPath) === 1) {
    throw new \InvalidArgumentException(
        'BENCH_FAILED_PATH must be a file below BENCH_RESULTS_DIRECTORY without dot segments.',
    );
}

return [
    'profile' => $profile,
    'connection' => $oneOf('BENCH_CONNECTION', 'redis', ['redis', 'queen']),
    // BENCH_QUEUE remains the one-queue compatibility input. When
    // BENCH_QUEUES is present its first entry becomes the default dispatch
    // queue and the complete list is shared by all three supervisors.
    'queue' => $queue,
    'queues' => $queues,
    'consumer_group' => $identifier('BENCH_GROUP', 'benchmark'),
    'workers' => $workers,
    'min_workers' => $minWorkers,
    'max_workers' => $maxWorkers,
    'strategy' => $oneOf('BENCH_STRATEGY', 'time', ['size', 'time']),
    'balance_cooldown' => $integer('BENCH_BALANCE_COOLDOWN', 3, 1, 3_600),
    'balance_max_shift' => $integer('BENCH_BALANCE_MAX_SHIFT', 1, 1, 256),
    'scale_down_delay' => $integer('BENCH_SCALE_DOWN_DELAY', 0, 0, 3_600),
    'target_jobs_per_process' => $integer('BENCH_TARGET_JOBS_PER_PROCESS', 10, 1, 1_000_000),
    'target_clear_seconds' => $number('BENCH_TARGET_CLEAR_SECONDS', 60.0, 0.001),
    'default_runtime_seconds' => $number('BENCH_DEFAULT_RUNTIME_SECONDS', 0.01, 0.000001),
    'poll_interval' => $integer('BENCH_POLL_INTERVAL', 1, 1, 3_600),
    'block_for' => $integer('BENCH_BLOCK_FOR', 1, 0, 60),
    'worker_sleep' => $integer('BENCH_WORKER_SLEEP', 1, 0, 60),
    'timeout' => $timeout,
    'retry_after' => $retryAfter,
    'worker_memory' => $integer('BENCH_WORKER_MEMORY', 128, 16, 32_768),
    'results_directory' => $resultsDirectory,
    'dispatch_mode' => $oneOf('BENCH_DISPATCH_MODE', 'single', ['single', 'bulk']),
    'ledger_mode' => $oneOf('BENCH_LEDGER_MODE', 'off', ['off', 'durable']),
    // The timed campaigns keep this disabled. GA feature probes opt into a
    // shared file repository on the results volume and exercise Laravel's
    // normal queue:failed/retry/forget lifecycle outside performance lanes.
    'failed_driver' => $failedDriver,
    'failed_path' => $failedPath,
    'failed_limit' => $integer('BENCH_FAILED_LIMIT', 1000, 1, 1_000_000),
    'queen_prefetch' => $queenPrefetch,
    'queen_ack_batch' => $queenAckBatch,
    'queen_bulk_batch' => $integer('QUEEN_BULK_BATCH', 100, 1, 1_000),
    'queen_partitions' => $integer('QUEEN_PARTITIONS', 64, 1, 64),
    'queen_pop_fusion' => $integer('QUEEN_POP_FUSION', 0, 0, 1) === 1,
];
