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

$identifier = static function (string $name, string $default, bool $forbidComma = false): string {
    $value = env($name, $default);
    if (!is_string($value)
        || preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $value) !== 1
        || ($forbidComma && str_contains($value, ','))) {
        throw new \InvalidArgumentException(
            "{$name} must be 1..128 ASCII letters, digits, dot, underscore, colon or dash.",
        );
    }

    return $value;
};

$profile = $oneOf('BENCH_PROFILE', 'fixed', ['fixed', 'auto']);
$workers = $integer('BENCH_WORKERS', 4, 1, 256);
$minWorkers = $profile === 'fixed' ? $workers : $integer('BENCH_MIN_WORKERS', 1, 1, 256);
$maxWorkers = $profile === 'fixed' ? $workers : $integer('BENCH_MAX_WORKERS', $workers, 1, 256);
if ($minWorkers > $maxWorkers) {
    throw new \InvalidArgumentException('BENCH_MIN_WORKERS must not exceed BENCH_MAX_WORKERS.');
}

$timeout = $integer('BENCH_TIMEOUT', 120, 1, 86_400);
$retryAfter = $integer('BENCH_RETRY_AFTER', 180, 2, 86_401);
if ($retryAfter <= $timeout) {
    throw new \InvalidArgumentException('BENCH_RETRY_AFTER must be longer than BENCH_TIMEOUT.');
}

return [
    'profile' => $profile,
    'connection' => $oneOf('BENCH_CONNECTION', 'redis', ['redis', 'queen']),
    'queue' => $identifier('BENCH_QUEUE', 'benchmark', true),
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
    'results_directory' => (string) env('BENCH_RESULTS_DIRECTORY', '/results'),
];
