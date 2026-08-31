<?php

declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use Queen\Laravel\Queue\ProcessLeaseRenewer;

$previousHandler = pcntl_signal_get_handler(SIGCHLD);
$previousAsync = pcntl_async_signals();
$observedPid = 0;
pcntl_async_signals(true);
pcntl_signal(SIGCHLD, static function (int $signal, ?array $info = null) use (&$observedPid): void {
    $observedPid = (int) ($info['pid'] ?? -1);
});

$renewer = new ProcessLeaseRenewer(
    ['url' => 'http://127.0.0.1:9'],
    leaseSeconds: 120,
    intervalSeconds: 30,
    requestTimeoutSeconds: 1,
    requestBudgetSeconds: 1,
);

try {
    $renewer->track('lease-isolation', intdiv(hrtime(true), 1_000_000) + 120_000);

    $pipes = [];
    $unrelated = proc_open(
        [PHP_BINARY, '-r', 'exit(0);'],
        [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
        $pipes,
        null,
        null,
        ['bypass_shell' => true],
    );
    if (!is_resource($unrelated)) {
        throw new RuntimeException('Unable to start an unrelated child.');
    }
    $status = proc_get_status($unrelated);
    $unrelatedPid = (int) ($status['pid'] ?? 0);
    foreach ($pipes as $pipe) {
        if (is_resource($pipe)) {
            fclose($pipe);
        }
    }
    proc_close($unrelated);

    $deadline = microtime(true) + 2;
    while ($observedPid !== $unrelatedPid && microtime(true) < $deadline) {
        usleep(10_000);
    }
    $renewer->assertHealthy('lease-isolation');

    fwrite(STDOUT, json_encode([
        'unrelated_pid' => $unrelatedPid,
        'observed_pid' => $observedPid,
        'worker_alive' => true,
    ], JSON_THROW_ON_ERROR) . "\n");
} finally {
    $renewer->forget('lease-isolation');
    $renewer->close();
    pcntl_signal(
        SIGCHLD,
        is_callable($previousHandler) || is_int($previousHandler) ? $previousHandler : SIG_DFL,
        true,
    );
    pcntl_async_signals($previousAsync);
}
