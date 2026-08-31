<?php

declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use Queen\Laravel\Queue\ProcessLeaseRenewer;

$helperCode = <<<'PHP'
fgets(STDIN);
fwrite(STDOUT, "{\"event\":\"ready\"}\n");
fflush(STDOUT);
$line = fgets(STDIN);
$command = is_string($line) ? json_decode($line, true) : null;
$lease = is_array($command) ? ($command['lease_id'] ?? '') : '';
fwrite(STDOUT, json_encode(['event' => 'tracked', 'lease_id' => $lease])."\n");
fflush(STDOUT);
while (true) {
    usleep(100000);
}
PHP;

$renewer = new ProcessLeaseRenewer(
    ['url' => 'http://127.0.0.1:9'],
    leaseSeconds: 120,
    intervalSeconds: 30,
    requestTimeoutSeconds: 1,
    requestBudgetSeconds: 1,
    workerCommand: [PHP_BINARY, '-r', $helperCode],
);

$renewer->track('lease-watchdog', intdiv(hrtime(true), 1_000_000) + 120_000);
$process = (new ReflectionProperty($renewer, 'process'))->getValue($renewer);
$status = is_resource($process) ? proc_get_status($process) : [];
$pid = (int) ($status['pid'] ?? 0);
if ($pid < 1) {
    fwrite(STDERR, "Unable to resolve the renewal helper PID.\n");
    exit(2);
}

fwrite(STDOUT, json_encode(['helper_pid' => $pid], JSON_THROW_ON_ERROR) . "\n");
fflush(STDOUT);

while (true) {
    usleep(100_000);
}
