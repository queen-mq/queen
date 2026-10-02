<?php

declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use Queen\Laravel\Queue\ProcessLeaseRenewer;

// A worker with the real renewal helper: it holds lease-x with three jobs
// journaled (the first running, the second unstarted, the third done), then
// crashes, or closes its renewer as a clean exit does.
[, $url, $mode] = $argv;

$renewer = new ProcessLeaseRenewer(
    ['url' => $url, 'bearerToken' => 'worker-secret'],
    leaseSeconds: 120,
    intervalSeconds: 30,
    requestTimeoutSeconds: 1,
    requestBudgetSeconds: 1,
);
$renewer->track('lease-x', intdiv(hrtime(true), 1_000_000) + 120_000);
$journal = $renewer->handBackJournal();
if ($journal === null) {
    fwrite(STDERR, "The renewal helper offered no hand-back journal.\n");
    exit(2);
}

$entry = static fn (int $number): array => [
    'ack' => [
        'type' => 'ack', 'transactionId' => "t{$number}", 'partitionId' => 'p', 'status' => 'completed',
        'consumerGroup' => 'laravel', 'leaseId' => 'lease-x',
    ],
    'unstarted' => ['type' => 'push', 'items' => [['queue' => 'q', 'partition' => 'p', 'transactionId' => "c{$number}",
        'payload' => ['uuid' => "job-{$number}", '_queen' => ['attempts' => 0]]]]],
    'ran' => ['type' => 'push', 'items' => [['queue' => 'q', 'partition' => 'p', 'transactionId' => "c{$number}",
        'payload' => ['uuid' => "job-{$number}", '_queen' => ['attempts' => 1]]]]],
];
$journal->plan('lease-x', [$entry(1), $entry(2), $entry(3)]);
$journal->owe('ru-');
$prefix = (new ReflectionProperty($journal, 'pathPrefix'))->getValue($journal);
fwrite(STDOUT, json_encode(['journal' => $prefix], JSON_THROW_ON_ERROR) . "\n");
fflush(STDOUT);

if ($mode === 'close') {
    $renewer->close();
    exit(0);
}
// As the kernel's OOM killer would: no shutdown, no destructor.
posix_kill(getmypid(), SIGKILL);
