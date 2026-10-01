<?php

declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use Queen\Laravel\Queue\ProcessLeaseRenewer;

// A worker whose job forks while its lease is renewed, as Laravel's fork
// concurrency driver does. argv[1] = "exit": the child exits at once;
// "subprocess": the child first runs and waits for a subprocess of its own.
$mode = $argv[1] ?? 'exit';

$renewer = new ProcessLeaseRenewer(
    ['url' => 'http://127.0.0.1:9'],
    leaseSeconds: 120,
    intervalSeconds: 30,
    requestTimeoutSeconds: 1,
    requestBudgetSeconds: 1,
);
$renewer->track('lease-forked', intdiv(hrtime(true), 1_000_000) + 120_000);

$child = pcntl_fork();
if ($child === -1) {
    fwrite(STDERR, "Unable to fork.\n");
    exit(2);
}
if ($child === 0) {
    if ($mode === 'subprocess') {
        $pipes = [];
        $process = proc_open(
            [PHP_BINARY, '-r', 'exit(0);'],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            null,
            null,
            ['bypass_shell' => true],
        );
        foreach ($pipes as $pipe) {
            fclose($pipe);
        }
        proc_close($process);
        // Room for the SIGCHLD of that subprocess to be handled.
        usleep(200_000);
    }
    // A normal exit runs every destructor, the inherited renewer's included.
    exit(0);
}

pcntl_waitpid($child, $status);
// Room for a helper told to stop to exit, and for its SIGCHLD to arrive.
usleep(1_000_000);
$renewer->assertHealthy('lease-forked');

fwrite(STDOUT, json_encode([
    'child_exit' => pcntl_wifexited($status) ? pcntl_wexitstatus($status) : null,
    'child_signal' => pcntl_wifsignaled($status) ? pcntl_wtermsig($status) : null,
], JSON_THROW_ON_ERROR) . "\n");
$renewer->forget('lease-forked');
$renewer->close();
