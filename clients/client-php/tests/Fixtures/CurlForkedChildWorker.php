<?php

declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use Queen\Http\HttpClient;
use Queen\Tests\Support\KeepAliveServer;

// A worker whose job forks right after its connections were opened, as
// Laravel's fork concurrency driver can. argv[1] = "sync": one synchronous
// request opened a connection; "detached": a detached one did too.
$mode = $argv[1] ?? 'sync';

$server = new KeepAliveServer();
$client = new HttpClient(['baseUrl' => $server->url, 'timeoutMillis' => 5_000]);
$client->get('/echo');
if ($mode === 'detached') {
    $client->settleDetached($client->postDetached('/echo', ['a' => 1]));
}

$child = pcntl_fork();
if ($child === -1) {
    fwrite(STDERR, "Unable to fork.\n");
    exit(2);
}
if ($child === 0) {
    // Only the parent stops the test server.
    (new ReflectionProperty($server, 'process'))->setValue($server, null);
    // A normal exit frees every cURL handle the child inherited.
    exit(0);
}

$exited = false;
$deadline = microtime(true) + 3;
while (microtime(true) < $deadline) {
    if (pcntl_waitpid($child, $status, WNOHANG) === $child) {
        $exited = true;
        break;
    }
    usleep(20_000);
}
if (!$exited) {
    posix_kill($child, SIGKILL);
    pcntl_waitpid($child, $status);
}

fwrite(STDOUT, json_encode(['child_exited' => $exited], JSON_THROW_ON_ERROR) . "\n");
