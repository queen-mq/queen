<?php

// A fork server whose "worker" reports what it received instead of running
// queue:work: argv is [mode, report file, exit code].

require __DIR__ . '/../../../vendor/autoload.php';

use Queen\Laravel\Supervisor\Prefork\ForkServer;

$events = fopen('php://fd/3', 'wb');

exit((new ForkServer(STDIN, $events, function (array $argv): int {
    [$mode, $report, $code] = $argv + [null, null, '0'];
    file_put_contents($report, json_encode([
        'argv' => $argv,
        'value' => getenv('QUEEN_TEST_VALUE'),
        'removed' => getenv('QUEEN_TEST_REMOVED'),
        'pid' => getmypid(),
        'pgid' => posix_getpgid(0),
    ]));
    while ($mode === 'sleep') {
        usleep(50_000);
    }

    return (int) $code;
}))->serve());
