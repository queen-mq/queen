<?php

// A fork server that runs, as queen:fork-server does, under an error handler
// that throws every reported warning (Laravel's HandleExceptions), and that
// may start no process: every fork fails with EAGAIN, as at a pids or nproc
// limit.

require __DIR__ . '/../../../vendor/autoload.php';

use Queen\Laravel\Supervisor\Prefork\ForkServer;

set_error_handler(static function (int $level, string $message, string $file = '', int $line = 0): bool {
    if ((error_reporting() & $level) === 0) {
        return false;
    }
    throw new ErrorException($message, 0, $level, $file, $line);
});
$hard = posix_getrlimit()['hard maxproc'] ?? 'unlimited';
posix_setrlimit(POSIX_RLIMIT_NPROC, 1, is_numeric($hard) ? (int) $hard : POSIX_RLIMIT_INFINITY);

$events = fopen('php://fd/3', 'wb');

exit((new ForkServer(STDIN, $events, static fn (array $argv): int => 0))->serve());
