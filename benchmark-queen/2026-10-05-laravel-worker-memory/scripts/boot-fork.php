<?php

// What a worker pays before its first job: booting Laravel in a new PHP
// process, against forking one that is booted, as the fork server does.
//
//   php boot-fork.php boot     boot the console kernel once, print the time
//   php boot-fork.php fork N   boot once, then fork N children one by one and
//                              time each fork until the child runs
//
// Run from the application directory, with the application's environment.

declare(strict_types=1);

use Illuminate\Contracts\Console\Kernel;

$mode = $argv[1] ?? 'boot';
$started = hrtime(true);
$cpu = static function (): float {
    $usage = getrusage();

    return ($usage['ru_utime.tv_sec'] + $usage['ru_stime.tv_sec']) * 1e3
        + ($usage['ru_utime.tv_usec'] + $usage['ru_stime.tv_usec']) / 1e3;
};

require getcwd().'/vendor/autoload.php';
$app = require getcwd().'/bootstrap/app.php';
$app->make(Kernel::class)->bootstrap();
$booted = hrtime(true);

if ($mode === 'boot') {
    echo json_encode([
        'mode' => 'boot',
        'boot_ms' => round(($booted - $started) / 1e6, 2),
        'cpu_ms' => round($cpu(), 2),
        'opcache_cli' => (bool) ini_get('opcache.enable_cli'),
    ]), "\n";
    exit(0);
}

$forks = [];
for ($index = 0, $count = (int) ($argv[2] ?? 20); $index < $count; ++$index) {
    [$read, $write] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    $before = hrtime(true);
    $pid = pcntl_fork();
    if ($pid === 0) {
        fwrite($write, '1');
        // Skip PHP's shutdown: the child only proves that it runs.
        posix_kill(getmypid(), SIGKILL);
    }
    fread($read, 1);
    $forks[] = (hrtime(true) - $before) / 1e6;
    pcntl_waitpid($pid, $status);
    fclose($read);
    fclose($write);
}
sort($forks);
echo json_encode([
    'mode' => 'fork',
    'boot_ms' => round(($booted - $started) / 1e6, 2),
    'forks' => count($forks),
    'fork_ms_median' => round($forks[intdiv(count($forks), 2)], 3),
    'fork_ms_max' => round(end($forks), 3),
    'opcache_cli' => (bool) ini_get('opcache.enable_cli'),
]), "\n";
