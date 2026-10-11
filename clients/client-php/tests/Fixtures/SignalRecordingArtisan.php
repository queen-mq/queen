<?php

// A queue:work stand-in for a supervisor's tests: it says it started, then
// waits; on SIGTERM it records when it got it, and exits. Both files go to
// QUEEN_TEST_SIGNAL_DIRECTORY, named after its pid.

$directory = getenv('QUEEN_TEST_SIGNAL_DIRECTORY');
if (!is_string($directory) || $directory === '') {
    fwrite(STDERR, "QUEEN_TEST_SIGNAL_DIRECTORY is not set.\n");
    exit(2);
}
$pid = getmypid();

pcntl_async_signals(true);
pcntl_signal(SIGTERM, static function () use ($directory, $pid): void {
    file_put_contents("{$directory}/{$pid}.sigterm", (string) microtime(true));
    exit(0);
});
file_put_contents("{$directory}/{$pid}.started", (string) microtime(true));

$until = microtime(true) + 60;
while (microtime(true) < $until) {
    usleep(10_000);
}

exit(3);
