<?php

declare(strict_types=1);

// This launcher becomes the Artisan worker through exec(), so the PID tracked
// by the PHP supervisor remains stable while every worker receives a private
// process group. Forced shutdown can then contain subprocesses created by job
// code instead of killing only the queue:work leader.
if (PHP_SAPI !== 'cli'
    || !function_exists('posix_setsid')
    || !function_exists('pcntl_exec')
    || count($argv) < 3) {
    fwrite(STDERR, "Queen worker launcher requires CLI, ext-posix, ext-pcntl and a command.\n");
    exit(126);
}

$session = @posix_setsid();
if (!is_int($session) || $session < 0) {
    fwrite(STDERR, "Queen worker launcher could not create a private process group.\n");
    exit(126);
}

$binary = $argv[1];
$arguments = array_slice($argv, 2);
pcntl_exec($binary, $arguments);

$error = error_get_last();
fwrite(STDERR, 'Queen worker launcher could not exec the worker'
    . (is_array($error) && is_string($error['message'] ?? null) ? ': ' . $error['message'] : '')
    . ".\n");
exit(126);
