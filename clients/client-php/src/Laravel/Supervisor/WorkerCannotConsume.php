<?php

namespace Queen\Laravel\Supervisor;

use RuntimeException;

/**
 * Ends a supervised worker that no retry can make consume; see WorkerPopGuard.
 * Thrown from the worker loop, it reaches the application's exception handler
 * and the worker exits 1, which the master counts as a crash.
 */
final class WorkerCannotConsume extends RuntimeException
{
    public static function wrongConnection(string $supervisor, string $worked, string $expected): self
    {
        return new self(
            "Queen supervisor worker of [{$supervisor}] works connection [{$worked}], not its pool's connection "
            . "[{$expected}]; it stops, and the supervisor counts the exit as a crash.",
        );
    }

    public static function popFailed(string $supervisor, string $connection, \Throwable $error): self
    {
        return new self(
            "Queen supervisor worker of [{$supervisor}] cannot pop from connection [{$connection}]: "
            . get_class($error) . ': ' . $error->getMessage()
            . '; it stops, and the supervisor counts the exit as a crash.',
            0,
            $error,
        );
    }
}
