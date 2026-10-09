<?php

namespace Queen\Laravel\Supervisor;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\JobTimedOut;
use Illuminate\Queue\Events\WorkerStopping;

/**
 * Tells a supervisor master why its worker is about to exit.
 *
 * Laravel kills a worker whose job outlives its timeout: the timeout handler
 * dispatches JobTimedOut, then SIGKILLs its own process. Seen from the
 * master, that exit looks like any other SIGKILL, such as the OOM killer's.
 * So the worker first leaves a marker, `<directory>/<pid>` holding `timeout`,
 * in a private directory of the master's state directory, and the master
 * restarts it without counting a crash. Both engines read the marker.
 *
 * A worker that passes --memory exits 12, after a job or after an idle sleep
 * alike. Only after a job is that a deliberate stop: a worker whose boot
 * footprint is above the limit stops the same way on every start. So the
 * marker says `memory` only when this process handled a job.
 *
 * A worker that stops for `php artisan queue:restart` says `restart`
 * (Laravel 12 gives the reason). Its exit is clean either way, but a
 * preforked worker was forked from a fork server that still holds the code it
 * booted: the master starts a new server, so the workers it forks from then
 * on run the code on disk, as spawned ones do.
 *
 * The master passes the directory in QUEEN_SUPERVISOR_EXITS_DIR. A spawned
 * worker registers the listeners at boot, a preforked one right after its fork.
 */
final class WorkerExitMarker
{
    public const ENVIRONMENT = 'QUEEN_SUPERVISOR_EXITS_DIR';

    public const TIMEOUT = 'timeout';

    public const MEMORY = 'memory';

    public const RESTART = 'restart';

    /** Laravel's Worker::EXIT_MEMORY_LIMIT. */
    public const MEMORY_EXIT_CODE = 12;

    public function __construct(private string $directory)
    {
    }

    /** The marker of this worker, or null when no supervisor asked for one. */
    public static function fromEnvironment(): ?self
    {
        $directory = getenv(self::ENVIRONMENT);

        return is_string($directory) && $directory !== '' ? new self($directory) : null;
    }

    public static function listenFromEnvironment(Dispatcher $events): void
    {
        $marker = self::fromEnvironment();
        if ($marker === null) {
            return;
        }

        $events->listen(JobTimedOut::class, static function () use ($marker): void {
            $marker->write(self::TIMEOUT);
        });
        $handledJob = false;
        $events->listen(JobProcessing::class, static function () use (&$handledJob): void {
            $handledJob = true;
        });
        $events->listen(WorkerStopping::class, static function (WorkerStopping $event) use ($marker, &$handledJob): void {
            if ($handledJob && self::stopsForMemory($event)) {
                $marker->write(self::MEMORY);
            } elseif (self::reason($event) === 'restart_signal') {
                $marker->write(self::RESTART);
            }
        });
    }

    private static function stopsForMemory(WorkerStopping $event): bool
    {
        // Laravel 12 says why; before it, the exit status alone tells.
        $reason = self::reason($event);
        if ($reason !== null) {
            return $reason === 'memory';
        }

        return $event->status === self::MEMORY_EXIT_CODE;
    }

    /** Laravel 12's WorkerStopReason value; null before Laravel 12. */
    private static function reason(WorkerStopping $event): ?string
    {
        $reason = $event->reason ?? null;

        return $reason instanceof \BackedEnum ? (string) $reason->value : null;
    }

    /**
     * Never throws: Laravel calls this from its timeout handler, which must
     * still reach the SIGKILL that follows. A marker that could not be
     * written only means the master counts the exit as a crash.
     */
    public function write(string $reason): bool
    {
        try {
            return $this->publish($reason);
        } catch (\Throwable) {
            return false;
        }
    }

    private function publish(string $reason): bool
    {
        $directory = rtrim($this->directory, DIRECTORY_SEPARATOR);
        $pid = getmypid();
        if ($directory === ''
            || !str_starts_with($directory, DIRECTORY_SEPARATOR)
            || preg_match('/[\x00-\x1F\x7F]/', $directory) === 1
            || !is_int($pid)
            || $pid < 1
            || !function_exists('posix_geteuid')) {
            return false;
        }
        // Only into the master's private directory, never through a link.
        clearstatcache(true, $directory);
        $metadata = @lstat($directory);
        if ($metadata === false
            || ($metadata['mode'] & 0170000) !== 0040000
            || ($metadata['mode'] & 07777) !== 0700
            || ($metadata['uid'] ?? null) !== posix_geteuid()) {
            return false;
        }

        $path = $directory . DIRECTORY_SEPARATOR . $pid;
        $temporary = $path . '.tmp';
        // Only this process writes these names. A leftover of an earlier
        // process with the same pid is removed, and 'x' refuses any entry
        // that reappears, a symbolic link included.
        @unlink($temporary);
        $handle = @fopen($temporary, 'xb');
        if ($handle === false) {
            return false;
        }
        $written = @fwrite($handle, $reason);
        $closed = @fclose($handle);
        if ($written !== strlen($reason) || !$closed || !@chmod($temporary, 0600) || !@rename($temporary, $path)) {
            @unlink($temporary);

            return false;
        }

        return true;
    }
}
