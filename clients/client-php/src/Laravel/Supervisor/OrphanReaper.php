<?php

namespace Queen\Laravel\Supervisor;

/**
 * Reaps the orphans that a PHP master inherits as PID 1.
 *
 * A container without an init process, as a Kubernetes pod is unless it asks
 * for one, makes the master PID 1. A process that a job starts and leaves
 * behind, or the lease helper of a worker that was killed, then reparents to
 * the master. When it ends, nothing waits for it, and its zombie holds a pid
 * until the container has none left to start a worker with. PHP cannot make
 * the master a child subreaper, so orphans come to it only as PID 1, and only
 * then does it reap them: on Linux, every zombie child it does not track. A
 * worker or a fork server that it tracks is never reaped here, so its exit
 * status still reaches the supervisor.
 */
final class OrphanReaper
{
    /**
     * @param bool|null $init whether this process inherits orphans; null asks
     *        whether it is PID 1
     */
    public function __construct(private string $proc = '/proc', private ?bool $init = null)
    {
    }

    /**
     * @param list<int> $tracked the pids of the children the master tracks
     * @return list<int> the pids reaped
     */
    public function reap(array $tracked): array
    {
        $self = getmypid();
        if (!is_int($self) || !($this->init ?? $self === 1)
            || !function_exists('pcntl_waitpid') || !is_dir($this->proc)) {
            return [];
        }
        $tracked = array_flip($tracked);
        $reaped = [];
        foreach (glob($this->proc . '/[0-9]*/stat', GLOB_NOSORT) ?: [] as $file) {
            $stat = @file_get_contents($file);
            // "pid (comm) state ppid ...", where comm may hold spaces and parentheses.
            $close = is_string($stat) ? strrpos($stat, ')') : false;
            if ($close === false) {
                continue;
            }
            $pid = (int) $stat;
            $fields = explode(' ', substr($stat, $close + 2), 3);
            if (($fields[0] ?? '') !== 'Z' || (int) ($fields[1] ?? 0) !== $self || isset($tracked[$pid])) {
                continue;
            }
            if (pcntl_waitpid($pid, $status, WNOHANG) === $pid) {
                $reaped[] = $pid;
            }
        }

        return $reaped;
    }
}
