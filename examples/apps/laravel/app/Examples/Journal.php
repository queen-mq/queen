<?php

namespace App\Examples;

use Illuminate\Contracts\Queue\Job;

/**
 * What the jobs of an example record, one JSON line per event, in a file per
 * queue under storage/examples/. Workers are separate processes, so a file is
 * the simplest place they can all write to and the example can read back.
 */
final class Journal
{
    /** Append an event of the job running in $job: the worker's pid and the time are added. */
    public static function record(Job $job, array $fields): void
    {
        $row = [...$fields, 'pid' => getmypid(), 'at' => microtime(true)];
        file_put_contents(self::path($job->getQueue()), json_encode($row) . "\n", FILE_APPEND | LOCK_EX);
    }

    /** @return list<array<string, mixed>> the events recorded on $queue, oldest first */
    public static function read(string $queue, ?string $event = null): array
    {
        $path = self::path($queue);
        if (!is_file($path)) {
            return [];
        }
        $rows = [];
        foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
            $row = json_decode($line, true);
            if (is_array($row) && ($event === null || ($row['event'] ?? null) === $event)) {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    /** Where a worker of $queue leaves a mark once it has booted. */
    public static function readyPath(string $queue, int $pid): string
    {
        return storage_path("examples/{$queue}-ready-{$pid}");
    }

    public static function path(string $queue): string
    {
        return storage_path('examples/' . $queue . '.jsonl');
    }
}
