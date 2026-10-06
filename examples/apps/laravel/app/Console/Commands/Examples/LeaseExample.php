<?php

namespace App\Console\Commands\Examples;

use App\Examples\ExampleCommand;
use App\Examples\Journal;
use App\Jobs\BuildReport;
use Illuminate\Support\Facades\Queue;
use Queen\Queen;

// docs:start(app-laravel-lease)
final class LeaseExample extends ExampleCommand
{
    protected $signature = 'example:lease';

    protected $description = 'A job longer than its lease, without and with lease renewal';

    private const JOB_SECONDS = 8;

    private const LEASE_SECONDS = 6; // retry_after of both connections

    protected function example(): void
    {
        $this->line(sprintf(
            "\na) lease of %d s, not renewed, a job of %d s, --tries=2",
            self::LEASE_SECONDS,
            self::JOB_SECONDS,
        ));
        $events = $this->runOneJob('queen-short-lease', 'expiring');

        $started = $events['started'] ?? [];
        $this->check(count($started) === 2, 'the job ran twice');
        $this->check(
            $started[0]['pid'] !== $started[1]['pid'],
            'the second run was on the other worker',
        );
        $gap = $started[1]['at'] - $started[0]['at'];
        $this->check(
            $gap >= self::LEASE_SECONDS - 0.5,
            sprintf('the second run began when the first lease expired, %.1f s in', $gap),
        );
        $refused = array_filter(
            $events['exception'] ?? [],
            fn ($e) => str_contains($e['error'], 'expired lease'),
        );
        $this->check(count($refused) === 2, 'both ACKs were refused: each run outlived its lease');
        $this->check(
            count($events['failed'] ?? []) === 1,
            'the third delivery failed the job without running it',
        );
        $dead = app(Queen::class)->queue($events['queue'])->dlq()->limit(10)->get();
        $this->check(
            count($dead['messages'] ?? []) === 1,
            'the job is in the dead-letter queue, never completed',
        );

        $this->line("\nb) the same lease, renewed every second, the same job");
        $events = $this->runOneJob('queen-renewed-lease', 'renewed');

        $this->check(count($events['started'] ?? []) === 1, 'the job ran once');
        $this->check(count($events['exception'] ?? []) === 0, 'no ACK was refused');
        $this->check(count($events['acked'] ?? []) === 1, 'its ACK was accepted');
        $this->check(
            Queue::connection('queen-renewed-lease')->size($events['queue']) === 0,
            'nothing is left in the queue',
        );
    }

    /**
     * One job, two workers, a fresh queue. Waits until the job is either
     * acknowledged or failed, and every run of it has ended.
     *
     * @return array<string, mixed> the job's events by kind, and the queue
     */
    private function runOneJob(string $connection, string $name): array
    {
        $queue = $this->freshQueue("lease-{$name}");
        BuildReport::dispatch(self::JOB_SECONDS)->onConnection($connection)->onQueue($queue);
        $t0 = microtime(true);
        // --timeout=20: Laravel would stop the job after 20 s, long after it
        // ends. --tries=2: a third delivery fails the job instead of running it.
        [$one, $two] = $this->startWorkers(2, $connection, $queue, ['timeout' => 20, 'tries' => 2]);

        $this->waitFor(
            'the job to be acknowledged or failed',
            40,
            fn () => Journal::read($queue, 'acked') ?: Journal::read($queue, 'failed'),
        );
        // A run ends with its ACK, accepted or refused.
        $ended = fn () => count(Journal::read($queue, 'acked')) + count(Journal::read($queue, 'exception'));
        $this->waitFor('every run to end', 20, fn () => $ended() === count(Journal::read($queue, 'started')));
        $this->stopProcesses();

        $events = ['queue' => $queue];
        $workers = [$one => 'w1', $two => 'w2'];
        foreach (Journal::read($queue) as $row) {
            $events[$row['event']][] = $row;
            $what = match ($row['event']) {
                'started' => "started attempt {$row['attempt']}",
                'acked' => 'ended, ACK accepted',
                // The driver says "Unable to acknowledge job: <the broker's reason>".
                'exception' => 'ended, ACK refused: '
                    . preg_replace('/^Unable to acknowledge job: /', '', $row['error']),
                'failed' => 'failed: ' . str_replace(BuildReport::class, 'BuildReport', $row['error']),
            };
            $this->line(sprintf('  %5.1f s  %s  %s', $row['at'] - $t0, $workers[$row['pid']], $what));
        }

        return $events;
    }
}
// docs:end
