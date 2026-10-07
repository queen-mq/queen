<?php

namespace App\Console\Commands\Examples;

use App\Examples\ExampleCommand;
use App\Jobs\ProbeBatch;
use Illuminate\Support\Facades\Queue;

// docs:start(app-laravel-prefetch)
final class PrefetchExample extends ExampleCommand
{
    protected $signature = 'example:prefetch';

    protected $description = 'How prefetch fills a batch: prefetch 1, then auto';

    protected function example(): void
    {
        $this->line("\na) prefetch 1: 40 jobs of 10 ms, one worker");
        $sizes = $this->drain('queen', 'one', 40, 10);
        $this->check($sizes === array_fill(0, 40, 1), 'every pop took one job');

        $this->line("\nb) prefetch 'auto': a backlog of 200 jobs of 10 ms, one worker");
        $sizes = $this->drain('queen-auto', 'backlog', 200, 10);
        $this->check(
            array_slice($sizes, 0, 8) === [1, 1, 2, 2, 4, 4, 8, 8],
            'the batch doubled after every two full pops: 1 1 2 2 4 4 8 8',
        );
        $grewAtMostDouble = true;
        for ($i = 1; $i < count($sizes); $i++) {
            $grewAtMostDouble = $grewAtMostDouble && $sizes[$i] <= 2 * $sizes[$i - 1];
        }
        $this->check(
            $grewAtMostDouble && max($sizes) <= 16,
            'no batch was more than twice the one before it, or above 16',
        );
        $this->check(
            count($sizes) * 3 <= 200,
            sprintf('a pop served %.1f jobs on average, 3 or more', 200 / count($sizes)),
        );

        $this->line("\nc) prefetch 'auto': 10 jobs of 300 ms, one worker");
        $sizes = $this->drain('queen-auto', 'long', 10, 300);
        $this->check(
            $sizes === array_fill(0, 10, 1),
            'every pop took one job: one of them is more than 250 ms of work',
        );
    }

    /**
     * Queue $jobs jobs of $millis ms on a fresh queue, then let one worker
     * run them all, and print what its pops took.
     *
     * @return list<int> the number of jobs of each pop, in order
     */
    private function drain(string $connection, string $name, int $jobs, int $millis): array
    {
        $queue = $this->freshQueue("prefetch-{$name}");
        // The whole backlog first, in bulk requests of up to 100 jobs.
        $batch = [];
        for ($seq = 1; $seq <= $jobs; $seq++) {
            $batch[] = new ProbeBatch($seq, $millis);
        }
        Queue::connection($connection)->bulk($batch, '', $queue);

        $this->startWorkers(1, $connection, $queue);
        $ran = $this->waitForEvents($queue, 'ran', $jobs, 60);
        $this->stopProcesses();

        // One lease per pop: group the jobs by lease, in the order they ran.
        $pops = [];
        $gaps = [];
        foreach ($ran as $i => $job) {
            if (isset($pops[$job['lease']]) && $ran[$i - 1]['lease'] === $job['lease']) {
                $gaps[] = $job['at'] - $ran[$i - 1]['at'];
            }
            $pops[$job['lease']] = ($pops[$job['lease']] ?? 0) + 1;
        }
        $sizes = array_values($pops);

        $this->line(sprintf('  jobs           %d', count($ran)));
        $this->line(sprintf('  pops           %d', count($sizes)));
        $this->line(sprintf('  jobs per pop   %.1f', count($ran) / count($sizes)));
        $sizesText = wordwrap($this->runLengths($sizes), 54, "\n" . str_repeat(' ', 17));
        $this->line("  batch sizes    {$sizesText}");
        if ($gaps !== []) {
            sort($gaps);
            $each = $gaps[intdiv(count($gaps), 2)] * 1000;
            $this->line(sprintf(
                '  one job took about %d ms, its ACK included: 250 ms holds %d',
                $each,
                intdiv(250, max(1, (int) $each)),
            ));
        }

        return $sizes;
    }
}
// docs:end
