<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Queen\Laravel\Dashboard\JobMetricsReader;
use Queen\Laravel\Queue\QueenQueue;
use RuntimeException;

/**
 * The per-class job metrics the workers wrote, as the dashboard's Jobs page
 * reads them, in one JSON line. No cache, so a read sees every write before it.
 */
final class JobMetricsCommand extends Command
{
    protected $signature = 'bench:job-metrics {--range=1h : The window, as the dashboard names it}';

    protected $description = 'Print the per-class job metrics of the window';

    public function handle(QueueFactory $queues): int
    {
        $connection = (string) config('queen.job_metrics.connection', 'queen');
        $driver = $queues->connection($connection);
        if (!$driver instanceof QueenQueue) {
            throw new RuntimeException("Connection [{$connection}] is not a Queen connection.");
        }
        $reader = new JobMetricsReader($driver->getQueen(), (string) config('queen.job_metrics.namespace', 'queen-metrics'));
        $this->line(json_encode($reader->read(JobMetricsReader::range((string) $this->option('range'))),
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }
}
