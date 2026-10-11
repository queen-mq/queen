<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use InvalidArgumentException;
use Queen\Laravel\Queue\QueenQueue;
use RuntimeException;

/**
 * Push messages that carry no Laravel job straight to a Queen queue, as
 * another producer of the same queue would: the run ID is in each one, so
 * bench:matrix-report finds them in the dead-letter queue.
 *
 * Kinds:
 * - `not-json`: a string that is not JSON;
 * - `no-job`: a JSON object that names no job;
 * - `missing-class`: a Laravel payload whose command class does not exist,
 *   which is a Laravel job that fails when its worker unserializes it.
 */
final class FailureMatrixRawCommand extends Command
{
    public const KINDS = ['not-json', 'no-job', 'missing-class'];

    protected $signature = 'bench:matrix-raw
        {--run-id= : Run identifier}
        {--kind=no-job : not-json, no-job or missing-class}
        {--partition= : The Queen partition}
        {--connection= : Queue connection; defaults to BENCH_CONNECTION}
        {--queue= : Queue name; defaults to BENCH_QUEUE}';

    protected $description = 'Push a message that is not a runnable Laravel job';

    public function handle(QueueFactory $queues): int
    {
        $runId = (string) $this->option('run-id');
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new InvalidArgumentException('--run-id is required: 1..128 letters, digits, dot, underscore, colon or dash.');
        }
        $kind = (string) $this->option('kind');
        if (!in_array($kind, self::KINDS, true)) {
            throw new InvalidArgumentException('--kind must be one of: ' . implode(', ', self::KINDS) . '.');
        }
        $partition = (string) $this->option('partition');
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $partition) !== 1) {
            throw new InvalidArgumentException('--partition is required: 1..128 letters, digits, dot, underscore, colon or dash.');
        }
        $connection = (string) ($this->option('connection') ?: config('benchmark.connection'));
        $queue = (string) ($this->option('queue') ?: config('benchmark.queue'));
        $driver = $queues->connection($connection);
        if (!$driver instanceof QueenQueue) {
            throw new InvalidArgumentException("Connection [{$connection}] is not a Queen connection.");
        }

        $data = match ($kind) {
            'not-json' => "not json: {$runId}",
            'no-job' => ['event' => 'order.shipped', 'run_id' => $runId],
            'missing-class' => [
                'uuid' => (string) \Illuminate\Support\Str::uuid(),
                'displayName' => 'App\\Jobs\\RemovedInTheLastDeploy',
                'job' => 'Illuminate\\Queue\\CallQueuedHandler@call',
                'maxTries' => 1,
                'timeout' => 10,
                'data' => [
                    'commandName' => 'App\\Jobs\\RemovedInTheLastDeploy',
                    'command' => 'O:31:"App\\Jobs\\RemovedInTheLastDeploy":1:{s:5:"runId";s:'
                        . strlen($runId) . ':"' . $runId . '";}',
                ],
                '_queen' => ['attempts' => 0, 'partition' => $partition],
            ],
        };
        $result = $driver->getQueen()->queue($queue)->partition($partition)->push([['data' => $data]])->execute();
        if (!is_array($result) || !in_array($result[0]['status'] ?? null, ['queued', 'buffered'], true)) {
            throw new RuntimeException('The broker refused the push: ' . json_encode($result));
        }
        $this->line(json_encode(['run_id' => $runId, 'kind' => $kind, 'queue' => $queue, 'partition' => $partition],
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }
}
