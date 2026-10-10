<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;
use Illuminate\Queue\Jobs\RedisJob;
use Queen\Laravel\Queue\QueenJob;

/**
 * Logs where a worker ran it: the connection and queue Laravel gave the job,
 * the pool settings the supervisor handed the worker, and the settings of the
 * queue instance that popped it, read from its private state because no
 * public method returns them. A fixture's look inside, not an API.
 */
final class CompatRouteJob extends CompatJob
{
    protected function work(FailureMatrixLog $log): void
    {
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode, null, $this->where());
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }

    /** @return array<string, mixed> */
    private function where(): array
    {
        $job = $this->job;
        $cmdline = (string) @file_get_contents('/proc/self/cmdline');
        $where = [
            'connection' => $job?->getConnectionName(),
            'queue' => $job?->getQueue(),
            'process' => trim(str_replace("\0", ' ', $cmdline)),
            'pool_connection' => getenv('QUEEN_LARAVEL_CONNECTION') ?: null,
            'pool_retry_after' => getenv('QUEEN_LARAVEL_RETRY_AFTER') ?: null,
            'pool_consumer_group' => getenv('QUEEN_LARAVEL_CONSUMER_GROUP') ?: null,
            'partition' => $job?->payload()['_queen']['partition'] ?? null,
        ];
        if ($job instanceof QueenJob) {
            $queue = (fn () => $this->queen)->call($job);
            $where += (fn (): array => [
                'retry_after' => $this->retryAfter,
                'consumer_group' => $this->consumerGroup,
                'partitions' => $this->partitionCount,
                'partition_prefix' => $this->partitionPrefix,
            ])->call($queue);
            $message = $job->getQueenMessage();
            $where['message_partition'] = $message['partitionName'] ?? $message['partition'] ?? null;
        } elseif ($job instanceof RedisJob) {
            $queue = (fn () => $this->redis)->call($job);
            $where['retry_after'] = (fn () => $this->retryAfter)->call($queue);
        }

        return $where;
    }
}
