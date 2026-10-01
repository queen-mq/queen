<?php

namespace App\Console\Commands;

use App\Support\FailureMatrixLog;
use Illuminate\Console\Command;
use Throwable;

/**
 * What happened to a failure-matrix run: the attempts of every job, the
 * Laravel failed-job rows, and on Queen the broker's dead-letter entries.
 */
final class FailureMatrixReportCommand extends Command
{
    protected $signature = 'bench:matrix-report {run-id}
        {--queue= : Queue name; defaults to BENCH_QUEUE}';

    protected $description = 'Print a failure-matrix run as one JSON object';

    public function handle(FailureMatrixLog $log): int
    {
        $runId = (string) $this->argument('run-id');
        $jobs = [];
        foreach ($log->read($runId) as $event) {
            $job = $event['job_id'];
            $jobs[$job] ??= ['events' => [], 'pids' => []];
            $jobs[$job]['events'][] = [$event['event'], $event['attempt'], round($event['at'], 3), $event['exception']];
            $jobs[$job]['pids'][$event['pid']] = true;
        }
        foreach ($jobs as &$job) {
            $job['pids'] = array_keys($job['pids']);
        }
        unset($job);
        ksort($jobs);

        $this->line(json_encode([
            'run_id' => $runId,
            // An object even when empty: job ids are the keys.
            'jobs' => (object) $jobs,
            'failed_store' => $this->failedStore($runId),
            'dead_letter' => $this->deadLetter($runId),
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }

    /** @return array{available: bool, job_ids?: list<string>, error?: string} */
    private function failedStore(string $runId): array
    {
        try {
            $ids = [];
            foreach (app('queue.failer')->all() as $record) {
                $payload = (string) (is_array($record) ? ($record['payload'] ?? '') : ($record->payload ?? ''));
                $command = (string) (json_decode($payload, true)['data']['command'] ?? '');
                if (str_contains($command, $runId) && preg_match('/s:5:"jobId";s:\d+:"(\d+)"/', $command, $match)) {
                    $ids[] = $match[1];
                }
            }
            sort($ids);

            return ['available' => true, 'job_ids' => $ids];
        } catch (Throwable $error) {
            return ['available' => false, 'error' => $error->getMessage()];
        }
    }

    /** @return array{available: bool, entries?: int, error?: string} */
    private function deadLetter(string $runId): array
    {
        if (config('benchmark.connection') !== 'queen') {
            return ['available' => false, 'error' => 'not a Queen connection'];
        }
        $queue = (string) ($this->option('queue') ?: config('benchmark.queue'));
        $url = rtrim((string) config('queen.url'), '/') . '/api/v1/dlq?' . http_build_query([
            'queue' => $queue,
            'consumerGroup' => (string) config('queen.consumer_group'),
            'limit' => 1000,
        ]);
        $body = @file_get_contents($url, false, stream_context_create(['http' => ['timeout' => 10, 'ignore_errors' => true]]));
        $decoded = is_string($body) ? json_decode($body, true) : null;
        if (!is_array($decoded) || !is_array($decoded['messages'] ?? null)) {
            return ['available' => false, 'error' => 'unreadable answer from ' . $url];
        }
        $entries = 0;
        foreach ($decoded['messages'] as $message) {
            if (str_contains(json_encode($message), $runId)) {
                ++$entries;
            }
        }

        return ['available' => true, 'entries' => $entries];
    }
}
