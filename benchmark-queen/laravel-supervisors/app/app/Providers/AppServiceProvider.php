<?php

namespace App\Providers;

use App\Support\BenchmarkEffectLedger;
use App\Support\FailureMatrixLog;
use App\Support\JsonlResultSink;
use App\Support\RetryingBatchRepository;
use Illuminate\Bus\BatchFactory;
use Illuminate\Bus\DatabaseBatchRepository;
use Illuminate\Cache\RateLimiting\Limit;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\RateLimiter;
use Illuminate\Support\ServiceProvider;

final class AppServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->singleton(BenchmarkEffectLedger::class, function (): BenchmarkEffectLedger {
            return new BenchmarkEffectLedger(
                (string) config('benchmark.results_directory'),
                (string) config('benchmark.ledger_mode'),
            );
        });

        $this->app->singleton(JsonlResultSink::class, function (): JsonlResultSink {
            return new JsonlResultSink((string) config('benchmark.results_directory'));
        });

        // Batches in SQLite: see RetryingBatchRepository. An extender, because
        // BusServiceProvider is deferred: it binds DatabaseBatchRepository
        // when first needed, over any plain binding made here.
        $this->app->extend(DatabaseBatchRepository::class, static function (DatabaseBatchRepository $repository, Application $app): RetryingBatchRepository {
            return new RetryingBatchRepository(
                $app->make(BatchFactory::class),
                $app->make('db')->connection($app['config']->get('queue.batching.database')),
                $app['config']->get('queue.batching.table', 'job_batches'),
            );
        });
    }

    public function boot(): void
    {
        // The Laravel compatibility lanes: one rate-limited job per second,
        // and Laravel's queue events recorded for the compatibility jobs.
        RateLimiter::for('compat', static fn (): Limit => Limit::perSecond(1));
        Queue::before(static fn (JobProcessing $event) => self::recordEvent('event_before', $event->job));
        Queue::after(static fn (JobProcessed $event) => self::recordEvent('event_after', $event->job));
        Queue::failing(static fn (JobFailed $event) => self::recordEvent('event_failing', $event->job, $event->exception));
    }

    private static function recordEvent(string $event, Job $job, ?\Throwable $exception = null): void
    {
        $payload = $job->payload();
        if (!str_starts_with((string) ($payload['displayName'] ?? ''), 'App\\Jobs\\Compat\\')) {
            return;
        }
        $command = (string) ($payload['data']['command'] ?? '');
        if (preg_match('/s:5:"runId";s:\d+:"([^"]+)"/', $command, $run) !== 1
            || preg_match('/s:5:"jobId";s:\d+:"([^"]+)"/', $command, $id) !== 1) {
            return;
        }
        app(FailureMatrixLog::class)->record($run[1], $id[1], $job->attempts(), $event, 'event',
            $exception === null ? null : $exception::class);
    }
}
