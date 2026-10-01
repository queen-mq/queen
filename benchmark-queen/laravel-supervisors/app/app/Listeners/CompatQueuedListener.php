<?php

namespace App\Listeners;

use App\Events\CompatEvent;
use App\Support\FailureMatrixLog;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use RuntimeException;
use Throwable;

/**
 * A queued listener on the lane's connection and queue. Laravel's event
 * discovery registers it (app/Listeners): no Event::listen() call. It is
 * tried twice, then failed() runs.
 */
final class CompatQueuedListener implements ShouldQueue
{
    use InteractsWithQueue;

    public int $tries = 2;

    public function viaConnection(): string
    {
        return (string) config('benchmark.connection');
    }

    public function viaQueue(): string
    {
        return (string) config('benchmark.queue');
    }

    public function handle(CompatEvent $event): void
    {
        $log = app(FailureMatrixLog::class);
        $attempt = $this->attempts();
        $log->record($event->runId, $event->jobId, $attempt, 'started', $event->mode);
        if ($event->mode === 'throw') {
            $log->record($event->runId, $event->jobId, $attempt, 'threw', $event->mode);
            throw new RuntimeException("compat: listener {$event->jobId} attempt {$attempt}");
        }
        $log->record($event->runId, $event->jobId, $attempt, 'completed', $event->mode);
    }

    public function failed(CompatEvent $event, Throwable $exception): void
    {
        app(FailureMatrixLog::class)->record($event->runId, $event->jobId, null, 'failed_hook', $event->mode, $exception::class);
    }
}
