<?php

namespace App\Providers;

use App\Examples\Journal;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\MaxAttemptsExceededException;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\ServiceProvider;

final class AppServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        // What happened to a job after it ran, which the examples check besides
        // what the jobs record themselves. Laravel fires JobProcessed once the
        // job is deleted, which on Queen is an accepted ACK; JobFailed when the
        // job fails for good; JobExceptionOccurred for any other exception,
        // such as an ACK the broker refused.
        Queue::after(static fn (JobProcessed $event) => Journal::record($event->job, ['event' => 'acked']));
        Queue::failing(static fn (JobFailed $event) => Journal::record($event->job, [
            'event' => 'failed',
            'error' => $event->exception->getMessage(),
        ]));
        // A worker that loops has booted and is about to pop: example:prefork
        // waits for this before it measures the workers' memory.
        Queue::looping(static function (Looping $event): void {
            static $ready = false;
            if (!$ready) {
                $ready = touch(Journal::readyPath($event->queue, getmypid()));
            }
        });
        Queue::exceptionOccurred(static function (JobExceptionOccurred $event): void {
            // A delivery past --tries fails the job unrun: JobFailed records it.
            if (!$event->exception instanceof MaxAttemptsExceededException) {
                Journal::record($event->job, ['event' => 'exception', 'error' => $event->exception->getMessage()]);
            }
        });
    }
}
