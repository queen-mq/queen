<?php

// A queue:work stand-in whose job outlives a one-second timeout, so Laravel's
// own timeout handler runs: it dispatches JobTimedOut, then SIGKILLs this
// process. The exit marker is registered as a Queen worker registers it, from
// QUEEN_SUPERVISOR_EXITS_DIR.

require __DIR__ . '/../../vendor/autoload.php';

use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Events\Dispatcher;
use Illuminate\Queue\Jobs\SyncJob;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use Queen\Laravel\Supervisor\WorkerExitMarker;

$container = new Container();
$events = new Dispatcher($container);
WorkerExitMarker::listenFromEnvironment($events);

$exceptions = new class implements ExceptionHandler {
    public function report(Throwable $e)
    {
    }

    public function shouldReport(Throwable $e)
    {
        return false;
    }

    public function render($request, Throwable $e)
    {
        throw $e;
    }

    public function renderForConsole($output, Throwable $e)
    {
    }
};

$worker = new class(new QueueManager($container), $events, $exceptions, fn (): bool => false) extends Worker {
    public function armTimeout($job, WorkerOptions $options): void
    {
        $this->registerTimeoutHandler($job, $options);
    }
};

pcntl_async_signals(true);
$payload = json_encode(['displayName' => 'SlowJob', 'job' => 'SlowJob@handle', 'data' => []]);
// maxTries 0: the handler must not try to fail a job this stand-in cannot resolve.
$worker->armTimeout(new SyncJob($container, $payload, 'queen', 'default'), new WorkerOptions(timeout: 1, maxTries: 0));
sleep(10);

fwrite(STDERR, "Laravel's timeout handler never fired.\n");
exit(3);
