<?php

namespace Queen\Tests;

use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use Orchestra\Testbench\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Queen\Laravel\Commands\ForkServerCommand;
use Queen\Laravel\QueenServiceProvider;
use Queen\Tests\Support\WorkerInvocationFixture;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * A pool's worker works the same connection, queues and options whether the
 * supervisor spawned it (`php artisan queue:work ...`, through the console
 * kernel as `artisan` does) or forked it from the fork server: the arguments
 * of tests/Fixtures/Supervisor/worker-invocation.json, run through Laravel's
 * own queue:work, with the Worker replaced by one that records what it was
 * asked to do.
 */
final class LaravelWorkerInvocationTest extends TestCase
{
    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        // The application's default connection is none of the pools', so a
        // worker that lost its connection argument fails here instead of
        // working the default by luck.
        $app['config']->set('queue.default', 'application-default');
        $app['config']->set('queue.connections.application-default', ['driver' => 'sync']);
        foreach (WorkerInvocationFixture::cases() as $case) {
            $app['config']->set('queue.connections.' . $case['pool']['connection'], ['driver' => 'sync']);
        }
    }

    /** @return iterable<string, array{0: array<string, mixed>}> */
    public static function pools(): iterable
    {
        foreach (WorkerInvocationFixture::cases() as $case) {
            yield $case['name'] => [$case];
        }
    }

    #[DataProvider('pools')]
    public function testASpawnedAndAForkedWorkerOfAPoolRunTheSameWork(array $case): void
    {
        $worker = new RecordingWorker(
            $this->app['queue'],
            $this->app['events'],
            $this->app->make(ExceptionHandler::class),
            fn (): bool => false,
        );
        $this->app->instance('queue.worker', $worker);
        $kernel = $this->app->make(Kernel::class);
        $arguments = $case['arguments'];

        $spawned = $kernel->handle(new ArgvInput(['artisan', 'queue:work', ...$arguments]), $output = new BufferedOutput());
        $forked = (new \ReflectionMethod(ForkServerCommand::class, 'runWorker'))
            ->invoke($this->forkServer(), $kernel->all()['queue:work'], $arguments);

        $this->assertSame([0, 0], [$spawned, $forked], $output->fetch());
        $this->assertCount(2, $worker->runs, 'one run each');
        [$bySpawn, $byFork] = $worker->runs;
        $this->assertEquals($bySpawn, $byFork, 'the forked worker runs something else than the spawned one');

        $pool = $case['pool'];
        [$mode, $connection, $queue, $options] = $byFork;
        $this->assertSame('daemon', $mode);
        $this->assertSame($pool['connection'], $connection);
        $this->assertSame(WorkerInvocationFixture::workerQueue($arguments), $queue);
        $this->assertSame([
            'sleep' => $pool['sleep'],
            'timeout' => $pool['timeout'],
            'tries' => $pool['tries'],
            'memory' => $pool['memory'],
            'backoff' => $pool['backoff'],
            'max_jobs' => $pool['max_jobs'],
            'max_time' => $pool['max_time'],
            'rest' => $pool['rest'],
            'force' => $pool['force'],
            'name' => 'default',
            'stop_when_empty' => false,
        ], [
            'sleep' => (int) $options->sleep,
            'timeout' => (int) $options->timeout,
            'tries' => (int) $options->maxTries,
            'memory' => (int) $options->memory,
            'backoff' => (int) $options->backoff,
            'max_jobs' => (int) $options->maxJobs,
            'max_time' => (int) $options->maxTime,
            'rest' => (int) $options->rest,
            'force' => (bool) $options->force,
            'name' => $options->name,
            'stop_when_empty' => (bool) $options->stopWhenEmpty,
        ]);
    }

    private function forkServer(): ForkServerCommand
    {
        $server = new ForkServerCommand();
        $server->setLaravel($this->app);
        $server->setOutput(new OutputStyle(new ArrayInput([]), new BufferedOutput()));

        return $server;
    }
}

/** Laravel's Worker, recording what queue:work asked of it instead of working. */
final class RecordingWorker extends Worker
{
    /** @var list<array{0: string, 1: string, 2: string, 3: WorkerOptions}> */
    public array $runs = [];

    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        $this->runs[] = ['daemon', (string) $connectionName, (string) $queue, $options];

        return 0;
    }

    public function runNextJob($connectionName, $queue, WorkerOptions $options)
    {
        $this->runs[] = ['once', (string) $connectionName, (string) $queue, $options];

        return 0;
    }
}
