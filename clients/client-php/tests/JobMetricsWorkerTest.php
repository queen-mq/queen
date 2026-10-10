<?php

namespace Queen\Tests;

use Illuminate\Console\OutputStyle;
use Illuminate\Container\Container;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Jobs\SyncJob;
use Illuminate\Queue\NullQueue;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\Commands\ForkServerCommand;
use Queen\Laravel\Dashboard\JobMetricsReader;
use Queen\Laravel\Monitoring\JobMetricsRecorder;
use Queen\Laravel\QueenServiceProvider;
use Queen\Tests\Support\MetricsBroker;
use Queen\Tests\Support\WorkerInvocationFixture;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * Per-class job metrics as a supervisor's workers record them: Laravel's own
 * queue:work, with the arguments the supervisor gives a quiet pool, spawned
 * (through the console kernel, as `artisan` runs it) or forked (as the fork
 * server runs it), against a broker that keeps what the workers write; then
 * read back as the dashboard reads it. A quiet worker prints no line with a
 * duration, so these numbers are the only runtimes it leaves.
 */
final class JobMetricsWorkerTest extends TestCase
{
    /** Every write falls in one five-minute bucket and one ten-second flush window; runtimes stay real. */
    private const NOW = 1_790_000_100.0;

    private MetricsBroker $broker;

    /** @var array<string, string|false> */
    private array $savedEnvironment = [];

    private ?string $scratch = null;

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $this->broker = new MetricsBroker();
        $app['config']->set('queue.default', 'queen');
        $app['config']->set('queue.connections.queen', [
            'driver' => 'queen',
            'url' => 'http://queen.test:6632',
            'handler' => $this->broker->handler(),
            'queue' => 'default',
            'consumer_group' => 'workers',
            'block_for' => 0,
            'retry_attempts' => 1,
            'retry_delay' => 0,
        ]);
        // A worker that is not on Queen, such as one Horizon still runs.
        $app['config']->set('queue.connections.redis', ['driver' => 'other', 'queue' => 'default']);
        $app['config']->set('queen.sync_failed_jobs', false);
        $app['config']->set('queue.failed.driver', 'null');
    }

    protected function setUp(): void
    {
        parent::setUp();

        MetricsJob::$ran = [];
        // The environment the supervisor gives a worker of this pool.
        foreach ($this->supervisedPool()['environment'] as $name => $value) {
            $this->savedEnvironment[$name] = getenv($name);
            putenv("{$name}={$value}");
        }
        $this->app['queue']->extend('other', fn (): ConnectorInterface => new OtherQueueConnector());
        $recorder = $this->app->make(JobMetricsRecorder::class);
        (new \ReflectionProperty($recorder, 'clock'))->setValue($recorder, fn (): float => self::NOW);
    }

    protected function tearDown(): void
    {
        foreach ($this->savedEnvironment as $name => $value) {
            putenv($value === false ? $name : "{$name}={$value}");
        }
        if ($this->scratch !== null) {
            foreach (glob($this->scratch . '/*') ?: [] as $path) {
                @unlink($path);
            }
            @rmdir($this->scratch);
        }

        parent::tearDown();
    }

    public function testASpawnedQuietWorkerRecordsTheRuntimeOfEachJobClass(): void
    {
        $this->broker->serve([
            MetricsBroker::delivery(new MetricsInvoiceJob(20), 'job-1'),
            MetricsBroker::delivery(new MetricsInvoiceJob(40), 'job-2'),
            MetricsBroker::delivery(new MetricsExportJob(30, 'throw'), 'job-3'),
        ]);

        [$code, $output] = $this->spawn();

        $this->assertSame(0, $code, $output);
        $this->assertSame('', $output, 'a quiet worker prints nothing');
        $this->assertSame(['MetricsInvoiceJob', 'MetricsInvoiceJob', 'MetricsExportJob'], MetricsJob::$ran);
        $classes = $this->classes($this->broker);
        $invoice = $classes[MetricsInvoiceJob::class];
        $this->assertSame([2, 0], [$invoice['processed'], $invoice['failed']]);
        $this->assertGreaterThanOrEqual(60, $invoice['runtime_ms']);
        $this->assertGreaterThanOrEqual(40, $invoice['max_ms']);
        $this->assertLessThan($invoice['runtime_ms'], $invoice['max_ms'], 'the longest run, not the sum');
        $this->assertGreaterThanOrEqual(30, $invoice['average_ms']);
        // Retried after it threw: a failed attempt, with its runtime.
        $export = $classes[MetricsExportJob::class];
        $this->assertSame([0, 1], [$export['processed'], $export['failed']]);
        $this->assertGreaterThanOrEqual(30, $export['max_ms']);
    }

    /**
     * A worker writes after its first job, then at most once every ten
     * seconds: what its last jobs left is written when it stops.
     */
    public function testAWorkerThatStopsWritesWhatItHasNotWrittenYet(): void
    {
        $this->broker->serve([
            MetricsBroker::delivery(new MetricsInvoiceJob(), 'job-1'),
            MetricsBroker::delivery(new MetricsInvoiceJob(), 'job-2'),
            MetricsBroker::delivery(new MetricsInvoiceJob(), 'job-3'),
        ]);

        $this->assertSame(0, $this->spawn()[0]);

        $this->assertSame(
            ['pop', 'ack', 'kv', 'pop', 'ack', 'pop', 'ack', 'pop', 'kv'],
            array_map(static fn (array $request): string => explode('/', $request['path'])[3], $this->broker->requests),
            'one write after the first job, one when the worker stops',
        );
        [$first, $last] = $this->broker->puts();
        $this->assertSame($first['key'], $last['key'], 'one key per worker and bucket, overwritten');
        $this->assertSame(1, $first['value']['classes'][MetricsInvoiceJob::class]['processed']);
        $this->assertSame(3, $last['value']['classes'][MetricsInvoiceJob::class]['processed']);
        $this->assertSame(JobMetricsRecorder::TTL_SECONDS, $last['ttlSeconds']);
    }

    /**
     * The fork server boots the application once, with the job listeners,
     * and forks. Whatever its recorder holds then (it runs no job itself,
     * but an application's boot could make it record one) stays its own: a
     * forked worker counts its own jobs under a key of its own, writes them
     * when it stops, and writes through a client it built itself, never the
     * server's. The sum is each job once.
     */
    public function testAForkedWorkerRecordsItsJobsOnceUnderItsOwnKey(): void
    {
        $this->requiresFork();
        $log = $this->scratch('broker.log');
        $this->broker->logTo($log);
        $recorder = $this->app->make(JobMetricsRecorder::class);
        // Written once, then a run the server has not written yet.
        foreach (['boot-1', 'boot-2'] as $id) {
            $job = $this->createStub(Job::class);
            $job->method('resolveName')->willReturn('App\Jobs\RanAtBoot');
            $recorder->start($job);
            $recorder->finish($job, false);
        }
        $serverClient = $this->app['queue']->connection('queen')->getBestEffortQueen();
        [$server, $output] = $this->forkServer();
        (new \ReflectionMethod($server, 'releaseBootResources'))->invoke($server);

        $children = [];
        foreach ([
            [MetricsBroker::delivery(new MetricsInvoiceJob(20), 'a-1'), MetricsBroker::delivery(new MetricsInvoiceJob(20), 'a-2')],
            [MetricsBroker::delivery(new MetricsInvoiceJob(20), 'b-1'), MetricsBroker::delivery(new MetricsExportJob(20, 'throw'), 'b-2')],
            // Stops before its first job.
            [],
        ] as $index => $deliveries) {
            $report = $this->scratch("child-{$index}.json");
            $children[$this->fork(function () use ($deliveries, $server, $output, $report, $serverClient): void {
                $this->broker->serve($deliveries);
                (new \ReflectionMethod($server, 'prepareChild'))->invoke($server);
                $code = $this->runForked($server, $this->supervisedArguments());
                file_put_contents($report, json_encode([
                    'code' => $code,
                    'output' => $output->fetch(),
                    'own_client' => $this->app['queue']->connection('queen')->getBestEffortQueen() !== $serverClient,
                ]));
            })] = $report;
        }
        foreach (array_keys($children) as $pid) {
            pcntl_waitpid($pid, $status);
        }

        foreach ($children as $report) {
            $this->assertSame(['code' => 0, 'output' => '', 'own_client' => true], json_decode((string) @file_get_contents($report), true));
        }
        $keys = [];
        foreach (MetricsBroker::putsIn(MetricsBroker::logged($log)) as $write) {
            $keys[$write['pid']][basename($write['key'])] = true;
        }
        [$first, $second, $idle] = array_keys($children);
        $this->assertCount(1, $keys[$first] ?? []);
        $this->assertCount(1, $keys[$second] ?? []);
        $this->assertCount(1, $keys[getmypid()] ?? []);
        $this->assertCount(3, array_unique([...array_keys($keys[$first]), ...array_keys($keys[$second]), ...array_keys($keys[getmypid()])]), 'a key of its own');
        $this->assertArrayNotHasKey($idle, $keys, 'a worker that ran no job writes nothing, not even what it inherited');

        $byWorkers = $this->classes(MetricsBroker::fromLog($log, array_keys($children)));
        $this->assertEqualsCanonicalizing([MetricsInvoiceJob::class, MetricsExportJob::class], array_keys($byWorkers));
        $this->assertSame([3, 0], [$byWorkers[MetricsInvoiceJob::class]['processed'], $byWorkers[MetricsInvoiceJob::class]['failed']]);
        $this->assertGreaterThanOrEqual(20, $byWorkers[MetricsInvoiceJob::class]['max_ms']);
        $this->assertSame([0, 1], [$byWorkers[MetricsExportJob::class]['processed'], $byWorkers[MetricsExportJob::class]['failed']]);

        // The server writes its own runs once, under its own key.
        $recorder->flush();
        $all = $this->classes(MetricsBroker::fromLog($log));
        $this->assertSame(2, $all['App\Jobs\RanAtBoot']['processed']);
        $this->assertSame(3, $all[MetricsInvoiceJob::class]['processed']);
    }

    /**
     * Laravel's timeout handler raises JobTimedOut, then WorkerStopping,
     * then kills the worker: no JobProcessed and no JobExceptionOccurred.
     * The attempt is a failed one, its runtime about the timeout, and it is
     * written before the worker dies. Before 2.4.2 it was not recorded at all.
     */
    public function testAJobThatOutlivesItsTimeoutIsRecordedAsAFailedAttempt(): void
    {
        $this->requiresFork();
        $log = $this->scratch('broker.log');
        $this->broker->logTo($log);
        $returned = $this->scratch('returned');

        // A process of its own, since Laravel kills it.
        $pid = $this->fork(function () use ($returned): void {
            $this->broker->serve([
                MetricsBroker::delivery(new MetricsInvoiceJob(20), 'job-1'),
                MetricsBroker::delivery(new MetricsExportJob(5000), 'job-2'),
            ]);
            $this->spawn(['--timeout=1']);
            touch($returned);
        });
        pcntl_waitpid($pid, $status);

        $this->assertTrue(pcntl_wifsignaled($status) && pcntl_wtermsig($status) === SIGKILL);
        $this->assertFileDoesNotExist($returned, 'killed inside the job');
        $requests = MetricsBroker::logged($log);
        $this->assertSame('/api/v1/kv', end($requests)['path'], 'written before the worker died');
        $acknowledged = array_map(static fn (array $request): ?string => $request['body']['transactionId'] ?? null, $requests);
        $this->assertNotContains('transaction-job-2', $acknowledged, 'never acknowledged');
        $classes = $this->classes(MetricsBroker::fromLog($log));
        $this->assertSame([1, 0], [$classes[MetricsInvoiceJob::class]['processed'], $classes[MetricsInvoiceJob::class]['failed']]);
        $export = $classes[MetricsExportJob::class];
        $this->assertSame([0, 1], [$export['processed'], $export['failed']]);
        $this->assertGreaterThanOrEqual(1000, $export['max_ms']);
        $this->assertLessThan(5000, $export['max_ms']);
    }

    /**
     * `$this->fail()` returns normally, and Laravel raises JobProcessed for
     * it; a delivery past its tries is failed before it runs. Both are failed
     * attempts. Before 2.4.2 the first counted as processed.
     */
    public function testAJobThatFailsWithoutThrowingIsAFailedAttempt(): void
    {
        $this->broker->serve([
            MetricsBroker::delivery(new MetricsExportJob(10, 'fail'), 'job-1'),
            // The fourth delivery, for a pool with --tries=3.
            MetricsBroker::delivery(new MetricsExportJob(10), 'job-2', 4),
            MetricsBroker::delivery(new MetricsInvoiceJob(10), 'job-3'),
        ]);

        $this->assertSame(0, $this->spawn()[0]);

        $classes = $this->classes($this->broker);
        $this->assertSame(['MetricsExportJob', 'MetricsInvoiceJob'], MetricsJob::$ran, 'the fourth delivery never ran');
        $this->assertSame([0, 2], [$classes[MetricsExportJob::class]['processed'], $classes[MetricsExportJob::class]['failed']]);
        $this->assertSame([1, 0], [$classes[MetricsInvoiceJob::class]['processed'], $classes[MetricsInvoiceJob::class]['failed']]);
    }

    /**
     * Laravel raises JobProcessed for a job that released itself, or that
     * middleware such as WithoutOverlapping released: the attempt counts as
     * processed, with its runtime. Horizon leaves released jobs out of its
     * metrics, so a class released often shows more runs, and shorter ones,
     * here than there.
     */
    public function testAReleasedAttemptIsCountedAsProcessed(): void
    {
        $this->broker->serve([MetricsBroker::delivery(new MetricsExportJob(30, 'release'), 'job-1')]);

        $this->assertSame(0, $this->spawn()[0]);

        $this->assertContains('/api/v1/transaction', array_column($this->broker->requests, 'path'), 'released');
        $export = $this->classes($this->broker)[MetricsExportJob::class];
        $this->assertSame([1, 0], [$export['processed'], $export['failed']]);
        $this->assertGreaterThanOrEqual(30, $export['runtime_ms']);
    }

    /**
     * The listeners are in every worker of the application, Horizon's too:
     * only the jobs of a Queen connection are recorded.
     */
    public function testTheJobsOfAnotherConnectionAreNotRecorded(): void
    {
        [$code] = $this->spawn([], 'redis');

        $this->assertSame(0, $code);
        $this->assertSame(['MetricsInvoiceJob'], MetricsJob::$ran);
        $this->assertSame([], $this->broker->requests, 'nothing written, nothing read');
    }

    /** @return array{name: string, pool: array, arguments: list<string>, environment: array<string, string|true>} */
    private function supervisedPool(): array
    {
        foreach (WorkerInvocationFixture::cases() as $case) {
            if ($case['pool']['connection'] === 'queen') {
                return $case;
            }
        }

        throw new \LogicException('The fixture has no pool on the queen connection.');
    }

    /**
     * What the supervisor gives a quiet pool's worker, then: no sleep and a
     * stop once the queue is empty, so that the worker returns, and room for
     * this test process's memory.
     *
     * @param list<string> $extra
     * @return list<string>
     */
    private function supervisedArguments(array $extra = [], ?string $connection = null): array
    {
        $arguments = $this->supervisedPool()['arguments'];
        if ($connection !== null) {
            $arguments[0] = $connection;
        }

        return [...$arguments, '--sleep=0', '--memory=4096', '--stop-when-empty', ...$extra];
    }

    /** @return array{int, string} queue:work's exit code and output, run as `artisan` runs it */
    private function spawn(array $extra = [], ?string $connection = null): array
    {
        $code = $this->app->make(Kernel::class)->handle(
            new ArgvInput(['artisan', 'queue:work', ...$this->supervisedArguments($extra, $connection)]),
            $output = new BufferedOutput(),
        );

        return [$code, $output->fetch()];
    }

    /** @return array{ForkServerCommand, BufferedOutput} */
    private function forkServer(): array
    {
        $server = new ForkServerCommand();
        $server->setLaravel($this->app);
        $server->setOutput(new OutputStyle(new ArrayInput([]), $output = new BufferedOutput()));

        return [$server, $output];
    }

    /** @param list<string> $argv */
    private function runForked(ForkServerCommand $server, array $argv): int
    {
        return (new \ReflectionMethod($server, 'runWorker'))
            ->invoke($server, $this->app->make(Kernel::class)->all()['queue:work'], $argv);
    }

    /** Runs $child in a forked process, which leaves by SIGKILL: no destructor or test of the parent's may run there. */
    private function fork(\Closure $child): int
    {
        $pid = pcntl_fork();
        if ($pid === -1) {
            $this->fail('Unable to fork.');
        }
        if ($pid === 0) {
            try {
                $child();
            } catch (\Throwable $error) {
                fwrite(STDERR, $error . "\n");
            } finally {
                posix_kill(getmypid(), SIGKILL);
            }
        }

        return $pid;
    }

    private function requiresFork(): void
    {
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            $this->markTestSkipped('Needs ext-pcntl and ext-posix.');
        }
    }

    /** @return array<string, array<string, mixed>> the classes the dashboard reads from $broker, by name */
    private function classes(MetricsBroker $broker): array
    {
        $metrics = (new JobMetricsReader($broker->queen(), 'queen-metrics', null, fn (): int => (int) self::NOW + 60))->read('1h');
        $this->assertTrue($metrics['available']);

        return array_column($metrics['classes'], null, 'class');
    }

    private function scratch(string $name): string
    {
        if ($this->scratch === null) {
            $this->scratch = sys_get_temp_dir() . '/queen-job-metrics-' . bin2hex(random_bytes(6));
            mkdir($this->scratch, 0700);
        }

        return $this->scratch . '/' . $name;
    }
}

abstract class MetricsJob implements ShouldQueue
{
    use InteractsWithQueue;

    /** @var list<string> The short class name of every run, in order. */
    public static array $ran = [];

    public function __construct(public int $sleepMilliseconds = 0, public string $then = 'complete')
    {
    }

    public function handle(): void
    {
        self::$ran[] = (new \ReflectionClass($this))->getShortName();
        usleep($this->sleepMilliseconds * 1000);
        match ($this->then) {
            'throw' => throw new \RuntimeException('the export failed'),
            'release' => $this->release(30),
            'fail' => $this->fail(new \RuntimeException('the export gave up')),
            default => null,
        };
    }
}

final class MetricsInvoiceJob extends MetricsJob
{
}

final class MetricsExportJob extends MetricsJob
{
}

/** A queue connection that is not Queen's: one invoice job, then none. */
final class OtherQueueConnector implements ConnectorInterface
{
    public function connect(array $config): OtherQueue
    {
        return new OtherQueue();
    }
}

final class OtherQueue extends NullQueue
{
    private bool $popped = false;

    public function pop($queue = null)
    {
        if ($this->popped) {
            return null;
        }
        $this->popped = true;
        $payload = MetricsBroker::delivery(new MetricsInvoiceJob(), 'other-1')['data'];

        return new SyncJob($this->container ?? Container::getInstance(), json_encode($payload), $this->getConnectionName(), (string) $queue);
    }
}
