<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Bus\Queueable;
use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Queue;
use Orchestra\Testbench\TestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Queen\Laravel\Commands\ForkServerCommand;
use Queen\Laravel\QueenServiceProvider;
use Queen\Tests\Support\MemoryBroker;
use Queen\Tests\Support\RoutedQueue;
use Queen\Tests\Support\ScriptedWorker;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * An application whose default connection only dispatches, a `routed`
 * connection: every dispatch goes through it to the Queen connection of the
 * pool the queue belongs to, and a pool's worker, spawned or forked, works
 * that connection with the pool's settings. One broker in memory carries
 * the job from the dispatch to the worker.
 */
final class LaravelRoutedDispatchTest extends TestCase
{
    private const ENVIRONMENT = [
        'QUEEN_LARAVEL_CONNECTION',
        'QUEEN_LARAVEL_SUPERVISOR',
        'QUEEN_LARAVEL_CONSUMER_GROUP',
        'QUEEN_LARAVEL_RETRY_AFTER',
    ];

    /** The pools: retry_after and partitions of their connections. */
    private const POOLS = [
        'interactive' => [180, 64],
        'batch' => [360, 64],
        'ordered' => [120, 1],
    ];

    /** @var array<string, string|false> */
    private array $savedEnvironment = [];

    private MemoryBroker $broker;

    protected function setUp(): void
    {
        foreach (self::ENVIRONMENT as $name) {
            $this->savedEnvironment[$name] = getenv($name);
            putenv($name);
        }
        $this->broker = new MemoryBroker();
        RoutedProbeJob::$runs = [];
        RoutedQueue::$pops = 0;

        parent::setUp();
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        foreach ($this->savedEnvironment as $name => $value) {
            putenv($value === false ? $name : "{$name}={$value}");
        }
    }

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queue.default', 'routed');
        $app['config']->set('queue.connections.routed', ['driver' => 'routed']);
        foreach (self::POOLS as $pool => [$retryAfter, $partitions]) {
            // A pool's queen-<pool>: no lease of its own, after_commit.
            $app['config']->set("queue.connections.queen-{$pool}", [
                'driver' => 'queen',
                'queue' => 'app.testing.default',
                'retry_after' => $retryAfter,
                'partitions' => $partitions,
                'after_commit' => true,
                'url' => 'http://queen.test:6632',
                'retry_attempts' => 1,
                'handler' => HandlerStack::create($this->broker),
            ]);
        }
        $app['config']->set('queen.job_metrics.enabled', false);
        $app->resolving('queue', static function (QueueManager $manager): void {
            $manager->extend('routed', static fn (): ConnectorInterface => new class ($manager) implements ConnectorInterface {
                public function __construct(private QueueManager $manager)
                {
                }

                public function connect(array $config): QueueContract
                {
                    return new RoutedQueue($this->manager);
                }
            });
        });
    }

    /**
     * Dispatched through the router, a job runs on its pool's connection,
     * from the pool's queue, with the pool's lease, consumer group and
     * partitions, and the router is never asked to pop.
     */
    #[TestWith(['spawned'])]
    #[TestWith(['forked'])]
    public function testAJobDispatchedThroughTheRouterRunsOnItsPoolsConnection(string $mode): void
    {
        RoutedProbeJob::dispatch('compliance-1')->onQueue('compliance');

        $pushed = $this->broker->pushed();
        $this->assertCount(1, $pushed);
        $this->assertSame('app.testing.compliance', $pushed[0]['queue']);
        $this->assertMatchesRegularExpression('/^laravel-00([0-5]\d|6[0-3])$/', $pushed[0]['partition'], 'not one of 64 stripes');
        $this->assertSame(RoutedProbeJob::class, $pushed[0]['payload']['displayName']);

        [$code, $output] = $this->workAsPool($mode, 'batch', 'app.testing.compliance', 2);

        $this->assertSame(0, $code, $output);
        $this->assertSame([[
            'name' => 'compliance-1',
            'connection' => 'queen-batch',
            'queue' => 'app.testing.compliance',
            'attempts' => 1,
        ]], RoutedProbeJob::$runs);
        $pops = $this->broker->pops();
        $this->assertCount(2, $pops, 'one pop for the job, one empty');
        $this->assertSame('app.testing.compliance', $pops[0]['queue']);
        $this->assertSame('app-production', $pops[0]['consumerGroup']);
        $this->assertSame('360', $pops[0]['leaseSeconds']);
        $this->assertSame('64', $pops[0]['partitions']);
        $this->assertNotSame([], $this->broker->requestsTo('/api/v1/ack/batch') ?: $this->broker->requestsTo('/api/v1/ack'));
        $this->assertSame(0, RoutedQueue::$pops);
    }

    /** later() and bulk() take the same route as push(), to the right pool. */
    public function testLaterAndBulkGoThroughTheRouterToTheirPools(): void
    {
        Queue::later(30, new RoutedProbeJob('ical-later'), '', 'ical');
        RoutedProbeJob::dispatch('notification-later')->onQueue('notifications')->delay(5);
        Queue::bulk([new RoutedProbeJob('background-1'), new RoutedProbeJob('background-2')], '', 'background');
        RoutedProbeJob::dispatch('ordered-1')->onQueue('reservation-sync');

        $timers = array_merge(...array_map(
            static fn ($request): array => json_decode((string) $request->getBody(), true)['operations'],
            $this->broker->requestsTo('/api/v1/timers'),
        ));
        $this->assertSame(
            [['app.testing.ical', 30000], ['app.testing.notifications', 5000]],
            array_map(static fn (array $timer): array => [$timer['queue'], $timer['delayMs']], $timers),
        );
        $pushed = $this->broker->pushed();
        $this->assertSame(
            ['app.testing.background', 'app.testing.background', 'app.testing.reservation-sync'],
            array_column($pushed, 'queue'),
        );
        $this->assertSame(
            ['background-1', 'background-2', 'ordered-1'],
            array_map(static fn (array $item): string => unserialize($item['payload']['data']['command'])->name, $pushed),
        );
        // The ordered pool's connection has one partition.
        $this->assertSame('laravel-0000', $pushed[2]['partition']);
    }

    /**
     * The router leaves after_commit to the pool's connection: inside a
     * transaction nothing reaches the broker before the commit, and nothing
     * at all after a rollback, whether pushed, delayed or in bulk.
     */
    public function testAfterCommitIsHonouredThroughTheRouter(): void
    {
        DB::transaction(function (): void {
            RoutedProbeJob::dispatch('committed')->onQueue('compliance');
            Queue::later(10, new RoutedProbeJob('committed-later'), '', 'ical');
            Queue::bulk([new RoutedProbeJob('committed-bulk')], '', 'background');

            $this->assertSame([], $this->broker->requests, 'the broker heard of a job before the commit');
        });

        $this->assertSame(
            ['committed', 'committed-bulk'],
            array_map(static fn (array $item): string => unserialize($item['payload']['data']['command'])->name, $this->broker->pushed()),
        );
        $this->assertCount(1, $this->broker->requestsTo('/api/v1/timers'));

        $this->broker->requests = [];
        DB::beginTransaction();
        RoutedProbeJob::dispatch('rolled-back')->onQueue('compliance');
        Queue::later(10, new RoutedProbeJob('rolled-back-later'), '', 'ical');
        Queue::bulk([new RoutedProbeJob('rolled-back-bulk')], '', 'background');
        DB::rollBack();

        $this->assertSame([], $this->broker->requests, 'a rolled-back job reached the broker');
    }

    /**
     * Run this test process as one worker of a pool, with what the
     * supervisor puts in its environment, for $loops loops: spawned, the
     * application boots with it; forked, it boots without it, and the child
     * reads it after the fork.
     *
     * @return array{0: int, 1: string} the exit status and the output
     */
    private function workAsPool(string $mode, string $pool, string $queue, int $loops): array
    {
        $environment = [
            'QUEEN_LARAVEL_CONNECTION' => "queen-{$pool}",
            'QUEEN_LARAVEL_SUPERVISOR' => $pool,
            'QUEEN_LARAVEL_CONSUMER_GROUP' => 'app-production',
            'QUEEN_LARAVEL_RETRY_AFTER' => (string) self::POOLS[$pool][0],
        ];
        if ($mode === 'spawned') {
            foreach ($environment as $name => $value) {
                putenv("{$name}={$value}");
            }
            $this->refreshApplication();
        }
        $worker = new ScriptedWorker(
            $this->app['queue'],
            $this->app['events'],
            $this->app->make(ExceptionHandler::class),
            fn (): bool => false,
        );
        $worker->maxLoops = $loops;
        $this->app->instance('queue.worker', $worker);
        $server = new ForkServerCommand();
        $server->setLaravel($this->app);
        $server->setOutput(new OutputStyle(new ArrayInput([]), $output = new BufferedOutput()));
        if ($mode === 'forked') {
            foreach ($environment as $name => $value) {
                putenv("{$name}={$value}");
            }
            (new \ReflectionMethod(ForkServerCommand::class, 'prepareChild'))->invoke($server);
        }
        // What the supervisor sends for one queue of the batch pool.
        $arguments = [
            "queen-{$pool}", "--queue={$queue}", '--sleep=1', '--timeout=300', '--tries=1', '--memory=448',
            '--backoff=0', '--max-jobs=0', '--max-time=0', '--rest=0', '--quiet',
        ];
        $kernel = $this->app->make(Kernel::class);
        if ($mode === 'spawned') {
            return [$kernel->handle(new ArgvInput(['artisan', 'queue:work', ...$arguments]), $output), $output->fetch()];
        }
        $code = (new \ReflectionMethod(ForkServerCommand::class, 'runWorker'))
            ->invoke($server, $kernel->all()['queue:work'], $arguments);

        return [$code, $output->fetch()];
    }
}

final class RoutedProbeJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    /** @var list<array{name: string, connection: ?string, queue: ?string, attempts: int}> */
    public static array $runs = [];

    public function __construct(public string $name)
    {
    }

    public function handle(): void
    {
        self::$runs[] = [
            'name' => $this->name,
            'connection' => $this->job?->getConnectionName(),
            'queue' => $this->job?->getQueue(),
            'attempts' => $this->attempts(),
        ];
    }
}
