<?php

namespace Queen\Tests;

use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Log\LogManager;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\NullQueue;
use Orchestra\Testbench\TestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Psr\Log\NullLogger;
use Queen\Laravel\Commands\ForkServerCommand;
use Queen\Laravel\QueenServiceProvider;
use Queen\Tests\Support\RecordingExceptionHandler;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * What queen:fork-server runs in a forked child: Laravel's own queue:work,
 * with the arguments the master sent, the connection first (see ForkServer).
 */
final class LaravelForkServerWorkerTest extends TestCase
{
    /** @var list<array{0: string, 1: string}> The connection and the queue of every pop. */
    private array $pops = [];

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        // The application's default connection is not the pool's, as in an
        // application that dispatches through one connection and works
        // through others.
        $app['config']->set('queue.default', 'application-default');
        $app['config']->set('queue.connections.application-default', ['driver' => 'spy', 'queue' => 'default']);
        $app['config']->set('queue.connections.queen-batch', ['driver' => 'spy', 'queue' => 'batch']);
    }

    private string|false $savedExitsDirectory = false;

    private ?string $exitsDirectory = null;

    protected function setUp(): void
    {
        parent::setUp();

        $this->savedExitsDirectory = getenv('QUEEN_SUPERVISOR_EXITS_DIR');
        $this->pops = [];
        $this->app['queue']->extend('spy', fn (): ConnectorInterface => new SpyQueueConnector(
            function (string $connection, string $queue): void {
                $this->pops[] = [$connection, $queue];
            },
        ));
    }

    protected function tearDown(): void
    {
        putenv($this->savedExitsDirectory === false ? 'QUEEN_SUPERVISOR_EXITS_DIR' : "QUEEN_SUPERVISOR_EXITS_DIR={$this->savedExitsDirectory}");
        if ($this->exitsDirectory !== null) {
            foreach (glob($this->exitsDirectory . '/*') ?: [] as $path) {
                @unlink($path);
            }
            @rmdir($this->exitsDirectory);
        }

        parent::tearDown();
    }

    /**
     * The master sends queue:work's arguments with the connection first and
     * no command name. Bound as they came, Symfony took the connection for
     * the command name, and the worker fell back to queue.default: a forked
     * pool never read its own queue, with its own settings (php-client 2.3.1
     * to 2.4.0).
     */
    #[TestWith([['--queue=cb-backend.production.compliance'], 'cb-backend.production.compliance'])]
    #[TestWith([[], 'batch'])]
    public function testAForkedWorkerPopsFromTheConnectionItWasGiven(array $queue, string $expected): void
    {
        // What a supervisor sends for a pool, plus --once so that the worker
        // returns after one pop.
        $argv = [
            'queen-batch', ...$queue, '--sleep=0', '--timeout=300', '--tries=1', '--memory=448',
            '--backoff=0', '--max-jobs=0', '--max-time=0', '--rest=0', '--quiet', '--once',
        ];

        $code = $this->runWorker($argv);

        $this->assertSame(0, $code);
        $this->assertSame([['queen-batch', $expected]], $this->pops);
    }

    /**
     * queen:fork-server is started by a supervisor with prefork on, with fd 3
     * open and QUEEN_FORK_SERVER set; run by hand it refuses and says so.
     */
    public function testTheServerRefusesToRunByHand(): void
    {
        $this->assertNotSame(\Queen\Laravel\Supervisor\Prefork\ForkServer::PROTOCOL, getenv('QUEEN_FORK_SERVER'));

        $this->artisan('queen:fork-server')
            ->expectsOutputToContain('started by a Queen supervisor')
            ->assertFailed();
    }

    /**
     * Before each fork the server lets go of what the boot opened: a log
     * channel and a database connection are built again by whichever worker
     * first needs them, so no two workers share a socket.
     */
    public function testTheServerForgetsTheChannelsAndConnectionsTheBootOpened(): void
    {
        $log = $this->app['log'];
        $this->assertInstanceOf(LogManager::class, $log);
        $log->channel('null')->info('opened by the boot');
        $this->app['db']->connection()->select('select 1');
        $this->assertNotSame([], $log->getChannels());
        $this->assertNotSame([], $this->app['db']->getConnections());
        $server = $this->server();

        (new \ReflectionMethod($server, 'releaseBootResources'))->invoke($server);

        $this->assertSame([], $log->getChannels());
        $this->assertSame([], $this->app['db']->getConnections());
    }

    /**
     * An application may bind a logger of its own as `log`; the server
     * forgets channels only from a LogManager, which has them.
     */
    public function testTheServerLeavesALoggerThatIsNotALogManagerAlone(): void
    {
        $this->app->instance('log', new NullLogger());
        $server = $this->server();

        (new \ReflectionMethod($server, 'releaseBootResources'))->invoke($server);

        $this->assertInstanceOf(NullLogger::class, $this->app['log']);
    }


    /**
     * `php artisan queue:restart` after this server booted: the code on disk
     * is newer than the code a child of this server carries. Laravel's worker
     * would read the restart signal only now, as its baseline, and never stop
     * for it. The child says why it leaves, and leaves: the master replaces
     * the server, and the replacement runs the code on disk.
     */
    public function testAWorkerForkedAfterAQueueRestartLeavesWithTheRestartMarker(): void
    {
        $this->app['cache.store']->forever('illuminate:queue:restart', 1000);
        $server = $this->server();
        (new \ReflectionMethod($server, 'rememberRestartSignal'))->invoke($server);
        $work = $this->recordingWork();
        putenv('QUEEN_SUPERVISOR_EXITS_DIR=' . $this->exitsDirectory());

        $before = (new \ReflectionMethod($server, 'runWorker'))->invoke($server, $work, ['queen-batch', '--queue=x', '--once']);
        $this->app['cache.store']->forever('illuminate:queue:restart', 2000);
        $after = (new \ReflectionMethod($server, 'runWorker'))->invoke($server, $work, ['queen-batch', '--queue=x', '--once']);

        $this->assertSame([0, 0], [$before, $after]);
        $this->assertSame(1, $work->runs, 'the worker ran before the restart and not after it');
        $this->assertSame('restart', file_get_contents($this->exitsDirectory() . '/' . getmypid()));
    }

    /**
     * The queue manager keeps every connection it resolved. One the boot
     * resolved carries the server's environment, not the pool's, and its
     * socket would be shared by every worker: the server lets it go.
     */
    public function testTheServerForgetsTheQueueConnectionsTheBootResolved(): void
    {
        $this->app['queue']->connection('queen-batch');
        $this->assertTrue($this->app['queue']->connected('queen-batch'));
        $server = $this->server();

        $forgotten = (new \ReflectionMethod($server, 'releaseBootResources'))->invoke($server);

        $this->assertSame(['queen-batch'], $forgotten);
        $this->assertFalse($this->app['queue']->connected('queen-batch'));
    }

    /**
     * A spawned worker's exception reaches the console kernel, which reports
     * it to the application's handler; a forked worker has no kernel above
     * it, and its exception went to stderr alone.
     */
    public function testAForkedWorkersExceptionIsReportedToTheApplication(): void
    {
        $handler = new RecordingExceptionHandler();
        $this->app->instance(ExceptionHandler::class, $handler);
        $server = $this->server();
        $work = $this->recordingWork(new \RuntimeException('the broker is unreachable'));

        $code = (new \ReflectionMethod($server, 'runWorker'))->invoke($server, $work, ['queen-batch', '--queue=x']);

        $this->assertSame(1, $code);
        $this->assertSame(['the broker is unreachable'], array_map(fn (\Throwable $e): string => $e->getMessage(), $handler->reported));
    }

    private function exitsDirectory(): string
    {
        if ($this->exitsDirectory === null) {
            $this->exitsDirectory = sys_get_temp_dir() . '/queen-exits-' . bin2hex(random_bytes(6));
            mkdir($this->exitsDirectory, 0700);
        }

        return $this->exitsDirectory;
    }

    /** A stand-in for queue:work that counts its runs, or throws. */
    private function recordingWork(?\Throwable $throws = null): \Symfony\Component\Console\Command\Command
    {
        $work = new class($throws) extends \Symfony\Component\Console\Command\Command {
            public int $runs = 0;

            public function __construct(private ?\Throwable $throws)
            {
                parent::__construct('queue:work');
                $this->ignoreValidationErrors();
            }

            protected function execute(
                \Symfony\Component\Console\Input\InputInterface $input,
                \Symfony\Component\Console\Output\OutputInterface $output,
            ): int {
                ++$this->runs;
                if ($this->throws !== null) {
                    throw $this->throws;
                }

                return 0;
            }
        };
        $work->setApplication(new \Symfony\Component\Console\Application());

        return $work;
    }

    /** @param list<string> $argv */
    private function runWorker(array $argv): int
    {
        $work = $this->app->make(Kernel::class)->all()['queue:work'];
        $server = $this->server();

        return (new \ReflectionMethod($server, 'runWorker'))->invoke($server, $work, $argv);
    }

    private function server(): ForkServerCommand
    {
        $server = new ForkServerCommand();
        $server->setLaravel($this->app);
        $server->setOutput(new OutputStyle(new ArrayInput([]), new BufferedOutput()));

        return $server;
    }
}

/** A queue connection that records every pop and never has a job. */
final class SpyQueueConnector implements ConnectorInterface
{
    /** @param \Closure(string, string): void $onPop */
    public function __construct(private readonly \Closure $onPop)
    {
    }

    public function connect(array $config): SpyQueue
    {
        return new SpyQueue($this->onPop);
    }
}

final class SpyQueue extends NullQueue
{
    /** @param \Closure(string, string): void $onPop */
    public function __construct(private readonly \Closure $onPop)
    {
    }

    public function pop($queue = null)
    {
        ($this->onPop)((string) $this->getConnectionName(), (string) $queue);

        return null;
    }
}
