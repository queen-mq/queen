<?php

namespace Queen\Tests;

use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\NullQueue;
use Orchestra\Testbench\TestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Psr\Log\NullLogger;
use Queen\Laravel\Commands\ForkServerCommand;
use Queen\Laravel\QueenServiceProvider;
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

    protected function setUp(): void
    {
        parent::setUp();

        $this->pops = [];
        $this->app['queue']->extend('spy', fn (): ConnectorInterface => new SpyQueueConnector(
            function (string $connection, string $queue): void {
                $this->pops[] = [$connection, $queue];
            },
        ));
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
