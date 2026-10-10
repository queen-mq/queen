<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\Queue;
use Orchestra\Testbench\TestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Psr\Http\Message\RequestInterface;
use Queen\Laravel\Commands\ForkServerCommand;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Laravel\Supervisor\WorkerPopGuard;
use Queen\Tests\Support\PlanHandler;
use Queen\Tests\Support\ScriptedWorker;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * A worker under a supervisor that is alive but consumes nothing: Laravel's
 * own queue:work, spawned (through the console kernel, as `artisan` runs it)
 * and forked (as the fork server runs it), on a pool connection whose pops
 * fail; and what `queen:supervisor status` makes of it. See WorkerPopGuard.
 */
final class WorkerPopGuardTest extends TestCase
{
    private const ENVIRONMENT = [
        'QUEEN_LARAVEL_CONNECTION',
        'QUEEN_LARAVEL_SUPERVISOR',
        'QUEEN_SUPERVISOR_EXITS_DIR',
    ];

    /** @var array<string, string|false> */
    private array $savedEnvironment = [];

    private string $stateDirectory;

    private string $exitsDirectory;

    private BrokerScript $broker;

    private int $routedPops = 0;

    protected function setUp(): void
    {
        foreach (self::ENVIRONMENT as $name) {
            $this->savedEnvironment[$name] = getenv($name);
            putenv($name);
        }
        $this->broker = new BrokerScript();
        $this->stateDirectory = sys_get_temp_dir() . '/queen-pop-guard-' . bin2hex(random_bytes(8));
        $this->exitsDirectory = $this->stateDirectory . '/exits';
        mkdir($this->stateDirectory, 0700);
        mkdir($this->exitsDirectory, 0700);
        chmod($this->stateDirectory, 0700);
        chmod($this->exitsDirectory, 0700);

        parent::setUp();
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        foreach ($this->savedEnvironment as $name => $value) {
            putenv($value === false ? $name : "{$name}={$value}");
        }
        foreach ([$this->exitsDirectory, $this->stateDirectory] as $directory) {
            foreach (glob($directory . '/{,.}*', GLOB_BRACE) ?: [] as $path) {
                if (is_file($path) || is_link($path)) {
                    @unlink($path);
                }
            }
            @rmdir($directory);
        }
    }

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        // As in cb3-backend: the default connection only dispatches, and
        // every pool works a Queen connection of its own.
        $app['config']->set('queue.default', 'routed');
        $app['config']->set('queue.connections.routed', ['driver' => 'routed']);
        $app['config']->set('queue.connections.queen-batch', [
            'driver' => 'queen',
            'queue' => 'batch',
            'url' => 'http://queen.test:6632',
            'retry_attempts' => 1,
            'handler' => HandlerStack::create($this->broker),
        ]);
        $app['config']->set('queen.supervisor.state_directory', $this->stateDirectory);
        $app['config']->set('queen.job_metrics.enabled', false);
    }

    /**
     * php-client 2.3.1 forked workers that popped from the default
     * connection, which only dispatches: each threw on every pop for as long
     * as it lived, and the supervisor said ready. The worker now leaves at
     * its first loop, before it pops at all.
     */
    #[TestWith(['spawned'])]
    #[TestWith(['forked'])]
    public function testAWorkerOnAnotherConnectionThanItsPoolsLeavesAtItsFirstLoop(string $mode): void
    {
        $worker = $this->superviseAs($mode);

        [$code, $output] = $this->work($mode, $worker, 'routed', 3);

        $this->assertSame(1, $code);
        $this->assertSays('works connection [routed], not its pool\'s connection [queen-batch]', $output);
        $this->assertSame(0, $this->routedPops, 'it popped');
        $this->assertSame(0, $this->broker->count());
        $this->assertSame(1, $worker->loops);
    }

    /** A LogicException says the pop is wrong, not the broker: no retry fixes it. */
    #[TestWith(['spawned'])]
    #[TestWith(['forked'])]
    public function testAPopThatThrowsALogicExceptionEndsTheWorkerAtItsNextLoop(string $mode): void
    {
        $this->broker->answer(new \InvalidArgumentException('Invalid queue name [batch!]'));
        $worker = $this->superviseAs($mode);

        [$code, $output] = $this->work($mode, $worker, 'queen-batch', 5);

        $this->assertSame(1, $code);
        $this->assertSays('cannot pop from connection [queen-batch]: InvalidArgumentException: Invalid queue name [batch!]', $output);
        $this->assertSame(1, $this->broker->count(), 'it popped again');
        $this->assertSame(2, $worker->loops);
    }

    /**
     * A job whose timeout its lease cannot cover is wrong in the code, and
     * only a deploy fixes it: the worker leaves, so its pool shows it, and the
     * delivery waits for a worker that runs the fixed code.
     */
    #[TestWith(['spawned'])]
    #[TestWith(['forked'])]
    public function testAJobWhoseTimeoutItsLeaseCannotCoverEndsTheWorker(string $mode): void
    {
        $this->broker->answer($this->delivery(['job' => 'Handler@handle', 'uuid' => 'too-long', 'timeout' => 400]), times: 1);
        $worker = $this->superviseAs($mode);

        [$code, $output] = $this->work($mode, $worker, 'queen-batch', 5);

        $this->assertSame(1, $code);
        $this->assertSays('cannot pop from connection [queen-batch]: Queen\Laravel\Queue\UnsafeJobTimeoutException: '
            . 'Queen Laravel job timeout [400] must be positive and shorter than retry_after [90]', $output);
        $this->assertSame(2, $worker->loops);
    }

    /**
     * A delivery that carries no Laravel job goes to the dead-letter queue,
     * and the worker goes on: one bad message must not stop a pool.
     */
    #[TestWith(['spawned'])]
    #[TestWith(['forked'])]
    public function testADeliveryThatCarriesNoLaravelJobDoesNotEndTheWorker(string $mode): void
    {
        $this->broker->answer($this->delivery(['poison' => 'not a Laravel job']), times: 1);
        $this->broker->answer(['success' => true, 'leaseReleased' => true, 'dlq' => true], times: 1);
        $worker = $this->superviseAs($mode);
        $files = [];
        $worker->onLoop = function (int $loop) use (&$files): void {
            $files[$loop] = $this->popFailures() !== null;
        };

        [$code, $output] = $this->work($mode, $worker, 'queen-batch', 3);

        $this->assertSame(0, $code, $output);
        $this->assertSame([1 => false, 2 => false, 3 => false], $files, 'a dead-lettered delivery counted as a failed pop');
        $this->assertSame(4, $this->broker->count(), 'one pop, its dlq ACK, two empty pops');
    }

    /**
     * A broker that is down, slow or refusing is not the worker's fault, and
     * no restart helps: restarting every worker of every pool would only turn
     * a short outage into a restart storm. The worker stays, says since when
     * its pops fail, and takes that back when it stops.
     */
    #[TestWith(['spawned'])]
    #[TestWith(['forked'])]
    public function testAWorkerWhoseBrokerRefusesStaysAndSaysSinceWhen(string $mode): void
    {
        $this->broker->answer(503);
        $worker = $this->superviseAs($mode);
        $seen = null;
        $worker->onLoop = function (int $loop) use (&$seen): void {
            if ($loop === 4) {
                $seen = $this->popFailures();
            }
        };
        $before = time();

        [$code, $output] = $this->work($mode, $worker, 'queen-batch', 4);

        $this->assertSame(0, $code, $output);
        $this->assertSame(4, $this->broker->count());
        $this->assertIsArray($seen, 'no file while its pops failed');
        $this->assertSame(getmypid(), $seen['pid']);
        $this->assertSame('batch', $seen['supervisor']);
        $this->assertSame('queen-batch', $seen['connection']);
        $this->assertGreaterThanOrEqual($before, $seen['failing_since']);
        $this->assertLessThanOrEqual(time(), $seen['failing_since']);
        $this->assertGreaterThanOrEqual(1, $seen['failures']);
        $this->assertStringContainsString('503', $seen['error']);
        $this->assertNull($this->popFailures(), 'a stopped worker left its file');
    }

    /**
     * Thirty seconds of a broker that refuses every pop, about thirty pops of
     * Laravel's worker, which sleeps a second after each failure: the worker
     * stays, the pool stays ready, and the first pop that works, empty or
     * not, takes the file back.
     */
    #[TestWith(['spawned'])]
    #[TestWith(['forked'])]
    public function testAPopThatWorksAgainTakesTheFileBack(string $mode): void
    {
        $this->broker->answer(503, times: 30);
        $this->broker->answer(204);
        $worker = $this->superviseAs($mode);
        $seen = [];
        $worker->onLoop = function (int $loop) use (&$seen): void {
            $seen[$loop] = $this->popFailures() !== null;
        };

        [$code, $output] = $this->work($mode, $worker, 'queen-batch', 32);

        $this->assertSame(0, $code, $output);
        $this->assertFalse($seen[1], 'a file before the first pop');
        $this->assertTrue($seen[31], 'no file after thirty failed pops');
        $this->assertFalse($seen[32], 'the file outlived a pop that worked');
    }

    /** queue:work run by hand, or by Horizon, is Laravel's as it was. */
    public function testAWorkerNoSupervisorStartedIsLeftAsLaravelMadeIt(): void
    {
        $this->broker->answer(503);
        $worker = $this->worker();

        [$code, $output] = $this->work('spawned', $worker, 'queen-batch', 6);

        $this->assertSame(0, $code, $output);
        $this->assertSame(6, $this->broker->count());
        $this->assertFalse($this->app->bound(WorkerPopGuard::class));
        $this->assertSame([], glob($this->exitsDirectory . '/*') ?: []);
    }

    /**
     * A job may dispatch onto, or read, another Queen connection: only the
     * pool's pops say whether the worker consumes.
     */
    public function testOnlyThePoolsConnectionIsWatched(): void
    {
        $this->superviseAs('spawned');
        $this->app['config']->set('queue.connections.queen-other', [
            'driver' => 'queen',
            'queue' => 'other',
            'url' => 'http://queen.test:6632',
            'retry_attempts' => 1,
            'handler' => HandlerStack::create(new PlanHandler([], ['status' => 503, 'json' => []])),
        ]);
        $this->assertTrue($this->app->bound(WorkerPopGuard::class));

        try {
            $this->app['queue']->connection('queen-other')->pop();
            $this->fail('the pop worked');
        } catch (\Throwable) {
        }

        $this->assertNull($this->popFailures());
    }

    /**
     * The pool's issues come from the workers it lists, after a minute: a
     * shorter outage, such as a broker failing over, reports nothing.
     */
    public function testAPoolIsNotReadyOnceEveryWorkerHasNotConsumedForAMinute(): void
    {
        $state = new SupervisorState($this->stateDirectory);
        $status = $this->supervisorStatus([101, 102]);

        $this->writePopFailures(101, 30);
        $this->writePopFailures(102, 30);
        $this->assertSame([], $this->issueCodes($state, $status));
        $this->assertAges([101 => 30, 102 => 30], $state, $status);

        $this->writePopFailures(101, 61);
        $this->assertSame(['pool_worker_not_consuming'], $this->issueCodes($state, $status));

        $this->writePopFailures(102, 60);
        $this->assertSame(['pool_not_consuming', 'pool_worker_not_consuming'], $this->issueCodes($state, $status));
        $this->assertAges([101 => 61, 102 => 60], $state, $status);

        unlink($this->exitsDirectory . '/101' . WorkerPopGuard::FILE_SUFFIX);
        unlink($this->exitsDirectory . '/102' . WorkerPopGuard::FILE_SUFFIX);
        $this->assertSame([], $this->issueCodes($state, $status));
        $this->assertAges([], $state, $status);
    }

    /**
     * A pool with one worker that has not consumed for a minute and one that
     * consumes still serves: it is ready, below full capacity.
     */
    public function testAPoolWithOneWorkerThatConsumesStaysReady(): void
    {
        $state = new SupervisorState($this->stateDirectory);
        $status = $this->supervisorStatus([101, 102]);

        $this->writePopFailures(101, 61);

        $this->assertTrue($state->readiness($status, true)['ready']);
        $this->assertSame(['pool_worker_not_consuming'], $this->issueCodes($state, $status));
        $this->assertAges([101 => 61], $state, $status);
    }

    /**
     * A file says something only about a worker the pool lists, of this
     * host, and only when it is the private regular file a worker writes.
     */
    public function testOnlyTheFilesOfThePoolsWorkersOnThisHostCount(): void
    {
        $state = new SupervisorState($this->stateDirectory);
        $status = $this->supervisorStatus([101]);

        $this->writePopFailures(999, 120);
        $this->assertAges([], $state, $status, 'a dead worker counted');

        $this->writePopFailures(101, 120);
        $elsewhere = [...$status, 'hostname' => 'another-' . gethostname()];
        $this->assertAges([], $state, $elsewhere, 'another host\'s pid counted');
        $this->assertAges([101 => 120], $state, $status);

        chmod($this->exitsDirectory . '/101' . WorkerPopGuard::FILE_SUFFIX, 0644);
        $this->assertAges([], $state, $status, 'a file others may write counted');

        unlink($this->exitsDirectory . '/101' . WorkerPopGuard::FILE_SUFFIX);
        $this->writePopFailures(999, 120);
        symlink(
            $this->exitsDirectory . '/999' . WorkerPopGuard::FILE_SUFFIX,
            $this->exitsDirectory . '/101' . WorkerPopGuard::FILE_SUFFIX,
        );
        $this->assertAges([], $state, $status, 'a link was followed');

        unlink($this->exitsDirectory . '/101' . WorkerPopGuard::FILE_SUFFIX);
        file_put_contents($this->exitsDirectory . '/101' . WorkerPopGuard::FILE_SUFFIX, str_repeat(' ', 4096));
        chmod($this->exitsDirectory . '/101' . WorkerPopGuard::FILE_SUFFIX, 0600);
        $this->assertAges([], $state, $status, 'an oversized file was read');
    }

    /**
     * `status --json` lists every worker of a pool that does not consume and
     * since how long, however short, so a monitor can apply a threshold of
     * its own; the checks fail after a minute.
     */
    public function testStatusJsonListsTheWorkersThatDoNotConsume(): void
    {
        $state = new SupervisorState($this->stateDirectory);
        $lock = $state->acquireLock();

        try {
            $state->writeStatus($this->supervisorStatus([101, 102]));
            $this->writePopFailures(102, 12);

            $document = $this->statusJson();
            $this->assertTrue($document['ready']);
            $this->assertTrue($document['processing_healthy']);
            $this->assertContains($document['pool_status'][0]['not_consuming_seconds'], [12, 13]);
            $this->assertSame(102, $document['pool_status'][0]['not_consuming'][0]['pid']);
            $this->assertSame(3, $document['pool_status'][0]['not_consuming'][0]['failures']);
            $this->assertSame('Queen\Exceptions\HttpException: 503', $document['pool_status'][0]['not_consuming'][0]['error']);

            $this->writePopFailures(101, 90);
            $this->writePopFailures(102, 61);
            $document = $this->statusJson();
            $this->assertFalse($document['ready']);
            $this->assertSame('pool_not_consuming', $document['readiness_issues'][0]['code']);
            $this->assertSame('pool_worker_not_consuming', $document['processing_health_issues'][1]['code']);
            $this->assertContains($document['pool_status'][0]['not_consuming_seconds'], [90, 91]);
            $this->artisan('queen:supervisor', ['action' => 'status', '--check' => true])->assertFailed();
            $this->artisan('queen:supervisor', ['action' => 'status', '--check-capacity' => true])->assertFailed();
            $this->artisan('queen:supervisor', ['action' => 'status', '--check-liveness' => true])->assertSuccessful();
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    /**
     * The whole chain, worker to probe: a minute of a refusing broker makes
     * the pool not ready, and the broker's return makes it ready again with
     * nobody's help.
     */
    public function testAPoolIsReadyAgainOnItsOwnWhenTheBrokerComesBack(): void
    {
        $state = new SupervisorState($this->stateDirectory);
        $status = $this->supervisorStatus([getmypid()]);
        $this->broker->answer(503, times: 3);
        $worker = $this->superviseAs('spawned');
        $ready = [];
        $worker->onLoop = function (int $loop) use ($state, $status, &$ready): void {
            if ($loop === 4) {
                // The broker has refused this worker's pops for a minute.
                $path = $this->exitsDirectory . '/' . getmypid() . WorkerPopGuard::FILE_SUFFIX;
                $document = json_decode((string) file_get_contents($path), true, 4, JSON_THROW_ON_ERROR);
                $document['failing_since'] -= 61;
                file_put_contents($path, json_encode($document));
            }
            $ready[$loop] = $state->readiness($status, true)['ready'];
        };

        [$code, $output] = $this->work('spawned', $worker, 'queen-batch', 5);

        $this->assertSame(0, $code, $output);
        $this->assertSame([1 => true, 2 => true, 3 => true, 4 => false, 5 => true], $ready);
    }

    /**
     * Start this test process as a worker of the `batch` pool: a spawned one
     * reads the environment as the application boots; a forked one is forked
     * from a server that booted without it, and reads it in the child.
     */
    private function superviseAs(string $mode): ScriptedWorker
    {
        $environment = [
            'QUEEN_LARAVEL_CONNECTION' => 'queen-batch',
            'QUEEN_LARAVEL_SUPERVISOR' => 'batch',
            'QUEEN_SUPERVISOR_EXITS_DIR' => $this->exitsDirectory,
        ];
        if ($mode === 'spawned') {
            foreach ($environment as $name => $value) {
                putenv("{$name}={$value}");
            }
            $this->refreshApplication();
            $worker = $this->worker();
        } else {
            $worker = $this->worker();
            foreach ($environment as $name => $value) {
                putenv("{$name}={$value}");
            }
            (new \ReflectionMethod(ForkServerCommand::class, 'prepareChild'))->invoke($this->forkServer());
        }

        return $worker;
    }

    private function worker(): ScriptedWorker
    {
        $this->app['queue']->extend('routed', fn (): ConnectorInterface => new DispatchOnlyConnector(
            function (): void {
                $this->routedPops++;
            },
        ));
        $worker = new ScriptedWorker(
            $this->app['queue'],
            $this->app['events'],
            $this->app->make(ExceptionHandler::class),
            fn (): bool => false,
        );
        $this->app->instance('queue.worker', $worker);

        return $worker;
    }

    /** @return array{0: int, 1: string} the exit status and the output */
    private function work(string $mode, ScriptedWorker $worker, string $connection, int $loops): array
    {
        $worker->maxLoops = $loops;
        // What a supervisor sends for the pool.
        $arguments = [
            $connection, '--queue=batch', '--sleep=1', '--timeout=60', '--tries=1', '--memory=448',
            '--backoff=0', '--max-jobs=0', '--max-time=0', '--rest=0',
        ];
        $kernel = $this->app->make(Kernel::class);
        if ($mode === 'spawned') {
            try {
                $code = $kernel->handle(new ArgvInput(['artisan', 'queue:work', ...$arguments]), $output = new BufferedOutput());
            } catch (\Throwable $error) {
                // Testbench's console kernel throws what the application's
                // kernel reports and renders before `artisan` exits 1.
                return [1, $error->getMessage()];
            }

            return [$code, $output->fetch()];
        }
        $server = $this->forkServer($output = new BufferedOutput());
        $code = (new \ReflectionMethod(ForkServerCommand::class, 'runWorker'))
            ->invoke($server, $kernel->all()['queue:work'], $arguments);

        return [$code, $output->fetch()];
    }

    private function forkServer(?BufferedOutput $output = null): ForkServerCommand
    {
        $server = new ForkServerCommand();
        $server->setLaravel($this->app);
        $server->setOutput(new OutputStyle(new ArrayInput([]), $output ?? new BufferedOutput()));

        return $server;
    }

    /** The console wraps a message in a box, at the terminal's width. */
    private function assertSays(string $message, string $output): void
    {
        $this->assertStringContainsString(
            (string) preg_replace('/\s+/', '', $message),
            (string) preg_replace('/\s+/', '', $output),
        );
    }

    /** @return array<string, mixed>|null this process's pop-failures file */
    private function popFailures(): ?array
    {
        $path = $this->exitsDirectory . '/' . getmypid() . WorkerPopGuard::FILE_SUFFIX;
        clearstatcache(true, $path);
        if (!is_file($path)) {
            return null;
        }
        $this->assertSame(0600, fileperms($path) & 07777);

        return json_decode((string) file_get_contents($path), true, 4, JSON_THROW_ON_ERROR);
    }

    /** @return array<string, mixed> a pop's answer: one delivery of $data */
    private function delivery(array $data): array
    {
        return [
            'success' => true,
            'queue' => 'batch',
            'leaseId' => 'lease-1',
            'messages' => [[
                'id' => 'message-1',
                'transactionId' => 'transaction-1',
                'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => 'laravel-0001',
                'leaseId' => 'lease-1',
                'deliveryAttempt' => 1,
                'data' => $data,
            ]],
        ];
    }

    /** What a worker of pid $pid leaves when its pops failed for $seconds. */
    private function writePopFailures(int $pid, int $seconds): void
    {
        $path = $this->exitsDirectory . '/' . $pid . WorkerPopGuard::FILE_SUFFIX;
        @unlink($path);
        file_put_contents($path, json_encode([
            'pid' => $pid,
            'supervisor' => 'batch',
            'connection' => 'queen-batch',
            'failing_since' => time() - $seconds,
            'failures' => 3,
            'updated_at' => time(),
            'error' => 'Queen\Exceptions\HttpException: 503',
        ]));
        chmod($path, 0600);
    }

    /** @param list<int> $pids */
    private function supervisorStatus(array $pids): array
    {
        return [
            'engine' => 'rust',
            'state' => 'running',
            'hostname' => gethostname(),
            'pools' => [],
            'pool_status' => [[
                'supervisor' => 'batch',
                'queue' => 'compliance',
                'desired' => count($pids),
                'running' => count($pids),
                'pids' => $pids,
                'healthy' => true,
                'restart_state' => 'closed',
                'restart_failures' => 0,
                'depth' => 0,
                'depth_available' => true,
            ]],
        ];
    }

    /** @return list<string> the readiness and capacity issues, in that order */
    private function issueCodes(SupervisorState $state, array $status): array
    {
        $readiness = array_column($state->readiness($status, true)['issues'], 'code');
        $capacity = array_diff(array_column($state->capacityHealth($status, true)['issues'], 'code'), $readiness);

        return [...$readiness, ...array_values($capacity)];
    }

    /**
     * The workers of the pool that do not consume, in order, each for
     * $expected seconds or one more: the clock may tick in between.
     *
     * @param array<int, int> $expected seconds by pid
     */
    private function assertAges(array $expected, SupervisorState $state, array $status, string $message = ''): void
    {
        $workers = $state->workersNotConsuming($status, $status['pool_status'][0]);
        $this->assertSame(array_keys($expected), array_column($workers, 'pid'), $message);
        foreach ($workers as $worker) {
            $this->assertContains($worker['seconds'] - $expected[$worker['pid']], [0, 1], $message);
        }
    }

    /** @return array<string, mixed> */
    private function statusJson(): array
    {
        $output = new BufferedOutput();
        $this->app->make(Kernel::class)->call('queen:supervisor', ['action' => 'status', '--json' => true], $output);

        return json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
    }
}

/** The broker of the pool's connection: a scripted answer to every request. */
final class BrokerScript
{
    /** @var list<int|\Throwable|array<string, mixed>> */
    private array $plan = [];

    private int|\Throwable|array $default = 204;

    private int $requests = 0;

    /**
     * Answer the next $times requests so, with a status, a failure or a JSON
     * body; without $times, every one after the plan.
     */
    public function answer(int|\Throwable|array $answer, ?int $times = null): void
    {
        if ($times === null) {
            $this->default = $answer;

            return;
        }
        array_push($this->plan, ...array_fill(0, $times, $answer));
    }

    public function count(): int
    {
        return $this->requests;
    }

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $this->requests++;
        $answer = array_shift($this->plan) ?? $this->default;
        if ($answer instanceof \Throwable) {
            throw $answer;
        }
        if (is_array($answer)) {
            return new FulfilledPromise(new Response(200, ['Content-Type' => 'application/json'], json_encode($answer)));
        }

        // An empty pop is a bodiless 204.
        return new FulfilledPromise(new Response($answer, ['Content-Type' => 'application/json'], $answer === 204 ? '' : '{}'));
    }
}

/** cb3-backend's `routed` connection: it dispatches to the pools and never pops. */
final class DispatchOnlyConnector implements ConnectorInterface
{
    public function __construct(private \Closure $popped)
    {
    }

    public function connect(array $config): QueueContract
    {
        return new class ($this->popped) extends Queue implements QueueContract {
            public function __construct(private \Closure $popped)
            {
            }

            public function size($queue = null)
            {
                return 0;
            }

            public function push($job, $data = '', $queue = null)
            {
                return null;
            }

            public function pushRaw($payload, $queue = null, array $options = [])
            {
                return null;
            }

            public function later($delay, $job, $data = '', $queue = null)
            {
                return null;
            }

            public function pop($queue = null)
            {
                ($this->popped)();

                throw new \LogicException('The routed queue connection only dispatches; workers pop from the pool connections.');
            }
        };
    }
}
