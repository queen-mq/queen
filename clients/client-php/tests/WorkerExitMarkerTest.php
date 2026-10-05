<?php

namespace Queen\Tests;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Events\Dispatcher;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\JobTimedOut;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerStopReason;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\Prefork\ForkedProcess;
use Queen\Laravel\Supervisor\Prefork\ForkServerClient;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Laravel\Supervisor\WorkerExitMarker;
use ReflectionMethod;
use ReflectionProperty;
use Symfony\Component\Process\Process;

/**
 * Laravel SIGKILLs a worker whose job outlives its timeout, and stops one
 * that passes --memory. The worker says which first, in a marker in the
 * master's state directory, and the master restarts it without counting a
 * crash.
 */
final class WorkerExitMarkerTest extends TestCase
{
    private const OPTIONS = ['restart_backoff' => 1, 'restart_backoff_max' => 8, 'stable_after' => 60];

    /** @var list<string> */
    private array $directories = [];

    /** @var list<string> */
    private array $output = [];

    protected function setUp(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_geteuid') || !function_exists('pcntl_async_signals')) {
            $this->markTestSkipped('ext-posix and ext-pcntl on a Unix host are required.');
        }
    }

    protected function tearDown(): void
    {
        putenv(WorkerExitMarker::ENVIRONMENT);
        foreach (array_reverse($this->directories) as $directory) {
            $this->removeDirectory($directory);
        }
    }

    public function testTheTimeoutListenerIsOnlyRegisteredUnderASupervisor(): void
    {
        $events = new Dispatcher(new Container());
        putenv(WorkerExitMarker::ENVIRONMENT);
        WorkerExitMarker::listenFromEnvironment($events);
        $this->assertFalse($events->hasListeners(JobTimedOut::class));

        $directory = $this->privateDirectory();
        putenv(WorkerExitMarker::ENVIRONMENT . '=' . $directory);
        WorkerExitMarker::listenFromEnvironment($events);
        $events->dispatch(new JobTimedOut('queen', $this->createStub(Job::class)));

        $marker = $directory . '/' . getmypid();
        $this->assertSame('timeout', file_get_contents($marker));
        $this->assertSame(0600, fileperms($marker) & 07777);
        $this->assertSame([(string) getmypid()], $this->entries($directory));
    }

    public function testTheListenerNeverThrowsInsideLaravelsTimeoutHandler(): void
    {
        $events = new Dispatcher(new Container());
        putenv(WorkerExitMarker::ENVIRONMENT . '=' . $this->privateDirectory() . '/missing');
        WorkerExitMarker::listenFromEnvironment($events);

        $events->dispatch(new JobTimedOut('queen', $this->createStub(Job::class)));

        $this->assertTrue($events->hasListeners(JobTimedOut::class));
    }

    public function testAMemoryStopIsAnnouncedOnlyAfterAJob(): void
    {
        $directory = $this->privateDirectory();
        putenv(WorkerExitMarker::ENVIRONMENT . '=' . $directory);
        $marker = $directory . '/' . getmypid();

        // Laravel also checks --memory after an idle sleep: a worker whose
        // boot footprint is above the limit stops at once, every time.
        $idle = new Dispatcher(new Container());
        WorkerExitMarker::listenFromEnvironment($idle);
        $idle->dispatch(new WorkerStopping(12, null, WorkerStopReason::MaxMemoryExceeded));
        $this->assertFileDoesNotExist($marker);

        $busy = new Dispatcher(new Container());
        WorkerExitMarker::listenFromEnvironment($busy);
        $busy->dispatch(new JobProcessing('queen', $this->createStub(Job::class)));
        $busy->dispatch(new WorkerStopping(0, null, WorkerStopReason::MaxJobsExceeded));
        $this->assertFileDoesNotExist($marker);
        $busy->dispatch(new WorkerStopping(12, null, WorkerStopReason::MaxMemoryExceeded));
        $this->assertSame('memory', file_get_contents($marker));
        unlink($marker);

        // Before Laravel 12 the event carries the exit status alone.
        $legacy = new Dispatcher(new Container());
        WorkerExitMarker::listenFromEnvironment($legacy);
        $legacy->dispatch(new JobProcessing('queen', $this->createStub(Job::class)));
        $legacy->dispatch(new WorkerStopping(12));
        $this->assertSame('memory', file_get_contents($marker));
    }

    public function testAQueueRestartIsAnnouncedWithOrWithoutAJob(): void
    {
        $directory = $this->privateDirectory();
        putenv(WorkerExitMarker::ENVIRONMENT . '=' . $directory);
        $marker = $directory . '/' . getmypid();

        $idle = new Dispatcher(new Container());
        WorkerExitMarker::listenFromEnvironment($idle);
        $idle->dispatch(new WorkerStopping(0, null, WorkerStopReason::ReceivedRestartSignal));
        $this->assertSame('restart', file_get_contents($marker));
        unlink($marker);

        $busy = new Dispatcher(new Container());
        WorkerExitMarker::listenFromEnvironment($busy);
        $busy->dispatch(new JobProcessing('queen', $this->createStub(Job::class)));
        $busy->dispatch(new WorkerStopping(0, null, WorkerStopReason::ReceivedRestartSignal));
        $this->assertSame('restart', file_get_contents($marker));
        unlink($marker);

        // Before Laravel 12 nothing says why a worker exited 0.
        $legacy = new Dispatcher(new Container());
        WorkerExitMarker::listenFromEnvironment($legacy);
        $legacy->dispatch(new WorkerStopping(0));
        $this->assertFileDoesNotExist($marker);
    }

    public function testTheMarkerIsOnlyWrittenIntoAPrivateRealDirectory(): void
    {
        $shared = $this->privateDirectory();
        chmod($shared, 0755);
        $this->assertFalse((new WorkerExitMarker($shared))->write(WorkerExitMarker::TIMEOUT));
        $this->assertSame([], $this->entries($shared));

        $private = $this->privateDirectory();
        $link = $this->privateDirectory() . '/exits';
        symlink($private, $link);
        $this->assertFalse((new WorkerExitMarker($link))->write(WorkerExitMarker::TIMEOUT));
        $this->assertSame([], $this->entries($private));

        $missing = $this->privateDirectory() . '/missing';
        $this->assertFalse((new WorkerExitMarker($missing))->write(WorkerExitMarker::TIMEOUT));
        $this->assertDirectoryDoesNotExist($missing);

        $this->assertFalse((new WorkerExitMarker('relative/exits'))->write(WorkerExitMarker::TIMEOUT));
    }

    public function testTheMarkerNeverFollowsALinkLeftAtItsTemporaryName(): void
    {
        $directory = $this->privateDirectory();
        $outside = $this->privateDirectory() . '/outside';
        symlink($outside, $directory . '/' . getmypid() . '.tmp');

        $this->assertTrue((new WorkerExitMarker($directory))->write(WorkerExitMarker::TIMEOUT));

        $this->assertFileDoesNotExist($outside);
        $this->assertSame('timeout', file_get_contents($directory . '/' . getmypid()));
        $this->assertSame([(string) getmypid()], $this->entries($directory));
    }

    public function testALaravelJobTimeoutLeavesTheMarkerThenDiesBySigkill(): void
    {
        $directory = $this->privateDirectory();

        [$supervised, $pid] = $this->runTimedOutWorker($directory);
        $this->assertSame(128 + SIGKILL, $supervised->getExitCode(), $supervised->getErrorOutput());
        $this->assertSame('timeout', file_get_contents($directory . '/' . $pid));

        [$unsupervised] = $this->runTimedOutWorker(null);
        $this->assertSame(128 + SIGKILL, $unsupervised->getExitCode(), $unsupervised->getErrorOutput());
        $this->assertSame([(string) $pid], $this->entries($directory));
    }

    public function testThePhpSupervisorRestartsATimedOutWorkerWithoutBackoff(): void
    {
        [$supervisor, $exits] = $this->supervisor();
        [$worker, $pid] = $this->runTimedOutWorker($exits);
        $this->track($supervisor, $worker, $pid);

        $this->reap($supervisor);

        $this->assertSame([], $this->property($supervisor, 'crashCount'));
        $this->assertSame('normal', $this->permission($supervisor));
        $this->assertSame([], $this->entries($exits));
        $this->assertStringContainsString(
            "[orders:high] worker pid={$pid} exited after Laravel's job timeout; restarted without backoff",
            implode('', $this->output),
        );

        // The OOM killer, a lease fence or kill -9 leave no marker: still a crash.
        $killed = new Process([PHP_BINARY, '-r', 'posix_kill(getmypid(), SIGKILL); sleep(5);']);
        $killed->start();
        $killedPid = $killed->getPid();
        $this->waitForExit($killed);
        $this->assertSame(128 + SIGKILL, $killed->getExitCode());
        $this->track($supervisor, $killed, $killedPid);

        $this->reap($supervisor);

        $this->assertSame([$this->key($supervisor) => 1], $this->property($supervisor, 'crashCount'));
        $this->assertNull($this->permission($supervisor));
    }

    public function testAMarkerOnlyExplainsASigkill(): void
    {
        [$supervisor, $exits] = $this->supervisor();
        $this->writeMarker($exits, 4242, 'timeout');
        $this->track($supervisor, $this->exited(1), 4242);

        $this->reap($supervisor);

        $this->assertSame([$this->key($supervisor) => 1], $this->property($supervisor, 'crashCount'));
        $this->assertSame([], $this->entries($exits));
    }

    public function testAMemoryStopAfterAJobRestartsLikeACleanExit(): void
    {
        [$supervisor, $exits] = $this->supervisor();
        $this->writeMarker($exits, 4244, 'memory');
        $this->track($supervisor, $this->exited(12), 4244);

        $this->reap($supervisor);

        $this->assertSame([], $this->property($supervisor, 'crashCount'));
        $this->assertSame('normal', $this->permission($supervisor));
        $this->assertSame([], $this->entries($exits));

        // No marker: no job ran before the limit, so the boot footprint is
        // above --memory and every restart stops the same way.
        $this->track($supervisor, $this->exited(12), 4245);

        $this->reap($supervisor);

        $this->assertSame([$this->key($supervisor) => 1], $this->property($supervisor, 'crashCount'));
        $this->assertNull($this->permission($supervisor));
    }

    public function testAQueueRestartIsACleanExitAndOnlyExplainsExitZero(): void
    {
        [$supervisor, $exits] = $this->supervisor();
        $this->writeMarker($exits, 4246, 'restart');
        $this->track($supervisor, $this->exited(0), 4246);

        $this->reap($supervisor);

        $this->assertSame([], $this->property($supervisor, 'crashCount'));
        $this->assertSame([], $this->entries($exits));
        // A spawned worker starts from the code on disk anyway.
        $this->assertFalse($this->property($supervisor, 'forkServerStale'));

        $this->writeMarker($exits, 4247, 'restart');
        $this->track($supervisor, $this->exited(1), 4247);

        $this->reap($supervisor);

        $this->assertSame([$this->key($supervisor) => 1], $this->property($supervisor, 'crashCount'));
    }

    public function testAQueueRestartReplacesTheForkServerAndRetiresTheOldOneWithItsLastWorker(): void
    {
        if (!function_exists('pcntl_fork') || !function_exists('posix_setsid')) {
            $this->markTestSkipped('ext-pcntl and ext-posix are required.');
        }
        $stateDirectory = $this->privateDirectory();
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $stateDirectory,
                'supervisors' => ['orders' => self::OPTIONS],
                'prefork' => true,
                'php_binary' => PHP_BINARY,
                'artisan' => __DIR__ . '/Fixtures/Prefork/fork_server.php',
                'cwd' => __DIR__,
            ],
            output: function (string $buffer): void {
                $this->output[] = $buffer;
            },
        );
        (new ReflectionMethod(PhpSupervisor::class, 'prepareExitMarkers'))->invoke($supervisor);
        (new ReflectionMethod(PhpSupervisor::class, 'startForkServer'))->invoke($supervisor);
        $first = $this->property($supervisor, 'forkServer');
        $this->assertInstanceOf(ForkServerClient::class, $first);
        $exits = realpath($stateDirectory) . '/exits';
        $fork = new ReflectionMethod(PhpSupervisor::class, 'forkWorker');
        $report = $exits . '/report';

        try {
            $restarted = $fork->invoke($supervisor, 'orders', 'high', ['exit', $report, '0'], []);
            $busy = $fork->invoke($supervisor, 'orders', 'high', ['sleep', $report, '0'], []);
            $this->assertInstanceOf(ForkedProcess::class, $restarted);
            $restartedPid = $restarted->getPid() ?? $this->waitForPid($restarted);
            $deadline = microtime(true) + 10;
            while ($restarted->isRunning() && microtime(true) < $deadline) {
                usleep(20_000);
            }
            $this->writeMarker($exits, $restartedPid, 'restart');
            $this->setProperty($supervisor, 'processes', ['orders' => ['high' => [$restarted, $busy]]]);
            $this->setProperty($supervisor, 'workerPids', [
                spl_object_id($restarted) => $restartedPid,
                spl_object_id($busy) => $busy->getPid(),
            ]);
            $refresh = new ReflectionMethod(PhpSupervisor::class, 'refreshForkServer');

            $this->reap($supervisor);
            $refresh->invoke($supervisor);

            $second = $this->property($supervisor, 'forkServer');
            $this->assertInstanceOf(ForkServerClient::class, $second);
            $this->assertNotSame($first, $second);
            $this->assertSame([$first], $this->property($supervisor, 'retiredForkServers'));
            $this->assertTrue($first->isAlive(), 'the old server stays open for the worker it forked');
            $this->assertTrue($busy->isRunning());
            $this->assertStringContainsString('prefork: queue:restart received, starting a new fork server', implode('', $this->output));

            // The new server forks; the old one closes with its last worker.
            $next = $fork->invoke($supervisor, 'orders', 'high', ['sleep', $report, '0'], []);
            $this->assertSame($second, $next->server());
            $busy->signal(SIGTERM);
            $deadline = microtime(true) + 10;
            while ($busy->isRunning() && microtime(true) < $deadline) {
                usleep(20_000);
            }
            $this->setProperty($supervisor, 'processes', ['orders' => ['high' => [$next]]]);
            $refresh->invoke($supervisor);

            $this->assertSame([], $this->property($supervisor, 'retiredForkServers'));
            $this->assertFalse($first->isAlive());
            $this->assertTrue($next->isRunning());
        } finally {
            (new ReflectionMethod(PhpSupervisor::class, 'closeForkServer'))->invoke($supervisor);
        }
    }

    public function testATimedOutProbeIsReplacedAtOnceAndLeavesTheCircuitAsItWas(): void
    {
        [$supervisor, $exits] = $this->supervisor();
        $key = $this->key($supervisor);
        $probe = $this->exited(128 + SIGKILL);
        $this->writeMarker($exits, 4243, 'timeout');
        $this->track($supervisor, $probe, 4243);
        $this->setProperty($supervisor, 'crashCount', [$key => 5]);
        $this->setProperty($supervisor, 'restartPhase', [$key => 'probe']);
        $this->setProperty($supervisor, 'restartAfter', [$key => microtime(true) - 30]);
        $this->setProperty($supervisor, 'restartProbes', [spl_object_id($probe) => $key]);

        $this->reap($supervisor);

        $this->assertSame(5, $this->property($supervisor, 'crashCount')[$key]);
        $this->assertSame('open', $this->property($supervisor, 'restartPhase')[$key]);
        $this->assertSame('probe', $this->permission($supervisor));
        $this->assertSame([], $this->property($supervisor, 'restartProbes'));
    }

    public function testTheMasterClearsStaleMarkersAndPassesTheirDirectoryToWorkers(): void
    {
        $stateDirectory = $this->privateDirectory();
        $outside = $this->privateDirectory() . '/target';
        file_put_contents($outside, 'timeout');
        mkdir($stateDirectory . '/exits', 0700);
        $this->writeMarker($stateDirectory . '/exits', 123, 'timeout');
        file_put_contents($stateDirectory . '/exits/123.tmp', 'timeout');
        symlink($outside, $stateDirectory . '/exits/456');
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $stateDirectory,
                'cwd' => dirname(__DIR__),
                'php_binary' => PHP_BINARY,
                'artisan' => __DIR__ . '/Fixtures/FakeArtisan.php',
                'shutdown_grace' => 1,
            ],
        );

        (new ReflectionMethod(PhpSupervisor::class, 'prepareExitMarkers'))->invoke($supervisor);

        $exits = realpath($stateDirectory) . '/exits';
        $this->assertSame([], $this->entries($exits));
        $this->assertSame(0700, fileperms($exits) & 07777);
        $this->assertFileExists($outside);

        $process = (new ReflectionMethod(PhpSupervisor::class, 'startWorker'))
            ->invoke($supervisor, 'orders', 'high', $this->workerOptions());
        $this->assertSame(0, $process->wait());
        $document = json_decode(trim($process->getOutput()), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame($exits, $document['exits_directory']);
    }

    public function testWorkersGetNoExitDirectoryWhenTheMasterHasNone(): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $this->privateDirectory(),
                'cwd' => dirname(__DIR__),
                'php_binary' => PHP_BINARY,
                'artisan' => __DIR__ . '/Fixtures/FakeArtisan.php',
                'shutdown_grace' => 1,
            ],
        );
        putenv(WorkerExitMarker::ENVIRONMENT . '=/tmp/inherited-queen-exits');

        $process = (new ReflectionMethod(PhpSupervisor::class, 'startWorker'))
            ->invoke($supervisor, 'orders', 'high', $this->workerOptions());
        $this->assertSame(0, $process->wait());

        $document = json_decode(trim($process->getOutput()), true, 512, JSON_THROW_ON_ERROR);
        $this->assertFalse($document['exits_directory']);
    }

    public function testTheStateReadsOnlyABoundedPrivateRegularMarker(): void
    {
        $state = new SupervisorState($this->privateDirectory());
        $exits = $state->resetExitMarkers();
        $outside = $this->privateDirectory() . '/target';
        file_put_contents($outside, 'timeout');
        chmod($outside, 0600);

        symlink($outside, $exits . '/1');
        $this->writeMarker($exits, 2, str_repeat('x', 65));
        $this->writeMarker($exits, 3, 'timeout');
        chmod($exits . '/3', 0644);
        $this->writeMarker($exits, 4, "timeout\n");
        file_put_contents($exits . '/5.tmp', 'timeout');
        mkdir($exits . '/6', 0700);

        $this->assertNull($state->takeExitMarker(1));
        $this->assertSame('timeout', file_get_contents($outside));
        $this->assertNull($state->takeExitMarker(2));
        $this->assertNull($state->takeExitMarker(3));
        $this->assertSame('timeout', $state->takeExitMarker(4));
        $this->assertNull($state->takeExitMarker(4));
        $this->assertNull($state->takeExitMarker(5));
        $this->assertNull($state->takeExitMarker(6));
        $this->assertSame(['6'], $this->entries($exits));

        $this->writeMarker($exits, 7, 'timeout');
        file_put_contents($exits . '/7.tmp', 'timeout');
        $state->removeExitMarker(7);
        $this->assertSame(['6'], $this->entries($exits));
    }

    /** @return array{Process, int} the exited worker and its pid */
    private function runTimedOutWorker(?string $exits): array
    {
        $process = new Process(
            [PHP_BINARY, __DIR__ . '/Fixtures/TimedOutWorker.php'],
            env: [WorkerExitMarker::ENVIRONMENT => $exits ?? false],
            timeout: 20,
        );
        $process->start();
        $pid = $process->getPid();
        $this->waitForExit($process);

        return [$process, $pid];
    }

    /** Like the supervisor: wait() throws for a signal it did not send itself. */
    private function waitForExit(Process $process): void
    {
        $deadline = microtime(true) + 20;
        while ($process->isRunning()) {
            if (microtime(true) > $deadline) {
                $process->stop(0);
                $this->fail('The worker did not exit.');
            }
            usleep(20_000);
        }
    }

    /** @return array{PhpSupervisor, string} the supervisor and its exit-marker directory */
    private function supervisor(): array
    {
        $stateDirectory = $this->privateDirectory();
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $stateDirectory, 'supervisors' => ['orders' => self::OPTIONS]],
            output: function (string $buffer): void {
                $this->output[] = $buffer;
            },
        );
        (new ReflectionMethod(PhpSupervisor::class, 'prepareExitMarkers'))->invoke($supervisor);

        return [$supervisor, realpath($stateDirectory) . '/exits'];
    }

    private function track(PhpSupervisor $supervisor, Process $process, int $pid): void
    {
        $this->setProperty($supervisor, 'processes', ['orders' => ['high' => [$process]]]);
        $this->setProperty($supervisor, 'startedAt', [spl_object_id($process) => microtime(true) - 3]);
        $this->setProperty($supervisor, 'workerPids', [spl_object_id($process) => $pid]);
    }

    private function reap(PhpSupervisor $supervisor): void
    {
        (new ReflectionMethod(PhpSupervisor::class, 'reap'))->invoke($supervisor, 'orders', self::OPTIONS);
    }

    private function permission(PhpSupervisor $supervisor): ?string
    {
        return (new ReflectionMethod(PhpSupervisor::class, 'restartPermission'))->invoke($supervisor, 'orders', 'high');
    }

    private function key(PhpSupervisor $supervisor): string
    {
        return (new ReflectionMethod(PhpSupervisor::class, 'poolKey'))->invoke($supervisor, 'orders', 'high');
    }

    private function exited(int $exitCode): Process
    {
        $process = $this->createStub(Process::class);
        $process->method('isRunning')->willReturn(false);
        $process->method('getExitCode')->willReturn($exitCode);

        return $process;
    }

    private function waitForPid(ForkedProcess $process): int
    {
        $pid = (new ReflectionProperty($process, 'forkedPid'))->getValue($process);
        $this->assertIsInt($pid);

        return $pid;
    }

    private function writeMarker(string $directory, int $pid, string $contents): void
    {
        file_put_contents($directory . '/' . $pid, $contents);
        chmod($directory . '/' . $pid, 0600);
    }

    /** @return array<string, mixed> */
    private function workerOptions(): array
    {
        return [
            'connection' => 'queen',
            'consumer_group' => 'orders-v1',
            'queues' => ['high'],
            'balance' => 'auto',
            'strategy' => 'size',
            'sleep' => 0,
            'timeout' => 30,
            'tries' => 3,
            'memory' => 128,
            'backoff' => 0,
            'max_jobs' => 0,
            'max_time' => 0,
            'rest' => 0,
            'force' => false,
        ];
    }

    /** @return list<string> */
    private function entries(string $directory): array
    {
        $entries = array_values(array_diff(scandir($directory) ?: [], ['.', '..']));
        sort($entries);

        return $entries;
    }

    private function privateDirectory(): string
    {
        $directory = sys_get_temp_dir() . '/queen-exit-marker-' . bin2hex(random_bytes(8));
        mkdir($directory, 0700);
        chmod($directory, 0700);

        return $this->directories[] = $directory;
    }

    private function setProperty(object $object, string $property, mixed $value): void
    {
        (new ReflectionProperty($object, $property))->setValue($object, $value);
    }

    private function property(object $object, string $property): mixed
    {
        return (new ReflectionProperty($object, $property))->getValue($object);
    }

    private function removeDirectory(string $directory): void
    {
        if (!is_dir($directory) || is_link($directory)) {
            return;
        }
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($directory, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );
        foreach ($iterator as $item) {
            $item->isDir() && !$item->isLink() ? rmdir($item->getPathname()) : unlink($item->getPathname());
        }
        rmdir($directory);
    }
}
