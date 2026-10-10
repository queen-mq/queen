<?php

namespace Queen\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use Illuminate\Queue\QueueManager;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Queen;

/**
 * The PHP master in a process of its own, with a worker stand-in: its stop
 * signal, as a platform sends it, to the master alone, which then has the
 * platform's stop deadline to drain its workers; and its status writes.
 */
final class LaravelSupervisorSignalTest extends TestCase
{
    /** How long the broker takes to fail a call once it stops answering. */
    private const BLACK_HOLE_SECONDS = 2.0;

    private string $directory;

    private string|false $savedSignalDirectory;

    protected function setUp(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            $this->markTestSkipped('Needs ext-pcntl and ext-posix.');
        }
        $this->directory = sys_get_temp_dir() . '/queen-supervisor-signal-' . bin2hex(random_bytes(6));
        mkdir($this->directory . '/state', 0700, true);
        mkdir($this->directory . '/signals', 0700);
        $this->savedSignalDirectory = getenv('QUEEN_TEST_SIGNAL_DIRECTORY');
        // Symfony Process passes on what getenv() and $_SERVER both have.
        putenv('QUEEN_TEST_SIGNAL_DIRECTORY=' . $this->directory . '/signals');
        $_SERVER['QUEEN_TEST_SIGNAL_DIRECTORY'] = $this->directory . '/signals';
    }

    protected function tearDown(): void
    {
        putenv($this->savedSignalDirectory === false
            ? 'QUEEN_TEST_SIGNAL_DIRECTORY'
            : 'QUEEN_TEST_SIGNAL_DIRECTORY=' . $this->savedSignalDirectory);
        unset($_SERVER['QUEEN_TEST_SIGNAL_DIRECTORY']);
        $this->removeTree($this->directory);
    }

    /**
     * SIGTERM only stopped the loop, and the loop saw it after its broker
     * calls: the heartbeat, then each pool's depth, each up to http_timeout
     * per endpoint. With a broker that stopped answering, three URLs and an
     * http_timeout of 5, the workers got their SIGTERM some 45 seconds
     * later, and the platform killed them in the middle of a job. The
     * workers get it at once, whatever the broker does.
     */
    public function testWorkersGetSigtermAtOnceWhileTheMasterWaitsOnTheBroker(): void
    {
        $slowSince = $this->directory . '/slow-since';
        $master = $this->startMaster(new BlackHoledAfterFirstAnswer($slowSince, self::BLACK_HOLE_SECONDS));

        try {
            $worker = $this->waitFor(fn (): ?int => $this->startedWorker(), 10.0, 'the worker to start');
            // A depth call that the broker does not answer is on the wire.
            $this->waitFor(fn (): ?bool => is_file($slowSince) ? true : null, 10.0, 'the broker to stop answering');
            usleep(100_000);
            $signalled = microtime(true);
            posix_kill($master, SIGTERM);

            $received = $this->waitFor(
                fn (): ?float => is_file($file = $this->directory . "/signals/{$worker}.sigterm")
                    ? (float) file_get_contents($file)
                    : null,
                10.0,
                'the worker\'s SIGTERM',
            );
            $this->assertLessThan(1.0, $received - $signalled, 'the worker waited behind the broker calls');
        } finally {
            $this->reap($master);
        }
    }

    /**
     * One status.json write that failed, on a full or failing disk, ended the
     * master, which drained every worker. The workers keep running while the
     * master retries the write each pass; meanwhile status.json ages, and
     * the probes report the master stale.
     */
    public function testAFailedStatusWriteDoesNotDrainTheWorkers(): void
    {
        $master = $this->startMaster(static fn (): FulfilledPromise => new FulfilledPromise(new Response(
            200,
            ['Content-Type' => 'application/json'],
            json_encode(['pending' => 0, 'ready' => 0, 'processing' => 0]),
        )));
        $status = $this->directory . '/state/status.json';

        try {
            $worker = $this->waitFor(fn (): ?int => $this->startedWorker(), 10.0, 'the worker to start');
            // What the master writes can no longer replace status.json.
            unlink($status);
            mkdir($status, 0700);
            usleep(2_500_000);

            $this->assertFileDoesNotExist($this->directory . "/signals/{$worker}.sigterm", 'the workers were drained');
            $this->assertSame(0, pcntl_waitpid($master, $exit, WNOHANG), 'the master stopped');
            rmdir($status);
            $this->waitFor(fn (): ?bool => is_file($status) ? true : null, 5.0, 'status.json to be written again');
        } finally {
            $this->reap($master);
        }
    }

    /** Run the master in a process of its own, against a broker that $handler answers for. */
    private function startMaster(callable $handler): int
    {
        $config = $this->config();
        $master = pcntl_fork();
        if ($master === -1) {
            $this->fail('Unable to fork.');
        }
        if ($master === 0) {
            try {
                (new PhpSupervisor(
                    $this->createStub(QueueManager::class),
                    $config,
                    queenFactory: fn (string $name, array $options): Queen => new Queen([
                        ...$options,
                        'handler' => HandlerStack::create($handler),
                    ]),
                ))->run();
            } catch (\Throwable $error) {
                fwrite(STDERR, $error . "\n");
            } finally {
                posix_kill(getmypid(), SIGKILL);
            }
        }

        return $master;
    }

    /** @return array<string, mixed> one pool of one worker, spawned from the stand-in */
    private function config(): array
    {
        $config = SupervisorConfiguration::resolve([
            'url' => 'http://queen-1.test:6632',
            'urls' => ['http://queen-1.test:6632', 'http://queen-2.test:6632', 'http://queen-3.test:6632'],
            'supervisor' => [
                'poll_interval' => 1,
                'http_timeout' => 5,
                'supervisors' => ['default' => ['queues' => ['high'], 'balance' => 'simple', 'processes' => 1]],
            ],
        ], dirname(__DIR__));
        $config['state_directory'] = $this->directory . '/state';
        $config['artisan'] = __DIR__ . '/Fixtures/SignalRecordingArtisan.php';
        $config['shutdown_grace'] = 5;

        return $config;
    }

    private function startedWorker(): ?int
    {
        foreach (glob($this->directory . '/signals/*.started') ?: [] as $file) {
            return (int) basename($file, '.started');
        }

        return null;
    }

    /**
     * @template T
     * @param \Closure(): (T|null) $probe
     * @return T
     */
    private function waitFor(\Closure $probe, float $seconds, string $what): mixed
    {
        $deadline = microtime(true) + $seconds;
        while (($value = $probe()) === null) {
            if (microtime(true) > $deadline) {
                $this->fail("Waited {$seconds} seconds for {$what}.");
            }
            usleep(10_000);
        }

        return $value;
    }

    /** The master still waits on the broker: it and any worker left go now. */
    private function reap(int $master): void
    {
        posix_kill($master, SIGKILL);
        pcntl_waitpid($master, $status);
        foreach (glob($this->directory . '/signals/*.started') ?: [] as $file) {
            $pid = (int) basename($file, '.started');
            if ($pid > 0 && !is_file($this->directory . "/signals/{$pid}.sigterm")) {
                @posix_kill($pid, SIGKILL);
            }
        }
    }

    private function removeTree(string $path): void
    {
        if (is_link($path) || is_file($path)) {
            @unlink($path);

            return;
        }
        foreach (glob($path . '/{,.}[!.,!..]*', GLOB_BRACE) ?: [] as $child) {
            $this->removeTree($child);
        }
        @rmdir($path);
    }
}

/**
 * A broker that answers the first request at once, then stops answering:
 * each later request fails after $seconds, as a connect timeout does, and
 * nothing interrupts the wait, as nothing interrupts libcurl's.
 */
final class BlackHoledAfterFirstAnswer
{
    private static int $requests = 0;

    public function __construct(private string $slowSince, private float $seconds)
    {
    }

    public function __invoke(RequestInterface $request, array $options): FulfilledPromise
    {
        if (self::$requests++ === 0) {
            return new FulfilledPromise(new Response(200, ['Content-Type' => 'application/json'], json_encode([
                'pending' => 0,
                'ready' => 0,
                'processing' => 0,
            ])));
        }
        if (!is_file($this->slowSince)) {
            file_put_contents($this->slowSince, (string) microtime(true));
        }
        $until = microtime(true) + $this->seconds;
        while (microtime(true) < $until) {
            usleep(10_000);
        }

        throw new ConnectException('Connection timed out', $request);
    }
}
