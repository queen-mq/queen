<?php

namespace Queen\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\CallQueuedHandler;
use Illuminate\Queue\InteractsWithQueue;
use Orchestra\Testbench\TestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Psr\Http\Message\RequestInterface;
use Queen\Laravel\QueenServiceProvider;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * Laravel's timeout handler fails a job that outlives its timeout when it
 * fails on timeout, or on its last try, then kills the worker. The job's
 * dead-letter ACK runs inside that handler, from SIGALRM: Laravel's own
 * worker, in a process of its own, since Laravel kills it.
 */
final class LaravelTimeoutDeadLetterTest extends TestCase
{
    private TimeoutBroker $broker;

    private string $scratch;

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $this->scratch = sys_get_temp_dir() . '/queen-timeout-dlq-' . bin2hex(random_bytes(6));
        mkdir($this->scratch, 0700);
        $this->broker = new TimeoutBroker($this->scratch . '/broker.log');
        // The ordinary client as a connection has it by default: three
        // tries, 30 seconds each.
        $app['config']->set('queue.default', 'queen');
        $app['config']->set('queue.connections.queen', [
            'driver' => 'queen',
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($this->broker),
            'queue' => 'default',
            'consumer_group' => 'workers',
            'block_for' => 0,
        ]);
        $app['config']->set('queen.sync_failed_jobs', false);
        $app['config']->set('queen.job_metrics.enabled', false);
        $app['config']->set('queue.failed.driver', 'null');
    }

    protected function tearDown(): void
    {
        foreach (glob($this->scratch . '/*') ?: [] as $path) {
            @unlink($path);
        }
        @rmdir($this->scratch);

        parent::tearDown();
    }

    /**
     * The ACK went through the ordinary client, three tries of up to 30
     * seconds each; when it threw, the exception left the signal handler
     * into the job's code, so JobTimedOut and the kill never came: the job
     * ran on past its timeout, and a second worker got it after retry_after.
     * Inside the handler the ACK is one short request, and nothing it does
     * prevents the kill: if it fails, the lease expires.
     */
    #[TestWith(['503'])]
    #[TestWith(['hang'])]
    public function testAnAckThatFailsInLaravelsTimeoutHandlerNeverPreventsTheKill(string $ack): void
    {
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            $this->markTestSkipped('Needs ext-pcntl and ext-posix.');
        }
        $this->broker->ack = $ack;
        $returned = $this->scratch . '/returned';
        $started = microtime(true);

        $pid = pcntl_fork();
        if ($pid === -1) {
            $this->fail('Unable to fork.');
        }
        if ($pid === 0) {
            try {
                $this->app->make(Kernel::class)->handle(new ArgvInput([
                    'artisan', 'queue:work', 'queen', '--queue=default', '--sleep=0', '--tries=3',
                    '--timeout=60', '--memory=4096', '--stop-when-empty',
                ]), new BufferedOutput());
                touch($returned);
            } finally {
                posix_kill(getmypid(), SIGKILL);
            }
        }
        $status = $this->waitFor($pid, 15.0);
        $elapsed = microtime(true) - $started;

        $this->assertTrue(pcntl_wifsignaled($status) && pcntl_wtermsig($status) === SIGKILL);
        $this->assertFileDoesNotExist($returned, 'the job ran on past its timeout');
        $this->assertLessThan(6.0, $elapsed, 'killed within a few seconds of the timeout');
        $acks = array_values(array_filter(
            TimeoutBroker::logged($this->scratch . '/broker.log'),
            static fn (array $request): bool => $request['path'] === '/api/v1/ack',
        ));
        $this->assertCount(1, $acks, 'one request, no retry chain');
        $this->assertSame('dlq', $acks[0]['body']['status']);
        $this->assertLessThanOrEqual(2.0, $acks[0]['timeout']);
    }

    /** The status of $pid, SIGKILLed if it outlives $seconds. */
    private function waitFor(int $pid, float $seconds): int
    {
        $deadline = microtime(true) + $seconds;
        while (pcntl_waitpid($pid, $status, WNOHANG) === 0) {
            if (microtime(true) > $deadline) {
                posix_kill($pid, SIGKILL);
                pcntl_waitpid($pid, $status);
                $this->fail("The worker was still running {$seconds} seconds later.");
            }
            usleep(20_000);
        }

        return $status;
    }
}

/** Fails on timeout, after one second. */
final class OutlivesItsTimeoutJob implements ShouldQueue
{
    use InteractsWithQueue;
    use Queueable;

    public $timeout = 1;

    public $failOnTimeout = true;

    public function handle(): void
    {
        $until = microtime(true) + 10;
        while (microtime(true) < $until) {
            usleep(50_000);
        }
    }
}

/**
 * The broker of a forked worker: one delivery of OutlivesItsTimeoutJob, then
 * none; an ACK answered with a 503, or not at all until the client's
 * timeout. Each request is appended to a log the parent reads.
 */
final class TimeoutBroker
{
    public string $ack = '503';

    private bool $delivered = false;

    public function __construct(private string $log)
    {
    }

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $path = $request->getUri()->getPath();
        file_put_contents($this->log, json_encode([
            'path' => $path,
            'body' => json_decode((string) $request->getBody(), true),
            'timeout' => $options['timeout'] ?? null,
        ]) . "\n", FILE_APPEND | LOCK_EX);

        if (str_starts_with($path, '/api/v1/pop/')) {
            if ($this->delivered) {
                return new FulfilledPromise(new Response(204));
            }
            $this->delivered = true;

            return self::json(200, ['success' => true, 'messages' => [self::delivery()]]);
        }
        if ($path === '/api/v1/ack') {
            if ($this->ack === 'hang') {
                usleep((int) (($options['timeout'] ?? 30) * 1_000_000));
                throw new ConnectException('Operation timed out', $request);
            }

            return self::json(503, ['error' => 'unavailable']);
        }

        return self::json(200, ['success' => true]);
    }

    /** @return list<array{path: string, body: mixed, timeout: float|int|null}> */
    public static function logged(string $log): array
    {
        $lines = is_file($log) ? file($log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) : [];

        return array_map(static fn (string $line): array => json_decode($line, true, 512, JSON_THROW_ON_ERROR), $lines ?: []);
    }

    private static function delivery(): array
    {
        $command = new OutlivesItsTimeoutJob();

        return [
            'id' => 'message-1',
            'transactionId' => 'transaction-1',
            'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
            'partition' => 'laravel-0001',
            'leaseId' => 'lease-1',
            'consumerGroup' => 'workers',
            'deliveryAttempt' => 1,
            'data' => [
                'uuid' => 'job-1',
                'displayName' => OutlivesItsTimeoutJob::class,
                'job' => CallQueuedHandler::class . '@call',
                'maxTries' => null,
                'maxExceptions' => null,
                'failOnTimeout' => true,
                'backoff' => null,
                'timeout' => 1,
                'retryUntil' => null,
                'data' => [
                    'commandName' => OutlivesItsTimeoutJob::class,
                    'command' => serialize($command),
                ],
                'createdAt' => time(),
                '_queen' => ['partition' => 'laravel-0001', 'attempts' => 0],
            ],
        ];
    }

    private static function json(int $status, array $body): PromiseInterface
    {
        return new FulfilledPromise(new Response($status, ['Content-Type' => 'application/json'], json_encode($body)));
    }
}
