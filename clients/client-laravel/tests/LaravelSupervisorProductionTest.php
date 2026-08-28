<?php

namespace Queen\Tests;

use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Psr7\Response;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\QueueManager;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\AutoScaler;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Laravel\Supervisor\TelemetryReader;
use Queen\Laravel\Supervisor\WorkerTelemetry;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;
use ReflectionMethod;
use ReflectionProperty;
use RuntimeException;

class LaravelSupervisorProductionTest extends TestCase
{
    /** @var list<string> */
    private array $temporaryDirectories = [];

    protected function tearDown(): void
    {
        foreach (array_reverse($this->temporaryDirectories) as $directory) {
            $this->removeDirectory($directory);
        }

        parent::tearDown();
    }

    public function testConfigurationRejectsAPoolAboveTheProcessLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);

        SupervisorConfiguration::resolve([
            'retry_after' => 90,
            'supervisor' => [
                'process_limit' => 4,
                'shutdown_grace' => 75,
                'supervisors' => [
                    'jobs' => ['max_processes' => 5, 'timeout' => 60],
                ],
            ],
        ], '/app');
    }

    public function testConfigurationRejectsAnUnsafeGlobalProcessLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);

        SupervisorConfiguration::resolve([
            'supervisor' => ['process_limit' => 4097],
        ], '/app');
    }

    public function testConfigurationRejectsAggregatePoolsAboveTheProcessLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);

        SupervisorConfiguration::resolve([
            'retry_after' => 90,
            'supervisor' => [
                'process_limit' => 5,
                'shutdown_grace' => 75,
                'supervisors' => [
                    'mail' => ['max_processes' => 3, 'timeout' => 60],
                    'billing' => ['max_processes' => 3, 'timeout' => 60],
                ],
            ],
        ], '/app');
    }

    public function testConfigurationRejectsShutdownGraceAtOrBelowWorkerTimeout(): void
    {
        $this->expectException(InvalidArgumentException::class);

        SupervisorConfiguration::resolve([
            'retry_after' => 90,
            'supervisor' => [
                'shutdown_grace' => 60,
                'supervisors' => [
                    'jobs' => ['timeout' => 60],
                ],
            ],
        ], '/app');
    }

    public function testConfigurationRejectsLeaseAtOrBelowWorkerTimeout(): void
    {
        $this->expectException(InvalidArgumentException::class);

        SupervisorConfiguration::resolve([
            'retry_after' => 60,
            'supervisor' => [
                'shutdown_grace' => 75,
                'supervisors' => [
                    'jobs' => ['timeout' => 60],
                ],
            ],
        ], '/app');
    }

    public function testConfigurationDerivesASafeShutdownGraceFromTheLargestTimeout(): void
    {
        $config = SupervisorConfiguration::resolve([
            'retry_after' => 180,
            'supervisor' => [
                'supervisors' => [
                    'fast' => ['timeout' => 30],
                    'slow' => ['timeout' => 120],
                ],
            ],
        ], '/app');

        $this->assertSame(135, $config['shutdown_grace']);
    }

    public function testAutoScalingKeepsEveryBusyQueueServiceableWhenCapacityAllows(): void
    {
        $desired = (new AutoScaler())->desired([
            'queues' => ['high', 'default', 'low'],
            'balance' => 'auto',
            'strategy' => 'size',
            'processes' => 10,
            'min_processes' => 1,
            'max_processes' => 10,
            // Three jobs require three workers, so allocation order must not
            // strand a busy queue when there is enough target capacity.
            'target_jobs_per_process' => 1,
            'target_clear_seconds' => 60.0,
            'default_runtime_seconds' => 1.0,
        ], [
            'high' => 1,
            'default' => 1,
            'low' => 1,
        ]);

        $this->assertSame(['high' => 1, 'default' => 1, 'low' => 1], $desired);
    }

    public function testAutoScalingFloorsASmallTargetAtTheBusyQueueCount(): void
    {
        $desired = (new AutoScaler())->desired([
            'queues' => ['high', 'default', 'low'],
            'balance' => 'auto',
            'strategy' => 'size',
            'processes' => 3,
            'min_processes' => 1,
            'max_processes' => 3,
            'target_jobs_per_process' => 100,
            'target_clear_seconds' => 60.0,
            'default_runtime_seconds' => 1.0,
        ], ['high' => 1, 'default' => 1, 'low' => 1]);

        $this->assertSame(['high' => 1, 'default' => 1, 'low' => 1], $desired);
    }

    public function testTelemetryReaderFiltersScopeWeightsEwmasAndDeletesStaleFiles(): void
    {
        $directory = $this->temporaryDirectory();
        $scope = [
            'supervisor' => 'orders',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ];

        $this->writeTelemetry($directory . '/matching-a.json', $scope, 2, 4.0);
        $this->writeTelemetry($directory . '/matching-b.json', $scope, 4, 8.0);
        $this->writeTelemetry($directory . '/other-supervisor.json', [
            ...$scope,
            'supervisor' => 'billing',
        ], 100, 500.0);
        $stale = $directory . '/stale.json';
        $this->writeTelemetry($stale, $scope, 100, 1_000.0);
        touch($stale, time() - 120);
        clearstatcache(true, $stale);

        $runtimes = (new TelemetryReader())->runtimes($directory, 60, $scope);

        $this->assertEqualsWithDelta(40 / 6, $runtimes['high'], 0.000001);
        $this->assertSame(['high'], array_keys($runtimes));
        $this->assertFileDoesNotExist($stale);
    }

    public function testWorkerTelemetryPublishesScopedEwma(): void
    {
        $directory = $this->temporaryDirectory();
        $telemetry = new WorkerTelemetry($directory, 'queen-eu', 'orders', 'orders-v1');
        $job = $this->createStub(Job::class);
        $job->method('getQueue')->willReturn('high');

        $this->setStartedAt($telemetry, $job, hrtime(true) - 1_000_000_000);
        $telemetry->finish('queen-eu', $job);
        $this->setStartedAt($telemetry, $job, hrtime(true) - 3_000_000_000);
        $this->setProperty($telemetry, 'lastPublish', 0.0);
        $telemetry->finish('queen-eu', $job, true);

        $document = json_decode(
            (string) file_get_contents($directory . '/' . getmypid() . '.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );

        $this->assertSame('orders', $document['supervisor']);
        $this->assertSame('queen-eu', $document['connection']);
        $this->assertSame('orders-v1', $document['consumer_group']);
        $this->assertSame(2, $document['queues']['high']['samples']);
        $this->assertSame(1, $document['queues']['high']['failures']);
        $this->assertEqualsWithDelta(1.4, $document['queues']['high']['runtime_ewma_seconds'], 0.05);
    }

    public function testStateLockIsExclusiveAndControlCommandsAreConsumedOnce(): void
    {
        $directory = $this->temporaryDirectory();
        $owner = new SupervisorState($directory);
        $observer = new SupervisorState($directory);
        $lock = $owner->acquireLock();

        try {
            $this->assertTrue($observer->isOwned());

            try {
                $observer->acquireLock();
                $this->fail('A second supervisor unexpectedly acquired the state lock.');
            } catch (RuntimeException) {
                $this->addToAssertionCount(1);
            }

            $owner->request('pause');
            $command = $owner->command(null);

            $this->assertSame('pause', $command['command']);
            $this->assertNull((new SupervisorState($directory))->command(null));
            $this->assertFileDoesNotExist($directory . '/control.json');
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        $this->assertFalse($observer->isOwned());
    }

    public function testPhpSupervisorBuildsPriorityWorkerWithScopedEnvironment(): void
    {
        $stateDirectory = $this->temporaryDirectory();
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
        $options = [
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
            'queues' => ['high', 'default'],
            'balance' => 'off',
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

        $method = new ReflectionMethod(PhpSupervisor::class, 'startWorker');
        $process = $method->invoke($supervisor, 'orders', 'high', $options);
        $this->assertSame(0, $process->wait());
        $document = json_decode(trim($process->getOutput()), true, 512, JSON_THROW_ON_ERROR);

        $this->assertSame('queue:work', $document['arguments'][0]);
        $this->assertSame('queen-eu', $document['arguments'][1]);
        $this->assertContains('--queue=high,default', $document['arguments']);
        $this->assertSame('orders-v1', $document['consumer_group']);
        $this->assertSame('queen-eu', $document['connection']);
        $this->assertSame('orders', $document['supervisor']);
        $this->assertSame('31', $document['retry_after']);
        $this->assertSame('0', $document['block_for']);
        $this->assertSame($stateDirectory . '/telemetry', $document['telemetry_directory']);
    }

    public function testPhpSupervisorUsesTheV2ReadOnlyConnectionInsteadOfTheWorkerClient(): void
    {
        $handler = new PlanHandler([], [
            'status' => 200,
            'json' => ['pending' => 99, 'effectivePending' => 3],
        ]);
        $queues = $this->createMock(QueueManager::class);
        $queues->expects($this->never())->method('connection');
        $captured = [];
        $supervisor = new PhpSupervisor(
            $queues,
            [
                'state_directory' => $this->temporaryDirectory(),
                'http_timeout' => 2,
                'connections' => [
                    'queen-eu' => [
                        'url' => 'https://queen-read.test',
                        'urls' => ['https://queen-read.test'],
                        'bearer_token' => 'read-secret',
                        'headers' => [
                            'X-Tenant' => 'eu',
                            'authorization' => 'Bearer worker-secret',
                        ],
                    ],
                ],
            ],
            queenFactory: function (string $name, array $options) use (&$captured, $handler): Queen {
                $captured[] = [$name, $options];
                return new Queen([...$options, 'handler' => $handler]);
            },
        );

        $depths = (new ReflectionMethod(PhpSupervisor::class, 'depths'))->invoke($supervisor, [
            'connection' => 'queen-eu',
            'consumer_group' => 'orders/v1',
            'queues' => ['high priority', 'default'],
        ]);

        $this->assertSame(['high priority' => 3, 'default' => 3], $depths);
        $this->assertCount(1, $captured);
        $this->assertSame('queen-eu', $captured[0][0]);
        $this->assertSame('read-secret', $captured[0][1]['bearerToken']);
        $this->assertSame('eu', $captured[0][1]['headers']['X-Tenant']);
        $this->assertSame(2000, $captured[0][1]['timeoutMillis']);
        $this->assertSame(1, $captured[0][1]['retryAttempts']);
        $this->assertSame(0, $captured[0][1]['retryDelayMillis']);
        $this->assertCount(2, $handler->requests);
        foreach ($handler->requests as $request) {
            $this->assertSame('Bearer read-secret', $request->getHeaderLine('Authorization'));
            $this->assertStringNotContainsString('worker-secret', $request->getHeaderLine('Authorization'));
            $this->assertSame('eu', $request->getHeaderLine('X-Tenant'));
        }
        $this->assertSame('/api/v1/resources/queues/high%20priority/depth', $handler->requests[0]->getUri()->getPath());
        $this->assertSame('group=orders%2Fv1', $handler->requests[0]->getUri()->getQuery());
    }

    public function testPhpSupervisorBoundsConcurrentDepthPolling(): void
    {
        $active = 0;
        $peak = 0;
        $requests = 0;
        $handler = function () use (&$active, &$peak, &$requests) {
            $active++;
            $requests++;
            $peak = max($peak, $active);
            $promise = null;
            $promise = new Promise(function () use (&$promise, &$active): void {
                $active--;
                $promise->resolve(new Response(200, ['Content-Type' => 'application/json'], '{"pending":1}'));
            });
            return $promise;
        };
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $this->temporaryDirectory(),
                'http_timeout' => 1,
                'connections' => [
                    'queen' => [
                        'url' => 'http://queen.test',
                        'urls' => ['http://queen.test'],
                        'bearer_token' => null,
                        'headers' => [],
                    ],
                ],
            ],
            queenFactory: fn (string $_name, array $options): Queen => new Queen([...$options, 'handler' => $handler]),
        );
        $queueNames = array_map(fn (int $index): string => "queue-{$index}", range(1, 37));

        $depths = (new ReflectionMethod(PhpSupervisor::class, 'depths'))->invoke($supervisor, [
            'connection' => 'queen',
            'consumer_group' => 'workers',
            'queues' => $queueNames,
        ]);

        $this->assertCount(37, $depths);
        $this->assertSame(37, $requests);
        $this->assertSame(16, $peak);
        $this->assertSame(0, $active);
    }

    public function testPhpSupervisorRejectsNonIntegerDepthCounters(): void
    {
        $handler = new PlanHandler([], [
            'status' => 200,
            'json' => ['effectivePending' => '3'],
        ]);
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $this->temporaryDirectory(),
                'http_timeout' => 1,
                'connections' => [
                    'queen' => [
                        'url' => 'http://queen.test',
                        'urls' => ['http://queen.test'],
                        'bearer_token' => null,
                        'headers' => [],
                    ],
                ],
            ],
            queenFactory: fn (string $_name, array $options): Queen => new Queen([...$options, 'handler' => $handler]),
        );

        $this->expectException(\UnexpectedValueException::class);
        (new ReflectionMethod(PhpSupervisor::class, 'depths'))->invoke($supervisor, [
            'connection' => 'queen',
            'consumer_group' => 'workers',
            'queues' => ['default'],
        ]);
    }

    public function testPhpSupervisorDoesNotFallBackToWorkerCredentialsForAMalformedV2Contract(): void
    {
        $queues = $this->createMock(QueueManager::class);
        $queues->expects($this->never())->method('connection');
        $supervisor = new PhpSupervisor($queues, [
            'version' => SupervisorConfiguration::VERSION,
            'state_directory' => $this->temporaryDirectory(),
            'http_timeout' => 1,
            'connections' => [],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('missing from the resolved contract');
        (new ReflectionMethod(PhpSupervisor::class, 'depths'))->invoke($supervisor, [
            'connection' => 'queen',
            'consumer_group' => 'workers',
            'queues' => ['default'],
        ]);
    }

    public function testDownscaleMustRemainStableBeforeWorkersAreRemoved(): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $this->temporaryDirectory()],
        );
        $this->setProperty($supervisor, 'processes', [
            'orders' => [
                'high' => [new \Symfony\Component\Process\Process(['true'])],
                'default' => [new \Symfony\Component\Process\Process(['true'])],
            ],
        ]);
        $method = new ReflectionMethod(PhpSupervisor::class, 'stabilizeDownscale');
        $options = ['queues' => ['high', 'default'], 'scale_down_delay' => 10];

        $this->assertSame(
            ['high' => 1, 'default' => 1],
            $method->invoke($supervisor, 'orders', $options, ['high' => 0, 'default' => 0]),
        );
        $this->setProperty($supervisor, 'downscaleCandidates', [
            'orders' => ['target' => 0, 'since' => microtime(true) - 11],
        ]);
        $this->assertSame(
            ['high' => 0, 'default' => 0],
            $method->invoke($supervisor, 'orders', $options, ['high' => 0, 'default' => 0]),
        );
    }

    public function testRepeatedWorkerCrashesReceiveCappedExponentialBackoff(): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $this->temporaryDirectory()],
        );
        $method = new ReflectionMethod(PhpSupervisor::class, 'registerCrash');
        $options = ['restart_backoff' => 2, 'restart_backoff_max' => 8, 'stable_after' => 60];

        $before = microtime(true);
        $method->invoke($supervisor, 'orders', 'high', $options, 'test');
        $first = $this->property($supervisor, 'restartAfter')['orders:high'];
        $method->invoke($supervisor, 'orders', 'high', $options, 'test');
        $second = $this->property($supervisor, 'restartAfter')['orders:high'];

        $this->assertEqualsWithDelta($before + 2, $first, 0.1);
        $this->assertEqualsWithDelta($before + 4, $second, 0.1);
    }

    private function temporaryDirectory(): string
    {
        $directory = sys_get_temp_dir() . '/queen-supervisor-production-' . bin2hex(random_bytes(8));
        if (!mkdir($directory, 0700, true) && !is_dir($directory)) {
            throw new RuntimeException("Unable to create test directory [{$directory}].");
        }
        $this->temporaryDirectories[] = $directory;

        return $directory;
    }

    private function writeTelemetry(string $path, array $scope, int $samples, float $ewma): void
    {
        file_put_contents($path, json_encode([
            ...$scope,
            'queues' => [
                'high' => [
                    'samples' => $samples,
                    'runtime_ewma_seconds' => $ewma,
                    'failures' => 0,
                ],
            ],
        ], JSON_THROW_ON_ERROR));
    }

    private function setStartedAt(WorkerTelemetry $telemetry, Job $job, int $startedAt): void
    {
        $this->setProperty($telemetry, 'started', [spl_object_id($job) => $startedAt]);
    }

    private function setProperty(object $object, string $property, mixed $value): void
    {
        $reflection = new ReflectionProperty($object, $property);
        $reflection->setValue($object, $value);
    }

    private function property(object $object, string $property): mixed
    {
        return (new ReflectionProperty($object, $property))->getValue($object);
    }

    private function removeDirectory(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($directory, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );
        foreach ($iterator as $item) {
            $item->isDir() ? rmdir($item->getPathname()) : unlink($item->getPathname());
        }
        rmdir($directory);
    }
}
