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

    public function testConfigurationEnforcesRustQueueAndDurationBounds(): void
    {
        $queues = array_map(static fn (int $index): string => "queue-{$index}", range(1, 1025));
        try {
            SupervisorConfiguration::resolve([
                'supervisor' => [
                    'supervisors' => [
                        'jobs' => ['balance' => 'off', 'queues' => $queues],
                    ],
                ],
            ], '/app');
            $this->fail('The PHP resolver accepted more queues than the Rust engine supports.');
        } catch (InvalidArgumentException $error) {
            $this->assertStringContainsString('1024 queues', $error->getMessage());
        }

        try {
            SupervisorConfiguration::resolve([
                'supervisor' => ['poll_interval' => 31536001],
            ], '/app');
            $this->fail('The PHP resolver accepted a timing value rejected by Rust.');
        } catch (InvalidArgumentException $error) {
            $this->assertStringContainsString('31536000 seconds', $error->getMessage());
        }
    }

    public function testConfigurationRejectsRootStateDirectoryAliasesLikeRust(): void
    {
        foreach (['/', '/./', '////./'] as $directory) {
            try {
                SupervisorConfiguration::stateDirectory($directory, '/app');
                $this->fail("The PHP resolver accepted root alias [{$directory}].");
            } catch (InvalidArgumentException $error) {
                $this->assertStringContainsString('non-root path', $error->getMessage());
            }

            try {
                new SupervisorState($directory);
                $this->fail("SupervisorState accepted filesystem root alias [{$directory}].");
            } catch (RuntimeException $error) {
                $this->assertStringContainsString('filesystem root', $error->getMessage());
            }
        }

        if (DIRECTORY_SEPARATOR === '/') {
            foreach (['C:\\queen-state', '\\\\server\\share\\queen-state'] as $directory) {
                try {
                    SupervisorConfiguration::stateDirectory($directory, '/app');
                    $this->fail("The Unix resolver accepted foreign absolute path [{$directory}].");
                } catch (InvalidArgumentException $error) {
                    $this->assertStringContainsString('absolute Unix path', $error->getMessage());
                }
            }
        }
    }

    public function testStateAcquisitionDoesNotRepairAnExistingSharedDirectory(): void
    {
        $parent = $this->temporaryDirectory();
        $directory = $parent . '/tmp-like';
        $this->assertTrue(mkdir($directory, 0700));
        $this->assertTrue(chmod($directory, 0777));
        clearstatcache(true, $directory);
        $modeBefore = fileperms($directory) & 07777;

        try {
            (new SupervisorState($directory))->acquireLock();
            $this->fail('A shared pre-existing state directory was accepted.');
        } catch (RuntimeException $error) {
            $this->assertStringContainsString('must use mode 0700', $error->getMessage());
        }

        clearstatcache(true, $directory);
        $this->assertSame($modeBefore, fileperms($directory) & 07777);
        $this->assertFileDoesNotExist($directory . '/control.lock');
        $this->assertFileDoesNotExist($directory . '/supervisor.lock');
    }

    public function testStateAcquisitionRejectsAWritableNonStickyAncestorBeforePublishingLocks(): void
    {
        $root = $this->temporaryDirectory();
        $unsafeParent = $root . '/shared';
        $stateDirectory = $unsafeParent . '/state';
        $this->assertTrue(mkdir($unsafeParent, 0700));
        $this->assertTrue(chmod($unsafeParent, 0777));

        try {
            (new SupervisorState($stateDirectory))->acquireLock();
            $this->fail('A state path below a writable non-sticky ancestor was accepted.');
        } catch (RuntimeException $error) {
            $this->assertStringContainsString('group/world-writable', $error->getMessage());
        }

        $this->assertDirectoryDoesNotExist($stateDirectory);
        $this->assertFileDoesNotExist($stateDirectory . '/control.lock');
        $this->assertFileDoesNotExist($stateDirectory . '/supervisor.lock');
    }

    public function testStateAcquisitionAcceptsAPrivateChildBelowATrustedStickyAncestor(): void
    {
        $root = $this->temporaryDirectory();
        $stickyParent = $root . '/sticky';
        $stateDirectory = $stickyParent . '/state';
        $this->assertTrue(mkdir($stickyParent, 0700));
        $this->assertTrue(chmod($stickyParent, 01777));

        $lock = (new SupervisorState($stateDirectory))->acquireLock();
        $this->assertDirectoryExists($stateDirectory);
        $this->assertSame(0700, fileperms($stateDirectory) & 07777);
        $this->assertFileExists($stateDirectory . '/control.lock');
        $this->assertFileExists($stateDirectory . '/supervisor.lock');
        fclose($lock);
    }

    public function testAcquiredGenerationFailsClosedAfterStateDirectoryReplacement(): void
    {
        $stateDirectory = $this->temporaryDirectory();
        $state = new SupervisorState($stateDirectory);
        $lock = $state->acquireLock();
        $previousGeneration = $stateDirectory . '-previous';
        $this->assertTrue(rename($stateDirectory, $previousGeneration));
        $this->temporaryDirectories[] = $previousGeneration;
        $this->assertTrue(mkdir($stateDirectory, 0700));

        try {
            $state->writeStatus(['state' => 'running']);
            $this->fail('An acquired generation wrote through a replaced state directory path.');
        } catch (RuntimeException $error) {
            $this->assertStringContainsString('changed after generation acquisition', $error->getMessage());
        } finally {
            fclose($lock);
        }

        $this->assertFileDoesNotExist($stateDirectory . '/status.json');
        $this->assertFileExists($previousGeneration . '/supervisor.lock');
    }

    public function testStateAcquisitionValidatesASymlinkTargetChainBeforeCanonicalizingIt(): void
    {
        $root = $this->temporaryDirectory();
        $unsafeParent = $root . '/shared';
        $target = $unsafeParent . '/target';
        $alias = $root . '/alias';
        $this->assertTrue(mkdir($unsafeParent, 0700));
        $this->assertTrue(mkdir($target, 0700));
        $this->assertTrue(chmod($unsafeParent, 0777));
        $this->assertTrue(symlink($target, $alias));

        try {
            new SupervisorState($alias . '/state');
            $this->fail('A symlink through a writable non-sticky target ancestor was accepted.');
        } catch (RuntimeException $error) {
            $this->assertStringContainsString('group/world-writable', $error->getMessage());
        }

        $this->assertDirectoryDoesNotExist($target . '/state');
        $this->assertTrue(unlink($alias));
    }

    public function testStateAcquisitionCanonicalizesATrustedSymlinkAncestorOnce(): void
    {
        $root = $this->temporaryDirectory();
        $target = $root . '/target';
        $alias = $root . '/alias';
        $this->assertTrue(mkdir($target, 0700));
        $this->assertTrue(symlink($target, $alias));

        $lock = (new SupervisorState($alias . '/state'))->acquireLock();
        $this->assertFileExists($target . '/state/supervisor.lock');
        $this->assertFileDoesNotExist($alias . '/supervisor.lock');
        fclose($lock);
        $this->assertTrue(unlink($alias));
    }

    public function testConfigurationCapsTheAggregateStatusPoolCardinality(): void
    {
        $queues = array_map(
            static fn (int $index): string => "queue-{$index}",
            range(1, SupervisorConfiguration::MAX_STATUS_POOLS),
        );
        $resolved = SupervisorConfiguration::resolve([
            'supervisor' => [
                'supervisors' => [
                    'jobs' => ['balance' => 'off', 'queues' => $queues],
                ],
            ],
        ], '/app');
        $this->assertCount(SupervisorConfiguration::MAX_STATUS_POOLS, $resolved['supervisors']['jobs']['queues']);

        $queues[] = 'one-too-many';
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('256 aggregate status pools');
        SupervisorConfiguration::resolve([
            'supervisor' => [
                'supervisors' => [
                    'jobs' => ['balance' => 'off', 'queues' => $queues],
                ],
            ],
        ], '/app');
    }

    public function testConfigurationBoundsStatusIdentityFields(): void
    {
        foreach ([
            [str_repeat('s', 129), ['default']],
            ['jobs', [str_repeat('q', 257)]],
        ] as [$name, $queues]) {
            try {
                SupervisorConfiguration::resolve([
                    'supervisor' => [
                        'supervisors' => [
                            $name => ['balance' => 'off', 'queues' => $queues],
                        ],
                    ],
                ], '/app');
                $this->fail('The PHP resolver accepted a status identity above its byte bound.');
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testConfigurationRejectsRootAndParentTraversalStatePaths(): void
    {
        foreach (['/', '../queen-state', '/srv/app/../queen-state'] as $path) {
            try {
                SupervisorConfiguration::resolve([
                    'supervisor' => ['state_directory' => $path],
                ], '/app');
                $this->fail("The PHP resolver accepted unsafe state path [{$path}].");
            } catch (InvalidArgumentException $error) {
                $this->assertStringContainsString('absolute, non-root path', $error->getMessage());
            }
        }
    }

    public function testConfigurationRequiresControlTtlAboveTheBoundedDepthLoop(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('must exceed the bounded depth/control loop budget');

        SupervisorConfiguration::resolve([
            'supervisor' => [
                'http_timeout' => 10,
                'control_ttl' => 30,
                'supervisors' => [
                    'jobs' => [
                        'balance' => 'off',
                        'queues' => array_map(
                            static fn (int $index): string => "queue-{$index}",
                            range(1, 64),
                        ),
                    ],
                ],
            ],
        ], '/app');
    }

    public function testConfigurationRequiresHeartbeatTimeoutAboveTheBoundedControlLoop(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('heartbeat_timeout [60]');

        SupervisorConfiguration::resolve([
            'supervisor' => [
                'heartbeat_timeout' => 60,
                'supervisors' => [
                    'jobs' => ['max_processes' => 10],
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

    public function testConfigurationRejectsPrefetchWithoutLeaseRenewal(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('connection prefetch [16] requires lease_renewal');

        SupervisorConfiguration::resolve([
            'supervisor' => [
                'supervisors' => [
                    'jobs' => ['timeout' => 10, 'retry_after' => 160],
                ],
            ],
        ], '/app', queueConnections: [
            'queen' => ['driver' => 'queen', 'prefetch' => 16],
        ]);
    }

    public function testEvenALongLeaseAndRestCannotMakeUnrenewedPrefetchPauseSafe(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('connection prefetch [16] requires lease_renewal');

        SupervisorConfiguration::resolve([
            'supervisor' => [
                'supervisors' => [
                    'jobs' => ['timeout' => 10, 'retry_after' => 3600, 'rest' => 60],
                ],
            ],
        ], '/app', queueConnections: [
            'queen' => ['driver' => 'queen', 'prefetch' => 16],
        ]);
    }

    public function testLeaseRenewalReplacesThePrefetchTimesTimeoutBound(): void
    {
        $config = SupervisorConfiguration::resolve([
            'supervisor' => [
                'supervisors' => [
                    'jobs' => ['timeout' => 60, 'retry_after' => 90],
                ],
            ],
        ], '/app', queueConnections: [
            'queen' => [
                'driver' => 'queen',
                'prefetch' => 16,
                'lease_renewal' => true,
                'lease_renewal_interval' => 30,
                'lease_renewal_timeout' => 5,
            ],
        ]);

        $this->assertTrue($config['supervisors']['jobs']['lease_renewal']);
        $this->assertSame(90, $config['supervisors']['jobs']['retry_after']);
    }

    public function testLeaseRenewalUsesTheDefaultIntervalForNullAndEmptyConfiguration(): void
    {
        foreach ([null, ''] as $interval) {
            $config = SupervisorConfiguration::resolve([
                'supervisor' => [
                    'supervisors' => [
                        'jobs' => ['timeout' => 60, 'retry_after' => 90],
                    ],
                ],
            ], '/app', queueConnections: [
                'queen' => [
                    'driver' => 'queen',
                    'prefetch' => 16,
                    'lease_renewal' => true,
                    'lease_renewal_interval' => $interval,
                ],
            ]);

            $this->assertTrue($config['supervisors']['jobs']['lease_renewal']);
            $this->assertSame(90, $config['supervisors']['jobs']['retry_after']);
        }
    }

    public function testConfigurationRejectsUnsafeLeaseRenewalTimingAcrossAllBackends(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('lease renewal timing budget');

        SupervisorConfiguration::resolve([
            'supervisor' => [
                'supervisors' => [
                    'jobs' => ['timeout' => 30, 'retry_after' => 90],
                ],
            ],
        ], '/app', queueConnections: [
            'queen' => [
                'driver' => 'queen',
                'prefetch' => 16,
                'urls' => [
                    'https://queen-a.test',
                    'https://queen-b.test',
                    'https://queen-c.test',
                ],
                'lease_renewal' => true,
                'lease_renewal_interval' => 60,
                'lease_renewal_timeout' => 5,
            ],
        ]);
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

    public function testTelemetryReaderProgressivelyRecoversFromPidFileChurn(): void
    {
        $directory = $this->temporaryDirectory();
        for ($index = 0; $index < 8192; $index++) {
            $path = $directory . '/' . sprintf('%05d.json', $index);
            file_put_contents($path, '{}');
            chmod($path, 0600);
        }
        $sample = $directory . '/latest.json';
        $scope = [
            'supervisor' => 'orders',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ];
        $this->writeTelemetry($sample, $scope, 1, 4.0);
        touch($sample, time() + 1);

        $reader = new TelemetryReader();
        // The first bounded pass fails closed, but still discards enough old
        // PID files that a subsequent poll can recover.
        $this->assertSame([], $reader->runtimes($directory, 60, $scope));
        $this->assertSame(['high' => 4.0], $reader->runtimes($directory, 60, $scope));
        $this->assertLessThanOrEqual(4096, iterator_count(new \FilesystemIterator($directory)));
    }

    public function testTimeTelemetryKeepsADeadWorkersFinalSampleUntilOneScan(): void
    {
        $stateDirectory = $this->temporaryDirectory();
        $state = new SupervisorState($stateDirectory);
        $telemetryDirectory = $state->telemetryDirectory();
        $scope = [
            'supervisor' => 'orders',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ];
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $stateDirectory, 'telemetry_ttl' => 60],
        );
        $schedule = new ReflectionMethod(PhpSupervisor::class, 'scheduleTelemetryCleanup');
        $observe = new ReflectionMethod(PhpSupervisor::class, 'observedRuntimes');
        $options = [...$scope, 'strategy' => 'time', 'balance' => 'auto'];

        $finalSample = $telemetryDirectory . '/12345.json';
        $this->writeTelemetry($finalSample, $scope, 1, 4.0);
        $schedule->invoke($supervisor, 'orders', $options, 12345);
        $this->assertFileExists($finalSample);
        $this->assertSame(['high' => 4.0], $observe->invoke($supervisor, 'orders', $options));
        $this->assertFileDoesNotExist($finalSample);

        $simpleSample = $telemetryDirectory . '/12346.json';
        $this->writeTelemetry($simpleSample, $scope, 1, 8.0);
        $schedule->invoke($supervisor, 'orders', [...$options, 'balance' => 'simple'], 12346);
        $this->assertFileDoesNotExist($simpleSample);
    }

    public function testBrokenTelemetryScanStillBoundsPendingPidCleanup(): void
    {
        $stateDirectory = $this->temporaryDirectory();
        file_put_contents($stateDirectory . '/telemetry', 'not-a-directory');
        chmod($stateDirectory . '/telemetry', 0600);
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $stateDirectory, 'telemetry_ttl' => 60],
        );
        $this->setProperty($supervisor, 'pendingTelemetryCleanup', [
            'orders' => array_fill_keys(range(10000, 10100), true),
        ]);
        $options = [
            'strategy' => 'time',
            'balance' => 'auto',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ];

        try {
            (new ReflectionMethod(PhpSupervisor::class, 'observedRuntimes'))
                ->invoke($supervisor, 'orders', $options);
            $this->fail('A non-directory telemetry path was accepted.');
        } catch (RuntimeException) {
            $this->addToAssertionCount(1);
        }

        $this->assertSame([], $this->property($supervisor, 'pendingTelemetryCleanup'));
    }

    public function testPhpSupervisorOnlyReadsRuntimeTelemetryForTimeStrategy(): void
    {
        $stateDirectory = $this->temporaryDirectory();
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $stateDirectory,
                'telemetry_ttl' => 60,
            ],
        );
        $options = [
            'strategy' => 'size',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ];

        $method = new ReflectionMethod(PhpSupervisor::class, 'observedRuntimes');
        $this->assertSame([], $method->invoke($supervisor, 'orders', $options));
        $this->assertDirectoryDoesNotExist($stateDirectory . '/telemetry');

        $telemetryDirectory = (new SupervisorState($stateDirectory))->telemetryDirectory();
        $this->writeTelemetry($telemetryDirectory . '/matching.json', [
            'supervisor' => 'orders',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ], 2, 4.0);
        $options['strategy'] = 'time';

        $this->assertSame(['high' => 4.0], $method->invoke($supervisor, 'orders', $options));

        $options['balance'] = 'simple';
        $this->assertSame([], $method->invoke($supervisor, 'orders', $options));
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
        $this->assertSame(0600, fileperms($directory . '/' . getmypid() . '.json') & 0777);
    }

    public function testTelemetryReaderRejectsNonPrivateAndOversizedFiles(): void
    {
        $directory = $this->temporaryDirectory();
        $scope = [
            'supervisor' => 'orders',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ];
        $path = $directory . '/unsafe.json';
        $this->writeTelemetry($path, $scope, 1, 2.0);
        chmod($path, 0644);

        $this->assertSame([], (new TelemetryReader())->runtimes($directory, 60, $scope));

        unlink($path);
        file_put_contents($path, str_repeat('x', 65537));
        chmod($path, 0600);
        $this->assertSame([], (new TelemetryReader())->runtimes($directory, 60, $scope));
    }

    public function testTelemetryReaderRejectsDocumentsWithTooManyQueues(): void
    {
        $directory = $this->temporaryDirectory();
        $queues = [];
        foreach (range(1, 257) as $index) {
            $queues["queue-{$index}"] = ['samples' => 1, 'runtime_ewma_seconds' => 1.0];
        }
        $path = $directory . '/too-many-queues.json';
        file_put_contents($path, json_encode(['queues' => $queues], JSON_THROW_ON_ERROR));
        chmod($path, 0600);

        $this->assertSame([], (new TelemetryReader())->runtimes($directory, 60));
    }

    public function testTelemetryReaderSkipsOverflowingWeightedEwmas(): void
    {
        $directory = $this->temporaryDirectory();
        $scope = [
            'supervisor' => 'orders',
            'connection' => 'queen-eu',
            'consumer_group' => 'orders-v1',
        ];
        $this->writeTelemetry($directory . '/huge.json', $scope, 100, 1.0e308);

        $this->assertSame([], (new TelemetryReader())->runtimes($directory, 60, $scope));
    }

    public function testStateLockIsExclusiveAndControlCommandsAreConsumedOnce(): void
    {
        $directory = $this->temporaryDirectory();
        $owner = new SupervisorState($directory);
        $observer = new SupervisorState($directory);
        $lock = $owner->acquireLock();

        try {
            $owner->writeStatus(['engine' => 'php', 'state' => 'running', 'pools' => [], 'pool_status' => []]);
            $instanceId = $owner->instanceId();
            $this->assertTrue($observer->isOwned());

            try {
                $observer->acquireLock();
                $this->fail('A second supervisor unexpectedly acquired the state lock.');
            } catch (RuntimeException) {
                $this->addToAssertionCount(1);
            }

            $owner->request('pause', $instanceId);
            $command = $owner->command(null, $instanceId);

            $this->assertSame('pause', $command['command']);
            $this->assertNull((new SupervisorState($directory))->command(null, $instanceId));
            $this->assertFileDoesNotExist($directory . '/control.json');
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        $this->assertFalse($observer->isOwned());
    }

    public function testPendingControlCommandCannotBeOverwritten(): void
    {
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();
        $state->writeStatus(['engine' => 'php', 'state' => 'running', 'pools' => [], 'pool_status' => []]);
        $instanceId = $state->instanceId();
        $state->request('pause', $instanceId);
        $pending = file_get_contents($directory . '/control.json');
        $pendingDocument = json_decode($pending, true, 8, JSON_THROW_ON_ERROR);
        $this->assertSame(3600, $pendingDocument['expires_at_epoch'] - $pendingDocument['requested_at_epoch']);

        try {
            $state->request('terminate', $instanceId);
            $this->fail('A pending command was overwritten.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('already pending', $exception->getMessage());
        }
        $this->assertSame($pending, file_get_contents($directory . '/control.json'));

        $pause = $state->command(null, $instanceId);
        $this->assertSame('pause', $pause['command']);
        $state->request('terminate', $instanceId);
        $this->assertSame('terminate', $state->command($pause['nonce'], $instanceId)['command']);
        flock($lock, LOCK_UN);
        fclose($lock);
    }

    public function testGenerationTimingIsUsedAndMalformedSnapshotsFailClosed(): void
    {
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();

        try {
            $state->writeStatus([
                'engine' => 'php',
                'state' => 'running',
                'pools' => [],
                'pool_status' => [],
                'configuration' => [
                    'heartbeat_timeout' => 120,
                    'control_ttl' => 47,
                ],
            ]);
            $status = $state->status();
            $this->assertTrue($state->isLive($status));
            $state->request('pause', $state->instanceId());
            $command = $state->command(null, $state->instanceId());
            $this->assertSame(
                47,
                ($command['expires_at_epoch'] ?? 0) - ($command['requested_at_epoch'] ?? 0),
            );

            $state->writeStatus([
                'engine' => 'php',
                'state' => 'running',
                'pools' => [],
                'pool_status' => [],
                'configuration' => [
                    'heartbeat_timeout' => '120',
                    'control_ttl' => 47,
                ],
            ]);
            $malformed = $state->status();
            $this->assertFalse($state->isLive($malformed));
            $this->assertFalse($state->isLive($malformed, 120));
            try {
                $state->request('terminate', $state->instanceId(), 120, 47);
                $this->fail('Malformed generation timing accepted a control request.');
            } catch (RuntimeException $error) {
                $this->assertStringContainsString('missing, stale', $error->getMessage());
            }
            $this->assertFileDoesNotExist($directory . '/control.json');
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testMalformedControlDoesNotPoisonThePhpSupervisorInbox(): void
    {
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();
        $state->writeStatus(['engine' => 'php', 'state' => 'running', 'pools' => [], 'pool_status' => []]);
        $instanceId = $state->instanceId();
        file_put_contents($directory . '/control.json', '{malformed');
        chmod($directory . '/control.json', 0600);

        try {
            $this->assertNull($state->command(null, $instanceId));
            $this->assertFileDoesNotExist($directory . '/control.json');
            $state->request('pause', $instanceId);
            $this->assertSame('pause', $state->command(null, $instanceId)['command']);
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testStatusV1IsNormalizedAndCarriesSafeDepthSnapshots(): void
    {
        $directory = $this->temporaryDirectory();
        $supervisor = new PhpSupervisor($this->createStub(QueueManager::class), [
            'state_directory' => $directory,
            'supervisors' => [
                'orders' => ['queues' => ['high', 'default']],
            ],
        ]);
        $stateProperty = new ReflectionProperty($supervisor, 'state');
        /** @var SupervisorState $state */
        $state = $stateProperty->getValue($supervisor);
        $lock = $state->acquireLock();

        try {
            $this->setProperty($supervisor, 'lastDesired', ['orders' => ['high' => 2, 'default' => 1]]);
            $this->setProperty($supervisor, 'lastDepths', ['orders' => ['high' => 7, 'default' => 0]]);
            $this->setProperty($supervisor, 'depthsAvailable', ['orders' => true]);
            (new ReflectionMethod($supervisor, 'writeStatus'))->invoke($supervisor, 'running');
            $status = $state->status();

            $this->assertSame(SupervisorState::STATUS_SCHEMA, $status['schema']);
            $this->assertSame('php', $status['engine']);
            $this->assertSame($state->instanceId(), $status['instance_id']);
            $this->assertIsInt($status['updated_at_epoch']);
            $this->assertFalse($status['paused']);
            $this->assertFalse($status['stopping']);
            $this->assertSame(['high', 'default'], array_column($status['pool_status'], 'queue'));
            $this->assertSame(2, $status['pool_status'][0]['desired']);
            $this->assertSame(0, $status['pool_status'][0]['running']);
            $this->assertSame(7, $status['pool_status'][0]['depth']);
            $this->assertTrue($status['pool_status'][0]['depth_available']);
            $this->assertSame('closed', $status['pool_status'][0]['restart_state']);
            $this->assertTrue($status['pool_status'][0]['healthy']);
            $this->assertSame(0, $status['pools']['orders']['high']['processes']);
            $this->assertSame(3600, $status['configuration']['control_ttl']);
            $this->assertSame('orders', $status['configuration']['supervisors'][0]['name']);
            $this->assertSame('queen', $status['configuration']['supervisors'][0]['connection']);
            $this->assertSame('laravel', $status['configuration']['supervisors'][0]['consumer_group']);
            $this->assertSame(['high', 'default'], $status['configuration']['supervisors'][0]['queues']);
            $this->assertArrayNotHasKey('queen', $status['configuration']);
            $this->assertArrayNotHasKey('connections', $status['configuration']);
            $this->assertTrue($state->isLive($status));
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testMaximumStatusPoolContractFitsBelowOneMebibyte(): void
    {
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();
        $pools = [];
        $poolStatus = [];
        $configuredSupervisors = [];
        $pid = 1000;
        foreach (range(1, SupervisorConfiguration::MAX_STATUS_POOLS) as $index) {
            $supervisor = str_pad("s{$index}", 128, 's');
            $queue = str_pad("q{$index}", 256, 'q');
            $pids = range($pid, $pid + 15);
            $pid += 16;
            $drainingPids = range($pid, $pid + 15);
            $pid += 16;
            $pools[$supervisor][$queue] = [
                'processes' => 16,
                'pids' => $pids,
                'desired' => 16,
                'draining' => 16,
                'restart_state' => 'backoff',
                'restart_failures' => 4,
                'restart_in_seconds' => 31536000,
                'depth' => PHP_INT_MAX,
                'depth_available' => true,
            ];
            $poolStatus[] = [
                'supervisor' => $supervisor,
                'queue' => $queue,
                'desired' => 16,
                'running' => 16,
                'draining' => 16,
                'pids' => $pids,
                'draining_pids' => $drainingPids,
                'restart_state' => 'backoff',
                'restart_failures' => 4,
                'restart_in_seconds' => 31536000,
                'healthy' => false,
                'depth' => PHP_INT_MAX,
                'depth_available' => true,
            ];
            $configuredSupervisors[] = [
                'name' => $supervisor,
                'connection' => str_repeat('c', 128),
                'consumer_group' => str_repeat('g', 128),
                'queues' => [$queue],
                'balance' => 'off',
                'strategy' => 'time',
                'processes' => 16,
                'min_processes' => 0,
                'max_processes' => 16,
                'timeout' => 31535999,
                'retry_after' => 31536000,
                'tries' => PHP_INT_MAX,
                'memory' => PHP_INT_MAX,
            ];
        }

        try {
            $state->writeStatus([
                'engine' => 'php',
                'state' => 'running',
                'draining' => 4096,
                'pools' => $pools,
                'pool_status' => $poolStatus,
                'configuration' => [
                    'poll_interval' => 31536000,
                    'http_timeout' => 31536000,
                    'control_ttl' => 86400,
                    'heartbeat_timeout' => 86400,
                    'shutdown_grace' => 31536000,
                    'telemetry_ttl' => 31536000,
                    'process_limit' => 4096,
                    'supervisors' => $configuredSupervisors,
                ],
            ]);

            $this->assertLessThan(1048576, filesize($directory . '/status.json'));
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testPhpPauseImmediatelyInvalidatesDepthSnapshotsLikeRust(): void
    {
        $directory = $this->temporaryDirectory();
        $supervisor = new PhpSupervisor($this->createStub(QueueManager::class), [
            'state_directory' => $directory,
            'supervisors' => [
                'orders' => ['queues' => ['high']],
            ],
        ]);
        $stateProperty = new ReflectionProperty($supervisor, 'state');
        /** @var SupervisorState $state */
        $state = $stateProperty->getValue($supervisor);
        $lock = $state->acquireLock();

        try {
            $this->setProperty($supervisor, 'lastDepths', ['orders' => ['high' => 7]]);
            $this->setProperty($supervisor, 'depthsAvailable', ['orders' => true]);
            (new ReflectionMethod($supervisor, 'pause'))->invoke($supervisor);
            (new ReflectionMethod($supervisor, 'writeStatus'))->invoke($supervisor, 'paused');

            $status = $state->status();
            $this->assertSame('paused', $status['state']);
            $this->assertNull($status['pool_status'][0]['depth']);
            $this->assertFalse($status['pool_status'][0]['depth_available']);
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testPhpPauseDrainsPrefetchingWorkersAndFencesControlsDuringShutdown(): void
    {
        $directory = $this->temporaryDirectory();
        $options = ['queues' => ['high'], 'strategy' => 'size', 'balance' => 'auto'];
        $supervisor = new PhpSupervisor($this->createStub(QueueManager::class), [
            'state_directory' => $directory,
            'shutdown_grace' => 30,
            'supervisors' => ['orders' => $options],
        ]);
        $process = $this->getMockBuilder(\Symfony\Component\Process\Process::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isRunning', 'signal', 'getPid'])
            ->getMock();
        $process->expects($this->once())->method('isRunning')->willReturn(true);
        $process->expects($this->once())->method('signal')->with(SIGTERM)->willReturnSelf();
        $process->method('getPid')->willReturn(12345);
        $this->setProperty($supervisor, 'processes', ['orders' => ['high' => [$process]]]);
        $this->setProperty($supervisor, 'workerPids', [spl_object_id($process) => 12345]);

        $state = $this->property($supervisor, 'state');
        $lock = $state->acquireLock();
        try {
            (new ReflectionMethod($supervisor, 'pause'))->invoke($supervisor);
            (new ReflectionMethod($supervisor, 'writeStatus'))->invoke($supervisor, 'paused');

            $status = $state->status();
            $this->assertSame(0, $status['pool_status'][0]['running']);
            $this->assertSame(1, $status['pool_status'][0]['draining']);
            $this->assertSame([], $this->property($supervisor, 'processes')['orders']['high']);
            $this->assertCount(1, $this->property($supervisor, 'draining'));
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testPhpOncePublishesTerminatingBeforeDrainAndRejectsNewControls(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !extension_loaded('pcntl')) {
            $this->markTestSkipped('Unix process signals are required.');
        }
        $directory = $this->temporaryDirectory();
        $supervisor = new PhpSupervisor($this->createStub(QueueManager::class), [
            'state_directory' => $directory,
            'poll_interval' => 1,
            'shutdown_grace' => 0,
            'telemetry_ttl' => 60,
            'supervisors' => [],
        ]);
        /** @var SupervisorState $state */
        $state = $this->property($supervisor, 'state');
        $process = $this->getMockBuilder(\Symfony\Component\Process\Process::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isRunning', 'getPid'])
            ->getMock();
        $process->expects($this->once())->method('isRunning')->willReturnCallback(
            function () use ($state): bool {
                $status = $state->status();
                $this->assertSame('terminating', $status['state']);
                $this->assertTrue($status['stopping']);
                $this->assertFalse($state->isLive($status));
                try {
                    $state->request('pause', $status['instance_id']);
                    $this->fail('A control command was accepted after shutdown began.');
                } catch (RuntimeException $error) {
                    $this->assertStringContainsString('stopping', $error->getMessage());
                }

                return false;
            },
        );
        $process->method('getPid')->willReturn(null);
        $this->setProperty($supervisor, 'processes', ['orders' => ['high' => [$process]]]);

        $supervisor->run(true);
        $this->assertSame('stopped', $state->status()['state']);
    }

    public function testStatusReadsAreReadOnlyAndLivenessRejectsFutureOrReleasedOwners(): void
    {
        $missing = sys_get_temp_dir() . '/queen-supervisor-missing-' . bin2hex(random_bytes(8));
        $reader = new SupervisorState($missing);
        $this->assertNull($reader->status());
        $this->assertFalse($reader->isOwned());
        $this->assertDirectoryDoesNotExist($missing);

        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();
        $state->writeStatus(['engine' => 'php', 'state' => 'running', 'pools' => [], 'pool_status' => []]);
        $status = $state->status();
        $this->assertTrue($state->isLive($status));
        try {
            $state->request('pause', 'replaced-instance');
            $this->fail('A stale supervisor generation was accepted.');
        } catch (RuntimeException $error) {
            $this->assertStringContainsString('stale', $error->getMessage());
        }
        $this->assertFileDoesNotExist($directory . '/control.json');

        $status['updated_at_epoch'] = time() + 60;
        file_put_contents($directory . '/status.json', json_encode($status, JSON_THROW_ON_ERROR));
        chmod($directory . '/status.json', 0600);
        $this->assertFalse($state->isLive($state->status()));

        $state->writeStatus(['engine' => 'php', 'state' => 'running', 'pools' => [], 'pool_status' => []]);
        $live = $state->status();
        flock($lock, LOCK_UN);
        fclose($lock);
        $this->assertFalse($state->isLive($live));
    }

    public function testStateReadersRejectSymlinksAndInsecureModesWithoutRepairingThem(): void
    {
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $state->writeStatus(['engine' => 'php', 'state' => 'running', 'pools' => [], 'pool_status' => []]);
        chmod($directory . '/status.json', 0644);
        try {
            $state->status();
            $this->fail('An insecure status mode was accepted.');
        } catch (RuntimeException $error) {
            $this->assertStringContainsString('bounded regular file', $error->getMessage());
        }
        clearstatcache(true, $directory . '/status.json');
        $this->assertSame(0644, fileperms($directory . '/status.json') & 0777);

        chmod($directory . '/status.json', 0600);
        rename($directory . '/status.json', $directory . '/target.json');
        symlink($directory . '/target.json', $directory . '/status.json');
        try {
            $state->status();
            $this->fail('A symbolic-link status document was accepted.');
        } catch (RuntimeException $error) {
            $this->assertStringContainsString('bounded regular file', $error->getMessage());
        }
        $this->assertTrue(is_link($directory . '/status.json'));
    }

    public function testControlAndStatusReadersRejectUnsafeOrOversizedFiles(): void
    {
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        file_put_contents($directory . '/control.json', str_repeat('x', 16385));

        $this->assertNull($state->command(null, 'instance-a'));
        $this->assertFileDoesNotExist($directory . '/control.json');
        file_put_contents($directory . '/status.json', str_repeat('x', 1048577));
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('bounded regular file');
        $state->status();
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
            'strategy' => 'time',
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
        $this->assertSame(realpath($stateDirectory) . '/telemetry', $document['telemetry_directory']);
    }

    public function testPhpSupervisorDoesNotExposeTelemetryToSizeOrFixedSimpleWorkers(): void
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
            'balance' => 'simple',
            'strategy' => 'time',
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

        $previous = getenv('QUEEN_SUPERVISOR_TELEMETRY_DIR');
        putenv('QUEEN_SUPERVISOR_TELEMETRY_DIR=/tmp/inherited-queen-telemetry');
        try {
            $method = new ReflectionMethod(PhpSupervisor::class, 'startWorker');
            $process = $method->invoke($supervisor, 'orders', 'high', $options);
            $this->assertSame(0, $process->wait());
        } finally {
            putenv(is_string($previous)
                ? 'QUEEN_SUPERVISOR_TELEMETRY_DIR=' . $previous
                : 'QUEEN_SUPERVISOR_TELEMETRY_DIR');
        }
        $document = json_decode(trim($process->getOutput()), true, 512, JSON_THROW_ON_ERROR);

        $this->assertFalse($document['telemetry_directory']);
        $this->assertDirectoryDoesNotExist($stateDirectory . '/telemetry');
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

    public function testReconcileBudgetImmediatelyEstablishesBaselineCapacity(): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $this->temporaryDirectory()],
        );
        $method = new ReflectionMethod(PhpSupervisor::class, 'reconcileBudget');
        $simple = [
            'balance' => 'simple',
            'processes' => 6,
            'min_processes' => 2,
            'balance_max_shift' => 1,
        ];
        $auto = [...$simple, 'balance' => 'auto'];

        $this->assertSame(6, $method->invoke($supervisor, $simple, 0));
        $this->assertSame(3, $method->invoke($supervisor, $simple, 3));
        $this->assertSame(1, $method->invoke($supervisor, $simple, 6));
        $this->assertSame(2, $method->invoke($supervisor, $auto, 0));
        $this->assertSame(1, $method->invoke($supervisor, $auto, 2));
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
        $first = array_values($this->property($supervisor, 'restartAfter'))[0];
        $method->invoke($supervisor, 'orders', 'high', $options, 'test');
        $second = array_values($this->property($supervisor, 'restartAfter'))[0];

        $this->assertEqualsWithDelta($before + 2, $first, 0.1);
        $this->assertEqualsWithDelta($before + 4, $second, 0.1);
    }

    public function testRestartBackoffKeysCannotCollideOnColons(): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $this->temporaryDirectory()],
        );
        $method = new ReflectionMethod(PhpSupervisor::class, 'registerCrash');
        $options = ['restart_backoff' => 1, 'restart_backoff_max' => 8, 'stable_after' => 60];

        $method->invoke($supervisor, 'a', 'b:c', $options, 'test');
        $method->invoke($supervisor, 'a:b', 'c', $options, 'test');

        $this->assertCount(2, $this->property($supervisor, 'restartAfter'));
        $this->assertCount(2, $this->property($supervisor, 'crashCount'));
    }

    public function testDrainingWorkersRemainInsideTheGlobalProcessLimit(): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $this->temporaryDirectory(), 'process_limit' => 2],
        );
        $this->setProperty($supervisor, 'processes', [
            'orders' => ['high' => [new \Symfony\Component\Process\Process(['true'])]],
        ]);
        $this->setProperty($supervisor, 'draining', [[
            'process' => new \Symfony\Component\Process\Process(['true']),
            'deadline' => microtime(true) + 30,
            'label' => 'orders:low',
            'supervisor' => 'orders',
            'queue' => 'low',
        ]]);
        $method = new ReflectionMethod(PhpSupervisor::class, 'remainingProcessSlots');

        $this->assertSame(0, $method->invoke($supervisor));
        $this->setProperty($supervisor, 'draining', []);
        $this->assertSame(1, $method->invoke($supervisor));
    }

    public function testForcedDrainRemainsTrackedUntilTheWorkerActuallyExits(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !defined('SIGKILL')) {
            $this->markTestSkipped('Unix process signals are required.');
        }
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $this->temporaryDirectory(), 'process_limit' => 1],
        );
        $process = new \Symfony\Component\Process\Process(['/bin/sh', '-c', 'sleep 30']);
        $process->start();
        $this->setProperty($supervisor, 'draining', [[
            'process' => $process,
            'deadline' => microtime(true) - 1,
            'label' => 'orders:high',
            'supervisor' => 'orders',
            'queue' => 'high',
        ]]);
        $reap = new ReflectionMethod(PhpSupervisor::class, 'reapDraining');
        $slots = new ReflectionMethod(PhpSupervisor::class, 'remainingProcessSlots');

        try {
            $reap->invoke($supervisor);
            $this->assertCount(1, $this->property($supervisor, 'draining'));
            $this->assertSame(0, $slots->invoke($supervisor));

            $deadline = microtime(true) + 2;
            while ($process->isRunning() && microtime(true) < $deadline) {
                usleep(10_000);
            }
            $reap->invoke($supervisor);
            $this->assertCount(0, $this->property($supervisor, 'draining'));
            $this->assertSame(1, $slots->invoke($supervisor));
        } finally {
            if ($process->isRunning()) {
                $process->stop(0, SIGKILL);
            }
        }
    }

    public function testShutdownObservesWorkerDeathAfterForcedKillBeforeClearingTracking(): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $this->temporaryDirectory(), 'shutdown_grace' => 0],
        );
        $process = $this->getMockBuilder(\Symfony\Component\Process\Process::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isRunning', 'signal'])
            ->getMock();
        $process->expects($this->exactly(3))
            ->method('isRunning')
            ->willReturnOnConsecutiveCalls(true, true, false);
        $signals = [];
        $process->expects($this->exactly(2))
            ->method('signal')
            ->willReturnCallback(function (int $signal) use (&$signals, $process) {
                $signals[] = $signal;

                return $process;
            });
        $this->setProperty($supervisor, 'processes', ['orders' => ['high' => [$process]]]);

        (new ReflectionMethod(PhpSupervisor::class, 'shutdown'))->invoke($supervisor);

        $this->assertSame([SIGTERM, SIGKILL], $signals);
        $this->assertSame([], $this->property($supervisor, 'processes'));
    }

    public function testTelemetryCleanupCannotAbortShutdownOwnershipFencing(): void
    {
        $directory = $this->temporaryDirectory();
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            ['state_directory' => $directory, 'shutdown_grace' => 0],
            output: static fn (): null => null,
        );
        $process = $this->getMockBuilder(\Symfony\Component\Process\Process::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isRunning', 'getPid'])
            ->getMock();
        $process->expects($this->once())->method('isRunning')->willReturn(false);
        $process->method('getPid')->willReturn(12345);
        $this->setProperty($supervisor, 'processes', ['orders' => ['high' => [$process]]]);
        $this->setProperty($supervisor, 'workerPids', [spl_object_id($process) => 12345]);
        $telemetry = (new SupervisorState($directory))->telemetryDirectory();
        file_put_contents($telemetry . '/12345.json', '{}');
        chmod($telemetry . '/12345.json', 0600);
        chmod($telemetry, 0755);

        (new ReflectionMethod(PhpSupervisor::class, 'shutdown'))->invoke($supervisor);

        $this->assertSame([], $this->property($supervisor, 'processes'));
        $this->assertFileExists($telemetry . '/12345.json');
        chmod($telemetry, 0700);
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
        chmod($path, 0600);
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
