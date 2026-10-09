<?php

namespace Queen\Tests;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\AutoScaler;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Laravel\Supervisor\TelemetryReader;

class LaravelSupervisorTest extends TestCase
{
    public function testConfigurationResolvesToTheSharedVersionedContract(): void
    {
        $config = SupervisorConfiguration::resolve([
            'url' => 'http://queen.test:6632',
            'bearer_token' => 'secret',
            'consumer_group' => 'workers',
            'supervisor' => [
                'poll_interval' => 5,
                'supervisors' => [
                    'jobs' => [
                        'queues' => 'high, default',
                        'min_processes' => 2,
                        'max_processes' => 12,
                    ],
                ],
            ],
        ], '/app', '/usr/bin/php');

        $this->assertSame(2, $config['version']);
        $this->assertSame('/usr/bin/php', $config['php_binary']);
        $this->assertSame('/app/artisan', $config['artisan']);
        $this->assertSame('/app/storage/queen-supervisor', $config['state_directory']);
        $this->assertSame(5, $config['poll_interval']);
        $this->assertSame(3600, $config['control_ttl']);
        $this->assertSame(76, $config['heartbeat_timeout']);
        $this->assertSame('http://queen.test:6632', $config['queen']['url']);
        $this->assertSame('secret', $config['connections']['queen']['bearer_token']);
        $this->assertSame(['high', 'default'], $config['supervisors']['jobs']['queues']);
        $this->assertSame('workers', $config['supervisors']['jobs']['consumer_group']);
        $this->assertSame(90, $config['supervisors']['jobs']['retry_after']);
        $this->assertTrue($config['supervisors']['jobs']['quiet']);
    }

    public function testConfigurationRejectsAnUnknownBalanceStrategy(): void
    {
        $this->expectException(InvalidArgumentException::class);

        SupervisorConfiguration::resolve([
            'supervisor' => ['supervisors' => ['jobs' => ['balance' => 'magic']]],
        ], '/app');
    }

    public function testConfigurationExportsTheBrokerUsedByEachLaravelConnection(): void
    {
        $config = SupervisorConfiguration::resolve([
            'supervisor' => [
                'supervisors' => [
                    'orders' => ['connection' => 'queen-eu', 'timeout' => 30],
                ],
            ],
        ], '/app', queueConnections: [
            'queen-eu' => [
                'driver' => 'queen',
                'urls' => ['https://queen-a.test', 'https://queen-b.test/'],
                'bearer_token' => 'eu-secret',
                'headers' => ['X-Tenant' => 'eu'],
                'retry_after' => 120,
            ],
        ]);

        $this->assertSame(
            ['https://queen-a.test', 'https://queen-b.test'],
            $config['connections']['queen-eu']['urls'],
        );
        $this->assertSame('eu-secret', $config['connections']['queen-eu']['bearer_token']);
        $this->assertSame(120, $config['supervisors']['orders']['retry_after']);
    }

    public function testAnotherQueenConnectionStartsFromConfigQueenLikeItsWorkers(): void
    {
        // QueenConnector starts every queen connection from config/queen.php,
        // so the supervisor checks the settings its workers will run with.
        $config = SupervisorConfiguration::resolve([
            'retry_after' => 300,
            'lease_renewal' => true,
            'supervisor' => [
                'supervisors' => ['reports' => ['connection' => 'queen-auto', 'timeout' => 30]],
            ],
        ], '/app', queueConnections: [
            'queen-auto' => ['driver' => 'queen', 'prefetch' => 'auto'],
        ]);

        $this->assertSame(300, $config['supervisors']['reports']['retry_after']);
        $this->assertTrue($config['supervisors']['reports']['lease_renewal']);
    }

    public function testReadOnlyBearerTokenReplacesAWorkerAuthorizationHeader(): void
    {
        $config = SupervisorConfiguration::resolve([
            'supervisor' => [
                'read_bearer_token' => 'read-secret',
                'supervisors' => ['jobs' => []],
            ],
        ], '/app', queueConnections: [
            'queen' => [
                'driver' => 'queen',
                'url' => 'https://queen.test',
                'bearer_token' => 'worker-secret',
                'headers' => [
                    'authorization' => 'Bearer worker-secret',
                    'X-Tenant' => 'one',
                ],
            ],
        ]);

        $this->assertSame('read-secret', $config['connections']['queen']['bearer_token']);
        $this->assertArrayNotHasKey('authorization', $config['connections']['queen']['headers']);
        $this->assertSame('one', $config['connections']['queen']['headers']['X-Tenant']);
    }

    public function testConfigurationRejectsAnUnsafeReadOnlyBearerToken(): void
    {
        $this->expectException(InvalidArgumentException::class);

        SupervisorConfiguration::resolve([
            'supervisor' => [
                'read_bearer_token' => "read\r\nInjected: yes",
                'supervisors' => ['jobs' => []],
            ],
        ], '/app');
    }

    public function testConfigurationRejectsUnsafeDepthEndpointsAndHeaders(): void
    {
        foreach ([
            ['url' => 'https://user:secret@queen.test'],
            ['url' => 'https://queen.test?target=elsewhere'],
            ['url' => 'https://queen.test#fragment'],
            ['url' => 'https://queen.test', 'headers' => ['Bad Header' => 'value']],
            ['url' => 'https://queen.test', 'headers' => ['Bad,Header' => 'value']],
            ['url' => 'https://queen.test', 'headers' => ['X-Queen' => "ok\r\nInjected: yes"]],
            ['url' => 'https://queen.test', 'headers' => ['X-Queen' => "ok\x01unsafe"]],
            ['url' => 'https://queen.test', 'bearer_token' => 'token with spaces'],
        ] as $connection) {
            try {
                SupervisorConfiguration::resolve([
                    'supervisor' => ['supervisors' => ['jobs' => []]],
                ], '/app', queueConnections: [
                    'queen' => array_replace(['driver' => 'queen'], $connection),
                ]);
                $this->fail('Unsafe Queen supervisor connection configuration was accepted.');
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testConfigurationRejectsMalformedQueueAndBooleanOptions(): void
    {
        foreach ([
            ['queues' => ['default', 12]],
            ['queues' => ['   ']],
            ['force' => 'false'],
            ['quiet' => 1],
        ] as $options) {
            try {
                SupervisorConfiguration::resolve([
                    'supervisor' => ['supervisors' => ['jobs' => $options]],
                ], '/app');
                $this->fail('Malformed Queen supervisor configuration was accepted.');
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testConfigurationRejectsABalanceShiftBeyondThePoolBound(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('balance_max_shift must not exceed max_processes [10]');

        SupervisorConfiguration::resolve([
            'supervisor' => [
                'process_limit' => 256,
                'supervisors' => [
                    'jobs' => [
                        'max_processes' => 10,
                        'balance_max_shift' => 11,
                    ],
                ],
            ],
        ], '/app');
    }

    public function testAutoScalingAllocatesProcessesTowardTheBusyQueue(): void
    {
        $desired = (new AutoScaler())->desired($this->options(), [
            'high' => 95,
            'default' => 5,
        ]);

        $this->assertSame(10, array_sum($desired));
        $this->assertGreaterThan($desired['default'], $desired['high']);
    }

    public function testAutoScalingFallsBackToTheMinimumWhenIdle(): void
    {
        $desired = (new AutoScaler())->desired($this->options(), ['high' => 0, 'default' => 0]);

        $this->assertSame(2, array_sum($desired));
    }

    public function testSimpleBalancingSpreadsAFixedPoolEvenly(): void
    {
        $options = array_replace($this->options(), ['balance' => 'simple', 'processes' => 6]);

        $this->assertSame(
            ['high' => 3, 'default' => 3],
            (new AutoScaler())->desired($options, ['high' => 100, 'default' => 0]),
        );
    }

    public function testOffBalancingPreservesQueuePriority(): void
    {
        $options = array_replace($this->options(), ['balance' => 'off']);

        $this->assertSame(
            ['high' => 10, 'default' => 0],
            (new AutoScaler())->desired($options, ['high' => 100, 'default' => 100]),
        );
    }

    public function testTimeScalingUsesObservedRuntimeToMeetTheClearanceTarget(): void
    {
        $options = array_replace($this->options(), [
            'strategy' => 'time',
            'target_clear_seconds' => 60.0,
            'default_runtime_seconds' => 1.0,
        ]);

        $desired = (new AutoScaler())->desired($options, ['high' => 30, 'default' => 30], [
            'high' => 10.0,
            'default' => 2.0,
        ]);

        $this->assertSame(6, array_sum($desired));
        $this->assertGreaterThan($desired['default'], $desired['high']);
    }

    public function testTimeScalingSaturatesAtMaximumForNonFiniteAggregatePressure(): void
    {
        $options = array_replace($this->options(), [
            'strategy' => 'time',
            'target_clear_seconds' => 1.0,
            'default_runtime_seconds' => 1.0,
            'max_processes' => 10,
        ]);

        $desired = (new AutoScaler())->desired(
            $options,
            ['high' => PHP_INT_MAX, 'default' => PHP_INT_MAX],
            ['high' => 1.0e308, 'default' => 1.0e308],
        );

        $this->assertSame(10, array_sum($desired));
    }

    public function testConfigurationRejectsScalingDurationsThatCanOverflowTheSharedContract(): void
    {
        foreach ([
            ['target_clear_seconds' => 1.0e-308],
            ['default_runtime_seconds' => 1.0e308],
        ] as $policy) {
            try {
                SupervisorConfiguration::resolve([
                    'supervisor' => ['supervisors' => ['jobs' => $policy]],
                ], '/app');
                $this->fail('An unsafe scaling duration was accepted.');
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testStateCommandsAndTelemetryUseAtomicLocalFiles(): void
    {
        $directory = sys_get_temp_dir() . '/queen-supervisor-test-' . bin2hex(random_bytes(6));
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();
        $state->writeStatus(['engine' => 'php', 'state' => 'running', 'pools' => [], 'pool_status' => []]);
        $first = json_decode(file_get_contents($directory . '/status.json'), true, 512, JSON_THROW_ON_ERROR);
        $this->assertGreaterThan(0, $first['started_at_epoch']);
        $this->assertLessThanOrEqual($first['updated_at_epoch'], $first['started_at_epoch']);
        $this->assertIsInt($first['uptime_seconds']);
        $this->assertGreaterThanOrEqual(0, $first['uptime_seconds']);
        $this->assertSame(\Queen\Laravel\Supervisor\SupervisorMetadata::clientVersion(), $first['client_version']);
        $this->assertSame($first['client_version'], $first['engine_version']);
        $state->writeStatus(['engine' => 'php', 'state' => 'paused', 'pools' => [], 'pool_status' => []]);
        $next = json_decode(file_get_contents($directory . '/status.json'), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame($first['started_at_epoch'], $next['started_at_epoch']);
        $this->assertSame($first['instance_id'], $next['instance_id']);
        $this->assertGreaterThanOrEqual($first['uptime_seconds'], $next['uptime_seconds']);
        $instanceId = $state->instanceId();
        $state->request('pause', $instanceId);
        $command = $state->command(null, $instanceId);
        $this->assertSame('pause', $command['command']);
        $this->assertNull($state->command($command['nonce'], $instanceId));

        $telemetry = $state->telemetryDirectory();
        $telemetryFile = $telemetry . '/1.json';
        file_put_contents($telemetryFile, json_encode([
            'queues' => ['high' => ['samples' => 2, 'runtime_ewma_seconds' => 4.0]],
        ]));
        chmod($telemetryFile, 0600);
        $this->assertSame(['high' => 4.0], (new TelemetryReader())->runtimes($telemetry, 60));

        flock($lock, LOCK_UN);
        fclose($lock);
        foreach (glob($telemetry . '/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($telemetry);
        foreach (glob($directory . '/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($directory);
    }

    private function options(): array
    {
        return [
            'queues' => ['high', 'default'],
            'balance' => 'auto',
            'strategy' => 'size',
            'processes' => 10,
            'min_processes' => 2,
            'max_processes' => 10,
            'target_jobs_per_process' => 10,
            'target_clear_seconds' => 60.0,
            'default_runtime_seconds' => 1.0,
        ];
    }

    /**
     * The watcher parks on POST /api/v1/fetch with the connection's exported
     * token, which read_bearer_token replaces with a read-only one: the
     * broker refused it and the master polled for the whole run, in silence.
     */
    public function testEventDrivenRefusesAReadOnlyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('event_driven needs a token that may consume');

        SupervisorConfiguration::resolve([
            'url' => 'http://queen.test:6632',
            'bearer_token' => 'workers',
            'supervisor' => [
                'event_driven' => true,
                'read_bearer_token' => 'read-only',
                'supervisors' => ['jobs' => ['queues' => 'default', 'balance' => 'simple', 'processes' => 1]],
            ],
        ], '/app');
    }

    /** The string form was trimmed; an array of names was exported as given. */
    public function testQueueNamesGivenAsAnArrayAreTrimmedLikeTheStringForm(): void
    {
        $config = SupervisorConfiguration::resolve([
            'url' => 'http://queen.test:6632',
            'supervisor' => ['supervisors' => ['jobs' => ['queues' => ['  high ', 'high', 'default '], 'balance' => 'off', 'processes' => 1]]],
        ], '/app');

        $this->assertSame(['high', 'default'], $config['supervisors']['jobs']['queues']);
    }

    /**
     * config/queen.php's urls, a cluster, replaced the single url of a
     * connection on another broker, so its pool was watched on the wrong one.
     */
    public function testAConnectionsOwnUrlIsNotReplacedByTheDefaultUrls(): void
    {
        $config = SupervisorConfiguration::resolve([
            'url' => 'http://a:6632',
            'urls' => ['http://a:6632', 'http://b:6632'],
            'supervisor' => ['supervisors' => ['orders' => ['connection' => 'orders', 'queues' => 'orders', 'balance' => 'simple', 'processes' => 1]]],
        ], '/app', null, ['orders' => ['driver' => 'queen', 'url' => 'http://orders:6632']]);

        $this->assertSame(['http://orders:6632'], $config['connections']['orders']['urls']);
        $this->assertSame(['orders'], array_keys($config['connections']));
    }
}
