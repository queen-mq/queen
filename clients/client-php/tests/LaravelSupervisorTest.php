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
}
