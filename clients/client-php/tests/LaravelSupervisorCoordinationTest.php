<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Queue\QueueManager;
use InvalidArgumentException;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\ReplicaCoordinator;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;
use ReflectionMethod;
use ReflectionProperty;
use Symfony\Component\Console\Output\BufferedOutput;

final class LaravelSupervisorCoordinationTest extends TestCase
{
    private const OTHER = '000000000000000018da146e6dc7d0d900000001';

    private ?string $stateDirectory = null;

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function tearDown(): void
    {
        if ($this->stateDirectory !== null && is_dir($this->stateDirectory)) {
            foreach (glob($this->stateDirectory . '/*') ?: [] as $path) {
                is_dir($path) ? rmdir($path) : unlink($path);
            }
            rmdir($this->stateDirectory);
        }

        parent::tearDown();
    }

    public function testDisabledCoordinationKeepsTheEngineContractUnchanged(): void
    {
        $withoutSetting = $this->queen([]);
        unset($withoutSetting['supervisor']['coordination']);

        $config = SupervisorConfiguration::resolve($this->queen(['enabled' => false]), '/app');

        $this->assertArrayNotHasKey('coordination', $config);
        $this->assertSame(SupervisorConfiguration::resolve($withoutSetting, '/app'), $config);
    }

    public function testEnabledCoordinationExportsItsWriteConnectionAndTheHeartbeatAsTtl(): void
    {
        $config = SupervisorConfiguration::resolve($this->queen([]), '/app');

        $this->assertSame([
            'connection' => [
                'url' => 'http://queen.test:6632',
                'urls' => ['http://queen.test:6632'],
                'bearer_token' => 'write-token',
                'headers' => [],
            ],
            'namespace' => 'queen-supervisor',
            'ttl' => $config['heartbeat_timeout'],
        ], $config['coordination']);
        // Depth polling keeps the read-only token; registering is a write.
        $this->assertSame('read-token', $config['connections']['queen']['bearer_token']);
    }

    public function testAnExplicitlyLongHeartbeatDoesNotKeepACrashedReplicaCounted(): void
    {
        $queen = $this->queen([]);
        $queen['supervisor']['heartbeat_timeout'] = 600;

        $config = SupervisorConfiguration::resolve($queen, '/app');

        // The loop bound, plus one: poll 5 + depth 5 + ten process starts 50
        // + margin 5 + coordination 5.
        $this->assertSame(71, $config['coordination']['ttl']);
        $this->assertSame(600, $config['heartbeat_timeout']);
    }

    public function testEveryCoordinationCallIsPartOfTheHeartbeat(): void
    {
        $disabled = SupervisorConfiguration::resolve($this->queen(['enabled' => false]), '/app');
        $enabled = SupervisorConfiguration::resolve($this->queen([]), '/app');
        $this->assertSame($disabled['heartbeat_timeout'] + 5, $enabled['heartbeat_timeout']);

        // Five autoscaling pools need two calls; a fixed pool needs none.
        $supervisors = [];
        foreach (range(1, 5) as $index) {
            $supervisors["pool-{$index}"] = ['queues' => ["queue-{$index}"]];
        }
        $supervisors['fixed'] = ['queues' => ['fixed'], 'balance' => 'simple', 'processes' => 1];
        $many = $this->queen([]);
        $many['supervisor']['supervisors'] = $supervisors;
        $manyDisabled = $this->queen(['enabled' => false]);
        $manyDisabled['supervisor']['supervisors'] = $supervisors;

        $this->assertSame(
            SupervisorConfiguration::resolve($manyDisabled, '/app')['heartbeat_timeout'] + 10,
            SupervisorConfiguration::resolve($many, '/app')['heartbeat_timeout'],
        );
    }

    public function testInvalidCoordinationSettingsAreRefused(): void
    {
        foreach ([
            [['enabled' => 'true'], []],
            [['namespace' => ''], []],
            [['namespace' => 42], []],
            [['connection' => 'redis'], ['redis' => ['driver' => 'redis']]],
            [['connection' => 'missing'], []],
        ] as [$settings, $connections]) {
            try {
                SupervisorConfiguration::resolve($this->queen($settings), '/app', null, $connections);
                $this->fail('Invalid coordination settings were accepted: ' . json_encode($settings));
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testTheInspectionRedactsAndTheEngineExportKeepsTheCredential(): void
    {
        $this->app['config']->set('queen.supervisor.coordination', ['enabled' => true]);
        $this->app['config']->set('queue.connections.queen.bearer_token', 'write-secret');
        $this->app['config']->set('queue.connections.queen.headers', ['X-Queen-Key' => 'header-secret']);
        $kernel = $this->app->make(\Illuminate\Contracts\Console\Kernel::class);

        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', [], $output));
        $redacted = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('[redacted]', $redacted['coordination']['connection']['bearer_token']);
        $this->assertSame('[redacted]', $redacted['coordination']['connection']['headers']['X-Queen-Key']);

        $this->app['config']->set('queue.connections.queen.headers', []);
        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', ['--for-engine' => true], $output));
        $json = trim($output->fetch());
        $engine = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('write-secret', $engine['coordination']['connection']['bearer_token']);
        $this->assertSame($engine['heartbeat_timeout'], $engine['coordination']['ttl']);
        // Rust decodes headers as a string map, never as a JSON list.
        $this->assertInstanceOf(\stdClass::class, json_decode($json)->coordination->connection->headers);
    }

    public function testThePhpEngineSizesItsShareAndLeavesWhenItPauses(): void
    {
        $config = SupervisorConfiguration::resolve($this->queen([]), '/app');
        $config['state_directory'] = $this->stateDirectory = sys_get_temp_dir() . '/queen-coordination-' . bin2hex(random_bytes(6));
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['results' => [['applied' => true]]]]);
        $clients = [];
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            $config,
            queenFactory: function (string $name, array $options) use ($handler, &$clients): Queen {
                $clients[$name] = $options;

                return new Queen([...$options, 'handler' => HandlerStack::create($handler)]);
            },
        );
        $lock = (new ReflectionProperty(PhpSupervisor::class, 'state'))->getValue($supervisor)->acquireLock();

        try {
            $coordinator = (new ReflectionMethod(PhpSupervisor::class, 'replicaCoordinator'))->invoke($supervisor);
            $this->assertInstanceOf(ReplicaCoordinator::class, $coordinator);
            $this->assertSame([$scope], array_values(
                (new ReflectionMethod(PhpSupervisor::class, 'coordinatedScopes'))->invoke($supervisor),
            ));
            $self = (new ReflectionProperty(PhpSupervisor::class, 'state'))->getValue($supervisor)->instanceId();
            $handler->requests = [];
            $this->answerWith($handler, $scope, [self::OTHER, $self]);
            $coordinator->heartbeat([$scope]);

            $position = (new ReflectionMethod(PhpSupervisor::class, 'replicaPosition'))->invoke($supervisor, 'default');
            $this->assertSame([strcmp($self, self::OTHER) < 0 ? 0 : 1, 2], $position);
            (new ReflectionMethod(PhpSupervisor::class, 'writeStatus'))->invoke($supervisor, 'running');
            $status = json_decode((string) file_get_contents($this->stateDirectory . '/status.json'), true);
            $this->assertSame([2], array_column($status['pool_status'], 'replicas'));

            (new ReflectionMethod(PhpSupervisor::class, 'pause'))->invoke($supervisor);
            $operations = json_decode((string) end($handler->requests)->getBody(), true)['operations'];
            $this->assertSame([['op' => 'delete', 'ns' => 'queen-supervisor', 'key' => "coordination/v1/{$scope}/{$self}"]], $operations);
            $this->assertSame([0, 1], (new ReflectionMethod(PhpSupervisor::class, 'replicaPosition'))->invoke($supervisor, 'default'));
            $this->assertSame('write-token', $clients['coordination']['bearerToken']);
            $this->assertSame(1, $clients['coordination']['retryAttempts']);
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    /**
     * Each coordination or remote status call may take http_timeout per
     * endpoint when the broker is slow or unreachable. The workers' SIGTERM
     * must not wait behind them, or the platform's stop deadline kills the
     * workers in the middle of a job.
     */
    public function testWorkersGetSigtermBeforeTheReplicaLeavesTheCoordination(): void
    {
        $config = SupervisorConfiguration::resolve($this->queen([]), '/app');
        $config['state_directory'] = $this->stateDirectory = sys_get_temp_dir() . '/queen-coordination-' . bin2hex(random_bytes(6));
        $config['shutdown_grace'] = 0;
        $events = [];
        $plan = new PlanHandler([], ['status' => 200, 'json' => ['results' => [['applied' => true]]]]);
        $recording = static function ($request, array $options) use ($plan, &$events) {
            if (str_contains((string) $request->getBody(), '"op":"delete"')) {
                $events[] = 'leave';
            }

            return $plan($request, $options);
        };
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            $config,
            queenFactory: fn (string $name, array $options): Queen => new Queen([...$options, 'handler' => HandlerStack::create($recording)]),
        );
        $lock = (new ReflectionProperty(PhpSupervisor::class, 'state'))->getValue($supervisor)->acquireLock();
        $worker = $this->createStub(\Symfony\Component\Process\Process::class);
        $signalled = false;
        $worker->method('isRunning')->willReturnCallback(static function () use (&$signalled): bool {
            return !$signalled;
        });
        $worker->method('signal')->willReturnCallback(
            static function (int $signal) use (&$events, &$signalled, $worker): \Symfony\Component\Process\Process {
                $events[] = $signal === SIGTERM ? 'SIGTERM' : "signal {$signal}";
                $signalled = true;

                return $worker;
            },
        );
        $worker->method('getPid')->willReturn(null);
        (new ReflectionProperty(PhpSupervisor::class, 'processes'))->setValue($supervisor, ['default' => ['high' => [$worker]]]);

        $failure = null;
        try {
            $drained = (new ReflectionMethod(PhpSupervisor::class, 'drain'))->invokeArgs($supervisor, [&$failure]);
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        $this->assertTrue($drained);
        $this->assertNull($failure);
        $this->assertSame(['SIGTERM', 'leave'], $events);
    }

    /** @param list<string> $members */
    private function answerWith(PlanHandler $handler, string $scope, array $members): void
    {
        (new ReflectionProperty(PlanHandler::class, 'plan'))->setValue($handler, [['status' => 200, 'json' => ['results' => [
            ['applied' => true],
            ['rows' => array_map(fn (string $id): array => ['key' => "coordination/v1/{$scope}/{$id}"], $members), 'truncated' => false],
        ]]]]);
    }

    /**
     * @param array<string, mixed> $coordination
     * @return array<string, mixed>
     */
    private function queen(array $coordination): array
    {
        return [
            'url' => 'http://queen.test:6632',
            'bearer_token' => 'write-token',
            'supervisor' => [
                'poll_interval' => 5,
                'read_bearer_token' => 'read-token',
                'coordination' => ['enabled' => true, ...$coordination],
                'supervisors' => ['default' => ['queues' => ['high']]],
            ],
        ];
    }
}
