<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Queue\QueueManager;
use InvalidArgumentException;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\RemoteStatusDocument;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;
use ReflectionMethod;
use ReflectionProperty;
use Symfony\Component\Console\Output\BufferedOutput;

final class LaravelSupervisorRemoteStatusTest extends TestCase
{
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

    public function testDisabledRemoteStatusKeepsTheEngineContractUnchanged(): void
    {
        $withoutSetting = $this->queen([]);
        unset($withoutSetting['supervisor']['remote_status']);

        $config = SupervisorConfiguration::resolve($this->queen(['enabled' => false]), '/app');

        $this->assertArrayNotHasKey('remote_status', $config);
        $this->assertSame(SupervisorConfiguration::resolve($withoutSetting, '/app'), $config);
    }

    public function testEnabledRemoteStatusExportsItsOwnConnectionAndDerivedTiming(): void
    {
        $config = SupervisorConfiguration::resolve($this->queen(['key' => 'orders-prod']), '/app');

        $this->assertSame([
            'connection' => [
                'url' => 'http://queen.test:6632',
                'urls' => ['http://queen.test:6632'],
                'bearer_token' => 'write-token',
                'headers' => [],
            ],
            'namespace' => 'queen-supervisor',
            'key' => 'orders-prod',
            'interval' => 5,
            'ttl' => 300,
        ], $config['remote_status']);
        // The supervisor pools poll depth with the read-only token; publishing
        // is a write and keeps the connection's own credential.
        $this->assertSame('read-token', $config['connections']['queen']['bearer_token']);
    }

    public function testThePublishBudgetIsPartOfTheHeartbeat(): void
    {
        $disabled = SupervisorConfiguration::resolve($this->queen(['enabled' => false]), '/app');
        $enabled = SupervisorConfiguration::resolve($this->queen(['key' => 'orders']), '/app');

        $this->assertSame($disabled['heartbeat_timeout'] + 5, $enabled['heartbeat_timeout']);
    }

    public function testExplicitIntervalAndTtlAreValidatedAgainstTheHeartbeat(): void
    {
        $config = SupervisorConfiguration::resolve($this->queen(['key' => 'orders', 'interval' => '10', 'ttl' => '600']), '/app');
        $this->assertSame(10, $config['remote_status']['interval']);
        $this->assertSame(600, $config['remote_status']['ttl']);

        $heartbeat = $config['heartbeat_timeout'];
        foreach ([
            ['interval' => $heartbeat],
            ['ttl' => $heartbeat - 1],
            ['ttl' => 86401],
            ['interval' => 0],
        ] as $override) {
            try {
                SupervisorConfiguration::resolve($this->queen(['key' => 'orders', ...$override]), '/app');
                $this->fail('Unsafe remote status timing was accepted: ' . json_encode($override));
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testEnabledRemoteStatusRequiresAKeyAndAQueenConnection(): void
    {
        foreach ([
            [['key' => null], []],
            [['key' => ''], []],
            [['key' => 'orders', 'connection' => 'redis'], ['redis' => ['driver' => 'redis']]],
            [['key' => 'orders', 'connection' => 'missing'], []],
            [['key' => 'orders', 'enabled' => 'true'], []],
        ] as [$settings, $connections]) {
            try {
                SupervisorConfiguration::resolve($this->queen($settings), '/app', null, $connections);
                $this->fail('Invalid remote status settings were accepted: ' . json_encode($settings));
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testConfigurationExportRedactsTheRemoteStatusCredential(): void
    {
        $this->app['config']->set('queen.supervisor.remote_status', ['enabled' => true, 'key' => 'orders']);
        $this->app['config']->set('queue.connections.queen.bearer_token', 'write-secret');
        $this->app['config']->set('queue.connections.queen.headers', ['X-Queen-Key' => 'header-secret']);
        $kernel = $this->app->make(\Illuminate\Contracts\Console\Kernel::class);

        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', [], $output));
        $redacted = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('[redacted]', $redacted['remote_status']['connection']['bearer_token']);
        $this->assertSame('[redacted]', $redacted['remote_status']['connection']['headers']['X-Queen-Key']);

        $this->app['config']->set('queue.connections.queen.headers', []);
        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', ['--pretty' => true], $output));
        $this->assertInstanceOf(\stdClass::class, json_decode(trim($output->fetch()))->remote_status->connection->headers);
    }

    public function testTheNativeEngineExportCarriesRemoteStatusWithItsWriteCredentials(): void
    {
        $this->app['config']->set('queen.supervisor.remote_status', ['enabled' => true, 'key' => 'orders']);
        $this->app['config']->set('queue.connections.queen.bearer_token', 'write-secret');
        $kernel = $this->app->make(\Illuminate\Contracts\Console\Kernel::class);

        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', ['--for-engine' => true], $output));
        $json = trim($output->fetch());
        $engine = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        // The Rust engine publishes too, so it receives the same resolved
        // setting as the PHP engine, unredacted like every other credential.
        $this->assertSame(2, $engine['version']);
        $this->assertSame('orders', $engine['remote_status']['key']);
        $this->assertSame('queen-supervisor', $engine['remote_status']['namespace']);
        $this->assertSame('write-secret', $engine['remote_status']['connection']['bearer_token']);
        $this->assertSame($engine['poll_interval'], $engine['remote_status']['interval']);
        $this->assertGreaterThanOrEqual($engine['heartbeat_timeout'], $engine['remote_status']['ttl']);
        // Rust decodes headers as a string map, never as a JSON list.
        $this->assertInstanceOf(\stdClass::class, json_decode($json)->remote_status->connection->headers);
    }

    public function testThePhpEngineAlsoPublishesEveryStatusItWritesLocally(): void
    {
        $this->stateDirectory = sys_get_temp_dir() . '/queen-remote-status-' . bin2hex(random_bytes(6));
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['results' => [
            ['applied' => true],
            ['applied' => true],
        ]]]);
        $clients = [];
        $config = SupervisorConfiguration::resolve($this->queen(['key' => 'orders']), '/app');
        $config['state_directory'] = $this->stateDirectory;
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            $config,
            queenFactory: function (string $name, array $options) use ($handler, &$clients): Queen {
                $clients[$name] = $options;

                return new Queen([...$options, 'handler' => HandlerStack::create($handler)]);
            },
        );
        $writeStatus = new ReflectionMethod(PhpSupervisor::class, 'writeStatus');
        // run() owns the generation before it writes any status.
        $lock = (new ReflectionProperty(PhpSupervisor::class, 'state'))->getValue($supervisor)->acquireLock();

        try {
            $writeStatus->invoke($supervisor, 'running');
            $writeStatus->invoke($supervisor, 'running');
            $writeStatus->invoke($supervisor, 'paused');
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        $this->assertSame(1, $clients['remote_status']['retryAttempts']);
        $this->assertSame('write-token', $clients['remote_status']['bearerToken']);
        $this->assertSame(2, $handler->count());
        $operations = json_decode((string) $handler->requests[1]->getBody(), true)['operations'];
        $local = json_decode((string) file_get_contents($this->stateDirectory . '/status.json'), true);
        $published = RemoteStatusDocument::decode(
            array_map(fn (array $op): array => ['key' => $op['key'], 'value' => $op['value']], $operations),
            RemoteStatusDocument::instanceKey('orders', $local['instance_id']),
        );
        unset($local['pools']);
        $this->assertSame($local, $published);
        $this->assertSame('paused', $published['state']);
        // The dashboard tells published instances apart by host.
        $this->assertSame(gethostname(), $published['hostname']);
    }

    /**
     * @param array<string, mixed> $remoteStatus
     * @return array<string, mixed>
     */
    private function queen(array $remoteStatus): array
    {
        return [
            'url' => 'http://queen.test:6632',
            'bearer_token' => 'write-token',
            'supervisor' => [
                'poll_interval' => 5,
                'read_bearer_token' => 'read-token',
                'remote_status' => ['enabled' => true, ...$remoteStatus],
                'supervisors' => ['default' => ['queues' => ['high']]],
            ],
        ];
    }
}
