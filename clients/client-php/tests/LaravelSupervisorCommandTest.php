<?php

namespace Queen\Tests;

use Orchestra\Testbench\TestCase;
use Queen\Laravel\Commands\SupervisorControlCommand;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\SupervisorState;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\ConsoleSectionOutput;
use Symfony\Component\Console\Output\OutputInterface;

class LaravelSupervisorCommandTest extends TestCase
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

    public function testProviderRegistersTheSupervisorControlCommand(): void
    {
        $this->artisan('list')
            ->expectsOutputToContain('queen:supervisor')
            ->assertSuccessful();

        $this->assertTrue(class_exists(SupervisorControlCommand::class));
    }

    public function testDefaultBinaryInstallAndRuntimeStateDirectoriesAreDisjoint(): void
    {
        $stateDirectory = $this->app['config']->get('queen.supervisor.state_directory');
        $installDirectory = $this->app['config']->get('queen.supervisor_binary.install_path');

        $this->assertSame(storage_path('queen-supervisor'), $stateDirectory);
        $this->assertSame(storage_path('queen-supervisor-bin'), $installDirectory);
        $this->assertNotSame($stateDirectory, dirname((string) $installDirectory));
        $this->assertFalse(str_starts_with((string) $installDirectory, rtrim((string) $stateDirectory, '/') . '/'));
        $this->assertFalse(str_starts_with((string) $stateDirectory, rtrim((string) $installDirectory, '/') . '/'));
    }

    public function testConfigurationCommandExportsTheProductionContract(): void
    {
        $output = new BufferedOutput();
        $exitCode = $this->app->make(\Illuminate\Contracts\Console\Kernel::class)
            ->call('queen:supervisor-config', [], $output);
        $json = trim($output->fetch());
        $document = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        $object = json_decode($json, false, 512, JSON_THROW_ON_ERROR);

        $this->assertSame(0, $exitCode);
        $this->assertSame(2, $document['version']);
        $this->assertSame(256, $document['process_limit']);
        $this->assertInstanceOf(\stdClass::class, $object->queen->headers);
        $this->assertInstanceOf(\stdClass::class, $object->connections->queen->headers);
    }

    public function testClientMetadataRequiresExplicitNativeEngineNegotiation(): void
    {
        $previous = getenv('QUEEN_SUPERVISOR_ENGINE_METADATA');
        $kernel = $this->app->make(\Illuminate\Contracts\Console\Kernel::class);
        try {
            foreach ([false, '1'] as $negotiated) {
                putenv($negotiated === false ? 'QUEEN_SUPERVISOR_ENGINE_METADATA' : 'QUEEN_SUPERVISOR_ENGINE_METADATA=1');
                foreach ([false, true] as $forEngine) {
                    $output = new BufferedOutput();
                    $this->assertSame(0, $kernel->call('queen:supervisor-config', $forEngine ? ['--for-engine' => true] : [], $output));
                    $document = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
                    if ($negotiated && $forEngine) {
                        $this->assertArrayHasKey('client_version', $document);
                        $this->assertSame(\Queen\Laravel\Supervisor\SupervisorMetadata::clientVersion(), $document['client_version']);
                    } else {
                        $this->assertArrayNotHasKey('client_version', $document, 'Older strict native engines must keep their existing export.');
                    }
                }
            }
        } finally {
            putenv($previous === false ? 'QUEEN_SUPERVISOR_ENGINE_METADATA' : 'QUEEN_SUPERVISOR_ENGINE_METADATA=' . $previous);
        }
    }

    public function testConfigurationExportRedactsSecretsUnlessRequestedForAnEngine(): void
    {
        $this->app['config']->set('queue.connections.queen.bearer_token', 'worker-secret');
        $this->app['config']->set('queue.connections.queen.headers', ['X-Queen-Key' => 'header-secret']);
        $kernel = $this->app->make(\Illuminate\Contracts\Console\Kernel::class);

        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', [], $output));
        $redacted = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('[redacted]', $redacted['connections']['queen']['bearer_token']);
        $this->assertSame('[redacted]', $redacted['connections']['queen']['headers']['X-Queen-Key']);

        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', ['--for-engine' => true], $output));
        $engine = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('worker-secret', $engine['connections']['queen']['bearer_token']);
        $this->assertSame('header-secret', $engine['connections']['queen']['headers']['X-Queen-Key']);
    }

    public function testTheEngineConfigurationCarriesTheLeaseServiceOnlyWhenItIsOff(): void
    {
        $kernel = $this->app->make(\Illuminate\Contracts\Console\Kernel::class);
        $this->assertTrue($this->app['config']->get('queen.supervisor.lease_service'), 'on by default');

        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', ['--for-engine' => true], $output));
        $document = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
        // The Rust engine reads an absent key as on, and rejects unknown keys.
        $this->assertArrayNotHasKey('lease_service', $document);

        $this->app['config']->set('queen.supervisor.lease_service', false);
        $output = new BufferedOutput();
        $this->assertSame(0, $kernel->call('queen:supervisor-config', ['--for-engine' => true], $output));
        $this->assertFalse(json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR)['lease_service']);

        // Read like every other switch: a real boolean only, never a string.
        $this->app['config']->set('queen.supervisor.lease_service', 'false');
        $output = new BufferedOutput();
        $this->assertSame(1, $kernel->call('queen:supervisor-config', ['--for-engine' => true], $output));
        $this->assertStringContainsString('Queen supervisor lease_service must be a boolean.', $output->fetch());
    }

    public function testEngineConfigurationWarnsOnStandardErrorAboutAPrefetchingPoolWithOneTry(): void
    {
        $this->app['config']->set('queen.supervisor.supervisors', ['default' => ['queues' => ['default'], 'tries' => 1]]);
        $this->app['config']->set('queue.connections.queen.prefetch', 4);
        $this->app['config']->set('queue.connections.queen.lease_renewal', true);
        $stderr = new BufferedOutput();
        $output = new class ($stderr) extends BufferedOutput implements ConsoleOutputInterface {
            public function __construct(private OutputInterface $stderr)
            {
                parent::__construct();
            }

            public function getErrorOutput(): OutputInterface
            {
                return $this->stderr;
            }

            public function setErrorOutput(OutputInterface $error): void
            {
                $this->stderr = $error;
            }

            public function section(): ConsoleSectionOutput
            {
                throw new \LogicException('No sections here.');
            }
        };

        $exitCode = $this->app->make(\Illuminate\Contracts\Console\Kernel::class)
            ->call('queen:supervisor-config', ['--for-engine' => true], $output);

        $this->assertSame(0, $exitCode);
        $document = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame(1, $document['supervisors']['default']['tries']);
        $this->assertStringContainsString(
            'Queen warning: Pool default can fail a job that never ran.',
            $stderr->fetch(),
        );
    }

    public function testEngineConfigurationExportRejectsDocumentsAboveOneMebibyte(): void
    {
        $this->app['config']->set(
            'queue.connections.queen.bearer_token',
            str_repeat('x', 1048576),
        );
        $output = new BufferedOutput();

        $exitCode = $this->app->make(\Illuminate\Contracts\Console\Kernel::class)
            ->call('queen:supervisor-config', ['--for-engine' => true], $output);

        $this->assertSame(1, $exitCode);
        $this->assertStringContainsString('exceeds the 1 MiB transport limit', $output->fetch());
    }

    public function testControlCommandRejectsRequestsWithoutALiveSupervisor(): void
    {
        $this->configureStateDirectory();

        $this->artisan('queen:supervisor', ['action' => 'pause'])
            ->expectsOutputToContain('No live Queen supervisor owns this state directory.')
            ->assertFailed();
    }

    public function testControlCommandReportsAnUnsafeStateDirectoryWithoutAStackTrace(): void
    {
        $directory = $this->configureStateDirectory();
        $this->assertTrue(mkdir($directory, 0755));

        $output = new BufferedOutput();
        $exitCode = $this->app->make(\Illuminate\Contracts\Console\Kernel::class)->call(
            'queen:supervisor',
            ['action' => 'status'],
            $output,
        );
        $rendered = $output->fetch();

        $this->assertSame(1, $exitCode);
        $this->assertStringContainsString('must be a private real directory', $rendered);
        $this->assertStringNotContainsString('Stack trace', $rendered);
        $this->assertLessThan(1024, strlen($rendered));
    }

    public function testStatusCommandEmitsMachineReadableState(): void
    {
        $directory = $this->configureStateDirectory();
        (new SupervisorState($directory))->writeStatus([
            'engine' => 'php',
            'state' => 'running',
            'pools' => [],
        ]);

        $output = new BufferedOutput();
        $exitCode = $this->app->make(\Illuminate\Contracts\Console\Kernel::class)->call(
            'queen:supervisor',
            ['action' => 'status', '--json' => true],
            $output,
        );
        $document = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);

        $this->assertSame(0, $exitCode);
        $this->assertSame('php', $document['engine']);
        $this->assertSame('stale', $document['state']);
        $this->assertFalse($document['live']);
    }

    public function testStatusHealthCheckFailsForStaleState(): void
    {
        $directory = $this->configureStateDirectory();
        (new SupervisorState($directory))->writeStatus([
            'engine' => 'rust',
            'state' => 'running',
            'pools' => [],
        ]);

        $this->artisan('queen:supervisor', ['action' => 'status', '--check' => true])
            ->assertFailed();
    }

    public function testStatusCheckSeparatesLivenessFromServingReadiness(): void
    {
        $directory = $this->configureStateDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();

        try {
            $state->writeStatus([
                'engine' => 'rust',
                'state' => 'running',
                'pools' => [],
                'pool_status' => [[
                    'supervisor' => 'orders',
                    'queue' => 'high',
                    'desired' => 1,
                    'running' => 0,
                    'healthy' => true,
                    'restart_state' => 'closed',
                    'restart_failures' => 0,
                    'depth' => 4,
                    'depth_available' => true,
                ]],
            ]);

            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check-liveness' => true,
            ])->assertSuccessful();
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check' => true,
            ])->assertFailed();
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check-capacity' => true,
            ])->assertFailed();

            $output = new BufferedOutput();
            $this->assertSame(0, $this->app->make(\Illuminate\Contracts\Console\Kernel::class)->call(
                'queen:supervisor',
                ['action' => 'status', '--json' => true],
                $output,
            ));
            $document = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
            $this->assertTrue($document['live']);
            $this->assertFalse($document['ready']);
            $this->assertSame('pool_zero_capacity', $document['readiness_issues'][0]['code']);

            $ready = $state->status();
            $ready['pool_status'][0]['running'] = 1;
            $ready['pool_status'][0]['desired'] = 2;
            $state->writeStatus($ready);
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check' => true,
            ])->assertSuccessful();
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check-capacity' => true,
            ])->assertFailed();

            $output = new BufferedOutput();
            $this->assertSame(0, $this->app->make(\Illuminate\Contracts\Console\Kernel::class)->call(
                'queen:supervisor',
                ['action' => 'status', '--json' => true],
                $output,
            ));
            $partial = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
            $this->assertTrue($partial['ready']);
            $this->assertFalse($partial['processing_healthy']);
            $this->assertSame(
                'pool_below_desired_capacity',
                $partial['processing_health_issues'][0]['code'],
            );

            $full = $state->status();
            $full['pool_status'][0]['running'] = 2;
            $state->writeStatus($full);
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check-capacity' => true,
            ])->assertSuccessful();
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testReadinessRequiresDepthButDoesNotRestartAStillServingCircuit(): void
    {
        $directory = $this->configureStateDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();

        try {
            $state->writeStatus([
                'engine' => 'php',
                'state' => 'running',
                'pools' => [],
                'pool_status' => [[
                    'supervisor' => 'orders',
                    'queue' => 'default',
                    'desired' => 1,
                    'running' => 1,
                    'healthy' => false,
                    'restart_state' => 'open',
                    'restart_failures' => 5,
                    'depth' => null,
                    'depth_available' => false,
                ]],
            ]);

            $readiness = $state->readiness($state->status());
            $this->assertFalse($readiness['ready']);
            $this->assertSame(
                ['queue_depth_unavailable'],
                array_column($readiness['issues'], 'code'),
            );
            $capacity = $state->capacityHealth($state->status());
            $this->assertSame(
                ['queue_depth_unavailable', 'worker_restart_circuit_unhealthy'],
                array_column($capacity['issues'], 'code'),
            );
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check' => true,
            ])->assertFailed();

            $serving = $state->status();
            $serving['pool_status'][0]['depth'] = 4;
            $serving['pool_status'][0]['depth_available'] = true;
            $state->writeStatus($serving);
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check' => true,
            ])->assertSuccessful();
            $this->artisan('queen:supervisor', [
                'action' => 'status',
                '--check-capacity' => true,
            ])->assertFailed();
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testCliUsesRelativeStatePathAndActiveGenerationTiming(): void
    {
        $relative = 'queen-supervisor-command-' . bin2hex(random_bytes(8));
        $this->stateDirectory = $this->app->basePath($relative);
        $this->app['config']->set('queen.supervisor.state_directory', $relative);
        $this->app['config']->set('queen.supervisor.poll_interval', 1);
        $this->app['config']->set('queen.supervisor.heartbeat_timeout', 1);
        $this->app['config']->set('queen.supervisor.control_ttl', 86400);
        $state = new SupervisorState($this->stateDirectory);
        $lock = $state->acquireLock();

        try {
            $state->writeStatus([
                'engine' => 'rust',
                'state' => 'running',
                'pools' => [],
                'pool_status' => [],
                'configuration' => [
                    'heartbeat_timeout' => 120,
                    'control_ttl' => 47,
                ],
            ]);
            $status = $state->status();
            $this->assertIsArray($status);
            $status['updated_at_epoch'] = time() - 60;
            $status['updated_at'] = gmdate('Y-m-d\TH:i:s\Z', $status['updated_at_epoch']);
            file_put_contents(
                $this->stateDirectory . '/status.json',
                json_encode($status, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
            );

            $output = new BufferedOutput();
            $exitCode = $this->app->make(\Illuminate\Contracts\Console\Kernel::class)->call(
                'queen:supervisor',
                ['action' => 'status', '--json' => true],
                $output,
            );
            $document = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);
            $this->assertSame(0, $exitCode);
            $this->assertTrue($document['live']);
            $this->assertSame('rust', $document['engine']);

            $this->artisan('queen:supervisor', ['action' => 'pause'])->assertSuccessful();
            $command = $state->command(null, $state->instanceId());
            $this->assertSame(
                47,
                ($command['expires_at_epoch'] ?? 0) - ($command['requested_at_epoch'] ?? 0),
            );
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    private function configureStateDirectory(): string
    {
        $this->stateDirectory = sys_get_temp_dir() . '/queen-supervisor-command-' . bin2hex(random_bytes(8));
        $this->app['config']->set('queen.supervisor.state_directory', $this->stateDirectory);

        return $this->stateDirectory;
    }
}
