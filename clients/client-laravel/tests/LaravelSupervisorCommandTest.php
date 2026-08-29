<?php

namespace Queen\Tests;

use Orchestra\Testbench\TestCase;
use Queen\Laravel\Commands\SupervisorControlCommand;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\SupervisorState;
use Symfony\Component\Console\Output\BufferedOutput;

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
