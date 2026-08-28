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

    public function testConfigurationCommandExportsTheProductionContract(): void
    {
        $output = new BufferedOutput();
        $exitCode = $this->app->make(\Illuminate\Contracts\Console\Kernel::class)
            ->call('queen:supervisor-config', [], $output);
        $document = json_decode(trim($output->fetch()), true, 512, JSON_THROW_ON_ERROR);

        $this->assertSame(0, $exitCode);
        $this->assertSame(2, $document['version']);
        $this->assertSame(256, $document['process_limit']);
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

    public function testControlCommandRejectsRequestsWithoutALiveSupervisor(): void
    {
        $this->configureStateDirectory();

        $this->artisan('queen:supervisor', ['action' => 'pause'])
            ->expectsOutputToContain('No live Queen supervisor owns this state directory.')
            ->assertFailed();
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

    private function configureStateDirectory(): string
    {
        $this->stateDirectory = sys_get_temp_dir() . '/queen-supervisor-command-' . bin2hex(random_bytes(8));
        $this->app['config']->set('queen.supervisor.state_directory', $this->stateDirectory);

        return $this->stateDirectory;
    }
}
