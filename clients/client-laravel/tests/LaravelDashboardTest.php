<?php

namespace Queen\Tests;

use Illuminate\Contracts\Auth\Authenticatable;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Gate;
use Illuminate\Support\Facades\Schema;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\SupervisorState;

final class LaravelDashboardTest extends TestCase
{
    private string $stateDirectory;

    private string $failedPath;

    /** @var list<resource> */
    private array $supervisorLocks = [];

    protected function setUp(): void
    {
        $suffix = bin2hex(random_bytes(8));
        $this->stateDirectory = sys_get_temp_dir() . '/queen-dashboard-state-' . $suffix;
        $this->failedPath = sys_get_temp_dir() . '/queen-dashboard-failed-' . $suffix . '.json';
        parent::setUp();
    }

    protected function tearDown(): void
    {
        foreach ($this->supervisorLocks as $lock) {
            if (is_resource($lock)) {
                flock($lock, LOCK_UN);
                fclose($lock);
            }
        }
        $this->supervisorLocks = [];
        @unlink($this->failedPath);
        $this->removeDirectory($this->stateDirectory);
        parent::tearDown();
    }

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('app.key', 'base64:' . base64_encode(str_repeat('q', 32)));
        $app['config']->set('queen.dashboard', [
            'enabled' => true,
            'path' => 'queen',
            'domain' => null,
            'middleware' => ['web'],
            'refresh_seconds' => 5,
            'allow_local' => true,
            'failed_jobs_limit' => 2,
        ]);
        $app['config']->set('queen.supervisor.state_directory', $this->stateDirectory);
        $app['config']->set('queen.supervisor.supervisors', [
            'default' => [
                'connection' => 'queen',
                'consumer_group' => 'laravel',
                'queues' => ['high', 'default'],
                'balance' => 'auto',
                'strategy' => 'size',
                'min_processes' => 1,
                'max_processes' => 8,
                'timeout' => 60,
                'retry_after' => 90,
                'tries' => 3,
                'memory' => 128,
            ],
        ]);
        $app['config']->set('queue.failed', [
            'driver' => 'file',
            'path' => $this->failedPath,
            'limit' => 10,
        ]);
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);
    }

    public function testLocalDashboardRendersLivePoolsAndSecurityHeaders(): void
    {
        $this->liveSupervisor([
            'engine' => 'rust',
            'state' => 'running',
            'draining' => 1,
            'pool_status' => [[
                'supervisor' => 'default',
                'queue' => 'high',
                'running' => 3,
                'desired' => 4,
                'draining' => 1,
                'pids' => [101, 102, 103],
                'draining_pids' => [99],
                'restart_state' => 'backoff',
                'restart_failures' => 2,
                'restart_in_seconds' => 3,
                'depth' => 71,
                'depth_available' => true,
            ]],
        ]);

        $response = $this->get('/queen');

        $response->assertOk()
            ->assertSee('Laravel supervisor')
            ->assertSee('live')
            ->assertSee('3 / 4')
            ->assertSee('71')
            ->assertHeader('X-Frame-Options', 'DENY')
            ->assertHeader('X-Content-Type-Options', 'nosniff')
            ->assertHeader('Referrer-Policy', 'no-referrer');
        $this->assertStringContainsString("default-src 'none'", (string) $response->headers->get('Content-Security-Policy'));
        $this->assertStringContainsString("frame-ancestors 'none'", (string) $response->headers->get('Content-Security-Policy'));
        $this->assertStringContainsString("form-action 'self'", (string) $response->headers->get('Content-Security-Policy'));
        $this->assertStringContainsString('no-store', (string) $response->headers->get('Cache-Control'));
        $this->assertStringNotContainsString('http://', $response->getContent());
        $this->assertStringNotContainsString('https://', $response->getContent());
    }

    public function testJsonContractUsesHeartbeatDepthsWithoutPollingAndMarksMissingDepthUnavailable(): void
    {
        $this->liveSupervisor([
            'engine' => 'php',
            'state' => 'paused',
            'pool_status' => [[
                'supervisor' => 'default',
                'queue' => 'high',
                'running' => 2,
                'desired' => 2,
                'depth' => 4,
                'depth_available' => true,
            ]],
        ]);

        $response = $this->getJson('/queen/api/status')->assertOk();
        $response->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('supervisor.state', 'paused')
            ->assertJsonPath('supervisor.pools.1.processes', 2)
            ->assertJsonPath('queues.0.depth', 4)
            ->assertJsonPath('queues.0.available', true)
            ->assertJsonPath('queues.1.depth', null)
            ->assertJsonPath('queues.1.available', false);
    }

    public function testQueueDepthIdentityIncludesTheConsumerGroup(): void
    {
        $this->app['config']->set('queen.supervisor.supervisors', [
            'orders' => [
                'connection' => 'queen',
                'consumer_group' => 'orders-v1',
                'queues' => ['shared'],
            ],
            'billing' => [
                'connection' => 'queen',
                'consumer_group' => 'billing-v1',
                'queues' => ['shared'],
            ],
        ]);
        $this->liveSupervisor([
            'engine' => 'rust',
            'state' => 'running',
            'pool_status' => [
                [
                    'supervisor' => 'orders',
                    'queue' => 'shared',
                    'running' => 1,
                    'depth' => 7,
                    'depth_available' => true,
                ],
                [
                    'supervisor' => 'billing',
                    'queue' => 'shared',
                    'running' => 1,
                    'depth' => 3,
                    'depth_available' => true,
                ],
            ],
        ]);

        $queues = $this->getJson('/queen/api/status')->assertOk()->json('queues');
        $this->assertCount(2, $queues);
        $byGroup = collect($queues)->keyBy('consumer_group');
        $this->assertSame(7, $byGroup['orders-v1']['depth']);
        $this->assertSame(3, $byGroup['billing-v1']['depth']);
    }

    public function testStatusConfigurationSnapshotPreventsLaravelConfigDriftFromRelabelingPools(): void
    {
        $this->liveSupervisor([
            'engine' => 'rust',
            'state' => 'running',
            'pool_status' => [[
                'supervisor' => 'default',
                'queue' => 'high',
                'running' => 1,
                'depth' => 11,
                'depth_available' => true,
            ]],
        ]);
        $this->app['config']->set('queen.supervisor.supervisors', [
            'replacement' => [
                'connection' => 'other',
                'consumer_group' => 'new-deploy',
                'queues' => ['renamed'],
            ],
        ]);

        $response = $this->getJson('/queen/api/status')->assertOk();
        $response->assertJsonPath('configuration.supervisors.0.name', 'default')
            ->assertJsonPath('configuration.supervisors.0.connection', 'queen')
            ->assertJsonPath('configuration.supervisors.0.consumer_group', 'laravel')
            ->assertJsonPath('queues.0.queue', 'high')
            ->assertJsonPath('queues.0.depth', 11);
        $this->assertFalse(collect($response->json('queues'))->contains('queue', 'renamed'));
    }

    public function testActiveGenerationTimingSurvivesLaravelConfigDrift(): void
    {
        $configuration = $this->statusConfiguration();
        $configuration['control_ttl'] = 47;
        $configuration['heartbeat_timeout'] = 120;
        $state = $this->liveSupervisor([
            'engine' => 'rust',
            'state' => 'running',
            'pool_status' => [],
            'configuration' => $configuration,
        ]);
        $status = $state->status();
        $this->assertIsArray($status);
        $status['updated_at_epoch'] = time() - 60;
        $status['updated_at'] = gmdate('Y-m-d\TH:i:s\Z', $status['updated_at_epoch']);
        file_put_contents(
            $this->stateDirectory . '/status.json',
            json_encode($status, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
        );

        // These values describe a later deployment and must not redefine the
        // generation that still owns supervisor.lock.
        $this->app['config']->set('queen.dashboard.stale_after', 1);
        $this->app['config']->set('queen.supervisor.heartbeat_timeout', 1);
        $this->app['config']->set('queen.supervisor.control_ttl', 86400);

        $this->getJson('/queen/api/status')
            ->assertOk()
            ->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('configuration.heartbeat_timeout', 120)
            ->assertJsonPath('configuration.control_ttl', 47);

        $response = $this->withSession(['_token' => 'queen-csrf'])
            ->post('/queen/control/pause', [
                '_token' => 'queen-csrf',
                'instance_id' => $state->instanceId(),
            ]);
        $response->assertStatus(303);
        $command = $state->command(null, $state->instanceId());
        $this->assertSame(
            47,
            ($command['expires_at_epoch'] ?? 0) - ($command['requested_at_epoch'] ?? 0),
        );
    }

    public function testDashboardResolvesRelativeStateDirectoryFromLaravelRoot(): void
    {
        $relative = 'queen-dashboard-relative-' . bin2hex(random_bytes(8));
        $this->stateDirectory = $this->app->basePath($relative);
        $this->app['config']->set('queen.supervisor.state_directory', $relative);
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $this->getJson('/queen/api/status')
            ->assertOk()
            ->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('supervisor.engine', 'php');
        $this->assertFileExists($this->stateDirectory . '/status.json');
    }

    public function testMissingStatusConfigurationFailsClosedWithoutLaravelConfigFallback(): void
    {
        $this->liveSupervisor([
            'engine' => 'php',
            'state' => 'running',
            'pool_status' => [],
            'configuration' => null,
        ]);

        $this->getJson('/queen/api/status')
            ->assertOk()
            ->assertJsonPath('supervisor.availability', 'unavailable')
            ->assertJsonPath('configuration.supervisors', [])
            ->assertJsonPath('queues', []);
    }

    public function testRefreshRateCannotBeOverriddenByAQueryString(): void
    {
        $this->app['config']->set('queen.dashboard.refresh_seconds', 17);

        $this->get('/queen?refresh=2')
            ->assertOk()
            ->assertSee('<meta http-equiv="refresh" content="17;url=/queen">', false)
            ->assertDontSee('<meta http-equiv="refresh" content="2;', false);
    }

    public function testUnknownStatusSchemaFailsClosed(): void
    {
        $state = $this->liveSupervisor(['engine' => 'php', 'state' => 'running']);
        $status = $state->status();
        $this->assertIsArray($status);
        $status['schema'] = 'queen.supervisor.status/v999';
        file_put_contents(
            $this->stateDirectory . '/status.json',
            json_encode($status, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
        );

        $this->getJson('/queen/api/status')
            ->assertOk()
            ->assertJsonPath('supervisor.availability', 'unavailable')
            ->assertJsonPath('supervisor.instance_id', null)
            ->assertJsonPath('supervisor.pools', []);
    }

    public function testAuthorizationIsDenyByDefaultInProductionAndAnExplicitGateWins(): void
    {
        $this->app['env'] = 'production';
        $this->get('/queen')->assertForbidden();

        Gate::define('viewQueenDashboard', static fn (?Authenticatable $user): bool => false);
        $this->get('/queen')->assertForbidden();

        Gate::define('viewQueenDashboard', static fn (?Authenticatable $user): bool => true);
        $this->get('/queen')->assertOk();
    }

    public function testLocalFallbackCanBeDisabledAndExplicitGateDenialStillWins(): void
    {
        $this->app['config']->set('queen.dashboard.allow_local', false);
        $this->get('/queen')->assertForbidden();

        $this->app['config']->set('queen.dashboard.allow_local', true);
        Gate::define('viewQueenDashboard', static fn (?Authenticatable $user): bool => false);
        $this->get('/queen')->assertForbidden();
    }

    public function testRuntimeKillSwitchStillWorksWhenEnabledRoutesAreAlreadyRegistered(): void
    {
        $this->app['config']->set('queen.dashboard.enabled', false);

        $this->get('/queen')->assertNotFound();
        $this->get('/queen/api/status')->assertNotFound();
        $this->post('/queen/control/pause')->assertNotFound();
    }

    public function testAuthorizationRunsBeforeAnySupervisorOrFailedBackendRead(): void
    {
        if (!is_dir($this->stateDirectory)) {
            mkdir($this->stateDirectory, 0700, true);
        }
        file_put_contents($this->stateDirectory . '/status.json', str_repeat('x', 1048577));
        file_put_contents($this->failedPath, str_repeat('x', 4194305));
        $this->app['env'] = 'production';

        $this->get('/queen')->assertForbidden();
    }

    public function testControlsRequireCsrfUsePostAndReturnAccepted(): void
    {
        $state = $this->liveSupervisor(['engine' => 'php', 'state' => 'running']);
        $instanceId = $state->instanceId();
        $this->app['env'] = 'local';

        $this->post('/queen/control/pause', ['instance_id' => $instanceId])->assertStatus(419);
        $this->get('/queen/control/pause')->assertStatus(405);

        $response = $this->withSession(['_token' => 'queen-csrf'])
            ->post('/queen/control/pause', ['_token' => 'queen-csrf', 'instance_id' => $instanceId]);
        $response->assertStatus(303)
            ->assertHeader('Location', '/queen')
            ->assertSessionHas(
                'queen_dashboard_control_status',
                'Supervisor command [pause] accepted and pending consumption.',
            );

        $command = $state->command(null, $instanceId);
        $this->assertSame('pause', $command['command'] ?? null);
        $this->assertSame($instanceId, $command['instance_id'] ?? null);
    }

    public function testControlsRejectMissingReplacedStaleAndPendingInstances(): void
    {
        $state = $this->liveSupervisor(['engine' => 'rust', 'state' => 'running']);
        $instanceId = $state->instanceId();

        $this->post('/queen/control/pause')->assertStatus(422);
        $this->post('/queen/control/pause', ['instance_id' => str_repeat('a', 32)])->assertStatus(409);

        $state->request('pause', $instanceId, 15);
        $this->post('/queen/control/continue', ['instance_id' => $instanceId])->assertStatus(409);
        $this->assertSame('pause', $state->command(null, $instanceId)['command'] ?? null);

        $status = $state->status();
        $this->assertIsArray($status);
        $status['updated_at_epoch'] = time() - 3601;
        $status['updated_at'] = gmdate('Y-m-d\TH:i:s\Z', $status['updated_at_epoch']);
        file_put_contents(
            $this->stateDirectory . '/status.json',
            json_encode($status, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
        );
        $this->post('/queen/control/terminate', ['instance_id' => $instanceId])->assertStatus(409);
    }

    public function testUnavailableBackendsDegradeIndependently(): void
    {
        $this->app['config']->set('queue.failed.driver', 'dynamodb');

        $response = $this->getJson('/queen/api/status')->assertOk();
        $response->assertJsonPath('supervisor.availability', 'unavailable')
            ->assertJsonPath('failed_jobs.available', false)
            ->assertJsonPath('queues', []);
    }

    public function testFailedJobsAreBoundedAndPayloadExceptionAndSecretsAreNeverRendered(): void
    {
        $secret = 'QUEEN_TOP_SECRET_TOKEN';
        $this->app['config']->set('queen.bearer_token', $secret);
        $records = [];
        foreach (range(1, 3) as $id) {
            $records[] = [
                'id' => "failed-{$id}",
                'connection' => 'queen',
                'queue' => $id === 1 ? '<img src=x onerror=alert(1)>' : 'default',
                'payload' => "payload-{$secret}",
                'exception' => "exception-{$secret}",
                'failed_at' => '2026-08-29 10:00:00',
            ];
        }
        file_put_contents($this->failedPath, json_encode($records, JSON_THROW_ON_ERROR));

        $response = $this->get('/queen')->assertOk();
        $response->assertSee('&lt;img src=x onerror=alert(1)&gt;', false)
            ->assertDontSee('<img src=x onerror=alert(1)>', false)
            ->assertDontSee($secret)
            ->assertDontSee('payload-')
            ->assertDontSee('exception-')
            ->assertDontSee('failed-3');

        $json = $this->getJson('/queen/api/status')->assertOk();
        $json->assertJsonPath('failed_jobs.available', true)
            ->assertJsonPath('failed_jobs.total', 3)
            ->assertJsonPath('failed_jobs.total_exact', true)
            ->assertJsonPath('failed_jobs.showing', 2)
            ->assertJsonPath('failed_jobs.items.0.lifecycle_policy', 'laravel+queen-dlq')
            ->assertJsonMissing(['payload' => "payload-{$secret}"])
            ->assertJsonMissing(['exception' => "exception-{$secret}"]);
        $this->assertStringNotContainsString($secret, $json->getContent());
    }

    public function testDatabaseFailedSummaryUsesALimitSentinelWithoutCountScan(): void
    {
        Schema::create('failed_jobs', function (Blueprint $table): void {
            $table->bigIncrements('id');
            $table->string('connection');
            $table->string('queue');
            $table->timestamp('failed_at');
        });
        DB::table('failed_jobs')->insert(array_map(static fn (int $id): array => [
            'connection' => 'queen',
            'queue' => "queue-{$id}",
            'failed_at' => '2026-08-29 10:00:00',
        ], range(1, 3)));
        $this->app['config']->set('queue.failed', [
            'driver' => 'database',
            'database' => 'testing',
            'table' => 'failed_jobs',
        ]);
        $queries = [];
        DB::listen(static function ($query) use (&$queries): void {
            $queries[] = strtolower($query->sql);
        });

        $response = $this->getJson('/queen/api/status')->assertOk();
        $response->assertJsonPath('failed_jobs.total', 3)
            ->assertJsonPath('failed_jobs.total_exact', false)
            ->assertJsonPath('failed_jobs.showing', 2);
        $this->assertSame([], array_values(array_filter(
            $queries,
            static fn (string $query): bool => str_contains($query, 'count('),
        )));
    }

    public function testArbitraryStatusFieldsAndConfigurationSecretsAreRedacted(): void
    {
        $secret = 'DO_NOT_EXPOSE_THIS';
        $this->app['config']->set('queue.connections.queen', [
            'driver' => 'queen',
            'url' => 'https://user:' . $secret . '@queen.invalid',
            'bearer_token' => $secret,
            'headers' => ['Authorization' => 'Bearer ' . $secret],
        ]);
        $this->liveSupervisor([
            'engine' => 'php',
            'state' => 'running',
            'debug' => ['token' => $secret, 'payload' => '<script>alert(1)</script>'],
        ]);

        $response = $this->getJson('/queen/api/status')->assertOk();
        $this->assertStringNotContainsString($secret, $response->getContent());
        $this->assertStringNotContainsString('queen.invalid', $response->getContent());
        $this->assertStringNotContainsString('<script>', $response->getContent());
    }

    public function testPackageRoutesCanBeCachedFromConsole(): void
    {
        try {
            $this->artisan('route:cache')->assertSuccessful();
            $this->assertFileExists($this->app->getCachedRoutesPath());
        } finally {
            $this->artisan('route:clear')->assertSuccessful();
        }
    }

    /** @param array<string, mixed> $status */
    private function liveSupervisor(array $status): SupervisorState
    {
        $state = new SupervisorState($this->stateDirectory);
        $this->supervisorLocks[] = $state->acquireLock();
        if (!array_key_exists('configuration', $status)) {
            $status['configuration'] = $this->statusConfiguration();
        }
        $state->writeStatus($status);

        return $state;
    }

    /** @return array<string, mixed> */
    private function statusConfiguration(): array
    {
        $supervisors = [];
        $configured = $this->app['config']->get('queen.supervisor.supervisors', []);
        if (is_array($configured)) {
            ksort($configured, SORT_STRING);
            foreach ($configured as $name => $options) {
                if (!is_array($options)) {
                    continue;
                }
                $supervisors[] = [
                    'name' => (string) $name,
                    'connection' => (string) ($options['connection'] ?? 'queen'),
                    'consumer_group' => (string) ($options['consumer_group'] ?? 'laravel'),
                    'queues' => array_values($options['queues'] ?? []),
                    'balance' => (string) ($options['balance'] ?? 'auto'),
                    'strategy' => (string) ($options['strategy'] ?? 'size'),
                    'processes' => (int) ($options['processes'] ?? $options['max_processes'] ?? 10),
                    'min_processes' => (int) ($options['min_processes'] ?? 1),
                    'max_processes' => (int) ($options['max_processes'] ?? 10),
                    'timeout' => (int) ($options['timeout'] ?? 60),
                    'retry_after' => (int) ($options['retry_after'] ?? 90),
                    'tries' => (int) ($options['tries'] ?? 3),
                    'memory' => (int) ($options['memory'] ?? 128),
                ];
            }
        }

        return [
            'poll_interval' => 3,
            'http_timeout' => 5,
            'control_ttl' => 3600,
            'heartbeat_timeout' => 3600,
            'shutdown_grace' => 75,
            'telemetry_ttl' => 300,
            'process_limit' => 256,
            'supervisors' => $supervisors,
        ];
    }

    private function removeDirectory(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }
        $entries = scandir($directory);
        if (!is_array($entries)) {
            return;
        }
        foreach ($entries as $entry) {
            if ($entry !== '.' && $entry !== '..') {
                @unlink($directory . DIRECTORY_SEPARATOR . $entry);
            }
        }
        @rmdir($directory);
    }
}
