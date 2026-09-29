<?php

namespace Queen\Tests;

use Illuminate\Contracts\Auth\Authenticatable;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Gate;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\ServiceProvider;
use GuzzleHttp\HandlerStack;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\Dashboard\DashboardScript;
use Queen\Laravel\Dashboard\DashboardStylesheet;
use Queen\Laravel\Dashboard\RemoteStatusReader;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\RemoteStatusDocument;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

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

        $this->get('/queen/supervisors')->assertOk()->assertSee('3 / 4')->assertSee('Active');
        $response = $this->get('/queen');

        $response->assertOk()
            ->assertSee('Queen Supervisor')
            ->assertSee('Dashboard sections')
            ->assertSee('live')
            ->assertSee('71+')
            ->assertSee('1 queue depth unavailable')
            ->assertHeader('X-Frame-Options', 'DENY')
            ->assertHeader('X-Content-Type-Options', 'nosniff')
            ->assertHeader('Referrer-Policy', 'no-referrer');
        $contentSecurityPolicy = (string) $response->headers->get('Content-Security-Policy');
        $this->assertStringContainsString("default-src 'none'", $contentSecurityPolicy);
        $this->assertStringContainsString("style-src 'self'", $contentSecurityPolicy);
        $this->assertStringContainsString("style-src-attr 'none'", $contentSecurityPolicy);
        $this->assertStringContainsString("frame-ancestors 'none'", $contentSecurityPolicy);
        $this->assertStringContainsString("form-action 'self'", $contentSecurityPolicy);
        $this->assertStringContainsString('no-store', (string) $response->headers->get('Cache-Control'));
        $content = $response->getContent();
        $this->assertStringNotContainsString('http://', $content);
        $this->assertStringNotContainsString('https://', $content);
        // Exactly one script: the packaged, same-origin, content-hashed file
        // with Subresource Integrity. No inline code.
        $this->assertSame(1, substr_count($content, '<script'));
        $this->assertStringNotContainsString(' style=', $content);

        $xpath = $this->dashboardXPath($content);
        $this->assertSame(1, $xpath->query('//h1')->length);
        $this->assertSame(1, $xpath->query('//main[@id="main-content"]')->length);
        // One section per page; the sidebar links to every page.
        $this->assertSame(1, $xpath->query('//section[@id="overview"]')->length);
        $this->assertSame(1, $xpath->query('//main//section')->length);
        foreach (['/queen', '/queen/workload', '/queen/supervisors', '/queen/failed-jobs', '/queen/configuration'] as $url) {
            $this->assertSame(1, $xpath->query('//nav[@aria-label="Dashboard sections"]//a[@href="' . $url . '"]')->length, $url);
        }
        foreach (['workload' => 'workload', 'supervisors' => 'supervisors', 'failed-jobs' => 'failed-jobs', 'configuration' => 'configuration'] as $path => $section) {
            $page = $this->dashboardXPath($this->get('/queen/' . $path)->assertOk()->getContent());
            $this->assertSame(1, $page->query('//main//section[@id="' . $section . '"]')->length, $path);
            $this->assertSame('/queen/' . $path, $page->query('//nav//a[@aria-current="page"]')->item(0)->getAttribute('href'));
        }
        $this->assertSame(0, $xpath->query('//th[not(@scope="col")]')->length);
        $this->assertSame(0, $xpath->query('//style')->length);
        $this->assertSame(0, $xpath->query('//script[not(@src)]')->length);
        $script = $xpath->query('//script[@src]')->item(0);
        $this->assertInstanceOf(\DOMElement::class, $script);
        $this->assertMatchesRegularExpression('#^/queen/assets/dashboard-[a-f0-9]{64}\.js$#D', $script->getAttribute('src'));
        $this->assertMatchesRegularExpression('#^sha256-[A-Za-z0-9+/]{43}=$#D', $script->getAttribute('integrity'));
        $this->assertTrue($script->hasAttribute('defer'));
        $this->assertStringContainsString("script-src 'self'", $contentSecurityPolicy);
        // Without scripts the page still refreshes itself, whole.
        $this->assertSame(1, $xpath->query('//noscript/meta[@http-equiv="refresh"]')->length);
        $this->assertSame(0, $xpath->query('//head/meta[@http-equiv="refresh"]')->length);
        $this->assertSame(0, $xpath->query('//*[@style]')->length);
        $this->assertSame(1, $xpath->query('//link[@rel="stylesheet"]')->length);

        $supervisors = $this->dashboardXPath($this->get('/queen/supervisors')->assertOk()->getContent());
        $this->assertDashboardButtonState($supervisors, 'Pause', false);
        $this->assertDashboardButtonState($supervisors, 'Continue', true);
        $this->assertDashboardButtonState($supervisors, 'Terminate', false);
    }

    public function testDashboardSeparatesLivenessReadinessAndDesiredCapacityAndShowsProcessBudget(): void
    {
        $state = $this->liveSupervisor([
            'engine' => 'rust',
            'state' => 'running',
            'ready' => true,
            'capacity_satisfied' => false,
            'draining' => 1,
            'process_budget' => [
                'limit' => 256,
                'used' => 9,
                'available' => 247,
                'active_worker_processes' => 4,
                'draining_worker_processes' => 1,
                'renewal_helpers_reserved' => 4,
            ],
            'pool_status' => [
                [
                    'supervisor' => 'default',
                    'queue' => 'high',
                    'running' => 3,
                    'desired' => 4,
                    'draining' => 1,
                    'pids' => [101, 102, 103],
                    'draining_pids' => [99],
                    'healthy' => true,
                    'ready' => true,
                    'capacity_satisfied' => false,
                    'restart_state' => 'closed',
                    'restart_failures' => 0,
                    'restart_in_seconds' => null,
                    'depth' => 71,
                    'depth_available' => true,
                    'process_cost_per_worker' => 2,
                    'reserved_processes' => 8,
                    'renewal_helpers_reserved' => 4,
                ],
                [
                    'supervisor' => 'default',
                    'queue' => 'default',
                    'running' => 1,
                    'desired' => 1,
                    'draining' => 0,
                    'pids' => [104],
                    'draining_pids' => [],
                    'healthy' => true,
                    'ready' => true,
                    'capacity_satisfied' => true,
                    'restart_state' => 'closed',
                    'restart_failures' => 0,
                    'restart_in_seconds' => null,
                    'depth' => 2,
                    'depth_available' => true,
                    'process_cost_per_worker' => 1,
                    'reserved_processes' => 1,
                    'renewal_helpers_reserved' => 0,
                ],
            ],
        ]);

        $response = $this->getJson('/queen/api/status')->assertOk();
        $response->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('supervisor.ready', true)
            ->assertJsonPath('supervisor.capacity_satisfied', false)
            ->assertJsonPath('supervisor.processing_healthy', false)
            ->assertJsonPath('supervisor.process_budget.valid', true)
            ->assertJsonPath('supervisor.process_budget.limit', 256)
            ->assertJsonPath('supervisor.process_budget.used', 9)
            ->assertJsonPath('supervisor.process_budget.available', 247)
            ->assertJsonPath('supervisor.process_budget.active_worker_processes', 4)
            ->assertJsonPath('supervisor.process_budget.draining_worker_processes', 1)
            ->assertJsonPath('supervisor.process_budget.renewal_helpers_reserved', 4);
        $high = collect($response->json('supervisor.pools'))->firstWhere('queue', 'high');
        $this->assertIsArray($high);
        $this->assertTrue($high['ready']);
        $this->assertFalse($high['capacity_satisfied']);
        $this->assertSame(8, $high['reserved_processes']);
        $this->assertSame(4, $high['renewal_helpers_reserved']);

        $pages = $this->allSectionPages();
        foreach (['Live', 'Ready', 'Processing health is degraded', '9 / 256', '247 available', 'Reserved / helpers', '8 / 4'] as $text) {
            $this->assertStringContainsString($text, $pages);
        }
        $this->assertStringNotContainsString(' style=', $pages);

        $status = $state->status();
        $this->assertIsArray($status);
        $status['capacity_satisfied'] = true;
        $status['pool_status'][0]['desired'] = 3;
        $status['pool_status'][0]['capacity_satisfied'] = true;
        $state->writeStatus($status);
        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.ready', true)
            ->assertJsonPath('supervisor.capacity_satisfied', true)
            ->assertJsonPath('supervisor.processing_healthy', true);

        $degraded = $state->status();
        $this->assertIsArray($degraded);
        $degraded['pool_status'][0]['healthy'] = false;
        $degraded['pool_status'][0]['restart_state'] = 'probe';
        $degraded['pool_status'][0]['restart_failures'] = 1;
        $state->writeStatus($degraded);
        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.ready', true)
            ->assertJsonPath('supervisor.capacity_satisfied', true)
            ->assertJsonPath('supervisor.processing_healthy', false);
    }

    public function testDashboardHealthAndProcessBudgetFailClosedOnMalformedOrInconsistentStatus(): void
    {
        $this->liveSupervisor([
            'engine' => 'php',
            'state' => 'running',
            'ready' => 'true',
            'capacity_satisfied' => 1,
            'draining' => 0,
            'process_budget' => [
                'limit' => 256,
                'used' => PHP_INT_MAX,
                'available' => 0,
                'active_worker_processes' => 2,
                'draining_worker_processes' => 0,
                'renewal_helpers_reserved' => 0,
            ],
            'pool_status' => [
                [
                    'supervisor' => 'default',
                    'queue' => 'high',
                    'running' => 1,
                    'desired' => 1,
                    'draining' => 0,
                    'healthy' => true,
                    'ready' => true,
                    'capacity_satisfied' => true,
                    'restart_state' => 'closed',
                    'restart_failures' => 0,
                    'depth' => 1,
                    'depth_available' => true,
                    'process_cost_per_worker' => 1,
                    'reserved_processes' => 1,
                    'renewal_helpers_reserved' => 0,
                ],
                [
                    'supervisor' => 'default',
                    'queue' => 'default',
                    'running' => 1,
                    'desired' => 1,
                    'draining' => 0,
                    'healthy' => true,
                    'ready' => true,
                    'capacity_satisfied' => true,
                    'restart_state' => 'closed',
                    'restart_failures' => 0,
                    'depth' => 1,
                    'depth_available' => true,
                    'process_cost_per_worker' => 1,
                    'reserved_processes' => 1,
                    'renewal_helpers_reserved' => 0,
                ],
            ],
        ]);

        $response = $this->getJson('/queen/api/status')->assertOk();
        $response->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('supervisor.ready', false)
            ->assertJsonPath('supervisor.capacity_satisfied', false)
            ->assertJsonPath('supervisor.processing_healthy', false)
            ->assertJsonPath('supervisor.process_budget.valid', false)
            ->assertJsonPath('supervisor.process_budget.limit', 256)
            ->assertJsonPath('supervisor.process_budget.used', null)
            ->assertJsonPath('supervisor.process_budget.available', null)
            ->assertJsonPath('supervisor.process_budget.renewal_helpers_reserved', null);
        $this->assertStringNotContainsString((string) PHP_INT_MAX, $response->getContent());

        $this->get('/queen')->assertOk()
            ->assertSee('Supervisor live, but not ready')
            ->assertSee('Report unavailable');
    }

    public function testVersionedStylesheetHasIntegrityImmutableCachingAndConditionalRequests(): void
    {
        $page = $this->get('/queen')->assertOk();
        $xpath = $this->dashboardXPath($page->getContent());
        $link = $xpath->query('//link[@rel="stylesheet"]')->item(0);
        $this->assertInstanceOf(\DOMElement::class, $link);

        $stylesheetUrl = $link->getAttribute('href');
        $integrity = $link->getAttribute('integrity');
        $this->assertSame(1, preg_match(
            '#^/queen/assets/dashboard-([a-f0-9]{64})\.css$#D',
            $stylesheetUrl,
            $urlMatch,
        ));
        $this->assertMatchesRegularExpression('#^sha256-[A-Za-z0-9+/]{43}=$#D', $integrity);

        $stylesheet = $this->get($stylesheetUrl)->assertOk()
            ->assertHeader('Content-Type', 'text/css; charset=UTF-8')
            ->assertHeader('X-Content-Type-Options', 'nosniff');
        $css = $stylesheet->getContent();
        $this->assertStringContainsString('.topbar', $css);
        $this->assertSame(hash('sha256', $css), $urlMatch[1]);
        $this->assertSame(
            'sha256-' . base64_encode(hash('sha256', $css, true)),
            $integrity,
        );

        $cacheControl = (string) $stylesheet->headers->get('Cache-Control');
        $this->assertStringContainsString('private', $cacheControl);
        $this->assertStringContainsString('max-age=31536000', $cacheControl);
        $this->assertStringContainsString('immutable', $cacheControl);
        $this->assertStringContainsString('no-transform', $cacheControl);
        $this->assertStringNotContainsString('public', $cacheControl);
        $this->assertStringNotContainsString('no-store', $cacheControl);
        $this->assertSame(
            "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self'; style-src-attr 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
            $stylesheet->headers->get('Content-Security-Policy'),
        );

        $etag = $stylesheet->headers->get('ETag');
        $this->assertIsString($etag);
        $this->assertSame('"' . $urlMatch[1] . '"', $etag);
        $notModified = $this->withHeaders(['If-None-Match' => $etag])->get($stylesheetUrl);
        $notModified->assertStatus(304)->assertHeader('ETag', $etag);
        $this->assertSame('', $notModified->getContent());
        $this->assertStringContainsString(
            'immutable',
            (string) $notModified->headers->get('Cache-Control'),
        );
        $this->assertStringContainsString(
            'private',
            (string) $notModified->headers->get('Cache-Control'),
        );
    }

    public function testStylesheetRejectsAnUnknownContentHash(): void
    {
        $response = $this->get('/queen/assets/dashboard-' . str_repeat('0', 64) . '.css')
            ->assertNotFound();
        $cacheControl = (string) $response->headers->get('Cache-Control');
        $this->assertStringContainsString('no-store', $cacheControl);
        $this->assertStringNotContainsString('immutable', $cacheControl);
    }

    public function testPausedDashboardEnablesOnlyApplicableSupervisorControls(): void
    {
        $this->liveSupervisor([
            'engine' => 'rust',
            'state' => 'paused',
            'pool_status' => [],
        ]);

        $response = $this->get('/queen/supervisors')->assertOk();
        $response->assertSee('Paused');

        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertDashboardButtonState($xpath, 'Pause', true);
        $this->assertDashboardButtonState($xpath, 'Continue', false);
        $this->assertDashboardButtonState($xpath, 'Terminate', false);
    }

    public function testStaleDashboardDoesNotPresentRunningStateOrEnableControls(): void
    {
        $state = $this->liveSupervisor([
            'engine' => 'rust',
            'state' => 'running',
            'pool_status' => [],
        ]);
        $status = $state->status();
        $this->assertIsArray($status);
        $status['updated_at_epoch'] = time() - 3601;
        $status['updated_at'] = gmdate('Y-m-d\TH:i:s\Z', $status['updated_at_epoch']);
        file_put_contents(
            $this->stateDirectory . '/status.json',
            json_encode($status, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
        );

        $response = $this->get('/queen/supervisors')->assertOk();
        $response->assertSee('Stale')->assertDontSee('Active');
        $this->get('/queen')->assertOk()->assertSee('Stale')->assertDontSee('Active');

        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertDashboardButtonState($xpath, 'Pause', true);
        $this->assertDashboardButtonState($xpath, 'Continue', true);
        $this->assertDashboardButtonState($xpath, 'Terminate', true);
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
        $stylesheetUrl = '/queen/assets/dashboard-'
            . $this->app->make(DashboardStylesheet::class)->version()
            . '.css';
        $this->app['env'] = 'production';
        $this->get('/queen')->assertForbidden();
        $forbiddenStylesheet = $this->get($stylesheetUrl)->assertForbidden();
        $forbiddenCacheControl = (string) $forbiddenStylesheet->headers->get('Cache-Control');
        $this->assertStringContainsString('no-store', $forbiddenCacheControl);
        $this->assertStringNotContainsString('immutable', $forbiddenCacheControl);

        Gate::define('viewQueenDashboard', static fn (?Authenticatable $user): bool => false);
        $this->get('/queen')->assertForbidden();

        Gate::define('viewQueenDashboard', static fn (?Authenticatable $user): bool => true);
        $this->get('/queen')->assertOk();
        $this->get($stylesheetUrl)->assertOk();
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
        $stylesheetUrl = '/queen/assets/dashboard-'
            . $this->app->make(DashboardStylesheet::class)->version()
            . '.css';
        $this->app['config']->set('queen.dashboard.enabled', false);

        $this->get('/queen')->assertNotFound();
        $this->get($stylesheetUrl)->assertNotFound();
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
            ->assertHeader('Location', '/queen/supervisors')
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
        $conflict = $this->post('/queen/control/pause', ['instance_id' => str_repeat('a', 32)])
            ->assertStatus(409);
        $conflictCsp = (string) $conflict->headers->get('Content-Security-Policy');
        $this->assertStringContainsString("style-src 'self'", $conflictCsp);
        $conflictXPath = $this->dashboardXPath($conflict->getContent());
        $conflictStylesheet = $conflictXPath->query('//link[@rel="stylesheet"]')->item(0);
        $this->assertInstanceOf(\DOMElement::class, $conflictStylesheet);
        $this->assertMatchesRegularExpression(
            '#^/queen/assets/dashboard-[a-f0-9]{64}\.css$#D',
            $conflictStylesheet->getAttribute('href'),
        );
        $this->assertMatchesRegularExpression(
            '#^sha256-[A-Za-z0-9+/]{43}=$#D',
            $conflictStylesheet->getAttribute('integrity'),
        );
        $this->assertSame(0, $conflictXPath->query('//style|//script[not(@src)]|//*[@style]')->length);

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

        $response = $this->get('/queen/failed-jobs')->assertOk();
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

    public function testRemoteStatusShowsASupervisorRunningOnAnotherHostReadOnly(): void
    {
        $handler = $this->remoteSupervisor([
            'engine' => 'php',
            'state' => 'running',
            'ready' => true,
            'capacity_satisfied' => true,
            'pool_status' => [[
                'supervisor' => 'default',
                'queue' => 'high',
                'running' => 2,
                'desired' => 2,
                'pids' => [201, 202],
                'depth' => 9,
                'depth_available' => true,
            ]],
        ]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('supervisor.source', 'remote')
            ->assertJsonPath('supervisor.controls_available', false)
            ->assertJsonPath('supervisor.engine', 'php')
            ->assertJsonPath('supervisor.workers', 2)
            ->assertJsonPath('queues.0.depth', 9);

        $request = $handler->requests[0];
        $this->assertSame('/api/v1/kv', $request->getUri()->getPath());
        $this->assertSame(
            [['op' => 'getPrefix', 'ns' => 'queen-supervisor', 'prefix' => 'orders/', 'limit' => 100]],
            json_decode((string) $request->getBody(), true)['operations'],
        );

        $response = $this->get('/queen/supervisors')->assertOk();
        $response->assertSee('published through the broker')
            ->assertSee('Read-only: this supervisor runs on another host.');
        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertSame(0, $xpath->query('//form[contains(@action, "/queen/control/")]')->length);
    }

    public function testRemoteStatusWithAnOldHeartbeatIsStale(): void
    {
        $this->remoteSupervisor(['engine' => 'rust', 'state' => 'running', 'pool_status' => []], time() - 3601);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.availability', 'stale')
            ->assertJsonPath('supervisor.source', 'remote');
    }

    public function testALiveLocalSupervisorTakesPrecedenceOverTheRemoteCopy(): void
    {
        $this->remoteSupervisor(['engine' => 'rust', 'state' => 'paused', 'pool_status' => []]);
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.source', 'local')
            ->assertJsonPath('supervisor.controls_available', true)
            ->assertJsonPath('supervisor.state', 'running');
    }

    public function testControlsAreRefusedForARemoteSupervisor(): void
    {
        $this->remoteSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $this->post('/queen/control/pause', ['instance_id' => str_repeat('c', 32)])
            ->assertStatus(409)
            ->assertSee('This supervisor runs on another host.');
    }

    public function testAnUnreachableBrokerLeavesTheRemoteSupervisorUnavailable(): void
    {
        $this->app['config']->set('queen.supervisor.remote_status', ['enabled' => true, 'key' => 'orders']);
        $this->app->instance(RemoteStatusReader::class, new RemoteStatusReader(
            new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create(new PlanHandler([], ['status' => 503, 'json' => []])),
            ]),
            'queen-supervisor',
            'orders',
        ));

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.availability', 'unavailable')
            ->assertJsonPath('supervisor.source', null);
    }

    public function testEachPageRefreshesItselfAndIgnoresUnknownQueryParameters(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $xpath = $this->dashboardXPath($this->get('/queen/supervisors?view=../../etc&x=1')->assertOk()->getContent());

        $this->assertSame('5;url=/queen/supervisors', $xpath->query('//noscript/meta[@http-equiv="refresh"]')->item(0)->getAttribute('content'));
        $this->assertSame('Supervisors · Queen Supervisor', trim($xpath->query('//title')->item(0)->textContent));
        $this->assertSame('Supervisors', trim($xpath->query('//h1')->item(0)->textContent));
    }

    public function testTheFooterNamesWhereTheSupervisorStateComesFrom(): void
    {
        $this->remoteSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $this->get('/queen')->assertOk()
            ->assertSee('supervisor state published through the broker')
            ->assertDontSee('local supervisor state only');
    }

    public function testVersionedScriptIsServedWithIntegrityAndImmutableCaching(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $script = $this->app->make(DashboardScript::class);

        $response = $this->get('/queen/assets/dashboard-' . $script->version() . '.js')->assertOk();
        $this->assertSame('text/javascript; charset=UTF-8', $response->headers->get('Content-Type'));
        $this->assertStringContainsString('immutable', (string) $response->headers->get('Cache-Control'));
        $this->assertSame($script->contents(), $response->getContent());
        $this->assertSame(
            'sha256-' . base64_encode(hash('sha256', (string) $response->getContent(), true)),
            $script->integrity(),
        );

        $this->withHeader('If-None-Match', (string) $response->headers->get('ETag'))
            ->get('/queen/assets/dashboard-' . $script->version() . '.js')
            ->assertStatus(304);
        $this->get('/queen/assets/dashboard-' . str_repeat('0', 64) . '.js')->assertNotFound();
    }

    public function testThePauseControlIsRenderedForTheScriptToEnable(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $xpath = $this->dashboardXPath($this->get('/queen')->assertOk()->getContent());

        $toggle = $xpath->query('//button[@data-refresh-toggle]')->item(0);
        $this->assertInstanceOf(\DOMElement::class, $toggle);
        $this->assertTrue($toggle->hasAttribute('hidden'), 'Without the script the control must not appear.');
        $this->assertSame('false', $toggle->getAttribute('aria-pressed'));
        $this->assertSame('5', $xpath->query('//body')->item(0)->getAttribute('data-refresh-seconds'));
        $this->assertSame(1, $xpath->query('//*[@data-refresh-state]')->length);
    }

    public function testAPublishedScriptIsLinkedWhileItMatchesThePackage(): void
    {
        $script = $this->app->make(DashboardScript::class);
        $packaged = $script->contents();
        $this->usePublishedAsset(DashboardScript::PUBLISHED_FILE, $packaged);
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $element = $this->dashboardXPath($this->get('/queen')->assertOk()->getContent())
            ->query('//script[@src]')->item(0);

        $this->assertInstanceOf(\DOMElement::class, $element);
        $this->assertSame('/vendor/queen/dashboard.js?v=' . hash('sha256', $packaged), $element->getAttribute('src'));
    }

    public function testAPublishedStylesheetIsLinkedWhileItMatchesThePackage(): void
    {
        $stylesheet = $this->app->make(DashboardStylesheet::class);
        $packaged = $stylesheet->contents();
        $this->usePublishedStylesheet($packaged);
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $link = $this->dashboardXPath($this->get('/queen')->assertOk()->getContent())
            ->query('//link[@rel="stylesheet"]')->item(0);

        $this->assertInstanceOf(\DOMElement::class, $link);
        $this->assertSame(
            '/vendor/queen/dashboard.css?v=' . hash('sha256', $packaged),
            $link->getAttribute('href'),
        );
        $this->assertSame('sha256-' . base64_encode(hash('sha256', $packaged, true)), $link->getAttribute('integrity'));
    }

    public function testAStalePublishedStylesheetFallsBackToThePackageRoute(): void
    {
        $this->usePublishedStylesheet('/* left behind by an older release */');
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $link = $this->dashboardXPath($this->get('/queen')->assertOk()->getContent())
            ->query('//link[@rel="stylesheet"]')->item(0);

        $this->assertInstanceOf(\DOMElement::class, $link);
        $this->assertMatchesRegularExpression('#^/queen/assets/dashboard-[a-f0-9]{64}\.css$#D', $link->getAttribute('href'));
    }

    public function testTheAssetsArePublishableWithTheQueenAndLaravelAssetTags(): void
    {
        foreach (['queen-assets', 'laravel-assets'] as $tag) {
            $paths = array_map('realpath', array_flip(ServiceProvider::pathsToPublish(QueenServiceProvider::class, $tag)));
            foreach ([
                'resources/css/dashboard.css' => 'vendor/queen/dashboard.css',
                'resources/js/dashboard.js' => 'vendor/queen/dashboard.js',
            ] as $source => $target) {
                $published = array_search(realpath(__DIR__ . '/../' . $source), $paths, true);
                $this->assertSame(public_path($target), $published, "{$tag}: {$source}");
            }
        }
    }

    public function testDatabaseFailedJobsArePagedWithAKeysetCursorAndNeverAnOffsetOrCount(): void
    {
        $this->failedJobsTable(5);
        $queries = [];
        DB::listen(static function ($query) use (&$queries): void {
            $queries[] = strtolower($query->sql);
        });

        $first = $this->dashboardXPath($this->get('/queen/failed-jobs')->assertOk()->getContent());
        $this->assertSame(['5', '4'], $this->failedJobIds($first));
        $this->assertSame('/queen/failed-jobs?cursor=4', $first->query('//nav[@class="pager"]/a[@rel="next"]')->item(0)->getAttribute('href'));
        $this->assertSame(0, $first->query('//nav[@class="pager"]/a[normalize-space(.)="Newest"]')->length);

        $second = $this->dashboardXPath($this->get('/queen/failed-jobs?cursor=4')->assertOk()->getContent());
        $this->assertSame(['3', '2'], $this->failedJobIds($second));
        $this->assertSame('/queen/failed-jobs?cursor=2', $second->query('//nav[@class="pager"]/a[@rel="next"]')->item(0)->getAttribute('href'));
        $this->assertSame('/queen/failed-jobs', $second->query('//nav[@class="pager"]/a[normalize-space(.)="Newest"]')->item(0)->getAttribute('href'));
        // The refresh keeps the reader on the page they are reading.
        $this->assertSame('5;url=/queen/failed-jobs?cursor=4', $second->query('//noscript/meta[@http-equiv="refresh"]')->item(0)->getAttribute('content'));

        $last = $this->dashboardXPath($this->get('/queen/failed-jobs?cursor=2')->assertOk()->getContent());
        $this->assertSame(['1'], $this->failedJobIds($last));
        $this->assertSame(0, $last->query('//nav[@class="pager"]/a[@rel="next"]')->length);

        $this->assertNotSame([], $queries);
        foreach ($queries as $query) {
            $this->assertStringNotContainsString('offset', $query);
            $this->assertStringNotContainsString('count(', $query);
        }
    }

    public function testFileFailedJobsArePagedByPosition(): void
    {
        $this->failedJobsFile(3);

        $first = $this->dashboardXPath($this->get('/queen/failed-jobs')->assertOk()->getContent());
        $this->assertSame(['failed-1', 'failed-2'], $this->failedJobIds($first));
        $this->assertSame('/queen/failed-jobs?cursor=2', $first->query('//nav[@class="pager"]/a[@rel="next"]')->item(0)->getAttribute('href'));

        $second = $this->dashboardXPath($this->get('/queen/failed-jobs?cursor=2')->assertOk()->getContent());
        $this->assertSame(['failed-3'], $this->failedJobIds($second));
        $this->assertSame(0, $second->query('//nav[@class="pager"]/a[@rel="next"]')->length);
    }

    public function testAMalformedCursorShowsTheNewestPage(): void
    {
        $this->failedJobsTable(3);

        foreach (['abc', '0', '-4', '4 OR 1=1', str_repeat('9', 19)] as $cursor) {
            $xpath = $this->dashboardXPath($this->get('/queen/failed-jobs?cursor=' . rawurlencode($cursor))->assertOk()->getContent());
            $this->assertSame(['3', '2'], $this->failedJobIds($xpath), "Cursor {$cursor}");
            $this->assertSame('5;url=/queen/failed-jobs', $xpath->query('//noscript/meta[@http-equiv="refresh"]')->item(0)->getAttribute('content'));
        }
    }

    public function testOtherPagesIgnoreTheFailedJobsCursor(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        $xpath = $this->dashboardXPath($this->get('/queen/supervisors?cursor=4')->assertOk()->getContent());

        $this->assertSame('5;url=/queen/supervisors', $xpath->query('//noscript/meta[@http-equiv="refresh"]')->item(0)->getAttribute('content'));
    }

    public function testFailedJobDetailShowsWhyItFailedWithoutThePayload(): void
    {
        $secret = 'QUEEN_PAYLOAD_SECRET';
        $basePath = rtrim($this->app->basePath(), '/');
        $exception = "RuntimeException: Mail server refused the connection in {$basePath}/app/Jobs/SendInvoice.php:42\n"
            . "Stack trace:\n"
            . "#0 {$basePath}/vendor/laravel/framework/src/Illuminate/Queue/CallQueuedHandler.php(134): App\\Jobs\\SendInvoice->handle()\n"
            . '#1 {main}';
        $this->failedJobsFile(1, [
            'payload' => json_encode([
                'uuid' => '9b2c1d3e-0000-4000-8000-000000000001',
                'displayName' => 'App\\Jobs\\SendInvoice',
                'attempts' => 3,
                'maxTries' => 3,
                'timeout' => 60,
                'data' => ['command' => "serialized-{$secret}"],
            ], JSON_THROW_ON_ERROR),
            'exception' => $exception,
        ]);

        $list = $this->dashboardXPath($this->get('/queen/failed-jobs')->assertOk()->getContent());
        $this->assertSame('/queen/failed-jobs/failed-1', $list->query('//table//a')->item(0)->getAttribute('href'));

        $response = $this->get('/queen/failed-jobs/failed-1')->assertOk();
        $response->assertSee('RuntimeException')
            ->assertSee('App\\Jobs\\SendInvoice')
            ->assertSee('Mail server refused the connection in app/Jobs/SendInvoice.php:42')
            ->assertSee('vendor/laravel/framework/src/Illuminate/Queue/CallQueuedHandler.php(134)')
            ->assertSee('3 of 3')
            ->assertSee('60s')
            ->assertSee('php artisan queue:retry failed-1')
            ->assertDontSee($secret)
            ->assertDontSee($basePath . '/');
        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertSame('Failed job · Queen Supervisor', trim($xpath->query('//title')->item(0)->textContent));
        $this->assertStringStartsWith('RuntimeException: Mail server refused', trim($xpath->query('//pre[@class="exception-summary"]')->item(0)->textContent));
        $this->assertStringStartsWith('Stack trace:', trim($xpath->query('//details/pre[@class="exception-trace"]')->item(0)->textContent));
        $this->assertSame('/queen/failed-jobs', $xpath->query('//nav//a[@aria-current="page"]')->item(0)?->getAttribute('href'));
        // A failed job does not change: no refresh of any kind.
        $this->assertFalse($xpath->query('//body')->item(0)->hasAttribute('data-refresh-seconds'));
        $this->assertSame(0, $xpath->query('//noscript/meta[@http-equiv="refresh"]')->length);
    }

    public function testFailedJobDetailReadsDatabaseStoresByTheirLaravelIdentifier(): void
    {
        $this->failedJobsTable(2, uuids: true);

        $this->get('/queen/failed-jobs/00000000-0000-4000-8000-000000000002')->assertOk()
            ->assertSee('RuntimeException')
            ->assertSee('Failure 2');
        // The database id is not the identifier of a database-uuids store.
        $this->get('/queen/failed-jobs/2')->assertNotFound();
    }

    public function testUnknownOrMalformedFailedJobIdentifiersAreNotFound(): void
    {
        $this->failedJobsTable(1);

        $this->get('/queen/failed-jobs/99')->assertNotFound();
        $this->get('/queen/failed-jobs/not-a-number')->assertNotFound();
        $this->get('/queen/failed-jobs/' . rawurlencode('<script>'))->assertNotFound();
        $this->get('/queen/failed-jobs/' . str_repeat('a', 129))->assertNotFound();
        $this->get('/queen/failed-jobs/1')->assertOk();
    }

    public function testAnIdentifierTheDetailRouteCannotCarryIsListedWithoutALink(): void
    {
        $this->failedJobsFile(1, ['id' => 'failed job/1']);

        $xpath = $this->dashboardXPath($this->get('/queen/failed-jobs')->assertOk()->getContent());

        $this->assertSame(['failed job/1'], $this->failedJobIds($xpath));
        $this->assertSame(0, $xpath->query('//table//a')->length);
    }

    public function testFailedJobDetailIsAuthorizedLikeTheDashboard(): void
    {
        $this->failedJobsFile(1);
        Gate::define('viewQueenDashboard', static fn (?Authenticatable $user = null): bool => false);

        $this->get('/queen/failed-jobs/failed-1')->assertForbidden();
    }

    private function failedJobsTable(int $rows, bool $uuids = false): void
    {
        Schema::create('failed_jobs', function (Blueprint $table): void {
            $table->bigIncrements('id');
            $table->string('uuid')->unique();
            $table->string('connection');
            $table->string('queue');
            $table->longText('payload');
            $table->longText('exception');
            $table->timestamp('failed_at');
        });
        DB::table('failed_jobs')->insert(array_map(static fn (int $id): array => [
            'uuid' => sprintf('00000000-0000-4000-8000-%012d', $id),
            'connection' => 'queen',
            'queue' => "queue-{$id}",
            'payload' => json_encode(['displayName' => 'App\\Jobs\\Example'], JSON_THROW_ON_ERROR),
            'exception' => "RuntimeException: Failure {$id}\nStack trace:\n#0 {main}",
            'failed_at' => '2026-08-29 10:00:00',
        ], range(1, $rows)));
        $this->app['config']->set('queue.failed', [
            'driver' => $uuids ? 'database-uuids' : 'database',
            'database' => 'testing',
            'table' => 'failed_jobs',
        ]);
    }

    /** @param array<string, mixed> $overrides applied to every record */
    private function failedJobsFile(int $records, array $overrides = []): void
    {
        file_put_contents($this->failedPath, json_encode(array_map(static fn (int $id): array => array_replace([
            'id' => "failed-{$id}",
            'connection' => 'queen',
            'queue' => 'default',
            'payload' => '{}',
            'exception' => "RuntimeException: Failure {$id}",
            'failed_at' => '2026-08-29 10:00:00',
        ], $overrides), range(1, $records)), JSON_THROW_ON_ERROR));
    }

    /** @return list<string> */
    private function failedJobIds(\DOMXPath $xpath): array
    {
        $ids = [];
        foreach ($xpath->query('//table/tbody/tr/td[1]') as $cell) {
            $ids[] = trim($cell->textContent);
        }

        return $ids;
    }

    private function allSectionPages(): string
    {
        $content = '';
        foreach (['/queen', '/queen/workload', '/queen/supervisors', '/queen/failed-jobs', '/queen/configuration'] as $url) {
            $content .= $this->get($url)->assertOk()->getContent();
        }

        return $content;
    }

    private function usePublishedStylesheet(string $contents): void
    {
        $this->usePublishedAsset(DashboardStylesheet::PUBLISHED_FILE, $contents);
    }

    private function usePublishedAsset(string $file, string $contents): void
    {
        $publicPath = sys_get_temp_dir() . '/queen-dashboard-public-' . bin2hex(random_bytes(6));
        mkdir(dirname($publicPath . '/' . $file), 0755, true);
        file_put_contents($publicPath . '/' . $file, $contents);
        $this->beforeApplicationDestroyed(function () use ($publicPath): void {
            $this->removeDirectory($publicPath);
        });
        $this->app->usePublicPath($publicPath);
        $this->app->forgetInstance(DashboardStylesheet::class);
        $this->app->forgetInstance(DashboardScript::class);
    }

    /** @param array<string, mixed> $status */
    private function remoteSupervisor(array $status, ?int $updatedAtEpoch = null): PlanHandler
    {
        $this->app['config']->set('queen.supervisor.remote_status', ['enabled' => true, 'key' => 'orders']);
        $updatedAtEpoch ??= time();
        $document = array_replace([
            'configuration' => $this->statusConfiguration(),
        ], $status, [
            'schema' => SupervisorState::STATUS_SCHEMA,
            'updated_at' => gmdate('Y-m-d\TH:i:s\Z', $updatedAtEpoch),
            'updated_at_epoch' => $updatedAtEpoch,
            'pid' => 4242,
            'instance_id' => str_repeat('c', 32),
            'paused' => ($status['state'] ?? null) === 'paused',
            'stopping' => ($status['state'] ?? null) === 'terminating',
        ]);
        $operations = RemoteStatusDocument::operations($document, 'queen-supervisor', 'orders', 600, str_repeat('d', 32));
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['results' => [[
            'rows' => array_map(fn (array $op): array => ['key' => $op['key'], 'value' => $op['value']], $operations),
            'truncated' => false,
            'nextAfter' => null,
        ]]]]);
        $this->app->instance(RemoteStatusReader::class, new RemoteStatusReader(
            new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create($handler),
            ]),
            'queen-supervisor',
            'orders',
        ));

        return $handler;
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

    private function dashboardXPath(string $content): \DOMXPath
    {
        $document = new \DOMDocument();
        $previous = libxml_use_internal_errors(true);
        try {
            $this->assertTrue($document->loadHTML($content, LIBXML_NONET | LIBXML_NOERROR | LIBXML_NOWARNING));
        } finally {
            libxml_clear_errors();
            libxml_use_internal_errors($previous);
        }

        return new \DOMXPath($document);
    }

    private function assertDashboardButtonState(\DOMXPath $xpath, string $label, bool $disabled): void
    {
        $button = $xpath->query('//button[normalize-space(.)="' . $label . '"]')->item(0);
        $this->assertInstanceOf(\DOMElement::class, $button, "Expected the {$label} dashboard button.");
        $this->assertSame($disabled, $button->hasAttribute('disabled'), "Unexpected disabled state for {$label}.");
    }
}
