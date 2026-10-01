<?php

namespace Queen\Tests;

use Illuminate\Contracts\Auth\Authenticatable;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Gate;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\ServiceProvider;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\Dashboard\DashboardScript;
use Queen\Laravel\Dashboard\DashboardStylesheet;
use Queen\Laravel\Dashboard\QueueContentsReader;
use Queen\Laravel\Dashboard\RemoteStatusReader;
use Queen\Laravel\Dashboard\ThroughputReader;
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

    /** @var list<RequestInterface> */
    private array $throughputRequests = [];

    /** @var list<RequestInterface> */
    private array $queueContentsRequests = [];

    /** 2026-09-29T15:03:30Z */
    private const THROUGHPUT_NOW = 1790694210;

    protected function setUp(): void
    {
        $suffix = bin2hex(random_bytes(8));
        $this->stateDirectory = sys_get_temp_dir() . '/queen-dashboard-state-' . $suffix;
        $this->failedPath = sys_get_temp_dir() . '/queen-dashboard-failed-' . $suffix . '.json';
        parent::setUp();
        // No test reaches a real broker for the throughput counters, the
        // queue contents or the job metrics.
        $this->throughputBroker([]);
        $this->queueContentsBroker([]);
        $this->jobMetricsBroker([]);
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
        $app['config']->set('cache.default', 'array');
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

    public function testALiveLocalSupervisorTakesPrecedenceOverItsOwnPublishedCopy(): void
    {
        $state = $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->remoteSupervisor([
            'engine' => 'php',
            'state' => 'paused',
            'instance_id' => $state->instanceId(),
            'pool_status' => [],
        ]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonCount(1, 'instances')
            ->assertJsonPath('supervisor.instances', 1)
            ->assertJsonPath('supervisor.source', 'local')
            ->assertJsonPath('supervisor.controls_available', true)
            ->assertJsonPath('supervisor.state', 'running');
    }

    public function testEveryPodPublishingToTheKeyIsListedAndSummed(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $pool = fn (int $running, int $depth): array => [[
            'supervisor' => 'default',
            'queue' => 'high',
            'running' => $running,
            'desired' => $running,
            'draining' => 0,
            'pids' => range(10, 9 + $running),
            'ready' => true,
            'capacity_satisfied' => true,
            'depth' => $depth,
            'depth_available' => true,
        ]];
        $this->remoteSupervisors([
            $this->remoteDocument([
                'engine' => 'rust',
                'state' => 'running',
                'ready' => true,
                'capacity_satisfied' => true,
                'configuration' => $configuration,
                'instance_id' => '000000000000000018d9e392917a928f00000001',
                'hostname' => 'orders-worker-7d9f-a',
                'pid' => 1,
                'pool_status' => $pool(2, 5),
            ], time() - 1),
            $this->remoteDocument([
                'engine' => 'rust',
                'state' => 'running',
                'ready' => true,
                'capacity_satisfied' => true,
                'configuration' => $configuration,
                'instance_id' => '000000000000000018d9e392a0c3417700000001',
                'hostname' => 'orders-worker-7d9f-b',
                'pid' => 1,
                'pool_status' => $pool(3, 7),
            ]),
        ]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonCount(2, 'instances')
            ->assertJsonPath('instances.0.hostname', 'orders-worker-7d9f-b')
            ->assertJsonPath('instances.1.hostname', 'orders-worker-7d9f-a')
            ->assertJsonPath('instances.0.workers', 3)
            ->assertJsonPath('supervisor.instances', 2)
            ->assertJsonPath('supervisor.live_instances', 2)
            ->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('supervisor.engine', 'rust')
            ->assertJsonPath('supervisor.state', 'running')
            ->assertJsonPath('supervisor.ready', true)
            ->assertJsonPath('supervisor.workers', 5)
            ->assertJsonPath('supervisor.instance_id', null)
            ->assertJsonPath('supervisor.controls_available', false)
            // Both pods sample the queue; the freshest sample is shown.
            ->assertJsonPath('queues.0.depth', 7)
            // Two masters on one consumer group both scale to their maximum.
            ->assertJsonPath('shared_queues', [[
                'connection' => 'queen',
                'consumer_group' => 'laravel',
                'queue' => 'high',
                'instances' => 2,
            ]]);

        $response = $this->get('/queen/supervisors')->assertOk()
            ->assertSeeInOrder([
                'Supervisor instance 1 of 2',
                'orders-worker-7d9f-b',
                '000000000000000018d9e392a0c3417700000001',
                'Supervisor instance 2 of 2',
                'orders-worker-7d9f-a',
                '000000000000000018d9e392917a928f00000001',
            ])
            ->assertSee('2 supervisor instances');
        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertSame(2, $xpath->query('//section[@aria-labelledby and .//h2[starts-with(normalize-space(.), "Supervisor instance")]]')->length);
        $this->assertSame(1, $xpath->query('//*[@id="supervisors"]')->length);
        $this->assertSame(0, $xpath->query('//form[contains(@action, "/queen/control/")]')->length);

        $overviewResponse = $this->get('/queen')->assertOk()
            ->assertSee('2 running supervisors share queue high of consumer group laravel')
            ->assertSee('Enable QUEEN_SUPERVISOR_COORDINATION on every replica so they share one target.');
        $overview = $this->dashboardXPath($overviewResponse->getContent());
        $this->assertSame('All 2 supervisor instances live', trim($overview->query('//dt[.="Liveness"]/following-sibling::small')->item(0)->textContent));
    }

    public function testCoordinatedReplicasShareOneTargetAndAreNotWarnedAbout(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $pod = fn (string $id, string $host, ?int $replicas): array => $this->remoteDocument([
            'engine' => 'rust',
            'state' => 'running',
            'configuration' => $configuration,
            'instance_id' => $id,
            'hostname' => $host,
            'pool_status' => [[
                'supervisor' => 'default',
                'queue' => 'high',
                'running' => 3,
                'desired' => 3,
                'replicas' => $replicas,
            ]],
        ]);
        $this->remoteSupervisors([$pod(str_repeat('a', 32), 'pod-a', 2), $pod(str_repeat('b', 32), 'pod-b', 2)]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('shared_queues', [])
            ->assertJsonPath('instances.0.pools.0.replicas', 2)
            ->assertJsonPath('supervisor.workers', 6);
        $this->get('/queen/supervisors')->assertOk()
            ->assertSee('share of 2 replicas')
            ->assertDontSee('running supervisors share queue');
    }

    public function testReplicasThatCannotSeeEachOtherAreStillWarnedAbout(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $pod = fn (string $id): array => $this->remoteDocument([
            'engine' => 'rust',
            'state' => 'running',
            'configuration' => $configuration,
            'instance_id' => $id,
            'pool_status' => [['supervisor' => 'default', 'queue' => 'high', 'running' => 1, 'desired' => 1, 'replicas' => 1]],
        ]);
        // Both enabled coordination, but each sees only itself: another
        // namespace, or a broker outage.
        $this->remoteSupervisors([$pod(str_repeat('a', 32)), $pod(str_repeat('b', 32))]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('shared_queues.0.queue', 'high')
            ->assertJsonPath('shared_queues.0.instances', 2);
    }

    public function testTwoPoolsOfOneInstanceOnOneQueueAreNotSeveralSupervisors(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $second = $configuration['supervisors'][0];
        $second['name'] = 'second';
        $configuration['supervisors'][] = $second;
        $this->remoteSupervisors([
            $this->remoteDocument(['engine' => 'rust', 'state' => 'running', 'configuration' => $configuration, 'pool_status' => []]),
        ]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.live_instances', 1)
            ->assertJsonCount(2, 'configuration.supervisors')
            ->assertJsonPath('shared_queues', []);
    }

    public function testAReplicaThatDoesNotCoordinateIsStillWarnedAbout(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $pod = fn (string $id, ?int $replicas): array => $this->remoteDocument([
            'engine' => 'rust',
            'state' => 'running',
            'configuration' => $configuration,
            'instance_id' => $id,
            'pool_status' => [['supervisor' => 'default', 'queue' => 'high', 'running' => 1, 'desired' => 1, 'replicas' => $replicas]],
        ]);
        $this->remoteSupervisors([$pod(str_repeat('a', 32), 1), $pod(str_repeat('b', 32), null)]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('shared_queues.0.queue', 'high')
            ->assertJsonPath('shared_queues.0.instances', 2);
    }

    public function testFixedPoolsOnSeveralPodsAreNotWarnedAbout(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $configuration['supervisors'][0]['balance'] = 'simple';
        $configuration['supervisors'][0]['processes'] = 2;
        $configuration['supervisors'][0]['min_processes'] = 1;
        $this->remoteSupervisors([
            $this->remoteDocument(['engine' => 'rust', 'state' => 'running', 'configuration' => $configuration, 'pool_status' => []]),
            $this->remoteDocument([
                'engine' => 'rust',
                'state' => 'running',
                'configuration' => $configuration,
                'instance_id' => str_repeat('f', 32),
                'pool_status' => [],
            ]),
        ]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.live_instances', 2)
            ->assertJsonPath('shared_queues', []);
    }

    public function testTheJobsPageShowsEveryClassFromEveryWorker(): void
    {
        $this->liveSupervisor(['engine' => 'rust', 'state' => 'running', 'pool_status' => []]);
        $this->app->instance(\Queen\Laravel\Dashboard\JobMetricsReader::class, new \Queen\Laravel\Dashboard\JobMetricsReader(
            new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create(new PlanHandler([], ['status' => 200, 'json' => ['results' => [[
                    'rows' => [
                        ['key' => 'jobs/v1/' . sprintf('%010d', intdiv(time(), 300) * 300) . '/aaaa', 'value' => ['classes' => [
                            'App\Jobs\SendInvoice' => ['processed' => 12, 'failed' => 1, 'runtime_ms' => 2600],
                            'App\Jobs\ResizeImage' => ['processed' => 3, 'failed' => 0, 'runtime_ms' => 900],
                        ]]],
                    ],
                    'truncated' => false,
                ]]]])),
            ]),
            'queen-metrics',
        ));

        $response = $this->get('/queen/jobs')->assertOk()
            ->assertSee('Jobs by class')
            ->assertSeeInOrder(['App\Jobs\SendInvoice', '12', 'App\Jobs\ResizeImage', '3'])
            ->assertSee('200 ms');
        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertSame('Jobs', trim($xpath->query('//a[@aria-current="page" and contains(@class, "nav-link")]')->item(0)->textContent));
        $this->get('/queen/jobs?range=24h')->assertOk()->assertSee('Last 24 hours');
    }

    public function testWorkersOfQueenConnectionsRecordJobMetrics(): void
    {
        $this->app['config']->set('queue.connections.queen', ['driver' => 'queen']);
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['results' => [['applied' => true]]]]);
        $queen = new Queen(['url' => 'http://queen.test:6632', 'retryAttempts' => 1, 'retryDelayMillis' => 0, 'handler' => HandlerStack::create($handler)]);
        $this->app->instance(\Queen\Laravel\Monitoring\JobMetricsRecorder::class, new \Queen\Laravel\Monitoring\JobMetricsRecorder(fn (): Queen => $queen, 'queen-metrics'));
        $job = $this->createStub(\Illuminate\Contracts\Queue\Job::class);
        $job->method('resolveName')->willReturn('App\Jobs\SendInvoice');
        $events = $this->app['events'];

        $events->dispatch(new \Illuminate\Queue\Events\JobProcessing('queen', $job));
        $events->dispatch(new \Illuminate\Queue\Events\JobProcessed('queen', $job));
        $events->dispatch(new \Illuminate\Queue\Events\JobProcessing('queen', $job));
        $events->dispatch(new \Illuminate\Queue\Events\JobExceptionOccurred('queen', $job, new \RuntimeException('boom')));
        $events->dispatch(new \Illuminate\Queue\Events\JobFailed('queen', $job, new \RuntimeException('boom')));
        $events->dispatch(new \Illuminate\Queue\Events\JobProcessed('sync', $job));
        $events->dispatch(new \Illuminate\Queue\Events\WorkerStopping(0));

        $puts = [];
        foreach ($handler->requests as $request) {
            foreach (json_decode((string) $request->getBody(), true)['operations'] as $operation) {
                $puts[] = $operation;
            }
        }
        $last = end($puts);
        $this->assertSame('queen-metrics', $last['ns']);
        $this->assertSame(1, $last['value']['classes']['App\Jobs\SendInvoice']['processed']);
        $this->assertSame(1, $last['value']['classes']['App\Jobs\SendInvoice']['failed']);
    }

    public function testTheTagsPageListsMonitoredTagsAndTheirRecentJobs(): void
    {
        $this->liveSupervisor(['engine' => 'rust', 'state' => 'running', 'pool_status' => []]);
        $this->tagMonitor([
            ['status' => 200, 'json' => ['results' => [['found' => true, 'value' => ['App\Models\User:42', 'vip'], 'version' => 2]]]],
            ['status' => 200, 'json' => ['results' => [['rows' => [
                ['key' => 'k', 'value' => ['tag' => 'vip', 'class' => 'App\Jobs\Charge', 'queue' => 'high', 'status' => 'failed', 'attempts' => 3, 'runtime_ms' => 812, 'at' => '2026-09-30T10:00:00Z']],
            ], 'truncated' => false]]]],
        ]);

        $response = $this->get('/queen/tags?tag=vip')->assertOk()
            ->assertSee('App\Models\User:42')
            ->assertSee('Recent jobs tagged')
            ->assertSeeInOrder(['App\Jobs\Charge', 'high', 'Failed', '3', '812 ms']);
        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertSame(1, $xpath->query('//form[contains(@action, "/queen/tags/stop")]/input[@name="tag" and @value="vip"]')->length);
        $this->assertSame(1, $xpath->query('//form[@action="/queen/tags"]//input[@name="tag"]')->length);
    }

    public function testMonitoringATagFromTheDashboardRedirectsToIt(): void
    {
        $handler = $this->tagMonitor([
            ['status' => 200, 'json' => ['results' => [['found' => false]]]],
            ['status' => 200, 'json' => ['results' => [['applied' => true]]]],
        ]);

        $this->post('/queen/tags', ['tag' => 'vip'])
            ->assertStatus(303)
            ->assertHeader('Location', '/queen/tags?tag=vip')
            ->assertSessionHas('queen_dashboard_control_status', 'Monitoring tag [vip].');
        $this->assertSame(['vip'], json_decode((string) $handler->requests[1]->getBody(), true)['operations'][0]['value']);
        $this->post('/queen/tags', ['tag' => ''])->assertStatus(422);
    }

    /** @param list<array<string, mixed>> $plan */
    private function tagMonitor(array $plan): PlanHandler
    {
        $handler = new PlanHandler($plan);
        $queen = new Queen(['url' => 'http://queen.test:6632', 'retryAttempts' => 1, 'retryDelayMillis' => 0, 'handler' => HandlerStack::create($handler)]);
        $this->app->instance(\Queen\Laravel\Monitoring\TagMonitor::class, new \Queen\Laravel\Monitoring\TagMonitor(fn (): Queen => $queen, 'queen-metrics'));

        return $handler;
    }

    public function testSupervisorsOfDifferentConsumerGroupsDoNotShareAQueue(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $emails = $configuration;
        $emails['supervisors'][0]['consumer_group'] = 'emails';
        $depth = fn (int $depth): array => [['supervisor' => 'default', 'queue' => 'high', 'depth' => $depth, 'depth_available' => true]];
        $this->remoteSupervisors([
            $this->remoteDocument(['engine' => 'rust', 'state' => 'running', 'configuration' => $configuration, 'pool_status' => $depth(3)]),
            $this->remoteDocument([
                'engine' => 'rust',
                'state' => 'running',
                'configuration' => $emails,
                'instance_id' => str_repeat('f', 32),
                'pool_status' => $depth(8),
            ], time() - 1),
        ]);

        // Both pools are named default/high; each depth belongs to its own group.
        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.live_instances', 2)
            ->assertJsonPath('shared_queues', [])
            ->assertJsonCount(2, 'queues')
            ->assertJsonPath('queues.0.consumer_group', 'laravel')
            ->assertJsonPath('queues.0.depth', 3)
            ->assertJsonPath('queues.1.consumer_group', 'emails')
            ->assertJsonPath('queues.1.depth', 8);
        $this->get('/queen')->assertOk()->assertDontSee('running supervisors share queue');
    }

    public function testAStaleInstanceIsListedButLeftOutOfTheSummary(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $this->remoteSupervisors([
            $this->remoteDocument([
                'engine' => 'php',
                'state' => 'running',
                'ready' => false,
                'configuration' => $configuration,
                'instance_id' => str_repeat('a', 32),
                'hostname' => 'gone-pod',
                'pool_status' => [['supervisor' => 'default', 'queue' => 'high', 'running' => 5, 'desired' => 5]],
            ], time() - 3601),
            $this->remoteDocument([
                'engine' => 'php',
                'state' => 'running',
                'ready' => true,
                'capacity_satisfied' => true,
                'configuration' => $configuration,
                'instance_id' => str_repeat('b', 32),
                'hostname' => 'current-pod',
                'pool_status' => [[
                    'supervisor' => 'default',
                    'queue' => 'high',
                    'running' => 2,
                    'desired' => 2,
                    'draining' => 0,
                    'ready' => true,
                    'capacity_satisfied' => true,
                    'depth' => 0,
                    'depth_available' => true,
                ]],
            ]),
        ]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('shared_queues', [])
            ->assertJsonPath('instances.0.hostname', 'current-pod')
            ->assertJsonPath('instances.1.hostname', 'gone-pod')
            ->assertJsonPath('instances.1.availability', 'stale')
            ->assertJsonPath('supervisor.availability', 'live')
            ->assertJsonPath('supervisor.live_instances', 1)
            ->assertJsonPath('supervisor.instances', 2)
            ->assertJsonPath('supervisor.ready', true)
            ->assertJsonPath('supervisor.workers', 2)
            // One live instance keeps the single-supervisor summary.
            ->assertJsonPath('supervisor.hostname', 'current-pod')
            ->assertJsonPath('supervisor.instance_id', str_repeat('b', 32))
            ->assertJsonPath('supervisor.process_budget.valid', false);

        $overview = $this->dashboardXPath($this->get('/queen')->assertOk()->getContent());
        $this->assertSame('1 of 2 supervisor instances live', trim($overview->query('//dt[.="Liveness"]/following-sibling::small')->item(0)->textContent));
    }

    public function testAPodThatStoppedDuringARolloutIsListedButLeftOutOfTheSummary(): void
    {
        $configuration = $this->highQueueOnlyConfiguration();
        $this->remoteSupervisors([
            $this->remoteDocument([
                'engine' => 'rust',
                'state' => 'stopped',
                'configuration' => $configuration,
                'instance_id' => str_repeat('a', 32),
                'hostname' => 'old-pod',
                'pool_status' => [['supervisor' => 'default', 'queue' => 'high', 'running' => 0, 'desired' => 0]],
            ], time() - 5),
            $this->remoteDocument([
                'engine' => 'rust',
                'state' => 'running',
                'ready' => true,
                'capacity_satisfied' => true,
                'configuration' => $configuration,
                'instance_id' => str_repeat('b', 32),
                'hostname' => 'new-pod',
                'pool_status' => [[
                    'supervisor' => 'default',
                    'queue' => 'high',
                    'running' => 1,
                    'desired' => 1,
                    'draining' => 0,
                    'ready' => true,
                    'capacity_satisfied' => true,
                    'depth' => 0,
                    'depth_available' => true,
                ]],
            ]),
        ]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('instances.0.hostname', 'new-pod')
            ->assertJsonPath('instances.1.hostname', 'old-pod')
            ->assertJsonPath('instances.1.state', 'stopped')
            ->assertJsonPath('instances.1.availability', 'stale')
            ->assertJsonPath('supervisor.instances', 2)
            ->assertJsonPath('supervisor.live_instances', 1)
            ->assertJsonPath('supervisor.state', 'running')
            ->assertJsonPath('supervisor.ready', true)
            ->assertJsonPath('shared_queues', []);
        $this->get('/queen/supervisors')->assertOk()->assertSeeInOrder(['new-pod', 'Active', 'old-pod', 'Stale']);
    }

    public function testALocalSupervisorKeepsItsControlsBesideRemotePods(): void
    {
        $state = $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->remoteSupervisor(['engine' => 'rust', 'state' => 'running', 'hostname' => 'other-pod', 'pool_status' => []]);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('instances.0.source', 'local')
            ->assertJsonPath('instances.0.instance_id', $state->instanceId())
            ->assertJsonPath('instances.0.controls_available', true)
            ->assertJsonPath('instances.1.source', 'remote')
            ->assertJsonPath('instances.1.hostname', 'other-pod')
            ->assertJsonPath('supervisor.state', 'running')
            ->assertJsonPath('supervisor.engine', 'mixed')
            ->assertJsonPath('supervisor.controls_available', false);

        $response = $this->get('/queen/supervisors')->assertOk()
            ->assertSee('Read-only: this supervisor runs on another host.');
        $xpath = $this->dashboardXPath($response->getContent());
        $targets = $xpath->query('//form[contains(@action, "/queen/control/")]/input[@name="instance_id"]');
        $this->assertSame(3, $targets->length);
        foreach ($targets as $target) {
            $this->assertSame($state->instanceId(), $target->getAttribute('value'));
        }

        $this->post('/queen/control/pause', ['instance_id' => str_repeat('c', 32)])
            ->assertStatus(409)
            ->assertSee('This supervisor runs on another host.');
        $this->post('/queen/control/pause', ['instance_id' => $state->instanceId()])->assertStatus(303);
        $this->assertSame('pause', $state->command(null, $state->instanceId())['command'] ?? null);
    }

    public function testADocumentPublishedByAnEarlierReleaseIsStillShown(): void
    {
        $this->remoteSupervisors([
            $this->remoteDocument(['engine' => 'rust', 'state' => 'running', 'hostname' => 'old-pod', 'pool_status' => []]),
            $this->remoteDocument([
                'engine' => 'rust',
                'state' => 'running',
                'instance_id' => str_repeat('e', 40),
                'hostname' => 'new-pod',
                'pool_status' => [],
            ]),
        ], legacySlot: true);

        $this->getJson('/queen/api/status')->assertOk()
            ->assertJsonPath('supervisor.instances', 2)
            ->assertJsonPath('supervisor.live_instances', 2);
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

        $this->get('/queen/failed-jobs?cursor=1')->assertOk()->assertSee('No older failed jobs.');

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
        $exception = "RuntimeException: Mail server refused the connection in {$basePath}/app/Jobs/SendInvoice.php:42 (see https://status.example{$basePath}/mail)\n"
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
        $link = $list->query('//table//a')->item(0);
        $this->assertSame('/queen/failed-jobs/failed-1', $link->getAttribute('href'));
        // The arrow at the end of the row opens the drawer; the whole row is its hit area.
        $this->assertTrue($link->hasAttribute('data-drawer'));
        $this->assertSame('row-link', $link->getAttribute('class'));
        $this->assertSame('Why job failed-1 failed', $link->getAttribute('aria-label'));
        $this->assertSame(1, $list->query('//table//a/svg[@class="chevron"][@aria-hidden="true"]')->length);
        $this->assertSame('has-detail', $list->query('//table/tbody/tr')->item(0)->getAttribute('class'));

        $response = $this->get('/queen/failed-jobs/failed-1')->assertOk();
        $response->assertSee('RuntimeException')
            ->assertSee('App\\Jobs\\SendInvoice')
            ->assertSee('Mail server refused the connection in app/Jobs/SendInvoice.php:42')
            ->assertSee('vendor/laravel/framework/src/Illuminate/Queue/CallQueuedHandler.php(134)')
            ->assertSee('60s')
            ->assertSee('https://status.example' . $basePath . '/mail')
            ->assertSee('php artisan queue:retry failed-1')
            ->assertDontSee($secret)
            ->assertDontSee(' ' . $basePath . '/');
        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertSame('3', trim($xpath->query('//dl[@class="detail-list"]/div[dt="Max tries"]/dd')->item(0)->textContent));
        // Laravel's payload counter is not the number of attempts made; it is not shown.
        $this->assertSame(0, $xpath->query('//dl[@class="detail-list"]/div[dt="Attempts"]')->length);
        $this->assertSame('Failed job · Queen Supervisor', trim($xpath->query('//title')->item(0)->textContent));
        // The drawer takes #failed-job from this page and hides what only the page needs.
        $section = $xpath->query('//section[@id="failed-job"]')->item(0);
        $this->assertSame('failed-job-title', $section->getAttribute('aria-labelledby'));
        $this->assertSame('Failed', trim($xpath->query('.//span[@class="badge danger"]', $section)->item(0)->textContent));
        $this->assertSame('RuntimeException', trim($xpath->query('.//h2[@class="failure-title"]', $section)->item(0)->textContent));
        $this->assertSame(1, $xpath->query('.//div[contains(@class, "failure-reason")]/pre[@class="exception-summary"]', $section)->length);
        $this->assertTrue($xpath->query('.//a[normalize-space(.)="All failed jobs"]', $section)->item(0)->hasAttribute('data-page-only'));
        // One copy button per block, each copying the <pre> of its own block.
        $copyButtons = [];
        foreach ($xpath->query('.//button[@data-copy]', $section) as $button) {
            $pre = $xpath->query('ancestor::*[contains(@class, "detail-block")][1]//pre', $button)->item(0);
            $copyButtons[$button->getAttribute('aria-label')] = $pre?->getAttribute('class') ?? 'command';
        }
        $this->assertSame([
            'Copy the error message' => 'exception-summary',
            'Copy the stack trace' => 'exception-trace',
            'Copy the retry command' => '',
        ], $copyButtons);
        $this->assertSame(1, $xpath->query('.//*[@role="status"][@data-copy-status]', $section)->length);
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

    public function testFailedJobDetailReadsABoundedPrefixOfLargeColumns(): void
    {
        $this->failedJobsTable(1);
        $payload = json_encode([
            'uuid' => '00000000-0000-4000-8000-000000000001',
            'displayName' => 'App\\Jobs\\ImportCatalogue',
            'maxTries' => 5,
            'timeout' => 120,
            'data' => ['command' => str_repeat('x', 2 * 1048576)],
        ], JSON_THROW_ON_ERROR);
        DB::table('failed_jobs')->where('id', 1)->update([
            'payload' => $payload,
            'exception' => "RuntimeException: Catalogue too large\n" . str_repeat('y', 2 * 1048576),
        ]);
        $queries = [];
        DB::listen(static function ($query) use (&$queries): void {
            $queries[] = strtolower($query->sql);
        });

        $response = $this->get('/queen/failed-jobs/1')->assertOk()
            ->assertSee('App\\Jobs\\ImportCatalogue')
            ->assertSee('RuntimeException: Catalogue too large')
            ->assertSee('[truncated]');
        $xpath = $this->dashboardXPath($response->getContent());
        $this->assertSame('5', trim($xpath->query('//dl[@class="detail-list"]/div[dt="Max tries"]/dd')->item(0)->textContent));
        $this->assertSame('120s', trim($xpath->query('//dl[@class="detail-list"]/div[dt="Timeout"]/dd')->item(0)->textContent));
        $detailQueries = array_values(array_filter($queries, static fn (string $query): bool => str_contains($query, '"exception"')));
        $this->assertCount(1, $detailQueries);
        $this->assertStringContainsString('substr("exception", 1, 1048576)', $detailQueries[0]);
        $this->assertStringContainsString('substr("payload", 1, 1048576)', $detailQueries[0]);
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

    public function testAFailedJobIsRetriedFromItsDetailWithOneClick(): void
    {
        $this->app['config']->set('queue.connections.discard', ['driver' => 'null']);
        $this->failedJobsFile(2, ['connection' => 'discard', 'payload' => json_encode([
            'uuid' => '9b2c1d3e-0000-4000-8000-000000000001',
            'displayName' => 'App\\Jobs\\SendInvoice',
            // queue:retry unserializes the command to refresh retryUntil.
            'data' => ['command' => serialize(new \stdClass())],
        ], JSON_THROW_ON_ERROR)]);
        $this->app['env'] = 'local';

        $form = $this->dashboardXPath($this->get('/queen/failed-jobs/failed-1')->assertOk()->getContent())
            ->query('//section[@id="failed-job"]//form[@method="post"]')->item(0);
        $this->assertNotNull($form, 'the detail offers a retry button');
        $this->assertSame('/queen/failed-jobs/failed-1/retry', $form->getAttribute('action'));

        $this->withSession(['_token' => 'queen-csrf'])
            ->post('/queen/failed-jobs/failed-1/retry', ['_token' => 'queen-csrf'])
            ->assertStatus(303)
            ->assertHeader('Location', '/queen/failed-jobs')
            ->assertSessionHas('queen_dashboard_control_status', 'Failed job [failed-1] was pushed back onto queue [default].');
        $remaining = array_column(json_decode((string) file_get_contents($this->failedPath), true), 'id');
        $this->assertSame(['failed-2'], $remaining, 'queue:retry removed it from the failed-job store');
    }

    public function testRetryNeedsCsrfAndTheDashboardAbility(): void
    {
        $this->failedJobsFile(1);
        $this->app['env'] = 'local';
        $this->post('/queen/failed-jobs/failed-1/retry')->assertStatus(419);
        $this->get('/queen/failed-jobs/failed-1/retry')->assertStatus(405);

        Gate::define('viewQueenDashboard', static fn (?Authenticatable $user = null): bool => false);
        $this->app['env'] = 'production';
        $this->withSession(['_token' => 'queen-csrf'])
            ->post('/queen/failed-jobs/failed-1/retry', ['_token' => 'queen-csrf'])
            ->assertForbidden();
        $this->assertCount(1, json_decode((string) file_get_contents($this->failedPath), true));
    }

    public function testARetryThatCannotRunKeepsTheJobAndSaysWhy(): void
    {
        $this->failedJobsFile(1, ['connection' => 'missing-connection']);
        $this->app['env'] = 'local';

        $this->withSession(['_token' => 'queen-csrf'])
            ->post('/queen/failed-jobs/failed-1/retry', ['_token' => 'queen-csrf'])
            ->assertStatus(303)
            ->assertHeader('Location', '/queen/failed-jobs/failed-1')
            ->assertSessionHas('queen_dashboard_control_error');
        $this->assertStringContainsString(
            'Failed job [failed-1] was not retried',
            (string) session('queen_dashboard_control_error'),
        );
        $this->assertCount(1, json_decode((string) file_get_contents($this->failedPath), true));
    }

    public function testRetryingAJobThatIsNoLongerFailedSaysSo(): void
    {
        $this->failedJobsFile(1);
        $this->app['env'] = 'local';

        $this->withSession(['_token' => 'queen-csrf'])
            ->post('/queen/failed-jobs/failed-9/retry', ['_token' => 'queen-csrf'])
            ->assertStatus(303)
            ->assertHeader('Location', '/queen/failed-jobs')
            ->assertSessionHas('queen_dashboard_control_error', 'Failed job [failed-9] is no longer in the failed-job store.');
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

    public function testWorkloadShowsJobsProcessedFromTheBrokerCounters(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->throughputBroker([
            'high' => [
                $this->queueOps('high', '2026-09-29T15:02:00Z', 5, 1, 6),
            ],
            'default' => [
                $this->queueOps('default', '2026-09-29T15:00:00Z', 2, 0, 2),
                $this->queueOps('default', '2026-09-29T15:02:00Z', 3, 0, 4),
            ],
        ]);

        $xpath = $this->dashboardXPath($this->get('/queen/workload')->assertOk()->getContent());

        $this->assertSame(['10', '1', '12', '9'], $this->throughputMetrics($xpath));
        // 14:03 to 15:03 UTC: one bar slot per minute, bars only where jobs finished.
        $this->assertSame(61, $xpath->query('//figure[@class="throughput-chart"]//g')->length);
        $this->assertSame(2, $xpath->query('//figure[@class="throughput-chart"]//rect[@class="completed"]')->length);
        $this->assertSame(1, $xpath->query('//figure[@class="throughput-chart"]//rect[@class="failed"]')->length);
        $this->assertSame('15:02 UTC: 8 completed, 1 failed, 10 dispatched', trim($xpath->query('//figure[@class="throughput-chart"]//g[60]/title')->item(0)->textContent));
        $this->assertSame(
            [['high', 'queen', '5', '1', '6'], ['default', 'queen', '5', '0', '6']],
            $this->tableRows($xpath, 'Jobs processed per queue'),
        );
        $this->assertSame('/queen/workload', $xpath->query('//nav[@aria-label="Time range"]/a[@aria-current="page"]')->item(0)->getAttribute('href'));

        $this->assertCount(2, $this->throughputRequests);
        foreach ($this->throughputRequests as $request) {
            $this->assertSame('/api/v1/analytics/queue-ops', $request->getUri()->getPath());
            parse_str($request->getUri()->getQuery(), $query);
            $this->assertSame('2026-09-29T14:03:30Z', $query['from']);
            $this->assertSame('2026-09-29T15:03:30Z', $query['to']);
        }
    }

    public function testTheRangeSelectorWidensTheWindowAndTheRefreshKeepsIt(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->throughputBroker([
            'high' => [$this->queueOps('high', '2026-09-29T14:45:00Z', 40, 2, 42)],
        ]);

        $xpath = $this->dashboardXPath($this->get('/queen/workload?range=24h')->assertOk()->getContent());

        parse_str($this->throughputRequests[0]->getUri()->getQuery(), $query);
        $this->assertSame('2026-09-28T15:03:30Z', $query['from']);
        $this->assertSame('/queen/workload?range=24h', $xpath->query('//nav[@aria-label="Time range"]/a[@aria-current="page"]')->item(0)->getAttribute('href'));
        $this->assertSame('5;url=/queen/workload?range=24h', $xpath->query('//noscript/meta[@http-equiv="refresh"]')->item(0)->getAttribute('content'));
        // A day in 15-minute buckets: 15:00 yesterday to 15:00 today.
        $this->assertSame(97, $xpath->query('//figure[@class="throughput-chart"]//g')->length);
        $this->assertSame('14:45 UTC: 40 completed, 2 failed, 42 dispatched', trim($xpath->query('//figure[@class="throughput-chart"]//g[96]/title')->item(0)->textContent));
        $this->assertSame(['40', '2', '42', '42'], $this->throughputMetrics($xpath));

        $unknown = $this->dashboardXPath($this->get('/queen/workload?range=90d')->assertOk()->getContent());
        $this->assertSame('/queen/workload', $unknown->query('//nav[@aria-label="Time range"]/a[@aria-current="page"]')->item(0)->getAttribute('href'));
        $this->assertSame('5;url=/queen/workload', $unknown->query('//noscript/meta[@http-equiv="refresh"]')->item(0)->getAttribute('content'));
    }

    public function testBucketWidthsFollowTheBrokerWindowRule(): void
    {
        $this->assertSame(1, ThroughputReader::bucketMinutes(ThroughputReader::RANGES['1h']));
        $this->assertSame(5, ThroughputReader::bucketMinutes(ThroughputReader::RANGES['6h']));
        $this->assertSame(15, ThroughputReader::bucketMinutes(ThroughputReader::RANGES['24h']));
        $this->assertSame(60, ThroughputReader::bucketMinutes(ThroughputReader::RANGES['7d']));
        $this->assertSame('1h', ThroughputReader::range(['7d']));
    }

    public function testAnUnreachableBrokerLeavesTheCountersUnavailable(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->throughputBroker([], 503);

        $this->get('/queen/workload')->assertOk()
            ->assertSee("The broker's queue counters could not be read.", false)
            // The depth table does not depend on the counters.
            ->assertSee('Current workload');
    }

    public function testOneUnreadableQueueIsMarkedWithoutHidingTheOthers(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->throughputBroker([
            'high' => [$this->queueOps('high', '2026-09-29T15:02:00Z', 7, 0, 7)],
        ], 200, ['default' => 500]);

        $xpath = $this->dashboardXPath($this->get('/queen/workload')->assertOk()->getContent());

        $this->assertSame(['7', '0', '7', '7'], $this->throughputMetrics($xpath));
        $this->assertStringContainsString('Counters for 1 queue could not be read.', $xpath->query('//p[@class="throughput-note"]')->item(0)->textContent);
        $this->assertSame([['high', 'queen', '7', '0', '7'], ['default', 'queen', 'Unavailable']], $this->tableRows($xpath, 'Jobs processed per queue'));
    }

    public function testMalformedAndForeignCounterRowsAreIgnored(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->throughputBroker([
            'high' => [
                $this->queueOps('orders', '2026-09-29T15:02:00Z', 100, 0, 100),
                $this->queueOps('high', 'not a time', 100, 0, 100),
                ['queueName' => 'high', 'bucket' => '2026-09-29T15:02:00Z', 'ackSuccess' => -3, 'ackFailed' => 0, 'pushMessages' => 0],
                ['queueName' => 'high', 'bucket' => '2026-09-29T15:02:00Z', 'ackSuccess' => '9', 'ackFailed' => 0, 'pushMessages' => 0],
                $this->queueOps('high', '2026-09-29T15:02:00Z', 4, 0, 4),
                // Outside the window: neither drawn nor counted.
                $this->queueOps('high', '2026-09-27T10:00:00Z', 1, 0, 1),
            ],
        ]);

        $xpath = $this->dashboardXPath($this->get('/queen/workload')->assertOk()->getContent());

        $this->assertSame(['4', '0', '4', '4'], $this->throughputMetrics($xpath));
        $this->assertSame(['high', 'queen', '4', '0', '4'], $this->tableRows($xpath, 'Jobs processed per queue')[0]);
    }

    public function testCountersAreCachedBrieflyAcrossRefreshes(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->throughputBroker(['high' => [$this->queueOps('high', '2026-09-29T15:02:00Z', 1, 0, 1)]]);

        $this->get('/queen/workload')->assertOk();
        $this->get('/queen/workload')->assertOk();
        $this->assertCount(2, $this->throughputRequests);

        $this->get('/queen/workload?range=6h')->assertOk();
        $this->assertCount(4, $this->throughputRequests);
    }

    public function testOnlyTheWorkloadPageReadsTheCounters(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);

        foreach (['/queen', '/queen/supervisors', '/queen/failed-jobs', '/queen/configuration', '/queen/api/status'] as $url) {
            $this->get($url)->assertOk();
        }

        $this->assertSame([], $this->throughputRequests);
        $this->assertSame([], $this->queueContentsRequests, 'only the workload page reads the queue contents');
    }

    public function testTheWorkloadPageShowsWhatIsInEachQueueNow(): void
    {
        $this->app['config']->set('queen.supervisor.supervisors.default.queues', ['high', 'default', 'mail', 'reports', 'idle']);
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->queueContentsBroker([
            'high' => ['pending' => 5, 'processing' => 1, 'ready' => 4],
            'default' => ['pending' => 2, 'processing' => 0, 'ready' => 2],
            'mail' => ['pending' => 3, 'processing' => 2, 'ready' => 1],
            'reports' => ['pending' => 1, 'processing' => 0, 'ready' => 1],
            'idle' => ['pending' => 0, 'processing' => 0, 'ready' => 0],
        ], [
            ['consumer_group' => 'laravel', 'queue_name' => 'high', 'partition_name' => 'laravel-0003', 'time_lag_seconds' => 3913],
            ['consumer_group' => 'laravel', 'queue_name' => 'default', 'partition_name' => 'laravel-0001', 'time_lag_seconds' => 900],
            ['consumer_group' => 'laravel', 'queue_name' => 'mail', 'partition_name' => 'laravel-0002', 'time_lag_seconds' => 185],
            // Another consumer group's lag is not this one's.
            ['consumer_group' => 'billing', 'queue_name' => 'reports', 'partition_name' => 'laravel-0000', 'time_lag_seconds' => 7200],
        ]);

        $xpath = $this->dashboardXPath($this->get('/queen/workload')->assertOk()->getContent());

        $this->assertSame('queue-contents', $xpath->query('//main//section')->item(0)->getAttribute('id'), 'the card opens the page');
        $card = $xpath->query('//section[@id="queue-contents"]')->item(0);
        $this->assertSame('In the queue now', trim($xpath->query('.//h2', $card)->item(0)->textContent));
        $this->assertStringContainsString(
            'Oldest unfinished is how long the oldest job not yet acknowledged has been in the queue, waiting or running; the broker reports it from one minute.',
            $card->textContent,
        );
        $this->assertSame([
            ['high laravel', '4', '1', '1 h 05 min'],
            ['default laravel', '2', '0', '15 min'],
            ['mail laravel', '1', '2', '3 min'],
            ['reports laravel', '1', '0', 'under a minute'],
            ['idle laravel', '0', '0', '—'],
        ], $this->tableRows($xpath, 'Jobs in each queue now'));
        $badge = fn (int $row): ?string => $xpath->query('//div[@aria-label="Jobs in each queue now"]//tbody/tr[' . $row . ']/td[4]/span[contains(@class, "badge")]')->item(0)?->getAttribute('class');
        $this->assertSame('badge danger', $badge(1), 'an hour or more');
        $this->assertSame('badge warning', $badge(2), 'ten minutes or more');
        $this->assertNull($badge(3));
        $this->assertNull($badge(4));
        $this->assertSame(0, $xpath->query('//section[@id="queue-contents"]//a')->length, 'no console, no links');
        $this->assertSame(1, count(array_filter(
            $this->queueContentsRequests,
            fn (RequestInterface $request): bool => $request->getUri()->getPath() === '/api/v1/consumer-groups/lagging',
        )), 'one lag read for the connection');
    }

    public function testQueueRowsLinkToTheQueenConsoleWhenOneIsConfigured(): void
    {
        $this->app['config']->set('queen.supervisor.supervisors.default.queues', ['high', 'billing/eu west']);
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->queueContentsBroker(['billing/eu west' => ['pending' => 3, 'processing' => 1, 'ready' => 2]], 503);
        $this->app['config']->set('queen.dashboard.console_url', 'https://console.example.test/queen/');

        $xpath = $this->dashboardXPath($this->get('/queen/workload')->assertOk()->getContent());

        $links = iterator_to_array($xpath->query('//div[@aria-label="Jobs in each queue now"]//tbody/tr[2]//a'));
        $this->assertSame([
            'https://console.example.test/queen/messages?queue=billing%2Feu%20west&status=pending',
            'https://console.example.test/queen/messages?queue=billing%2Feu%20west&status=processing',
            'https://console.example.test/queen/queues/billing%2Feu%20west',
        ], array_map(fn (\DOMElement $link): string => $link->getAttribute('href'), $links));
        $this->assertSame(['2', '1'], [trim($links[0]->textContent), trim($links[1]->textContent)]);
        foreach ($xpath->query('//section[@id="queue-contents"]//a') as $link) {
            $this->assertSame('_blank', $link->getAttribute('target'));
            $this->assertSame('noopener noreferrer', $link->getAttribute('rel'));
        }
        $this->assertSame(
            ['2', '1', '—'],
            array_slice($this->tableRows($xpath, 'Jobs in each queue now')[1], 1, 3),
            'without a lag answer the oldest age is unknown, not under a minute',
        );
    }

    public function testAnUnreachableBrokerLeavesTheQueueContentsUnavailable(): void
    {
        $this->liveSupervisor(['engine' => 'php', 'state' => 'running', 'pool_status' => []]);
        $this->queueContentsBroker(['high' => 503, 'default' => 503], 503);
        $this->app['config']->set('queen.dashboard.console_url', 'https://console.example.test');

        $xpath = $this->dashboardXPath($this->get('/queen/workload')->assertOk()->getContent());

        $empty = $xpath->query('//section[@id="queue-contents"]//div[@class="empty"]')->item(0);
        $this->assertNotNull($empty);
        $this->assertSame('Unavailable The broker could not be read for these queues.', preg_replace('/\s+/', ' ', trim($empty->textContent)));
        $this->assertSame(1, $xpath->query('//section[@id="throughput"]')->length, 'the other cards stay');

        $this->queueContentsBroker(['default' => 503]);
        $xpath = $this->dashboardXPath($this->get('/queen/workload')->assertOk()->getContent());

        $this->assertSame([
            ['high laravel', '0', '0', '—', 'Open in console'],
            ['default laravel', '—', '—', 'Unavailable', 'Open in console'],
        ], $this->tableRows($xpath, 'Jobs in each queue now'));
        $this->assertSame('badge warning', $xpath->query('//div[@aria-label="Jobs in each queue now"]//tbody/tr[2]/td[4]/span')->item(0)->getAttribute('class'));
        $this->assertSame(0, $xpath->query('//div[@aria-label="Jobs in each queue now"]//tbody/tr[2]/td[position() < 4]//a')->length, 'no message links without numbers');
    }

    public function testTheConfigurationPageGivesAdviceAboveTheSettings(): void
    {
        $this->app['config']->set('queen.supervisor.prefork', false);
        $this->app['config']->set('queen.supervisor.event_driven', false);
        $this->liveSupervisor(['engine' => 'rust', 'state' => 'running', 'pool_status' => []]);
        $this->failedJobsFile(2);
        $this->jobMetricsBroker(['App\Jobs\Export' => ['processed' => 3, 'failed' => 0, 'runtime_ms' => 300000, 'max_ms' => 130000]]);

        $xpath = $this->dashboardXPath($this->get('/queen/configuration')->assertOk()->getContent());

        $this->assertSame(
            ['advice', 'configuration'],
            array_map(fn (\DOMElement $section): string => $section->getAttribute('id'), iterator_to_array($xpath->query('//main//section'))),
            'the advice comes above the settings',
        );
        $items = iterator_to_array($xpath->query('//section[@id="advice"]//li[contains(@class, "advice")]'));
        $this->assertSame(
            ['Deploys interrupt App\Jobs\Export', 'Every worker boots Laravel on its own', 'Bursts wait for the next poll', '2 failed jobs'],
            array_map(fn (\DOMElement $item): string => trim($xpath->query('.//h3', $item)->item(0)->textContent), $items),
            'most severe first, from the settings, the running supervisor, the failed jobs and the last hour of jobs',
        );
        $this->assertSame(['Warning', 'Info', 'Info', 'Info'], array_map(
            fn (\DOMElement $item): string => trim($xpath->query('.//span[contains(@class, "badge")]', $item)->item(0)->textContent),
            $items,
        ));
        $this->assertStringContainsString('2 min 10 s', $items[0]->textContent);
        $this->assertStringContainsString('QUEEN_SUPERVISOR_SHUTDOWN_GRACE', $items[0]->textContent);
        $docs = iterator_to_array($xpath->query('//section[@id="advice"]//a[starts-with(@href, "https://queenmq.com/")]'));
        $this->assertCount(4, $docs);
        foreach ($docs as $link) {
            $this->assertStringStartsWith('Read more', trim($link->textContent));
            $this->assertSame('_blank', $link->getAttribute('target'));
            $this->assertSame('noopener noreferrer', $link->getAttribute('rel'));
        }
        $this->assertSame('Open Failed jobs', trim($xpath->query('//section[@id="advice"]//a[@href="/queen/failed-jobs"]')->item(0)->textContent));
    }

    public function testTheConfigurationPageSaysPlainlyWhenThereIsNoAdvice(): void
    {
        $this->app['config']->set('queen.supervisor.prefork', true);
        $this->app['config']->set('queen.supervisor.event_driven', true);
        $this->liveSupervisor(['engine' => 'rust', 'state' => 'running', 'pool_status' => []]);

        $xpath = $this->dashboardXPath($this->get('/queen/configuration')->assertOk()->getContent());

        $this->assertSame(0, $xpath->query('//section[@id="advice"]//li')->length);
        $this->assertSame(
            'No advice: nothing in these settings, the running supervisor or the last hour of jobs calls for a change.',
            trim($xpath->query('//section[@id="advice"]//div[@class="empty"]')->item(0)->textContent),
        );
    }

    public function testTheSettingsShowEachValueWithItsDefaultAndMeaning(): void
    {
        $this->app['config']->set('queue.connections.queen.prefetch', 4);
        $this->app['config']->set('queue.connections.queen.lease_renewal', true);
        $this->app['config']->set('queue.connections.queen.ack_async', 'yes');
        $this->app['config']->set('queen.supervisor.supervisors.default.backoff', 5);
        $this->app['config']->set('queen.supervisor.supervisors.default.max_jobs', 1000);
        $this->liveSupervisor(['engine' => 'rust', 'state' => 'running', 'pool_status' => []]);

        $xpath = $this->dashboardXPath($this->get('/queen/configuration')->assertOk()->getContent());

        $connection = $this->settingsRows($xpath, 'Connection settings');
        $this->assertSame(['4', '1'], [$connection['prefetch'][1], $connection['prefetch'][2]]);
        $this->assertSame(1, $xpath->query('//div[@aria-label="Connection settings"]//tr[td[1]/code="prefetch"]/td[2]/strong')->length, 'a changed value stands out');
        $this->assertSame('invalid', $connection['ack_async'][1]);
        $this->assertSame(1, $xpath->query('//div[@aria-label="Connection settings"]//tr[td[1]/code="ack_async"]/td[2]/span[@class="badge warning"]')->length);
        $this->assertSame(['on', 'off'], [$connection['lease_renewal'][1], $connection['lease_renewal'][2]]);
        $this->assertSame('30,000 ms', $connection['timeout'][1]);
        $this->assertStringContainsString('QUEEN_PREFETCH', $connection['prefetch'][0]);
        $supervisor = $this->settingsRows($xpath, 'Supervisor settings');
        $this->assertSame(['75 s', '75 s'], [$supervisor['shutdown_grace'][1], $supervisor['shutdown_grace'][2]]);
        $this->assertSame('on', $supervisor['QUEEN_SUPERVISOR_LEASE_SERVICE'][1]);
        foreach ([...$connection, ...$supervisor] as $name => $cells) {
            $this->assertNotSame('', $cells[3], "{$name} has a meaning");
        }

        $pool = $this->tableRows($xpath, 'Worker pools')[0];
        $this->assertSame('default', $pool[0]);
        $this->assertStringContainsString('backoff 5 s', $pool[6]);
        $this->assertStringContainsString('max jobs 1,000', $pool[6]);
        $this->assertStringContainsString('max time no limit', $pool[6]);
        $this->assertStringContainsString('As the running supervisor published them', $xpath->query('//section[@id="configuration"]')->item(0)->textContent);
    }

    public function testCredentialsInTheConfigurationNeverReachTheConfigurationPage(): void
    {
        $secrets = [
            'queen.bearer_token' => 'Tok-Bearer-91c3e',
            'queue.connections.queen.bearer_token' => 'Tok-Connection-5ab02',
            'queen.supervisor.read_bearer_token' => 'Tok-Read-77d1f',
            'queen.supervisor.remote_status.key' => 'Key-Status-0e9d4',
            'queen.metrics.token' => 'Tok-Metrics-' . str_repeat('8', 32),
            'queen.supervisor_binary.manifest_sha256' => 'Sha-Manifest-3f6a1',
            // A key the page does not list at all.
            'queen.custom_password' => 'Pass-Custom-e1f47',
        ];
        foreach ($secrets as $key => $value) {
            $this->app['config']->set($key, $value);
        }
        $this->app['config']->set('queue.connections.queen.headers', ['Authorization' => 'Bearer Hdr-Value-2c8b7', 'X-Api-Key' => 'Hdr-Value-a41e0']);
        $this->app['config']->set('queue.connections.queen.urls', ['https://ops-user:Url-Pass-6d3c9@queen-0.internal.test:6632/tenant?key=Url-Query-b7e25']);
        $this->liveSupervisor(['engine' => 'rust', 'state' => 'running', 'pool_status' => []]);

        $content = $this->get('/queen/configuration')->assertOk()->getContent();

        foreach ([...array_values($secrets), 'Hdr-Value-2c8b7', 'Hdr-Value-a41e0', 'ops-user', 'Url-Pass-6d3c9', 'Url-Query-b7e25'] as $secret) {
            $this->assertStringNotContainsString($secret, $content);
        }
        $xpath = $this->dashboardXPath($content);
        $connection = $this->settingsRows($xpath, 'Connection settings');
        $this->assertSame('set', $connection['bearer_token'][1]);
        $this->assertSame('https://queen-0.internal.test:6632', $connection['urls'][1]);
        $this->assertSame('2 set, values hidden', $connection['headers'][1]);
        $supervisor = $this->settingsRows($xpath, 'Supervisor settings');
        $this->assertSame('set', $supervisor['read_bearer_token'][1]);
        $this->assertSame('set', $supervisor['remote_status.key'][1]);
    }

    public function testAnInvalidConsoleUrlIsRefusedAtBoot(): void
    {
        foreach ([
            'ftp://console.example.test',
            'https://admin:hunter2@console.example.test',
            'https://console.example.test/?tenant=1',
            'https://console.example.test/#queues',
            'javascript:alert(1)',
            '/queen-console',
            "https://console.example.test/\nX-Injected: 1",
            42,
        ] as $invalid) {
            $this->app['config']->set('queen.dashboard.console_url', $invalid);
            try {
                (new QueenServiceProvider($this->app))->boot();
                $this->fail('accepted ' . var_export($invalid, true));
            } catch (\InvalidArgumentException $exception) {
                $this->assertStringContainsString('queen.dashboard.console_url', $exception->getMessage());
                $this->assertStringNotContainsString('hunter2', $exception->getMessage(), 'the value may hold a credential');
            }
        }
    }

    public function testCountersUseTheSupervisorsReadCredential(): void
    {
        $this->app['config']->set('queen.bearer_token', 'write-token');
        $this->app['config']->set('queen.supervisor.read_bearer_token', 'read-token');

        $connection = \Queen\Laravel\Supervisor\SupervisorConfiguration::readOnlyConnection(
            'queen',
            (array) $this->app['config']->get('queen.supervisor'),
            (array) $this->app['config']->get('queen'),
            (array) $this->app['config']->get('queue.connections', []),
        );

        $this->assertSame('read-token', $connection['bearer_token']);
    }

    /**
     * @param array<string, list<array<string, mixed>>> $seriesByQueue
     * @param array<string, int> $statusByQueue
     */
    private function throughputBroker(array $seriesByQueue, int $status = 200, array $statusByQueue = []): void
    {
        $this->throughputRequests = [];
        $handler = function (RequestInterface $request) use ($seriesByQueue, $status, $statusByQueue): FulfilledPromise {
            $this->throughputRequests[] = $request;
            parse_str($request->getUri()->getQuery(), $query);
            $queue = (string) ($query['queue'] ?? '');
            $code = $statusByQueue[$queue] ?? $status;
            $body = $code === 200
                ? ['bucketMinutes' => 1, 'series' => $seriesByQueue[$queue] ?? [], 'queues' => [$queue]]
                : ['error' => 'unavailable'];

            return new FulfilledPromise(new Response($code, ['Content-Type' => 'application/json'], json_encode($body)));
        };
        $this->app->instance(ThroughputReader::class, new ThroughputReader(
            fn (string $connection): Queen => new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'enableFailover' => false,
                'handler' => HandlerStack::create($handler),
            ]),
            fn () => $this->app['cache']->store(),
            fn (): int => self::THROUGHPUT_NOW,
        ));
    }

    /**
     * The job metrics of one worker in the current five-minute bucket.
     *
     * @param array<string, array<string, int>> $classes counts per job class
     */
    private function jobMetricsBroker(array $classes): void
    {
        $rows = $classes === [] ? [] : [[
            'key' => 'jobs/v1/' . sprintf('%010d', intdiv(time(), 300) * 300) . '/aaaa',
            'value' => ['classes' => $classes],
        ]];
        $this->app->instance(\Queen\Laravel\Dashboard\JobMetricsReader::class, new \Queen\Laravel\Dashboard\JobMetricsReader(
            new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create(new PlanHandler([], ['status' => 200, 'json' => ['results' => [[
                    'rows' => $rows,
                    'truncated' => false,
                ]]]])),
            ]),
            'queen-metrics',
        ));
    }

    /**
     * The broker behind the queue-contents card: a depth answer (or a failing
     * status) per queue, an empty queue for any other, and the lagging
     * partitions (or a failing status).
     *
     * @param array<string, array<string, mixed>|int> $depths
     * @param list<array<string, mixed>>|int $lagging
     */
    private function queueContentsBroker(array $depths, array|int $lagging = []): void
    {
        $this->queueContentsRequests = [];
        $handler = function (RequestInterface $request) use ($depths, $lagging): FulfilledPromise {
            $this->queueContentsRequests[] = $request;
            $path = $request->getUri()->getPath();
            $answer = 404;
            if (preg_match('#^/api/v1/resources/queues/([^/]+)/depth$#D', $path, $matches) === 1) {
                $answer = $depths[rawurldecode($matches[1])] ?? ['pending' => 0, 'processing' => 0, 'ready' => 0];
            } elseif ($path === '/api/v1/consumer-groups/lagging') {
                $answer = $lagging;
            }
            [$status, $body] = is_int($answer) ? [$answer, ['error' => 'unavailable']] : [200, $answer];

            return new FulfilledPromise(new Response($status, ['Content-Type' => 'application/json'], json_encode($body)));
        };
        $this->app->instance(QueueContentsReader::class, new QueueContentsReader(
            fn (string $connection): Queen => new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'enableFailover' => false,
                'handler' => HandlerStack::create($handler),
            ]),
        ));
    }

    /** @return array<string, mixed> */
    private function queueOps(string $queue, string $bucket, int $completed, int $failed, int $pushed): array
    {
        return [
            'bucket' => $bucket,
            'queueName' => $queue,
            'ackSuccess' => $completed,
            'ackFailed' => $failed,
            'pushMessages' => $pushed,
            'popMessages' => $completed + $failed,
        ];
    }

    /** @return list<string> */
    private function throughputMetrics(\DOMXPath $xpath): array
    {
        $values = [];
        foreach ($xpath->query('//section[@id="throughput"]//dl[@class="metrics"]//dd') as $value) {
            $values[] = trim($value->textContent);
        }

        return $values;
    }

    /** @return list<list<string>> */
    private function tableRows(\DOMXPath $xpath, string $label): array
    {
        $rows = [];
        foreach ($xpath->query('//div[@aria-label="' . $label . '"]//tbody/tr') as $row) {
            $cells = [];
            foreach ($xpath->query('./td', $row) as $cell) {
                $cells[] = trim($cell->textContent);
            }
            $rows[] = $cells;
        }

        return $rows;
    }

    /** @return array<string, list<string>> the cells of a settings table, by setting name */
    private function settingsRows(\DOMXPath $xpath, string $label): array
    {
        $rows = [];
        foreach ($xpath->query('//div[@aria-label="' . $label . '"]//tbody/tr') as $row) {
            $name = trim((string) $xpath->query('./td[1]/code', $row)->item(0)?->textContent);
            $rows[$name] = array_map(
                fn (\DOMNode $cell): string => trim($cell->textContent),
                iterator_to_array($xpath->query('./td', $row)),
            );
        }

        return $rows;
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
        return $this->remoteSupervisors([$this->remoteDocument($status, $updatedAtEpoch)]);
    }

    /**
     * A published status document; `instance_id`, `hostname` and `pid` in
     * $status replace the defaults.
     *
     * @param array<string, mixed> $status
     * @return array<string, mixed>
     */
    private function remoteDocument(array $status, ?int $updatedAtEpoch = null): array
    {
        $updatedAtEpoch ??= time();

        return array_replace([
            'configuration' => $this->statusConfiguration(),
            'instance_id' => str_repeat('c', 32),
            'hostname' => 'worker-0',
            'pid' => 4242,
        ], $status, [
            'schema' => SupervisorState::STATUS_SCHEMA,
            'updated_at' => gmdate('Y-m-d\TH:i:s\Z', $updatedAtEpoch),
            'updated_at_epoch' => $updatedAtEpoch,
            'paused' => ($status['state'] ?? null) === 'paused',
            'stopping' => ($status['state'] ?? null) === 'terminating',
        ]);
    }

    /**
     * Serve documents as their supervisors publish them, each in its own slot.
     *
     * @param list<array<string, mixed>> $documents
     * @param bool $legacySlot write the first document where earlier releases did
     */
    private function remoteSupervisors(array $documents, bool $legacySlot = false): PlanHandler
    {
        $this->app['config']->set('queen.supervisor.remote_status', ['enabled' => true, 'key' => 'orders']);
        $rows = [];
        foreach ($documents as $index => $document) {
            $key = $legacySlot && $index === 0
                ? 'orders'
                : RemoteStatusDocument::instanceKey('orders', $document['instance_id']);
            foreach (RemoteStatusDocument::operations($document, 'queen-supervisor', $key, 600, str_repeat('d', 32)) as $op) {
                $rows[$op['key']] = ['key' => $op['key'], 'value' => $op['value']];
            }
        }
        // The broker returns a prefix in byte order.
        ksort($rows, SORT_STRING);
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['results' => [[
            'rows' => array_values($rows),
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
    private function highQueueOnlyConfiguration(): array
    {
        $configuration = $this->statusConfiguration();
        $configuration['supervisors'][0]['queues'] = ['high'];

        return $configuration;
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
