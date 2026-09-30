<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\Dashboard\RemoteStatusReader;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Supervisor\RemoteStatusDocument;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class LaravelMetricsTest extends TestCase
{
    private const TOKEN = 'scrape-token-0123456789abcdef0123456789';

    private bool $metricsEnabled = true;

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queen.metrics', [
            'enabled' => $this->metricsEnabled,
            'path' => 'queen/metrics',
            'token' => self::TOKEN,
        ]);
        $app['config']->set('queen.supervisor.state_directory', sys_get_temp_dir() . '/queen-metrics-' . bin2hex(random_bytes(6)));
        $app['config']->set('queen.supervisor.supervisors', [
            'default' => ['connection' => 'queen', 'consumer_group' => 'laravel', 'queues' => ['high']],
        ]);
    }

    public function testTheScrapeNeedsTheBearerToken(): void
    {
        $this->get('/queen/metrics')->assertStatus(401)->assertHeader('WWW-Authenticate', 'Bearer');
        $this->withToken('wrong-token')->get('/queen/metrics')->assertStatus(401);
    }

    public function testItExposesDepthWorkersAndReplicasOfEveryPod(): void
    {
        $this->pods([
            $this->pod(str_repeat('a', 32), 'orders-worker-a', 3, 7, 2),
            $this->pod(str_repeat('b', 32), 'orders-worker-"b"', 2, 7, 2),
        ]);

        $response = $this->withToken(self::TOKEN)->get('/queen/metrics')->assertOk();

        $this->assertStringStartsWith('text/plain; version=0.0.4', (string) $response->headers->get('Content-Type'));
        $body = (string) $response->getContent();
        $this->assertStringContainsString("# TYPE queen_queue_depth gauge\n", $body);
        $this->assertStringContainsString('queen_queue_depth{connection="queen",consumer_group="laravel",queue="high"} 7' . "\n", $body);
        $this->assertStringContainsString('queen_supervisor_instances{availability="live"} 2' . "\n", $body);
        $this->assertStringContainsString(
            'queen_workers{instance_id="' . str_repeat('a', 32) . '",hostname="orders-worker-a",supervisor="default",queue="high"} 3' . "\n",
            $body,
        );
        // Label values are escaped.
        $this->assertStringContainsString('hostname="orders-worker-\"b\""', $body);
        $this->assertStringContainsString('queen_pool_replicas{instance_id="' . str_repeat('b', 32), $body);
        $this->assertStringNotContainsString('queen_shared_queue_supervisors{', $body);

        // Every family's samples follow its HELP and TYPE lines, as promtool requires.
        $seen = [];
        $current = null;
        foreach (explode("\n", trim($body)) as $line) {
            $family = preg_replace('/^# (?:HELP|TYPE) (\S+).*$|^([a-z_]+)[{ ].*$/', '$1$2', $line);
            if ($family !== $current) {
                $this->assertArrayNotHasKey($family, $seen, "{$family} is split");
                $seen[$family] = true;
                $current = $family;
            }
        }
    }

    public function testUncoordinatedPodsOnOneQueueAreExposed(): void
    {
        $this->pods([
            $this->pod(str_repeat('a', 32), 'pod-a', 1, 4, null),
            $this->pod(str_repeat('b', 32), 'pod-b', 1, 4, null),
        ]);

        $this->withToken(self::TOKEN)->get('/queen/metrics')->assertOk()
            ->assertSee('queen_shared_queue_supervisors{connection="queen",consumer_group="laravel",queue="high"} 2', false);
    }

    /** @return array<string, mixed> */
    private function pod(string $instanceId, string $hostname, int $running, int $depth, ?int $replicas): array
    {
        $now = time();

        return [
            'schema' => SupervisorState::STATUS_SCHEMA,
            'engine' => 'rust',
            'state' => 'running',
            'paused' => false,
            'stopping' => false,
            'pid' => 1,
            'hostname' => $hostname,
            'instance_id' => $instanceId,
            'updated_at' => gmdate('Y-m-d\TH:i:s\Z', $now),
            'updated_at_epoch' => $now,
            'pool_status' => [[
                'supervisor' => 'default',
                'queue' => 'high',
                'running' => $running,
                'desired' => $running,
                'draining' => 0,
                'depth' => $depth,
                'depth_available' => true,
                'replicas' => $replicas,
            ]],
            'configuration' => [
                'poll_interval' => 3,
                'http_timeout' => 5,
                'control_ttl' => 3600,
                'heartbeat_timeout' => 3600,
                'shutdown_grace' => 75,
                'telemetry_ttl' => 300,
                'process_limit' => 256,
                'supervisors' => [[
                    'name' => 'default',
                    'connection' => 'queen',
                    'consumer_group' => 'laravel',
                    'queues' => ['high'],
                    'balance' => 'auto',
                    'strategy' => 'size',
                    'processes' => 10,
                    'min_processes' => 1,
                    'max_processes' => 10,
                    'timeout' => 60,
                    'retry_after' => 90,
                    'tries' => 3,
                    'memory' => 128,
                ]],
            ],
        ];
    }

    /** @param list<array<string, mixed>> $documents */
    private function pods(array $documents): void
    {
        $this->app['config']->set('queen.supervisor.remote_status', ['enabled' => true, 'key' => 'orders']);
        $rows = [];
        foreach ($documents as $document) {
            $key = RemoteStatusDocument::instanceKey('orders', $document['instance_id']);
            foreach (RemoteStatusDocument::operations($document, 'queen-supervisor', $key, 600, str_repeat('d', 32)) as $op) {
                $rows[$op['key']] = ['key' => $op['key'], 'value' => $op['value']];
            }
        }
        ksort($rows, SORT_STRING);
        $this->app->instance(RemoteStatusReader::class, new RemoteStatusReader(
            new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create(new PlanHandler([], ['status' => 200, 'json' => ['results' => [[
                    'rows' => array_values($rows),
                    'truncated' => false,
                ]]]])),
            ]),
            'queen-supervisor',
            'orders',
        ));
    }
}
