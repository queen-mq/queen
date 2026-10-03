<?php

namespace Queen\Tests\Integration;

use Illuminate\Contracts\Queue\Job;
use Orchestra\Testbench\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Laravel\Monitoring\JobMetricsRecorder;
use Queen\Laravel\QueenServiceProvider;
use Queen\Queen;

/**
 * The dashboard's Jobs page read with a read-only credential, against a live
 * broker that checks tokens.
 *
 * The dashboard reads with `queen.supervisor.read_bearer_token`. A Queen 2
 * broker refuses POST /api/v1/kv to a read-only token, so the Jobs page reads
 * the job metrics through POST /api/v1/resources/kv/list.
 *
 * Needs a broker started with JWT_ENABLED=true, JWT_ALGORITHM=HS256 and a
 * JWT_SECRET: QUEEN_JWT_HTTP_URL is its address and QUEEN_JWT_SECRET the
 * secret, from which the test signs a read-only and a read-write token with
 * the broker's default role names. It skips without them, because the shared
 * integration stack runs without authentication.
 */
final class DashboardReadOnlyTokenIntegrationTest extends TestCase
{
    /** Everything this test writes lives here, and is purged at both ends. */
    private const NS = 'phpmetrics';

    private const JOB_CLASS = 'App\Jobs\ReadOnlyProbe';

    private string $directory;

    protected function setUp(): void
    {
        if (!getenv('QUEEN_JWT_HTTP_URL') || !getenv('QUEEN_JWT_SECRET')) {
            $this->markTestSkipped('QUEEN_JWT_HTTP_URL and QUEEN_JWT_SECRET are not set; this test needs a broker that checks JWTs');
        }
        $this->directory = sys_get_temp_dir() . '/queen-dashboard-ro-' . bin2hex(random_bytes(6));
        mkdir($this->directory, 0700);
        parent::setUp();
        $this->purge();
    }

    protected function tearDown(): void
    {
        if (isset($this->directory)) {
            $this->purge();
            @unlink("{$this->directory}/failed.json");
            @rmdir($this->directory);
        }
        parent::tearDown();
    }

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('app.key', 'base64:' . base64_encode(str_repeat('q', 32)));
        // The workers' credential may write; the dashboard's may only read.
        $app['config']->set('queue.connections.queen', [
            'driver' => 'queen',
            'url' => getenv('QUEEN_JWT_HTTP_URL'),
            'bearer_token' => $this->token('read-write'),
        ]);
        $app['config']->set('queen.supervisor.read_bearer_token', $this->token('read-only'));
        $app['config']->set('queen.supervisor.state_directory', "{$this->directory}/state");
        $app['config']->set('queen.job_metrics.namespace', self::NS);
        $app['config']->set('queen.dashboard', [
            'enabled' => true,
            'path' => 'queen',
            'domain' => null,
            'middleware' => ['web'],
            'refresh_seconds' => 5,
            'allow_local' => true,
            'failed_jobs_limit' => 2,
        ]);
        $app['config']->set('queue.failed', ['driver' => 'file', 'path' => "{$this->directory}/failed.json", 'limit' => 10]);
        $app['config']->set('cache.default', 'array');
    }

    public function testTheJobsPageReadsTheJobMetricsWithAReadOnlyToken(): void
    {
        try {
            $this->client('read-only')->kv()->getPrefix(self::NS, JobMetricsRecorder::PREFIX);
            $this->fail('the broker must refuse POST /api/v1/kv to a read-only token');
        } catch (HttpException $refused) {
            $this->assertSame(403, $refused->statusCode);
        }

        // A worker records a job with its own credential.
        $recorder = new JobMetricsRecorder(fn (): Queen => $this->client('read-write'), self::NS);
        $job = $this->createStub(Job::class);
        $job->method('resolveName')->willReturn(self::JOB_CLASS);
        $recorder->start($job);
        $recorder->finish($job, false);
        $recorder->flush();

        $this->get('/queen/jobs')
            ->assertOk()
            ->assertSee(self::JOB_CLASS)
            ->assertDontSee('could not be read from the broker');
    }

    private function client(string $role): Queen
    {
        return new Queen([
            'url' => getenv('QUEEN_JWT_HTTP_URL'),
            'bearerToken' => $this->token($role),
            'timeoutMillis' => 15000,
            'retryAttempts' => 1,
        ]);
    }

    /** An HS256 token with the broker's default role claim. */
    private function token(string $role): string
    {
        $encode = static fn (string $bytes): string => rtrim(strtr(base64_encode($bytes), '+/', '-_'), '=');
        $signed = $encode(json_encode(['alg' => 'HS256', 'typ' => 'JWT']))
            . '.' . $encode(json_encode(['sub' => "php-dashboard-{$role}", 'role' => $role, 'exp' => time() + 600]));

        return $signed . '.' . $encode(hash_hmac('sha256', $signed, (string) getenv('QUEEN_JWT_SECRET'), true));
    }

    /** Every metrics record in the namespace, which only this test writes. */
    private function purge(): void
    {
        $queen = $this->client('read-write');
        do {
            $page = $queen->admin()->listKv(self::NS, ['prefix' => JobMetricsRecorder::PREFIX, 'keysOnly' => true, 'limit' => 1000]);
            foreach ($page['rows'] ?? [] as $row) {
                $queen->kv()->delete(self::NS, $row['key']);
            }
        } while (($page['truncated'] ?? false) === true && ($page['rows'] ?? []) !== []);
    }
}
