<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Contracts\Queue\Job;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Monitoring\TagMonitor;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class TagMonitorTest extends TestCase
{
    private float $now = 1_790_000_000.25;

    public function testOnlyMonitoredTagsAreRecordedNewestFirst(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [['found' => true, 'value' => ['customer:7', 'vip'], 'version' => 3]]]],
            ['status' => 200, 'json' => ['results' => [['applied' => true], ['applied' => true]]]],
        ], ['status' => 200, 'json' => ['results' => [['applied' => true]]]]);
        $monitor = $this->monitor($handler);
        $job = $this->job(['billing', 'customer:7', 'vip']);

        $monitor->start($job);
        $monitor->record($job, 'completed');

        $puts = $this->operations($handler, 1);
        $this->assertSame(['customer:7', 'vip'], array_column(array_column($puts, 'value'), 'tag'));
        $this->assertSame(3600, $puts[0]['ttlSeconds']);
        $this->assertMatchesRegularExpression('#^tags/v1/jobs/[0-9a-f]{16}/8209999999749-[0-9a-f]{8}$#', $puts[0]['key']);
        $this->assertSame('completed', $puts[0]['value']['status']);
        $this->assertSame('App\Jobs\Charge', $puts[0]['value']['class']);
        $this->assertSame('2026-09-21T14:13:20Z', $puts[0]['value']['at']);

        // The monitored list is read again only after 30 seconds.
        $this->now += 1;
        $monitor->record($this->job(['customer:7']), 'failed');
        $this->assertSame('put', $this->operations($handler, 2)[0]['op']);
    }

    public function testAJobWithoutAMonitoredTagWritesNothing(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [['found' => false]]]],
        ]);
        $monitor = $this->monitor($handler);

        $monitor->record($this->job(['billing']), 'completed');
        $monitor->record($this->job([]), 'completed');

        $this->assertSame(1, $handler->count());
    }

    public function testMonitoringIsACompareAndSwapRetriedOnAConcurrentChange(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [['found' => true, 'value' => ['vip'], 'version' => 4]]]],
            ['status' => 200, 'json' => ['results' => [['applied' => false, 'reason' => 'version']]]],
            ['status' => 200, 'json' => ['results' => [['found' => true, 'value' => ['vip', 'other'], 'version' => 5]]]],
            ['status' => 200, 'json' => ['results' => [['applied' => true]]]],
        ]);

        $this->monitor($handler)->change(' customer:7 ', true);

        $put = $this->operations($handler, 3)[0];
        $this->assertSame(['vip', 'other', 'customer:7'], $put['value']);
        $this->assertSame(5, $put['expect']);
        $this->assertTrue($put['forever']);
    }

    public function testTheFirstTagCreatesTheListAndStoppingRemovesIt(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [['found' => false]]]],
            ['status' => 200, 'json' => ['results' => [['applied' => true]]]],
            ['status' => 200, 'json' => ['results' => [['found' => true, 'value' => ['vip', 'customer:7'], 'version' => 1]]]],
            ['status' => 200, 'json' => ['results' => [['applied' => true]]]],
        ]);
        $monitor = $this->monitor($handler);

        $monitor->change('vip', true);
        $monitor->change('vip', false);

        $this->assertSame(0, $this->operations($handler, 1)[0]['expect']);
        $this->assertSame(['customer:7'], $this->operations($handler, 3)[0]['value']);
    }

    public function testRecentJobsAreValidated(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['results' => [['rows' => [
            ['key' => 'a', 'value' => ['tag' => 'vip', 'class' => 'App\Jobs\A', 'status' => 'failed', 'attempts' => 3, 'runtime_ms' => 12, 'at' => '2026-09-30T10:00:00Z', 'queue' => 'high', 'job_id' => 'x']],
            ['key' => 'b', 'value' => ['tag' => 'other', 'class' => 'App\Jobs\B']],
            ['key' => 'c', 'value' => ['tag' => 'vip', 'status' => 'exploded', 'runtime_ms' => 'slow']],
        ], 'truncated' => false]]]]]);

        $jobs = $this->monitor($handler)->recent('vip');

        $this->assertCount(2, $jobs);
        $this->assertSame('failed', $jobs[0]['status']);
        $this->assertSame(['Unknown', 'unknown', null], [$jobs[1]['class'], $jobs[1]['status'], $jobs[1]['runtime_ms']]);
    }

    /** @param list<string> $tags */
    private function job(array $tags): Job
    {
        $job = $this->createStub(Job::class);
        $job->method('payload')->willReturn(['tags' => $tags]);
        $job->method('resolveName')->willReturn('App\Jobs\Charge');
        $job->method('getJobId')->willReturn('job-1');
        $job->method('getQueue')->willReturn('high');
        $job->method('attempts')->willReturn(1);

        return $job;
    }

    private function monitor(PlanHandler $handler): TagMonitor
    {
        $queen = new Queen(['url' => 'http://queen.test:6632', 'retryAttempts' => 1, 'retryDelayMillis' => 0, 'handler' => HandlerStack::create($handler)]);

        return new TagMonitor(fn (): Queen => $queen, 'queen-metrics', 3600, fn (): float => $this->now);
    }

    /** @return list<array<string, mixed>> */
    private function operations(PlanHandler $handler, int $request): array
    {
        return json_decode((string) $handler->requests[$request]->getBody(), true)['operations'];
    }
}
