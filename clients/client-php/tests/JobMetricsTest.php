<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Contracts\Queue\Job;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Dashboard\JobMetricsReader;
use Queen\Laravel\Monitoring\JobMetricsRecorder;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class JobMetricsTest extends TestCase
{
    private float $now = 1_790_000_100.0;

    public function testAWorkerWritesItsCountsOncePerFlushWindow(): void
    {
        $handler = $this->applying();
        $recorder = $this->recorder($handler);

        $this->process($recorder, 'App\Jobs\SendInvoice', false);
        $this->now += 3;
        $this->process($recorder, 'App\Jobs\SendInvoice', true);
        $this->process($recorder, 'App\Jobs\Resize', false);
        $this->assertSame(1, $handler->count());

        $this->now += 8;
        $this->process($recorder, 'App\Jobs\Resize', false);
        $this->assertSame(2, $handler->count());

        $put = $this->operations($handler, 1)[0];
        $this->assertSame('put', $put['op']);
        $this->assertSame('queen-metrics', $put['ns']);
        $this->assertMatchesRegularExpression('#^jobs/v1/1790000100/[0-9a-f]{16}$#', $put['key']);
        $this->assertSame(JobMetricsRecorder::TTL_SECONDS, $put['ttlSeconds']);
        $this->assertSame(['processed' => 1, 'failed' => 1], array_intersect_key($put['value']['classes']['App\Jobs\SendInvoice'], ['processed' => 0, 'failed' => 0]));
        $this->assertSame(2, $put['value']['classes']['App\Jobs\Resize']['processed']);
    }

    public function testAnIdleWorkerWritesWhatItsLastBurstLeft(): void
    {
        $handler = $this->applying();
        $recorder = $this->recorder($handler);
        $this->process($recorder, 'App\Jobs\A', false);
        $this->now += 3;
        $this->process($recorder, 'App\Jobs\A', false);
        $this->assertSame(1, $handler->count());

        $this->now += 5;
        $recorder->tick();
        $this->assertSame(1, $handler->count());

        $this->now += 5;
        $recorder->tick();
        $recorder->tick();
        $this->assertSame(2, $handler->count());
        $this->assertSame(2, $this->operations($handler, 1)[0]['value']['classes']['App\Jobs\A']['processed']);
    }

    public function testANewBucketWritesThePreviousWindowFirst(): void
    {
        $handler = $this->applying();
        $recorder = $this->recorder($handler);
        $this->process($recorder, 'App\Jobs\A', false);
        $this->now += 2;
        $this->process($recorder, 'App\Jobs\A', false);

        $this->now += 300;
        $this->process($recorder, 'App\Jobs\B', false);
        // A worker that stops writes what it holds.
        $recorder->flush();

        $keys = array_map(fn (int $request): string => $this->operations($handler, $request)[0]['key'], range(0, $handler->count() - 1));
        $this->assertSame(2, (int) preg_match_all('#jobs/v1/1790000100/#', implode(' ', $keys)));
        $last = $this->operations($handler, $handler->count() - 1)[0];
        $this->assertStringStartsWith('jobs/v1/1790000400/', $last['key']);
        $this->assertSame(['App\Jobs\B'], array_keys($last['value']['classes']));
    }

    public function testClassesBeyondTheCapAreGroupedAndABrokerErrorNeverReachesTheJob(): void
    {
        $handler = new PlanHandler([], ['status' => 503, 'json' => []]);
        $recorder = $this->recorder($handler);

        for ($i = 0; $i <= JobMetricsRecorder::MAX_CLASSES; ++$i) {
            $this->process($recorder, "App\\Jobs\\Job{$i}", false);
        }
        $this->now += 11;
        $this->process($recorder, 'App\Jobs\Late', false);

        $this->assertGreaterThanOrEqual(1, $handler->count());
    }

    public function testTheReaderSumsEveryWorkerOverTheWindow(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['rows' => [
                ['key' => 'jobs/v1/1790000100/aaaa', 'value' => ['classes' => ['App\Jobs\Send' => ['processed' => 10, 'failed' => 1, 'runtime_ms' => 1100]]]],
                ['key' => 'jobs/v1/1790000100/bbbb', 'value' => ['classes' => ['App\Jobs\Send' => ['processed' => 5, 'failed' => 0, 'runtime_ms' => 400]]]],
            ], 'truncated' => true]],
            ['status' => 200, 'json' => ['rows' => [
                ['key' => 'jobs/v1/1790000400/aaaa', 'value' => ['classes' => [
                    'App\Jobs\Resize' => ['processed' => 2, 'failed' => 2, 'runtime_ms' => 4000],
                    "Bad\nName" => ['processed' => 99],
                    'App\Jobs\Negative' => ['processed' => -5],
                ]]],
                ['key' => 'jobs/v1/1790000400/cccc', 'value' => 'not metrics'],
            ], 'truncated' => false]],
        ]);
        $reader = new JobMetricsReader($this->queen($handler), 'queen-metrics', null, fn (): int => 1_790_000_500);

        $metrics = $reader->read('1h');

        $this->assertTrue($metrics['available']);
        $this->assertFalse($metrics['truncated']);
        $this->assertSame(['processed' => 17, 'failed' => 3], $metrics['totals']);
        $this->assertSame(['App\Jobs\Send', 'App\Jobs\Resize'], array_column($metrics['classes'], 'class'));
        $this->assertSame(93, $metrics['classes'][0]['average_ms']);
        $this->assertSame(1000, $metrics['classes'][1]['average_ms']);
        $first = $this->operations($handler, 0)[0];
        // The window starts at the first bucket still inside the last hour.
        $this->assertSame('jobs/v1/1789997099', $first['after']);
        $this->assertSame('jobs/v1/', $first['prefix']);
        $this->assertSame('jobs/v1/1790000100/bbbb', $this->operations($handler, 1)[0]['after']);
    }

    public function testAWindowLargerThanOneReadIsFlaggedPartial(): void
    {
        $page = ['status' => 200, 'json' => ['rows' => [
            ['key' => 'jobs/v1/1790000100/aaaa', 'value' => ['classes' => ['App\Jobs\Send' => ['processed' => 1]]]],
        ], 'truncated' => true]];
        $handler = new PlanHandler([], $page);
        $reader = new JobMetricsReader($this->queen($handler), 'queen-metrics', null, fn (): int => 1_790_000_500);

        $metrics = $reader->read('24h');

        $this->assertTrue($metrics['available']);
        $this->assertTrue($metrics['truncated']);
        $this->assertSame(50, $handler->count());
        $this->assertSame(1000, $this->operations($handler, 0)[0]['limit']);
    }

    public function testAnUnreachableBrokerIsReportedUnavailable(): void
    {
        $reader = new JobMetricsReader($this->queen(new PlanHandler([], ['status' => 503, 'json' => []])), 'queen-metrics');

        $this->assertFalse($reader->read('6h')['available']);
        $this->assertSame('1h', JobMetricsReader::range('7d'));
    }

    private function process(JobMetricsRecorder $recorder, string $class, bool $failed): void
    {
        $job = $this->createStub(Job::class);
        $job->method('resolveName')->willReturn($class);
        $recorder->start($job);
        $recorder->finish($job, $failed);
    }

    private function recorder(PlanHandler $handler): JobMetricsRecorder
    {
        $queen = $this->queen($handler);

        return new JobMetricsRecorder(fn (): Queen => $queen, 'queen-metrics', fn (): float => $this->now);
    }

    private function applying(): PlanHandler
    {
        return new PlanHandler([], ['status' => 200, 'json' => ['results' => [['applied' => true]]]]);
    }

    private function queen(PlanHandler $handler): Queen
    {
        return new Queen(['url' => 'http://queen.test:6632', 'retryAttempts' => 1, 'retryDelayMillis' => 0, 'handler' => HandlerStack::create($handler)]);
    }

    /** @return list<array<string, mixed>> */
    private function operations(PlanHandler $handler, int $request): array
    {
        return json_decode((string) $handler->requests[$request]->getBody(), true)['operations'];
    }
}
