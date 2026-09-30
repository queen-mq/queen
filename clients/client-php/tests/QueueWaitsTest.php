<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Monitoring\QueueWaits;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class QueueWaitsTest extends TestCase
{
    public function testNothingWaitsWhenTheGroupHasNoPendingJob(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['pending' => 0, 'effectivePending' => 0]]]);

        $this->assertSame(0, $this->waits($handler)->seconds('high', 'laravel'));
        $this->assertSame(1, $handler->count());
    }

    public function testTheGroupsLagIsTheWait(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['effectivePending' => 4]],
            ['status' => 200, 'json' => [
                ['queue_name' => 'high', 'consumer_group' => 'laravel', 'time_lag_seconds' => 40],
                ['queue_name' => 'high', 'consumer_group' => 'laravel', 'time_lag_seconds' => 95],
                ['queue_name' => 'high', 'consumer_group' => 'emails', 'time_lag_seconds' => 900],
                ['queue_name' => 'low', 'consumer_group' => 'laravel', 'time_lag_seconds' => 900],
            ]],
        ]);

        $this->assertSame(95, $this->waits($handler)->seconds('high', 'laravel'));
        $this->assertStringContainsString('minLagSeconds=0', (string) $handler->requests[1]->getUri());
    }

    public function testAQueueNoWorkerEverServedWaitsSinceItsOldestPendingJob(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['effectivePending' => 2]],
            ['status' => 200, 'json' => []],
            ['status' => 200, 'json' => ['partitions' => [
                ['oldestMessage' => '2026-09-30T12:00:00Z', 'stats' => ['pending' => 1]],
                ['oldestMessage' => '2026-09-30T11:58:00Z', 'stats' => ['pending' => 1]],
                ['oldestMessage' => '2026-09-30T11:00:00Z', 'stats' => ['pending' => 0]],
            ]]],
        ]);

        $this->assertSame(300, $this->waits($handler, strtotime('2026-09-30T12:03:00Z'))->seconds('high', 'laravel'));
        $this->assertSame('/api/v1/resources/queues/high', $handler->requests[2]->getUri()->getPath());
    }

    public function testAMalformedDepthIsAnError(): void
    {
        $this->expectException(\UnexpectedValueException::class);

        $this->waits(new PlanHandler([['status' => 200, 'json' => ['pending' => 'many']]]))->seconds('high', 'laravel');
    }

    private function waits(PlanHandler $handler, ?int $now = null): QueueWaits
    {
        return new QueueWaits(
            new Queen(['url' => 'http://queen.test:6632', 'retryAttempts' => 1, 'retryDelayMillis' => 0, 'handler' => HandlerStack::create($handler)]),
            $now === null ? null : fn (): int => $now,
        );
    }
}
