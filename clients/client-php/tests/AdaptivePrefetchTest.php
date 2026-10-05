<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Container\Container;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\AdaptiveBatch;
use Queen\Laravel\Queue\HandBackJournal;
use Queen\Laravel\Queue\LeaseRenewer;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * prefetch "auto": the connection accepts it like a prefetch above 1, and
 * the driver sizes each pop from its jobs' runtime, up to the ceiling.
 */
final class AdaptivePrefetchTest extends TestCase
{
    private const ACKED = ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]];

    private float $now = 1_000.0;

    public function testAutoNeedsLeaseRenewalLikeAnyPrefetchAboveOne(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('prefetch [auto] requires lease_renewal');

        (new QueenConnector())->connect([...$this->config(), 'prefetch' => 'auto', 'lease_renewal' => false]);
    }

    public function testAutoBuildsAnAdaptiveConnectionUpToTheCeiling(): void
    {
        $queue = (new QueenConnector())->connect([...$this->config(), 'prefetch' => 'auto', 'lease_renewal' => true]);

        $this->assertSame(AdaptiveBatch::CEILING, (new \ReflectionProperty($queue, 'prefetch'))->getValue($queue));
        $this->assertInstanceOf(AdaptiveBatch::class, (new \ReflectionProperty($queue, 'adaptiveBatch'))->getValue($queue));

        $fixed = (new QueenConnector())->connect([...$this->config(), 'prefetch' => 4, 'lease_renewal' => true]);
        $this->assertNull((new \ReflectionProperty($fixed, 'adaptiveBatch'))->getValue($fixed));
    }

    public function testAckBatchStaysWithinTheCeiling(): void
    {
        $queue = (new QueenConnector())->connect([
            ...$this->config(), 'prefetch' => 'auto', 'lease_renewal' => true, 'ack_batch' => AdaptiveBatch::CEILING,
        ]);
        $this->assertInstanceOf(QueenQueue::class, $queue);

        $this->expectException(InvalidArgumentException::class);
        (new QueenConnector())->connect([
            ...$this->config(), 'prefetch' => 'auto', 'lease_renewal' => true, 'ack_batch' => AdaptiveBatch::CEILING + 1,
        ]);
    }

    public function testAnyOtherWordIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new QueenConnector())->connect([...$this->config(), 'prefetch' => 'fast', 'lease_renewal' => true]);
    }

    public function testShortJobsGrowAfterTwoFullPopsAndTheBufferIsServedFirst(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            self::ACKED,
            $this->pop('lease-2', ['job-2']),
            self::ACKED,
            $this->pop('lease-3', ['job-3', 'job-4']),
            self::ACKED,
            self::ACKED,
            $this->pop('lease-4', ['job-5', 'job-6']),
        ]);
        $queue = $this->queue($handler);

        $this->runJob($queue, 'job-1');
        $this->runJob($queue, 'job-2');
        $this->runJob($queue, 'job-3');
        $this->runJob($queue, 'job-4');
        $this->assertSame('job-5', $queue->pop('emails')->getJobId());

        $this->assertSame(['1', '1', '2', '2'], $this->batches($handler), 'twice the batch after two full ones');
    }

    public function testLongJobsKeepAskingForOneJob(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            self::ACKED,
            $this->pop('lease-2', ['job-2']),
            self::ACKED,
            $this->pop('lease-3', ['job-3']),
        ]);
        $queue = $this->queue($handler);

        $this->runJob($queue, 'job-1', 2_000.0);
        $this->runJob($queue, 'job-2', 2_000.0);
        $queue->pop('emails');

        $this->assertSame(['1', '1', '1'], $this->batches($handler));
    }

    /** @return array<string, mixed> */
    private function config(): array
    {
        return [
            'url' => 'http://queen.test:6632',
            'queue' => 'default',
            'consumer_group' => 'workers',
            'partitions' => 8,
            'retry_after' => 90,
            'block_for' => 0,
        ];
    }

    private function queue(PlanHandler $handler): QueenQueue
    {
        $queue = new QueenQueue(
            new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]),
            consumerGroup: 'workers',
            retryAfter: 120,
            prefetch: AdaptiveBatch::CEILING,
            leaseRenewer: new AdaptivePrefetchLeaseRenewer(),
            adaptiveBatch: new AdaptiveBatch(clock: fn (): float => $this->now),
        );
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        return $queue;
    }

    private function runJob(QueenQueue $queue, string $expected, float $millis = 5.0): void
    {
        $job = $queue->pop('emails');
        $this->assertSame($expected, $job->getJobId());
        $this->now += $millis;
        $job->delete();
    }

    /** @return list<string> the batch each pop asked for */
    private function batches(PlanHandler $handler): array
    {
        $batches = [];
        foreach ($handler->requests as $request) {
            if (str_contains($request->getUri()->getPath(), '/ack')) {
                continue;
            }
            parse_str($request->getUri()->getQuery(), $query);
            $batches[] = $query['batch'];
        }

        return $batches;
    }

    private function pop(string $leaseId, array $uuids): array
    {
        $messages = [];
        foreach ($uuids as $uuid) {
            $number = substr($uuid, 4);
            $messages[] = [
                'id' => "message-{$number}",
                'transactionId' => "transaction-{$number}",
                'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => 'job-0001',
                'leaseId' => $leaseId,
                'consumerGroup' => 'workers',
                'deliveryAttempt' => 1,
                'data' => [
                    'uuid' => $uuid,
                    'displayName' => 'Handler',
                    'job' => 'Handler@handle',
                    'maxTries' => null,
                    'timeout' => null,
                    'data' => [],
                ],
            ];
        }

        return ['status' => 200, 'json' => ['success' => true, 'leaseId' => $leaseId, 'messages' => $messages]];
    }
}

final class AdaptivePrefetchLeaseRenewer implements LeaseRenewer
{
    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
    }

    public function forget(string $leaseId): void
    {
    }

    public function assertHealthy(string $leaseId): void
    {
    }

    public function close(): void
    {
    }

    public function handBackJournal(): ?HandBackJournal
    {
        return null;
    }
}
