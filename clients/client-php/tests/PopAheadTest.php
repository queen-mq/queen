<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Container\Container;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\LeaseRenewer;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

class PopAheadTest extends TestCase
{
    private const ACKED = ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]];

    public function testTheNextBatchIsPoppedWithTheLastJobAndTrackedWhenItEnds(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1', 'job-2']),
            self::ACKED,
            $this->pop('lease-2', ['job-3', 'job-4']),
            self::ACKED,
            self::ACKED,
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $queue = $this->queue($handler, $renewer, prefetch: 2);

        $queue->pop('emails')->delete();
        $this->assertSame(['pop', 'ack'], $this->paths($handler), 'the buffer still holds job-2');

        $second = $queue->pop('emails');
        $this->assertSame('job-2', $second->getJobId());
        $this->assertSame(['pop', 'ack', 'pop'], $this->paths($handler), 'the next batch travels with job-2');
        $this->assertSame(['track lease-1'], $renewer->calls, 'not taken in while job-2 runs');

        $second->delete();
        $this->assertSame(['track lease-1', 'forget lease-1', 'track lease-2'], $renewer->calls);

        $third = $queue->pop('emails');
        $this->assertSame('job-3', $third->getJobId());
        $this->assertSame(['pop', 'ack', 'pop', 'ack'], $this->paths($handler), 'job-3 came from the buffer');
        parse_str($handler->requests[2]->getUri()->getQuery(), $query);
        $this->assertSame('false', $query['wait'], 'a pop ahead never long-polls');
    }

    public function testABatchTooLateToTrackIsHandedBackAtOnce(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            $this->pop('lease-2', ['job-2', 'job-3']),
            self::ACKED,
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $renewer->refuse = 'lease-2';
        $queue = $this->queue($handler, $renewer);

        $queue->pop('emails')->delete();

        $this->assertSame(['pop', 'pop', 'ack', 'ack'], $this->paths($handler));
        $release = json_decode((string) $handler->requests[3]->getBody(), true);
        $this->assertSame('/api/v1/ack/batch', $handler->requests[3]->getUri()->getPath());
        $this->assertSame(['transaction-2'], array_column($release['acknowledgments'], 'transactionId'), 'one per partition');
        $this->assertSame('retry', $release['acknowledgments'][0]['status']);
    }

    public function testShutdownHandsBackTheBatchPoppedAhead(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            $this->pop('lease-2', ['job-2']),
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $queue = $this->queue($handler, $renewer);

        $queue->pop('emails');
        $queue->shutdown();

        $this->assertSame(['pop', 'pop', 'ack'], $this->paths($handler));
        $release = json_decode((string) $handler->requests[2]->getBody(), true);
        $this->assertSame(['transaction-2'], array_column($release['acknowledgments'], 'transactionId'));
        $this->assertNotContains('track lease-2', $renewer->calls, 'released, never renewed');
    }

    public function testAWorkerOnAQueueListNeverPopsAhead(): void
    {
        $empty = ['status' => 200, 'json' => ['success' => true, 'messages' => []]];
        $handler = new PlanHandler([$empty, $this->pop('lease-1', ['job-1'])]);
        $queue = $this->queue($handler, new PopAheadLeaseRenewer());

        $this->assertNull($queue->pop('high'));
        $this->assertSame('job-1', $queue->pop('low')->getJobId());

        $this->assertSame(['pop', 'pop'], $this->paths($handler), 'the next job may come from high');
    }

    public function testNoBatchIsPoppedAheadAfterALongJob(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            $this->pop('lease-2', ['job-2']),
            self::ACKED,
            self::ACKED,
        ]);
        // A job over a third of retry_after (2 s) is long.
        $queue = $this->queue($handler, new PopAheadLeaseRenewer(), retryAfter: 2);

        $first = $queue->pop('emails');
        usleep(700_000);
        $first->delete();
        $this->assertSame(['pop', 'pop', 'ack'], $this->paths($handler), 'job-1 was quick to hand out');

        $queue->pop('emails')->delete();
        $this->assertSame(['pop', 'pop', 'ack', 'ack'], $this->paths($handler), 'job-2 came after a long job');
    }

    public function testABatchThatWaitedHalfItsLeaseIsHandedBackUntracked(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            $this->pop('lease-2', ['job-2']),
            self::ACKED,
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $queue = $this->queue($handler, $renewer, retryAfter: 1);

        $first = $queue->pop('emails');
        usleep(600_000);
        $first->delete();

        $this->assertSame(['pop', 'pop', 'ack', 'ack'], $this->paths($handler));
        $this->assertSame('retry', json_decode((string) $handler->requests[3]->getBody(), true)['acknowledgments'][0]['status']);
        $this->assertNotContains('track lease-2', $renewer->calls);
    }

    public function testAsynchronousAcknowledgementsAndPopAheadTogether(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            $this->pop('lease-2', ['job-2']),
            self::ACKED,
            ['status' => 200, 'json' => ['success' => true, 'messages' => []]],
            self::ACKED,
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $queue = $this->queue($handler, $renewer, ackAsync: true);

        $queue->pop('emails')->delete();
        $this->assertSame(['track lease-1', 'forget lease-1', 'track lease-2'], $renewer->calls);

        $second = $queue->pop('emails');
        $this->assertSame('job-2', $second->getJobId());
        $second->delete();
        // job-2 came from the buffer; the fourth request pops ahead for the next batch.
        $this->assertSame(['pop', 'pop', 'ack', 'pop', 'ack'], $this->paths($handler));
        $queue->shutdown();
        $this->assertSame(5, $handler->count(), 'the pending ACK settled without a retry');
    }

    public function testTheConnectorRequiresLeaseRenewal(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('pop_ahead requires lease_renewal');

        (new QueenConnector())->connect(['url' => 'http://queen.test:6632', 'pop_ahead' => true]);
    }

    private function queue(
        PlanHandler $handler,
        LeaseRenewer $renewer,
        int $prefetch = 1,
        int $retryAfter = 120,
        bool $ackAsync = false,
    ): QueenQueue
    {
        $queue = new QueenQueue(
            new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]),
            consumerGroup: 'workers',
            retryAfter: $retryAfter,
            prefetch: $prefetch,
            leaseRenewer: $renewer,
            ackAsync: $ackAsync,
            popAhead: true,
        );
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        return $queue;
    }

    /** @return list<string> */
    private function paths(PlanHandler $handler): array
    {
        return array_map(
            static fn ($request): string => str_contains($request->getUri()->getPath(), '/ack') ? 'ack' : 'pop',
            $handler->requests,
        );
    }

    /** @param list<string> $uuids */
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

class PopAheadLeaseRenewer implements LeaseRenewer
{
    /** @var list<string> */
    public array $calls = [];

    public ?string $refuse = null;

    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
        if ($leaseId === $this->refuse) {
            throw new \RuntimeException("Queen lease [{$leaseId}] reached its renewal deadline before tracking began.");
        }
        $this->calls[] = "track {$leaseId}";
    }

    public function forget(string $leaseId): void
    {
        $this->calls[] = "forget {$leaseId}";
    }

    public function assertHealthy(string $leaseId): void
    {
    }

    public function close(): void
    {
    }
}
