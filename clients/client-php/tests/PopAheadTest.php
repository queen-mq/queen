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

    private const COMMITTED = ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']];

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

    public function testAShortBatchMeansTheQueueIsNearlyEmptySoNothingIsPoppedAhead(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            self::ACKED,
            $this->pop('lease-2', ['job-2', 'job-3']),
            self::ACKED,
            $this->pop('lease-3', ['job-4', 'job-5']),
        ]);
        $queue = $this->queue($handler, new PopAheadLeaseRenewer(), prefetch: 2);

        $queue->pop('emails')->delete();
        $this->assertSame(['pop', 'ack'], $this->paths($handler), 'one job of two: no backlog to pop ahead into');

        $queue->pop('emails')->delete();
        $this->assertSame(['pop', 'ack', 'pop', 'ack'], $this->paths($handler));
        $this->assertSame('job-3', $queue->pop('emails')->getJobId());
        $this->assertSame(['pop', 'ack', 'pop', 'ack', 'pop'], $this->paths($handler), 'a full batch pops ahead again');
    }

    public function testAShortBatchPoppedAheadStopsPoppingAheadUntilAFullOne(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1', 'job-2']),
            self::ACKED,
            $this->pop('lease-2', ['job-3']),
            self::ACKED,
            self::ACKED,
            $this->pop('lease-3', ['job-4', 'job-5']),
        ]);
        $queue = $this->queue($handler, new PopAheadLeaseRenewer(), prefetch: 2);

        $queue->pop('emails')->delete();
        $queue->pop('emails')->delete();
        $this->assertSame(['pop', 'ack', 'pop', 'ack'], $this->paths($handler), 'job-3 was popped ahead with job-2');

        $third = $queue->pop('emails');
        $this->assertSame('job-3', $third->getJobId());
        $third->delete();
        $this->assertSame(['pop', 'ack', 'pop', 'ack', 'ack'], $this->paths($handler), 'one job of two: none popped ahead with job-3');

        $this->assertSame('job-4', $queue->pop('emails')->getJobId());
        $this->assertSame(['pop', 'ack', 'pop', 'ack', 'ack', 'pop'], $this->paths($handler));
    }

    public function testABatchTooLateToTrackIsHandedBackAtOnceWithoutChargingAnAttempt(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            // A redelivered batch: the attempt it reports must survive.
            $this->pop('lease-2', ['job-2', 'job-3'], deliveryAttempt: 2),
            self::ACKED,
            self::COMMITTED,
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $renewer->refuse = 'lease-2';
        $queue = $this->queue($handler, $renewer);

        $queue->pop('emails')->delete();

        $this->assertSame(['pop', 'pop', 'ack', 'transaction'], $this->paths($handler));
        $handBack = $this->handBack($handler->requests[3]);
        $this->assertSame(['transaction-2', 'transaction-3'], array_column($handBack['acks'], 'transactionId'));
        $this->assertSame(['completed', 'completed'], array_column($handBack['acks'], 'status'));
        $this->assertSame(['lease-2'], $handBack['body']['requiredLeases']);
        $this->assertSame(['job-2', 'job-3'], array_map(
            static fn (array $copy): string => $copy['payload']['uuid'],
            $handBack['copies'],
        ));
        $this->assertSame([1, 1], array_map(
            static fn (array $copy): int => $copy['payload']['_queen']['attempts'],
            $handBack['copies'],
        ));
        $this->assertSame(2, $this->redeliveredAttempts($handBack['copies'][0]), 'as it was when popped ahead');
    }

    public function testShutdownHandsBackTheBatchPoppedAhead(): void
    {
        $handler = new PlanHandler([
            $this->pop('lease-1', ['job-1']),
            $this->pop('lease-2', ['job-2']),
            self::COMMITTED,
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $queue = $this->queue($handler, $renewer);

        $queue->pop('emails');
        $queue->shutdown();

        $this->assertSame(['pop', 'pop', 'transaction'], $this->paths($handler));
        $handBack = $this->handBack($handler->requests[2]);
        $this->assertSame(['transaction-2'], array_column($handBack['acks'], 'transactionId'));
        $this->assertSame(['job-2'], array_map(
            static fn (array $copy): string => $copy['payload']['uuid'],
            $handBack['copies'],
        ));
        $this->assertSame(1, $this->redeliveredAttempts($handBack['copies'][0]));
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
            self::COMMITTED,
        ]);
        $renewer = new PopAheadLeaseRenewer();
        $queue = $this->queue($handler, $renewer, retryAfter: 1);

        $first = $queue->pop('emails');
        usleep(600_000);
        $first->delete();

        $this->assertSame(['pop', 'pop', 'ack', 'transaction'], $this->paths($handler));
        $handBack = $this->handBack($handler->requests[3]);
        $this->assertSame(['completed'], array_column($handBack['acks'], 'status'));
        $this->assertSame(0, $handBack['copies'][0]['payload']['_queen']['attempts']);
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
            static fn ($request): string => match (true) {
                str_contains($request->getUri()->getPath(), '/ack') => 'ack',
                str_contains($request->getUri()->getPath(), '/transaction') => 'transaction',
                default => 'pop',
            },
            $handler->requests,
        );
    }

    /** @return array{body: array, acks: list<array>, copies: list<array>} */
    private function handBack($request): array
    {
        $this->assertSame('/api/v1/transaction', $request->getUri()->getPath());
        $body = json_decode((string) $request->getBody(), true, 512, JSON_THROW_ON_ERROR);
        $acks = [];
        $copies = [];
        foreach ($body['operations'] as $operation) {
            if ($operation['type'] === 'ack') {
                $acks[] = $operation;
            } else {
                array_push($copies, ...$operation['items']);
            }
        }

        return ['body' => $body, 'acks' => $acks, 'copies' => $copies];
    }

    /** The attempt Laravel sees when the broker delivers a copy for the first time. */
    private function redeliveredAttempts(array $copy): int
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => ['success' => true, 'messages' => [[
                'id' => 'message-copy',
                'transactionId' => $copy['transactionId'],
                'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => $copy['partition'],
                'leaseId' => 'lease-copy',
                'consumerGroup' => 'workers',
                'deliveryAttempt' => 1,
                'data' => $copy['payload'],
            ]]],
        ]]);
        $queue = new QueenQueue(
            new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]),
            consumerGroup: 'workers',
        );
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        return $queue->pop($copy['queue'])->attempts();
    }

    /** @param list<string> $uuids */
    private function pop(string $leaseId, array $uuids, int $deliveryAttempt = 1): array
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
                'deliveryAttempt' => $deliveryAttempt,
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
