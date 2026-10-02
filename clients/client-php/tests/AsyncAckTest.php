<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\LeaseRenewer;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

class AsyncAckTest extends TestCase
{
    private const ACKED = ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]];

    private const EMPTY_POP = ['status' => 200, 'json' => ['success' => true, 'messages' => []]];

    public function testEachAckIsSettledAtTheNextAckAndTheLeaseIsForgottenWithItsLastAck(): void
    {
        $handler = new PlanHandler([$this->pop(['job-1', 'job-2']), self::ACKED, self::ACKED, self::EMPTY_POP]);
        $renewer = new AsyncAckLeaseRenewer();
        $queue = $this->queue($handler, $renewer, prefetch: 2);

        $queue->pop('emails')->delete();
        $this->assertSame(['pop', 'ack'], $this->paths($handler));
        $this->assertSame([], $renewer->forgotten, 'the second job still holds the lease');

        $queue->pop('emails')->delete();
        $this->assertSame(['pop', 'ack', 'ack'], $this->paths($handler));
        // The lease's last job is done: renewing it would outlive the broker's lease.
        $this->assertSame(['lease-1'], $renewer->forgotten);

        $this->assertNull($queue->pop('emails'));
        $this->assertSame(['pop', 'ack', 'ack', 'pop'], $this->paths($handler));
        $this->assertSame('completed', json_decode((string) $handler->requests[1]->getBody(), true)['status']);
    }

    public function testARefusedAckIsReportedAndAbandonsTheLeaseAndItsLocalTail(): void
    {
        $handler = new PlanHandler([
            $this->pop(['job-1', 'job-2', 'job-3']),
            ['status' => 200, 'json' => [['success' => false, 'error' => 'lease moved']]],
            self::ACKED,
            self::EMPTY_POP,
        ]);
        $renewer = new AsyncAckLeaseRenewer();
        $queue = $this->queue($handler, $renewer, prefetch: 3);
        $reported = $this->captureReports($queue);

        $queue->pop('emails')->delete();
        // The second job already runs on the lease; its ACK reads the refusal.
        $queue->pop('emails')->delete();

        $this->assertCount(1, $reported);
        $this->assertStringContainsString('lease moved', $reported[0]->getMessage());
        $this->assertSame(['lease-1'], $renewer->forgotten);
        $this->assertNull($queue->pop('emails'), 'job-3 shared the abandoned lease');
        $this->assertSame(['pop', 'ack', 'ack', 'pop'], $this->paths($handler));
    }

    public function testALostAckIsRetriedWithTheOrdinaryClient(): void
    {
        $handler = new PlanHandler([
            $this->pop(['job-1']),
            ['status' => 503, 'json' => ['error' => 'unavailable']],
            self::ACKED,
            self::EMPTY_POP,
        ]);
        $renewer = new AsyncAckLeaseRenewer();
        $queue = $this->queue($handler, $renewer);
        $reported = $this->captureReports($queue);

        $queue->pop('emails')->delete();
        $this->assertNull($queue->pop('emails'));

        $this->assertCount(0, $reported);
        $this->assertSame(['lease-1'], $renewer->forgotten);
        $this->assertSame(['pop', 'ack', 'ack', 'pop'], $this->paths($handler));
    }

    public function testADefinitiveRefusalIsNotSentAgain(): void
    {
        $handler = new PlanHandler([
            $this->pop(['job-1']),
            ['status' => 409, 'json' => ['error' => 'lease moved']],
            self::EMPTY_POP,
        ]);
        $queue = $this->queue($handler, new AsyncAckLeaseRenewer());
        $reported = $this->captureReports($queue);

        $queue->pop('emails')->delete();
        $this->assertNull($queue->pop('emails'));

        $this->assertCount(1, $reported);
        $this->assertSame(['pop', 'ack', 'pop'], $this->paths($handler));
    }

    public function testAReportingFailureDoesNotEscapeIntoTheNextJob(): void
    {
        $handler = new PlanHandler([
            $this->pop(['job-1', 'job-2']),
            ['status' => 200, 'json' => [['success' => false, 'error' => 'lease moved']]],
            self::ACKED,
        ]);
        $queue = $this->queue($handler, new AsyncAckLeaseRenewer(), prefetch: 2);
        $container = new Container();
        $container->bind(ExceptionHandler::class, static fn () => throw new \RuntimeException('no logger'));
        $queue->setContainer($container);
        $previousLog = ini_set('error_log', '/dev/null');

        try {
            $queue->pop('emails')->delete();
            $queue->pop('emails')->delete();
        } finally {
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
        }

        $this->assertSame(['pop', 'ack', 'ack'], $this->paths($handler));
    }

    public function testShutdownSettlesThePendingAckWithoutARetry(): void
    {
        $handler = new PlanHandler([$this->pop(['job-1']), ['status' => 503, 'json' => ['error' => 'unavailable']]]);
        $renewer = new AsyncAckLeaseRenewer();
        $queue = $this->queue($handler, $renewer);
        $reported = $this->captureReports($queue);

        $queue->pop('emails')->delete();
        $queue->shutdown();

        $this->assertCount(1, $reported);
        $this->assertSame(['pop', 'ack'], $this->paths($handler), 'lease expiry covers a lost answer');
        $this->assertSame(1, $renewer->closed);
    }

    public function testAFailedJobIsDeadLetteredSynchronouslyAfterThePendingAck(): void
    {
        $handler = new PlanHandler([$this->pop(['job-1', 'job-2']), self::ACKED, self::ACKED]);
        $queue = $this->queue($handler, new AsyncAckLeaseRenewer(), prefetch: 2);

        $queue->pop('emails')->delete();
        $second = $queue->pop('emails');
        $queue->deleteReserved($second->getQueenMessage(), 'workers', failed: true, queue: 'emails');

        $this->assertSame(['pop', 'ack', 'ack'], $this->paths($handler));
        $this->assertSame('dlq', json_decode((string) $handler->requests[2]->getBody(), true)['status']);
    }

    public function testTheConnectorRequiresAckBatchOne(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ack_async requires ack_batch 1');

        (new QueenConnector())->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create(new PlanHandler()),
            'prefetch' => 2,
            'ack_batch' => 2,
            'ack_async' => true,
        ]);
    }

    private function queue(PlanHandler $handler, LeaseRenewer $renewer, int $prefetch = 1): QueenQueue
    {
        $queue = new QueenQueue(
            new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]),
            consumerGroup: 'workers',
            retryAfter: 120,
            prefetch: $prefetch,
            leaseRenewer: $renewer,
            ackAsync: true,
        );
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        return $queue;
    }

    /** @return \ArrayObject<int, \Throwable> */
    private function captureReports(QueenQueue $queue): \ArrayObject
    {
        $reported = new \ArrayObject();
        $container = new Container();
        $container->instance(ExceptionHandler::class, new class ($reported) implements ExceptionHandler {
            public function __construct(private \ArrayObject $reported)
            {
            }

            public function report(\Throwable $e)
            {
                $this->reported[] = $e;
            }

            public function shouldReport(\Throwable $e)
            {
                return true;
            }

            public function render($request, \Throwable $e)
            {
                throw $e;
            }

            public function renderForConsole($output, \Throwable $e)
            {
            }
        });
        $queue->setContainer($container);

        return $reported;
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
    private function pop(array $uuids): array
    {
        $messages = [];
        foreach ($uuids as $index => $uuid) {
            $number = $index + 1;
            $messages[] = [
                'id' => "message-{$number}",
                'transactionId' => "transaction-{$number}",
                'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => 'job-0001',
                'leaseId' => 'lease-1',
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

        return ['status' => 200, 'json' => ['success' => true, 'leaseId' => 'lease-1', 'messages' => $messages]];
    }
}

class AsyncAckLeaseRenewer implements LeaseRenewer
{
    /** @var list<string> */
    public array $forgotten = [];

    public int $closed = 0;

    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
    }

    public function forget(string $leaseId): void
    {
        $this->forgotten[] = $leaseId;
    }

    public function assertHealthy(string $leaseId): void
    {
    }

    public function close(): void
    {
        $this->closed++;
    }

    public function handBackJournal(): ?\Queen\Laravel\Queue\HandBackJournal
    {
        return null;
    }
}
