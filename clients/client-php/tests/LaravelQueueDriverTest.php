<?php

namespace Queen\Tests;

use DateInterval;
use DateTimeImmutable;
use GuzzleHttp\HandlerStack;
use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Events\Dispatcher;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\WorkerStopping;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\ConflationPolicyMismatchException;
use Queen\Exceptions\HttpException;
use Queen\Laravel\Contracts\QueenPartitionable;
use Queen\Laravel\Queue\HandBackJournal;
use Queen\Laravel\Queue\LeaseRenewer;
use Queen\Laravel\Queue\NotALaravelJobException;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\QueenJob;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Laravel\Queue\UnsafeJobTimeoutException;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;
use Queen\Tests\Support\RecordingExceptionHandler;

class LaravelQueueDriverTest extends TestCase
{
    /** @var list<string> */
    private array $journalDirectories = [];

    protected function tearDown(): void
    {
        foreach ($this->journalDirectories as $directory) {
            foreach (glob($directory . '/*') ?: [] as $file) {
                @unlink($file);
            }
            @rmdir($directory);
        }
        parent::tearDown();
    }

    public function testConnectorBuildsAQueenQueue(): void
    {
        [$queue] = $this->queueFor(new PlanHandler());

        $this->assertInstanceOf(QueenQueue::class, $queue);
        $this->assertSame('workers', $queue->getConsumerGroup());
    }

    public function testSupervisorCanOverrideTheConsumerGroupPerWorkerProcess(): void
    {
        putenv('QUEEN_LARAVEL_CONSUMER_GROUP=priority-workers');
        try {
            [$queue] = $this->queueFor(new PlanHandler());
            $this->assertSame('priority-workers', $queue->getConsumerGroup());
        } finally {
            putenv('QUEEN_LARAVEL_CONSUMER_GROUP');
        }
    }

    public function testSupervisorCanOverrideTheLeasePerWorkerProcess(): void
    {
        putenv('QUEEN_LARAVEL_RETRY_AFTER=180');
        try {
            $handler = new PlanHandler([[
                'status' => 200,
                'json' => $this->popResponse($this->payload('job-123')),
            ]]);
            [$queue] = $this->queueFor($handler);
            $queue->pop('emails');
            parse_str($handler->requests[0]->getUri()->getQuery(), $query);
            $this->assertSame('180', $query['leaseSeconds']);
        } finally {
            putenv('QUEEN_LARAVEL_RETRY_AFTER');
        }
    }

    public function testPrioritySupervisorCanDisableLongPollingPerWorkerProcess(): void
    {
        putenv('QUEEN_LARAVEL_BLOCK_FOR=0');
        try {
            $handler = new PlanHandler([[
                'status' => 200,
                'json' => ['success' => true, 'messages' => []],
            ]]);
            $connector = new QueenConnector();
            $queue = $connector->connect([
                'url' => 'http://queen.test:6632',
                'handler' => HandlerStack::create($handler),
                'consumer_group' => 'workers',
                'block_for' => 30,
            ]);

            $queue->pop('high');

            parse_str($handler->requests[0]->getUri()->getQuery(), $query);
            $this->assertSame('false', $query['wait']);
            $this->assertSame('30000', $query['timeout']);
        } finally {
            putenv('QUEEN_LARAVEL_BLOCK_FOR');
        }
    }

    public function testBlockingWorkerUsesBlockForAsTheBrokerPollTimeout(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => ['success' => true, 'messages' => []],
        ]]);
        [$queue] = $this->queueFor($handler, ['block_for' => 30]);

        $queue->pop('high');

        parse_str($handler->requests[0]->getUri()->getQuery(), $query);
        $this->assertSame('true', $query['wait']);
        $this->assertSame('30000', $query['timeout']);
    }

    public function testPopPinsBothSizingKnobsWhileAutopilotIsOff(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => ['success' => true, 'messages' => []],
        ]]);
        [$queue] = $this->queueFor($handler, ['prefetch' => 4]);

        $this->assertNull($queue->pop('emails'));

        parse_str($handler->requests[0]->getUri()->getQuery(), $query);
        $this->assertArrayNotHasKey('autopilot', $query);
        $this->assertSame('4', $query['batch']);
        $this->assertSame('8', $query['partitions']);
    }

    public function testAutopilotDelegatesOnlyTheSweepWidthToTheBroker(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => ['success' => true, 'messages' => []],
        ]]);
        [$queue] = $this->queueFor($handler, ['autopilot' => true, 'prefetch' => 4]);

        $this->assertNull($queue->pop('emails'));

        parse_str($handler->requests[0]->getUri()->getQuery(), $query);
        $this->assertSame('true', $query['autopilot']);
        // The batch stays the worker's own prefetch budget; only the width is
        // left for the broker to size, so it must not travel.
        $this->assertSame('4', $query['batch']);
        $this->assertArrayNotHasKey('partitions', $query);
    }

    public function testAutopilotLeavesThePushStripeModulusAlone(): void
    {
        $handler = new PlanHandler([['status' => 201, 'json' => [['status' => 'queued']]]]);
        [$queue] = $this->queueFor($handler, ['autopilot' => true]);

        $this->assertSame('job-123', $queue->pushRaw(json_encode($this->payload('job-123')), 'emails'));

        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $expectedSlot = hexdec(substr(hash('sha256', 'job-123'), 0, 8)) % 8;
        $this->assertSame(sprintf('job-%04d', $expectedSlot), $body['items'][0]['partition']);
    }

    public function testPushRawStoresTheLaravelPayloadOnADeterministicStripe(): void
    {
        $handler = new PlanHandler([["status" => 201, "json" => [["status" => "queued"]]]]);
        [$queue] = $this->queueFor($handler);
        $payload = $this->payload('job-123');

        $this->assertSame('job-123', $queue->pushRaw(json_encode($payload), 'emails'));

        $request = $handler->requests[0];
        $body = json_decode((string) $request->getBody(), true);
        $expectedSlot = hexdec(substr(hash('sha256', 'job-123'), 0, 8)) % 8;

        $this->assertSame('POST', $request->getMethod());
        $this->assertSame('/api/v1/push', $request->getUri()->getPath());
        $this->assertSame('emails', $body['items'][0]['queue']);
        $this->assertSame(sprintf('job-%04d', $expectedSlot), $body['items'][0]['partition']);
        $this->assertSame('job-123', $body['items'][0]['transactionId']);
        $this->assertSame($payload['job'], $body['items'][0]['payload']['job']);
        $this->assertSame(0, $body['items'][0]['payload']['_queen']['attempts']);
    }

    public function testManualRetryBypassesOriginalDeduplicationAndResetsAttempts(): void
    {
        $handler = new PlanHandler([['status' => 201, 'json' => [['status' => 'queued']]]]);
        [$queue] = $this->queueFor($handler);
        $payload = $this->payload('job-123');
        $payload['_queen'] = [
            'partition' => 'customer-42',
            'attempts' => 7,
            'manual_retry' => 'retry-transaction-123',
        ];

        $this->assertSame('job-123', $queue->pushRaw(json_encode($payload), 'emails'));

        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $item = $body['items'][0];
        $this->assertSame('retry-transaction-123', $item['transactionId']);
        $this->assertSame('customer-42', $item['partition']);
        $this->assertSame(0, $item['payload']['_queen']['attempts']);
        $this->assertArrayNotHasKey('manual_retry', $item['payload']['_queen']);
    }

    public function testAPushedJobCarriesItsTagsInThePayload(): void
    {
        $handler = new PlanHandler([['status' => 201, 'json' => [['status' => 'queued']]]]);
        [$queue] = $this->queueFor($handler);

        $queue->push(new QueenTaggedTestJob(), '', 'emails');

        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $this->assertSame(['billing', 'customer:7'], $body['items'][0]['payload']['tags']);
    }

    public function testDuplicatePushIsAcceptedAsAnIdempotentSuccess(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [['status' => 'duplicate']]]]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame('job-123', $queue->pushRaw(json_encode($this->payload('job-123')), 'emails'));
    }

    public function testBufferedPushIsAccepted(): void
    {
        $handler = new PlanHandler([['status' => 202, 'json' => [['status' => 'buffered']]]]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame('job-123', $queue->pushRaw(json_encode($this->payload('job-123')), 'emails'));
    }

    public function testBulkPushUsesOneMultiPartitionRequest(): void
    {
        $handler = new PlanHandler([['status' => 201, 'json' => [
            ['status' => 'queued'],
            ['status' => 'queued'],
            ['status' => 'duplicate'],
        ]]]);
        [$queue] = $this->queueFor($handler, ['bulk_batch' => 100]);

        $queue->bulk([
            new PartitionedTestJob('customer-1'),
            new PartitionedTestJob('customer-2'),
            new PartitionedTestJob('customer-3'),
        ], queue: 'orders');

        $this->assertCount(1, $handler->requests);
        $request = $handler->requests[0];
        $body = json_decode((string) $request->getBody(), true);
        $this->assertSame('/api/v1/push', $request->getUri()->getPath());
        $this->assertSame(['customer-1', 'customer-2', 'customer-3'], array_column($body['items'], 'partition'));
        $this->assertSame(['orders', 'orders', 'orders'], array_column($body['items'], 'queue'));
        $this->assertCount(3, array_unique(array_column($body['items'], 'transactionId')));
    }

    public function testBulkPushUsesBoundedChunks(): void
    {
        $handler = new PlanHandler([
            ['status' => 201, 'json' => [['status' => 'queued'], ['status' => 'queued']]],
            ['status' => 201, 'json' => [['status' => 'queued']]],
        ]);
        [$queue] = $this->queueFor($handler, ['bulk_batch' => 2]);

        $queue->bulk([
            new PartitionedTestJob('customer-1'),
            new PartitionedTestJob('customer-2'),
            new PartitionedTestJob('customer-3'),
        ], queue: 'orders');

        $this->assertCount(2, $handler->requests);
        $this->assertCount(2, json_decode((string) $handler->requests[0]->getBody(), true)['items']);
        $this->assertCount(1, json_decode((string) $handler->requests[1]->getBody(), true)['items']);
    }

    public function testBulkFallbackPreservesDelayedJobs(): void
    {
        $handler = new PlanHandler([
            ['status' => 201, 'json' => [['status' => 'queued']]],
            ['status' => 200, 'json' => ['results' => [[
                'ok' => true,
                'status' => 'scheduled',
                'queue' => 'orders',
                'timerKey' => 'laravel:delay:delayed',
                'txn' => 'delayed',
            ]]]],
        ]);
        [$queue] = $this->queueFor($handler);

        $queue->bulk([
            new PartitionedTestJob('customer-1'),
            new DelayedPartitionedTestJob('customer-2', 10),
        ], queue: 'orders');

        $this->assertSame([
            '/api/v1/push',
            '/api/v1/timers',
        ], array_map(fn ($request) => $request->getUri()->getPath(), $handler->requests));
        $timer = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame(10_000, $timer['operations'][0]['delayMs']);
        $this->assertSame('customer-2', $timer['operations'][0]['partition']);
    }

    public function testMalformedPushResponseIsRejected(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => []]]);
        [$queue] = $this->queueFor($handler);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('malformed response');

        $queue->pushRaw(json_encode($this->payload('job-123')), 'emails');
    }

    public function testUnknownPushStatusIsRejected(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [['status' => 'maybe']]]]);
        [$queue] = $this->queueFor($handler);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('unexpected status maybe');

        $queue->pushRaw(json_encode($this->payload('job-123')), 'emails');
    }

    public function testPushResponseWithWrongCardinalityIsRejected(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [
            ['status' => 'queued'],
            ['status' => 'queued'],
        ]]]);
        [$queue] = $this->queueFor($handler);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('malformed response');

        $queue->pushRaw(json_encode($this->payload('job-123')), 'emails');
    }

    public function testFailedPushStatusIsRejected(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [[
            'status' => 'failed',
            'error' => 'partition unavailable',
        ]]]]);
        [$queue] = $this->queueFor($handler);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('partition unavailable');

        $queue->pushRaw(json_encode($this->payload('job-123')), 'emails');
    }

    public function testManualRetryCleansItsDlqSourceAndTreatsMissingSourceAsSuccess(): void
    {
        $handler = new PlanHandler([
            ['status' => 201, 'json' => [['status' => 'queued']]],
            ['status' => 404, 'json' => ['error' => 'Message not found']],
        ]);
        [$queue] = $this->queueFor($handler);
        $payload = $this->payload('job-123');
        $payload['_queen'] = [
            'attempts' => 4,
            'manual_retry' => 'retry-transaction-123',
            'failed_source' => [
                'partition_id' => 'partition/id',
                'transaction_id' => 'transaction id',
            ],
        ];

        $this->assertSame('job-123', $queue->pushRaw(json_encode($payload), 'emails'));
        $this->assertSame('/api/v1/messages/partition%2Fid/transaction%20id', $handler->requests[1]->getUri()->getPath());
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('failedCleanupRouteMismatches')]
    public function testManualRetryDoesNotHideADeleteRouteMismatch(array $response): void
    {
        $handler = new PlanHandler([
            ['status' => 201, 'json' => [['status' => 'queued']]],
            ['status' => 404, 'json' => $response],
        ]);
        [$queue] = $this->queueFor($handler);
        $payload = $this->payload('job-123');
        $payload['_queen'] = [
            'attempts' => 4,
            'manual_retry' => 'retry-transaction-123',
            'failed_source' => [
                'partition_id' => 'partition-1',
                'transaction_id' => 'transaction-1',
            ],
        ];

        try {
            $queue->pushRaw(json_encode($payload, JSON_THROW_ON_ERROR), 'emails');
            $this->fail('A failed DLQ delete route must abort the Laravel retry hand-off.');
        } catch (HttpException $exception) {
            $this->assertSame(404, $exception->statusCode);
            $this->assertSame($response['code'] ?? null, $exception->errorCode);
            $this->assertSame($response['error'], $exception->serverError);
        }

        $this->assertCount(2, $handler->requests);
    }

    public static function failedCleanupRouteMismatches(): array
    {
        return [
            'explicit no_such_route' => [[
                'error' => 'Not Found',
                'code' => 'no_such_route',
            ]],
            'generic router 404 without a code' => [[
                'error' => 'Not Found',
            ]],
            'route code wins over misleading error prose' => [[
                'error' => 'Message not found',
                'code' => 'no_such_route',
            ]],
        ];
    }

    public function testFailedJobRawBodyMarksThePayloadForManualRetry(): void
    {
        $payload = $this->payload('job-123');
        $payload['job'] = RetryableTestHandler::class . '@handle';
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => $this->popResponse($payload),
        ], [
            'status' => 200,
            'json' => [['success' => true, 'leaseReleased' => true]],
        ]]);
        [$queue] = $this->queueFor($handler);
        $container = new Container();
        $events = new \Illuminate\Events\Dispatcher($container);
        $container->instance(\Illuminate\Contracts\Events\Dispatcher::class, $events);
        $queue->setContainer($container);
        $failedBody = null;
        $events->listen(\Illuminate\Queue\Events\JobFailed::class, function ($event) use (&$failedBody): void {
            $failedBody = $event->job->getRawBody();
        });
        $job = $queue->pop('emails');
        // Exercise failure after Laravel has already decoded worker metadata;
        // Queen must invalidate that cache before adding retry provenance.
        $job->payload();
        $job->fail(new \RuntimeException('failed intentionally'));

        $payload = json_decode($failedBody, true, 512, JSON_THROW_ON_ERROR);

        $this->assertIsString($payload['_queen']['manual_retry']);
        $this->assertNotSame('', $payload['_queen']['manual_retry']);
        $this->assertSame('0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83', $payload['_queen']['failed_source']['partition_id']);
        $this->assertSame('transaction-1', $payload['_queen']['failed_source']['transaction_id']);
    }

    public function testPartitionableJobUsesItsEntityKey(): void
    {
        $handler = new PlanHandler([["status" => 201, "json" => [["status" => "queued"]]]]);
        [$queue] = $this->queueFor($handler);

        $queue->push(new PartitionedTestJob('customer-42'), queue: 'orders');

        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $this->assertSame('customer-42', $body['items'][0]['partition']);
        $this->assertSame('customer-42', $body['items'][0]['payload']['_queen']['partition']);
    }

    public function testPopReturnsALaravelJobAndDeclaresTheWorkerContract(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => $this->popResponse($this->payload('job-123'), deliveryAttempt: 3),
        ]]);
        [$queue] = $this->queueFor($handler);

        $job = $queue->pop('emails');

        $this->assertInstanceOf(QueenJob::class, $job);
        $this->assertSame('job-123', $job->getJobId());
        $this->assertSame(3, $job->attempts());
        $this->assertSame($this->payload('job-123'), json_decode($job->getRawBody(), true));
        $this->assertSame($this->payload('job-123'), $job->payload());

        $message = new \ReflectionProperty(QueenJob::class, 'message');
        $changed = $this->popResponse($this->payload('changed'))['messages'][0];
        $message->setValue($job, $changed);
        $this->assertSame($this->payload('job-123'), $job->payload());

        parse_str($handler->requests[0]->getUri()->getQuery(), $query);
        $this->assertSame('/api/v1/pop/queue/emails', $handler->requests[0]->getUri()->getPath());
        $this->assertSame('workers', $query['consumerGroup']);
        $this->assertSame('all', $query['subscriptionMode']);
        $this->assertSame('8', $query['partitions']);
        $this->assertSame('120', $query['leaseSeconds']);
        $this->assertSame('false', $query['wait']);
    }

    public function testPrefetchAndAckBatchUseOnePopAndOneFullLeaseAcknowledgement(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
            ['status' => 200, 'json' => [
                ['success' => true, 'leaseReleased' => true],
                ['success' => true, 'leaseReleased' => true],
                ['success' => true, 'leaseReleased' => true],
            ]],
        ]);
        $queue = $this->queueWithLeaseRenewer(
            $handler,
            new RecordingLeaseRenewer(),
            prefetch: 3,
            ackBatch: 3,
        );

        $first = $queue->pop('emails');
        $first->delete();
        $this->assertCount(1, $handler->requests, 'the first successful ACK remains bounded in memory');

        $second = $queue->pop('emails');
        $second->delete();
        $this->assertCount(1, $handler->requests, 'prefetched work avoids both an HTTP pop and a partial ACK');

        $third = $queue->pop('emails');
        $third->delete();

        $this->assertSame(['job-1', 'job-2', 'job-3'], [
            $first->getJobId(),
            $second->getJobId(),
            $third->getJobId(),
        ]);
        $this->assertCount(2, $handler->requests);
        parse_str($handler->requests[0]->getUri()->getQuery(), $query);
        $this->assertSame('3', $query['batch']);
        $ack = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame('/api/v1/ack/batch', $handler->requests[1]->getUri()->getPath());
        $this->assertSame(['transaction-1', 'transaction-2', 'transaction-3'],
            array_column($ack['acknowledgments'], 'transactionId'));
    }

    public function testLeaseRenewerTracksTheWholePrefetchedLeaseUntilEveryAckSucceeds(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 3);

        $queue->pop('emails')->delete();
        $this->assertSame(['lease-1'], $renewer->tracked);
        $this->assertSame([], $renewer->forgotten);

        $queue->pop('emails')->delete();
        $this->assertSame([], $renewer->forgotten);

        $queue->pop('emails')->delete();
        $this->assertSame(['lease-1'], $renewer->forgotten);
        $this->assertSame(['lease-1', 'lease-1', 'lease-1'], $renewer->healthChecks);
    }

    public function testWorkerStoppingHandsBackTheUnstartedTailWithoutChargingAnAttempt(): void
    {
        $payloads = [];
        foreach (['job-a1', 'job-a2', 'job-b1'] as $uuid) {
            $payload = $this->payload($uuid);
            // One earlier run, recorded when the job was released.
            $payload['_queen'] = ['partition' => 'job-0001', 'attempts' => 1];
            $payloads[] = $payload;
        }
        $payloads[2]['_queen']['partition'] = 'job-0002';
        $response = $this->popBatchResponse($payloads);
        $response['messages'][2]['partitionId'] = '0298f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83';
        $response['messages'][2]['partition'] = 'job-0002';
        foreach (array_keys($response['messages']) as $index) {
            // The broker delivers this batch for the second time.
            $response['messages'][$index]['deliveryAttempt'] = 2;
        }
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $response],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 3);
        $container = new Container();
        $events = new Dispatcher($container);
        $container->instance('events', $events);
        $queue->setContainer($container);

        $first = $queue->pop('emails');
        $attemptBeforeHandBack = $first->attempts();
        $this->assertSame(3, $attemptBeforeHandBack);
        $first->delete();
        $events->dispatch(new WorkerStopping());

        $this->assertCount(3, $handler->requests);
        $handBack = $this->handBack($handler->requests[2]);
        $this->assertSame(['transaction-2', 'transaction-3'], array_column($handBack['acks'], 'transactionId'));
        $this->assertSame(['completed', 'completed'], array_column($handBack['acks'], 'status'));
        $this->assertSame(['workers', 'workers'], array_column($handBack['acks'], 'consumerGroup'));
        $this->assertSame(['lease-1'], $handBack['body']['requiredLeases']);
        $this->assertSame(['job-a2', 'job-b1'], $this->copiedJobs($handBack));
        $this->assertSame(['emails', 'emails'], array_column($handBack['copies'], 'queue'));
        $this->assertSame(['job-0001', 'job-0002'], array_column($handBack['copies'], 'partition'));
        $this->assertNotContains('transaction-2', array_column($handBack['copies'], 'transactionId'));
        // The release recorded one run, the first delivery of this batch another.
        $this->assertSame([2, 2], $this->copiedAttempts($handBack));
        foreach ($handBack['copies'] as $copy) {
            $this->assertSame(
                $attemptBeforeHandBack,
                $this->redeliveredAttempts($copy),
                'a job that never ran keeps its attempt number',
            );
        }
        $this->assertSame(['lease-1'], $renewer->forgotten);
        $this->assertSame(1, $renewer->closed);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('after worker shutdown began');
        $queue->pop('emails');
    }

    public function testShutdownCompletesDeferredSuccessAndHandsBackTheTailInOneTransaction(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer(
            $handler,
            $renewer,
            prefetch: 3,
            ackBatch: 3,
        );

        $queue->pop('emails')->delete();
        $queue->shutdown();

        $this->assertCount(2, $handler->requests);
        $handBack = $this->handBack($handler->requests[1]);
        $this->assertSame(
            ['transaction-1', 'transaction-2', 'transaction-3'],
            array_column($handBack['acks'], 'transactionId'),
        );
        $this->assertSame(['completed', 'completed', 'completed'], array_column($handBack['acks'], 'status'));
        $this->assertSame(['job-2', 'job-3'], $this->copiedJobs($handBack), 'job-1 ran and succeeded');
        $this->assertSame([0, 0], $this->copiedAttempts($handBack));
        $this->assertSame(1, $this->redeliveredAttempts($handBack['copies'][0]));
        $this->assertSame(['lease-1'], $renewer->forgotten);
        $this->assertSame(1, $renewer->closed);
    }

    /**
     * A worker on a queue list keeps one queue's prefetched tail while it runs
     * a batch of the next, so its hand-back spans two leases. A Queen 2 broker
     * lends requiredLeases to ACKs only when it names one lease: each ACK has
     * to name its own, or it is applied whoever holds the partition by then.
     */
    public function testAHandBackSpanningTwoLeasesFencesEveryAckWithItsOwnLease(): void
    {
        $high = $this->popBatchResponse([$this->payload('job-h1'), $this->payload('job-h2')]);
        $high['leaseId'] = 'lease-2';
        foreach ($high['messages'] as $index => $message) {
            $high['messages'][$index] = array_replace($message, [
                'transactionId' => 'transaction-h' . ($index + 1),
                'partitionId' => '0298f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => 'job-0002',
                'leaseId' => 'lease-2',
            ]);
        }
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([$this->payload('job-l1'), $this->payload('job-l2')])],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
            ['status' => 200, 'json' => $high],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        $queue = $this->queueWithLeaseRenewer($handler, new RecordingLeaseRenewer(), prefetch: 2);

        $queue->pop('low')->delete();
        $this->assertSame('job-h1', $queue->pop('high')->getJobId());
        $queue->shutdown();

        $this->assertCount(4, $handler->requests);
        $handBack = $this->handBack($handler->requests[3]);
        $copied = $this->copiedJobs($handBack);
        sort($copied);
        $this->assertSame(['job-h1', 'job-h2', 'job-l2'], $copied);
        $leases = array_column($handBack['acks'], 'leaseId', 'transactionId');
        ksort($leases);
        $this->assertSame(
            ['transaction-2' => 'lease-1', 'transaction-h1' => 'lease-2', 'transaction-h2' => 'lease-2'],
            $leases,
            'each ACK names the lease of its own delivery',
        );
        $required = $handBack['body']['requiredLeases'];
        sort($required);
        $this->assertSame(['lease-1', 'lease-2'], $required);
    }

    public function testAJobStillRunningAtShutdownIsHandedBackWithItsRunCounted(): void
    {
        // Laravel's timeout handler dispatches WorkerStopping while the job
        // that timed out is still the current delivery.
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 3);

        $timedOut = $queue->pop('emails');
        $this->assertSame(1, $timedOut->attempts());
        $queue->shutdown();

        $this->assertCount(2, $handler->requests);
        $handBack = $this->handBack($handler->requests[1]);
        // The broker completes every position before the last one acknowledged:
        // completing job-2 and job-3 alone would complete job-1 with them.
        $this->assertSame(
            ['transaction-1', 'transaction-2', 'transaction-3'],
            array_column($handBack['acks'], 'transactionId'),
        );
        $this->assertSame(['job-1', 'job-2', 'job-3'], $this->copiedJobs($handBack));
        $this->assertSame([1, 0, 0], $this->copiedAttempts($handBack));
        $this->assertSame(2, $this->redeliveredAttempts($handBack['copies'][0]), 'the run that timed out counts');
        $this->assertSame(1, $this->redeliveredAttempts($handBack['copies'][1]));
        $this->assertSame(['lease-1'], $renewer->forgotten);
    }

    public function testAJobStillRunningAloneInItsPartitionIsLeftToLeaseExpiry(): void
    {
        $response = $this->popBatchResponse([$this->payload('job-a1'), $this->payload('job-b1')]);
        $response['messages'][1]['partitionId'] = '0298f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83';
        $response['messages'][1]['partition'] = 'job-0002';
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $response],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        $queue = $this->queueWithLeaseRenewer($handler, new RecordingLeaseRenewer(), prefetch: 2);

        $queue->pop('emails');
        $queue->shutdown();

        $handBack = $this->handBack($handler->requests[1]);
        $this->assertSame(['transaction-2'], array_column($handBack['acks'], 'transactionId'));
        $this->assertSame(['job-b1'], $this->copiedJobs($handBack));
    }

    /**
     * Laravel's timeout handler runs from SIGALRM, as soon as a blocking
     * request returns: the broker has settled the job, the queue has not seen
     * the answer yet. A copy of that job would run it twice.
     */
    #[\PHPUnit\Framework\Attributes\TestWith(['delete', '/api/v1/ack', false])]
    #[\PHPUnit\Framework\Attributes\TestWith(['delete', '/api/v1/ack', true])]
    #[\PHPUnit\Framework\Attributes\TestWith(['release', '/api/v1/transaction', false])]
    public function testATimeoutWhileTheJobIsSettledNeverCopiesIt(string $settle, string $path, bool $ackAsync): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
            $settle === 'delete'
                ? ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]]
                : ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'release-1']],
        ], ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']]);
        $queue = null;
        $timeout = new InterruptingHandler($handler, $path, static function () use (&$queue): void {
            $queue->shutdown();
        });
        $queue = new QueenQueue(
            new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($timeout)]),
            consumerGroup: 'workers',
            retryAfter: 120,
            prefetch: 3,
            leaseRenewer: new RecordingLeaseRenewer(),
            ackAsync: $ackAsync,
        );
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        $job = $queue->pop('emails');
        $settle === 'delete' ? $job->delete() : $job->release();

        $this->assertTrue($timeout->interrupted);
        $copied = [];
        foreach (array_slice($handler->requests, 2) as $request) {
            if ($request->getUri()->getPath() === '/api/v1/transaction') {
                array_push($copied, ...$this->copiedJobs($this->handBack($request)));
            }
        }
        $this->assertNotContains('job-1', $copied, 'job-1 was already settled by the broker');
    }

    public function testACrashLeavesTheTailToLeaseExpiryWhichChargesItOneAttempt(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([$this->payload('job-1'), $this->payload('job-2')])],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 2);

        $queue->pop('emails')->delete();
        // SIGKILL, an out-of-memory error or a lost node runs no PHP code: the
        // worker that crashed is an object whose consumer process is gone.
        (new \ReflectionProperty(QueenQueue::class, 'consumerPid'))->setValue($queue, -1);
        unset($queue);

        $this->assertCount(2, $handler->requests, 'nothing hands the tail back');
        $this->assertSame([], $renewer->forgotten);
        $this->assertSame(0, $renewer->closed);

        // The lease expires and the broker delivers job-2 again.
        [$restarted] = $this->queueFor(new PlanHandler([[
            'status' => 200,
            'json' => $this->popResponse($this->payload('job-2'), deliveryAttempt: 2),
        ]]));
        $this->assertSame(2, $restarted->pop('emails')->attempts(), 'a crash charges the tail one attempt');
    }

    /**
     * A job whose timeout its lease cannot cover is wrong in its code, and
     * only a deploy fixes it. Thrown from pop(), it ended the worker; the
     * lease expired without charging an attempt, the next worker popped the
     * same job and ended too, and the job never reached the dead-letter
     * queue. It fails as Laravel fails a job: dead-letter queue, JobFailed
     * (which queue:work's failed-job row listens to) and the job's failed().
     */
    #[TestWith([120, 'Queen Laravel job timeout [120] must be positive and shorter than retry_after [120] when lease_renewal is disabled.'])]
    #[TestWith([0, 'Queen Laravel job timeout [0] must be positive and shorter than retry_after [120]'])]
    #[TestWith(['600', 'Queen Laravel job timeout [600] must be positive and shorter than retry_after [120]'])]
    #[TestWith([-1, 'Queen Laravel job timeout must be a non-negative integer or null.'])]
    #[TestWith(['ten minutes', 'Queen Laravel job timeout must be a non-negative integer or null.'])]
    public function testAJobWhoseTimeoutItsLeaseCannotCoverFailsAndThePopGoesOn(int|string $timeout, string $says): void
    {
        $payload = $this->payload('job-too-long');
        $payload['job'] = FailureRecordingTestHandler::class . '@handle';
        $payload['timeout'] = $timeout;
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($payload)],
            ['status' => 200, 'json' => ['success' => true, 'leaseReleased' => true, 'dlq' => true]],
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-next'))],
        ]);
        [$queue] = $this->queueFor($handler);
        $container = new Container();
        $container->instance(ExceptionHandler::class, $reported = new RecordingExceptionHandler());
        $container->instance(\Illuminate\Contracts\Events\Dispatcher::class, $events = new Dispatcher($container));
        $failed = [];
        $events->listen(JobFailed::class, function (JobFailed $event) use (&$failed): void {
            $failed[] = $event;
        });
        $queue->setContainer($container);
        FailureRecordingTestHandler::$failures = [];

        $this->assertNull($queue->pop('emails'));

        $this->assertSame('/api/v1/ack', $handler->requests[1]->getUri()->getPath());
        $ack = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame('dlq', $ack['status']);
        $this->assertSame('transaction-1', $ack['transactionId']);
        $this->assertStringContainsString($says, $ack['error']);
        $this->assertCount(1, $failed);
        $this->assertInstanceOf(UnsafeJobTimeoutException::class, $failed[0]->exception);
        $this->assertSame([$failed[0]->exception], FailureRecordingTestHandler::$failures, 'the job\'s failed()');
        $this->assertSame([$failed[0]->exception], $reported->reported, 'the operator sees the setting to fix');
        $this->assertSame('job-next', $queue->pop('emails')?->getJobId());
    }

    /**
     * `$this->timeout = env('JOB_TIMEOUT')` gives a string. Laravel's worker
     * arms its alarm with it as with the integer, and so does the driver.
     */
    #[TestWith(['60'])]
    #[TestWith(['60.0'])]
    public function testANumericStringTimeoutIsTheTimeoutLaravelArms(string $timeout): void
    {
        $payload = $this->payload('job-from-env');
        $payload['timeout'] = $timeout;
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => $this->popResponse($payload),
        ]]);
        [$queue] = $this->queueFor($handler);

        $job = $queue->pop('emails');

        $this->assertInstanceOf(QueenJob::class, $job);
        $this->assertCount(1, $handler->requests, 'nothing was dead-lettered');
    }

    /**
     * A delivery that carries no Laravel job never will. Left to lease expiry,
     * it came back forever, since an expiry never charges the broker's retry
     * budget, and held its partition: it goes to the dead-letter queue at its
     * first delivery instead, and the report says which one it was.
     */
    #[TestWith(['not JSON {', 'its data is not JSON'])]
    #[TestWith(['"a JSON string"', 'its data is not a JSON object'])]
    #[TestWith([['poison' => 'not a Laravel job'], 'its payload names no job to call'])]
    #[TestWith([['job' => ''], 'its payload names no job to call'])]
    public function testADeliveryThatCarriesNoLaravelJobGoesToTheDeadLetterQueueAtOnce(string|array $data, string $reason): void
    {
        $response = $this->popResponse($this->payload('unused'), deliveryAttempt: 2);
        $response['messages'][0]['data'] = $data;
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $response],
            ['status' => 200, 'json' => ['success' => true, 'leaseReleased' => true, 'dlq' => true]],
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-next'))],
        ]);
        [$queue] = $this->queueFor($handler);
        $reported = new RecordingExceptionHandler();
        $container = new Container();
        $container->instance(ExceptionHandler::class, $reported);
        $queue->setContainer($container);

        $this->assertNull($queue->pop('emails'));

        $this->assertSame('/api/v1/ack', $handler->requests[1]->getUri()->getPath());
        $ack = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame('dlq', $ack['status']);
        $this->assertSame('transaction-1', $ack['transactionId']);
        $this->assertSame('lease-1', $ack['leaseId']);
        $this->assertCount(1, $reported->reported);
        $this->assertInstanceOf(NotALaravelJobException::class, $reported->reported[0]);
        $message = $reported->reported[0]->getMessage();
        $this->assertSame($message, $ack['error']);
        $this->assertStringContainsString('delivery [transaction-1] of queue [emails], partition [job-0001], delivery attempt 2', $message);
        $this->assertStringContainsString($reason, $message);
        $this->assertStringContainsString(
            'Its first bytes: ' . json_encode(is_string($data) ? $data : json_encode($data)),
            $message,
        );

        // The partition is free: the next delivery is a job again.
        $this->assertSame('job-next', $queue->pop('emails')->getJobId());
    }

    public function testTheReportQuotesOnlyTheFirstBytesOfALongDelivery(): void
    {
        $response = $this->popResponse($this->payload('unused'));
        $response['messages'][0]['data'] = str_repeat('x', 5000);
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $response],
            ['status' => 200, 'json' => ['success' => true, 'leaseReleased' => true, 'dlq' => true]],
        ]);
        [$queue] = $this->queueFor($handler);
        $reported = new RecordingExceptionHandler();
        $container = new Container();
        $container->instance(ExceptionHandler::class, $reported);
        $queue->setContainer($container);

        $this->assertNull($queue->pop('emails'));

        $message = $reported->reported[0]->getMessage();
        $this->assertStringContainsString('Its first bytes: "' . str_repeat('x', 120) . '" (5000 bytes in all)', $message);
        $this->assertLessThan(1024, strlen($message));
    }

    public function testRenewedLeaseAcceptsAJobSpecificTimeoutLongerThanItsInitialLease(): void
    {
        $payload = $this->payload('job-renewed');
        $payload['timeout'] = 180;
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($payload)],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer);

        $job = $queue->pop('emails');
        $this->assertSame(180, $job->timeout());
        $job->delete();
        $this->assertSame(['lease-1'], $renewer->forgotten);
    }

    public function testUnsafeRenewalDiscardsTheWholeLeaseBeforeAnotherPrefetchedJobRuns(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
            ['status' => 200, 'json' => ['success' => true, 'messages' => []]],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 3);

        $queue->pop('emails')->delete();
        $renewer->failure = 'deadline exhausted';
        try {
            $queue->pop('emails');
            $this->fail('A job under an unsafe lease was returned to Laravel.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('deadline exhausted', $exception->getMessage());
        }

        $renewer->failure = null;
        $this->assertNull($queue->pop('emails'), 'the remaining same-lease tail was discarded');
        $this->assertSame(['lease-1'], $renewer->forgotten);
        $this->assertCount(3, $handler->requests, 'a fresh broker pop follows the discarded local lease');
    }

    public function testLeaseDeadlineStartsBeforeASlowPopRequest(): void
    {
        $inner = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-1'))],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $handler = new DelayedPlanHandler($inner, 250_000);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer);

        $before = intdiv(hrtime(true), 1_000_000);
        $job = $queue->pop('emails');
        $after = intdiv(hrtime(true), 1_000_000);
        $deadline = $renewer->deadlines['lease-1'];

        $this->assertGreaterThanOrEqual(200, $after - $before);
        $this->assertGreaterThanOrEqual($before + 119_900, $deadline);
        $this->assertLessThanOrEqual($before + 120_100, $deadline);
        $this->assertLessThan($after + 119_900, $deadline,
            'the HTTP response time was incorrectly added to the lease deadline');
        $job->delete();
    }

    public function testJobIsNotDeliveredWhenLeaseTrackingIsNotConfirmed(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => $this->popResponse($this->payload('job-1')),
        ]]);
        $renewer = new RecordingLeaseRenewer();
        $renewer->trackFailure = 'child died before tracked ACK';
        $queue = $this->queueWithLeaseRenewer($handler, $renewer);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('before tracked ACK');
        $queue->pop('emails');
    }

    public function testAmbiguousAckStopsRenewingAndDiscardsEveryPartitionInTheLease(): void
    {
        $response = $this->popBatchResponse([
            $this->payload('job-a'),
            $this->payload('job-b'),
        ]);
        $response['messages'][1]['partitionId'] = '0298f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83';
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $response],
            ['status' => 200, 'json' => [['success' => false, 'error' => 'lease moved']]],
            ['status' => 200, 'json' => ['success' => true, 'messages' => []]],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 2);

        try {
            $queue->pop('emails')->delete();
            $this->fail('An ambiguous ACK was silently accepted.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('lease moved', $exception->getMessage());
        }

        $this->assertSame(['lease-1'], $renewer->forgotten);
        $this->assertNull($queue->pop('emails'));
        $this->assertCount(3, $handler->requests);
    }

    public function testBatchAckTransportFailureStopsRenewingAndIsNotRetriedLocally(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
            ])],
            ['status' => 503, 'json' => ['error' => 'database unavailable']],
            ['status' => 503, 'json' => ['error' => 'database unavailable']],
            ['status' => 503, 'json' => ['error' => 'database unavailable']],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 2, ackBatch: 2);

        $queue->pop('emails')->delete();
        try {
            $queue->pop('emails')->delete();
            $this->fail('An ambiguous batch ACK transport failure was silently accepted.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('database unavailable', $exception->getMessage());
        }

        $this->assertSame(['lease-1'], $renewer->forgotten);
        $requestCount = count($handler->requests);
        $queue->flushAcknowledgements();
        $this->assertCount($requestCount, $handler->requests, 'an ambiguous renewed lease must not be retried locally');
    }

    public function testPrefetchRejectsReentrantPopUntilCurrentDeliveryIsHandled(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
            ])],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $queue = $this->queueWithLeaseRenewer($handler, new RecordingLeaseRenewer(), prefetch: 2);

        $first = $queue->pop('emails');
        try {
            $queue->pop('emails');
            $this->fail('A reentrant prefetched pop was accepted.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('before the current job is deleted or released', $exception->getMessage());
        }
        $this->assertCount(1, $handler->requests);

        $first->delete();
        $second = $queue->pop('emails');
        $this->assertSame('job-2', $second->getJobId());
        $second->delete();
        $this->assertCount(3, $handler->requests);
    }

    public function testFailureDiscardsOnlyStalePrefetchedSiblingsFromItsPartition(): void
    {
        $response = $this->popBatchResponse([
            $this->payload('job-a1'),
            $this->payload('job-a2'),
            $this->payload('job-b1'),
        ]);
        $response['messages'][2]['partitionId'] = '0298f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83';
        $response['messages'][2]['partition'] = 'job-0002';

        $handler = new PlanHandler([
            ['status' => 200, 'json' => $response],
            ['status' => 200, 'json' => [['success' => true, 'dlq' => true]]],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $queue = $this->queueWithLeaseRenewer($handler, new RecordingLeaseRenewer(), prefetch: 3);

        $failed = $queue->pop('emails');
        $failed->markAsFailed();
        $failed->delete();

        $survivor = $queue->pop('emails');
        $this->assertSame('job-b1', $survivor->getJobId(), 'same-partition stale tail was not returned');
        $survivor->delete();
        $this->assertCount(3, $handler->requests, 'the safe sibling remained local; no second pop was needed');
    }

    public function testShortPrefetchResponseFlushesAtLeaseBoundaryBeforeThreshold(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
            ])],
            ['status' => 200, 'json' => [
                ['success' => true, 'leaseReleased' => true],
                ['success' => true, 'leaseReleased' => true],
            ]],
        ]);
        $queue = $this->queueWithLeaseRenewer(
            $handler,
            new RecordingLeaseRenewer(),
            prefetch: 8,
            ackBatch: 8,
        );

        $queue->pop('emails')->delete();
        $this->assertCount(1, $handler->requests);
        $queue->pop('emails')->delete();

        $this->assertCount(2, $handler->requests);
        $this->assertSame('/api/v1/ack/batch', $handler->requests[1]->getUri()->getPath());
    }

    public function testReleaseFlushesEarlierDeferredSuccessBeforeItsTransaction(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
            ])],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        $queue = $this->queueWithLeaseRenewer(
            $handler,
            new RecordingLeaseRenewer(),
            prefetch: 2,
            ackBatch: 2,
        );

        $queue->pop('emails')->delete();
        $queue->pop('emails')->release();

        $this->assertSame([
            '/api/v1/pop/queue/emails',
            '/api/v1/ack/batch',
            '/api/v1/transaction',
        ], array_map(fn ($request) => $request->getUri()->getPath(), $handler->requests));
    }

    public function testSuccessfulReleaseKeepsTheNextPrefetchedSiblingAvailable(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
            ])],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        $queue = $this->queueWithLeaseRenewer($handler, new RecordingLeaseRenewer(), prefetch: 2);

        $queue->pop('emails')->release();
        $sibling = $queue->pop('emails');

        $this->assertSame('job-2', $sibling->getJobId());
        $this->assertCount(2, $handler->requests, 'the valid sibling stayed in the local prefetch buffer');
        $sibling->delete();
    }

    public function testAmbiguousReleaseDiscardsItsTailAndDoesNotLeaveTheQueueReentrantlyLocked(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
            ])],
            ['status' => 503, 'json' => ['error' => 'database unavailable']],
            ['status' => 503, 'json' => ['error' => 'database unavailable']],
            ['status' => 503, 'json' => ['error' => 'database unavailable']],
            ['status' => 200, 'json' => ['success' => true, 'messages' => []]],
        ]);
        $queue = $this->queueWithLeaseRenewer($handler, new RecordingLeaseRenewer(), prefetch: 2);

        try {
            $queue->pop('emails')->release();
            $this->fail('An ambiguous release was silently accepted.');
        } catch (HttpException $exception) {
            $this->assertSame(503, $exception->statusCode);
        }

        $this->assertNull($queue->pop('emails'));
        $this->assertSame('/api/v1/pop/queue/emails', $handler->requests[array_key_last($handler->requests)]->getUri()->getPath());
    }

    public function testPartialBatchAckFailureIsVisibleAndNotRetriedUnderRenewal(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
            ])],
            ['status' => 200, 'json' => [
                ['success' => true, 'leaseReleased' => true],
                ['success' => false, 'error' => 'lease expired'],
            ]],
        ]);
        $queue = $this->queueWithLeaseRenewer(
            $handler,
            new RecordingLeaseRenewer(),
            prefetch: 2,
            ackBatch: 2,
        );

        $queue->pop('emails')->delete();
        try {
            $queue->pop('emails')->delete();
            $this->fail('A partial batch ACK failure was silently accepted.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('lease expired', $exception->getMessage());
        }

        $queue->flushAcknowledgements();
        $this->assertCount(2, $handler->requests, 'an ambiguous renewed lease must expire before redelivery');
    }

    public function testPopRejectsAConsumerGroupWithPersistedConflation(): void
    {
        $response = $this->popResponse($this->payload('job-123'));
        $response['conflation'] = true;
        $handler = new PlanHandler([['status' => 200, 'json' => $response]]);
        [$queue] = $this->queueFor($handler);

        $this->expectException(ConflationPolicyMismatchException::class);
        $this->expectExceptionMessage('requires conflation=false');

        $queue->pop('emails');
    }

    public function testPopPercentEncodesQueuePathSegments(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true, 'messages' => []]]]);
        [$queue] = $this->queueFor($handler);

        $queue->pop('tenant/jobs 100%');

        $this->assertSame('/api/v1/pop/queue/tenant%2Fjobs%20100%25', $handler->requests[0]->getUri()->getPath());
    }

    public function testDeletingACompletedJobAcknowledgesItsLease(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-123'))],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        [$queue] = $this->queueFor($handler);

        $queue->pop('emails')->delete();

        $request = $handler->requests[1];
        $body = json_decode((string) $request->getBody(), true);
        $this->assertSame('/api/v1/ack', $request->getUri()->getPath());
        $this->assertSame('completed', $body['status']);
        $this->assertSame('workers', $body['consumerGroup']);
        $this->assertSame('lease-1', $body['leaseId']);
    }

    public function testPopAndAckUseTheSameBackendAffinity(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-123'))],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => true]]],
        ]);
        [$queue] = $this->queueFor($handler, [
            'urls' => ['http://queen-a.test:6632', 'http://queen-b.test:6632', 'http://queen-c.test:6632'],
            'load_balancing_strategy' => 'affinity',
        ]);

        $queue->pop('emails')->delete();

        $this->assertSame(
            $handler->requests[0]->getUri()->getHost(),
            $handler->requests[1]->getUri()->getHost(),
            'the ACK should hit the broker whose process-local registry saw the pop',
        );
    }

    public function testRejectedCompletionIsNotSilentlyAccepted(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-123'))],
            ['status' => 200, 'json' => [['success' => false, 'error' => 'lease expired']]],
        ]);
        [$queue] = $this->queueFor($handler);

        $job = $queue->pop('emails');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('lease expired');
        $job->delete();
    }

    public function testMalformedCompletionIsNotSilentlyAccepted(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-123'))],
            ['status' => 200, 'json' => []],
        ]);
        [$queue] = $this->queueFor($handler);

        $job = $queue->pop('emails');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('malformed acknowledgement');
        $job->delete();
    }

    public function testReleasingAJobAtomicallyAcknowledgesAndSchedulesTheNextAttempt(): void
    {
        $payload = $this->payload('job-123');
        $payload['_queen'] = ['partition' => 'job-0001', 'attempts' => 2];
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($payload, deliveryAttempt: 2)],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        [$queue] = $this->queueFor($handler);

        $job = $queue->pop('emails');
        $this->assertSame(4, $job->attempts());
        $job->release(5);

        $request = $handler->requests[1];
        $body = json_decode((string) $request->getBody(), true);
        $this->assertSame('/api/v1/transaction', $request->getUri()->getPath());
        $this->assertSame('completed', $body['operations'][0]['status']);
        $this->assertSame('workers', $body['operations'][0]['consumerGroup']);
        $this->assertSame(['lease-1'], $body['requiredLeases']);
        $this->assertSame(5000, $body['timers'][0]['delayMs']);
        $this->assertSame('job-0001', $body['timers'][0]['partition']);

        $releasedPayload = json_decode(base64_decode($body['timers'][0]['payload'], true), true);
        $this->assertSame(4, $releasedPayload['_queen']['attempts']);
        $this->assertSame('job-123', $releasedPayload['uuid']);
    }

    public function testImmediateReleaseAtomicallyAcknowledgesAndPushesTheNextAttempt(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-123'))],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        [$queue] = $this->queueFor($handler);

        $queue->pop('emails')->release();

        $body = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame('/api/v1/transaction', $handler->requests[1]->getUri()->getPath());
        $this->assertSame('ack', $body['operations'][0]['type']);
        $this->assertSame('push', $body['operations'][1]['type']);
        $this->assertSame('emails', $body['operations'][1]['items'][0]['queue']);
        $this->assertSame('job-0001', $body['operations'][1]['items'][0]['partition']);
        $this->assertSame(1, $body['operations'][1]['items'][0]['payload']['_queen']['attempts']);
        $this->assertArrayNotHasKey('timers', $body);
    }

    public function testReleaseAcceptsDateIntervalAndDateTimeDelays(): void
    {
        foreach ([new DateInterval('PT30S'), new DateTimeImmutable('+30 seconds')] as $delay) {
            $handler = new PlanHandler([
                ['status' => 200, 'json' => $this->popResponse($this->payload('job-123'))],
                ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
            ]);
            [$queue] = $this->queueFor($handler);

            $queue->pop('emails')->release($delay);

            $body = json_decode((string) $handler->requests[1]->getBody(), true);
            $this->assertGreaterThanOrEqual(29_000, $body['timers'][0]['delayMs']);
            $this->assertLessThanOrEqual(30_000, $body['timers'][0]['delayMs']);
        }
    }

    public function testDeletingAMarkedFailedJobForcesItToTheDlq(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-123'))],
            ['status' => 200, 'json' => [['success' => true, 'dlq' => true]]],
        ]);
        [$queue] = $this->queueFor($handler);

        $job = $queue->pop('emails');
        $job->markAsFailed();
        $job->delete();

        $body = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame('dlq', $body['status']);
    }

    public function testLaterUsesAQueenTimer(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => ['results' => [[
                'ok' => true,
                'status' => 'scheduled',
                'queue' => 'emails',
                'timerKey' => 'laravel:delay:job-123',
                'txn' => 'job-123',
            ]]],
        ]]);
        [$queue] = $this->queueFor($handler);

        $jobId = $queue->later(10, 'Handler@handle', [], 'emails');

        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $this->assertMatchesRegularExpression('/^[0-9a-f-]{36}$/', $jobId);
        $this->assertSame('/api/v1/timers', $handler->requests[0]->getUri()->getPath());
        $this->assertSame('emails', $body['operations'][0]['queue']);
        $this->assertSame(10000, $body['operations'][0]['delayMs']);
        $this->assertSame($jobId, $body['operations'][0]['txn']);
    }

    public function testDelayedSizeUsesTheBrokerPrefixCount(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => ['count' => 2],
        ]]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(2, $queue->delayedSize('emails'));
        $this->assertSame('/api/v1/timers/emails', $handler->requests[0]->getUri()->getPath());
        $this->assertSame('mode=count&prefix=laravel%3A', $handler->requests[0]->getUri()->getQuery());
    }

    public function testDelayedSizeAcceptsTheExactLegacyListDuringARollingDeploy(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => [
                'rows' => [
                    ['timerKey' => 'application:reminder:1'],
                    ['timerKey' => 'laravel:delay:job-1'],
                ],
                'truncated' => false,
                'nextAfter' => null,
            ],
        ]]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(1, $queue->delayedSize('emails'));
        $this->assertCount(1, $handler->requests, 'the legacy response is reused as the first page');
    }

    public function testDelayedSizePagesOnlyAfterExplicitNoSuchRoute(): void
    {
        $handler = new PlanHandler([
            ['status' => 404, 'json' => ['error' => 'Not Found', 'code' => 'no_such_route']],
            ['status' => 200, 'json' => [
                'rows' => [
                    ['timerKey' => 'application:reminder:1'],
                    ['timerKey' => 'laravel:delay:job-1'],
                    ['timerKey' => 'laravel:release:job-2:attempt-2'],
                ],
                'truncated' => false,
                'nextAfter' => null,
            ]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(2, $queue->delayedSize('emails'));
        $this->assertCount(2, $handler->requests);
        $this->assertSame('mode=count&prefix=laravel%3A', $handler->requests[0]->getUri()->getQuery());
        $this->assertSame('limit=1000', $handler->requests[1]->getUri()->getQuery());
    }

    public function testDelayedSizePagesOnlyAfterExplicitUnsupported(): void
    {
        $handler = new PlanHandler([
            ['status' => 400, 'json' => ['error' => 'unsupported']],
            ['status' => 200, 'json' => [
                'rows' => [],
                'truncated' => false,
                'nextAfter' => null,
            ]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(0, $queue->delayedSize('emails'));
        $this->assertCount(2, $handler->requests);
    }

    public function testDelayedSizeDoesNotHideMalformedOrTransientCountFailures(): void
    {
        foreach ([
            [['status' => 200, 'json' => ['rows' => [], 'truncated' => false]]],
            [
                ['status' => 503, 'json' => ['error' => 'unsupported']],
                ['status' => 503, 'json' => ['error' => 'unsupported']],
                ['status' => 503, 'json' => ['error' => 'unsupported']],
            ],
        ] as $plan) {
            $handler = new PlanHandler($plan);
            [$queue] = $this->queueFor($handler);

            try {
                $queue->delayedSize('emails');
                $this->fail('Malformed and transient count failures must remain visible.');
            } catch (\UnexpectedValueException|HttpException) {
                foreach ($handler->requests as $request) {
                    $this->assertSame('mode=count&prefix=laravel%3A', $request->getUri()->getQuery());
                }
            }
        }
    }

    public function testSizeCountsDelayedJobsBeforeTheDestinationQueueExists(): void
    {
        $handler = new PlanHandler([
            ['status' => 404, 'json' => ['error' => 'Queue not found']],
            ['status' => 200, 'json' => ['count' => 1]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(1, $queue->size('emails'));
    }

    public function testSizeCountsTotalPendingIncludingLeasesPlusDelayedJobs(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => [
                'pending' => 10,
                'processing' => 4,
                'ready' => 6,
                'effectivePending' => 2,
            ]],
            ['status' => 200, 'json' => ['count' => 2]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(12, $queue->size('emails'));
        $this->assertSame('/api/v1/resources/queues/emails/depth', $handler->requests[0]->getUri()->getPath());
        $this->assertSame('/api/v1/timers/emails', $handler->requests[1]->getUri()->getPath());
    }

    public function testPendingSizeUsesGroupScopedReadyDepth(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [
            'pending' => 12,
            'processing' => 5,
            'ready' => 7,
            'effectivePending' => 12,
        ]]]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(7, $queue->pendingSize('emails'));
        parse_str($handler->requests[0]->getUri()->getQuery(), $query);
        $this->assertSame('workers', $query['group']);
    }

    public function testPendingSizeRetainsTheRollingUpgradeFallbackOrder(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['pending' => 12, 'effectivePending' => 9]],
            ['status' => 200, 'json' => ['pending' => 8]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(9, $queue->pendingSize('emails'));
        $this->assertSame(8, $queue->pendingSize('emails'));
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('malformedDepthCounters')]
    public function testDepthMetricsRejectMalformedCounters(mixed $value): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['pending' => $value]]]);
        [$queue] = $this->queueFor($handler);

        $this->expectException(\UnexpectedValueException::class);
        $queue->pendingSize('emails');
    }

    public static function malformedDepthCounters(): array
    {
        return [
            'numeric string' => ['12'],
            'fraction' => [1.5],
            'negative' => [-1],
            'boolean' => [true],
        ];
    }

    public function testReservedSizeUsesGroupScopedProcessingWithoutQueueDetail(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [
            'pending' => 12,
            'processing' => 5,
            'ready' => 7,
        ]]]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(5, $queue->reservedSize('emails'));
        $this->assertCount(1, $handler->requests);
        $this->assertSame('/api/v1/resources/queues/emails/depth', $handler->requests[0]->getUri()->getPath());
    }

    public function testReservedSizeFallsBackToQueueDetailForAnOlderBroker(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['pending' => 12]],
            ['status' => 200, 'json' => [
                'totals' => ['messages' => ['processing' => 4]],
            ]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(4, $queue->reservedSize('emails'));
        $this->assertCount(2, $handler->requests);
        $this->assertSame('/api/v1/status/queues/emails', $handler->requests[1]->getUri()->getPath());
    }

    public function testOldestPendingSkipsQueueDetailWhenNoJobIsReady(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [
            'pending' => 5,
            'processing' => 5,
            'ready' => 0,
            'partitions' => [['partition' => 'busy', 'pending' => 5, 'processing' => 5, 'ready' => 0]],
        ]]]);
        [$queue] = $this->queueFor($handler);

        $this->assertNull($queue->creationTimeOfOldestPendingJob('emails'));
        $this->assertCount(1, $handler->requests);
    }

    public function testOldestPendingUsesOnlyPartitionsWithGroupScopedReadyWork(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => [
                'pending' => 7,
                'processing' => 4,
                'ready' => 3,
                'partitions' => [
                    ['partition' => 'leased', 'pending' => 4, 'processing' => 4, 'ready' => 0],
                    ['partition' => 'claimable', 'pending' => 3, 'processing' => 0, 'ready' => 3],
                ],
            ]],
            ['status' => 200, 'json' => ['partitions' => [
                [
                    'name' => 'leased',
                    'messages' => ['pending' => 4],
                    'oldestMessage' => '2024-01-01T00:00:00.000Z',
                ],
                [
                    'name' => 'claimable',
                    'messages' => ['pending' => 3],
                    'oldestMessage' => '2024-02-03T04:05:06.000Z',
                ],
            ]]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(
            strtotime('2024-02-03T04:05:06.000Z'),
            $queue->creationTimeOfOldestPendingJob('emails'),
        );
        $this->assertCount(2, $handler->requests);
    }

    public function testOldestPendingRetainsQueueDetailFallbackForAnOlderBroker(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['pending' => 2]],
            ['status' => 200, 'json' => ['partitions' => [
                [
                    'name' => 'empty',
                    'messages' => ['pending' => 0],
                    'oldestMessage' => '2024-01-01T00:00:00.000Z',
                ],
                [
                    'name' => 'legacy-ready',
                    'messages' => ['pending' => 2],
                    'oldestMessage' => '2024-03-04T05:06:07.000Z',
                ],
            ]]],
        ]);
        [$queue] = $this->queueFor($handler);

        $this->assertSame(
            strtotime('2024-03-04T05:06:07.000Z'),
            $queue->creationTimeOfOldestPendingJob('emails'),
        );
    }

    private function queueFor(PlanHandler $handler, array $overrides = []): array
    {
        $connector = new QueenConnector();
        $queue = $connector->connect(array_replace([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'queue' => 'default',
            'consumer_group' => 'workers',
            'partitions' => 8,
            'partition_prefix' => 'job',
            'retry_after' => 120,
            'block_for' => 0,
        ], $overrides));
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        return [$queue, $handler];
    }

    public function testACrashHandsBackFromTheJournalWhatShutdownWould(): void
    {
        $response = $this->popBatchResponse([
            $this->payload('job-a1'),
            $this->payload('job-a2'),
            $this->payload('job-b1'),
            $this->payload('job-c1'),
        ]);
        // job-c1 shares no partition lease with the running job-a1.
        $response['messages'][3]['partitionId'] = '0298f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83';
        $response['messages'][3]['partition'] = 'job-0002';
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $response],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'bundle-1']],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $renewer->journal = new HandBackJournal($prefix = $this->journalPrefix());
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 4);

        $queue->pop('emails');
        $this->assertSame('ruuu', $this->journaledCodes($prefix));
        $journaled = $this->journaledHandBack($prefix);
        $queue->shutdown();

        $shutdown = $this->handBack($handler->requests[1]);
        $this->assertSame(['lease-1'], $journaled['body']['requiredLeases']);
        $this->assertSame($shutdown['acks'], $journaled['acks'], 'every ACK is fenced by its lease, at shutdown as after a crash');
        $withoutId = static fn (array $copy): array => array_diff_key($copy, ['transactionId' => true]);
        $this->assertSame(array_map($withoutId, $shutdown['copies']), array_map($withoutId, $journaled['copies']));
        $this->assertSame(['job-a1', 'job-a2', 'job-b1', 'job-c1'], $this->copiedJobs($journaled));
        $this->assertSame([1, 0, 0, 0], $this->copiedAttempts($journaled), 'the running job counts its run');
        $this->assertSame('----', $this->journaledCodes($prefix), 'shutdown withdrew the journal first');
    }

    public function testTheJournalNeverOwesAJobWhoseAcknowledgementIsOnTheWire(): void
    {
        $prefix = $this->journalPrefix();
        $plan = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
            ['status' => 200, 'json' => [['success' => true, 'leaseReleased' => false]]],
            ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'release-1']],
        ]);
        $owedDuringRequests = [];
        $handler = function ($request, array $options) use ($plan, $prefix, &$owedDuringRequests) {
            $owedDuringRequests[] = is_file("{$prefix}.state") ? $this->journaledCodes($prefix) : null;

            return $plan($request, $options);
        };
        $renewer = new RecordingLeaseRenewer();
        $renewer->journal = new HandBackJournal($prefix);
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 3);

        $first = $queue->pop('emails');
        $this->assertSame('ruu', $this->journaledCodes($prefix));
        $first->delete();
        $this->assertSame('-uu', $this->journaledCodes($prefix));
        $second = $queue->pop('emails');
        $this->assertSame('-ru', $this->journaledCodes($prefix));
        $second->release();
        $this->assertSame('--u', $this->journaledCodes($prefix));

        // Nothing in the partition lease of an ACK or release on the wire.
        $this->assertSame([null, '---', '---'], $owedDuringRequests);
    }

    public function testADeferredCompletionIsJournaledAsCompletedNotRunAgain(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popBatchResponse([
                $this->payload('job-1'),
                $this->payload('job-2'),
                $this->payload('job-3'),
            ])],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $renewer->journal = new HandBackJournal($prefix = $this->journalPrefix());
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 3, ackBatch: 3);

        $queue->pop('emails')->delete();
        $this->assertSame('cuu', $this->journaledCodes($prefix));
        $queue->pop('emails');
        $this->assertSame('cru', $this->journaledCodes($prefix));

        $journaled = $this->journaledHandBack($prefix);
        $this->assertSame(
            ['transaction-1', 'transaction-2', 'transaction-3'],
            array_column($journaled['acks'], 'transactionId'),
        );
        $this->assertSame(['job-2', 'job-3'], $this->copiedJobs($journaled));
    }

    public function testABatchOfOneJobJournalsNothing(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->popResponse($this->payload('job-1'))],
        ]);
        $renewer = new RecordingLeaseRenewer();
        $renewer->journal = new HandBackJournal($prefix = $this->journalPrefix());
        // A short batch on a prefetching worker: the queue held one job.
        $queue = $this->queueWithLeaseRenewer($handler, $renewer, prefetch: 4);

        $queue->pop('emails');

        $this->assertFileDoesNotExist("{$prefix}.plan");
        $this->assertFileDoesNotExist("{$prefix}.state");
    }

    private function journalPrefix(): string
    {
        $directory = sys_get_temp_dir() . '/qhb-' . bin2hex(random_bytes(4));
        mkdir($directory, 0700);
        $this->journalDirectories[] = $directory;

        return "{$directory}/hand-back-1";
    }

    private function journaledCodes(string $prefix): string
    {
        $state = json_decode((string) file_get_contents("{$prefix}.state"), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('lease-1', $state['lease_id']);

        return $state['entries'];
    }

    /**
     * The transaction the supervisor's lease service builds from the journal
     * (lease/hand_back.rs), split as handBack() splits a request.
     *
     * @return array{body: array, acks: list<array>, copies: list<array>}
     */
    private function journaledHandBack(string $prefix): array
    {
        $plan = json_decode((string) file_get_contents("{$prefix}.plan"), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('lease-1', $plan['lease_id']);
        $operations = [];
        foreach (str_split($this->journaledCodes($prefix)) as $index => $code) {
            $entry = $plan['entries'][$index];
            match ($code) {
                '-' => null,
                'c' => $operations[] = $entry['ack'],
                'u' => array_push($operations, $entry['ack'], $entry['unstarted']),
                'r' => array_push($operations, $entry['ack'], $entry['ran']),
            };
        }
        $body = ['operations' => $operations, 'requiredLeases' => [$plan['lease_id']]];

        return $this->handBack(new \GuzzleHttp\Psr7\Request(
            'POST',
            'http://queen.test:6632/api/v1/transaction',
            [],
            json_encode($body, JSON_THROW_ON_ERROR),
        ));
    }

    private function queueWithLeaseRenewer(
        callable $handler,
        LeaseRenewer $renewer,
        int $prefetch = 1,
        int $ackBatch = 1,
    ): QueenQueue {
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);
        $queue = new QueenQueue(
            $queen,
            defaultQueue: 'default',
            consumerGroup: 'workers',
            partitionCount: 8,
            partitionPrefix: 'job',
            retryAfter: 120,
            prefetch: $prefetch,
            ackBatch: $ackBatch,
            leaseRenewer: $renewer,
        );
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        return $queue;
    }

    /** @return array{body: array, acks: list<array>, copies: list<array>} */
    private function handBack(\Psr\Http\Message\RequestInterface $request): array
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

    /** @return list<string> */
    private function copiedJobs(array $handBack): array
    {
        return array_map(static fn (array $copy): string => $copy['payload']['uuid'], $handBack['copies']);
    }

    /** @return list<int> */
    private function copiedAttempts(array $handBack): array
    {
        return array_map(static fn (array $copy): int => $copy['payload']['_queen']['attempts'], $handBack['copies']);
    }

    /** The attempt Laravel sees when the broker delivers a copy for the first time. */
    private function redeliveredAttempts(array $copy): int
    {
        [$queue] = $this->queueFor(new PlanHandler([[
            'status' => 200,
            'json' => $this->popResponse($copy['payload']),
        ]]));

        return $queue->pop($copy['queue'])->attempts();
    }

    private function payload(string $uuid): array
    {
        return [
            'uuid' => $uuid,
            'displayName' => 'Handler',
            'job' => 'Handler@handle',
            'maxTries' => null,
            'maxExceptions' => null,
            'failOnTimeout' => false,
            'backoff' => null,
            'timeout' => null,
            'data' => [],
            'createdAt' => 1_700_000_000,
        ];
    }

    private function popResponse(array $payload, int $deliveryAttempt = 1): array
    {
        return [
            'success' => true,
            'queue' => 'emails',
            'leaseId' => 'lease-1',
            'consumerGroup' => 'workers',
            'messages' => [[
                'id' => 'message-1',
                'transactionId' => 'transaction-1',
                'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => $payload['_queen']['partition'] ?? 'job-0001',
                'leaseId' => 'lease-1',
                'consumerGroup' => 'workers',
                'deliveryAttempt' => $deliveryAttempt,
                'data' => $payload,
            ]],
        ];
    }

    /** @param list<array> $payloads */
    private function popBatchResponse(array $payloads): array
    {
        $response = $this->popResponse($payloads[0]);
        $response['messages'] = [];
        foreach ($payloads as $index => $payload) {
            $number = $index + 1;
            $response['messages'][] = [
                'id' => "message-{$number}",
                'transactionId' => "transaction-{$number}",
                'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => 'job-0001',
                'leaseId' => 'lease-1',
                'consumerGroup' => 'workers',
                'deliveryAttempt' => 1,
                'data' => $payload,
            ];
        }

        return $response;
    }
}

class PartitionedTestJob implements QueenPartitionable
{
    public function __construct(private string $partition)
    {
    }

    public function queenPartition(): string
    {
        return $this->partition;
    }
}

class RecordingLeaseRenewer implements LeaseRenewer
{
    /** @var list<string> */
    public array $tracked = [];

    /** @var list<string> */
    public array $forgotten = [];

    /** @var list<string> */
    public array $healthChecks = [];

    /** @var array<string, int> */
    public array $deadlines = [];

    public ?string $failure = null;

    public ?string $trackFailure = null;

    public int $closed = 0;

    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
        $this->tracked[] = $leaseId;
        $this->deadlines[$leaseId] = $deadlineMonotonicMillis;
        if ($this->trackFailure !== null) {
            throw new \RuntimeException($this->trackFailure);
        }
    }

    public function forget(string $leaseId): void
    {
        $this->forgotten[] = $leaseId;
    }

    public function assertHealthy(string $leaseId): void
    {
        $this->healthChecks[] = $leaseId;
        if ($this->failure !== null) {
            throw new \RuntimeException($this->failure);
        }
    }

    public function close(): void
    {
        $this->closed++;
    }

    public ?\Queen\Laravel\Queue\HandBackJournal $journal = null;

    public function handBackJournal(): ?\Queen\Laravel\Queue\HandBackJournal
    {
        return $this->journal;
    }
}

/**
 * Runs $interrupt once, when the first request to $path is answered, before
 * the caller reads the answer: a signal handler running as a blocking cURL
 * call returns.
 */
class InterruptingHandler
{
    public bool $interrupted = false;

    public function __construct(private PlanHandler $inner, private string $path, private \Closure $interrupt)
    {
    }

    public function __invoke($request, array $options): mixed
    {
        $response = ($this->inner)($request, $options);
        if (!$this->interrupted && $request->getUri()->getPath() === $this->path) {
            $this->interrupted = true;
            ($this->interrupt)();
        }

        return $response;
    }
}

class DelayedPlanHandler
{
    public function __construct(private PlanHandler $inner, private int $delayMicros)
    {
    }

    public function __invoke($request, array $options): mixed
    {
        usleep($this->delayMicros);
        return ($this->inner)($request, $options);
    }
}

class FailureRecordingTestHandler
{
    /** @var list<\Throwable> */
    public static array $failures = [];

    public function handle(): void
    {
    }

    public function failed(array $data, \Throwable $exception, string $uuid, mixed $job): void
    {
        self::$failures[] = $exception;
    }
}

class RetryableTestHandler
{
    public function handle(): void
    {
    }
}

class DelayedPartitionedTestJob implements QueenPartitionable
{
    public function __construct(private string $partition, public int $delay)
    {
    }

    public function queenPartition(): string
    {
        return $this->partition;
    }
}

final class QueenTaggedTestJob implements \Illuminate\Contracts\Queue\ShouldQueue
{
    public function handle(): void
    {
    }

    public function tags(): array
    {
        return ['billing', 'customer:7'];
    }
}
