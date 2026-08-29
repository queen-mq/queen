<?php

namespace Queen\Tests;

use DateInterval;
use DateTimeImmutable;
use GuzzleHttp\HandlerStack;
use Illuminate\Container\Container;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\ConflationPolicyMismatchException;
use Queen\Exceptions\HttpException;
use Queen\Laravel\Contracts\QueenPartitionable;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\QueenJob;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Tests\Support\PlanHandler;

class LaravelQueueDriverTest extends TestCase
{
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
        [$queue] = $this->queueFor($handler, ['prefetch' => 3, 'ack_batch' => 3]);

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
        [$queue] = $this->queueFor($handler, ['prefetch' => 2, 'ack_batch' => 1]);

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
        [$queue] = $this->queueFor($handler, ['prefetch' => 3, 'ack_batch' => 1]);

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
        [$queue] = $this->queueFor($handler, ['prefetch' => 8, 'ack_batch' => 8]);

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
        [$queue] = $this->queueFor($handler, ['prefetch' => 2, 'ack_batch' => 2]);

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
        [$queue] = $this->queueFor($handler, ['prefetch' => 2, 'ack_batch' => 1]);

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
        [$queue] = $this->queueFor($handler, ['prefetch' => 2, 'ack_batch' => 1]);

        try {
            $queue->pop('emails')->release();
            $this->fail('An ambiguous release was silently accepted.');
        } catch (HttpException $exception) {
            $this->assertSame(503, $exception->statusCode);
        }

        $this->assertNull($queue->pop('emails'));
        $this->assertSame('/api/v1/pop/queue/emails', $handler->requests[array_key_last($handler->requests)]->getUri()->getPath());
    }

    public function testBatchAckFailureRemainsRetryableAndVisible(): void
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
            ['status' => 200, 'json' => [
                ['success' => true, 'noop' => true],
                ['success' => true, 'leaseReleased' => true],
            ]],
        ]);
        [$queue] = $this->queueFor($handler, ['prefetch' => 2, 'ack_batch' => 2]);

        $queue->pop('emails')->delete();
        try {
            $queue->pop('emails')->delete();
            $this->fail('A partial batch ACK failure was silently accepted.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('lease expired', $exception->getMessage());
        }

        $queue->flushAcknowledgements();
        $this->assertCount(3, $handler->requests, 'the complete idempotent batch is retried');
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
