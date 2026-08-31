<?php

namespace Queen\Tests\Support;

use GuzzleHttp\HandlerStack;
use Illuminate\Queue\CallQueuedHandler;
use Queen\Laravel\Queue\QueenJob;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Queen;

trait BuildsQueenJobs
{
    /** @return array{QueenJob, PlanHandler} */
    private function queenJob(
        OverlappingQueenJob $command,
        string $jobId,
        array $completionResponse,
    ): array {
        $payload = $this->queenPayload($command, $jobId);
        $handler = new PlanHandler([
            ['status' => 200, 'json' => $this->queenPopResponse($payload, $jobId)],
            $completionResponse,
        ]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);
        $queue = new QueenQueue(
            $queen,
            defaultQueue: 'emails',
            consumerGroup: 'workers',
            partitionCount: 1,
            partitionPrefix: 'job',
            retryAfter: 30,
        );
        $queue->setContainer($this->app);
        $queue->setConnectionName('queen');

        $job = $queue->pop('emails');
        $this->assertInstanceOf(QueenJob::class, $job);

        return [$job, $handler];
    }

    private function queenPayload(OverlappingQueenJob $command, string $jobId): array
    {
        return [
            'uuid' => $jobId,
            'displayName' => $command::class,
            'job' => CallQueuedHandler::class . '@call',
            'maxTries' => null,
            'maxExceptions' => null,
            'failOnTimeout' => false,
            'backoff' => null,
            'timeout' => null,
            'retryUntil' => null,
            'data' => [
                'commandName' => $command::class,
                'command' => serialize($command),
            ],
            'createdAt' => 1_787_782_400,
            '_queen' => [
                'partition' => $jobId,
                'attempts' => 0,
            ],
        ];
    }

    private function queenPopResponse(array $payload, string $jobId): array
    {
        return [
            'success' => true,
            'queue' => 'emails',
            'leaseId' => 'lease-' . $jobId,
            'consumerGroup' => 'workers',
            'messages' => [[
                'id' => 'message-' . $jobId,
                'transactionId' => 'transaction-' . $jobId,
                'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
                'partition' => $jobId,
                'leaseId' => 'lease-' . $jobId,
                'consumerGroup' => 'workers',
                'deliveryAttempt' => 1,
                'data' => $payload,
            ]],
        ];
    }

    private function acknowledgementResponse(): array
    {
        return [
            'status' => 200,
            'json' => [['success' => true, 'leaseReleased' => true]],
        ];
    }

    private function transactionResponse(): array
    {
        return [
            'status' => 200,
            'json' => ['success' => true, 'transactionId' => 'release-transaction'],
        ];
    }

    /** @return list<string> */
    private function requestPaths(PlanHandler $handler): array
    {
        return array_map(
            static fn ($request): string => $request->getUri()->getPath(),
            $handler->requests,
        );
    }
}
