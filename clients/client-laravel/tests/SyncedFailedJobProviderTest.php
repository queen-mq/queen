<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Queue\Failed\CountableFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Failed\PrunableFailedJobProvider;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\SyncedFailedJobProvider;
use Queen\Tests\Support\PlanHandler;

class SyncedFailedJobProviderTest extends TestCase
{
    public function testForgetDeletesTheQueenDlqSnapshotBeforeTheLaravelRecord(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);
        $queue = (new QueenConnector())->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $inner = new InMemoryFailedJobProvider([$this->record()]);
        $provider = new SyncedFailedJobProvider($inner, fn (string $connection) => $queue);

        $this->assertTrue($provider->forget('failed-1'));

        $this->assertSame('/api/v1/messages/partition-1/transaction-1', $handler->requests[0]->getUri()->getPath());
        $this->assertNull($inner->find('failed-1'));
    }

    public function testForgetTreatsAnAlreadyMissingQueenSnapshotAsIdempotent(): void
    {
        $handler = new PlanHandler([['status' => 404, 'json' => ['error' => 'Message not found']]]);
        $queue = (new QueenConnector())->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $inner = new InMemoryFailedJobProvider([$this->record()]);
        $provider = new SyncedFailedJobProvider($inner, fn (string $connection) => $queue);

        $this->assertTrue($provider->forget('failed-1'));
        $this->assertNull($inner->find('failed-1'));
    }

    public function testForgetKeepsTheLaravelRecordWhenDlqCleanupIsUnavailable(): void
    {
        $handler = new PlanHandler([], ['status' => 503, 'json' => ['error' => 'unavailable']]);
        $queue = (new QueenConnector())->connect([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $inner = new InMemoryFailedJobProvider([$this->record()]);
        $provider = new SyncedFailedJobProvider($inner, fn (string $connection) => $queue);

        try {
            $provider->forget('failed-1');
            $this->fail('A failed DLQ cleanup must abort the Laravel deletion.');
        } catch (HttpException $exception) {
            $this->assertSame(503, $exception->statusCode);
        }

        $this->assertNotNull($inner->find('failed-1'));
    }

    private function record(): object
    {
        return (object) [
            'id' => 'failed-1',
            'connection' => 'queen',
            'queue' => 'default',
            'payload' => json_encode([
                'uuid' => 'job-1',
                '_queen' => [
                    'manual_retry' => 'retry-1',
                    'failed_source' => [
                        'partition_id' => 'partition-1',
                        'transaction_id' => 'transaction-1',
                    ],
                ],
            ], JSON_THROW_ON_ERROR),
            'failed_at' => '2026-01-01 00:00:00',
        ];
    }
}

final class InMemoryFailedJobProvider implements
    FailedJobProviderInterface,
    CountableFailedJobProvider,
    PrunableFailedJobProvider
{
    /** @var array<string, object> */
    private array $records = [];

    public function __construct(array $records)
    {
        foreach ($records as $record) {
            $this->records[(string) $record->id] = $record;
        }
    }

    public function log($connection, $queue, $payload, $exception)
    {
        return null;
    }

    public function ids($queue = null)
    {
        return array_keys(array_filter(
            $this->records,
            fn (object $record): bool => $queue === null || $record->queue === $queue,
        ));
    }

    public function all()
    {
        return array_values($this->records);
    }

    public function find($id)
    {
        return $this->records[(string) $id] ?? null;
    }

    public function forget($id)
    {
        if (!isset($this->records[(string) $id])) {
            return false;
        }
        unset($this->records[(string) $id]);
        return true;
    }

    public function flush($hours = null)
    {
        $this->records = [];
    }

    public function prune(\DateTimeInterface $before)
    {
        $count = count($this->records);
        $this->records = [];
        return $count;
    }

    public function count($connection = null, $queue = null)
    {
        return count($this->records);
    }
}
