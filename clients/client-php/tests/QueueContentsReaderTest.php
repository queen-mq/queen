<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Laravel\Dashboard\QueueContentsReader;
use Queen\Queen;

class QueueContentsReaderTest extends TestCase
{
    /** @var list<string> */
    private array $paths = [];

    public function testWaitingRunningAndTheOldestUnfinishedJobPerQueue(): void
    {
        $reader = $this->reader([
            '/api/v1/resources/queues/app.default/depth' => ['pending' => 5, 'processing' => 1, 'ready' => 4],
            '/api/v1/resources/queues/app.mail/depth' => ['pending' => 0, 'processing' => 0, 'ready' => 0],
            '/api/v1/consumer-groups/lagging' => [
                ['consumer_group' => 'app', 'queue_name' => 'app.default', 'partition_name' => 'laravel-0000', 'time_lag_seconds' => 3913],
                ['consumer_group' => 'app', 'queue_name' => 'app.default', 'partition_name' => 'laravel-0007', 'time_lag_seconds' => 120],
                ['consumer_group' => 'other', 'queue_name' => 'app.mail', 'partition_name' => 'laravel-0001', 'time_lag_seconds' => 999],
            ],
        ]);

        $contents = $reader->read($this->queues(['app.default', 'app.mail']));

        $this->assertTrue($contents['available']);
        $this->assertSame([
            ['connection' => 'queen', 'consumer_group' => 'app', 'queue' => 'app.default', 'available' => true,
                'waiting' => 4, 'running' => 1, 'oldest_seconds' => 3913, 'oldest_partition' => 'laravel-0000'],
            ['connection' => 'queen', 'consumer_group' => 'app', 'queue' => 'app.mail', 'available' => true,
                'waiting' => 0, 'running' => 0, 'oldest_seconds' => null, 'oldest_partition' => null],
        ], $contents['queues'], 'another group\'s lag is not this queue\'s');
        $this->assertContains('/api/v1/consumer-groups/lagging', $this->paths);
        $this->assertSame(1, count(array_keys($this->paths, '/api/v1/consumer-groups/lagging')), 'one lag read per connection');
    }

    public function testABrokerWithoutLeaseAwareDepthFallsBackToPending(): void
    {
        $contents = $this->reader([
            '/api/v1/resources/queues/app.default/depth' => ['pending' => 7],
            '/api/v1/consumer-groups/lagging' => ['data' => []],
        ])->read($this->queues(['app.default']));

        $this->assertSame([7, null], [$contents['queues'][0]['waiting'], $contents['queues'][0]['running']]);
    }

    public function testAnUnreadableQueueIsMarkedAndInvalidNumbersAreDropped(): void
    {
        $contents = $this->reader([
            '/api/v1/resources/queues/app.default/depth' => ['pending' => -3, 'processing' => 'many', 'ready' => 2.5],
            '/api/v1/consumer-groups/lagging' => [['consumer_group' => 'app', 'queue_name' => 'app.default', 'time_lag_seconds' => -1]],
        ], ['/api/v1/resources/queues/app.mail/depth' => 503])->read($this->queues(['app.default', 'app.mail']));

        $this->assertSame([null, null, null], [
            $contents['queues'][0]['waiting'], $contents['queues'][0]['running'], $contents['queues'][0]['oldest_seconds'],
        ]);
        $this->assertFalse($contents['queues'][1]['available']);
        $this->assertTrue($contents['available'], 'one readable queue keeps the card');
    }

    public function testNothingReadableMeansUnavailable(): void
    {
        $contents = $this->reader([], [
            '/api/v1/resources/queues/app.default/depth' => 503,
            '/api/v1/consumer-groups/lagging' => 503,
        ])->read($this->queues(['app.default']));

        $this->assertFalse($contents['available']);
    }

    /** @return list<array<string, string>> */
    private function queues(array $names): array
    {
        return array_map(static fn (string $queue): array => [
            'connection' => 'queen', 'consumer_group' => 'app', 'queue' => $queue,
        ], $names);
    }

    /**
     * @param array<string, mixed> $bodies answer per path
     * @param array<string, int> $statuses a non-200 status per path
     */
    private function reader(array $bodies, array $statuses = []): QueueContentsReader
    {
        $this->paths = [];
        $handler = function (RequestInterface $request) use ($bodies, $statuses): FulfilledPromise {
            $path = $request->getUri()->getPath();
            $this->paths[] = $path;
            $status = $statuses[$path] ?? (array_key_exists($path, $bodies) ? 200 : 404);

            return new FulfilledPromise(new Response($status, ['Content-Type' => 'application/json'],
                json_encode($status === 200 ? $bodies[$path] : ['error' => 'unavailable'])));
        };

        return new QueueContentsReader(fn (string $connection): Queen => new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'retryDelayMillis' => 0,
            'enableFailover' => false,
            'handler' => HandlerStack::create($handler),
        ]));
    }
}
