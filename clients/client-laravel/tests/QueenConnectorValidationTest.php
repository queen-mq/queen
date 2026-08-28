<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Tests\Support\PlanHandler;

class QueenConnectorValidationTest extends TestCase
{
    private const WORKER_ENVIRONMENT = [
        'QUEEN_LARAVEL_CONSUMER_GROUP',
        'QUEEN_LARAVEL_RETRY_AFTER',
        'QUEEN_LARAVEL_BLOCK_FOR',
    ];

    private array $previousEnvironment = [];

    protected function setUp(): void
    {
        parent::setUp();

        foreach (self::WORKER_ENVIRONMENT as $name) {
            $this->previousEnvironment[$name] = getenv($name);
            putenv($name);
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->previousEnvironment as $name => $value) {
            putenv($value === false ? $name : "{$name}={$value}");
        }

        parent::tearDown();
    }

    public function testInvalidDriverConfigurationFailsFast(): void
    {
        $cases = [
            [['queue' => ''], 'Queen Laravel queue'],
            [['queue' => "orders\n"], 'Queen Laravel queue'],
            [['consumer_group' => '   '], 'Queen Laravel consumer_group'],
            [['consumer_group' => []], 'Queen Laravel consumer_group'],
            [['partition_prefix' => ''], 'Queen Laravel partition_prefix'],
            [['retry_after' => 0], 'Queen Laravel retry_after'],
            [['retry_after' => '90 seconds'], 'Queen Laravel retry_after'],
            [['retry_after' => true], 'Queen Laravel retry_after'],
            [['block_for' => -1], 'Queen Laravel block_for'],
            [['block_for' => '1.5'], 'Queen Laravel block_for'],
            [['partitions' => 0], 'Queen Laravel partitions'],
            [['partitions' => 65], 'Queen Laravel partitions'],
            [['partitions' => 'many'], 'Queen Laravel partitions'],
            [['after_commit' => 'false'], 'Queen Laravel after_commit'],
            [['timeout' => true], 'timeoutMillis'],
            [['enable_failover' => 'false'], 'enableFailover'],
            [['retry_429' => 'invalid'], 'retry429'],
        ];

        foreach ($cases as [$override, $expectedMessage]) {
            try {
                (new QueenConnector())->connect(array_replace($this->validConfig(), $override));
                $this->fail('Invalid Queen Laravel driver configuration was accepted.');
            } catch (InvalidArgumentException $exception) {
                $this->assertStringContainsString($expectedMessage, $exception->getMessage());
            }
        }
    }

    public function testInvalidSupervisorEnvironmentOverridesFailFast(): void
    {
        $cases = [
            ['QUEEN_LARAVEL_CONSUMER_GROUP', "workers\n", 'Queen Laravel consumer_group'],
            ['QUEEN_LARAVEL_RETRY_AFTER', '0', 'Queen Laravel retry_after'],
            ['QUEEN_LARAVEL_BLOCK_FOR', '-1', 'Queen Laravel block_for'],
        ];

        foreach ($cases as [$name, $value, $expectedMessage]) {
            putenv("{$name}={$value}");
            try {
                (new QueenConnector())->connect($this->validConfig());
                $this->fail("Invalid {$name} override was accepted.");
            } catch (InvalidArgumentException $exception) {
                $this->assertStringContainsString($expectedMessage, $exception->getMessage());
            } finally {
                putenv($name);
            }
        }
    }

    public function testDocumentedIntegerStringsAndBoundaryPartitionCountRemainValid(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => ['success' => true, 'messages' => []],
        ]]);
        $queue = (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'handler' => HandlerStack::create($handler),
            'queue' => 'orders/v2',
            'consumer_group' => 'workers/v2',
            'partitions' => '064',
            'retry_after' => '090',
            'block_for' => '0',
        ]));

        $this->assertNull($queue->pop());

        $request = $handler->requests[0];
        parse_str($request->getUri()->getQuery(), $query);
        $this->assertSame('/api/v1/pop/queue/orders%2Fv2', $request->getUri()->getPath());
        $this->assertSame('workers/v2', $query['consumerGroup']);
        $this->assertSame('64', $query['partitions']);
        $this->assertSame('90', $query['leaseSeconds']);
        $this->assertSame('false', $query['wait']);
    }

    private function validConfig(): array
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
}
