<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\LazyLeaseRenewer;
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
            [['prefetch' => 0], 'Queen Laravel prefetch'],
            [['prefetch' => 1001], 'Queen Laravel prefetch'],
            [['ack_batch' => 0], 'Queen Laravel ack_batch'],
            [['prefetch' => 4, 'ack_batch' => 5], 'Queen Laravel ack_batch'],
            [['bulk_batch' => 0], 'Queen Laravel bulk_batch'],
            [['bulk_batch' => 1001], 'Queen Laravel bulk_batch'],
            [['after_commit' => 'false'], 'Queen Laravel after_commit'],
            [['lease_renewal' => 'true'], 'Queen Laravel lease_renewal'],
            [['lease_renewal_interval' => 0], 'Queen Laravel lease_renewal_interval'],
            [['lease_renewal_timeout' => 0], 'Queen Laravel lease_renewal_timeout'],
            [['lease_renewal_kill_grace' => -1], 'Queen Laravel lease_renewal_kill_grace'],
            [['lease_renewal_safety_margin' => 0], 'Queen Laravel lease_renewal_safety_margin'],
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
            'prefetch' => '001',
            'ack_batch' => '001',
            'bulk_batch' => '100',
        ]));

        $this->assertNull($queue->pop());

        $request = $handler->requests[0];
        parse_str($request->getUri()->getQuery(), $query);
        $this->assertSame('/api/v1/pop/queue/orders%2Fv2', $request->getUri()->getPath());
        $this->assertSame('workers/v2', $query['consumerGroup']);
        $this->assertSame('64', $query['partitions']);
        $this->assertSame('1', $query['batch']);
        $this->assertSame('90', $query['leaseSeconds']);
        $this->assertSame('false', $query['wait']);
    }

    public function testEveryConnectorPathRejectsUnrenewedPrefetch(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('prefetch [4] requires lease_renewal');

        (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'prefetch' => 4,
            'ack_batch' => 1,
            'lease_renewal' => false,
        ]));
    }

    public function testRetryAfterCannotExceedTheSignedWireInteger(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('retry_after');
        $this->expectExceptionMessage('1..2147483647');

        (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'retry_after' => 2_147_483_648,
        ]));
    }

    public function testLeaseRenewalRejectsAnUnsafeDeadlineBudget(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('lease_renewal timing is unsafe');

        (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'lease_renewal' => true,
            'retry_after' => 30,
            'lease_renewal_interval' => 10,
            'lease_renewal_timeout' => 5,
            'lease_renewal_kill_grace' => 5,
            'lease_renewal_safety_margin' => 4,
        ]));
    }

    public function testLeaseRenewalRejectsTheInternalMockHandler(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('test HTTP handler');

        (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'handler' => HandlerStack::create(new PlanHandler()),
            'lease_renewal' => true,
            'retry_after' => 120,
        ]));
    }

    public function testSafeLeaseRenewalConfigurationBuildsWithoutStartingTheHelper(): void
    {
        $queue = (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'lease_renewal' => true,
            'prefetch' => 4,
            'retry_after' => 120,
            'lease_renewal_interval' => 30,
            'lease_renewal_timeout' => 5,
        ]));

        $this->assertInstanceOf(\Queen\Laravel\Queue\QueenQueue::class, $queue);
        $property = new \ReflectionProperty($queue, 'leaseRenewer');
        $renewer = $property->getValue($queue);
        $this->assertInstanceOf(LazyLeaseRenewer::class, $renewer);
        $delegate = new \ReflectionProperty($renewer, 'delegate');
        $this->assertNull($delegate->getValue($renewer));
    }

    public function testShutdownTailReleaseUsesOneBoundedNonFailoverAttempt(): void
    {
        $handler = new PlanHandler([[
            'status' => 200,
            'json' => [['success' => true, 'leaseReleased' => true]],
        ]]);
        $queue = (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'handler' => HandlerStack::create($handler),
            'timeout' => 30_000,
            'retry_attempts' => 9,
        ]));
        $releaser = (new \ReflectionProperty($queue, 'shutdownTailReleaser'))->getValue($queue);
        $message = [
            'transactionId' => 'transaction-1',
            'partitionId' => 'partition-1',
            'leaseId' => 'lease-1',
            '_status' => 'retry',
        ];

        $result = $releaser([$message], 'workers', 'emails:Default:workers');

        $this->assertTrue($result['success']);
        $this->assertCount(1, $handler->requests);
        $this->assertSame(2, $handler->options[0]['timeout']);
        $this->assertSame('retry', json_decode(
            (string) $handler->requests[0]->getBody(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        )['acknowledgments'][0]['status']);
    }

    public function testEmptyOptionalLeaseRenewalIntervalUsesTheRetryAfterDefault(): void
    {
        $queue = (new QueenConnector())->connect(array_replace($this->validConfig(), [
            'lease_renewal' => false,
            'lease_renewal_interval' => '',
        ]));

        $this->assertInstanceOf(\Queen\Laravel\Queue\QueenQueue::class, $queue);
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
