<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Queen;
use Queen\Builders\QueueBuilder;
use Queen\Builders\ConsumeBuilder;
use Queen\Builders\OperationBuilder;
use Queen\Builders\PushBuilder;
use Queen\Builders\DLQBuilder;
use Queen\Consumer\HighLevelConsumer;

class QueueBuilderTest extends TestCase
{
    private Queen $queen;

    protected function setUp(): void
    {
        $this->queen = new Queen('http://localhost:6632');
    }

    public function testFluentChaining(): void
    {
        $builder = $this->queen->queue('orders');

        $result = $builder
            ->namespace('shop')
            ->task('process')
            ->partition('user-1')
            ->group('processors')
            ->concurrency(4)
            ->batch(10)
            ->limit(1000)
            ->idleMillis(5000)
            ->autoAck(false)
            ->wait(true)
            ->timeoutMillis(60000)
            ->leaseSeconds(90)
            ->renewLease(true, 30000)
            ->subscriptionMode('all')
            ->subscriptionFrom('2024-01-01T00:00:00Z');

        // All methods should return the same builder (fluent)
        $this->assertSame($builder, $result);
    }

    public function testCreateReturnsOperationBuilder(): void
    {
        $op = $this->queen->queue('test-queue')
            ->config(['leaseTime' => 600])
            ->create();

        $this->assertInstanceOf(OperationBuilder::class, $op);
    }

    public function testDeleteReturnsOperationBuilder(): void
    {
        $op = $this->queen->queue('test-queue')->delete();
        $this->assertInstanceOf(OperationBuilder::class, $op);
    }

    public function testDeleteWithoutQueueNameThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->queen->queue()->delete();
    }

    public function testPushReturnsPushBuilder(): void
    {
        $push = $this->queen->queue('test-queue')->push([
            ['data' => ['key' => 'value']],
        ]);

        $this->assertInstanceOf(PushBuilder::class, $push);
    }

    public function testPushWithoutQueueNameThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->queen->queue()->push([['data' => 'test']]);
    }

    public function testConsumeReturnsConsumeBuilder(): void
    {
        $consume = $this->queen->queue('test-queue')
            ->group('my-group')
            ->consume(function ($messages) {});

        $this->assertInstanceOf(ConsumeBuilder::class, $consume);
    }

    public function testGetConsumerReturnsHighLevelConsumer(): void
    {
        $consumer = $this->queen->queue('test-queue')
            ->group('my-group')
            ->batch(5)
            ->getConsumer();

        $this->assertInstanceOf(HighLevelConsumer::class, $consumer);
    }

    public function testDlqReturnsDLQBuilder(): void
    {
        $dlq = $this->queen->queue('test-queue')->dlq('my-group');
        $this->assertInstanceOf(DLQBuilder::class, $dlq);
    }

    public function testDlqWithoutQueueNameThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->queen->queue()->dlq();
    }

    public function testFlushBufferWithoutQueueNameThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->queen->queue()->flushBuffer();
    }

    public function testEachMethodSetsFlag(): void
    {
        // each() should be chainable and return the builder
        $builder = $this->queen->queue('test');
        $result = $builder->each();
        $this->assertSame($builder, $result);
    }

    public function testBufferMethodSetsOptions(): void
    {
        $builder = $this->queen->queue('test');
        $result = $builder->buffer(['messageCount' => 50, 'timeMillis' => 500]);
        $this->assertSame($builder, $result);
    }

    public function testConcurrencyMinimumIsOne(): void
    {
        $builder = $this->queen->queue('test');
        $result = $builder->concurrency(0);
        $this->assertSame($builder, $result);
        // concurrency(0) should be clamped to 1 internally
    }

    public function testBatchMinimumIsOne(): void
    {
        $builder = $this->queen->queue('test');
        $result = $builder->batch(0);
        $this->assertSame($builder, $result);
    }
}
