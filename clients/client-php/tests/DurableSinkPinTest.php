<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Buffer\BufferManager;
use Queen\Buffer\Destination;
use Queen\Buffer\Sink;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * The durable sink, pinned to the byte.
 *
 * EPHEMERAL_QUEUES.md §4.1 buys the ephemeral buffered push by PARAMETRIZING
 * the drain rather than duplicating it, and §7.3 names the price of that
 * bargain: "a pin that the DURABLE sink's bodies are byte-identical before and
 * after the sink refactor". This file is that pin.
 *
 * It is written against the durable path only. Nothing here mentions an
 * ephemeral queue, and that is deliberate: the question it answers is not "does
 * the new feature work" but "did the refactor that made the new feature
 * possible move a single byte on the path that was already in production". An
 * item whose key ORDER changed, an envelope that grew a `queue` field because
 * the ephemeral wire has one, a path that became /api/v1/push/batch — none of
 * those would fail an assertion on a DECODED body, and every one of them is a
 * broken 1.0.6 producer.
 *
 * The literal below is the request the buffered durable push made before sinks
 * existed. It is not derived from the code under test: derive it and the pin
 * pins nothing.
 */
class DurableSinkPinTest extends TestCase
{
    /** The exact bytes a buffered durable push of two items has always produced. */
    private const PINNED_BODY = '{"items":['
        . '{"queue":"orders","partition":"Default","payload":{"n":1},"transactionId":"fixed-1"},'
        . '{"queue":"orders","partition":"Default","payload":{"n":2},"transactionId":"fixed-2"}'
        . ']}';

    public function testDrainsTheBufferedDurablePushToTheSamePathWithTheSameBytes(): void
    {
        $handler = new PlanHandler();

        (new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]))
            ->queue('orders')
            ->partition('Default')
            ->buffer(['messageCount' => 2, 'timeMillis' => 60000])
            ->push([
                ['data' => ['n' => 1], 'transactionId' => 'fixed-1'],
                ['data' => ['n' => 2], 'transactionId' => 'fixed-2'],
            ])
            ->execute();

        $this->assertSame(1, $handler->count());
        $this->assertSame('POST', $handler->requests[0]->getMethod());
        $this->assertSame('/api/v1/push', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(self::PINNED_BODY, (string) $handler->requests[0]->getBody());
    }

    /**
     * The concurrent flush has its own POST site and therefore its own way to
     * drift.
     */
    public function testFlushAllBuffersPostsTheSameDurableBytes(): void
    {
        $handler = new PlanHandler();
        $queen = new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);

        $queen->queue('orders')
            ->partition('Default')
            ->buffer(['messageCount' => 1000, 'timeMillis' => 60000])
            ->push([
                ['data' => ['n' => 1], 'transactionId' => 'fixed-1'],
                ['data' => ['n' => 2], 'transactionId' => 'fixed-2'],
            ])
            ->execute();
        $this->assertSame(0, $handler->count());

        $queen->flushAllBuffers();
        $this->assertSame('/api/v1/push', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(self::PINNED_BODY, (string) $handler->requests[0]->getBody());
    }

    /**
     * An unbuffered durable push does not go through a buffer at all, so the
     * refactor must not have reached it. Asserted anyway: "did not touch it" is
     * cheap to claim and cheap to check.
     */
    public function testAnUnbufferedDurablePushIsUntouchedBySinks(): void
    {
        $handler = new PlanHandler();

        (new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]))
            ->queue('orders')
            ->partition('Default')
            ->push([['data' => ['n' => 1], 'transactionId' => 'fixed-1']])
            ->execute();

        $this->assertSame(
            '{"items":[{"queue":"orders","partition":"Default","payload":{"n":1},"transactionId":"fixed-1"}]}',
            (string) $handler->requests[0]->getBody()
        );
    }

    /**
     * A buffer created without a destination — which is every caller that
     * existed before ephemeral queues did — drains where it always did.
     */
    public function testABufferBuiltWithoutADestinationStillPostsTheDurableWire(): void
    {
        $this->assertSame(Sink::durable(), Destination::durable()->sink);
        $this->assertSame('/api/v1/push', Sink::durable()->path);
        $this->assertSame(
            ['items' => [['a' => 1]]],
            Sink::durable()->format(null, null, [['a' => 1]])
        );

        $handler = new PlanHandler();
        $queen = new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);
        $manager = new BufferManager($this->httpClientOf($queen));

        $manager->addMessage(
            'orders/Default',
            ['queue' => 'orders', 'partition' => 'Default', 'payload' => ['n' => 1]],
            ['messageCount' => 1, 'timeMillis' => 60000]
        );

        $this->assertSame('/api/v1/push', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(
            '{"items":[{"queue":"orders","partition":"Default","payload":{"n":1}}]}',
            (string) $handler->requests[0]->getBody()
        );
    }

    /** The Queen's own HttpClient, so this test drives the real transport stack. */
    private function httpClientOf(Queen $queen): \Queen\Http\HttpClient
    {
        $property = new \ReflectionProperty(Queen::class, 'httpClient');

        return $property->getValue($queen);
    }
}
