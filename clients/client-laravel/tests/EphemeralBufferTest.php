<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Buffer\Destination;
use Queen\Buffer\Sink;
use Queen\Ephemeral;
use Queen\Http\HttpClient;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * Buffered ephemeral push: the request it drains into, and the semantics it
 * inherits (EPHEMERAL_QUEUES.md §4.1, §7.3).
 *
 * §4.1 buys this feature by PARAMETRIZING the drain rather than duplicating it,
 * so there are two separable questions and this file answers both:
 *
 *  - the ephemeral sink formats the right envelope for the right route, on BOTH
 *    drain paths (the synchronous one and the concurrent flushAllBuffers pass),
 *    and addresses its buffers under `eph:` so the two storage classes never
 *    share a buffer;
 *  - every property the 1.0.6 buffer rewrite bought — the bound that refuses to
 *    grow, a failed batch back at the FRONT and retried in order, nothing
 *    dropped — is still there on the ephemeral path, because it is literally the
 *    same loop.
 *
 * The third question, "did the refactor move a byte on the DURABLE path", is
 * answered separately in DurableSinkPinTest.
 *
 * PHP has no background flusher, so a buffered push flushes INLINE when the
 * count threshold or the bound says so: every assertion below is made straight
 * after the call that caused the drain, with no waiting anywhere.
 */
class EphemeralBufferTest extends TestCase
{
    private const QUEUE = 'presence';

    // Big enough that the time trigger never fires inside a test: every drain
    // here is caused by the count threshold, the bound, or an explicit flush.
    private const NO_LINGER = 60000;

    // ===========================
    // The request it drains into
    // ===========================

    public function testDrainsOneBatchToTheEphemeralRouteWithIdentityOnTheEnvelope(): void
    {
        $handler = new PlanHandler();
        $result = $this->queen($handler)->ephemeral()->push(
            self::QUEUE,
            [['n' => 1], ['n' => 2]],
            [
                'partition' => 'room-7',
                'buffered' => ['messageCount' => 2, 'timeMillis' => self::NO_LINGER],
            ]
        );

        $this->assertSame(['buffered' => true, 'count' => 2], $result);
        $this->assertSame(1, $handler->count());
        $this->assertSame('/api/v1/ephemeral/push', $handler->requests[0]->getUri()->getPath());
        $this->assertSame([
            'queue' => self::QUEUE,
            'partition' => 'room-7',
            'messages' => [['payload' => ['n' => 1]], ['payload' => ['n' => 2]]],
        ], $this->body($handler->requests[0]));
    }

    public function testOmitsPartitionOnTheDrainedBatchWhenThePushNamedNone(): void
    {
        $handler = new PlanHandler();
        $this->queen($handler)->ephemeral()->push(
            self::QUEUE,
            [['n' => 1]],
            ['buffered' => ['messageCount' => 1, 'timeMillis' => self::NO_LINGER]]
        );

        $this->assertSame([
            'queue' => self::QUEUE,
            'messages' => [['payload' => ['n' => 1]]],
        ], $this->body($handler->requests[0]));
    }

    public function testAnExplicitFlushSendsWhatIsBufferedNow(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->push(self::QUEUE, [['n' => 1]], [
            'partition' => 'room-7',
            'buffered' => ['messageCount' => 1000, 'timeMillis' => self::NO_LINGER],
        ]);
        $this->assertSame(0, $handler->count(), 'nothing may leave below the threshold');

        $eph->flush(self::QUEUE, 'room-7');
        $this->assertSame(1, $handler->count());
        $this->assertSame(
            [['payload' => ['n' => 1]]],
            $this->body($handler->requests[0])['messages']
        );

        // A flush of an address with no buffer is a no-op, not a raise.
        $eph->flush('never-pushed-to');
        $this->assertSame(1, $handler->count());
    }

    /**
     * The concurrent pass has its own POST site, so it needs its own proof: a
     * flushAllBuffers that posted every batch to the durable path would deliver
     * an ephemeral queue's messages to the log engine.
     */
    public function testFlushAllBuffersDrainsAnEphemeralBufferToTheEphemeralRoute(): void
    {
        $handler = new PlanHandler();
        $queen = $this->queen($handler);

        $queen->ephemeral()->push(self::QUEUE, [['n' => 1]], [
            'buffered' => ['messageCount' => 1000, 'timeMillis' => self::NO_LINGER],
        ]);
        $this->assertSame(0, $handler->count());

        $queen->flushAllBuffers();
        $this->assertSame('/api/v1/ephemeral/push', $handler->requests[0]->getUri()->getPath());
        $this->assertSame([
            'queue' => self::QUEUE,
            'messages' => [['payload' => ['n' => 1]]],
        ], $this->body($handler->requests[0]));
    }

    /**
     * §10 Q8: the same name on both engines is legal, and they are unrelated
     * objects. A shared buffer would post one family's messages to the other
     * family's route.
     */
    public function testNeverSharesABufferWithTheDurableQueueOfTheSameName(): void
    {
        $handler = new PlanHandler();
        $queen = $this->queen($handler);

        $queen->ephemeral()->push('orders', [['n' => 1]], [
            'buffered' => ['messageCount' => 1, 'timeMillis' => self::NO_LINGER],
        ]);
        $queen->queue('orders')
            ->partition('Default')
            ->buffer(['messageCount' => 1, 'timeMillis' => self::NO_LINGER])
            ->push([['data' => ['n' => 2], 'transactionId' => 'fixed-2']])
            ->execute();

        $this->assertSame(2, $handler->count());
        $paths = array_map(fn(RequestInterface $r) => $r->getUri()->getPath(), $handler->requests);
        $this->assertSame(['/api/v1/ephemeral/push', '/api/v1/push'], $paths);
        $this->assertSame(
            ['queue' => 'orders', 'messages' => [['payload' => ['n' => 1]]]],
            $this->body($handler->requests[0])
        );
        $this->assertSame(['items'], array_keys($this->body($handler->requests[1])));
    }

    public function testAddressesOneBufferPerQueuePartitionNamespacedUnderEph(): void
    {
        // A named partition and no partition are DIFFERENT destinations: the
        // broker picks when none was named, and merging the two would hand it a
        // partition the caller never chose.
        $this->assertSame('eph:orders/Default', Destination::ephemeralAddress('orders', 'Default'));
        $this->assertSame('eph:orders', Destination::ephemeralAddress('orders'));
        $this->assertSame('orders/Default', Destination::durableAddress('orders', 'Default'));
        $this->assertSame(Sink::ephemeral(), Destination::ephemeral('orders')->sink);
        $this->assertSame(Sink::durable(), Destination::durable()->sink);
    }

    // ===========================
    // The semantics it inherits
    // ===========================

    /**
     * Nothing is dropped on a failed drain, and the producer's order survives
     * the retry — which is the property a restore-at-the-front exists for.
     */
    public function testPutsAFailedBatchBackAtTheFrontAndRetriesItInOrder(): void
    {
        $handler = new PlanHandler([
            ['status' => 503, 'json' => ['error' => 'ephemeral_unavailable']],
            ['status' => 201, 'json' => ['pushed' => 3]],
        ]);

        $this->queen($handler)->ephemeral()->push(
            self::QUEUE,
            [['n' => 1], ['n' => 2], ['n' => 3]],
            [
                'buffered' => [
                    'messageCount' => 3,
                    'timeMillis' => self::NO_LINGER,
                    'retryDelayMillis' => 1,
                    'maxWaitMillis' => 500,
                ],
            ]
        );

        $this->assertSame(2, $handler->count(), 'the failed batch must be retried, not dropped');
        $first = $this->body($handler->requests[0])['messages'];
        $retry = $this->body($handler->requests[1])['messages'];
        $this->assertSame($first, $retry);
        $this->assertSame([['payload' => ['n' => 1]], ['payload' => ['n' => 2]], ['payload' => ['n' => 3]]], $retry);
    }

    /**
     * A batch the broker will not take, ever, is reported rather than silently
     * held: the deadline bounds the retry loop and the original refusal is what
     * reaches the caller.
     */
    public function testRaisesWhenTheDeadlineExpiresWithTheBatchStillUndelivered(): void
    {
        $handler = new PlanHandler([], ['status' => 503, 'json' => ['error' => 'ephemeral_unavailable']]);

        $this->expectException(\Queen\Exceptions\HttpException::class);
        $this->queen($handler)->ephemeral()->push(
            self::QUEUE,
            [['n' => 1]],
            [
                'buffered' => [
                    'messageCount' => 1,
                    'timeMillis' => self::NO_LINGER,
                    'retryDelayMillis' => 1,
                    'maxWaitMillis' => 10,
                ],
            ]
        );
    }

    /**
     * normalizeOptions carries unknown keys untouched, so an untranslated
     * `intervalMillis` would be a linger that quietly does nothing: a producer
     * batching on count alone, stalled below its threshold.
     */
    public function testTranslatesIntervalMillisIntoTheLingerTheBufferReads(): void
    {
        $handler = new PlanHandler();
        $queen = $this->queen($handler);

        // A linger of 1ms with a threshold it will never reach: the drain can
        // only be the TIME trigger, which is checked on the next add.
        $queen->ephemeral()->push(self::QUEUE, [['n' => 1]], [
            'buffered' => ['intervalMillis' => 1, 'messageCount' => 1000],
        ]);
        $this->assertSame(0, $handler->count());

        usleep(5000);
        $queen->ephemeral()->push(self::QUEUE, [['n' => 2]], [
            'buffered' => ['intervalMillis' => 1, 'messageCount' => 1000],
        ]);
        $this->assertSame(1, $handler->count(), 'the linger must be the one the buffer reads');

        $this->expectException(\InvalidArgumentException::class);
        $queen->ephemeral()->push(self::QUEUE, [['n' => 3]], [
            'buffered' => ['intervalMillis' => 1, 'timeMillis' => 2],
        ]);
    }

    public function testAHandBuiltEphemeralSaysSoInsteadOfDroppingTheOption(): void
    {
        $bare = new Ephemeral(new HttpClient(['baseUrl' => 'http://queen.test']));

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/buffer manager/');
        $bare->push(self::QUEUE, [['n' => 1]], ['buffered' => true]);
    }

    // ===========================
    // Helpers
    // ===========================

    private function queen(PlanHandler $handler): Queen
    {
        return new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);
    }

    private function body(RequestInterface $request): array
    {
        return json_decode((string) $request->getBody(), true) ?? [];
    }
}
