<?php

namespace Queen\Tests\Integration;

use Queen\Exceptions\HttpException;

/**
 * The timer surface against a live broker.
 *
 * Delays are asserted as "it arrived", never as "it arrived at T": `deliverAt`
 * is not before, never exactly at, and a test that asserted an instant would be
 * asserting a guarantee the product does not make.
 */
class TimersIntegrationTest extends IntegrationTestCase
{
    public function testScheduledTimerIsDeliveredToTheDestinationQueue(): void
    {
        $scheduled = $this->queen->timers()->schedule(
            self::TIMER_QUEUE,
            'soon',
            250,
            ['orderId' => 42, 'reason' => 'retry']
        );

        $this->assertTrue($scheduled['ok']);
        $this->assertSame('scheduled', $scheduled['status']);
        // The message id is promised AT SCHEDULE TIME, so the caller can
        // correlate the delivered frame without a second API.
        $this->assertNotEmpty($scheduled['messageId']);
        $this->assertNotEmpty($scheduled['txn']);
        $this->assertNotEmpty($scheduled['deliverAt']);

        $messages = $this->waitFor(fn() => $this->popAll(self::TIMER_QUEUE));

        $this->assertNotNull($messages, 'the timer never arrived in its destination queue; still pending: '
            . json_encode($this->queen->timers()->peek(self::TIMER_QUEUE, 'soon'))
            . ' queues: ' . json_encode($this->queen->admin()->listQueues()));
        $this->assertSame(['orderId' => 42, 'reason' => 'retry'], $messages[0]['data']);
        $this->assertSame($scheduled['messageId'], $messages[0]['id']);
        $this->assertSame($scheduled['txn'], $messages[0]['transactionId']);
    }

    public function testPeekShowsAPendingTimerAndItsPayload(): void
    {
        $scheduled = $this->queen->timers()->schedule(self::TIMER_QUEUE, 'later', 3_600_000, ['v' => 1]);

        $peeked = $this->queen->timers()->peek(self::TIMER_QUEUE, 'later');

        $this->assertTrue($peeked['found']);
        $this->assertSame('later', $peeked['timerKey']);
        $this->assertSame($scheduled['txn'], $peeked['txn']);
        $this->assertSame(0, $peeked['attempts']);
        // A row nobody holds reads claimed:false — and a row in retry backoff
        // does too, deliberately, because it is still cancellable.
        $this->assertFalse($peeked['claimed']);
        $this->assertSame(['v' => 1], json_decode(base64_decode($peeked['payload'], true), true));
    }

    public function testPeekOnAnAbsentTimerIsFoundFalseAndNotA404(): void
    {
        $peeked = $this->queen->timers()->peek(self::TIMER_QUEUE, 'later');

        $this->assertFalse($peeked['found']);
        $this->assertSame('later', $peeked['timerKey']);
    }

    /**
     * Reschedule is the SAME upsert as schedule, which is what makes a client
     * retry after a crash safe by construction: the second call replaces the
     * first rather than creating a second timer.
     */
    public function testRescheduleReplacesRatherThanDuplicating(): void
    {
        $timers = $this->queen->timers();
        $timers->schedule(self::TIMER_QUEUE, 'replaced', 3_600_000, ['v' => 'old']);

        $again = $timers->reschedule(self::TIMER_QUEUE, 'replaced', 7_200_000, ['v' => 'new']);
        $this->assertTrue($again['ok']);
        $this->assertSame('rescheduled', $again['status']);

        $peeked = $timers->peek(self::TIMER_QUEUE, 'replaced');
        $this->assertSame(['v' => 'new'], json_decode(base64_decode($peeked['payload'], true), true));

        $this->assertCount(1, $timers->list(self::TIMER_QUEUE)['rows']);
    }

    public function testCancelRemovesAPendingTimer(): void
    {
        $timers = $this->queen->timers();
        $scheduled = $timers->schedule(self::TIMER_QUEUE, 'cancelled', 3_600_000, ['v' => 1]);

        $cancelled = $timers->cancel(self::TIMER_QUEUE, 'cancelled', $scheduled['txn']);

        $this->assertTrue($cancelled['ok']);
        $this->assertSame('cancelled', $cancelled['status']);
        $this->assertSame($scheduled['txn'], $cancelled['txn']);
        $this->assertFalse($timers->peek(self::TIMER_QUEUE, 'cancelled')['found']);
    }

    /**
     * THE CONTRACT THAT HURTS PEOPLE. There is no tombstone: cancelling a timer
     * that was never scheduled — or one that has already been DELIVERED —
     * answers `absent`, and `absent` carries ok:false precisely so that a
     * caller which trusts the field does not read it as success.
     *
     * The echoed txn is what makes the difference checkable: look for it in the
     * destination queue, and the log is the authority.
     */
    public function testCancellingAnUnknownTimerIsAbsentWithOkFalse(): void
    {
        $cancelled = $this->queen->timers()->cancel(self::TIMER_QUEUE, 'cancelled', 'txn-i-expected');

        $this->assertFalse($cancelled['ok']);
        $this->assertSame('absent', $cancelled['status']);
        $this->assertSame('txn-i-expected', $cancelled['txn']);
    }

    public function testListIsScopedToOneQueueAndPagesByKeyset(): void
    {
        $timers = $this->queen->timers();
        $timers->schedule(self::TIMER_QUEUE, 'listed-a', 3_600_000, 1);
        $timers->schedule(self::TIMER_QUEUE, 'listed-b', 3_600_000, 2);

        $page = $timers->list(self::TIMER_QUEUE, ['limit' => 1]);
        $this->assertCount(1, $page['rows']);
        $this->assertSame('listed-a', $page['rows'][0]['timerKey']);
        $this->assertTrue($page['truncated']);
        $this->assertSame('listed-a', $page['nextAfter']);
        // No payloads in a listing; peek() is where they live.
        $this->assertArrayNotHasKey('payload', $page['rows'][0]);

        $rest = $timers->list(self::TIMER_QUEUE, ['after' => $page['nextAfter']]);
        $this->assertSame(['listed-b'], array_column($rest['rows'], 'timerKey'));
        $this->assertFalse($rest['truncated']);

        $this->assertSame(
            2,
            $timers->count(self::TIMER_QUEUE, 'listed-'),
            'the exact prefix count must agree without fetching either list page'
        );
    }

    /**
     * A delay in the past is legal and fires on the first cycle. It is the
     * shape a caller reaches for when replaying a schedule computed earlier,
     * and clamping it client-side would silently move the delivery.
     */
    public function testANegativeDelayFiresImmediately(): void
    {
        $this->queen->timers()->schedule(self::TIMER_QUEUE, 'soon', -1000, ['late' => true]);

        $messages = $this->waitFor(fn() => $this->popAll(self::TIMER_QUEUE));

        $this->assertNotNull($messages);
        $this->assertSame(['late' => true], $messages[0]['data']);
    }

    /**
     * §4.2: the provenance fields are the broker's. A client that could set
     * producerSub would get a frame in the log whose provenance is attested by
     * the broker and forged by the client — and producer_sub is the one
     * non-repudiable field a frame has. The client refuses first; this asserts
     * the broker refuses too, so the guarantee does not depend on the SDK.
     */
    public function testTheBrokerRefusesAForgedProducerSub(): void
    {
        try {
            $this->queen->timers()->batch([[
                'op' => 'schedule',
                'queue' => self::TIMER_QUEUE,
                'timerKey' => 'soon',
                'delayMs' => 1000,
                'txn' => 'txn-forged',
                'payload' => base64_encode('null'),
                'producerSub' => 'billing-service',
            ]]);
            $this->fail('a supplied producerSub must be refused');
        } catch (HttpException $e) {
            $this->assertSame(400, $e->statusCode);
        }
    }

    /**
     * And the same for an absolute instant, which is not expressible on this
     * wire at all: one clock, Postgres's, so no skew between brokers can enter.
     */
    public function testTheBrokerRefusesAnAbsoluteDeliverAt(): void
    {
        try {
            $this->queen->timers()->batch([[
                'op' => 'schedule',
                'queue' => self::TIMER_QUEUE,
                'timerKey' => 'soon',
                'deliverAt' => '2030-01-01T00:00:00Z',
                'txn' => 'txn-absolute',
                'payload' => base64_encode('null'),
            ]]);
            $this->fail('an absolute deliverAt must be refused');
        } catch (HttpException $e) {
            $this->assertSame(400, $e->statusCode);
        }
    }
}
