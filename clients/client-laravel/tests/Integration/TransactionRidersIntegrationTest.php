<?php

namespace Queen\Tests\Integration;

/**
 * KV and timer riders committing with acks and pushes, against a live broker.
 *
 * This is the shape the feature exists for: the transaction is the primary
 * fence, and `expect` is only the secondary assertion. A marker written here
 * shares its fate with the ack — which compare-and-swap cannot do, because an
 * `expect` on a still-matching version succeeds even from a zombie consumer
 * whose lease has already expired.
 */
class TransactionRidersIntegrationTest extends IntegrationTestCase
{
    public function testRidersCommitWithTheBundle(): void
    {
        $result = $this->queen->transaction()
            ->kv(self::NS_STATE)->put('txn-marker', ['step' => 'reserved'], ['ttlSeconds' => 300])
            ->kv(self::NS_COUNTER)->incr('window', 1, ['ttlSeconds' => 300])
            ->timers(self::TIMER_QUEUE)->schedule('txn-timer', 3_600_000, ['compensate' => true])
            ->commit();

        $this->assertTrue($result['success']);

        $this->assertSame(['step' => 'reserved'], $this->queen->kv()->get(self::NS_STATE, 'txn-marker')['value']);
        $this->assertSame(1, $this->queen->kv()->get(self::NS_COUNTER, 'window')['value']);
        $this->assertTrue($this->queen->timers()->peek(self::TIMER_QUEUE, 'txn-timer')['found']);
    }

    /**
     * THE IDIOM THE WHOLE FEATURE EXISTS FOR, end to end.
     *
     * A redelivery finds the marker already there. The bundle is refused whole,
     * so the push does not happen twice — and the answer is a VALUE, not an
     * exception, because a lost precondition is the expected outcome of every
     * legitimate redelivery.
     */
    public function testALostRequiredGateRefusesTheBundleAndReturnsAVerdict(): void
    {
        $first = $this->queen->transaction()
            ->kv(self::NS_STATE)->putIfAbsent('txn-gate', ['owner' => 'first'], ['ttlSeconds' => 300, 'required' => true])
            ->queue(self::TIMER_QUEUE)->push([['data' => ['attempt' => 1]]])
            ->commit();
        $this->assertTrue($first['success']);

        $second = $this->queen->transaction()
            ->kv(self::NS_STATE)->putIfAbsent('txn-gate', ['owner' => 'second'], ['ttlSeconds' => 300, 'required' => true])
            ->queue(self::TIMER_QUEUE)->push([['data' => ['attempt' => 2]]])
            ->commit();

        $this->assertFalse($second['success']);
        $this->assertSame('kv_precondition', $second['reason']);
        $this->assertSame('exists', $second['kvReason']);
        $this->assertSame(['owner' => 'first'], $second['value']);
        // The flat index space: `operations` first (one push), then the kv
        // array — so the gate that lost is index 1.
        $this->assertSame(1, $second['failedIndex']);

        // And the push really did not happen a second time.
        $messages = $this->waitFor(fn() => $this->popAll(self::TIMER_QUEUE));
        $this->assertNotNull($messages);
        $this->assertCount(1, $messages);
        $this->assertSame(['attempt' => 1], $messages[0]['data']);
    }

    /**
     * A timer cancelled inside a bundle. The saga shape: close the saga and
     * withdraw its compensation atomically.
     */
    public function testATimerCanBeCancelledInsideABundle(): void
    {
        $this->queen->timers()->schedule(self::TIMER_QUEUE, 'txn-timer', 3_600_000, ['compensate' => true]);

        $result = $this->queen->transaction()
            ->kv(self::NS_STATE)->put('txn-marker', ['step' => 'done'], ['ttlSeconds' => 300])
            ->timers(self::TIMER_QUEUE)->cancel('txn-timer')
            ->commit();

        $this->assertTrue($result['success']);
        $this->assertFalse($this->queen->timers()->peek(self::TIMER_QUEUE, 'txn-timer')['found']);
    }

    /**
     * §6.3: a bundle carrying neither array must behave exactly as it always
     * has. The rider support is additive, and this is the assertion that says
     * so against a real broker rather than against a mock.
     */
    public function testABundleWithNoRidersIsUnchanged(): void
    {
        $result = $this->queen->transaction()
            ->queue(self::TIMER_QUEUE)->push([['data' => ['plain' => true]]])
            ->commit();

        $this->assertTrue($result['success']);

        $messages = $this->waitFor(fn() => $this->popAll(self::TIMER_QUEUE));
        $this->assertNotNull($messages);
        $this->assertSame(['plain' => true], $messages[0]['data']);
    }
}
