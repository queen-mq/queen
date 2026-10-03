<?php

namespace Queen\Tests\Integration;

/**
 * A bundle that acknowledges messages of two leases, against a live broker.
 *
 * This is the shape of a Laravel worker's hand-back when it holds the tails of
 * two batches. A Queen 2 broker fences an ACK with the lease the operation
 * carries, and lends requiredLeases to an ACK without one only when the bundle
 * names a single lease. So an ACK that does not carry its own lease is applied
 * in a bundle of two leases even after its lease expired and another consumer
 * took the message.
 */
class TransactionLeaseFencingIntegrationTest extends IntegrationTestCase
{
    private const QUEUE_A = 'php-fence-a';
    private const QUEUE_B = 'php-fence-b';

    public function testABundleWithAnAckUnderAnExpiredLeaseIsRefusedWhole(): void
    {
        $this->pushOne(self::QUEUE_A, 'a');
        $this->pushOne(self::QUEUE_B, 'b');
        $a = $this->popOne(self::QUEUE_A, 1);
        $b = $this->popOne(self::QUEUE_B, 60);
        $this->assertNotSame($a['leaseId'], $b['leaseId']);

        // Lease A expires, and another consumer takes the message.
        $taken = $this->waitFor(fn () => $this->queen->queue(self::QUEUE_A)
            ->subscriptionMode('all')->leaseSeconds(60)->pop(), 15000);
        $this->assertNotNull($taken, 'the message of the expired lease is delivered again');
        $this->assertSame($a['transactionId'], $taken[0]['transactionId']);
        $this->assertNotSame($a['leaseId'], $taken[0]['leaseId']);

        $refused = null;
        try {
            $this->queen->transaction()->ack($a)->ack($b)->commit();
        } catch (\RuntimeException $exception) {
            $refused = $exception;
        }
        $this->assertNotNull($refused, 'an ACK under an expired lease must refuse the whole bundle');
        $this->assertStringContainsString('rolled back', $refused->getMessage());

        // Nothing of the bundle happened: each holder still settles its own message.
        $this->assertTrue($this->queen->transaction()->ack($taken[0])->commit()['success']);
        $this->assertTrue($this->queen->transaction()->ack($b)->commit()['success']);
        $this->assertSame([], $this->popNow(self::QUEUE_A));
        $this->assertSame([], $this->popNow(self::QUEUE_B));
    }

    public function testABundleOfTwoLiveLeasesCompletesBoth(): void
    {
        $this->pushOne(self::QUEUE_A, 'a');
        $this->pushOne(self::QUEUE_B, 'b');
        $a = $this->popOne(self::QUEUE_A, 60);
        $b = $this->popOne(self::QUEUE_B, 60);

        $result = $this->queen->transaction()->ack($a)->ack($b)->commit();

        $this->assertTrue($result['success']);
        $this->assertSame([], $this->popNow(self::QUEUE_A));
        $this->assertSame([], $this->popNow(self::QUEUE_B));
    }

    protected function cleanupTestData(): void
    {
        parent::cleanupTestData();
        foreach ([self::QUEUE_A, self::QUEUE_B] as $queue) {
            try {
                $this->queen->queue($queue)->delete()->execute();
            } catch (\Throwable $e) {
                // Never created yet.
            }
        }
    }

    private function pushOne(string $queue, string $name): void
    {
        $this->queen->queue($queue)->push([['data' => ['name' => $name]]])->execute();
    }

    /** What a pop that does not wait finds: a pop waits for a message by default. */
    private function popNow(string $queue): array
    {
        return $this->queen->queue($queue)->subscriptionMode('all')->wait(false)->pop();
    }

    /** @return array the one delivery, leased for $leaseSeconds */
    private function popOne(string $queue, int $leaseSeconds): array
    {
        $messages = $this->waitFor(fn () => $this->queen->queue($queue)
            ->subscriptionMode('all')->leaseSeconds($leaseSeconds)->pop());
        $this->assertNotNull($messages, "nothing to pop from {$queue}");
        $this->assertCount(1, $messages);
        $this->assertIsString($messages[0]['leaseId'] ?? null);

        return $messages[0];
    }
}
