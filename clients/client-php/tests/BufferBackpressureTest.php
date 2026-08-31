<?php

namespace Queen\Tests;

use GuzzleHttp\Promise\Create;
use PHPUnit\Framework\TestCase;
use Queen\Buffer\BufferManager;
use Queen\Buffer\MessageBuffer;
use Queen\Http\HttpClient;

/**
 * The client-buffer contract: a bounded buffer that pushes back instead of
 * growing, and a failed batch that is re-queued in order and retried instead
 * of dropped.
 *
 * Measured motivation (2026-08-20, on the Go SDK whose buffer had the same
 * shape as this one): an unbounded buffer filling at 1.46M msg/s against a
 * 1.0M msg/s flush pipeline accumulated 20.9M messages (11.7 GB RSS) in 45
 * seconds and lost every one at process exit, with zero client-side errors
 * reported. Bounded, the same producer sustains 881,148 msg/s with exact
 * send/receive parity at 71 MB RSS.
 *
 * Every test here fakes the sink and drives the buffer with millisecond knobs:
 * no broker, no network, and never the production defaults (250ms between
 * retries, a 5s deadline) which would park the suite for seconds at a time.
 */
class BufferBackpressureTest extends TestCase
{
    /** Knobs shared by the tests that need a fast, deterministic retry loop. */
    private const FAST = [
        'timeMillis' => 99999,
        'retryDelayMillis' => 1,
        'maxWaitMillis' => 200,
    ];

    // =====================================================================
    // (e) Option resolution: unbounded is not expressible
    // =====================================================================

    public function testMaxSizeResolvesToFourTimesMessageCount(): void
    {
        $noop = function (string $addr): void {
        };

        // Absent, zero and negative all mean "not set", never "unbounded":
        // unbounded is the defect this bound exists to remove.
        $absent = new MessageBuffer('q/p', ['messageCount' => 25, 'timeMillis' => 1000], $noop);
        $this->assertSame(100, $absent->getMaxSize());

        $zero = new MessageBuffer('q/p', ['messageCount' => 25, 'maxSize' => 0], $noop);
        $this->assertSame(100, $zero->getMaxSize());

        $negative = new MessageBuffer('q/p', ['messageCount' => 25, 'maxSize' => -1], $noop);
        $this->assertSame(100, $negative->getMaxSize());

        // Floored up to messageCount: a bound under the flush threshold would
        // make the buffer "full" before it ever reached the count that
        // triggers a flush.
        $floored = new MessageBuffer('q/p', ['messageCount' => 25, 'maxSize' => 10], $noop);
        $this->assertSame(25, $floored->getMaxSize());

        $explicit = new MessageBuffer('q/p', ['messageCount' => 25, 'maxSize' => 60], $noop);
        $this->assertSame(60, $explicit->getMaxSize());
    }

    public function testNormalizeOptionsFillsInTheDefaults(): void
    {
        $defaults = MessageBuffer::normalizeOptions([]);

        $this->assertSame(100, $defaults['messageCount']);
        $this->assertSame(1000, $defaults['timeMillis']);
        $this->assertSame(400, $defaults['maxSize']);
        $this->assertSame(250, $defaults['retryDelayMillis']);
        $this->assertSame(5000, $defaults['maxWaitMillis']);

        // Idempotent: the manager normalizes, then the buffer normalizes again.
        $this->assertSame($defaults, MessageBuffer::normalizeOptions($defaults));
    }

    // =====================================================================
    // (a) The add path holds the bound instead of growing past it
    // =====================================================================

    public function testAddPathNeverGrowsPastTheMaxSizeBound(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')
            ->willThrowException(new \RuntimeException('broker down'));

        $manager = new BufferManager($httpClient);
        $options = array_merge(self::FAST, [
            'messageCount' => 4,
            'maxSize' => 8,
            'maxWaitMillis' => 3,
        ]);

        $raised = 0;
        for ($i = 0; $i < 40; $i++) {
            try {
                $manager->addMessage('q/p', ['payload' => "m{$i}"], $options);
            } catch (\Throwable $error) {
                // Backpressure, or a flush that could not land. Either way the
                // producer is TOLD. The old code reported success here and
                // kept the message in a heap that dies with the process.
                $raised++;
            }

            $this->assertLessThanOrEqual(
                8,
                $manager->getStats()['totalBufferedMessages'],
                "buffer grew past its bound at add {$i}"
            );
        }

        // Pinned at the bound with a dead broker, not 40 messages deep.
        $this->assertSame(8, $manager->getStats()['totalBufferedMessages']);
        $this->assertGreaterThan(0, $raised);
        $this->assertSame(0, $manager->getStats()['flushesPerformed']);
    }

    public function testBoundAppliesWhenMaxSizeWasNeverConfigured(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')
            ->willThrowException(new \RuntimeException('broker down'));

        $manager = new BufferManager($httpClient);
        // No maxSize at all: the derived bound (4 * messageCount = 8) must
        // still hold, because "not configured" is not "unbounded".
        $options = array_merge(self::FAST, ['messageCount' => 2, 'maxWaitMillis' => 3]);

        for ($i = 0; $i < 30; $i++) {
            try {
                $manager->addMessage('q/p', ['payload' => "m{$i}"], $options);
            } catch (\Throwable $error) {
                // expected while the broker is down
            }
        }

        $this->assertSame(8, $manager->getStats()['totalBufferedMessages']);
    }

    // =====================================================================
    // (b) A failed batch is re-queued at the front, in order, and retried
    // =====================================================================

    public function testFailedBatchIsRequeuedInOrderAndRetried(): void
    {
        $sent = [];
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')
            ->willReturnCallback(function (string $path, ?array $body = null) use (&$sent) {
                $sent[] = array_column($body['items'], 'payload');
                if (count($sent) < 3) {
                    throw new \RuntimeException('transient 503');
                }
                return ['success' => true];
            });

        $manager = new BufferManager($httpClient);
        $options = array_merge(self::FAST, ['messageCount' => 3]);

        foreach (['a', 'b', 'c'] as $payload) {
            $manager->addMessage('q/p', ['payload' => $payload], $options);
        }

        // Two failures and one success: the retry actually happened, rather
        // than the batch being handed back to a caller who never asked to own
        // the retry.
        $this->assertCount(3, $sent);
        $this->assertSame(['a', 'b', 'c'], $sent[0]);
        $this->assertSame(['a', 'b', 'c'], $sent[1], 'the retry must resend the same batch');
        $this->assertSame(['a', 'b', 'c'], $sent[2], 'and in the same order');

        $this->assertSame(0, $manager->getStats()['totalBufferedMessages']);
        $this->assertSame(1, $manager->getStats()['flushesPerformed']);
    }

    // =====================================================================
    // (c) Nothing is dropped, ever
    // =====================================================================

    public function testNothingIsDroppedAcrossIntermittentFailures(): void
    {
        $received = [];
        $attempt = 0;
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')
            ->willReturnCallback(function (string $path, ?array $body = null) use (&$received, &$attempt) {
                $attempt++;
                // Every third send fails. The failure lands BEFORE anything is
                // recorded, so a message counted here is a message the sink
                // really accepted — exact-once parity is the assertion.
                if ($attempt % 3 === 0) {
                    throw new \RuntimeException('flaky broker');
                }
                foreach ($body['items'] as $item) {
                    $received[] = $item['payload'];
                }
                return ['success' => true];
            });

        $manager = new BufferManager($httpClient);
        $options = array_merge(self::FAST, ['messageCount' => 5]);

        $expected = [];
        for ($i = 0; $i < 200; $i++) {
            $expected[] = "m{$i}";
            $manager->addMessage('q/p', ['payload' => "m{$i}"], $options);
        }
        $manager->flushBuffer('q/p');

        $this->assertCount(200, $received, 'send/receive parity must be exact');
        $this->assertSame($expected, $received, 'and the stream must stay in producer order');
        $this->assertSame(0, $manager->getStats()['totalBufferedMessages']);
        $this->assertGreaterThan(0, $attempt - $manager->getStats()['flushesPerformed']);
    }

    // =====================================================================
    // (d) The deadline raises — it never drops and never lies
    // =====================================================================

    public function testDeadlineExpiryRaisesAndKeepsTheMessages(): void
    {
        $down = true;
        $sent = [];
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')
            ->willReturnCallback(function (string $path, ?array $body = null) use (&$down, &$sent) {
                if ($down) {
                    throw new \RuntimeException('broker down');
                }
                $sent[] = array_column($body['items'], 'payload');
                return ['success' => true];
            });

        $manager = new BufferManager($httpClient);
        $options = array_merge(self::FAST, ['messageCount' => 3, 'maxWaitMillis' => 10]);

        $manager->addMessage('q/p', ['payload' => 'a'], $options);
        $manager->addMessage('q/p', ['payload' => 'b'], $options);

        $startedAt = microtime(true);
        try {
            // Hits messageCount, so this add flushes inline — and the flush
            // cannot land.
            $manager->addMessage('q/p', ['payload' => 'c'], $options);
            $this->fail('the add must raise when the flush cannot land inside maxWaitMillis');
        } catch (\RuntimeException $error) {
            // The original transport error, not a wrapper: callers branch on it.
            $this->assertSame('broker down', $error->getMessage());
        }

        // It spent the deadline retrying rather than giving up on the first
        // error, and it did not park past it either.
        $elapsedMs = (microtime(true) - $startedAt) * 1000;
        $this->assertGreaterThanOrEqual(8, $elapsedMs);

        // Raised, but NOT lost: still queued, ready for the caller to retry
        // the flush or for the next add to trigger one.
        $this->assertSame(3, $manager->getStats()['totalBufferedMessages']);
        $this->assertSame(0, $manager->getStats()['flushesPerformed']);

        $down = false;
        $manager->flushBuffer('q/p');
        $this->assertSame([['a', 'b', 'c']], $sent, 'the held messages are intact and in order');
        $this->assertSame(0, $manager->getStats()['totalBufferedMessages']);
    }

    // =====================================================================
    // flushAllBuffers: its own restore path had to learn the same contract
    // =====================================================================

    public function testFlushAllBuffersRestoresInOrderWithTheBuffersOwnOptions(): void
    {
        $down = true;
        $sent = [];
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')
            ->willReturnCallback(function (string $path, ?array $body = null) use (&$down, &$sent) {
                if ($down) {
                    throw new \RuntimeException('broker down');
                }
                $sent[] = array_column($body['items'], 'payload');
                return ['success' => true];
            });
        // The concurrent pass always fails, so every batch takes the restore
        // path and then the sequential retry.
        $httpClient->method('postAsync')
            ->willReturnCallback(fn() => Create::rejectionFor(new \RuntimeException('async down')));

        $manager = new BufferManager($httpClient);
        $options = array_merge(self::FAST, [
            'messageCount' => 2,
            'maxSize' => 8,
            'maxWaitMillis' => 5,
        ]);

        // Park three messages in the buffer while the sink is down.
        foreach (['m0', 'm1', 'm2'] as $payload) {
            try {
                $manager->addMessage('q/p', ['payload' => $payload], $options);
            } catch (\Throwable $error) {
                // expected: the inline flush cannot land yet
            }
        }
        $this->assertSame(3, $manager->getStats()['totalBufferedMessages']);

        $down = false;
        $manager->flushAllBuffers();

        // Two batches failed the concurrent pass and were restored. Restoring
        // them one at a time unshifts each in front of the previous one, which
        // reverses the stream — [m2, m0, m1]. They must come back in order,
        // and the retry must batch by the buffer's OWN messageCount of 2: a
        // buffer re-created from Defaults::BUFFER_DEFAULTS would batch by 100
        // and send all three in one call, silently retuning what the caller
        // asked for.
        $this->assertSame([['m0', 'm1'], ['m2']], $sent);
        $this->assertSame(0, $manager->getStats()['activeBuffers']);
        $this->assertSame(2, $manager->getStats()['flushesPerformed']);
    }

    // =====================================================================
    // The time trigger must not eat the message that follows it
    // =====================================================================

    public function testMessageAfterATimeTriggeredFlushIsNotDropped(): void
    {
        $sent = [];
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')
            ->willReturnCallback(function (string $path, ?array $body = null) use (&$sent) {
                $sent[] = array_column($body['items'], 'payload');
                return ['success' => true];
            });

        $manager = new BufferManager($httpClient);
        $options = array_merge(self::FAST, ['messageCount' => 100, 'timeMillis' => 5]);

        $manager->addMessage('q/p', ['payload' => 'first'], $options);
        usleep(8_000); // let the time trigger fall due

        // checkTimeTrigger() flushes 'first' from inside this call. While a
        // drained buffer was retired mid-add, 'second' landed in a buffer that
        // was no longer reachable from the manager: invisible to getStats(),
        // never flushed, gone.
        $manager->addMessage('q/p', ['payload' => 'second'], $options);
        $this->assertSame(1, $manager->getStats()['totalBufferedMessages']);

        $manager->flushBuffer('q/p');
        $this->assertSame([['first'], ['second']], $sent);
    }
}
