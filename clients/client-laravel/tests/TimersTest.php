<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Queen;
use Queen\Support\TimerOp;
use Queen\Tests\Support\PlanHandler;
use Queen\Timers;

/**
 * The timer wire contract, asserted as the EXACT JSON body of every operation.
 *
 * Two rules of PLAN_KV_TIMERS.md are load-bearing here and are cheap to break
 * silently:
 *
 *   * §4.2 — only RELATIVE durations travel, and the field is `delayMs`. An
 *     absolute instant is not expressible: `deliverAt`, `delaySeconds` and
 *     their snake_case spellings are all rejected by the stored procedure. One
 *     clock, Postgres's, so no inter-broker skew can enter anywhere. The
 *     declared rule of the product is "durations that can be sub-second are in
 *     milliseconds, the ones that cannot are in seconds" — a 250 ms retry
 *     backoff is a real and central use of timers, a sub-second TTL is not.
 *
 *   * §4.2 — `producerSub`, `messageId` and the tenant are NOT inputs. Present
 *     in an op they are a 400, never a silent drop: a client posting
 *     {"producerSub":"billing-service"} would otherwise get, one second later,
 *     a frame in the log whose provenance is attested by the broker and forged
 *     by the client, and producer_sub is the one non-repudiable field a frame
 *     has.
 */
class TimersTest extends TestCase
{
    // ===========================
    // Operation shapes (pure)
    // ===========================

    public function testScheduleShape(): void
    {
        $this->assertSame(
            '{"op":"schedule","queue":"payments.retry","timerKey":"order-1",'
            . '"delayMs":250,"txn":"txn-abc","payload":"eyJvcmRlciI6MX0="}',
            json_encode(TimerOp::schedule('payments.retry', 'order-1', 250, ['order' => 1], ['txn' => 'txn-abc']))
        );
    }

    public function testPayloadIsJsonThenBase64(): void
    {
        $op = TimerOp::schedule('q', 'k', 0, ['order' => 1], ['txn' => 't']);

        $this->assertSame('{"order":1}', base64_decode($op['payload'], true));
    }

    public function testScheduleWithPartition(): void
    {
        $this->assertSame(
            '{"op":"schedule","queue":"q","timerKey":"k","delayMs":1000,"txn":"t",'
            . '"payload":"bnVsbA==","partition":"user-123"}',
            json_encode(TimerOp::schedule('q', 'k', 1000, null, ['txn' => 't', 'partition' => 'user-123']))
        );
    }

    /**
     * §4.1: reschedule is the SAME upsert as schedule, which is what makes a
     * client retry after a crash safe by construction. Only the op name differs
     * on the wire.
     */
    public function testRescheduleIsScheduleUnderAnotherName(): void
    {
        $a = TimerOp::schedule('q', 'k', 60_000, ['v' => 1], ['txn' => 't']);
        $b = TimerOp::reschedule('q', 'k', 60_000, ['v' => 1], ['txn' => 't']);

        $this->assertSame('schedule', $a['op']);
        $this->assertSame('reschedule', $b['op']);
        unset($a['op'], $b['op']);
        $this->assertSame($a, $b);
    }

    public function testCancelShape(): void
    {
        $this->assertSame(
            '{"op":"cancel","queue":"q","timerKey":"k"}',
            json_encode(TimerOp::cancel('q', 'k'))
        );
        // §4.4: the caller may echo the txn it expects, and the broker hands it
        // back on `absent` so "was it already delivered?" needs no second API.
        $this->assertSame(
            '{"op":"cancel","queue":"q","timerKey":"k","txn":"txn-abc"}',
            json_encode(TimerOp::cancel('q', 'k', 'txn-abc'))
        );
    }

    /**
     * The txn is mandatory on the wire and identifies the frame the timer will
     * become, so the SDK mints one rather than leaving the broker to do it —
     * the same contract QueueBuilder::push already keeps for transactionId.
     */
    public function testTxnIsMintedWhenAbsent(): void
    {
        $op = TimerOp::schedule('q', 'k', 0, null);

        $this->assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $op['txn']
        );
    }

    // ===========================
    // The client-side rules
    // ===========================

    public function testDelaySecondsIsNotAnOption(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('delaySeconds');
        TimerOp::schedule('q', 'k', 1, null, ['delaySeconds' => 5]);
    }

    public function testDeliverAtIsNotExpressible(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('deliverAt');
        TimerOp::schedule('q', 'k', 1, null, ['deliverAt' => '2026-09-01T00:00:00Z']);
    }

    public function testProducerSubIsNotAnInput(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('producerSub');
        TimerOp::schedule('q', 'k', 1, null, ['producerSub' => 'billing-service']);
    }

    public function testMessageIdIsNotAnInput(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('messageId');
        TimerOp::schedule('q', 'k', 1, null, ['messageId' => '00000000-0000-7000-8000-000000000000']);
    }

    /**
     * §4.2: a delay in the past is LEGAL and fires on the first cycle. It must
     * not be clamped, rounded up or refused by the client.
     */
    public function testANegativeDelayIsLegalAndTravelsUnchanged(): void
    {
        $this->assertSame(-5000, TimerOp::schedule('q', 'k', -5000, null, ['txn' => 't'])['delayMs']);
    }

    // ===========================
    // The client: routes and bodies
    // ===========================

    public function testScheduleGoesToTheBatchRoute(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'results' => [[
                'ok' => true,
                'status' => 'scheduled',
                'queue' => 'q',
                'timerKey' => 'k',
                'txn' => 't',
                'messageId' => '00000000-0000-7000-8000-000000000000',
                'deliverAt' => '2026-08-17T10:00:00.000000Z',
            ]],
        ]]);

        $result = $this->timersFor($handler)->schedule('q', 'k', 250, ['order' => 1], ['txn' => 't']);

        $request = $handler->requests[0];
        $this->assertSame('POST', $request->getMethod());
        $this->assertSame('/api/v1/timers', $request->getUri()->getPath());
        $this->assertSame(
            '{"operations":[{"op":"schedule","queue":"q","timerKey":"k","delayMs":250,'
            . '"txn":"t","payload":"eyJvcmRlciI6MX0="}]}',
            (string) $request->getBody()
        );
        $this->assertSame('scheduled', $result['status']);
    }

    /**
     * §9.6, and it is the reason cancel is not just another element of the
     * batch: DELETE /api/v1/timers/:queue/*timerKey is a route and a class of
     * its own, and it is the one the proxy is required never to block. A tenant
     * that cannot cancel keeps producing messages it cannot stop, because the
     * fire never switches itself off. An SDK that cancelled through
     * POST /api/v1/timers would inherit the schedule route's class and lose
     * exactly that guarantee.
     */
    public function testCancelUsesTheDedicatedDeleteRoute(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'ok' => false, 'status' => 'absent', 'queue' => 'q', 'timerKey' => 'k', 'txn' => 'txn-abc',
        ]]);

        $result = $this->timersFor($handler)->cancel('q', 'k', 'txn-abc');

        $request = $handler->requests[0];
        $this->assertSame('DELETE', $request->getMethod());
        $this->assertSame('/api/v1/timers/q/k', $request->getUri()->getPath());
        $this->assertSame('txn=txn-abc', $request->getUri()->getQuery());
        // §4.4: `absent` means "no longer pending" and MAY MEAN ALREADY
        // DELIVERED. It carries ok:false on purpose — the lesson already paid
        // in-house on queue delete, where deleted:false with a 200 read as
        // success to every client that trusted the field.
        $this->assertFalse($result['ok']);
        $this->assertSame('absent', $result['status']);
    }

    public function testCancelWithoutTxnSendsNoQueryString(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['ok' => true, 'status' => 'cancelled']]);

        $this->timersFor($handler)->cancel('q', 'k');

        $this->assertSame('', $handler->requests[0]->getUri()->getQuery());
    }

    /**
     * The timer key is a catch-all path segment, so a key containing a slash is
     * expressible only as %2F — the extractor decodes it once. Encoding the
     * whole key is what keeps `tenant/42` addressing one timer instead of two
     * path segments that happen to work by accident.
     */
    public function testKeysAndQueuesArePercentEncoded(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['ok' => true, 'status' => 'cancelled']]);

        $this->timersFor($handler)->cancel('payments.retry', 'tenant/42 a');

        $this->assertSame(
            '/api/v1/timers/payments.retry/tenant%2F42%20a',
            $handler->requests[0]->getUri()->getPath()
        );
    }

    public function testPeekIsAGetOnTheSamePath(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['found' => false, 'queue' => 'q', 'timerKey' => 'k']]);

        $result = $this->timersFor($handler)->peek('q', 'k');

        $this->assertSame('GET', $handler->requests[0]->getMethod());
        $this->assertSame('/api/v1/timers/q/k', $handler->requests[0]->getUri()->getPath());
        $this->assertFalse($result['found']);
    }

    public function testListIsKeysetAndTheQueueIsMandatory(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'rows' => [], 'truncated' => false, 'nextAfter' => null,
        ]]);

        $this->timersFor($handler)->list('q', ['after' => 'order-1', 'limit' => 50]);

        $request = $handler->requests[0];
        $this->assertSame('GET', $request->getMethod());
        $this->assertSame('/api/v1/timers/q', $request->getUri()->getPath());
        $this->assertSame('after=order-1&limit=50', $request->getUri()->getQuery());
    }

    public function testBatchSendsOperationsVerbatimAndInOrder(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['results' => [[], []]]]);

        $this->timersFor($handler)->batch([
            TimerOp::schedule('q', 'a', 1000, null, ['txn' => 't1']),
            TimerOp::cancel('q', 'b'),
        ]);

        $this->assertSame(
            '{"operations":['
            . '{"op":"schedule","queue":"q","timerKey":"a","delayMs":1000,"txn":"t1","payload":"bnVsbA=="},'
            . '{"op":"cancel","queue":"q","timerKey":"b"}'
            . ']}',
            (string) $handler->requests[0]->getBody()
        );
    }

    public function testQueenExposesTimersAndReusesTheSameInstance(): void
    {
        $queen = new Queen('http://queen.test:6632');

        $this->assertInstanceOf(Timers::class, $queen->timers());
        $this->assertSame($queen->timers(), $queen->timers());
    }

    private function timersFor(PlanHandler $handler): Timers
    {
        return (new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]))->timers();
    }
}
