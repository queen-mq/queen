<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Exceptions\EphemeralUnsupportedException;
use Queen\Exceptions\ErrorCode;
use Queen\Exceptions\HttpException;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * The ephemeral wire contract, asserted on the EXACT request — no broker.
 *
 * EPHEMERAL_QUEUES.md §3.1 is the authority for every method, path, query
 * string and body below, and this file is what keeps this SDK on it. The reason
 * to assert the request rather than the return value is the reason the KV suite
 * gives for the same choice: a wrong shape does not raise anywhere useful. A
 * push whose messages carried the durable per-item {queue, partition, payload}
 * is a 400 nobody sees until a live broker is involved; a pop that forgot to
 * send `timeout` beside `wait=true` is a long poll returning on the BROKER's
 * default instead of the caller's, which nothing observes at all.
 *
 * One more thing is pinned here that no end-to-end run against a 1.1 broker
 * could ever produce: the 404 mapping (§4). A broker or proxy older than 1.1
 * answers 404 on the whole family, and the SDK has to turn that into one clear
 * "upgrade" verdict rather than let it read as "your queue is missing".
 */
class EphemeralTest extends TestCase
{
    private const QUEUE = 'inbox';

    // ===========================
    // Declaration
    // ===========================

    public function testConfigureSendsTheQueueAndItsOptionsUnderOptions(): void
    {
        $handler = new PlanHandler();
        $this->queen($handler)->ephemeral()->configure(self::QUEUE, [
            'maxBytes' => 1048576,
            'maxLength' => 500,
            'policy' => 'dropOldest',
            'ttlSeconds' => 30,
            'leaseSeconds' => 15,
            'retryLimit' => 3,
            'windowBuffer' => ['ms' => 20, 'count' => 50],
        ]);

        $this->assertSame('POST', $handler->requests[0]->getMethod());
        $this->assertSame('/api/v1/ephemeral/configure', $handler->requests[0]->getUri()->getPath());
        $this->assertSame([
            'queue' => self::QUEUE,
            'options' => [
                'maxBytes' => 1048576,
                'maxLength' => 500,
                'policy' => 'dropOldest',
                'ttlSeconds' => 30,
                'leaseSeconds' => 15,
                'retryLimit' => 3,
                'windowBuffer' => ['ms' => 20, 'count' => 50],
            ],
        ], $this->body($handler->requests[0]));
    }

    public function testConfigureSendsOnlyTheOptionsItWasGiven(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->configure(self::QUEUE, ['ttlSeconds' => 30]);
        $this->assertSame(
            ['queue' => self::QUEUE, 'options' => ['ttlSeconds' => 30]],
            $this->body($handler->requests[0])
        );

        $eph->configure(self::QUEUE);
        // An empty PHP array serializes as `[]`, and the broker reads the field
        // as an object; asserting the decoded value keeps that from being a
        // test about json_encode.
        $this->assertSame([], $this->body($handler->requests[1])['options']);
    }

    /**
     * Refused, not dropped: every one of the seven knobs bounds something, and
     * a silently ignored `ttlSecond` is a ring that grows until a global budget
     * answers 503.
     */
    public function testConfigureRefusesAnOptionThisClientDoesNotKnow(): void
    {
        $handler = new PlanHandler();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/ttlSecond/');

        try {
            $this->queen($handler)->ephemeral()->configure(self::QUEUE, ['ttlSecond' => 30]);
        } finally {
            $this->assertSame(0, $handler->count(), 'nothing may reach the wire');
        }
    }

    public function testResetAndDeleteNameTheQueueWhereEachRouteExpectsIt(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->reset(self::QUEUE);
        $this->assertSame('POST', $handler->requests[0]->getMethod());
        $this->assertSame('/api/v1/ephemeral/reset', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(['queue' => self::QUEUE], $this->body($handler->requests[0]));

        $eph->delete(self::QUEUE);
        $this->assertSame('DELETE', $handler->requests[1]->getMethod());
        $this->assertSame('/api/v1/ephemeral/queue/inbox', $handler->requests[1]->getUri()->getPath());
    }

    /**
     * A queue name with a slash must not become two path segments — that is a
     * different queue, or a 404, depending on the route.
     */
    public function testPercentEncodesAQueueNameThatWouldChangeThePath(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->delete('rooms/7');
        $this->assertSame('/api/v1/ephemeral/queue/rooms%2F7', $handler->requests[0]->getRequestTarget());

        $eph->depth('rooms/7');
        $this->assertSame('/api/v1/ephemeral/queues/rooms%2F7/depth', $handler->requests[1]->getRequestTarget());
    }

    public function testRefusesAMissingQueueNameBeforeSpendingARequest(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $calls = [
            fn() => $eph->configure(''),
            fn() => $eph->reset(''),
            fn() => $eph->delete(''),
            fn() => $eph->push('', [['a' => 1]]),
            fn() => $eph->pop(''),
            fn() => $eph->ack('', ['e:1']),
            fn() => $eph->depth(''),
        ];

        foreach ($calls as $call) {
            try {
                $call();
                $this->fail('an empty queue name must raise');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('non-empty', $error->getMessage());
            }
        }

        $this->assertSame(0, $handler->count());
    }

    // ===========================
    // Push
    // ===========================

    public function testPushSendsTheFlatEnvelopeWithPayloadOnlyMessages(): void
    {
        $handler = new PlanHandler([['status' => 201, 'json' => ['pushed' => 2]]]);
        $result = $this->queen($handler)->ephemeral()->push(self::QUEUE, [['a' => 1], ['a' => 2]]);

        $this->assertSame('/api/v1/ephemeral/push', $handler->requests[0]->getUri()->getPath());
        $this->assertSame([
            'queue' => self::QUEUE,
            'messages' => [['payload' => ['a' => 1]], ['payload' => ['a' => 2]]],
        ], $this->body($handler->requests[0]));
        $this->assertSame(['pushed' => 2], $result);
    }

    public function testPushOmitsPartitionUnlessTheCallerNamedOne(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->push(self::QUEUE, [['a' => 1]]);
        $this->assertArrayNotHasKey('partition', $this->body($handler->requests[0]));

        $eph->push(self::QUEUE, [['a' => 1]], ['partition' => 'room-7']);
        $this->assertSame([
            'queue' => self::QUEUE,
            'partition' => 'room-7',
            'messages' => [['payload' => ['a' => 1]]],
        ], $this->body($handler->requests[1]));
    }

    /**
     * A bare value, ['data' => …] or ['payload' => …] — one mental model across
     * both families, including the trap: an array with a `data` key is read as
     * the sugar and its other keys do not travel.
     */
    public function testPushAcceptsTheDurablePushSugar(): void
    {
        $handler = new PlanHandler();
        $this->queen($handler)->ephemeral()->push(self::QUEUE, [
            'plain',
            7,
            ['data' => ['n' => 1]],
            ['payload' => ['n' => 2]],
        ]);

        $this->assertSame([
            ['payload' => 'plain'],
            ['payload' => 7],
            ['payload' => ['n' => 1]],
            ['payload' => ['n' => 2]],
        ], $this->body($handler->requests[0])['messages']);
    }

    /** There is no dedup index on this engine to hold one (§9). */
    public function testPushCarriesNoTransactionId(): void
    {
        $handler = new PlanHandler();
        $this->queen($handler)->ephemeral()->push(self::QUEUE, [
            ['payload' => ['n' => 1], 'transactionId' => 't-1'],
        ]);

        $this->assertSame(['payload'], array_keys($this->body($handler->requests[0])['messages'][0]));
    }

    public function testPushOfNothingAnswersPushedZeroWithoutSpendingARequest(): void
    {
        $handler = new PlanHandler();
        $this->assertSame(['pushed' => 0], $this->queen($handler)->ephemeral()->push(self::QUEUE, []));
        $this->assertSame(0, $handler->count());
    }

    // ===========================
    // Pop
    // ===========================

    public function testPopSendsTheQueueAndNothingElseByDefault(): void
    {
        $handler = new PlanHandler([$this->popped([])]);
        $this->queen($handler)->ephemeral()->pop(self::QUEUE);

        $this->assertSame('/api/v1/ephemeral/pop', $handler->requests[0]->getUri()->getPath());
        $this->assertSame('queue=inbox', $handler->requests[0]->getUri()->getQuery());
    }

    /**
     * In §3.1's order, so a query read out of an access log is the query the
     * plan documents.
     */
    public function testPopPutsEveryDeclaredParameterOnTheQueryString(): void
    {
        $handler = new PlanHandler([$this->popped([])]);
        $this->queen($handler)->ephemeral()->pop(self::QUEUE, [
            'partition' => 'room-7',
            'batch' => 10,
            'wait' => true,
            'timeout' => 1500,
            'group' => 'workers',
            'autoAck' => true,
        ]);

        $this->assertSame(
            'queue=inbox&partition=room-7&batch=10&wait=true&timeout=1500&group=workers&autoAck=true',
            $handler->requests[0]->getUri()->getQuery()
        );
    }

    /**
     * And none when it does not: a plain pop leaves every default to the broker,
     * while a long poll states the deadline it is holding the socket open for.
     */
    public function testPopSendsAnExplicitTimeoutWheneverItWaits(): void
    {
        $handler = new PlanHandler([$this->popped([]), $this->popped([])]);
        $eph = $this->queen($handler)->ephemeral();

        $eph->pop(self::QUEUE, ['wait' => true]);
        $this->assertSame('queue=inbox&wait=true&timeout=30000', $handler->requests[0]->getUri()->getQuery());

        $eph->pop(self::QUEUE, ['batch' => 5]);
        $this->assertSame('queue=inbox&batch=5', $handler->requests[1]->getUri()->getQuery());
    }

    public function testPopAcceptsTimeoutMillisAndRefusesBothSpellings(): void
    {
        $handler = new PlanHandler([$this->popped([])]);
        $eph = $this->queen($handler)->ephemeral();

        $eph->pop(self::QUEUE, ['wait' => true, 'timeoutMillis' => 2500]);
        $this->assertStringContainsString('timeout=2500', $handler->requests[0]->getUri()->getQuery());

        $this->expectException(\InvalidArgumentException::class);
        $eph->pop(self::QUEUE, ['wait' => true, 'timeout' => 1, 'timeoutMillis' => 2]);
    }

    /**
     * An empty ARRAY, never null: an idle queue must not make `foreach
     * ($result['messages'] …)` a TypeError, and a 204 decodes to null in this
     * client.
     */
    public function testPopReturnsAnEmptyArrayOnATimeoutAndOnABodiless204(): void
    {
        $handler = new PlanHandler([
            $this->popped([]),
            ['status' => 204, 'json' => null],
        ]);
        $eph = $this->queen($handler)->ephemeral();

        $this->assertSame(['queue' => self::QUEUE, 'messages' => []], $eph->pop(self::QUEUE, ['wait' => true]));
        $this->assertSame(['queue' => self::QUEUE, 'messages' => []], $eph->pop(self::QUEUE));
    }

    // ===========================
    // Ack
    // ===========================

    public function testAckSendsTheIdsUnderAcksWithTheGroupBesideThem(): void
    {
        $handler = new PlanHandler();
        $this->queen($handler)->ephemeral()->ack(self::QUEUE, ['e:beef:Default:1'], ['group' => 'workers']);

        $this->assertSame('/api/v1/ephemeral/ack', $handler->requests[0]->getUri()->getPath());
        $this->assertSame([
            'queue' => self::QUEUE,
            'group' => 'workers',
            'acks' => [['id' => 'e:beef:Default:1']],
        ], $this->body($handler->requests[0]));
    }

    public function testAckTakesPoppedMessagesBareIdsOrTheWireArrays(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->ack(self::QUEUE, ['id' => 'e:beef:Default:9', 'payload' => ['n' => 9], 'attempts' => 0]);
        $this->assertSame([['id' => 'e:beef:Default:9']], $this->body($handler->requests[0])['acks']);

        $eph->ack(self::QUEUE, ['e:1', ['id' => 'e:2', 'status' => 'retry']]);
        $this->assertSame([
            ['id' => 'e:1'],
            ['id' => 'e:2', 'status' => 'retry'],
        ], $this->body($handler->requests[1])['acks']);
    }

    public function testAckMapsTheBooleanSugarAndLetsAPerMessageStatusWin(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->ack(self::QUEUE, ['e:1'], ['status' => false]);
        $this->assertSame([['id' => 'e:1', 'status' => 'failed']], $this->body($handler->requests[0])['acks']);

        $eph->ack(
            self::QUEUE,
            ['e:1', ['id' => 'e:2', 'status' => 'retry', 'error' => 'downstream 503']],
            ['status' => true]
        );
        $this->assertSame([
            ['id' => 'e:1', 'status' => 'completed'],
            ['id' => 'e:2', 'status' => 'retry', 'error' => 'downstream 503'],
        ], $this->body($handler->requests[1])['acks']);
    }

    public function testAckOmitsGroupInQueueModeAndRefusesAnAckWithNoId(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->ack(self::QUEUE, ['e:1']);
        $this->assertArrayNotHasKey('group', $this->body($handler->requests[0]));

        $this->expectException(\InvalidArgumentException::class);
        $eph->ack(self::QUEUE, [['payload' => ['n' => 1]]]);
    }

    public function testAckOfNothingAnswersEmptyResultsWithoutSpendingARequest(): void
    {
        $handler = new PlanHandler();
        $this->assertSame(['results' => []], $this->queen($handler)->ephemeral()->ack(self::QUEUE, []));
        $this->assertSame(0, $handler->count());
    }

    // ===========================
    // Status
    // ===========================

    public function testQueuesAndDepthArePlainGetsOnTheStatusRoutes(): void
    {
        $handler = new PlanHandler();
        $eph = $this->queen($handler)->ephemeral();

        $eph->queues();
        $this->assertSame('GET', $handler->requests[0]->getMethod());
        $this->assertSame('/api/v1/ephemeral/queues', $handler->requests[0]->getUri()->getPath());
        $this->assertSame('', $handler->requests[0]->getUri()->getQuery());

        $eph->depth(self::QUEUE);
        $this->assertSame('GET', $handler->requests[1]->getMethod());
        $this->assertSame('/api/v1/ephemeral/queues/inbox/depth', $handler->requests[1]->getUri()->getPath());
    }

    // ===========================
    // An old broker, or an old proxy: one verdict (§4, §8)
    // ===========================

    public function testMapsAMissingBrokerRouteToTheOneClearError(): void
    {
        $handler = new PlanHandler([['status' => 404, 'json' => ['error' => 'not_found']]]);

        try {
            $this->queen($handler)->ephemeral()->push(self::QUEUE, [['a' => 1]]);
            $this->fail('a 404 on this family must raise');
        } catch (EphemeralUnsupportedException $error) {
            $this->assertSame(EphemeralUnsupportedException::MESSAGE, $error->getMessage());
            $this->assertSame(404, $error->statusCode);
            $this->assertSame(ErrorCode::EPHEMERAL_UNSUPPORTED, $error->errorCode);
        }
    }

    /**
     * The old PROXY's 404 is the same verdict wearing different words, and the
     * original is kept as the previous exception: "the proxy answered
     * route_blocked" is the evidence for "upgrade", and an SDK that threw it
     * away would leave the operator with a claim and no proof.
     */
    public function testMapsTheOldProxyRouteBlockedToTheSameError(): void
    {
        $handler = new PlanHandler([
            ['status' => 404, 'json' => ['error' => 'route_blocked', 'code' => 'route_blocked']],
        ]);

        try {
            $this->queen($handler)->ephemeral()->pop(self::QUEUE);
            $this->fail('a 404 on this family must raise');
        } catch (EphemeralUnsupportedException $error) {
            $previous = $error->getPrevious();
            $this->assertInstanceOf(HttpException::class, $previous);
            $this->assertSame('route_blocked', $previous->errorCode);
        }
    }

    /**
     * Eight verbs, one verdict. A family where six routes say "upgrade" and two
     * say "HTTP 404" is a family somebody will branch on by accident.
     */
    public function testEveryVerbOfTheFamilyMapsThe404(): void
    {
        $handler = new PlanHandler([], ['status' => 404, 'json' => ['error' => 'not_found']]);
        $eph = $this->queen($handler)->ephemeral();

        $calls = [
            fn() => $eph->configure(self::QUEUE),
            fn() => $eph->reset(self::QUEUE),
            fn() => $eph->delete(self::QUEUE),
            fn() => $eph->push(self::QUEUE, [['a' => 1]]),
            fn() => $eph->pop(self::QUEUE),
            fn() => $eph->ack(self::QUEUE, ['e:1']),
            fn() => $eph->queues(),
            fn() => $eph->depth(self::QUEUE),
        ];

        foreach ($calls as $call) {
            try {
                $call();
                $this->fail('a 404 on this family must raise');
            } catch (EphemeralUnsupportedException $error) {
                $this->assertSame(ErrorCode::EPHEMERAL_UNSUPPORTED, $error->errorCode);
            }
        }

        $this->assertSame(8, $handler->count());
    }

    /**
     * 429 `queue_full` is the per-queue bound doing its job (§1.6) and 403 is
     * the grant; neither is a version verdict and neither may be dressed up as
     * one.
     */
    public function testLeavesEveryOtherRefusalAloneCodeAndStatusIntact(): void
    {
        $handler = new PlanHandler([
            ['status' => 403, 'json' => ['error' => 'not granted', 'code' => 'feature_gated']],
        ]);

        try {
            $this->queen($handler)->ephemeral()->push(self::QUEUE, [['a' => 1]]);
            $this->fail('a 403 must raise');
        } catch (HttpException $error) {
            $this->assertNotInstanceOf(EphemeralUnsupportedException::class, $error);
            $this->assertSame(403, $error->statusCode);
            $this->assertSame(ErrorCode::FEATURE_GATED, $error->errorCode);
        }
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

    private function popped(array $messages): array
    {
        return ['status' => 200, 'json' => ['queue' => self::QUEUE, 'messages' => $messages]];
    }
}
