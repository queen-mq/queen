<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Exceptions\ConflationUnsupportedException;
use Queen\Laravel\Commands\ConsumeCommand;
use Queen\Queen;
use Queen\Support\ConflationGuard;
use Queen\Support\Defaults;
use Queen\Tests\Support\PlanHandler;

/**
 * PLAN_CONFLATION §4, the PHP row — last-value delivery as a consumer-group
 * option, and the SDK half of the contract that keeps it from failing silently.
 *
 * Three things are pinned here, because three things can independently rot:
 *
 *  1. `conflation` reaches the WIRE from every one of this SDK's four pop
 *     surfaces. PHP has THREE param builders — ConsumerManager::buildParams, a
 *     byte-identical copy in HighLevelConsumer::buildParams, and the inline one
 *     in QueueBuilder::pop — plus the async round in
 *     ConsumerManager::concurrentWorkers. An option added to one and forgotten
 *     in the others is the standing hazard in this family of clients.
 *  2. DEGRADE LOUDLY (§4). No SDK negotiates capabilities, so an old broker
 *     silently ignores `conflation=true` and answers with the whole backlog.
 *     A conflating response therefore echoes `"conflation":true` — on EMPTY
 *     responses too — and a missing echo is an ERROR on the first round trip,
 *     before a single message is handled.
 *  3. The declaration conflict (§3.3) is a WARNING, not an error: the stored
 *     group policy wins, both consumers keep working, and the disagreeing one
 *     says so exactly ONCE per (queue, group) per process.
 */
class ConflationTest extends TestCase
{
    protected function setUp(): void
    {
        // The "warn once per (queue, group)" ledger is per PROCESS, and a test
        // run is one process: without this every test after the first would see
        // an already-warned pair and assert nothing.
        ConflationGuard::reset();
    }

    protected function tearDown(): void
    {
        ConflationGuard::reset();
    }

    // ===========================
    // 1. The option reaches the wire
    // ===========================

    public function testConflationIsOffByDefault(): void
    {
        $this->assertArrayHasKey('conflation', Defaults::CONSUME_DEFAULTS);
        $this->assertFalse(Defaults::CONSUME_DEFAULTS['conflation']);
    }

    public function testBuilderSetterIsFluent(): void
    {
        $builder = (new Queen('http://queen.test'))->queue('orders');
        $this->assertSame($builder, $builder->conflation(true));
    }

    /** Builder #1: the inline param block in QueueBuilder::pop. */
    public function testPopSendsConflationOnTheWire(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $messages = $queen->queue('orders')->group('workers')->conflation(true)->pop();

        $this->assertCount(1, $messages);
        $this->assertSame('true', self::query($handler->requests[0])['conflation'] ?? null);
    }

    public function testPopOmitsConflationWhenItWasNotAskedFor(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false)));
        $queen = $this->queen($handler);

        $queen->queue('orders')->group('workers')->pop();

        $this->assertArrayNotHasKey('conflation', self::query($handler->requests[0]));
    }

    /** Builder #2: ConsumerManager::buildParams, the callback consume loop. */
    public function testConsumeSendsConflationOnTheWire(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $handled = 0;
        $queen->queue('orders')->group('workers')
            ->conflation(true)->autoAck(false)->limit(1)
            ->consume(function (array $messages) use (&$handled): void {
                $handled += count($messages);
            })
            ->execute();

        $this->assertSame(1, $handled);
        $this->assertSame('true', self::query($handler->requests[0])['conflation'] ?? null);
    }

    public function testConsumeOmitsConflationWhenItWasNotAskedFor(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false)));
        $queen = $this->queen($handler);

        $queen->queue('orders')->group('workers')->autoAck(false)->limit(1)
            ->consume(function (): void {})
            ->execute();

        $this->assertArrayNotHasKey('conflation', self::query($handler->requests[0]));
    }

    /** The async round in ConsumerManager::concurrentWorkers is a fourth site. */
    public function testConcurrentConsumeSendsConflationOnTheWire(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $queen->queue('orders')->group('workers')
            ->conflation(true)->autoAck(false)->concurrency(2)->limit(2)
            ->consume(function (): void {})
            ->execute();

        $this->assertGreaterThanOrEqual(1, $handler->count());
        $this->assertSame('true', self::query($handler->requests[0])['conflation'] ?? null);
    }

    /** Builder #3: HighLevelConsumer::buildParams, the rdkafka-style surface. */
    public function testHighLevelConsumerSendsConflationOnTheWire(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->conflation(true)->getConsumer();
        $consumer->subscribe();
        $message = $consumer->consume(50);

        $this->assertNotNull($message);
        $this->assertSame('true', self::query($handler->requests[0])['conflation'] ?? null);
    }

    public function testHighLevelConsumerBatchSendsConflationOnTheWire(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(messages: 2)));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->conflation(true)->getConsumer();
        $consumer->subscribe();
        $messages = $consumer->consumeBatch(50, 10);

        $this->assertCount(2, $messages);
        $this->assertSame('true', self::query($handler->requests[0])['conflation'] ?? null);
    }

    public function testConsumeCommandExposesTheFlag(): void
    {
        $definition = (new ConsumeCommand())->getDefinition();

        $this->assertTrue($definition->hasOption('conflation'));
    }

    // ===========================
    // 2. Degrade loudly (§4)
    // ===========================

    public function testPopRaisesWhenTheBrokerDoesNotEchoConflation(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, messages: 3)));
        $queen = $this->queen($handler);

        $this->expectException(ConflationUnsupportedException::class);
        $this->expectExceptionMessage('requires broker >= 1.1.0');

        $queen->queue('orders')->group('workers')->conflation(true)->pop();
    }

    /**
     * The old-broker case that matters most: an EMPTY pop. A pre-1.1.0 broker
     * answers a bodiless 204, which the HTTP layer turns into null — no echo,
     * no messages, nothing to notice. The check has to fire there too, or an
     * idle consumer silently "works" until the first backlog arrives.
     */
    public function testBodiless204FromAnOldBrokerStillRaises(): void
    {
        $handler = new PlanHandler([], ['status' => 204, 'json' => null]);
        $queen = $this->queen($handler);

        $this->expectException(ConflationUnsupportedException::class);

        $queen->queue('orders')->group('workers')->conflation(true)->pop();
    }

    public function testConsumeLoopStopsOnTheFirstResponseWithoutTheEcho(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, messages: 5)));
        $queen = $this->queen($handler);

        $handled = 0;

        try {
            $queen->queue('orders')->group('workers')
                ->conflation(true)->autoAck(false)->limit(1000)
                ->consume(function () use (&$handled): void {
                    $handled++;
                })
                ->execute();
            $this->fail('the consume loop must not survive a broker that ignored conflation');
        } catch (ConflationUnsupportedException $error) {
            $this->assertStringContainsString('requires broker >= 1.1.0', $error->getMessage());
        }

        $this->assertSame(0, $handled, 'the loop must stop BEFORE any message is processed');
        $this->assertSame(1, $handler->count(), 'it must stop on the FIRST such response');
    }

    public function testConcurrentConsumeLoopStopsOnTheFirstResponseWithoutTheEcho(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false)));
        $queen = $this->queen($handler);

        $handled = 0;

        $this->expectException(ConflationUnsupportedException::class);

        try {
            $queen->queue('orders')->group('workers')
                ->conflation(true)->autoAck(false)->concurrency(2)->limit(1000)
                ->consume(function () use (&$handled): void {
                    $handled++;
                })
                ->execute();
        } finally {
            $this->assertSame(0, $handled);
        }
    }

    public function testHighLevelConsumerRaisesWhenTheBrokerDoesNotEchoConflation(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false)));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->conflation(true)->getConsumer();
        $consumer->subscribe();

        $this->expectException(ConflationUnsupportedException::class);
        $consumer->consume(50);
    }

    public function testHighLevelConsumerBatchRaisesWhenTheBrokerDoesNotEchoConflation(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, messages: 2)));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->conflation(true)->getConsumer();
        $consumer->subscribe();

        $this->expectException(ConflationUnsupportedException::class);
        $consumer->consumeBatch(50, 10);
    }

    /**
     * The mirror case is a non-event: an SDK that never asked for conflation
     * must behave byte-identically to today against any broker.
     */
    public function testAConsumerThatNeverAskedIsNeverAffected(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, messages: 2)));
        $queen = $this->queen($handler);

        $messages = [];
        $warnings = $this->captureWarnings(function () use ($queen, &$messages): void {
            $messages = $queen->queue('orders')->group('workers')->pop();
        });

        $this->assertCount(2, $messages);
        $this->assertSame([], $warnings);
    }

    // ===========================
    // 3. Declaration conflict (§3.3) — warn ONCE, never error
    // ===========================

    public function testConflictWarnsExactlyOncePerQueueAndGroup(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, conflict: true)));
        $queen = $this->queen($handler);

        $served = 0;
        $warnings = $this->captureWarnings(function () use ($queen, &$served): void {
            for ($i = 0; $i < 3; $i++) {
                $served += count($queen->queue('orders')->group('workers')->conflation(true)->pop());
            }
        });

        $this->assertSame(3, $served, 'a conflict must not stop the consumer');
        $this->assertCount(1, $warnings);
        $this->assertStringContainsString('orders', $warnings[0]);
        $this->assertStringContainsString('workers', $warnings[0]);
    }

    public function testConflictOnADifferentGroupWarnsAgain(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, conflict: true)));
        $queen = $this->queen($handler);

        $warnings = $this->captureWarnings(function () use ($queen): void {
            $queen->queue('orders')->group('workers')->conflation(true)->pop();
            $queen->queue('orders')->group('audit')->conflation(true)->pop();
            $queen->queue('orders')->group('workers')->conflation(true)->pop();
        });

        $this->assertCount(2, $warnings);
    }

    public function testOneWarningCoversEveryPopSurfaceForTheSamePair(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, conflict: true)));
        $queen = $this->queen($handler);

        $warnings = $this->captureWarnings(function () use ($queen): void {
            $queen->queue('orders')->group('workers')->conflation(true)->pop();

            $consumer = $queen->queue('orders')->group('workers')->conflation(true)->getConsumer();
            $consumer->subscribe();
            $consumer->consume(50);

            $queen->queue('orders')->group('workers')
                ->conflation(true)->autoAck(false)->limit(1)
                ->consume(function (): void {})
                ->execute();
        });

        $this->assertCount(1, $warnings);
    }

    /**
     * The other half of §3.3: the group's stored policy is conflation=ON and
     * THIS consumer did not ask for it. The broker applies conflation anyway
     * and flags the disagreement — the consumer is being served last-value
     * delivery it never declared, which is exactly what it needs told.
     */
    public function testConflictIsReportedEvenWhenThisConsumerDidNotAskForConflation(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: true, conflict: true)));
        $queen = $this->queen($handler);

        $messages = [];
        $warnings = $this->captureWarnings(function () use ($queen, &$messages): void {
            $messages = $queen->queue('orders')->group('workers')->pop();
        });

        $this->assertCount(1, $messages);
        $this->assertCount(1, $warnings);
    }

    /**
     * A conflict proves the broker UNDERSTOOD the flag and declined it, so it
     * must never be mistaken for an old broker: warn, do not raise.
     */
    public function testConflictIsNotTreatedAsAnOldBroker(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(conflation: false, conflict: true)));
        $queen = $this->queen($handler);

        $handled = 0;
        $warnings = $this->captureWarnings(function () use ($queen, &$handled): void {
            $queen->queue('orders')->group('workers')
                ->conflation(true)->autoAck(false)->limit(1)
                ->consume(function (array $messages) use (&$handled): void {
                    $handled += count($messages);
                })
                ->execute();
        });

        $this->assertSame(1, $handled);
        $this->assertCount(1, $warnings);
    }

    // ===========================
    // 4. Depth fields (§5.3)
    // ===========================

    /**
     * `pending` is LOG depth, `effectivePending` is WORK depth: a conflating
     * queue at pending 4,000,000 / effectivePending 12 is healthy, while the
     * same numbers on a non-conflating group are an incident. The PHP admin
     * surface returns the decoded body as-is, so what is pinned here is that
     * the new fields survive the round trip untouched.
     */
    public function testQueueDepthCarriesTheConflationFields(): void
    {
        $handler = new PlanHandler([], self::ok([
            'queue' => 'orders',
            'group' => 'workers',
            'pending' => 4000000,
            'partitionsPending' => 12,
            'conflation' => true,
            'effectivePending' => 12,
            'partitions' => [['partition' => 'k-1', 'pending' => 900]],
        ]));
        $queen = $this->queen($handler);

        $depth = $queen->admin()->getQueueDepth('orders', 'workers');

        $this->assertSame(12, $depth['partitionsPending']);
        $this->assertTrue($depth['conflation']);
        $this->assertSame(12, $depth['effectivePending']);
        $this->assertSame(4000000, $depth['pending']);
        $this->assertSame('workers', self::query($handler->requests[0])['group'] ?? null);
    }

    // ===========================
    // Fixtures
    // ===========================

    /**
     * Run $action with the E_USER_WARNING stream captured, and return what it
     * raised. Scoped to the action rather than the whole test on purpose:
     * PHPUnit installs its own error handler around each test and objects,
     * rightly, to a handler still sitting on top of it when the test ends.
     *
     * @return string[]
     */
    private function captureWarnings(callable $action): array
    {
        $warnings = [];

        set_error_handler(function (int $errno, string $message) use (&$warnings): bool {
            if ($errno === E_USER_WARNING) {
                $warnings[] = $message;
                return true;
            }
            return false;
        });

        try {
            $action();
        } finally {
            restore_error_handler();
        }

        return $warnings;
    }

    private function queen(PlanHandler $handler): Queen
    {
        return new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);
    }

    private static function ok(array $json): array
    {
        return ['status' => 200, 'json' => $json];
    }

    /**
     * A pop response in the broker's shape. `conflation` and `conflationConflict`
     * are emitted only when true — that is the server rule, and it is why their
     * ABSENCE is what an SDK has to reason about.
     */
    private static function popBody(bool $conflation = true, bool $conflict = false, int $messages = 1): array
    {
        $body = [
            'messages' => array_map(fn(int $i): array => [
                'transactionId' => "tx-{$i}",
                'partitionId' => 'p1',
                'queue' => 'orders',
                'partition' => 'Default',
                'data' => ['n' => $i],
                'leaseId' => 'lease-1',
            ], $messages > 0 ? range(1, $messages) : []),
            'partitionsClaimed' => 1,
        ];

        if ($conflation) {
            $body['conflation'] = true;
        }
        if ($conflict) {
            $body['conflationConflict'] = true;
        }

        return $body;
    }

    /** @return array<string, string> */
    private static function query(RequestInterface $request): array
    {
        $parsed = [];
        parse_str($request->getUri()->getQuery(), $parsed);
        return $parsed;
    }
}
