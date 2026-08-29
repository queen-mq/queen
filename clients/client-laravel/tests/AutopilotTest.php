<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Queen;
use Queen\Support\Defaults;
use Queen\Support\PopAutopilot;
use Queen\Tests\Support\PlanHandler;

/**
 * Pop autopilot, the PHP row — the broker sizes the knobs the caller did not.
 *
 * The four things a client can be wrong about here, and why each is asserted
 * against the WHOLE query string rather than against one parameter:
 *
 *  1. EVERY BUILDER MUST AGREE. PHP has THREE pop param builders — the inline
 *     one in QueueBuilder::pop, ConsumerManager::buildParams, and a
 *     byte-identical copy in HighLevelConsumer::buildParams — which is the
 *     standing hazard in this family of clients and the reason the emission rule
 *     lives in one class. Every case below is run through all three, and all
 *     three are compared to the same expected string.
 *  2. NOT ENGAGING AUTOPILOT MUST BE BYTE-IDENTICAL TO THE OLD SDK. The escape
 *     hatch is only worth having if it is exact, and "exact" is not something a
 *     test of one parameter can show: a stray autopilot=true, or a batch that
 *     stopped being emitted, is a different request. Hence full-string equality
 *     including the parameters this feature never touches, and including their
 *     ORDER — this SDK builds an array and http_build_query's it, so order is
 *     part of the bytes.
 *  3. AN EXPLICIT VALUE IS SACRED, PER DIMENSION. partitions(1) and "never
 *     called partitions" both used to reach the wire as nothing at all; they are
 *     now different requests, and the pinned one must survive autopilot.
 *  4. THE ADDITIVE RESPONSE FIELD MUST NOT BE LOAD-BEARING. A broker that does
 *     not send it, sends it half-filled, or sends it with fields this SDK has
 *     never heard of, all have to work.
 */
class AutopilotTest extends TestCase
{
    /**
     * The shared spine of every case: a named queue and group, no long poll,
     * default timeout. Everything that varies below is sizing.
     */
    private const TAIL = 'wait=false&timeout=30000&consumerGroup=workers';

    protected function tearDown(): void
    {
        putenv(PopAutopilot::ENV_VAR);
        unset($_ENV[PopAutopilot::ENV_VAR], $_SERVER[PopAutopilot::ENV_VAR]);
    }

    /**
     * @return array<string, array{\Closure(\Queen\Builders\QueueBuilder): \Queen\Builders\QueueBuilder, string}>
     */
    public static function sizingCases(): array
    {
        $tail = self::TAIL;

        return [
            // Nothing set: both knobs go to the broker, neither travels.
            'nothing set' => [fn($b) => $b, "autopilot=true&{$tail}"],
            // A pinned width travels, the batch is delegated.
            'partitions only' => [fn($b) => $b->partitions(4), "autopilot=true&{$tail}&partitions=4"],
            // The pin that used to be indistinguishable from unset. partitions(1)
            // is a decision — hold this consumer to one partition — and the
            // broker has to be told, or autopilot would widen it.
            'partitions pinned to one' => [fn($b) => $b->partitions(1), "autopilot=true&{$tail}&partitions=1"],
            // A pinned batch travels, the sweep width is delegated.
            'batch only' => [fn($b) => $b->batch(50), "autopilot=true&batch=50&{$tail}"],
            // Both set: nothing left to decide, so no autopilot parameter and the
            // exact request the pre-autopilot SDK sent.
            'both set' => [fn($b) => $b->batch(50)->partitions(4), "batch=50&{$tail}&partitions=4"],
            // ...including the one where the old SDK never emitted partitions=1.
            'both set, partitions one' => [fn($b) => $b->batch(50)->partitions(1), "batch=50&{$tail}"],
            // The escape hatch: the client-side defaults are back.
            'autopilot off, nothing set' => [fn($b) => $b->autopilot(false), "batch=1&{$tail}"],
            'autopilot off, partitions one' => [fn($b) => $b->autopilot(false)->partitions(1), "batch=1&{$tail}"],
            'autopilot off, both set' => [
                fn($b) => $b->autopilot(false)->batch(50)->partitions(4),
                "batch=50&{$tail}&partitions=4",
            ],
            // autopilot(true) is the default spelled out: it must change nothing.
            'autopilot on, both set' => [
                fn($b) => $b->autopilot(true)->batch(50)->partitions(4),
                "batch=50&{$tail}&partitions=4",
            ],
            // batch(0) is not "a batch of zero" and never was: it is the absence
            // of an opinion, which now means the broker decides.
            'batch zero is unset' => [fn($b) => $b->batch(0), "autopilot=true&{$tail}"],
        ];
    }

    // ===========================
    // 1. Param assembly, from all three builders
    // ===========================

    /** Builder #1: the inline param block in QueueBuilder::pop. */
    #[\PHPUnit\Framework\Attributes\DataProvider('sizingCases')]
    public function testPopParamAssembly(\Closure $build, string $want): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $build($queen->queue('orders')->group('workers')->wait(false))->pop();

        $this->assertSame($want, self::rawQuery($handler->requests[0]));
    }

    /** Builder #2: ConsumerManager::buildParams, the callback consume loop. */
    #[\PHPUnit\Framework\Attributes\DataProvider('sizingCases')]
    public function testConsumeParamAssembly(\Closure $build, string $want): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $handled = 0;
        $build($queen->queue('orders')->group('workers')->wait(false))
            ->autoAck(false)->limit(1)
            ->consume(function (array $messages) use (&$handled): void {
                $handled += count($messages);
            })
            ->execute();

        $this->assertSame(1, $handled);
        $this->assertSame($want, self::rawQuery($handler->requests[0]));
    }

    /**
     * Builder #3: HighLevelConsumer::buildParams, the rdkafka-style surface.
     *
     * It gets its own cases because it pins the BATCH itself, per call, and
     * always did: consume() hands back exactly one message, so claiming more
     * would leave the rest leased and undelivered, and consumeBatch's
     * $maxMessages IS the budget being asked for. So on this surface the batch
     * is never the broker's; the sweep width still is, unless pinned.
     *
     * That is also why this consumer rebuilds its query per call instead of
     * patching `batch` into a rendered string, which is what it used to do:
     * a patched string would have carried `autopilot=true` next to two pinned
     * knobs — a request asking the broker to decide nothing.
     */
    public function testHighLevelConsumerPinsTheBatchAndDelegatesOnlyTheWidth(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->getConsumer();
        $consumer->subscribe();
        $consumer->consume(50);

        $this->assertSame(
            'autopilot=true&batch=1&wait=true&timeout=50&consumerGroup=workers',
            self::rawQuery($handler->requests[0])
        );
    }

    public function testHighLevelConsumerWithBothKnobsPinnedSendsNoFlag(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->partitions(4)->getConsumer();
        $consumer->subscribe();
        $consumer->consume(50);

        $this->assertSame(
            'batch=1&wait=true&timeout=50&consumerGroup=workers&partitions=4',
            self::rawQuery($handler->requests[0]),
            'the surface pinned the batch and the caller pinned the width: nothing left to decide'
        );
    }

    public function testHighLevelConsumerBatchPinsMaxMessages(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody(messages: 2)));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->getConsumer();
        $consumer->subscribe();
        $consumer->consumeBatch(50, 10);

        $this->assertSame(
            'autopilot=true&batch=10&wait=true&timeout=50&consumerGroup=workers',
            self::rawQuery($handler->requests[0])
        );
    }

    public function testHighLevelConsumerOffIsTheOldRequestByteForByte(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $consumer = $queen->queue('orders')->group('workers')->autopilot(false)->getConsumer();
        $consumer->subscribe();
        $consumer->consume(50);

        $this->assertSame(
            'batch=1&wait=true&timeout=50&consumerGroup=workers',
            self::rawQuery($handler->requests[0])
        );
    }

    // ===========================
    // 2. The process-wide rollback
    // ===========================

    public function testEnvVarDisablesAutopilot(): void
    {
        putenv(PopAutopilot::ENV_VAR . '=off');
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $queen->queue('orders')->group('workers')->wait(false)->pop();

        $this->assertSame('batch=1&' . self::TAIL, self::rawQuery($handler->requests[0]));
    }

    public function testEnvVarIsReadOnceAtConstruction(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        // Set AFTER the client exists: this is a deployment-level rollback, not
        // a per-request switch, so a live client must not move.
        putenv(PopAutopilot::ENV_VAR . '=off');
        $queen->queue('orders')->group('workers')->wait(false)->pop();

        $this->assertSame('autopilot=true&' . self::TAIL, self::rawQuery($handler->requests[0]));
    }

    public function testExplicitAutopilotOutranksTheEnvironment(): void
    {
        putenv(PopAutopilot::ENV_VAR . '=off');
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $queen->queue('orders')->group('workers')->wait(false)->autopilot(true)->pop();

        $this->assertSame('autopilot=true&' . self::TAIL, self::rawQuery($handler->requests[0]));
    }

    public function testEnvVarVocabulary(): void
    {
        foreach (['off', 'OFF', ' off ', 'false', '0', 'no', 'disabled'] as $value) {
            putenv(PopAutopilot::ENV_VAR . '=' . $value);
            $this->assertTrue(PopAutopilot::disabledByEnv(), "{$value} should disable autopilot");
        }
        foreach (['', 'on', 'true', '1', 'yes', 'nonsense'] as $value) {
            putenv(PopAutopilot::ENV_VAR . '=' . $value);
            $this->assertFalse(PopAutopilot::disabledByEnv(), "{$value} should leave autopilot on");
        }
        putenv(PopAutopilot::ENV_VAR);
        $this->assertFalse(PopAutopilot::disabledByEnv(), 'unset leaves autopilot on');
    }

    /** php-fpm pools and Laravel's .env land in the superglobals, not in getenv(). */
    public function testEnvVarIsAlsoReadFromTheSuperglobals(): void
    {
        $_ENV[PopAutopilot::ENV_VAR] = 'off';
        $this->assertTrue(PopAutopilot::disabledByEnv());
        unset($_ENV[PopAutopilot::ENV_VAR]);

        $_SERVER[PopAutopilot::ENV_VAR] = 'off';
        $this->assertTrue(PopAutopilot::disabledByEnv());
        unset($_SERVER[PopAutopilot::ENV_VAR]);
    }

    // ===========================
    // 3. The additive response field
    // ===========================

    public function testDecisionParsesWhatTheBrokerChose(): void
    {
        $this->assertNull(PopAutopilot::decision(null));
        $this->assertNull(PopAutopilot::decision(['messages' => []]), 'absent');
        $this->assertNull(PopAutopilot::decision(['autopilot' => null]), 'null');
        $this->assertNull(PopAutopilot::decision(['autopilot' => true]), 'not an object');

        $this->assertSame(
            ['partitions' => 8, 'batch' => 200, 'waitMillis' => 25],
            PopAutopilot::decision(['autopilot' => ['partitions' => 8, 'batch' => 200, 'waitMs' => 25]])
        );

        // waitMs is optional: the broker sends it only when it has an opinion.
        $this->assertSame(
            ['partitions' => 4, 'batch' => 64, 'waitMillis' => 0],
            PopAutopilot::decision(['autopilot' => ['partitions' => 4, 'batch' => 64]])
        );

        // Forward compatibility: a newer broker growing a field must not cost
        // this client the fields it does understand.
        $this->assertSame(
            ['partitions' => 2, 'batch' => 10, 'waitMillis' => 5],
            PopAutopilot::decision([
                'autopilot' => ['partitions' => 2, 'batch' => 10, 'waitMs' => 5, 'reason' => 'ready_age'],
            ])
        );

        // A field of the wrong type is dropped, not fatal.
        $this->assertSame(
            ['partitions' => 0, 'batch' => 10, 'waitMillis' => 0],
            PopAutopilot::decision(['autopilot' => ['partitions' => 'eight', 'batch' => 10]])
        );
    }

    public function testPopResultReportsWhatTheBrokerChose(): void
    {
        $body = self::popBody();
        $body['autopilot'] = ['partitions' => 8, 'batch' => 200, 'waitMs' => 25];
        $handler = new PlanHandler([], self::ok($body));
        $queen = $this->queen($handler);

        $res = $queen->queue('orders')->group('workers')->wait(false)->popResult();

        $this->assertCount(1, $res['messages']);
        $this->assertSame(['partitions' => 8, 'batch' => 200, 'waitMillis' => 25], $res['autopilot']);
    }

    public function testPopResultIsNullWhenTheBrokerSaidNothing(): void
    {
        $handler = new PlanHandler([], self::ok(self::popBody()));
        $queen = $this->queen($handler);

        $res = $queen->queue('orders')->group('workers')->wait(false)->popResult();

        $this->assertCount(1, $res['messages']);
        $this->assertNull($res['autopilot'], 'a broker older than 1.2 is not an error');
    }

    public function testPopStillReturnsABareMessageList(): void
    {
        $body = self::popBody();
        $body['autopilot'] = ['partitions' => 8, 'batch' => 200];
        $handler = new PlanHandler([], self::ok($body));
        $queen = $this->queen($handler);

        $messages = $queen->queue('orders')->group('workers')->wait(false)->pop();

        $this->assertCount(1, $messages);
        $this->assertArrayHasKey('transactionId', reset($messages));
    }

    // ===========================
    // 4. Empty-poll pacing
    // ===========================

    public function testEmptyPollDelayHonoursTheAdviceAndFallsBack(): void
    {
        $this->assertSame(
            PopAutopilot::EMPTY_POLL_BACKOFF_MICROS,
            PopAutopilot::emptyPollDelayMicros(null)
        );
        $this->assertSame(
            PopAutopilot::EMPTY_POLL_BACKOFF_MICROS,
            PopAutopilot::emptyPollDelayMicros(['partitions' => 1, 'batch' => 1, 'waitMillis' => 0])
        );
        $this->assertSame(
            250_000,
            PopAutopilot::emptyPollDelayMicros(['partitions' => 1, 'batch' => 1, 'waitMillis' => 250])
        );
    }

    // ===========================
    // 5. The rule itself, in isolation
    // ===========================

    public function testSizingLeavesAPinnedDimensionAlone(): void
    {
        $this->assertSame(
            ['autopilot' => true, 'batch' => null, 'partitions' => null],
            PopAutopilot::sizing(null, null, 1, true)
        );
        $this->assertSame(
            ['autopilot' => true, 'batch' => '50', 'partitions' => null],
            PopAutopilot::sizing(50, null, 1, true)
        );
        $this->assertSame(
            ['autopilot' => true, 'batch' => null, 'partitions' => '1'],
            PopAutopilot::sizing(null, 1, 1, true)
        );
        // Both set: nothing to decide, so the flag does not travel either.
        $this->assertSame(
            ['autopilot' => false, 'batch' => '50', 'partitions' => '4'],
            PopAutopilot::sizing(50, 4, 1, true)
        );
        // Off: the client-side default comes back and partitions keeps its >1 gate.
        $this->assertSame(
            ['autopilot' => false, 'batch' => '1', 'partitions' => null],
            PopAutopilot::sizing(null, null, 1, false)
        );
        $this->assertSame(
            ['autopilot' => false, 'batch' => '1', 'partitions' => null],
            PopAutopilot::sizing(null, 1, 1, false)
        );
    }

    public function testBuilderSetterIsFluent(): void
    {
        $builder = (new Queen('http://queen.test'))->queue('orders');
        $this->assertSame($builder, $builder->autopilot(false));
    }

    /**
     * The queen:consume command: a knob the operator TYPED is a pin, a knob left
     * out is the broker's. A --batch default of 1 would have pinned a dimension
     * on every run without anyone asking, which is the feature switched off by
     * accident.
     */
    public function testConsumeCommandLeavesTheSizingKnobsUnsetByDefault(): void
    {
        $definition = (new \Queen\Laravel\Commands\ConsumeCommand())->getDefinition();

        $this->assertTrue($definition->hasOption('batch'));
        $this->assertNull($definition->getOption('batch')->getDefault());
        $this->assertTrue($definition->hasOption('partitions'));
        $this->assertNull($definition->getOption('partitions')->getDefault());
        $this->assertTrue($definition->hasOption('no-autopilot'));
    }

    public function testConsumeDefaultsStillCarryTheAutopilotOffValues(): void
    {
        // They are no longer applied when autopilot is on, but they are still the
        // numbers .autopilot(false) restores, and the table is what documents it.
        $this->assertSame(1, Defaults::CONSUME_DEFAULTS['batch']);
        $this->assertSame(1, Defaults::CONSUME_DEFAULTS['maxPartitions']);
        $this->assertSame(1, Defaults::POP_DEFAULTS['batch']);
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

    private static function ok(array $json): array
    {
        return ['status' => 200, 'json' => $json];
    }

    private static function popBody(int $messages = 1): array
    {
        return [
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
    }

    /** The query string exactly as it left the client, order included. */
    private static function rawQuery(RequestInterface $request): string
    {
        return $request->getUri()->getQuery();
    }
}
