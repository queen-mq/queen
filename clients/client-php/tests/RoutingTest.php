<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Exceptions\HttpException;
use Queen\Exceptions\RoutingUnsupportedException;
use Queen\Queen;
use Queen\Support\Defaults;
use Queen\Support\TimerOp;
use Queen\Support\Where;
use Queen\Tests\Support\PlanHandler;

/**
 * Routing without copies (PLAN_ROUTING.md), as this client speaks it:
 * headers on every push and every read, the `where` of a filtered consumer
 * group, the options of a reading queue, and `link` in a transaction.
 * Asserted on the requests a scripted handler receives; no broker.
 *
 * What is at stake besides the shapes: a client that uses none of it sends
 * the requests it always sent, byte for byte.
 */
final class RoutingTest extends TestCase
{
    /** A message as a pop returns it. */
    private const POPPED = [
        'id' => '0190aaaa-0000-7000-8000-000000000001',
        'transactionId' => 'order-1-paid',
        'traceId' => null,
        'data' => ['total' => 9000],
        'producerSub' => null,
        'createdAt' => '2026-11-12T10:00:00.000Z',
        'partitionId' => '123',
        'partition' => 'order-1',
        'leaseId' => 'lease-1',
        'consumerGroup' => 'review',
        'deliveryAttempt' => 1,
        'offset' => 42,
    ];

    private function queen(PlanHandler $handler): Queen
    {
        return new Queen(['url' => 'http://queen.test', 'retryAttempts' => 1, 'handler' => HandlerStack::create($handler)]);
    }

    private static function body(RequestInterface $request): array
    {
        return json_decode((string) $request->getBody(), true, 512, JSON_THROW_ON_ERROR);
    }

    private static function query(RequestInterface $request): array
    {
        parse_str($request->getUri()->getQuery(), $query);

        return $query;
    }

    private static function queued(int $count = 1): array
    {
        return ['status' => 201, 'json' => array_fill(0, $count, ['status' => 'queued'])];
    }

    private static function empty(): array
    {
        // An empty pop: a bodiless 204.
        return ['status' => 204, 'body' => ''];
    }

    /** A filtered group's empty pop: a 200 that says it filtered. */
    private static function filteredEmpty(): array
    {
        return ['status' => 200, 'json' => ['success' => true, 'messages' => [], 'filtered' => true]];
    }

    /** A reading queue's configure answer: what it reads. */
    private static function routed(): array
    {
        return ['status' => 200, 'json' => [
            'configured' => true,
            'queue' => 'orders-it',
            'route' => ['from' => [['queue' => 'orders', 'where' => "data.country = 'IT'"]], 'partitionBy' => 'same', 'catchingUp' => true],
            'catchingUp' => true,
        ]];
    }

    private static function committed(): array
    {
        return ['status' => 200, 'json' => ['success' => true, 'transactionId' => 'tx', 'results' => []]];
    }

    // ===========================
    // Headers on a push
    // ===========================

    public function testAPushedItemCarriesItsHeadersBesideItsPayload(): void
    {
        $handler = new PlanHandler([self::queued(2)]);

        $this->queen($handler)->queue('orders')->partition('order-9137')->push([
            ['data' => ['orderId' => 9137], 'transactionId' => 'order-9137-paid', 'headers' => [
                'type' => 'order.paid',
                'traceparent' => '00-4bf92f35-00f067aa-01',
            ]],
            ['data' => ['orderId' => 9138], 'transactionId' => 'order-9138-paid'],
        ])->execute();

        $raw = (string) $handler->requests[0]->getBody();
        $this->assertStringContainsString('"headers":{"type":"order.paid","traceparent":"00-4bf92f35-00f067aa-01"}', $raw);
        $items = self::body($handler->requests[0])['items'];
        $this->assertSame(['orderId' => 9137], $items[0]['payload']);
        // An item without headers is the item this client always sent.
        $this->assertSame(['queue', 'partition', 'payload', 'transactionId'], array_keys($items[1]));
    }

    public function testAHeaderIsTextAndNamesThatLookLikeAListStillTravelAsAnObject(): void
    {
        $handler = new PlanHandler([self::queued(2)]);
        $stamp = new class () implements \Stringable {
            public function __toString(): string
            {
                return 'v2';
            }
        };

        $this->queen($handler)->queue('orders')->push([
            ['payload' => 1, 'headers' => ['customer' => 42, 'ratio' => 0.5, 'schema' => $stamp]],
            ['payload' => 2, 'headers' => ['0' => 'first', '1' => 'second']],
        ])->execute();

        $raw = (string) $handler->requests[0]->getBody();
        $this->assertStringContainsString('"headers":{"customer":"42","ratio":"0.5","schema":"v2"}', $raw);
        $this->assertStringContainsString('"headers":{"0":"first","1":"second"}', $raw);
    }

    public function testAHeaderThatIsNotTextIsRefusedBeforeAnyRequest(): void
    {
        $handler = new PlanHandler();

        try {
            $this->queen($handler)->queue('orders')->push([['data' => 1, 'headers' => ['vip' => true]]]);
            $this->fail('a boolean header reached the wire');
        } catch (\InvalidArgumentException $error) {
            $this->assertSame('header [vip] must be text, got bool', $error->getMessage());
        }
        $this->assertSame(0, $handler->count());
    }

    /** Without `data` or `payload` an item is the payload, every key in it, as it always was. */
    public function testAnItemThatIsItsOwnPayloadKeepsAHeadersKeyInIt(): void
    {
        $handler = new PlanHandler([self::queued()]);

        $this->queen($handler)->queue('http-log')->push([['url' => '/x', 'headers' => ['Accept' => 'json']]])->execute();

        $item = self::body($handler->requests[0])['items'][0];
        $this->assertArrayNotHasKey('headers', $item);
        $this->assertSame(['url' => '/x', 'headers' => ['Accept' => 'json']], $item['payload']);
    }

    public function testABufferedPushCarriesTheHeaders(): void
    {
        $handler = new PlanHandler([self::queued()]);
        $queen = $this->queen($handler);

        $queen->queue('orders')->partition('p1')->buffer(['messageCount' => 10, 'timeMillis' => 60000])
            ->push(['data' => ['a' => 1], 'headers' => ['type' => 'x']])->execute();
        $this->assertSame(0, $handler->count());
        $queen->flushAllBuffers();

        $this->assertSame(['type' => 'x'], self::body($handler->requests[0])['items'][0]['headers']);
    }

    // ===========================
    // Headers on a read
    // ===========================

    public function testAPoppedMessageHasTheHeadersTheBrokerSent(): void
    {
        $withHeaders = ['headers' => ['type' => 'order.paid']] + self::POPPED;
        $plain = ['transactionId' => 'order-2-paid', 'offset' => 43] + self::POPPED;
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['messages' => [$withHeaders, $plain]]]);
        $queen = $this->queen($handler);

        $popped = $queen->queue('orders')->group('review')->batch(2)->wait(false)->pop();
        $seen = [];
        $queen->queue('orders')->group('review')->batch(2)->wait(false)->autoAck(false)->each()->limit(2)
            ->consume(function (array $message) use (&$seen): void {
                $seen[] = $message['headers'] ?? null;
            })
            ->execute();

        $this->assertSame(['type' => 'order.paid'], $popped[0]['headers']);
        $this->assertArrayNotHasKey('headers', $popped[1]);
        $this->assertSame([['type' => 'order.paid'], null], $seen);
    }

    public function testAFetchedRecordKeepsItsHeaders(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['entries' => [[
            'highWatermark' => 2,
            'logStartOffset' => 0,
            'records' => [
                ['headers' => ['type' => 'order.paid'], 'offset' => 0, 'transactionId' => 't0', 'payload' => ['n' => 0], 'ts' => '2026-11-12T10:00:00.000Z'],
                ['offset' => 1, 'transactionId' => 't1', 'payload' => ['n' => 1], 'ts' => '2026-11-12T10:00:01.000Z'],
            ],
        ]]]]]);

        $records = $this->queen($handler)->queue('orders')->fetch('p1')['p1']['records'];

        $this->assertSame(['type' => 'order.paid'], $records[0]['headers']);
        $this->assertSame(['offset', 'transactionId', 'payload', 'ts'], array_keys($records[1]));
        $this->assertCount(1, $handler->requests);
    }

    // ===========================
    // Filtered consumer groups
    // ===========================

    public function testConditionsAreAndedAndTravelAsAListOnEveryPopOfTheGroup(): void
    {
        $handler = new PlanHandler([], self::filteredEmpty());
        $builder = $this->queen($handler)->queue('order-events')->group('billing')->wait(false)
            ->where('data.kind', 'in', ['order.paid', 'order.refunded'])
            ->where('headers.type', '=', 'order')
            ->where('data.vip', 'exists');

        $builder->pop();
        $builder->pop();

        $this->assertCount(2, $handler->requests);
        foreach ($handler->requests as $request) {
            $this->assertSame('/api/v1/pop/queue/order-events', $request->getUri()->getPath());
            $this->assertSame('billing', self::query($request)['consumerGroup']);
            $this->assertSame([
                ['field' => 'data.kind', 'op' => 'in', 'value' => ['order.paid', 'order.refunded']],
                ['field' => 'headers.type', 'op' => '=', 'value' => 'order'],
                ['field' => 'data.vip', 'op' => 'exists'],
            ], json_decode(self::query($request)['where'], true, 512, JSON_THROW_ON_ERROR));
        }
    }

    public function testOneStringIsSentAsItIs(): void
    {
        $handler = new PlanHandler([], self::filteredEmpty());
        $where = "data.kind in ('order.paid', 'order.refunded') and data.total > 100";

        $this->queen($handler)->queue('order-events')->partition('order-1')->group('billing')->wait(false)
            ->where($where)->pop();

        $request = $handler->requests[0];
        $this->assertSame('/api/v1/pop/queue/order-events/partition/order-1', $request->getUri()->getPath());
        $this->assertSame($where, self::query($request)['where']);
    }

    public function testAListOfConditionsIsTakenToo(): void
    {
        $handler = new PlanHandler([], self::filteredEmpty());

        $this->queen($handler)->queue('orders')->group('it')->wait(false)
            ->where([['data.country', '=', 'IT'], ['field' => 'data.total', 'op' => '>', 'value' => 100]])
            ->where('data.vip', 'exists')
            ->pop();

        $this->assertSame(
            '[{"field":"data.country","op":"=","value":"IT"},{"field":"data.total","op":">","value":100},{"field":"data.vip","op":"exists"}]',
            self::query($handler->requests[0])['where'],
        );
    }

    public function testEveryWayOfPoppingSendsTheWhere(): void
    {
        $handler = new PlanHandler([], self::filteredEmpty());
        $queen = $this->queen($handler);
        $filtered = static fn () => $queen->queue('orders')->group('it')->wait(false)->where('data.country', '=', 'IT');

        $filtered()->pop();
        $filtered()->popResult();
        $detached = $filtered();
        $detached->settlePop($detached->popDetached());
        $filtered()->idleMillis(1)->consume(function (): void {})->execute();
        $filtered()->concurrency(2)->idleMillis(1)->consume(function (): void {})->execute();
        $consumer = $filtered()->getConsumer();
        $consumer->subscribe();
        $consumer->consume(10);
        $consumer->consumeBatch(10, 5);

        $this->assertGreaterThanOrEqual(7, count($handler->requests));
        foreach ($handler->requests as $request) {
            $this->assertSame('[{"field":"data.country","op":"=","value":"IT"}]', self::query($request)['where'] ?? null);
        }
    }

    private static function filtered(Queen $queen): \Queen\Builders\QueueBuilder
    {
        return $queen->queue('orders')->group('it')->wait(false)->where('data.country', '=', 'IT');
    }

    /** @return array<string, array{array}> */
    public static function answersWithoutTheFilterEcho(): array
    {
        return [
            'a bodiless 204' => [self::empty()],
            'messages and no echo' => [['status' => 200, 'json' => ['success' => true, 'messages' => [self::POPPED]]]],
            'an echo that is not true' => [['status' => 200, 'json' => ['success' => true, 'messages' => [], 'filtered' => 'yes']]],
        ];
    }

    /**
     * A broker that does not filter ignores the `where` and hands out the
     * whole queue: every way of popping stops at its first answer, before a
     * handler sees a message.
     */
    #[DataProvider('answersWithoutTheFilterEcho')]
    public function testAFilteredPopTheBrokerDidNotFilterStopsEveryWayOfPopping(array $answer): void
    {
        $handled = false;
        $handler = function () use (&$handled): void {
            $handled = true;
        };
        $calls = [
            'pop' => static fn (Queen $q) => self::filtered($q)->pop(),
            'popResult' => static fn (Queen $q) => self::filtered($q)->popResult(),
            'popDetached' => static function (Queen $q): void {
                $builder = self::filtered($q);
                $builder->settlePop($builder->popDetached());
            },
            'consume' => static fn (Queen $q) => self::filtered($q)->limit(1)->consume($handler)->execute(),
            'consume with two workers' => static fn (Queen $q) => self::filtered($q)->concurrency(2)->limit(1)->consume($handler)->execute(),
            'getConsumer()->consume()' => static function (Queen $q): void {
                $consumer = self::filtered($q)->getConsumer();
                $consumer->subscribe();
                $consumer->consume(10);
            },
            'getConsumer()->consumeBatch()' => static function (Queen $q): void {
                $consumer = self::filtered($q)->getConsumer();
                $consumer->subscribe();
                $consumer->consumeBatch(10, 5);
            },
        ];

        foreach ($calls as $way => $call) {
            try {
                $call($this->queen(new PlanHandler([], $answer)));
                $this->fail("{$way}: an answer the broker did not filter was taken for a filtered one");
            } catch (RoutingUnsupportedException $error) {
                $this->assertStringStartsWith(RoutingUnsupportedException::FILTERING_MESSAGE, $error->getMessage(), $way);
            }
        }
        $this->assertFalse($handled, 'a handler ran on the whole queue');
    }

    public function testAFilteredPopThatSaysItFilteredHandsOutItsMessages(): void
    {
        $answer = ['status' => 200, 'json' => ['success' => true, 'messages' => [self::POPPED], 'filtered' => true]];

        $messages = self::filtered($this->queen(new PlanHandler([$answer])))->pop();

        $this->assertSame(['order-1-paid'], array_column($messages, 'transactionId'));
    }

    public function testAPopWithoutAWhereSendsNone(): void
    {
        $handler = new PlanHandler([], self::empty());

        $this->queen($handler)->queue('orders')->group('all')->wait(false)->pop();

        $this->assertArrayNotHasKey('where', self::query($handler->requests[0]));
    }

    public function testAStringIsTheWholeFilter(): void
    {
        $calls = [
            'a string, then a condition' => static fn (Queen $q) => $q->queue('o')->group('g')->where('data.a = 1')->where('data.b', '=', 2),
            'a string, then another' => static fn (Queen $q) => $q->queue('o')->group('g')->where('data.a = 1')->where('data.b = 2'),
            'a condition, then a string' => static fn (Queen $q) => $q->queue('o')->group('g')->where('data.a', '=', 1)->where('data.b = 2'),
        ];
        foreach ($calls as $case => $call) {
            try {
                $call($this->queen(new PlanHandler()));
                $this->fail("mixed: {$case}");
            } catch (\LogicException $error) {
                $this->assertStringContainsString('a string is the whole filter', $error->getMessage(), $case);
            }
        }
    }

    public function testANamespacePopCannotFilter(): void
    {
        $handler = new PlanHandler();
        $builder = $this->queen($handler)->queue()->namespace('shop')->group('g')->where('data.a', '=', 1);
        $calls = [
            'pop' => static fn () => $builder->pop(),
            'consume' => static fn () => $builder->consume(function (): void {}),
            'getConsumer' => static fn () => $builder->getConsumer(),
        ];

        foreach ($calls as $name => $call) {
            try {
                $call();
                $this->fail("{$name}: a namespace pop took a where");
            } catch (\LogicException $error) {
                $this->assertStringContainsString("filters a queue's consumer group", $error->getMessage(), $name);
            }
        }
        $this->assertSame(0, $handler->count());
    }

    /** @return array<string, array{\Closure, string}> */
    public static function badWheres(): array
    {
        return [
            'an unknown op' => [static fn () => Where::condition('data.kind', 'like', 'x'), 'unknown op [like]'],
            'in without a list' => [static fn () => Where::condition('data.kind', 'in', 'x'), 'in on data.kind needs a list'],
            'in with a map' => [static fn () => Where::condition('data.kind', 'in', ['a' => 'x']), 'needs a list'],
            'prefix without a string' => [static fn () => Where::condition('partition', 'prefix', 5), 'prefix on partition needs a string'],
            'no field' => [static fn () => Where::condition('', '=', 1), 'a field is a non-empty string'],
            'an empty string' => [static fn () => Where::of('  '), 'holds no condition'],
            'an empty list' => [static fn () => Where::of([]), 'no condition'],
            'four items' => [static fn () => Where::of([['data.a', '=', 1, 'x']]), 'a condition is [field, op, value]'],
            'an unknown key' => [static fn () => Where::of([['field' => 'data.a', 'op' => '=', 'valu' => 1]]), 'unknown key [valu]'],
            'a field that is not text' => [static fn () => Where::of([[7, '=', 1]]), 'as strings'],
            'an op on a list' => [static fn () => (new Queen('http://queen.test'))->queue('o')->where([['data.a', '=', 1]], '='), 'takes no op'],
        ];
    }

    #[DataProvider('badWheres')]
    public function testABadConditionIsNamedBeforeAnyRequest(\Closure $call, string $says): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($says);
        $call();
    }

    public function testTheFormsOfOneCondition(): void
    {
        $want = [['field' => 'data.a', 'op' => '=', 'value' => 1]];

        $this->assertSame($want, Where::of(['data.a', '=', 1]));
        $this->assertSame($want, Where::of(['field' => 'data.a', 'op' => '=', 'value' => 1]));
        $this->assertSame($want, Where::of([['data.a', '=', 1]]));
        $this->assertSame([['field' => 'data.vip', 'op' => 'exists']], Where::of([['data.vip', 'exists']]));
        $this->assertSame([['field' => 'data.gone', 'op' => '=', 'value' => null]], Where::of([['data.gone', '=', null]]));
        $this->assertSame('[{"field":"data.a","op":"=","value":1}]', Where::param($want));
        $this->assertSame("data.a = 1", Where::param('data.a = 1'));
    }

    public function testAGroupsWhereIsChangedOnPurposeThroughTheSubscriptionRoute(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true, 'consumerGroup' => 'billing team', 'where' => 'set']]]);

        $result = $this->queen($handler)->admin()
            ->setConsumerGroupWhere('billing team', 'order-events', ['data.kind', '=', 'order.paid']);

        $request = $handler->requests[0];
        $this->assertSame('POST', $request->getMethod());
        $this->assertSame('/api/v1/consumer-groups/billing%20team/subscription', $request->getUri()->getPath());
        $this->assertSame(
            ['queue' => 'order-events', 'where' => [['field' => 'data.kind', 'op' => '=', 'value' => 'order.paid']]],
            self::body($request),
        );
        $this->assertTrue($result['success']);
    }

    public function testAGroupThatDoesNotFilterTheQueueAnswers404(): void
    {
        $handler = new PlanHandler([['status' => 404, 'json' => [
            'success' => false,
            'error' => 'group billing does not filter order-events: its first pop with a where makes it filter',
        ]]]);

        try {
            $this->queen($handler)->admin()->setConsumerGroupWhere('billing', 'order-events', "data.kind = 'order.paid'");
            $this->fail('a 404 was taken for a change');
        } catch (HttpException $error) {
            $this->assertSame(404, $error->statusCode);
            $this->assertStringContainsString('does not filter order-events', $error->getMessage());
        }
        $this->assertSame("data.kind = 'order.paid'", self::body($handler->requests[0])['where']);
    }

    // ===========================
    // Reading queues
    // ===========================

    public function testAReadingQueueSendsNoKeepSettingItDidNotSet(): void
    {
        $handler = new PlanHandler([self::routed()]);

        $this->queen($handler)->queue('orders-it')->config([
            'from' => 'orders',
            'where' => "data.country = 'IT'",
            'partitionBy' => 'same',
            'start' => 'all',
            'leaseTime' => 30,
            'retryLimit' => 5,
        ])->create()->execute();

        $request = $handler->requests[0];
        $this->assertSame('/api/v1/configure', $request->getUri()->getPath());
        $body = self::body($request);
        $this->assertSame('orders-it', $body['queue']);
        $options = $body['options'];
        $this->assertSame([], array_values(array_intersect(array_keys($options), Defaults::READING_QUEUE_KEEPS)));
        $this->assertSame(
            ['from' => 'orders', 'where' => "data.country = 'IT'", 'partitionBy' => 'same', 'start' => 'all'],
            array_intersect_key($options, array_flip(['from', 'where', 'partitionBy', 'start'])),
        );
        $this->assertSame([30, 5], [$options['leaseTime'], $options['retryLimit']]);
        // The defaults that are not keep settings still fill in.
        $this->assertSame([0, 0, 0], [$options['priority'], $options['delayedProcessing'], $options['windowBuffer']]);
    }

    public function testAKeepSettingSetOnAReadingQueueIsSentForTheBrokerToRefuse(): void
    {
        $handler = new PlanHandler([['status' => 400, 'json' => [
            'error' => 'orders-it keeps what orders keeps: set it on orders',
            'code' => 'reading_queue',
        ]]]);

        try {
            $this->queen($handler)->queue('orders-it')->config(['from' => 'orders', 'retentionSeconds' => 60])->create()->execute();
            $this->fail('the refusal must reach the caller');
        } catch (HttpException $error) {
            $this->assertSame('reading_queue', $error->errorCode);
        }

        $options = self::body($handler->requests[0])['options'];
        $this->assertSame(60, $options['retentionSeconds']);
        foreach (['maxSize', 'completedRetentionSeconds', 'encryptionEnabled'] as $default) {
            $this->assertArrayNotHasKey($default, $options);
        }
    }

    public function testSourcesAndTheirWheresTakeEveryForm(): void
    {
        $handler = new PlanHandler([], self::routed());
        $queen = $this->queen($handler);

        $queen->queue('orders-vip')->config([
            'from' => ['orders', ['queue' => 'refunds', 'where' => ['data.vip', '=', true]]],
            'where' => ['data.total', '>', 100],
            'partitionBy' => 'headers.customer',
        ])->create()->execute();
        $queen->queue('orders-fr')->config(['from' => ['queue' => 'orders', 'where' => "data.country = 'FR'"]])->create()->execute();

        $vip = self::body($handler->requests[0])['options'];
        $this->assertSame(['orders', ['queue' => 'refunds', 'where' => [['field' => 'data.vip', 'op' => '=', 'value' => true]]]], $vip['from']);
        $this->assertSame([['field' => 'data.total', 'op' => '>', 'value' => 100]], $vip['where']);
        $this->assertSame('headers.customer', $vip['partitionBy']);
        // One source given as an object is a list of one.
        $this->assertSame([['queue' => 'orders', 'where' => "data.country = 'FR'"]], self::body($handler->requests[1])['options']['from']);
    }

    /** Any option only a reading queue has keeps the keep defaults home, not `from` alone. */
    public function testAWhereWithoutFromIsAReadingQueuesConfigToo(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);

        $this->queen($handler)->queue('orders-it')
            ->config(['where' => ['data.country', '=', 'IT'], 'leaseTime' => 30])->create()->execute();

        $options = self::body($handler->requests[0])['options'];
        $this->assertArrayNotHasKey('from', $options);
        $this->assertSame([], array_values(array_intersect(array_keys($options), Defaults::READING_QUEUE_KEEPS)));
        $this->assertSame([['field' => 'data.country', 'op' => '=', 'value' => 'IT']], $options['where']);
        $this->assertSame(30, $options['leaseTime']);
    }

    public function testStoppingTheReadingSendsANullFrom(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);

        $this->queen($handler)->queue('orders-it')->config(['from' => null])->create()->execute();

        $options = self::body($handler->requests[0])['options'];
        $this->assertArrayHasKey('from', $options);
        $this->assertNull($options['from']);
        $this->assertArrayNotHasKey('retentionSeconds', $options);
    }

    /**
     * A broker that does not read other queues configures an ordinary queue
     * and answers without a route. A queue configured without `from` (or with
     * `from` => null) needs none: the tests above answer it without one.
     */
    public function testAReadingQueueTheBrokerDidNotRouteThrows(): void
    {
        $ordinary = ['status' => 200, 'json' => ['configured' => true, 'queue' => 'orders-it', 'options' => ['leaseTime' => 30]]];

        try {
            $this->queen(new PlanHandler([$ordinary]))->queue('orders-it')->config(['from' => 'orders'])->create()->execute();
            $this->fail('an ordinary queue was taken for a reading queue');
        } catch (RoutingUnsupportedException $error) {
            $this->assertStringStartsWith(RoutingUnsupportedException::READING_MESSAGE, $error->getMessage());
            $this->assertStringContainsString("'orders-it' was configured as an ordinary queue", $error->getMessage());
        }

        // With onError, it goes there, as any failed operation does.
        $seen = null;
        $result = $this->queen(new PlanHandler([$ordinary]))->queue('orders-it')->config(['from' => 'orders'])->create()
            ->onError(function (\Throwable $error) use (&$seen): void {
                $seen = $error;
            })
            ->execute();
        $this->assertInstanceOf(RoutingUnsupportedException::class, $seen);
        $this->assertFalse($result['success']);
    }

    public function testDeletingASourceThatReadingQueuesReadTakesForce(): void
    {
        $handler = new PlanHandler([
            ['status' => 409, 'json' => [
                'deleted' => false,
                'code' => 'has_readers',
                'error' => 'orders is read by orders-it: delete them first, or delete with force=true (their links then read as gone)',
                'readers' => ['orders-it'],
            ]],
            ['status' => 200, 'json' => ['deleted' => true]],
        ]);
        $queen = $this->queen($handler);

        try {
            $queen->queue('orders')->delete()->execute();
            $this->fail('a source with readers was deleted without force');
        } catch (HttpException $error) {
            $this->assertSame([409, 'has_readers'], [$error->statusCode, $error->errorCode]);
        }
        $queen->queue('orders')->delete(force: true)->execute();

        [$plain, $forced] = $handler->requests;
        $this->assertSame(['DELETE', '/api/v1/resources/queues/orders', ''], [$plain->getMethod(), $plain->getUri()->getPath(), $plain->getUri()->getQuery()]);
        $this->assertSame(['DELETE', '/api/v1/resources/queues/orders', 'force=true'], [$forced->getMethod(), $forced->getUri()->getPath(), $forced->getUri()->getQuery()]);
    }

    public function testAQueueWithoutFromGetsEveryDefaultAsBefore(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['success' => true]]]);

        $this->queen($handler)->queue('orders')->config(['leaseTime' => 60])->create()->execute();

        $this->assertSame(
            array_merge(Defaults::QUEUE_DEFAULTS, ['leaseTime' => 60]),
            self::body($handler->requests[0])['options'],
        );
    }

    // ===========================
    // Timers
    // ===========================

    public function testATimerCarriesTheHeadersOfTheMessageItFires(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [[
                'ok' => true, 'status' => 'scheduled', 'queue' => 'payments.timeout', 'timerKey' => 'order-1', 'txn' => 't1',
            ]]]],
            self::committed(),
        ]);
        $queen = $this->queen($handler);

        $queen->timers()->schedule('payments.timeout', 'order-1', 900_000, ['orderId' => 1], [
            'headers' => ['type' => 'payment.timeout', 'attempt' => 2],
        ]);
        $queen->transaction()
            ->timers('payments.timeout')->schedule('order-2', 1000, ['orderId' => 2], ['headers' => ['type' => 'payment.timeout']])
            ->commit();

        $this->assertSame('/api/v1/timers', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(['type' => 'payment.timeout', 'attempt' => '2'], self::body($handler->requests[0])['operations'][0]['headers']);
        $this->assertSame(['type' => 'payment.timeout'], self::body($handler->requests[1])['timers'][0]['headers']);
        // None is none: the operation this client always sent.
        $this->assertArrayNotHasKey('headers', TimerOp::schedule('q', 'k', 1, 1, ['headers' => []]));
        $this->assertArrayNotHasKey('headers', TimerOp::reschedule('q', 'k', 1, 1));
        $this->assertSame(['type' => 'x'], TimerOp::reschedule('q', 'k', 1, 1, ['headers' => ['type' => 'x']])['headers']);
    }

    public function testATimerHeaderThatIsNotTextIsRefusedBeforeAnyRequest(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('header [vip] must be text, got bool');
        TimerOp::schedule('q', 'k', 1, 1, ['headers' => ['vip' => false]]);
    }

    // ===========================
    // Transactions
    // ===========================

    public function testATransactionPushCarriesHeaders(): void
    {
        $handler = new PlanHandler([self::committed()]);

        $this->queen($handler)->transaction()
            ->queue('out')->partition('p1')->push([['data' => 1, 'headers' => ['type' => 'x']], ['data' => 2]])
            ->commit();

        $items = self::body($handler->requests[0])['operations'][0]['items'];
        $this->assertSame(['type' => 'x'], $items[0]['headers']);
        $this->assertArrayNotHasKey('headers', $items[1]);
    }

    public function testALinkIsOneOperationPerMessageNamedAsThePopReturnedIt(): void
    {
        $handler = new PlanHandler([self::committed()]);
        $second = ['transactionId' => 'order-2-paid', 'partitionId' => '124', 'offset' => 7] + self::POPPED;

        $this->queen($handler)->transaction()
            ->ack(self::POPPED, 'completed', ['consumerGroup' => 'review'])
            ->queue('manual-review')->partition('order-1')
            ->link([self::POPPED, $second], ['review-reason' => 'amount'])
            ->commit();

        $body = self::body($handler->requests[0]);
        $this->assertSame(['ack', 'link', 'link'], array_column($body['operations'], 'type'));
        $this->assertSame([
            'type' => 'link',
            'queue' => 'manual-review',
            'partition' => 'order-1',
            'from' => ['partitionId' => '123', 'offset' => 42, 'transactionId' => 'order-1-paid', 'consumerGroup' => 'review'],
            'headers' => ['review-reason' => 'amount'],
        ], $body['operations'][1]);
        $this->assertSame(
            ['partitionId' => '124', 'offset' => 7, 'transactionId' => 'order-2-paid', 'consumerGroup' => 'review'],
            $body['operations'][2]['from'],
        );
        // A link names no lease: the ack beside it fences the bundle.
        $this->assertSame(['lease-1'], $body['requiredLeases']);
    }

    public function testALinkWithoutAPartitionOrHeadersLeavesThemOut(): void
    {
        $handler = new PlanHandler([self::committed()]);

        $this->queen($handler)->transaction()->queue('archive')->link(self::POPPED)->commit();

        $operation = self::body($handler->requests[0])['operations'][0];
        $this->assertSame(['type', 'queue', 'from'], array_keys($operation));
    }

    /** A feed entry's partitionId is a UUID: the entry is named by its queue and partition. */
    public function testAMessageWithoutANumericPartitionIdIsNamedByItsQueueAndPartition(): void
    {
        $handler = new PlanHandler([self::committed()]);
        $entry = [
            'queue' => 'orders',
            'partition' => 'order-1',
            'partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83',
            'offset' => '42',
            'transactionId' => 't1',
            'data' => ['n' => 1],
        ];

        $this->queen($handler)->transaction()->queue('archive')->partition('order-1')->link([$entry])->commit();

        $this->assertSame(
            ['queue' => 'orders', 'partition' => 'order-1', 'offset' => 42, 'transactionId' => 't1'],
            self::body($handler->requests[0])['operations'][0]['from'],
        );
    }

    /** @return array<string, array{array}> */
    public static function unnamedMessages(): array
    {
        $popped = self::POPPED;

        return [
            'no offset' => [array_diff_key($popped, ['offset' => true])],
            'a negative offset' => [['offset' => -1] + $popped],
            'no transactionId' => [array_diff_key($popped, ['transactionId' => true])],
            'no partitionId and no queue' => [array_diff_key($popped, ['partitionId' => true])],
            'a UUID partitionId and no queue' => [['partitionId' => '0198f2c1-4d3a-7c10-9f2b-6a1e5d0c7b83'] + $popped],
        ];
    }

    #[DataProvider('unnamedMessages')]
    public function testAMessageALinkCannotNameIsRefusedBeforeAnyRequest(array $message): void
    {
        $handler = new PlanHandler();

        try {
            $this->queen($handler)->transaction()->queue('archive')->link([$message]);
            $this->fail('a link that names no message was built');
        } catch (\InvalidArgumentException $error) {
            $this->assertStringStartsWith('link: a message needs', $error->getMessage());
        }
        $this->assertSame(0, $handler->count());
    }

    public function testALinkThatCannotBeMadeFailsTheCommit(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => [
            'transactionId' => 'tx',
            'success' => false,
            'reason' => 'source_gone',
            'error' => 'source_gone: offset 42 of orders is not retained',
            'results' => [],
        ]]]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('source_gone: offset 42 of orders is not retained');
        $this->queen($handler)->transaction()
            ->ack(self::POPPED, 'completed', ['consumerGroup' => 'review'])
            ->queue('manual-review')->link([self::POPPED])
            ->commit();
    }

    /** Each link is one place in the flat result space, as an ack is. */
    public function testAGuardAfterTwoLinksIsTheirsToLose(): void
    {
        $guard = ['op' => 'check', 'ns' => 'queen-locks', 'key' => 'job#0', 'expect' => 100, 'required' => true];
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [[
                'index' => 0, 'op' => 'acquire', 'name' => 'job', 'acquired' => true, 'slot' => 0,
                'token' => 100, 'owner' => 'o', 'guard' => $guard,
            ]]]],
            ['status' => 200, 'json' => [
                'transactionId' => 't', 'success' => false, 'reason' => 'kv_precondition', 'error' => 'QKV',
                'results' => [], 'failedIndex' => 2, 'kvReason' => 'version', 'value' => null, 'version' => 250,
            ]],
        ]);
        $queen = $this->queen($handler);
        $lock = $queen->lock('job', ['ttlSeconds' => 30]);
        $lock->acquire();

        $result = $queen->transaction()
            ->guard($lock)
            ->queue('review')->link([self::POPPED, ['offset' => 43] + self::POPPED])
            ->commit();

        $this->assertSame('kv_precondition', $result['reason']);
        $this->assertSame([$guard], self::body($handler->requests[1])['kv']);
        $this->assertFalse($lock->held(), 'flat index 2 is the guard, after the two links');
    }
}
