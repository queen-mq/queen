<?php

namespace Queen\Tests;

use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use Illuminate\Queue\QueueManager;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\QueueWatcher;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Tests\Support\PlanHandler;
use ReflectionMethod;
use ReflectionProperty;

/**
 * Event-driven scaling: a read-only long poll wakes the master when a watched
 * queue grows. The Rust engine asserts the same rules in supervisor/src/watch.rs
 * and supervisor/src/main.rs.
 */
final class EventDrivenScalingTest extends TestCase
{
    private float $now = 1000.0;

    public function testOnlyPoolsThatFollowTheBacklogAreWatchedOnEveryStripe(): void
    {
        $lanes = QueueWatcher::lanes([
            'event_driven' => ['stripes' => ['queen' => ['prefix' => 'laravel', 'count' => 2], 'other' => ['prefix' => 'jobs', 'count' => 2]]],
            'supervisors' => [
                'auto' => $this->pool('queen', ['high', 'default'], 'auto', 1, 10),
                'shared' => $this->pool('queen', ['default', 'emails'], 'off', 1, 4),
                'fixed' => $this->pool('queen', ['payments'], 'simple', 2, 2),
                'pinned' => $this->pool('other', ['reports'], 'auto', 3, 3),
            ],
        ]);

        $this->assertSame(['queen'], array_keys($lanes));
        $this->assertSame([
            ['queue' => 'high', 'partition' => 'laravel-0000'],
            ['queue' => 'high', 'partition' => 'laravel-0001'],
            ['queue' => 'default', 'partition' => 'laravel-0000'],
            ['queue' => 'default', 'partition' => 'laravel-0001'],
            ['queue' => 'emails', 'partition' => 'laravel-0000'],
            ['queue' => 'emails', 'partition' => 'laravel-0001'],
        ], $lanes['queen']);
    }

    public function testTheWatcherProbesThenParksFromTheBoundsAndReportsGrowth(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['entries' => [
                ['highWatermark' => 5, 'error' => 'OFFSET_OUT_OF_RANGE'],
                ['highWatermark' => 0, 'error' => 'OFFSET_OUT_OF_RANGE'],
            ]]],
            ['status' => 200, 'json' => ['entries' => [
                ['records' => [], 'highWatermark' => 5],
                ['records' => [['offset' => 0]], 'highWatermark' => 1],
            ]]],
        ]);
        $watcher = $this->watcher($handler);

        $this->assertSame([], $watcher->wait(800));
        $this->assertSame(['high'], $watcher->wait(800));
        // A wake rests the watcher for a wake interval.
        $this->assertFalse($watcher->ready());
        $this->now += QueueWatcher::WAKE_INTERVAL_SECONDS;
        $this->assertTrue($watcher->ready());

        $probe = json_decode((string) $handler->requests[0]->getBody(), true);
        $park = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame('http://queen.test:6632/api/v1/fetch', (string) $handler->requests[0]->getUri());
        $this->assertSame('Bearer token', $handler->requests[0]->getHeaderLine('Authorization'));
        $this->assertSame(0, $probe['maxWaitMs']);
        $this->assertSame(QueueWatcher::PROBE_OFFSET, $probe['entries'][0]['offset']);
        $this->assertSame(800, $park['maxWaitMs']);
        $this->assertSame([5, 0], array_column($park['entries'], 'offset'));
        $this->assertSame([1, 1], array_column($park['entries'], 'maxBytes'));
        // The request may park: its timeout covers the wait.
        $this->assertEqualsWithDelta(5.8, $handler->options[1]['timeout'], 0.001);
    }

    public function testARetentionMoveIsNotGrowthAndAnUnknownQueueCallsForAProbe(): void
    {
        $lanes = [['queue' => 'high', 'partition' => 'laravel-0000'], ['queue' => 'default', 'partition' => 'laravel-0000']];

        [$grown, $highs, $broken] = QueueWatcher::grown($lanes, [7, 9], ['entries' => [
            ['highWatermark' => 7],
            ['highWatermark' => 12, 'error' => 'OFFSET_OUT_OF_RANGE'],
        ]]);

        $this->assertSame([], $grown);
        $this->assertSame([7, 12], $highs);
        $this->assertFalse($broken);
        $this->assertTrue(QueueWatcher::grown($lanes, [7, 9], ['entries' => [
            ['highWatermark' => 7],
            ['highWatermark' => 0, 'error' => 'UNKNOWN_TOPIC_OR_PARTITION'],
        ]])[2]);
        $this->expectException(\UnexpectedValueException::class);
        QueueWatcher::grown($lanes, [7, 9], ['entries' => []]);
    }

    public function testQueuesThatDoNotExistYetAreLeftOutAndProbedAgainLater(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['entries' => [
                ['highWatermark' => 3, 'error' => 'OFFSET_OUT_OF_RANGE'],
                ['highWatermark' => 0, 'error' => 'UNKNOWN_TOPIC_OR_PARTITION'],
            ]]],
            ['status' => 200, 'json' => ['entries' => [['highWatermark' => 3]]]],
            ['status' => 200, 'json' => ['entries' => [
                ['highWatermark' => 4, 'error' => 'OFFSET_OUT_OF_RANGE'],
                ['highWatermark' => 0, 'error' => 'OFFSET_OUT_OF_RANGE'],
            ]]],
        ]);
        $watcher = $this->watcher($handler);

        $watcher->wait(800);
        // Released without growth: rest instead of asking again at once.
        $this->assertSame([], $watcher->wait(800));
        $this->assertFalse($watcher->ready());
        $park = json_decode((string) $handler->requests[1]->getBody(), true);
        $this->assertSame([['queue' => 'high', 'partition' => 'laravel-0000', 'offset' => 3, 'maxBytes' => 1]], $park['entries']);

        // The next probe includes every lane again, and growth since the last
        // answer still wakes.
        $this->now += 30;
        $this->assertSame(['high'], $watcher->wait(800));
        $this->assertSame(QueueWatcher::PROBE_OFFSET, json_decode((string) $handler->requests[2]->getBody(), true)['entries'][1]['offset']);
    }

    public function testAnOversizedAnswerWakesEveryWatchedQueueWithoutReadingIt(): void
    {
        $answers = [
            new Response(200, [], json_encode(['entries' => [['highWatermark' => 1], ['highWatermark' => 1]]])),
            new Response(200, ['Content-Length' => (string) (QueueWatcher::MAX_ANSWER_BYTES + 1)], '{}'),
        ];
        $watcher = $this->watcher(function (RequestInterface $request) use (&$answers): FulfilledPromise {
            return new FulfilledPromise(array_shift($answers));
        });

        $watcher->wait(800);
        $this->assertSame(['high'], $watcher->wait(800));
    }

    public function testTheWatcherTurnsOffOnlyWhenNoEndpointServesIt(): void
    {
        $missing = $this->watcher(new PlanHandler([], ['status' => 404, 'json' => ['code' => 'no_such_route']]));
        $this->assertSame([], $missing->wait(800));
        $this->assertFalse($missing->ready());
        $this->now += 3600;
        $this->assertFalse($missing->ready());

        $handler = new PlanHandler([
            ['status' => 404, 'json' => ['code' => 'no_such_route']],
            ['status' => 200, 'json' => ['entries' => [['highWatermark' => 1], ['highWatermark' => 0]]]],
        ]);
        $failover = $this->watcher($handler, ['http://a.test:6632', 'http://b.test:6632']);
        $failover->wait(800);
        $this->assertTrue($failover->ready());
        $this->assertSame(['a.test', 'b.test'], $handler->hosts());

        $down = $this->watcher(new PlanHandler([], ['status' => 503, 'json' => []]));
        $this->assertSame([], $down->wait(800));
        $this->assertFalse($down->ready());
        $this->now += 5;
        $this->assertTrue($down->ready());
    }

    /**
     * A connection gone silent after the headers: every read of the streamed
     * body times out empty, and it never reaches its end. The master loop
     * that waits on the watcher must not stall with it.
     */
    public function testAStalledAnswerCannotFreezeTheMasterLoop(): void
    {
        if (!function_exists('pcntl_alarm')) {
            $this->markTestSkipped('Needs ext-pcntl.');
        }
        $stalled = \GuzzleHttp\Psr7\FnStream::decorate(\GuzzleHttp\Psr7\Utils::streamFor(''), [
            'eof' => static fn (): bool => false,
            'read' => static function (int $length): string {
                usleep(10_000);

                return '';
            },
        ]);
        $watcher = new QueueWatcher(
            ['urls' => ['http://queen.test:6632'], 'bearer_token' => 'token', 'headers' => []],
            'queen',
            [['queue' => 'high', 'partition' => 'laravel-0000']],
            1.0,
            null,
            fn (): float => $this->now,
            static fn ($request, array $options) => new \GuzzleHttp\Promise\FulfilledPromise(
                new \GuzzleHttp\Psr7\Response(200, ['Content-Type' => 'application/json'], $stalled),
            ),
        );
        // This test's own safety net; the master loop has none.
        $async = pcntl_async_signals(true);
        $previous = pcntl_signal_get_handler(SIGALRM);
        pcntl_signal(SIGALRM, static function (): void {
            throw new \RuntimeException('the master loop froze');
        });
        pcntl_alarm(5);
        $started = microtime(true);
        try {
            $this->assertSame([], $watcher->wait(800));
        } finally {
            pcntl_alarm(0);
            pcntl_signal(SIGALRM, is_callable($previous) || is_int($previous) ? $previous : SIG_DFL);
            pcntl_async_signals($async);
        }

        $this->assertLessThan(3.0, microtime(true) - $started, 'a stalled answer held the master loop');
        $this->assertFalse($watcher->ready(), 'a stalled endpoint is retried later, as a failure');
    }

    public function testAWokenOrClimbingPoolIsDueBeforeTheNextPollOnceASecond(): void
    {
        $supervisor = new PhpSupervisor($this->createStub(QueueManager::class), [
            'state_directory' => sys_get_temp_dir() . '/unused',
            'supervisors' => [
                'default' => $this->pool('queen', ['high', 'default'], 'auto', 1, 10),
                'fixed' => $this->pool('queen', ['default'], 'simple', 2, 2),
            ],
        ]);
        $set = fn (string $property, mixed $value) => (new ReflectionProperty(PhpSupervisor::class, $property))->setValue($supervisor, $value);
        $due = fn (): array => (new ReflectionMethod(PhpSupervisor::class, 'eventDuePools'))->invoke($supervisor, 100.0);

        $this->assertSame([], $due());
        $set('watchers', []);
        $set('woken', ['default' => true]);
        $this->assertSame(['default' => true], $due());
        // Not due yet: the wake is kept until the pool is evaluated.
        $set('lastEvaluated', ['default' => 99.5]);
        $this->assertSame([], $due());
        $set('lastEvaluated', ['default' => 99.0]);
        $this->assertSame(['default' => true], $due());

        $set('woken', []);
        $set('climbing', ['default' => true]);
        $this->assertSame(['default' => true], $due());
    }

    public function testAGrowOnlyEvaluationNeverTakesAWorkerFromAQueue(): void
    {
        $options = $this->pool('queen', ['high', 'default'], 'auto', 1, 10);
        $current = ['high' => 4, 'default' => 0];

        // The backlog moved from high to default: grow default by what the
        // target adds overall, and keep high until the cooldown.
        $this->assertSame(['high' => 4, 'default' => 1], PhpSupervisor::growOnlyTarget($options, ['high' => 0, 'default' => 5], $current));
        $this->assertSame(['high' => 6, 'default' => 3], PhpSupervisor::growOnlyTarget($options, ['high' => 6, 'default' => 3], $current));
        $this->assertNull(PhpSupervisor::growOnlyTarget($options, ['high' => 1, 'default' => 3], $current));
    }

    public function testTheResolverExportsTheStripesOnlyWhenEnabled(): void
    {
        $queen = fn (array $supervisor, array $root = []): array => [
            'url' => 'http://queen.test:6632',
            ...$root,
            'supervisor' => ['supervisors' => ['default' => ['queues' => ['high'], 'balance' => 'auto']], ...$supervisor],
        ];

        $this->assertArrayNotHasKey('event_driven', SupervisorConfiguration::resolve($queen([]), '/app'));
        $this->assertSame(
            ['stripes' => ['queen' => ['prefix' => 'laravel', 'count' => 64]]],
            SupervisorConfiguration::resolve($queen(['event_driven' => true]), '/app')['event_driven'],
        );
        $this->assertSame(
            ['stripes' => ['queen' => ['prefix' => 'orders', 'count' => 8]]],
            SupervisorConfiguration::resolve($queen(['event_driven' => true], ['partitions' => '8', 'partition_prefix' => 'orders']), '/app')['event_driven'],
        );
        $this->expectException(\InvalidArgumentException::class);
        SupervisorConfiguration::resolve($queen(['event_driven' => true], ['partitions' => 65]), '/app');
    }

    /** @param list<string> $urls */
    private function watcher(callable $handler, array $urls = ['http://queen.test:6632']): QueueWatcher
    {
        return new QueueWatcher(
            ['urls' => $urls, 'bearer_token' => 'token', 'headers' => []],
            'queen',
            [['queue' => 'high', 'partition' => 'laravel-0000'], ['queue' => 'high', 'partition' => 'laravel-0001']],
            5.0,
            null,
            fn (): float => $this->now,
            $handler,
        );
    }

    /**
     * @param list<string> $queues
     * @return array<string, mixed>
     */
    private function pool(string $connection, array $queues, string $balance, int $min, int $max): array
    {
        return [
            'connection' => $connection,
            'consumer_group' => 'laravel',
            'queues' => $queues,
            'balance' => $balance,
            'min_processes' => $min,
            'max_processes' => $max,
        ];
    }
}
