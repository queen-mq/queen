<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * A raft cluster answers 503 for a few seconds while it elects a leader. The
 * body's code says whether the broker ran the request:
 *
 *   no_leader, standby          it did not;
 *   retry                       it refused it, or a follower lost its relay
 *                               to the leader after the request went out;
 *   outcome_unknown, timeout    it may have.
 *
 * A request whose second run changes nothing waits the election out, within
 * a bounded budget and at the pace of Retry-After; a pop whose outcome is
 * unknown is never sent again; a consume() loop backs off and goes on.
 */
final class LeaderElectionTest extends TestCase
{
    public function testAPushWaitsOutAnElectionOnASingleBackend(): void
    {
        $handler = new PlanHandler([
            self::unavailable('no_leader'),
            self::unavailable('no_leader'),
            self::unavailable('retry'),
            self::unavailable('no_leader'),
            ['status' => 201, 'json' => [['status' => 'queued', 'transactionId' => 't-1']]],
        ]);
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($handler),
        ]);

        $result = $client->post('/api/v1/push', ['items' => [['queue' => 'q', 'payload' => [], 'transactionId' => 't-1']]]);

        $this->assertSame('queued', $result[0]['status']);
        $this->assertSame(5, $handler->count(), 'the election used up the retry attempts');
    }

    public function testFailoverWaitsOutAnElectionThatAnswers503OnEveryBackend(): void
    {
        $handler = new PlanHandler([
            ...array_fill(0, 6, self::unavailable('no_leader')),
            ['status' => 200, 'json' => ['messages' => [['transactionId' => 't-1', 'partitionId' => 'p-1']]]],
        ]);
        $client = new HttpClient([
            'loadBalancer' => new LoadBalancer(
                ['http://queen-a:6632', 'http://queen-b:6632', 'http://queen-c:6632'],
                'affinity',
            ),
            'handler' => HandlerStack::create($handler),
        ]);

        $result = $client->get('/api/v1/pop/queue/orders?wait=false', affinityKey: 'orders:*:workers');

        $this->assertSame('t-1', $result['messages'][0]['transactionId'], 'a pop no leader ran is sent again');
        $this->assertSame(7, $handler->count());
    }

    public function testTheWaitFollowsRetryAfterAndEndsWithinTheRequestTimeout(): void
    {
        $handler = new PlanHandler([], self::unavailable('no_leader', '0.1'));
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'timeoutMillis' => 450,
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($handler),
        ]);

        $started = microtime(true);
        try {
            $client->get('/api/v1/resources/queues');
            $this->fail('An election that outlasted the budget was waited out.');
        } catch (HttpException $exception) {
            $this->assertSame(503, $exception->statusCode);
            $this->assertSame('no_leader', $exception->errorCode);
            $this->assertSame(0.1, $exception->retryAfterSeconds);
        }
        $elapsed = microtime(true) - $started;

        $this->assertGreaterThanOrEqual(0.3, $elapsed, 'Retry-After was not honoured');
        $this->assertLessThan(1.0, $elapsed, 'the wait outlived the request timeout');
        $this->assertGreaterThanOrEqual(4, $handler->count());
        $this->assertLessThanOrEqual(6, $handler->count());
    }

    /**
     * Only a request whose second run changes nothing waits: a transaction
     * (or any write the broker does not deduplicate) keeps the bounded
     * retryAttempts it had.
     */
    public function testAWriteTheBrokerDoesNotDeduplicateKeepsItsRetryAttempts(): void
    {
        $handler = new PlanHandler([], self::unavailable('no_leader'));
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($handler),
        ]);

        try {
            $client->post('/api/v1/transaction', ['operations' => []]);
            $this->fail('A 503 was taken for an answer.');
        } catch (HttpException) {
        }
        $this->assertSame(3, $handler->count());
    }

    #[\PHPUnit\Framework\Attributes\TestWith(['outcome_unknown'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['timeout'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['retry'])]
    public function testAPopWhoseOutcomeIsUnknownIsNeverSentAgain(string $code): void
    {
        foreach ([['baseUrl' => 'http://queen-a:6632'], ['loadBalancer' => new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'])]] as $where) {
            $handler = new PlanHandler([
                self::unavailable($code),
                ['status' => 200, 'json' => ['messages' => [['transactionId' => 't-1', 'partitionId' => 'p-1']]]],
            ]);
            $client = new HttpClient($where + ['retryDelayMillis' => 0, 'handler' => HandlerStack::create($handler)]);

            try {
                $client->get('/api/v1/pop/queue/orders?wait=false');
                $this->fail("A pop answered {$code} was sent again.");
            } catch (HttpException $exception) {
                $this->assertSame($code, $exception->errorCode);
            }
            $this->assertSame(1, $handler->count(), "a pop answered {$code} was sent again");
        }
    }

    #[\PHPUnit\Framework\Attributes\TestWith([1])]
    #[\PHPUnit\Framework\Attributes\TestWith([2])]
    public function testConsumeBacksOffAfterA503AndGoesOn(int $concurrency): void
    {
        $handler = new PlanHandler([
            self::unavailable('outcome_unknown'),
            self::unavailable('outcome_unknown'),
            self::unavailable('outcome_unknown'),
            ['status' => 200, 'json' => ['messages' => [[
                'transactionId' => 't-1',
                'partitionId' => 'p-1',
                'leaseId' => 'l-1',
                'data' => [],
            ]]]],
        ], ['status' => 200, 'json' => ['success' => true]]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($handler),
        ]);

        $handled = [];
        $queen->queue('orders')->group('workers')->concurrency($concurrency)->limit(1)
            ->consume(function (array $messages) use (&$handled): void {
                $handled[] = $messages[0]['transactionId'];
            })
            ->execute();

        $this->assertSame(['t-1'], $handled);
    }

    /**
     * A gateway in front of the broker may answer a pop with an empty 200 during
     * a rollout. That is no empty pop and no message, but neither is it the end
     * of consume(): it backs off and polls again, as after a network error.
     */
    public function testConsumeGoesOnAfterAnAnswerThatIsNotTheBrokers(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'body' => ''],
            ['status' => 200, 'json' => ['messages' => [[
                'transactionId' => 't-1',
                'partitionId' => 'p-1',
                'leaseId' => 'l-1',
                'data' => [],
            ]]]],
        ], ['status' => 200, 'json' => ['success' => true]]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'retryDelayMillis' => 0,
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);

        $handled = [];
        $queen->queue('orders')->group('workers')->limit(1)
            ->consume(function (array $messages) use (&$handled): void {
                $handled[] = $messages[0]['transactionId'];
            })
            ->execute();

        $this->assertSame(['t-1'], $handled);
    }

    public function testHighLevelConsumeReturnsNothingAfterA503(): void
    {
        $handler = new PlanHandler([], self::unavailable('outcome_unknown'));
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($handler),
        ]);
        $consumer = $queen->queue('orders')->group('workers')->getConsumer();
        $consumer->subscribe();

        $this->assertNull($consumer->consume(100));
        $this->assertStringContainsString('may or may not', (string) $consumer->lastPopError());
        $this->assertSame([], $consumer->consumeBatch(100));
    }

    private static function unavailable(string $code, string $retryAfter = '0'): array
    {
        $error = match ($code) {
            'no_leader' => 'no leader is known; retry after',
            'outcome_unknown' => 'the leader changed while this change was being made durable: it may or may not have applied',
            'timeout' => 'the request deadline elapsed',
            default => 'no quorum-ack lease; retry',
        };

        return ['status' => 503, 'retryAfter' => $retryAfter, 'json' => ['error' => $error, 'code' => $code]];
    }
}
