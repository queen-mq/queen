<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Exceptions\HttpException;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

class HttpClientTest extends TestCase
{
    public function testNoBaseUrlAndNoLoadBalancerThrows(): void
    {
        $client = new HttpClient([]);

        $this->expectException(\LogicException::class);
        $client->get('/test');
    }

    public function testHttpExceptionHasStatusCode(): void
    {
        $ex = new HttpException('Not Found', 404);

        $this->assertSame(404, $ex->statusCode);
        $this->assertSame('Not Found', $ex->getMessage());
        $this->assertInstanceOf(\RuntimeException::class, $ex);
    }

    public function testHttpExceptionWithPrevious(): void
    {
        $previous = new \RuntimeException('original');
        $ex = new HttpException('Wrapped', 500, 0, $previous);

        $this->assertSame(500, $ex->statusCode);
        $this->assertSame($previous, $ex->getPrevious());
    }

    public function testHttpExceptionKeepsServerErrorSeparateFromProxyCode(): void
    {
        $handler = new PlanHandler([], ['status' => 400, 'json' => [
            'error' => 'unsupported',
            'code' => 'operation_rejected',
            'reason' => 'timer_count_mode',
        ]]);
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);

        try {
            $client->get('/api/v1/timers/q?mode=count&prefix=laravel%3A');
            $this->fail('HTTP 400 must throw.');
        } catch (HttpException $exception) {
            $this->assertSame('unsupported', $exception->serverError);
            $this->assertSame('operation_rejected', $exception->errorCode);
            $this->assertSame('timer_count_mode', $exception->reason);
        }
    }

    public function testGetLoadBalancerReturnsNull(): void
    {
        $client = new HttpClient(['baseUrl' => 'http://localhost']);
        $this->assertNull($client->getLoadBalancer());
    }

    public function testGetLoadBalancerReturnsInstance(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b']);
        $client = new HttpClient(['loadBalancer' => $lb]);
        $this->assertSame($lb, $client->getLoadBalancer());
    }

    public function testAsyncFailoverMovesAReadAfterAServerError(): void
    {
        $handler = new PlanHandler([
            ['status' => 503, 'json' => ['error' => 'unavailable']],
            ['status' => 200, 'json' => ['pending' => 7]],
        ]);
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'round-robin');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'enableFailover' => true,
            'handler' => HandlerStack::create($handler),
        ]);

        $result = $client->getAsyncWithFailover('/depth')->wait();

        $this->assertSame(['pending' => 7], $result);
        $this->assertSame(['queen-a', 'queen-b'], $handler->hosts());
        $this->assertFalse($loadBalancer->getHealthStatus()['http://queen-a:6632']['healthy']);
    }

    /**
     * A leader election can leave every backend marked unhealthy: the
     * balancer then offers one URL whatever it is asked, and the failover
     * must still go on to the others.
     */
    #[\PHPUnit\Framework\Attributes\TestWith(['round-robin'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['affinity'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['session'])]
    public function testFailoverTriesEveryBackendWhenAllAreMarkedUnhealthy(string $strategy): void
    {
        $handler = new PlanHandler([
            ['status' => 500, 'json' => ['error' => 'down']],
            ['status' => 500, 'json' => ['error' => 'down']],
            ['status' => 200, 'json' => ['ok' => true]],
        ]);
        $urls = ['http://queen-a:6632', 'http://queen-b:6632', 'http://queen-c:6632'];
        $loadBalancer = new LoadBalancer($urls, $strategy);
        foreach ($urls as $url) {
            $loadBalancer->markUnhealthy($url);
        }
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'enableFailover' => true,
            'handler' => HandlerStack::create($handler),
        ]);

        $this->assertSame(['ok' => true], $client->get('/api/v1/status', affinityKey: 'orders:*:workers'));
        $this->assertSame(3, $handler->count());
        $this->assertCount(3, array_unique($handler->hosts()), 'a backend was tried twice while another was never tried');
    }

    /**
     * consume() with concurrency > 1 polls with getAsync(): one try, no
     * failover in flight. It must still tell the balancer, or the next round
     * goes back to the same dead backend for as long as the consumer runs.
     */
    public function testAnAsyncRequestThatFailsSendsTheNextOneToAnotherBackend(): void
    {
        $hosts = [];
        $dead = null;
        $handler = static function (\Psr\Http\Message\RequestInterface $request) use (&$hosts, &$dead): \GuzzleHttp\Promise\PromiseInterface {
            $host = $request->getUri()->getHost();
            $hosts[] = $host;
            $dead ??= $host;
            if ($host === $dead) {
                return \GuzzleHttp\Promise\Create::rejectionFor(
                    new \GuzzleHttp\Exception\ConnectException('cURL error 7: Connection refused', $request),
                );
            }

            return new \GuzzleHttp\Promise\FulfilledPromise(new \GuzzleHttp\Psr7\Response(204));
        };
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'affinity');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'handler' => HandlerStack::create($handler),
        ]);
        $pop = '/api/v1/pop/queue/orders?wait=true&timeout=100';

        $first = HttpClient::settleAll([$client->getAsync($pop, 1_000, 'orders:*:workers')]);
        $second = HttpClient::settleAll([$client->getAsync($pop, 1_000, 'orders:*:workers')]);

        $this->assertSame('rejected', $first[0]['state']);
        $this->assertSame('fulfilled', $second[0]['state'], 'the next poll went back to the dead backend');
        $this->assertNotSame($hosts[0], $hosts[1]);
        $this->assertFalse($loadBalancer->getHealthStatus()["http://{$dead}:6632"]['healthy']);
    }

    public function testAsyncFailoverDoesNotForwardCredentialsAcrossARedirect(): void
    {
        $handler = new PlanHandler([
            ['status' => 302, 'json' => []],
        ]);
        $client = new HttpClient([
            'baseUrl' => 'http://queen-a:6632',
            'bearerToken' => 'read-secret',
            'handler' => HandlerStack::create($handler),
        ]);

        try {
            $client->getAsyncWithFailover('/depth')->wait();
            $this->fail('A redirect was taken for an answer.');
        } catch (HttpException $exception) {
            $this->assertSame(302, $exception->statusCode);
        }
        $this->assertFalse($handler->options[0]['allow_redirects']);
        $this->assertSame('Bearer read-secret', $handler->requests[0]->getHeaderLine('Authorization'));
        $this->assertSame(1, $handler->count());
    }

    /**
     * A body json_encode() refuses (here a string that is not UTF-8) is the
     * caller's error: nothing was sent, so no backend is to blame and no
     * retry can help.
     */
    public function testABodyThatCannotBeEncodedFailsAtOnceAndLeavesEveryBackendHealthy(): void
    {
        $handler = new PlanHandler([], ['status' => 201, 'json' => [['status' => 'queued']]]);
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'round-robin');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'handler' => HandlerStack::create($handler),
        ]);

        $started = microtime(true);
        try {
            $client->post('/api/v1/push', ['items' => [['queue' => 'q', 'payload' => "caf\xE9"]]]);
            $this->fail('A body json_encode() refuses was sent.');
        } catch (\JsonException $exception) {
            $this->assertStringContainsString('UTF-8', $exception->getMessage());
        }

        $this->assertSame(0, $handler->count());
        $this->assertLessThan(0.5, microtime(true) - $started, 'no backoff for a request that was never sent');
        foreach ($loadBalancer->getHealthStatus() as $url => $status) {
            $this->assertTrue($status['healthy'], "{$url} was marked unhealthy for the caller's body");
        }
    }

    public function testABodyThatCannotBeEncodedIsNotRetriedAgainstASingleBackend(): void
    {
        $handler = new PlanHandler([], ['status' => 201, 'json' => [['status' => 'queued']]]);
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);

        $started = microtime(true);
        try {
            $client->post('/api/v1/push', ['items' => [['queue' => 'q', 'payload' => "caf\xE9"]]]);
            $this->fail('A body json_encode() refuses was sent.');
        } catch (\JsonException) {
        }

        $this->assertSame(0, $handler->count());
        $this->assertLessThan(0.5, microtime(true) - $started, 'the default 1 s + 2 s backoff ran');
    }

    /**
     * The client never follows a redirect (it could forward the token), so a
     * redirect answers nothing the caller asked: a gateway's http->https 307
     * must not read as a stored push.
     */
    public function testARedirectIsAnErrorThatIsNeitherRetriedNorFailedOver(): void
    {
        $handler = new PlanHandler([], ['status' => 307, 'body' => '']);
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'round-robin');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'handler' => HandlerStack::create($handler),
        ]);

        try {
            $client->post('/api/v1/push', ['items' => [['queue' => 'q', 'payload' => [], 'transactionId' => 't-1']]]);
            $this->fail('A redirect was taken for an answer.');
        } catch (HttpException $exception) {
            $this->assertSame(307, $exception->statusCode);
        }
        $this->assertSame(1, $handler->count());
        foreach ($loadBalancer->getHealthStatus() as $status) {
            $this->assertTrue($status['healthy']);
        }
    }

    public function testAPushAnsweredWithARedirectIsNotReportedAsStored(): void
    {
        $handler = new PlanHandler([], ['status' => 307, 'body' => '']);
        $queen = new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]);

        $stored = null;
        $failed = null;
        $queen->queue('orders')->push(['id' => 1])
            ->onSuccess(function (array $items) use (&$stored): void {
                $stored = $items;
            })
            ->onError(function (array $items, \Throwable $error) use (&$failed): void {
                $failed = $error;
            })
            ->execute();

        $this->assertNull($stored, 'the push was reported stored');
        $this->assertInstanceOf(HttpException::class, $failed);
        $this->assertSame(1, $handler->count());
    }

    /**
     * Push, ACK and transaction always answer a JSON body: an empty one (or a
     * 204) confirms nothing, so it must not pass for success.
     */
    #[\PHPUnit\Framework\Attributes\TestWith([200])]
    #[\PHPUnit\Framework\Attributes\TestWith([201])]
    #[\PHPUnit\Framework\Attributes\TestWith([204])]
    public function testAPushOrAnAckAnsweredWithoutABodyIsAnError(int $status): void
    {
        $handler = new PlanHandler([], ['status' => $status, 'body' => '']);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);

        $stored = null;
        $failed = null;
        $queen->queue('orders')->push(['id' => 1])
            ->onSuccess(function (array $items) use (&$stored): void {
                $stored = $items;
            })
            ->onError(function (array $items, \Throwable $error) use (&$failed): void {
                $failed = $error;
            })
            ->execute();
        $this->assertNull($stored, "a push answered {$status} without a body was reported stored");
        $this->assertNotNull($failed);

        $message = ['transactionId' => 't-1', 'partitionId' => 'p-1', 'leaseId' => 'l-1'];
        $this->assertFalse($queen->ack($message)['success'], 'a single ACK without an answer succeeded');
        $this->assertFalse($queen->ack([$message])['success'], 'a batch ACK without an answer succeeded');
    }

    public function testAnEmptyPopIsABodiless204AndAnEmpty200IsAnError(): void
    {
        $handler = new PlanHandler([
            ['status' => 204, 'body' => ''],
            ['status' => 200, 'body' => ''],
        ]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);

        $this->assertSame([], $queen->queue('orders')->wait(false)->pop());

        $this->expectException(\UnexpectedValueException::class);
        $queen->queue('orders')->wait(false)->pop();
    }

    public function testMalformedSuccessfulJsonUsesTheNormalRetryBoundary(): void
    {
        $calls = 0;
        $handler = static function () use (&$calls): \GuzzleHttp\Promise\PromiseInterface {
            $calls++;

            return new \GuzzleHttp\Promise\FulfilledPromise(new \GuzzleHttp\Psr7\Response(
                200,
                ['Content-Type' => 'application/json'],
                $calls === 1 ? '{"messages":[' : '{"messages":[]}',
            ));
        };
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'retryAttempts' => 2,
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($handler),
        ]);

        $this->assertSame(['messages' => []], $client->get('/api/v1/pop/queue/default'));
        $this->assertSame(2, $calls);
    }

    public function testMalformedSuccessfulJsonFailsClosedAfterRetriesAreExhausted(): void
    {
        $handler = static fn (): \GuzzleHttp\Promise\PromiseInterface =>
            new \GuzzleHttp\Promise\FulfilledPromise(new \GuzzleHttp\Psr7\Response(
                200,
                ['Content-Type' => 'application/json'],
                '{"messages":',
            ));
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);

        $this->expectException(\UnexpectedValueException::class);
        $this->expectExceptionMessage('malformed JSON response');

        $client->get('/api/v1/pop/queue/default');
    }
}
