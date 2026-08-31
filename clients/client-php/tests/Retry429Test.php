<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\ErrorCode;
use Queen\Exceptions\HttpException;
use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Http\Retry429Policy;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * Proxy rate-limiting contract:
 *
 *   429  Retry-After: <seconds>  {"error": "...", "code": "rate_limited" | "quota_exceeded"}
 *   403                          {"error": "...", "code": "cluster_suspended" | "storage_quota_exceeded"
 *                                                        | "feature_gated" | "forbidden"}
 *
 * 429 is retried in place with jittered backoff; 403 is terminal and must
 * surface its code. Timings use tiny base/cap values so the suite stays fast.
 */
class Retry429Test extends TestCase
{
    // ===========================
    // Retry429Policy (pure)
    // ===========================

    public function testOrdinaryRequestsAreBoundedByDefault(): void
    {
        $policy = Retry429Policy::forKind([]);

        $this->assertSame(Retry429Policy::DEFAULT_MAX_ATTEMPTS, $policy->maxAttempts);
        $this->assertSame(Retry429Policy::DEFAULT_BASE_MILLIS, $policy->baseMillis);
        $this->assertSame(Retry429Policy::DEFAULT_CAP_MILLIS, $policy->capMillis);
        $this->assertFalse($policy->isExhausted(9));
        $this->assertTrue($policy->isExhausted(10));
    }

    public function testLongPollPopIsUnboundedByDefault(): void
    {
        $policy = Retry429Policy::forKind([], Retry429Policy::KIND_POP);

        $this->assertSame(Retry429Policy::UNBOUNDED, $policy->maxAttempts);
        $this->assertFalse($policy->isExhausted(10_000));
    }

    public function testExplicitMaxAttemptsAppliesToBothKinds(): void
    {
        $config = ['maxAttempts' => 3];

        $this->assertSame(3, Retry429Policy::forKind($config)->maxAttempts);
        $this->assertSame(3, Retry429Policy::forKind($config, Retry429Policy::KIND_POP)->maxAttempts);
    }

    public function testZeroConfigValuesFallBackToDefaults(): void
    {
        $policy = Retry429Policy::forKind(['maxAttempts' => 0, 'baseMs' => 0, 'capMs' => 0]);

        $this->assertSame(Retry429Policy::DEFAULT_MAX_ATTEMPTS, $policy->maxAttempts);
        $this->assertSame(Retry429Policy::DEFAULT_BASE_MILLIS, $policy->baseMillis);
        $this->assertSame(Retry429Policy::DEFAULT_CAP_MILLIS, $policy->capMillis);
    }

    public function testDelayGrowsExponentiallyWithinJitterBounds(): void
    {
        $policy = new Retry429Policy(10, 500, 30000);

        foreach ([0 => 500, 1 => 1000, 2 => 2000, 3 => 4000] as $attemptIndex => $expected) {
            $delay = $policy->delayMillis($attemptIndex);
            $this->assertGreaterThanOrEqual((int) ($expected * 0.8), $delay);
            $this->assertLessThanOrEqual((int) ceil($expected * 1.2), $delay);
        }
    }

    public function testDelayIsCappedAndJitterStaysWithinTwentyPercent(): void
    {
        $policy = new Retry429Policy(10, 500, 30000);

        for ($i = 0; $i < 50; $i++) {
            $delay = $policy->delayMillis(20);
            $this->assertGreaterThanOrEqual(24000, $delay);
            $this->assertLessThanOrEqual(36000, $delay);
        }
    }

    public function testRetryAfterWinsOverComputedBackoff(): void
    {
        $policy = new Retry429Policy(10, 30000, 60000);

        $delay = $policy->delayMillis(5, 2.0);

        $this->assertGreaterThanOrEqual(1600, $delay);
        $this->assertLessThanOrEqual(2400, $delay);
    }

    public function testRetryAfterIsCappedBeforeJitterToProtectWorkerSleep(): void
    {
        $policy = new Retry429Policy(10, 500, 30000);

        $delay = $policy->delayMillis(0, PHP_FLOAT_MAX);

        $this->assertGreaterThanOrEqual(24000, $delay);
        $this->assertLessThanOrEqual(36000, $delay);
    }

    public function testConfiguredDelayCapHasAProductionSafetyCeiling(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('retry429.capMs must be between 1 and 300000');

        Retry429Policy::forKind(['capMs' => Retry429Policy::MAX_CAP_MILLIS + 1]);
    }

    public function testNegativeRetryAfterFallsBackToBackoff(): void
    {
        $policy = new Retry429Policy(10, 500, 30000);

        $delay = $policy->delayMillis(0, -5.0);

        $this->assertGreaterThanOrEqual(400, $delay);
        $this->assertLessThanOrEqual(600, $delay);
    }

    // ===========================
    // HttpClient: the retry loop
    // ===========================

    public function testRetriesRateLimitedRequestThenSucceeds(): void
    {
        $handler = new PlanHandler([
            self::rateLimited('0'),
            ['status' => 200, 'json' => ['ok' => true]],
        ]);
        $client = $this->clientFor($handler, ['baseMs' => 5, 'capMs' => 50]);

        $result = $client->get('/api/v1/status');

        $this->assertSame(['ok' => true], $result);
        $this->assertSame(2, $handler->count());
    }

    public function testHonorsRetryAfterHeaderInsteadOfLongBackoff(): void
    {
        $handler = new PlanHandler([
            self::rateLimited('0'),
            ['status' => 200, 'json' => ['ok' => true]],
        ]);
        // A computed backoff would sleep ~5s here; Retry-After: 0 must win.
        $client = $this->clientFor($handler, ['baseMs' => 5000, 'capMs' => 30000]);

        $started = microtime(true);
        $client->get('/api/v1/status');
        $elapsed = microtime(true) - $started;

        $this->assertSame(2, $handler->count());
        $this->assertLessThan(1.0, $elapsed);
    }

    public function testBacksOffExponentiallyWhenNoRetryAfter(): void
    {
        $handler = new PlanHandler([
            self::rateLimited(),
            self::rateLimited(),
            ['status' => 200, 'json' => ['ok' => true]],
        ]);
        $client = $this->clientFor($handler, ['baseMs' => 60, 'capMs' => 2000]);

        $started = microtime(true);
        $client->get('/api/v1/status');
        $elapsed = microtime(true) - $started;

        $this->assertSame(3, $handler->count());
        // 60ms + 120ms, minus the -20% jitter floor.
        $this->assertGreaterThanOrEqual(0.14, $elapsed);
    }

    public function testOrdinaryRequestGivesUpAfterTenAttempts(): void
    {
        $handler = new PlanHandler([], self::rateLimited());
        $client = $this->clientFor($handler, ['baseMs' => 1, 'capMs' => 5]);

        try {
            $client->post('/api/v1/push', ['items' => []]);
            $this->fail('expected the exhausted 429 to surface');
        } catch (HttpException $error) {
            $this->assertSame(429, $error->statusCode);
            $this->assertSame(ErrorCode::RATE_LIMITED, $error->errorCode);
            $this->assertTrue($error->isRateLimited());
        }

        $this->assertSame(Retry429Policy::DEFAULT_MAX_ATTEMPTS, $handler->count());
    }

    public function testLongPollPopRetriesPastTheBoundedBudget(): void
    {
        $plan = array_fill(0, 14, self::rateLimited('0'));
        $plan[] = ['status' => 200, 'json' => ['messages' => [['id' => 1]]]];
        $handler = new PlanHandler($plan);
        $client = $this->clientFor($handler, ['baseMs' => 1, 'capMs' => 5]);

        $result = $client->get('/api/v1/pop/queue/orders', null, null, Retry429Policy::KIND_POP);

        $this->assertSame(15, $handler->count());
        $this->assertCount(1, $result['messages']);
    }

    public function testExplicitMaxAttemptsCapsEvenLongPollPop(): void
    {
        $handler = new PlanHandler([], self::rateLimited());
        $client = $this->clientFor($handler, ['maxAttempts' => 2, 'baseMs' => 1, 'capMs' => 5]);

        $this->expectException(HttpException::class);

        try {
            $client->get('/api/v1/pop/queue/orders', null, null, Retry429Policy::KIND_POP);
        } finally {
            $this->assertSame(2, $handler->count());
        }
    }

    public function testNonFiniteRetryAfterIsNotExposedAsAValidDelay(): void
    {
        $handler = new PlanHandler([], self::rateLimited('1e309'));
        $client = $this->clientFor($handler, ['maxAttempts' => 1, 'baseMs' => 1, 'capMs' => 5]);

        try {
            $client->get('/api/v1/status');
            $this->fail('expected an exhausted 429 response');
        } catch (HttpException $error) {
            $this->assertNull($error->retryAfterSeconds);
        }
    }

    public function testQuotaExceededIsRetriedLikeRateLimited(): void
    {
        $handler = new PlanHandler([
            ['status' => 429, 'retryAfter' => '0', 'json' => ['error' => 'over quota', 'code' => ErrorCode::QUOTA_EXCEEDED]],
            ['status' => 200, 'json' => ['ok' => true]],
        ]);
        $client = $this->clientFor($handler, ['baseMs' => 1, 'capMs' => 5]);

        $client->get('/api/v1/status');

        $this->assertSame(2, $handler->count());
    }

    // ===========================
    // HttpClient: terminal 403s
    // ===========================

    public function testClusterSuspendedIsTerminalAndTyped(): void
    {
        $handler = new PlanHandler([], [
            'status' => 403,
            'json' => ['error' => 'cluster suspended', 'code' => ErrorCode::CLUSTER_SUSPENDED],
        ]);
        $client = $this->clientFor($handler, ['baseMs' => 1, 'capMs' => 5]);

        try {
            $client->get('/api/v1/status');
            $this->fail('expected a terminal 403');
        } catch (HttpException $error) {
            $this->assertSame(403, $error->statusCode);
            $this->assertSame(ErrorCode::CLUSTER_SUSPENDED, $error->errorCode);
            $this->assertTrue($error->isClusterSuspended());
            $this->assertSame('cluster suspended', $error->getMessage());
            $this->assertNull($error->retryAfterSeconds);
        }

        $this->assertSame(1, $handler->count());
    }

    public function testOtherForbiddenCodesSurfaceOnFirstAttempt(): void
    {
        $codes = [ErrorCode::STORAGE_QUOTA_EXCEEDED, ErrorCode::FEATURE_GATED, ErrorCode::FORBIDDEN];

        foreach ($codes as $code) {
            $handler = new PlanHandler([], ['status' => 403, 'json' => ['error' => 'denied', 'code' => $code]]);
            $client = $this->clientFor($handler, ['baseMs' => 1, 'capMs' => 5]);

            try {
                $client->post('/api/v1/push', ['items' => []]);
                $this->fail("expected a terminal 403 for {$code}");
            } catch (HttpException $error) {
                $this->assertSame($code, $error->errorCode);
                $this->assertFalse($error->isClusterSuspended());
            }

            $this->assertSame(1, $handler->count());
        }
    }

    public function testErrorWithoutCodeLeavesErrorCodeNull(): void
    {
        $handler = new PlanHandler([], ['status' => 404, 'json' => ['error' => 'queue not found']]);
        $client = $this->clientFor($handler, []);

        try {
            $client->get('/api/v1/resources/queues/nope');
            $this->fail('expected a 404');
        } catch (HttpException $error) {
            $this->assertSame(404, $error->statusCode);
            $this->assertNull($error->errorCode);
            $this->assertNull($error->retryAfterSeconds);
        }
    }

    // ===========================
    // HttpClient: failover boundary
    // ===========================

    public function testRateLimitNeverFailsOverToAnotherBackend(): void
    {
        $handler = new PlanHandler([], self::rateLimited());
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'round-robin');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'enableFailover' => true,
            'retry429' => ['maxAttempts' => 3, 'baseMs' => 1, 'capMs' => 5],
            'handler' => HandlerStack::create($handler),
        ]);

        try {
            $client->get('/api/v1/status');
            $this->fail('expected the exhausted 429 to surface');
        } catch (HttpException $error) {
            $this->assertSame(429, $error->statusCode);
        }

        // Every attempt stayed on the backend the balancer first picked, and
        // rate limiting left both backends marked healthy.
        $this->assertSame(3, $handler->count());
        $this->assertCount(1, array_unique($handler->hosts()));
        foreach ($loadBalancer->getHealthStatus() as $status) {
            $this->assertTrue($status['healthy']);
        }
    }

    public function testServerErrorStillFailsOver(): void
    {
        $handler = new PlanHandler([
            ['status' => 500, 'json' => ['error' => 'boom']],
            ['status' => 200, 'json' => ['ok' => true]],
        ]);
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'round-robin');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'enableFailover' => true,
            'handler' => HandlerStack::create($handler),
        ]);

        $result = $client->get('/api/v1/status');

        $this->assertSame(['ok' => true], $result);
        $this->assertSame(['queen-a', 'queen-b'], $handler->hosts());
    }

    public function testGetRetry429PolicyExposesTheResolvedPolicy(): void
    {
        $client = new HttpClient([
            'baseUrl' => 'http://localhost:6632',
            'retry429' => ['baseMs' => 25],
        ]);

        $this->assertSame(25, $client->getRetry429Policy()->baseMillis);
        $this->assertSame(Retry429Policy::DEFAULT_MAX_ATTEMPTS, $client->getRetry429Policy()->maxAttempts);
        $this->assertSame(Retry429Policy::UNBOUNDED, $client->getRetry429Policy(Retry429Policy::KIND_POP)->maxAttempts);
    }

    // ===========================
    // Queen wiring: config + call-site marking
    // ===========================

    public function testQueenConfigReachesTheRetryLoop(): void
    {
        $handler = new PlanHandler([], self::rateLimited());
        $queen = new Queen([
            'url' => 'http://localhost:6632',
            'retry429' => ['maxAttempts' => 2, 'baseMs' => 1, 'capMs' => 5],
            'handler' => HandlerStack::create($handler),
        ]);

        $this->expectException(HttpException::class);

        try {
            $queen->queue('orders')->wait(false)->pop();
        } finally {
            $this->assertSame(2, $handler->count());
        }
    }

    public function testWaitingPopIsMarkedAsLongPoll(): void
    {
        $plan = array_fill(0, 12, self::rateLimited('0'));
        $plan[] = ['status' => 200, 'json' => ['messages' => [['id' => 1], ['id' => 2]]]];
        $handler = new PlanHandler($plan);
        $queen = new Queen([
            'url' => 'http://localhost:6632',
            'retry429' => ['baseMs' => 1, 'capMs' => 5],
            'handler' => HandlerStack::create($handler),
        ]);

        // wait=true by default: past the bounded 10-attempt budget.
        $messages = $queen->queue('orders')->pop();

        $this->assertSame(13, $handler->count());
        $this->assertCount(2, $messages);
    }

    // ===========================
    // Helpers
    // ===========================

    private function clientFor(PlanHandler $handler, array $retry429): HttpClient
    {
        return new HttpClient([
            'baseUrl' => 'http://localhost:6632',
            'retry429' => $retry429,
            'handler' => HandlerStack::create($handler),
        ]);
    }

    private static function rateLimited(?string $retryAfter = null): array
    {
        $descriptor = ['status' => 429, 'json' => ['error' => 'slow down', 'code' => ErrorCode::RATE_LIMITED]];
        if ($retryAfter !== null) {
            $descriptor['retryAfter'] = $retryAfter;
        }

        return $descriptor;
    }
}
