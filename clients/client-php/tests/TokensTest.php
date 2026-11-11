<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Exceptions\TokenSourceException;
use Queen\Http\CurlTransport;
use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Http\TokenSource;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\QueenServiceProvider;
use Queen\MintedToken;
use Queen\Queen;
use Queen\Tests\Support\KeepAliveServer;
use Queen\Tests\Support\PlanHandler;

/**
 * Client tokens on the PHP client: `bearerToken` as a callable (TokenSource),
 * fetched again before a client token expires and after a 401, and
 * `$queen->tokens()->mint()` (POST /api/v1/tokens).
 *
 * Against PlanHandler's canned answers, as Retry429Test, and once against the
 * real cURL path (KeepAliveServer): no broker.
 */
class TokensTest extends TestCase
{
    // ===========================
    // TokenSource
    // ===========================

    public function testAFixedCredentialIsSentAsItIsAndNothingWithoutOne(): void
    {
        $fixed = new TokenSource('qk_live_x');
        $this->assertSame('qk_live_x', $fixed->get());
        $this->assertTrue($fixed->configured());
        $this->assertFalse($fixed->refreshable());

        $none = new TokenSource(null);
        $this->assertNull($none->get());
        $this->assertFalse($none->configured());
    }

    public function testTheCallableIsCalledOnceAndAgainOnlyNearItsExp(): void
    {
        $calls = 0;
        $long = new TokenSource(function () use (&$calls): string {
            $calls++;
            return self::clientToken(3600);
        });
        $long->get();
        $long->get();
        $long->get();
        $this->assertSame(1, $calls, 'a token with an hour left is kept');

        $calls = 0;
        $short = new TokenSource(function () use (&$calls): string {
            $calls++;
            return self::clientToken(10);
        });
        $short->get();
        $short->get();
        $this->assertSame(2, $calls, 'a token 10 s from its exp is fetched again');
    }

    public function testA401MetByManyRequestsRefreshesOnce(): void
    {
        $calls = 0;
        $tokens = new TokenSource(self::counting($calls));
        $first = $tokens->get();

        $again = array_map(fn (): ?string => $tokens->get($first), range(1, 4));

        $this->assertSame(2, $calls, 'one refresh for four requests refused with one token');
        $this->assertSame(array_fill(0, 4, 'token-1'), $again);
    }

    public function testTheExpiryIsReadFromAClientTokenAndNothingElse(): void
    {
        $at = TokenSource::expiresAt(self::clientToken(100));

        $this->assertNotNull($at);
        $this->assertGreaterThan(time() + 90, $at);
        $this->assertLessThanOrEqual(time() + 100, $at);
        $this->assertNull(TokenSource::expiresAt('qk_live_abc'));
        $this->assertNull(TokenSource::expiresAt('qt_not.a.jwt'));
        $this->assertNull(TokenSource::expiresAt('qt_garbage'));
        $this->assertNull(TokenSource::expiresAt('qt_a.!!!.c'));
    }

    public function testACallableThatThrowsOrReturnsNoTokenIsATokenSourceError(): void
    {
        $down = new \RuntimeException('backend down');
        foreach ([
            [fn (): string => throw $down, 'backend down', $down],
            [fn (): string => '  ', 'non-empty string', null],
            [fn (): mixed => 42, 'non-empty string', null],
            [fn (): string => 'qt_a b', 'space or a control character', null],
        ] as [$callable, $message, $previous]) {
            try {
                (new TokenSource($callable))->get();
                $this->fail('A callable without a token was taken for one.');
            } catch (TokenSourceException $error) {
                $this->assertStringStartsWith('token source: ', $error->getMessage());
                $this->assertStringContainsString($message, $error->getMessage());
                $this->assertSame($previous, $error->getPrevious());
            }
        }
    }

    // ===========================
    // HttpClient: a token callable
    // ===========================

    public function testA401FetchesANewTokenAndSendsTheRequestOnceMore(): void
    {
        $calls = 0;
        $handler = new PlanHandler([self::expired(), ['status' => 200, 'json' => ['ok' => 1]]]);
        $client = $this->clientFor($handler, self::counting($calls));

        $this->assertSame(['ok' => 1], $client->get('/api/v1/resources/queues'));
        $this->assertSame(['Bearer token-0', 'Bearer token-1'], self::authorizations($handler), 'the resend carries the new token');

        // The new token is kept: the next request fetches nothing.
        $client->get('/api/v1/resources/queues');
        $this->assertSame(2, $calls);
        $this->assertSame('Bearer token-1', self::authorizations($handler)[2]);
    }

    public function testASecond401IsTheCallers(): void
    {
        $calls = 0;
        $handler = new PlanHandler([self::expired(), self::expired(), self::expired()]);
        $client = $this->clientFor($handler, self::counting($calls));

        try {
            $client->get('/api/v1/resources/queues');
            $this->fail('A second 401 was not the caller\'s.');
        } catch (HttpException $error) {
            $this->assertSame(401, $error->statusCode);
            $this->assertSame('token_expired', $error->errorCode);
        }
        $this->assertSame(2, $handler->count(), 'one refresh, one resend');
    }

    public function testAFixedCredentialIsNotRetriedOnA401(): void
    {
        $handler = new PlanHandler([self::expired()]);
        $client = $this->clientFor($handler, 'qk_live_static');

        try {
            $client->get('/api/v1/resources/queues');
            $this->fail('A 401 was answered.');
        } catch (HttpException $error) {
            $this->assertSame(401, $error->statusCode);
        }
        $this->assertSame(['Bearer qk_live_static'], self::authorizations($handler));
    }

    public function testTheResendAfterA401UsesUpNoAttempt(): void
    {
        $calls = 0;
        $handler = new PlanHandler([
            self::expired(),
            ['status' => 429, 'retryAfter' => '0', 'json' => ['error' => 'slow down', 'code' => 'rate_limited']],
            ['status' => 200, 'json' => ['ok' => true]],
        ]);
        // Two tries in the 429 budget and one attempt in the 5xx one: the
        // resend after the 401 takes from neither.
        $client = $this->clientFor($handler, self::counting($calls), [
            'retry429' => ['maxAttempts' => 2, 'baseMs' => 1, 'capMs' => 5],
        ]);

        $this->assertSame(['ok' => true], $client->get('/api/v1/status'));
        $this->assertSame(3, $handler->count());
    }

    public function testATokenSourceFailureSendsNothingAndBlamesNoBackend(): void
    {
        $calls = 0;
        $handler = new PlanHandler();
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'round-robin');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'retryAttempts' => 3,
            'retryDelayMillis' => 0,
            'bearerToken' => function () use (&$calls): string {
                $calls++;
                throw new \RuntimeException('backend down');
            },
            'handler' => HandlerStack::create($handler),
        ]);

        foreach ([
            fn () => $client->get('/api/v1/resources/queues'),
            fn () => $client->getAsync('/api/v1/resources/queues')->wait(),
            fn () => $client->getAsyncWithFailover('/api/v1/resources/queues')->wait(),
            fn () => $client->postDetached('/api/v1/ack', ['transactionId' => 't1']),
        ] as $request) {
            try {
                $request();
                $this->fail('A request went out without a token.');
            } catch (TokenSourceException $error) {
                $this->assertStringContainsString('backend down', $error->getMessage());
            }
        }

        $this->assertSame(0, $handler->count(), 'nothing was sent');
        $this->assertSame(4, $calls, 'each request asked once: no retry, no failover');
        foreach ($loadBalancer->getHealthStatus() as $status) {
            $this->assertTrue($status['healthy'], 'no backend is to blame');
        }
    }

    public function testTheAsyncApiFetchesANewTokenAfterA401Too(): void
    {
        $calls = 0;
        $handler = new PlanHandler([
            self::expired(),
            ['status' => 200, 'json' => ['ok' => 1]],
            self::expired(),
            self::expired(),
        ]);
        $client = $this->clientFor($handler, self::counting($calls));

        $this->assertSame(['ok' => 1], $client->getAsync('/api/v1/resources/queues')->wait());
        $this->assertSame(['Bearer token-0', 'Bearer token-1'], self::authorizations($handler));

        try {
            $client->getAsyncWithFailover('/api/v1/resources/queues')->wait();
            $this->fail('A second 401 was not the caller\'s.');
        } catch (HttpException $error) {
            $this->assertSame(401, $error->statusCode);
        }
        $this->assertSame(4, $handler->count());
        $this->assertSame(3, $calls);
    }

    public function testTheCurlTransportSendsTheTokenOfEachAttempt(): void
    {
        if (!CurlTransport::isAvailable() || !function_exists('proc_open')) {
            $this->markTestSkipped('Needs ext-curl and proc_open.');
        }
        $server = new KeepAliveServer();
        $calls = 0;
        $client = new HttpClient([
            'baseUrl' => $server->url,
            'timeoutMillis' => 5_000,
            'bearerToken' => self::counting($calls),
        ]);
        if ((new \ReflectionProperty($client, 'transport'))->getValue($client) === null) {
            $this->markTestSkipped('A proxy variable or QUEEN_SDK_HTTP_TRANSPORT keeps Guzzle here.');
        }

        try {
            $client->get('/status/401');
            $this->fail('A second 401 was not the caller\'s.');
        } catch (HttpException $error) {
            $this->assertSame(401, $error->statusCode);
        }

        $this->assertSame(['GET /status/401', 'GET /status/401'], $client->get('/received'));
        $this->assertSame('Bearer token-1', $client->get('/echo')['headers']['authorization'], 'the new token is kept');
        $this->assertSame(2, $calls);
    }

    // ===========================
    // Queen: one source per client
    // ===========================

    public function testOneCallableServesEveryRequestOfAClient(): void
    {
        $calls = 0;
        $handler = new PlanHandler([], ['status' => 200, 'json' => ['messages' => []]]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'bearerToken' => self::counting($calls),
            // A configured credential wins over a stale header in any case,
            // as a fixed bearerToken does.
            'headers' => ['authorization' => 'Basic stale'],
            'handler' => HandlerStack::create($handler),
        ]);

        $queen->queue('commands')->partition('dev1')->wait(false)->pop();
        $queen->kv()->get('devices', 'dev1');
        $queen->admin()->getQueueDepthAsync('commands')->wait();
        $queen->ackDetached(['transactionId' => 't1', 'partitionId' => 'p1', 'leaseId' => 'l1']);

        $this->assertSame(1, $calls);
        $this->assertSame(array_fill(0, 4, 'Bearer token-0'), self::authorizations($handler));

        // A TokenSource given to two clients is one source.
        $calls = 0;
        $shared = new TokenSource(self::counting($calls));
        $config = ['url' => 'http://queen.test:6632', 'bearerToken' => $shared, 'handler' => HandlerStack::create($handler)];
        (new Queen($config))->admin()->health();
        (new Queen($config))->admin()->health();
        $this->assertSame(1, $calls);
    }

    public function testBearerTokenTakesAStringACallableOrATokenSource(): void
    {
        $holder = new class {
            public function __invoke(): string
            {
                return 'token-invoked';
            }

            public function token(): string
            {
                return 'token-method';
            }
        };
        foreach ([
            // A string is the token itself, even one that names a function.
            ['strtoupper', 'Bearer strtoupper'],
            [$holder, 'Bearer token-invoked'],
            [[$holder, 'token'], 'Bearer token-method'],
            [$holder->token(...), 'Bearer token-method'],
            [new TokenSource(fn (): string => 'token-shared'), 'Bearer token-shared'],
        ] as [$bearerToken, $sent]) {
            $handler = new PlanHandler();
            (new Queen([
                'url' => 'http://queen.test:6632',
                'bearerToken' => $bearerToken,
                'handler' => HandlerStack::create($handler),
            ]))->admin()->health();
            $this->assertSame([$sent], self::authorizations($handler));
        }

        foreach ([42, [new \stdClass(), 'missing'], new \stdClass(), '', 'tok en'] as $bearerToken) {
            try {
                new Queen(['url' => 'http://queen.test:6632', 'bearerToken' => $bearerToken]);
                $this->fail('An unusable bearerToken was accepted.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('bearerToken must be', $error->getMessage());
            }
        }
    }

    public function testLaravelConfigurationKeepsTheBearerTokenAString(): void
    {
        $callable = fn (): string => 'token-0';
        foreach ([
            fn () => (new QueenConnector())->connect(['url' => 'http://queen.test:6632', 'bearer_token' => $callable]),
            fn () => QueenServiceProvider::clientConfig([
                'url' => 'http://queen.test:6632',
                'bearer_token' => $callable,
                'timeout' => 30000,
                'retry_attempts' => 3,
                'load_balancing_strategy' => 'affinity',
            ]),
        ] as $configure) {
            try {
                $configure();
                $this->fail('A token callable was taken from configuration.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('Queen Laravel bearer_token must be a string or null', $error->getMessage());
            }
        }
    }

    // ===========================
    // tokens()->mint()
    // ===========================

    public function testMintPostsTheEntityWithTheConfiguredCredentialAndReadsTheAnswer(): void
    {
        $answer = [
            'token' => self::clientToken(60, 'dev-8f3a'),
            'sub' => 'dev-8f3a',
            'key' => 'k1',
            'expiresAt' => '2026-11-11T12:00:00+00:00',
            'expiresIn' => 60,
        ];
        $handler = new PlanHandler([['status' => 201, 'json' => $answer]]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'bearerToken' => 'qk_live_role',
            'handler' => HandlerStack::create($handler),
        ]);

        $minted = $queen->tokens()->mint('dev-8f3a', ['ttlSeconds' => 60]);

        $this->assertInstanceOf(MintedToken::class, $minted);
        $this->assertSame(
            [$answer['token'], 'dev-8f3a', 'k1', '2026-11-11T12:00:00+00:00', 60],
            [$minted->token, $minted->sub, $minted->key, $minted->expiresAt, $minted->expiresIn],
        );
        $request = $handler->requests[0];
        $this->assertSame(['POST', '/api/v1/tokens'], [$request->getMethod(), $request->getUri()->getPath()]);
        $this->assertSame('{"sub":"dev-8f3a","ttlSeconds":60}', (string) $request->getBody());
        $this->assertSame('Bearer qk_live_role', $request->getHeaderLine('Authorization'));
        $this->assertSame($queen->tokens(), $queen->tokens(), 'one Tokens per client, like admin()');
    }

    public function testMintSendsTheKeyAndTheGrantsAsGiven(): void
    {
        $handler = new PlanHandler([['status' => 201, 'json' => [
            'token' => 'qt_a.b.c',
            'sub' => 'dev-8f3a',
            'key' => 'k1',
            'expiresAt' => '2026-11-11T12:00:00+00:00',
            'expiresIn' => 3600,
        ]]]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'bearerToken' => 'qk_live_admin',
            'handler' => HandlerStack::create($handler),
        ]);
        $allow = [['queue' => 'telemetry', 'partition' => '{sub}', 'ops' => ['produce']]];

        $queen->tokens()->mint('dev-8f3a', ['key' => 'devices', 'allow' => $allow]);

        $this->assertSame(
            ['sub' => 'dev-8f3a', 'key' => 'devices', 'allow' => $allow],
            json_decode((string) $handler->requests[0]->getBody(), true),
        );
    }

    public function testMintRefusesAnEmptySubAndUnknownOptionsBeforeSending(): void
    {
        $handler = new PlanHandler();
        $queen = new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]);

        foreach ([
            ['', [], 'sub is required'],
            ['dev-8f3a', ['ttl' => 60], 'unknown option `ttl`'],
            ['dev-8f3a', ['ttlSeconds' => 0], 'ttlSeconds'],
            ['dev-8f3a', ['ttlSeconds' => '60'], 'ttlSeconds'],
            ['dev-8f3a', ['key' => ''], 'key names a scoped key'],
            ['dev-8f3a', ['allow' => ['queue' => 'telemetry', 'ops' => ['produce']]], 'allow is a list of grants'],
        ] as [$sub, $opts, $message]) {
            try {
                $queen->tokens()->mint($sub, $opts);
                $this->fail('An invalid mint was sent.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString($message, $error->getMessage());
            }
        }
        $this->assertSame(0, $handler->count());
    }

    public function testMintRefusesAnAnswerThatIsNotAMintedToken(): void
    {
        $handler = new PlanHandler([['status' => 201, 'json' => ['token' => 'qt_secret', 'sub' => 'dev-8f3a']]]);
        $queen = new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]);

        try {
            $queen->tokens()->mint('dev-8f3a');
            $this->fail('A partial answer was taken for a minted token.');
        } catch (\UnexpectedValueException $error) {
            $this->assertStringNotContainsString('qt_secret', $error->getMessage(), 'the answer may hold the token');
        }
    }

    // ===========================
    // Helpers
    // ===========================

    /** A client token whose `exp` is $secondsFromNow away; the signature is fake. */
    private static function clientToken(int $secondsFromNow, string $sub = 'dev1'): string
    {
        $segment = static fn (array $part): string => rtrim(strtr(base64_encode(json_encode($part)), '+/', '-_'), '=');

        return 'qt_' . $segment(['alg' => 'HS256'])
            . '.' . $segment(['sub' => $sub, 'exp' => time() + $secondsFromNow, 'typ' => 'client'])
            . '.sig';
    }

    /** A token callable answering token-0, token-1, ... and counting its calls. */
    private static function counting(int &$calls): \Closure
    {
        return function () use (&$calls): string {
            return 'token-' . $calls++;
        };
    }

    private static function expired(): array
    {
        return ['status' => 401, 'json' => ['error' => 'client token expired', 'code' => 'token_expired']];
    }

    /** @return list<string> the Authorization header of every request, in order */
    private static function authorizations(PlanHandler $handler): array
    {
        return array_map(
            static fn ($request): string => $request->getHeaderLine('Authorization'),
            $handler->requests,
        );
    }

    private function clientFor(PlanHandler $handler, string|callable $bearerToken, array $options = []): HttpClient
    {
        // One attempt: the resend after a 401 must not need a second.
        return new HttpClient($options + [
            'baseUrl' => 'http://queen.test:6632',
            'bearerToken' => $bearerToken,
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);
    }
}
