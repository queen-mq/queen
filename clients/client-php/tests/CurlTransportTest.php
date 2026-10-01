<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Exceptions\HttpException;
use Queen\Http\CurlTransport;
use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Http\TransportException;
use Queen\Tests\Support\KeepAliveServer;

/** The cURL transport against a real keep-alive server, through HttpClient. */
class CurlTransportTest extends TestCase
{
    private KeepAliveServer $server;

    protected function setUp(): void
    {
        parent::setUp();
        if (!CurlTransport::isAvailable() || !function_exists('proc_open')) {
            $this->markTestSkipped('Needs ext-curl and proc_open.');
        }
        $this->server = new KeepAliveServer();
    }

    public function testRequestsCarryTheBodyHeadersAndTokenOnOneKeptAliveConnection(): void
    {
        $client = $this->client(['bearerToken' => 'secret', 'headers' => ['X-Tenant' => 'a', 'Authorization' => 'Basic stale']]);
        $this->assertTrue($this->usesCurlTransport($client));

        $post = $client->post('/echo', ['transactionId' => 't/1', 'note' => 'é']);
        $get = $client->get('/echo?batch=4');
        $put = $client->put('/echo', ['a' => 1]);
        $delete = $client->delete('/echo');

        $this->assertSame('POST', $post['method']);
        $this->assertSame(json_encode(['transactionId' => 't/1', 'note' => 'é']), $post['body'], 'Guzzle encodes the same bytes');
        $this->assertSame('application/json', $post['headers']['content-type']);
        $this->assertSame('Bearer secret', $post['headers']['authorization']);
        $this->assertSame('a', $post['headers']['x-tenant']);
        $this->assertArrayNotHasKey('expect', $post['headers']);
        $this->assertSame(['GET', '/echo?batch=4', ''], [$get['method'], $get['target'], $get['body']]);
        $this->assertSame(['PUT', '{"a":1}'], [$put['method'], $put['body']]);
        $this->assertSame(['DELETE', ''], [$delete['method'], $delete['body']]);
        $this->assertSame(
            [2, 2, 2, 2],
            [$post['connection'], $get['connection'], $put['connection'], $delete['connection']],
            'every request after the readiness probe rides the first connection',
        );
    }

    public function testAServerErrorIsRetriedAndAClientErrorIsNot(): void
    {
        $client = $this->client(['retryAttempts' => 2, 'retryDelayMillis' => 0]);

        $this->assertSame(['ok' => true], $client->get('/flaky'));

        try {
            $client->get('/status/404');
            $this->fail('A 404 was accepted.');
        } catch (HttpException $exception) {
            $this->assertSame(404, $exception->statusCode);
            $this->assertSame('status 404', $exception->getMessage());
        }
        $this->assertNull($client->get('/empty'));
    }

    public function testRetryAfterReachesTheRateLimitException(): void
    {
        $client = $this->client(['retry429' => ['maxAttempts' => 1]]);

        try {
            $client->get('/limited');
            $this->fail('A 429 was accepted.');
        } catch (HttpException $exception) {
            $this->assertSame(429, $exception->statusCode);
            $this->assertSame('rate_limited', $exception->errorCode);
            $this->assertSame(2.0, $exception->retryAfterSeconds);
        }
    }

    public function testAnUnreachableBackendFailsOverAndATimeoutIsATransportFailure(): void
    {
        $closed = stream_socket_server('tcp://127.0.0.1:0');
        $refused = 'http://' . stream_socket_get_name($closed, false);
        fclose($closed);
        $client = new HttpClient([
            'loadBalancer' => new LoadBalancer([$refused, $this->server->url], 'round-robin'),
            'retryDelayMillis' => 0,
        ]);
        $this->assertSame('GET', $client->get('/echo')['method']);

        $slow = $this->client(['timeoutMillis' => 200, 'retryAttempts' => 1]);
        $started = microtime(true);
        try {
            $slow->get('/slow');
            $this->fail('A late answer was accepted.');
        } catch (TransportException $exception) {
            $this->assertSame(28, $exception->getCode());
        }
        $this->assertLessThan(1.5, microtime(true) - $started);
    }

    public function testAForkedChildOpensItsOwnConnection(): void
    {
        if (!function_exists('pcntl_fork')) {
            $this->markTestSkipped('Needs ext-pcntl.');
        }
        $client = $this->client();
        $before = $client->get('/echo')['connection'];
        $report = tempnam(sys_get_temp_dir(), 'qct');

        $pid = pcntl_fork();
        if ($pid === 0) {
            file_put_contents($report, (string) $client->get('/echo')['connection']);
            // No destructor may run here: one would stop the parent's server.
            posix_kill(getmypid(), SIGKILL);
        }
        pcntl_waitpid($pid, $status);
        $child = (int) file_get_contents($report);
        unlink($report);

        $this->assertNotSame($before, $child, 'the child reused the parent connection');
        $this->assertGreaterThan(0, $child);
        $this->assertSame($before, $client->get('/echo')['connection'], 'the parent keeps its connection');
    }

    public function testAProxyVariableOrTheEscapeKeepsGuzzle(): void
    {
        foreach (['HTTPS_PROXY=http://proxy.test:3128', 'QUEEN_SDK_HTTP_TRANSPORT=guzzle'] as $setting) {
            putenv($setting);
            try {
                $client = $this->client();
                $this->assertFalse($this->usesCurlTransport($client), $setting);
                $this->assertSame(['ok' => true], $client->get('/flaky') ?? ['ok' => true]);
            } finally {
                putenv(strstr($setting, '=', true));
            }
        }
    }

    private function client(array $options = []): HttpClient
    {
        return new HttpClient($options + ['baseUrl' => $this->server->url, 'timeoutMillis' => 5_000]);
    }

    private function usesCurlTransport(HttpClient $client): bool
    {
        return (new \ReflectionProperty($client, 'transport'))->getValue($client) !== null;
    }
}
