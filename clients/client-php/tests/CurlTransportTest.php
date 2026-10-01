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
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            $this->markTestSkipped('Needs ext-pcntl and ext-posix.');
        }
        $client = $this->client();
        $before = $client->get('/echo')['connection'];
        $report = tempnam(sys_get_temp_dir(), 'qct');

        $pid = pcntl_fork();
        if ($pid === 0) {
            try {
                file_put_contents($report, (string) $client->get('/echo')['connection']);
            } finally {
                // No destructor or test may run here: one would stop the parent's server.
                posix_kill(getmypid(), SIGKILL);
            }
        }
        pcntl_waitpid($pid, $status);
        $child = (int) file_get_contents($report);
        unlink($report);

        $this->assertNotSame($before, $child, 'the child reused the parent connection');
        $this->assertGreaterThan(0, $child);
        $this->assertSame($before, $client->get('/echo')['connection'], 'the parent keeps its connection');
    }

    /**
     * libcurl's resolver threads do not survive fork(): a child freeing an
     * inherited handle at exit waited for them forever. Resolving an IP
     * literal uses them where libcurl resolves literals (macOS); a broker
     * host name always does.
     */
    #[\PHPUnit\Framework\Attributes\TestWith(['sync'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['detached'])]
    public function testAForkedChildExitsRightAfterTheParentOpenedItsConnections(string $mode): void
    {
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            $this->markTestSkipped('Needs ext-pcntl and ext-posix.');
        }

        $pipes = [];
        $worker = proc_open(
            [PHP_BINARY, __DIR__ . '/Fixtures/CurlForkedChildWorker.php', $mode],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            null,
            null,
            ['bypass_shell' => true],
        );
        $this->assertIsResource($worker);
        fclose($pipes[0]);
        $stdout = stream_get_contents($pipes[1]);
        $stderr = trim((string) stream_get_contents($pipes[2]));
        fclose($pipes[1]);
        fclose($pipes[2]);

        $this->assertSame(0, proc_close($worker), $stderr);
        $this->assertTrue(
            json_decode((string) $stdout, true, 512, JSON_THROW_ON_ERROR)['child_exited'],
            'the forked child hung at exit, waiting for resolver threads it does not have',
        );
    }

    public function testAProxyVariableOrTheEscapeKeepsGuzzle(): void
    {
        foreach (['HTTPS_PROXY=http://proxy.test:3128', 'QUEEN_SDK_HTTP_TRANSPORT=guzzle'] as $setting) {
            putenv($setting);
            try {
                $client = $this->client();
                $this->assertFalse($this->usesCurlTransport($client), $setting);
                $this->assertSame('GET', $client->get('/echo')['method']);
                $pending = $client->postDetached('/echo', ['a' => 1]);
                $this->assertSame('POST', $client->settleDetached($pending)['method'], 'the Guzzle detached path');
            } finally {
                putenv(strstr($setting, '=', true));
            }
        }
    }

    public function testCompressedMalformedAndBodylessAnswersBehaveAsWithGuzzle(): void
    {
        $client = $this->client(['retryAttempts' => 1]);

        $this->assertSame(['compressed' => true], $client->get('/gzip'));
        $put = $client->put('/echo');
        $this->assertSame(['PUT', '0'], [$put['method'], $put['headers']['content-length'] ?? null]);
        $this->expectException(\UnexpectedValueException::class);
        $client->get('/malformed');
    }

    public function testHeaderValuesAreSentEmptyButNeverWithALineBreak(): void
    {
        $empty = $this->client(['headers' => ['X-Empty' => '']])->get('/echo');
        $this->assertSame('', $empty['headers']['x-empty'] ?? null);

        $this->expectException(\InvalidArgumentException::class);
        $this->client(['bearerToken' => "secret\n"])->get('/echo');
    }

    public function testDetachedRequestsSettleWaitFailAndFreeTheirHandle(): void
    {
        $client = $this->client();
        $this->assertSame('POST', $client->postDetached('/echo', ['a' => 1])->wait()['method'], 'wait() settles');

        $closed = stream_socket_server('tcp://127.0.0.1:0');
        $refused = new HttpClient(['baseUrl' => 'http://' . stream_socket_get_name($closed, false)]);
        fclose($closed);
        try {
            $refused->settleDetached($refused->postDetached('/echo', ['a' => 1]), 2_000);
            $this->fail('A refused connection was answered.');
        } catch (TransportException $exception) {
            $this->assertSame(7, $exception->getCode());
            $this->assertInstanceOf(\GuzzleHttp\Exception\GuzzleException::class, $exception);
        }

        // A dropped request leaves nothing behind in the shared multi handle.
        $client->postDetached('/slow', []);
        gc_collect_cycles();
        $transport = (new \ReflectionProperty($client, 'transport'))->getValue($client);
        $this->assertSame([], (new \ReflectionProperty($transport, 'finished'))->getValue($transport));
        $this->assertSame('GET', $client->settleDetached($client->getDetached('/echo'))['method']);
    }

    /**
     * With prefetch, an ACK sent detached is settled only after the next job:
     * if a new connection held it back until then, a hard kill during that
     * job would run the acknowledged job again.
     */
    #[\PHPUnit\Framework\Attributes\TestWith(['curl'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['guzzle'])]
    public function testADetachedRequestOnANewConnectionReachesTheServerBeforeItIsSettled(string $transport): void
    {
        if ($transport === 'guzzle') {
            putenv('QUEEN_SDK_HTTP_TRANSPORT=guzzle');
        }
        try {
            $client = $this->client();
            $this->assertSame($transport === 'curl', $this->usesCurlTransport($client));

            // The first detached request of a client opens its own connection.
            $ack = $client->postDetached('/echo?ack=1', ['transactionId' => 't-1']);
            $pop = $transport === 'curl' ? $client->getDetached('/echo?pop=1') : null;
            // The next job runs: nothing settles the requests meanwhile.
            usleep(300_000);
            $received = $this->client()->get('/received');

            $this->assertContains('POST /echo?ack=1', $received, 'the ACK waited for settle()');
            if ($pop !== null) {
                $this->assertContains('GET /echo?pop=1', $received, 'the pop sent ahead waited for settle()');
                $this->assertSame('GET', $client->settleDetached($pop)['method']);
            }
            $this->assertSame('POST', $client->settleDetached($ack)['method']);
        } finally {
            putenv('QUEEN_SDK_HTTP_TRANSPORT');
        }
    }

    public function testEveryRequestAsksForTcpKeepAlive(): void
    {
        $options = new \ReflectionMethod(CurlTransport::class, 'options');
        $retryAfter = [];
        foreach ([[5_000, 5_000], [0, 0]] as [$timeout, $connectTimeout]) {
            $set = $options->invokeArgs(new CurlTransport(), ['GET', $this->server->url, [], null, $timeout, $connectTimeout, &$retryAfter]);
            $this->assertSame(1, $set[CURLOPT_TCP_KEEPALIVE]);
            $this->assertSame(30, $set[CURLOPT_TCP_KEEPIDLE]);
            $this->assertSame(15, $set[CURLOPT_TCP_KEEPINTVL]);
        }
    }

    public function testAnIdleConnectionRunsTheKernelKeepAliveTimer(): void
    {
        if (!is_readable('/proc/net/tcp')) {
            $this->markTestSkipped('Needs Linux /proc/net/tcp.');
        }
        $client = $this->client();
        $synchronous = $client->get('/echo')['peer'];
        $detached = $client->settleDetached($client->postDetached('/echo', []))['peer'];

        foreach (['synchronous' => $synchronous, 'detached' => $detached] as $kind => $peer) {
            $port = (int) substr($peer, strrpos($peer, ':') + 1);
            $this->assertSame('02', $this->tcpTimer($port), "the {$kind} connection has no keep-alive timer");
        }
    }

    /** The kernel's timer type for the established socket on a local port: 02 is keep-alive. */
    private function tcpTimer(int $port): ?string
    {
        $local = sprintf(':%04X', $port);
        foreach (['/proc/net/tcp', '/proc/net/tcp6'] as $table) {
            foreach (array_slice(file($table, FILE_IGNORE_NEW_LINES) ?: [], 1) as $line) {
                $fields = preg_split('/\s+/', trim($line));
                // local_address rem_address st tx_queue:rx_queue tr:tm->when
                if (str_ends_with($fields[1], $local) && $fields[3] === '01') {
                    return substr($fields[5], 0, 2);
                }
            }
        }

        return null;
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
