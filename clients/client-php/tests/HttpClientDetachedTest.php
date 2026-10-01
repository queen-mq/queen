<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Http\HttpClient;

/** Detached POSTs over real cURL against a local keep-alive server. */
class HttpClientDetachedTest extends TestCase
{
    /** A keep-alive HTTP/1.1 server that logs each arrival, then answers late. */
    private const SERVER = <<<'PHP'
[$script, $address, $arrivals, $delay] = $argv;
$server = stream_socket_server("tcp://{$address}");
while ($connection = @stream_socket_accept($server, 60)) {
    while (($line = fgets($connection)) !== false) {
        $length = 0;
        while (($header = fgets($connection)) !== false && trim($header) !== '') {
            if (stripos($header, 'content-length:') === 0) {
                $length = (int) trim(substr($header, 15));
            }
        }
        $body = $length > 0 ? stream_get_contents($connection, $length) : '';
        file_put_contents($arrivals, microtime(true) . "\n", FILE_APPEND);
        usleep((int) $delay);
        fwrite($connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
            . strlen($body) . "\r\n\r\n" . $body);
    }
}
PHP;

    private string $directory;

    /** @var resource|null */
    private $server = null;

    protected function setUp(): void
    {
        parent::setUp();
        if (!function_exists('curl_multi_init') || !function_exists('proc_open')) {
            $this->markTestSkipped('Needs cURL and proc_open.');
        }
        $this->directory = sys_get_temp_dir() . '/qhd-' . bin2hex(random_bytes(4));
        mkdir($this->directory, 0700);
    }

    protected function tearDown(): void
    {
        if (is_resource($this->server)) {
            proc_terminate($this->server);
            proc_close($this->server);
        }
        array_map('unlink', glob($this->directory . '/*') ?: []);
        @rmdir($this->directory);
        parent::tearDown();
    }

    public function testTheRequestReachesTheServerBeforeItIsSettled(): void
    {
        $client = new HttpClient(['baseUrl' => $this->startServer(200_000), 'timeoutMillis' => 5_000]);
        // A new connection sends its first request only when it is settled.
        $this->assertSame(['attempt' => 0], $client->settleDetached($client->postDetached('/api/v1/ack', ['attempt' => 0])));

        foreach ([1, 2] as $attempt) {
            $started = microtime(true);
            $promise = $client->postDetached('/api/v1/ack', ['attempt' => $attempt]);
            $this->assertLessThan(0.15, microtime(true) - $started, 'sending waited for the answer');

            // Work that never drives cURL, while the server answers.
            usleep(300_000);
            $arrivals = file($this->directory . '/arrivals', FILE_IGNORE_NEW_LINES) ?: [];
            $this->assertCount($attempt + 1, $arrivals, 'the request was not on the wire');

            $this->assertSame(['attempt' => $attempt], $client->settleDetached($promise));
        }
    }

    public function testAnUnansweredRequestIsCancelledAtTheSettleDeadline(): void
    {
        $client = new HttpClient(['baseUrl' => $this->startServer(3_000_000), 'timeoutMillis' => 5_000]);
        $promise = $client->postDetached('/api/v1/ack', ['late' => true]);

        $started = microtime(true);
        try {
            $client->settleDetached($promise, 200);
            $this->fail('A late answer was awaited past the deadline.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('within 200 ms', $exception->getMessage());
        }
        $this->assertLessThan(1.0, microtime(true) - $started);
    }

    private function startServer(int $answerDelayMicros): string
    {
        $probe = stream_socket_server('tcp://127.0.0.1:0');
        $address = stream_socket_get_name($probe, false);
        fclose($probe);
        touch($this->directory . '/arrivals');

        $pipes = [];
        $this->server = proc_open(
            [PHP_BINARY, '-r', self::SERVER, '--', $address, $this->directory . '/arrivals', (string) $answerDelayMicros],
            [1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
            $pipes,
        );
        $deadline = microtime(true) + 5;
        while (($probe = @stream_socket_client("tcp://{$address}", $code, $message, 0.1)) === false) {
            if (microtime(true) > $deadline) {
                $this->fail("The test server did not listen on {$address}.");
            }
            usleep(20_000);
        }
        fclose($probe);

        return "http://{$address}";
    }
}
