<?php

namespace Queen\Tests\Support;

/**
 * An HTTP/1.1 server with keep-alive and several connections at once, in a
 * child PHP process, for tests of the real cURL path. Routes:
 *
 * - `/echo`: 200 with the method, path, lower-cased headers, body, the
 *   number of the connection that carried the request and the client's
 *   address;
 * - `/status/<code>`: that status with `{"error": "status <code>"}`;
 * - `/flaky`: 503 once, then 200 `{"ok": true}`;
 * - `/limited`: 429 with `Retry-After: 2` and a rate-limit error;
 * - `/slow`: 200 after two seconds;
 * - `/empty`: 204;
 * - `/gzip`: a gzip-encoded JSON answer, whatever the request asked;
 * - `/malformed`: 200 with a truncated JSON body.
 */
final class KeepAliveServer
{
    private const SCRIPT = <<<'PHP'
[$script, $address] = $argv;
$server = stream_socket_server("tcp://{$address}");
$clients = [];
$buffers = [];
$peers = [];
$connections = 0;
$flaky = 0;
$answer = static function (string $method, string $target, array $headers, string $body, int $connection, string $peer) use (&$flaky): string {
    $path = (string) parse_url($target, PHP_URL_PATH);
    $status = 200;
    $extra = '';
    $payload = json_encode(['ok' => true]);
    if ($path === '/echo') {
        $payload = json_encode(['method' => $method, 'target' => $target, 'headers' => $headers, 'body' => $body, 'connection' => $connection, 'peer' => $peer]);
    } elseif (preg_match('#^/status/(\d+)$#', $path, $match)) {
        $status = (int) $match[1];
        $payload = json_encode(['error' => "status {$status}"]);
    } elseif ($path === '/flaky' && $flaky++ === 0) {
        $status = 503;
        $payload = json_encode(['error' => 'warming up']);
    } elseif ($path === '/limited') {
        $status = 429;
        $extra = "Retry-After: 2\r\n";
        $payload = json_encode(['error' => 'slow down', 'code' => 'rate_limited']);
    } elseif ($path === '/slow') {
        sleep(2);
    } elseif ($path === '/empty') {
        $status = 204;
        $payload = '';
    } elseif ($path === '/gzip') {
        // A gateway that compresses whatever the client asked.
        $extra = "Content-Encoding: gzip\r\n";
        $payload = gzencode(json_encode(['compressed' => true]));
    } elseif ($path === '/malformed') {
        $payload = '{"messages": [';
    }

    return "HTTP/1.1 {$status} X\r\n{$extra}Content-Type: application/json\r\nContent-Length: "
        . strlen($payload) . "\r\n\r\n" . $payload;
};
while (true) {
    $read = array_merge([$server], array_values($clients));
    $write = null;
    $except = null;
    if (@stream_select($read, $write, $except, 60) < 1) {
        break;
    }
    foreach ($read as $stream) {
        if ($stream === $server) {
            $clients[++$connections] = stream_socket_accept($server, 0, $peer);
            $buffers[$connections] = '';
            $peers[$connections] = (string) $peer;
            continue;
        }
        $id = array_search($stream, $clients, true);
        $chunk = fread($stream, 65536);
        if ($chunk === '' || $chunk === false) {
            if (feof($stream)) {
                fclose($stream);
                unset($clients[$id], $buffers[$id], $peers[$id]);
            }
            continue;
        }
        $buffers[$id] .= $chunk;
        while (($end = strpos($buffers[$id], "\r\n\r\n")) !== false) {
            $lines = explode("\r\n", substr($buffers[$id], 0, $end));
            [$method, $target] = explode(' ', array_shift($lines)) + [1 => ''];
            $headers = [];
            foreach ($lines as $line) {
                [$name, $value] = explode(':', $line, 2) + [1 => ''];
                $headers[strtolower(trim($name))] = trim($value);
            }
            $length = (int) ($headers['content-length'] ?? 0);
            if (strlen($buffers[$id]) < $end + 4 + $length) {
                break;
            }
            $body = (string) substr($buffers[$id], $end + 4, $length);
            $buffers[$id] = (string) substr($buffers[$id], $end + 4 + $length);
            fwrite($stream, $answer($method, $target, $headers, $body, $id, $peers[$id]));
        }
    }
}
PHP;

    /** @var resource */
    private $process;

    public readonly string $url;

    public function __construct()
    {
        $probe = stream_socket_server('tcp://127.0.0.1:0');
        $address = stream_socket_get_name($probe, false);
        fclose($probe);
        $pipes = [];
        $this->process = proc_open(
            [PHP_BINARY, '-r', self::SCRIPT, '--', $address],
            [1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
            $pipes,
        );
        $deadline = microtime(true) + 5;
        while (($connection = @stream_socket_client("tcp://{$address}", $code, $message, 0.1)) === false) {
            if (microtime(true) > $deadline) {
                throw new \RuntimeException("The test server did not listen on {$address}.");
            }
            usleep(20_000);
        }
        // The readiness probe counts as the server's first connection.
        fclose($connection);
        $this->url = "http://{$address}";
    }

    public function __destruct()
    {
        if (is_resource($this->process)) {
            proc_terminate($this->process);
            proc_close($this->process);
        }
    }
}
