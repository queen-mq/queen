<?php

namespace Queen\Http;

use InvalidArgumentException;

/**
 * HTTP/1.1 over cURL handles kept alive between requests, without Guzzle's
 * PSR-7 objects, middleware and promises: a pop or an ACK is the hot path of
 * every worker, and those layers cost more CPU than the request itself.
 *
 * It keeps the Guzzle path's semantics: TLS verified, no redirects, the
 * caller's headers, a total and a connect timeout, compressed answers decoded,
 * and only Retry-After read from the response headers. HttpClient owns
 * retries, failover and parsing.
 *
 * @internal
 */
final class CurlTransport
{
    private const CONNECT_TIMEOUT_MILLIS = 5_000;

    /**
     * How long start() waits to write a request without a body, a pop sent
     * ahead: losing it loses no work, so a slow or dead backend must not hold
     * up the job that the worker already has. The request goes on, and
     * settle() finishes it.
     */
    private const BODYLESS_WRITE_MILLIS = 250;

    /**
     * TCP keep-alive on every connection: a NAT gateway, firewall or load
     * balancer that silently forgets an idle connection would otherwise
     * leave the next request waiting for its whole timeout. Probes start
     * after 30 idle seconds and repeat every 15.
     */
    private const KEEPALIVE_IDLE_SECONDS = 30;
    private const KEEPALIVE_INTERVAL_SECONDS = 15;

    /** The longest single wait for a detached answer, so deadlines stay prompt. */
    private const SELECT_SECONDS = 0.25;

    /**
     * curl_multi_select() returns at once when libcurl has no socket to
     * watch, for instance while it resolves a name; such a wait sleeps from
     * 50 µs, doubling to 1 ms, instead of spinning.
     */
    private const IDLE_MIN_MICROS = 50;
    private const IDLE_MAX_MICROS = 1_000;

    private ?\CurlHandle $handle = null;

    private ?\CurlMultiHandle $multi = null;

    /**
     * The DNS cache of every handle. libcurl's resolver threads do not
     * survive fork(): a child that frees a handle whose resolver ran in the
     * last seconds waits for them forever at exit, and a job may fork
     * (Laravel's fork concurrency driver does). The synchronous handle never
     * waits for them (CURLOPT_QUICK_EXIT), and detached requests reuse what
     * it resolved, so the multi handle, which PHP cannot tell to skip that
     * wait, never starts a resolver of its own.
     */
    private ?\CurlShareHandle $share = null;

    /** The process that owns the handles: a forked child must not share a connection. */
    private ?int $owner = null;

    /**
     * @var list<object> Handles a fork inherited, kept open while the child
     *      runs so the parent's connections live. PHP frees them at its exit.
     */
    private array $inherited = [];

    /** @var array<int, int> cURL result code per finished detached handle id. */
    private array $finished = [];

    public static function isAvailable(): bool
    {
        return function_exists('curl_init') && function_exists('curl_multi_init');
    }

    /**
     * @param array<string, string|list<string>> $headers
     * @return array{status: int, body: string, retryAfter: string}
     * @throws TransportException when no HTTP answer arrived
     */
    public function request(string $method, string $url, array $headers, ?string $body, int $timeoutMillis): array
    {
        $handle = $this->handle();
        $retryAfter = [];
        curl_reset($handle);
        curl_setopt_array($handle, $this->options($method, $url, $headers, $body, $timeoutMillis, self::CONNECT_TIMEOUT_MILLIS, $retryAfter));
        $responseBody = curl_exec($handle);
        if (!is_string($responseBody)) {
            throw TransportException::fromCurl(curl_errno($handle), curl_error($handle), $url);
        }

        return [
            'status' => (int) curl_getinfo($handle, CURLINFO_RESPONSE_CODE),
            'body' => $responseBody,
            'retryAfter' => implode(', ', $retryAfter),
        ];
    }

    /**
     * Start a request and return once cURL has written all of it: on a
     * kept-alive connection one pass does, a new connection is established
     * first. Writing must not wait for settle(), which with prefetch comes
     * after the next job: a request still unwritten when the process dies is
     * lost, and a lost ACK runs its job again. Writing takes at most the
     * connect timeout; a request still unwritten then is dropped, so nothing
     * reached the server, and reported. A request without a body, a pop sent
     * ahead, is waited for only BODYLESS_WRITE_MILLIS and then left to
     * settle(). No timeout runs on the answer until settle(). A request
     * dropped without settle() is freed with it.
     *
     * @param array<string, string|list<string>> $headers
     * @throws TransportException when the request could not be written in time
     */
    public function start(string $method, string $url, array $headers, ?string $body): DetachedRequest
    {
        $multi = $this->multi();
        $easy = curl_init();
        $request = new DetachedRequest($easy, $url, fn () => $this->release($easy));
        curl_setopt_array($easy, $this->options($method, $url, $headers, $body, 0, 0, $request->retryAfter));
        // The synchronous handle resolved the host first (a worker pops before
        // anything else): never resolve it again here. Its next new
        // connection refreshes the entry.
        curl_setopt($easy, CURLOPT_DNS_CACHE_TIMEOUT, -1);
        $added = curl_multi_add_handle($multi, $easy);
        if ($added !== CURLM_OK) {
            throw new TransportException('cURL could not start a detached request: ' . curl_multi_strerror($added), $added);
        }
        $this->drive();
        $id = spl_object_id($easy);
        $bodyless = $body === null;
        $deadline = hrtime(true) + ($bodyless ? self::BODYLESS_WRITE_MILLIS : self::CONNECT_TIMEOUT_MILLIS) * 1_000_000;
        $idle = self::IDLE_MIN_MICROS;
        while (!isset($this->finished[$id]) && !self::written($easy, $body)) {
            if (!$this->wait($multi, $deadline, $idle)) {
                if ($bodyless) {
                    return $request;
                }
                $request->release();
                throw new TransportException(
                    'Queen could not send a detached request within ' . self::CONNECT_TIMEOUT_MILLIS . ' ms.',
                    28,
                );
            }
            $this->drive();
        }

        return $request;
    }

    /**
     * The answer to a started request, waiting at most $timeoutMillis.
     *
     * @return array{status: int, body: string, retryAfter: string}
     * @throws TransportException when no HTTP answer arrived in time
     */
    public function settle(DetachedRequest $request, int $timeoutMillis): array
    {
        $multi = $this->multi();
        $id = spl_object_id($request->handle);
        $deadline = hrtime(true) + $timeoutMillis * 1_000_000;
        // An answer that already arrived is taken even with no time left.
        $this->drive();
        $idle = self::IDLE_MIN_MICROS;
        while (!isset($this->finished[$id])) {
            if (!$this->wait($multi, $deadline, $idle)) {
                $request->release();
                throw new TransportException("Queen did not answer a detached request within {$timeoutMillis} ms.", 28);
            }
            $this->drive();
        }

        $result = $this->finished[$id];
        $answer = [
            'status' => (int) curl_getinfo($request->handle, CURLINFO_RESPONSE_CODE),
            'body' => (string) curl_multi_getcontent($request->handle),
            'retryAfter' => implode(', ', $request->retryAfter),
        ];
        $error = curl_error($request->handle);
        $request->release();
        if ($result !== CURLE_OK) {
            throw TransportException::fromCurl($result, $error, $request->url);
        }

        return $answer;
    }

    /**
     * @param array<string, string|list<string>> $headers
     * @param list<string> $retryAfter receives every Retry-After value
     */
    private function options(
        string $method,
        string $url,
        array $headers,
        ?string $body,
        int $timeoutMillis,
        int $connectTimeoutMillis,
        array &$retryAfter,
    ): array {
        // No Expect: 100-continue round trip, and no Accept-Encoding sent
        // although compressed answers are decoded, both as Guzzle does.
        $lines = ['Expect:', 'Accept-Encoding:', 'User-Agent: queen-php-client'];
        foreach ($headers as $name => $value) {
            $value = is_array($value) ? implode(', ', $value) : (string) $value;
            if (strpbrk($name . $value, "\r\n") !== false) {
                throw new InvalidArgumentException("Header [{$name}] must not contain a line break.");
            }
            // libcurl drops "Name:" with no value; "Name;" sends it empty.
            $lines[] = $value === '' ? "{$name};" : "{$name}: {$value}";
        }
        $options = [
            CURLOPT_URL => $url,
            CURLOPT_HTTPHEADER => $lines,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => false,
            CURLOPT_PROTOCOLS => CURLPROTO_HTTP | CURLPROTO_HTTPS,
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
            CURLOPT_ENCODING => '',
            CURLOPT_TIMEOUT_MS => $timeoutMillis,
            CURLOPT_CONNECTTIMEOUT_MS => $connectTimeoutMillis,
            // Laravel workers own SIGALRM for job timeouts.
            CURLOPT_NOSIGNAL => true,
            CURLOPT_TCP_KEEPALIVE => 1,
            CURLOPT_TCP_KEEPIDLE => self::KEEPALIVE_IDLE_SECONDS,
            CURLOPT_TCP_KEEPINTVL => self::KEEPALIVE_INTERVAL_SECONDS,
            CURLOPT_HEADERFUNCTION => static function ($handle, string $line) use (&$retryAfter): int {
                if (strncasecmp($line, 'Retry-After:', 12) === 0) {
                    $retryAfter[] = trim(substr($line, 12));
                }

                return strlen($line);
            },
            CURLOPT_SHARE => $this->share(),
        ];
        // libcurl 7.87 and later: see $share.
        if (defined('CURLOPT_QUICK_EXIT')) {
            $options[CURLOPT_QUICK_EXIT] = 1;
        }
        if ($method === 'GET' && $body === null) {
            $options[CURLOPT_HTTPGET] = true;
        } elseif ($method === 'POST') {
            $options[CURLOPT_POST] = true;
            $options[CURLOPT_POSTFIELDS] = $body ?? '';
        } else {
            $options[CURLOPT_CUSTOMREQUEST] = $method;
            // A PUT always states its length, as Guzzle sends it.
            if ($body !== null || $method === 'PUT') {
                $options[CURLOPT_POSTFIELDS] = $body ?? '';
            }
        }

        return $options;
    }

    /**
     * Wait until one of cURL's sockets is ready, or briefly when it has none
     * to watch: false once $deadline (hrtime) has passed.
     */
    private function wait(\CurlMultiHandle $multi, int $deadline, int &$idle): bool
    {
        $left = ($deadline - hrtime(true)) / 1e9;
        if ($left <= 0) {
            return false;
        }
        // Blocks until a socket is ready, unlike a Guzzle tick.
        $selected = hrtime(true);
        if (curl_multi_select($multi, min($left, self::SELECT_SECONDS)) < 1
            && hrtime(true) - $selected < 1_000_000) {
            usleep($idle);
            $idle = min($idle * 2, self::IDLE_MAX_MICROS);
        }

        return true;
    }

    /** Whether cURL has written the whole request: its headers, then its body. */
    private static function written(\CurlHandle $easy, ?string $body): bool
    {
        return curl_getinfo($easy, CURLINFO_REQUEST_SIZE) > 0
            && curl_getinfo($easy, CURLINFO_SIZE_UPLOAD_T) >= strlen($body ?? '');
    }

    /** Run cURL without waiting and record every request that finished. */
    private function drive(): void
    {
        $multi = $this->multi();
        do {
            $status = curl_multi_exec($multi, $running);
        } while ($status === CURLM_CALL_MULTI_PERFORM);
        if ($status !== CURLM_OK) {
            throw new TransportException('cURL failed: ' . curl_multi_strerror($status), $status);
        }
        while (($message = curl_multi_info_read($multi)) !== false) {
            if ($message['msg'] === CURLMSG_DONE) {
                $this->finished[spl_object_id($message['handle'])] = $message['result'];
            }
        }
    }

    private function release(\CurlHandle $easy): void
    {
        unset($this->finished[spl_object_id($easy)]);
        if ($this->multi !== null && $this->owner === getmypid()) {
            curl_multi_remove_handle($this->multi, $easy);
        }
    }

    private function handle(): \CurlHandle
    {
        $this->claim();

        return $this->handle ??= curl_init();
    }

    private function multi(): \CurlMultiHandle
    {
        $this->claim();

        return $this->multi ??= curl_multi_init();
    }

    private function share(): \CurlShareHandle
    {
        $this->claim();
        if ($this->share === null) {
            $this->share = curl_share_init();
            curl_share_setopt($this->share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
        }

        return $this->share;
    }

    private function claim(): void
    {
        $pid = getmypid();
        if ($this->owner === $pid) {
            return;
        }
        if ($this->owner !== null) {
            // Closing an inherited handle could end the parent's TLS session.
            array_push($this->inherited, ...array_filter([$this->handle, $this->multi, $this->share]));
            $this->handle = null;
            $this->multi = null;
            $this->share = null;
            $this->finished = [];
        }
        $this->owner = $pid;
    }
}
