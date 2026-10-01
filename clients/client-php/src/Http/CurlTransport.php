<?php

namespace Queen\Http;

/**
 * HTTP/1.1 over cURL handles kept alive between requests, without Guzzle's
 * PSR-7 objects, middleware and promises: a pop or an ACK is the hot path of
 * every worker, and those layers cost more CPU than the request itself.
 *
 * It keeps the Guzzle path's semantics: TLS verified, no redirects, the
 * caller's headers, a total and a connect timeout, and only Retry-After read
 * from the response headers. HttpClient owns retries, failover and parsing.
 *
 * @internal
 */
final class CurlTransport
{
    private const CONNECT_TIMEOUT_MILLIS = 5_000;

    /** The longest single wait for a detached answer, so deadlines stay prompt. */
    private const SELECT_SECONDS = 0.25;

    private ?\CurlHandle $handle = null;

    private ?\CurlMultiHandle $multi = null;

    /** The process that owns the handles: a forked child must not share a connection. */
    private ?int $owner = null;

    /** @var list<object> Handles a fork inherited, never closed so the parent's connections live. */
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
        $retryAfter = '';
        curl_reset($handle);
        curl_setopt_array($handle, $this->options($method, $url, $headers, $body, $timeoutMillis, self::CONNECT_TIMEOUT_MILLIS, $retryAfter));
        $responseBody = curl_exec($handle);
        if (!is_string($responseBody)) {
            throw TransportException::fromCurl(curl_errno($handle), curl_error($handle), $url);
        }

        return [
            'status' => (int) curl_getinfo($handle, CURLINFO_RESPONSE_CODE),
            'body' => $responseBody,
            'retryAfter' => $retryAfter,
        ];
    }

    /**
     * Start a request and return once cURL has written what it can: on a
     * kept-alive connection the whole request; a new connection proceeds
     * while settle() waits. No timeout runs until then.
     *
     * @param array<string, string|list<string>> $headers
     */
    public function start(string $method, string $url, array $headers, ?string $body): DetachedRequest
    {
        $multi = $this->multi();
        $easy = curl_init();
        $request = new DetachedRequest($easy, $url);
        curl_setopt_array($easy, $this->options($method, $url, $headers, $body, 0, 0, $request->retryAfter));
        curl_multi_add_handle($multi, $easy);
        $this->drive();

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
        while (!isset($this->finished[$id])) {
            $left = ($deadline - hrtime(true)) / 1e9;
            if ($left <= 0) {
                $this->release($request);
                throw new TransportException("Queen did not answer a detached request within {$timeoutMillis} ms.", 28);
            }
            // Blocks until a socket is ready, unlike a Guzzle tick.
            curl_multi_select($multi, min($left, self::SELECT_SECONDS));
            $this->drive();
        }

        $result = $this->finished[$id];
        unset($this->finished[$id]);
        $answer = [
            'status' => (int) curl_getinfo($request->handle, CURLINFO_RESPONSE_CODE),
            'body' => (string) curl_multi_getcontent($request->handle),
            'retryAfter' => $request->retryAfter,
        ];
        $error = curl_error($request->handle);
        $this->release($request);
        if ($result !== CURLE_OK) {
            throw TransportException::fromCurl($result, $error, $request->url);
        }

        return $answer;
    }

    /** Stop waiting for a started request. */
    public function cancel(DetachedRequest $request): void
    {
        unset($this->finished[spl_object_id($request->handle)]);
        $this->release($request);
    }

    /**
     * @param array<string, string|list<string>> $headers
     */
    private function options(
        string $method,
        string $url,
        array $headers,
        ?string $body,
        int $timeoutMillis,
        int $connectTimeoutMillis,
        string &$retryAfter,
    ): array {
        $lines = ['Expect:', 'User-Agent: queen-php-client'];
        foreach ($headers as $name => $value) {
            $lines[] = $name . ': ' . (is_array($value) ? implode(', ', $value) : $value);
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
            CURLOPT_TIMEOUT_MS => $timeoutMillis,
            CURLOPT_CONNECTTIMEOUT_MS => $connectTimeoutMillis,
            // Laravel workers own SIGALRM for job timeouts.
            CURLOPT_NOSIGNAL => true,
            CURLOPT_HEADERFUNCTION => static function ($handle, string $line) use (&$retryAfter): int {
                if (strncasecmp($line, 'Retry-After:', 12) === 0) {
                    $retryAfter = trim(substr($line, 12));
                }

                return strlen($line);
            },
        ];
        if ($method === 'GET' && $body === null) {
            $options[CURLOPT_HTTPGET] = true;
        } elseif ($method === 'POST') {
            $options[CURLOPT_POST] = true;
            $options[CURLOPT_POSTFIELDS] = $body ?? '';
        } else {
            $options[CURLOPT_CUSTOMREQUEST] = $method;
            if ($body !== null) {
                $options[CURLOPT_POSTFIELDS] = $body;
            }
        }

        return $options;
    }

    /** Run cURL without waiting and record every request that finished. */
    private function drive(): void
    {
        $multi = $this->multi();
        do {
            $status = curl_multi_exec($multi, $running);
        } while ($status === CURLM_CALL_MULTI_PERFORM);
        while (($message = curl_multi_info_read($multi)) !== false) {
            if ($message['msg'] === CURLMSG_DONE) {
                $this->finished[spl_object_id($message['handle'])] = $message['result'];
            }
        }
    }

    private function release(DetachedRequest $request): void
    {
        if ($this->multi !== null) {
            curl_multi_remove_handle($this->multi, $request->handle);
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

    private function claim(): void
    {
        $pid = getmypid();
        if ($this->owner === $pid) {
            return;
        }
        if ($this->owner !== null) {
            // Closing an inherited handle could end the parent's TLS session.
            array_push($this->inherited, ...array_filter([$this->handle, $this->multi]));
            $this->handle = null;
            $this->multi = null;
            $this->finished = [];
        }
        $this->owner = $pid;
    }
}
