<?php

namespace Queen\Http;

use GuzzleHttp\Client;
use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\Utils as PromiseUtils;
use JsonException;
use Psr\Http\Message\ResponseInterface;
use Queen\Exceptions\HttpException;
use UnexpectedValueException;

class HttpClient
{
    /**
     * Waiting for a detached answer polls, since a tick must not block: from
     * 50 µs, doubling to 1 ms, so a long wait costs little CPU.
     */
    private const DETACHED_POLL_MIN_MICROS = 50;
    private const DETACHED_POLL_MAX_MICROS = 1_000;

    /** A detached request is written within the connect timeout, or dropped. */
    private const DETACHED_WRITE_MILLIS = 5_000;

    /** 503 codes with which the broker says it did not run the request. */
    private const NOT_RUN_CODES = ['no_leader', 'standby'];

    /**
     * 503 code with which the broker refused a request before it ran, unless
     * a follower lost its relay to the leader after the request went out.
     */
    private const REFUSED_CODE = 'retry';

    /** 503 codes after which the request may have run. */
    private const MAY_HAVE_RUN_CODES = ['outcome_unknown', 'timeout', 'retry'];

    /** The longest a request waits for a leader, when its timeout is longer. */
    private const LEADER_WAIT_MAX_MILLIS = 10_000;

    /** The pause while a leader is elected: Retry-After within these bounds. */
    private const LEADER_WAIT_DEFAULT_PAUSE_MILLIS = 1_000;
    private const LEADER_WAIT_MIN_PAUSE_MILLIS = 50;
    private const LEADER_WAIT_MAX_PAUSE_MILLIS = 5_000;

    /** Routes whose every answer is a JSON body; see answersWithJson(). */
    private const JSON_ANSWER_PATHS = [
        '/api/v1/push',
        '/api/v1/ack',
        '/api/v1/ack/batch',
        '/api/v1/transaction',
        '/api/v1/ephemeral/push',
        '/api/v1/ephemeral/pop',
        '/api/v1/ephemeral/ack',
    ];

    private ?string $baseUrl;
    private ?LoadBalancer $loadBalancer;
    private int $timeoutMillis;
    private int $retryAttempts;
    private int $retryDelayMillis;
    private bool $enableFailover;
    private ?string $bearerToken;
    private array $headers;
    private array $retry429;
    private Client $guzzle;
    private bool $customHandler;
    private ?CurlMultiHandler $detachedHandler = null;
    private ?Client $detachedGuzzle = null;
    /** Synchronous and detached requests without Guzzle, when allowed. */
    private ?CurlTransport $transport;
    /** @var \WeakMap<PromiseInterface, DetachedRequest> */
    private \WeakMap $detached;

    public function __construct(array $options = [])
    {
        $this->baseUrl = $options['baseUrl'] ?? null;
        $this->loadBalancer = $options['loadBalancer'] ?? null;
        $this->timeoutMillis = $options['timeoutMillis'] ?? 30000;
        $this->retryAttempts = $options['retryAttempts'] ?? 3;
        $this->retryDelayMillis = $options['retryDelayMillis'] ?? 1000;
        $this->enableFailover = $options['enableFailover'] ?? true;
        $this->bearerToken = $options['bearerToken'] ?? null;
        $this->headers = $options['headers'] ?? [];
        // 429 (rate-limited) backoff policy, separate from the 5xx/network
        // retryAttempts above: ['maxAttempts' => int, 'baseMs' => int,
        // 'capMs' => int], all optional. See Retry429Policy.
        $this->retry429 = $options['retry429'] ?? [];

        // 'handler' overrides Guzzle's handler stack, the seam tests use to
        // drive the retry/failover paths without a live server.
        $handler = $options['handler'] ?? null;
        $this->customHandler = $handler !== null;
        $this->guzzle = new Client($handler !== null ? ['handler' => $handler] : []);
        $this->transport = $handler === null && self::curlTransportAllowed() ? new CurlTransport() : null;
        $this->detached = new \WeakMap();
    }

    /**
     * The cURL transport replaces Guzzle for synchronous and detached
     * requests, the hot path of a worker. Guzzle stays where its behavior
     * would differ: a proxy from the environment, which Guzzle and libcurl
     * read differently, and QUEEN_SDK_HTTP_TRANSPORT=guzzle as an escape.
     */
    private static function curlTransportAllowed(): bool
    {
        if (!CurlTransport::isAvailable() || strtolower((string) getenv('QUEEN_SDK_HTTP_TRANSPORT')) === 'guzzle') {
            return false;
        }
        foreach (['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'] as $name) {
            if ((string) getenv($name) !== '') {
                return false;
            }
        }

        return true;
    }

    // ===========================
    // Synchronous API
    // ===========================

    /**
     * $retryKind selects the 429 backoff budget: pass Retry429Policy::KIND_POP
     * for long-poll (wait=true) pop requests to get the unbounded policy,
     * null/anything else for the bounded one.
     */
    public function get(string $path, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        return $this->requestWithFailover('GET', $path, null, $requestTimeoutMillis, $affinityKey, $retryKind);
    }

    public function post(string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        return $this->requestWithFailover('POST', $path, $body, $requestTimeoutMillis, $affinityKey, $retryKind);
    }

    public function put(string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        return $this->requestWithFailover('PUT', $path, $body, $requestTimeoutMillis, $affinityKey, $retryKind);
    }

    public function delete(string $path, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        return $this->requestWithFailover('DELETE', $path, null, $requestTimeoutMillis, $affinityKey, $retryKind);
    }

    // ===========================
    // Async API (returns Guzzle promises)
    // ===========================
    //
    // Unlike the synchronous API these do NOT retry 429 in flight: the only
    // way to wait inside a promise chain here is to block, which would stall
    // every other request sharing the cURL multi-handle. The rejection is an
    // HttpException carrying errorCode/retryAfterSeconds, so callers pace
    // themselves between rounds (see ConsumerManager::concurrentWorkers) using
    // the policy from getRetry429Policy().

    public function getAsync(string $path, ?int $requestTimeoutMillis = null, ?string $affinityKey = null): PromiseInterface
    {
        return $this->executeRequestAsync($this->resolveUrl($affinityKey) . $path, 'GET', null, $requestTimeoutMillis);
    }

    /**
     * Async GET with the same network/5xx retry and backend failover boundary
     * as get(). HTTP 4xx responses, including 429, are terminal here: unlike
     * the synchronous API an async batch must not sleep the shared event loop.
     */
    public function getAsyncWithFailover(string $path, ?int $requestTimeoutMillis = null, ?string $affinityKey = null): PromiseInterface
    {
        if ($this->loadBalancer === null || !$this->enableFailover) {
            return $this->requestAsyncWithRetry('GET', $path, null, $requestTimeoutMillis, $affinityKey);
        }

        $firstUrl = $this->loadBalancer->getNextUrl($affinityKey);
        $urls = [$firstUrl];
        foreach ($this->loadBalancer->getAllUrls() as $url) {
            if ($url !== $firstUrl) {
                $urls[] = $url;
            }
        }

        return $this->requestAsyncAcrossUrls($urls, 0, 'GET', $path, null, $requestTimeoutMillis);
    }

    public function postAsync(string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null): PromiseInterface
    {
        return $this->executeRequestAsync($this->resolveUrl($affinityKey) . $path, 'POST', $body, $requestTimeoutMillis);
    }

    public function putAsync(string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null): PromiseInterface
    {
        return $this->executeRequestAsync($this->resolveUrl($affinityKey) . $path, 'PUT', $body, $requestTimeoutMillis);
    }

    public function deleteAsync(string $path, ?int $requestTimeoutMillis = null, ?string $affinityKey = null): PromiseInterface
    {
        return $this->executeRequestAsync($this->resolveUrl($affinityKey) . $path, 'DELETE', null, $requestTimeoutMillis);
    }

    // ===========================
    // Detached requests
    // ===========================
    //
    // A detached request is on the wire when it is sent, and nothing else runs
    // until settleDetached(): the caller does other work meanwhile, such as
    // the next job. One attempt against one backend, no 429 retry; the caller
    // retries a rejection synchronously. A request that cannot be written
    // within the connect timeout throws at once: nothing reached the server.

    public function postDetached(string $path, array $body, ?string $affinityKey = null): PromiseInterface
    {
        return $this->sendDetached('POST', $path, $body, $affinityKey);
    }

    public function getDetached(string $path, ?string $affinityKey = null): PromiseInterface
    {
        return $this->sendDetached('GET', $path, null, $affinityKey);
    }

    private function sendDetached(string $method, string $path, ?array $body, ?string $affinityKey): PromiseInterface
    {
        if ($this->transport !== null) {
            $request = $this->transport->start(
                $method,
                $this->resolveUrl($affinityKey) . $path,
                $this->requestHeaders(),
                $body === null ? null : json_encode($body, JSON_THROW_ON_ERROR),
            );
            // wait() settles like settleDetached(). The promise reaches itself
            // weakly, so dropping it still frees the request.
            $self = new \stdClass();
            $promise = new Promise(function () use ($self): void {
                $promise = $self->promise->get();
                if ($promise !== null && isset($this->detached[$promise])) {
                    $this->settleDetached($promise);
                }
            });
            $self->promise = \WeakReference::create($promise);
            $this->detached[$promise] = $request;

            return $promise;
        }

        $options = $this->buildRequestOptions($method, $body, null);
        // The answer may be settled long after it arrived, when the caller's
        // work ends: curl must not count that time against the request.
        // settleDetached() bounds the wait instead.
        $options['timeout'] = 0;
        $options['connect_timeout'] = 0;
        // Guzzle hides the handle: the upload progress tells when a body is
        // written.
        $written = $body === null;
        if (!$written) {
            $options['progress'] = static function (
                int $downloadTotal,
                int $downloaded,
                int $uploadTotal,
                int $uploaded,
            ) use (&$written): void {
                $written = $written || ($uploadTotal > 0 && $uploaded >= $uploadTotal);
            };
        }

        $url = $this->resolveUrl($affinityKey) . $path;
        $promise = $this->detachedClient()->requestAsync($method, $url, $options)
            ->then(fn (ResponseInterface $response) => $this->parseResponse($response, $url));
        if ($this->detachedHandler === null) {
            return $promise;
        }
        // cURL writes only while it is driven. One pass writes the whole
        // request on a reused keep-alive connection; a new connection is
        // driven until the body is written, as CurlTransport::start() does:
        // the caller may settle only after its next job, and an ACK still
        // unwritten when the process dies is lost. A request without a body
        // (a pop sent ahead) is not tracked: losing it loses no work.
        $deadline = hrtime(true) + self::DETACHED_WRITE_MILLIS * 1_000_000;
        $pause = self::DETACHED_POLL_MIN_MICROS;
        $this->detachedHandler->tick();
        while (!$written && $promise->getState() === PromiseInterface::PENDING) {
            if (hrtime(true) >= $deadline) {
                $promise->cancel();
                throw new TransportException(
                    'Queen could not send a detached request within ' . self::DETACHED_WRITE_MILLIS . ' ms.',
                    28,
                );
            }
            usleep($pause);
            $pause = min($pause * 2, self::DETACHED_POLL_MAX_MICROS);
            $this->detachedHandler->tick();
        }

        return $promise;
    }

    /**
     * The detached request's result, waiting at most $timeoutMillis (the
     * client timeout by default).
     *
     * @throws \Throwable the transport or HTTP failure; a request still
     *         unanswered at the deadline is cancelled.
     */
    public function settleDetached(PromiseInterface $promise, ?int $timeoutMillis = null): mixed
    {
        $timeoutMillis ??= $this->timeoutMillis;
        if ($this->transport !== null && isset($this->detached[$promise])) {
            $request = $this->detached[$promise];
            unset($this->detached[$promise]);
            try {
                $answer = $this->transport->settle($request, $timeoutMillis);
                $result = $this->parseAnswer($answer['status'], $answer['body'], $answer['retryAfter'], $request->url);
            } catch (\Throwable $failure) {
                $promise->reject($failure);
                throw $failure;
            }
            $promise->resolve($result);

            return $result;
        }
        if ($this->detachedHandler !== null) {
            $deadline = hrtime(true) + $timeoutMillis * 1_000_000;
            $pause = self::DETACHED_POLL_MIN_MICROS;
            while (true) {
                $this->detachedHandler->tick();
                if ($promise->getState() !== PromiseInterface::PENDING) {
                    break;
                }
                if (hrtime(true) >= $deadline) {
                    $promise->cancel();
                    throw new \RuntimeException("Queen did not answer a detached request within {$timeoutMillis} ms.");
                }
                usleep($pause);
                $pause = min($pause * 2, self::DETACHED_POLL_MAX_MICROS);
            }
        }

        return $promise->wait();
    }

    private function detachedClient(): Client
    {
        // Tests drive every request through their own handler.
        if ($this->customHandler) {
            return $this->guzzle;
        }
        if ($this->detachedGuzzle === null) {
            // A tick must never wait in select() for the answer: that would
            // make a detached request synchronous again.
            $this->detachedHandler = new CurlMultiHandler(['select_timeout' => 0]);
            $this->detachedGuzzle = new Client(['handler' => HandlerStack::create($this->detachedHandler)]);
        }

        return $this->detachedGuzzle;
    }

    /**
     * Wait for multiple promises to resolve concurrently.
     *
     * @param PromiseInterface[] $promises
     * @return array Results indexed same as input
     */
    public static function awaitAll(array $promises): array
    {
        return PromiseUtils::unwrap($promises);
    }

    /**
     * Settle all promises (no exceptions on failure). Returns array of
     * ['state' => 'fulfilled'|'rejected', 'value' => ..., 'reason' => ...]
     *
     * @param PromiseInterface[] $promises
     * @return array
     */
    public static function settleAll(array $promises): array
    {
        return PromiseUtils::settle($promises)->wait();
    }

    // ===========================
    // Internals
    // ===========================

    public function getLoadBalancer(): ?LoadBalancer
    {
        return $this->loadBalancer;
    }

    /**
     * Effective 429 backoff policy for a request kind. Exposed so callers of
     * the async API — which has no in-flight 429 retry — pace their own poll
     * rounds with the same numbers the synchronous path uses.
     */
    public function getRetry429Policy(?string $retryKind = null): Retry429Policy
    {
        return Retry429Policy::forKind($this->retry429, $retryKind);
    }

    private function resolveUrl(?string $affinityKey = null): string
    {
        if ($this->loadBalancer !== null) {
            return $this->loadBalancer->getNextUrl($affinityKey);
        }
        if ($this->baseUrl === null) {
            throw new \LogicException('HttpClient has no baseUrl and no LoadBalancer configured');
        }
        return $this->baseUrl;
    }

    /** @return array<string, string|list<string>> */
    private function requestHeaders(): array
    {
        $headers = ['Content-Type' => 'application/json'];
        foreach ($this->headers as $name => $value) {
            // An explicitly configured bearer token is authoritative. This is
            // especially important for the supervisor's read-only token: a
            // stale worker Authorization header must not silently override it.
            if ($this->bearerToken !== null && strcasecmp((string) $name, 'Authorization') === 0) {
                continue;
            }
            $headers[$name] = $value;
        }
        if ($this->bearerToken !== null) {
            $headers['Authorization'] = "Bearer {$this->bearerToken}";
        }

        return $headers;
    }

    private function buildRequestOptions(string $method, ?array $body, ?int $requestTimeoutMillis): array
    {
        $effectiveTimeout = $requestTimeoutMillis ?? $this->timeoutMillis;

        $options = [
            'headers' => $this->requestHeaders(),
            'timeout' => $effectiveTimeout / 1000,
            'connect_timeout' => 5,
            'http_errors' => false,
            // A broker endpoint is authoritative. Following redirects could
            // forward bearer credentials or custom headers to another host.
            'allow_redirects' => false,
        ];

        if ($body !== null) {
            $options['json'] = $body;
        }

        return $options;
    }

    private function parseResponse(ResponseInterface $response, string $url): mixed
    {
        return $this->parseAnswer(
            $response->getStatusCode(),
            (string) $response->getBody(),
            $response->getHeaderLine('Retry-After'),
            $url,
        );
    }

    /**
     * Whether an answer to $url must carry a JSON body. The message path
     * always answers one, except for an empty pop, which is a bodiless 204:
     * an empty answer there confirms nothing (a gateway's, say), so it must
     * not read as a stored push or a successful acknowledgement. The other
     * routes keep reading an empty answer as null.
     */
    private static function answersWithJson(string $url, int $statusCode): bool
    {
        $path = (string) parse_url($url, PHP_URL_PATH);
        if (str_starts_with($path, '/api/v1/pop')) {
            return $statusCode !== 204;
        }

        return in_array($path, self::JSON_ANSWER_PATHS, true);
    }

    /**
     * A redirect or a client error: the same request cannot succeed by being
     * sent again, here or to another backend.
     */
    private static function isTerminalStatus(int $statusCode): bool
    {
        return $statusCode >= 300 && $statusCode < 500;
    }

    private function parseAnswer(int $statusCode, string $responseBody, string $retryAfter, string $url = ''): mixed
    {
        if ($statusCode === 204 && !self::answersWithJson($url, $statusCode)) {
            return null;
        }

        // A redirect is never followed (it could carry the bearer token to
        // another host), so it answers nothing that was asked: an error, like
        // a 4xx, and never a success with an empty result.
        if ($statusCode >= 300) {
            $error = $statusCode < 400
                ? "HTTP {$statusCode}: Queen answered with a redirect, which the client does not follow; check the Queen URL"
                : "HTTP {$statusCode}";
            $serverError = null;
            $errorCode = null;
            $reason = null;
            $detail = null;
            if ($responseBody) {
                $decoded = json_decode($responseBody, true);
                if (isset($decoded['error']) && is_string($decoded['error'])) {
                    $serverError = $decoded['error'];
                    $error = $serverError;
                }
                // Proxy error contract: 429 {error, code: 'rate_limited' |
                // 'quota_exceeded'} with Retry-After (seconds); 403 {error,
                // code: 'cluster_suspended' | 'storage_quota_exceeded' |
                // 'feature_gated' | 'forbidden'}. See ErrorCode.
                if (isset($decoded['code']) && is_string($decoded['code'])) {
                    $errorCode = $decoded['code'];
                }
                // The kv/timers envelope carries two more fields: `reason`, a
                // finer stable identifier, and `detail`, which names the
                // offending operation index. Dropping them would leave the
                // caller with "kv_bad_request" and nothing to act on, on the
                // one surface whose ops arrive in batches.
                if (isset($decoded['reason']) && is_string($decoded['reason'])) {
                    $reason = $decoded['reason'];
                }
                if (isset($decoded['detail']) && is_string($decoded['detail'])) {
                    $detail = $decoded['detail'];
                }
            }

            // 429: rate limited. 503: the broker is electing a leader or is
            // otherwise briefly unavailable, and says when to come back.
            $retryAfterSeconds = $statusCode === 429 || $statusCode === 503
                ? $this->parseRetryAfter($retryAfter)
                : null;

            // The message stays the code so existing string-free branching is
            // unchanged, with the finer identifier and the human half appended
            // when the server sent them — a failing assertion that reads
            // "kv_bad_request" and nothing else costs an hour.
            $message = $error;
            if ($reason !== null && $reason !== $error) {
                $message .= ": {$reason}";
            }
            if ($detail !== null) {
                $message .= " ({$detail})";
            }

            throw new HttpException(
                $message,
                $statusCode,
                0,
                null,
                $errorCode,
                $retryAfterSeconds,
                $reason,
                $detail,
                $serverError,
            );
        }

        if (empty($responseBody)) {
            if (self::answersWithJson($url, $statusCode)) {
                // Like a malformed body: a transport failure, so the normal
                // retry/failover boundary runs and the caller fails closed.
                throw new UnexpectedValueException(
                    "Queen answered HTTP {$statusCode} without a body, where a JSON answer was expected; "
                    . 'nothing confirms the request took effect.',
                );
            }

            return null;
        }

        try {
            return json_decode($responseBody, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            // A malformed success body is never an empty pop or a successful
            // acknowledgement. Surface it as a transport failure so the normal
            // retry/failover boundary runs and queue consumers fail closed.
            throw new UnexpectedValueException(
                'Queen returned a malformed JSON response: ' . $exception->getMessage(),
                previous: $exception,
            );
        }
    }

    /**
     * Parse the Retry-After header (seconds, per the proxy contract) into a
     * float. Null when absent, non-numeric or negative.
     */
    private function parseRetryAfter(string $value): ?float
    {
        if ($value === '' || !is_numeric($value)) {
            return null;
        }

        $seconds = (float) $value;

        return is_finite($seconds) && $seconds >= 0 ? $seconds : null;
    }

    /**
     * The request body as JSON, encoded once before any attempt: a body
     * json_encode() refuses (a string that is not UTF-8, say) is the caller's
     * error. Nothing is sent, so it must neither be retried nor count against
     * the health of a backend.
     *
     * @throws JsonException
     */
    private function encodeBody(?array $body): ?string
    {
        return $body === null ? null : json_encode($body, JSON_THROW_ON_ERROR);
    }

    private function executeRequest(string $url, string $method, ?string $payload = null, ?int $requestTimeoutMillis = null): mixed
    {
        if ($this->transport !== null) {
            $answer = $this->transport->request(
                $method,
                $url,
                $this->requestHeaders(),
                $payload,
                $requestTimeoutMillis ?? $this->timeoutMillis,
            );

            return $this->parseAnswer($answer['status'], $answer['body'], $answer['retryAfter'], $url);
        }

        $options = $this->buildRequestOptions($method, null, $requestTimeoutMillis);
        if ($payload !== null) {
            // The bytes Guzzle's 'json' option would send; the Content-Type
            // header is already set.
            $options['body'] = $payload;
        }
        $response = $this->guzzle->request($method, $url, $options);
        return $this->parseResponse($response, $url);
    }

    /**
     * Run one logical request against a single URL, transparently retrying
     * HTTP 429 with backoff until the policy for $retryKind is exhausted (or
     * never, for the unbounded pop policy). Every other outcome — success,
     * network error, non-429 4xx, 5xx — passes straight through: 429 is the
     * only status this layer retries, and 5xx/network retry plus
     * cross-backend failover stay with the callers below.
     */
    private function executeRequestWithRetry429(string $url, string $method, ?string $payload, ?int $requestTimeoutMillis, ?string $retryKind): mixed
    {
        $policy = Retry429Policy::forKind($this->retry429, $retryKind);
        $tries = 0;

        while (true) {
            $tries++;

            try {
                return $this->executeRequest($url, $method, $payload, $requestTimeoutMillis);
            } catch (HttpException $error) {
                if ($error->statusCode !== 429 || $policy->isExhausted($tries)) {
                    throw $error;
                }

                usleep($policy->delayMillis($tries - 1, $error->retryAfterSeconds) * 1000);
            }
        }
    }

    private function executeRequestAsync(
        string $url,
        string $method,
        ?array $body = null,
        ?int $requestTimeoutMillis = null,
        int $delayMillis = 0,
    ): PromiseInterface
    {
        $options = $this->buildRequestOptions($method, $body, $requestTimeoutMillis);
        if ($delayMillis > 0) {
            // Guzzle schedules this delay on its multi handler. It does not
            // block unrelated promises in the supervisor's polling batch.
            $options['delay'] = $delayMillis;
        }

        return $this->guzzle->requestAsync($method, $url, $options)->then(
            fn(ResponseInterface $response) => $this->parseResponse($response, $url)
        );
    }

    private function requestAsyncWithRetry(
        string $method,
        string $path,
        ?array $body,
        ?int $requestTimeoutMillis,
        ?string $affinityKey,
        int $attempt = 0,
    ): PromiseInterface
    {
        $url = $this->resolveUrl($affinityKey);
        $delayMillis = $attempt === 0 ? 0 : $this->retryDelayMillis * (2 ** ($attempt - 1));

        return $this->executeRequestAsync($url . $path, $method, $body, $requestTimeoutMillis, $delayMillis)->then(
            null,
            function (mixed $reason) use ($method, $path, $body, $requestTimeoutMillis, $affinityKey, $attempt): PromiseInterface {
                $error = $reason instanceof \Throwable
                    ? $reason
                    : new \RuntimeException('Queen async request failed with a non-exception rejection.');
                $statusCode = $this->getStatusCode($error);
                $nextAttempt = $attempt + 1;
                if (self::isTerminalStatus($statusCode) || $nextAttempt >= $this->retryAttempts) {
                    throw $error;
                }

                return $this->requestAsyncWithRetry(
                    $method,
                    $path,
                    $body,
                    $requestTimeoutMillis,
                    $affinityKey,
                    $nextAttempt,
                );
            },
        );
    }

    /** @param list<string> $urls */
    private function requestAsyncAcrossUrls(
        array $urls,
        int $index,
        string $method,
        string $path,
        ?array $body,
        ?int $requestTimeoutMillis,
    ): PromiseInterface
    {
        $url = $urls[$index];

        return $this->executeRequestAsync($url . $path, $method, $body, $requestTimeoutMillis)->then(
            function (mixed $result) use ($url): mixed {
                $this->loadBalancer?->markHealthy($url);
                return $result;
            },
            function (mixed $reason) use ($urls, $index, $method, $path, $body, $requestTimeoutMillis, $url): PromiseInterface {
                $error = $reason instanceof \Throwable
                    ? $reason
                    : new \RuntimeException('Queen async request failed with a non-exception rejection.');
                $statusCode = $this->getStatusCode($error);
                if ($statusCode === 0 || $statusCode >= 500) {
                    $this->loadBalancer?->markUnhealthy($url);
                }
                if (self::isTerminalStatus($statusCode) || !isset($urls[$index + 1])) {
                    throw $error;
                }

                return $this->requestAsyncAcrossUrls(
                    $urls,
                    $index + 1,
                    $method,
                    $path,
                    $body,
                    $requestTimeoutMillis,
                );
            },
        );
    }

    private function getStatusCode(\Throwable $error): int
    {
        return ($error instanceof HttpException) ? $error->statusCode : 0;
    }


    // ===========================
    // Leader elections
    // ===========================
    //
    // A raft cluster answers 503 for a few seconds while it elects a leader,
    // and its `code` says whether the request ran (server: RsmError and the
    // follower's relay in handlers/raft.rs):
    //
    //   no_leader, standby         it did not run;
    //   retry                      refused before it ran, or a follower lost
    //                              its relay to the leader after it went out;
    //   outcome_unknown, timeout   it may have run.
    //
    // A request whose second run changes nothing waits the election out, at
    // the pace of Retry-After and within a bounded budget, instead of failing
    // after the few quick attempts the retry loops make. A pop is never sent
    // again after a 503 that leaves its outcome unknown.

    /**
     * The 503 codes after which this request may be sent again while the
     * cluster elects a leader:
     *
     * - a read, or a push whose every item has a transactionId (the broker
     *   deduplicates it): a second run changes nothing, so after any 503 that
     *   says it did not run or was refused;
     * - a pop or an ACK: only after a 503 that says it did not run, since a
     *   second run of one that ran claims other messages, or calls the
     *   messages it acknowledged stale;
     * - anything else: none, and it keeps the retryAttempts it had.
     *
     * @return list<string>
     */
    private static function resendableCodes(string $method, string $path, ?array $body): array
    {
        $route = (string) parse_url($path, PHP_URL_PATH);
        if (self::isPopRoute($route)
            || ($method === 'POST' && in_array($route, ['/api/v1/ack', '/api/v1/ack/batch'], true))) {
            return self::NOT_RUN_CODES;
        }
        if ($method === 'GET'
            || ($method === 'POST' && $route === '/api/v1/push' && self::everyItemHasTransactionId($body))) {
            return [...self::NOT_RUN_CODES, self::REFUSED_CODE];
        }

        return [];
    }

    private static function isPopRoute(string $route): bool
    {
        return str_starts_with($route, '/api/v1/pop') || $route === '/api/v1/ephemeral/pop';
    }

    private static function everyItemHasTransactionId(?array $body): bool
    {
        $items = $body['items'] ?? null;
        if (!is_array($items) || $items === []) {
            return false;
        }
        foreach ($items as $item) {
            if (!is_array($item) || !is_string($item['transactionId'] ?? null) || $item['transactionId'] === '') {
                return false;
            }
        }

        return true;
    }

    /** A 503 that names the request one the broker may have run, on a pop. */
    private static function isUnknownPopOutcome(string $path, \Throwable $error): bool
    {
        return $error instanceof HttpException
            && $error->statusCode === 503
            && in_array($error->errorCode, self::MAY_HAVE_RUN_CODES, true)
            && self::isPopRoute((string) parse_url($path, PHP_URL_PATH));
    }

    /** @param list<string> $resendableCodes */
    private static function isElectionAnswer(\Throwable $error, array $resendableCodes): bool
    {
        return $error instanceof HttpException
            && $error->statusCode === 503
            && in_array($error->errorCode, $resendableCodes, true);
    }

    /**
     * The pause before the request is sent again, in milliseconds: the 503's
     * Retry-After, within bounds. Null once the next attempt could not start
     * inside the budget, the shorter of LEADER_WAIT_MAX_MILLIS and the
     * request's own timeout.
     */
    private function leaderWaitPause(HttpException $error, int $startedNanos, ?int $requestTimeoutMillis): ?int
    {
        $pause = $error->retryAfterSeconds !== null
            ? (int) round(min($error->retryAfterSeconds * 1000, self::LEADER_WAIT_MAX_PAUSE_MILLIS))
            : self::LEADER_WAIT_DEFAULT_PAUSE_MILLIS;
        $pause = max($pause, self::LEADER_WAIT_MIN_PAUSE_MILLIS);
        $budget = min(self::LEADER_WAIT_MAX_MILLIS, $requestTimeoutMillis ?? $this->timeoutMillis);
        $elapsed = intdiv(hrtime(true) - $startedNanos, 1_000_000);

        return $elapsed + $pause <= $budget ? $pause : null;
    }

    private function requestWithRetry(string $method, string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        $payload = $this->encodeBody($body);
        $resendableCodes = self::resendableCodes($method, $path, $body);
        $startedNanos = hrtime(true);
        $attempt = 0;

        while (true) {
            try {
                $url = $this->resolveUrl($affinityKey) . $path;
                return $this->executeRequestWithRetry429($url, $method, $payload, $requestTimeoutMillis, $retryKind);
            } catch (\Throwable $error) {
                $statusCode = $this->getStatusCode($error);
                if (self::isTerminalStatus($statusCode) || self::isUnknownPopOutcome($path, $error)) {
                    throw $error;
                }

                // An election uses up no attempt: it is waited out within
                // its own budget.
                if (self::isElectionAnswer($error, $resendableCodes)) {
                    $pause = $this->leaderWaitPause($error, $startedNanos, $requestTimeoutMillis);
                    if ($pause === null) {
                        throw $error;
                    }
                    usleep($pause * 1000);
                    continue;
                }

                $attempt++;
                if ($attempt >= $this->retryAttempts) {
                    throw $error;
                }
                $delay = $this->retryDelayMillis * (2 ** ($attempt - 1));
                usleep($delay * 1000);
            }
        }
    }

    private function requestWithFailover(string $method, string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        if ($this->loadBalancer === null || !$this->enableFailover) {
            return $this->requestWithRetry($method, $path, $body, $requestTimeoutMillis, $affinityKey, $retryKind);
        }

        $payload = $this->encodeBody($body);
        $resendableCodes = self::resendableCodes($method, $path, $body);
        // A read or a deduplicated push may be sent again after any failure;
        // a pop or an ACK only after a 503 that says it did not run.
        $resendsHarmlessly = in_array(self::REFUSED_CODE, $resendableCodes, true);
        $startedNanos = hrtime(true);
        $urls = $this->loadBalancer->getAllUrls();

        while (true) {
            $attemptedUrls = [];
            $lastError = null;
            $electionAnswer = null;
            $harmless = true;

            while (count($attemptedUrls) < count($urls)) {
                $url = $this->loadBalancer->getNextUrl($affinityKey);

                if (in_array($url, $attemptedUrls, true)) {
                    // The balancer offers a backend this call already tried, as it
                    // does when every backend is marked unhealthy (a leader
                    // election marks them all): go on with the ones not tried yet,
                    // in order, rather than give up with them untried.
                    $url = array_values(array_diff($urls, $attemptedUrls))[0];
                }

                $attemptedUrls[] = $url;

                try {
                    // 429s are retried in place against this same backend inside
                    // executeRequestWithRetry429: rate limiting is a tenant-quota
                    // signal, not a backend-health one, so it must neither mark
                    // the server unhealthy nor fail over to another (every backend
                    // would answer the same, and spraying the fleet only makes the
                    // limiter angrier). An exhausted 429 is a 4xx and therefore
                    // leaves the loop below without a second server being tried.
                    $result = $this->executeRequestWithRetry429($url . $path, $method, $payload, $requestTimeoutMillis, $retryKind);
                    $this->loadBalancer->markHealthy($url);
                    return $result;
                } catch (\Throwable $error) {
                    $lastError = $error;

                    $statusCode = $this->getStatusCode($error);
                    if ($statusCode === 0 || $statusCode >= 500) {
                        $this->loadBalancer->markUnhealthy($url);
                    }

                    if (self::isTerminalStatus($statusCode) || self::isUnknownPopOutcome($path, $error)) {
                        throw $error;
                    }

                    if (self::isElectionAnswer($error, $resendableCodes)) {
                        $electionAnswer = $error;
                    } elseif (!$resendsHarmlessly) {
                        $harmless = false;
                    }
                }
            }

            // Every backend failed. While the cluster elects a leader, wait
            // and go round again, unless a backend failed in a way that leaves
            // this request's outcome unknown.
            $pause = $electionAnswer !== null && $harmless
                ? $this->leaderWaitPause($electionAnswer, $startedNanos, $requestTimeoutMillis)
                : null;
            if ($pause === null) {
                throw $lastError ?? new \RuntimeException('All servers failed');
            }
            usleep($pause * 1000);
        }
    }
}
