<?php

namespace Queen\Http;

use GuzzleHttp\Client;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\Utils as PromiseUtils;
use Psr\Http\Message\ResponseInterface;
use Queen\Exceptions\HttpException;

class HttpClient
{
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
        $this->guzzle = new Client($handler !== null ? ['handler' => $handler] : []);
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

    private function buildRequestOptions(string $method, ?array $body, ?int $requestTimeoutMillis): array
    {
        $effectiveTimeout = $requestTimeoutMillis ?? $this->timeoutMillis;

        $headers = ['Content-Type' => 'application/json'];
        if ($this->bearerToken !== null) {
            $headers['Authorization'] = "Bearer {$this->bearerToken}";
        }
        $headers = array_merge($headers, $this->headers);

        $options = [
            'headers' => $headers,
            'timeout' => $effectiveTimeout / 1000,
            'connect_timeout' => 5,
            'http_errors' => false,
        ];

        if ($body !== null) {
            $options['json'] = $body;
        }

        return $options;
    }

    private function parseResponse(ResponseInterface $response): mixed
    {
        $statusCode = $response->getStatusCode();

        if ($statusCode === 204) {
            return null;
        }

        $responseBody = (string) $response->getBody();

        if ($statusCode >= 400) {
            $error = "HTTP {$statusCode}";
            $errorCode = null;
            $reason = null;
            $detail = null;
            if ($responseBody) {
                $decoded = json_decode($responseBody, true);
                if (isset($decoded['error'])) {
                    $error = $decoded['error'];
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

            $retryAfterSeconds = $statusCode === 429
                ? $this->parseRetryAfter($response->getHeaderLine('Retry-After'))
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

            throw new HttpException($message, $statusCode, 0, null, $errorCode, $retryAfterSeconds, $reason, $detail);
        }

        if (empty($responseBody)) {
            return null;
        }

        return json_decode($responseBody, true);
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

        return $seconds >= 0 ? $seconds : null;
    }

    private function executeRequest(string $url, string $method, ?array $body = null, ?int $requestTimeoutMillis = null): mixed
    {
        $options = $this->buildRequestOptions($method, $body, $requestTimeoutMillis);
        $response = $this->guzzle->request($method, $url, $options);
        return $this->parseResponse($response);
    }

    /**
     * Run one logical request against a single URL, transparently retrying
     * HTTP 429 with backoff until the policy for $retryKind is exhausted (or
     * never, for the unbounded pop policy). Every other outcome — success,
     * network error, non-429 4xx, 5xx — passes straight through: 429 is the
     * only status this layer retries, and 5xx/network retry plus
     * cross-backend failover stay with the callers below.
     */
    private function executeRequestWithRetry429(string $url, string $method, ?array $body, ?int $requestTimeoutMillis, ?string $retryKind): mixed
    {
        $policy = Retry429Policy::forKind($this->retry429, $retryKind);
        $tries = 0;

        while (true) {
            $tries++;

            try {
                return $this->executeRequest($url, $method, $body, $requestTimeoutMillis);
            } catch (HttpException $error) {
                if ($error->statusCode !== 429 || $policy->isExhausted($tries)) {
                    throw $error;
                }

                usleep($policy->delayMillis($tries - 1, $error->retryAfterSeconds) * 1000);
            }
        }
    }

    private function executeRequestAsync(string $url, string $method, ?array $body = null, ?int $requestTimeoutMillis = null): PromiseInterface
    {
        $options = $this->buildRequestOptions($method, $body, $requestTimeoutMillis);

        return $this->guzzle->requestAsync($method, $url, $options)->then(
            fn(ResponseInterface $response) => $this->parseResponse($response)
        );
    }

    private function getStatusCode(\Throwable $error): int
    {
        return ($error instanceof HttpException) ? $error->statusCode : 0;
    }

    private function requestWithRetry(string $method, string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        $lastError = null;

        for ($attempt = 0; $attempt < $this->retryAttempts; $attempt++) {
            try {
                $url = $this->resolveUrl($affinityKey) . $path;
                return $this->executeRequestWithRetry429($url, $method, $body, $requestTimeoutMillis, $retryKind);
            } catch (\Throwable $error) {
                $lastError = $error;

                $statusCode = $this->getStatusCode($error);
                if ($statusCode >= 400 && $statusCode < 500) {
                    throw $error;
                }

                if ($attempt < $this->retryAttempts - 1) {
                    $delay = $this->retryDelayMillis * (2 ** $attempt);
                    usleep($delay * 1000);
                }
            }
        }

        throw $lastError;
    }

    private function requestWithFailover(string $method, string $path, ?array $body = null, ?int $requestTimeoutMillis = null, ?string $affinityKey = null, ?string $retryKind = null): mixed
    {
        if ($this->loadBalancer === null || !$this->enableFailover) {
            return $this->requestWithRetry($method, $path, $body, $requestTimeoutMillis, $affinityKey, $retryKind);
        }

        $urls = $this->loadBalancer->getAllUrls();
        $attemptedUrls = [];
        $lastError = null;

        for ($i = 0; $i < count($urls); $i++) {
            $url = $this->loadBalancer->getNextUrl($affinityKey);

            if (in_array($url, $attemptedUrls, true)) {
                continue;
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
                $result = $this->executeRequestWithRetry429($url . $path, $method, $body, $requestTimeoutMillis, $retryKind);
                $this->loadBalancer->markHealthy($url);
                return $result;
            } catch (\Throwable $error) {
                $lastError = $error;

                $statusCode = $this->getStatusCode($error);
                if ($statusCode === 0 || $statusCode >= 500) {
                    $this->loadBalancer->markUnhealthy($url);
                }

                if ($statusCode >= 400 && $statusCode < 500) {
                    throw $error;
                }
            }
        }

        throw $lastError ?? new \RuntimeException('All servers failed');
    }
}
