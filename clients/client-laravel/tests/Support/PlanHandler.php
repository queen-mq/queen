<?php

namespace Queen\Tests\Support;

use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;

/**
 * Guzzle handler serving a scripted list of responses, then $default forever.
 * Wrap it in a HandlerStack and hand it to HttpClient/Queen as the 'handler'
 * option to drive retry and failover paths without a live server.
 *
 * Response descriptors: ['status' => int, 'json' => mixed, 'retryAfter' => string].
 */
class PlanHandler
{
    public array $requests = [];

    public function __construct(private array $plan = [], private ?array $default = null)
    {
    }

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $this->requests[] = $request;

        $descriptor = array_shift($this->plan) ?? $this->default ?? ['status' => 200, 'json' => []];

        $headers = ['Content-Type' => 'application/json'];
        if (isset($descriptor['retryAfter'])) {
            $headers['Retry-After'] = (string) $descriptor['retryAfter'];
        }

        return new FulfilledPromise(new Response(
            $descriptor['status'],
            $headers,
            json_encode($descriptor['json'] ?? [])
        ));
    }

    public function count(): int
    {
        return count($this->requests);
    }

    /**
     * Host of every request received, in order — used to assert which backend
     * a retry landed on.
     */
    public function hosts(): array
    {
        return array_map(fn(RequestInterface $r) => $r->getUri()->getHost(), $this->requests);
    }
}
