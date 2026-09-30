<?php

namespace Queen\Laravel\Http\Middleware;

use Closure;
use Illuminate\Contracts\Config\Repository as ConfigRepository;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

/**
 * Scrapers have no session and no user, so the metrics route has its own
 * credential: the configured bearer token, compared in constant time.
 */
final class AuthorizeMetrics
{
    public function __construct(private ConfigRepository $config)
    {
    }

    public function handle(Request $request, Closure $next): Response
    {
        $token = $this->config->get('queen.metrics.token');
        // Route caches are snapshots; keep the kill switch fail-closed.
        abort_unless(
            $this->config->get('queen.metrics.enabled', false) === true && is_string($token) && $token !== '',
            404,
        );

        $presented = $request->bearerToken();
        if (!is_string($presented) || !hash_equals($token, $presented)) {
            return new \Illuminate\Http\Response('', 401, ['WWW-Authenticate' => 'Bearer']);
        }

        return $next($request);
    }
}
