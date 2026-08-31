<?php

namespace Queen\Laravel\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

final class SecureDashboardResponse
{
    public function handle(Request $request, Closure $next): Response
    {
        $response = $next($request);
        $response->headers->set(
            'Content-Security-Policy',
            "default-src 'none'; style-src 'self'; style-src-attr 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
        );
        $cacheableStylesheet = $request->routeIs('queen.dashboard.stylesheet')
            && in_array($response->getStatusCode(), [Response::HTTP_OK, Response::HTTP_NOT_MODIFIED], true);
        if ($cacheableStylesheet) {
            // The stylesheet route intentionally shares the dashboard's web
            // session and authorization middleware. Keep its content-addressed
            // response in the browser cache, never a shared cache that could
            // replay Set-Cookie headers. `no-transform` also protects the SRI
            // digest from intermediary rewrites.
            $response->headers->set('Cache-Control', 'private, max-age=31536000, immutable, no-transform');
            $response->headers->remove('Pragma');
        } else {
            $response->headers->set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
            $response->headers->set('Pragma', 'no-cache');
        }
        $response->headers->set('X-Frame-Options', 'DENY');
        $response->headers->set('X-Content-Type-Options', 'nosniff');
        $response->headers->set('Referrer-Policy', 'no-referrer');

        return $response;
    }
}
