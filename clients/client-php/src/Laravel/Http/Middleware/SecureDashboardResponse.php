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
            // script-src and connect-src serve the packaged, content-hashed
            // refresh script and its same-origin fetch of this page; neither
            // allows inline code or another origin.
            "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self'; style-src-attr 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
        );
        $cacheableAsset = $request->routeIs('queen.dashboard.stylesheet', 'queen.dashboard.script')
            && in_array($response->getStatusCode(), [Response::HTTP_OK, Response::HTTP_NOT_MODIFIED], true);
        if ($cacheableAsset) {
            // The asset routes intentionally share the dashboard's web
            // session and authorization middleware. Keep their content-addressed
            // responses in the browser cache, never a shared cache that could
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
