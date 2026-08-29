<?php

namespace Queen\Laravel\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

final class SecureDashboardResponse
{
    public function handle(Request $request, Closure $next): Response
    {
        $nonce = rtrim(strtr(base64_encode(random_bytes(24)), '+/', '-_'), '=');
        $request->attributes->set('queen_dashboard_csp_nonce', $nonce);

        $response = $next($request);
        $response->headers->set(
            'Content-Security-Policy',
            "default-src 'none'; style-src 'nonce-{$nonce}'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
        );
        $response->headers->set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
        $response->headers->set('Pragma', 'no-cache');
        $response->headers->set('X-Frame-Options', 'DENY');
        $response->headers->set('X-Content-Type-Options', 'nosniff');
        $response->headers->set('Referrer-Policy', 'no-referrer');

        return $response;
    }
}
