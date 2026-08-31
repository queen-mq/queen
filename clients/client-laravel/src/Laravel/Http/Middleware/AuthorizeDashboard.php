<?php

namespace Queen\Laravel\Http\Middleware;

use Closure;
use Illuminate\Contracts\Auth\Access\Gate;
use Illuminate\Contracts\Config\Repository as ConfigRepository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

final class AuthorizeDashboard
{
    public const ABILITY = 'viewQueenDashboard';

    public function __construct(
        private Application $app,
        private Gate $gate,
        private ConfigRepository $config,
    ) {
    }

    public function handle(Request $request, Closure $next): Response
    {
        // Route caches are snapshots. Keep the runtime kill switch fail-closed
        // even when an older enabled route cache is still deployed.
        abort_unless($this->config->get('queen.dashboard.enabled', false) === true, 404);

        if ($this->gate->has(self::ABILITY)) {
            abort_unless($this->gate->check(self::ABILITY), 403);

            return $next($request);
        }

        $allowLocal = $this->config->get('queen.dashboard.allow_local', true) === true;
        abort_unless($allowLocal && $this->app->environment(['local', 'testing']), 403);

        return $next($request);
    }
}
