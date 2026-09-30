<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Http\Response;
use Queen\Laravel\Dashboard\DashboardRepository;
use Queen\Laravel\Dashboard\PrometheusMetrics;

final class MetricsController
{
    public function __invoke(DashboardRepository $dashboard): Response
    {
        return new Response(PrometheusMetrics::render($dashboard->supervision()), 200, [
            'Content-Type' => 'text/plain; version=0.0.4; charset=utf-8',
            'Cache-Control' => 'no-store',
        ]);
    }
}
