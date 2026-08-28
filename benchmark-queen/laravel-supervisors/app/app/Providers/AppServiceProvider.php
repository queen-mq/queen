<?php

namespace App\Providers;

use App\Support\JsonlResultSink;
use Illuminate\Support\ServiceProvider;

final class AppServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->singleton(JsonlResultSink::class, function (): JsonlResultSink {
            return new JsonlResultSink((string) config('benchmark.results_directory'));
        });
    }
}
