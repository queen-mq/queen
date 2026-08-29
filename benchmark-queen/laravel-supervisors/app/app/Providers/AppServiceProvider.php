<?php

namespace App\Providers;

use App\Support\BenchmarkEffectLedger;
use App\Support\JsonlResultSink;
use Illuminate\Support\ServiceProvider;

final class AppServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->singleton(BenchmarkEffectLedger::class, function (): BenchmarkEffectLedger {
            return new BenchmarkEffectLedger(
                (string) config('benchmark.results_directory'),
                (string) config('benchmark.ledger_mode'),
            );
        });

        $this->app->singleton(JsonlResultSink::class, function (): JsonlResultSink {
            return new JsonlResultSink((string) config('benchmark.results_directory'));
        });
    }
}
