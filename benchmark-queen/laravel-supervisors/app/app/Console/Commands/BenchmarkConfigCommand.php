<?php

namespace App\Console\Commands;

use Composer\InstalledVersions;
use Illuminate\Console\Command;
use JsonException;
use RuntimeException;

final class BenchmarkConfigCommand extends Command
{
    protected $signature = 'bench:config';

    protected $description = 'Print the normalized benchmark configuration and pinned package versions';

    public function handle(): int
    {
        $horizonDefaults = config('horizon.defaults.bench', []);
        $horizonEnvironment = config('horizon.environments.benchmark.bench', []);
        $payload = [
            'php' => PHP_VERSION,
            'laravel' => InstalledVersions::getPrettyVersion('laravel/framework'),
            'horizon' => InstalledVersions::getPrettyVersion('laravel/horizon'),
            'queen_client' => InstalledVersions::getPrettyVersion('queen-mq/php-client'),
            'benchmark' => config('benchmark'),
            'horizon_supervisor' => array_replace(
                is_array($horizonDefaults) ? $horizonDefaults : [],
                is_array($horizonEnvironment) ? $horizonEnvironment : [],
            ),
            'queen_process_limit' => config('queen.supervisor.process_limit'),
            'queen_supervisor' => config('queen.supervisor.supervisors.bench'),
            'queen_connection' => config('queue.connections.queen'),
        ];

        try {
            $this->line(json_encode(
                $payload,
                JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES,
            ));
        } catch (JsonException $exception) {
            throw new RuntimeException('Unable to encode benchmark configuration.', previous: $exception);
        }

        return self::SUCCESS;
    }
}
