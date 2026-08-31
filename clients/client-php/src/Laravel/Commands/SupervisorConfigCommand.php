<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Queen\Laravel\Supervisor\SupervisorConfiguration;

class SupervisorConfigCommand extends Command
{
    protected $signature = 'queen:supervisor-config
        {--pretty : Pretty-print the JSON document}
        {--for-engine : Include credentials required by a supervisor engine}';
    protected $description = 'Print the resolved Queen supervisor configuration as JSON';

    public function handle(): int
    {
        try {
            $resolved = SupervisorConfiguration::resolve(
                $this->laravel['config']->get('queen', []),
                $this->laravel->basePath(),
                queueConnections: $this->laravel['config']->get('queue.connections', []),
            );
        } catch (\InvalidArgumentException $exception) {
            $this->components->error($exception->getMessage());

            return self::FAILURE;
        }
        if (!$this->option('for-engine')) {
            $resolved = $this->redact($resolved);
        }
        $resolved = $this->normalizeJsonMaps($resolved);

        $flags = JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR;
        if ($this->option('pretty')) {
            $flags |= JSON_PRETTY_PRINT;
        }
        $json = json_encode($resolved, $flags);
        // Symfony appends a newline; include it because the Rust reader bounds
        // the complete stdout transport rather than only the JSON payload.
        if ($this->option('for-engine') && strlen($json) + 1 > SupervisorConfiguration::MAX_CONFIG_BYTES) {
            $this->components->error(
                'Resolved Queen supervisor engine configuration exceeds the 1 MiB transport limit.',
            );

            return self::FAILURE;
        }
        $this->output->writeln($json);

        return self::SUCCESS;
    }

    private function redact(array $config): array
    {
        foreach (array_keys($config['connections'] ?? []) as $connectionName) {
            if (($config['connections'][$connectionName]['bearer_token'] ?? null) !== null) {
                $config['connections'][$connectionName]['bearer_token'] = '[redacted]';
            }
            foreach ($config['connections'][$connectionName]['headers'] ?? [] as $name => $_value) {
                $config['connections'][$connectionName]['headers'][$name] = '[redacted]';
            }
        }
        if (($config['queen']['bearer_token'] ?? null) !== null) {
            $config['queen']['bearer_token'] = '[redacted]';
        }
        foreach ($config['queen']['headers'] ?? [] as $name => $_value) {
            $config['queen']['headers'][$name] = '[redacted]';
        }
        return $config;
    }

    private function normalizeJsonMaps(array $config): array
    {
        // PHP encodes an empty array as `[]`, but the v2 contract declares
        // headers as a string map and Rust correctly expects a JSON object.
        // Preserve non-empty associative maps and make the empty shape
        // unambiguous for every engine consuming this command.
        if (($config['queen']['headers'] ?? null) === []) {
            $config['queen']['headers'] = new \stdClass();
        }
        foreach (array_keys($config['connections'] ?? []) as $connectionName) {
            if (($config['connections'][$connectionName]['headers'] ?? null) === []) {
                $config['connections'][$connectionName]['headers'] = new \stdClass();
            }
        }

        return $config;
    }
}
