<?php

namespace Queen\Tests;

use Orchestra\Testbench\TestCase;
use Queen\Laravel\Supervisor\SupervisorConfiguration;

/**
 * The published config/queen.php reads the environment only for values that
 * differ per environment or deployment. Every other value is plain config, so
 * the list below cannot grow without this test changing with it.
 */
final class LaravelConfigFileTest extends TestCase
{
    private const CONFIG = __DIR__ . '/../config/queen.php';

    /** The Queen environment variables config/queen.php reads. */
    public const ENVIRONMENT = [
        'QUEEN_URL',
        'QUEEN_URLS',
        'QUEEN_BEARER_TOKEN',
        'QUEEN_QUEUE',
        'QUEEN_CONSUMER_GROUP',
        'QUEEN_PARTITIONS',
        'QUEEN_SUPERVISOR_READ_BEARER_TOKEN',
        'QUEEN_SUPERVISOR_STATE_DIRECTORY',
        'QUEEN_SUPERVISOR_REMOTE_STATUS',
        'QUEEN_SUPERVISOR_PREFORK',
        'QUEEN_SUPERVISOR_COORDINATION',
        'QUEEN_DASHBOARD_ENABLED',
        'QUEEN_DASHBOARD_PATH',
        'QUEEN_DASHBOARD_DOMAIN',
        'QUEEN_DASHBOARD_CONSOLE_URL',
        'QUEEN_NOTIFY_MAIL',
        'QUEEN_METRICS_ENABLED',
        'QUEEN_METRICS_TOKEN',
        'QUEEN_SUPERVISOR_INSTALL_PATH',
        'QUEEN_SUPERVISOR_RELEASE_BASE_URL',
    ];

    /** Laravel's own variables, for the default remote status key. */
    private const LARAVEL_ENVIRONMENT = ['APP_NAME', 'APP_ENV'];

    public function testTheConfigReadsOnlyTheEnvironmentVariablesListedHere(): void
    {
        $read = $this->environmentNames((string) file_get_contents(self::CONFIG));
        $expected = [...self::ENVIRONMENT, ...self::LARAVEL_ENVIRONMENT];
        sort($read);
        sort($expected);

        $this->assertCount(20, self::ENVIRONMENT);
        $this->assertSame($expected, $read);
    }

    public function testTheDashboardNamesOnlyEnvironmentVariablesTheConfigReads(): void
    {
        $sources = [
            __DIR__ . '/../src/Laravel/Dashboard/ApplicationSettings.php',
            __DIR__ . '/../src/Laravel/Dashboard/TuningAdvisor.php',
            ...(glob(__DIR__ . '/../resources/views/dashboard/{,*/}*.blade.php', GLOB_BRACE) ?: []),
        ];
        foreach ($sources as $source) {
            preg_match_all('/\bQUEEN_[A-Z0-9_]+\b/', (string) file_get_contents($source), $matches);
            $this->assertSame([], array_values(array_diff(array_unique($matches[0]), self::ENVIRONMENT)), basename($source));
        }
    }

    public function testUnsetVariablesLeaveTheDefaults(): void
    {
        $config = $this->evaluate([]);

        $this->assertSame(['http://localhost:6632', null, null], [$config['url'], $config['urls'], $config['bearer_token']]);
        $this->assertSame([30000, 3, 1000, 30000], [$config['timeout'], $config['retry_attempts'], $config['retry_delay'], $config['health_retry_after']]);
        $this->assertSame([64, 90, 1, false], [$config['partitions'], $config['retry_after'], $config['prefetch'], $config['lease_renewal']]);
        $this->assertSame(['default', 'laravel'], [$config['queue'], $config['consumer_group']]);
        $this->assertSame(['queen:default' => 60], $config['waits']);
        $supervisor = $config['supervisor'];
        $this->assertSame([3, 75, 256, null], [$supervisor['poll_interval'], $supervisor['shutdown_grace'], $supervisor['process_limit'], $supervisor['heartbeat_timeout']]);
        $this->assertSame([false, false, true, false], [$supervisor['prefork'], $supervisor['event_driven'], $supervisor['lease_service'], $supervisor['coordination']['enabled']]);
        $this->assertSame([['default'], 'laravel', 10, false], [
            $supervisor['supervisors']['default']['queues'],
            $supervisor['supervisors']['default']['consumer_group'],
            $supervisor['supervisors']['default']['max_processes'],
            $supervisor['supervisors']['default']['fast_scale_up'],
        ]);
        $this->assertSame([true, true, false], [$config['job_metrics']['enabled'], $config['tags']['enabled'], $config['metrics']['enabled']]);
        $this->assertSame([null, null], [$config['supervisor_binary']['manifest'], $config['supervisor_binary']['manifest_sha256']]);
    }

    public function testTheQueueAndConsumerGroupReachEveryKeyThatUsesThem(): void
    {
        $config = $this->evaluate(['QUEEN_QUEUE' => 'emails', 'QUEEN_CONSUMER_GROUP' => 'mailers']);

        $this->assertSame(['emails', 'mailers'], [$config['queue'], $config['consumer_group']]);
        $pool = $config['supervisor']['supervisors']['default'];
        $this->assertSame([['emails'], 'mailers'], [$pool['queues'], $pool['consumer_group']]);
        $this->assertSame(['queen:emails' => 60], $config['waits']);
    }

    public function testTheRemoteStatusKeyDefaultsToTheApplicationNameAndEnvironment(): void
    {
        foreach ([
            [[], 'laravel-production'],
            [['APP_NAME' => 'Acme Shop', 'APP_ENV' => 'staging'], 'acme-shop-staging'],
            // The instance slots are <key>/<instance>: no slash, no space.
            [['APP_NAME' => 'Ölshop/EU', 'APP_ENV' => 'prod eu'], 'olshopeu-prod-eu'],
        ] as [$environment, $expected]) {
            $config = $this->evaluate($environment);
            $remoteStatus = $config['supervisor']['remote_status'];

            $this->assertSame($expected, $remoteStatus['key']);
            $this->assertFalse($remoteStatus['enabled']);
            // As the supervisor and the dashboard resolve it once remote status is on.
            $settings = SupervisorConfiguration::remoteStatusSettings(
                ['remote_status' => [...$remoteStatus, 'enabled' => true]],
                $config,
                [],
            );
            $this->assertSame($expected, $settings['key']);
        }
    }

    /**
     * The names of every env() call, each of which must take a literal name.
     * No other way of reading the environment is allowed in the file.
     *
     * @return list<string>
     */
    private function environmentNames(string $source): array
    {
        $tokens = array_values(array_filter(
            token_get_all($source),
            static fn (mixed $token): bool => !is_array($token) || !in_array($token[0], [T_WHITESPACE, T_COMMENT, T_DOC_COMMENT], true),
        ));
        $names = [];
        foreach ($tokens as $index => $token) {
            if (!is_array($token)) {
                continue;
            }
            $name = strtolower(ltrim($token[1], '\\'));
            $next = $tokens[$index + 1] ?? null;
            $this->assertFalse(
                in_array($name, ['getenv', '$_env', '$_server'], true)
                    || str_ends_with($name, 'support\env')
                    || ($name === 'env' && is_array($next) && $next[0] === T_DOUBLE_COLON),
                "config/queen.php reads the environment through {$token[1]}",
            );
            if (!in_array($token[0], [T_STRING, T_NAME_FULLY_QUALIFIED], true) || $name !== 'env' || $next !== '(') {
                continue;
            }
            $argument = $tokens[$index + 2] ?? null;
            $this->assertTrue(is_array($argument) && $argument[0] === T_CONSTANT_ENCAPSED_STRING, 'env() takes a literal name');
            $names[] = stripslashes(substr($argument[1], 1, -1));
        }

        return array_values(array_unique($names));
    }

    /**
     * config/queen.php evaluated with these variables set and every other one
     * it reads unset.
     *
     * @param array<string, string> $environment
     * @return array<string, mixed>
     */
    private function evaluate(array $environment): array
    {
        $saved = [];
        foreach ([...self::ENVIRONMENT, ...self::LARAVEL_ENVIRONMENT] as $name) {
            $saved[$name] = [getenv($name), $_ENV[$name] ?? null, $_SERVER[$name] ?? null];
            $value = $environment[$name] ?? null;
            if ($value === null) {
                putenv($name);
                unset($_ENV[$name], $_SERVER[$name]);
            } else {
                putenv("{$name}={$value}");
                $_ENV[$name] = $_SERVER[$name] = $value;
            }
        }

        try {
            return require self::CONFIG;
        } finally {
            foreach ($saved as $name => [$fromPutenv, $fromEnv, $fromServer]) {
                putenv($fromPutenv === false ? $name : "{$name}={$fromPutenv}");
                if ($fromEnv === null) {
                    unset($_ENV[$name]);
                } else {
                    $_ENV[$name] = $fromEnv;
                }
                if ($fromServer === null) {
                    unset($_SERVER[$name]);
                } else {
                    $_SERVER[$name] = $fromServer;
                }
            }
        }
    }
}
