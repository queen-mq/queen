<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Dashboard\ApplicationSettings;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\QueenQueue;

final class ApplicationSettingsTest extends TestCase
{
    public function testAConnectionRunsWithItsOwnValuesOverTheQueenDefaults(): void
    {
        $settings = $this->settings(['prefetch' => 1, 'ack_batch' => 1], ['prefetch' => '4', 'lease_renewal' => true]);

        $this->assertSame(4, $settings->connectionInteger('prefetch', 1));
        $this->assertTrue($settings->connectionSwitch('lease_renewal', false));
        $this->assertSame(1, $settings->connectionInteger('ack_batch', 1));
        // Both read integers with filter_var or a signed pattern, so "+8" and " 8 " are 8.
        $this->assertSame(8, $this->settings(['bulk_batch' => '+8'])->connectionInteger('bulk_batch', 100));
        $this->assertSame(8, $this->settings(['bulk_batch' => ' 8 '])->connectionInteger('bulk_batch', 100));
        $this->assertSame(
            ['name' => 'prefetch', 'env' => null, 'value' => '4', 'default' => '1', 'changed' => true, 'invalid' => false],
            array_diff_key($this->row($settings, 'connection', 'prefetch'), ['meaning' => true]),
        );
        $this->assertNotSame('', $this->row($settings, 'connection', 'prefetch')['meaning']);
    }

    public function testAValueOfTheWrongTypeShowsAsInvalidNeverAsItself(): void
    {
        $settings = $this->settings(['lease_renewal' => 'yes please', 'retry_after' => '-5', 'supervisor' => ['poll_interval' => 'soon']]);

        $this->assertNull($settings->connectionSwitch('lease_renewal', false));
        $this->assertSame('invalid', $this->row($settings, 'connection', 'lease_renewal')['value']);
        $this->assertTrue($this->row($settings, 'connection', 'retry_after')['invalid']);
        $this->assertSame('invalid', $this->row($settings, 'supervisor', 'poll_interval')['value']);
        $this->assertStringNotContainsString('yes please', json_encode($settings->rows()));
    }

    public function testCredentialsShowOnlyWhetherTheyAreSet(): void
    {
        $settings = $this->settings([
            'url' => 'https://usr-0b3c:pw-6f1d@queen.example.test:6632/base?tenant=t-8c2e',
            'bearer_token' => 'tok-3a9b',
            'headers' => ['Authorization' => 'Bearer hdr-77e1', 'X-Tenant' => 'tenant-91fa'],
            'supervisor' => [
                'read_bearer_token' => 'read-41c0',
                'remote_status' => ['enabled' => true, 'key' => 'status-key-5d2f'],
            ],
        ]);

        $rendered = json_encode($settings->rows());
        foreach (['pw-6f1d', 'usr-0b3c', 't-8c2e', 'tok-3a9b', 'hdr-77e1', 'tenant-91fa', 'read-41c0', 'status-key-5d2f'] as $secret) {
            $this->assertStringNotContainsString($secret, $rendered);
        }
        $this->assertSame('https://queen.example.test:6632', $this->row($settings, 'connection', 'url')['value']);
        $this->assertSame('set', $this->row($settings, 'connection', 'bearer_token')['value']);
        $this->assertSame('2 set, values hidden', $this->row($settings, 'connection', 'headers')['value']);
        $this->assertSame('set', $this->row($settings, 'supervisor', 'read_bearer_token')['value']);
        $this->assertSame('set', $this->row($settings, 'supervisor', 'remote_status.key')['value']);
        $this->assertSame('not set', $this->row($this->settings([]), 'connection', 'bearer_token')['value']);
        // QueenConnector also reads the client's own spelling.
        $this->assertSame('set', $this->row($this->settings([], ['bearerToken' => 'tok-c4d2']), 'connection', 'bearer_token')['value']);
    }

    public function testWhatTheConnectorRefusesTogetherIsMarkedWithTheReason(): void
    {
        $refused = [
            'prefetch' => [['prefetch' => 4], '4 (needs lease_renewal)'],
            'pop_ahead' => [['pop_ahead' => true], 'on (needs lease_renewal)'],
            'ack_batch' => [['prefetch' => 2, 'ack_batch' => 3, 'lease_renewal' => true], '3 (above prefetch)'],
            'ack_async' => [['prefetch' => 2, 'ack_batch' => 2, 'ack_async' => true, 'lease_renewal' => true], 'on (needs ack_batch 1)'],
            'lease_renewal' => [['lease_renewal' => true, 'retry_after' => 10], 'on (renewal timing does not fit in retry_after)'],
        ];
        foreach ($refused as $name => [$connection, $shown]) {
            $row = $this->row($this->settings([], $connection), 'connection', $name);
            $this->assertSame([$shown, true], [$row['value'], $row['invalid']], $name);
            try {
                (new QueenConnector())->connect(['driver' => 'queen', ...$connection]);
                $this->fail("QueenConnector accepted what the page marks: {$name}");
            } catch (\InvalidArgumentException) {
                // The worker refuses it too.
            }
        }

        $tuned = ['prefetch' => 4, 'ack_async' => true, 'pop_ahead' => true, 'lease_renewal' => true];
        $this->assertInstanceOf(QueenQueue::class, (new QueenConnector())->connect(['driver' => 'queen', ...$tuned]));
        $this->assertSame([], array_values(array_filter(
            $this->settings([], $tuned)->rows()['connection'],
            static fn (array $row): bool => $row['invalid'],
        )));
    }

    public function testAutoPrefetchShowsAsAutoAndNeedsLeaseRenewalLikeAPrefetchAboveOne(): void
    {
        $row = $this->row($this->settings([], ['prefetch' => 'auto', 'lease_renewal' => true]), 'connection', 'prefetch');
        $this->assertSame(['auto, up to 16 per pop', false, true], [$row['value'], $row['invalid'], $row['changed']]);

        $refused = $this->row($this->settings([], ['prefetch' => 'auto']), 'connection', 'prefetch');
        $this->assertSame(['auto, up to 16 per pop (needs lease_renewal)', true], [$refused['value'], $refused['invalid']]);

        $tooMany = $this->row($this->settings([], ['prefetch' => 'auto', 'ack_batch' => 17, 'lease_renewal' => true]), 'connection', 'ack_batch');
        $this->assertSame(['17 (above prefetch)', true], [$tooMany['value'], $tooMany['invalid']]);
    }

    public function testAPoolReadsItsConnectionAsTheSupervisorDoes(): void
    {
        $settings = new ApplicationSettings([
            'queen' => ['lease_renewal' => true, 'retry_after' => 120, 'supervisor' => ['supervisors' => [
                'mail' => ['connection' => 'mail', 'queues' => ['mail']],
                'main' => ['queues' => ['default'], 'min_processes' => 6, 'max_processes' => 4],
            ]]],
            'queue' => ['connections' => ['queen' => ['driver' => 'queen'], 'mail' => ['driver' => 'queen']]],
        ]);

        $pools = array_column($settings->pools(), null, 'name');
        // config('queen') stands under the queen connection only, except retry_after.
        $this->assertSame([false, 120], [$pools['mail']['lease_renewal'], $pools['mail']['retry_after']]);
        $this->assertSame([true, 120], [$pools['main']['lease_renewal'], $pools['main']['retry_after']]);
        $this->assertSame('min_processes above max_processes', $pools['main']['refused']);
        $this->assertNull($pools['mail']['refused']);
    }

    public function testEveryBrokerEndpointIsListedWithoutItsCredentials(): void
    {
        $settings = $this->settings(['urls' => 'http://queen-0:6632, https://ops:pw-e5a1@queen-1.example.test ,ftp://nope']);

        $row = $this->row($settings, 'connection', 'urls');
        $this->assertSame('http://queen-0:6632, https://queen-1.example.test, 1 invalid', $row['value']);
        $this->assertTrue($row['invalid']);
        $this->assertSame('QUEEN_URLS', $row['env']);
        $this->assertStringNotContainsString('pw-e5a1', json_encode($settings->rows()));
    }

    public function testUnsetComputedDefaultsSayHowTheSupervisorComputesThem(): void
    {
        $settings = $this->settings(['retry_after' => 90, 'lease_renewal_interval' => null, 'supervisor' => ['heartbeat_timeout' => null, 'poll_interval' => 2]]);

        $this->assertSame(['30 s', 'retry_after / 3'], $this->valueAndDefault($settings, 'connection', 'lease_renewal_interval'));
        $this->assertSame(['computed at start', 'control-loop budget + 1 s'], $this->valueAndDefault($settings, 'supervisor', 'heartbeat_timeout'));
        $this->assertSame(['2 s', 'poll_interval'], $this->valueAndDefault($settings, 'supervisor', 'remote_status.interval'));
        $this->assertFalse($this->row($settings, 'supervisor', 'heartbeat_timeout')['changed']);
    }

    public function testTheLeaseServiceIsOnUnlessTheConfigurationTurnsItOff(): void
    {
        // As SupervisorConfiguration reads it: a real boolean, on when unset.
        foreach ([[null, false, 'on'], [true, false, 'on'], [false, true, 'off'], ['false', false, 'invalid'], [0, false, 'invalid']] as [$value, $disabled, $shown]) {
            $settings = $this->settings(['supervisor' => ['lease_service' => $value]]);
            $this->assertSame($disabled, $settings->leaseServiceDisabled(), var_export($value, true));
            $this->assertSame([$shown, 'on'], $this->valueAndDefault($settings, 'supervisor', 'lease_service'), var_export($value, true));
        }
        $off = $this->row($this->settings(['supervisor' => ['lease_service' => false]]), 'supervisor', 'lease_service');
        $this->assertSame([null, true], [$off['env'], $off['changed']]);
    }

    public function testOnlyTheEnvironmentVariablesTheConfigReadsAreShown(): void
    {
        $settings = $this->settings(['autopilot' => false]);
        $variables = array_column([...$settings->rows()['connection'], ...$settings->rows()['supervisor']], 'env', 'name');

        $this->assertSame([
            'url' => 'QUEEN_URL',
            'bearer_token' => 'QUEEN_BEARER_TOKEN',
            'partitions' => 'QUEEN_PARTITIONS',
            'read_bearer_token' => 'QUEEN_SUPERVISOR_READ_BEARER_TOKEN',
            'prefork' => 'QUEEN_SUPERVISOR_PREFORK',
            'coordination.enabled' => 'QUEEN_SUPERVISOR_COORDINATION',
            'remote_status.enabled' => 'QUEEN_SUPERVISOR_REMOTE_STATUS',
        ], array_filter($variables));
        $this->assertSame([], array_values(array_diff(array_filter($variables), LaravelConfigFileTest::ENVIRONMENT)));
        // Every other row is a plain config value, shown by its key alone.
        foreach (['prefetch', 'retry_after', 'sync_failed_jobs', 'autopilot', 'shutdown_grace', 'event_driven', 'fast_scale_up', 'lease_service', 'remote_status.key', 'remote_status.ttl'] as $name) {
            $this->assertArrayHasKey($name, $variables);
            $this->assertNull($variables[$name], $name);
        }
    }

    public function testFastScaleUpNamesThePoolsThatUseIt(): void
    {
        $settings = $this->settings(['supervisor' => ['supervisors' => [
            'emails' => ['queues' => ['emails'], 'fast_scale_up' => true],
            'reports' => ['queues' => ['reports']],
        ]]]);

        $this->assertSame(['on for emails', 'off'], $this->valueAndDefault($settings, 'supervisor', 'fast_scale_up'));
    }

    public function testPreforkNamesThePoolsThatDifferFromTheSwitch(): void
    {
        $pools = ['kafka' => ['queues' => ['kafka'], 'prefork' => false], 'emails' => ['queues' => ['emails']]];

        $this->assertSame(
            ['on, off for kafka', 'off'],
            $this->valueAndDefault($this->settings(['supervisor' => ['prefork' => true, 'supervisors' => $pools]]), 'supervisor', 'prefork'),
        );
        $this->assertSame(
            ['off, on for emails', 'off'],
            $this->valueAndDefault($this->settings(['supervisor' => ['supervisors' => [
                'emails' => ['queues' => ['emails'], 'prefork' => true],
                'reports' => ['queues' => ['reports']],
            ]]]), 'supervisor', 'prefork'),
        );
        $this->assertSame(
            'invalid',
            $this->row($this->settings(['supervisor' => ['supervisors' => ['default' => ['prefork' => 'yes']]]]), 'supervisor', 'prefork')['value'],
        );
    }

    public function testPoolsGetTheDefaultsTheSupervisorApplies(): void
    {
        $settings = $this->settings(
            ['queue' => 'default', 'retry_after' => 120, 'supervisor' => ['supervisors' => [
                'emails' => ['queues' => 'emails, notifications', 'max_processes' => '4', 'balance' => false],
                'broken' => ['queues' => ['x'], 'timeout' => 'long', 'balance' => 'sideways'],
            ]]],
            ['lease_renewal' => true],
        );

        $pools = array_column($settings->pools(), null, 'name');
        $this->assertSame(['emails', 'notifications'], $pools['emails']['queues']);
        $this->assertSame(
            ['balance' => 'off', 'strategy' => 'size', 'processes' => 4, 'min_processes' => 1, 'max_processes' => 4, 'timeout' => 60,
                'retry_after' => 120, 'tries' => 3, 'memory' => 128, 'backoff' => 0, 'max_jobs' => 0, 'max_time' => 0, 'sleep' => 1],
            array_intersect_key($pools['emails'], array_flip(['balance', 'strategy', 'processes', 'min_processes', 'max_processes', 'timeout', 'retry_after', 'tries', 'memory', 'backoff', 'max_jobs', 'max_time', 'sleep'])),
        );
        $this->assertTrue($pools['emails']['lease_renewal']);
        $this->assertSame(['laravel', 'queen'], [$pools['emails']['consumer_group'], $pools['emails']['connection']]);
        $this->assertNull($pools['broken']['timeout'], 'the supervisor would refuse it');
        $this->assertNull($pools['broken']['balance']);
        $this->assertSame(['default'], array_column((new ApplicationSettings(['queen' => []]))->pools(), 'name'), 'no pool configured is one default pool');
    }

    public function testThePoolTableShowsWhatTheRunningSupervisorPublished(): void
    {
        $settings = $this->settings(['supervisor' => ['supervisors' => ['default' => ['queues' => ['high'], 'timeout' => 60, 'backoff' => 5, 'sleep' => 3]]]]);
        $published = [
            ['name' => 'default', 'connection' => 'queen', 'consumer_group' => 'laravel', 'queues' => ['high'], 'balance' => 'auto', 'strategy' => 'size',
                'processes' => 8, 'min_processes' => 1, 'max_processes' => 8, 'timeout' => 120, 'retry_after' => 180, 'tries' => 3, 'memory' => 256],
            ['name' => 'elsewhere', 'connection' => 'queen', 'consumer_group' => 'laravel', 'queues' => ['low'], 'balance' => 'simple', 'strategy' => 'size',
                'processes' => 2, 'min_processes' => 1, 'max_processes' => 2, 'timeout' => 60, 'retry_after' => 90, 'tries' => 3, 'memory' => 128],
        ];

        $table = $settings->poolTable($published);

        $this->assertSame('published', $table['source']);
        $this->assertSame([120, 5, 3], [$table['pools'][0]['timeout'], $table['pools'][0]['backoff'], $table['pools'][0]['sleep']]);
        $this->assertSame([null, null], [$table['pools'][1]['backoff'], $table['pools'][1]['max_jobs']], 'not configured on this host');

        $local = $settings->poolTable([]);
        $this->assertSame('application', $local['source']);
        $this->assertSame([60, 5], [$local['pools'][0]['timeout'], $local['pools'][0]['backoff']]);
    }

    public function testConfigurationOfTheWrongShapeYieldsDefaultsNotErrors(): void
    {
        $settings = new ApplicationSettings(['queen' => 'nonsense', 'queue' => ['connections' => 7]]);

        $this->assertSame(1, $settings->connectionInteger('prefetch', 1));
        $this->assertFalse($settings->leaseServiceDisabled());
        $this->assertNotSame([], $settings->rows()['connection']);
        $this->assertSame('default', $settings->pools()[0]['name']);
    }

    /**
     * @param array<string, mixed> $queen
     * @param array<string, mixed> $connection
     */
    private function settings(array $queen, array $connection = []): ApplicationSettings
    {
        return new ApplicationSettings([
            'queen' => $queen,
            'queue' => ['connections' => ['queen' => ['driver' => 'queen', ...$connection]]],
        ]);
    }

    /** @return array<string, mixed> */
    private function row(ApplicationSettings $settings, string $group, string $name): array
    {
        foreach ($settings->rows()[$group] as $row) {
            if ($row['name'] === $name) {
                return $row;
            }
        }
        $this->fail("No {$group} setting {$name}.");
    }

    /** @return array{0: string, 1: string} */
    private function valueAndDefault(ApplicationSettings $settings, string $group, string $name): array
    {
        $row = $this->row($settings, $group, $name);

        return [$row['value'], $row['default']];
    }
}
