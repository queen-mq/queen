<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Dashboard\ApplicationSettings;

final class ApplicationSettingsTest extends TestCase
{
    public function testAConnectionRunsWithItsOwnValuesOverTheQueenDefaults(): void
    {
        $settings = $this->settings(['prefetch' => 1, 'ack_batch' => 1], ['prefetch' => '4', 'lease_renewal' => true]);

        $this->assertSame(4, $settings->connectionInteger('prefetch', 1));
        $this->assertTrue($settings->connectionSwitch('lease_renewal', false));
        $this->assertSame(1, $settings->connectionInteger('ack_batch', 1));
        $this->assertSame(
            ['name' => 'prefetch', 'env' => 'QUEEN_PREFETCH', 'value' => '4', 'default' => '1', 'changed' => true, 'invalid' => false],
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

    public function testTheLeaseServiceIsOnUnlessTheMastersEnvironmentTurnsItOff(): void
    {
        // As the Rust supervisor reads it: 0, false, no or off, in any case.
        foreach ([[null, false], ['', false], ['false', true], [' Off ', true], ['0', true], ['no', true], ['true', false], ['banana', false]] as [$value, $disabled]) {
            $settings = new ApplicationSettings(['queen' => [], 'env' => ['QUEEN_SUPERVISOR_LEASE_SERVICE' => $value]]);
            $this->assertSame($disabled, $settings->leaseServiceDisabled(), var_export($value, true));
        }
        $off = new ApplicationSettings(['queen' => [], 'env' => ['QUEEN_SUPERVISOR_LEASE_SERVICE' => 'false']]);
        $this->assertSame(['off', 'on'], $this->valueAndDefault($off, 'supervisor', 'QUEEN_SUPERVISOR_LEASE_SERVICE'));
    }

    public function testFastScaleUpNamesThePoolsThatUseIt(): void
    {
        $settings = $this->settings(['supervisor' => ['supervisors' => [
            'emails' => ['queues' => ['emails'], 'fast_scale_up' => true],
            'reports' => ['queues' => ['reports']],
        ]]]);

        $this->assertSame(['on for emails', 'off'], $this->valueAndDefault($settings, 'supervisor', 'fast_scale_up'));
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
        $settings = new ApplicationSettings(['queen' => 'nonsense', 'queue' => ['connections' => 7], 'env' => 3]);

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
            'env' => [],
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
