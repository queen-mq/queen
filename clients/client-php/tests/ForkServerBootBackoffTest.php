<?php

namespace Queen\Tests;

use Illuminate\Queue\QueueManager;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use ReflectionMethod;
use ReflectionProperty;

/**
 * A fork server that fails to boot is not booted again at every exit that
 * queue:restart causes: a boot can block the master for up to a minute. The
 * Rust engine asserts the same
 * (`a_failed_fork_server_boot_backs_off_and_a_boot_resets_it`).
 */
final class ForkServerBootBackoffTest extends TestCase
{
    private string $directory = '';

    protected function setUp(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_kill')) {
            $this->markTestSkipped('ext-posix on a Unix host is required.');
        }
        $this->directory = sys_get_temp_dir() . '/queen-boot-' . bin2hex(random_bytes(6));
        mkdir($this->directory, 0700);
        // The fork server's stand-in: it counts its boots and fails each one.
        file_put_contents($this->directory . '/artisan', "echo boot >> \"\$(dirname \"\$0\")/boots\"\nexit 1\n");
    }

    protected function tearDown(): void
    {
        foreach (glob($this->directory . '/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->directory);
    }

    public function testAFailedBootWaitsBeforeTheNextAndRequestsMeanwhileShareOneBoot(): void
    {
        $supervisor = $this->supervisor();
        $refresh = new ReflectionMethod(PhpSupervisor::class, 'refreshForkServer');

        (new ReflectionMethod(PhpSupervisor::class, 'startForkServer'))->invoke($supervisor);
        $this->assertSame(1, $this->boots());
        $firstWait = $this->property($supervisor, 'forkBootRetryAt') - microtime(true);
        $this->assertEqualsWithDelta(60, $firstWait, 2);

        // Workers stop for queue:restart, one pass after another, during the wait.
        foreach (range(1, 3) as $_) {
            $this->setProperty($supervisor, 'forkServerStale', true);
            $refresh->invoke($supervisor);
        }
        $this->assertSame(1, $this->boots(), 'each queue:restart exit booted the failing server again');
        $this->assertTrue($this->property($supervisor, 'forkServerStale'), 'the request is kept for the next boot');

        // The wait is over: one boot for every request, and the next wait doubles.
        $this->setProperty($supervisor, 'forkBootRetryAt', microtime(true) - 1);
        $refresh->invoke($supervisor);
        $refresh->invoke($supervisor);
        $this->assertSame(2, $this->boots());
        $this->assertFalse($this->property($supervisor, 'forkServerStale'));
        $this->assertEqualsWithDelta(120, $this->property($supervisor, 'forkBootRetryAt') - microtime(true), 2);
    }

    private function supervisor(): PhpSupervisor
    {
        $config = SupervisorConfiguration::resolve([
            'url' => 'http://queen.test:6632',
            'supervisor' => ['prefork' => true, 'supervisors' => ['orders' => ['queues' => ['high']]]],
        ], $this->directory);
        $config['php_binary'] = '/bin/sh';
        $config['artisan'] = $this->directory . '/artisan';
        $config['state_directory'] = $this->directory . '/state';

        return new PhpSupervisor($this->createStub(QueueManager::class), $config, output: static function (): void {
        });
    }

    private function boots(): int
    {
        $boots = @file_get_contents($this->directory . '/boots');

        return is_string($boots) ? substr_count($boots, 'boot') : 0;
    }

    private function property(object $object, string $name): mixed
    {
        return (new ReflectionProperty($object, $name))->getValue($object);
    }

    private function setProperty(object $object, string $name, mixed $value): void
    {
        (new ReflectionProperty($object, $name))->setValue($object, $value);
    }
}
