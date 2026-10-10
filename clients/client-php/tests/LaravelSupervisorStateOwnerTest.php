<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\ProcessIdentity;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Tests\Support\FakeProcessIdentity;
use RuntimeException;

/**
 * Who may read and write a supervisor state that another user owns. A
 * Kubernetes exec probe runs as the container's user, often root, while the
 * supervisor runs as www-data: root reads that state as www-data, and may
 * not write to it; anyone else is told whom to run as.
 */
class LaravelSupervisorStateOwnerTest extends TestCase
{
    /** @var list<string> */
    private array $temporaryDirectories = [];

    protected function tearDown(): void
    {
        foreach (array_reverse($this->temporaryDirectories) as $directory) {
            $this->removeDirectory($directory);
        }

        parent::tearDown();
    }

    public function testRootReadsAnotherUsersStateAsThatUserAndLeavesItAsItWas(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();
        $owner = posix_geteuid();
        $root = new FakeProcessIdentity(0, [0 => 'root', $owner => 'supervisor']);

        try {
            $state->writeStatus($this->readyStatus());
            $before = $this->directorySnapshot($directory);

            $reader = new SupervisorState($directory, $root);
            $status = $reader->status();
            $this->assertSame($state->instanceId(), $status['instance_id']);
            $this->assertTrue($reader->isLive($status));
            $this->assertTrue($reader->isOwned());
            $this->assertTrue($reader->isOwnedBy($state->instanceId()));
            $this->assertTrue($reader->readiness($status)['ready']);
            $this->assertTrue($reader->capacityHealth($status)['healthy']);

            // No lock file, control request, temporary file or mode change.
            $this->assertSame($before, $this->directorySnapshot($directory));
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        // The constructor's checks and each read ran as the owner, once per
        // public call (readiness() inside capacityHealth() does not switch
        // again), and root came back every time.
        $this->assertSame(
            array_fill(0, 7, ['uid' => $owner, 'name' => 'supervisor', 'gid' => 4321]),
            $root->assumed,
        );
        $this->assertSame(7, $root->restored);
        $this->assertSame(0, $root->effectiveUid());
    }

    public function testRootMayNotWriteToAnotherUsersState(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $lock = $state->acquireLock();
        $owner = posix_geteuid();

        try {
            $state->writeStatus($this->readyStatus());
            $before = $this->directorySnapshot($directory);
            $reader = new SupervisorState($directory, new FakeProcessIdentity(0, [0 => 'root', $owner => 'supervisor']));
            $instanceId = $state->instanceId();
            $writes = [
                'request' => fn () => $reader->request('pause', $instanceId),
                'command' => fn () => $reader->command(null, $instanceId),
                'writeStatus' => fn () => $reader->writeStatus(['state' => 'running']),
                'acquireLock' => fn () => $reader->acquireLock(),
                'telemetryDirectory' => fn () => $reader->telemetryDirectory(),
                'resetExitMarkers' => fn () => $reader->resetExitMarkers(),
                'takeExitMarker' => fn () => $reader->takeExitMarker(12345),
                'removeExitMarker' => fn () => $reader->removeExitMarker(12345),
                'removeTelemetryForPid' => fn () => $reader->removeTelemetryForPid(12345),
            ];
            foreach ($writes as $method => $write) {
                try {
                    $write();
                    $this->fail("Root wrote to another user's state through {$method}().");
                } catch (RuntimeException $error) {
                    $this->assertSame(
                        'Queen supervisor state ancestor [' . realpath($directory) . '] must be owned by root or the'
                        . " current user. It is owned by uid {$owner} (supervisor) and this process runs as uid 0"
                        . " (root). Root may only read another user's supervisor state; run the supervisor, pause,"
                        . ' continue and terminate as that user, for example su -s /bin/sh supervisor -c'
                        . ' "php artisan queen:supervisor pause".',
                        $error->getMessage(),
                        $method,
                    );
                }
            }
            $this->assertSame($before, $this->directorySnapshot($directory));

            // The owner's own control path is unchanged.
            $state->request('pause', $instanceId);
            $this->assertFileExists($directory . '/control.json');
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testAReaderThatIsNeitherRootNorTheOwnerIsToldWhoOwnsTheStateAndWhomToRunAs(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        (new SupervisorState($directory))->writeStatus($this->readyStatus());
        $owner = posix_geteuid();
        $stranger = $owner + 1000;
        $before = $this->directorySnapshot($directory);

        foreach ([
            [[$owner => 'supervisor'], " It is owned by uid {$owner} (supervisor) and this process runs as uid {$stranger}."
                . ' Run the supervisor and its probes as that user, for example su -s /bin/sh supervisor -c'
                . " \"php artisan queen:supervisor status --check\", or securityContext.runAsUser: {$owner} on the container."],
            // Without an account there is no name to give su.
            [[], " It is owned by uid {$owner} and this process runs as uid {$stranger}. Run the supervisor and its"
                . " probes as that user, for example securityContext.runAsUser: {$owner} on the container."],
        ] as [$names, $detail]) {
            try {
                new SupervisorState($directory, new FakeProcessIdentity($stranger, $names));
                $this->fail('A reader that is neither root nor the owner was accepted.');
            } catch (RuntimeException $error) {
                // The refusal starts as before; which one depends on whether
                // the temporary directory is below a sticky /tmp.
                $this->assertMatchesRegularExpression(
                    '/^Queen supervisor state (ancestor \[[^\]]+\] must be owned by root or the current user'
                    . '|child \[[^\]]+\] below a sticky directory must be owned by the current user)\. It is owned/',
                    $error->getMessage(),
                );
                $this->assertStringEndsWith($detail, $error->getMessage());
            }
        }
        $this->assertSame($before, $this->directorySnapshot($directory));
    }

    public function testRootDoesNotReadAStateWhoseOwnerHasNoAccountToSwitchTo(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        (new SupervisorState($directory))->writeStatus($this->readyStatus());
        $owner = posix_geteuid();
        $root = new FakeProcessIdentity(0, [0 => 'root']);

        try {
            new SupervisorState($directory, $root);
            $this->fail('Root read a state whose owner it cannot act as.');
        } catch (RuntimeException $error) {
            $this->assertStringEndsWith(
                " It is owned by uid {$owner} and this process runs as uid 0 (root). Run the supervisor and its probes"
                . " as that user, for example securityContext.runAsUser: {$owner} on the container.",
                $error->getMessage(),
            );
        }
        $this->assertSame([], $root->assumed);
    }

    public function testRootReadsStillRefuseSymlinksAndInsecureModesWithoutRepairingThem(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        $state = new SupervisorState($directory);
        $state->writeStatus($this->readyStatus());
        $owner = posix_geteuid();
        $reader = fn (): SupervisorState => new SupervisorState(
            $directory,
            new FakeProcessIdentity(0, [0 => 'root', $owner => 'supervisor']),
        );

        chmod($directory . '/status.json', 0644);
        $this->assertReadRefused(fn () => $reader()->status(), 'bounded regular file');
        clearstatcache(true, $directory . '/status.json');
        $this->assertSame(0644, fileperms($directory . '/status.json') & 0777);

        chmod($directory . '/status.json', 0600);
        rename($directory . '/status.json', $directory . '/target.json');
        symlink($directory . '/target.json', $directory . '/status.json');
        $this->assertReadRefused(fn () => $reader()->status(), 'bounded regular file');
        $this->assertTrue(is_link($directory . '/status.json'));
        unlink($directory . '/status.json');
        rename($directory . '/target.json', $directory . '/status.json');

        file_put_contents($directory . '/supervisor.lock', '{"pid":1}');
        chmod($directory . '/supervisor.lock', 0644);
        $this->assertReadRefused(fn () => $reader()->isOwned(), 'private owned regular file');
        clearstatcache(true, $directory . '/supervisor.lock');
        $this->assertSame(0644, fileperms($directory . '/supervisor.lock') & 0777);
        unlink($directory . '/supervisor.lock');

        chmod($directory, 0755);
        $this->assertReadRefused(fn () => $reader()->status(), 'must be a private real directory');
        clearstatcache(true, $directory);
        $this->assertSame(0755, fileperms($directory) & 0777);
        chmod($directory, 0700);

        // A symbolic link as the state directory is not followed to its
        // owner: root does not switch, and the path is refused.
        $alias = $directory . '-alias';
        $this->assertTrue(symlink($directory, $alias));
        $identity = new FakeProcessIdentity(0, [0 => 'root', $owner => 'supervisor']);
        try {
            new SupervisorState($alias, $identity);
            $this->fail('Root followed a symbolic-link state directory.');
        } catch (RuntimeException) {
            $this->assertSame([], $identity->assumed);
        } finally {
            unlink($alias);
        }
    }

    public function testRootReadsAnotherUsersStateAsThatUserWithTheRealIdentity(): void
    {
        if (posix_geteuid() !== 0) {
            $this->markTestSkipped('Needs root: it reads a state that belongs to another user.');
        }
        $account = posix_getpwnam('nobody');
        if (!is_array($account) || $account['uid'] === 0) {
            $this->markTestSkipped('Needs an unprivileged nobody account.');
        }
        $uid = $account['uid'];
        $egid = posix_getegid();
        $parent = $this->temporaryDirectory();
        $this->assertTrue(chmod($parent, 0755));
        $directory = $parent . '/state';
        // What a container entrypoint does: root creates the directory and
        // hands it to the supervisor's user, keeping its own group.
        $this->assertTrue(mkdir($directory, 0700));
        $this->assertTrue(chown($directory, $uid));

        $restore = (new ProcessIdentity())->assume($uid, $account['name'], $account['gid']);
        try {
            $this->assertSame($uid, posix_geteuid());
            $state = new SupervisorState($directory);
            $lock = $state->acquireLock();
            $state->writeStatus($this->readyStatus());
            $instanceId = $state->instanceId();
        } finally {
            $restore();
        }
        $this->assertSame(0, posix_geteuid());
        $this->assertSame($egid, posix_getegid());
        $before = $this->directorySnapshot($directory);

        try {
            $reader = new SupervisorState($directory);
            $status = $reader->status();
            $this->assertTrue($reader->isLive($status));
            $this->assertTrue($reader->readiness($status, true)['ready']);
            $this->assertTrue($reader->capacityHealth($status)['healthy']);
            $this->assertSame(0, posix_geteuid());
            $this->assertSame($egid, posix_getegid());
            $this->assertNotContains($account['gid'], posix_getgroups());

            try {
                $reader->request('pause', $instanceId);
                $this->fail('Root wrote a control request into another user\'s state.');
            } catch (RuntimeException $error) {
                $this->assertStringContainsString("Root may only read another user's supervisor state", $error->getMessage());
            }
            $this->assertSame($before, $this->directorySnapshot($directory));
            foreach ($before as $name => [, $owner]) {
                $this->assertSame($uid, $owner, "{$name} is not the supervisor user's.");
            }

            // A link to a file only root may read: the open runs as the
            // owner, so it fails instead of reaching the file.
            $secret = $parent . '/secret.json';
            file_put_contents($secret, '{"secret":true}');
            $this->assertTrue(chmod($secret, 0600));
            $this->assertTrue(rename($directory . '/status.json', $directory . '/status.saved'));
            $this->assertTrue(symlink($secret, $directory . '/status.json'));
            try {
                $reader->status();
                $this->fail('Root followed a symbolic link out of another user\'s state.');
            } catch (RuntimeException $error) {
                $this->assertStringContainsString('Unable to open Queen supervisor state', $error->getMessage());
            }
            $this->assertSame(0, posix_geteuid());
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    /**
     * The tests that play root or a stranger make the state belong to the
     * user running them; when that user is root, the state is root's own.
     * The test that needs root covers that run instead.
     */
    private function skipWhenRoot(): void
    {
        if (posix_geteuid() === 0) {
            $this->markTestSkipped('Plays another user against a state that the test user owns, so needs a test user that is not root.');
        }
    }

    private function assertReadRefused(\Closure $read, string $reason): void
    {
        try {
            $read();
            $this->fail("A read that should fail with [{$reason}] succeeded.");
        } catch (RuntimeException $error) {
            $this->assertStringContainsString($reason, $error->getMessage());
        }
    }

    /** @return array<string, mixed> */
    private function readyStatus(): array
    {
        return [
            'engine' => 'rust',
            'state' => 'running',
            'pools' => [],
            'pool_status' => [[
                'supervisor' => 'orders',
                'queue' => 'default',
                'desired' => 1,
                'running' => 1,
                'healthy' => true,
                'restart_state' => 'closed',
                'restart_failures' => 0,
                'depth' => 0,
                'depth_available' => true,
            ]],
        ];
    }

    /**
     * What a read must leave as it was: every entry's type and mode, owner,
     * inode, size, and modification and change times. A chmod to the same
     * mode still moves the change time.
     *
     * @return array<string, list<int>>
     */
    private function directorySnapshot(string $directory): array
    {
        clearstatcache();
        $paths = ['.' => $directory];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($directory, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::SELF_FIRST,
        );
        foreach ($iterator as $path => $_) {
            $paths[substr($path, strlen($directory) + 1)] = $path;
        }
        $snapshot = [];
        foreach ($paths as $name => $path) {
            $metadata = lstat($path);
            $snapshot[$name] = [
                $metadata['mode'],
                $metadata['uid'],
                $metadata['ino'],
                $metadata['size'],
                $metadata['mtime'],
                $metadata['ctime'],
            ];
        }
        ksort($snapshot);

        return $snapshot;
    }

    private function temporaryDirectory(): string
    {
        $directory = sys_get_temp_dir() . '/queen-supervisor-owner-' . bin2hex(random_bytes(8));
        if (!mkdir($directory, 0700, true) && !is_dir($directory)) {
            throw new RuntimeException("Unable to create test directory [{$directory}].");
        }
        $this->temporaryDirectories[] = $directory;

        return $directory;
    }

    private function removeDirectory(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($directory, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );
        foreach ($iterator as $item) {
            $item->isDir() && !$item->isLink() ? rmdir($item->getPathname()) : unlink($item->getPathname());
        }
        rmdir($directory);
    }
}
