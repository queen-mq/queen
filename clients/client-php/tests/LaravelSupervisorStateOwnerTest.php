<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Tests\Support\FakeProcessIdentity;
use RuntimeException;

/**
 * A supervisor state is the supervisor user's alone. A Kubernetes exec probe
 * runs as the container's user, often root, while the supervisor runs as
 * www-data: every reader that is not the owner, root included, is refused,
 * and the refusal says who owns the state, who is asking and whom to run as.
 */
class LaravelSupervisorStateOwnerTest extends TestCase
{
    /** The first sentence of each refusal, as it was before the detail. */
    private const REFUSAL = '/^Queen supervisor state (ancestor \[[^\]]+\] must be owned by root or the current user'
        . '|child \[[^\]]+\] below a sticky directory must be owned by the current user)\. It is owned/';

    /** @var list<string> */
    private array $temporaryDirectories = [];

    protected function tearDown(): void
    {
        foreach (array_reverse($this->temporaryDirectories) as $directory) {
            $this->removeDirectory($directory);
        }

        parent::tearDown();
    }

    public function testARootReaderOfAnotherUsersStateIsToldWhoOwnsItAndWhomToRunAs(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        (new SupervisorState($directory))->writeStatus($this->runningStatus());
        $owner = posix_geteuid();
        $before = $this->directorySnapshot($directory);

        $this->assertRefused(
            $directory,
            new FakeProcessIdentity(0, [0 => 'root', $owner => 'supervisor']),
            " It is owned by uid {$owner} (supervisor) and this process runs as uid 0 (root). Run the supervisor"
            . ' and its probes as that user, for example su -s /bin/sh supervisor -c'
            . " \"php artisan queen:supervisor status --check\", or securityContext.runAsUser: {$owner} on the"
            . ' container.',
        );
        // Without an account there is no name, and no name to give su.
        $this->assertRefused(
            $directory,
            new FakeProcessIdentity(0),
            " It is owned by uid {$owner} and this process runs as uid 0. Run the supervisor and its probes as"
            . " that user, for example securityContext.runAsUser: {$owner} on the container.",
        );
        $this->assertSame($before, $this->directorySnapshot($directory));
    }

    public function testAReaderThatIsNeitherRootNorTheOwnerIsToldWhoOwnsTheStateAndWhomToRunAs(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        (new SupervisorState($directory))->writeStatus($this->runningStatus());
        $owner = posix_geteuid();
        $stranger = $owner + 1000;
        $before = $this->directorySnapshot($directory);

        $this->assertRefused(
            $directory,
            new FakeProcessIdentity($stranger, [$owner => 'supervisor', $stranger => 'deploy']),
            " It is owned by uid {$owner} (supervisor) and this process runs as uid {$stranger} (deploy). Run the"
            . ' supervisor and its probes as that user, for example su -s /bin/sh supervisor -c'
            . " \"php artisan queen:supervisor status --check\", or securityContext.runAsUser: {$owner} on the"
            . ' container.',
        );
        $this->assertRefused(
            $directory,
            new FakeProcessIdentity($stranger),
            " It is owned by uid {$owner} and this process runs as uid {$stranger}. Run the supervisor and its"
            . " probes as that user, for example securityContext.runAsUser: {$owner} on the container.",
        );
        $this->assertSame($before, $this->directorySnapshot($directory));
    }

    public function testTheOwnerStillReadsAndControlsItsStateAndNobodyElseCan(): void
    {
        $this->skipWhenRoot();
        $directory = $this->temporaryDirectory();
        $owner = posix_geteuid();
        $state = new SupervisorState($directory, new FakeProcessIdentity($owner, [$owner => 'supervisor']));
        $lock = $state->acquireLock();

        try {
            $state->writeStatus($this->runningStatus());
            $status = $state->status();
            $this->assertTrue($state->isLive($status));
            $state->request('pause', $state->instanceId());
            $this->assertSame('pause', $state->command(null, $state->instanceId())['command'] ?? null);

            // Root and a stranger stop at the constructor: there is no read
            // path that trusts another owner, and nothing they could write.
            $before = $this->directorySnapshot($directory);
            foreach ([0, $owner + 1000] as $uid) {
                try {
                    new SupervisorState($directory, new FakeProcessIdentity($uid));
                    $this->fail("uid {$uid} opened another user's state.");
                } catch (RuntimeException $error) {
                    $this->assertMatchesRegularExpression(self::REFUSAL, $error->getMessage());
                }
            }
            $this->assertSame($before, $this->directorySnapshot($directory));
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testRootIsToldWhoOwnsTheStateWithTheRealIdentity(): void
    {
        if (posix_geteuid() !== 0) {
            $this->markTestSkipped('Needs root: it opens a state that belongs to another user.');
        }
        $account = posix_getpwnam('nobody');
        if (!is_array($account) || $account['uid'] === 0) {
            $this->markTestSkipped('Needs an unprivileged nobody account.');
        }
        $uid = $account['uid'];
        $parent = $this->temporaryDirectory();
        $this->assertTrue(chmod($parent, 0755));
        $directory = $parent . '/state';
        // What a container entrypoint does: root creates the directory and
        // hands it to the supervisor's user.
        $this->assertTrue(mkdir($directory, 0700));
        $this->assertTrue(chown($directory, $uid));
        $before = $this->directorySnapshot($directory);

        try {
            (new SupervisorState($directory))->status();
            $this->fail("Root opened another user's state.");
        } catch (RuntimeException $error) {
            $this->assertSame(
                'Queen supervisor state ancestor [' . realpath($directory) . '] must be owned by root or the current'
                . " user. It is owned by uid {$uid} ({$account['name']}) and this process runs as uid 0 (root)."
                . " Run the supervisor and its probes as that user, for example su -s /bin/sh {$account['name']} -c"
                . " \"php artisan queen:supervisor status --check\", or securityContext.runAsUser: {$uid} on the"
                . ' container.',
                $error->getMessage(),
            );
        }
        $this->assertSame($before, $this->directorySnapshot($directory));
    }

    private function assertRefused(string $directory, FakeProcessIdentity $identity, string $detail): void
    {
        try {
            new SupervisorState($directory, $identity);
            $this->fail('A reader that is not the owner was accepted.');
        } catch (RuntimeException $error) {
            // Which first sentence depends on whether the temporary directory
            // is below a sticky /tmp; either keeps its old text.
            $this->assertMatchesRegularExpression(self::REFUSAL, $error->getMessage());
            $this->assertStringEndsWith($detail, $error->getMessage());
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

    /** @return array<string, mixed> */
    private function runningStatus(): array
    {
        return ['engine' => 'rust', 'state' => 'running', 'pools' => [], 'pool_status' => []];
    }

    /**
     * Every entry's type and mode, owner, inode, size, and modification and
     * change times: what a refused reader must leave as it was.
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
