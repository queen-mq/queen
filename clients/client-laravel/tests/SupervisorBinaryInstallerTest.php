<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\Binary\SupervisorBinary;
use Queen\Laravel\Supervisor\Binary\SupervisorBinaryInstaller;
use Queen\Laravel\Supervisor\Binary\SupervisorReleaseManifest;
use RuntimeException;

class SupervisorBinaryInstallerTest extends TestCase
{
    /** @var list<string> */
    private array $temporaryDirectories = [];

    protected function tearDown(): void
    {
        foreach (array_reverse($this->temporaryDirectories) as $directory) {
            $this->removeDirectory($directory);
        }
    }

    public function testPlatformSelectionIsExplicitAndNeverFallsBackAcrossOperatingSystems(): void
    {
        $this->assertSame([
            'os' => 'linux',
            'arch' => 'amd64',
            'target' => 'x86_64-unknown-linux-musl',
        ], SupervisorBinary::platform('Linux', 'x86_64'));
        $this->assertSame([
            'os' => 'linux',
            'arch' => 'arm64',
            'target' => 'aarch64-unknown-linux-musl',
        ], SupervisorBinary::platform('linux', 'aarch64'));
        $this->assertSame([
            'os' => 'darwin',
            'arch' => 'amd64',
            'target' => 'x86_64-apple-darwin',
        ], SupervisorBinary::platform('Darwin', 'amd64'));
        $this->assertSame([
            'os' => 'darwin',
            'arch' => 'arm64',
            'target' => 'aarch64-apple-darwin',
        ], SupervisorBinary::platform('Darwin', 'arm64'));

        try {
            SupervisorBinary::platform('Windows NT', 'AMD64');
            $this->fail('Windows must not fall back to a Linux artifact.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('not available on Windows', $exception->getMessage());
            $this->assertStringContainsString('Unix signals and process groups', $exception->getMessage());
        }
    }

    public function testManifestRequiresPinnedVersionHttpsAndSha256(): void
    {
        $valid = $this->manifest([
            'sha256' => str_repeat('a', 64),
        ]);
        $artifact = SupervisorReleaseManifest::fromJson($valid)->artifactFor(
            SupervisorBinary::platform('Linux', 'x86_64'),
        );
        $this->assertSame(str_repeat('a', 64), $artifact['sha256']);

        foreach ([
            ['version' => '9.9.9'],
            ['artifacts.0.url' => 'http://mirror.example.test/supervisor.tar.gz'],
            ['artifacts.0.sha256' => 'missing'],
            ['artifacts.0.target' => 'x86_64-pc-windows-msvc'],
        ] as $mutation) {
            try {
                SupervisorReleaseManifest::fromJson($this->manifest($mutation));
                $this->fail('An unsafe release manifest must be rejected.');
            } catch (RuntimeException) {
                $this->addToAssertionCount(1);
            }
        }

        $extensible = json_decode($valid, true, flags: JSON_THROW_ON_ERROR);
        $extensible['artifacts'][] = [
            'target' => 'x86_64-pc-windows-msvc',
            'os' => 'windows',
            'arch' => 'amd64',
            'filename' => 'queen-supervisor-0.1.0-windows-amd64.tar.gz',
            'url' => 'https://github.example.test/queen-supervisor-0.1.0-windows-amd64.tar.gz',
            'sha256' => str_repeat('b', 64),
        ];
        $parsed = SupervisorReleaseManifest::fromJson(json_encode($extensible, JSON_THROW_ON_ERROR));
        $this->assertSame('linux', $parsed->artifactFor(
            SupervisorBinary::platform('Linux', 'amd64'),
        )['os']);
    }

    public function testOfflineInstallVerifiesArchiveSmokeTestsBinaryAndWritesReceipt(): void
    {
        [$archive, $manifest] = $this->releaseFixture();
        $installBase = $this->temporaryDirectory() . '/installed';
        $installer = new SupervisorBinaryInstaller(
            static fn () => throw new \LogicException('Offline installation must not use the network.'),
        );

        $result = $installer->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: SupervisorBinary::platform('Linux', 'amd64'),
        );

        $this->assertTrue($result['installed']);
        $this->assertSame('0.1.0', $result['version']);
        $this->assertTrue(is_executable($result['binary']));
        $this->assertSame(realpath($result['binary']), realpath(SupervisorBinary::assertInstalled(
            $installBase,
            SupervisorBinary::platform('Linux', 'amd64'),
        )));

        $second = $installer->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: SupervisorBinary::platform('Linux', 'amd64'),
        );
        $this->assertFalse($second['installed']);
        $this->assertSame($result['binary_sha256'], $second['binary_sha256']);
    }

    public function testArchiveHashMismatchFailsClosedWithoutPublishingBinary(): void
    {
        [$archive, $manifest] = $this->releaseFixture(str_repeat('0', 64));
        $installBase = $this->temporaryDirectory() . '/installed';
        $platform = SupervisorBinary::platform('Linux', 'amd64');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('failed its SHA-256 integrity check');
        try {
            (new SupervisorBinaryInstaller())->install(
                $installBase,
                $manifest,
                archiveSource: $archive,
                platform: $platform,
            );
        } finally {
            $this->assertFileDoesNotExist(SupervisorBinary::binaryPath($installBase, $platform));
            $this->assertFileDoesNotExist(SupervisorBinary::receiptPath($installBase, $platform));
        }
    }

    public function testArchiveWithUnexpectedEntriesIsRejectedBeforeExecution(): void
    {
        [$archive] = $this->releaseFixture(extraEntries: ['unexpected' => 'not part of the release contract']);
        $manifest = $this->temporaryDirectory() . '/manifest.json';
        file_put_contents($manifest, $this->manifest(['sha256' => hash_file('sha256', $archive)]));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('unexpected entry set');
        (new SupervisorBinaryInstaller())->install(
            $this->temporaryDirectory() . '/installed',
            $manifest,
            archiveSource: $archive,
            platform: SupervisorBinary::platform('Linux', 'amd64'),
        );
    }

    public function testHttpsMirrorReplacesOnlyTheArtifactBaseAndKeepsManifestDigest(): void
    {
        [$archive, $localManifest] = $this->releaseFixture();
        $manifestJson = (string) file_get_contents($localManifest);
        $requested = [];
        $installer = new SupervisorBinaryInstaller(
            static function (string $url, string $destination) use (&$requested, $archive, $manifestJson): void {
                $requested[] = $url;
                if (str_ends_with($url, SupervisorBinary::MANIFEST_FILENAME)) {
                    file_put_contents($destination, $manifestJson);
                } else {
                    copy($archive, $destination);
                }
            },
        );

        $installer->install(
            $this->temporaryDirectory() . '/installed',
            'https://mirror.example.test/releases/' . SupervisorBinary::MANIFEST_FILENAME,
            releaseBaseUrl: 'https://cdn.example.test/queen/v0.1.0/',
            platform: SupervisorBinary::platform('Linux', 'amd64'),
        );

        $this->assertSame([
            'https://mirror.example.test/releases/' . SupervisorBinary::MANIFEST_FILENAME,
            'https://cdn.example.test/queen/v0.1.0/queen-supervisor-0.1.0-linux-amd64.tar.gz',
        ], $requested);
    }

    public function testTamperedInstalledBinaryIsRejectedByLauncherIntegrityCheck(): void
    {
        [$archive, $manifest] = $this->releaseFixture();
        $installBase = $this->temporaryDirectory() . '/installed';
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        (new SupervisorBinaryInstaller())->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: $platform,
        );
        file_put_contents(SupervisorBinary::binaryPath($installBase, $platform), "\n# tampered\n", FILE_APPEND);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('failed its SHA-256 integrity check');
        SupervisorBinary::assertInstalled($installBase, $platform);
    }

    public function testLocalManifestAndArchiveSymlinksAreRejected(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('symlink')) {
            $this->markTestSkipped('Symbolic links are not available.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $directory = $this->temporaryDirectory();
        $manifestLink = $directory . '/manifest-link.json';
        $archiveLink = $directory . '/archive-link.tar.gz';
        $this->assertTrue(symlink($manifest, $manifestLink));
        $this->assertTrue(symlink($archive, $archiveLink));
        $installer = new SupervisorBinaryInstaller();

        foreach ([[$manifestLink, $archive], [$manifest, $archiveLink]] as [$manifestSource, $archiveSource]) {
            try {
                $installer->install(
                    $directory . '/installed-' . bin2hex(random_bytes(2)),
                    $manifestSource,
                    archiveSource: $archiveSource,
                    platform: SupervisorBinary::platform('Linux', 'amd64'),
                );
                $this->fail('Local symlink input must be rejected.');
            } catch (RuntimeException $exception) {
                $this->assertStringContainsString('non-symlink file', $exception->getMessage());
            }
        }
    }

    private function releaseFixture(?string $manifestHash = null, array $extraEntries = []): array
    {
        $directory = $this->temporaryDirectory();
        $tar = $directory . '/queen-supervisor-0.1.0-linux-amd64.tar';
        $archive = $tar . '.gz';
        $fixtureBinary = __DIR__ . '/Fixtures/Supervisor/queen-supervisor';
        $phar = new \PharData($tar);
        $phar->addFile($fixtureBinary, 'queen-supervisor');
        $phar->addFromString('LICENSE.md', 'test license');
        $phar->addFromString('queen-supervisor.service.example', "[Service]\n");
        foreach ($extraEntries as $name => $contents) {
            $phar->addFromString($name, $contents);
        }
        $phar->compress(\Phar::GZ);
        unset($phar);
        @unlink($tar);

        $manifest = $directory . '/manifest.json';
        file_put_contents($manifest, $this->manifest([
            'sha256' => $manifestHash ?? hash_file('sha256', $archive),
        ]));

        return [$archive, $manifest];
    }

    private function manifest(array $changes = []): string
    {
        $manifest = [
            'schema_version' => 1,
            'name' => 'queen-supervisor',
            'version' => SupervisorBinary::VERSION,
            'release_tag' => 'supervisor/v' . SupervisorBinary::VERSION,
            'artifacts' => [[
                'target' => 'x86_64-unknown-linux-musl',
                'os' => 'linux',
                'arch' => 'amd64',
                'filename' => 'queen-supervisor-0.1.0-linux-amd64.tar.gz',
                'url' => 'https://github.example.test/queen-supervisor-0.1.0-linux-amd64.tar.gz',
                'sha256' => str_repeat('a', 64),
            ]],
        ];
        foreach ($changes as $path => $value) {
            if ($path === 'sha256') {
                $manifest['artifacts'][0]['sha256'] = $value;
                continue;
            }
            $segments = explode('.', $path);
            $cursor =& $manifest;
            foreach ($segments as $segment) {
                $segment = ctype_digit($segment) ? (int) $segment : $segment;
                $cursor =& $cursor[$segment];
            }
            $cursor = $value;
            unset($cursor);
        }

        return json_encode($manifest, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
    }

    private function temporaryDirectory(): string
    {
        $directory = sys_get_temp_dir() . '/queen-supervisor-test-' . bin2hex(random_bytes(8));
        if (!mkdir($directory, 0700, true)) {
            throw new \RuntimeException("Cannot create {$directory}.");
        }
        $this->temporaryDirectories[] = $directory;

        return $directory;
    }

    private function removeDirectory(string $directory): void
    {
        if (!is_dir($directory) || is_link($directory)) {
            @unlink($directory);

            return;
        }
        foreach (new \FilesystemIterator($directory) as $entry) {
            $entry->isDir() && !$entry->isLink()
                ? $this->removeDirectory($entry->getPathname())
                : @unlink($entry->getPathname());
        }
        @rmdir($directory);
    }
}
