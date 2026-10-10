<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\Binary\SupervisorBinary;
use Queen\Laravel\Supervisor\Binary\SupervisorBinaryInstaller;
use Queen\Laravel\Supervisor\Binary\SupervisorReleaseManifest;
use Queen\Laravel\Supervisor\SupervisorState;
use RuntimeException;
use Symfony\Component\Process\Process;

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

    public function testThreadSafePhpExecutesThePinnedDirectoryByItsAbsoluteName(): void
    {
        $directory = realpath(sys_get_temp_dir()) . '/queen-exec-path-' . bin2hex(random_bytes(4));
        mkdir($directory, 0700);
        $previousDirectory = getcwd();

        try {
            chdir($directory);

            // A non-thread-safe chdir() moved the process: the pinned relative path resolves there.
            $this->assertNull(SupervisorBinary::pinnedDirectoryForExec(threadSafe: false));
            $this->assertSame(
                './queen-supervisor',
                SupervisorBinary::executablePath('./queen-supervisor', threadSafe: false),
            );
            // A ZTS chdir() moved only PHP's virtual directory, which the kernel never sees.
            $this->assertSame($directory, SupervisorBinary::pinnedDirectoryForExec(threadSafe: true));
            $this->assertSame(
                $directory . '/queen-supervisor',
                SupervisorBinary::executablePath('./queen-supervisor', threadSafe: true),
            );
        } finally {
            chdir($previousDirectory);
            rmdir($directory);
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
        $this->assertSame(str_repeat('c', 40), SupervisorReleaseManifest::fromJson($valid)->sourceCommit());

        foreach ([
            ['version' => '9.9.9'],
            ['artifacts.0.url' => 'http://mirror.example.test/supervisor.tar.gz'],
            ['artifacts.0.sha256' => 'missing'],
            ['artifacts.0.target' => 'x86_64-pc-windows-msvc'],
            ['release_tag' => 'supervisor/v9.9.9'],
            ['source_commit' => 'not-a-commit'],
            ['source_commit' => str_repeat('C', 40)],
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
            'filename' => 'queen-supervisor-0.8.0-windows-amd64.tar.gz',
            'url' => 'https://github.example.test/queen-supervisor-0.8.0-windows-amd64.tar.gz',
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
        $this->assertSame('0.8.0', $result['version']);
        $this->assertSame(str_repeat('c', 40), $result['source_commit']);
        $this->assertSame(hash_file('sha256', $manifest), $result['manifest_sha256']);
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

    public function testInstallerPinsItsBaseAndFailsIfTheConfiguredPathIsReplaced(): void
    {
        [$archive, $manifest] = $this->releaseFixture();
        $root = $this->temporaryDirectory();
        $installBase = $root . '/installed';
        $movedBase = $root . '/installed-moved';
        $downloads = 0;
        $installer = new SupervisorBinaryInstaller(
            static function (string $url, string $destination) use (
                &$downloads,
                $archive,
                $manifest,
                $installBase,
                $movedBase,
            ): void {
                $downloads++;
                if ($downloads === 1) {
                    if (!rename($installBase, $movedBase) || !mkdir($installBase, 0755)) {
                        throw new \RuntimeException('Cannot simulate installation-base replacement.');
                    }
                    copy($manifest, $destination);

                    return;
                }
                copy($archive, $destination);
            },
        );

        try {
            $installer->install(
                $installBase,
                'https://releases.example.test/queen-supervisor-manifest.json',
                platform: SupervisorBinary::platform('Linux', 'amd64'),
            );
            $this->fail('Replacing the configured installation path must fail the install operation.');
        } catch (RuntimeException $exception) {
            // A ZTS chdir() pins by name, so its inode check reports the swap first.
            $this->assertMatchesRegularExpression(
                '/(installation base path|pinned installation base) changed/',
                $exception->getMessage(),
            );
        }

        $this->assertFileDoesNotExist(SupervisorBinary::binaryPath(
            $installBase,
            SupervisorBinary::platform('Linux', 'amd64'),
        ));
        if (PHP_ZTS) {
            // A ZTS chdir() follows the name into the replacement, where the base
            // check refuses the install before the archive is even downloaded.
            $this->assertSame(1, $downloads);
            $this->assertFileDoesNotExist(SupervisorBinary::receiptPath(
                $installBase,
                SupervisorBinary::platform('Linux', 'amd64'),
            ));

            return;
        }
        $this->assertSame(2, $downloads);
        $this->assertFileExists(SupervisorBinary::binaryPath(
            $movedBase,
            SupervisorBinary::platform('Linux', 'amd64'),
        ));
    }

    public function testInstallBaseValidationRejectsFilesystemRootAliasesWithoutWriting(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('symlink')) {
            $this->markTestSkipped('The native supervisor installer currently requires Unix symbolic-link semantics.');
        }
        foreach (['/', '/./', '////./'] as $path) {
            try {
                SupervisorBinary::assertInstallBaseIsNotFilesystemRoot($path);
                $this->fail("The filesystem root alias [{$path}] was accepted as an install base.");
            } catch (RuntimeException $exception) {
                $this->assertStringContainsString('must not be a filesystem root', $exception->getMessage());
            }
        }

        try {
            SupervisorBinary::assertInstallBaseIsNotFilesystemRoot('/queen-supervisor-missing/..');
            $this->fail('An install base containing parent traversal was accepted.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('must not contain parent traversal', $exception->getMessage());
        }

        $rootAlias = $this->temporaryDirectory() . '/root-alias';
        $this->assertTrue(symlink(DIRECTORY_SEPARATOR, $rootAlias));
        try {
            SupervisorBinary::assertInstallBaseIsNotFilesystemRoot($rootAlias);
            $this->fail('A symlink resolving to the filesystem root was accepted as an install base.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('must not be a filesystem root', $exception->getMessage());
        }

        $previousDirectory = getcwd();
        $this->assertIsString($previousDirectory);
        try {
            $this->assertTrue(chdir(DIRECTORY_SEPARATOR));
            try {
                SupervisorBinary::assertInstallBaseIsNotFilesystemRoot('.');
                $this->fail('A relative current directory resolving to the filesystem root was accepted.');
            } catch (RuntimeException $exception) {
                $this->assertStringContainsString('must not be a filesystem root', $exception->getMessage());
            }
        } finally {
            $this->assertTrue(chdir($previousDirectory));
        }
    }

    public function testInstallerRejectsSymlinkedInstallBaseSpellingsBeforeWriting(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('symlink')) {
            $this->markTestSkipped('The native supervisor installer currently requires Unix symbolic-link semantics.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $root = $this->temporaryDirectory();
        $target = $root . '/target';
        $link = $root . '/install-link';
        $this->assertTrue(mkdir($target, 0755));
        $this->assertTrue(symlink($target, $link));

        foreach ([$link . '/', $link . '/.'] as $spelling) {
            try {
                (new SupervisorBinaryInstaller())->install(
                    $spelling,
                    $manifest,
                    archiveSource: $archive,
                    platform: SupervisorBinary::platform('Linux', 'amd64'),
                );
                $this->fail("The symlinked install base spelling [{$spelling}] was accepted.");
            } catch (RuntimeException $exception) {
                $this->assertStringContainsString('real, owned directory', $exception->getMessage());
            }
        }

        $this->assertSame(0, iterator_count(new \FilesystemIterator($target)));
    }

    public function testInstallBaseIdentityComparisonRejectsAReplacement(): void
    {
        $root = $this->temporaryDirectory();
        $base = $root . '/installed';
        $moved = $root . '/installed-moved';
        $installer = new SupervisorBinaryInstaller();
        $reflection = new \ReflectionClass($installer);
        $prepare = $reflection->getMethod('prepareInstallBase');
        $assertMatches = $reflection->getMethod('assertDirectoryStillMatches');
        $previousDirectory = getcwd();
        $this->assertIsString($previousDirectory);
        $prepared = $prepare->invoke($installer, $base, $previousDirectory);
        $this->assertIsArray($prepared);
        $this->assertSame(realpath($base), getcwd());
        $this->assertTrue(chdir($previousDirectory));

        $this->assertTrue(rename($base, $moved));
        $this->assertTrue(mkdir($base, 0755));

        try {
            $this->assertTrue(chdir($prepared['path']));
            try {
                $assertMatches->invoke(
                    $installer,
                    '.',
                    $prepared['metadata'],
                    'pinned installation base',
                );
                $this->fail('A replaced installation base was accepted by the pre-write pin.');
            } catch (RuntimeException $exception) {
                $this->assertStringContainsString('changed during installation', $exception->getMessage());
            }
        } finally {
            $this->assertTrue(chdir($previousDirectory));
        }

        foreach ([$base, $moved] as $directory) {
            $this->assertFileDoesNotExist($directory . '/.install.lock');
            $this->assertDirectoryDoesNotExist($directory . '/' . SupervisorBinary::VERSION);
        }
    }

    public function testRelativeOfflineSourcesRemainRelativeToTheCallersDirectory(): void
    {
        [$archive, $manifest] = $this->releaseFixture();
        $sourceDirectory = realpath(dirname($manifest));
        $this->assertIsString($sourceDirectory);
        $installBase = $this->temporaryDirectory() . '/installed';
        $previousDirectory = getcwd();
        $this->assertIsString($previousDirectory);

        try {
            $this->assertTrue(chdir($sourceDirectory));
            $result = (new SupervisorBinaryInstaller())->install(
                $installBase,
                basename($manifest),
                archiveSource: basename($archive),
                platform: SupervisorBinary::platform('Linux', 'amd64'),
            );
            $this->assertSame($sourceDirectory, getcwd());
        } finally {
            if (is_string($previousDirectory)) {
                $this->assertTrue(chdir($previousDirectory));
            }
        }

        $this->assertTrue($result['installed']);
        $this->assertSame(
            realpath(SupervisorBinary::binaryPath(
                $installBase,
                SupervisorBinary::platform('Linux', 'amd64'),
            )),
            realpath($result['binary']),
        );
    }

    public function testBinaryInstallLeavesASeparatePrivateRuntimeStateReadyForAcquisition(): void
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('The native supervisor currently requires Unix filesystem permissions.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $root = $this->temporaryDirectory();
        $installBase = $root . '/queen-supervisor-bin';
        $stateDirectory = $root . '/queen-supervisor';

        (new SupervisorBinaryInstaller())->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: SupervisorBinary::platform('Linux', 'amd64'),
        );

        $this->assertDirectoryExists($installBase);
        $this->assertDirectoryDoesNotExist($stateDirectory);
        $lock = (new SupervisorState($stateDirectory))->acquireLock();
        try {
            $this->assertSame(0700, fileperms($stateDirectory) & 0777);
            $this->assertFileExists($stateDirectory . '/supervisor.lock');
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    public function testComposerLauncherUsesTheDisjointDefaultInstallDirectory(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('pcntl_exec')) {
            $this->markTestSkipped('The Composer launcher currently requires a Unix pcntl runtime.');
        }
        $platform = SupervisorBinary::platform();
        $application = $this->temporaryDirectory();
        $storage = $application . '/storage';
        $installBase = $storage . '/queen-supervisor-bin';
        $installation = SupervisorBinary::installationDirectory($installBase, $platform);
        $this->assertTrue(mkdir($installation, 0755, true));

        $binary = SupervisorBinary::binaryPath($installBase, $platform);
        $this->assertTrue(copy(__DIR__ . '/Fixtures/Supervisor/queen-supervisor', $binary));
        $this->assertTrue(chmod($binary, 0755));
        $receipt = SupervisorBinary::receiptPath($installBase, $platform);
        $this->assertNotFalse(file_put_contents($receipt, json_encode([
            'version' => SupervisorBinary::VERSION,
            'target' => $platform['target'],
            'source_commit' => str_repeat('c', 40),
            'manifest_sha256' => str_repeat('a', 64),
            'binary_sha256' => hash_file('sha256', $binary),
        ], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR)));
        $this->assertTrue(chmod($receipt, 0600));

        $process = new Process(
            [PHP_BINARY, dirname(__DIR__) . '/bin/queen-supervisor', '--version'],
            $application,
            [
                'LARAVEL_STORAGE_PATH' => $storage,
                'QUEEN_SUPERVISOR_INSTALL_PATH' => false,
            ],
        );
        $process->run();

        $this->assertSame(0, $process->getExitCode(), $process->getErrorOutput());
        $this->assertSame("queen-supervisor 0.8.0\n", $process->getOutput());
        $this->assertDirectoryDoesNotExist($storage . '/queen-supervisor');

        $defaultContext = new Process(
            [PHP_BINARY, dirname(__DIR__) . '/bin/queen-supervisor'],
            $application,
            [
                'LARAVEL_STORAGE_PATH' => $storage,
                'QUEEN_SUPERVISOR_INSTALL_PATH' => false,
                'QUEEN_TEST_LAUNCH_CONTEXT' => '1',
            ],
        );
        $defaultContext->run();
        $this->assertSame(0, $defaultContext->getExitCode(), $defaultContext->getErrorOutput());
        $launchDirectory = realpath($application);
        $this->assertIsString($launchDirectory);
        $this->assertSame([
            // pcntl_exec() cannot move a ZTS process, so the binary starts where PHP was launched.
            PHP_ZTS ? $launchDirectory : realpath($installation),
            '--artisan',
            $launchDirectory . '/artisan',
        ], preg_split('/\R/', trim($defaultContext->getOutput())));

        $relativeContext = new Process(
            [
                PHP_BINARY,
                dirname(__DIR__) . '/bin/queen-supervisor',
                '--php',
                './runtime/php',
                '--artisan',
                'artisan',
            ],
            $application,
            [
                'LARAVEL_STORAGE_PATH' => $storage,
                'QUEEN_SUPERVISOR_INSTALL_PATH' => false,
                'QUEEN_TEST_LAUNCH_CONTEXT' => '1',
            ],
        );
        $relativeContext->run();
        $this->assertSame(0, $relativeContext->getExitCode(), $relativeContext->getErrorOutput());
        $this->assertSame([
            PHP_ZTS ? $launchDirectory : realpath($installation),
            '--php',
            $launchDirectory . '/./runtime/php',
            '--artisan',
            $launchDirectory . '/artisan',
        ], preg_split('/\R/', trim($relativeContext->getOutput())));

        $configContext = new Process(
            [
                PHP_BINARY,
                dirname(__DIR__) . '/bin/queen-supervisor',
                '--config',
                'config/supervisor.json',
            ],
            $application,
            [
                'LARAVEL_STORAGE_PATH' => $storage,
                'QUEEN_SUPERVISOR_INSTALL_PATH' => false,
                'QUEEN_TEST_LAUNCH_CONTEXT' => '1',
            ],
        );
        $configContext->run();
        $this->assertSame(0, $configContext->getExitCode(), $configContext->getErrorOutput());
        $this->assertSame([
            PHP_ZTS ? $launchDirectory : realpath($installation),
            '--config',
            $launchDirectory . '/config/supervisor.json',
        ], preg_split('/\R/', trim($configContext->getOutput())));

        $pathPhpContext = new Process(
            [
                PHP_BINARY,
                dirname(__DIR__) . '/bin/queen-supervisor',
                '--php',
                'php',
                '--artisan',
                '/srv/app/artisan',
            ],
            $application,
            [
                'LARAVEL_STORAGE_PATH' => $storage,
                'QUEEN_SUPERVISOR_INSTALL_PATH' => false,
                'QUEEN_TEST_LAUNCH_CONTEXT' => '1',
            ],
        );
        $pathPhpContext->run();
        $this->assertSame(0, $pathPhpContext->getExitCode(), $pathPhpContext->getErrorOutput());
        $this->assertSame([
            PHP_ZTS ? $launchDirectory : realpath($installation),
            '--php',
            'php',
            '--artisan',
            '/srv/app/artisan',
        ], preg_split('/\R/', trim($pathPhpContext->getOutput())));
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
            releaseBaseUrl: 'https://cdn.example.test/queen/v0.8.0/',
            platform: SupervisorBinary::platform('Linux', 'amd64'),
        );

        $this->assertSame([
            'https://mirror.example.test/releases/' . SupervisorBinary::MANIFEST_FILENAME,
            'https://cdn.example.test/queen/v0.8.0/queen-supervisor-0.8.0-linux-amd64.tar.gz',
        ], $requested);
    }

    public function testTrustedManifestDigestCanBePinnedBeforeArchiveExecution(): void
    {
        [$archive, $manifest] = $this->releaseFixture();
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        $expected = hash_file('sha256', $manifest);

        $result = (new SupervisorBinaryInstaller())->install(
            $this->temporaryDirectory() . '/valid',
            $manifest,
            archiveSource: $archive,
            platform: $platform,
            manifestSha256: $expected,
        );
        $this->assertTrue($result['installed']);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('failed its pinned SHA-256 check');
        (new SupervisorBinaryInstaller())->install(
            $this->temporaryDirectory() . '/invalid',
            $manifest,
            archiveSource: $archive,
            platform: $platform,
            manifestSha256: str_repeat('0', 64),
        );
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

    public function testLauncherRevalidatesOwnedDirectoriesAndFilesAtEveryStart(): void
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
        $versionDirectory = SupervisorBinary::versionDirectory($installBase);
        $target = SupervisorBinary::installationDirectory($installBase, $platform);
        $binary = SupervisorBinary::binaryPath($installBase, $platform);
        $receipt = SupervisorBinary::receiptPath($installBase, $platform);

        foreach ([
            [$installBase, 0777, 0755, 'installation base'],
            [$versionDirectory, 0777, 0755, 'version directory'],
            [$target, 0777, 0755, 'target directory'],
            [$binary, 0777, 0755, 'binary'],
            [$receipt, 0666, 0644, 'installation receipt'],
        ] as [$path, $unsafeMode, $safeMode, $expectedMessage]) {
            $this->assertTrue(chmod($path, $unsafeMode));
            try {
                SupervisorBinary::assertInstalled($installBase, $platform);
                $this->fail("Unsafe native supervisor path [{$path}] was accepted.");
            } catch (RuntimeException $exception) {
                $this->assertStringContainsString($expectedMessage, $exception->getMessage());
            } finally {
                $this->assertTrue(chmod($path, $safeMode));
            }
        }

        $this->assertSame($binary, SupervisorBinary::assertInstalled($installBase, $platform));
    }

    public function testALauncherRunningAsAnotherUserIsToldBothOwnersAndHowToFixIt(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_getpwnam')) {
            $this->markTestSkipped('Unix users are required.');
        }
        $installer = posix_geteuid();
        $runtime = posix_getpwnam('nobody');
        if (!is_array($runtime) || $runtime['uid'] === $installer) {
            $this->markTestSkipped('A second named user, nobody, is required.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $installBase = SupervisorBinary::normalizeInstallBasePath($this->temporaryDirectory() . '/installed');
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        (new SupervisorBinaryInstaller())->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: $platform,
        );

        // The image was built by one user and the launcher runs as another.
        $message = $this->refusalOf(fn () => $this->assertInstalledAs($installBase, $platform, $runtime['uid']));
        $this->assertSame(
            'The Queen supervisor installation base must be a real, owned directory without group/world write access. '
            . "{$installBase} is owned by {$this->userLabel($installer)}, "
            . "not by the effective user uid {$runtime['uid']} (nobody). "
            . "Install it as the user that runs the supervisor, run chown -R nobody {$installBase}, "
            . 'or install it as root with php artisan queen:supervisor-install --owner=nobody.',
            $message,
        );

        // A uid without a name, as some platforms assign, is named by its number alone.
        $anonymous = 1234567;
        while (posix_getpwuid($anonymous) !== false) {
            $anonymous++;
        }
        $message = $this->refusalOf(fn () => $this->assertInstalledAs($installBase, $platform, $anonymous));
        $this->assertStringContainsString("not by the effective user uid {$anonymous}. ", $message);
        $this->assertStringContainsString("run chown -R {$anonymous} {$installBase}, ", $message);
        $this->assertStringContainsString("--owner={$anonymous}.", $message);

        $this->assertSame(
            SupervisorBinary::binaryPath($installBase, $platform),
            $this->assertInstalledAs($installBase, $platform, $installer),
        );
    }

    public function testTheLauncherExitsSeventyAndNamesTheOwnerOfAnInstallBaseItDoesNotOwn(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('pcntl_exec')) {
            $this->markTestSkipped('The Composer launcher currently requires a Unix pcntl runtime.');
        }
        $runtime = posix_geteuid();
        $foreign = null;
        foreach (['/usr', '/bin', '/etc'] as $candidate) {
            $metadata = @lstat($candidate);
            if (is_array($metadata)
                && ($metadata['mode'] & 0170000) === 0040000
                && ($metadata['mode'] & 0022) === 0
                && $metadata['uid'] !== $runtime) {
                $foreign = [$candidate, $metadata['uid']];
                break;
            }
        }
        if ($foreign === null) {
            $this->markTestSkipped('No real directory owned by another user, as when the tests run as root.');
        }
        [$installBase, $owner] = $foreign;
        $user = $this->userArgument($runtime);

        // A real directory of another user stands in for an image built as root: the launcher only reads it.
        $process = new Process(
            [PHP_BINARY, dirname(__DIR__) . '/bin/queen-supervisor', '--version'],
            $this->temporaryDirectory(),
            ['QUEEN_SUPERVISOR_INSTALL_PATH' => $installBase],
        );
        $process->run();

        $this->assertSame(70, $process->getExitCode());
        $this->assertSame('', $process->getOutput());
        $this->assertSame(
            'queen-supervisor: The Queen supervisor installation base must be a real, owned directory without '
            . "group/world write access. {$installBase} is owned by {$this->userLabel($owner)}, "
            . "not by the effective user {$this->userLabel($runtime)}. "
            . "Install it as the user that runs the supervisor, run chown -R {$user} {$installBase}, "
            . "or install it as root with php artisan queen:supervisor-install --owner={$user}.\n",
            $process->getErrorOutput(),
        );
    }

    public function testEachUnsafeInstallationConditionIsNamedAfterTheUnchangedRefusal(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('symlink')) {
            $this->markTestSkipped('Unix permissions and symbolic links are required.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $root = SupervisorBinary::normalizeInstallBasePath($this->temporaryDirectory());
        $installBase = $root . '/installed';
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        (new SupervisorBinaryInstaller())->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: $platform,
        );
        $versionDirectory = SupervisorBinary::versionDirectory($installBase);
        $target = SupervisorBinary::installationDirectory($installBase, $platform);
        $binary = SupervisorBinary::binaryPath($installBase, $platform);
        $receipt = SupervisorBinary::receiptPath($installBase, $platform);
        $refusal = fn (): string => $this->refusalOf(
            static fn () => SupervisorBinary::assertInstalled($installBase, $platform),
        );
        $directory = 'The Queen supervisor %s must be a real, owned directory without group/world write access. ';
        $file = 'The Queen supervisor %s is unsafe, non-executable or oversized. Reinstall it. ';

        foreach ([[0775, 'group'], [0757, 'world'], [0777, 'group and world']] as [$mode, $who]) {
            $this->assertTrue(chmod($installBase, $mode));
            $this->assertSame(
                sprintf($directory, 'installation base') . sprintf(
                    '%s has mode %04o, which grants %s write access; remove it with chmod go-w %s.',
                    $installBase,
                    $mode,
                    $who,
                    $installBase,
                ),
                $refusal(),
            );
            $this->assertTrue(chmod($installBase, 0755));
        }

        $this->assertTrue(rename($target, $target . '.real'));
        $this->assertTrue(symlink($target . '.real', $target));
        $this->assertSame(
            sprintf($directory, 'target directory') . "{$target} is a symbolic link, which is never followed.",
            $refusal(),
        );
        $this->assertTrue(unlink($target));
        $this->assertTrue(rename($target . '.real', $target));

        $this->assertTrue(rename($versionDirectory, $versionDirectory . '.real'));
        $this->assertNotFalse(file_put_contents($versionDirectory, 'not a directory'));
        $this->assertSame(
            sprintf($directory, 'version directory') . "{$versionDirectory} is not a directory.",
            $refusal(),
        );
        $this->assertTrue(unlink($versionDirectory));
        $this->assertTrue(rename($versionDirectory . '.real', $versionDirectory));

        $this->assertTrue(chmod($binary, 0775));
        $this->assertSame(
            sprintf($file, 'binary')
            . "{$binary} has mode 0775, which grants group write access; remove it with chmod go-w {$binary}.",
            $refusal(),
        );
        $this->assertTrue(chmod($binary, 0644));
        $this->assertSame(sprintf($file, 'binary') . "{$binary} is not executable by its owner.", $refusal());
        $this->assertTrue(chmod($binary, 0755));

        $this->assertTrue(rename($receipt, $receipt . '.real'));
        $this->assertTrue(symlink($receipt . '.real', $receipt));
        $this->assertSame(
            sprintf($file, 'installation receipt') . "{$receipt} is a symbolic link, which is never followed.",
            $refusal(),
        );
        $this->assertTrue(unlink($receipt));
        $this->assertTrue(rename($receipt . '.real', $receipt));

        $this->assertSame($binary, SupervisorBinary::assertInstalled($installBase, $platform));

        $missing = $root . '/never-installed';
        $this->assertSame(
            sprintf($directory, 'installation base') . "{$missing} does not exist or cannot be inspected.",
            $this->refusalOf(static fn () => SupervisorBinary::assertInstalled($missing, $platform)),
        );
    }

    public function testTheInstallerNamesTheOwnerOfAnInstallBaseItDoesNotOwnWithoutWritingToIt(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_geteuid') || posix_geteuid() === 0) {
            $this->markTestSkipped('Needs a user other than root, and a root-owned directory.');
        }
        $metadata = @lstat('/usr');
        if (!is_array($metadata) || ($metadata['mode'] & 0170000) !== 0040000 || $metadata['uid'] !== 0) {
            $this->markTestSkipped('/usr is not a real root-owned directory here.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $previousDirectory = getcwd();
        $user = $this->userArgument(posix_geteuid());

        $message = $this->refusalOf(static fn () => (new SupervisorBinaryInstaller())->install(
            '/usr',
            $manifest,
            archiveSource: $archive,
            platform: SupervisorBinary::platform('Linux', 'amd64'),
        ));

        $this->assertSame(
            'Queen supervisor installation base must be a real, owned directory without group/world write access. '
            . "/usr is owned by {$this->userLabel(0)}, "
            . "not by the effective user {$this->userLabel(posix_geteuid())}. "
            . "Run the installer as the user that owns it, or chown -R {$user} /usr first; "
            . 'as root, --owner=<user> hands the installation to the user that runs the supervisor.',
            $message,
        );
        $this->assertSame($previousDirectory, getcwd());
    }

    public function testAnOwnerThatDoesNotResolveIsRefusedBeforeAnythingIsWritten(): void
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Unix users are required.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $installBase = $this->temporaryDirectory() . '/installed';
        $unknown = 'queen-no-such-user-' . bin2hex(random_bytes(3));

        foreach ([
            $unknown => "The --owner user {$unknown} does not exist on this system. Name a user that exists "
                . 'where the installer runs, or give a numeric uid.',
            '4294967295' => 'The --owner uid 4294967295 is out of range.',
            'www data' => 'The --owner value must be a user name or a numeric uid.',
            '' => 'The --owner value must be a user name or a numeric uid.',
        ] as $owner => $expected) {
            $this->assertSame($expected, $this->refusalOf(static fn () => (new SupervisorBinaryInstaller())->install(
                $installBase,
                $manifest,
                archiveSource: $archive,
                platform: SupervisorBinary::platform('Linux', 'amd64'),
                owner: (string) $owner,
            )));
        }
        $this->assertDirectoryDoesNotExist($installBase);
    }

    public function testOwnerIsRefusedWithoutRootBeforeAnythingIsWritten(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_geteuid') || posix_geteuid() === 0) {
            $this->markTestSkipped('Needs a user other than root.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $installBase = $this->temporaryDirectory() . '/installed';

        foreach ([$this->userArgument(posix_geteuid()), (string) posix_geteuid()] as $owner) {
            $this->assertSame(
                'The --owner option of queen:supervisor-install requires root, but the effective user is '
                . "{$this->userLabel(posix_geteuid())}. Without root, run the installer as the user that runs "
                . 'the supervisor, without --owner.',
                $this->refusalOf(static fn () => (new SupervisorBinaryInstaller())->install(
                    $installBase,
                    $manifest,
                    archiveSource: $archive,
                    platform: SupervisorBinary::platform('Linux', 'amd64'),
                    owner: $owner,
                )),
            );
        }
        $this->assertDirectoryDoesNotExist($installBase);
    }

    public function testHandOverChangesEveryEntryAndKeepsItsModeButNeverFollowsALink(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('symlink') || !function_exists('lchown')) {
            $this->markTestSkipped('Unix ownership and symbolic links are required.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $root = $this->temporaryDirectory();
        $installBase = $root . '/installed';
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        (new SupervisorBinaryInstaller())->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: $platform,
        );
        $entries = [
            $installBase,
            $installBase . '/.install.lock',
            SupervisorBinary::versionDirectory($installBase),
            SupervisorBinary::installationDirectory($installBase, $platform),
            SupervisorBinary::binaryPath($installBase, $platform),
            SupervisorBinary::receiptPath($installBase, $platform),
        ];
        $modes = array_map(static fn (string $path): int => fileperms($path) & 07777, $entries);
        // Without root the only owner a process may hand its files to is itself, which still
        // walks every entry through the same checks and lchown() calls as root does.
        $owner = ['uid' => posix_geteuid(), 'gid' => posix_getegid()];

        $this->handOver($installBase, $platform, $owner);

        foreach ($entries as $index => $path) {
            clearstatcache(true, $path);
            $metadata = lstat($path);
            $this->assertSame($owner['uid'], $metadata['uid'], $path);
            $this->assertSame($owner['gid'], $metadata['gid'], $path);
            $this->assertSame($modes[$index], $metadata['mode'] & 07777, $path);
        }

        $receipt = SupervisorBinary::receiptPath($installBase, $platform);
        $elsewhere = $root . '/elsewhere.json';
        $this->assertTrue(rename($receipt, $elsewhere));
        $this->assertTrue(symlink($elsewhere, $receipt));
        $message = $this->refusalOf(fn () => $this->handOver($installBase, $platform, $owner));
        $this->assertStringStartsWith('Cannot hand the Queen supervisor installation to ', $message);
        $this->assertStringEndsWith('receipt.json is a symbolic link, which is never followed.', $message);
        $this->assertTrue(is_link($receipt));
    }

    public function testRootHandsTheWholeInstallationToTheOwnerWithTheModesOfANormalInstall(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_geteuid') || posix_geteuid() !== 0) {
            $this->markTestSkipped('--owner needs root.');
        }
        if (PHP_ZTS) {
            $this->markTestSkipped('--owner is refused on a thread-safe PHP.');
        }
        $owner = posix_getpwnam('nobody');
        if (!is_array($owner) || $owner['uid'] === 0) {
            $this->markTestSkipped('The user nobody is required.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $installBase = $this->temporaryDirectory() . '/installed';
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        $directoryMode = 0755 & ~umask();

        $result = (new SupervisorBinaryInstaller())->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: $platform,
            owner: 'nobody',
        );

        $this->assertTrue($result['installed']);
        $this->assertSame($owner['uid'], $result['owner_uid']);
        foreach ([
            [$installBase, $directoryMode],
            [$installBase . '/.install.lock', 0600],
            [SupervisorBinary::versionDirectory($installBase), $directoryMode],
            [SupervisorBinary::installationDirectory($installBase, $platform), $directoryMode],
            [SupervisorBinary::binaryPath($installBase, $platform), 0755],
            [SupervisorBinary::receiptPath($installBase, $platform), 0644],
        ] as [$path, $mode]) {
            clearstatcache(true, $path);
            $metadata = lstat($path);
            $this->assertSame($owner['uid'], $metadata['uid'], $path);
            $this->assertSame($owner['gid'], $metadata['gid'], $path);
            $this->assertSame($mode, $metadata['mode'] & 07777, $path);
        }
        // What the launcher running as that user will check.
        $this->assertSame(
            SupervisorBinary::binaryPath($installBase, $platform),
            $this->assertInstalledAs($installBase, $platform, $owner['uid']),
        );

        // Root does not write into a tree another user can change; that user updates it.
        $this->assertStringContainsString(
            'Run the installer as the user that owns it',
            $this->refusalOf(static fn () => (new SupervisorBinaryInstaller())->install(
                $installBase,
                $manifest,
                archiveSource: $archive,
                platform: $platform,
                owner: 'nobody',
            )),
        );
    }

    public function testInstallerRejectsAnUnsafeOrSymlinkedVersionDirectory(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('symlink')) {
            $this->markTestSkipped('Unix ownership and symbolic links are required.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        $root = $this->temporaryDirectory();
        $installBase = $root . '/installed';
        $versionDirectory = SupervisorBinary::versionDirectory($installBase);
        $this->assertTrue(mkdir($versionDirectory, 0755, true));
        $this->assertTrue(chmod($versionDirectory, 0777));

        try {
            (new SupervisorBinaryInstaller())->install(
                $installBase,
                $manifest,
                archiveSource: $archive,
                platform: $platform,
            );
            $this->fail('A group-writable version directory must be rejected.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('without group/world write access', $exception->getMessage());
        }

        $this->assertTrue(chmod($versionDirectory, 0755));
        $this->assertTrue(rmdir($versionDirectory));
        $symlinkTarget = $root . '/shared-version';
        $this->assertTrue(mkdir($symlinkTarget, 0755));
        $this->assertTrue(symlink($symlinkTarget, $versionDirectory));

        try {
            (new SupervisorBinaryInstaller())->install(
                $installBase,
                $manifest,
                archiveSource: $archive,
                platform: $platform,
            );
            $this->fail('A symlinked version directory must be rejected.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('real, owned directory', $exception->getMessage());
        }
    }

    public function testInstallerRejectsTargetReplacementDuringTheVersionSmokeTest(): void
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('The native supervisor installer currently requires Unix.');
        }
        $replacingBinary = <<<'SH'
#!/bin/sh
if [ "${1:-}" = "--version" ]; then
    original="$(pwd)"
    moved="${original}.moved"
    filename="$(basename "$0")"
    mv "$original" "$moved" || exit 91
    mkdir -m 0755 "$original" || exit 92
    printf '%s\n' '#!/bin/sh' "printf '%s\\n' 'substituted binary'" > "$original/$filename" || exit 93
    chmod 0755 "$original/$filename" || exit 94
    printf '%s\n' 'queen-supervisor 0.8.0'
    exit 0
fi
exit 95
SH;
        [$archive, $manifest] = $this->releaseFixture(binaryContents: $replacingBinary);
        $installBase = $this->temporaryDirectory() . '/installed';
        $platform = SupervisorBinary::platform('Linux', 'amd64');

        try {
            (new SupervisorBinaryInstaller())->install(
                $installBase,
                $manifest,
                archiveSource: $archive,
                platform: $platform,
            );
            $this->fail('A target replacement during the executable smoke test must fail installation.');
        } catch (RuntimeException $exception) {
            // A ZTS chdir() pins by name, so the smoke test itself sees the substituted binary first.
            $this->assertMatchesRegularExpression(
                '/target directory changed during installation|changed during its version smoke test/',
                $exception->getMessage(),
            );
        }

        $this->assertFileDoesNotExist(SupervisorBinary::binaryPath($installBase, $platform));
        $this->assertFileDoesNotExist(SupervisorBinary::receiptPath($installBase, $platform));
    }

    public function testLauncherPinsTheVerifiedTargetBeforeAnAncestorCanReplaceIt(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('pcntl_exec')) {
            $this->markTestSkipped('The Composer launcher currently requires a Unix pcntl runtime.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $installBase = $this->temporaryDirectory() . '/installed';
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        (new SupervisorBinaryInstaller())->install(
            $installBase,
            $manifest,
            archiveSource: $archive,
            platform: $platform,
        );
        $target = SupervisorBinary::installationDirectory($installBase, $platform);
        $originalBinary = SupervisorBinary::binaryPath($installBase, $platform);
        $originalHash = hash_file('sha256', $originalBinary);
        $previousDirectory = getcwd();
        $this->assertIsString($previousDirectory);

        $symlinkTarget = $target . '.real';
        $this->assertTrue(rename($target, $symlinkTarget));
        $this->assertTrue(symlink($symlinkTarget, $target));
        try {
            SupervisorBinary::pinInstalledForExecution($installBase, $platform);
            $this->fail('The launcher must reject a symlink target before changing directory.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('target directory', $exception->getMessage());
        } finally {
            $this->assertTrue(unlink($target));
            $this->assertTrue(rename($symlinkTarget, $target));
        }
        $this->assertSame($previousDirectory, getcwd());

        $absoluteTarget = realpath($target);
        $this->assertIsString($absoluteTarget);
        try {
            $pinnedBinary = SupervisorBinary::pinInstalledForExecution($installBase, $platform);
            $movedTarget = $target . '.moved';
            $this->assertTrue(rename($target, $movedTarget));
            $this->assertTrue(mkdir($target, 0755));
            $replacement = $target . '/queen-supervisor';
            $this->assertNotFalse(file_put_contents($replacement, "#!/bin/sh\necho attacker\n"));
            $this->assertTrue(chmod($replacement, 0755));

            $this->assertNotSame($originalHash, hash_file('sha256', $replacement));
            if (PHP_ZTS) {
                // The kernel never saw the ZTS chdir(): exec re-resolves the verified directory by
                // name, so this platform keeps every check but not the inode pin.
                $this->assertSame($absoluteTarget . DIRECTORY_SEPARATOR . 'queen-supervisor', $pinnedBinary);

                return;
            }
            $this->assertSame('.' . DIRECTORY_SEPARATOR . 'queen-supervisor', $pinnedBinary);
            $this->assertSame($originalHash, hash_file('sha256', $pinnedBinary));
        } finally {
            if (is_string($previousDirectory)) {
                $this->assertTrue(chdir($previousDirectory));
            }
        }
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

    public function testInstallDirectoryAndLockRejectSharedWriteAndSymlinks(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('symlink')) {
            $this->markTestSkipped('Unix ownership and symbolic links are required.');
        }
        [$archive, $manifest] = $this->releaseFixture();
        $platform = SupervisorBinary::platform('Linux', 'amd64');
        $shared = $this->temporaryDirectory() . '/shared';
        mkdir($shared, 0770);
        chmod($shared, 0770);
        try {
            (new SupervisorBinaryInstaller())->install(
                $shared,
                $manifest,
                archiveSource: $archive,
                platform: $platform,
            );
            $this->fail('A group-writable install directory must be rejected.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('without group/world write access', $exception->getMessage());
        }

        $locked = $this->temporaryDirectory() . '/locked';
        mkdir($locked, 0755);
        $target = $locked . '/target';
        file_put_contents($target, 'unsafe');
        $this->assertTrue(symlink($target, $locked . '/.install.lock'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('installation lock is unsafe');
        (new SupervisorBinaryInstaller())->install(
            $locked,
            $manifest,
            archiveSource: $archive,
            platform: $platform,
        );
    }

    private function refusalOf(callable $operation): string
    {
        try {
            $operation();
        } catch (RuntimeException $exception) {
            return $exception->getMessage();
        }
        $this->fail('The operation was expected to be refused.');
    }

    /** The launcher's checks, as a process whose effective user is $effectiveUserId would run them. */
    private function assertInstalledAs(string $installBase, array $platform, int $effectiveUserId): string
    {
        return (new \ReflectionMethod(SupervisorBinary::class, 'assertInstalledAs'))
            ->invoke(null, $installBase, $platform, $effectiveUserId);
    }

    /** The installer's hand-over, from the pinned installation base as install() runs it. */
    private function handOver(string $installBase, array $platform, array $owner): void
    {
        $previousDirectory = getcwd();
        $this->assertTrue(chdir($installBase));
        try {
            $installer = new SupervisorBinaryInstaller();
            (new \ReflectionMethod($installer, 'handOver'))->invoke($installer, lstat('.'), $platform, $owner);
        } finally {
            $this->assertTrue(chdir($previousDirectory));
        }
    }

    private function userLabel(int $uid): string
    {
        $entry = posix_getpwuid($uid);

        return is_array($entry) ? "uid {$uid} ({$entry['name']})" : "uid {$uid}";
    }

    private function userArgument(int $uid): string
    {
        $entry = posix_getpwuid($uid);

        return is_array($entry) ? $entry['name'] : (string) $uid;
    }

    private function releaseFixture(
        ?string $manifestHash = null,
        array $extraEntries = [],
        ?string $binaryContents = null,
    ): array
    {
        $directory = $this->temporaryDirectory();
        $tar = $directory . '/queen-supervisor-0.8.0-linux-amd64.tar';
        $archive = $tar . '.gz';
        $fixtureBinary = __DIR__ . '/Fixtures/Supervisor/queen-supervisor';
        $phar = new \PharData($tar);
        if ($binaryContents === null) {
            $phar->addFile($fixtureBinary, 'queen-supervisor');
        } else {
            $phar->addFromString('queen-supervisor', $binaryContents);
        }
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
            'source_commit' => str_repeat('c', 40),
            'artifacts' => [[
                'target' => 'x86_64-unknown-linux-musl',
                'os' => 'linux',
                'arch' => 'amd64',
                'filename' => 'queen-supervisor-0.8.0-linux-amd64.tar.gz',
                'url' => 'https://github.example.test/queen-supervisor-0.8.0-linux-amd64.tar.gz',
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
