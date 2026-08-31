<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;

final class PackageMetadataTest extends TestCase
{
    public function testThePublishedPackageHasStandaloneMetadata(): void
    {
        $root = dirname(__DIR__);
        $manifest = json_decode(
            (string) file_get_contents($root . '/composer.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );

        self::assertSame('queen-mq/php-client', $manifest['name']);
        self::assertSame('Apache-2.0', $manifest['license']);
        self::assertSame('https://github.com/queen-mq/php-client', $manifest['support']['source']);
        self::assertFileExists($root . '/LICENSE.md');
        self::assertStringContainsString(
            'Apache License',
            (string) file_get_contents($root . '/LICENSE.md'),
        );
    }

    public function testTheReadmeUsesThePublishedPackageAndAbsoluteMonorepoLinks(): void
    {
        $readme = (string) file_get_contents(dirname(__DIR__) . '/README.md');

        self::assertStringContainsString('composer require queen-mq/php-client', $readme);
        self::assertStringNotContainsString('../../supervisor/README.md', $readme);
    }
}
