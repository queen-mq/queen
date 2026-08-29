<?php

namespace Queen\Laravel\Supervisor\Binary;

use RuntimeException;

final class SupervisorReleaseManifest
{
    public const MAX_BYTES = 65536;

    private function __construct(
        private readonly array $artifacts,
    ) {
    }

    public static function fromJson(string $json): self
    {
        if (strlen($json) > self::MAX_BYTES) {
            throw new RuntimeException('The Queen supervisor release manifest exceeds 64 KiB.');
        }

        try {
            $manifest = json_decode($json, true, 32, JSON_THROW_ON_ERROR);
        } catch (\JsonException $exception) {
            throw new RuntimeException('The Queen supervisor release manifest is not valid JSON.', previous: $exception);
        }

        if (!is_array($manifest) || ($manifest['schema_version'] ?? null) !== 1) {
            throw new RuntimeException('Unsupported Queen supervisor release manifest schema.');
        }
        if (($manifest['name'] ?? null) !== 'queen-supervisor') {
            throw new RuntimeException('The release manifest does not describe queen-supervisor.');
        }
        if (($manifest['version'] ?? null) !== SupervisorBinary::VERSION) {
            throw new RuntimeException(
                'The release manifest version does not match the Composer package pin '
                . SupervisorBinary::VERSION . '.',
            );
        }
        if (!is_array($manifest['artifacts'] ?? null) || $manifest['artifacts'] === []) {
            throw new RuntimeException('The Queen supervisor release manifest has no artifacts.');
        }

        $artifacts = [];
        foreach ($manifest['artifacts'] as $index => $artifact) {
            if (!is_array($artifact)) {
                throw new RuntimeException("Release manifest artifact {$index} must be an object.");
            }

            $os = self::plainToken($artifact['os'] ?? null, "artifacts[{$index}].os");
            $arch = self::plainToken($artifact['arch'] ?? null, "artifacts[{$index}].arch");
            $target = self::plainToken($artifact['target'] ?? null, "artifacts[{$index}].target");
            $filename = self::plainToken($artifact['filename'] ?? null, "artifacts[{$index}].filename");
            $url = $artifact['url'] ?? null;
            $sha256 = strtolower((string) ($artifact['sha256'] ?? ''));

            $knownTargets = [
                'linux/amd64' => 'x86_64-unknown-linux-musl',
                'linux/arm64' => 'aarch64-unknown-linux-musl',
                'darwin/amd64' => 'x86_64-apple-darwin',
                'darwin/arm64' => 'aarch64-apple-darwin',
            ];
            $key = "{$os}/{$arch}";
            if (isset($knownTargets[$key]) && $target !== $knownTargets[$key]) {
                throw new RuntimeException("Release manifest artifact {$index} has an invalid target tuple.");
            }
            $expectedFilename = 'queen-supervisor-' . SupervisorBinary::VERSION . "-{$os}-{$arch}.tar.gz";
            if ($filename !== $expectedFilename || basename($filename) !== $filename) {
                throw new RuntimeException("Release manifest artifact {$index} has an unexpected filename.");
            }
            if (!is_string($url)) {
                throw new RuntimeException("Release manifest artifact {$index} has no URL.");
            }
            SupervisorBinary::assertHttpsUrl($url, "artifact {$index} URL");
            if (preg_match('/^[a-f0-9]{64}$/D', $sha256) !== 1) {
                throw new RuntimeException("Release manifest artifact {$index} has no valid SHA-256 digest.");
            }

            if (isset($artifacts[$key])) {
                throw new RuntimeException("Release manifest contains duplicate artifact {$key}.");
            }
            $artifacts[$key] = [
                'os' => $os,
                'arch' => $arch,
                'target' => $target,
                'filename' => $filename,
                'url' => $url,
                'sha256' => $sha256,
            ];
        }

        return new self($artifacts);
    }

    public function artifactFor(array $platform): array
    {
        $key = "{$platform['os']}/{$platform['arch']}";
        $artifact = $this->artifacts[$key] ?? null;
        if (!is_array($artifact) || $artifact['target'] !== $platform['target']) {
            throw new RuntimeException("The release manifest has no artifact for {$key}.");
        }

        return $artifact;
    }

    private static function plainToken(mixed $value, string $field): string
    {
        if (
            !is_string($value)
            || $value === ''
            || strlen($value) > 128
            || preg_match('/^[A-Za-z0-9._-]+$/D', $value) !== 1
        ) {
            throw new RuntimeException("Release manifest field {$field} is invalid.");
        }

        return $value;
    }
}
