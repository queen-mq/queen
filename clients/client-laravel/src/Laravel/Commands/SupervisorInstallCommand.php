<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Queen\Laravel\Supervisor\Binary\SupervisorBinary;
use Queen\Laravel\Supervisor\Binary\SupervisorBinaryInstaller;

class SupervisorInstallCommand extends Command
{
    protected $signature = 'queen:supervisor-install
        {--manifest= : Pinned release manifest HTTPS URL or an offline local file}
        {--archive= : Offline local release archive (requires --manifest)}
        {--base-url= : HTTPS mirror base URL for both manifest and artifact}
        {--manifest-sha256= : Trusted expected SHA-256 of the release manifest}
        {--install-path= : Application-local binary installation directory}
        {--force : Revalidate and replace an existing installation}';

    protected $description = 'Install the pinned native Queen supervisor with SHA-256 verification';

    public function handle(SupervisorBinaryInstaller $installer): int
    {
        try {
            $binaryConfig = $this->laravel['config']->get('queen.supervisor_binary', []);
            $binaryConfig = is_array($binaryConfig) ? $binaryConfig : [];

            $installPath = $this->optionString('install-path')
                ?? $this->configurationString($binaryConfig['install_path'] ?? null, 'queen.supervisor_binary.install_path');
            $baseUrl = $this->optionString('base-url')
                ?? $this->optionalConfigurationString(
                    $binaryConfig['release_base_url'] ?? null,
                    'queen.supervisor_binary.release_base_url',
                );
            $explicitManifest = $this->optionString('manifest')
                ?? $this->optionalConfigurationString(
                    $binaryConfig['manifest'] ?? null,
                    'queen.supervisor_binary.manifest',
                );
            $archive = $this->optionString('archive');
            $manifestSha256 = $this->optionString('manifest-sha256')
                ?? $this->optionalConfigurationString(
                    $binaryConfig['manifest_sha256'] ?? null,
                    'queen.supervisor_binary.manifest_sha256',
                );
            if ($archive !== null && ($explicitManifest === null || $this->isUrl($explicitManifest))) {
                throw new \InvalidArgumentException(
                    'Offline --archive requires an explicit local --manifest.',
                );
            }
            $manifest = $explicitManifest ?? ($baseUrl === null
                ? SupervisorBinary::defaultManifestUrl()
                : SupervisorBinary::manifestUrlForBase($baseUrl));

            $result = $installer->install(
                $installPath,
                $manifest,
                archiveSource: $archive,
                releaseBaseUrl: $baseUrl,
                force: (bool) $this->option('force'),
                manifestSha256: $manifestSha256,
            );
        } catch (\Throwable $exception) {
            $this->components->error($exception->getMessage());

            return self::FAILURE;
        }

        $action = $result['installed'] ? 'Installed' : 'Verified existing';
        $this->components->info(
            "{$action} Queen supervisor {$result['version']} ({$result['target']})",
        );
        $this->line($result['binary']);

        return self::SUCCESS;
    }

    private function isUrl(string $source): bool
    {
        return preg_match('#^[A-Za-z][A-Za-z0-9+.-]*://#D', $source) === 1;
    }

    private function optionString(string $name): ?string
    {
        $value = $this->option($name);
        if ($value === null) {
            return null;
        }
        if (!is_string($value) || trim($value) === '' || str_contains($value, "\0")) {
            throw new \InvalidArgumentException("--{$name} must be a non-empty string.");
        }

        return $value;
    }

    private function configurationString(mixed $value, string $name): string
    {
        $resolved = $this->optionalConfigurationString($value, $name);
        if ($resolved === null) {
            throw new \InvalidArgumentException("{$name} must be configured.");
        }

        return $resolved;
    }

    private function optionalConfigurationString(mixed $value, string $name): ?string
    {
        if ($value === null) {
            return null;
        }
        if (!is_string($value) || trim($value) === '' || str_contains($value, "\0")) {
            throw new \InvalidArgumentException("{$name} must be a non-empty string.");
        }

        return $value;
    }
}
