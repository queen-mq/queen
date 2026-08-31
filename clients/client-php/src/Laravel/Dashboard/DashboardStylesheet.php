<?php

namespace Queen\Laravel\Dashboard;

use RuntimeException;

/**
 * The immutable stylesheet shipped with the Composer package.
 *
 * Its digest is used for both the route version and Subresource Integrity, so
 * changing the CSS produces a new cache key without a publish or build step.
 */
final class DashboardStylesheet
{
    private ?string $contents = null;

    private ?string $digest = null;

    public function contents(): string
    {
        if ($this->contents !== null) {
            return $this->contents;
        }

        $path = dirname(__DIR__, 3) . '/resources/css/dashboard.css';
        if (!is_file($path) || is_link($path) || !is_readable($path)) {
            throw new RuntimeException('The Queen dashboard stylesheet is unavailable.');
        }

        $contents = file_get_contents($path);
        if (!is_string($contents) || $contents === '') {
            throw new RuntimeException('The Queen dashboard stylesheet could not be read.');
        }

        return $this->contents = $contents;
    }

    public function version(): string
    {
        return bin2hex($this->digest());
    }

    public function integrity(): string
    {
        return 'sha256-' . base64_encode($this->digest());
    }

    private function digest(): string
    {
        return $this->digest ??= hash('sha256', $this->contents(), true);
    }
}
