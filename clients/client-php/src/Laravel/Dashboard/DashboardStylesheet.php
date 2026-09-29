<?php

namespace Queen\Laravel\Dashboard;

use RuntimeException;

/**
 * The immutable stylesheet shipped with the Composer package.
 *
 * Its digest is used for both the route version and Subresource Integrity, so
 * changing the CSS produces a new cache key without a publish or build step.
 *
 * Applications whose web server answers every `*.css` request from the public
 * directory (a common nginx or Caddy static rule) can publish a copy with
 * `vendor:publish --tag=queen-assets`. The dashboard links to that copy only
 * while it is byte-identical to the packaged file, so a copy left stale by an
 * upgrade falls back to the route instead of reaching the browser.
 */
final class DashboardStylesheet
{
    /** Where `vendor:publish --tag=queen-assets` copies the stylesheet, relative to the public directory. */
    public const PUBLISHED_FILE = 'vendor/queen/dashboard.css';

    private ?string $contents = null;

    private ?string $digest = null;

    private ?bool $publishedCopyIsCurrent = null;

    public function __construct(private ?string $publicPath = null)
    {
    }

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

    /**
     * The stylesheet URL for the dashboard view: the published copy when it is
     * current, otherwise the package route. Both are relative to the request
     * base path and carry the content version, so either can be cached forever.
     */
    public function url(string $basePath = ''): string
    {
        if ($this->hasCurrentPublishedCopy()) {
            return rtrim($basePath, '/') . '/' . self::PUBLISHED_FILE . '?v=' . $this->version();
        }

        return route('queen.dashboard.stylesheet', ['version' => $this->version()], false);
    }

    /**
     * Whether the public directory holds a regular, byte-identical copy of the
     * packaged stylesheet. Checked once per instance: a deployment that
     * republishes also restarts the processes that hold this singleton.
     */
    public function hasCurrentPublishedCopy(): bool
    {
        if ($this->publishedCopyIsCurrent !== null) {
            return $this->publishedCopyIsCurrent;
        }
        if ($this->publicPath === null) {
            return $this->publishedCopyIsCurrent = false;
        }

        $path = rtrim($this->publicPath, '/') . '/' . self::PUBLISHED_FILE;
        if (!is_file($path) || is_link($path) || !is_readable($path)) {
            return $this->publishedCopyIsCurrent = false;
        }
        $contents = file_get_contents($path);

        return $this->publishedCopyIsCurrent = is_string($contents)
            && hash_equals($this->digest(), hash('sha256', $contents, true));
    }

    private function digest(): string
    {
        return $this->digest ??= hash('sha256', $this->contents(), true);
    }
}
