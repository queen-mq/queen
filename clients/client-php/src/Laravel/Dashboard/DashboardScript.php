<?php

namespace Queen\Laravel\Dashboard;

/**
 * The dashboard script: partial auto-refresh with a pause control and in-page
 * section navigation. The page works without it, falling back to a
 * `<noscript>` meta refresh. See DashboardAsset for versioning and publishing.
 */
final class DashboardScript extends DashboardAsset
{
    public const PUBLISHED_FILE = 'vendor/queen/dashboard.js';

    public function sourceFile(): string
    {
        return 'resources/js/dashboard.js';
    }

    public function publishedFile(): string
    {
        return self::PUBLISHED_FILE;
    }

    protected function routeName(): string
    {
        return 'queen.dashboard.script';
    }
}
