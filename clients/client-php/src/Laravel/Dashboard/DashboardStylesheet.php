<?php

namespace Queen\Laravel\Dashboard;

/** The dashboard stylesheet. See DashboardAsset for versioning and publishing. */
final class DashboardStylesheet extends DashboardAsset
{
    public const PUBLISHED_FILE = 'vendor/queen/dashboard.css';

    public function sourceFile(): string
    {
        return 'resources/css/dashboard.css';
    }

    public function publishedFile(): string
    {
        return self::PUBLISHED_FILE;
    }

    protected function routeName(): string
    {
        return 'queen.dashboard.stylesheet';
    }
}
