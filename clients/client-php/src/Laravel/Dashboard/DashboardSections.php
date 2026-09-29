<?php

namespace Queen\Laravel\Dashboard;

/**
 * The dashboard pages: one route per section, in sidebar order.
 */
final class DashboardSections
{
    /** Failed-job identifiers the detail route accepts: Laravel ids and uuids. */
    public const FAILED_JOB_ID_PATTERN = '[A-Za-z0-9._:-]{1,128}';

    /** @var array<string, array{route: string, view: string, title: string, description: string}> */
    private const PAGES = [
        'overview' => [
            'route' => 'queen.dashboard.index',
            'view' => 'queen::dashboard.partials.overview',
            'title' => 'Overview',
            'description' => "Current state of this application's Queen worker supervisor.",
        ],
        'workload' => [
            'route' => 'queen.dashboard.workload',
            'view' => 'queen::dashboard.partials.workload',
            'title' => 'Workload',
            'description' => 'Queue depth last sampled by the supervisor.',
        ],
        'supervisors' => [
            'route' => 'queen.dashboard.supervisors',
            'view' => 'queen::dashboard.partials.supervisor',
            'title' => 'Supervisors',
            'description' => 'The master instance, its worker pools and its controls.',
        ],
        'failed-jobs' => [
            'route' => 'queen.dashboard.failed-jobs',
            'view' => 'queen::dashboard.partials.failed-jobs',
            'title' => 'Failed jobs',
            'description' => "Laravel's failed-job index, newest first.",
        ],
        'configuration' => [
            'route' => 'queen.dashboard.configuration',
            'view' => 'queen::dashboard.partials.configuration',
            'title' => 'Configuration',
            'description' => 'Worker settings published by the running supervisor generation.',
        ],
    ];

    public static function active(mixed $section): string
    {
        return is_string($section) && array_key_exists($section, self::PAGES) ? $section : 'overview';
    }

    /** @return array<string, string> section => relative URL */
    public static function urls(): array
    {
        $urls = [];
        foreach (self::PAGES as $section => $page) {
            $urls[$section] = route($page['route'], [], false);
        }

        return $urls;
    }

    public static function url(string $section): string
    {
        return route(self::PAGES[self::active($section)]['route'], [], false);
    }

    /** Relative URL of a failed job's detail page, or null for an identifier the route cannot carry. */
    public static function failedJobUrl(string|int $id): ?string
    {
        $id = (string) $id;
        if (preg_match('/^' . self::FAILED_JOB_ID_PATTERN . '$/D', $id) !== 1) {
            return null;
        }

        return route('queen.dashboard.failed-job', ['id' => $id], false);
    }

    public static function view(string $section): string
    {
        return self::PAGES[self::active($section)]['view'];
    }

    public static function title(string $section): string
    {
        return self::PAGES[self::active($section)]['title'];
    }

    public static function description(string $section): string
    {
        return self::PAGES[self::active($section)]['description'];
    }
}
