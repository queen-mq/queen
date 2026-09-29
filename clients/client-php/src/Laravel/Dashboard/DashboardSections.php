<?php

namespace Queen\Laravel\Dashboard;

/**
 * Sidebar sections of the dashboard and the URLs that keep one of them in view.
 *
 * The panel runs without JavaScript, so a fragment alone cannot survive the
 * automatic refresh: a refresh to the current URL with the same fragment is a
 * same-document scroll that fetches nothing, and a refresh to the bare route
 * forgets the section. Section links therefore carry the section in the query
 * string as well (`?view=supervisors#supervisors`). The server marks the active
 * sidebar entry from it and builds a refresh URL that keeps the section and
 * changes on every render (`at`), which forces a real reload.
 */
final class DashboardSections
{
    /** In sidebar order; the first one is the top of the page. */
    public const ALL = ['overview', 'workload', 'supervisors', 'failed-jobs', 'configuration'];

    public static function active(mixed $view): string
    {
        return is_string($view) && in_array($view, self::ALL, true) ? $view : self::ALL[0];
    }

    /** @return array<string, string> section => relative URL */
    public static function urls(): array
    {
        $urls = [];
        foreach (self::ALL as $section) {
            $urls[$section] = $section === self::ALL[0]
                ? self::base()
                : route('queen.dashboard.index', ['view' => $section], false) . '#' . $section;
        }

        return $urls;
    }

    public static function refreshUrl(string $active, int $renderedAt): string
    {
        if ($active === self::ALL[0]) {
            return self::base();
        }

        return route('queen.dashboard.index', ['view' => $active, 'at' => $renderedAt], false) . '#' . $active;
    }

    private static function base(): string
    {
        return route('queen.dashboard.index', [], false);
    }
}
