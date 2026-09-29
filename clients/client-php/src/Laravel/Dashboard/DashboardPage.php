<?php

namespace Queen\Laravel\Dashboard;

use Illuminate\Http\Request;

/**
 * View data shared by every dashboard page: layout, navigation, assets and
 * the refresh policy.
 */
final class DashboardPage
{
    public function __construct(
        private DashboardStylesheet $stylesheet,
        private DashboardScript $script,
    ) {
    }

    /**
     * @param array<string, mixed> $snapshot
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    public function data(Request $request, string $section, array $snapshot, array $overrides = []): array
    {
        $section = DashboardSections::active($section);
        $basePath = $request->getBasePath();

        return array_replace([
            'snapshot' => $snapshot,
            'activeSection' => $section,
            'sectionUrls' => DashboardSections::urls(),
            'contentView' => DashboardSections::view($section),
            'pageTitle' => DashboardSections::title($section),
            'pageDescription' => DashboardSections::description($section),
            'autoRefresh' => true,
            'refreshSeconds' => $this->refreshSeconds(),
            // The page itself, rebuilt from the route rather than echoed from
            // the request, so only the parameters the page understands survive.
            'refreshUrl' => $request->getBaseUrl() . $request->getPathInfo(),
            'stylesheetUrl' => $this->stylesheet->url($basePath),
            'stylesheetIntegrity' => $this->stylesheet->integrity(),
            'scriptUrl' => $this->script->url($basePath),
            'scriptIntegrity' => $this->script->integrity(),
            'failedJobDetails' => config('queen.dashboard.failed_job_details', false) === true,
            'controlError' => $request->hasSession() ? $request->session()->get('queen_dashboard_control_error') : null,
            'controlStatus' => $request->hasSession() ? $request->session()->get('queen_dashboard_control_status') : null,
        ], $overrides);
    }

    public function refreshSeconds(): int
    {
        // Refresh frequency is deployment policy. A query string must not let
        // a visitor turn the dashboard into an application-level poller.
        $value = config('queen.dashboard.refresh_seconds', 5);
        if (is_string($value) && preg_match('/^[0-9]+$/D', $value) === 1) {
            $value = (int) $value;
        }

        return is_int($value) && $value >= 2 && $value <= 60
            ? $value
            : 5;
    }
}
