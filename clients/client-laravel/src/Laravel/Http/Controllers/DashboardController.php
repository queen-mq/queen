<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Contracts\View\View;
use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardRepository;
use Queen\Laravel\Dashboard\DashboardStylesheet;

final class DashboardController
{
    public function __invoke(
        Request $request,
        DashboardRepository $dashboard,
        DashboardStylesheet $stylesheet,
    ): View {
        return view('queen::dashboard', [
            'snapshot' => $dashboard->snapshot(),
            'refreshSeconds' => $this->refreshSeconds($request),
            'refreshUrl' => route('queen.dashboard.index', [], false),
            'stylesheetUrl' => route('queen.dashboard.stylesheet', [
                'version' => $stylesheet->version(),
            ], false),
            'stylesheetIntegrity' => $stylesheet->integrity(),
            'controlError' => $request->session()->get('queen_dashboard_control_error'),
            'controlStatus' => $request->session()->get('queen_dashboard_control_status'),
        ]);
    }

    private function refreshSeconds(Request $request): int
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
