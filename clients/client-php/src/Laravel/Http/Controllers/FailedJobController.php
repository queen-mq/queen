<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Contracts\View\View;
use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardPage;
use Queen\Laravel\Dashboard\DashboardRepository;

/**
 * The detail page of one failed job: why it failed, never its payload.
 */
final class FailedJobController
{
    public function __invoke(Request $request, string $id, DashboardRepository $dashboard, DashboardPage $page): View
    {
        $failedJob = $dashboard->failedJob($id, app()->basePath());
        abort_if($failedJob === null, 404);

        return view('queen::dashboard', $page->data($request, 'failed-jobs', $dashboard->snapshot(), [
            'contentView' => 'queen::dashboard.partials.failed-job',
            'pageTitle' => 'Failed job',
            'pageDescription' => $failedJob['job'] ?? 'Unknown job',
            'failedJob' => $failedJob,
            // A failed job does not change; nothing to refresh.
            'autoRefresh' => false,
        ]));
    }
}
