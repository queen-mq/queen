<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Contracts\Console\Kernel as Artisan;
use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardRepository;
use Queen\Laravel\Dashboard\DashboardSections;
use Symfony\Component\HttpFoundation\RedirectResponse;

/**
 * Retry one failed job from its detail: Laravel's own queue:retry, so a
 * Queen job keeps its retry fence and its dead-letter entry is cleared.
 */
final class FailedJobRetryController
{
    private const MAX_ERROR_BYTES = 300;

    public function __invoke(Request $request, string $id, DashboardRepository $dashboard, Artisan $artisan): RedirectResponse
    {
        // queue:retry reads "all" as every failed job: never pass it through.
        if (strtolower($id) === 'all') {
            return $this->back($request, DashboardSections::url('failed-jobs'), error: 'Failed job [all] cannot be retried from the dashboard.');
        }
        $basePath = app()->basePath();
        $failedJob = $dashboard->failedJob($id, $basePath);
        if ($failedJob === null) {
            return $this->back($request, DashboardSections::url('failed-jobs'), error: "Failed job [{$id}] is no longer in the failed-job store.");
        }

        try {
            $exitCode = $artisan->call('queue:retry', ['id' => [$id]]);
            $reason = trim($artisan->output());
        } catch (\Throwable $error) {
            // An exception can quote a query and its bindings, the job's
            // payload included: report it, and show only its class.
            report($error);
            $exitCode = 1;
            $reason = 'the application raised ' . (new \ReflectionClass($error))->getShortName() . ', which was reported to its log';
        }
        if ($exitCode === 0 && $dashboard->failedJob($id, $basePath) === null) {
            $queue = $failedJob['queue'] ?? 'its queue';

            return $this->back($request, DashboardSections::url('failed-jobs'), status: "Failed job [{$id}] was pushed back onto queue [{$queue}].");
        }

        // The reason may name a path of this host: show only the part after the application root.
        $reason = str_replace(rtrim($basePath, '/') . '/', '', $reason);
        if (strlen($reason) > self::MAX_ERROR_BYTES) {
            $reason = mb_strcut($reason, 0, self::MAX_ERROR_BYTES, 'UTF-8') . '…';
        }

        return $this->back(
            $request,
            DashboardSections::url('failed-jobs') . '/' . rawurlencode($id),
            error: "Failed job [{$id}] was not retried" . ($reason === '' ? '.' : ": {$reason}"),
        );
    }

    private function back(Request $request, string $url, ?string $status = null, ?string $error = null): RedirectResponse
    {
        if ($status !== null) {
            $request->session()->flash('queen_dashboard_control_status', $status);
        }
        if ($error !== null) {
            $request->session()->flash('queen_dashboard_control_error', $error);
        }

        return new RedirectResponse($url, 303);
    }
}
