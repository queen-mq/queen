<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardConflictException;
use Queen\Laravel\Dashboard\DashboardRepository;
use Queen\Laravel\Dashboard\DashboardStylesheet;
use Symfony\Component\HttpFoundation\RedirectResponse;
use Symfony\Component\HttpFoundation\Response;

final class SupervisorControlController
{
    public function __invoke(
        Request $request,
        DashboardRepository $dashboard,
        DashboardStylesheet $stylesheet,
        string $command,
    ): Response {
        $instanceId = $request->input('instance_id');
        if (!is_string($instanceId)
            || $instanceId === ''
            || strlen($instanceId) > 128
            || preg_match('/^[A-Za-z0-9._:-]+$/D', $instanceId) !== 1) {
            abort(422, 'A valid supervisor instance identifier is required.');
        }

        try {
            $dashboard->request($command, $instanceId);
        } catch (DashboardConflictException $exception) {
            return response()->view('queen::dashboard', [
                'snapshot' => $dashboard->snapshot(),
                'refreshSeconds' => $this->refreshSeconds(),
                'refreshUrl' => route('queen.dashboard.index', [], false),
                'stylesheetUrl' => route('queen.dashboard.stylesheet', [
                    'version' => $stylesheet->version(),
                ], false),
                'stylesheetIntegrity' => $stylesheet->integrity(),
                'controlError' => $exception->getMessage(),
                'controlStatus' => null,
            ], 409);
        }

        $request->session()->flash(
            'queen_dashboard_control_status',
            "Supervisor command [{$command}] accepted and pending consumption.",
        );

        return new RedirectResponse(route('queen.dashboard.index', [], false), 303);
    }

    private function refreshSeconds(): int
    {
        $value = config('queen.dashboard.refresh_seconds', 5);
        if (is_string($value) && preg_match('/^[0-9]+$/D', $value) === 1) {
            $value = (int) $value;
        }

        return is_int($value) && $value >= 2 && $value <= 60 ? $value : 5;
    }
}
