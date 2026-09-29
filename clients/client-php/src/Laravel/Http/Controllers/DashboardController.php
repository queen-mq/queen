<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Contracts\View\View;
use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardPage;
use Queen\Laravel\Dashboard\DashboardRepository;
use Queen\Laravel\Dashboard\ThroughputReader;

final class DashboardController
{
    public function __invoke(
        Request $request,
        DashboardRepository $dashboard,
        DashboardPage $page,
        ThroughputReader $throughput,
    ): View {
        $section = (string) $request->route()?->parameter('section', 'overview');
        // Keyset cursor of the failed-jobs page; every other page ignores it.
        $cursor = $section === 'failed-jobs' ? $this->cursor($request->query('cursor')) : null;
        $data = $page->data($request, $section, $dashboard->snapshot($cursor));
        if ($cursor !== null) {
            $data['refreshUrl'] .= '?cursor=' . $cursor;
        }
        if ($section === 'workload') {
            // Only the workload page reads the broker's counters.
            $range = ThroughputReader::range($request->query('range'));
            $data['throughput'] = $throughput->read($data['snapshot']['queues'], $range);
            if ($range !== ThroughputReader::DEFAULT_RANGE) {
                $data['refreshUrl'] .= '?range=' . $range;
            }
        }

        return view('queen::dashboard', $data);
    }

    private function cursor(mixed $value): ?int
    {
        if (!is_string($value) || preg_match('/^[0-9]{1,18}$/D', $value) !== 1) {
            return null;
        }
        $cursor = (int) $value;

        return $cursor > 0 ? $cursor : null;
    }
}
