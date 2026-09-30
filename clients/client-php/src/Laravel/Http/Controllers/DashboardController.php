<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Contracts\View\View;
use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardPage;
use Queen\Laravel\Dashboard\DashboardRepository;
use Queen\Laravel\Dashboard\JobMetricsReader;
use Queen\Laravel\Dashboard\ThroughputReader;
use Queen\Laravel\Monitoring\JobTags;
use Queen\Laravel\Monitoring\TagMonitor;

final class DashboardController
{
    public function __invoke(
        Request $request,
        DashboardRepository $dashboard,
        DashboardPage $page,
        ThroughputReader $throughput,
        JobMetricsReader $jobMetrics,
        TagMonitor $tags,
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

        if ($section === 'jobs') {
            $range = JobMetricsReader::range($request->query('range'));
            $data['jobMetrics'] = $jobMetrics->read($range);
            if ($range !== JobMetricsReader::DEFAULT_RANGE) {
                $data['refreshUrl'] .= '?range=' . $range;
            }
        }

        if ($section === 'tags') {
            $data['tagMonitor'] = $this->tags($tags, $request->query('tag'));
            if ($data['tagMonitor']['selected'] !== null) {
                $data['refreshUrl'] .= '?tag=' . rawurlencode($data['tagMonitor']['selected']);
            }
        }

        return view('queen::dashboard', $data);
    }

    /** @return array{available: bool, monitored: list<string>, selected: ?string, jobs: list<array<string, mixed>>} */
    private function tags(TagMonitor $tags, mixed $selected): array
    {
        try {
            $monitored = $tags->monitored();
            $selected = is_string($selected) ? (JobTags::normalize([$selected])[0] ?? null) : null;
            $selected = in_array($selected, $monitored, true) ? $selected : ($monitored[0] ?? null);

            return [
                'available' => true,
                'monitored' => $monitored,
                'selected' => $selected,
                'jobs' => $selected === null ? [] : $tags->recent($selected),
            ];
        } catch (\Throwable) {
            return ['available' => false, 'monitored' => [], 'selected' => null, 'jobs' => []];
        }
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
