<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Contracts\View\View;
use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardPage;
use Queen\Laravel\Dashboard\DashboardRepository;

final class DashboardController
{
    public function __invoke(Request $request, DashboardRepository $dashboard, DashboardPage $page): View
    {
        $section = (string) $request->route()?->parameter('section', 'overview');
        // Keyset cursor of the failed-jobs page; every other page ignores it.
        $cursor = $section === 'failed-jobs' ? $this->cursor($request->query('cursor')) : null;
        $data = $page->data($request, $section, $dashboard->snapshot($cursor));
        if ($cursor !== null) {
            $data['refreshUrl'] .= '?cursor=' . $cursor;
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
