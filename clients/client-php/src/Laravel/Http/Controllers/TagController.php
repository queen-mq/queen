<?php

namespace Queen\Laravel\Http\Controllers;

use Illuminate\Http\Request;
use Queen\Laravel\Dashboard\DashboardSections;
use Queen\Laravel\Monitoring\TagMonitor;
use Symfony\Component\HttpFoundation\RedirectResponse;

final class TagController
{
    public function __invoke(Request $request, TagMonitor $tags): RedirectResponse
    {
        $tag = $request->input('tag');
        if (!is_string($tag) || trim($tag) === '' || strlen($tag) > 128) {
            abort(422, 'A tag of at most 128 characters is required.');
        }
        $monitor = (bool) $request->route()?->defaults['monitor'];

        try {
            $tags->change($tag, $monitor);
            $message = $monitor ? "Monitoring tag [{$tag}]." : "Stopped monitoring tag [{$tag}].";
            $request->session()->flash('queen_dashboard_control_status', $message);
        } catch (\Throwable $error) {
            $request->session()->flash('queen_dashboard_control_error', $error instanceof \InvalidArgumentException
                ? $error->getMessage()
                : 'The monitored tags could not be changed.');
        }

        return new RedirectResponse(DashboardSections::url('tags') . ($monitor ? '?tag=' . rawurlencode(trim($tag)) : ''), 303);
    }
}
