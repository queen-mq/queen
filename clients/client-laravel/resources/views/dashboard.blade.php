<!doctype html>
<html lang="en" data-theme="auto">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="{{ $refreshSeconds }};url={{ $refreshUrl }}">
    <title>Queen Supervisor</title>
    <link rel="stylesheet" href="{{ $stylesheetUrl }}" integrity="{{ $stylesheetIntegrity }}">
</head>
<body>
@php
    $supervisor = $snapshot['supervisor'];
    $queues = $snapshot['queues'];
    $failedJobs = $snapshot['failed_jobs'];
    $poolCount = count($supervisor['pools']);
    $knownDepth = 0;
    $unknownDepths = 0;
    foreach ($queues as $queue) {
        if (($queue['available'] ?? false) && is_int($queue['depth'] ?? null)) {
            $knownDepth += $queue['depth'];
        } else {
            $unknownDepths++;
        }
    }
    $depthLabel = $unknownDepths === count($queues) && $queues !== []
        ? '—'
        : number_format($knownDepth) . ($unknownDepths > 0 ? '+' : '');
    $failedLabel = ($failedJobs['available'] ?? false)
        ? number_format($failedJobs['total']) . (($failedJobs['total_exact'] ?? false) ? '' : '+')
        : '—';
    if ($supervisor['availability'] === 'live') {
        $stateLabel = match ($supervisor['state']) {
            'running' => 'Active',
            'paused' => 'Paused',
            'terminating' => 'Stopping',
            'starting' => 'Starting',
            'stopped' => 'Stopped',
            default => 'Unknown',
        };
        $stateTone = $supervisor['state'] === 'running' ? 'success' : 'warning';
    } elseif ($supervisor['availability'] === 'stale') {
        $stateLabel = 'Stale';
        $stateTone = 'warning';
    } else {
        $stateLabel = 'Unavailable';
        $stateTone = 'danger';
    }
@endphp

<a class="skip-link" href="#main-content">Skip to content</a>

<div class="app">
    @include('queen::dashboard.partials.header')

    <div class="layout">
        @include('queen::dashboard.partials.sidebar')

        <main id="main-content" class="content" tabindex="-1">
            <div class="page-heading">
                <h1>Overview</h1>
                <p>Current state of this application's local Queen worker supervisor.</p>
            </div>

            @include('queen::dashboard.partials.notices')
            @include('queen::dashboard.partials.overview')
            @include('queen::dashboard.partials.workload')
            @include('queen::dashboard.partials.supervisor')
            @include('queen::dashboard.partials.failed-jobs')
            @include('queen::dashboard.partials.configuration')

            <footer class="footer">Auto-refreshes every {{ $refreshSeconds }} seconds · local supervisor state only</footer>
        </main>
    </div>
</div>
</body>
</html>
