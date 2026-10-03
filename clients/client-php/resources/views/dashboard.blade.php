<!doctype html>
<html lang="en" data-theme="auto">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    @if ($autoRefresh)
    <noscript><meta http-equiv="refresh" content="{{ $refreshSeconds }};url={{ $refreshUrl }}"></noscript>
    @endif
    <title>{{ $pageTitle }} · Queen Supervisor</title>
    <link rel="stylesheet" href="{{ $stylesheetUrl }}" integrity="{{ $stylesheetIntegrity }}">
    <script src="{{ $scriptUrl }}" integrity="{{ $scriptIntegrity }}" defer></script>
</head>
<body @if ($autoRefresh) data-refresh-seconds="{{ $refreshSeconds }}" @endif>
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
    $processBudget = $supervisor['process_budget'];
    $budgetLabel = $processBudget['valid']
        ? number_format($processBudget['used']) . ' / ' . number_format($processBudget['limit'])
        : '—';
    // Shared with the per-instance cards of the supervisors page.
    $stateLabelFor = fn (array $supervisor): string => match ($supervisor['availability']) {
        'live' => match ($supervisor['state']) {
            'running' => 'Active',
            'paused' => 'Paused',
            'terminating' => 'Stopping',
            'starting' => 'Starting',
            'stopped' => 'Stopped',
            'mixed' => 'Mixed',
            default => 'Unknown',
        },
        'stale' => 'Stale',
        default => 'Unavailable',
    };
    $stateLabel = $stateLabelFor($supervisor);
    [$livenessLabel, $livenessTone] = match ($supervisor['availability']) {
        'live' => ['Live', 'success'],
        'stale' => ['Stale', 'warning'],
        default => ['Unavailable', 'danger'],
    };
    $instanceCount = $supervisor['instances'];
    $liveInstanceCount = $supervisor['live_instances'];
    $stateSourceLabel = ($instanceCount > 1 ? $instanceCount . ' supervisor instances · ' : '')
        . (($supervisor['source'] ?? null) === 'remote' ? 'supervisor state published through the broker' : 'local supervisor state only');
    $readinessLabel = $supervisor['ready'] ? 'Ready' : 'Not ready';
    $readinessTone = $supervisor['ready'] ? 'success' : ($supervisor['availability'] === 'live' ? 'warning' : 'danger');
    $capacityLabel = $supervisor['capacity_satisfied'] ? 'Satisfied' : 'Below desired';
    $capacityTone = $supervisor['capacity_satisfied'] ? 'success' : 'warning';
@endphp

<a class="skip-link" href="#main-content">Skip to content</a>

<div class="app">
    @include('queen::dashboard.partials.header')

    <div class="layout">
        @include('queen::dashboard.partials.sidebar')

        <main id="main-content" class="content" tabindex="-1">
            <div class="page-heading">
                <h1>{{ $pageTitle }}</h1>
                <p>{{ $pageDescription }}</p>
            </div>

            @include('queen::dashboard.partials.notices')
            @include($contentView)

            <footer class="footer">@if ($autoRefresh)<span data-refresh-state>Auto-refreshes every {{ $refreshSeconds }} seconds</span> · @endif{{ $stateSourceLabel }}</footer>
        </main>
    </div>
</div>
</body>
</html>
