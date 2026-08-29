<!doctype html>
<html lang="en" data-theme="auto">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="{{ $refreshSeconds }};url={{ $refreshUrl }}">
    <title>Queen Supervisor</title>
    <style nonce="{{ $cspNonce }}">
        :root {
            color-scheme: light;
            --background: #f3f4f6;
            --surface: #ffffff;
            --surface-muted: #f9fafb;
            --surface-hover: #f3f4f6;
            --border: #e5e7eb;
            --border-strong: #d1d5db;
            --text: #111827;
            --muted: #6b7280;
            --muted-strong: #4b5563;
            --brand: #6952d5;
            --brand-hover: #5842c2;
            --brand-soft: #eeebff;
            --success: #047857;
            --success-soft: #d1fae5;
            --warning: #b45309;
            --warning-soft: #fef3c7;
            --danger: #b91c1c;
            --danger-soft: #fee2e2;
            --shadow: 0 4px 6px -1px rgb(0 0 0 / .08), 0 2px 4px -2px rgb(0 0 0 / .08);
            --radius: 6px;
        }

        @media (prefers-color-scheme: dark) {
            :root:not([data-theme="light"]) {
                color-scheme: dark;
                --background: #111827;
                --surface: #1f2937;
                --surface-muted: #182130;
                --surface-hover: #263244;
                --border: #374151;
                --border-strong: #4b5563;
                --text: #f3f4f6;
                --muted: #9ca3af;
                --muted-strong: #d1d5db;
                --brand: #a78bfa;
                --brand-hover: #c4b5fd;
                --brand-soft: #312e5d;
                --success: #6ee7b7;
                --success-soft: #064e3b;
                --warning: #fcd34d;
                --warning-soft: #78350f;
                --danger: #fca5a5;
                --danger-soft: #7f1d1d;
                --shadow: 0 4px 6px -1px rgb(0 0 0 / .24), 0 2px 4px -2px rgb(0 0 0 / .2);
            }
        }

        * { box-sizing: border-box; }
        html { scroll-behavior: smooth; }
        body {
            margin: 0;
            background: var(--background);
            color: var(--text);
            font: 15px/1.5 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            -webkit-font-smoothing: antialiased;
        }
        button, input { font: inherit; }
        a { color: inherit; }
        code {
            color: var(--muted-strong);
            font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
            overflow-wrap: anywhere;
        }
        .skip-link {
            position: fixed;
            z-index: 20;
            top: 12px;
            left: 12px;
            padding: 9px 13px;
            border-radius: var(--radius);
            background: var(--text);
            color: var(--surface);
            transform: translateY(-180%);
        }
        .skip-link:focus { transform: translateY(0); }
        :focus-visible {
            outline: 3px solid var(--brand);
            outline-offset: 2px;
        }
        .app {
            width: min(1340px, calc(100% - 40px));
            margin: 0 auto;
            padding-bottom: 64px;
        }
        .topbar {
            min-height: 81px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 24px;
            border-bottom: 1px solid var(--border);
        }
        .brand {
            display: inline-flex;
            align-items: center;
            gap: 11px;
            color: var(--text);
            text-decoration: none;
        }
        .brand-mark {
            width: 32px;
            height: 32px;
            color: var(--brand);
            flex: none;
        }
        .brand-name {
            display: block;
            font-size: 18px;
            line-height: 1.2;
            letter-spacing: -.015em;
        }
        .brand-name strong { font-weight: 700; }
        .brand-name span { font-weight: 400; }
        .brand-context {
            display: block;
            margin-top: 2px;
            color: var(--muted);
            font-size: 12px;
        }
        .topbar-meta {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 18px;
            color: var(--muted);
            font-size: 13px;
        }
        .operational-state {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            color: var(--muted-strong);
            font-weight: 600;
        }
        .state-mark {
            width: 17px;
            height: 17px;
            display: inline-grid;
            place-items: center;
            border: 2px solid currentColor;
            border-radius: 50%;
        }
        .state-mark::after {
            content: "";
            width: 5px;
            height: 5px;
            border-radius: 50%;
            background: currentColor;
        }
        .tone-success { color: var(--success); }
        .tone-warning { color: var(--warning); }
        .tone-danger { color: var(--danger); }
        .layout {
            display: grid;
            grid-template-columns: 184px minmax(0, 1fr);
            gap: 32px;
            padding-top: 30px;
        }
        .sidebar { min-width: 0; }
        .sidebar nav {
            position: sticky;
            top: 24px;
        }
        .nav-list {
            list-style: none;
            margin: 0;
            padding: 0;
            display: grid;
            gap: 4px;
        }
        .nav-link {
            min-height: 40px;
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 8px 12px;
            border-radius: var(--radius);
            color: var(--muted-strong);
            font-size: 14px;
            font-weight: 500;
            text-decoration: none;
        }
        .nav-link:hover {
            background: var(--surface-hover);
            color: var(--brand);
        }
        .nav-link[aria-current="page"] {
            background: var(--brand-soft);
            color: var(--brand);
            font-weight: 600;
        }
        .nav-icon {
            width: 19px;
            height: 19px;
            flex: none;
            fill: none;
            stroke: currentColor;
            stroke-width: 1.7;
            stroke-linecap: round;
            stroke-linejoin: round;
        }
        .content { min-width: 0; }
        .page-heading { margin-bottom: 20px; }
        .page-heading h1 {
            margin: 0;
            font-size: 25px;
            line-height: 1.2;
            letter-spacing: -.025em;
        }
        .page-heading p {
            margin: 6px 0 0;
            color: var(--muted);
        }
        .notice {
            padding: 12px 14px;
            margin-bottom: 18px;
            border: 1px solid var(--success);
            border-radius: var(--radius);
            background: var(--success-soft);
            color: var(--success);
        }
        .notice.error {
            border-color: var(--danger);
            background: var(--danger-soft);
            color: var(--danger);
        }
        .availability-notice {
            display: flex;
            align-items: flex-start;
            gap: 10px;
            padding: 12px 14px;
            margin-bottom: 18px;
            border: 1px solid var(--warning);
            border-radius: var(--radius);
            background: var(--warning-soft);
            color: var(--warning);
        }
        .availability-notice strong { display: block; }
        .availability-notice span { color: var(--muted-strong); }
        .card {
            margin-top: 24px;
            overflow: hidden;
            border-radius: var(--radius);
            background: var(--surface);
            box-shadow: var(--shadow);
        }
        .card:first-of-type { margin-top: 0; }
        .card-header {
            min-height: 60px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 18px;
            padding: 13px 20px;
            border-bottom: 1px solid var(--border);
        }
        .card-header h2 {
            margin: 0;
            font-size: 16px;
            line-height: 1.3;
        }
        .card-header p {
            margin: 3px 0 0;
            color: var(--muted);
            font-size: 13px;
        }
        .header-meta {
            color: var(--muted);
            font-size: 13px;
            white-space: nowrap;
        }
        .metrics {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            margin: 0;
            background: var(--surface-muted);
        }
        .metric {
            min-width: 0;
            padding: 20px 22px;
            border-left: 1px solid var(--border);
        }
        .metric:nth-child(4n + 1) { border-left: 0; }
        .metric:nth-child(n + 5) { border-top: 1px solid var(--border); }
        .metric dt {
            color: var(--muted);
            font-size: 12px;
            font-weight: 650;
        }
        .metric dd {
            margin: 7px 0 0;
            font-size: 22px;
            font-weight: 600;
            line-height: 1.2;
            font-variant-numeric: tabular-nums;
        }
        .metric small {
            display: block;
            min-height: 18px;
            margin-top: 5px;
            color: var(--muted);
            font-size: 12px;
            font-weight: 400;
        }
        .metric .operational-state { font-size: 18px; }
        .metric .state-mark { width: 19px; height: 19px; }
        .controls {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 8px;
            flex-wrap: wrap;
        }
        .controls form { margin: 0; }
        .button {
            min-height: 40px;
            padding: 7px 12px;
            border: 1px solid var(--border-strong);
            border-radius: var(--radius);
            background: var(--surface);
            color: var(--muted-strong);
            font-weight: 600;
            cursor: pointer;
        }
        .button:hover:not(:disabled) {
            border-color: var(--brand);
            color: var(--brand);
        }
        .button.primary {
            border-color: var(--brand);
            background: var(--brand);
            color: #ffffff;
        }
        .button.primary:hover:not(:disabled) {
            border-color: var(--brand-hover);
            background: var(--brand-hover);
            color: #ffffff;
        }
        .button.danger {
            border-color: var(--danger);
            color: var(--danger);
        }
        .button.danger:hover:not(:disabled) {
            background: var(--danger-soft);
            color: var(--danger);
        }
        .button:disabled {
            opacity: .45;
            cursor: not-allowed;
        }
        .instance-meta {
            display: grid;
            grid-template-columns: minmax(0, 1.5fr) repeat(2, minmax(110px, .5fr));
            gap: 16px;
            padding: 16px 20px;
            border-bottom: 1px solid var(--border);
            background: var(--surface-muted);
        }
        .meta-label {
            display: block;
            margin-bottom: 3px;
            color: var(--muted);
            font-size: 11px;
            font-weight: 650;
        }
        .meta-value {
            display: block;
            color: var(--muted-strong);
            font-size: 13px;
            font-variant-numeric: tabular-nums;
        }
        .subsection-title {
            margin: 0;
            padding: 15px 20px 10px;
            font-size: 13px;
            font-weight: 650;
        }
        .table-wrap {
            max-width: 100%;
            overflow-x: auto;
        }
        .table-wrap:focus-visible { outline-offset: -3px; }
        table {
            width: 100%;
            min-width: 720px;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px 20px;
            border-top: 1px solid var(--border);
            text-align: left;
            vertical-align: middle;
        }
        th {
            border-top: 0;
            background: var(--surface-muted);
            color: var(--muted);
            font-size: 12px;
            font-weight: 650;
        }
        tbody tr:hover { background: var(--surface-hover); }
        td {
            color: var(--muted-strong);
            font-size: 14px;
        }
        td strong { color: var(--text); font-weight: 600; }
        .number {
            text-align: right;
            white-space: nowrap;
            font-variant-numeric: tabular-nums;
        }
        .technical { white-space: nowrap; }
        .pid-list { white-space: nowrap; }
        .badge {
            display: inline-flex;
            align-items: center;
            min-height: 24px;
            padding: 2px 8px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 600;
            white-space: nowrap;
        }
        .badge.success { background: var(--success-soft); color: var(--success); }
        .badge.warning { background: var(--warning-soft); color: var(--warning); }
        .badge.danger { background: var(--danger-soft); color: var(--danger); }
        .badge.muted { background: var(--surface-hover); color: var(--muted-strong); }
        .empty {
            padding: 42px 20px;
            color: var(--muted);
            text-align: center;
        }
        .failed-help {
            margin: 0;
            padding: 12px 20px;
            border-bottom: 1px solid var(--border);
            background: var(--surface-muted);
            color: var(--muted);
            font-size: 13px;
        }
        .configuration-disclosure summary {
            min-height: 48px;
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 12px 20px;
            color: var(--muted-strong);
            font-weight: 600;
            cursor: pointer;
        }
        .configuration-disclosure summary::marker { color: var(--brand); }
        .footer {
            padding-top: 26px;
            color: var(--muted);
            font-size: 12px;
            text-align: right;
        }
        .sr-only {
            position: absolute;
            width: 1px;
            height: 1px;
            padding: 0;
            margin: -1px;
            overflow: hidden;
            clip: rect(0, 0, 0, 0);
            white-space: nowrap;
            border: 0;
        }

        @media (max-width: 1040px) {
            .app { width: min(100% - 28px, 980px); }
            .layout { grid-template-columns: 164px minmax(0, 1fr); gap: 22px; }
            .metric { padding: 18px; }
        }

        @media (max-width: 820px) {
            .topbar { align-items: flex-start; padding: 18px 0; }
            .layout { display: block; padding-top: 18px; }
            .sidebar nav { position: static; }
            .nav-list {
                grid-auto-flow: column;
                grid-auto-columns: max-content;
                display: grid;
                gap: 4px;
                margin: 0 -14px 22px;
                padding: 0 14px 8px;
                overflow-x: auto;
            }
            .content { width: 100%; }
            .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .metric { border-top: 1px solid var(--border); border-left: 1px solid var(--border); }
            .metric:nth-child(2n + 1) { border-left: 0; }
            .metric:nth-child(-n + 2) { border-top: 0; }
            .instance-meta { grid-template-columns: 1fr 1fr; }
            .instance-meta > :first-child { grid-column: 1 / -1; }
        }

        @media (max-width: 600px) {
            .app { width: min(100% - 24px, 560px); }
            .topbar { display: block; }
            .topbar-meta { justify-content: space-between; margin-top: 14px; }
            .card-header { align-items: flex-start; flex-direction: column; }
            .controls { width: 100%; justify-content: stretch; }
            .controls form { flex: 1 1 100%; }
            .button { width: 100%; min-height: 44px; }
            .instance-meta { grid-template-columns: 1fr; }
            .instance-meta > :first-child { grid-column: auto; }
        }

        @media (max-width: 430px) {
            .topbar-meta { align-items: flex-start; flex-direction: column; gap: 8px; }
            .metrics { grid-template-columns: 1fr; }
            .metric { border-top: 1px solid var(--border); border-left: 0; }
            .metric:first-child { border-top: 0; }
            .nav-icon { display: none; }
        }

        @media (prefers-reduced-motion: reduce) {
            html { scroll-behavior: auto; }
        }

        @media print {
            .sidebar, .controls, .skip-link { display: none; }
            .app { width: 100%; }
            .layout { display: block; }
            .card { box-shadow: none; border: 1px solid var(--border); break-inside: avoid; }
        }
    </style>
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
    <header class="topbar">
        <a class="brand" href="{{ $refreshUrl }}" aria-label="Queen Supervisor dashboard">
            <svg class="brand-mark" viewBox="2.01 2.01 255.99 255.99" aria-hidden="true" focusable="false">
                <g fill="currentColor">
                    <path d="M166.295 209.709A105 105 0 1 1 209.709 166.295l-34.434-18.31a66 66 0 1 0-27.29 27.29Z"/>
                    <g transform="translate(43.134 43.134) translate(177.458 177.458) scale(.85) translate(-177.458 -177.458)">
                        <path d="M209.709 166.295a105 105 0 0 1-43.414 43.414l-18.31-34.434a66 66 0 0 0 27.29-27.29Z"/>
                    </g>
                </g>
            </svg>
            <span>
                <span class="brand-name"><strong>Queen</strong> <span>Supervisor</span></span>
                <span class="brand-context">Laravel control plane</span>
            </span>
        </a>

        <div class="topbar-meta">
            <span>
                Updated
                @if ($supervisor['updated_at'])
                    <time datetime="{{ $supervisor['updated_at'] }}">{{ $supervisor['age_seconds'] }}s ago</time>
                @else
                    unavailable
                @endif
            </span>
            <span class="operational-state tone-{{ $stateTone }}">
                <span class="state-mark" aria-hidden="true"></span>{{ $stateLabel }}
            </span>
        </div>
    </header>

    <div class="layout">
        <aside class="sidebar">
            <nav aria-label="Dashboard sections">
                <ul class="nav-list">
                    <li>
                        <a class="nav-link" href="#overview" aria-current="page">
                            <svg class="nav-icon" viewBox="0 0 20 20" aria-hidden="true"><rect x="2.5" y="2.5" width="6" height="6" rx="1"/><rect x="11.5" y="2.5" width="6" height="6" rx="1"/><rect x="2.5" y="11.5" width="6" height="6" rx="1"/><rect x="11.5" y="11.5" width="6" height="6" rx="1"/></svg>
                            Overview
                        </a>
                    </li>
                    <li>
                        <a class="nav-link" href="#workload">
                            <svg class="nav-icon" viewBox="0 0 20 20" aria-hidden="true"><path d="M3 15.5h14M4.5 13V8.5M8.2 13V4.5M11.8 13V7M15.5 13V3"/></svg>
                            Workload
                        </a>
                    </li>
                    <li>
                        <a class="nav-link" href="#supervisors">
                            <svg class="nav-icon" viewBox="0 0 20 20" aria-hidden="true"><rect x="2.5" y="3" width="15" height="5" rx="1.5"/><rect x="2.5" y="12" width="15" height="5" rx="1.5"/><path d="M5.5 5.5h.01M5.5 14.5h.01M9 5.5h5M9 14.5h5"/></svg>
                            Supervisors
                        </a>
                    </li>
                    <li>
                        <a class="nav-link" href="#failed-jobs">
                            <svg class="nav-icon" viewBox="0 0 20 20" aria-hidden="true"><circle cx="10" cy="10" r="7.5"/><path d="M10 5.8v4.8M10 14.1h.01"/></svg>
                            Failed jobs
                        </a>
                    </li>
                    <li>
                        <a class="nav-link" href="#configuration">
                            <svg class="nav-icon" viewBox="0 0 20 20" aria-hidden="true"><path d="M3 5.5h8M14 5.5h3M3 10h3M9 10h8M3 14.5h7M13 14.5h4"/><circle cx="12.5" cy="5.5" r="1.5"/><circle cx="7.5" cy="10" r="1.5"/><circle cx="11.5" cy="14.5" r="1.5"/></svg>
                            Configuration
                        </a>
                    </li>
                </ul>
            </nav>
        </aside>

        <main id="main-content" class="content" tabindex="-1">
            <div class="page-heading">
                <h1>Overview</h1>
                <p>Current state of this application's local Queen worker supervisor.</p>
            </div>

            @if ($controlStatus)
                <div class="notice" role="status">{{ $controlStatus }}</div>
            @endif
            @if ($controlError)
                <div class="notice error" role="alert">{{ $controlError }}</div>
            @endif
            @if ($supervisor['availability'] !== 'live')
                <div class="availability-notice" role="status">
                    <span class="state-mark" aria-hidden="true"></span>
                    <div>
                        <strong>Supervisor {{ strtolower($stateLabel) }}</strong>
                        <span>
                            @if ($supervisor['availability'] === 'stale')
                                The last published generation is no longer live. Controls remain disabled until a current heartbeat is available.
                            @else
                                Start a Queen supervisor to publish local worker state and enable controls.
                            @endif
                        </span>
                    </div>
                </div>
            @endif

            <section id="overview" class="card" aria-labelledby="overview-title">
                <div class="card-header">
                    <h2 id="overview-title">Overview</h2>
                    <span class="header-meta">Generated <time datetime="{{ $snapshot['generated_at'] }}">{{ $snapshot['generated_at'] }}</time></span>
                </div>
                <dl class="metrics">
                    <div class="metric">
                        <dt>Processes</dt>
                        <dd>{{ number_format($supervisor['workers']) }}</dd>
                        <small>Across all worker pools</small>
                    </div>
                    <div class="metric">
                        <dt>Queued jobs</dt>
                        <dd>{{ $depthLabel }}</dd>
                        <small>{{ $unknownDepths > 0 ? $unknownDepths . ' queue depth' . ($unknownDepths === 1 ? '' : 's') . ' unavailable' : 'Latest sampled depth' }}</small>
                    </div>
                    <div class="metric">
                        <dt>Failed jobs</dt>
                        <dd>{{ $failedLabel }}</dd>
                        <small>{{ ($failedJobs['available'] ?? false) ? 'Laravel failed-job store' : 'Backend unavailable' }}</small>
                    </div>
                    <div class="metric">
                        <dt>Status</dt>
                        <dd>
                            <span class="operational-state tone-{{ $stateTone }}">
                                <span class="state-mark" aria-hidden="true"></span>{{ $stateLabel }}
                            </span>
                        </dd>
                        <small>{{ $supervisor['availability'] }}</small>
                    </div>
                    <div class="metric">
                        <dt>Engine</dt>
                        <dd>{{ $supervisor['engine'] ?? '—' }}</dd>
                        <small>Local master process</small>
                    </div>
                    <div class="metric">
                        <dt>Worker pools</dt>
                        <dd>{{ number_format($poolCount) }}</dd>
                        <small>Configured queue allocations</small>
                    </div>
                    <div class="metric">
                        <dt>Draining</dt>
                        <dd>{{ number_format($supervisor['draining']) }}</dd>
                        <small>Processes exiting gracefully</small>
                    </div>
                    <div class="metric">
                        <dt>Heartbeat</dt>
                        <dd>{{ $supervisor['age_seconds'] === null ? '—' : $supervisor['age_seconds'] . 's' }}</dd>
                        <small>Age of local state</small>
                    </div>
                </dl>
            </section>

            <section id="workload" class="card" aria-labelledby="workload-title">
                <div class="card-header">
                    <div>
                        <h2 id="workload-title">Current workload</h2>
                        <p>Depth is scoped by connection, consumer group and queue.</p>
                    </div>
                    <span class="header-meta">{{ count($queues) }} {{ count($queues) === 1 ? 'queue' : 'queues' }}</span>
                </div>
                @if ($queues === [])
                    <div class="empty">No configured Queen queues.</div>
                @else
                    <div class="table-wrap" role="region" aria-label="Current workload table" tabindex="0">
                        <table>
                            <thead><tr><th scope="col">Queue</th><th scope="col">Consumer group</th><th scope="col">Connection</th><th scope="col" class="number">Queued jobs</th><th scope="col">Backend</th></tr></thead>
                            <tbody>
                            @foreach ($queues as $queue)
                                <tr>
                                    <td><strong>{{ $queue['queue'] }}</strong></td>
                                    <td class="technical">{{ $queue['consumer_group'] }}</td>
                                    <td>{{ $queue['connection'] }}</td>
                                    <td class="number">{{ $queue['depth'] ?? '—' }}</td>
                                    <td><span class="badge {{ $queue['available'] ? 'success' : 'warning' }}">{{ $queue['available'] ? 'Available' : 'Unavailable' }}</span></td>
                                </tr>
                            @endforeach
                            </tbody>
                        </table>
                    </div>
                @endif
            </section>

            <section id="supervisors" class="card" aria-labelledby="supervisors-title">
                <div class="card-header">
                    <div>
                        <h2 id="supervisors-title">Supervisor instance</h2>
                        <p>{{ ucfirst($supervisor['engine'] ?? 'unknown') }} master · {{ $stateLabel }}</p>
                    </div>
                    <div class="controls" aria-label="Supervisor controls">
                        <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'pause'], false) }}">
                            @csrf
                            <input type="hidden" name="instance_id" value="{{ $supervisor['instance_id'] }}">
                            <button class="button" type="submit" @disabled($supervisor['availability'] !== 'live' || $supervisor['state'] === 'paused')>Pause</button>
                        </form>
                        <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'continue'], false) }}">
                            @csrf
                            <input type="hidden" name="instance_id" value="{{ $supervisor['instance_id'] }}">
                            <button class="button primary" type="submit" @disabled($supervisor['availability'] !== 'live' || $supervisor['state'] !== 'paused')>Continue</button>
                        </form>
                        <form method="post" action="{{ route('queen.dashboard.control', ['command' => 'terminate'], false) }}">
                            @csrf
                            <input type="hidden" name="instance_id" value="{{ $supervisor['instance_id'] }}">
                            <button class="button danger" type="submit" @disabled($supervisor['availability'] !== 'live')>Terminate</button>
                        </form>
                    </div>
                </div>
                <div class="instance-meta">
                    <div><span class="meta-label">Instance ID</span><code>{{ $supervisor['instance_id'] ?? 'No active instance' }}</code></div>
                    <div><span class="meta-label">Master PID</span><span class="meta-value">{{ $supervisor['pid'] ?? '—' }}</span></div>
                    <div><span class="meta-label">Last heartbeat</span><span class="meta-value">{{ $supervisor['updated_at'] ?? 'Unavailable' }}</span></div>
                </div>

                <h3 class="subsection-title">Worker pools</h3>
                @if ($supervisor['pools'] === [])
                    <div class="empty">No pool state is available.</div>
                @else
                    <div class="table-wrap" role="region" aria-label="Worker pools table" tabindex="0">
                        <table>
                            <thead><tr><th scope="col">Supervisor</th><th scope="col">Queue</th><th scope="col" class="number">Running / desired</th><th scope="col" class="number">Draining</th><th scope="col">Restart</th><th scope="col">PIDs</th></tr></thead>
                            <tbody>
                            @foreach ($supervisor['pools'] as $pool)
                                @php
                                    $visiblePids = array_slice($pool['pids'], 0, 4);
                                    $hiddenPids = max(0, count($pool['pids']) - count($visiblePids));
                                    $restartTone = match ($pool['restart_state']) {
                                        'open' => 'danger',
                                        'backoff', 'probe' => 'warning',
                                        default => 'success',
                                    };
                                    $restartLabel = $pool['restart_state'] === 'closed' && $pool['restart_failures'] === 0
                                        ? 'Healthy'
                                        : ucfirst($pool['restart_state']);
                                @endphp
                                <tr>
                                    <td><strong>{{ $pool['supervisor'] }}</strong></td>
                                    <td>{{ $pool['queue'] }}</td>
                                    <td class="number">{{ $pool['processes'] }} / {{ $pool['desired'] }}</td>
                                    <td class="number">{{ $pool['draining'] }}</td>
                                    <td>
                                        <span class="badge {{ $restartTone }}">{{ $restartLabel }}</span>
                                        @if ($pool['restart_in_seconds'] !== null)
                                            <span class="header-meta">in {{ $pool['restart_in_seconds'] }}s</span>
                                        @endif
                                    </td>
                                    <td class="pid-list"><code>{{ $visiblePids === [] ? '—' : implode(', ', $visiblePids) }}@if($hiddenPids > 0) +{{ $hiddenPids }}@endif</code></td>
                                </tr>
                            @endforeach
                            </tbody>
                        </table>
                    </div>
                @endif
            </section>

            <section id="failed-jobs" class="card" aria-labelledby="failed-jobs-title">
                <div class="card-header">
                    <div>
                        <h2 id="failed-jobs-title">Failed jobs</h2>
                        <p>Bounded metadata from Laravel's configured failed-job store.</p>
                    </div>
                    <span class="header-meta">{{ ($failedJobs['available'] ?? false) ? $failedLabel . ' total' : 'Backend unavailable' }}</span>
                </div>
                <p class="failed-help">Use Laravel's queue commands to retry, forget, flush or prune jobs. This dashboard never displays payloads or exception bodies.</p>
                @if (!($failedJobs['available'] ?? false))
                    <div class="empty"><span class="badge warning">Unavailable</span> Failed-job metadata could not be read safely.</div>
                @elseif ($failedJobs['items'] === [])
                    <div class="empty">No failed jobs.</div>
                @else
                    <div class="table-wrap" role="region" aria-label="Failed jobs table" tabindex="0">
                        <table>
                            <thead><tr><th scope="col">ID</th><th scope="col">Connection</th><th scope="col">Queue</th><th scope="col">Index policy</th><th scope="col">Failed at</th></tr></thead>
                            <tbody>
                            @foreach ($failedJobs['items'] as $failed)
                                <tr><td><code>{{ $failed['id'] }}</code></td><td>{{ $failed['connection'] ?? '—' }}</td><td>{{ $failed['queue'] ?? '—' }}</td><td><span class="badge muted">{{ $failed['lifecycle_policy'] }}</span></td><td class="technical">{{ $failed['failed_at'] ?? '—' }}</td></tr>
                            @endforeach
                            </tbody>
                        </table>
                    </div>
                @endif
            </section>

            <section id="configuration" class="card" aria-labelledby="configuration-title">
                <div class="card-header">
                    <div>
                        <h2 id="configuration-title">Resolved configuration</h2>
                        <p>Safe worker settings published by the active supervisor generation.</p>
                    </div>
                    <span class="header-meta">Troubleshooting</span>
                </div>
                @if ($snapshot['configuration']['supervisors'] === [])
                    <div class="empty">No configured supervisors.</div>
                @else
                    <details class="configuration-disclosure">
                        <summary>Show worker configuration</summary>
                        <div class="table-wrap" role="region" aria-label="Resolved worker configuration table" tabindex="0">
                            <table>
                                <thead><tr><th scope="col">Name</th><th scope="col">Connection / group</th><th scope="col">Queues</th><th scope="col">Balance</th><th scope="col">Processes</th><th scope="col">Runtime limits</th></tr></thead>
                                <tbody>
                                @foreach ($snapshot['configuration']['supervisors'] as $configured)
                                    <tr>
                                        <td><strong>{{ $configured['name'] }}</strong></td>
                                        <td>{{ $configured['connection'] }} / <span class="technical">{{ $configured['consumer_group'] }}</span></td>
                                        <td>{{ implode(', ', $configured['queues']) }}</td>
                                        <td>{{ $configured['balance'] ?? '—' }} / {{ $configured['strategy'] ?? '—' }}</td>
                                        <td>{{ $configured['processes'] ?? (($configured['min_processes'] ?? '—') . '–' . ($configured['max_processes'] ?? '—')) }}</td>
                                        <td>timeout {{ $configured['timeout'] ?? '—' }}s · retry {{ $configured['retry_after'] ?? '—' }}s · tries {{ $configured['tries'] ?? '—' }} · memory {{ $configured['memory'] ?? '—' }} MB</td>
                                    </tr>
                                @endforeach
                                </tbody>
                            </table>
                        </div>
                    </details>
                @endif
            </section>

            <footer class="footer">Auto-refreshes every {{ $refreshSeconds }} seconds · local supervisor state only</footer>
        </main>
    </div>
</div>
</body>
</html>
